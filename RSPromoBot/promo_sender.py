from __future__ import annotations

import asyncio
import io
import random
from typing import Any
from urllib.parse import urlparse

import aiohttp
import discord

from promo_campaigns import PromoCampaignStore
from promo_queue import PromoQueueStore
from send_log_store import SendLogStore
from utils import build_dm_embeds, format_log_user_id, iso_now, next_run_iso, parse_attachment_urls, utc_now


class OptOutButton(discord.ui.Button):
    def __init__(self, bot: discord.Client, config: dict[str, Any], logger) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            emoji="🔕",
            custom_id="rspromobot:notify_off",
        )
        self._bot = bot
        self._config = config
        self._logger = logger

    async def callback(self, interaction: discord.Interaction) -> None:
        guild_id_raw = str(self._config.get("guild_id", "")).strip()
        role_id_raw = str(self._config.get("notify_role_id", "")).strip()
        if not guild_id_raw.isdigit() or not role_id_raw.isdigit():
            await interaction.response.send_message("Opt-out is not configured. Please contact staff.", ephemeral=True)
            return

        guild = self._bot.get_guild(int(guild_id_raw))
        if guild is None:
            try:
                guild = await self._bot.fetch_guild(int(guild_id_raw))
            except Exception:
                guild = None
        if guild is None:
            await interaction.response.send_message("Unable to locate the server to opt you out.", ephemeral=True)
            return

        role = guild.get_role(int(role_id_raw))
        if role is None:
            await interaction.response.send_message("Opt-out role was not found. Please contact staff.", ephemeral=True)
            return

        member: discord.Member | None = None
        if isinstance(interaction.user, discord.User | discord.Member):
            member = guild.get_member(interaction.user.id)
            if member is None:
                try:
                    member = await guild.fetch_member(interaction.user.id)
                except Exception:
                    member = None
        if member is None:
            await interaction.response.send_message("Unable to update your notification role.", ephemeral=True)
            return

        if role not in member.roles:
            await interaction.response.send_message("You're already opted out.", ephemeral=True)
            try:
                self.disabled = True
                await interaction.message.edit(view=self.view)
            except Exception:
                pass
            return

        try:
            await member.remove_roles(role, reason="User opted out via DM button")
        except discord.Forbidden:
            await interaction.response.send_message("I don't have permission to remove that role.", ephemeral=True)
            return
        except Exception as exc:
            self._logger.warning("dm_optout_failed %s error=%s", format_log_user_id(interaction.user.id), exc)
            await interaction.response.send_message("Something went wrong opting you out.", ephemeral=True)
            return

        await interaction.response.send_message("Opted out. You won't receive promo DMs anymore.", ephemeral=True)
        try:
            self.disabled = True
            await interaction.message.edit(view=self.view)
        except Exception:
            pass


class PromoSender:
    def __init__(
        self,
        bot: discord.Client,
        config: dict[str, Any],
        messages: dict[str, Any],
        campaign_store: PromoCampaignStore,
        queue_store: PromoQueueStore,
        send_log_store: SendLogStore,
        logger,
    ) -> None:
        self.bot = bot
        self.config = config
        self.messages = messages
        self.campaign_store = campaign_store
        self.queue_store = queue_store
        self.send_log_store = send_log_store
        self.logger = logger
        self._loop_task: asyncio.Task | None = None

    async def build_attachment_files_payload(self, campaign: dict[str, Any]) -> list[tuple[str, bytes]]:
        attachment_urls = parse_attachment_urls(campaign.get("attachment_urls"), max_urls=2)
        if not attachment_urls:
            return []

        files_payload: list[tuple[str, bytes]] = []
        timeout_seconds = max(5, int(self.config.get("send_timeout_seconds", 30)))

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_seconds)) as session:
            for index, url in enumerate(attachment_urls, start=1):
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            self.logger.warning("attachment_fetch_failed url=%s status=%s", url, resp.status)
                            continue
                        payload = await resp.read()
                        if not payload:
                            self.logger.warning("attachment_fetch_failed url=%s reason=empty_payload", url)
                            continue
                except Exception as exc:
                    self.logger.warning("attachment_fetch_failed url=%s error=%s", url, exc)
                    continue

                parsed = urlparse(url)
                file_name = (parsed.path.rsplit("/", 1)[-1] or f"attachment_{index}.png").split("?", 1)[0]
                if "." not in file_name:
                    file_name = f"{file_name}.png"
                file_name = f"promo_attachment_{index}_{file_name}"

                files_payload.append((file_name, payload))

        return files_payload

    @staticmethod
    def build_discord_files(files_payload: list[tuple[str, bytes]]) -> list[discord.File]:
        return [discord.File(io.BytesIO(content), filename=name) for name, content in files_payload]

    def start(self) -> None:
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.create_task(self._runner_loop())

    async def _runner_loop(self) -> None:
        interval = max(5, int(self.config["status_update_interval_seconds"]))
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self.process_once()
            except Exception as exc:  # pragma: no cover
                self.logger.exception("Promo sender loop error: %s", exc)
            await asyncio.sleep(interval)

    async def process_once(self) -> None:
        queue = self.queue_store.get()
        if queue.get("status") != "running":
            return

        campaign_id = queue.get("campaign_id", "")
        next_run_at = queue.get("next_run_at")
        if next_run_at:
            from utils import parse_iso
            target_dt = parse_iso(next_run_at)
            if target_dt and utc_now() < target_dt:
                try:
                    await self.bot.update_live_status(campaign_id)
                except Exception as exc:
                    self.logger.debug("update_live_status(waiting): %s", exc)
                return

        campaign = self.campaign_store.get(campaign_id)
        if not campaign:
            self.logger.warning("Queue references missing campaign: %s", campaign_id)
            return

        recipient_records = queue.get("recipients", [])
        pending = [item for item in recipient_records if item.get("status") == "pending"]
        if not pending:
            self.logger.info("campaign_completed campaign_id=%s sent=%s failed=%s", campaign_id, queue.get("sent_count", 0), queue.get("failed_count", 0))
            queue["status"] = "completed"
            queue["next_run_at"] = ""
            queue["last_run_at"] = iso_now()
            self.queue_store.save(queue)
            campaign["status"] = "completed"
            campaign["completed_at"] = iso_now()
            self.campaign_store.upsert(campaign)
            await self._log_to_channel(self.messages.get("log_completed", "Campaign completed."), campaign)
            return

        batch_size = int(campaign["batch_size"])
        batch = pending[:batch_size]
        timeout_seconds = int(self.config["send_timeout_seconds"])
        dm_delay_min_seconds = float(self.config["dm_delay_min_seconds"])
        dm_delay_max_seconds = float(self.config["dm_delay_max_seconds"])
        dm_delay_lo = min(dm_delay_min_seconds, dm_delay_max_seconds)
        dm_delay_hi = max(dm_delay_min_seconds, dm_delay_max_seconds)
        send_view = self._build_send_view(campaign)
        embed_color = int(self.config["embed_color"])
        dm_embeds = build_dm_embeds(campaign, embed_color)
        attachment_files_payload = await self.build_attachment_files_payload(campaign)

        self.logger.info("send_batch_start campaign_id=%s batch_size=%d pending_total=%d", campaign_id, len(batch), len(pending))
        for recipient in batch:
            user_id = int(recipient["user_id"])
            try:
                user = await asyncio.wait_for(self.bot.fetch_user(user_id), timeout=timeout_seconds)
                files = self.build_discord_files(attachment_files_payload)
                await asyncio.wait_for(user.send(embeds=dm_embeds, files=files, view=send_view), timeout=timeout_seconds)
                recipient["status"] = "sent"
                recipient["sent_at"] = iso_now()
                queue["sent_count"] = int(queue.get("sent_count", 0)) + 1
                campaign["sent_count"] = int(campaign.get("sent_count", 0)) + 1
                self.send_log_store.append({
                    "campaign_id": campaign_id,
                    "user_id": str(user_id),
                    "status": "sent",
                    "timestamp": iso_now(),
                    "error": ""
                })
                self.logger.info("send_ok campaign_id=%s %s", campaign_id, format_log_user_id(user_id))
            except Exception as exc:
                recipient["status"] = "failed"
                recipient["sent_at"] = iso_now()
                recipient["error"] = str(exc)
                queue["failed_count"] = int(queue.get("failed_count", 0)) + 1
                campaign["failed_count"] = int(campaign.get("failed_count", 0)) + 1
                self.send_log_store.append({
                    "campaign_id": campaign_id,
                    "user_id": str(user_id),
                    "status": "failed",
                    "timestamp": iso_now(),
                    "error": str(exc)
                })
                self.logger.warning("send_failed campaign_id=%s %s error=%s", campaign_id, format_log_user_id(user_id), exc)
            if dm_delay_hi > 0:
                await asyncio.sleep(random.uniform(max(0.0, dm_delay_lo), dm_delay_hi))

        queue["recipients"] = recipient_records
        queue["pending_count"] = len([item for item in recipient_records if item.get("status") == "pending"])
        queue["last_run_at"] = iso_now()
        if queue["pending_count"] > 0:
            queue["next_run_at"] = next_run_iso(int(campaign["batch_interval_minutes"]))
            self.logger.info("send_batch_done campaign_id=%s next_batch_at=%s pending=%d", campaign_id, queue["next_run_at"], queue["pending_count"])
        else:
            queue["next_run_at"] = ""
            queue["status"] = "completed"
            campaign["status"] = "completed"
            campaign["completed_at"] = iso_now()
            self.logger.info("campaign_completed campaign_id=%s sent=%s failed=%s", campaign_id, queue.get("sent_count", 0), queue.get("failed_count", 0))
            await self._log_to_channel(self.messages.get("log_completed", "Campaign completed."), campaign)

        self.queue_store.save(queue)
        self.campaign_store.upsert(campaign)

        try:
            await self.bot.update_live_status(campaign_id)
        except Exception as exc:
            self.logger.debug("update_live_status: %s", exc)

    def _build_send_view(self, campaign: dict[str, Any]) -> discord.ui.View | None:
        label = campaign.get("cta_label", "").strip()
        url = campaign.get("cta_url", "").strip()
        notify_role_id = str(self.config.get("notify_role_id", "")).strip()

        if not (label and url) and not notify_role_id.isdigit():
            return None

        view = discord.ui.View(timeout=None)
        if label and url:
            view.add_item(discord.ui.Button(label=label, url=url))
        if notify_role_id.isdigit():
            view.add_item(OptOutButton(self.bot, self.config, self.logger))
        return view

    async def _log_to_channel(self, message: str, campaign: dict[str, Any]) -> None:
        channel_id_raw = str(self.config.get("log_channel_id", "")).strip()
        if not channel_id_raw or not channel_id_raw.isdigit():
            return
        channel = self.bot.get_channel(int(channel_id_raw))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(channel_id_raw))
            except Exception:
                channel = None
        if channel is None:
            return
        embed = discord.Embed(
            title=f"📣 {campaign.get('campaign_name', 'Promo Campaign')}",
            description=message,
            color=int(self.config["embed_color"]),
        )
        embed.add_field(name="Campaign ID", value=campaign["campaign_id"], inline=False)
        embed.add_field(name="Status", value=campaign.get("status", "unknown"), inline=True)
        embed.add_field(name="Recipients", value=str(campaign.get("recipient_count", 0)), inline=True)
        embed.add_field(name="Sent", value=str(campaign.get("sent_count", 0)), inline=True)
        embed.add_field(name="Failed", value=str(campaign.get("failed_count", 0)), inline=True)
        await channel.send(embed=embed)
