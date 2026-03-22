from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiohttp
import discord
from discord import app_commands
from dotenv import load_dotenv
import os

from config_loader import load_config, load_json_file
from promo_campaigns import PromoCampaignStore
from promo_logging import configure_logging
from promo_queue import PromoQueueStore
from promo_sender import PromoSender
from promo_sessions import PromoSessionStore
from promo_views import CampaignControlView, CampaignReuseView, PromoBuilderView
from send_log_store import SendLogStore
from storage import JSONStorage
from utils import estimated_duration_str, format_log_user_id, has_any_allowed_role, human_rate, iso_now, parse_iso, utc_now


BASE_DIR = Path(__file__).resolve().parent


class PromoBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

        self.config = load_config(BASE_DIR)
        self.messages = load_json_file(BASE_DIR / "messages.json")
        self.storage = JSONStorage(BASE_DIR / self.config["data_dir"])
        self.session_store = PromoSessionStore(self.storage)
        self.queue_store = PromoQueueStore(self.storage)
        self.campaign_store = PromoCampaignStore(self.storage)
        self.send_log_store = SendLogStore(self.storage)
        self.logger = configure_logging(BASE_DIR / self.config["logs_dir"])
        self.sender = PromoSender(
            self,
            self.config,
            self.messages,
            self.campaign_store,
            self.queue_store,
            self.send_log_store,
            self.logger,
        )
        self.register_commands()

    async def setup_hook(self) -> None:
        guild_obj = discord.Object(id=int(self.config["guild_id"]))
        self.tree.copy_global_to(guild=guild_obj)
        await self.tree.sync(guild=guild_obj)
        self.logger.info("slash_commands_synced Guild-ID=%s", self.config["guild_id"])
        self.sender.start()

    async def on_ready(self) -> None:
        self.logger.info("bot_ready %s Guild-ID=%s", format_log_user_id(self.user.id), self.config["guild_id"])

    def register_commands(self) -> None:
        guild_obj = discord.Object(id=int(self.config["guild_id"]))

        @self.tree.command(name="promo_dm", description="Open the promo DM builder.", guild=guild_obj)
        async def promo_dm(interaction: discord.Interaction) -> None:
            if not self.user_can_manage(interaction):
                self.logger.info("command=promo_dm denied %s username=%s Guild-ID=%s", format_log_user_id(interaction.user.id), getattr(interaction.user, "name", ""), interaction.guild_id)
                await interaction.response.send_message(self.messages["permission_error"], ephemeral=True)
                return
            self.logger.info("command=promo_dm %s username=%s Guild-ID=%s", format_log_user_id(interaction.user.id), getattr(interaction.user, "name", ""), interaction.guild_id)
            session = self.session_store.get(interaction.guild_id, interaction.user.id)
            if not session:
                session = self.session_store.build_default(interaction.guild_id, interaction.user.id, self.config)
                self.session_store.upsert(interaction.guild_id, interaction.user.id, session)
            embed = self.build_builder_embed(interaction.guild, session)
            await interaction.response.send_message(embed=embed, view=PromoBuilderView(self, session), ephemeral=True)

        @self.tree.command(name="promo_status", description="Show the current promo campaign status.", guild=guild_obj)
        async def promo_status(interaction: discord.Interaction) -> None:
            if not self.user_can_manage(interaction):
                self.logger.info("command=promo_status denied %s Guild-ID=%s", format_log_user_id(interaction.user.id), interaction.guild_id)
                await interaction.response.send_message(self.messages["permission_error"], ephemeral=True)
                return
            self.logger.info("command=promo_status %s Guild-ID=%s", format_log_user_id(interaction.user.id), interaction.guild_id)
            queue = self.queue_store.get()
            campaign_id = queue.get("campaign_id", "")
            campaign = self.campaign_store.get(campaign_id) if campaign_id else None
            if not campaign:
                recent = self.campaign_store.list_recent(limit=1)
                campaign = recent[0] if recent else None
            if not campaign:
                await interaction.response.send_message(self.messages["status_none"], ephemeral=True)
                return
            embed = self.build_status_embed(interaction.guild, campaign, queue)
            await interaction.response.send_message(embed=embed, view=CampaignControlView(self, campaign["campaign_id"]), ephemeral=True)
            queue["interaction_token"] = interaction.token
            self.queue_store.save(queue)

        @self.tree.command(name="promo_history", description="Show recent promo campaigns.", guild=guild_obj)
        async def promo_history(interaction: discord.Interaction) -> None:
            if not self.user_can_manage(interaction):
                self.logger.info("command=promo_history denied %s Guild-ID=%s", format_log_user_id(interaction.user.id), interaction.guild_id)
                await interaction.response.send_message(self.messages["permission_error"], ephemeral=True)
                return
            self.logger.info("command=promo_history %s Guild-ID=%s", format_log_user_id(interaction.user.id), interaction.guild_id)
            campaigns = self.campaign_store.list_recent(limit=10)
            if not campaigns:
                await interaction.response.send_message(self.messages.get("history_empty", "No campaign history yet."), ephemeral=True)
                return
            embed = discord.Embed(title="🗂️ Recent Promo Campaigns", color=int(self.config["embed_color"]))
            embed.set_footer(text=self.messages.get("history_reuse_footer", "Use the dropdown below to reuse a campaign in the builder."))
            for campaign in campaigns:
                embed.add_field(
                    name=campaign.get("campaign_name", campaign["campaign_id"]),
                    value=(
                        f"ID: `{campaign['campaign_id']}`\n"
                        f"Status: **{campaign.get('status', 'unknown')}**\n"
                        f"Recipients: **{campaign.get('recipient_count', 0)}**\n"
                        f"Sent: **{campaign.get('sent_count', 0)}**\n"
                        f"Failed: **{campaign.get('failed_count', 0)}**"
                    ),
                    inline=False,
                )
            await interaction.response.send_message(embed=embed, view=CampaignReuseView(self, campaigns), ephemeral=True)

        @self.tree.command(name="notify_on", description="Opt in to receive promo DMs.", guild=guild_obj)
        async def notify_on(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            if not isinstance(interaction.user, discord.Member):
                await interaction.response.send_message("Unable to update roles for this user.", ephemeral=True)
                return
            role_id_raw = str(self.config.get("notify_role_id", "")).strip()
            if not role_id_raw.isdigit():
                await interaction.response.send_message("Notify role is not configured. Ask staff to set it up.", ephemeral=True)
                return
            role = interaction.guild.get_role(int(role_id_raw))
            if role is None:
                await interaction.response.send_message("Notify role was not found. Ask staff to set it up.", ephemeral=True)
                return
            if role in interaction.user.roles:
                await interaction.response.send_message("You already have notifications enabled.", ephemeral=True)
                return
            try:
                await interaction.user.add_roles(role, reason="User opted in via /notify_on")
            except discord.Forbidden:
                await interaction.response.send_message("I don't have permission to add that role.", ephemeral=True)
                return
            except Exception as exc:
                self.logger.warning("notify_on failed %s error=%s", format_log_user_id(interaction.user.id), exc)
                await interaction.response.send_message("Something went wrong enabling notifications.", ephemeral=True)
                return
            await interaction.response.send_message("Notifications enabled. You'll receive promo DMs from this server.", ephemeral=True)

        @self.tree.command(name="notify_off", description="Opt out of promo DMs.", guild=guild_obj)
        async def notify_off(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return
            if not isinstance(interaction.user, discord.Member):
                await interaction.response.send_message("Unable to update roles for this user.", ephemeral=True)
                return
            role_id_raw = str(self.config.get("notify_role_id", "")).strip()
            if not role_id_raw.isdigit():
                await interaction.response.send_message("Notify role is not configured. Ask staff to set it up.", ephemeral=True)
                return
            role = interaction.guild.get_role(int(role_id_raw))
            if role is None:
                await interaction.response.send_message("Notify role was not found. Ask staff to set it up.", ephemeral=True)
                return
            if role not in interaction.user.roles:
                await interaction.response.send_message("Notifications are already disabled.", ephemeral=True)
                return
            try:
                await interaction.user.remove_roles(role, reason="User opted out via /notify_off")
            except discord.Forbidden:
                await interaction.response.send_message("I don't have permission to remove that role.", ephemeral=True)
                return
            except Exception as exc:
                self.logger.warning("notify_off failed %s error=%s", format_log_user_id(interaction.user.id), exc)
                await interaction.response.send_message("Something went wrong disabling notifications.", ephemeral=True)
                return
            await interaction.response.send_message("Notifications disabled. You will no longer receive promo DMs.", ephemeral=True)

        async def _do_campaign_control(interaction: discord.Interaction, action: str) -> None:
            if not self.user_can_manage(interaction):
                self.logger.info("command=promo_%s denied %s Guild-ID=%s", action, format_log_user_id(interaction.user.id), interaction.guild_id)
                await interaction.response.send_message(self.messages["permission_error"], ephemeral=True)
                return
            self.logger.info("command=promo_%s %s Guild-ID=%s", action, format_log_user_id(interaction.user.id), interaction.guild_id)
            queue = self.queue_store.get()
            campaign_id = queue.get("campaign_id", "")
            campaign = self.campaign_store.get(campaign_id) if campaign_id else None
            if not campaign:
                await interaction.response.send_message(self.messages["status_none"], ephemeral=True)
                return
            if action == "pause":
                if queue.get("status") != "running":
                    await interaction.response.send_message(self.messages.get("control_not_running", "Campaign is not running."), ephemeral=True)
                    return
                queue["status"] = "paused"
                campaign["status"] = "paused"
                campaign["paused_at"] = iso_now()
            elif action == "resume":
                if queue.get("status") != "paused":
                    await interaction.response.send_message(self.messages.get("control_not_paused", "Campaign is not paused."), ephemeral=True)
                    return
                queue["status"] = "running"
                queue["next_run_at"] = ""
                campaign["status"] = "running"
                self.sender.start()
            elif action == "cancel":
                queue["status"] = "cancelled"
                queue["next_run_at"] = ""
                campaign["status"] = "cancelled"
                campaign["cancelled_at"] = iso_now()
            self.queue_store.save(queue)
            self.campaign_store.upsert(campaign)
            if action in ("pause", "cancel"):
                await self.sender._log_to_channel(
                    self.messages["log_paused"] if action == "pause" else self.messages["log_cancelled"],
                    campaign,
                )
            if action == "resume":
                await self.sender._log_to_channel(self.messages["log_resumed"], campaign)
            embed = self.build_status_embed(interaction.guild, campaign, queue)
            await interaction.response.send_message(embed=embed, view=CampaignControlView(self, campaign_id), ephemeral=True)

        @self.tree.command(name="promo_pause", description="Pause the active promo campaign.", guild=guild_obj)
        async def promo_pause(interaction: discord.Interaction) -> None:
            await _do_campaign_control(interaction, "pause")

        @self.tree.command(name="promo_resume", description="Resume a paused promo campaign.", guild=guild_obj)
        async def promo_resume(interaction: discord.Interaction) -> None:
            await _do_campaign_control(interaction, "resume")

        @self.tree.command(name="promo_cancel", description="Cancel the active promo campaign.", guild=guild_obj)
        async def promo_cancel(interaction: discord.Interaction) -> None:
            await _do_campaign_control(interaction, "cancel")

    def user_can_manage(self, interaction: discord.Interaction) -> bool:
        if interaction.user.guild_permissions.administrator:
            return True
        if not isinstance(interaction.user, discord.Member):
            return False
        return has_any_allowed_role(interaction.user, self.config["allowed_launcher_role_ids"])

    def validate_session(self, session: dict[str, Any]) -> str | None:
        if not session.get("target_role_id"):
            return self.messages["validation_missing_role"]
        if not session.get("campaign_name", "").strip():
            return self.messages["validation_missing_campaign_name"]
        if not session.get("message_body", "").strip():
            return self.messages["validation_missing_message"]
        return None

    def resolve_recipient_ids(self, guild: discord.Guild, session: dict[str, Any]) -> list[int]:
        role_id_raw = str(session.get("target_role_id", "")).strip()
        if not role_id_raw.isdigit():
            return []
        role = guild.get_role(int(role_id_raw))
        if role is None:
            return []

        recipients: list[int] = []
        exclude_bots = bool(self.config["exclude_bots"])
        for member in role.members:
            if exclude_bots and member.bot:
                continue
            recipients.append(member.id)
        return recipients

    def build_builder_embed(self, guild: discord.Guild, session: dict[str, Any]) -> discord.Embed:
        role_display = self._role_display(guild, session.get("target_role_id", ""))
        recipients = self.resolve_recipient_ids(guild, session) if session.get("target_role_id") else []
        embed = discord.Embed(
            title=self.messages["builder_title"],
            description=self.messages["builder_description"],
            color=int(self.config["embed_color"]),
        )
        embed.add_field(name="Campaign Name", value=session.get("campaign_name") or self.messages["panel_not_selected"], inline=False)
        embed.add_field(name="Target Role", value=role_display, inline=True)
        embed.add_field(name="Eligible Recipients", value=str(len(recipients)), inline=True)
        embed.add_field(
            name="Send Rate",
            value=human_rate(int(session["batch_size"]), int(session["batch_interval_minutes"])),
            inline=True,
        )
        message_preview = session.get("message_body", "").strip() or self.messages["panel_no_message"]
        if len(message_preview) > 900:
            message_preview = f"{message_preview[:897]}..."
        embed.add_field(name="Message Preview", value=message_preview, inline=False)
        banner_url = session.get("banner_url", "").strip() or self.messages["panel_not_selected"]
        if len(banner_url) > 60:
            banner_url = banner_url[:57] + "..."
        embed.add_field(name="Banner URL", value=banner_url, inline=False)
        cta_label = session.get("cta_label", "").strip() or "None"
        cta_url = session.get("cta_url", "").strip() or "None"
        embed.add_field(name="CTA Button", value=f"Label: {cta_label}\nURL: {cta_url}", inline=False)
        embed.set_footer(text=self.messages.get("builder_footer", "Nothing has been sent yet."))
        return embed

    def build_confirm_embed(self, guild: discord.Guild, session: dict[str, Any], recipients: list[int]) -> discord.Embed:
        batch_size = int(session["batch_size"])
        interval = int(session["batch_interval_minutes"])
        estimated = estimated_duration_str(len(recipients), batch_size, interval)
        embed = discord.Embed(
            title=self.messages["confirm_title"],
            description=self.messages.get("confirm_description", "Review the frozen recipient snapshot before sending."),
            color=int(self.config["embed_color"]),
        )
        embed.add_field(name="Campaign Name", value=session["campaign_name"], inline=False)
        embed.add_field(name="Target Role", value=self._role_display(guild, session.get("target_role_id", "")), inline=True)
        embed.add_field(name="Recipients", value=str(len(recipients)), inline=True)
        embed.add_field(name="Rate", value=human_rate(batch_size, interval), inline=True)
        embed.add_field(name="Estimated duration", value=estimated, inline=True)
        embed.add_field(name="Message", value=session["message_body"][:1000], inline=False)
        if session.get("cta_label") and session.get("cta_url"):
            embed.add_field(name="CTA", value=f"[{session['cta_label']}]({session['cta_url']})", inline=False)
        embed.set_footer(text=self.messages["confirm_footer"])
        return embed

    def build_status_embed(self, guild: discord.Guild | None, campaign: dict[str, Any], queue: dict[str, Any]) -> discord.Embed:
        embed = discord.Embed(
            title=self.messages["status_title"],
            description=campaign.get("campaign_name", "Promo Campaign"),
            color=int(self.config["embed_color"]),
        )
        role_display = self._role_display(guild, campaign.get("target_role_id", "")) if guild else campaign.get("target_role_id", "Unknown")
        embed.add_field(name="Campaign ID", value=campaign["campaign_id"], inline=False)
        embed.add_field(name="Target Role", value=role_display, inline=True)
        embed.add_field(name="Status", value=campaign.get("status", "unknown"), inline=True)
        embed.add_field(name="Recipients", value=str(campaign.get("recipient_count", 0)), inline=True)
        embed.add_field(name="Sent", value=str(queue.get("sent_count", campaign.get("sent_count", 0))), inline=True)
        embed.add_field(name="Failed", value=str(queue.get("failed_count", campaign.get("failed_count", 0))), inline=True)
        embed.add_field(name="Pending", value=str(queue.get("pending_count", 0)), inline=True)
        embed.add_field(name="Rate", value=human_rate(int(campaign["batch_size"]), int(campaign["batch_interval_minutes"])), inline=True)
        next_run = queue.get("next_run_at", "")
        embed.add_field(name="Next Run", value=next_run or "Immediate / none scheduled", inline=True)
        next_run_in = "Now"
        if next_run:
            target_dt = parse_iso(next_run)
            if target_dt:
                seconds_left = max(0, int((target_dt - utc_now()).total_seconds()))
                if seconds_left <= 0:
                    next_run_in = "Now"
                else:
                    mins, secs = divmod(seconds_left, 60)
                    hours, mins = divmod(mins, 60)
                    if hours > 0:
                        next_run_in = f"{hours}h {mins}m {secs}s"
                    elif mins > 0:
                        next_run_in = f"{mins}m {secs}s"
                    else:
                        next_run_in = f"{secs}s"
        embed.add_field(name="Next Run In", value=next_run_in, inline=True)
        started = campaign.get("started_at", "") or "Not started"
        embed.add_field(name="Started", value=started, inline=True)
        completed = campaign.get("completed_at", "") or "Not completed"
        embed.add_field(name="Completed", value=completed, inline=True)
        preview_text = (campaign.get("message_body", "") or "").strip() or "No message body set."
        if len(preview_text) > 400:
            preview_text = f"{preview_text[:397]}..."
        embed.add_field(name="Preview", value=preview_text, inline=False)
        return embed

    def _role_display(self, guild: discord.Guild | None, role_id: str) -> str:
        if not guild or not str(role_id).isdigit():
            return self.messages["panel_not_selected"]
        role = guild.get_role(int(role_id))
        return role.mention if role else self.messages["panel_not_selected"]

    async def update_live_status(self, campaign_id: str) -> None:
        """Edit the last-shown status message so it reflects current queue/campaign (live update). Token valid ~15 min."""
        queue = self.queue_store.get()
        if queue.get("campaign_id") != campaign_id:
            return
        token = (queue.get("interaction_token") or "").strip()
        if not token:
            return
        campaign = self.campaign_store.get(campaign_id)
        if not campaign:
            return
        guild = self.get_guild(int(queue.get("guild_id", 0)))
        embed = self.build_status_embed(guild, campaign, queue)
        view = CampaignControlView(self, campaign_id)
        payload = {"embeds": [embed.to_dict()], "components": view.to_components()}
        url = f"https://discord.com/api/v10/webhooks/{self.application_id}/{token}/messages/@original"
        bot_token = getattr(self, "_token", None) or os.getenv("DISCORD_TOKEN", "")
        if not bot_token:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.patch(
                    url,
                    json=payload,
                    headers={"Authorization": f"Bot {bot_token}", "Content-Type": "application/json"},
                ) as resp:
                    if resp.status in (401, 404, 403):
                        queue["interaction_token"] = ""
                        self.queue_store.save(queue)
        except Exception as exc:
            self.logger.debug("update_live_status failed: %s", exc)
        if queue.get("status") in ("completed", "cancelled"):
            queue["interaction_token"] = ""
            self.queue_store.save(queue)


def main() -> None:
    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN in .env")
    bot = PromoBot()
    bot._token = token
    bot.run(token)


if __name__ == "__main__":
    main()
