from __future__ import annotations

from typing import Any

import discord
from discord import app_commands

from utils import build_cta_view, build_dm_embeds, format_log_user_id, has_any_allowed_role, human_rate, iso_now


def _session_dict_from_campaign(campaign: dict[str, Any]) -> dict[str, Any]:
    """Subset of session-shaped fields for reusing builder modals on an active campaign."""
    return {
        "campaign_name": campaign.get("campaign_name", ""),
        "message_body": campaign.get("message_body", ""),
        "cta_label": campaign.get("cta_label", ""),
        "cta_url": campaign.get("cta_url", ""),
        "banner_url": campaign.get("banner_url", ""),
        "batch_size": int(campaign.get("batch_size", 5)),
        "batch_interval_minutes": int(campaign.get("batch_interval_minutes", 5)),
    }


class CampaignReuseSelect(discord.ui.Select):
    """Select menu to load a past campaign into the builder for reuse."""

    def __init__(self, bot_ref, campaigns: list[dict[str, Any]]) -> None:
        self.bot_ref = bot_ref
        options = []
        for c in campaigns[:25]:
            name = (c.get("campaign_name") or c["campaign_id"])[:100]
            options.append(discord.SelectOption(label=name, value=c["campaign_id"], description=f"ID: {c['campaign_id']}"))
        super().__init__(
            placeholder=bot_ref.messages.get("history_reuse_placeholder", "Select a campaign to reuse…"),
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        campaign_id = self.values[0]
        campaign = self.bot_ref.campaign_store.get(campaign_id)
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages.get("campaign_not_found", "Campaign not found."), ephemeral=True)
            return
        session = {
            "guild_id": str(interaction.guild_id),
            "user_id": str(interaction.user.id),
            "campaign_name": campaign.get("campaign_name", ""),
            "target_role_id": campaign.get("target_role_id", ""),
            "message_body": campaign.get("message_body", ""),
            "embed_title": campaign.get("embed_title", ""),
            "banner_url": campaign.get("banner_url", ""),
            "cta_label": campaign.get("cta_label", ""),
            "cta_url": campaign.get("cta_url", ""),
            "batch_size": int(campaign.get("batch_size", self.bot_ref.config["default_batch_size"])),
            "batch_interval_minutes": int(campaign.get("batch_interval_minutes", self.bot_ref.config["default_batch_interval_minutes"])),
            "status": "draft",
            "created_at": iso_now(),
            "updated_at": iso_now(),
        }
        self.bot_ref.session_store.upsert(interaction.guild_id, interaction.user.id, session)
        embed = self.bot_ref.build_builder_embed(interaction.guild, session)
        await interaction.response.send_message(
            self.bot_ref.messages.get("history_reuse_loaded", "Campaign loaded into the builder. Edit and run when ready."),
            embed=embed,
            view=PromoBuilderView(self.bot_ref, session),
            ephemeral=True,
        )


class CampaignDeleteSelect(discord.ui.Select):
    """Select menu to delete a campaign from history."""

    def __init__(self, bot_ref, campaigns: list[dict[str, Any]]) -> None:
        self.bot_ref = bot_ref
        self._campaigns = campaigns
        options = []
        for c in campaigns[:25]:
            name = (c.get("campaign_name") or c["campaign_id"])[:100]
            options.append(discord.SelectOption(label=name, value=c["campaign_id"], description=f"ID: {c['campaign_id']}"))
        super().__init__(
            placeholder=bot_ref.messages.get("history_delete_placeholder", "Select a campaign to delete…"),
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        campaign_id = self.values[0]
        removed = self.bot_ref.campaign_store.delete(campaign_id)
        if not removed:
            await interaction.response.send_message(
                self.bot_ref.messages.get("history_delete_not_found", "Campaign not found or already deleted."),
                ephemeral=True,
            )
            return
        queue = self.bot_ref.queue_store.get()
        if queue.get("campaign_id") == campaign_id:
            self.bot_ref.queue_store.save({
                "campaign_id": "",
                "guild_id": queue.get("guild_id", ""),
                "status": "idle",
                "recipients": [],
                "pending_count": 0,
                "sent_count": 0,
                "failed_count": 0,
                "last_run_at": "",
                "next_run_at": "",
            })
        await interaction.response.send_message(
            self.bot_ref.messages.get("history_deleted", "Campaign deleted from history."),
            ephemeral=True,
        )


class CampaignReuseView(discord.ui.View):
    def __init__(self, bot_ref, campaigns: list[dict[str, Any]]) -> None:
        super().__init__(timeout=300)
        self.add_item(CampaignReuseSelect(bot_ref, campaigns))
        self.add_item(CampaignDeleteSelect(bot_ref, campaigns))


class MessageModal(discord.ui.Modal, title="Edit Promo Message"):
    def __init__(self, bot_ref, session: dict[str, Any], status_campaign_id: str | None = None) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot_ref
        self.session = session
        self.status_campaign_id = status_campaign_id

        self.campaign_name = discord.ui.TextInput(
            label="Campaign Name",
            default=session.get("campaign_name", ""),
            max_length=100,
            required=True,
        )
        self.message_body = discord.ui.TextInput(
            label="Message Body",
            default=session.get("message_body", ""),
            style=discord.TextStyle.paragraph,
            max_length=2000,
            required=True,
        )
        self.cta_label = discord.ui.TextInput(
            label="CTA Button Label (optional)",
            default=session.get("cta_label", ""),
            max_length=80,
            required=False,
        )
        self.cta_url = discord.ui.TextInput(
            label="CTA Button URL (optional)",
            default=session.get("cta_url", ""),
            max_length=300,
            required=False,
        )

        self.add_item(self.campaign_name)
        self.add_item(self.message_body)
        self.add_item(self.cta_label)
        self.add_item(self.cta_url)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        self.session["campaign_name"] = str(self.campaign_name.value).strip()
        self.session["message_body"] = str(self.message_body.value).strip()
        self.session["cta_label"] = str(self.cta_label.value).strip()
        self.session["cta_url"] = str(self.cta_url.value).strip()

        if self.status_campaign_id:
            campaign = self.bot_ref.campaign_store.get(self.status_campaign_id)
            queue = self.bot_ref.queue_store.get()
            if not campaign or queue.get("campaign_id") != self.status_campaign_id or queue.get("status") != "paused":
                await interaction.response.send_message(
                    self.bot_ref.messages.get("status_edit_only_when_paused", "Pause the campaign before editing."),
                    ephemeral=True,
                )
                return
            merged = dict(campaign)
            merged["campaign_name"] = self.session["campaign_name"]
            merged["message_body"] = self.session["message_body"]
            merged["cta_label"] = self.session["cta_label"]
            merged["cta_url"] = self.session["cta_url"]
            validation_error = self.bot_ref.validate_session(merged)
            if validation_error:
                await interaction.response.send_message(validation_error, ephemeral=True)
                return
            campaign["campaign_name"] = self.session["campaign_name"]
            campaign["message_body"] = self.session["message_body"]
            campaign["cta_label"] = self.session["cta_label"]
            campaign["cta_url"] = self.session["cta_url"]
            self.bot_ref.campaign_store.upsert(campaign)
            embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue)
            await interaction.response.edit_message(embed=embed, view=CampaignControlView(self.bot_ref, self.status_campaign_id))
            return

        self.session["status"] = "draft"
        self.bot_ref.session_store.upsert(interaction.guild_id, interaction.user.id, self.session)
        embed = self.bot_ref.build_builder_embed(interaction.guild, self.session)
        view = PromoBuilderView(self.bot_ref, self.session)
        await interaction.response.edit_message(embed=embed, view=view)


class SettingsModal(discord.ui.Modal, title="Edit Campaign Settings"):
    def __init__(self, bot_ref, session: dict[str, Any], status_campaign_id: str | None = None) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot_ref
        self.session = session
        self.status_campaign_id = status_campaign_id

        self.batch_size = discord.ui.TextInput(
            label="Batch Size",
            default=str(session.get("batch_size", self.bot_ref.config["default_batch_size"])),
            max_length=2,
            required=True,
        )
        self.batch_interval_minutes = discord.ui.TextInput(
            label="Batch Interval Minutes",
            default=str(session.get("batch_interval_minutes", self.bot_ref.config["default_batch_interval_minutes"])),
            max_length=3,
            required=True,
        )
        self.banner_url = discord.ui.TextInput(
            label="Banner Image URL (optional)",
            default=session.get("banner_url", "") or (self.bot_ref.config.get("default_banner_url") or ""),
            max_length=500,
            required=False,
        )
        self.add_item(self.batch_size)
        self.add_item(self.batch_interval_minutes)
        self.add_item(self.banner_url)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            batch_size = int(str(self.batch_size.value).strip())
            interval = int(str(self.batch_interval_minutes.value).strip())
        except ValueError:
            await interaction.response.send_message(
                self.bot_ref.messages.get("validation_batch_numbers", "Batch size and interval must be numbers."),
                ephemeral=True,
            )
            return

        max_batch = int(self.bot_ref.config["max_batch_size"])
        if batch_size < 1 or batch_size > max_batch:
            msg = self.bot_ref.messages.get("validation_batch_size_range", "Batch size must be between 1 and {max_batch_size}.")
            await interaction.response.send_message(msg.replace("{max_batch_size}", str(max_batch)), ephemeral=True)
            return

        max_interval = int(self.bot_ref.config["max_batch_interval_minutes"])
        if interval < 1 or interval > max_interval:
            msg = self.bot_ref.messages.get("validation_interval_range", "Batch interval must be between 1 and {max_batch_interval_minutes} minutes.")
            await interaction.response.send_message(msg.replace("{max_batch_interval_minutes}", str(max_interval)), ephemeral=True)
            return

        self.session["batch_size"] = batch_size
        self.session["batch_interval_minutes"] = interval
        self.session["banner_url"] = str(self.banner_url.value).strip()

        if self.status_campaign_id:
            campaign = self.bot_ref.campaign_store.get(self.status_campaign_id)
            queue = self.bot_ref.queue_store.get()
            if not campaign or queue.get("campaign_id") != self.status_campaign_id or queue.get("status") != "paused":
                await interaction.response.send_message(
                    self.bot_ref.messages.get("status_edit_only_when_paused", "Pause the campaign before editing."),
                    ephemeral=True,
                )
                return
            campaign["batch_size"] = batch_size
            campaign["batch_interval_minutes"] = interval
            campaign["banner_url"] = self.session["banner_url"]
            self.bot_ref.campaign_store.upsert(campaign)
            embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue)
            await interaction.response.edit_message(embed=embed, view=CampaignControlView(self.bot_ref, self.status_campaign_id))
            return

        self.bot_ref.session_store.upsert(interaction.guild_id, interaction.user.id, self.session)
        embed = self.bot_ref.build_builder_embed(interaction.guild, self.session)
        view = PromoBuilderView(self.bot_ref, self.session)
        await interaction.response.edit_message(embed=embed, view=view)


class RoleSelect(discord.ui.RoleSelect):
    def __init__(self, bot_ref, session: dict[str, Any]) -> None:
        super().__init__(placeholder="Select a target role", min_values=1, max_values=1)
        self.bot_ref = bot_ref
        self.session = session

    async def callback(self, interaction: discord.Interaction) -> None:
        role = self.values[0]
        self.session["target_role_id"] = str(role.id)
        self.bot_ref.session_store.upsert(interaction.guild_id, interaction.user.id, self.session)
        embed = self.bot_ref.build_builder_embed(interaction.guild, self.session)
        view = PromoBuilderView(self.bot_ref, self.session)
        await interaction.response.edit_message(embed=embed, view=view)


class RolePickerView(discord.ui.View):
    def __init__(self, bot_ref, session: dict[str, Any]) -> None:
        super().__init__(timeout=300)
        self.add_item(RoleSelect(bot_ref, session))


class LaunchConfirmView(discord.ui.View):
    def __init__(self, bot_ref, session: dict[str, Any], recipients: list[int]) -> None:
        super().__init__(timeout=300)
        self.bot_ref = bot_ref
        self.session = session
        self.recipients = recipients

    @discord.ui.button(label="Confirm Send", style=discord.ButtonStyle.danger)
    async def confirm_send(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        queue = self.bot_ref.queue_store.get()
        if queue.get("status") in {"running", "paused"}:
            await interaction.response.send_message(self.bot_ref.messages["campaign_already_running"], ephemeral=True)
            return

        campaign = self.bot_ref.campaign_store.create_from_session(
            interaction.guild_id,
            interaction.user.id,
            self.session,
            self.recipients,
        )
        campaign["status"] = "running"
        campaign["started_at"] = iso_now()
        self.bot_ref.campaign_store.upsert(campaign)

        recipient_records = [{"user_id": str(user_id), "status": "pending", "sent_at": "", "error": ""} for user_id in self.recipients]
        queue_payload = {
            "campaign_id": campaign["campaign_id"],
            "guild_id": str(interaction.guild_id),
            "status": "running",
            "recipients": recipient_records,
            "pending_count": len(recipient_records),
            "sent_count": 0,
            "failed_count": 0,
            "last_run_at": "",
            "next_run_at": "",
            "interaction_token": interaction.token,
        }
        self.bot_ref.queue_store.save(queue_payload)
        self.bot_ref.session_store.delete(interaction.guild_id, interaction.user.id)
        self.bot_ref.logger.info("campaign_started campaign_id=%s %s Guild-ID=%s recipients=%d", campaign["campaign_id"], format_log_user_id(interaction.user.id), interaction.guild_id, len(self.recipients))
        self.bot_ref.sender.start()
        status_embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue_payload)
        status_view = CampaignControlView(self.bot_ref, campaign["campaign_id"])
        await interaction.response.edit_message(embed=status_embed, view=status_view)
        await self.bot_ref.sender._log_to_channel(self.bot_ref.messages["log_started"], campaign)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def go_back(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        embed = self.bot_ref.build_builder_embed(interaction.guild, self.session)
        view = PromoBuilderView(self.bot_ref, self.session)
        await interaction.response.edit_message(embed=embed, view=view)


class CampaignControlView(discord.ui.View):
    def __init__(self, bot_ref, campaign_id: str) -> None:
        super().__init__(timeout=600)
        self.bot_ref = bot_ref
        self.campaign_id = campaign_id

    @discord.ui.button(label="Pause", style=discord.ButtonStyle.secondary, row=0)
    async def pause(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        queue = self.bot_ref.queue_store.get()
        campaign = self.bot_ref.campaign_store.get(self.campaign_id)
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages["campaign_not_found"], ephemeral=True)
            return
        self.bot_ref.logger.info("campaign_paused campaign_id=%s %s Guild-ID=%s", self.campaign_id, format_log_user_id(interaction.user.id), interaction.guild_id)
        queue["status"] = "paused"
        self.bot_ref.queue_store.save(queue)
        campaign["status"] = "paused"
        campaign["paused_at"] = iso_now()
        self.bot_ref.campaign_store.upsert(campaign)
        embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue)
        await interaction.response.edit_message(embed=embed, view=CampaignControlView(self.bot_ref, self.campaign_id))
        await self.bot_ref.sender._log_to_channel(self.bot_ref.messages["log_paused"], campaign)

    @discord.ui.button(label="Resume", style=discord.ButtonStyle.success, row=0)
    async def resume(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        queue = self.bot_ref.queue_store.get()
        campaign = self.bot_ref.campaign_store.get(self.campaign_id)
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages["campaign_not_found"], ephemeral=True)
            return
        self.bot_ref.logger.info("campaign_resumed campaign_id=%s %s Guild-ID=%s", self.campaign_id, format_log_user_id(interaction.user.id), interaction.guild_id)
        queue["status"] = "running"
        queue["next_run_at"] = ""
        self.bot_ref.queue_store.save(queue)
        campaign["status"] = "running"
        self.bot_ref.campaign_store.upsert(campaign)
        self.bot_ref.sender.start()
        embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue)
        await interaction.response.edit_message(embed=embed, view=CampaignControlView(self.bot_ref, self.campaign_id))
        await self.bot_ref.sender._log_to_channel(self.bot_ref.messages["log_resumed"], campaign)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary, row=0)
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        campaign = self.bot_ref.campaign_store.get(self.campaign_id)
        queue = self.bot_ref.queue_store.get()
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages["campaign_not_found"], ephemeral=True)
            return
        embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue)
        await interaction.response.edit_message(embed=embed, view=CampaignControlView(self.bot_ref, self.campaign_id))

    @discord.ui.button(label="Edit Message", style=discord.ButtonStyle.secondary, row=0)
    async def edit_message(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        queue = self.bot_ref.queue_store.get()
        if queue.get("campaign_id") != self.campaign_id or queue.get("status") != "paused":
            await interaction.response.send_message(
                self.bot_ref.messages.get("status_edit_only_when_paused", "Pause the campaign before editing."),
                ephemeral=True,
            )
            return
        campaign = self.bot_ref.campaign_store.get(self.campaign_id)
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages["campaign_not_found"], ephemeral=True)
            return
        session = _session_dict_from_campaign(campaign)
        await interaction.response.send_modal(MessageModal(self.bot_ref, session, status_campaign_id=self.campaign_id))

    @discord.ui.button(label="Edit Settings", style=discord.ButtonStyle.secondary, row=0)
    async def edit_settings(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        queue = self.bot_ref.queue_store.get()
        if queue.get("campaign_id") != self.campaign_id or queue.get("status") != "paused":
            await interaction.response.send_message(
                self.bot_ref.messages.get("status_edit_only_when_paused", "Pause the campaign before editing."),
                ephemeral=True,
            )
            return
        campaign = self.bot_ref.campaign_store.get(self.campaign_id)
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages["campaign_not_found"], ephemeral=True)
            return
        session = _session_dict_from_campaign(campaign)
        await interaction.response.send_modal(SettingsModal(self.bot_ref, session, status_campaign_id=self.campaign_id))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=1)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        queue = self.bot_ref.queue_store.get()
        campaign = self.bot_ref.campaign_store.get(self.campaign_id)
        if not campaign:
            await interaction.response.send_message(self.bot_ref.messages["campaign_not_found"], ephemeral=True)
            return
        self.bot_ref.logger.info("campaign_cancelled campaign_id=%s %s Guild-ID=%s", self.campaign_id, format_log_user_id(interaction.user.id), interaction.guild_id)
        queue["status"] = "cancelled"
        queue["next_run_at"] = ""
        self.bot_ref.queue_store.save(queue)
        campaign["status"] = "cancelled"
        campaign["cancelled_at"] = iso_now()
        self.bot_ref.campaign_store.upsert(campaign)
        embed = self.bot_ref.build_status_embed(interaction.guild, campaign, queue)
        await interaction.response.edit_message(embed=embed, view=CampaignControlView(self.bot_ref, self.campaign_id))
        await self.bot_ref.sender._log_to_channel(self.bot_ref.messages["log_cancelled"], campaign)


class PromoBuilderView(discord.ui.View):
    def __init__(self, bot_ref, session: dict[str, Any]) -> None:
        super().__init__(timeout=1800)
        self.bot_ref = bot_ref
        self.session = session

    @discord.ui.button(label="Select Role", style=discord.ButtonStyle.secondary)
    async def select_role(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        await interaction.response.send_message("Choose the role to target.", view=RolePickerView(self.bot_ref, self.session), ephemeral=True)

    @discord.ui.button(label="Edit Message", style=discord.ButtonStyle.primary)
    async def edit_message(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        await interaction.response.send_modal(MessageModal(self.bot_ref, self.session))

    @discord.ui.button(label="Edit Settings", style=discord.ButtonStyle.secondary)
    async def edit_settings(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        await interaction.response.send_modal(SettingsModal(self.bot_ref, self.session))

    @discord.ui.button(label="Preview", style=discord.ButtonStyle.success)
    async def preview(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        error = self.bot_ref.validate_session(self.session)
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return
        color = int(self.bot_ref.config["embed_color"])
        embeds = build_dm_embeds(self.session, color)
        preview_content = self.bot_ref.messages.get("preview_label", "**Preview:**")
        preview_view = self.bot_ref.sender._build_send_view(self.session)
        await interaction.response.send_message(content=preview_content, embeds=embeds, view=preview_view, ephemeral=True)

    @discord.ui.button(label="Test Send", style=discord.ButtonStyle.primary)
    async def test_send(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        error = self.bot_ref.validate_session(self.session)
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return
        try:
            color = int(self.bot_ref.config["embed_color"])
            embeds = build_dm_embeds(self.session, color)
            view = self.bot_ref.sender._build_send_view(self.session)
            await interaction.user.send(embeds=embeds, view=view)
            await interaction.response.send_message(self.bot_ref.messages["test_send_success"], ephemeral=True)
        except Exception as exc:
            await interaction.response.send_message(f"{self.bot_ref.messages['test_send_failed']} Error: {exc}", ephemeral=True)

    @discord.ui.button(label="Start Campaign", style=discord.ButtonStyle.danger)
    async def start_campaign(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.bot_ref.user_can_manage(interaction):
            await interaction.response.send_message(self.bot_ref.messages["permission_error"], ephemeral=True)
            return
        error = self.bot_ref.validate_session(self.session)
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return
        recipients = self.bot_ref.resolve_recipient_ids(interaction.guild, self.session)
        if not recipients:
            await interaction.response.send_message(self.bot_ref.messages["validation_no_recipients"], ephemeral=True)
            return
        max_recipients = int(self.bot_ref.config["max_campaign_recipients"])
        if len(recipients) > max_recipients:
            msg = self.bot_ref.messages.get("max_recipients_exceeded", "Recipient count exceeds max ({max_campaign_recipients}).")
            await interaction.response.send_message(msg.replace("{max_campaign_recipients}", str(max_recipients)), ephemeral=True)
            return
        embed = self.bot_ref.build_confirm_embed(interaction.guild, self.session, recipients)
        await interaction.response.edit_message(embed=embed, view=LaunchConfirmView(self.bot_ref, self.session, recipients))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.bot_ref.session_store.delete(interaction.guild_id, interaction.user.id)
        await interaction.response.edit_message(content="Promo draft cancelled.", embed=None, view=None)
