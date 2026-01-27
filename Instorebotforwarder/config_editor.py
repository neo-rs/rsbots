"""
Instorebotforwarder - Config Editor

This bot stores per-guild settings in JSON files:
  `Instorebotforwarder/guild_configs/guild_config.json` (contains all guilds)

This module powers `/instore editor` and is designed to be interaction-safe:
- never tries to edit non-bot messages
- uses ephemeral sends/modals only (avoids "Unknown interaction" 10062)
"""

from __future__ import annotations

from typing import Optional

import discord
from discord import ui

from instore_auto_mirror_bot import get_config, set_config

def _is_admin(interaction: discord.Interaction) -> bool:
    perms = interaction.user.guild_permissions if interaction.guild else None
    return bool(perms and (perms.manage_guild or perms.administrator))

def _fmt_role(role_id: Optional[int]) -> str:
    return f"<@&{role_id}>" if role_id else "Not set"

def _fmt_channel(ch_id: Optional[int]) -> str:
    return f"<#{ch_id}>" if ch_id else "Not set"

def _fmt_bool(value: Optional[bool]) -> str:
    if value is None:
        return "Not set"
    return "Yes" if value else "No"

class MainEditorView(ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id

    async def build_embed(self) -> discord.Embed:
        cfg = await get_config(self.guild_id)
        embed = discord.Embed(
            title="🛠️ Instore Mirror Editor",
            description="Pick what you want to change. All changes apply to the **next** processed lead.",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Mode", value=f"`{cfg.get('post_mode') or 'Not set'}` (preview / manual / disabled)", inline=False)
        embed.add_field(name="Role", value=_fmt_role(cfg.get("role_id")), inline=True)
        embed.add_field(name="Profit Emoji", value=(cfg.get("profit_emoji") or "Not set")[:80], inline=True)
        embed.add_field(name="Success Channel", value=_fmt_channel(cfg.get("success_channel_id")), inline=True)
        footer_text = (cfg.get("footer_text") or "").strip()
        embed.add_field(name="Footer", value=footer_text[:200] or "Not set", inline=False)
        embed.add_field(name="Wrap Links", value=_fmt_bool(cfg.get("wrap_links")), inline=True)
        embed.add_field(name="OpenAI Model", value=(cfg.get("openai_model") or "Not set"), inline=True)
        embed.add_field(name="Temperature", value=str(cfg.get("openai_temperature") or "Not set"), inline=True)
        embed.set_footer(text="Editor is ephemeral and safe; no message edits are performed.")
        return embed

    @ui.button(label="Set Mode", style=discord.ButtonStyle.primary, row=0)
    async def set_mode(self, interaction: discord.Interaction, button: ui.Button):
        if not _is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        await interaction.response.send_modal(ModeModal(self.guild_id))

    @ui.button(label="Set Role ID", style=discord.ButtonStyle.primary, row=0)
    async def set_role(self, interaction: discord.Interaction, button: ui.Button):
        if not _is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        await interaction.response.send_modal(RoleModal(self.guild_id))

    @ui.button(label="Set Profit Emoji", style=discord.ButtonStyle.primary, row=0)
    async def set_emoji(self, interaction: discord.Interaction, button: ui.Button):
        if not _is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        await interaction.response.send_modal(EmojiModal(self.guild_id))

    @ui.button(label="Set Success Channel ID", style=discord.ButtonStyle.secondary, row=1)
    async def set_success_ch(self, interaction: discord.Interaction, button: ui.Button):
        if not _is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        await interaction.response.send_modal(SuccessChannelModal(self.guild_id))

    @ui.button(label="Set Footer", style=discord.ButtonStyle.secondary, row=1)
    async def set_footer(self, interaction: discord.Interaction, button: ui.Button):
        if not _is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        await interaction.response.send_modal(FooterModal(self.guild_id))

    @ui.button(label="OpenAI Settings", style=discord.ButtonStyle.secondary, row=1)
    async def openai_settings(self, interaction: discord.Interaction, button: ui.Button):
        if not _is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        await interaction.response.send_modal(OpenAIModal(self.guild_id))

    @ui.button(label="Close", style=discord.ButtonStyle.danger, row=2)
    async def close(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        self.stop()

async def open_config_editor(interaction: discord.Interaction, bot):
    if not interaction.guild:
        return await interaction.response.send_message("Run this in a server.", ephemeral=True)
    view = MainEditorView(interaction.guild.id)
    embed = await view.build_embed()
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# -----------------------
# Modals
# -----------------------
class ModeModal(ui.Modal, title="Set Posting Mode"):
    mode = ui.TextInput(
        label="Mode",
        placeholder="preview / manual / disabled",
        required=True,
        max_length=20
    )

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        m = (self.mode.value or "").lower().strip()
        if m not in {"preview", "manual", "disabled"}:
            return await interaction.response.send_message("Mode must be: preview, manual, disabled.", ephemeral=True)
        cfg = await get_config(self.guild_id)
        cfg["post_mode"] = m
        await set_config(self.guild_id, cfg)
        await interaction.response.send_message(f"✅ post_mode set to `{m}`", ephemeral=True)

class RoleModal(ui.Modal, title="Set Role ID"):
    role_id = ui.TextInput(label="Role ID (numbers) - blank removes", required=False, max_length=24)

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        val = (self.role_id.value or "").strip()
        cfg = await get_config(self.guild_id)
        if not val:
            cfg["role_id"] = None
        elif val.isdigit():
            cfg["role_id"] = int(val)
        else:
            return await interaction.response.send_message("Role ID must be numbers only.", ephemeral=True)
        await set_config(self.guild_id, cfg)
        await interaction.response.send_message(f"✅ Role set to {_fmt_role(cfg.get('role_id'))}", ephemeral=True)

class EmojiModal(ui.Modal, title="Set Profit Emoji"):
    emoji = ui.TextInput(label="Emoji string", placeholder="(blank removes)", required=False, max_length=120)

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        cfg = await get_config(self.guild_id)
        cfg["profit_emoji"] = (self.emoji.value or "").strip()
        await set_config(self.guild_id, cfg)
        await interaction.response.send_message("✅ Profit emoji updated.", ephemeral=True)

class SuccessChannelModal(ui.Modal, title="Set Success Channel ID"):
    channel_id = ui.TextInput(label="Channel ID (numbers) - blank removes", required=False, max_length=24)

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        val = (self.channel_id.value or "").strip()
        cfg = await get_config(self.guild_id)
        if not val:
            cfg["success_channel_id"] = None
        elif val.isdigit():
            cfg["success_channel_id"] = int(val)
        else:
            return await interaction.response.send_message("Channel ID must be numbers only.", ephemeral=True)
        await set_config(self.guild_id, cfg)
        await interaction.response.send_message(f"✅ Success channel set to {_fmt_channel(cfg.get('success_channel_id'))}", ephemeral=True)

class FooterModal(ui.Modal, title="Set Footer Text"):
    footer = ui.TextInput(
        label="Footer text",
        style=discord.TextStyle.paragraph,
        placeholder="(optional)",
        required=True,
        max_length=400
    )

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        cfg = await get_config(self.guild_id)
        cfg["footer_text"] = (self.footer.value or "").strip()
        await set_config(self.guild_id, cfg)
        await interaction.response.send_message("✅ Footer updated.", ephemeral=True)

class OpenAIModal(ui.Modal, title="OpenAI Settings"):
    model = ui.TextInput(label="Model", placeholder="(required for OpenAI calls)", required=False, max_length=60)
    temperature = ui.TextInput(label="Temperature (0.0 - 1.0)", placeholder="(optional)", required=False, max_length=10)

    def __init__(self, guild_id: int):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        cfg = await get_config(self.guild_id)
        m = (self.model.value or "").strip()
        t_raw = (self.temperature.value or "").strip()
        if t_raw:
            try:
                t = float(t_raw)
            except ValueError:
                return await interaction.response.send_message("Temperature must be a number.", ephemeral=True)
            if t < 0.0 or t > 1.0:
                return await interaction.response.send_message("Temperature must be between 0.0 and 1.0.", ephemeral=True)
            cfg["openai_temperature"] = t
        else:
            cfg["openai_temperature"] = None
        cfg["openai_model"] = m
        await set_config(self.guild_id, cfg)
        await interaction.response.send_message("✅ OpenAI settings updated.", ephemeral=True)
