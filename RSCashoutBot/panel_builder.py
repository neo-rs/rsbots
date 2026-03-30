"""
Ephemeral admin UI for editing the cashout panel embed and ticket banner (RSPromoBot-style).
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Dict, Tuple

import discord

if TYPE_CHECKING:
    from bot import RSTicketBot

PANEL_OVERRIDES_PATH_NAME = 'panel_overrides.json'

# Default banner on new ticket welcome embeds (override via panel admin or panel_overrides.json).
DEFAULT_TICKET_BANNER_URL = (
    'https://media.discordapp.net/attachments/1486070151756517538/1487963079252770836/image.png'
)


def _parse_embed_color(raw: str) -> int | None:
    s = str(raw or '').strip()
    if not s:
        return None
    if s.lower().startswith('0x'):
        s = s[2:]
    elif s.startswith('#'):
        s = s[1:]
    if not re.fullmatch(r'[0-9a-fA-F]{6}', s):
        return None
    return int(s, 16)


def _is_http_url(s: str) -> bool:
    t = s.strip().lower()
    return t.startswith('http://') or t.startswith('https://')


async def resolve_panel_presentation(bot: 'RSTicketBot') -> Tuple[str, str, str, int]:
    o = await bot.panel_overrides.read()
    t = bot.runtime.ticket
    title = o['panel_title'] if 'panel_title' in o else t.panel_title
    desc = o['panel_description'] if 'panel_description' in o else t.panel_description
    footer = o['footer_text'] if 'footer_text' in o else t.footer_text
    if 'panel_embed_color' in o:
        try:
            color = int(o['panel_embed_color'])
        except (TypeError, ValueError):
            color = t.panel_embed_color
    else:
        color = t.panel_embed_color
    return title, desc, footer, color


async def resolve_ticket_banner_url(bot: 'RSTicketBot') -> str | None:
    o = await bot.panel_overrides.read()
    if 'ticket_banner_url' in o:
        url = str(o.get('ticket_banner_url') or '').strip()
        if not url:
            return None
        if _is_http_url(url):
            return url
        return None
    return DEFAULT_TICKET_BANNER_URL


def build_panel_admin_summary_embed(snapshot: Dict[str, Any]) -> discord.Embed:
    title = snapshot['panel_title']
    desc = snapshot['panel_description']
    footer = snapshot['footer_text']
    color = snapshot['panel_embed_color']
    banner = snapshot.get('ticket_banner_effective_display') or '(none)'
    body = (
        f'**Panel title:** {title[:200]}{"…" if len(title) > 200 else ""}\n'
        f'**Footer:** {footer[:120]}{"…" if len(footer) > 120 else ""}\n'
        f'**Panel color:** `#{color:06x}`\n'
        f'**Ticket banner:** {str(banner)[:200]}'
    )
    emb = discord.Embed(
        title='Cashout panel editor',
        description=body,
        color=color,
    )
    if desc.strip():
        emb.add_field(name='Panel description', value=desc[:1024] or '—', inline=False)
    emb.set_footer(text='Edit text / settings, use Preview, then Post to panel channel.')
    return emb


class PanelTextModal(discord.ui.Modal, title='Edit panel message'):
    def __init__(self, bot: 'RSTicketBot', snapshot: Dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

        self.panel_title = discord.ui.TextInput(
            label='Panel title',
            default=str(snapshot.get('panel_title', ''))[:100],
            max_length=100,
            required=True,
        )
        self.panel_description = discord.ui.TextInput(
            label='Panel description',
            default=str(snapshot.get('panel_description', ''))[:4000],
            style=discord.TextStyle.paragraph,
            max_length=4000,
            required=False,
        )
        self.footer_text = discord.ui.TextInput(
            label='Panel footer (optional)',
            default=str(snapshot.get('footer_text', ''))[:500],
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=False,
        )
        self.add_item(self.panel_title)
        self.add_item(self.panel_description)
        self.add_item(self.footer_text)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        o = await self.bot_ref.panel_overrides.read()
        o['panel_title'] = str(self.panel_title.value).strip()
        o['panel_description'] = str(self.panel_description.value).strip()
        o['footer_text'] = str(self.footer_text.value).strip()
        await self.bot_ref.panel_overrides.write(o)

        snap = await self.bot_ref.panel_presentation_snapshot()
        emb = build_panel_admin_summary_embed(snap)
        await interaction.response.edit_message(embed=emb, view=PanelAdminView(self.bot_ref))


class PanelSettingsModal(discord.ui.Modal, title='Edit panel settings'):
    def __init__(self, bot: 'RSTicketBot', snapshot: Dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

        color_hex = f'{int(snapshot.get("panel_embed_color", 0x5865F2)):06x}'
        banner_default = str(snapshot.get('settings_banner_input', ''))[:500]

        self.embed_color = discord.ui.TextInput(
            label='Panel embed color (RRGGBB hex)',
            default=color_hex,
            max_length=8,
            required=True,
            placeholder='5865F2 or 0x5865F2',
        )
        self.ticket_banner = discord.ui.TextInput(
            label='Ticket banner URL (empty = default Spiff)',
            default=banner_default,
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=False,
            placeholder='https://…',
        )
        self.add_item(self.embed_color)
        self.add_item(self.ticket_banner)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        parsed = _parse_embed_color(str(self.embed_color.value))
        if parsed is None:
            await interaction.response.send_message(
                'Invalid color. Use 6 hex digits, e.g. `5865F2` or `0x5865F2`.',
                ephemeral=True,
            )
            return

        banner_raw = str(self.ticket_banner.value).strip()
        o = await self.bot_ref.panel_overrides.read()
        o['panel_embed_color'] = parsed
        if not banner_raw:
            o.pop('ticket_banner_url', None)
        elif _is_http_url(banner_raw):
            o['ticket_banner_url'] = banner_raw
        else:
            await interaction.response.send_message(
                'Ticket banner must be empty or a valid http(s) URL.',
                ephemeral=True,
            )
            return

        await self.bot_ref.panel_overrides.write(o)

        snap = await self.bot_ref.panel_presentation_snapshot()
        emb = build_panel_admin_summary_embed(snap)
        await interaction.response.edit_message(embed=emb, view=PanelAdminView(self.bot_ref))


class PanelAdminView(discord.ui.View):
    def __init__(self, bot: 'RSTicketBot') -> None:
        super().__init__(timeout=1800)
        self.bot_ref = bot

    @discord.ui.button(label='Edit panel text', style=discord.ButtonStyle.primary, row=0)
    async def edit_text(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        snap = await self.bot_ref.panel_presentation_snapshot()
        await interaction.response.send_modal(PanelTextModal(self.bot_ref, snap))

    @discord.ui.button(label='Edit settings', style=discord.ButtonStyle.secondary, row=0)
    async def edit_settings(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        snap = await self.bot_ref.panel_presentation_snapshot()
        await interaction.response.send_modal(PanelSettingsModal(self.bot_ref, snap))

    @discord.ui.button(label='Preview', style=discord.ButtonStyle.success, row=1)
    async def preview(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        emb = await self.bot_ref.build_panel_embed_async()
        preview = discord.Embed.from_dict(emb.to_dict())
        ft = preview.footer.text if preview.footer else ''
        preview.set_footer(text=f'{ft}\n\nPreview only — not posted to the channel.'.strip())
        await interaction.response.send_message(
            content='**Panel preview** (what members see on the cashout panel):',
            embed=preview,
            ephemeral=True,
        )

    @discord.ui.button(label='Post to panel channel', style=discord.ButtonStyle.danger, row=1)
    async def post_panel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message('Use this inside the server.', ephemeral=True)
            return
        panel_id = self.bot_ref.runtime.ticket.panel_channel_id
        panel_ch = guild.get_channel(panel_id)
        if not isinstance(panel_ch, discord.TextChannel):
            await interaction.response.send_message('Panel channel is not available.', ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await self.bot_ref.send_panel_card(panel_ch)
        await interaction.followup.send(f'Posted panel to {panel_ch.mention}.', ephemeral=True)
