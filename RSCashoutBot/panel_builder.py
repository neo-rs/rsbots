"""
Ephemeral admin UI: panel embed, ticket welcome + DM copy, ticket banner, extra sheet editors.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Dict, List

import discord

if TYPE_CHECKING:
    from bot import RSTicketBot, ButtonDefinition

PANEL_OVERRIDES_PATH_NAME = 'panel_overrides.json'

DEFAULT_TICKET_BANNER_URL = (
    'https://media.discordapp.net/attachments/1486070151756517538/1487963079252770836/image.png'
)

DEFAULT_TICKET_NEXT_STEP_TEMPLATE = (
    'Fill out the sheet fully, then send the completed link back in this ticket when ready.\n'
    'If you have multiple products, add additional rows in the same sheet (row 4+).\n'
    'View link: {link}'
)

DEFAULT_DM_TITLE = 'RS Cashout Submission Received'
DEFAULT_DM_SHEET_OK = (
    'Your personal cashout sheet copy is ready. Keep this link for reference and '
    'send the completed version back in your ticket when finished.'
)
DEFAULT_DM_SHEET_FAIL = (
    'Your ticket is ready. The Google Sheets auto-copy failed, so staff will handle the sheet.'
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


def parse_editor_email_lines(blob: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for line in str(blob or '').splitlines():
        e = line.strip()
        if not e or e.startswith('#'):
            continue
        if '@' not in e:
            continue
        key = e.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


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


async def resolve_cashout_ticket_intro(bot: 'RSTicketBot', button_def: 'ButtonDefinition') -> Tuple[str, str]:
    if button_def.key != 'request_submit':
        return button_def.intro_title, button_def.intro_body
    o = await bot.panel_overrides.read()
    title = o['cashout_ticket_intro_title'] if 'cashout_ticket_intro_title' in o else button_def.intro_title
    body = o['cashout_ticket_intro_body'] if 'cashout_ticket_intro_body' in o else button_def.intro_body
    return str(title), str(body)


async def resolve_cashout_ticket_next_template(bot: 'RSTicketBot') -> str:
    o = await bot.panel_overrides.read()
    if 'cashout_ticket_next_step_template' in o:
        return str(o['cashout_ticket_next_step_template'])
    return DEFAULT_TICKET_NEXT_STEP_TEMPLATE


async def resolve_cashout_dm_copy(bot: 'RSTicketBot') -> Dict[str, str]:
    o = await bot.panel_overrides.read()
    return {
        'title': str(o['cashout_dm_title']) if 'cashout_dm_title' in o else DEFAULT_DM_TITLE,
        'sheet_ok': str(o['cashout_dm_sheet_ok']) if 'cashout_dm_sheet_ok' in o else DEFAULT_DM_SHEET_OK,
        'sheet_fail': str(o['cashout_dm_sheet_fail']) if 'cashout_dm_sheet_fail' in o else DEFAULT_DM_SHEET_FAIL,
        'field_sheet_link': str(o['cashout_dm_field_sheet_link']) if 'cashout_dm_field_sheet_link' in o else 'Sheet Link',
        'field_multi': str(o['cashout_dm_field_multi']) if 'cashout_dm_field_multi' in o else 'Multiple products?',
        'field_multi_value': str(o['cashout_dm_field_multi_value']) if 'cashout_dm_field_multi_value' in o else 'Add additional rows in the same sheet (row 4+).',
        'field_ticket': str(o['cashout_dm_field_ticket']) if 'cashout_dm_field_ticket' in o else 'Ticket',
        'field_error': str(o['cashout_dm_field_error']) if 'cashout_dm_field_error' in o else 'Sheet Error',
    }


def build_cashout_admin_hub_embed(snapshot: Dict[str, Any]) -> discord.Embed:
    title = snapshot['panel_title']
    desc = snapshot['panel_description']
    footer = snapshot['footer_text']
    color = snapshot['panel_embed_color']
    banner = snapshot.get('ticket_banner_effective_display') or '(none)'
    t_intro = snapshot.get('cashout_ticket_intro_title_preview', '')[:80]
    dm_t = snapshot.get('cashout_dm_title_preview', '')[:60]
    n_editors = snapshot.get('sheet_extra_editor_count', 0)
    body = (
        f'**Panel title:** {title[:120]}{"…" if len(title) > 120 else ""}\n'
        f'**Panel color:** `#{color:06x}` · **Ticket banner:** {str(banner)[:120]}\n'
        f'**Ticket card (Submit):** intro *{t_intro}…*\n'
        f'**DM card title:** {dm_t}\n'
        f'**Extra sheet editors:** {n_editors} (merged with config `google_sheet.extra_editor_emails`)'
    )
    emb = discord.Embed(
        title='Cashout message editor',
        description=body,
        color=color,
    )
    if desc.strip():
        emb.add_field(name='Panel description (public)', value=desc[:900] or '—', inline=False)
    emb.set_footer(
        text='Edit panel, ticket, DM, or sheet editors · Preview each · Post panel when ready.',
    )
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
        emb = build_cashout_admin_hub_embed(snap)
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
        emb = build_cashout_admin_hub_embed(snap)
        await interaction.response.edit_message(embed=emb, view=PanelAdminView(self.bot_ref))


class TicketCardModal(discord.ui.Modal, title='Edit ticket card (Submit Cashout)'):
    def __init__(self, bot: 'RSTicketBot', snapshot: Dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

        self.intro_title = discord.ui.TextInput(
            label='Embed title',
            default=str(snapshot.get('ticket_intro_title_field', ''))[:256],
            max_length=256,
            required=True,
        )
        self.intro_body = discord.ui.TextInput(
            label='Embed description',
            default=str(snapshot.get('ticket_intro_body_field', ''))[:4000],
            style=discord.TextStyle.paragraph,
            max_length=4000,
            required=False,
        )
        self.next_step = discord.ui.TextInput(
            label='Next Step (use {link} for sheet URL)',
            default=str(snapshot.get('ticket_next_step_field', ''))[:1000],
            style=discord.TextStyle.paragraph,
            max_length=1000,
            required=True,
        )
        self.add_item(self.intro_title)
        self.add_item(self.intro_body)
        self.add_item(self.next_step)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        o = await self.bot_ref.panel_overrides.read()
        o['cashout_ticket_intro_title'] = str(self.intro_title.value).strip()
        o['cashout_ticket_intro_body'] = str(self.intro_body.value).strip()
        o['cashout_ticket_next_step_template'] = str(self.next_step.value).strip()
        await self.bot_ref.panel_overrides.write(o)

        snap = await self.bot_ref.panel_presentation_snapshot()
        emb = build_cashout_admin_hub_embed(snap)
        await interaction.response.edit_message(embed=emb, view=PanelAdminView(self.bot_ref))


class DmCardModalA(discord.ui.Modal, title='Edit DM card (copy)'):
    def __init__(self, bot: 'RSTicketBot', snapshot: Dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

        self.dm_title = discord.ui.TextInput(
            label='DM embed title',
            default=str(snapshot.get('dm_title_field', ''))[:256],
            max_length=256,
            required=True,
        )
        self.sheet_ok = discord.ui.TextInput(
            label='Description when sheet OK',
            default=str(snapshot.get('dm_sheet_ok_field', ''))[:2000],
            style=discord.TextStyle.paragraph,
            max_length=2000,
            required=True,
        )
        self.sheet_fail = discord.ui.TextInput(
            label='Description when sheet fails',
            default=str(snapshot.get('dm_sheet_fail_field', ''))[:2000],
            style=discord.TextStyle.paragraph,
            max_length=2000,
            required=True,
        )
        self.add_item(self.dm_title)
        self.add_item(self.sheet_ok)
        self.add_item(self.sheet_fail)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        o = await self.bot_ref.panel_overrides.read()
        o['cashout_dm_title'] = str(self.dm_title.value).strip()
        o['cashout_dm_sheet_ok'] = str(self.sheet_ok.value).strip()
        o['cashout_dm_sheet_fail'] = str(self.sheet_fail.value).strip()
        await self.bot_ref.panel_overrides.write(o)

        snap = await self.bot_ref.panel_presentation_snapshot()
        emb = build_cashout_admin_hub_embed(snap)
        await interaction.response.edit_message(embed=emb, view=PanelAdminView(self.bot_ref))


class DmCardModalB(discord.ui.Modal, title='Edit DM field labels'):
    def __init__(self, bot: 'RSTicketBot', snapshot: Dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

        self.f_sheet = discord.ui.TextInput(
            label='Sheet link field name',
            default=str(snapshot.get('dm_field_sheet', ''))[:80],
            max_length=80,
            required=True,
        )
        self.f_multi = discord.ui.TextInput(
            label='Multiple products field name',
            default=str(snapshot.get('dm_field_multi', ''))[:80],
            max_length=80,
            required=True,
        )
        self.f_multi_val = discord.ui.TextInput(
            label='Multiple products field value',
            default=str(snapshot.get('dm_field_multi_val', ''))[:500],
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=True,
        )
        self.f_ticket = discord.ui.TextInput(
            label='Ticket field name',
            default=str(snapshot.get('dm_field_ticket', ''))[:80],
            max_length=80,
            required=True,
        )
        self.f_err = discord.ui.TextInput(
            label='Sheet error field name',
            default=str(snapshot.get('dm_field_error', ''))[:80],
            max_length=80,
            required=True,
        )
        self.add_item(self.f_sheet)
        self.add_item(self.f_multi)
        self.add_item(self.f_multi_val)
        self.add_item(self.f_ticket)
        self.add_item(self.f_err)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        o = await self.bot_ref.panel_overrides.read()
        o['cashout_dm_field_sheet_link'] = str(self.f_sheet.value).strip()
        o['cashout_dm_field_multi'] = str(self.f_multi.value).strip()
        o['cashout_dm_field_multi_value'] = str(self.f_multi_val.value).strip()
        o['cashout_dm_field_ticket'] = str(self.f_ticket.value).strip()
        o['cashout_dm_field_error'] = str(self.f_err.value).strip()
        await self.bot_ref.panel_overrides.write(o)

        snap = await self.bot_ref.panel_presentation_snapshot()
        emb = build_cashout_admin_hub_embed(snap)
        await interaction.response.edit_message(embed=emb, view=PanelAdminView(self.bot_ref))


class SheetEditorsModal(discord.ui.Modal, title='Extra Google Sheet editors'):
    def __init__(self, bot: 'RSTicketBot', snapshot: Dict[str, Any]) -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot
        lines = str(snapshot.get('sheet_editors_text_field', ''))[:4000]
        self.emails = discord.ui.TextInput(
            label='Emails (one per line; merged with config)',
            default=lines,
            style=discord.TextStyle.paragraph,
            max_length=4000,
            required=False,
            placeholder='admin@example.com',
        )
        self.add_item(self.emails)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        parsed = parse_editor_email_lines(str(self.emails.value))
        o = await self.bot_ref.panel_overrides.read()
        if parsed:
            o['sheet_extra_editor_emails'] = parsed
        else:
            o.pop('sheet_extra_editor_emails', None)
        await self.bot_ref.panel_overrides.write(o)

        snap = await self.bot_ref.panel_presentation_snapshot()
        emb = build_cashout_admin_hub_embed(snap)
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

    @discord.ui.button(label='Edit sheet editors', style=discord.ButtonStyle.secondary, row=0)
    async def edit_sheet_editors(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        snap = await self.bot_ref.panel_presentation_snapshot()
        await interaction.response.send_modal(SheetEditorsModal(self.bot_ref, snap))

    @discord.ui.button(label='Edit ticket card', style=discord.ButtonStyle.primary, row=1)
    async def edit_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        snap = await self.bot_ref.panel_presentation_snapshot()
        await interaction.response.send_modal(TicketCardModal(self.bot_ref, snap))

    @discord.ui.button(label='Edit DM (copy)', style=discord.ButtonStyle.primary, row=1)
    async def edit_dm_a(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        snap = await self.bot_ref.panel_presentation_snapshot()
        await interaction.response.send_modal(DmCardModalA(self.bot_ref, snap))

    @discord.ui.button(label='Edit DM (labels)', style=discord.ButtonStyle.secondary, row=1)
    async def edit_dm_b(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        snap = await self.bot_ref.panel_presentation_snapshot()
        await interaction.response.send_modal(DmCardModalB(self.bot_ref, snap))

    @discord.ui.button(label='Preview panel', style=discord.ButtonStyle.success, row=2)
    async def preview_panel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        emb = await self.bot_ref.build_panel_embed_async()
        preview = discord.Embed.from_dict(emb.to_dict())
        ft = preview.footer.text if preview.footer else ''
        preview.set_footer(text=f'{ft}\n\nPreview only — not posted.'.strip())
        await interaction.response.send_message(
            content='**Panel preview** (public channel card):',
            embed=preview,
            ephemeral=True,
        )

    @discord.ui.button(label='Preview ticket', style=discord.ButtonStyle.success, row=2)
    async def preview_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message('Use in a server.', ephemeral=True)
            return
        emb = await self.bot_ref.build_preview_ticket_embed(guild)
        await interaction.response.send_message(
            content='**Ticket preview** (Submit Cashout, sheet link mocked):',
            embed=emb,
            ephemeral=True,
        )

    @discord.ui.button(label='Preview DM', style=discord.ButtonStyle.success, row=2)
    async def preview_dm(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message('Use in a server.', ephemeral=True)
            return
        emb = await self.bot_ref.build_preview_dm_embed(guild)
        await interaction.response.send_message(
            content='**DM preview** (sheet OK path, ticket link mocked):',
            embed=emb,
            ephemeral=True,
        )

    @discord.ui.button(label='Post to panel channel', style=discord.ButtonStyle.danger, row=3)
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
