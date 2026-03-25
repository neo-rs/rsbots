import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / 'config.json'
MESSAGES_PATH = BASE_DIR / 'messages.json'
LOG_PATH = BASE_DIR / 'bot.log'

LOG = logging.getLogger('rscashoutbot')

# Aligned key column (matches datamanager_message_flow_tester-style terminal output)
_FLOW_KV_COL = 13


class FlowReporter:
    """Human-readable console blocks: [HH:MM:SS], rules, sections, aligned KV lines."""

    SEP = '=' * 78

    @staticmethod
    def _ts() -> str:
        return datetime.now().strftime('%H:%M:%S')

    def _emit(self, body: str, log_level: int = logging.INFO) -> None:
        line = f'[{self._ts()}] {body}'
        print(line, file=sys.stdout, flush=True)
        LOG.log(log_level, body)

    def rule(self) -> None:
        self._emit(self.SEP)

    def title(self, name: str) -> None:
        self.rule()
        self._emit(name)
        self.rule()

    def section(self, heading: str) -> None:
        self.rule()
        self._emit(heading)
        self.rule()

    def kv(self, label: str, value: Any) -> None:
        lbl = f'{label}:'
        pad = max(_FLOW_KV_COL, len(lbl) + 1)
        self._emit(f'{lbl:<{pad}}{value}')

    def note(self, first: str, *rest: str) -> None:
        self._emit(first)
        for r in rest:
            self._emit(f'      {r}')

    def warn_note(self, first: str, *rest: str) -> None:
        self._emit(first, logging.WARNING)
        for r in rest:
            self._emit(f'      {r}', logging.WARNING)

    def explain(self, *bullets: str) -> None:
        for b in bullets:
            self._emit(f'- {b}')

    def milestone(self, title: str) -> None:
        inner = f' {title} '
        total = len(self.SEP)
        if len(inner) >= total:
            self._emit(inner.strip())
            return
        pad = (total - len(inner)) // 2
        self._emit('=' * pad + inner + '=' * (total - pad - len(inner)))

    def event_block(self, title: str, rows: List[Tuple[str, Any]]) -> None:
        self.milestone(title)
        for k, v in rows:
            self.kv(k, v)
        self.rule()


FLOW = FlowReporter()


def fmt_ch(channel_id: Optional[int]) -> str:
    if channel_id is None or channel_id == 0:
        return '(none)'
    return f'<#{channel_id}>'


def fmt_user(user_id: int) -> str:
    return f'<@{user_id}>'


def fmt_role(role_id: int) -> str:
    return f'<@&{role_id}>'


def fmt_channel_named(ch: discord.abc.GuildChannel) -> str:
    return f'#{ch.name} {fmt_ch(ch.id)}'


def fmt_member(user: Union[discord.User, discord.Member]) -> str:
    name = getattr(user, 'display_name', None) or user.name
    return f'{name} {fmt_user(user.id)}'


def fmt_channel_resolve(guild: Optional[discord.Guild], channel_id: int) -> str:
    if guild:
        ch = guild.get_channel(channel_id)
        if isinstance(ch, discord.abc.GuildChannel):
            return fmt_channel_named(ch)
    return fmt_ch(channel_id)


def configure_logging() -> None:
    """File gets full INFO trail; console is FlowReporter (startup + events). Discord HTTP stays quiet."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    file_fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    fh = logging.FileHandler(LOG_PATH, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(file_fmt)

    term_fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.WARNING)
    sh.setFormatter(term_fmt)

    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(sh)

    for name in ('discord', 'discord.client', 'discord.http', 'discord.gateway', 'discord.state'):
        logging.getLogger(name).setLevel(logging.WARNING)

    aiohttp_l = logging.getLogger('aiohttp')
    aiohttp_l.setLevel(logging.WARNING)


class ConfigError(RuntimeError):
    pass


# Treat legacy ticket_type as the same slot when checking max_open_per_user (config rename request_custom_quote → request_submit).
TICKET_TYPE_OPEN_EQUIV: Dict[str, frozenset[str]] = {
    'request_submit': frozenset({'request_submit', 'request_custom_quote'}),
}


@dataclass(frozen=True)
class ButtonDefinition:
    key: str
    label: str
    style: str
    emoji: Optional[str]
    ticket_name_prefix: str
    intro_title: str
    intro_body: str
    modal: Optional[Dict[str, Any]]
    sheet_route: Optional[str]
    max_open_per_user: int


@dataclass(frozen=True)
class TicketSettings:
    guild_id: int
    panel_channel_id: int
    ticket_category_id: int
    transcript_channel_id: Optional[int]
    auto_post_panel_on_ready: bool
    support_role_ids: List[int]
    admin_role_ids: List[int]
    close_delay_seconds: int
    topic_template: str
    panel_embed_color: int
    ticket_embed_color: int
    panel_title: str
    panel_description: str
    footer_text: str
    buttons: List[ButtonDefinition]


@dataclass(frozen=True)
class SheetIntegration:
    enabled: bool
    endpoint_url: str
    auth_header_name: str
    auth_token: str
    timeout_seconds: int


@dataclass(frozen=True)
class RuntimeConfig:
    token: str
    ticket: TicketSettings
    sheet: SheetIntegration


class ConfigLoader:
    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise ConfigError(f'Missing required file: {path.name}')
        with path.open('r', encoding='utf-8') as fp:
            return json.load(fp)

    @classmethod
    def load(cls) -> RuntimeConfig:
        config = cls._read_json(CONFIG_PATH)
        messages = cls._read_json(MESSAGES_PATH)

        token = str(config.get('discord_bot_token', '')).strip()
        if not token:
            raise ConfigError('config.json -> discord_bot_token is required')

        ticket_cfg = config.get('ticket_system', {})
        sheet_cfg = config.get('google_sheet', {})
        msg_cfg = messages.get('ticket_system', {})
        button_msgs = msg_cfg.get('buttons', {})

        buttons: List[ButtonDefinition] = []
        for raw_button in ticket_cfg.get('buttons', []):
            key = raw_button['key']
            message_def = dict(button_msgs.get(key, {}))
            if key == 'request_submit' and 'intro_title' not in message_def:
                legacy = button_msgs.get('request_custom_quote')
                if isinstance(legacy, dict):
                    message_def = {**legacy, **message_def}
            if 'intro_title' not in message_def or 'intro_body' not in message_def:
                raise ConfigError(
                    f'messages.json -> ticket_system.buttons[{key!r}] must define intro_title and intro_body'
                )
            buttons.append(
                ButtonDefinition(
                    key=key,
                    label=raw_button['label'],
                    style=raw_button.get('style', 'blurple'),
                    emoji=raw_button.get('emoji'),
                    ticket_name_prefix=raw_button['ticket_name_prefix'],
                    intro_title=message_def['intro_title'],
                    intro_body=message_def['intro_body'],
                    modal=raw_button.get('modal'),
                    sheet_route=raw_button.get('sheet_route'),
                    max_open_per_user=int(raw_button.get('max_open_per_user', 1)),
                )
            )

        ticket = TicketSettings(
            guild_id=int(ticket_cfg['guild_id']),
            panel_channel_id=int(ticket_cfg['panel_channel_id']),
            ticket_category_id=int(ticket_cfg['ticket_category_id']),
            transcript_channel_id=(
                int(ticket_cfg['transcript_channel_id']) if ticket_cfg.get('transcript_channel_id') else None
            ),
            auto_post_panel_on_ready=bool(ticket_cfg.get('auto_post_panel_on_ready', True)),
            support_role_ids=[int(x) for x in ticket_cfg.get('support_role_ids', [])],
            admin_role_ids=[int(x) for x in ticket_cfg.get('admin_role_ids', [])],
            close_delay_seconds=int(ticket_cfg.get('close_delay_seconds', 10)),
            topic_template=str(ticket_cfg.get('topic_template', 'type={ticket_type};owner={user_id}')),
            panel_embed_color=int(str(ticket_cfg.get('panel_embed_color', '0x5865F2')), 16),
            ticket_embed_color=int(str(ticket_cfg.get('ticket_embed_color', '0x5865F2')), 16),
            panel_title=msg_cfg['panel_title'],
            panel_description=str(msg_cfg.get('panel_description', '') or ''),
            footer_text=str(msg_cfg.get('footer_text', '') or ''),
            buttons=buttons,
        )

        sheet = SheetIntegration(
            enabled=bool(sheet_cfg.get('enabled', False)),
            endpoint_url=str(sheet_cfg.get('endpoint_url', '')).strip(),
            auth_header_name=str(sheet_cfg.get('auth_header_name', 'X-API-Key')).strip(),
            auth_token=str(sheet_cfg.get('auth_token', '')).strip(),
            timeout_seconds=int(sheet_cfg.get('timeout_seconds', 15)),
        )

        if sheet.enabled and not sheet.endpoint_url:
            raise ConfigError('google_sheet.enabled is true but endpoint_url is empty')

        return RuntimeConfig(token=token, ticket=ticket, sheet=sheet)


class JsonStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        if not self.path.exists():
            self.path.write_text(json.dumps({'tickets': {}}, indent=2), encoding='utf-8')

    async def read(self) -> Dict[str, Any]:
        async with self._lock:
            return json.loads(self.path.read_text(encoding='utf-8'))

    async def write(self, payload: Dict[str, Any]) -> None:
        async with self._lock:
            self.path.write_text(json.dumps(payload, indent=2), encoding='utf-8')

    async def upsert_ticket(self, channel_id: int, record: Dict[str, Any]) -> None:
        data = await self.read()
        data.setdefault('tickets', {})[str(channel_id)] = record
        await self.write(data)

    async def delete_ticket(self, channel_id: int) -> Optional[Dict[str, Any]]:
        data = await self.read()
        removed = data.setdefault('tickets', {}).pop(str(channel_id), None)
        await self.write(data)
        return removed

    async def find_open_ticket(self, guild_id: int, user_id: int, ticket_type: str) -> Optional[Dict[str, Any]]:
        equiv = TICKET_TYPE_OPEN_EQUIV.get(ticket_type, frozenset({ticket_type}))
        data = await self.read()
        for channel_id, record in data.get('tickets', {}).items():
            if (
                int(record.get('guild_id', 0)) == guild_id
                and int(record.get('owner_id', 0)) == user_id
                and str(record.get('ticket_type')) in equiv
                and bool(record.get('is_open', False))
            ):
                found = dict(record)
                found['channel_id'] = int(channel_id)
                return found
        return None


class SheetClient:
    def __init__(self, config: SheetIntegration) -> None:
        self.config = config

    async def submit(self, route: str, payload: Dict[str, Any]) -> None:
        if not self.config.enabled:
            return
        headers = {'Content-Type': 'application/json'}
        if self.config.auth_token:
            headers[self.config.auth_header_name] = self.config.auth_token
        body = {'route': route, 'payload': payload}
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.config.endpoint_url, json=body, headers=headers) as resp:
                if resp.status >= 300:
                    text = await resp.text()
                    raise RuntimeError(f'Sheet submission failed ({resp.status}): {text[:500]}')
        cid = payload.get('channel_id')
        uid = int(payload.get('user_id') or 0)
        FLOW.event_block(
            'GOOGLE SHEET — SUBMIT OK',
            [
                ('Route', route),
                ('Ticket channel', fmt_ch(int(cid)) if cid is not None else '(?)'),
                ('User', fmt_user(uid) if uid else '(?)'),
            ],
        )


class TicketModal(discord.ui.Modal):
    def __init__(self, bot: 'RSTicketBot', button_def: ButtonDefinition) -> None:
        super().__init__(title=button_def.label[:45])
        self.bot_ref = bot
        self.button_def = button_def
        self.field_keys: List[str] = []
        for field in button_def.modal.get('fields', []):
            text_input = discord.ui.TextInput(
                label=field['label'][:45],
                placeholder=field.get('placeholder', '')[:100],
                default=field.get('default', '')[:4000],
                required=bool(field.get('required', True)),
                style=(discord.TextStyle.paragraph if field.get('paragraph', False) else discord.TextStyle.short),
                max_length=int(field.get('max_length', 400)),
            )
            self.field_keys.append(field['key'])
            self.add_item(text_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        values: Dict[str, str] = {}
        for key, child in zip(self.field_keys, self.children):
            if isinstance(child, discord.ui.TextInput):
                values[key] = child.value.strip()
        await self.bot_ref.create_ticket_from_request(interaction, self.button_def, values)


class CloseTicketView(discord.ui.View):
    def __init__(self, bot: 'RSTicketBot') -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

    @discord.ui.button(label='Close Ticket', style=discord.ButtonStyle.danger, custom_id='rs_ticket:close')
    async def close_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot_ref.close_ticket(interaction)


class TicketPanelView(discord.ui.View):
    def __init__(self, bot: 'RSTicketBot') -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot
        for button_def in bot.runtime.ticket.buttons:
            self.add_item(TicketActionButton(button_def))


class TicketActionButton(discord.ui.Button):
    def __init__(self, button_def: ButtonDefinition) -> None:
        style_map = {
            'blurple': discord.ButtonStyle.primary,
            'gray': discord.ButtonStyle.secondary,
            'green': discord.ButtonStyle.success,
            'red': discord.ButtonStyle.danger,
        }
        super().__init__(
            label=button_def.label,
            style=style_map.get(button_def.style, discord.ButtonStyle.primary),
            emoji=button_def.emoji,
            custom_id=f'rs_ticket:panel:{button_def.key}',
        )
        self.button_def = button_def

    async def callback(self, interaction: discord.Interaction) -> None:
        bot = interaction.client
        assert isinstance(bot, RSTicketBot)
        if self.button_def.modal and self.button_def.modal.get('fields'):
            await interaction.response.send_modal(TicketModal(bot, self.button_def))
            return
        await bot.create_ticket_from_request(interaction, self.button_def, {})




def has_any_configured_role(member: discord.Member, role_ids: List[int]) -> bool:
    member_role_ids = {role.id for role in member.roles}
    return any(role_id in member_role_ids for role_id in role_ids if role_id)


async def is_ticket_admin(interaction: discord.Interaction) -> bool:
    """
    Admin check without requiring privileged member intent.
    If discord.py can't provide a full Member object in interaction.user, we fetch the member via REST.
    """
    runtime_cfg = interaction.client.runtime if isinstance(interaction.client, RSTicketBot) else runtime
    admin_role_ids = runtime_cfg.ticket.admin_role_ids

    member: Optional[discord.Member] = None
    if isinstance(interaction.user, discord.Member):
        member = interaction.user
    else:
        # interaction.user may be a plain User when member intent isn't enabled.
        if interaction.guild is None or not interaction.user:
            return False
        try:
            member = interaction.guild.get_member(interaction.user.id)
            if member is None:
                member = await interaction.guild.fetch_member(interaction.user.id)
        except discord.HTTPException:
            return False

    if member is None:
        return False
    if getattr(member.guild_permissions, 'administrator', False):
        return True
    return has_any_configured_role(member, admin_role_ids)


class RSTicketBot(commands.Bot):
    def __init__(self, runtime: RuntimeConfig) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        # Avoid privileged intent requirement: we can resolve member roles via REST fetch when needed.
        intents.members = False
        # Ensure we never request privileged message content intent.
        intents.message_content = False
        intents.messages = True
        super().__init__(command_prefix='!', intents=intents)
        self.runtime = runtime
        self.store = JsonStore(BASE_DIR / 'tickets.json')
        self.sheet_client = SheetClient(runtime.sheet)
        self._auto_panel_posted_once = False

    async def setup_hook(self) -> None:
        # Views must be built here: discord.py uses asyncio.get_running_loop() in View.__init__.
        self.close_view = CloseTicketView(self)
        self.panel_view = TicketPanelView(self)
        self.add_view(self.close_view)
        self.add_view(self.panel_view)
        guild = discord.Object(id=self.runtime.ticket.guild_id)
        self.tree.copy_global_to(guild=guild)
        FLOW.section('2) SLASH COMMANDS — SYNC')
        FLOW.kv('Guild ID', self.runtime.ticket.guild_id)
        try:
            # Avoid blocking startup on Discord rate limits / slow sync retries.
            await asyncio.wait_for(self.tree.sync(guild=guild), timeout=20)
            FLOW.note(
                'What happened: app commands were copied to this guild and synced with Discord.',
                'Staff can use /ticketpanel, /ticketadd, /ticketremove, /ticketclose as configured.',
            )
            FLOW.explain('Guild-scoped sync keeps command registration fast (no global propagation wait).')
        except asyncio.TimeoutError:
            FLOW.warn_note(
                'WARNING: Slash-command sync timed out; skipping.',
                'The bot will still start and auto-post the panel; slash commands may require manual sync.',
            )
            LOG.warning('Slash commands sync timed out | guild_id=%s', self.runtime.ticket.guild_id)
        except discord.Forbidden:
            FLOW.warn_note(
                'WARNING: Slash-command sync skipped (403 Missing Access).',
                'This usually means the bot lacks permission to manage application commands in this guild.',
                'The bot will still start and auto-post the panel; slash commands may not register.',
            )
            LOG.warning(
                'Slash commands sync forbidden | guild_id=%s panel=%s',
                self.runtime.ticket.guild_id,
                fmt_ch(self.runtime.ticket.panel_channel_id),
            )
        FLOW.rule()

    async def on_ready(self) -> None:
        if not self.user:
            return
        me = fmt_member(self.user)
        g = self.get_guild(self.runtime.ticket.guild_id)
        # On some deployments the guild may not be in the local cache yet; fall back to a REST fetch.
        if g is None:
            try:
                g = await self.fetch_guild(self.runtime.ticket.guild_id)
            except discord.HTTPException:
                g = None
        FLOW.section('3) LOGIN — BOT ONLINE')
        FLOW.kv('Bot', me)
        if g:
            panel = fmt_channel_resolve(g, self.runtime.ticket.panel_channel_id)
            cat = fmt_channel_resolve(g, self.runtime.ticket.ticket_category_id)
            trans = (
                fmt_channel_resolve(g, self.runtime.ticket.transcript_channel_id)
                if self.runtime.ticket.transcript_channel_id
                else '(transcript logging off)'
            )
            FLOW.kv('Guild', f'{g.name} ({g.id})')
            FLOW.kv('Panel channel', panel)
            FLOW.kv('Ticket category', cat)
            FLOW.kv('Transcript channel', trans)
            FLOW.note(
                'What this means: the bot sees your server and resolved channel/category names.',
                'Users open tickets from the panel channel; new channels appear under the ticket category.',
            )
        else:
            FLOW.kv('Guild ID', self.runtime.ticket.guild_id)
            FLOW.kv('Panel channel', fmt_ch(self.runtime.ticket.panel_channel_id))
            FLOW.kv('Ticket category', fmt_ch(self.runtime.ticket.ticket_category_id))
            FLOW.warn_note(
                'WARNING: This bot is online but the configured guild is not visible.',
                'Check: bot invite, guild_id in config, and that the bot process uses the right token.',
                'Members intent is required for some permission checks.',
            )
        FLOW.rule()

        if self.runtime.ticket.auto_post_panel_on_ready and not self._auto_panel_posted_once:
            self._auto_panel_posted_once = True
            panel_id = self.runtime.ticket.panel_channel_id
            pch = None
            if g:
                pch = g.get_channel(panel_id)
            if not isinstance(pch, discord.TextChannel):
                try:
                    fetched = await self.fetch_channel(panel_id)
                    if isinstance(fetched, discord.TextChannel):
                        pch = fetched
                except discord.Forbidden:
                    pch = None
                except discord.HTTPException:
                    pch = None

            if isinstance(pch, discord.TextChannel):
                try:
                    await self.send_panel_card(pch)
                    FLOW.section('4) TICKET PANEL CARD — SENT TO SERVER')
                    FLOW.kv('Channel', fmt_channel_named(pch))
                    FLOW.note(
                        'What was sent: the same embed + button row as /ticketpanel.',
                        'Disable duplicate posts on restart: set ticket_system.auto_post_panel_on_ready to false in config.json.',
                    )
                    FLOW.rule()
                except discord.Forbidden:
                    FLOW.warn_note(
                        'WARNING: Could not auto-post the panel (403 Forbidden).',
                        'Give the bot Send Messages + Embed Links + Attach Files in the panel channel (and View Channel).',
                    )
                    FLOW.rule()
                except discord.HTTPException as exc:
                    FLOW.warn_note(
                        f'WARNING: Could not auto-post the panel ({exc}).',
                        'Check bot permissions and channel exists; you can still use /ticketpanel in that channel.',
                    )
                    FLOW.rule()
            else:
                FLOW.warn_note(
                    'WARNING: auto_post_panel_on_ready is on but panel channel could not be resolved as a text channel.',
                    'Check ticket_system.panel_channel_id and bot permissions.',
                )
                FLOW.rule()

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        cmd = interaction.command.name if interaction.command else '?'
        actor = fmt_member(interaction.user) if interaction.user else '?'
        where = fmt_ch(interaction.channel_id) if interaction.channel_id else '(no channel)'
        FLOW.section('ERROR — SLASH COMMAND')
        FLOW.kv('Command', f'/{cmd}')
        FLOW.kv('Channel', where)
        FLOW.kv('Actor', actor)
        if isinstance(error, app_commands.CommandInvokeError):
            FLOW.kv('Exception', repr(error.original))
            FLOW.note('Full traceback is in bot.log (file handler).')
            LOG.error(
                'Slash /%s failed | %s | actor=%s',
                cmd,
                where,
                actor,
                exc_info=error.original,
            )
        else:
            FLOW.kv('Problem', str(error))
            LOG.warning('Slash /%s | %s | actor=%s | %s', cmd, where, actor, error)
        FLOW.rule()

    def _build_panel_embed(self) -> discord.Embed:
        ticket = self.runtime.ticket
        desc = ticket.panel_description.strip()
        embed = discord.Embed(
            title=ticket.panel_title,
            description=desc if desc else None,
            color=ticket.panel_embed_color,
        )
        return embed

    async def send_panel_card(self, channel: discord.TextChannel) -> None:
        """Post the ticket panel embed + buttons (same card as /ticketpanel)."""
        await channel.send(embed=self._build_panel_embed(), view=self.panel_view)

    async def _maybe_log_ticket_opened(
        self,
        guild: discord.Guild,
        ticket_channel: discord.TextChannel,
        opener: discord.abc.User,
        button_def: ButtonDefinition,
        form_values: Dict[str, str],
    ) -> None:
        """Post to transcript_channel_id when any ticket flow opens (request/signup/help)."""
        tid = self.runtime.ticket.transcript_channel_id
        if not tid:
            return
        log_ch = guild.get_channel(tid)
        if not isinstance(log_ch, discord.TextChannel):
            return
        lines = [
            f'**Channel:** {ticket_channel.mention}',
            f'**Owner:** {opener.mention} ({opener.id})',
            f'**Type:** {button_def.label}',
        ]
        filled = [(k, v) for k, v in form_values.items() if str(v).strip()]
        if filled:
            lines.append('')
            for k, v in filled:
                label = k.replace('_', ' ').title()
                lines.append(f'**{label}:** {str(v)[:800]}')
        body = '\n'.join(lines)[:4096]
        emb = discord.Embed(
            title='Ticket opened',
            description=body,
            color=self.runtime.ticket.ticket_embed_color,
        )
        try:
            await log_ch.send(embed=emb)
            FLOW.event_block(
                'TRANSCRIPT — OPEN LOGGED',
                [
                    ('Log channel', fmt_channel_named(log_ch)),
                    ('Ticket', fmt_ch(ticket_channel.id)),
                ],
            )
        except discord.HTTPException as exc:
            LOG.warning('Could not post ticket-open log to transcript channel: %s', exc)

    async def _ensure_panel_channel(self, interaction: discord.Interaction) -> bool:
        if interaction.channel_id != self.runtime.ticket.panel_channel_id:
            await interaction.response.send_message(
                f'This panel only works in <#{self.runtime.ticket.panel_channel_id}>.', ephemeral=True
            )
            return False
        return True

    async def create_ticket_from_request(
        self,
        interaction: discord.Interaction,
        button_def: ButtonDefinition,
        form_values: Dict[str, str],
    ) -> None:
        guild = interaction.guild
        user = interaction.user
        if guild is None:
            FLOW.event_block(
                'TICKET — REJECTED (NOT IN SERVER)',
                [
                    ('Flow key', button_def.key),
                    ('Button label', button_def.label),
                    ('User', fmt_member(user)),
                ],
            )
            FLOW.note('Why: interactions must run inside a guild for this ticket system.')
            await interaction.response.send_message('This can only be used inside the server.', ephemeral=True)
            return

        FLOW.section('TICKET — OPEN REQUEST')
        FLOW.kv('Flow key', button_def.key)
        FLOW.kv('Button label', button_def.label)
        FLOW.kv('User', fmt_member(user))
        FLOW.kv('Clicked in', fmt_ch(interaction.channel_id))
        FLOW.note(
            'What happens next: bot checks for an existing open ticket of this type,',
            'then creates a private text channel under the configured category.',
        )
        FLOW.rule()

        existing = await self.store.find_open_ticket(guild.id, user.id, button_def.key)
        if existing:
            ex_id = int(existing['channel_id'])
            FLOW.section('TICKET — ALREADY OPEN (STOPPED)')
            FLOW.kv('User', fmt_member(user))
            FLOW.kv('Flow key', button_def.key)
            FLOW.kv('Existing channel', fmt_ch(ex_id))
            FLOW.note(
                'Why: config limits one open ticket per user per flow (max_open_per_user).',
                'User was told in Discord to use the existing channel.',
            )
            FLOW.rule()
            await interaction.response.send_message(
                f'You already have an open {button_def.label} ticket: <#{existing["channel_id"]}>',
                ephemeral=True,
            )
            return

        category = guild.get_channel(self.runtime.ticket.ticket_category_id)
        if not isinstance(category, discord.CategoryChannel):
            FLOW.section('TICKET — CONFIG ERROR')
            FLOW.kv('ticket_category_id', fmt_ch(self.runtime.ticket.ticket_category_id))
            FLOW.warn_note(
                'ERROR: ticket_category_id is not a category (or ID is wrong).',
                'Fix config.json: set ticket_category_id to a real category the bot can manage.',
            )
            FLOW.rule()
            LOG.error('Invalid ticket_category_id %s', self.runtime.ticket.ticket_category_id)
            await interaction.response.send_message('Ticket category is not configured correctly.', ephemeral=True)
            return

        overwrite_map: Dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                manage_channels=True,
                manage_messages=True,
                read_message_history=True,
            ),
        }

        for role_id in self.runtime.ticket.support_role_ids + self.runtime.ticket.admin_role_ids:
            role = guild.get_role(role_id)
            if role:
                overwrite_map[role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    manage_messages=True,
                )

        safe_name = ''.join(c.lower() if c.isalnum() else '-' for c in user.display_name).strip('-') or f'user-{user.id}'
        channel_name = f'{button_def.ticket_name_prefix}-{safe_name}'[:95]
        topic = self.runtime.ticket.topic_template.format(ticket_type=button_def.key, user_id=user.id, username=user.name)
        channel = await guild.create_text_channel(
            name=channel_name,
            category=category,
            topic=topic,
            overwrites=overwrite_map,
            reason=f'{button_def.label} ticket opened by {user}',
        )

        body = (button_def.intro_body or '').strip()
        embed = discord.Embed(
            title=button_def.intro_title,
            description=body if body else None,
            color=self.runtime.ticket.ticket_embed_color,
        )
        embed.add_field(name='Opened By', value=user.mention, inline=True)
        embed.add_field(name='Ticket Type', value=button_def.label, inline=True)
        if form_values:
            lines = [f'**{key.replace("_", " ").title()}:** {value}' for key, value in form_values.items() if value]
            if lines:
                embed.add_field(name='Form Details', value='\n'.join(lines)[:1024], inline=False)
        ft = (self.runtime.ticket.footer_text or '').strip()
        if ft:
            embed.set_footer(text=ft)

        mentions = ' '.join(f'<@&{role_id}>' for role_id in self.runtime.ticket.support_role_ids)
        await channel.send(content=(f'{mentions} {user.mention}'.strip()), embed=embed, view=self.close_view)

        record = {
            'guild_id': guild.id,
            'owner_id': user.id,
            'owner_name': user.name,
            'ticket_type': button_def.key,
            'button_label': button_def.label,
            'is_open': True,
            'created_at': discord.utils.utcnow().isoformat(),
            'form_values': form_values,
        }
        await self.store.upsert_ticket(channel.id, record)
        await self._maybe_log_ticket_opened(guild, channel, user, button_def, form_values)

        form_preview = ', '.join(f'{k}={v!r}' for k, v in form_values.items()) if form_values else '(no modal fields)'
        FLOW.section('TICKET — CREATED')
        FLOW.kv('New channel', fmt_channel_named(channel))
        FLOW.kv('Channel topic', topic[:200] + ('…' if len(topic) > 200 else ''))
        FLOW.kv('Owner', fmt_member(user))
        FLOW.kv('Flow key', button_def.key)
        FLOW.kv('Form snapshot', form_preview[:500] + ('…' if len(form_preview) > 500 else ''))
        FLOW.explain(
            'Support/admin roles were granted channel access; @everyone cannot see the channel.',
            'First message pings support roles (if configured) and the opener.',
            'Ticket record saved to tickets.json so close/delete can run cleanly.',
        )
        FLOW.rule()

        if button_def.sheet_route:
            try:
                await self.sheet_client.submit(
                    button_def.sheet_route,
                    {
                        'channel_id': channel.id,
                        'guild_id': guild.id,
                        'user_id': user.id,
                        'username': user.name,
                        'display_name': user.display_name,
                        'ticket_type': button_def.key,
                        'ticket_label': button_def.label,
                        'created_at': record['created_at'],
                        'values': form_values,
                    },
                )
            except Exception:
                FLOW.section('GOOGLE SHEET — SUBMIT FAILED')
                FLOW.kv('Route', button_def.sheet_route)
                FLOW.kv('Ticket channel', fmt_ch(channel.id))
                FLOW.kv('User', fmt_user(user.id))
                FLOW.warn_note(
                    'The ticket still exists in Discord; staff saw a short notice in the channel.',
                    'See bot.log for the HTTP error / traceback.',
                )
                FLOW.rule()
                LOG.exception(
                    'Sheet FAIL | route=%s | ticket=%s | user=%s',
                    button_def.sheet_route,
                    fmt_ch(channel.id),
                    fmt_user(user.id),
                )
                await channel.send('Note: form data could not be forwarded to the sheet endpoint. Staff can still handle this ticket here.')

        if interaction.response.is_done():
            await interaction.followup.send(f'Your ticket is ready: {channel.mention}', ephemeral=True)
        else:
            await interaction.response.send_message(f'Your ticket is ready: {channel.mention}', ephemeral=True)

    async def close_ticket(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            FLOW.event_block(
                'TICKET — CLOSE REJECTED',
                [
                    ('Reason', 'Not a server text channel (e.g. DMs or wrong context)'),
                    ('Channel ref', fmt_ch(interaction.channel_id) if interaction.channel_id else '(?)'),
                    ('Actor', fmt_member(interaction.user) if interaction.user else '?'),
                ],
            )
            await interaction.response.send_message('This is not a ticket channel.', ephemeral=True)
            return

        record = await self.store.delete_ticket(channel.id)
        if not record:
            FLOW.event_block(
                'TICKET — CLOSE IGNORED (NOT TRACKED)',
                [
                    ('Channel', fmt_channel_named(channel)),
                    ('Actor', fmt_member(interaction.user) if interaction.user else '?'),
                ],
            )
            FLOW.note(
                'Why: tickets.json had no open record for this channel.',
                'It may be a normal channel, or the store was reset while the channel stayed open.',
            )
            FLOW.rule()
            await interaction.response.send_message('This channel is not tracked as an open ticket.', ephemeral=True)
            return

        closer = fmt_member(interaction.user) if interaction.user else '?'
        FLOW.section('TICKET — CLOSING')
        FLOW.kv('Channel', fmt_channel_named(channel))
        FLOW.kv('Original owner', fmt_user(int(record['owner_id'])))
        FLOW.kv('Ticket type', record.get('button_label', '?'))
        FLOW.kv('Closed by', closer)
        FLOW.kv('Delay seconds', self.runtime.ticket.close_delay_seconds)
        FLOW.note(
            'What happens: optional transcript embed, ephemeral countdown to the closer,',
            'wait, then channel delete. Record already removed from tickets.json.',
        )
        FLOW.rule()

        if self.runtime.ticket.transcript_channel_id:
            transcript_channel = interaction.guild.get_channel(self.runtime.ticket.transcript_channel_id) if interaction.guild else None
            if isinstance(transcript_channel, discord.TextChannel):
                summary = discord.Embed(
                    title='Ticket Closed',
                    color=self.runtime.ticket.ticket_embed_color,
                    description=f'Channel: {channel.mention}\nOwner: <@{record["owner_id"]}>\nType: {record["button_label"]}',
                )
                summary.set_footer(text=f'Closed by {interaction.user}')
                await transcript_channel.send(embed=summary)
                FLOW.event_block(
                    'TRANSCRIPT — POSTED',
                    [
                        ('Log channel', fmt_channel_named(transcript_channel)),
                        ('Ticket was', fmt_ch(channel.id)),
                    ],
                )

        await interaction.response.send_message(
            f'Ticket will close in {self.runtime.ticket.close_delay_seconds} seconds.', ephemeral=True
        )
        await asyncio.sleep(self.runtime.ticket.close_delay_seconds)
        await channel.delete(reason=f'Ticket closed by {interaction.user}')
        FLOW.event_block('TICKET — CHANNEL DELETED', [('Was', fmt_ch(channel.id))])


runtime = ConfigLoader.load()
bot = RSTicketBot(runtime)


@bot.tree.command(name='ticketpanel', description='Post or refresh the cashout ticket panel.')
async def ticketpanel(interaction: discord.Interaction) -> None:
    assert isinstance(interaction.client, RSTicketBot)
    bot_ref = interaction.client
    if not await is_ticket_admin(interaction):
        FLOW.event_block(
            'COMMAND — /ticketpanel DENIED',
            [
                ('Actor', fmt_member(interaction.user)),
                ('Channel', fmt_ch(interaction.channel_id)),
            ],
        )
        FLOW.note('Why: user is not administrator and has no configured admin_role_ids match.')
        FLOW.rule()
        await interaction.response.send_message('You do not have permission to use this command.', ephemeral=True)
        return
    if not await bot_ref._ensure_panel_channel(interaction):
        FLOW.event_block(
            'COMMAND — /ticketpanel WRONG CHANNEL',
            [
                ('Actor', fmt_member(interaction.user)),
                ('Current channel', fmt_ch(interaction.channel_id)),
                ('Required panel', fmt_ch(bot_ref.runtime.ticket.panel_channel_id)),
            ],
        )
        FLOW.note('Why: panel must only be posted from the configured panel channel (prevents stray panels).')
        FLOW.rule()
        return

    panel_ch = interaction.channel
    if not isinstance(panel_ch, discord.TextChannel):
        await interaction.response.send_message('Use /ticketpanel in a text channel.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await bot_ref.send_panel_card(panel_ch)
    await interaction.followup.send('Ticket panel posted.', ephemeral=True)
    FLOW.section('COMMAND — /ticketpanel POSTED')
    FLOW.kv('Actor', fmt_member(interaction.user))
    FLOW.kv('Channel', fmt_ch(interaction.channel_id))
    FLOW.note(
        'What was sent: embed + button row (persistent views) so users can open ticket flows.',
    )
    FLOW.rule()


@bot.tree.command(name='ticketadd', description='Add a member to the current ticket.')
@app_commands.describe(member='Member to add to this ticket channel')
async def ticketadd(interaction: discord.Interaction, member: discord.Member) -> None:
    if not await is_ticket_admin(interaction):
        await interaction.response.send_message('You do not have permission to use this command.', ephemeral=True)
        return
    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message('Use this inside a ticket channel.', ephemeral=True)
        return
    await channel.set_permissions(
        member,
        view_channel=True,
        send_messages=True,
        read_message_history=True,
        attach_files=True,
        embed_links=True,
    )
    await interaction.response.send_message(f'Added {member.mention} to {channel.mention}.', ephemeral=True)
    FLOW.event_block(
        'COMMAND — /ticketadd',
        [
            ('Actor', fmt_member(interaction.user)),
            ('Added member', fmt_member(member)),
            ('Ticket channel', fmt_channel_named(channel)),
        ],
    )


@bot.tree.command(name='ticketremove', description='Remove a member from the current ticket.')
@app_commands.describe(member='Member to remove from this ticket channel')
async def ticketremove(interaction: discord.Interaction, member: discord.Member) -> None:
    if not await is_ticket_admin(interaction):
        await interaction.response.send_message('You do not have permission to use this command.', ephemeral=True)
        return
    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message('Use this inside a ticket channel.', ephemeral=True)
        return
    await channel.set_permissions(member, overwrite=None)
    await interaction.response.send_message(f'Removed {member.mention} from {channel.mention}.', ephemeral=True)
    FLOW.event_block(
        'COMMAND — /ticketremove',
        [
            ('Actor', fmt_member(interaction.user)),
            ('Removed member', fmt_member(member)),
            ('Ticket channel', fmt_channel_named(channel)),
        ],
    )


@bot.tree.command(name='ticketclose', description='Close the current ticket channel.')
async def ticketclose(interaction: discord.Interaction) -> None:
    assert isinstance(interaction.client, RSTicketBot)
    await interaction.client.close_ticket(interaction)


if __name__ == '__main__':
    configure_logging()
    tk = bot.runtime.ticket
    support_roles = ', '.join(fmt_role(r) for r in tk.support_role_ids if r) or '(none configured)'
    admin_roles = ', '.join(fmt_role(r) for r in tk.admin_role_ids if r) or '(none configured)'
    FLOW.title('RS CASHOUT TICKET BOT')
    FLOW.note(
        'Mode: live bot — connects to Discord and handles real interactions.',
        'Logs below explain what the process is doing in plain language.',
    )
    FLOW.rule()
    FLOW.section('1) CONFIG LOADED (config.json + messages.json)')
    FLOW.kv('Guild ID', tk.guild_id)
    FLOW.kv('Panel channel', fmt_ch(tk.panel_channel_id))
    FLOW.kv('Ticket category', fmt_ch(tk.ticket_category_id))
    FLOW.kv('Transcript channel', fmt_ch(tk.transcript_channel_id) if tk.transcript_channel_id else '(off)')
    FLOW.kv('Auto-post panel on ready', 'yes' if tk.auto_post_panel_on_ready else 'no')
    FLOW.kv('Support roles', support_roles)
    FLOW.kv('Admin roles', admin_roles)
    FLOW.kv('Google Sheet', 'enabled' if bot.runtime.sheet.enabled else 'disabled')
    FLOW.kv('Ticket flows', ', '.join(f'{b.key} ({b.label})' for b in tk.buttons))
    FLOW.explain(
        'Panel channel: only place /ticketpanel is allowed to run.',
        'Ticket category: new private ticket channels are created here.',
        'Support roles: get pinged on new tickets when IDs are non-zero.',
    )
    if not tk.support_role_ids or all(x == 0 for x in tk.support_role_ids):
        FLOW.warn_note(
            'WARNING: support_role_ids look unset (0).',
            'Staff will not be pinged on new tickets until real role IDs are in config.',
        )
    FLOW.rule()
    FLOW.section('CONNECTING')
    FLOW.kv('Next step', 'Open Discord gateway session and run setup_hook (slash sync) then on_ready.')
    FLOW.rule()
    bot.run(runtime.token)
