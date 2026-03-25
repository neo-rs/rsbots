import asyncio
import json
import logging
import re
import sys
import time
import uuid
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
TICKETS_PATH = BASE_DIR / 'tickets.json'
PROFILES_PATH = BASE_DIR / 'profiles.json'

LOG = logging.getLogger('rscashoutbot')
_FLOW_KV_COL = 13


class FlowReporter:
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


class ConfigError(RuntimeError):
    pass


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


def fmt_ch(channel_id: Optional[int]) -> str:
    if channel_id in (None, 0):
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


def sanitize_channel_name(value: str) -> str:
    cleaned = re.sub(r'[^a-zA-Z0-9]+', '-', value.lower()).strip('-')
    return cleaned or 'user'


def chunk_modal_fields(fields: List[Dict[str, Any]], size: int = 5) -> List[List[Dict[str, Any]]]:
    return [fields[i:i + size] for i in range(0, len(fields), size)]


def format_form_key(key: str) -> str:
    mapping = {
        'email': 'Email',
        'name_of_shoe': 'Name of Shoe',
        'sku': 'SKU',
        'condition': 'Condition',
        'size': 'Size',
        'qty': 'QTY',
        'price': 'Price',
        'notes': 'Notes',
    }
    return mapping.get(key, key.replace('_', ' ').title())


def normalize_condition_(raw: Any) -> str:
    """
    Normalize arbitrary modal input into the exact dropdown values expected
    by the template spreadsheet (cell C3 validation).
    """
    s = str(raw or '').strip()
    low = s.lower()

    allowed = ['Brand New', 'Brand New (Flawed)', 'Used - Like New', 'Used - Worn']
    for opt in allowed:
        if low == opt.lower():
            return opt

    def has(t: str) -> bool:
        return t in low

    # Brand new + flawed/defect/damage => Brand New (Flawed)
    if (has('new') or has('brand')) and (has('flaw') or has('defect') or has('damage') or has('flawed')):
        return 'Brand New (Flawed)'
    if has('flaw') or has('defect') or has('damage') or has('flawed'):
        return 'Brand New (Flawed)'

    # Used
    if has('used') or has('worn'):
        # Like new hints => Used - Like New
        if has('like') or has('clean') or has('good') or has('excellent') or has('near'):
            return 'Used - Like New'
        return 'Used - Worn'

    if has('worn'):
        return 'Used - Worn'
    if has('like'):
        return 'Used - Like New'

    # Default: treat as Brand New (avoids spreadsheet validation crashes)
    return 'Brand New'


def configure_logging() -> None:
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
    logging.getLogger('aiohttp').setLevel(logging.WARNING)


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
            key = str(raw_button['key'])
            message_def = dict(button_msgs.get(key, {}))
            if key == 'request_submit' and 'intro_title' not in message_def:
                legacy = button_msgs.get('request_custom_quote')
                if isinstance(legacy, dict):
                    message_def = {**legacy, **message_def}
            if 'intro_title' not in message_def or 'intro_body' not in message_def:
                raise ConfigError(f'messages.json -> ticket_system.buttons[{key!r}] must define intro_title and intro_body')
            buttons.append(
                ButtonDefinition(
                    key=key,
                    label=str(raw_button['label']),
                    style=str(raw_button.get('style', 'blurple')),
                    emoji=raw_button.get('emoji'),
                    ticket_name_prefix=str(raw_button['ticket_name_prefix']),
                    intro_title=str(message_def['intro_title']),
                    intro_body=str(message_def['intro_body']),
                    modal=raw_button.get('modal'),
                    sheet_route=raw_button.get('sheet_route'),
                    max_open_per_user=int(raw_button.get('max_open_per_user', 1)),
                )
            )

        ticket = TicketSettings(
            guild_id=int(ticket_cfg['guild_id']),
            panel_channel_id=int(ticket_cfg['panel_channel_id']),
            ticket_category_id=int(ticket_cfg['ticket_category_id']),
            transcript_channel_id=(int(ticket_cfg['transcript_channel_id']) if ticket_cfg.get('transcript_channel_id') else None),
            auto_post_panel_on_ready=bool(ticket_cfg.get('auto_post_panel_on_ready', True)),
            support_role_ids=[int(x) for x in ticket_cfg.get('support_role_ids', [])],
            admin_role_ids=[int(x) for x in ticket_cfg.get('admin_role_ids', [])],
            close_delay_seconds=int(ticket_cfg.get('close_delay_seconds', 10)),
            topic_template=str(ticket_cfg.get('topic_template', 'ticket_type={ticket_type};owner={user_id};username={username}')),
            panel_embed_color=int(str(ticket_cfg.get('panel_embed_color', '0x5865F2')), 16),
            ticket_embed_color=int(str(ticket_cfg.get('ticket_embed_color', '0x5865F2')), 16),
            panel_title=str(msg_cfg['panel_title']),
            panel_description=str(msg_cfg.get('panel_description', '') or ''),
            footer_text=str(msg_cfg.get('footer_text', '') or ''),
            buttons=buttons,
        )

        sheet = SheetIntegration(
            enabled=bool(sheet_cfg.get('enabled', False)),
            endpoint_url=str(sheet_cfg.get('endpoint_url', '')).strip(),
            auth_header_name=str(sheet_cfg.get('auth_header_name', 'X-API-Key')).strip(),
            auth_token=str(sheet_cfg.get('auth_token', '')).strip(),
            timeout_seconds=int(sheet_cfg.get('timeout_seconds', 20)),
        )
        if sheet.enabled and not sheet.endpoint_url:
            raise ConfigError('google_sheet.enabled is true but endpoint_url is empty')

        return RuntimeConfig(token=token, ticket=ticket, sheet=sheet)


class JsonStore:
    def __init__(self, path: Path, default_payload: Dict[str, Any]) -> None:
        self.path = path
        self.default_payload = default_payload
        self._lock = asyncio.Lock()
        if not self.path.exists():
            self.path.write_text(json.dumps(default_payload, indent=2), encoding='utf-8')

    async def read(self) -> Dict[str, Any]:
        async with self._lock:
            return json.loads(self.path.read_text(encoding='utf-8'))

    async def write(self, payload: Dict[str, Any]) -> None:
        async with self._lock:
            self.path.write_text(json.dumps(payload, indent=2), encoding='utf-8')


class TicketStore(JsonStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path, {'tickets': {}})

    async def upsert_ticket(self, channel_id: int, record: Dict[str, Any]) -> None:
        data = await self.read()
        data.setdefault('tickets', {})[str(channel_id)] = record
        await self.write(data)

    async def delete_ticket(self, channel_id: int) -> Optional[Dict[str, Any]]:
        data = await self.read()
        removed = data.setdefault('tickets', {}).pop(str(channel_id), None)
        await self.write(data)
        return removed

    async def count_open_tickets(self, guild_id: int, user_id: int, ticket_type: str) -> int:
        equiv = TICKET_TYPE_OPEN_EQUIV.get(ticket_type, frozenset({ticket_type}))
        data = await self.read()
        count = 0
        for _, record in data.get('tickets', {}).items():
            if (
                int(record.get('guild_id', 0)) == guild_id
                and int(record.get('owner_id', 0)) == user_id
                and str(record.get('ticket_type')) in equiv
                and bool(record.get('is_open', False))
            ):
                count += 1
        return count

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


class ProfileStore(JsonStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path, {'profiles': {}})

    async def get_profile(self, user_id: int) -> Dict[str, Any]:
        data = await self.read()
        return dict(data.get('profiles', {}).get(str(user_id), {}))

    async def upsert_profile(self, user_id: int, patch: Dict[str, Any]) -> Dict[str, Any]:
        data = await self.read()
        profiles = data.setdefault('profiles', {})
        existing = dict(profiles.get(str(user_id), {}))
        existing.update(patch)
        profiles[str(user_id)] = existing
        await self.write(data)
        return existing


class SheetClient:
    def __init__(self, config: SheetIntegration) -> None:
        self.config = config

    async def submit(self, route: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.config.enabled:
            return {'ok': False, 'disabled': True}

        headers = {'Content-Type': 'application/json'}
        if self.config.auth_token:
            headers[self.config.auth_header_name] = self.config.auth_token
        body = {'route': route, 'payload': payload}
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self.config.endpoint_url, json=body, headers=headers) as resp:
                raw = await resp.text()
                if resp.status >= 300:
                    raise RuntimeError(f'Sheet submission failed ({resp.status}): {raw[:500]}')
        try:
            parsed = json.loads(raw or '{}')
        except json.JSONDecodeError:
            parsed = {'ok': True, 'raw': raw}

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
        return parsed


class DynamicModal(discord.ui.Modal):
    def __init__(self, bot: 'RSTicketBot', button_def: ButtonDefinition, title: str, fields: List[Dict[str, Any]], defaults: Optional[Dict[str, str]] = None, prior_values: Optional[Dict[str, str]] = None, step_index: int = 1, total_steps: int = 1) -> None:
        super().__init__(title=title[:45])
        self.bot_ref = bot
        self.button_def = button_def
        self.fields = fields
        self.defaults = defaults or {}
        self.prior_values = prior_values or {}
        self.step_index = step_index
        self.total_steps = total_steps
        self.field_keys: List[str] = []
        for field in fields:
            key = str(field['key'])
            default_value = self.defaults.get(key)
            if default_value is None:
                default_value = str(field.get('default', ''))
            input_widget = discord.ui.TextInput(
                label=str(field['label'])[:45],
                placeholder=str(field.get('placeholder', ''))[:100],
                default=str(default_value)[:4000],
                required=bool(field.get('required', True)),
                style=discord.TextStyle.paragraph if field.get('paragraph', False) else discord.TextStyle.short,
                max_length=int(field.get('max_length', 400)),
            )
            self.field_keys.append(key)
            self.add_item(input_widget)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        values = dict(self.prior_values)
        for key, child in zip(self.field_keys, self.children):
            if isinstance(child, discord.ui.TextInput):
                values[key] = child.value.strip()

        all_fields = list(self.button_def.modal.get('fields', [])) if self.button_def.modal else []
        chunks = chunk_modal_fields(all_fields)
        if self.step_index < len(chunks):
            # Discord may reject `send_modal()` when replying to a modal submit interaction.
            # Safer approach: respond with an ephemeral "Continue" button; the button callback
            # then opens the next modal.
            nonce = self.bot_ref.create_modal_next_session(
                button_key=self.button_def.key,
                next_step_index=self.step_index + 1,
                defaults=self.defaults,
                prior_values=values,
            )
            view = discord.ui.View(timeout=180)
            view.add_item(ModalNextButton(nonce=nonce))
            await interaction.response.send_message(
                f'Next step ({self.step_index + 1}/{len(chunks)}). Click Continue.',
                ephemeral=True,
                view=view,
            )
            return

        await self.bot_ref.create_ticket_from_request(interaction, self.button_def, values)


class ModalNextButton(discord.ui.Button):
    def __init__(self, *, nonce: str) -> None:
        super().__init__(
            label='Continue',
            style=discord.ButtonStyle.primary,
            custom_id=f'rs_ticket:modal_next:{nonce}',
        )
        self.nonce = nonce

    async def callback(self, interaction: discord.Interaction) -> None:
        bot = interaction.client
        assert isinstance(bot, RSTicketBot)
        await bot._open_modal_next_step(interaction, nonce=self.nonce)


class CloseTicketView(discord.ui.View):
    def __init__(self, bot: 'RSTicketBot') -> None:
        super().__init__(timeout=None)
        self.bot_ref = bot

    @discord.ui.button(label='Close Ticket', style=discord.ButtonStyle.danger, custom_id='rs_ticket:close')
    async def close_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.bot_ref.close_ticket(interaction)


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
        await bot.launch_button_flow(interaction, self.button_def)


class TicketPanelView(discord.ui.View):
    def __init__(self, bot: 'RSTicketBot') -> None:
        super().__init__(timeout=None)
        for button_def in bot.runtime.ticket.buttons:
            self.add_item(TicketActionButton(button_def))


def has_any_configured_role(member: discord.Member, role_ids: List[int]) -> bool:
    member_role_ids = {role.id for role in member.roles}
    return any(role_id in member_role_ids for role_id in role_ids if role_id)


async def is_ticket_admin(interaction: discord.Interaction) -> bool:
    runtime_cfg = interaction.client.runtime if isinstance(interaction.client, RSTicketBot) else runtime
    admin_role_ids = runtime_cfg.ticket.admin_role_ids

    member: Optional[discord.Member] = None
    if isinstance(interaction.user, discord.Member):
        member = interaction.user
    else:
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
    if member.guild_permissions.administrator:
        return True
    return has_any_configured_role(member, admin_role_ids)


class RSTicketBot(commands.Bot):
    def __init__(self, runtime: RuntimeConfig) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        # Privileged intents (members/message_content) must be disabled unless
        # they are enabled in the Discord Developer Portal. We fetch members
        # on-demand via REST in `is_ticket_admin(...)`.
        intents.members = False
        intents.messages = True
        intents.message_content = False
        intents.presences = False
        super().__init__(command_prefix='!', intents=intents)
        self.runtime = runtime
        self.store = TicketStore(TICKETS_PATH)
        self.profile_store = ProfileStore(PROFILES_PATH)
        self.sheet_client = SheetClient(runtime.sheet)
        self._auto_panel_posted_once = False
        # Used to bridge multi-step modal state across modal submit -> button click.
        # Key is a nonce embedded in the Continue button custom_id.
        self._modal_next_sessions: Dict[str, Dict[str, Any]] = {}

    def create_modal_next_session(
        self,
        *,
        button_key: str,
        next_step_index: int,
        defaults: Dict[str, str],
        prior_values: Dict[str, str],
        ttl_seconds: int = 180,
    ) -> str:
        nonce = uuid.uuid4().hex[:16]
        self._modal_next_sessions[nonce] = {
            'button_key': button_key,
            'next_step_index': next_step_index,
            'defaults': defaults,
            'prior_values': prior_values,
            'expires_at': time.time() + ttl_seconds,
        }
        return nonce

    def consume_modal_next_session(self, nonce: str) -> Optional[Dict[str, Any]]:
        sess = self._modal_next_sessions.pop(nonce, None)
        if not sess:
            return None
        if float(sess.get('expires_at', 0)) < time.time():
            return None
        return sess

    async def _open_modal_next_step(
        self,
        interaction: discord.Interaction,
        *,
        nonce: str,
    ) -> None:
        sess = self.consume_modal_next_session(nonce)
        if not sess:
            await interaction.response.send_message('This step expired. Please try again.', ephemeral=True)
            return

        button_key = sess['button_key']
        button_def = self.get_button(button_key)
        if button_def is None or not button_def.modal:
            await interaction.response.send_message('This flow is no longer available.', ephemeral=True)
            return

        all_fields = list(button_def.modal.get('fields', [])) if button_def.modal else []
        chunks = chunk_modal_fields(all_fields)
        next_step_index = int(sess['next_step_index'])
        if next_step_index < 1 or next_step_index > len(chunks):
            await interaction.response.send_message('This step is invalid. Please try again.', ephemeral=True)
            return

        # DynamicModal expects `fields` to be chunks[step_index - 1]
        fields = chunks[next_step_index - 1]
        title = f'{button_def.label} ({next_step_index}/{len(chunks)})' if len(chunks) > 1 else button_def.label
        modal = DynamicModal(
            self,
            button_def,
            title,
            fields,
            defaults=sess.get('defaults') or {},
            prior_values=sess.get('prior_values') or {},
            step_index=next_step_index,
            total_steps=len(chunks),
        )
        await interaction.response.send_modal(modal)

    def get_button(self, key: str) -> Optional[ButtonDefinition]:
        for button in self.runtime.ticket.buttons:
            if button.key == key:
                return button
        return None

    async def setup_hook(self) -> None:
        self.close_view = CloseTicketView(self)
        self.panel_view = TicketPanelView(self)
        self.add_view(self.close_view)
        self.add_view(self.panel_view)

        guild = discord.Object(id=self.runtime.ticket.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        FLOW.section('2) SLASH COMMANDS — SYNC')
        FLOW.kv('Guild ID', self.runtime.ticket.guild_id)
        FLOW.note(
            'App commands were copied to this guild and synced with Discord.',
            'Primary panel command is now /cashout. Members can use /cashoutnew for another cashout submission.',
        )
        FLOW.rule()

    async def on_ready(self) -> None:
        if not self.user:
            return
        guild = self.get_guild(self.runtime.ticket.guild_id)
        FLOW.section('3) LOGIN — BOT ONLINE')
        FLOW.kv('Bot', fmt_member(self.user))
        if guild:
            FLOW.kv('Guild', f'{guild.name} ({guild.id})')
            FLOW.kv('Panel channel', fmt_channel_resolve(guild, self.runtime.ticket.panel_channel_id))
            FLOW.kv('Ticket category', fmt_channel_resolve(guild, self.runtime.ticket.ticket_category_id))
            FLOW.kv('Transcript channel', fmt_channel_resolve(guild, self.runtime.ticket.transcript_channel_id) if self.runtime.ticket.transcript_channel_id else '(off)')
        else:
            FLOW.warn_note('Configured guild is not visible to this bot session.')
        FLOW.rule()

        if guild and self.runtime.ticket.auto_post_panel_on_ready and not self._auto_panel_posted_once:
            self._auto_panel_posted_once = True
            panel_ch = guild.get_channel(self.runtime.ticket.panel_channel_id)
            if isinstance(panel_ch, discord.TextChannel):
                try:
                    await self.send_panel_card(panel_ch)
                    FLOW.event_block('PANEL — AUTO POSTED', [('Channel', fmt_channel_named(panel_ch))])
                except discord.HTTPException as exc:
                    FLOW.warn_note(f'Could not auto-post panel: {exc}')

    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        cmd = interaction.command.name if interaction.command else '?'
        FLOW.section('ERROR — SLASH COMMAND')
        FLOW.kv('Command', f'/{cmd}')
        FLOW.kv('Channel', fmt_ch(interaction.channel_id) if interaction.channel_id else '(none)')
        FLOW.kv('Actor', fmt_member(interaction.user) if interaction.user else '?')
        FLOW.kv('Error', repr(error))
        FLOW.rule()
        if interaction.response.is_done():
            await interaction.followup.send('Something went wrong. Check bot.log.', ephemeral=True)
        else:
            await interaction.response.send_message('Something went wrong. Check bot.log.', ephemeral=True)
        LOG.exception('App command error: %s', cmd, exc_info=error)

    def _build_panel_embed(self) -> discord.Embed:
        ticket = self.runtime.ticket
        embed = discord.Embed(
            title=ticket.panel_title,
            description=ticket.panel_description.strip() or None,
            color=ticket.panel_embed_color,
        )
        footer = ticket.footer_text.strip()
        if footer:
            embed.set_footer(text=footer)
        return embed

    async def send_panel_card(self, channel: discord.TextChannel) -> None:
        await self._maybe_delete_previous_panel_card(channel)
        await channel.send(embed=self._build_panel_embed(), view=self.panel_view)

    async def _maybe_delete_previous_panel_card(self, channel: discord.TextChannel) -> None:
        """
        Best-effort: delete the previous panel message card from this bot so
        restarts don't spam multiple cards.
        """
        if not self.user:
            return
        panel_title = (self.runtime.ticket.panel_title or '').strip()
        if not panel_title:
            return

        try:
            async for msg in channel.history(limit=30):
                if msg.author.id != self.user.id:
                    continue
                if not msg.embeds:
                    continue
                if any((e.title or '').strip() == panel_title for e in msg.embeds):
                    # Delete only one most-recent match.
                    await msg.delete()
                    return
        except discord.Forbidden:
            LOG.warning('No permission to delete old panel cards in %s', channel.id)
        except Exception as exc:
            LOG.warning('Could not delete old panel cards in %s: %s', channel.id, exc)

    async def _ensure_panel_channel(self, interaction: discord.Interaction) -> bool:
        if interaction.channel_id != self.runtime.ticket.panel_channel_id:
            await interaction.response.send_message(f'This panel only works in <#{self.runtime.ticket.panel_channel_id}>.', ephemeral=True)
            return False
        return True

    async def get_modal_defaults(self, user_id: int, button_def: ButtonDefinition) -> Dict[str, str]:
        profile = await self.profile_store.get_profile(user_id)
        defaults: Dict[str, str] = {}
        last_values = profile.get('last_request_values', {}) if isinstance(profile.get('last_request_values'), dict) else {}
        for field in button_def.modal.get('fields', []) if button_def.modal else []:
            key = str(field['key'])
            if key == 'email' and profile.get('email'):
                defaults[key] = str(profile['email'])
            elif key in last_values and last_values.get(key):
                defaults[key] = str(last_values[key])
        return defaults

    async def launch_button_flow(self, interaction: discord.Interaction, button_def: ButtonDefinition) -> None:
        if interaction.guild is None:
            await interaction.response.send_message('This can only be used inside the server.', ephemeral=True)
            return

        defaults = await self.get_modal_defaults(interaction.user.id, button_def)
        fields = list(button_def.modal.get('fields', [])) if button_def.modal else []
        if fields:
            chunks = chunk_modal_fields(fields)
            modal = DynamicModal(
                self,
                button_def,
                f'{button_def.label} (1/{len(chunks)})' if len(chunks) > 1 else button_def.label,
                chunks[0],
                defaults=defaults,
                prior_values={},
                step_index=1,
                total_steps=len(chunks),
            )
            await interaction.response.send_modal(modal)
            return

        await self.create_ticket_from_request(interaction, button_def, {})

    async def _maybe_log_ticket_opened(self, guild: discord.Guild, ticket_channel: discord.TextChannel, opener: discord.abc.User, button_def: ButtonDefinition, form_values: Dict[str, str], extra_lines: Optional[List[str]] = None) -> None:
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
        if extra_lines:
            lines.extend(extra_lines)
        filled = [(k, v) for k, v in form_values.items() if str(v).strip()]
        if filled:
            lines.append('')
            for k, v in filled:
                lines.append(f'**{format_form_key(k)}:** {str(v)[:700]}')
        emb = discord.Embed(title='Ticket opened', description='\n'.join(lines)[:4096], color=self.runtime.ticket.ticket_embed_color)
        try:
            await log_ch.send(embed=emb)
        except discord.HTTPException as exc:
            LOG.warning('Could not post ticket-open log: %s', exc)

    async def create_ticket_from_request(self, interaction: discord.Interaction, button_def: ButtonDefinition, form_values: Dict[str, str]) -> None:
        guild = interaction.guild
        user = interaction.user
        if guild is None:
            if interaction.response.is_done():
                await interaction.followup.send('This can only be used inside the server.', ephemeral=True)
            else:
                await interaction.response.send_message('This can only be used inside the server.', ephemeral=True)
            return

        # Modal submissions (and slash command interactions) must receive an acknowledgment
        # quickly; the Google Sheets call can take longer than Discord's interaction window.
        # Deferring here guarantees we can still respond later without getting "Unknown interaction".
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)

        FLOW.section('TICKET — OPEN REQUEST')
        FLOW.kv('Flow key', button_def.key)
        FLOW.kv('Button label', button_def.label)
        FLOW.kv('User', fmt_member(user))
        FLOW.rule()

        open_limit = button_def.max_open_per_user
        if open_limit > 0:
            open_count = await self.store.count_open_tickets(guild.id, user.id, button_def.key)
            if open_count >= open_limit:
                existing = await self.store.find_open_ticket(guild.id, user.id, button_def.key)
                target = fmt_ch(existing['channel_id']) if existing else 'your existing ticket'
                msg = f'You already have the max number of open {button_def.label} tickets. Use {target} first.'
                if interaction.response.is_done():
                    await interaction.followup.send(msg, ephemeral=True)
                else:
                    await interaction.response.send_message(msg, ephemeral=True)
                return

        category = guild.get_channel(self.runtime.ticket.ticket_category_id)
        if not isinstance(category, discord.CategoryChannel):
            msg = 'Ticket category is not configured correctly.'
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
            return

        overwrite_map: Dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True, embed_links=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, manage_messages=True, read_message_history=True),
        }
        for role_id in self.runtime.ticket.support_role_ids + self.runtime.ticket.admin_role_ids:
            role = guild.get_role(role_id)
            if role:
                overwrite_map[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)

        suffix = datetime.utcnow().strftime('%m%d-%H%M%S') if button_def.max_open_per_user == 0 else ''
        base_name = sanitize_channel_name(getattr(user, 'display_name', user.name))
        channel_name = f"{button_def.ticket_name_prefix}-{base_name}{('-' + suffix) if suffix else ''}"[:95]
        topic = self.runtime.ticket.topic_template.format(ticket_type=button_def.key, user_id=user.id, username=user.name)
        channel = await guild.create_text_channel(
            name=channel_name,
            category=category,
            topic=topic,
            overwrites=overwrite_map,
            reason=f'{button_def.label} ticket opened by {user}',
        )

        # Load profile early so we can reuse one sheet per member if configured.
        profile = await self.profile_store.get_profile(user.id)
        existing_sheet_file_id = ''
        if button_def.key == 'request_submit':
            existing_sheet_file_id = str(profile.get('cashout_sheet_file_id') or '').strip()

        # Normalize condition to match spreadsheet dropdown validation exactly.
        norm_form_values = dict(form_values)
        if 'condition' in norm_form_values:
            norm_form_values['condition'] = normalize_condition_(norm_form_values.get('condition'))

        sheet_result: Dict[str, Any] = {}
        if button_def.sheet_route and self.runtime.sheet.enabled:
            payload = {
                'channel_id': channel.id,
                'guild_id': guild.id,
                'user_id': user.id,
                'username': user.name,
                'display_name': getattr(user, 'display_name', user.name),
                'ticket_type': button_def.key,
                'ticket_label': button_def.label,
                'created_at': discord.utils.utcnow().isoformat(),
                'values': norm_form_values,
            }
            if existing_sheet_file_id:
                payload['existing_file_id'] = existing_sheet_file_id
            try:
                sheet_result = await self.sheet_client.submit(button_def.sheet_route, payload)
            except Exception as exc:
                LOG.exception('Sheet submission failed for %s', button_def.key)
                sheet_result = {'ok': False, 'error': str(exc)}

        profile_patch: Dict[str, Any] = {'last_used_at': discord.utils.utcnow().isoformat()}
        if form_values.get('email'):
            profile_patch['email'] = form_values['email']
        if button_def.key == 'request_submit':
            profile_patch['last_request_values'] = norm_form_values
            if sheet_result.get('sheet_url'):
                profile_patch['last_sheet_url'] = sheet_result['sheet_url']
            if sheet_result.get('file_id'):
                profile_patch['cashout_sheet_file_id'] = str(sheet_result['file_id'])
        await self.profile_store.upsert_profile(user.id, profile_patch)

        embed = discord.Embed(
            title=button_def.intro_title,
            description=button_def.intro_body.strip() or None,
            color=self.runtime.ticket.ticket_embed_color,
        )
        embed.add_field(name='Opened By', value=user.mention, inline=True)
        embed.add_field(name='Ticket Type', value=button_def.label, inline=True)

        if norm_form_values:
            lines = [f'**{format_form_key(k)}:** {v}' for k, v in norm_form_values.items() if str(v).strip()]
            if lines:
                embed.add_field(name='Submitted Info', value='\n'.join(lines)[:1024], inline=False)

        if sheet_result.get('sheet_url'):
            embed.add_field(name='Cashout Sheet Copy', value=sheet_result['sheet_url'][:1024], inline=False)
            instructions = []
            if sheet_result.get('sheet_name'):
                instructions.append(f'**Sheet Name:** {sheet_result["sheet_name"]}')
            instructions.append('Fill out the sheet fully, then send the completed link back in this ticket when ready.')
            instructions.append('If you have multiple products, add additional rows in the same sheet (row 4+).')
            if sheet_result.get('view_url') and sheet_result.get('view_url') != sheet_result.get('sheet_url'):
                instructions.append(f'View link: {sheet_result["view_url"]}')
            embed.add_field(name='Next Step', value='\n'.join(instructions)[:1024], inline=False)
        elif button_def.key == 'request_submit':
            embed.add_field(name='Next Step', value='Staff will review your details here. If sheet integration is enabled later, your personal cashout sheet link will also appear here.', inline=False)

        footer = self.runtime.ticket.footer_text.strip()
        if footer:
            embed.set_footer(text=footer)

        mentions = ' '.join(f'<@&{role_id}>' for role_id in self.runtime.ticket.support_role_ids if role_id)
        await channel.send(content=(f'{mentions} {user.mention}'.strip()), embed=embed, view=self.close_view)

        if sheet_result.get('sheet_url'):
            await channel.send(
                f'Your personal cashout sheet is ready: {sheet_result["sheet_url"]}\n'
                'Please complete it using the required format, then drop the filled-out link back in this ticket.\n'
                'Multiple products? Add additional rows in the same sheet (row 4+).'
            )
        elif button_def.key == 'request_submit' and sheet_result.get('error'):
            await channel.send('Note: the Google Sheet copy could not be created automatically. Staff can still handle this ticket here.')

        record = {
            'guild_id': guild.id,
            'owner_id': user.id,
            'owner_name': user.name,
            'ticket_type': button_def.key,
            'button_label': button_def.label,
            'is_open': True,
            'created_at': discord.utils.utcnow().isoformat(),
            'form_values': form_values,
            'sheet_result': sheet_result,
        }
        await self.store.upsert_ticket(channel.id, record)

        extra_lines = []
        if sheet_result.get('sheet_url'):
            extra_lines.append(f'**Sheet Copy:** {sheet_result["sheet_url"]}')
        await self._maybe_log_ticket_opened(guild, channel, user, button_def, form_values, extra_lines=extra_lines)

        if button_def.key == 'request_submit':
            # Always DM so the member isn't left waiting silently, even if the
            # Apps Script copy fails (permissions, Drive errors, etc.).
            try:
                sheet_url = sheet_result.get('sheet_url')
                dm_description: str
                dm_embed = discord.Embed(
                    title='RS Cashout Submission Received',
                    color=self.runtime.ticket.ticket_embed_color,
                )
                if sheet_url:
                    dm_description = (
                        'Your personal cashout sheet copy is ready. Keep this link for reference and '
                        'send the completed version back in your ticket when finished.'
                    )
                    dm_embed.description = dm_description
                    dm_embed.add_field(name='Sheet Link', value=str(sheet_url)[:1024], inline=False)
                    dm_embed.add_field(name='Multiple products?', value='Add additional rows in the same sheet (row 4+).', inline=False)
                else:
                    dm_description = (
                        'Your ticket is ready. The Google Sheets auto-copy failed, so staff will handle the sheet.'
                    )
                    dm_embed.description = dm_description
                    err = sheet_result.get('error') or sheet_result.get('raw') or 'unknown_error'
                    dm_embed.add_field(name='Sheet Error', value=str(err)[:1024], inline=False)
                dm_embed.add_field(name='Ticket', value=channel.mention, inline=False)
                await user.send(embed=dm_embed)
            except discord.HTTPException:
                LOG.warning('Could not DM user %s for cashout ticket', user.id)

        msg = f'Your ticket is ready: {channel.mention}'
        if sheet_result.get('sheet_url'):
            msg += f'\nSheet copy: {sheet_result["sheet_url"]}'
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)

    async def close_ticket(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message('This is not a ticket channel.', ephemeral=True)
            return

        record = await self.store.delete_ticket(channel.id)
        if not record:
            await interaction.response.send_message('This channel is not tracked as an open ticket.', ephemeral=True)
            return

        if self.runtime.ticket.transcript_channel_id and interaction.guild:
            transcript_channel = interaction.guild.get_channel(self.runtime.ticket.transcript_channel_id)
            if isinstance(transcript_channel, discord.TextChannel):
                summary = discord.Embed(
                    title='Ticket closed',
                    color=self.runtime.ticket.ticket_embed_color,
                    description=f'Channel: {channel.mention}\nOwner: <@{record["owner_id"]}>\nType: {record.get("button_label", "?")}',
                )
                summary.set_footer(text=f'Closed by {interaction.user}')
                await transcript_channel.send(embed=summary)

        await interaction.response.send_message(f'Ticket will close in {self.runtime.ticket.close_delay_seconds} seconds.', ephemeral=True)
        await asyncio.sleep(self.runtime.ticket.close_delay_seconds)
        await channel.delete(reason=f'Ticket closed by {interaction.user}')


runtime = ConfigLoader.load()
bot = RSTicketBot(runtime)


@bot.tree.command(name='cashout', description='Post or refresh the RS cashout panel.')
async def cashout(interaction: discord.Interaction) -> None:
    assert isinstance(interaction.client, RSTicketBot)
    bot_ref = interaction.client
    if not await is_ticket_admin(interaction):
        await interaction.response.send_message('You do not have permission to use this command.', ephemeral=True)
        return
    if not await bot_ref._ensure_panel_channel(interaction):
        return
    panel_ch = interaction.channel
    if not isinstance(panel_ch, discord.TextChannel):
        await interaction.response.send_message('Use /cashout in a text channel.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await bot_ref.send_panel_card(panel_ch)
    await interaction.followup.send('Cashout panel posted.', ephemeral=True)


@bot.tree.command(name='cashoutnew', description='Open a new cashout submission form.')
async def cashoutnew(interaction: discord.Interaction) -> None:
    assert isinstance(interaction.client, RSTicketBot)
    button = interaction.client.get_button('request_submit')
    if not button:
        await interaction.response.send_message('Request/Submit is not configured right now.', ephemeral=True)
        return
    await interaction.client.launch_button_flow(interaction, button)


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
    await channel.set_permissions(member, view_channel=True, send_messages=True, read_message_history=True, attach_files=True, embed_links=True)
    await interaction.response.send_message(f'Added {member.mention} to {channel.mention}.', ephemeral=True)


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
        'Primary member flow is Request/Submit -> personal sheet copy -> private ticket.',
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
    FLOW.rule()
    bot.run(runtime.token)
