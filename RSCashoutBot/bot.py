import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / 'config.json'
MESSAGES_PATH = BASE_DIR / 'messages.json'
LOG_PATH = BASE_DIR / 'bot.log'


class ConfigError(RuntimeError):
    pass


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
            message_def = button_msgs.get(key, {})
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
            support_role_ids=[int(x) for x in ticket_cfg.get('support_role_ids', [])],
            admin_role_ids=[int(x) for x in ticket_cfg.get('admin_role_ids', [])],
            close_delay_seconds=int(ticket_cfg.get('close_delay_seconds', 10)),
            topic_template=str(ticket_cfg.get('topic_template', 'type={ticket_type};owner={user_id}')),
            panel_embed_color=int(str(ticket_cfg.get('panel_embed_color', '0x5865F2')), 16),
            ticket_embed_color=int(str(ticket_cfg.get('ticket_embed_color', '0x5865F2')), 16),
            panel_title=msg_cfg['panel_title'],
            panel_description=msg_cfg['panel_description'],
            footer_text=msg_cfg['footer_text'],
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
        data = await self.read()
        for channel_id, record in data.get('tickets', {}).items():
            if (
                int(record.get('guild_id', 0)) == guild_id
                and int(record.get('owner_id', 0)) == user_id
                and str(record.get('ticket_type')) == ticket_type
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


def is_ticket_admin(interaction: discord.Interaction) -> bool:
    if not isinstance(interaction.user, discord.Member):
        return False
    if interaction.user.guild_permissions.administrator:
        return True
    runtime_cfg = interaction.client.runtime if isinstance(interaction.client, RSTicketBot) else runtime
    return has_any_configured_role(interaction.user, runtime_cfg.ticket.admin_role_ids)


class RSTicketBot(commands.Bot):
    def __init__(self, runtime: RuntimeConfig) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.messages = True
        super().__init__(command_prefix='!', intents=intents)
        self.runtime = runtime
        self.store = JsonStore(BASE_DIR / 'tickets.json')
        self.sheet_client = SheetClient(runtime.sheet)
        self.close_view = CloseTicketView(self)
        self.panel_view = TicketPanelView(self)

    async def setup_hook(self) -> None:
        self.add_view(self.close_view)
        self.add_view(self.panel_view)
        guild = discord.Object(id=self.runtime.ticket.guild_id)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)

    async def on_ready(self) -> None:
        logging.info('Logged in as %s (%s)', self.user, self.user.id if self.user else 'unknown')

    def _build_panel_embed(self) -> discord.Embed:
        ticket = self.runtime.ticket
        embed = discord.Embed(
            title=ticket.panel_title,
            description=ticket.panel_description,
            color=ticket.panel_embed_color,
        )
        for button_def in ticket.buttons:
            embed.add_field(name=button_def.label, value=button_def.intro_body, inline=False)
        embed.set_footer(text=ticket.footer_text)
        return embed

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
            await interaction.response.send_message('This can only be used inside the server.', ephemeral=True)
            return

        existing = await self.store.find_open_ticket(guild.id, user.id, button_def.key)
        if existing:
            await interaction.response.send_message(
                f'You already have an open {button_def.label} ticket: <#{existing["channel_id"]}>',
                ephemeral=True,
            )
            return

        category = guild.get_channel(self.runtime.ticket.ticket_category_id)
        if not isinstance(category, discord.CategoryChannel):
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

        embed = discord.Embed(
            title=button_def.intro_title,
            description=button_def.intro_body,
            color=self.runtime.ticket.ticket_embed_color,
        )
        embed.add_field(name='Opened By', value=user.mention, inline=True)
        embed.add_field(name='Ticket Type', value=button_def.label, inline=True)
        if form_values:
            lines = [f'**{key.replace("_", " ").title()}:** {value}' for key, value in form_values.items() if value]
            if lines:
                embed.add_field(name='Form Details', value='\n'.join(lines)[:1024], inline=False)
        embed.set_footer(text=self.runtime.ticket.footer_text)

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
            except Exception as exc:
                logging.exception('Sheet submission failed: %s', exc)
                await channel.send('Note: form data could not be forwarded to the sheet endpoint. Staff can still handle this ticket here.')

        if interaction.response.is_done():
            await interaction.followup.send(f'Your ticket is ready: {channel.mention}', ephemeral=True)
        else:
            await interaction.response.send_message(f'Your ticket is ready: {channel.mention}', ephemeral=True)

    async def close_ticket(self, interaction: discord.Interaction) -> None:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message('This is not a ticket channel.', ephemeral=True)
            return

        record = await self.store.delete_ticket(channel.id)
        if not record:
            await interaction.response.send_message('This channel is not tracked as an open ticket.', ephemeral=True)
            return

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

        await interaction.response.send_message(
            f'Ticket will close in {self.runtime.ticket.close_delay_seconds} seconds.', ephemeral=True
        )
        await asyncio.sleep(self.runtime.ticket.close_delay_seconds)
        await channel.delete(reason=f'Ticket closed by {interaction.user}')


runtime = ConfigLoader.load()
bot = RSTicketBot(runtime)


@bot.tree.command(name='ticketpanel', description='Post or refresh the cashout ticket panel.')
async def ticketpanel(interaction: discord.Interaction) -> None:
    assert isinstance(interaction.client, RSTicketBot)
    bot_ref = interaction.client
    if not is_ticket_admin(interaction):
        await interaction.response.send_message('You do not have permission to use this command.', ephemeral=True)
        return
    if not await bot_ref._ensure_panel_channel(interaction):
        return

    embed = bot_ref._build_panel_embed()
    await interaction.channel.send(embed=embed, view=bot_ref.panel_view)
    await interaction.response.send_message('Ticket panel posted.', ephemeral=True)


@bot.tree.command(name='ticketadd', description='Add a member to the current ticket.')
@app_commands.describe(member='Member to add to this ticket channel')
async def ticketadd(interaction: discord.Interaction, member: discord.Member) -> None:
    if not is_ticket_admin(interaction):
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


@bot.tree.command(name='ticketremove', description='Remove a member from the current ticket.')
@app_commands.describe(member='Member to remove from this ticket channel')
async def ticketremove(interaction: discord.Interaction, member: discord.Member) -> None:
    if not is_ticket_admin(interaction):
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()],
    )
    bot.run(runtime.token)
