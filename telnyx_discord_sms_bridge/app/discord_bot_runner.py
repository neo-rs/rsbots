from __future__ import annotations

import asyncio
import logging
import threading
from typing import TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from app.conversation_service import ConversationService

log = logging.getLogger("discord_bot")


def _interaction_custom_id(interaction: discord.Interaction) -> str:
    direct = getattr(interaction, "custom_id", None)
    if direct:
        return str(direct)
    data = interaction.data
    if data is None:
        return ""
    if isinstance(data, dict):
        return str(data.get("custom_id") or "")
    return str(getattr(data, "custom_id", "") or "")


class SendMessageModal(discord.ui.Modal, title="Send SMS"):
    def __init__(self, *, our_line: str, remote_party: str, conversations: "ConversationService"):
        super().__init__()
        self.our_line = our_line
        self.remote_party = remote_party
        self.conversations = conversations
        self.message = discord.ui.TextInput(
            label="Message",
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=True,
        )
        self.add_item(self.message)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            await self.conversations.send_from_discord(
                our_line=self.our_line,
                remote_party=self.remote_party,
                text=str(self.message.value or ""),
            )
            await interaction.followup.send("Sent.", ephemeral=True)
        except Exception as exc:
            from app.telnyx_errors import format_send_error

            log.exception("event=discord_send_modal_failed our_line=%s remote=%s", self.our_line, self.remote_party)
            await interaction.followup.send(format_send_error(exc), ephemeral=True)


class EditNameModal(discord.ui.Modal, title="Edit contact name"):
    def __init__(
        self,
        *,
        our_line: str,
        remote_party: str,
        conversations: "ConversationService",
        current: str = "",
    ):
        super().__init__()
        self.our_line = our_line
        self.remote_party = remote_party
        self.conversations = conversations
        self.name = discord.ui.TextInput(
            label="Display name",
            max_length=64,
            required=True,
            default=current,
        )
        self.add_item(self.name)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            await self.conversations.rename_contact(
                our_line=self.our_line,
                remote_party=self.remote_party,
                display_name=str(self.name.value or ""),
            )
            await interaction.followup.send("Name updated.", ephemeral=True)
        except Exception as exc:
            log.exception("event=discord_rename_modal_failed our_line=%s remote=%s", self.our_line, self.remote_party)
            await interaction.followup.send(f"Rename failed: {exc}", ephemeral=True)


class DiscordBotRunner:
    """Discord bot on a dedicated thread/loop so FastAPI does not starve interactions."""

    def __init__(self, *, token: str, conversations: "ConversationService"):
        intents = discord.Intents.default()
        self.client = discord.Client(intents=intents)
        self.token = token
        self.conversations = conversations
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._setup_events()

    def _setup_events(self) -> None:
        @self.client.event
        async def on_ready() -> None:
            log.info("event=discord_bot_ready user=%s id=%s", self.client.user, getattr(self.client.user, "id", "?"))
            self._ready.set()

        @self.client.event
        async def on_interaction(interaction: discord.Interaction) -> None:
            if interaction.type != discord.InteractionType.component:
                return

            custom_id = _interaction_custom_id(interaction)
            log.info(
                "event=discord_interaction_received type=component custom_id=%s user=%s",
                custom_id,
                getattr(interaction.user, "id", "?"),
            )

            if not custom_id.startswith("telnyx:"):
                return

            try:
                parsed = self.conversations.parse_custom_id(custom_id)
                if not parsed:
                    log.warning("event=discord_interaction_rejected reason=unparsed_custom_id custom_id=%s", custom_id)
                    await interaction.response.send_message("Unknown thread.", ephemeral=True)
                    return
                our_line, remote_party = parsed

                if custom_id.startswith("telnyx:send:"):
                    await interaction.response.send_modal(
                        SendMessageModal(
                            our_line=our_line,
                            remote_party=remote_party,
                            conversations=self.conversations,
                        )
                    )
                    log.info("event=discord_send_modal_opened our_line=%s remote=%s", our_line, remote_party)
                    return

                if custom_id.startswith("telnyx:rename:"):
                    key = f"{our_line}|{remote_party}"
                    thread = self.conversations.store.get_thread(key) or {}
                    current = str(thread.get("display_name") or "")
                    await interaction.response.send_modal(
                        EditNameModal(
                            our_line=our_line,
                            remote_party=remote_party,
                            conversations=self.conversations,
                            current=current,
                        )
                    )
                    log.info("event=discord_rename_modal_opened our_line=%s remote=%s", our_line, remote_party)
                    return

                log.warning("event=discord_interaction_rejected reason=unknown_action custom_id=%s", custom_id)
                await interaction.response.send_message("Unknown action.", ephemeral=True)
            except Exception:
                log.exception("event=discord_interaction_failed custom_id=%s", custom_id)
                if not interaction.response.is_done():
                    await interaction.response.send_message("Button handler error — check bridge logs.", ephemeral=True)

    def start_background(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _run() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self.client.start(self.token))
            except Exception:
                log.exception("event=discord_bot_thread_crashed")

        self._thread = threading.Thread(target=_run, name="telnyx-discord-bot", daemon=True)
        self._thread.start()

    async def wait_ready(self, timeout: float = 90) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if self._ready.is_set():
                return
            await asyncio.sleep(0.2)
        raise TimeoutError("Discord bot did not become ready in time")

    async def close(self) -> None:
        if self._loop and self._loop.is_running() and not self.client.is_closed():
            future = asyncio.run_coroutine_threadsafe(self.client.close(), self._loop)
            try:
                await asyncio.wait_for(asyncio.wrap_future(future), timeout=3)
            except Exception:
                log.warning("event=discord_bot_close_timeout reason=daemon_thread_will_exit_with_process")

    async def _run_on_bot_loop(self, coro):
        if not self._loop:
            raise RuntimeError("Discord bot loop is not running")
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            return await coro
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return await asyncio.wrap_future(future)

    def _build_view(self, custom_id_key: str) -> discord.ui.View:
        view = discord.ui.View(timeout=None)
        send_id = f"telnyx:send:{custom_id_key}"
        rename_id = f"telnyx:rename:{custom_id_key}"
        view.add_item(discord.ui.Button(label="Send message", style=discord.ButtonStyle.primary, custom_id=send_id))
        view.add_item(discord.ui.Button(label="Edit name", style=discord.ButtonStyle.secondary, custom_id=rename_id))
        return view

    async def post_thread_message(self, *, channel_id: int, content: str, custom_id_key: str) -> int:
        await self.wait_ready()

        async def _impl() -> int:
            channel = self.client.get_channel(channel_id)
            if channel is None:
                channel = await self.client.fetch_channel(channel_id)
            if not isinstance(channel, discord.TextChannel):
                raise RuntimeError(f"Channel {channel_id} is not a text channel")
            msg = await channel.send(content=content, view=self._build_view(custom_id_key))
            return int(msg.id)

        return int(await self._run_on_bot_loop(_impl()))

    async def edit_thread_message(
        self,
        *,
        channel_id: int,
        message_id: int,
        content: str,
        custom_id_key: str,
    ) -> None:
        await self.wait_ready()

        async def _impl() -> None:
            channel = self.client.get_channel(channel_id)
            if channel is None:
                channel = await self.client.fetch_channel(channel_id)
            if not isinstance(channel, discord.TextChannel):
                raise RuntimeError(f"Channel {channel_id} is not a text channel")
            msg = await channel.fetch_message(message_id)
            await msg.edit(content=content, view=self._build_view(custom_id_key))

        await self._run_on_bot_loop(_impl())
