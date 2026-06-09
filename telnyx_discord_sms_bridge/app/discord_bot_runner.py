from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from app.conversation_service import ConversationService

log = logging.getLogger("discord_bot")


class SendMessageModal(discord.ui.Modal, title="Send SMS"):
    message = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )

    def __init__(self, *, our_line: str, remote_party: str, conversations: "ConversationService"):
        super().__init__()
        self.our_line = our_line
        self.remote_party = remote_party
        self.conversations = conversations

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.conversations.send_from_discord(
                our_line=self.our_line,
                remote_party=self.remote_party,
                text=str(self.message.value or ""),
            )
            await interaction.followup.send("Sent.", ephemeral=True)
        except Exception as exc:
            log.exception("event=discord_send_modal_failed")
            await interaction.followup.send(f"Send failed: {exc}", ephemeral=True)


class EditNameModal(discord.ui.Modal, title="Edit contact name"):
    name = discord.ui.TextInput(
        label="Display name",
        max_length=64,
        required=True,
    )

    def __init__(self, *, our_line: str, remote_party: str, conversations: "ConversationService", current: str = ""):
        super().__init__()
        self.our_line = our_line
        self.remote_party = remote_party
        self.conversations = conversations
        self.name.default = current

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            await self.conversations.rename_contact(
                our_line=self.our_line,
                remote_party=self.remote_party,
                display_name=str(self.name.value or ""),
            )
            await interaction.followup.send("Name updated.", ephemeral=True)
        except Exception as exc:
            log.exception("event=discord_rename_modal_failed")
            await interaction.followup.send(f"Rename failed: {exc}", ephemeral=True)


class DiscordBotRunner:
    def __init__(self, *, token: str, conversations: "ConversationService"):
        intents = discord.Intents.default()
        self.client = discord.Client(intents=intents)
        self.token = token
        self.conversations = conversations
        self._ready: asyncio.Event | None = None

        @self.client.event
        async def on_ready() -> None:
            log.info("event=discord_bot_ready user=%s", self.client.user)
            if self._ready is not None:
                self._ready.set()

        @self.client.event
        async def on_interaction(interaction: discord.Interaction) -> None:
            if interaction.type != discord.InteractionType.component:
                return
            custom_id = str(getattr(interaction.data, "custom_id", "") or "")
            if not custom_id.startswith("telnyx:"):
                return

            parsed = self.conversations.parse_custom_id(custom_id)
            if not parsed:
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

    async def wait_ready(self, timeout: float = 60) -> None:
        if self._ready is None:
            self._ready = asyncio.Event()
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    async def run(self) -> None:
        self._ready = asyncio.Event()
        await self.client.start(self.token)

    async def close(self) -> None:
        if not self.client.is_closed():
            await self.client.close()

    def _build_view(self, custom_id_key: str) -> discord.ui.View:
        view = discord.ui.View(timeout=None)

        send_id = f"telnyx:send:{custom_id_key}"
        rename_id = f"telnyx:rename:{custom_id_key}"

        send_btn = discord.ui.Button(label="Send message", style=discord.ButtonStyle.primary, custom_id=send_id)
        rename_btn = discord.ui.Button(label="Edit name", style=discord.ButtonStyle.secondary, custom_id=rename_id)
        view.add_item(send_btn)
        view.add_item(rename_btn)
        return view

    async def post_thread_message(self, *, channel_id: int, content: str, custom_id_key: str) -> int:
        await self.wait_ready()
        channel = self.client.get_channel(channel_id)
        if channel is None:
            channel = await self.client.fetch_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            raise RuntimeError(f"Channel {channel_id} is not a text channel")
        msg = await channel.send(content=content, view=self._build_view(custom_id_key))
        return int(msg.id)

    async def edit_thread_message(
        self,
        *,
        channel_id: int,
        message_id: int,
        content: str,
        custom_id_key: str,
    ) -> None:
        await self.wait_ready()
        channel = self.client.get_channel(channel_id)
        if channel is None:
            channel = await self.client.fetch_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            raise RuntimeError(f"Channel {channel_id} is not a text channel")
        msg = await channel.fetch_message(message_id)
        await msg.edit(content=content, view=self._build_view(custom_id_key))
