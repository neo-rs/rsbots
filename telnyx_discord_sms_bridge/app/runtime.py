from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from app.config import AppConfig
from app.conversation_service import ConversationService
from app.conversation_store import ConversationStore
from app.discord_bot_runner import DiscordBotRunner
from app.discord_client import DiscordClient
from app.telnyx_client import TelnyxClient

if TYPE_CHECKING:
    import asyncio


@dataclass
class BridgeRuntime:
    config: AppConfig
    telnyx: TelnyxClient
    discord: DiscordClient
    conversations: ConversationService
    discord_bot: DiscordBotRunner | None
    _bot_task: "asyncio.Task[None] | None" = None

    @classmethod
    def build(cls) -> "BridgeRuntime":
        config = AppConfig.load()
        conv_cfg = config.settings.get("conversations", {})
        data_file = Path(str(conv_cfg.get("data_file", "data/conversations.json")))
        store = ConversationStore(data_file)
        conversations = ConversationService(config=config, store=store)
        discord_bot = None
        if config.discord_bot_token:
            discord_bot = DiscordBotRunner(token=config.discord_bot_token, conversations=conversations)
            conversations.attach_bot(discord_bot)
        return cls(
            config=config,
            telnyx=TelnyxClient(config),
            discord=DiscordClient(config, conversations=conversations),
            conversations=conversations,
            discord_bot=discord_bot,
        )

    async def start_discord_bot(self) -> None:
        if not self.discord_bot:
            return
        self.discord_bot.start_background()

    async def stop_discord_bot(self) -> None:
        if self.discord_bot:
            await self.discord_bot.close()
