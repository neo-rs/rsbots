from __future__ import annotations

import asyncio
import logging

from app.config import AppConfig
from app.discord_client import DiscordClient
from app.logging_setup import setup_logging


async def main() -> None:
    config = AppConfig.load()
    setup_logging(config.log_level)

    log = logging.getLogger("test_discord")
    log.info("event=discord_test_start reason=operator_requested_test")
    discord = DiscordClient(config)
    await discord.post_test()
    log.info("event=discord_test_done reason=webhook_posted")


if __name__ == "__main__":
    asyncio.run(main())
