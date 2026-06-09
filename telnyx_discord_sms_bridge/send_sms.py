from __future__ import annotations

import argparse
import asyncio
import logging

from app.config import AppConfig
from app.discord_client import DiscordClient
from app.logging_setup import setup_logging
from app.telnyx_client import TelnyxClient


async def main() -> None:
    parser = argparse.ArgumentParser(description="Send an outbound SMS through Telnyx.")
    parser.add_argument("--to", required=True, help="Destination phone number in E.164 format")
    parser.add_argument("--text", required=True, help="SMS body text")
    parser.add_argument("--from", dest="from_number", default=None, help="Optional sender Telnyx number")
    args = parser.parse_args()

    config = AppConfig.load()
    setup_logging(config.log_level)

    log = logging.getLogger("send_sms_cli")
    telnyx = TelnyxClient(config)
    discord = DiscordClient(config)

    log.info("event=cli_send_start reason=operator_called_send_sms")
    try:
        from_number = config.resolve_from_number(args.from_number)
    except ValueError as exc:
        log.error("event=cli_send_rejected reason=invalid_from_number error=%s", exc)
        raise SystemExit(1) from exc
    response = await telnyx.send_sms(to_number=args.to, text=args.text, from_number=from_number)
    await discord.post_outbound_notice(
        to_number=args.to,
        text=args.text,
        telnyx_response=response,
        from_number=from_number,
    )
    log.info("event=cli_send_done reason=message_sent_and_logged")


if __name__ == "__main__":
    asyncio.run(main())
