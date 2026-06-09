from __future__ import annotations

import argparse
import asyncio
import logging

from app.config import AppConfig
from app.discord_client import DiscordClient
from app.logging_setup import setup_logging
from app.telnyx_client import TelnyxClient


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send test SMS in both directions between configured Telnyx numbers.",
    )
    parser.add_argument(
        "--text-a-to-b",
        default="Bridge test: local line -> toll-free line",
        help="Message sent from the first configured number to the second",
    )
    parser.add_argument(
        "--text-b-to-a",
        default="Bridge test: toll-free line -> local line",
        help="Message sent from the second configured number to the first",
    )
    args = parser.parse_args()

    config = AppConfig.load()
    setup_logging(config.log_level)

    log = logging.getLogger("test_bidirectional")
    numbers = config.allowed_from_numbers()
    if len(numbers) < 2:
        raise RuntimeError(
            f"Need at least two configured from numbers for bidirectional testing. Found: {numbers}"
        )

    first, second = numbers[0], numbers[1]
    telnyx = TelnyxClient(config)
    discord = DiscordClient(config)

    log.info("event=bidirectional_test_start first=%s second=%s", first, second)

    response_a_to_b = await telnyx.send_sms(from_number=first, to_number=second, text=args.text_a_to_b)
    await discord.post_outbound_notice(
        from_number=first,
        to_number=second,
        text=args.text_a_to_b,
        telnyx_response=response_a_to_b,
    )

    response_b_to_a = await telnyx.send_sms(from_number=second, to_number=first, text=args.text_b_to_a)
    await discord.post_outbound_notice(
        from_number=second,
        to_number=first,
        text=args.text_b_to_a,
        telnyx_response=response_b_to_a,
    )

    log.info("event=bidirectional_test_done reason=both_directions_sent")


if __name__ == "__main__":
    asyncio.run(main())
