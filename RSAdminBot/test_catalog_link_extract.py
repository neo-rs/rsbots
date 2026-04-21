"""
Local harness: fetch a Discord message by link and print the catalog parse output.

Usage (PowerShell):
  $env:RSADMINBOT_TOKEN = "<bot token>"
  py -3 RSAdminBot/test_catalog_link_extract.py "https://ptb.discord.com/channels/<g>/<c>/<m>"

Notes:
- Requires the bot to be in the guild and have access to the channel/message.
- Uses the same parsing logic as `RSAdminBot/review_rs_server_listener.py`.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Optional

import discord

# Ensure RSAdminBot folder is importable when running from repo root.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from review_rs_server_listener import (  # noqa: E402
    _extract_discord_message_links,
    ReviewRSConfig,
    ReviewRSServerListener,
)


async def _fetch_message(client: discord.Client, *, channel_id: int, message_id: int) -> Optional[discord.Message]:
    try:
        ch = await client.fetch_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return None
        return await ch.fetch_message(int(message_id))
    except Exception:
        return None


async def main() -> int:
    token = (os.environ.get("RSADMINBOT_TOKEN") or os.environ.get("DISCORD_BOT_TOKEN") or "").strip()
    if not token:
        # Fall back to RSAdminBot/config.secrets.json
        try:
            secrets_path = os.path.join(_HERE, "config.secrets.json")
            with open(secrets_path, "r", encoding="utf-8") as f:
                secrets = json.load(f)
            token = str((secrets or {}).get("bot_token") or "").strip()
        except Exception:
            token = ""
    if not token:
        print("Missing token. Set RSADMINBOT_TOKEN (or DISCORD_BOT_TOKEN) or add bot_token to RSAdminBot/config.secrets.json.")
        return 2

    if len(sys.argv) < 2:
        print("Provide one or more discord message links as args.")
        return 2

    links = []
    for arg in sys.argv[1:]:
        links.extend(_extract_discord_message_links(arg))

    if not links:
        print("No valid discord message links found in args.")
        return 2

    intents = discord.Intents.none()
    intents.guilds = True
    client = discord.Client(intents=intents)

    # Create a small parser instance (no network side-effects).
    parser = ReviewRSServerListener(bot=client, cfg=ReviewRSConfig())  # type: ignore[arg-type]

    async with client:
        await client.login(token)

        for (gid, cid, mid) in links:
            msg = await _fetch_message(client, channel_id=cid, message_id=mid)
            if not msg:
                print(f"\n---\nFAILED to fetch {gid}/{cid}/{mid} (no access / not found)\n---")
                continue

            title, ids, image_url = parser._extract_catalog_fields(msg)  # reuse canonical parsing
            out_lines = parser._format_catalog_reply(title=title, ids=ids, image_url=image_url)

            print("\n---")
            print(f"source={gid}/{cid}/{mid}")
            for line in out_lines:
                print(line)
            print("---")

        await client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

