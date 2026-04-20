"""One-shot Discord scan to build `data/member_status_logs_events.json` locally.

This script logs in with the bot token from `RSCheckerbot/config.secrets.json`,
scans `#member-status-logs` history using the Discord API, and writes/updates the
canonical ledger via `member_status_logs_ingest.py`.

Why a script (vs running the bot + command)?
- Lets you generate the file locally without starting the full bot runtime.
- Uses the same canonical embed parsing code by importing `RSCheckerbot/main.py`.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from contextlib import suppress
from datetime import datetime, timedelta, timezone

import discord

# Ensure `RSCheckerbot/` is importable when running from `RSCheckerbot/scripts/`.
from pathlib import Path

_RS_CHECKERBOT_DIR = Path(__file__).resolve().parents[1]
if str(_RS_CHECKERBOT_DIR) not in sys.path:
    sys.path.insert(0, str(_RS_CHECKERBOT_DIR))

# Import canonical parsing + config constants (does NOT start the bot).
import main as rs_main  # type: ignore

import member_status_logs_ingest


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scan member-status-logs and write member_status_logs_events.json")
    p.add_argument("--channel-id", type=int, default=0, help="Override member-status-logs channel id")
    p.add_argument("--days", type=int, default=14, help="How many days back to scan (default 14, max 365)")
    p.add_argument("--limit", type=int, default=3000, help="Max messages to scan (default 3000, max 50000)")
    p.add_argument("--out", type=str, default="", help="Override output path (defaults to RSCheckerbot/data/member_status_logs_events.json)")
    return p.parse_args()


async def _run() -> int:
    args = _parse_args()
    days = int(max(1, min(int(args.days or 14), 365)))
    limit = int(max(50, min(int(args.limit or 3000), 50000)))

    token = str(getattr(rs_main, "TOKEN", "") or "").strip()
    if not token:
        raise RuntimeError("Missing bot_token (check RSCheckerbot/config.secrets.json).")

    configured_ch = int(args.channel_id or getattr(rs_main, "MEMBER_STATUS_LOGS_CHANNEL_ID", 0) or 0)
    if configured_ch <= 0:
        raise RuntimeError("Missing member_status_logs_channel_id (set dm_sequence.member_status_logs_channel_id).")

    out_path = getattr(rs_main, "MEMBER_STATUS_LOGS_EVENTS_FILE", None)
    if args.out:
        from pathlib import Path

        out_path = Path(str(args.out))
    if not out_path:
        raise RuntimeError("Could not resolve output path for member_status_logs_events.json")

    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))

    intents = discord.Intents.none()
    intents.guilds = True
    intents.messages = True
    intents.message_content = False
    client = discord.Client(intents=intents)

    stats = {"scanned": 0, "embeds": 0, "wrote": 0, "skipped_no_did": 0, "errors": 0}

    @client.event
    async def on_ready():
        print("=== member-status-logs ledger scan ===")
        print(f"channel_id: {configured_ch}")
        print(f"days: {days} (cutoff={cutoff.isoformat().replace('+00:00','Z')})")
        print(f"limit: {limit}")
        print(f"out: {str(out_path)}")
        ch = client.get_channel(configured_ch)
        if ch is None:
            with suppress(Exception):
                ch = await client.fetch_channel(configured_ch)
        if not isinstance(ch, discord.TextChannel):
            print("ERROR: channel not found or not a text channel.")
            with suppress(Exception):
                await client.close()
            return

        try:
            async for msg in ch.history(limit=int(limit), oldest_first=False):
                stats["scanned"] += 1
                try:
                    created_at = getattr(msg, "created_at", None)
                    if created_at and created_at.replace(tzinfo=timezone.utc) < cutoff:
                        break
                except Exception:
                    pass

                if not getattr(msg, "embeds", None):
                    continue
                e0 = msg.embeds[0]
                if not isinstance(e0, discord.Embed):
                    continue
                stats["embeds"] += 1

                try:
                    ts_i, kind, did, whop_brief = rs_main._extract_reporting_from_member_status_embed(
                        e0,
                        fallback_ts=int((getattr(msg, "created_at", None) or datetime.now(timezone.utc)).timestamp()),
                    )
                    if not did:
                        stats["skipped_no_did"] += 1
                        continue
                    await member_status_logs_ingest.upsert_member_status_logs_message(
                        events_path=out_path,
                        configured_channel_id=int(configured_ch),
                        message=msg,
                        kind=str(kind or ""),
                        discord_id=int(did or 0),
                        whop_brief=whop_brief if isinstance(whop_brief, dict) else None,
                        source_name="member-status-logs",
                    )
                    stats["wrote"] += 1
                except Exception:
                    stats["errors"] += 1

                if stats["scanned"] % 250 == 0:
                    print(
                        f"progress: scanned={stats['scanned']} embeds={stats['embeds']} "
                        f"wrote={stats['wrote']} skipped_no_did={stats['skipped_no_did']} errors={stats['errors']}"
                    )
        finally:
            print("done:")
            print(
                f"scanned={stats['scanned']} embeds={stats['embeds']} wrote={stats['wrote']} "
                f"skipped_no_did={stats['skipped_no_did']} errors={stats['errors']}"
            )
            with suppress(Exception):
                await client.close()

    async with client:
        await client.start(token)
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())

