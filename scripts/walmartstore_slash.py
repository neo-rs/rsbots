#!/usr/bin/env python3
"""
Trigger the Reselling Secrets Monitors bot's /walmartstore slash command from Python.
Uses the same Discord user token as DailyScheduleReminder (config.secrets.json or DISCORD_USER_TOKEN).
The bot posts stock info (store, price, sales floor, back room, distance, address) in the channel.

Usage:
  python scripts/walmartstore_slash.py --guild GUILD_ID --channel CHANNEL_ID --upc 050946872926 --zip 35058
  python scripts/walmartstore_slash.py --guild GUILD_ID --channel CHANNEL_ID --upc 050946872926 --zip 35058 --wait

Requires: pip install discum (or use repo's Discumraw). Guild and channel are the server/channel
where the Reselling Secrets bot is present and you want the reply (e.g. your deal server + stock channel).

To get guild ID and channel ID: enable Developer Mode (Discord Settings > App Settings > Advanced),
then right-click the server title → Copy Server ID, and right-click the channel → Copy Channel ID.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Reselling Secrets Monitors APP (bot that has /walmartstore)
WALMARTSTORE_BOT_ID = "1255835799552004138"


def load_token() -> str:
    """Same token as DailyScheduleReminder: env or config.secrets.json."""
    import os
    token = os.environ.get("DISCORD_USER_TOKEN")
    if token:
        return token.strip()
    secrets = REPO_ROOT / "DailyScheduleReminder" / "config.secrets.json"
    if secrets.exists():
        with open(secrets, "r", encoding="utf-8") as f:
            data = json.load(f)
        t = data.get("token") or data.get("user_token")
        if t:
            return t.strip()
    raise RuntimeError(
        "Set DISCORD_USER_TOKEN or create DailyScheduleReminder/config.secrets.json with 'token'. "
        "Use the same Discord user token as DailyScheduleReminder."
    )


def run_slash(guild_id: str, channel_id: str, upc_sku: str, zip_code: str, wait_for_reply: bool) -> None:
    try:
        import discum
        from discum.utils.slash import SlashCommander
    except ImportError as e:
        print("Install discum (slash support is in development branch):", file=sys.stderr)
        print("  pip install git+https://github.com/Merubokkusu/Discord-S.C.U.M.git#egg=discum", file=sys.stderr)
        raise SystemExit(1) from e

    token = load_token()
    bot = discum.Client(token=token, log={"console": False, "file": False})

    result = {"triggered": False, "reply_content": None, "reply_embed": None}

    def on_slash_response(resp, gid, cid, upc, z):
        if not getattr(resp.event, "guild_application_commands_updated", None):
            return
        try:
            bot.gateway.removeCommand(on_slash_response)
            parsed = resp.parsed.auto()
            app_cmds = parsed.get("application_commands") or []
            if not app_cmds:
                print("No slash commands found for walmartstore in this guild. Is the bot in the server?", file=sys.stderr)
                bot.gateway.close()
                return
            s = SlashCommander(app_cmds, application_id=WALMARTSTORE_BOT_ID)
            data = s.get(["walmartstore"], inputs={"upc-sku": upc, "zip": z})
            if data is None:
                data = s.get(["walmartstore"], inputs={"upc_sku": upc, "zip": z})
            if data is None:
                print("Could not build slash payload. Command or option names may differ.", file=sys.stderr)
                bot.gateway.close()
                return
            bot.triggerSlashCommand(
                WALMARTSTORE_BOT_ID,
                channelID=cid,
                guildID=gid,
                data=data,
            )
            result["triggered"] = True
            if not wait_for_reply:
                bot.gateway.close()
        except Exception as e:
            print(f"Slash trigger error: {e}", file=sys.stderr)
            bot.gateway.close()

    if wait_for_reply:
        def on_message(resp, cid, bid):
            if not resp.event.message:
                return
            m = resp.parsed.auto()
            if str(m.get("channel_id")) != cid:
                return
            author = (m.get("author") or {}).get("id") or ""
            if str(author) != bid:
                return
            result["reply_content"] = m.get("content") or ""
            result["reply_embed"] = m.get("embeds")
            bot.gateway.removeCommand(on_message)
            bot.gateway.close()

        bot.gateway.command({
            "function": on_message,
            "params": {"cid": channel_id, "bid": WALMARTSTORE_BOT_ID},
        })

    bot.gateway.command({
        "function": on_slash_response,
        "params": {"gid": guild_id, "cid": channel_id, "upc": upc_sku, "z": zip_code},
    })
    bot.gateway.request.searchSlashCommands(guild_id, limit=20, query="walmartstore")
    bot.gateway.run(auto_reconnect=False)

    if result.get("reply_content"):
        print(result["reply_content"])
    if result.get("reply_embed"):
        for emb in result["reply_embed"]:
            if emb.get("description"):
                print(emb["description"])
            if emb.get("title"):
                print("Title:", emb["title"])
    if not result["triggered"] and not result.get("reply_content"):
        print("Slash command was not triggered or no reply captured. Check guild/channel and that the bot is in the server.", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Trigger /walmartstore slash command via Discord user account (discum).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--guild", required=True, help="Guild (server) ID where the Reselling Secrets bot is")
    ap.add_argument("--channel", required=True, help="Channel ID where you want the stock reply posted")
    ap.add_argument("--upc", required=True, dest="upc_sku", help="UPC or SKU (e.g. 050946872926)")
    ap.add_argument("--zip", required=True, dest="zip_code", help="ZIP code (e.g. 35058)")
    ap.add_argument("--wait", action="store_true", help="Wait for bot reply and print it (then exit)")
    args = ap.parse_args()

    run_slash(
        guild_id=args.guild.strip(),
        channel_id=args.channel.strip(),
        upc_sku=args.upc_sku.strip(),
        zip_code=args.zip_code.strip(),
        wait_for_reply=args.wait,
    )


if __name__ == "__main__":
    main()
