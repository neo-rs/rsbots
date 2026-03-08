"""Verify channel IDs from channel_map.json (or CLI) via Discord API.

Use this to see why some channels show as "# unknown" or "No Access" in Channel Mappings:
we send "<#channel_id>"; Discord resolves it. If the channel is deleted or the viewer has
no access, Discord shows "# unknown" or "No Access".

Usage:
  py -3 scripts/verify_discum_channel_ids.py
      -> checks all source channel IDs from MWDiscumBot/config/channel_map.json
  py -3 scripts/verify_discum_channel_ids.py 1159141256778223638 1249725020083589130
      -> checks only the given IDs
  py -3 scripts/verify_discum_channel_ids.py --remove-failed
      -> check all IDs, then remove 404 (deleted) and 403 (no access) from channel_map.json.

At the end, writes channel_map_info.json (channel name + server/guild) for OK channels.
channel_map.json stays as-is (id -> webhook). The bot uses channel_map_info for display.

Uses only the discumbot user token (source channels are in servers the user is in, not the bot):
  - DISCUM_USER_DISCUMBOT (or DISCUM_BOT) in env or MWDiscumBot/config/tokens.env
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# Repo root = parent of scripts/
ROOT = Path(__file__).resolve().parent.parent
MWDISCUM_CONFIG = ROOT / "MWBots" / "MWDiscumBot" / "config"
TOKENS_ENV = MWDISCUM_CONFIG / "tokens.env"
CHANNEL_MAP_JSON = MWDISCUM_CONFIG / "channel_map.json"
CHANNEL_MAP_INFO_JSON = MWDISCUM_CONFIG / "channel_map_info.json"


def load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip().lstrip("\ufeff")
            value = value.strip().strip('"').strip("'")
            if key:
                out[key] = value
    return out


def get_user_token() -> str:
    """Return discumbot user token from env then tokens.env."""
    env = {**os.environ, **load_env(TOKENS_ENV)}
    return (
        env.get("DISCUM_USER_DISCUMBOT")
        or env.get("DISCUM_BOT")
        or env.get("DISCORD_TOKEN")
        or ""
    ).strip()


def fetch_channel(channel_id: int, *, user_token: str) -> tuple[int, dict | None, str]:
    """GET /channels/{id} with user token. Returns (http_status, json_body_or_None, summary)."""
    url = f"https://discord.com/api/v10/channels/{channel_id}"
    headers = {"Content-Type": "application/json", "Authorization": user_token}
    if not user_token:
        return 0, None, "No token (set DISCUM_USER_DISCUMBOT in env / tokens.env)"
    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code == 200:
        return 200, r.json(), "OK"
    if r.status_code == 404:
        return 404, None, "404 Not Found (channel deleted or invalid)"
    if r.status_code == 403:
        return 403, None, "403 Forbidden (no access)"
    if r.status_code == 401:
        return 401, None, "401 Unauthorized (bad token)"
    return r.status_code, None, f"HTTP {r.status_code}"


def fetch_guild_name(guild_id: int, user_token: str) -> str:
    """GET /guilds/{id} with user token. Returns guild name or empty string."""
    url = f"https://discord.com/api/v10/guilds/{guild_id}"
    headers = {"Content-Type": "application/json", "Authorization": user_token}
    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code == 200:
        data = r.json()
        return (data.get("name") or "").strip() or f"Guild-{guild_id}"
    return f"Guild-{guild_id}"


def main() -> None:
    argv = [a for a in sys.argv[1:] if a != "--remove-failed"]
    remove_failed = "--remove-failed" in sys.argv

    user_token = get_user_token()
    if not user_token:
        env = load_env(TOKENS_ENV)
        keys = list(env.keys()) if env else []
        print("No token found. Set DISCUM_USER_DISCUMBOT in env or in:")
        print(f"  {TOKENS_ENV}")
        if TOKENS_ENV.exists():
            print(f"  (file exists; keys in file: {keys or 'none'})")
        else:
            print("  (file not found)")
        sys.exit(1)

    if not CHANNEL_MAP_JSON.exists():
        print(f"Missing {CHANNEL_MAP_JSON}")
        sys.exit(1)
    with open(CHANNEL_MAP_JSON, "r", encoding="utf-8") as f:
        channel_map_raw = json.load(f)
    if not isinstance(channel_map_raw, dict):
        channel_map_raw = {}

    if argv:
        ids = []
        for a in argv:
            try:
                ids.append(int(a))
            except ValueError:
                print(f"Invalid ID: {a}")
        if not ids:
            sys.exit(1)
    else:
        ids = [int(k) for k in channel_map_raw if str(k).strip().isdigit()]
        print(f"Loaded {len(ids)} channel IDs from channel_map.json\n")

    print("Channel ID              | Status        | Name / Guild")
    print("-" * 70)
    failed_status: dict[int, int] = {}  # cid -> status (non-200)
    success_info: list[tuple[int, dict]] = []  # (cid, channel body) for 200 responses

    for i, cid in enumerate(ids):
        if i > 0:
            time.sleep(0.6)  # avoid rate limit
        status, body, summary = fetch_channel(cid, user_token=user_token)
        if status == 200 and body:
            name = body.get("name") or "(no name)"
            gid = body.get("guild_id") or body.get("id")
            guild = f"guild_id={gid}" if gid else ""
            print(f"{cid} | {summary:12} | #{name} {guild}")
            success_info.append((cid, body))
        else:
            print(f"{cid} | {summary}")
            if status != 200:
                failed_status[cid] = status

    # --remove-failed: remove 404 (deleted) and 403 (no access with this token).
    to_remove = [cid for cid, st in failed_status.items() if st in (404, 403)] if remove_failed else []

    print("-" * 70)
    if remove_failed and to_remove:
        for cid in to_remove:
            channel_map_raw.pop(str(cid), None)
        with open(CHANNEL_MAP_JSON, "w", encoding="utf-8") as f:
            json.dump(channel_map_raw, f, indent=2, ensure_ascii=False)
        print(f"Removed {len(to_remove)} entries (404 + 403) from channel_map.json: {to_remove}")
    elif remove_failed:
        print("No 404/403 entries to remove.")

    # Write channel_map_info.json (name + server) for OK channels so the bot can show them without breaking channel_map.json
    if success_info:
        unique_guild_ids = set()
        for _cid, body in success_info:
            gid = body.get("guild_id")
            if gid is not None:
                try:
                    unique_guild_ids.add(int(gid))
                except (TypeError, ValueError):
                    pass
        guild_names: dict[int, str] = {}
        for gi, gid in enumerate(sorted(unique_guild_ids)):
            if gi > 0:
                time.sleep(0.6)
            guild_names[gid] = fetch_guild_name(gid, user_token)
        channels_out: dict[str, dict] = {}
        for cid, body in success_info:
            name = (body.get("name") or "").strip() or "(no name)"
            gid = body.get("guild_id")
            try:
                gid_int = int(gid) if gid is not None else 0
            except (TypeError, ValueError):
                gid_int = 0
            guild_name = guild_names.get(gid_int, f"Guild-{gid_int}" if gid_int else "")
            channels_out[str(cid)] = {"name": name, "guild_id": gid_int, "guild_name": guild_name}
        info_data = {
            "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "channels": channels_out,
        }
        with open(CHANNEL_MAP_INFO_JSON, "w", encoding="utf-8") as f:
            json.dump(info_data, f, indent=2, ensure_ascii=False)
        print(f"Updated {CHANNEL_MAP_INFO_JSON} with {len(channels_out)} channel(s) (name + server).")

    print("If you see 404 -> channel was deleted. 403 -> no access (both removed with --remove-failed).")
    print("# unknown in Discord = client could not resolve <#id> (same causes).")


if __name__ == "__main__":
    main()
