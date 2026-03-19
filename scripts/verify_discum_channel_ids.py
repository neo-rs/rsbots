"""Verify channel IDs from channel_map.json (or CLI) via Discord API.

Use this to see why some channels show as "# unknown" or "No Access" in Channel Mappings:
we send "<#channel_id>"; Discord resolves it. If the channel is deleted or the viewer has
no access, Discord shows "# unknown" or "No Access".

IMPORTANT — User token / API usage:
  This script uses a user account token and makes one API call per channel (and per guild).
  To avoid rate limits and risk: run rarely (e.g. after adding new mappings), use specific
  IDs when possible instead of "all", and do not run in tight loops or automation.

Usage:
  py -3 scripts/verify_discum_channel_ids.py
      -> checks all source channel IDs from MWDiscumBot/config/channel_map.json
  py -3 scripts/verify_discum_channel_ids.py 1159141256778223638 1249725020083589130
      -> checks only the given IDs (preferred to limit API calls)
  py -3 scripts/verify_discum_channel_ids.py --remove-failed
      -> check all IDs, then remove 404 (deleted) and 403 (no access) from channel_map.json.
  py -3 scripts/verify_discum_channel_ids.py --debug
      -> print token format and Discord 401 response to debug bad token.

channel_map_info.json: cleared at start, then updated after each successful channel fetch
(name + guild_id + guild_name). Same format as before. channel_map.json stays as-is (id -> webhook).

Uses only the discumbot user token (source channels are in servers the user is in, not the bot):
  - DISCUM_USER_DISCUMBOT (or DISCUM_BOT) in env or MWDiscumBot/config/tokens.env
"""

from __future__ import annotations

import json
import os
import sys
import time
import codecs
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# Repo root = parent of scripts/
ROOT = Path(__file__).resolve().parent.parent
#
# Canonical path differs between local mirrors vs Oracle layout:
# - Local repo often has:   MWBots/MWDiscumBot/config
# - Oracle server tree has: MWDiscumBot/config
#
_CONFIG_CANDIDATES = [
    ROOT / "MWBots" / "MWDiscumBot" / "config",
    ROOT / "MWDiscumBot" / "config",
]
MWDISCUM_CONFIG = next((p for p in _CONFIG_CANDIDATES if p.is_dir()), _CONFIG_CANDIDATES[0])
TOKENS_ENV = MWDISCUM_CONFIG / "tokens.env"
CHANNEL_MAP_JSON = MWDISCUM_CONFIG / "channel_map.json"
CHANNEL_MAP_INFO_JSON = MWDISCUM_CONFIG / "channel_map_info.json"

# Rate limiting: delay between API calls (seconds). Be conservative to avoid flags.
DELAY_BETWEEN_CHANNELS = 5.0
DELAY_BETWEEN_GUILDS = 5.0


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


def _auth_headers(user_token: str, *, use_bearer: bool = False) -> dict[str, str]:
    """Build Authorization header. Discord accepts raw token or 'Bearer <token>' for user tokens."""
    if use_bearer:
        return {"Content-Type": "application/json", "Authorization": f"Bearer {user_token}"}
    return {"Content-Type": "application/json", "Authorization": user_token}


def validate_user_token(user_token: str, debug: bool = False) -> tuple[bool, str, bool]:
    """Check token with GET /users/@me. Tries Bearer then raw. Returns (ok, error_message, use_bearer)."""
    if not user_token:
        return False, "Token is empty.", False
    url = "https://discord.com/api/v10/users/@me"
    if debug:
        parts = user_token.split(".")
        print(f"[DEBUG] Token length={len(user_token)} parts={len(parts)} (expect 3 for JWT-like)")
        if user_token.lower().startswith("bot "):
            print("[DEBUG] Token starts with 'Bot ' - use user token, not BOT_TOKEN")
    last_401_msg = ""
    for try_bearer in (True, False):
        headers = _auth_headers(user_token, use_bearer=try_bearer)
        try:
            r = requests.get(url, headers=headers, timeout=10)
        except Exception as e:
            return False, f"Request failed: {e}", False
        if r.status_code == 200:
            return True, "", try_bearer
        if r.status_code == 401:
            try:
                body = r.json()
                last_401_msg = (body.get("message") or r.text or "")[:200]
            except Exception:
                last_401_msg = (r.text or "")[:200]
            if debug:
                print(f"[DEBUG] 401 with {'Bearer' if try_bearer else 'raw'}: {last_401_msg}")
    return False, (
        f"401 Unauthorized: the user token is invalid or expired.\n"
        f"  Discord said: {last_401_msg or '(no message)'}\n"
        "  - Use a user account token (not BOT_TOKEN). Set in DISCUM_USER_DISCUMBOT in tokens.env.\n"
        "  - If using OAuth2, token may need to be refreshed. Run with --debug to see details."
    ), False


def fetch_channel(channel_id: int, *, user_token: str, use_bearer: bool = False) -> tuple[int, dict | None, str]:
    """GET /channels/{id} with user token. Returns (http_status, json_body_or_None, summary)."""
    url = f"https://discord.com/api/v10/channels/{channel_id}"
    if not user_token:
        return 0, None, "No token (set DISCUM_USER_DISCUMBOT in env / tokens.env)"
    headers = _auth_headers(user_token, use_bearer=use_bearer)
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


def fetch_guild_name(guild_id: int, user_token: str, *, use_bearer: bool = False) -> str:
    """GET /guilds/{id} with user token. Returns guild name or empty string."""
    url = f"https://discord.com/api/v10/guilds/{guild_id}"
    headers = _auth_headers(user_token, use_bearer=use_bearer)
    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code == 200:
        data = r.json()
        return (data.get("name") or "").strip() or f"Guild-{guild_id}"
    return f"Guild-{guild_id}"


def main() -> None:
    # Ensure Windows console can print Discord unicode safely.
    # Without this, cp1252 can throw UnicodeEncodeError and crash the run.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        try:
            sys.stdout = codecs.getwriter("utf-8")(sys.stdout)  # type: ignore[assignment]
            sys.stderr = codecs.getwriter("utf-8")(sys.stderr)  # type: ignore[assignment]
        except Exception:
            pass

    argv = [a for a in sys.argv[1:] if a not in ("--remove-failed", "--debug")]
    remove_failed = "--remove-failed" in sys.argv
    debug = "--debug" in sys.argv

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

    ok, err, use_bearer = validate_user_token(user_token, debug=debug)
    if not ok:
        print("Token validation failed:", err, sep="\n")
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
        print(f"Loaded {len(ids)} channel IDs from channel_map.json (delay {DELAY_BETWEEN_CHANNELS}s between calls)\n")

    # Start fresh: clear channel_map_info.json so we write incrementally as we fetch
    def _write_channel_map_info(channels: dict) -> None:
        data = {
            "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "channels": channels,
        }
        with open(CHANNEL_MAP_INFO_JSON, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    _write_channel_map_info({})

    print("Channel ID              | Status        | Name / Guild")
    print("-" * 70)
    failed_status: dict[int, int] = {}  # cid -> status (non-200)
    guild_names: dict[int, str] = {}  # guild_id -> guild name (filled on first use)
    channels_out: dict[str, dict] = {}  # written to channel_map_info.json after each success

    for i, cid in enumerate(ids):
        if i > 0:
            time.sleep(DELAY_BETWEEN_CHANNELS)
        status, body, summary = fetch_channel(cid, user_token=user_token, use_bearer=use_bearer)
        if status == 200 and body:
            name = (body.get("name") or "(no name)").strip() or "(no name)"
            gid = body.get("guild_id") or body.get("id")
            try:
                gid_int = int(gid) if gid is not None else 0
            except (TypeError, ValueError):
                gid_int = 0
            if gid_int and gid_int not in guild_names:
                time.sleep(DELAY_BETWEEN_GUILDS)
                guild_names[gid_int] = fetch_guild_name(gid_int, user_token, use_bearer=use_bearer)
            guild_display = guild_names.get(gid_int, f"Guild-{gid_int}" if gid_int else "")
            print(f"{cid} | {summary:12} | #{name}  |  {guild_display}")
            guild_name = guild_names.get(gid_int, f"Guild-{gid_int}" if gid_int else "")
            channels_out[str(cid)] = {"name": name, "guild_id": gid_int, "guild_name": guild_name}
            _write_channel_map_info(channels_out)
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

    if channels_out:
        print(f"Updated {CHANNEL_MAP_INFO_JSON} with {len(channels_out)} channel(s) (name + server).")

    print("If you see 404 -> channel was deleted. 403 -> no access (both removed with --remove-failed).")
    print("# unknown in Discord = client could not resolve <#id> (same causes).")


if __name__ == "__main__":
    main()
