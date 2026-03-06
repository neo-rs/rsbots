#!/usr/bin/env python3
"""
Scan Discord API: list slash commands registered per bot (global + guild).

Use this to see which bot has /discum, /ping, /send, etc. and to debug
when a slash command "keeps disappearing" (e.g. token conflict: if DiscumBot
uses DataManagers' token, both are the same app and the last process to
tree.sync(guild=...) overwrites what shows in the server).

Usage:
  # Scan one bot (token from env)
  set BOT_TOKEN=your_bot_token
  set MIRRORWORLD_SERVER=1431314516364230689
  python scripts/scan_discord_registered_commands.py

  # Scan multiple bots by name (loads tokens from each bot's config)
  python scripts/scan_discord_registered_commands.py --bots discumbot datamanagerbot pingbot

  # Single bot with token file
  python scripts/scan_discord_registered_commands.py --config MWDiscumBot/config/tokens.env --guild 1431314516364230689

  # List only (no guild); use --global for global commands
  python scripts/scan_discord_registered_commands.py --bots discumbot --global

Discord API does not provide "when registered" or "last updated" for commands;
this script only shows current state (what is live now).

Why /discum might "keep disappearing":
  If DiscumBot uses the SAME token as DataManagers (e.g. oracle_set_discum_bot_token.py
  was run), both are the same Discord application. Whichever process last ran
  tree.sync(guild=...) sets the guild's slash commands for that app. So if
  DataManagers (or another script) syncs after DiscumBot, only that process's
  commands appear — /discum can be overwritten. Fix: give DiscumBot its own
  bot token so /discum is registered under a separate application.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(1)

# Project root (script lives in scripts/)
ROOT = Path(__file__).resolve().parents[1]

# ANSI colors (no deps; enable when stdout is a TTY or FORCE_COLOR=1)
def _ansi(s: str, code: str) -> str:
    if not getattr(sys.stdout, "isatty", lambda: False)() and not os.environ.get("FORCE_COLOR"):
        return s
    return f"\033[{code}m{s}\033[0m"

class Colors:
    BOLD = "1"
    DIM = "2"
    RED = "31"
    GREEN = "32"
    YELLOW = "33"
    CYAN = "36"
    MAGENTA = "35"
    RESET = "0"

def _c(s: str, code: str) -> str:
    return _ansi(s, code)

def _bold(s: str) -> str:
    return _c(s, Colors.BOLD)

def _dim(s: str) -> str:
    return _c(s, Colors.DIM)

def _green(s: str) -> str:
    return _c(s, Colors.GREEN)

def _yellow(s: str) -> str:
    return _c(s, Colors.YELLOW)

def _red(s: str) -> str:
    return _c(s, Colors.RED)

def _cyan(s: str) -> str:
    return _c(s, Colors.CYAN)

def _magenta(s: str) -> str:
    return _c(s, Colors.MAGENTA)

# Default guild = Mirror World server
DEFAULT_GUILD_ID = os.environ.get("MIRRORWORLD_SERVER", "1431314516364230689").strip()

# Primary: MWBots (gitignored locally; present on server)
BOT_CONFIGS = {
    "discumbot": ROOT / "MWBots" / "MWDiscumBot" / "config" / "tokens.env",
    "datamanagerbot": ROOT / "MWBots" / "MWDataManagerBot" / "config" / "tokens.env",
    "pingbot": ROOT / "MWBots" / "MWPingBot" / "config" / "tokens.env",
}

# Fallback: Oraclserver-files-mwbots (downloaded server snapshot; folder names match server)
ORACLE_SNAPSHOTS_BASE = ROOT / "Oraclserver-files-mwbots"
SNAPSHOT_BOT_FOLDERS = {
    "discumbot": "MWDiscumBot",
    "datamanagerbot": "MWDataManagerBot",
    "pingbot": "MWPingBot",
}


def _latest_snapshot_dir() -> Path | None:
    """Return the latest server_full_snapshot_* directory under Oraclserver-files-mwbots, or None."""
    if not ORACLE_SNAPSHOTS_BASE.exists():
        return None
    try:
        subs = [p for p in ORACLE_SNAPSHOTS_BASE.iterdir() if p.is_dir() and p.name.startswith("server_full_snapshot_")]
        if not subs:
            return None
        subs.sort(key=lambda p: p.name, reverse=True)
        return subs[0]
    except Exception:
        return None


def _snapshot_tokens_path(bot_key: str) -> Path | None:
    """Return tokens.env path in latest snapshot for this bot, or None."""
    snapshot = _latest_snapshot_dir()
    if not snapshot:
        return None
    folder = SNAPSHOT_BOT_FOLDERS.get(bot_key)
    if not folder:
        return None
    path = snapshot / folder / "config" / "tokens.env"
    return path if path.exists() else None


def load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip().strip('"').strip("'")
                if key:
                    out[key] = val
    except FileNotFoundError:
        pass
    return out


def _read_token_from_env(env: dict[str, str], bot_key: str) -> str:
    if bot_key == "discumbot":
        return (env.get("BOT_TOKEN") or env.get("DISCORD_BOT_DISCUMBOT") or env.get("DISCORD_BOT_TOKEN") or "").strip()
    if bot_key == "datamanagerbot":
        return (env.get("DATAMANAGER_BOT") or env.get("DISCORD_BOT_DATAMANAGER") or env.get("BOT_TOKEN") or "").strip()
    if bot_key == "pingbot":
        return (env.get("PING_BOT") or env.get("BOT_TOKEN") or "").strip()
    return (env.get("BOT_TOKEN") or env.get("DISCORD_BOT_TOKEN") or "").strip()


def get_token_for_bot(bot_key: str) -> tuple[str, str]:
    """Return (token, skip_reason). Tries MWBots/config first, then Oraclserver-files-mwbots snapshot."""
    path = BOT_CONFIGS.get(bot_key)
    if not path:
        return "", f"No config path for {bot_key}"
    # 1) Try primary (MWBots/.../config/tokens.env)
    if path.exists():
        env = load_env(path)
        token = _read_token_from_env(env, bot_key)
        if token:
            return token, ""
        keys = {"discumbot": "BOT_TOKEN", "datamanagerbot": "DATAMANAGER_BOT", "pingbot": "PING_BOT"}.get(bot_key, "BOT_TOKEN")
        # 2) Primary exists but empty token -> try snapshot
        snap_path = _snapshot_tokens_path(bot_key)
        if snap_path:
            snap_env = load_env(snap_path)
            snap_token = _read_token_from_env(snap_env, bot_key)
            if snap_token:
                return snap_token, ""
        return "", f"No {keys} in {path}"
    # 3) Primary missing -> try snapshot (downloaded Oracle server files)
    snap_path = _snapshot_tokens_path(bot_key)
    if snap_path:
        env = load_env(snap_path)
        token = _read_token_from_env(env, bot_key)
        if token:
            return token, ""
        keys = {"discumbot": "BOT_TOKEN", "datamanagerbot": "DATAMANAGER_BOT", "pingbot": "PING_BOT"}.get(bot_key, "BOT_TOKEN")
        return "", f"No {keys} in snapshot {snap_path}"
    return "", f"File not found: {path} (and no Oraclserver-files-mwbots snapshot with tokens)"


def get_app_info(token: str) -> tuple[str | None, str | None]:
    """Return (application_id, app_name). Uses /applications/@me then fallback to /users/@me."""
    headers = {"Authorization": f"Bot {token}"}
    # Try Get Current Application first (returns app id + name)
    r = requests.get("https://discord.com/api/v10/applications/@me", headers=headers, timeout=10)
    if r.status_code == 200:
        data = r.json()
        return str(data.get("id", "")), (data.get("name") or "Unknown")
    # Fallback: bot user (id often same as app id for simple bots)
    r = requests.get("https://discord.com/api/v10/users/@me", headers=headers, timeout=10)
    if r.status_code == 200:
        data = r.json()
        uid = data.get("id")
        name = data.get("username") or "Unknown"
        return str(uid) if uid else None, name
    return None, None


def get_global_commands(token: str, app_id: str) -> list[dict]:
    headers = {"Authorization": f"Bot {token}"}
    r = requests.get(
        f"https://discord.com/api/v10/applications/{app_id}/commands",
        headers=headers,
        timeout=10,
    )
    if r.status_code == 200:
        return r.json() if isinstance(r.json(), list) else []
    return []


def get_guild_commands(token: str, app_id: str, guild_id: str) -> list[dict]:
    if not guild_id:
        return []
    headers = {"Authorization": f"Bot {token}"}
    r = requests.get(
        f"https://discord.com/api/v10/applications/{app_id}/guilds/{guild_id}/commands",
        headers=headers,
        timeout=10,
    )
    if r.status_code == 200:
        return r.json() if isinstance(r.json(), list) else []
    return []


def format_command(c: dict, *, use_color: bool = True) -> str:
    name = c.get("name", "?")
    desc = (c.get("description") or "—")[:55]
    if use_color:
        return f"    {_cyan('/' + name)}{_dim(' — ' + desc)}"
    return f"  /{name}  — {desc}"


def scan_one(
    label: str,
    token: str,
    guild_id: str,
    *,
    global_only: bool = False,
) -> tuple[bool, str | None, str, list[dict], list[dict]]:
    """Returns (ok, app_id, app_name, global_cmds, guild_cmds). app_id is None if no token or failed."""
    if not token:
        return False, None, "Unknown", [], []
    app_id, app_name = get_app_info(token)
    if not app_id:
        return False, None, "Unknown", [], []
    global_cmds = get_global_commands(token, app_id)
    guild_cmds = get_guild_commands(token, app_id, guild_id) if not global_only and guild_id else []
    return True, app_id, app_name or "Unknown", global_cmds, guild_cmds


def print_scan_results(
    results: list[tuple[str, bool, str | None, str, list[dict], list[dict], str]],
    guild_id: str,
    *,
    global_only: bool = False,
) -> None:
    """Pretty-print all bot results in a centralization-scanner style view. Last element of each tuple is skip_reason."""
    bar = "=" * 70
    print()
    print(_cyan(bar))
    print(_bold("  DISCORD SLASH COMMANDS — REGISTERED PER BOT (LIVE STATE)"))
    print(_cyan(bar))
    print(_dim("  Guild ID: " + (guild_id or "(global only)")))
    print()

    seen_app_ids: set[str | None] = set()
    for row in results:
        if len(row) == 7:
            label, ok, app_id, app_name, global_cmds, guild_cmds, skip_reason = row
        else:
            label, ok, app_id, app_name, global_cmds, guild_cmds = row
            skip_reason = "No token or config missing"
        print(_cyan("  " + "-" * 66))
        print(_bold(f"  BOT: {label.upper()}"))
        if not ok:
            print(_yellow("    Skipped: " + (skip_reason or "no token")))
            print()
            continue
        if not app_id:
            print(_red("    Failed to get application (invalid token or API error)"))
            print()
            continue
        print(_dim(f"    Application: {app_name}   (id: {app_id})"))
        if app_id in seen_app_ids:
            print(_yellow("    ⚠ Same application ID as another bot above (shared token)"))
        seen_app_ids.add(app_id)
        print()

        # Global
        print(_bold("    GLOBAL COMMANDS:") + " " + _dim(f"({len(global_cmds)} total)"))
        if global_cmds:
            for c in global_cmds:
                print(format_command(c, use_color=True))
        else:
            print(_dim("      (none)"))
        print()

        if not global_only and guild_id:
            print(_bold("    GUILD COMMANDS") + _dim(f" (guild_id={guild_id}): ") + _dim(f"({len(guild_cmds)} total)"))
            if guild_cmds:
                for c in guild_cmds:
                    print(format_command(c, use_color=True))
            else:
                print(_dim("      (none)"))
            if guild_cmds and not global_cmds:
                print(_dim("    → Slash visible in server come from guild-only sync."))
            elif global_cmds and guild_cmds:
                print(_dim("    → Guild overrides/extends global; both apply."))
            print()
    print(_cyan(bar))
    print(_bold("  SUMMARY"))
    print(_cyan(bar))
    total = sum(1 for r in results if r[1] and r[2])
    with_cmds = sum(1 for r in results if r[1] and (len(r[4]) + len(r[5])) > 0)
    print(_dim(f"  Bots scanned: {len(results)}  |  With token: {total}  |  With commands: {with_cmds}"))
    if total == 0:
        print()
        print(_yellow("  No tokens found. Script checks (in order):"))
        for key, p in BOT_CONFIGS.items():
            print(_dim(f"    1) {key}: {p}"))
        snap = _latest_snapshot_dir()
        if snap:
            print(_dim(f"    2) Snapshot fallback: {snap}/<MWDiscumBot|MWDataManagerBot|MWPingBot>/config/tokens.env"))
        else:
            print(_dim("    2) Snapshot fallback: Oraclserver-files-mwbots/server_full_snapshot_*/... (no snapshot found)"))
        print(_yellow("  Add tokens to MWBots/.../config/tokens.env (gitignored) or use a downloaded Oraclserver-files-mwbots snapshot."))
        print(_yellow("  Or run with: --config path/to/tokens.env  or  --token YOUR_BOT_TOKEN"))
    else:
        if len(seen_app_ids) < total and total > 1:
            print(_yellow("  If two bots show the SAME Application id, they share a token."))
            print(_yellow("  Only the last process that ran tree.sync(guild=...) sets what appears in the server."))
        # Check: Data Manager app (discumbot/datamanagerbot) should have /discum; if missing, say so
        for r in results:
            label, ok, app_id, app_name, global_cmds, guild_cmds = r[0], r[1], r[2], r[3], r[4], r[5]
            if not ok or not app_id or app_name != "Data Manager":
                continue
            names = {c.get("name", "") for c in (guild_cmds or [])}
            if "discum" not in names:
                print()
                print(_red("  /discum is NOT registered for the Data Manager app (discumbot/datamanagerbot)."))
                print(_yellow("  Fix: Deploy MWDataManagerBot/live_forwarder.py that calls register_discum_commands_to_bot(bot),"))
                print(_yellow("  then restart the DataManagerBot service on the server so one sync pushes /discum + DataManager commands."))
                break
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Scan Discord API for registered slash commands per bot (live state)."
    )
    ap.add_argument(
        "--bots",
        nargs="+",
        choices=list(BOT_CONFIGS),
        help="Bot keys to scan (default: all three if no other option given).",
    )
    ap.add_argument(
        "--config",
        type=Path,
        help="Single tokens.env path (overrides --bots).",
    )
    ap.add_argument(
        "--token",
        default=os.environ.get("BOT_TOKEN", "").strip(),
        help="Single bot token (env BOT_TOKEN or this).",
    )
    ap.add_argument(
        "--guild",
        default=DEFAULT_GUILD_ID,
        help="Guild ID for guild commands (default: MIRRORWORLD_SERVER or Mirror World id).",
    )
    ap.add_argument(
        "--global",
        dest="global_only",
        action="store_true",
        help="Only fetch global commands (no guild).",
    )
    args = ap.parse_args()

    guild_id = (args.guild or "").strip() if not args.global_only else ""

    # Default: scan all bots when no option given (no --config, no --token, no --bots)
    bots_to_scan = args.bots
    if not args.config and not args.token and not bots_to_scan:
        bots_to_scan = list(BOT_CONFIGS)

    if args.config and args.config.exists():
        env = load_env(args.config)
        token = (
            env.get("BOT_TOKEN")
            or env.get("DISCORD_BOT_TOKEN")
            or env.get("PING_BOT")
            or env.get("DATAMANAGER_BOT")
            or ""
        ).strip()
        if not token:
            print(_red("[ERROR] No BOT_TOKEN (or PING_BOT/DATAMANAGER_BOT) in ") + str(args.config))
            return 1
        ok, app_id, app_name, global_cmds, guild_cmds = scan_one(
            "Config: " + str(args.config), token, guild_id, global_only=args.global_only
        )
        results = [("config", ok, app_id, app_name, global_cmds, guild_cmds, "" if ok else "Invalid token or API error")]
        print_scan_results(results, guild_id, global_only=args.global_only)
        return 0

    if bots_to_scan:
        results_list: list[tuple[str, bool, str | None, str, list[dict], list[dict], str]] = []
        for bot_key in bots_to_scan:
            token, skip_reason = get_token_for_bot(bot_key)
            ok, app_id, app_name, global_cmds, guild_cmds = scan_one(
                bot_key, token, guild_id, global_only=args.global_only
            )
            results_list.append((bot_key, ok, app_id, app_name, global_cmds, guild_cmds, skip_reason if not ok else ""))
        print_scan_results(results_list, guild_id, global_only=args.global_only)
        return 0

    if args.token:
        ok, app_id, app_name, global_cmds, guild_cmds = scan_one(
            "BOT_TOKEN", args.token, guild_id, global_only=args.global_only
        )
        results = [("BOT_TOKEN", ok, app_id, app_name, global_cmds, guild_cmds, "" if ok else "Invalid token or API error")]
        print_scan_results(results, guild_id, global_only=args.global_only)
        return 0

    print(_yellow("Provide --bots, --config, or --token (or set BOT_TOKEN)."))
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
