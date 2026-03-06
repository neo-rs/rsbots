#!/usr/bin/env python3
"""
One-off: Register /discum on the Data Manager app and sync to the Mirror World guild.
Uses the same token as DataManagerBot so the next time DataManagerBot runs it will
see /discum in its tree (or run this after deploying the live_forwarder fix).

Usage (from repo root):
  python scripts/sync_discum_command_once.py

Reads token from: MWBots/MWDataManagerBot/config/tokens.env or Oraclserver-files-mwbots snapshot.
Guild: MIRRORWORLD_SERVER env or 1431314516364230689.
"""

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Add MWDiscumBot so we can load discum_command_bot
for _dir in [ROOT / "MWBots" / "MWDiscumBot", ROOT / "MWDiscumBot"]:
    if _dir.is_dir() and str(_dir) not in sys.path:
        sys.path.insert(0, str(_dir))
        break

GUILD_ID = int(os.environ.get("MIRRORWORLD_SERVER", "1431314516364230689").strip() or "1431314516364230689")


def load_env(path: Path) -> dict:
    out = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or "=" not in line or line.startswith("#"):
                    continue
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        pass
    return out


def get_token() -> str:
    # 1) MWDataManagerBot
    for base in [ROOT / "MWBots" / "MWDataManagerBot", ROOT / "MWDataManagerBot"]:
        p = base / "config" / "tokens.env"
        if p.exists():
            env = load_env(p)
            t = env.get("DATAMANAGER_BOT") or env.get("DISCORD_BOT_DATAMANAGER") or env.get("BOT_TOKEN") or ""
            if t.strip():
                return t.strip()
    # 2) Snapshot
    snap_base = ROOT / "Oraclserver-files-mwbots"
    if snap_base.exists():
        for d in sorted(snap_base.iterdir(), key=lambda x: x.name, reverse=True):
            if d.is_dir() and d.name.startswith("server_full_snapshot_"):
                p = d / "MWDataManagerBot" / "config" / "tokens.env"
                if p.exists():
                    env = load_env(p)
                    t = env.get("DATAMANAGER_BOT") or env.get("BOT_TOKEN") or ""
                    if t.strip():
                        return t.strip()
                break
    return ""


async def main():
    token = get_token()
    if not token:
        print("ERROR: No DataManager token found. Put DATAMANAGER_BOT in MWDataManagerBot/config/tokens.env or snapshot.")
        return 1
    import discord
    from discord.ext import commands

    # Load discum_command_bot and register /discum
    try:
        import importlib.util
        for _dir in [ROOT / "MWBots" / "MWDiscumBot", ROOT / "MWDiscumBot"]:
            _py = _dir / "discum_command_bot.py"
            if _py.exists():
                if str(_dir) not in sys.path:
                    sys.path.insert(0, str(_dir))
                spec = importlib.util.spec_from_file_location("discum_command_bot", _py)
                mod = importlib.util.module_from_spec(spec)
                sys.modules["discum_command_bot"] = mod
                spec.loader.exec_module(mod)
                break
        else:
            print("ERROR: discum_command_bot.py not found under MWBots/MWDiscumBot or MWDiscumBot")
            return 1
    except Exception as e:
        print(f"ERROR loading discum_command_bot: {e}")
        import traceback
        traceback.print_exc()
        return 1

    intents = discord.Intents.default()
    intents.guilds = True
    bot = commands.Bot(command_prefix="!", intents=intents)
    mod.register_discum_commands_to_bot(bot)

    @bot.event
    async def on_ready():
        try:
            guild_obj = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild_obj)
            synced = await bot.tree.sync(guild=guild_obj)
            names = sorted(c.name for c in (synced or []))
            print(f"Synced {len(synced)} command(s) to guild {GUILD_ID}: {names}")
            if "discum" in names:
                print("OK: /discum is now registered. Run scripts/scan_discord_registered_commands.py to verify.")
            else:
                print("WARN: /discum not in synced list. Check register_discum_commands_to_bot.")
        except Exception as e:
            print(f"Sync failed: {e}")
            import traceback
            traceback.print_exc()
        await bot.close()

    await bot.start(token)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
