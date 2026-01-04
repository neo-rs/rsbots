#!/usr/bin/env python3
"""
Discord Command Sync Utility
============================
Syncs Discord slash commands for MirrorWorld bots.
This is a standalone utility that can sync or clear commands.

Usage:
    python sync_discord_commands.py          # Sync commands
    python sync_discord_commands.py --clear  # Clear all commands
    python sync_discord_commands.py --list   # List current commands
"""

import os
import sys
import asyncio
import argparse
from pathlib import Path

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Add project root to path
_project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "neonxt"))

# Try to use ResellingSecrets path system
try:
    _RESELLING_SECRETS = _project_root / "ResellingSecrets"
    if _RESELLING_SECRETS.exists():
        sys.path.insert(0, str(_RESELLING_SECRETS))
        from path_utils import PROJECT_ROOT, TOKENKEYS_ENV  # type: ignore[import-not-found]
        _project_root = PROJECT_ROOT
except ImportError:
    TOKENKEYS_ENV = _project_root / "config" / "tokenkeys.env"

# Load environment
try:
    from dotenv import load_dotenv
    if 'TOKENKEYS_ENV' in dir() and TOKENKEYS_ENV.exists():
        load_dotenv(TOKENKEYS_ENV, override=True)
    else:
        env_file = _project_root / "config" / "tokenkeys.env"
        if env_file.exists():
            load_dotenv(env_file, override=True)
except ImportError as e:

    print(f"[WARN] Caught {ImportError.__name__ if hasattr(ImportError, "__name__") else "ImportError"}: {e}")

import requests

# Colors
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'


def get_bot_token():
    """Get bot token from environment."""
    token = os.getenv("TESTCENTER_BOT_TOKEN", "").strip()
    if not token:
        token = os.getenv("DISCUM_BOT", "").strip()
    return token


def get_guild_id():
    """Get MirrorWorld guild ID from environment."""
    return os.getenv("MIRRORWORLD_SERVER", "").strip()


def get_application_id(token: str) -> str:
    """Get the bot's application ID."""
    headers = {"Authorization": f"Bot {token}"}
    response = requests.get(
        "https://discord.com/api/v10/users/@me",
        headers=headers,
        timeout=10
    )
    if response.status_code == 200:
        return response.json().get("id")
    return None


def list_commands(token: str, guild_id: str = None):
    """List all registered commands."""
    C = Colors
    app_id = get_application_id(token)
    if not app_id:
        print(f"{C.RED}[ERROR] Could not get application ID{C.RESET}")
        return []
    
    headers = {"Authorization": f"Bot {token}"}
    
    if guild_id:
        url = f"https://discord.com/api/v10/applications/{app_id}/guilds/{guild_id}/commands"
        print(f"{C.CYAN}Fetching guild commands for {guild_id}...{C.RESET}")
    else:
        url = f"https://discord.com/api/v10/applications/{app_id}/commands"
        print(f"{C.CYAN}Fetching global commands...{C.RESET}")
    
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        commands = response.json()
        print(f"\n{C.GREEN}Found {len(commands)} command(s):{C.RESET}")
        for cmd in commands:
            print(f"  • {C.YELLOW}/{cmd['name']}{C.RESET} - {cmd.get('description', 'No description')[:50]}")
        return commands
    else:
        print(f"{C.RED}[ERROR] Failed to fetch commands: {response.status_code}{C.RESET}")
        return []


def clear_commands(token: str, guild_id: str = None):
    """Clear all registered commands."""
    C = Colors
    app_id = get_application_id(token)
    if not app_id:
        print(f"{C.RED}[ERROR] Could not get application ID{C.RESET}")
        return False
    
    headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
    
    if guild_id:
        url = f"https://discord.com/api/v10/applications/{app_id}/guilds/{guild_id}/commands"
        print(f"{C.YELLOW}Clearing guild commands for {guild_id}...{C.RESET}")
    else:
        url = f"https://discord.com/api/v10/applications/{app_id}/commands"
        print(f"{C.YELLOW}Clearing global commands...{C.RESET}")
    
    # Set empty command list to clear all
    response = requests.put(url, headers=headers, json=[], timeout=30)
    
    if response.status_code == 200:
        print(f"{C.GREEN}✓ Commands cleared successfully!{C.RESET}")
        return True
    else:
        print(f"{C.RED}[ERROR] Failed to clear commands: {response.status_code} - {response.text}{C.RESET}")
        return False


def sync_commands_message():
    """Show message about how commands sync automatically."""
    C = Colors
    print(f"""
{C.CYAN}{C.BOLD}Discord Command Sync{C.RESET}
{"=" * 50}

{C.YELLOW}Note:{C.RESET} Discord slash commands are automatically synced
when the TestCenter bot starts up.

{C.CYAN}To sync commands:{C.RESET}
  1. Start/restart the TestCenter bot
  2. Commands will sync automatically on startup

{C.CYAN}To manually check/clear commands:{C.RESET}
  --list   : Show all registered commands
  --clear  : Remove all commands (will re-sync on bot start)
""")


def main():
    C = Colors
    parser = argparse.ArgumentParser(description="Discord Command Sync Utility")
    parser.add_argument("--list", action="store_true", help="List all registered commands")
    parser.add_argument("--clear", action="store_true", help="Clear all commands")
    parser.add_argument("--global", dest="use_global", action="store_true", help="Use global commands instead of guild")
    
    args = parser.parse_args()
    
    print(f"\n{C.CYAN}{C.BOLD}🔧 Discord Command Sync Utility{C.RESET}")
    print("=" * 50)
    
    token = get_bot_token()
    if not token:
        print(f"{C.RED}[ERROR] No bot token configured!{C.RESET}")
        print(f"Set TESTCENTER_BOT_TOKEN or DISCUM_BOT in your environment.")
        return 1
    
    guild_id = None if args.use_global else get_guild_id()
    
    if args.list:
        list_commands(token, guild_id)
    elif args.clear:
        confirm = input(f"{C.YELLOW}Are you sure you want to clear all commands? (y/n): {C.RESET}")
        if confirm.lower() == 'y':
            clear_commands(token, guild_id)
        else:
            print("Cancelled.")
    else:
        sync_commands_message()
        # Show current commands
        print(f"\n{C.CYAN}Current registered commands:{C.RESET}")
        list_commands(token, guild_id)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

