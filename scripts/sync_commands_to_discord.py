#!/usr/bin/env python3
"""
Sync Commands to Discord
------------------------
Directly syncs all commands from command_registry.json to Discord API.
Bypasses discord.py tree.sync() issues.
"""

import sys
import os
import json
import time
import hashlib
import requests
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Add project root to path
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root))

def load_env() -> Dict[str, str]:
    """Load tokens and guild IDs from config."""
    env = {}
    
    # Try using config_loader first (preferred method)
    try:
        from neonxt.core.config_loader import config as _cfg
        token = _cfg.get_discord_bot_token('testcenter')
        if token:
            env['TESTCENTER_BOT_TOKEN'] = token
            env['DISCORD_BOT_TESTCENTER'] = token
        
        guild_id = _cfg.get_guild_id('mirrorworld')
        if guild_id:
            env['MIRRORWORLD_SERVER'] = str(guild_id)
        
        # If we got both, return early
        if token and guild_id:
            return env
    except Exception as e:
        print(f"[WARN] Could not load from config_loader: {e}")
    
    # Fallback: Try tokens-api.env
    env_path = _project_root / "config" / "tokens-api.env"
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    env[key.strip()] = value.strip()
    
    # Also try settings.env for MIRRORWORLD_SERVER
    settings_path = _project_root / "config" / "settings.env"
    if settings_path.exists() and 'MIRRORWORLD_SERVER' not in env:
        with open(settings_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    if key.strip() == 'MIRRORWORLD_SERVER':
                        env[key.strip()] = value.strip()
                        break
    
    return env

def load_registry() -> Dict[str, Any]:
    """Load command registry."""
    registry_path = _project_root / "config" / "command_registry.json"
    
    if registry_path.exists():
        with open(registry_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    return {"commands": {}}

def convert_to_discord_format(cmd_data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert registry command to Discord API format."""
    discord_cmd = {
        "name": cmd_data["name"],
        "description": cmd_data.get("description", "No description")[:100],
        "type": cmd_data.get("type", 1)
    }
    
    # Convert options
    if "options" in cmd_data and cmd_data["options"]:
        discord_cmd["options"] = cmd_data["options"]
    
    return discord_cmd

def normalize_command_for_comparison(cmd: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize command for comparison (remove Discord-specific fields)."""
    normalized = {
        "name": cmd.get("name", "").lower(),
        "description": cmd.get("description", "")[:100],
        "type": cmd.get("type", 1),
        "options": sorted(cmd.get("options", []), key=lambda x: x.get("name", ""))
    }
    return normalized

def commands_are_identical(registry_cmd: Dict[str, Any], discord_cmd: Dict[str, Any]) -> bool:
    """Check if registry command matches Discord command."""
    reg_norm = normalize_command_for_comparison(registry_cmd)
    disc_norm = normalize_command_for_comparison(discord_cmd)
    return reg_norm == disc_norm

def check_sync_needed(registry_commands: Dict[str, Any], discord_commands: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Check if sync is needed by comparing registry vs Discord.
    Returns: (needs_sync: bool, reason: str)
    """
    # Convert Discord commands to dict by name
    discord_dict = {cmd.get("name", "").lower(): cmd for cmd in discord_commands}
    registry_names = {name.lower() for name in registry_commands.keys()}
    discord_names = set(discord_dict.keys())
    
    # Check if counts match
    if len(registry_commands) != len(discord_commands):
        missing = registry_names - discord_names
        extra = discord_names - registry_names
        if missing:
            return True, f"{len(missing)} command(s) missing in Discord: {', '.join(list(missing)[:5])}"
        if extra:
            return True, f"{len(extra)} extra command(s) in Discord: {', '.join(list(extra)[:5])}"
        return True, f"Count mismatch: {len(registry_commands)} in registry vs {len(discord_commands)} in Discord"
    
    # Check if all commands match
    differences = []
    for name, reg_cmd in registry_commands.items():
        name_lower = name.lower()
        if name_lower not in discord_dict:
            differences.append(f"{name} (missing in Discord)")
            continue
        
        if not commands_are_identical(reg_cmd, discord_dict[name_lower]):
            differences.append(f"{name} (content differs)")
    
    if differences:
        return True, f"{len(differences)} command(s) differ: {', '.join(differences[:5])}"
    
    return False, "All commands match, sync not needed"

def sync_with_retry(url: str, headers: Dict[str, str], discord_commands: List[Dict[str, Any]], max_retries: int = 3) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Sync commands with rate limit retry logic.
    Returns: (result: List[Dict] or None, error: str or None)
    """
    for attempt in range(max_retries):
        try:
            r = requests.put(url, headers=headers, json=discord_commands, timeout=60)
            
            if r.status_code == 200:
                return r.json(), None
            
            elif r.status_code == 429:
                # Rate limited
                try:
                    error_data = r.json()
                    retry_after = int(error_data.get("retry_after", 60))
                    message = error_data.get("message", "Rate limited")
                    
                    if attempt < max_retries - 1:
                        print(f"  [RATE LIMIT] {message}")
                        print(f"  [WAIT] Retrying in {retry_after} seconds... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_after)
                        continue
                    else:
                        return None, f"Rate limited: {message} (retry after {retry_after}s)"
                except:
                    retry_after = 60
                    if attempt < max_retries - 1:
                        print(f"  [RATE LIMIT] Waiting {retry_after} seconds... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_after)
                        continue
                    else:
                        return None, f"Rate limited (retry after {retry_after}s)"
            
            else:
                # Other error
                error_text = r.text[:500] if r.text else "Unknown error"
                return None, f"HTTP {r.status_code}: {error_text}"
        
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"  [TIMEOUT] Retrying... (attempt {attempt + 1}/{max_retries})")
                time.sleep(2 ** attempt)
                continue
            else:
                return None, "Request timeout"
        
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  [ERROR] {str(e)} - Retrying... (attempt {attempt + 1}/{max_retries})")
                time.sleep(2 ** attempt)
                continue
            else:
                return None, str(e)
    
    return None, "Max retries exceeded"

def main():
    print("=" * 60)
    print("  DISCORD COMMAND SYNC")
    print("=" * 60)
    
    # Load config
    env = load_env()
    # Try multiple possible token names
    token = env.get("TESTCENTER_BOT_TOKEN") or env.get("DISCORD_BOT_TESTCENTER") or env.get("DISCORD_BOT_TESTCENTER_BOT")
    guild_id = env.get("MIRRORWORLD_SERVER")
    
    if not token:
        print("[ERROR] TestCenter bot token not found")
        print("  Checked: config_loader, config/tokens-api.env")
        print("  Tried: TESTCENTER_BOT_TOKEN, DISCORD_BOT_TESTCENTER, DISCORD_BOT_TESTCENTER_BOT")
        return 1
    
    if not guild_id:
        print("[ERROR] MIRRORWORLD_SERVER not found")
        print("  Checked: config_loader, config/tokens-api.env, config/settings.env")
        return 1
    
    # Get application ID
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json"
    }
    
    print("\n[1/5] Getting application ID...")
    r = requests.get("https://discord.com/api/v10/users/@me", headers=headers, timeout=10)
    if r.status_code != 200:
        print(f"[ERROR] Failed to get bot info: {r.status_code}")
        return 1
    
    app_id = r.json()["id"]
    bot_name = r.json().get("username", "Unknown")
    print(f"  Bot: {bot_name}")
    print(f"  App ID: {app_id}")
    
    # STEP 1: Check Discord server commands FIRST (before loading registry)
    print("\n[2/5] Checking current Discord commands...")
    url = f"https://discord.com/api/v10/applications/{app_id}/guilds/{guild_id}/commands"
    r = requests.get(url, headers=headers, timeout=15)
    current_cmds = r.json() if r.status_code == 200 else []
    print(f"  Currently registered in Discord: {len(current_cmds)} command(s)")
    
    if current_cmds:
        print("  Sample commands in Discord:")
        for cmd in current_cmds[:5]:
            print(f"    /{cmd.get('name', 'unknown')}")
        if len(current_cmds) > 5:
            print(f"    ... and {len(current_cmds) - 5} more")
    
    # STEP 2: Load registry (after checking Discord)
    print("\n[3/5] Loading command registry...")
    registry = load_registry()
    commands = registry.get("commands", {})
    print(f"  Found {len(commands)} command(s) in registry")
    
    if commands:
        print("  Sample commands in registry:")
        for i, cmd_name in enumerate(list(commands.keys())[:5]):
            print(f"    /{cmd_name}")
        if len(commands) > 5:
            print(f"    ... and {len(commands) - 5} more")
    
    # STEP 3: Compare and check if sync is needed
    print("\n[4/5] Comparing registry vs Discord...")
    needs_sync, reason = check_sync_needed(commands, current_cmds)
    
    if not needs_sync:
        print(f"\n[SKIP] {reason}")
        print("  All commands are already synced. No action needed.")
        return 0
    
    print(f"\n[SYNC NEEDED] {reason}")
    print(f"  Using INCREMENTAL sync: Only new/changed commands will be registered/updated")
    print(f"  Existing unchanged commands will be left alone (safe!)")
    
    # Auto-sync mode (non-interactive) - check for --yes flag
    auto_sync = '--yes' in sys.argv or '-y' in sys.argv
    force_sync = '--force' in sys.argv
    
    if not auto_sync and not force_sync:
        # Ask for confirmation
        print(f"\n[5/5] Ready to sync commands to guild {guild_id}")
        print("  This will only register new commands and update changed ones.")
        print("  Existing commands will NOT be deleted.")
        confirm = input("  Continue? (y/n): ").strip().lower()
        
        if confirm != 'y':
            print("\n  Cancelled.")
            return 0
    else:
        print(f"\n[5/5] Auto-syncing commands to guild {guild_id}...")
    
    # INCREMENTAL SYNC: Only register new commands and update changed ones
    # This prevents deleting all commands if rate limited!
    
    # Convert Discord commands to dict by name (with ID for updates)
    discord_dict = {cmd.get("name", "").lower(): cmd for cmd in current_cmds}
    
    # Categorize commands
    new_commands = []      # Commands that don't exist in Discord
    updated_commands = []  # Commands that exist but have changed
    unchanged_commands = [] # Commands that are identical
    
    for cmd_name, cmd_data in commands.items():
        try:
            discord_cmd = convert_to_discord_format(cmd_data)
            name_lower = cmd_name.lower()
            
            if name_lower not in discord_dict:
                # New command - needs to be created
                new_commands.append(discord_cmd)
            else:
                # Command exists - check if it needs updating
                existing_cmd = discord_dict[name_lower]
                if not commands_are_identical(cmd_data, existing_cmd):
                    # Command changed - needs update
                    updated_commands.append((existing_cmd.get("id"), discord_cmd))
                else:
                    # Command unchanged - skip it
                    unchanged_commands.append(cmd_name)
        except Exception as e:
            print(f"  [WARN] Skipping {cmd_name}: {e}")
    
    # Summary
    print(f"\n[SYNC PLAN]")
    print(f"  New commands to register: {len(new_commands)}")
    print(f"  Commands to update: {len(updated_commands)}")
    print(f"  Commands unchanged: {len(unchanged_commands)}")
    
    if len(new_commands) == 0 and len(updated_commands) == 0:
        print(f"\n[SKIP] No changes needed - all commands are up to date!")
        return 0
    
    # Register new commands (POST)
    created_count = 0
    if new_commands:
        print(f"\n[1/2] Registering {len(new_commands)} new command(s)...")
        for cmd in new_commands:
            try:
                r = requests.post(url, headers=headers, json=cmd, timeout=30)
                if r.status_code == 200 or r.status_code == 201:
                    created_count += 1
                    print(f"  ✅ Registered: /{cmd['name']}")
                elif r.status_code == 429:
                    error_data = r.json() if r.text else {}
                    retry_after = error_data.get("retry_after", 86400)
                    print(f"\n[ERROR] ⚠️ Rate limit reached while registering /{cmd['name']}")
                    print(f"  Retry after: {retry_after} seconds ({retry_after // 3600} hours)")
                    print(f"  Stopping sync to prevent issues. {created_count}/{len(new_commands)} commands registered.")
                    return 1
                else:
                    print(f"  ❌ Failed to register /{cmd['name']}: {r.status_code} - {r.text[:100]}")
            except Exception as e:
                print(f"  ❌ Error registering /{cmd['name']}: {e}")
    
    # Update changed commands (PATCH)
    updated_count = 0
    if updated_commands:
        print(f"\n[2/2] Updating {len(updated_commands)} changed command(s)...")
        for cmd_id, cmd in updated_commands:
            try:
                update_url = f"{url}/{cmd_id}"
                r = requests.patch(update_url, headers=headers, json=cmd, timeout=30)
                if r.status_code == 200:
                    updated_count += 1
                    print(f"  ✅ Updated: /{cmd['name']}")
                elif r.status_code == 429:
                    error_data = r.json() if r.text else {}
                    retry_after = error_data.get("retry_after", 86400)
                    print(f"\n[ERROR] ⚠️ Rate limit reached while updating /{cmd['name']}")
                    print(f"  Retry after: {retry_after} seconds ({retry_after // 3600} hours)")
                    print(f"  Stopping sync. {updated_count}/{len(updated_commands)} commands updated.")
                    return 1
                else:
                    print(f"  ❌ Failed to update /{cmd['name']}: {r.status_code} - {r.text[:100]}")
            except Exception as e:
                print(f"  ❌ Error updating /{cmd['name']}: {e}")
    
    # Success summary
    total_synced = created_count + updated_count
    if total_synced > 0:
        print(f"\n[SUCCESS] Synced {total_synced} command(s)!")
        print(f"  ✅ Registered: {created_count} new command(s)")
        print(f"  ✅ Updated: {updated_count} command(s)")
        print(f"  ⏭️  Skipped: {len(unchanged_commands)} unchanged command(s)")
        return 0
    else:
        print(f"\n[ERROR] No commands were synced")
        return 1
    
    return 0

def check_sync_needed_standalone() -> bool:
    """
    Standalone function to check if sync is needed.
    Can be called from batch files or other scripts.
    Returns: True if sync needed, False otherwise
    """
    try:
        env = load_env()
        token = env.get("TESTCENTER_BOT_TOKEN") or env.get("DISCORD_BOT_TESTCENTER") or env.get("DISCORD_BOT_TESTCENTER_BOT")
        guild_id = env.get("MIRRORWORLD_SERVER")
        
        if not token or not guild_id:
            return True  # Assume sync needed if we can't check
        
        headers = {
            "Authorization": f"Bot {token}",
            "Content-Type": "application/json"
        }
        
        # Get app ID
        r = requests.get("https://discord.com/api/v10/users/@me", headers=headers, timeout=10)
        if r.status_code != 200:
            return True  # Assume sync needed if we can't check
        
        app_id = r.json()["id"]
        
        # Load registry
        registry = load_registry()
        commands = registry.get("commands", {})
        
        # Get Discord commands
        url = f"https://discord.com/api/v10/applications/{app_id}/guilds/{guild_id}/commands"
        r = requests.get(url, headers=headers, timeout=15)
        current_cmds = r.json() if r.status_code == 200 else []
        
        # Check if sync needed
        needs_sync, _ = check_sync_needed(commands, current_cmds)
        return needs_sync
        
    except Exception:
        # On any error, assume sync needed (safer)
        return True

if __name__ == "__main__":
    # Check if called with --check flag
    if '--check' in sys.argv:
        needs_sync = check_sync_needed_standalone()
        sys.exit(0 if not needs_sync else 1)
    else:
        sys.exit(main())

