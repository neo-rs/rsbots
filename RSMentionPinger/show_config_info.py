#!/usr/bin/env python3
"""
Helper script to show what all the IDs in config.json represent.
This script connects to Discord and fetches the actual names.
"""

import json
import sys
from pathlib import Path

try:
    import discord
    from discord.ext import commands
except ImportError:
    print("ERROR: discord.py not installed. Run: pip install discord.py")
    sys.exit(1)

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

async def show_config_info():
    """Fetch and display config information"""
    base_path = Path(__file__).parent
    config_path = base_path / "config.json"
    
    if not config_path.exists():
        print(f"{Colors.RED}ERROR: config.json not found!{Colors.RESET}")
        return
    
    # Load config
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    bot_token = config.get("bot_token")
    if not bot_token:
        print(f"{Colors.RED}ERROR: bot_token not set in config.json{Colors.RESET}")
        return
    
    # Setup bot with minimal intents
    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True
    
    bot = commands.Bot(command_prefix="!", intents=intents)
    
    @bot.event
    async def on_ready():
        print(f"\n{Colors.CYAN}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}  📋 Config IDs Information{Colors.RESET}")
        print(f"{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        
        guild_id = config.get("guild_id")
        if not guild_id:
            print(f"{Colors.RED}ERROR: guild_id not set in config.json{Colors.RESET}")
            await bot.close()
            return
        
        guild = bot.get_guild(guild_id)
        if not guild:
            print(f"{Colors.RED}ERROR: Bot not in guild with ID {guild_id}{Colors.RESET}")
            await bot.close()
            return
        
        print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET}")
        print(f"   Name: {Colors.BOLD}{guild.name}{Colors.RESET}")
        print(f"   ID: {guild_id}\n")
        
        # Log channel
        log_channel_id = config.get("log_channel_id")
        if log_channel_id:
            log_channel = guild.get_channel(log_channel_id)
            if log_channel:
                print(f"{Colors.GREEN}📝 Log Channel:{Colors.RESET}")
                print(f"   Name: {Colors.BOLD}{log_channel.name}{Colors.RESET}")
                print(f"   ID: {log_channel_id}\n")
            else:
                print(f"{Colors.YELLOW}⚠️  Log Channel:{Colors.RESET}")
                print(f"   Channel not found (ID: {log_channel_id})\n")
        
        # Watched roles
        watched_role_ids = config.get("watched_role_ids", [])
        if watched_role_ids:
            print(f"{Colors.GREEN}👀 Watched Roles ({len(watched_role_ids)}):{Colors.RESET}")
            for role_id in watched_role_ids:
                role = guild.get_role(role_id)
                if role:
                    print(f"   • {Colors.BOLD}{role.name}{Colors.RESET} (ID: {role_id})")
                else:
                    print(f"   • {Colors.RED}❌ Role not found{Colors.RESET} (ID: {role_id})")
            print()
        
        # Excluded categories
        excluded_category_ids = config.get("excluded_category_ids", [])
        if excluded_category_ids:
            print(f"{Colors.GREEN}🚫 Excluded Categories ({len(excluded_category_ids)}):{Colors.RESET}")
            for cat_id in excluded_category_ids:
                category = guild.get_channel(cat_id)
                if category:
                    print(f"   • {Colors.BOLD}{category.name}{Colors.RESET} (ID: {cat_id})")
                else:
                    print(f"   • {Colors.RED}❌ Category not found{Colors.RESET} (ID: {cat_id})")
            print()
        
        print(f"{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        await bot.close()
    
    try:
        await bot.start(bot_token)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}ERROR: {e}{Colors.RESET}")

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(show_config_info())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted{Colors.RESET}")

