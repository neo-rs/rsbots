#!/usr/bin/env python3
"""
RS Bot Runner
-------------
Runs both RS Success Bot and RS Vouch Bot with a shared bot instance.
All configuration loaded from config files - no hardcoded values.
"""

import os
import sys
import json
import asyncio
from pathlib import Path
from typing import Optional

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import discord
from discord.ext import commands

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

# Import bot modules
from rs_success_bot import RSSuccessBot
from rs_vouch_bot import RSVouchBot

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class RSBotRunner:
    """Runner class that manages both bots with a shared bot instance"""
    
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        
        self.config: dict = {}
        self.bot: Optional[commands.Bot] = None
        self.success_bot: Optional[RSSuccessBot] = None
        self.vouch_bot: Optional[RSVouchBot] = None
        
        self.load_config()
        self.create_shared_bot()
    
    def load_config(self):
        """Load configuration from config.json + config.secrets.json (server-only)."""
        try:
            self.config, _, secrets_path = load_config_with_secrets(self.base_path)
            if not secrets_path.exists():
                print(f"{Colors.RED}[Runner] ERROR: Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
                sys.exit(1)
            print(f"{Colors.GREEN}[Runner] Configuration loaded from {self.config_path}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Runner] ERROR: Failed to load config: {e}{Colors.RESET}")
            sys.exit(1)
        
        # Validate bot token
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Runner] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
    
    def create_shared_bot(self):
        """Create a shared bot instance for both modules"""
        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        
        self.bot = commands.Bot(command_prefix="!", intents=intents)
        
        print(f"{Colors.CYAN}[Runner] Shared bot instance created{Colors.RESET}")
        print(f"{Colors.CYAN}[Runner] Note: RSSuccessBot will handle on_ready and sync all commands{Colors.RESET}")
    
    def initialize_modules(self):
        """Initialize both bot modules with the shared bot instance"""
        print(f"{Colors.CYAN}[Runner] Initializing bot modules...{Colors.RESET}")
        
        # Initialize Success Bot with shared bot
        try:
            self.success_bot = RSSuccessBot(bot_instance=self.bot)
            print(f"{Colors.GREEN}[Runner] ✅ Success Bot module initialized{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Runner] ❌ Failed to initialize Success Bot: {e}{Colors.RESET}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        # Initialize Vouch Bot with shared bot
        try:
            self.vouch_bot = RSVouchBot(bot_instance=self.bot)
            print(f"{Colors.GREEN}[Runner] ✅ Vouch Bot module initialized{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Runner] ❌ Failed to initialize Vouch Bot: {e}{Colors.RESET}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        print(f"{Colors.GREEN}[Runner] ✅ All modules initialized successfully{Colors.RESET}")
    
    def run(self):
        """Run the bot"""
        token = self.config.get("bot_token")
        if not token:
            print(f"{Colors.RED}[Runner] ERROR: bot_token not found in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        # Initialize modules before running
        self.initialize_modules()
        
        try:
            print(f"{Colors.CYAN}[Runner] Starting bot with shared instance...{Colors.RESET}")
            self.bot.run(token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Runner] Shutting down...{Colors.RESET}")
        finally:
            # Save data before shutdown
            if self.success_bot:
                self.success_bot.save_json_data()
                print(f"{Colors.GREEN}[Runner] Success Bot data saved{Colors.RESET}")
            if self.vouch_bot:
                self.vouch_bot.save_vouches_data()
                print(f"{Colors.GREEN}[Runner] Vouch Bot data saved{Colors.RESET}")


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--check-config", action="store_true", help="Validate config + secrets and exit (no Discord connection).")
    args = parser.parse_args()

    if args.check_config:
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")
        if errors:
            print(f"{Colors.RED}[ConfigCheck] FAILED{Colors.RESET}")
            for e in errors:
                print(f"- {e}")
            raise SystemExit(2)
        print(f"{Colors.GREEN}[ConfigCheck] OK{Colors.RESET}")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        raise SystemExit(0)

    runner = RSBotRunner()
    runner.run()


if __name__ == "__main__":
    main()
