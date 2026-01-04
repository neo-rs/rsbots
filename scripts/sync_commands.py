# -*- coding: utf-8 -*-
"""
Command Sync Script
-------------------
Syncs slash commands to Discord via the Discord API.
Run this script to manually register all commands.
"""

import sys
import os

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Ensure project root is in sys.path
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import asyncio
import discord
from discord import app_commands
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(_project_root, "config", "settings.env"))
load_dotenv(os.path.join(_project_root, "config", "tokens-api.env"))

MIRRORWORLD_SERVER = int(os.getenv("MIRRORWORLD_GUILD_ID", "0") or os.getenv("MIRRORWORLD_SERVER", "0"))
BOT_TOKEN = os.getenv("DISCORD_BOT_TESTCENTER")

if not BOT_TOKEN:
    print("[ERROR] DISCORD_BOT_TESTCENTER token not found in config/tokens-api.env")
    sys.exit(1)

if not MIRRORWORLD_SERVER:
    print("[ERROR] MIRRORWORLD_GUILD_ID not found in config/settings.env")
    sys.exit(1)
    
print(f"[INFO] Target Guild ID: {MIRRORWORLD_SERVER}")

class CommandSyncBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.synced = False
    
    async def on_ready(self):
        if self.synced:
            return
        
        print(f"[INFO] Logged in as {self.user}")
        print(f"[INFO] Syncing commands to guild {MIRRORWORLD_SERVER}...")
        
        # Register basic commands
        await self._register_commands()
        
        # Sync to guild
        try:
            guild_obj = discord.Object(id=MIRRORWORLD_SERVER)
            synced = await self.tree.sync(guild=guild_obj)
            print(f"[OK] Synced {len(synced)} command(s) to MirrorWorld")
            
            if synced:
                for cmd in synced:
                    print(f"  - /{cmd.name}: {cmd.description[:50]}...")
            
            # Check existing commands
            existing = await self.tree.fetch_commands(guild=guild_obj)
            print(f"[INFO] Existing commands in Discord: {len(existing)}")
            for cmd in existing:
                print(f"  - /{cmd.name}: {cmd.description[:50]}...")
            
        except Exception as e:
            print(f"[ERROR] Sync failed: {e}")
        
        self.synced = True
        print("[DONE] Command sync complete. Closing bot...")
        await self.close()
    
    async def _register_commands(self):
        """Register commands to the tree."""
        # Simple test commands
        @self.tree.command(name="ping", description="Check if the bot is responsive")
        async def ping(interaction: discord.Interaction):
            await interaction.response.send_message(f"Pong! Latency: {round(self.latency * 1000)}ms")
        
        @self.tree.command(name="status", description="Check bot status")
        async def status(interaction: discord.Interaction):
            embed = discord.Embed(
                title="Bot Status",
                description="All systems operational",
                color=0x00FF00
            )
            embed.add_field(name="Bot", value=str(self.user), inline=True)
            embed.add_field(name="Guilds", value=str(len(self.guilds)), inline=True)
            embed.add_field(name="Latency", value=f"{round(self.latency * 1000)}ms", inline=True)
            await interaction.response.send_message(embed=embed)
        
        @self.tree.command(name="help", description="Show available commands")
        async def help_cmd(interaction: discord.Interaction):
            commands = self.tree.get_commands()
            cmd_list = "\n".join([f"/{cmd.name} - {cmd.description}" for cmd in commands])
            embed = discord.Embed(
                title="Available Commands",
                description=cmd_list or "No commands registered",
                color=0x5865F2
            )
            await interaction.response.send_message(embed=embed)
        
        print(f"[INFO] Registered {len(self.tree.get_commands())} basic commands")


async def main():
    bot = CommandSyncBot()
    await bot.start(BOT_TOKEN)


if __name__ == "__main__":
    print("=" * 50)
    print("COMMAND SYNC SCRIPT")
    print("=" * 50)
    asyncio.run(main())

