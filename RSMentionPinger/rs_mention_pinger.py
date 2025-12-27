#!/usr/bin/env python3
"""
RS Mention Pinger Bot
---------------------
Monitors role mentions and user mentions, logs them, and sends DMs to mentioned users.
Configuration is split across:
- config.json (non-secret settings)
- config.secrets.json (server-only secrets, not committed)
"""

import os
import sys
import json
from pathlib import Path
from typing import Set, Optional

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import discord
from discord.ext import commands

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class RSMentionPinger:
    """Main bot class for mention pinging"""
    
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        
        self.config: dict = {}
        
        self.load_config()
        
        # Validate required config
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        # Setup bot
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        intents.messages = True
        
        # Disable default help command so our custom !help command can register cleanly.
        self.bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)
        self._setup_events()
    
    def load_config(self):
        """Load configuration from config.json + config.secrets.json (server-only)."""
        if not self.config_path.exists():
            print(f"{Colors.YELLOW}[Config] config.json not found, using defaults{Colors.RESET}")
            self.config = {}
            return
        try:
            self.config, _, secrets_path = load_config_with_secrets(self.base_path)
            if not secrets_path.exists():
                print(f"{Colors.YELLOW}[Config] Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
            print(f"{Colors.GREEN}[Config] Loaded configuration{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to load config: {e}{Colors.RESET}")
            self.config = {}
    
    def get_embed_color(self) -> discord.Color:
        """Get embed color from config"""
        color_config = self.config.get("embed_color", {})
        if isinstance(color_config, dict):
            r = color_config.get("r", 59)
            g = color_config.get("g", 130)
            b = color_config.get("b", 246)
            return discord.Color.from_rgb(r, g, b)
        return discord.Color.blue()
    
    def _first_image_from_message(self, message: discord.Message) -> Optional[str]:
        """Extract first image URL from message attachments or embeds"""
        # Check attachments
        for att in message.attachments:
            if (att.content_type and att.content_type.startswith("image/")) or \
               att.filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
                return att.url
        
        # Check embeds
        for e in message.embeds:
            if e.image and e.image.url:
                return e.image.url
            if e.type == "image" and e.url:
                return e.url
        
        return None
    
    async def _log_role_mention(self, message: discord.Message) -> None:
        """Log role mentions to log channel"""
        watched_role_ids: Set[int] = set(self.config.get("watched_role_ids", []))
        if not watched_role_ids:
            return
        
        mentioned_role_ids = {r.id for r in message.role_mentions}
        if not (mentioned_role_ids & watched_role_ids):
            return
        
        # Log to terminal
        mentioned_roles = [r.name for r in message.role_mentions if r.id in watched_role_ids]
        print(f"{Colors.CYAN}[Mention] Role mention detected: {', '.join(mentioned_roles)} in {message.channel.name}{Colors.RESET}")
        
        log_channel_id = self.config.get("log_channel_id")
        if not log_channel_id:
            return
        
        log_channel = self.bot.get_channel(log_channel_id)
        if not isinstance(log_channel, discord.TextChannel):
            print(f"{Colors.YELLOW}⚠️ Log channel not found or not a text channel.{Colors.RESET}")
            return
        
        try:
            embed = discord.Embed(
                title="🔔 Role Mentioned",
                description=message.content or "[No text content]",
                color=self.get_embed_color(),
                timestamp=message.created_at
            )
            embed.set_author(
                name=str(message.author),
                icon_url=message.author.display_avatar.url
            )
            embed.add_field(
                name="Channel",
                value=message.channel.mention,
                inline=True
            )
            embed.add_field(
                name="Jump to Message",
                value=f"[Click here]({message.jump_url})",
                inline=False
            )
            
            # Add image if present
            img = self._first_image_from_message(message)
            if img:
                embed.set_image(url=img)
            
            embed.set_footer(text="RS Mention Pinger Bot")
            
            await log_channel.send(embed=embed)
        except Exception as e:
            print(f"{Colors.RED}⚠️ Error logging role mention: {e}{Colors.RESET}")
    
    async def _dm_mentioned_users(self, message: discord.Message) -> None:
        """Send DMs to mentioned users (excluding certain categories)"""
        excluded_category_ids: Set[int] = set(self.config.get("excluded_category_ids", []))
        
        # Check if channel is in excluded category
        if message.channel.category and message.channel.category.id in excluded_category_ids:
            return
        
        # Get unique non-bot users
        unique_users = {u for u in message.mentions if not u.bot}
        if not unique_users:
            return
        
        dm_template = self.config.get(
            "dm_message_template",
            "{author_mention} mentioned you {jump_url}"
        )
        
        print(f"{Colors.CYAN}[DM] Sending mention DMs to {len(unique_users)} user(s) from {message.channel.name}{Colors.RESET}")
        for user in unique_users:
            try:
                dm_text = dm_template.format(
                    author_mention=message.author.mention,
                    jump_url=message.jump_url
                )
                await user.send(dm_text)
                print(f"{Colors.GREEN}✅ Sent mention DM to {user} ({user.id}){Colors.RESET}")
            except discord.Forbidden:
                print(f"{Colors.YELLOW}⚠️ Couldn't DM {user} ({user.id}): User has DMs disabled{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ Couldn't DM {user} ({user.id}): {e}{Colors.RESET}")
    
    def _setup_events(self):
        """Setup bot event handlers"""
        
        @self.bot.event
        async def on_ready():
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  🔔 RS Mention Pinger Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            
            guild_id = self.config.get("guild_id")
            guild = None
            if guild_id:
                guild = self.bot.get_guild(guild_id)
                if guild:
                    print(f"{Colors.GREEN}[Bot] Connected to: {guild.name}{Colors.RESET}")
            
            # Display config information
            print(f"\n{Colors.CYAN}[Config] Configuration Information:{Colors.RESET}")
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            
            if guild:
                print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET} {Colors.BOLD}{guild.name}{Colors.RESET} (ID: {guild_id})")
                
                # Log channel
                log_channel_id = self.config.get("log_channel_id")
                if log_channel_id:
                    log_channel = guild.get_channel(log_channel_id)
                    if log_channel:
                        print(f"{Colors.GREEN}📝 Log Channel:{Colors.RESET} {Colors.BOLD}{log_channel.name}{Colors.RESET} (ID: {log_channel_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Log Channel:{Colors.RESET} Not found (ID: {log_channel_id})")
                
                # Watched roles
                watched_role_ids = self.config.get("watched_role_ids", [])
                if watched_role_ids:
                    print(f"{Colors.GREEN}👀 Watched Roles:{Colors.RESET} {len(watched_role_ids)} role(s)")
                    for role_id in watched_role_ids[:5]:  # Show first 5
                        role = guild.get_role(role_id)
                        if role:
                            print(f"   • {Colors.BOLD}{role.name}{Colors.RESET} (ID: {role_id})")
                        else:
                            print(f"   • {Colors.RED}❌ Not found{Colors.RESET} (ID: {role_id})")
                    if len(watched_role_ids) > 5:
                        print(f"   ... and {len(watched_role_ids) - 5} more (use !configinfo to see all)")
                
                # Excluded categories
                excluded_category_ids = self.config.get("excluded_category_ids", [])
                if excluded_category_ids:
                    print(f"{Colors.GREEN}🚫 Excluded Categories:{Colors.RESET} {len(excluded_category_ids)} category/categories")
                    for cat_id in excluded_category_ids:
                        category = guild.get_channel(cat_id)
                        if category:
                            print(f"   • {Colors.BOLD}{category.name}{Colors.RESET} (ID: {cat_id})")
                        else:
                            print(f"   • {Colors.RED}❌ Not found{Colors.RESET} (ID: {cat_id})")
                
                # DM feature info
                dm_template = self.config.get("dm_message_template", "{author_mention} mentioned you {jump_url}")
                print(f"{Colors.GREEN}📩 DM Feature:{Colors.RESET} {Colors.BOLD}Enabled{Colors.RESET}")
                print(f"   Template: {dm_template}")
                print(f"   • Sends DM to users when they're mentioned")
                print(f"   • Skips DMs in excluded categories")
                print(f"   • Ignores bot mentions")
            else:
                print(f"{Colors.YELLOW}⚠️  Guild not found (ID: {guild_id}){Colors.RESET}")
            
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
        
        @self.bot.event
        async def on_message(message: discord.Message):
            # Ignore bot messages and DMs
            if message.author.bot or message.guild is None:
                await self.bot.process_commands(message)
                return
            
            # Check guild ID
            guild_id = self.config.get("guild_id")
            if guild_id and message.guild.id != guild_id:
                await self.bot.process_commands(message)
                return
            
            # Log role mentions
            try:
                await self._log_role_mention(message)
            except Exception as e:
                print(f"{Colors.RED}⚠️ Error logging role mention: {e}{Colors.RESET}")
            
            # DM mentioned users
            try:
                await self._dm_mentioned_users(message)
            except Exception as e:
                print(f"{Colors.RED}⚠️ Error DMing mentioned users: {e}{Colors.RESET}")
            
            # Process commands
            await self.bot.process_commands(message)
        
        @self.bot.command(name='reload')
        async def reload_config(ctx):
            """Reload config from file"""
            self.load_config()
            await ctx.send("✅ Config reloaded!", delete_after=5)
            try:
                await ctx.message.delete()
            except Exception:
                pass
        
        @self.bot.command(name='status')
        async def bot_status(ctx):
            """Show bot status"""
            guild_id = self.config.get("guild_id")
            guild = self.bot.get_guild(guild_id) if guild_id else None
            
            embed = discord.Embed(
                title="🔔 RS Mention Pinger Bot Status",
                color=self.get_embed_color()
            )
            embed.add_field(
                name="Bot Status",
                value=f"✅ Online" if self.bot.user else "❌ Offline",
                inline=True
            )
            embed.add_field(
                name="Bot User",
                value=str(self.bot.user) if self.bot.user else "N/A",
                inline=True
            )
            embed.add_field(
                name="Guild Status",
                value=f"✅ Connected to {guild.name}" if guild else "❌ Not connected",
                inline=True
            )
            embed.add_field(
                name="Watched Roles",
                value=str(len(self.config.get("watched_role_ids", []))),
                inline=True
            )
            embed.add_field(
                name="Excluded Categories",
                value=str(len(self.config.get("excluded_category_ids", []))),
                inline=True
            )
            embed.set_footer(text="RS Mention Pinger Bot")
            
            await ctx.send(embed=embed, delete_after=30)
            try:
                await ctx.message.delete()
            except Exception:
                pass
        
        @self.bot.command(name='configinfo', aliases=['config', 'ids', 'showids'])
        async def config_info(ctx):
            """Show what all the IDs in config.json actually represent"""
            guild_id = self.config.get("guild_id")
            guild = self.bot.get_guild(guild_id) if guild_id else None
            
            if not guild:
                await ctx.send("❌ Bot not connected to the configured guild.", delete_after=10)
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                return
            
            embed = discord.Embed(
                title="📋 Config IDs Information",
                description="What each ID in config.json represents:",
                color=self.get_embed_color()
            )
            
            # Guild info
            embed.add_field(
                name="🏠 Guild",
                value=f"**{guild.name}**\nID: `{guild_id}`",
                inline=False
            )
            
            # Log channel
            log_channel_id = self.config.get("log_channel_id")
            if log_channel_id:
                log_channel = guild.get_channel(log_channel_id)
                if log_channel:
                    embed.add_field(
                        name="📝 Log Channel",
                        value=f"**{log_channel.mention}** (`{log_channel.name}`)\nID: `{log_channel_id}`",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name="📝 Log Channel",
                        value=f"❌ Channel not found\nID: `{log_channel_id}`",
                        inline=False
                    )
            
            # Watched roles
            watched_role_ids = self.config.get("watched_role_ids", [])
            if watched_role_ids:
                role_list = []
                for role_id in watched_role_ids:
                    role = guild.get_role(role_id)
                    if role:
                        role_list.append(f"**{role.name}** (`{role_id}`)")
                    else:
                        role_list.append(f"❌ Role not found (`{role_id}`)")
                
                # Split into chunks if too long
                role_text = "\n".join(role_list[:10])  # Show first 10
                if len(role_list) > 10:
                    role_text += f"\n... and {len(role_list) - 10} more"
                
                embed.add_field(
                    name=f"👀 Watched Roles ({len(watched_role_ids)})",
                    value=role_text,
                    inline=False
                )
            
            # Excluded categories
            excluded_category_ids = self.config.get("excluded_category_ids", [])
            if excluded_category_ids:
                category_list = []
                for cat_id in excluded_category_ids:
                    category = guild.get_channel(cat_id)
                    if category:
                        category_list.append(f"**{category.name}** (`{cat_id}`)")
                    else:
                        category_list.append(f"❌ Category not found (`{cat_id}`)")
                
                embed.add_field(
                    name=f"🚫 Excluded Categories ({len(excluded_category_ids)})",
                    value="\n".join(category_list),
                    inline=False
                )
            
            embed.set_footer(text="RS Mention Pinger Bot | Use !configinfo to see this again")
            
            await ctx.send(embed=embed, delete_after=60)
            try:
                await ctx.message.delete()
            except Exception:
                pass
        
        @self.bot.command(name='help', aliases=['commands', 'h', 'helpme'])
        async def help_command(ctx):
            """Show all available commands with detailed explanations"""
            embed = discord.Embed(
                title="🔔 RS Mention Pinger Bot - Command Reference",
                description=(
                    "**Complete guide to all available commands**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All commands use the `!` prefix and auto-delete.\n"
                    "Replies are visible to everyone (not ephemeral)."
                ),
                color=self.get_embed_color()
            )
            
            # Quick Command Reference
            embed.add_field(
                name="⚡ Quick Commands",
                value=(
                    "`!help` - Show this help menu\n"
                    "`!status` - Show bot status\n"
                    "`!configinfo` - View what all IDs represent\n"
                    "`!reload` - Reload config from file"
                ),
                inline=True
            )
            
            embed.add_field(
                name="📋 Command Aliases",
                value=(
                    "`!help` = `!commands`, `!h`, `!helpme`\n"
                    "`!configinfo` = `!config`, `!ids`, `!showids`"
                ),
                inline=True
            )
            
            # Detailed Command Descriptions
            embed.add_field(
                name="📖 Command Details",
                value=(
                    "**`!status`**\n"
                    "Shows bot status, connection info, and configuration counts.\n\n"
                    
                    "**`!configinfo`**\n"
                    "Displays what all the IDs in config.json actually represent.\n"
                    "Shows actual names for roles, channels, and categories.\n\n"
                    
                    "**`!reload`**\n"
                    "Reloads configuration from config.json file.\n"
                    "Useful after editing config.json manually.\n\n"
                    
                    "**`!help`**\n"
                    "Shows this help menu with all available commands."
                ),
                inline=False
            )
            
            # Bot Features
            embed.add_field(
                name="🔔 Bot Features",
                value=(
                    "**Role Mention Logging**\n"
                    "Automatically logs when watched roles are mentioned.\n\n"
                    
                    "**User Mention DMs**\n"
                    "Sends DMs to users when they're mentioned (except in excluded categories).\n\n"
                    
                    "**Image Support**\n"
                    "Captures and displays images in role mention logs."
                ),
                inline=False
            )
            
            embed.set_footer(text="RS Mention Pinger Bot | Use !help to see this again")
            
            await ctx.send(embed=embed, delete_after=60)
            try:
                await ctx.message.delete()
            except Exception:
                pass
    
    def run(self):
        """Start the bot"""
        token = self.config.get("bot_token")
        if not token:
            print(f"{Colors.RED}[Bot] ERROR: No bot token configured!{Colors.RESET}")
            sys.exit(1)
        
        try:
            self.bot.run(token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Bot] Shutting down...{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Bot] Fatal error: {e}{Colors.RESET}")
            sys.exit(1)


if __name__ == "__main__":
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

    bot = RSMentionPinger()
    bot.run()

