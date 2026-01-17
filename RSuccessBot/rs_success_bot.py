#!/usr/bin/env python3
"""
RS Success Bot
--------------
Bot for tracking success points with image-based submissions.
Configuration is split across:
- config.json (non-secret settings)
- config.secrets.json (server-only secrets, not committed)
Messages are stored in messages.json.
Members use slash commands, admins use prefix commands (!).
"""

import os
import sys
import json
import time
import hashlib
import asyncio
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands
from discord import app_commands
from PIL import Image
import aiohttp
import io

from mirror_world_config import load_config_with_secrets

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class RSSuccessBot:
    """Main bot class for success points tracking"""
    
    def __init__(self, bot_instance: Optional[commands.Bot] = None):
        """
        Initialize success bot
        
        Args:
            bot_instance: Optional existing bot instance to attach to.
                         If None, creates its own bot instance.
        """
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        self.messages_path = self.base_path / "messages.json"
        
        self.config: Dict[str, Any] = {}
        self.messages: Dict[str, Any] = {}
        
        # JSON data storage
        self.json_data_path = self.base_path / "success_points.json"
        self.json_data: Dict[str, Any] = {
            "points": {},
            "image_hashes": {},
            "point_movements": []
        }
        
        # Stats tracking
        self.stats = {
            'points_awarded': 0,
            'images_processed': 0,
            'duplicates_rejected': 0,
            'errors': 0,
            'started_at': None
        }
        
        self.load_config()
        self.load_messages()
        self.load_json_data()
        
        # Use provided bot instance or create new one
        if bot_instance:
            self.bot = bot_instance
            self._is_shared_bot = True
        else:
            # Validate required config for standalone mode
            if not self.config.get("bot_token"):
                print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only) for standalone mode{Colors.RESET}")
                sys.exit(1)
            
            # Setup bot for standalone mode
            intents = discord.Intents.default()
            intents.messages = True
            intents.message_content = True
            intents.guilds = True
            intents.members = True
            
            self.bot = commands.Bot(command_prefix="!", intents=intents)
            self._is_shared_bot = False
        
        self._setup_events()
        self._setup_commands()
        self._setup_slash_commands()
    
    def load_config(self):
        """Load configuration from config.json + config.secrets.json (server-only)."""
        try:
            self.config, _, secrets_path = load_config_with_secrets(Path(__file__).parent)
            if not secrets_path.exists():
                print(f"{Colors.YELLOW}[Config] Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
            print(f"{Colors.GREEN}[Config] Configuration loaded from {self.config_path}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Config] ERROR: Failed to load config: {e}{Colors.RESET}")
            sys.exit(1)
    
    def load_messages(self):
        """Load messages from messages.json"""
        try:
            with open(self.messages_path, 'r', encoding='utf-8') as f:
                self.messages = json.load(f)
            print(f"{Colors.GREEN}[Messages] Messages loaded from {self.messages_path}{Colors.RESET}")
            return True
        except FileNotFoundError:
            print(f"{Colors.RED}[Messages] ERROR: {self.messages_path} not found{Colors.RESET}")
            # Only exit on startup, not on reload
            if not hasattr(self, 'messages'):
                sys.exit(1)
            return False
        except json.JSONDecodeError as e:
            print(f"{Colors.RED}[Messages] ERROR: Invalid JSON in {self.messages_path}: {e}{Colors.RESET}")
            # Only exit on startup, not on reload
            if not hasattr(self, 'messages'):
                sys.exit(1)
            return False
        except Exception as e:
            print(f"{Colors.RED}[Messages] ERROR: Failed to load messages: {e}{Colors.RESET}")
            if not hasattr(self, 'messages'):
                sys.exit(1)
            return False
    
    def save_messages(self):
        """Save messages to messages.json"""
        try:
            with open(self.messages_path, 'w', encoding='utf-8') as f:
                json.dump(self.messages, f, indent=2, ensure_ascii=False)
            print(f"{Colors.GREEN}[Messages] Messages saved to {self.messages_path}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Messages] ERROR: Failed to save messages: {e}{Colors.RESET}")
    
    def save_config(self):
        """Save configuration to config.json"""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                # Never write secrets back into config.json
                config_to_save = dict(self.config or {})
                config_to_save.pop("bot_token", None)
                json.dump(config_to_save, f, indent=2)
            print(f"{Colors.GREEN}[Config] Configuration saved to {self.config_path}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Config] ERROR: Failed to save config: {e}{Colors.RESET}")
    
    def load_json_data(self):
        """Load JSON data from file"""
        try:
            if self.json_data_path.exists():
                with open(self.json_data_path, 'r', encoding='utf-8') as f:
                    self.json_data = json.load(f)
                # Ensure all required keys exist
                if "points" not in self.json_data:
                    self.json_data["points"] = {}
                if "image_hashes" not in self.json_data:
                    self.json_data["image_hashes"] = {}
                if "point_movements" not in self.json_data:
                    self.json_data["point_movements"] = []
                print(f"{Colors.GREEN}[JSON] Loaded data from {self.json_data_path}{Colors.RESET}")
                print(f"{Colors.CYAN}[JSON]   - {len(self.json_data['points'])} users with points{Colors.RESET}")
                print(f"{Colors.CYAN}[JSON]   - {len(self.json_data['image_hashes'])} image hashes{Colors.RESET}")
                print(f"{Colors.CYAN}[JSON]   - {len(self.json_data['point_movements'])} point movements{Colors.RESET}")
            else:
                # Initialize empty structure
                self.json_data = {
                    "points": {},
                    "image_hashes": {},
                    "point_movements": [],
                    "migrated_at": datetime.now().isoformat()
                }
                self.save_json_data()
                print(f"{Colors.YELLOW}[JSON] Created new data file: {self.json_data_path}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[JSON] ERROR: Failed to load JSON data: {e}{Colors.RESET}")
            sys.exit(1)
    
    def save_json_data(self):
        """Save JSON data to file"""
        try:
            # Preserve migrated_at if it exists, otherwise set it on first save
            if "migrated_at" not in self.json_data:
                self.json_data["migrated_at"] = datetime.now(timezone.utc).isoformat()
            
            with open(self.json_data_path, 'w', encoding='utf-8') as f:
                json.dump(self.json_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"{Colors.RED}[JSON] ERROR: Failed to save JSON data: {e}{Colors.RESET}")
    
    def get_embed_color(self):
        """Get embed color from config"""
        color_config = self.config.get("embed_color", {})
        r = color_config.get("r", 169)
        g = color_config.get("g", 199)
        b = color_config.get("b", 220)
        return discord.Color.from_rgb(r, g, b)
    
    def get_message(self, key: str, **kwargs) -> str:
        """Get message from messages.json and format with kwargs"""
        keys = key.split(".")
        msg_data = self.messages
        for k in keys:
            if isinstance(msg_data, dict):
                msg_data = msg_data.get(k)
            else:
                return f"[Message not found: {key}]"
        
        if isinstance(msg_data, str):
            try:
                # Handle member objects - convert to mention string if needed
                formatted_kwargs = {}
                for k, v in kwargs.items():
                    if k == "member" and isinstance(v, (discord.Member, discord.User)):
                        formatted_kwargs[k] = v  # Keep as object for .mention access
                    elif k == "member" and isinstance(v, str):
                        # If it's already a string (mention), create a simple object wrapper
                        class MentionWrapper:
                            def __init__(self, mention_str):
                                self.mention = mention_str
                        formatted_kwargs[k] = MentionWrapper(v)
                    else:
                        formatted_kwargs[k] = v
                
                return msg_data.format(**formatted_kwargs)
            except (KeyError, AttributeError) as e:
                return f"[Missing key in message {key}: {e}]"
        return f"[Invalid message format: {key}]"
    
    def log_point_movement(self, user_id: int, change_amount: int, reason: str, admin_user_id: int = None):
        """Log a point movement to JSON (call before updating points)"""
        old_balance = self.get_user_points(user_id)
        new_balance = max(0, old_balance + change_amount)  # Ensure non-negative
        
        # Print to terminal
        sign = "+" if change_amount > 0 else ""
        admin_info = f" (by admin {admin_user_id})" if admin_user_id else ""
        print(f"{Colors.CYAN}[Points Movement] User {user_id}: {old_balance} -> {new_balance} ({sign}{change_amount}) - {reason}{admin_info}{Colors.RESET}")
        
        # Add to point_movements array
        movement = {
            "user_id": user_id,
            "change_amount": change_amount,
            "old_balance": old_balance,
            "new_balance": new_balance,
            "reason": reason,
            "admin_user_id": admin_user_id,
            "created_at": datetime.now().isoformat()
        }
        self.json_data["point_movements"].append(movement)
        self.save_json_data()
    
    async def log_point_movement_to_channel(self, guild: discord.Guild, user_id: int, change_amount: int, 
                                            reason: str, admin_user_id: int = None):
        """Log point movement to points log channel"""
        log_channel_id = self.config.get("points_log_channel_id")
        if not log_channel_id:
            return
        
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel:
            return
        
        try:
            member = guild.get_member(user_id)
            admin_member = guild.get_member(admin_user_id) if admin_user_id else None
            
            color = discord.Color.green() if change_amount > 0 else discord.Color.red() if change_amount < 0 else discord.Color.orange()
            sign = "+" if change_amount > 0 else ""
            
            embed = discord.Embed(
                title=f"Points {reason}",
                description=f"**Change:** {sign}{change_amount} points\n**Reason:** {reason}",
                color=color,
                timestamp=datetime.now(timezone.utc)
            )
            
            if member:
                current_balance = self.get_user_points(user_id)
                old_balance = current_balance - change_amount
                embed.add_field(
                    name="Member",
                    value=f"{member.mention}\nID: `{user_id}`",
                    inline=True
                )
                embed.add_field(
                    name="Balance",
                    value=f"**Old:** {old_balance}\n**New:** {current_balance}",
                    inline=True
                )
            
            if admin_member:
                embed.add_field(
                    name="Admin",
                    value=f"{admin_member.mention}\nID: `{admin_user_id}`",
                    inline=True
                )
            
            await log_channel.send(embed=embed)
        except Exception as e:
            print(f"{Colors.RED}[Log] Failed to log point movement: {e}{Colors.RESET}")
    
    def award_point(self, user_id: int):
        """Award a point to a user"""
        user_id_str = str(user_id)
        
        # Get current points or initialize
        if user_id_str in self.json_data["points"]:
            current_points = self.json_data["points"][user_id_str].get("points", 0)
            self.json_data["points"][user_id_str]["points"] = current_points + 1
            self.json_data["points"][user_id_str]["last_updated"] = datetime.now().isoformat()
        else:
            self.json_data["points"][user_id_str] = {
                "points": 1,
                "last_updated": datetime.now().isoformat()
            }
        
        self.save_json_data()
        self.log_point_movement(user_id, 1, "Image success posted")
        self.stats['points_awarded'] += 1
    
    def get_user_points(self, user_id: int) -> int:
        """Get user's current points"""
        user_id_str = str(user_id)
        if user_id_str in self.json_data["points"]:
            return self.json_data["points"][user_id_str].get("points", 0)
        return 0
    
    def save_image_hash(self, hash_value: str, user_id: int):
        """Save image hash to JSON"""
        if hash_value not in self.json_data["image_hashes"]:
            self.json_data["image_hashes"][hash_value] = {
                "user_id": user_id,
                "created_at": datetime.now().isoformat()
            }
            self.save_json_data()
    
    def is_duplicate_hash(self, hash_value: str) -> bool:
        """Check if image hash already exists"""
        return hash_value in self.json_data["image_hashes"]
    
    def get_image_hash(self, image_bytes: bytes) -> str:
        """Generate hash from image bytes"""
        with Image.open(io.BytesIO(image_bytes)) as img:
            img = img.convert("RGB").resize((64, 64))
            return hashlib.sha256(img.tobytes()).hexdigest()
    
    async def fetch_image_bytes(self, url: str) -> bytes:
        """Fetch image bytes from URL"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.read()
    
    async def log_action(self, guild: discord.Guild, message: str, log_type: str = "info",
                        member: discord.Member = None, channel: discord.TextChannel = None):
        """Log action to log channel with embed"""
        log_channel_id = self.config.get("log_channel_id")
        if not log_channel_id:
            return
        
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel:
            return
        
        try:
            # Determine embed color and title based on log type
            if log_type == "point_awarded":
                color = discord.Color.green()
                title = "✅ Point Awarded"
            elif log_type == "duplicate_rejected":
                color = discord.Color.orange()
                title = "⚠️ Duplicate Rejected"
            elif log_type == "points_reset":
                color = discord.Color.red()
                title = "🔄 Points Reset"
            elif log_type == "admin_action":
                color = discord.Color.blue()
                title = "⚙️ Admin Action"
            elif log_type == "error":
                color = discord.Color.red()
                title = "❌ Error"
                self.stats['errors'] += 1
            else:
                color = self.get_embed_color()
                title = "📋 Action"
            
            embed = discord.Embed(
                title=title,
                description=message,
                color=color,
                timestamp=datetime.now(timezone.utc)
            )
            
            if member:
                # Handle both Member objects and user IDs
                if isinstance(member, (discord.Member, discord.User)):
                    embed.add_field(
                        name="Member",
                        value=f"{member.mention}\nID: `{member.id}`",
                        inline=True
                    )
                elif isinstance(member, int):
                    # If it's just a user ID, show it
                    embed.add_field(
                        name="Member",
                        value=f"<@{member}>\nID: `{member}`",
                        inline=True
                    )
                else:
                    # Fallback for other types
                    embed.add_field(
                        name="Member",
                        value=f"{str(member)}",
                        inline=True
                    )
            
            if channel:
                embed.add_field(
                    name="Channel",
                    value=f"{channel.mention}\nID: `{channel.id}`",
                    inline=True
                )
            
            embed.set_footer(
                text=f"🎯 Success Bot | Points: {self.stats['points_awarded']} | "
                     f"Processed: {self.stats['images_processed']} | Errors: {self.stats['errors']}"
            )
            
            await log_channel.send(embed=embed)
        except Exception as e:
            print(f"{Colors.RED}[Log] Failed to log action: {e}{Colors.RESET}")
    
    def _setup_events(self):
        """Setup Discord event handlers"""
        
        @self.bot.event
        async def on_ready():
            # Set bot status to invisible/offline
            await self.bot.change_presence(status=discord.Status.invisible)
            
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  🎯 RS Success Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            print(f"{Colors.CYAN}[Bot] Status set to invisible (offline){Colors.RESET}")
            
            self.stats['started_at'] = datetime.now(timezone.utc)
            
            guild_id = self.config.get("guild_id")
            if guild_id:
                guild = self.bot.get_guild(guild_id)
                if guild:
                    print(f"{Colors.GREEN}[Bot] Connected to: {guild.name}{Colors.RESET}")
                    
                    # Sync slash commands - wait a bit for all modules to register
                    await asyncio.sleep(1)  # Give other modules time to register commands
                    try:
                        # First, get all registered commands
                        all_commands = list(self.bot.tree.get_commands())
                        print(f"{Colors.CYAN}[Commands] Found {len(all_commands)} registered command(s) before sync{Colors.RESET}")
                        for cmd in all_commands:
                            print(f"{Colors.CYAN}   • /{cmd.name}{Colors.RESET}")
                        
                        # Sync to guild (instant)
                        try:
                            synced = await self.bot.tree.sync(guild=discord.Object(id=guild_id))
                            if synced:
                                print(f"{Colors.GREEN}[Commands] Synced {len(synced)} slash command(s) to guild{Colors.RESET}")
                                for cmd in synced:
                                    print(f"{Colors.GREEN}   • /{cmd.name}{Colors.RESET}")
                            else:
                                # Commands already synced or no changes needed
                                print(f"{Colors.YELLOW}[Commands] Sync returned 0 commands (likely already synced){Colors.RESET}")
                                print(f"{Colors.YELLOW}[Commands] Registered {len(all_commands)} commands should be available{Colors.RESET}")
                                print(f"{Colors.YELLOW}[Commands] If commands don't appear, use !sync to force re-sync{Colors.RESET}")
                        except discord.HTTPException as http_e:
                            print(f"{Colors.RED}[Commands] HTTP Error during sync: {http_e.status} - {http_e.text}{Colors.RESET}")
                            if http_e.status == 429:
                                print(f"{Colors.YELLOW}[Commands] Rate limited. Commands will sync automatically when rate limit expires{Colors.RESET}")
                        except Exception as sync_e:
                            print(f"{Colors.RED}[Commands] Error during guild sync: {sync_e}{Colors.RESET}")
                            import traceback
                            traceback.print_exc()
                        
                        # Also try global sync (can take up to 1 hour, but ensures commands work everywhere)
                        try:
                            global_synced = await self.bot.tree.sync()
                            if global_synced:
                                print(f"{Colors.GREEN}[Commands] Also synced {len(global_synced)} command(s) globally (may take up to 1 hour){Colors.RESET}")
                        except Exception as global_e:
                            print(f"{Colors.YELLOW}[Commands] Global sync warning (this is normal): {global_e}{Colors.RESET}")
                            
                    except Exception as e:
                        print(f"{Colors.RED}[Commands] ERROR: Failed to sync slash commands: {e}{Colors.RESET}")
                        import traceback
                        traceback.print_exc()
                        print(f"{Colors.YELLOW}[Commands] You can manually sync using !sync command{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[Bot] Guild not found (ID: {guild_id}){Colors.RESET}")
            
            # Display config info
            print(f"\n{Colors.CYAN}[Config] Configuration Information:{Colors.RESET}")
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            if guild:
                print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET} {Colors.BOLD}{guild.name}{Colors.RESET} (ID: {guild_id})")
                
                success_channel_ids = self.config.get("success_channel_ids", [])
                print(f"{Colors.GREEN}📢 Success Channels:{Colors.RESET} {len(success_channel_ids)} channel(s)")
                for ch_id in success_channel_ids[:3]:
                    ch = guild.get_channel(ch_id)
                    if ch:
                        print(f"   • {Colors.BOLD}{ch.name}{Colors.RESET} (ID: {ch_id})")
                    else:
                        print(f"   • {Colors.RED}❌ Not found{Colors.RESET} (ID: {ch_id})")
                if len(success_channel_ids) > 3:
                    print(f"   ... and {len(success_channel_ids) - 3} more")
                
                role_id = self.config.get("role_id_to_watch")
                if role_id:
                    role = guild.get_role(role_id)
                    if role:
                        print(f"{Colors.GREEN}👤 Watch Role:{Colors.RESET} {Colors.BOLD}{role.name}{Colors.RESET} (ID: {role_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Watch Role:{Colors.RESET} Not found (ID: {role_id})")
            
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
        
        @self.bot.event
        async def on_message(message: discord.Message):
            if message.author.bot or message.guild is None:
                await self.bot.process_commands(message)
                return
            
            guild_id = self.config.get("guild_id")
            if message.guild.id != guild_id:
                await self.bot.process_commands(message)
                return
            
            success_channel_ids = self.config.get("success_channel_ids", [])
            if message.channel.id not in success_channel_ids:
                await self.bot.process_commands(message)
                return
            
            # We're in a success channel - process image attachments for points
            print(f"{Colors.CYAN}[Success Channel] Message in success channel {message.channel.id} from {message.author} (ID: {message.author.id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Success Channel] Message has {len(message.attachments)} attachment(s){Colors.RESET}")
            
            # Check if message has no attachments
            if len(message.attachments) == 0:
                print(f"{Colors.YELLOW}[Success Channel] No attachments detected, sending reminder{Colors.RESET}")
                
                # Send temporary message that auto-deletes (ephemeral-like behavior)
                try:
                    reminder_embed = discord.Embed(
                        title=self.get_message("no_attachment_reminder.title"),
                        description=self.get_message("no_attachment_reminder.description", member=message.author),
                        color=discord.Color.orange(),
                        timestamp=datetime.now(timezone.utc)
                    )
                    reminder_embed.set_footer(text=f"{self.get_message('no_attachment_reminder.footer')} • Only you can see this")
                    reminder_msg = await message.channel.send(content=message.author.mention, embed=reminder_embed)
                    # Auto-delete reminder after 10 seconds
                    await asyncio.sleep(10)
                    try:
                        await reminder_msg.delete()
                    except:
                        pass
                    print(f"{Colors.GREEN}[Success Channel] No attachment reminder sent (auto-delete){Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.RED}[Error] Failed to send reminder: {e}{Colors.RESET}")
                
                # Delete the original message
                try:
                    await message.delete()
                    print(f"{Colors.GREEN}[Success Channel] Deleted message without attachment{Colors.RESET}")
                except discord.Forbidden:
                    print(f"{Colors.RED}[Error] Missing permission to delete messages in channel {message.channel.id}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.RED}[Error] Failed to delete message: {e}{Colors.RESET}")
                
                # Still process commands, then return
                await self.bot.process_commands(message)
                return
            
            has_new_success = False
            
            for attachment in message.attachments:
                print(f"{Colors.CYAN}[Success Channel] Processing attachment: {attachment.filename} (type: {attachment.content_type}){Colors.RESET}")
                if attachment.content_type and attachment.content_type.startswith("image/"):
                    try:
                        print(f"{Colors.GREEN}[Success Channel] Fetching image from URL: {attachment.url}{Colors.RESET}")
                        image_bytes = await self.fetch_image_bytes(attachment.url)
                        img_hash = self.get_image_hash(image_bytes)
                        self.stats['images_processed'] += 1
                        print(f"{Colors.GREEN}[Success Channel] Image hash: {img_hash[:16]}...{Colors.RESET}")
                        
                        if self.is_duplicate_hash(img_hash):
                            self.stats['duplicates_rejected'] += 1
                            
                            # Delete the original message
                            try:
                                await message.delete()
                                print(f"{Colors.GREEN}[Success Channel] Deleted duplicate image message{Colors.RESET}")
                            except discord.Forbidden:
                                print(f"{Colors.RED}[Error] Missing permission to delete messages{Colors.RESET}")
                            except Exception as e:
                                print(f"{Colors.RED}[Error] Failed to delete message: {e}{Colors.RESET}")
                            
                            # Send temporary message that auto-deletes (ephemeral-like behavior)
                            try:
                                embed = discord.Embed(
                                    title=self.get_message("duplicate_image.title"),
                                    description=self.get_message("duplicate_image.description", member=message.author),
                                    color=discord.Color.orange(),
                                    timestamp=datetime.now(timezone.utc)
                                )
                                embed.set_footer(text=f"{self.get_message('duplicate_image.footer')} • Only you can see this")
                                temp_msg = await message.channel.send(content=message.author.mention, embed=embed)
                                # Auto-delete after 10 seconds
                                await asyncio.sleep(10)
                                try:
                                    await temp_msg.delete()
                                except:
                                    pass
                                print(f"{Colors.GREEN}[Success Channel] Duplicate image notification sent (auto-delete){Colors.RESET}")
                            except Exception as e:
                                print(f"{Colors.RED}[Error] Failed to send duplicate notification: {e}{Colors.RESET}")
                            
                            await self.log_action(
                                message.guild,
                                f"Duplicate image rejected for {message.author.mention}",
                                "duplicate_rejected",
                                member=message.author,
                                channel=message.channel
                            )
                            # Don't return early - continue to process commands
                            continue
                        else:
                            print(f"{Colors.GREEN}[Success Channel] New unique image detected, saving hash{Colors.RESET}")
                            self.save_image_hash(img_hash, message.author.id)
                            has_new_success = True
                    
                    except discord.Forbidden:
                        await self.log_action(
                            message.guild,
                            f"Missing permissions in channel {message.channel.mention}",
                            "error",
                            channel=message.channel
                        )
                        print(f"{Colors.RED}[Error] Missing permissions in channel {message.channel.id}{Colors.RESET}")
                    except Exception as e:
                        await self.log_action(
                            message.guild,
                            f"Error processing image: {str(e)}",
                            "error",
                            member=message.author,
                            channel=message.channel
                        )
                        print(f"{Colors.RED}[Error] Error processing image: {e}{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[Success Channel] Attachment is not an image (type: {attachment.content_type}){Colors.RESET}")
            
            if has_new_success:
                print(f"{Colors.GREEN}[Success Channel] Awarding point to {message.author} (ID: {message.author.id}){Colors.RESET}")
                self.award_point(message.author.id)
                total_points = self.get_user_points(message.author.id)
                print(f"{Colors.GREEN}[Success Channel] Point awarded! User now has {total_points} points{Colors.RESET}")
                
                # Log to points log channel
                await self.log_point_movement_to_channel(
                    message.guild,
                    message.author.id,
                    1,
                    "Image success posted"
                )
                
                # Add reaction
                reaction_emoji = self.config.get("reaction_emoji", "🔥")
                try:
                    await message.add_reaction(reaction_emoji)
                except Exception:
                    pass
                
                # Send award embed
                try:
                    embed = discord.Embed(
                        description=self.get_message(
                            "point_award.description",
                            member=message.author,
                            total_points=total_points
                        ),
                        color=self.get_embed_color()
                    )
                    await message.channel.send(embed=embed)
                    print(f"{Colors.GREEN}[Success Channel] Point award notification sent{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.RED}[Error] Failed to send point award notification: {e}{Colors.RESET}")
                
                await self.log_action(
                    message.guild,
                    f"Point awarded to {message.author.mention} (Total: {total_points})",
                    "point_awarded",
                    member=message.author,
                    channel=message.channel
                )
            else:
                print(f"{Colors.YELLOW}[Success Channel] No new success detected (has_new_success=False){Colors.RESET}")
            
            await self.bot.process_commands(message)
        
        @self.bot.event
        async def on_member_update(before: discord.Member, after: discord.Member):
            if before.guild.id != self.config.get("guild_id"):
                return
            
            role_id = self.config.get("role_id_to_watch")
            if not role_id:
                return
            
            had_role = discord.utils.get(before.roles, id=role_id)
            has_role = discord.utils.get(after.roles, id=role_id)
            
            if had_role and not has_role:
                # Role removed - reset points
                current_points = self.get_user_points(after.id)
                if current_points > 0:
                    self.log_point_movement(after.id, -current_points, "Role removed - points reset")
                    user_id_str = str(after.id)
                    if user_id_str in self.json_data["points"]:
                        self.json_data["points"][user_id_str]["points"] = 0
                        self.json_data["points"][user_id_str]["last_updated"] = datetime.now().isoformat()
                    self.save_json_data()
                    
                    # Log to points log channel
                    await self.log_point_movement_to_channel(
                        after.guild,
                        after.id,
                        -current_points,
                        "Role removed - points reset"
                    )
                    
                    try:
                        embed = discord.Embed(
                            title=self.get_message("role_removed_dm.title"),
                            description=self.get_message("role_removed_dm.description"),
                            color=discord.Color.orange(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.set_footer(text=self.get_message("role_removed_dm.footer"))
                        await after.send(embed=embed)
                    except discord.Forbidden:
                        print(f"{Colors.YELLOW}[Warning] Could not DM {after} about role removal.{Colors.RESET}")
                    
                    await self.log_action(
                        after.guild,
                        f"Points reset for {after.mention} (had {current_points} points) - role removed",
                        "points_reset",
                        member=after
                    )
    
    def _safe_channel_name(self, username: str) -> str:
        """Create safe channel name for redemption tickets"""
        base = f"pointsredeem-{username}".lower()
        return "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in base)[:95]

    def _whop_dashboard_link_for_member(self, discord_user_id: int) -> str:
        """Best-effort Whop Dashboard link using RSCheckerbot's cached link DB."""
        try:
            root = Path(__file__).resolve().parents[1]
            link_path = root / "RSCheckerbot" / "whop_discord_link.json"
            cfg_path = root / "RSCheckerbot" / "config.json"
            if not link_path.exists():
                return "—"
            db = json.loads(link_path.read_text(encoding="utf-8") or "{}")
            by = db.get("by_discord_id") if isinstance(db, dict) else None
            if not isinstance(by, dict):
                return "—"
            rec = by.get(str(int(discord_user_id)))  # normalize
            if not isinstance(rec, dict):
                return "—"
            # Prefer a cached full URL if available.
            cached_url = str(rec.get("dashboard_url") or "").strip()
            if cached_url.startswith("[Open]("):
                return cached_url

            user_id = str(rec.get("whop_user_id") or "").strip()
            if not user_id:
                return "—"

            company_id = ""
            if cfg_path.exists():
                cfg = json.loads(cfg_path.read_text(encoding="utf-8") or "{}")
                wa = cfg.get("whop_api") if isinstance(cfg, dict) else None
                if isinstance(wa, dict):
                    company_id = str(wa.get("company_id") or "").strip()
            if not company_id:
                return "—"

            url = f"https://whop.com/dashboard/{company_id}/users/{user_id}/"
            return f"[Open]({url})"
        except Exception:
            return "—"
    
    async def create_redemption_ticket(self, member: discord.Member, tier_name: str, points_required: int):
        """Create a redemption ticket channel"""
        guild = member.guild
        category_id = self.config.get("redemption_category_id")
        if not category_id:
            return None
        
        category = guild.get_channel(category_id)
        if not category:
            return None
        
        # Check if category is full (use overflow if needed)
        if hasattr(category, "channels") and len(category.channels) >= 50:
            overflow_id = self.config.get("redemption_overflow_category_id")
            if overflow_id:
                category = guild.get_channel(overflow_id)
                if not category:
                    return None
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_messages=True)
        }
        
        support_role_id = self.config.get("support_role_id")
        if support_role_id:
            support_role = guild.get_role(support_role_id)
            if support_role:
                overwrites[support_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        try:
            ticket = await guild.create_text_channel(
                name=self._safe_channel_name(member.name),
                overwrites=overwrites,
                category=category
            )
            return ticket
        except Exception as e:
            print(f"{Colors.RED}[Error] Failed to create redemption ticket: {e}{Colors.RESET}")
            return None
    
    class RedemptionView(discord.ui.View):
        """View for redemption tier selection"""
        def __init__(self, bot_instance, member: discord.Member):
            super().__init__(timeout=300)
            self.bot_instance = bot_instance
            self.member = member
            
            # Add buttons for each tier
            tiers = bot_instance.config.get("redemption_tiers", [])
            for tier in tiers[:25]:  # Discord limit
                button = discord.ui.Button(
                    label=f"{tier['name']} ({tier['points_required']} pts)",
                    style=discord.ButtonStyle.primary,
                    custom_id=f"redeem_{tier['points_required']}"
                )
                button.callback = self.make_tier_callback(tier)
                self.add_item(button)
        
        def make_tier_callback(self, tier):
            async def callback(interaction: discord.Interaction):
                await interaction.response.defer(ephemeral=True)
                
                user_points = self.bot_instance.get_user_points(self.member.id)
                points_required = tier['points_required']
                
                if user_points < points_required:
                    embed = discord.Embed(
                        title=self.bot_instance.get_message("redemption_insufficient_points.title"),
                        description=self.bot_instance.get_message(
                            "redemption_insufficient_points.description",
                            tier_name=tier['name'],
                            points_required=points_required,
                            current_points=user_points,
                            points_needed=points_required - user_points
                        ),
                        color=discord.Color.red(),
                        timestamp=datetime.now(timezone.utc)
                    )
                    embed.set_footer(text=self.bot_instance.get_message("redemption_insufficient_points.footer"))
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
                
                # Create ticket
                ticket = await self.bot_instance.create_redemption_ticket(
                    self.member,
                    tier['name'],
                    points_required
                )
                
                if not ticket:
                    await interaction.followup.send(
                        "❌ Failed to create redemption ticket. Please contact staff.",
                        ephemeral=True
                    )
                    return
                
                # Send message in ticket channel
                support_role_id = self.bot_instance.config.get("support_role_id")
                support_mention = f"<@&{support_role_id}>" if support_role_id else "@support"
                
                embed = discord.Embed(
                    title=self.bot_instance.get_message("redemption_ticket_channel_message.title"),
                    description=self.bot_instance.get_message(
                        "redemption_ticket_channel_message.description",
                        member=self.member,
                        tier_name=tier['name'],
                        points_required=points_required,
                        current_points=user_points
                    ),
                    color=self.bot_instance.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                try:
                    dash_link = self.bot_instance._whop_dashboard_link_for_member(self.member.id)
                    embed.description = (embed.description or "") + f"\n\n**Whop Dashboard:** {dash_link}"
                except Exception:
                    pass
                embed.set_footer(text=self.bot_instance.get_message("redemption_ticket_channel_message.footer"))
                await ticket.send(content=support_mention, embed=embed)
                
                # Send confirmation to user
                embed = discord.Embed(
                    title=self.bot_instance.get_message("redemption_ticket_created.title"),
                    description=self.bot_instance.get_message(
                        "redemption_ticket_created.description",
                        tier_name=tier['name'],
                        points_required=points_required,
                        current_points=user_points
                    ),
                    color=self.bot_instance.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text=self.bot_instance.get_message("redemption_ticket_created.footer"))
                await interaction.followup.send(embed=embed, ephemeral=True)
                
                await self.bot_instance.log_action(
                    interaction.guild,
                    f"Redemption ticket created for {self.member.mention} - {tier['name']} ({points_required} pts)",
                    "admin_action",
                    member=self.member,
                    channel=ticket
                )
            
            return callback
    
    def _setup_slash_commands(self):
        """Setup slash commands for members"""
        
        @self.bot.tree.command(name="rspoints", description="Check your current success points")
        async def points_slash(interaction: discord.Interaction):
            user_points = self.get_user_points(interaction.user.id)
            
            if user_points == 0:
                message_text = "Start sharing your successes to earn points!"
            elif user_points == 1:
                message_text = "You have 1 point! Keep it up! 🚀"
            else:
                message_text = f"Keep sharing your successes to earn more! 🎯"
            
            embed = discord.Embed(
                title=self.get_message("points_display.title"),
                description=self.get_message(
                    "points_display.description",
                    points=user_points,
                    message=message_text
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("points_display.footer"))
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        @self.bot.tree.command(name="rsleaderboard", description="View the top 10 members by success points")
        async def leaderboard_slash(interaction: discord.Interaction):
            # Get top 10 users from JSON
            sorted_users = sorted(
                self.json_data["points"].items(),
                key=lambda x: x[1].get("points", 0) if isinstance(x[1], dict) else x[1],
                reverse=True
            )[:10]
            top_users = [(int(uid), data.get("points", 0) if isinstance(data, dict) else data) for uid, data in sorted_users]
            
            if not top_users:
                embed = discord.Embed(
                    title=self.get_message("leaderboard.title"),
                    description=self.get_message("leaderboard.empty"),
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text=self.get_message("leaderboard.footer"))
            else:
                leaderboard_text = ""
                for rank, (user_id, points) in enumerate(top_users, start=1):
                    member = interaction.guild.get_member(user_id)
                    name = member.display_name if member else f"<@{user_id}>"
                    medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"**{rank}.**"
                    leaderboard_text += f"{medal} {name} — **{points}** point{'s' if points != 1 else ''}\n"
                
                embed = discord.Embed(
                    title=self.get_message("leaderboard.title"),
                    description=leaderboard_text,
                    color=discord.Color.gold(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text=self.get_message("leaderboard.footer"))
            
            await interaction.response.send_message(embed=embed, ephemeral=False)
        
        @self.bot.tree.command(name="rshelp", description="Learn how the success points system works")
        async def help_slash(interaction: discord.Interaction):
            reaction_emoji = self.config.get("reaction_emoji", "🤑")
            embed = discord.Embed(
                title=self.get_message("help_member.title"),
                description=self.get_message("help_member.description", reaction_emoji=reaction_emoji),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("help_member.footer"))
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        @self.bot.tree.command(name="rsredeeminfo", description="Learn about redeeming your success points")
        async def redeeminfo_slash(interaction: discord.Interaction):
            current_points = self.get_user_points(interaction.user.id)
            tiers = self.config.get("redemption_tiers", [])
            
            if not tiers:
                embed = discord.Embed(
                    title="No Redemption Tiers Available",
                    description="Redemption tiers have not been configured yet. Contact staff for more information.",
                    color=discord.Color.orange(),
                    timestamp=datetime.now(timezone.utc)
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            
            # Build tier list text with better clarity
            redeem_tiers_text = ""
            for tier in tiers:
                can_afford = "✅" if current_points >= tier['points_required'] else "❌"
                points_needed = tier['points_required'] - current_points
                
                if current_points >= tier['points_required']:
                    redeem_tiers_text += f"{can_afford} **{tier['name']}** - {tier['points_required']} points\n"
                else:
                    redeem_tiers_text += f"{can_afford} **{tier['name']}** - {tier['points_required']} points (Need {points_needed} more)\n"
                
                redeem_tiers_text += f"   {tier.get('description', 'No description')}\n\n"
            
            embed = discord.Embed(
                title=self.get_message("redemption_ticket_intro.title"),
                description=self.get_message(
                    "redemption_ticket_intro.description",
                    current_points=current_points,
                    redeem_tiers_text=redeem_tiers_text
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            # Set image from config if available
            image_url = self.config.get("redemption_info_image_url")
            if image_url:
                embed.set_image(url=image_url)
            embed.set_footer(text=self.get_message("redemption_ticket_intro.footer"))
            
            view = self.RedemptionView(self, interaction.user)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    def _setup_commands(self):
        """Setup prefix commands for admins"""
        
        @self.bot.command(name='addpoints')
        @commands.has_permissions(manage_messages=True)
        async def add_points(ctx: commands.Context, member: discord.Member, amount: int):
            """Add points to a member (Admin only)"""
            # Delete user's command message immediately
            try:
                await ctx.message.delete()
            except discord.Forbidden:
                print(f"{Colors.YELLOW}[Warning] Cannot delete message in channel {ctx.channel.id} - missing permissions{Colors.RESET}")
            except discord.NotFound:
                pass  # Message already deleted
            except Exception as e:
                print(f"{Colors.YELLOW}[Warning] Failed to delete message: {e}{Colors.RESET}")
            
            if amount <= 0:
                embed = discord.Embed(
                    title=self.get_message("invalid_amount.title"),
                    description=self.get_message("invalid_amount.description"),
                    color=discord.Color.red()
                )
                embed.set_footer(text=self.get_message("invalid_amount.footer"))
                await ctx.send(embed=embed, delete_after=10)
                return
            
            old_total = self.get_user_points(member.id)
            self.log_point_movement(member.id, amount, f"Admin added ({ctx.author.id})", ctx.author.id)
            user_id_str = str(member.id)
            if user_id_str in self.json_data["points"]:
                self.json_data["points"][user_id_str]["points"] += amount
                self.json_data["points"][user_id_str]["last_updated"] = datetime.now().isoformat()
            else:
                self.json_data["points"][user_id_str] = {
                    "points": amount,
                    "last_updated": datetime.now().isoformat()
                }
            self.save_json_data()
            new_total = self.get_user_points(member.id)
            
            # Log to points log channel
            await self.log_point_movement_to_channel(
                ctx.guild,
                member.id,
                amount,
                f"Admin added ({ctx.author.mention})",
                ctx.author.id
            )
            
            embed = discord.Embed(
                title=self.get_message("admin_points_added.title"),
                description=self.get_message(
                    "admin_points_added.description",
                    amount=amount,
                    member=member.mention,
                    new_total=new_total
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("admin_points_added.footer"))
            await ctx.send(embed=embed)
            
            await self.log_action(
                ctx.guild,
                f"{ctx.author.mention} added {amount} points to {member.mention} (now {new_total})",
                "admin_action",
                member=member
            )
        
        @self.bot.command(name='removepoints')
        @commands.has_permissions(manage_messages=True)
        async def remove_points(ctx: commands.Context, member: discord.Member, amount: int):
            """Remove points from a member (Admin only)"""
            # Delete user's command message immediately
            try:
                await ctx.message.delete()
            except discord.Forbidden:
                print(f"{Colors.YELLOW}[Warning] Cannot delete message in channel {ctx.channel.id} - missing permissions{Colors.RESET}")
            except discord.NotFound:
                pass  # Message already deleted
            except Exception as e:
                print(f"{Colors.YELLOW}[Warning] Failed to delete message: {e}{Colors.RESET}")
            
            if amount <= 0:
                embed = discord.Embed(
                    title=self.get_message("invalid_amount.title"),
                    description=self.get_message("invalid_amount.description"),
                    color=discord.Color.red()
                )
                embed.set_footer(text=self.get_message("invalid_amount.footer"))
                await ctx.send(embed=embed, delete_after=10)
                return
            
            old_total = self.get_user_points(member.id)
            if old_total == 0:
                embed = discord.Embed(
                    title="No Points",
                    description=f"{member.mention} has no points to remove.",
                    color=discord.Color.orange()
                )
                await ctx.send(embed=embed, delete_after=10)
                return
            
            change_amount = -min(amount, old_total)
            new_total = max(old_total - amount, 0)
            self.log_point_movement(member.id, change_amount, f"Admin removed ({ctx.author.id})", ctx.author.id)
            user_id_str = str(member.id)
            if user_id_str in self.json_data["points"]:
                self.json_data["points"][user_id_str]["points"] = new_total
                self.json_data["points"][user_id_str]["last_updated"] = datetime.now().isoformat()
            self.save_json_data()
            
            # Log to points log channel
            await self.log_point_movement_to_channel(
                ctx.guild,
                member.id,
                change_amount,
                f"Admin removed ({ctx.author.mention})",
                ctx.author.id
            )
            
            embed = discord.Embed(
                title=self.get_message("admin_points_removed.title"),
                description=self.get_message(
                    "admin_points_removed.description",
                    amount=abs(change_amount),
                    member=member.mention,
                    new_total=new_total
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("admin_points_removed.footer"))
            await ctx.send(embed=embed)
            
            await self.log_action(
                ctx.guild,
                f"{ctx.author.mention} removed {abs(change_amount)} points from {member.mention} (now {new_total})",
                "admin_action",
                member=member
            )
        
        @self.bot.command(name='checkpoints', aliases=['userpoints', 'points'])
        @commands.has_permissions(manage_messages=True)
        async def check_points(ctx: commands.Context, member: discord.Member):
            """Check points for a specific user (Admin only)"""
            # Delete user's command message immediately
            try:
                await ctx.message.delete()
            except discord.Forbidden:
                print(f"{Colors.YELLOW}[Warning] Cannot delete message in channel {ctx.channel.id} - missing permissions{Colors.RESET}")
            except discord.NotFound:
                pass  # Message already deleted
            except Exception as e:
                print(f"{Colors.YELLOW}[Warning] Failed to delete message: {e}{Colors.RESET}")
            
            user_points = self.get_user_points(member.id)
            
            embed = discord.Embed(
                title=self.get_message("admin_user_points.title"),
                description=self.get_message(
                    "admin_user_points.description",
                    member=member.mention,
                    user_id=member.id,
                    points=user_points
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("admin_user_points.footer"))
            embed.set_thumbnail(url=member.display_avatar.url if member.display_avatar else None)
            await ctx.send(embed=embed)
        
        @self.bot.command(name='setpoints')
        @commands.has_permissions(manage_messages=True)
        async def set_points(ctx: commands.Context, member: discord.Member, amount: int):
            """Set exact points for a member (Admin only)"""
            # Delete user's command message immediately
            try:
                await ctx.message.delete()
            except discord.Forbidden:
                print(f"{Colors.YELLOW}[Warning] Cannot delete message in channel {ctx.channel.id} - missing permissions{Colors.RESET}")
            except discord.NotFound:
                pass  # Message already deleted
            except Exception as e:
                print(f"{Colors.YELLOW}[Warning] Failed to delete message: {e}{Colors.RESET}")
            
            if amount < 0:
                embed = discord.Embed(
                    title=self.get_message("invalid_amount.title"),
                    description=self.get_message("invalid_amount.description"),
                    color=discord.Color.red()
                )
                embed.set_footer(text=self.get_message("invalid_amount.footer"))
                await ctx.send(embed=embed, delete_after=10)
                return
            
            old_total = self.get_user_points(member.id)
            change_amount = amount - old_total
            if change_amount != 0:
                self.log_point_movement(member.id, change_amount, f"Admin set ({ctx.author.id})", ctx.author.id)
            user_id_str = str(member.id)
            self.json_data["points"][user_id_str] = {
                "points": amount,
                "last_updated": datetime.now().isoformat()
            }
            self.save_json_data()
            
            # Log to points log channel
            if change_amount != 0:
                await self.log_point_movement_to_channel(
                    ctx.guild,
                    member.id,
                    change_amount,
                    f"Admin set ({ctx.author.mention})",
                    ctx.author.id
                )
            
            embed = discord.Embed(
                title=self.get_message("admin_points_set.title"),
                description=self.get_message(
                    "admin_points_set.description",
                    member=member.mention,
                    amount=amount,
                    old_total=old_total
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("admin_points_set.footer"))
            await ctx.send(embed=embed)
            
            await self.log_action(
                ctx.guild,
                f"{ctx.author.mention} set points for {member.mention} to {amount} (was {old_total})",
                "admin_action",
                member=member
            )
        
        @self.bot.command(name='status')
        @commands.has_permissions(manage_messages=True)
        async def bot_status(ctx: commands.Context):
            """Show bot status (Admin only)"""
            # Get JSON stats
            unique_users = len(self.json_data["points"])
            total_points = sum(
                data.get("points", 0) if isinstance(data, dict) else data
                for data in self.json_data["points"].values()
            )
            images_processed = len(self.json_data["image_hashes"])
            
            # Calculate uptime
            uptime_str = "Unknown"
            if self.stats['started_at']:
                uptime = datetime.now(timezone.utc) - self.stats['started_at']
                hours, remainder = divmod(int(uptime.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}h {minutes}m {seconds}s"
            
            json_status = "✅ Connected"
            try:
                # Verify JSON is accessible
                _ = len(self.json_data["points"])
            except Exception:
                json_status = "❌ Error"
            
            embed = discord.Embed(
                title=self.get_message("status.title"),
                description=self.get_message(
                    "status.description",
                    json_status=json_status,
                    total_points=total_points,
                    unique_users=unique_users,
                    images_processed=images_processed,
                    points_today=self.stats['points_awarded'],
                    errors=self.stats['errors'],
                    uptime=uptime_str
                ),
                color=self.get_embed_color(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=self.get_message("status.footer"))
            await ctx.send(embed=embed)
        
        @self.bot.command(name='reload')
        @commands.has_permissions(manage_messages=True)
        async def reload_config_cmd(ctx: commands.Context):
            """Reload config and messages from files (Admin only)"""
            errors = []
            
            # Reload config
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            except FileNotFoundError:
                errors.append(f"Config file not found: {self.config_path}")
            except json.JSONDecodeError as e:
                errors.append(f"Invalid JSON in config: {str(e)}")
            except Exception as e:
                errors.append(f"Error loading config: {str(e)}")
            
            # Reload messages
            messages_loaded = self.load_messages()
            if not messages_loaded:
                errors.append("Failed to reload messages (check console for details)")
            
            if errors:
                embed = discord.Embed(
                    title="⚠️ Reload Completed with Errors",
                    description="\n".join(f"• {error}" for error in errors),
                    color=discord.Color.orange()
                )
                await ctx.send(embed=embed, delete_after=10)
            else:
                embed = discord.Embed(
                    title="✅ Reloaded Successfully",
                    description="Configuration and messages reloaded successfully!",
                    color=discord.Color.green()
                )
                await ctx.send(embed=embed, delete_after=5)
        
        @self.bot.command(name='sync')
        @commands.has_permissions(manage_messages=True)
        async def sync_commands(ctx: commands.Context):
            """Manually sync slash commands (Admin only)"""
            guild_id = self.config.get("guild_id")
            if not guild_id:
                await ctx.send("❌ Guild ID not configured.", delete_after=10)
                return
            
            try:
                await ctx.send("🔄 Syncing slash commands...", delete_after=5)
                
                # Get all registered commands first
                all_commands = list(self.bot.tree.get_commands())
                print(f"{Colors.CYAN}[Sync] Found {len(all_commands)} registered command(s){Colors.RESET}")
                
                # Sync to guild
                synced = await self.bot.tree.sync(guild=discord.Object(id=guild_id))
                print(f"{Colors.GREEN}[Sync] Guild sync returned {len(synced)} command(s){Colors.RESET}")
                
                # Note: get_commands() is synchronous, not async
                # We can't easily check existing commands without making API calls
                # So we'll just show what we registered and synced
                existing_commands = []
                
                # Also sync globally (can take up to 1 hour to propagate)
                global_synced = []
                try:
                    global_synced = await self.bot.tree.sync()
                    print(f"{Colors.GREEN}[Sync] Global sync returned {len(global_synced)} command(s){Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.YELLOW}[Sync] Global sync warning: {e}{Colors.RESET}")
                
                # Determine what to show
                commands_to_show = synced if synced else (existing_commands if existing_commands else all_commands)
                sync_count = len(synced) if synced else len(existing_commands) if existing_commands else len(all_commands)
                
                embed = discord.Embed(
                    title="✅ Commands Synced",
                    description=f"**Registered:** {len(all_commands)} command(s)\n"
                               f"**Synced to guild:** {len(synced)} new/updated\n"
                               f"**Existing in guild:** {len(existing_commands)} command(s)\n\n"
                               f"Commands may take a few minutes to appear in Discord.",
                    color=discord.Color.green(),
                    timestamp=datetime.now(timezone.utc)
                )
                
                if commands_to_show:
                    cmd_list = "\n".join([f"• `/{cmd.name}`" for cmd in commands_to_show[:10]])
                    if len(commands_to_show) > 10:
                        cmd_list += f"\n... and {len(commands_to_show) - 10} more"
                    embed.add_field(name="Available Commands", value=cmd_list, inline=False)
                
                if not synced and not existing_commands:
                    embed.add_field(
                        name="⚠️ Note",
                        value="If commands don't appear, Discord may be rate-limited. Wait a few minutes and try again.",
                        inline=False
                    )
                
                embed.set_footer(text="Use /rspoints to test if commands are working")
                await ctx.send(embed=embed)
                
                print(f"{Colors.GREEN}[Commands] Manual sync completed: {len(synced)} command(s){Colors.RESET}")
                for cmd in synced:
                    print(f"{Colors.GREEN}   • /{cmd.name}{Colors.RESET}")
                    
            except Exception as e:
                error_msg = str(e)
                print(f"{Colors.RED}[Commands] Sync failed: {error_msg}{Colors.RESET}")
                import traceback
                traceback.print_exc()
                embed = discord.Embed(
                    title="❌ Sync Failed",
                    description=f"Failed to sync commands:\n```{error_msg}```",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed, delete_after=30)
        
        @self.bot.command(name='configinfo')
        @commands.has_permissions(manage_messages=True)
        async def config_info(ctx: commands.Context):
            """Show configuration information (Admin only)"""
            guild_id = self.config.get("guild_id")
            guild = ctx.guild
            
            embed = discord.Embed(
                title="📋 Configuration Information",
                description="Current bot configuration:",
                color=self.get_embed_color()
            )
            
            embed.add_field(
                name="🏠 Guild",
                value=f"**{guild.name}**\nID: `{guild_id}`",
                inline=False
            )
            
            success_channel_ids = self.config.get("success_channel_ids", [])
            channels_text = ""
            for ch_id in success_channel_ids[:5]:
                ch = guild.get_channel(ch_id)
                if ch:
                    channels_text += f"• {ch.mention} (ID: `{ch_id}`)\n"
                else:
                    channels_text += f"• ❌ Not found (ID: `{ch_id}`)\n"
            if len(success_channel_ids) > 5:
                channels_text += f"... and {len(success_channel_ids) - 5} more\n"
            embed.add_field(
                name="📢 Success Channels",
                value=channels_text or "None configured",
                inline=False
            )
            
            role_id = self.config.get("role_id_to_watch")
            if role_id:
                role = guild.get_role(role_id)
                if role:
                    embed.add_field(
                        name="👤 Watch Role",
                        value=f"**{role.name}**\nID: `{role_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="👤 Watch Role",
                        value=f"❌ Not found\nID: `{role_id}`",
                        inline=True
                    )
            
            embed.add_field(
                name="💾 Storage",
                value=f"`success_points.json`",
                inline=True
            )
            
            log_channel_id = self.config.get("log_channel_id")
            if log_channel_id:
                log_ch = guild.get_channel(log_channel_id)
                if log_ch:
                    embed.add_field(
                        name="📝 Log Channel",
                        value=f"{log_ch.mention}\nID: `{log_channel_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="📝 Log Channel",
                        value=f"❌ Not found\nID: `{log_channel_id}`",
                        inline=True
                    )
            else:
                embed.add_field(
                    name="📝 Log Channel",
                    value="Not configured",
                    inline=True
                )
            
            await ctx.send(embed=embed)

        @self.bot.command(name='postpointsguide')
        @commands.has_permissions(manage_messages=True)
        async def post_points_guide(ctx: commands.Context, channel: discord.TextChannel = None):
            """Post the points guide to a channel (Admin only)
            Usage: !postpointsguide [channel]
            If no channel is specified, posts to current channel.
            """
            target_channel = channel or ctx.channel
            
            # Build success channels list
            success_channel_ids = self.config.get("success_channel_ids", [])
            success_channels = ""
            for ch_id in success_channel_ids:
                ch = ctx.guild.get_channel(ch_id)
                if ch:
                    success_channels += f"• {ch.mention}\n"
            
            if not success_channels:
                success_channels = "• No success channels configured"
            
            # Build redemption tiers list
            tiers = self.config.get("redemption_tiers", [])
            guide_emojis = self.config.get("guide_emojis", {})
            moneyrain_emoji = guide_emojis.get("moneyrain", "💰")
            
            redemption_tiers_text = ""
            for tier in tiers:
                redemption_tiers_text += f"• **{tier['points_required']} points**  → {moneyrain_emoji} {tier['name']}\n"
            
            if not redemption_tiers_text:
                redemption_tiers_text = "• No redemption tiers configured yet."
            
            # Get reaction emoji
            reaction_emoji = self.config.get("reaction_emoji", "🤑")
            
            # Get emojis from config
            rocket_emoji = guide_emojis.get("rocket", "🚀")
            eye_emoji = guide_emojis.get("eye", "👁️")
            trophy_emoji = guide_emojis.get("trophy", "🏆")
            gun_emoji = guide_emojis.get("gun", "🔫")
            love_emoji = guide_emojis.get("love", "❤️")
            flag_emoji = guide_emojis.get("flag", "🏳️")
            
            # Get footer text from config
            footer_text = self.config.get("footer_text", "Reselling Secrets Staff")
            
            # Build and send embed guide as separate messages
            try:
                # Message 1: Title + Description
                embed1 = discord.Embed(
                    title=f"{rocket_emoji} Reselling Secrets — Success Points System",
                    description="Turn your real wins into real rewards.\n\nEvery verified success you share earns points — tracked automatically, reviewed fairly, and redeemable for free membership time.",
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed1)
                
                # Message 2: Where You Earn Points
                embed2 = discord.Embed(
                    title=f"{eye_emoji} Where You Earn Points",
                    description=f"Post a REAL success image in any official success channel:\n\n{success_channels.strip()}",
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed2)
                
                # Message 3: How Points Work
                points_work_text = (
                    f"• 1 valid success image = +1 Success Point\n"
                    f"• Bot reacts with {reaction_emoji} and confirms your updated total\n"
                    f"• Admins may adjust points when necessary"
                )
                embed3 = discord.Embed(
                    title=f"{trophy_emoji} How Points Work",
                    description=points_work_text,
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed3)
                
                # Message 4: Image Required
                image_required_text = (
                    "No image = no points.\n\n"
                    "• Messages without images are removed\n"
                    "• You'll receive an automatic reminder"
                )
                embed4 = discord.Embed(
                    title="📸 Image Required",
                    description=image_required_text,
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed4)
                
                # Message 5: Duplicates & Abuse
                abuse_text = (
                    "• Reposting the same image earns NO points\n"
                    "• Duplicate images are detected automatically\n"
                    "• Splitting one success into multiple posts is not allowed\n"
                    "• Abuse may result in point removal or a full reset"
                )
                embed5 = discord.Embed(
                    title=f"{gun_emoji} Duplicates & Abuse",
                    description=abuse_text,
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed5)
                
                # Message 6: Redemption Rewards
                embed6 = discord.Embed(
                    title=f"{love_emoji} Redemption Rewards",
                    description=f"Redeem using /rsredeeminfo\n\n{redemption_tiers_text.strip()}\n\n⚠ Points are NOT auto-deducted. All redemptions require staff approval.",
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed6)
                
                # Message 7: How to Redeem (Step-by-Step) with GIF
                redeem_steps_text = (
                    "1️⃣ Run /rsredeeminfo\n"
                    "2️⃣ View your points + rewards\n"
                    "3️⃣ Click a reward button\n"
                    "4️⃣ A ticket is created automatically\n"
                    "5️⃣ Staff reviews and applies your reward"
                )
                embed7 = discord.Embed(
                    title="▶️ How to Redeem (Step-by-Step)",
                    description=redeem_steps_text,
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                # Set GIF image for redemption guide
                image_url = self.config.get("redemption_info_image_url")
                if image_url:
                    embed7.set_image(url=image_url)
                await target_channel.send(embed=embed7)
                
                # Message 8: Member Slash Commands
                commands_text = (
                    "/rspoints        → Check your points (private)\n"
                    "/rsleaderboard   → Top 10 members\n"
                    "/rshelp          → Full system guide\n"
                    "/rsredeeminfo    → Redeem rewards"
                )
                embed8 = discord.Embed(
                    title=f"{flag_emoji} Member Slash Commands",
                    description=commands_text,
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                embed8.set_footer(text=f"{footer_text} • Earn. Share. Get Rewarded.")
                await target_channel.send(embed=embed8)
                
                # Message 9: Membership Role Notice
                embed9 = discord.Embed(
                    title=f"{gun_emoji} Membership Role Notice",
                    description="If your membership role is removed, your success points may reset to **0**.\nIf this happens by mistake, contact staff immediately.",
                    color=self.get_embed_color(),
                    timestamp=datetime.now(timezone.utc)
                )
                await target_channel.send(embed=embed9)
                
                embed = discord.Embed(
                    title="✅ Points Guide Posted",
                    description=f"Successfully posted points guide to {target_channel.mention}",
                    color=discord.Color.green(),
                    timestamp=datetime.now(timezone.utc)
                )
                await ctx.send(embed=embed, delete_after=10)
            except KeyError:
                await ctx.send("❌ Points guide message not found in messages.json. Please update messages.json on the server.", delete_after=10)
            except discord.Forbidden:
                await ctx.send(f"❌ Missing permissions to send messages in {target_channel.mention}", delete_after=10)
            except Exception as e:
                await ctx.send(f"❌ Failed to post guide: {str(e)}", delete_after=10)

        @self.bot.command(name="listsuccesschannels")
        @commands.has_permissions(manage_messages=True)
        async def list_success_channels(ctx: commands.Context):
            """List configured success channels (Admin only)"""
            # Reuse the same view as configinfo, but focused
            await ctx.invoke(self.bot.get_command("configinfo"))

        @self.bot.command(name="addsuccesschannel")
        @commands.has_permissions(manage_messages=True)
        async def add_success_channel(ctx: commands.Context, channel: discord.TextChannel):
            """Add a channel to success_channel_ids (Admin only)"""
            ids = self.config.get("success_channel_ids", [])
            if not isinstance(ids, list):
                ids = []
            if channel.id in ids:
                await ctx.send(f"ℹ️ {channel.mention} is already in success channels.")
                return
            ids.append(channel.id)
            self.config["success_channel_ids"] = ids
            self.save_config()
            await ctx.send(f"✅ Added {channel.mention} to success channels. Total: {len(ids)}")

        @self.bot.command(name="removesuccesschannel")
        @commands.has_permissions(manage_messages=True)
        async def remove_success_channel(ctx: commands.Context, channel: discord.TextChannel):
            """Remove a channel from success_channel_ids (Admin only)"""
            ids = self.config.get("success_channel_ids", [])
            if not isinstance(ids, list):
                ids = []
            if channel.id not in ids:
                await ctx.send(f"ℹ️ {channel.mention} is not currently in success channels.")
                return
            ids = [x for x in ids if x != channel.id]
            self.config["success_channel_ids"] = ids
            self.save_config()
            await ctx.send(f"✅ Removed {channel.mention} from success channels. Total: {len(ids)}")
        
        @self.bot.command(name='setredemptioncategory')
        @commands.has_permissions(manage_messages=True)
        async def set_redemption_category(ctx: commands.Context, category: discord.CategoryChannel):
            """Set the category for redemption tickets (Admin only)"""
            self.config["redemption_category_id"] = category.id
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            embed = discord.Embed(
                title="✅ Redemption Category Set",
                description=f"Redemption tickets will be created in **{category.name}**",
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
        
        @self.bot.command(name='setsupportrole')
        @commands.has_permissions(manage_messages=True)
        async def set_support_role(ctx: commands.Context, role: discord.Role):
            """Set the support role to ping in redemption tickets (Admin only)"""
            self.config["support_role_id"] = role.id
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            embed = discord.Embed(
                title="✅ Support Role Set",
                description=f"Support role set to **{role.name}**",
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
        
        @self.bot.command(name='edittiers', aliases=['tiereditor', 'tiers'])
        @commands.has_permissions(manage_messages=True)
        async def edit_tiers(ctx: commands.Context):
            """Open interactive tier editor (Admin only)"""
            try:
                from tier_editor import TierEditorView
            except ImportError as e:
                await ctx.send(f"❌ Failed to import tier editor: {e}", delete_after=10)
                return
            
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            view = TierEditorView(self)
            embed = view.get_main_embed()
            await ctx.send(embed=embed, view=view)
        
        @self.bot.command(name='editmessages', aliases=['messageeditor', 'messages'])
        @commands.has_permissions(manage_messages=True)
        async def edit_messages(ctx: commands.Context):
            """Open interactive message editor (Admin only)"""
            try:
                from message_editor import MessageEditorView
            except ImportError as e:
                await ctx.send(f"❌ Failed to import message editor: {e}", delete_after=10)
                return
            
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            view = MessageEditorView(self)
            embed = view.get_main_embed()
            await ctx.send(embed=embed, view=view)
        
        @self.bot.command(name='showredemptiontiers')
        @commands.has_permissions(manage_messages=True)
        async def show_redemption_tiers(ctx: commands.Context):
            """Show current redemption tiers (Admin only)"""
            tiers = self.config.get("redemption_tiers", [])
            if not tiers:
                embed = discord.Embed(
                    title="No Redemption Tiers",
                    description="No redemption tiers configured. Use `!addtier` to add tiers.",
                    color=discord.Color.orange()
                )
                await ctx.send(embed=embed)
                return
            
            tiers_text = ""
            for i, tier in enumerate(tiers, 1):
                tiers_text += f"**{i}. {tier['name']}**\n"
                tiers_text += f"   Points Required: {tier['points_required']}\n"
                tiers_text += f"   Description: {tier.get('description', 'No description')}\n\n"
            
            embed = discord.Embed(
                title="Current Redemption Tiers",
                description=tiers_text,
                color=self.get_embed_color()
            )
            embed.set_footer(text="Use !addtier, !removetier, or !edittier to modify tiers")
            await ctx.send(embed=embed)
        
        @self.bot.command(name='addtier')
        @commands.has_permissions(manage_messages=True)
        async def add_tier(ctx: commands.Context, name: str, points: int, *, description: str = "No description"):
            """Add a new redemption tier (Admin only)
            Usage: !addtier "Tier Name" 100 "Description here"
            """
            if points < 1:
                await ctx.send("❌ Points must be at least 1.", delete_after=10)
                return
            
            if not self.config.get("redemption_tiers"):
                self.config["redemption_tiers"] = []
            
            # Check if tier name already exists
            existing_names = [t.get("name", "").lower() for t in self.config["redemption_tiers"]]
            if name.lower() in existing_names:
                await ctx.send(f"❌ A tier with name '{name}' already exists.", delete_after=10)
                return
            
            # Add new tier
            new_tier = {
                "name": name,
                "points_required": points,
                "description": description
            }
            self.config["redemption_tiers"].append(new_tier)
            
            # Save to config file
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            
            embed = discord.Embed(
                title="✅ Tier Added",
                description=f"Successfully added redemption tier:\n\n"
                           f"**Name:** {name}\n"
                           f"**Points Required:** {points}\n"
                           f"**Description:** {description}",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=f"Added by {ctx.author.display_name}")
            await ctx.send(embed=embed)
            
            await self.log_action(
                ctx.guild,
                f"{ctx.author.mention} added redemption tier: {name} ({points} pts)",
                "admin_action",
                member=ctx.author
            )
        
        @self.bot.command(name='removetier')
        @commands.has_permissions(manage_messages=True)
        async def remove_tier(ctx: commands.Context, *, tier_name: str):
            """Remove a redemption tier by name (Admin only)
            Usage: !removetier "Tier Name" or !removetier Tier Name
            """
            tiers = self.config.get("redemption_tiers", [])
            if not tiers:
                await ctx.send("❌ No tiers configured.", delete_after=10)
                return
            
            # Find and remove tier (case-insensitive)
            removed = False
            for i, tier in enumerate(tiers):
                if tier.get("name", "").lower() == tier_name.lower():
                    removed_tier = tiers.pop(i)
                    removed = True
                    break
            
            if not removed:
                await ctx.send(f"❌ Tier '{tier_name}' not found.", delete_after=10)
                return
            
            # Save to config file
            self.config["redemption_tiers"] = tiers
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            
            embed = discord.Embed(
                title="✅ Tier Removed",
                description=f"Successfully removed redemption tier:\n\n"
                           f"**Name:** {removed_tier.get('name')}\n"
                           f"**Points Required:** {removed_tier.get('points_required')}",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=f"Removed by {ctx.author.display_name}")
            await ctx.send(embed=embed)
            
            await self.log_action(
                ctx.guild,
                f"{ctx.author.mention} removed redemption tier: {removed_tier.get('name')}",
                "admin_action",
                member=ctx.author
            )
        
        @self.bot.command(name='edittier')
        @commands.has_permissions(manage_messages=True)
        async def edit_tier(ctx: commands.Context, tier_name: str, field: str, *, new_value: str):
            """Edit a redemption tier (Admin only)
            Usage: !edittier "Tier Name" name "New Name"
                   !edittier "Tier Name" points 100
                   !edittier "Tier Name" description "New description"
            """
            tiers = self.config.get("redemption_tiers", [])
            if not tiers:
                await ctx.send("❌ No tiers configured.", delete_after=10)
                return
            
            # Find tier (case-insensitive)
            tier_index = None
            for i, tier in enumerate(tiers):
                if tier.get("name", "").lower() == tier_name.lower():
                    tier_index = i
                    break
            
            if tier_index is None:
                await ctx.send(f"❌ Tier '{tier_name}' not found.", delete_after=10)
                return
            
            tier = tiers[tier_index]
            field_lower = field.lower()
            old_value = None
            
            # Edit the field
            if field_lower == "name":
                old_value = tier["name"]
                # Check if new name already exists
                existing_names = [t.get("name", "").lower() for i, t in enumerate(tiers) if i != tier_index]
                if new_value.lower() in existing_names:
                    await ctx.send(f"❌ A tier with name '{new_value}' already exists.", delete_after=10)
                    return
                tier["name"] = new_value
            elif field_lower == "points":
                try:
                    points = int(new_value)
                    if points < 1:
                        await ctx.send("❌ Points must be at least 1.", delete_after=10)
                        return
                    old_value = tier["points_required"]
                    tier["points_required"] = points
                except ValueError:
                    await ctx.send("❌ Points must be a number.", delete_after=10)
                    return
            elif field_lower == "description":
                old_value = tier.get("description", "No description")
                tier["description"] = new_value
            else:
                await ctx.send("❌ Invalid field. Use: `name`, `points`, or `description`", delete_after=10)
                return
            
            # Save to config file
            self.config["redemption_tiers"] = tiers
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2)
            
            embed = discord.Embed(
                title="✅ Tier Updated",
                description=f"Successfully updated tier '{tier['name']}':\n\n"
                           f"**Field:** {field}\n"
                           f"**Old Value:** {old_value}\n"
                           f"**New Value:** {new_value}",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=f"Updated by {ctx.author.display_name}")
            await ctx.send(embed=embed)
            
            await self.log_action(
                ctx.guild,
                f"{ctx.author.mention} edited redemption tier '{tier['name']}': {field} = {new_value}",
                "admin_action",
                member=ctx.author
            )
        
        @self.bot.command(name='scanhistory')
        @commands.has_permissions(manage_messages=True)
        async def scan_history(ctx: commands.Context):
            """Scan message history to extract points from bot messages (Admin only)"""
            await ctx.send(f"🔍 Scanning message history... Bot ID: {self.bot.user.id}")
            
            points_count = {}
            total_messages = 0
            bot_messages_found = 0
            
            guild_id = self.config.get("guild_id")
            guild = self.bot.get_guild(guild_id)
            if not guild:
                await ctx.send("❌ Could not find the guild.")
                return
            
            success_channel_ids = self.config.get("success_channel_ids", [])
            
            for channel_id in success_channel_ids:
                channel = guild.get_channel(channel_id)
                if not channel or not hasattr(channel, 'history'):
                    continue
                
                await ctx.send(f"📂 Scanning channel <#{channel_id}>...")
                
                try:
                    async for message in channel.history(limit=None, oldest_first=True):
                        total_messages += 1
                        
                        if message.author.bot and message.embeds:
                            for embed in message.embeds:
                                desc = embed.description or ""
                                if "congratulations" in desc.lower() and "point" in desc.lower():
                                    bot_messages_found += 1
                        
                        if message.author.bot:
                            import re
                            text_to_check = ""
                            
                            if message.embeds:
                                for embed in message.embeds:
                                    text_to_check += (embed.description or "") + " "
                            
                            text_to_check += message.content or ""
                            
                            if "point" in text_to_check.lower() and ("awarded" in text_to_check.lower() or "congratulations" in text_to_check.lower() or "success" in text_to_check.lower()):
                                mention_matches = re.findall(r'<@!?(\d+)>', text_to_check)
                                for user_id_str in mention_matches:
                                    user_id = int(user_id_str)
                                    member = guild.get_member(user_id)
                                    username = member.display_name if member else f"User_{user_id}"
                                    if user_id not in points_count:
                                        points_count[user_id] = {"name": username, "points": 0}
                                    points_count[user_id]["points"] += 1
                                    if member:
                                        points_count[user_id]["name"] = username
                        
                        if total_messages % 500 == 0:
                            await ctx.send(f"⏳ Scanned {total_messages:,} messages... Found {len(points_count)} users with points so far...")
                            await asyncio.sleep(1)
                            
                except Exception as e:
                    await ctx.send(f"⚠️ Error scanning channel {channel_id}: {e}")
            
            # Merge scanned results with existing JSON data
            updates = 0
            additions = 0
            for user_id, scan_data in points_count.items():
                user_id_str = str(user_id)
                scanned_points = scan_data["points"]
                
                if user_id_str in self.json_data["points"]:
                    existing_entry = self.json_data["points"][user_id_str]
                    existing_points = existing_entry.get("points", 0) if isinstance(existing_entry, dict) else existing_entry
                    
                    if scanned_points > existing_points:
                        self.json_data["points"][user_id_str] = {
                            "points": scanned_points,
                            "last_updated": datetime.now().isoformat(),
                            "source": "history_scan"
                        }
                        updates += 1
                else:
                    self.json_data["points"][user_id_str] = {
                        "points": scanned_points,
                        "last_updated": datetime.now().isoformat(),
                        "source": "history_scan"
                    }
                    additions += 1
            
            # Save updated JSON
            self.save_json_data()
            
            # Generate history file
            history_file = self.base_path / "points_history.txt"
            with open(history_file, "w", encoding='utf-8') as f:
                f.write("=== POINTS HISTORY FROM MESSAGE SCAN ===\n\n")
                f.write(f"Total messages scanned: {total_messages}\n")
                f.write(f"Total users with points: {len(points_count)}\n\n")
                
                sorted_users = sorted(points_count.items(), key=lambda x: x[1]["points"], reverse=True)
                
                for rank, (user_id, data) in enumerate(sorted_users, start=1):
                    f.write(f"{rank}. {data['name']} (ID: {user_id}) - {data['points']} points\n")
            
            await ctx.send(f"✅ Scan complete! Found {len(points_count)} users with points from {total_messages} messages. (Bot award messages found: {bot_messages_found})")
            await ctx.send(f"📊 Updated JSON: {updates} users updated, {additions} new users added")
            await ctx.send("📄 Results saved to `points_history.txt` and `success_points.json`")
            
            if points_count:
                preview = "**Top 10 from history:**\n"
                sorted_users = sorted(points_count.items(), key=lambda x: x[1]["points"], reverse=True)
                for rank, (user_id, data) in enumerate(sorted_users[:10], start=1):
                    preview += f"{rank}. {data['name']} - {data['points']} points\n"
                await ctx.send(preview)
        
        @self.bot.command(name='importhistory')
        @commands.has_permissions(manage_messages=True)
        async def import_history(ctx: commands.Context):
            """Import points from the points_history.txt file into JSON (Admin only)"""
            await ctx.send("📥 Importing points from history file...")
            
            import re
            
            history_file = self.base_path / "points_history.txt"
            if not history_file.exists():
                await ctx.send("❌ No points_history.txt file found. Run `!scanhistory` first.")
                return
            
            imported = 0
            updated = 0
            errors = 0
            
            with open(history_file, "r", encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    match = re.match(r'\d+\. .+ \(ID: (\d+)\) - (\d+) points', line)
                    if match:
                        try:
                            user_id = int(match.group(1))
                            points = int(match.group(2))
                            
                            # Get current points
                            current_points = self.get_user_points(user_id)
                            
                            if current_points != points:
                                # Calculate change
                                change_amount = points - current_points
                                
                                # Log the movement
                                self.log_point_movement(user_id, change_amount, f"Imported from history (line {line_num})", ctx.author.id)
                                
                                # Update points
                                user_id_str = str(user_id)
                                self.json_data["points"][user_id_str] = {
                                    "points": points,
                                    "last_updated": datetime.now().isoformat()
                                }
                                self.save_json_data()
                                
                                if current_points == 0:
                                    imported += 1
                                    print(f"{Colors.GREEN}[Import] Imported {points} points for user {user_id}{Colors.RESET}")
                                else:
                                    updated += 1
                                    print(f"{Colors.YELLOW}[Import] Updated user {user_id}: {current_points} → {points} points{Colors.RESET}")
                            else:
                                print(f"{Colors.CYAN}[Import] User {user_id} already has {points} points, skipping{Colors.RESET}")
                        except Exception as e:
                            errors += 1
                            print(f"{Colors.RED}[Import] Error processing line {line_num}: {e}{Colors.RESET}")
            
            embed = discord.Embed(
                title="✅ Import Complete",
                description=f"Successfully processed points from history file!",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="New Users", value=f"{imported}", inline=True)
            embed.add_field(name="Updated Users", value=f"{updated}", inline=True)
            embed.add_field(name="Errors", value=f"{errors}", inline=True)
            embed.set_footer(text=f"Imported by {ctx.author.display_name}")
            await ctx.send(embed=embed)
            
            print(f"{Colors.GREEN}[Import] Import complete: {imported} new, {updated} updated, {errors} errors{Colors.RESET}")
        
        # Error handlers
        @add_points.error
        @remove_points.error
        @check_points.error
        @set_points.error
        @bot_status.error
        @reload_config_cmd.error
        @sync_commands.error
        @config_info.error
        @set_redemption_category.error
        @set_support_role.error
        @show_redemption_tiers.error
        @add_tier.error
        @remove_tier.error
        @edit_tier.error
        @edit_tiers.error
        @edit_messages.error
        @scan_history.error
        @import_history.error
        @post_points_guide.error
        async def admin_command_error(ctx: commands.Context, error: commands.CommandError):
            if isinstance(error, commands.MissingPermissions):
                embed = discord.Embed(
                    title=self.get_message("no_permission.title"),
                    description=self.get_message("no_permission.description"),
                    color=discord.Color.red()
                )
                embed.set_footer(text=self.get_message("no_permission.footer"))
                await ctx.send(embed=embed, delete_after=10)
            elif isinstance(error, commands.MemberNotFound):
                embed = discord.Embed(
                    title=self.get_message("member_not_found.title"),
                    description=self.get_message("member_not_found.description"),
                    color=discord.Color.red()
                )
                embed.set_footer(text=self.get_message("member_not_found.footer"))
                await ctx.send(embed=embed, delete_after=10)
            elif isinstance(error, commands.MissingRequiredArgument):
                await ctx.send(f"❌ Missing required argument: `{error.param.name}`", delete_after=10)
            else:
                await ctx.send(f"❌ An error occurred: {str(error)}", delete_after=10)
    
    def run(self):
        """Run the bot (only for standalone mode)"""
        if self._is_shared_bot:
            print(f"{Colors.RED}[Bot] ERROR: Cannot run in standalone mode when using shared bot instance{Colors.RESET}")
            return
        
        token = self.config.get("bot_token")
        if not token:
            print(f"{Colors.RED}[Bot] ERROR: bot_token not found in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        try:
            self.bot.run(token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Bot] Shutting down...{Colors.RESET}")
        finally:
            # Save JSON data on shutdown
            self.save_json_data()
            print(f"{Colors.GREEN}[JSON] Data saved on shutdown{Colors.RESET}")


def main():
    """Main entry point"""
    bot = RSSuccessBot()
    bot.run()


if __name__ == "__main__":
    main()

