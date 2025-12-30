#!/usr/bin/env python3
"""
RS Onboarding Bot
-----------------
Standalone bot for managing onboarding tickets with fully configurable messages.
Configuration is split across:
- config.json (non-secret settings)
- config.secrets.json (server-only secrets, not committed)
Messages are stored in messages.json.
"""

import os
import sys
import json
import time
import asyncio
from typing import Dict, Any, Optional
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import discord
from discord.ext import commands, tasks
from discord import ui

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class RSOnboardingBot:
    """Main bot class for onboarding tickets"""
    
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        self.messages_path = self.base_path / "messages.json"
        
        self.config: Dict[str, Any] = {}
        self.messages: Dict[str, Any] = {}
        
        self.ticket_data: Dict[str, Any] = {}
        self._close_locks = defaultdict(asyncio.Lock)
        self._open_locks = defaultdict(asyncio.Lock)
        self._recent_member_dm: Dict[int, float] = {}
        
        # Stats tracking (similar to RSForwarder)
        self.stats = {
            'tickets_created': 0,
            'tickets_closed': 0,
            'dms_sent': 0,
            'errors': 0,
            'started_at': None
        }
        
        self.load_config()
        self.load_messages()
        self.load_tickets()
        
        # Validate required config
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        # Setup bot
        intents = discord.Intents.none()
        intents.guilds = True
        intents.members = True
        intents.messages = True
        intents.reactions = True
        intents.message_content = True
        
        self.bot = commands.Bot(command_prefix="?", intents=intents)
        self._setup_events()
        self._setup_commands()
    
    def load_config(self):
        """Load configuration from config.json + config.secrets.json (server-only)."""
        if self.config_path.exists():
            try:
                self.config, _, secrets_path = load_config_with_secrets(self.base_path)
                if not secrets_path.exists():
                    print(f"{Colors.YELLOW}[Config] Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
                print(f"{Colors.GREEN}[Config] Loaded configuration{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}[Config] Failed to load config: {e}{Colors.RESET}")
                self.config = {}
    
    def load_messages(self):
        """Load messages from JSON file"""
        if self.messages_path.exists():
            try:
                with open(self.messages_path, 'r', encoding='utf-8') as f:
                    self.messages = json.load(f)
                print(f"{Colors.GREEN}[Messages] Loaded messages{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}[Messages] Failed to load messages: {e}{Colors.RESET}")
                self.messages = {}
    
    def save_config(self):
        """Save configuration to JSON file"""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                # Never write secrets back into config.json
                config_to_save = dict(self.config or {})
                config_to_save.pop("bot_token", None)
                json.dump(config_to_save, f, indent=2, ensure_ascii=False)
            print(f"{Colors.GREEN}[Config] Saved configuration{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to save config: {e}{Colors.RESET}")
    
    def save_messages(self):
        """Save messages to JSON file"""
        try:
            with open(self.messages_path, 'w', encoding='utf-8') as f:
                json.dump(self.messages, f, indent=2, ensure_ascii=False)
            print(f"{Colors.GREEN}[Messages] Saved messages{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Messages] Failed to save messages: {e}{Colors.RESET}")
    
    def load_tickets(self):
        """Load ticket data from file (for active tickets only) with corruption handling"""
        tickets_file = self.base_path / self.config.get("tickets_file", "tickets.json")
        if tickets_file.exists():
            try:
                with open(tickets_file, 'r', encoding='utf-8') as f:
                    self.ticket_data = json.load(f)
                print(f"{Colors.GREEN}[Tickets] Loaded {len(self.ticket_data)} active tickets{Colors.RESET}")
            except json.JSONDecodeError as e:
                print(f"{Colors.RED}[Tickets] JSON corruption detected in {tickets_file}: {e}{Colors.RESET}")
                print(f"{Colors.YELLOW}[Tickets] Attempting to load backup or falling back to empty state{Colors.RESET}")
                # Try to load backup if it exists
                backup_file = tickets_file.with_suffix('.json.bak')
                if backup_file.exists():
                    try:
                        with open(backup_file, 'r', encoding='utf-8') as f:
                            self.ticket_data = json.load(f)
                        print(f"{Colors.GREEN}[Tickets] Restored from backup{Colors.RESET}")
                    except Exception:
                        self.ticket_data = {}
                else:
                    self.ticket_data = {}
            except Exception as e:
                print(f"{Colors.RED}[Tickets] Failed to load tickets: {repr(e)}{Colors.RESET}")
                self.ticket_data = {}
        else:
            self.ticket_data = {}
    
    def save_tickets(self):
        """Save ticket data to file (active tickets only) with atomic writes"""
        tickets_file = self.base_path / self.config.get("tickets_file", "tickets.json")
        temp_file = tickets_file.with_suffix('.json.tmp')
        backup_file = tickets_file.with_suffix('.json.bak')
        
        try:
            os.makedirs(tickets_file.parent, exist_ok=True)
            
            # Write to temp file first
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.ticket_data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk (Ubuntu-safe)
            
            # Create backup of existing file if it exists
            if tickets_file.exists():
                import shutil
                shutil.copy2(tickets_file, backup_file)
            
            # Atomic rename (Ubuntu: rename is atomic on same filesystem)
            os.replace(temp_file, tickets_file)
            
        except Exception as e:
            print(f"{Colors.RED}[Tickets] Failed to save tickets: {repr(e)}{Colors.RESET}")
            # Clean up temp file on error
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass
    
    def _migrate_ticket_schema_if_needed(self):
        """Migrate old schema {user_id: channel_id} -> {user_id: {channel_id, opened_at}} (match original)"""
        changed = False
        for uid, v in list(self.ticket_data.items()):
            if isinstance(v, int):
                # Old format: {user_id: channel_id}
                self.ticket_data[uid] = {
                    "channel_id": v, 
                    "opened_at": time.time()
                }
                changed = True
            elif isinstance(v, dict):
                # Remove extra fields not in original (status, last_activity, creating_at)
                if "status" in v or "last_activity" in v or "creating_at" in v:
                    # Keep only channel_id and opened_at (match original)
                    channel_id = v.get("channel_id", 0)
                    opened_at = v.get("opened_at", time.time())
                    # Remove entries with channel_id=0 (stuck "creating" entries)
                    if channel_id == 0:
                        self.ticket_data.pop(uid, None)
                        changed = True
                    else:
                        self.ticket_data[uid] = {
                            "channel_id": channel_id,
                            "opened_at": opened_at
                        }
                    changed = True
        if changed:
            self.save_tickets()
    
    def get_embed_color(self) -> discord.Color:
        """Get embed color from config"""
        color = self.config.get("embed_color", {})
        return discord.Color.from_rgb(
            color.get("r", 169),
            color.get("g", 199),
            color.get("b", 220)
        )
    
    def get_step_embed(self, step: int, member: discord.Member) -> discord.Embed:
        """Get embed for onboarding step"""
        steps = self.messages.get("steps", [])
        if step >= len(steps):
            step = len(steps) - 1
        
        step_data = steps[step]
        
        # Generate progress indicator
        progress_emojis = self.config.get("progress_emojis", {})
        completed = progress_emojis.get("completed", "✅")
        current = progress_emojis.get("current", "🔥")
        pending = progress_emojis.get("pending", "⬜")
        
        progress = "".join([
            f"{completed} " if i < step else
            f"{current} " if i == step else
            f"{pending} "
            for i in range(len(steps))
        ])
        
        # Format description with placeholders
        description = step_data.get("description", "").format(
            progress=progress,
            member=member
        )
        
        embed = discord.Embed(
            title=step_data.get("title", ""),
            description=description,
            color=self.get_embed_color()
        )
        
        image_url = step_data.get("image_url")
        if image_url:
            embed.set_image(url=image_url)
        
        return embed
    
    async def send_member_granted_dm(self, member: discord.Member, source: str = "role_update"):
        """Send DM when Member role is granted"""
        guild = member.guild
        
        # Check for duplicate DMs
        ttl = self.config.get("recent_dm_ttl_seconds", 300)
        now = time.time()
        last = self._recent_member_dm.get(member.id, 0)
        if now - last < ttl:
            await self.log_action(
                guild, 
                f"Skipped duplicate Member DM to {member.mention} (recent)",
                log_type="info",
                member=member,
                source=source
            )
            return
        
        self._recent_member_dm[member.id] = now
        
        try:
            dm_data = self.messages.get("dms", {}).get("member_granted", {})
            description = dm_data.get("description", "").format(member=member)
            footer_text = dm_data.get("footer_text", self.config.get("footer_text", ""))
            
            embed = discord.Embed(
                description=description,
                color=self.get_embed_color()
            )
            
            banner_url = self.config.get("banner_url", "")
            if banner_url:
                embed.set_image(url=banner_url)
            
            if footer_text:
                embed.set_footer(text=footer_text)
            
            await member.send(embed=embed)
            await self.log_action(
                guild, 
                f"Sent Member welcome DM to {member.mention}",
                log_type="dm_sent",
                member=member,
                source=source
            )
        except Exception as e:
            await self.log_error(
                guild, 
                f"Failed to send Member welcome DM to {member} ({member.id}): {e}",
                context=f"send_member_granted_dm - {source}"
            )
    
    async def log_action(self, guild: discord.Guild, message: str, log_type: str = "info", 
                        member: discord.Member = None, ticket_channel: discord.TextChannel = None,
                        source: str = None):
        """Log action to log channel with embed (similar to RSForwarder style)"""
        log_channel_id = self.config.get("log_channel_id")
        if not log_channel_id:
            return
        
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel:
            return
        
        try:
            from datetime import datetime, timezone
            
            # Determine embed color and title based on log type
            if log_type == "ticket_created":
                color = discord.Color.green()
                title = "✅ Ticket Created"
                self.stats['tickets_created'] += 1
            elif log_type == "ticket_closed":
                color = discord.Color.blue()
                title = "🔒 Ticket Closed"
                self.stats['tickets_closed'] += 1
            elif log_type == "dm_sent":
                color = discord.Color.green()
                title = "📩 DM Sent"
                self.stats['dms_sent'] += 1
            elif log_type == "info":
                color = discord.Color.blue()
                title = "ℹ️ Info"
            elif log_type == "warning":
                color = discord.Color.orange()
                title = "⚠️ Warning"
            else:
                color = discord.Color.blue()
                title = "📋 Action"
            
            embed = discord.Embed(
                title=title,
                description=message,
                color=color,
                timestamp=datetime.now(timezone.utc)
            )
            
            # Add member info if available
            if member:
                embed.add_field(
                    name="Member",
                    value=f"{member.mention}\nID: `{member.id}`",
                    inline=True
                )
            
            # Add ticket channel info if available
            if ticket_channel:
                embed.add_field(
                    name="Ticket Channel",
                    value=f"{ticket_channel.mention}\nID: `{ticket_channel.id}`",
                    inline=True
                )
                # Add jump link (defensive check - ticket_channel should exist but be safe)
                if hasattr(ticket_channel, 'jump_url') and ticket_channel.jump_url:
                    embed.add_field(
                        name="Channel Link",
                        value=f"[Jump to Channel]({ticket_channel.jump_url})",
                        inline=True
                    )
            
            # Add source if provided
            if source:
                embed.add_field(
                    name="Source",
                    value=f"`{source}`",
                    inline=False
                )
            
            # Add stats footer (similar to RSForwarder, but clearly labeled as Onboarding Bot)
            embed.set_footer(
                text=f"🎫 RS Onboarding Bot | Tickets: {self.stats['tickets_created']} created, {self.stats['tickets_closed']} closed | DMs: {self.stats['dms_sent']} | Errors: {self.stats['errors']}"
            )
            
            await log_channel.send(embed=embed)
        except Exception as e:
            # Fallback to plain text if embed fails
            try:
                await log_channel.send(f"📋 {message}")
            except Exception as fallback_error:
                # Log to console if both embed and fallback fail (critical path)
                print(f"{Colors.RED}[Log] Failed to send log message to channel {log_channel_id}: {repr(e)} (fallback also failed: {repr(fallback_error)}){Colors.RESET}")
    
    async def log_error(self, guild: discord.Guild, error: str, context: str = None):
        """Log error to log channel with embed (similar to RSForwarder style)"""
        if "10003" in error and "Unknown Channel" in error:
            await self.log_action(guild, "Skipped noisy 10003 (channel already deleted).", "info")
            return
        
        log_channel_id = self.config.get("log_channel_id")
        alert_user_id = self.config.get("alert_user_id")
        
        if not log_channel_id:
            return
        
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel:
            return
        
        try:
            from datetime import datetime, timezone
            
            self.stats['errors'] += 1
            
            embed = discord.Embed(
                title="❌ Error Occurred",
                description=f"An error occurred in RS Onboarding Bot",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc)
            )
            
            # Add error details
            embed.add_field(
                name="Error",
                value=f"```{error[:1024]}```",
                inline=False
            )
            
            # Add context if provided
            if context:
                embed.add_field(
                    name="Context",
                    value=f"`{context}`",
                    inline=False
                )
            
            # Alert user mention
            if alert_user_id:
                embed.add_field(
                    name="Alert",
                    value=f"<@{alert_user_id}>",
                    inline=False
                )
            
            # Add stats footer (clearly labeled as Onboarding Bot)
            embed.set_footer(
                text=f"🎫 RS Onboarding Bot | Tickets: {self.stats['tickets_created']} created, {self.stats['tickets_closed']} closed | DMs: {self.stats['dms_sent']} | Errors: {self.stats['errors']}"
            )
            
            await log_channel.send(embed=embed)
        except Exception as e:
            # Fallback to plain text if embed fails
            try:
                alert = f"<@{alert_user_id}> " if alert_user_id else ""
                await log_channel.send(f"{alert}⚠️ An error occurred:\n```{error}```")
            except Exception:
                pass
    
    def _bot_has_core_perms(self, guild: discord.Guild) -> bool:
        """Check if bot has core permissions"""
        me = guild.me
        if not me:
            return False
        perms = me.guild_permissions
        return perms.manage_roles and perms.manage_channels
    
    async def _assert_core_perms_or_log(self, guild: discord.Guild, context: str):
        """Assert bot has core permissions or log error"""
        if not self._bot_has_core_perms(guild):
            await self.log_error(
                guild, 
                f"Bot lacks Manage Roles and/or Manage Channels. Fix role positions/category perms.",
                context=context
            )
    
    async def validate_config(self, guild: discord.Guild) -> bool:
        """Validate bot configuration and permissions on startup. Returns True if valid."""
        errors = []
        warnings = []
        
        me = guild.me
        if not me:
            errors.append("Bot member object not available")
            return False
        
        # Check core permissions
        perms = me.guild_permissions
        if not perms.manage_roles:
            errors.append("Bot lacks 'Manage Roles' permission")
        if not perms.manage_channels:
            errors.append("Bot lacks 'Manage Channels' permission")
        
        # Check ticket category exists and bot can create channels
        ticket_category_id = self.config.get("ticket_category_id")
        if ticket_category_id:
            category = guild.get_channel(ticket_category_id)
            if not category:
                errors.append(f"Ticket category not found (ID: {ticket_category_id})")
            elif not isinstance(category, discord.CategoryChannel):
                errors.append(f"Ticket category ID points to non-category channel (ID: {ticket_category_id})")
            else:
                # Check bot can create channels in this category
                overwrite = category.overwrites_for(me)
                if overwrite.create_instant_invite is False or (overwrite.manage_channels is False and not perms.administrator):
                    warnings.append(f"Bot may not be able to create channels in ticket category '{category.name}' (check category permissions)")
        else:
            warnings.append("No ticket_category_id configured")
        
        # Check overflow category if configured
        overflow_category_id = self.config.get("overflow_category_id")
        if overflow_category_id:
            overflow = guild.get_channel(overflow_category_id)
            if not overflow:
                warnings.append(f"Overflow category not found (ID: {overflow_category_id})")
            elif not isinstance(overflow, discord.CategoryChannel):
                warnings.append(f"Overflow category ID points to non-category channel (ID: {overflow_category_id})")
        
        # Check log channel exists and bot can send messages
        log_channel_id = self.config.get("log_channel_id")
        if log_channel_id:
            log_channel = guild.get_channel(log_channel_id)
            if not log_channel:
                warnings.append(f"Log channel not found (ID: {log_channel_id})")
            elif isinstance(log_channel, discord.TextChannel):
                overwrite = log_channel.overwrites_for(me)
                if overwrite.send_messages is False and not perms.administrator:
                    warnings.append(f"Bot may not be able to send messages in log channel '{log_channel.name}' (check channel permissions)")
        
        # Check role hierarchy (bot role must be above Welcome/Member roles)
        welcome_role_id = self.config.get("welcome_role_id")
        member_role_id = self.config.get("member_role_id")
        
        if welcome_role_id:
            welcome_role = guild.get_role(welcome_role_id)
            if not welcome_role:
                errors.append(f"Welcome role not found (ID: {welcome_role_id})")
            elif me.top_role <= welcome_role:
                errors.append(f"Bot role '{me.top_role.name}' must be above Welcome role '{welcome_role.name}' (role hierarchy)")
        
        if member_role_id:
            member_role = guild.get_role(member_role_id)
            if not member_role:
                errors.append(f"Member role not found (ID: {member_role_id})")
            elif me.top_role <= member_role:
                errors.append(f"Bot role '{me.top_role.name}' must be above Member role '{member_role.name}' (role hierarchy)")
        
        # Check required intents (bot relies on members and message_content)
        intents = self.bot.intents
        if not intents.members:
            warnings.append("Members intent is disabled - bot may not receive member updates correctly")
        if not intents.message_content:
            warnings.append("Message content intent is disabled - bot may not process messages correctly")
        
        # Print validation results
        if errors:
            print(f"{Colors.RED}[Config Validation] ERRORS:{Colors.RESET}")
            for error in errors:
                print(f"  {Colors.RED}❌ {error}{Colors.RESET}")
            await self.log_error(guild, f"Config validation failed:\n" + "\n".join(f"- {e}" for e in errors), context="validate_config")
        
        if warnings:
            print(f"{Colors.YELLOW}[Config Validation] WARNINGS:{Colors.RESET}")
            for warning in warnings:
                print(f"  {Colors.YELLOW}⚠️  {warning}{Colors.RESET}")
        
        if not errors and not warnings:
            print(f"{Colors.GREEN}[Config Validation] ✅ All checks passed{Colors.RESET}")
        
        return len(errors) == 0
    
    async def remove_cleanup_roles(self, member: discord.Member, reason: str = "Cleanup"):
        """Remove cleanup roles any time user gains Welcome or Member"""
        guild = member.guild
        cleanup_role_ids = self.config.get("cleanup_role_ids", [])
        roles_to_remove = []
        for rid in cleanup_role_ids:
            role = guild.get_role(rid)
            if role and role in member.roles:
                roles_to_remove.append(role)
        if roles_to_remove:
            try:
                await member.remove_roles(*roles_to_remove, reason=reason)
                await self.log_action(
                    guild,
                    f"Removed cleanup roles: {', '.join([r.name for r in roles_to_remove])}",
                    log_type="info",
                    member=member,
                    source=reason
                )
            except Exception as e:
                await self.log_error(
                    guild, 
                    f"Failed to remove roles from {member} ({member.id}): {e}",
                    context=f"remove_cleanup_roles - {reason}"
                )
    
    async def grant_member_and_close(self, member: discord.Member,
                                     channel: Optional[discord.TextChannel],
                                     source: str):
        """Grant member role, remove welcome, close ticket channel, and clean storage"""
        async with self._close_locks[member.id]:
            guild = member.guild
            await self._assert_core_perms_or_log(guild, f"grant_member_and_close for {member.id}")
            try:
                welcome_role_id = self.config.get("welcome_role_id")
                member_role_id = self.config.get("member_role_id")
                
                welcome = guild.get_role(welcome_role_id) if welcome_role_id else None
                member_role = guild.get_role(member_role_id) if member_role_id else None

                # Ensure roles
                if member_role and member_role not in member.roles:
                    try:
                        await member.add_roles(member_role, reason=f"Auto-close ({source})")
                    except Exception as e:
                        await self.log_error(
                        guild, 
                        f"Could not add Member to {member} ({member.id}): {e}",
                        context=f"grant_member_and_close - {source}"
                    )

                # Send the Member DM
                await self.send_member_granted_dm(member, source=f"{source}/grant_member_and_close")

                if welcome and welcome in member.roles:
                    try:
                        await member.remove_roles(welcome, reason=f"Auto-close ({source})")
                    except Exception as e:
                        await self.log_error(
                            guild, 
                            f"Could not remove Welcome from {member} ({member.id}): {e}",
                            context=f"grant_member_and_close - {source}"
                        )

                # Prefer the passed-in channel (button/cleanup paths), otherwise resolve from storage.
                ch = channel
                if ch is None:
                    data = self.ticket_data.get(str(member.id))
                    if data:
                        ch = await self.safe_get_channel(guild, data.get("channel_id"))

                # Best-effort message + delete if channel exists
                if ch:
                    try:
                        auto_close_msg = self.messages.get("auto_close_message", "⏰ 24 hours passed. Access granted automatically.")
                        try:
                            await ch.send(auto_close_msg)
                        except discord.NotFound:
                            ch = None
                        if ch and ch.permissions_for(guild.me).manage_channels:
                            try:
                                await ch.delete()
                            except discord.NotFound:
                                pass
                    except Exception as e:
                        if "10003" not in str(e):
                            await self.log_error(
                                guild, 
                                f"Could not delete ticket channel {getattr(ch, 'id', 'unknown')}: {e}",
                                context=f"grant_member_and_close - {source}"
                            )

                # Clean storage regardless of channel existence (match original)
                removed = self.ticket_data.pop(str(member.id), None)
                if removed is not None:
                    self.save_tickets()

                    print(f"{Colors.GREEN}[Success] Ticket closed for {member.name}{Colors.RESET}")
                    await self.log_action(
                        guild,
                        f"Auto-closed onboarding ticket",
                        log_type="ticket_closed",
                        member=member,
                        source=source
                    )

            except Exception as e:
                await self.log_error(
                    guild, 
                    f"Error in grant_member_and_close: {e}",
                    context=f"grant_member_and_close - {source}"
                )
    
    async def safe_get_channel(self, guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.abc.GuildChannel]:
        """Safely get channel, handling NotFound errors"""
        if not channel_id:
            return None
        ch = guild.get_channel(channel_id)
        if ch is not None:
            return ch
        try:
            return await guild.fetch_channel(channel_id)
        except discord.NotFound:
            return None
        except Exception:
            return None
    
    async def schedule_auto_close(self, member_id: int, guild: discord.Guild):
        """Schedule (or immediately perform) auto-close based on opened_at timestamp"""
        data = self.ticket_data.get(str(member_id))
        if not data:
            return

        auto_close_seconds = self.config.get("auto_close_seconds", 86400)
        opened_at = float(data.get("opened_at", time.time()))
        remaining = max(0, auto_close_seconds - (time.time() - opened_at))

        # Resolve member
        member = guild.get_member(member_id)
        if member is None:
            try:
                member = await guild.fetch_member(member_id)
            except discord.NotFound:
                # Member truly doesn't exist - safe to clean up
                print(f"{Colors.YELLOW}[Auto-Close] Member {member_id} not found in guild, cleaning up storage{Colors.RESET}")
                self.ticket_data.pop(str(member_id), None)
                self.save_tickets()
                return
            except Exception as e:
                # Transient API failure - don't delete, will retry on next restart
                print(f"{Colors.YELLOW}[Auto-Close] Failed to fetch member {member_id} (transient error): {repr(e)} - will retry later{Colors.RESET}")
                return

        # If they already have Member, close immediately
        member_role_id = self.config.get("member_role_id")
        if member_role_id and any(r.id == member_role_id for r in member.roles):
            await self.grant_member_and_close(member, None, source="already_member_on_resume")
            return

        async def _wait_and_close():
            await asyncio.sleep(remaining)
            m = guild.get_member(member_id) or member
            await self.grant_member_and_close(m, None, source="timer")

        asyncio.create_task(_wait_and_close())
    
    def _safe_channel_name(self, username: str) -> str:
        """Create safe channel name"""
        prefix = self.config.get("ticket_channel_name_prefix", "🔥welcome-")
        base = f"{prefix}{username}".lower()
        # Discord channel names: allow letters, numbers, '-', '_', and emoji
        return "".join(ch if ch.isalnum() or ch in "-_🔥" else "-" for ch in base)[:95]
    
    async def open_onboarding_ticket(self, member: discord.Member):
        """Open onboarding ticket for member - unified lock prevents race conditions"""
        guild = member.guild
        
        # Single lock block for entire flow (prevents race conditions)
        async with self._open_locks[member.id]:
            # Check if ticket already exists in storage
            if str(member.id) in self.ticket_data:
                ticket_data = self.ticket_data.get(str(member.id), {})
                channel_id = ticket_data.get("channel_id")
                # Verify channel still exists
                if channel_id:
                    existing_channel = guild.get_channel(channel_id)
                    if existing_channel:
                        print(f"{Colors.YELLOW}[Ticket] Skipped - ticket already exists for {member.name} ({member.id}){Colors.RESET}")
                        return
                    else:
                        # Channel was deleted but data remains - clean it up
                        self.ticket_data.pop(str(member.id), None)
                        self.save_tickets()
            
            # Check if a channel with this name pattern already exists
            expected_channel_name = self._safe_channel_name(member.name)
            ticket_category_id = self.config.get("ticket_category_id")
            overflow_category_id = self.config.get("overflow_category_id")
            
            # Check both categories for existing channels
            for cat_id in [ticket_category_id, overflow_category_id]:
                if not cat_id:
                    continue
                category = guild.get_channel(cat_id)
                if category and hasattr(category, "channels"):
                    for channel in category.channels:
                        if isinstance(channel, discord.TextChannel) and channel.name == expected_channel_name:
                            # Channel with same name exists - store it to prevent duplicates
                            self.ticket_data[str(member.id)] = {"channel_id": channel.id, "opened_at": time.time()}
                            self.save_tickets()
                            print(f"{Colors.YELLOW}[Ticket] Skipped - channel {channel.name} already exists for {member.name} ({member.id}){Colors.RESET}")
                            return
            
            # Mark as creating immediately to prevent race conditions (still in lock)
            self.ticket_data[str(member.id)] = {"channel_id": 0, "opened_at": time.time()}
            self.save_tickets()
            
            try:
                await self._assert_core_perms_or_log(guild, f"open_onboarding_ticket for {member.id}")

                ticket_category_id = self.config.get("ticket_category_id")
                overflow_category_id = self.config.get("overflow_category_id")

                category = guild.get_channel(ticket_category_id) if ticket_category_id else None
                if category and hasattr(category, "channels") and len(category.channels) >= 50:
                    overflow = guild.get_channel(overflow_category_id) if overflow_category_id else None
                    if overflow:
                        category = overflow

                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(read_messages=False),
                    member: discord.PermissionOverwrite(read_messages=True, send_messages=True),
                }
                if guild.me:
                    overwrites[guild.me] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

                welcome_ping_user_id = self.config.get("welcome_ping_user_id")
                if welcome_ping_user_id:
                    ping_member = guild.get_member(welcome_ping_user_id)
                    if ping_member:
                        overwrites[ping_member] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

                # Create ticket channel
                ticket = await guild.create_text_channel(
                    name=self._safe_channel_name(member.name),
                    overwrites=overwrites,
                    category=category if category else None,
                )

                # Remove cleanup roles immediately
                await self.remove_cleanup_roles(member, reason="Onboarding cleanup")

                # Send onboarding embed (stepper view)
                view = OnboardingView(self, member)
                embed = self.get_step_embed(0, member)
                msg = await ticket.send(content=member.mention, embed=embed, view=view)
                view.message = msg

                # Send persistent access controls
                await ticket.send(view=PersistentAccessView(self))

                # Update with actual channel ID (was set to 0 earlier to prevent race condition)
                self.ticket_data[str(member.id)] = {"channel_id": ticket.id, "opened_at": time.time()}
                self.save_tickets()

                # Log and notify staff (enhanced embed format)
                welcome_log_channel_id = self.config.get("welcome_log_channel_id")
                welcome_ping_user_id = self.config.get("welcome_ping_user_id")
                if welcome_log_channel_id:
                    welcome_log_channel = guild.get_channel(welcome_log_channel_id)
                    if welcome_log_channel:
                        try:
                            from datetime import datetime, timezone

                            embed = discord.Embed(
                                title="🎫 New Welcome Ticket Opened",
                                description=f"New onboarding ticket created for {member.mention}",
                                color=discord.Color.green(),
                                timestamp=datetime.now(timezone.utc)
                            )

                            embed.add_field(
                                name="Member",
                                value=f"{member.mention}\nID: `{member.id}`",
                                inline=True
                            )
                            embed.add_field(
                                name="Ticket Channel",
                                value=f"{ticket.mention}\nID: `{ticket.id}`",
                                inline=True
                            )
                            embed.add_field(
                                name="Channel Link",
                                value=f"[Jump to Ticket]({ticket.jump_url})",
                                inline=True
                            )
                            if welcome_ping_user_id:
                                embed.add_field(
                                    name="Notification",
                                    value=f"<@{welcome_ping_user_id}>",
                                    inline=False
                                )
                            embed.set_footer(
                                text=f"🎫 RS Onboarding Bot | Tickets: {self.stats['tickets_created']} created, {self.stats['tickets_closed']} closed"
                            )
                            await welcome_log_channel.send(embed=embed)
                        except Exception:
                            # Fallback to plain text if embed fails
                            try:
                                log_msg = self.config.get(
                                    "welcome_log_message",
                                    "<@{welcome_ping_user_id}> New welcome ticket opened for {member.mention}: {ticket.jump_url}"
                                )
                                formatted_msg = log_msg.format(
                                    welcome_ping_user_id=welcome_ping_user_id,
                                    member=member,
                                    ticket=ticket
                                )
                                await welcome_log_channel.send(formatted_msg)
                            except Exception:
                                pass

                await self.log_action(
                    guild,
                    f"Created onboarding ticket",
                    log_type="ticket_created",
                    member=member,
                    ticket_channel=ticket,
                    source="open_onboarding_ticket"
                )
                print(f"{Colors.GREEN}[Success] ✅ Ticket creation complete for {member.name}{Colors.RESET}")

                # Auto-close scheduler
                asyncio.create_task(self.schedule_auto_close(member.id, guild))

                # DM the user (on ticket open)
                try:
                    staff_user_id = self.config.get("staff_user_id")
                    ticket_open_dm = self.messages.get("dms", {}).get("ticket_open", {})

                    dm_title = ticket_open_dm.get("title", "📩 Message Sent from Staff")
                    dm_desc = ticket_open_dm.get("description", "").format(
                        staff_user_id=staff_user_id,
                        member=member,
                        ticket=ticket
                    )
                    dm_footer = ticket_open_dm.get("footer_text", self.config.get("footer_text", ""))

                    dm = discord.Embed(
                        title=dm_title,
                        description=dm_desc,
                        color=self.get_embed_color(),
                    )
                    if dm_footer:
                        dm.set_footer(text=dm_footer)
                    banner_url = self.config.get("banner_url", "")
                    if banner_url:
                        dm.set_image(url=banner_url)
                    await member.send(embed=dm)
                except Exception:
                    await self.log_error(
                        guild,
                        f"Failed to DM {member.name}",
                        context="open_onboarding_ticket - DM send"
                    )

            except discord.Forbidden as e:
                # Remove ticket data so retry can happen
                self.ticket_data.pop(str(member.id), None)
                self.save_tickets()
                error_details = str(e)
                if "50013" in error_details or "Missing Permissions" in error_details:
                    error_msg = (
                        f"❌ **PERMISSION DENIED** - Cannot create ticket channel\n\n"
                        f"**Error:** `{error_details}`\n\n"
                        f"**Possible causes:**\n"
                        f"• Bot lacks 'Manage Channels' permission\n"
                        f"• Bot's role is below the category in role hierarchy\n"
                        f"• Category has permission restrictions\n"
                        f"• Bot cannot modify channel permissions"
                    )
                elif "50035" in error_details or "Invalid Form Body" in error_details:
                    error_msg = (
                        f"❌ **INVALID REQUEST** - Channel creation failed\n\n"
                        f"**Error:** `{error_details}`\n\n"
                        f"**Possible causes:**\n"
                        f"• Channel name is invalid\n"
                        f"• Category is full (50 channels max)\n"
                        f"• Invalid permission overwrites"
                    )
                else:
                    error_msg = f"❌ **FORBIDDEN ERROR** - `{error_details}`"

                await self.log_error(
                    guild,
                    error_msg,
                    context=f"open_onboarding_ticket - Permission Error for {member.mention} (ID: {member.id})"
                )
                print(f"{Colors.RED}[ERROR] Failed to create ticket for {member.name} ({member.id}): {error_details}{Colors.RESET}")

            except discord.HTTPException as e:
                # HTTP/API error
                status = getattr(e, "status", None)
                error_details = str(e)
                # Remove ticket data so retry can happen
                self.ticket_data.pop(str(member.id), None)
                self.save_tickets()
                error_msg = (
                    f"❌ **HTTP ERROR** - Discord API request failed\n\n"
                    f"**Error:** `{error_details}`\n\n"
                    f"**Status Code:** `{status if status is not None else 'Unknown'}`\n"
                    f"**Response:** `{getattr(e, 'response', 'Unknown')}`"
                )
                await self.log_error(
                    guild,
                    error_msg,
                    context=f"open_onboarding_ticket - HTTP Error for {member.mention} (ID: {member.id})"
                )
                print(f"{Colors.RED}[ERROR] HTTP error creating ticket for {member.name} ({member.id}): {error_details}{Colors.RESET}")

            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                error_msg = (
                    f"❌ **UNEXPECTED ERROR** - Ticket creation failed\n\n"
                    f"**Error Type:** `{type(e).__name__}`\n"
                    f"**Error Message:** `{str(e)}`\n\n"
                    f"**Full Traceback:**\n```python\n{error_trace[:1500]}```"
                )
                await self.log_error(
                    guild,
                    error_msg,
                    context=f"open_onboarding_ticket - Unexpected Error for {member.mention} (ID: {member.id})"
                )
                print(f"{Colors.RED}[ERROR] Unexpected error creating ticket for {member.name} ({member.id}):{Colors.RESET}")
                print(f"{Colors.RED}{error_trace}{Colors.RESET}")
    
    async def reconcile_tickets(self, guild: discord.Guild):
        """Covers offline gaps: anyone with Welcome but not Member and no open ticket gets one"""
        welcome_role_id = self.config.get("welcome_role_id")
        member_role_id = self.config.get("member_role_id")
        
        if not (welcome_role_id and member_role_id):
            return
        
        welcome = guild.get_role(welcome_role_id)
        member = guild.get_role(member_role_id)
        if not (welcome and member):
            return
        
        for m in guild.members:
            rids = {r.id for r in m.roles}
            # Only create tickets for members with Welcome role, no Member role, and who joined less than 24h ago
            if (welcome_role_id in rids) and (member_role_id not in rids) and (str(m.id) not in self.ticket_data):
                # Check if member joined more than 24 hours ago
                if m.joined_at:
                    member_age_hours = (time.time() - m.joined_at.timestamp()) / 3600
                    if member_age_hours > 24:
                        print(f"{Colors.YELLOW}[Reconcile] Skipped {m.name} - joined {member_age_hours:.1f}h ago (should have completed onboarding){Colors.RESET}")
                        continue
                await self.open_onboarding_ticket(m)
    
    async def cleanup_stale_tickets(self, guild: discord.Guild):
        """Find and delete stale/orphaned ticket channels that should be closed"""
        print(f"{Colors.CYAN}[Cleanup] Starting stale ticket cleanup...{Colors.RESET}")
        
        welcome_role_id = self.config.get("welcome_role_id")
        member_role_id = self.config.get("member_role_id")
        ticket_category_id = self.config.get("ticket_category_id")
        overflow_category_id = self.config.get("overflow_category_id")
        
        if not (welcome_role_id and member_role_id):
            print(f"{Colors.YELLOW}[Cleanup] Skipped - missing welcome_role_id or member_role_id in config{Colors.RESET}")
            return
        
        welcome_role = guild.get_role(welcome_role_id)
        member_role = guild.get_role(member_role_id)
        if not (welcome_role and member_role):
            print(f"{Colors.YELLOW}[Cleanup] Skipped - welcome or member role not found in guild{Colors.RESET}")
            return
        
        cleaned_count = 0
        orphaned_count = 0
        total_tickets = len(self.ticket_data)
        
        if total_tickets == 0:
            print(f"{Colors.CYAN}[Cleanup] No tickets in storage to check{Colors.RESET}")
        else:
            print(f"{Colors.CYAN}[Cleanup] Checking {total_tickets} ticket(s) in storage...{Colors.RESET}")
        
        # Check all tickets in storage
        checked_count = 0
        skipped_creating = 0
        for user_id_str, ticket_info in list(self.ticket_data.items()):
            try:
                user_id = int(user_id_str)
                channel_id = ticket_info.get("channel_id", 0)
                
                # Skip if channel_id is 0 (creating state)
                if channel_id == 0:
                    skipped_creating += 1
                    continue
                
                checked_count += 1
                member = guild.get_member(user_id)
                channel = guild.get_channel(channel_id) if channel_id else None
                
                # Case 1: Channel doesn't exist anymore - clean up storage
                if not channel:
                    self.ticket_data.pop(user_id_str, None)
                    orphaned_count += 1
                    print(f"{Colors.YELLOW}[Cleanup] Removed orphaned ticket data for user {user_id_str} (channel deleted){Colors.RESET}")
                    continue
                
                # Case 2: User has Member role - ticket should be closed
                if member and member_role in member.roles:
                    try:
                        await self.grant_member_and_close(member, channel, source="cleanup_stale")
                        cleaned_count += 1
                        print(f"{Colors.GREEN}[Cleanup] Closed stale ticket for {member.name} (has Member role){Colors.RESET}")
                    except Exception as e:
                        print(f"{Colors.RED}[Cleanup] Error closing ticket for {member.name}: {e}{Colors.RESET}")
                    continue
                
                # Case 3: User no longer has Welcome role - ticket should be closed
                if member and welcome_role not in member.roles:
                    try:
                        # User lost Welcome role - close ticket
                        if channel and channel.permissions_for(guild.me).manage_channels:
                            await channel.delete()
                        self.ticket_data.pop(user_id_str, None)
                        self.save_tickets()
                        cleaned_count += 1
                        print(f"{Colors.GREEN}[Cleanup] Closed ticket for {member.name} (no Welcome role){Colors.RESET}")
                    except Exception as e:
                        print(f"{Colors.RED}[Cleanup] Error closing ticket for {member.name}: {e}{Colors.RESET}")
                    continue
                
                # Case 3.5: Member joined more than 24 hours ago - should have completed onboarding
                if member and member.joined_at:
                    try:
                        member_age_hours = (time.time() - member.joined_at.timestamp()) / 3600
                        if member_age_hours > 24:
                            # Member joined more than 24 hours ago - close ticket (should have completed onboarding)
                            try:
                                await self.grant_member_and_close(member, channel, source="cleanup_stale_joined_24h")
                                cleaned_count += 1
                                print(f"{Colors.GREEN}[Cleanup] Closed stale ticket for {member.name} (joined {member_age_hours:.1f}h ago, should have completed onboarding){Colors.RESET}")
                            except Exception as e:
                                print(f"{Colors.RED}[Cleanup] Error closing stale ticket for {member.name}: {e}{Colors.RESET}")
                            continue
                    except Exception as e:
                        # If we can't check join date, continue with other checks
                        pass
                
                # Case 4: Ticket is older than auto_close_seconds - should be auto-closed
                auto_close_seconds = self.config.get("auto_close_seconds", 86400)  # Default 24 hours
                opened_at = float(ticket_info.get("opened_at", time.time()))
                age_seconds = time.time() - opened_at
                
                if age_seconds >= auto_close_seconds:
                    # Ticket is past auto-close time - close it
                    if member:
                        try:
                            await self.grant_member_and_close(member, channel, source="cleanup_stale_24h")
                            cleaned_count += 1
                            age_hours = age_seconds / 3600
                            print(f"{Colors.GREEN}[Cleanup] Closed stale ticket for {member.name} (opened {age_hours:.1f} hours ago, past {auto_close_seconds/3600:.0f}h limit){Colors.RESET}")
                        except Exception as e:
                            print(f"{Colors.RED}[Cleanup] Error closing stale ticket for {member.name}: {e}{Colors.RESET}")
                    else:
                        # Member not found - just clean up storage
                        if channel and channel.permissions_for(guild.me).manage_channels:
                            try:
                                await channel.delete()
                            except Exception:
                                pass
                        self.ticket_data.pop(user_id_str, None)
                        self.save_tickets()
                        orphaned_count += 1
                        age_hours = age_seconds / 3600
                        print(f"{Colors.YELLOW}[Cleanup] Removed orphaned ticket for user {user_id_str} (opened {age_hours:.1f} hours ago, member not found){Colors.RESET}")
                    continue
                
                # Ticket is valid - user has Welcome role, doesn't have Member role, channel exists, not past 24h
                # This is expected state, no action needed
                
            except (ValueError, KeyError) as e:
                # Invalid user_id or missing data - clean up
                self.ticket_data.pop(user_id_str, None)
                orphaned_count += 1
                print(f"{Colors.YELLOW}[Cleanup] Removed invalid ticket data: {user_id_str}{Colors.RESET}")
        
        if checked_count > 0 and cleaned_count == 0 and orphaned_count == 0:
            print(f"{Colors.CYAN}[Cleanup] Checked {checked_count} ticket(s) in storage - all valid (users have Welcome role, no Member role yet, not past 24h){Colors.RESET}")
        if skipped_creating > 0:
            print(f"{Colors.YELLOW}[Cleanup] Skipped {skipped_creating} ticket(s) in 'creating' state (channel_id: 0){Colors.RESET}")
        
        # Case 4: Find orphaned channels in ticket categories (not in tickets.json)
        ticket_categories = []
        channels_checked = 0
        
        if ticket_category_id:
            cat = guild.get_channel(ticket_category_id)
            if cat:
                ticket_categories.append(cat)
                print(f"{Colors.CYAN}[Cleanup] Checking category: {cat.name} ({len(cat.channels) if hasattr(cat, 'channels') else 0} channels){Colors.RESET}")
        if overflow_category_id:
            cat = guild.get_channel(overflow_category_id)
            if cat:
                ticket_categories.append(cat)
                print(f"{Colors.CYAN}[Cleanup] Checking overflow category: {cat.name} ({len(cat.channels) if hasattr(cat, 'channels') else 0} channels){Colors.RESET}")
        
        for category in ticket_categories:
            if not hasattr(category, "channels"):
                continue
            
            for channel in category.channels:
                if not isinstance(channel, discord.TextChannel):
                    continue
                
                channels_checked += 1
                
                # Check if this channel matches ticket naming pattern
                # Ticket channels are named like the user's name (sanitized)
                # Check if channel is in tickets.json
                found_in_storage = False
                for user_id_str, ticket_info in self.ticket_data.items():
                    if ticket_info.get("channel_id") == channel.id:
                        found_in_storage = True
                        break
                
                if not found_in_storage:
                    # Orphaned channel - check if it's a ticket channel
                    # Try to find member by channel name
                    channel_name = channel.name
                    matched_member = None
                    for member in guild.members:
                        expected_name = self._safe_channel_name(member.name)
                        if channel_name == expected_name:
                            matched_member = member
                            break
                    
                    if matched_member:
                        # Found matching member - check if they should have a ticket
                        has_welcome = welcome_role in matched_member.roles
                        has_member = member_role in matched_member.roles
                        
                        # If they have Member role, delete orphaned channel
                        if has_member:
                            try:
                                if channel.permissions_for(guild.me).manage_channels:
                                    await channel.delete()
                                    cleaned_count += 1
                                    print(f"{Colors.GREEN}[Cleanup] Deleted orphaned ticket channel {channel.name} (user {matched_member.name} has Member role){Colors.RESET}")
                            except Exception as e:
                                print(f"{Colors.RED}[Cleanup] Error deleting orphaned channel {channel.name}: {e}{Colors.RESET}")
                        elif has_welcome:
                            # Channel matches member name, member has Welcome but no Member
                            # Check if member joined more than 24 hours ago - if so, this is likely stale
                            try:
                                member_joined_at = matched_member.joined_at
                                if member_joined_at:
                                    member_age_hours = (time.time() - member_joined_at.timestamp()) / 3600
                                    if member_age_hours > 24:
                                        # Member joined more than 24 hours ago but still has Welcome role and orphaned channel
                                        # This is likely stale - delete the channel
                                        if channel.permissions_for(guild.me).manage_channels:
                                            await channel.delete()
                                            cleaned_count += 1
                                            print(f"{Colors.GREEN}[Cleanup] Deleted stale orphaned channel {channel.name} (user {matched_member.name} joined {member_age_hours:.1f}h ago, should have completed onboarding){Colors.RESET}")
                                        else:
                                            print(f"{Colors.YELLOW}[Cleanup] Found stale orphaned channel {channel.name} (user {matched_member.name} joined {member_age_hours:.1f}h ago, but no delete permission){Colors.RESET}")
                                        continue
                            except Exception as e:
                                # If we can't check join date, continue with reconciliation logic
                                pass
                            
                            # Member joined recently (< 24h) or join date unavailable - reconcile it
                            try:
                                # Check if this user already has a ticket in storage (different channel)
                                existing_ticket = self.ticket_data.get(str(matched_member.id))
                                if existing_ticket:
                                    existing_channel_id = existing_ticket.get("channel_id", 0)
                                    if existing_channel_id and existing_channel_id != channel.id:
                                        # User has a ticket in storage but this is a different channel - delete this duplicate
                                        if channel.permissions_for(guild.me).manage_channels:
                                            await channel.delete()
                                            cleaned_count += 1
                                            print(f"{Colors.GREEN}[Cleanup] Deleted duplicate ticket channel {channel.name} (user {matched_member.name} already has ticket in storage){Colors.RESET}")
                                    else:
                                        # Same channel or invalid - update storage
                                        self.ticket_data[str(matched_member.id)] = {
                                            "channel_id": channel.id,
                                            "opened_at": existing_ticket.get("opened_at", time.time())
                                        }
                                        self.save_tickets()
                                        orphaned_count += 1
                                        print(f"{Colors.GREEN}[Cleanup] Reconciled orphaned channel {channel.name} for {matched_member.name} (updated tickets.json){Colors.RESET}")
                                else:
                                    # No ticket in storage - add this channel to tickets.json
                                    self.ticket_data[str(matched_member.id)] = {
                                        "channel_id": channel.id,
                                        "opened_at": time.time()  # Use current time as fallback
                                    }
                                    self.save_tickets()
                                    orphaned_count += 1
                                    print(f"{Colors.GREEN}[Cleanup] Reconciled orphaned channel {channel.name} for {matched_member.name} (added to tickets.json){Colors.RESET}")
                            except discord.NotFound:
                                # Channel was already deleted - skip
                                pass
                            except Exception as e:
                                if "10003" not in str(e):  # Ignore Unknown Channel errors
                                    print(f"{Colors.RED}[Cleanup] Error reconciling orphaned channel {channel.name}: {e}{Colors.RESET}")
                        else:
                            # Member doesn't have Welcome role - shouldn't have a ticket channel
                            try:
                                if channel.permissions_for(guild.me).manage_channels:
                                    await channel.delete()
                                    cleaned_count += 1
                                    print(f"{Colors.GREEN}[Cleanup] Deleted orphaned ticket channel {channel.name} (user {matched_member.name} doesn't have Welcome role){Colors.RESET}")
                            except Exception as e:
                                print(f"{Colors.RED}[Cleanup] Error deleting orphaned channel {channel.name}: {e}{Colors.RESET}")
                    else:
                        # Channel doesn't match any member's expected ticket name
                        # Could be an old ticket or manually created channel
                        # Check channel creation date - if it's old, delete it
                        try:
                            channel_age = (time.time() - channel.created_at.timestamp()) / 3600  # hours
                            if channel_age > 24:
                                # Channel is older than 24 hours and doesn't match any member - likely orphaned
                                if channel.permissions_for(guild.me).manage_channels:
                                    await channel.delete()
                                    cleaned_count += 1
                                    print(f"{Colors.GREEN}[Cleanup] Deleted orphaned channel {channel.name} (doesn't match any member, {channel_age:.1f}h old){Colors.RESET}")
                                else:
                                    print(f"{Colors.YELLOW}[Cleanup] Found orphaned channel {channel.name} (doesn't match any member, {channel_age:.1f}h old, but no delete permission){Colors.RESET}")
                            else:
                                print(f"{Colors.YELLOW}[Cleanup] Found orphaned channel {channel.name} (doesn't match any member's ticket name pattern, {channel_age:.1f}h old){Colors.RESET}")
                        except Exception as e:
                            print(f"{Colors.YELLOW}[Cleanup] Found orphaned channel {channel.name} (doesn't match any member's ticket name pattern, error checking age: {e}){Colors.RESET}")
        
        if channels_checked > 0:
            print(f"{Colors.CYAN}[Cleanup] Checked {channels_checked} channel(s) in ticket categories{Colors.RESET}")
        
        if cleaned_count > 0 or orphaned_count > 0:
            self.save_tickets()
            print(f"{Colors.CYAN}[Cleanup] ✅ Cleanup complete: {cleaned_count} stale tickets closed, {orphaned_count} orphaned entries removed{Colors.RESET}")
            await self.log_action(
                guild,
                f"Cleanup: {cleaned_count} stale tickets closed, {orphaned_count} orphaned entries removed",
                log_type="cleanup"
            )
        else:
            print(f"{Colors.GREEN}[Cleanup] ✅ No stale tickets found - everything is clean!{Colors.RESET}")
    
    @tasks.loop(hours=24)
    async def periodic_cleanup(self):
        """Periodic cleanup task - runs every 24 hours"""
        guild_id = self.config.get("guild_id")
        if guild_id:
            guild = self.bot.get_guild(guild_id)
            if guild:
                await self.cleanup_stale_tickets(guild)
    
    def _setup_events(self):
        """Setup Discord event handlers"""
        
        @self.bot.event
        async def on_ready():
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  🎫 RS Onboarding Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            
            self._migrate_ticket_schema_if_needed()
            
            guild_id = self.config.get("guild_id")
            guild = None
            if guild_id:
                guild = self.bot.get_guild(guild_id)
                if guild:
                    print(f"{Colors.GREEN}[Bot] Connected to: {guild.name}{Colors.RESET}")
                    await self._assert_core_perms_or_log(guild, "on_ready")
                    # Validate configuration
                    await self.validate_config(guild)
            
            # Display config information
            print(f"\n{Colors.CYAN}[Config] Configuration Information:{Colors.RESET}")
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            
            if guild:
                print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET} {Colors.BOLD}{guild.name}{Colors.RESET} (ID: {guild_id})")
                
                # Roles
                welcome_role_id = self.config.get("welcome_role_id")
                member_role_id = self.config.get("member_role_id")
                if welcome_role_id:
                    welcome_role = guild.get_role(welcome_role_id)
                    if welcome_role:
                        print(f"{Colors.GREEN}👋 Welcome Role:{Colors.RESET} {Colors.BOLD}{welcome_role.name}{Colors.RESET} (ID: {welcome_role_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Welcome Role:{Colors.RESET} Not found (ID: {welcome_role_id})")
                if member_role_id:
                    member_role = guild.get_role(member_role_id)
                    if member_role:
                        print(f"{Colors.GREEN}✅ Member Role:{Colors.RESET} {Colors.BOLD}{member_role.name}{Colors.RESET} (ID: {member_role_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Member Role:{Colors.RESET} Not found (ID: {member_role_id})")
                
                cleanup_role_ids = self.config.get("cleanup_role_ids", [])
                if cleanup_role_ids:
                    print(f"{Colors.GREEN}🧹 Cleanup Roles:{Colors.RESET} {len(cleanup_role_ids)} role(s)")
                    for role_id in cleanup_role_ids[:3]:  # Show first 3
                        role = guild.get_role(role_id)
                        if role:
                            print(f"   • {Colors.BOLD}{role.name}{Colors.RESET} (ID: {role_id})")
                        else:
                            print(f"   • {Colors.RED}❌ Not found{Colors.RESET} (ID: {role_id})")
                    if len(cleanup_role_ids) > 3:
                        print(f"   ... and {len(cleanup_role_ids) - 3} more")
                
                # Channels
                ticket_category_id = self.config.get("ticket_category_id")
                if ticket_category_id:
                    ticket_category = guild.get_channel(ticket_category_id)
                    if ticket_category:
                        print(f"{Colors.GREEN}🎫 Ticket Category:{Colors.RESET} {Colors.BOLD}{ticket_category.name}{Colors.RESET} (ID: {ticket_category_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Ticket Category:{Colors.RESET} Not found (ID: {ticket_category_id})")
                
                overflow_category_id = self.config.get("overflow_category_id")
                if overflow_category_id:
                    overflow_category = guild.get_channel(overflow_category_id)
                    if overflow_category:
                        print(f"{Colors.GREEN}📦 Overflow Category:{Colors.RESET} {Colors.BOLD}{overflow_category.name}{Colors.RESET} (ID: {overflow_category_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Overflow Category:{Colors.RESET} Not found (ID: {overflow_category_id})")
                
                log_channel_id = self.config.get("log_channel_id")
                if log_channel_id:
                    log_channel = guild.get_channel(log_channel_id)
                    if log_channel:
                        print(f"{Colors.GREEN}📝 Log Channel:{Colors.RESET} {Colors.BOLD}{log_channel.name}{Colors.RESET} (ID: {log_channel_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Log Channel:{Colors.RESET} Not found (ID: {log_channel_id})")
                
                welcome_log_channel_id = self.config.get("welcome_log_channel_id")
                if welcome_log_channel_id:
                    welcome_log_channel = guild.get_channel(welcome_log_channel_id)
                    if welcome_log_channel:
                        print(f"{Colors.GREEN}📢 Welcome Log Channel:{Colors.RESET} {Colors.BOLD}{welcome_log_channel.name}{Colors.RESET} (ID: {welcome_log_channel_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Welcome Log Channel:{Colors.RESET} Not found (ID: {welcome_log_channel_id})")
                
                # Users
                staff_user_id = self.config.get("staff_user_id")
                if staff_user_id:
                    staff_user = guild.get_member(staff_user_id)
                    if staff_user:
                        print(f"{Colors.GREEN}👤 Staff User:{Colors.RESET} {Colors.BOLD}{staff_user.name}{Colors.RESET} (ID: {staff_user_id})")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Staff User:{Colors.RESET} Not found (ID: {staff_user_id})")
            else:
                print(f"{Colors.YELLOW}⚠️  Guild not found (ID: {guild_id}){Colors.RESET}")
            
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            
            # Register persistent views
            self.bot.add_view(PersistentAccessView(self))
            
            # Re-schedule auto-close timers
            if guild_id:
                guild = self.bot.get_guild(guild_id)
                if guild:
                    for user_id in list(self.ticket_data.keys()):
                        asyncio.create_task(self.schedule_auto_close(int(user_id), guild))
                    
                    # Catch-up sweep for Welcome holders without tickets
                    asyncio.create_task(self.reconcile_tickets(guild))
                    
                    # Cleanup stale tickets on startup
                    asyncio.create_task(self.cleanup_stale_tickets(guild))
                    
                    # Start periodic cleanup (every 24 hours)
                    if not self.periodic_cleanup.is_running():
                        self.periodic_cleanup.start()
            
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
            
            # Initialize stats
            from datetime import datetime, timezone
            self.stats['started_at'] = datetime.now(timezone.utc).isoformat()
        
        @self.bot.event
        async def on_member_update(before, after):
            welcome_role_id = self.config.get("welcome_role_id")
            member_role_id = self.config.get("member_role_id")
            
            if not (welcome_role_id and member_role_id):
                return
            
            before_roles = {r.id for r in before.roles}
            after_roles = {r.id for r in after.roles}
            guild = after.guild
            
            # Welcome role added -> open ticket (match original - no duplicate check here)
            if (welcome_role_id not in before_roles) and (welcome_role_id in after_roles):
                await self.remove_cleanup_roles(after, reason="Welcome-gained cleanup")
                if member_role_id not in after_roles:
                    await self.open_onboarding_ticket(after)
            
            # Member role added -> send DM and close ticket
            if (member_role_id not in before_roles) and (member_role_id in after_roles):
                print(f"{Colors.CYAN}[Event] Member role added to {after.name} ({after.id}){Colors.RESET}")
                await self.log_action(
                    guild,
                    f"Member role added to {after.mention} - processing",
                    log_type="info",
                    member=after,
                    source="on_member_update"
                )
                await self.remove_cleanup_roles(after, reason="Member-gained cleanup")
                if welcome_role_id in after_roles:
                    try:
                        role = guild.get_role(welcome_role_id)
                        if role:
                            await after.remove_roles(role, reason="Member role granted – removing Welcome role")
                        await self.log_action(
                            guild,
                            f"Removed Welcome role (Member role granted)",
                            log_type="info",
                            member=after,
                            source="on_member_update"
                        )
                    except Exception as e:
                        await self.log_error(
                            guild, 
                            f"Couldn't remove Welcome role: {e}",
                            context="on_member_update - remove Welcome role"
                        )

                # Send branded DM for full access (only if not already sent recently)
                await self.send_member_granted_dm(after, source="on_member_update")

                # If they became a Member while a ticket exists, close it immediately
                # grant_member_and_close() already locks per-user; do not lock here (asyncio.Lock is not re-entrant).
                if str(after.id) in self.ticket_data:
                    await self.log_action(
                        guild,
                        f"Closing ticket for {after.mention} (Member role added)",
                        log_type="info",
                        member=after,
                        source="on_member_update"
                    )
                    await self.grant_member_and_close(after, None, source="member_role_added")
        
        @self.bot.event
        async def on_member_join(member):
            """Handle member join.

            Match original behavior: if a member joins already holding Welcome (edge case),
            open the onboarding ticket (and remove cleanup roles).
            """
            welcome_role_id = self.config.get("welcome_role_id")
            member_role_id = self.config.get("member_role_id")
            
            if not (welcome_role_id and member_role_id):
                return
            
            # Only remove cleanup roles on join - tickets are created when Welcome role is ADDED (on_member_update)
            role_ids = {r.id for r in member.roles}
            if (welcome_role_id in role_ids) and (member_role_id not in role_ids):
                await self.remove_cleanup_roles(member, reason="Welcome-on-join cleanup")
                await self.open_onboarding_ticket(member)
        
        @self.bot.event
        async def on_message(message):
            """Track activity in ticket channels"""
            # Ignore bot messages
            if message.author.bot:
                return
            
            # Check if this is a ticket channel
            if not isinstance(message.channel, discord.TextChannel):
                return
            
            # Note: Activity tracking removed to match original simplicity
            
            # Process commands
            await self.bot.process_commands(message)
        
    def _setup_commands(self):
        """Setup bot commands - imports editors here to avoid circular imports"""
        try:
            from message_editor import MessageEditorView
            from config_editor import ConfigEditorView
        except ImportError as e:
            print(f"{Colors.RED}[Error] Failed to import editors: {e}{Colors.RESET}")
            print(f"{Colors.YELLOW}[Error] Make sure message_editor.py and config_editor.py are in the same directory{Colors.RESET}")
            return
        
        @self.bot.command(name='editmessages', aliases=['edit', 'emsg'])
        async def edit_messages(ctx):
            """Edit all messages via embedded interface"""
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            view = MessageEditorView(self)
            embed = view.get_main_embed()
            await ctx.send(embed=embed, view=view)
        
        @self.bot.command(name='editconfig', aliases=['econfig', 'config'])
        async def edit_config(ctx):
            """Edit configuration via embedded interface"""
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            view = ConfigEditorView(self)
            embed = view.get_main_embed()
            await ctx.send(embed=embed, view=view)
        
        @self.bot.command(name='reload')
        async def reload_config(ctx):
            """Reload config and messages from files"""
            self.load_config()
            self.load_messages()
            self.load_tickets()
            try:
                await ctx.message.delete()
            except Exception:
                pass
            await ctx.send("✅ Configuration and messages reloaded!", delete_after=5)
        
        @self.bot.command(name='cleanup', aliases=['clean', 'cleanstale'])
        async def cleanup_tickets(ctx):
            """Manually trigger cleanup of stale tickets"""
            guild = ctx.guild
            if not guild:
                await ctx.send("❌ Command must be used in a server.", delete_after=5)
                return
            
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            await ctx.send("🧹 Starting cleanup of stale tickets...", delete_after=3)
            await self.cleanup_stale_tickets(guild)
            await ctx.send("✅ Cleanup complete! Check logs for details.", delete_after=10)
        
        @self.bot.command(name='status')
        async def bot_status(ctx):
            """Show bot status"""
            embed = discord.Embed(
                title="🤖 RS Onboarding Bot Status",
                color=discord.Color.blue()
            )
            
            embed.add_field(
                name="Bot",
                value=f"✅ Online\nUser: {self.bot.user}",
                inline=False
            )
            
            guild_id = self.config.get("guild_id")
            if guild_id:
                guild = self.bot.get_guild(guild_id)
                if guild:
                    embed.add_field(
                        name="Guild",
                        value=f"✅ Connected\n{guild.name}",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="Guild",
                        value=f"❌ Not found\nID: {guild_id}",
                        inline=True
                    )
            
            embed.add_field(
                name="Tickets",
                value=f"Active: {len(self.ticket_data)}",
                inline=True
            )
            
            try:
                await ctx.message.delete()
            except Exception:
                pass
            await ctx.send(embed=embed)
        
        # NOTE: 'config' is already used as an alias by ?editconfig; avoid alias conflicts.
        @self.bot.command(name='configinfo', aliases=['ids', 'showids'])
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
            
            # Roles
            welcome_role_id = self.config.get("welcome_role_id")
            member_role_id = self.config.get("member_role_id")
            if welcome_role_id:
                welcome_role = guild.get_role(welcome_role_id)
                if welcome_role:
                    embed.add_field(
                        name="👋 Welcome Role",
                        value=f"**{welcome_role.name}**\nID: `{welcome_role_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="👋 Welcome Role",
                        value=f"❌ Role not found\nID: `{welcome_role_id}`",
                        inline=True
                    )
            
            if member_role_id:
                member_role = guild.get_role(member_role_id)
                if member_role:
                    embed.add_field(
                        name="✅ Member Role",
                        value=f"**{member_role.name}**\nID: `{member_role_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="✅ Member Role",
                        value=f"❌ Role not found\nID: `{member_role_id}`",
                        inline=True
                    )
            
            cleanup_role_ids = self.config.get("cleanup_role_ids", [])
            if cleanup_role_ids:
                cleanup_list = []
                for role_id in cleanup_role_ids[:10]:  # Show first 10
                    role = guild.get_role(role_id)
                    if role:
                        cleanup_list.append(f"**{role.name}** (`{role_id}`)")
                    else:
                        cleanup_list.append(f"❌ Not found (`{role_id}`)")
                
                cleanup_text = "\n".join(cleanup_list)
                if len(cleanup_role_ids) > 10:
                    cleanup_text += f"\n... and {len(cleanup_role_ids) - 10} more"
                
                embed.add_field(
                    name=f"🧹 Cleanup Roles ({len(cleanup_role_ids)})",
                    value=cleanup_text,
                    inline=False
                )
            
            # Categories
            ticket_category_id = self.config.get("ticket_category_id")
            if ticket_category_id:
                ticket_category = guild.get_channel(ticket_category_id)
                if ticket_category:
                    embed.add_field(
                        name="🎫 Ticket Category",
                        value=f"**{ticket_category.name}**\nID: `{ticket_category_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="🎫 Ticket Category",
                        value=f"❌ Category not found\nID: `{ticket_category_id}`",
                        inline=True
                    )
            
            overflow_category_id = self.config.get("overflow_category_id")
            if overflow_category_id:
                overflow_category = guild.get_channel(overflow_category_id)
                if overflow_category:
                    embed.add_field(
                        name="📦 Overflow Category",
                        value=f"**{overflow_category.name}**\nID: `{overflow_category_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="📦 Overflow Category",
                        value=f"❌ Category not found\nID: `{overflow_category_id}`",
                        inline=True
                    )
            
            # Channels
            log_channel_id = self.config.get("log_channel_id")
            if log_channel_id:
                log_channel = guild.get_channel(log_channel_id)
                if log_channel:
                    embed.add_field(
                        name="📝 Log Channel",
                        value=f"**{log_channel.mention}** (`{log_channel.name}`)\nID: `{log_channel_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="📝 Log Channel",
                        value=f"❌ Channel not found\nID: `{log_channel_id}`",
                        inline=True
                    )
            
            welcome_log_channel_id = self.config.get("welcome_log_channel_id")
            if welcome_log_channel_id:
                welcome_log_channel = guild.get_channel(welcome_log_channel_id)
                if welcome_log_channel:
                    embed.add_field(
                        name="📢 Welcome Log Channel",
                        value=f"**{welcome_log_channel.mention}** (`{welcome_log_channel.name}`)\nID: `{welcome_log_channel_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="📢 Welcome Log Channel",
                        value=f"❌ Channel not found\nID: `{welcome_log_channel_id}`",
                        inline=True
                    )
            
            # Users
            staff_user_id = self.config.get("staff_user_id")
            if staff_user_id:
                staff_user = guild.get_member(staff_user_id)
                if staff_user:
                    embed.add_field(
                        name="👤 Staff User",
                        value=f"**{staff_user.mention}** (`{staff_user.name}`)\nID: `{staff_user_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="👤 Staff User",
                        value=f"❌ User not found\nID: `{staff_user_id}`",
                        inline=True
                    )
            
            alert_user_id = self.config.get("alert_user_id")
            if alert_user_id:
                alert_user = guild.get_member(alert_user_id)
                if alert_user:
                    embed.add_field(
                        name="🔔 Alert User",
                        value=f"**{alert_user.mention}** (`{alert_user.name}`)\nID: `{alert_user_id}`",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="🔔 Alert User",
                        value=f"❌ User not found\nID: `{alert_user_id}`",
                        inline=True
                    )
            
            embed.set_footer(text="🎫 RS Onboarding Bot | Use ?configinfo to see this again")
            
            await ctx.send(embed=embed)
            try:
                await ctx.message.delete()
            except Exception:
                pass
        
        @self.bot.command(name='test', aliases=['openticket', 'testticket'])
        async def test_ticket(ctx, *, args: str = None):
            """Manually trigger ticket creation for testing
            
            Usage:
            ?test - Create ticket for yourself
            ?test @user - Create ticket for specific user
            ?test @user force - Force create ticket even if they have Member role or existing ticket
            """
            # Parse arguments
            force_mode = False
            member = None
            
            if args:
                # Check for force flag
                parts = args.split()
                if 'force' in [p.lower() for p in parts]:
                    force_mode = True
                    parts = [p for p in parts if p.lower() != 'force']
                
                # Try to find member mention
                if parts:
                    # Try to extract member from mentions
                    if ctx.message.mentions:
                        member = ctx.message.mentions[0]
                    else:
                        # Try to find by username
                        member_name = ' '.join(parts)
                        member = discord.utils.get(ctx.guild.members, name=member_name) or \
                                 discord.utils.get(ctx.guild.members, display_name=member_name)
            
            # If no member specified, use the command author
            if member is None:
                member = ctx.author
            
            # Check if member is in the guild
            if not isinstance(member, discord.Member):
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                await ctx.send("❌ Member not found in this server.", delete_after=10)
                return
            
            # Check if they already have a ticket (unless force)
            if str(member.id) in self.ticket_data and not force_mode:
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                ticket_data = self.ticket_data.get(str(member.id), {})
                ticket_channel_id = ticket_data.get("channel_id")
                ticket_channel = ctx.guild.get_channel(ticket_channel_id) if ticket_channel_id else None
                
                if ticket_channel:
                    await ctx.send(
                        f"⚠️ {member.mention} already has an open ticket: {ticket_channel.mention}\n"
                        f"Use `?test @{member.name} force` to create a new one.",
                        delete_after=15
                    )
                else:
                    # Ticket data exists but channel is gone, clean it up
                    self.ticket_data.pop(str(member.id), None)
                    self.save_tickets()
                    await ctx.send(
                        f"ℹ️ Found orphaned ticket data. Cleaning up and creating new ticket...",
                        delete_after=10
                    )
                    # Continue to create ticket
            
            # Check if they already have Member role (unless force)
            member_role_id = self.config.get("member_role_id")
            if member_role_id and any(r.id == member_role_id for r in member.roles) and not force_mode:
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                await self.log_action(
                    ctx.guild,
                    f"Test skipped - {member.mention} already has Member role",
                    log_type="info",
                    member=member,
                    source="test_command"
                )
                await ctx.send(
                    f"ℹ️ {member.mention} already has the Member role. No ticket needed.\n"
                    f"Use `?test @{member.name} force` to create a test ticket anyway.",
                    delete_after=15
                )
                return
            
            # Trigger ticket creation
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            if force_mode:
                await ctx.send(
                    f"🔄 Creating test ticket for {member.mention} (FORCE MODE - bypassing checks)...",
                    delete_after=10
                )
            else:
                await ctx.send(
                    f"🔄 Creating test ticket for {member.mention}...",
                    delete_after=10
                )
            
            # Check if ticket already exists before opening
            ticket_exists_before = str(member.id) in self.ticket_data
            
            # If force mode and ticket exists, clean it up first
            if force_mode and ticket_exists_before:
                ticket_data = self.ticket_data.get(str(member.id), {})
                ticket_channel_id = ticket_data.get("channel_id")
                if ticket_channel_id:
                    try:
                        ticket_channel = ctx.guild.get_channel(ticket_channel_id)
                        if ticket_channel:
                            await ticket_channel.delete()
                    except Exception:
                        pass
                self.ticket_data.pop(str(member.id), None)
                self.save_tickets()
                await self.log_action(
                    ctx.guild,
                    f"Cleaned up existing ticket for {member.mention} (force mode)",
                    log_type="info",
                    member=member,
                    source="test_command"
                )
            
            # Open the ticket
            try:
                await self.open_onboarding_ticket(member)
                
                # Verify ticket was actually created
                await asyncio.sleep(0.5)  # Small delay to ensure ticket data is saved
                ticket_exists_after = str(member.id) in self.ticket_data
                
                if ticket_exists_after and not ticket_exists_before:
                    # Get ticket channel info for confirmation
                    ticket_data = self.ticket_data.get(str(member.id), {})
                    ticket_channel_id = ticket_data.get("channel_id")
                    ticket_channel = ctx.guild.get_channel(ticket_channel_id) if ticket_channel_id else None
                    
                    if ticket_channel:
                        await ctx.send(
                            f"✅ Test ticket created successfully!\n"
                            f"📝 Ticket: {ticket_channel.mention}\n"
                            f"🔗 [Jump to Ticket]({ticket_channel.jump_url})",
                            delete_after=10
                        )
                    else:
                        await ctx.send(
                            f"✅ Test ticket created for {member.mention}!",
                            delete_after=5
                        )
                else:
                    await ctx.send(
                        f"⚠️ Ticket creation may have failed. Check logs for errors.",
                        delete_after=10
                    )
                    await self.log_error(
                        ctx.guild,
                        f"Test ticket creation verification failed for {member.mention}",
                        context="test_command - verification"
                    )
            except Exception as e:
                await ctx.send(
                    f"❌ Failed to create test ticket: {str(e)[:200]}",
                    delete_after=10
                )
                await self.log_error(
                    ctx.guild,
                    f"Test ticket creation failed for {member.mention}: {e}",
                    context="test_command"
                )
        
        @self.bot.command(name='clearticket', aliases=['closeticket', 'removeticket'])
        async def clear_ticket(ctx, member: discord.Member = None):
            """Manually close/clear a ticket for testing"""
            # If no member specified, use the command author
            if member is None:
                member = ctx.author
            
            # Check if member is in the guild
            if not isinstance(member, discord.Member):
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                await ctx.send("❌ Member not found in this server.", delete_after=10)
                return
            
            # Check if they have a ticket
            if str(member.id) not in self.ticket_data:
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                await ctx.send(
                    f"ℹ️ {member.mention} doesn't have an open ticket.",
                    delete_after=10
                )
                return
            
            # Get the ticket channel
            data = self.ticket_data.get(str(member.id))
            channel_id = data.get("channel_id") if data else None
            
            # Delete the ticket channel if it exists
            if channel_id:
                try:
                    channel = ctx.guild.get_channel(channel_id)
                    if channel:
                        await channel.delete()
                except Exception:
                    pass
            
            # Remove from ticket data
            self.ticket_data.pop(str(member.id), None)
            self.save_tickets()
            
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            await ctx.send(
                f"✅ Ticket cleared for {member.mention}!",
                delete_after=10
            )
        
        @self.bot.command(name='onboardhelp', aliases=['commands', 'h', 'helpme'])
        async def help_command(ctx):
            """Show all available commands with detailed explanations"""
            # Get embed color from config
            color = self.get_embed_color()
            
            embed = discord.Embed(
                title="🎫 RS Onboarding Bot - Command Reference",
                description=(
                    "**Complete guide to all available commands**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "All commands use the `?` prefix.\n"
                    "Replies may auto-delete after a short time (Discord text commands cannot be truly ephemeral)."
                ),
                color=color
            )
            
            # Quick Command Reference (inline fields for compact display)
            embed.add_field(
                name="⚡ Quick Commands",
                value=(
                    "`?onboardhelp` - Show this help menu\n"
                    "`?editmessages` - Edit all bot messages\n"
                    "`?editconfig` - Edit bot configuration\n"
                    "`?test` - Create a test ticket\n"
                    "`?status` - Check bot status"
                ),
                inline=True
            )
            
            embed.add_field(
                name="🔧 Utility Commands",
                value=(
                    "`?reload` - Reload config files\n"
                    "`?clearticket` - Clear test tickets\n"
                    "`?test @user` - Test for specific user\n"
                    "`?clearticket @user` - Clear user's ticket"
                ),
                inline=True
            )
            
            embed.add_field(
                name="📋 Command Aliases",
                value=(
                    "`?editmessages` = `?edit`, `?emsg`\n"
                    "`?editconfig` = `?econfig`, `?config`\n"
                    "`?test` = `?openticket`, `?testticket`\n"
                    "`?clearticket` = `?closeticket`, `?removeticket`\n"
                    "`?onboardhelp` = `?commands`, `?h`, `?helpme`"
                ),
                inline=True
            )
            
            # Detailed Command Descriptions
            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="**📝 Detailed Command Information**",
                inline=False
            )
            
            # Message Editing
            embed.add_field(
                name="📝 `?editmessages` - Message Editor",
                value=(
                    "**What it does:** Edit all bot messages via interactive interface\n\n"
                    "**You can edit:**\n"
                    "• Onboarding step messages (5 steps)\n"
                    "  └ Titles, descriptions, and images\n"
                    "• Direct messages (DMs)\n"
                    "  └ Member granted DM\n"
                    "  └ Ticket open DM\n"
                    "• Auto-close message\n"
                    "• Button labels\n\n"
                    "**Features:** Preview before saving, instant updates"
                ),
                inline=False
            )
            
            # Configuration Editing
            embed.add_field(
                name="⚙️ `?editconfig` - Configuration Editor",
                value=(
                    "**What it does:** Edit bot configuration via interactive interface\n\n"
                    "**You can edit:**\n"
                    "• **IDs & Roles:** Guild, roles, channels, users\n"
                    "• **Appearance:** Embed colors, banner URL, footer text\n"
                    "• **Timing:** Auto-close seconds, DM TTL\n\n"
                    "**Features:** All changes save immediately to `config.json`"
                ),
                inline=False
            )
            
            # Testing Commands
            embed.add_field(
                name="🧪 Testing Commands",
                value=(
                    "**`?test`** - Manually create a test ticket\n"
                    "• Creates ticket for you: `?test`\n"
                    "• Creates ticket for user: `?test @username`\n\n"
                    "**`?clearticket`** - Manually close/clear a ticket\n"
                    "• Clears your ticket: `?clearticket`\n"
                    "• Clears user's ticket: `?clearticket @username`"
                ),
                inline=False
            )
            
            # Utility Commands
            embed.add_field(
                name="🔧 Utility Commands",
                value=(
                    "**`?reload`** - Reload configuration files\n"
                    "Use after manually editing `config.json` or `messages.json`\n\n"
                    "**`?status`** - Show bot status\n"
                    "Displays: Bot online status, guild connection, active ticket count"
                ),
                inline=False
            )
            
            # Automatic Behavior
            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="**🔄 Automatic Bot Behavior**",
                inline=False
            )
            
            embed.add_field(
                name="✅ Tickets Created Automatically",
                value=(
                    "• When member receives **Welcome** role\n"
                    "• When member joins with Welcome role already assigned"
                ),
                inline=True
            )
            
            embed.add_field(
                name="🔒 Tickets Closed Automatically",
                value=(
                    "• When member receives **Member** role\n"
                    "• After 24 hours (auto-close timer)\n"
                    "• When user clicks 'Get Full Access' button"
                ),
                inline=True
            )
            
            # Footer with helpful info
            embed.set_footer(
                text="💡 Tip: Use ?onboardhelp anytime | Commands try to stay low-noise (auto-delete where safe)"
            )
            
            # Add timestamp
            from datetime import datetime, timezone
            embed.timestamp = datetime.now(timezone.utc)
            
            try:
                await ctx.message.delete()
            except Exception:
                pass
            
            await ctx.send(embed=embed, delete_after=120)


# Persistent Access View
class PersistentAccessView(ui.View):
    def __init__(self, bot_instance: RSOnboardingBot):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
    
    @ui.button(label="Get Full Access", style=discord.ButtonStyle.success, custom_id="onb:finish")
    async def finish(self, interaction: discord.Interaction, button: ui.Button):
        try:
            if interaction.user and isinstance(interaction.user, discord.Member):
                channel = interaction.message.channel if interaction.message else None
                await self.bot_instance.grant_member_and_close(
                    interaction.user, channel, source="button_persistent_finish"
                )
                try:
                    full_access_msg = self.bot_instance.config.get("full_access_message", "✅ You now have full access!")
                    await interaction.response.send_message(full_access_msg, ephemeral=True)
                except discord.NotFound:
                    # Channel already deleted, ignore
                    pass
            else:
                await interaction.response.send_message("Error: Could not identify user.", ephemeral=True)
        except Exception as e:
            if interaction.guild:
                await self.bot_instance.log_error(
                    interaction.guild, 
                    f"Error granting access (persistent): {repr(e)}",
                    context="PersistentAccessView.finish"
                )
            try:
                await interaction.response.send_message(
                    "Something went wrong. Please contact support.", 
                    ephemeral=True
                )
            except Exception:
                pass
    
    @ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="onb:close")
    async def close(self, interaction: discord.Interaction, button: ui.Button):
        """Unified close path - routes through grant_member_and_close()"""
        try:
            if not interaction.user or not isinstance(interaction.user, discord.Member):
                await interaction.response.send_message("Error: Could not identify user.", ephemeral=True)
                return
            
            # Route through unified close logic (handles locks, storage, role updates)
            channel = interaction.message.channel if interaction.message else None
            await self.bot_instance.grant_member_and_close(
                interaction.user, 
                channel, 
                source="button_persistent_close"
            )
            
            try:
                await interaction.response.send_message("Ticket closed.", ephemeral=True)
            except discord.NotFound:
                # Channel already deleted, ignore
                pass
        except Exception as e:
            if interaction.guild:
                await self.bot_instance.log_error(
                    interaction.guild, 
                    f"CloseButton (persistent) error: {repr(e)}",
                    context="PersistentAccessView.close"
                )
            try:
                await interaction.response.send_message(
                    "Something went wrong closing the ticket. Please contact support.", 
                    ephemeral=True
                )
            except Exception:
                pass


# Onboarding View (Stepper)
class OnboardingView(ui.View):
    def __init__(self, bot_instance: RSOnboardingBot, member: discord.Member):
        super().__init__(timeout=None)
        self.bot_instance = bot_instance
        self.member = member
        self.step = 0
        self.message: Optional[discord.Message] = None
        self.update_buttons()
    
    def update_buttons(self):
        """Update button layout based on current step"""
        self.clear_items()
        button_labels = self.bot_instance.messages.get("button_labels", {})
        
        if self.step > 0:
            prev_label = button_labels.get("previous", "Previous")
            self.add_item(self.PrevButton(prev_label))
        
        if self.step < len(self.bot_instance.messages.get("steps", [])) - 1:
            next_label = button_labels.get("next", "Next")
            self.add_item(self.NextButton(next_label))
        
        if self.step == len(self.bot_instance.messages.get("steps", [])) - 1:
            full_access_label = button_labels.get("get_full_access", "Get Full Access")
            close_label = button_labels.get("close_ticket", "Close Ticket")
            self.add_item(self.FinishButton(full_access_label))
            self.add_item(self.CloseButton(close_label))
    
    class NextButton(ui.Button):
        def __init__(self, label: str):
            super().__init__(label=label, style=discord.ButtonStyle.success)
        
        async def callback(self, interaction: discord.Interaction):
            view: "OnboardingView" = self.view
            view.step += 1
            view.update_buttons()
            try:
                embed = view.bot_instance.get_step_embed(view.step, view.member)
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.NotFound:
                pass
    
    class PrevButton(ui.Button):
        def __init__(self, label: str):
            super().__init__(label=label, style=discord.ButtonStyle.primary)
        
        async def callback(self, interaction: discord.Interaction):
            view: "OnboardingView" = self.view
            view.step -= 1
            view.update_buttons()
            try:
                embed = view.bot_instance.get_step_embed(view.step, view.member)
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.NotFound:
                pass
    
    class FinishButton(ui.Button):
        def __init__(self, label: str):
            super().__init__(label=label, style=discord.ButtonStyle.success)
        
        async def callback(self, interaction: discord.Interaction):
            view: "OnboardingView" = self.view
            try:
                # Validate interaction user matches view member
                if interaction.user.id != view.member.id:
                    await interaction.response.send_message(
                        "This ticket belongs to another user.", 
                        ephemeral=True
                    )
                    return
                
                channel = interaction.message.channel if interaction.message else None
                await view.bot_instance.grant_member_and_close(
                    view.member, 
                    channel, 
                    source="button_stepper_finish"
                )
                full_access_msg = view.bot_instance.config.get("full_access_message", "✅ You now have full access!")
                try:
                    await interaction.response.send_message(full_access_msg, ephemeral=True)
                except discord.NotFound:
                    try:
                        await view.member.send(full_access_msg)
                    except Exception:
                        pass
            except Exception as e:
                await view.bot_instance.log_error(
                    interaction.guild, 
                    f"Error granting access (stepper): {repr(e)}",
                    context="OnboardingView.FinishButton"
                )
                try:
                    await interaction.response.send_message(
                        "Something went wrong. Please contact support.", ephemeral=True
                    )
                except Exception:
                    pass
    
    class CloseButton(ui.Button):
        def __init__(self, label: str):
            super().__init__(label=label, style=discord.ButtonStyle.danger)
        
        async def callback(self, interaction: discord.Interaction):
            """Unified close path - routes through grant_member_and_close()"""
            view: "OnboardingView" = self.view
            try:
                # Validate interaction user matches view member
                if interaction.user.id != view.member.id:
                    await interaction.response.send_message(
                        "This ticket belongs to another user.", 
                        ephemeral=True
                    )
                    return
                
                # Route through unified close logic (handles locks, storage, role updates)
                channel = interaction.message.channel if interaction.message else None
                await view.bot_instance.grant_member_and_close(
                    view.member, 
                    channel, 
                    source="button_stepper_close"
                )
                
                try:
                    await interaction.response.send_message("Ticket closed.", ephemeral=True)
                except discord.NotFound:
                    # Channel already deleted, ignore
                    pass
            except Exception as e:
                await view.bot_instance.log_error(
                    interaction.guild, 
                    f"CloseButton (stepper) error: {repr(e)}",
                    context="OnboardingView.CloseButton"
                )
                try:
                    await interaction.response.send_message(
                        "Something went wrong closing the ticket. Please contact support.", 
                        ephemeral=True
                    )
                except Exception:
                    pass


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
            return
        print(f"{Colors.GREEN}[ConfigCheck] OK{Colors.RESET}")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        return

    # Best-effort single-instance lock (prevents two bot processes creating duplicate tickets).
    lock_fh = None
    try:
        import fcntl  # Only available on Linux/Unix
        lock_path = Path(__file__).parent / ".rs_onboarding_bot.lock"
        lock_fh = open(lock_path, "a+", encoding="utf-8")
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"{Colors.RED}[Bot] Another RSOnboarding instance is already running. Exiting.{Colors.RESET}")
            return
    except Exception:
        # If locking isn't available, continue without it (still guarded by tickets.json 'creating' marker).
        lock_fh = None

    bot = RSOnboardingBot()
    try:
        asyncio.run(bot.bot.start(bot.config.get("bot_token")))
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[Bot] Stopped{Colors.RESET}")
    finally:
        try:
            if lock_fh:
                lock_fh.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
