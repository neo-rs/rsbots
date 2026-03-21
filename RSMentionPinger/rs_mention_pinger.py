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
from typing import Any, List, Optional, Set, Tuple

# Type alias to avoid deep nesting that can trigger SyntaxError on older Python
_MonitorEntry = Tuple[int, str, Optional[Any]]
_MonitorEntriesByCategory = List[Tuple[str, List[_MonitorEntry]]]

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import discord
from discord.ext import commands

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

# Max buttons per Discord message (5 rows x 5)
MONITOR_BUTTONS_PER_VIEW = 25

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
    
    # ----- Monitor role picker (button-based channel subscription) -----
    
    def _monitor_roles_config(self) -> Optional[dict]:
        """Return monitor_roles config section or None if disabled."""
        mr = self.config.get("monitor_roles")
        if not mr or not mr.get("picker_channel_id"):
            return None
        return mr
    
    def _channel_display_name(self, ch_name: str) -> str:
        """Human-readable label from channel name; strip -monitor, use config map or heuristic."""
        mr = self._monitor_roles_config()
        if mr:
            names = mr.get("channel_display_names") or {}
            if isinstance(names, dict) and ch_name in names:
                return str(names[ch_name])
        s = ch_name.replace("-monitor", "").replace("_", " ").replace("-", " ").strip()
        return s.title() if s else ch_name

    def _resolve_button_emoji(self, guild: discord.Guild, ch_name: str) -> Optional[Any]:
        """Resolve config button_emojis[ch_name] to a Discord emoji (for Button). Returns None if not set."""
        mr = self._monitor_roles_config()
        if not mr:
            return None
        emojis_cfg = mr.get("button_emojis")
        if not isinstance(emojis_cfg, dict) or ch_name not in emojis_cfg:
            return None
        val = emojis_cfg[ch_name]
        if isinstance(val, int):
            em = guild.get_emoji(val)
            return em
        if isinstance(val, str):
            em = discord.utils.get(guild.emojis, name=val)
            return em
        return None

    def _build_monitor_entries_by_category(self, guild: discord.Guild) -> _MonitorEntriesByCategory:
        """Build per-category list of (role_id, display_label, emoji) for buttons. Uses config 'entries' or 'categories'."""
        mr = self._monitor_roles_config()
        if not mr:
            return []
        entries_cfg = mr.get("entries")
        if entries_cfg:
            flat: List[_MonitorEntry] = [(int(e["role_id"]), e.get("label") or str(e.get("role_id", "")), None) for e in entries_cfg]
            return [("Monitor channels", flat)]
        categories_cfg = mr.get("categories") or []
        out: _MonitorEntriesByCategory = []
        for cat_cfg in categories_cfg:
            cat_id = cat_cfg.get("id") if isinstance(cat_cfg, dict) else cat_cfg
            if not cat_id:
                continue
            cat_title = cat_cfg.get("title", "Monitor channels") if isinstance(cat_cfg, dict) else "Monitor channels"
            excluded_ids: Set[int] = set()
            if isinstance(cat_cfg, dict):
                for cid in cat_cfg.get("excluded_channel_ids") or []:
                    excluded_ids.add(int(cid))
            category = guild.get_channel(int(cat_id))
            if not category or not isinstance(category, discord.CategoryChannel):
                continue
            cat_entries: List[_MonitorEntry] = []
            for ch in category.text_channels:
                if ch.id in excluded_ids:
                    continue
                role_name = f"Monitor | {ch.name}"
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    label = self._channel_display_name(ch.name)
                    emoji = self._resolve_button_emoji(guild, ch.name)
                    cat_entries.append((role.id, label, emoji))
            if cat_entries:
                out.append((cat_title, cat_entries))
        return out

    def _build_monitor_entries(self, guild: discord.Guild) -> List[_MonitorEntry]:
        """Flat list of (role_id, label, emoji) for persistent view registration (same order as by_category)."""
        by_cat = self._build_monitor_entries_by_category(guild)
        return [e for _, entries in by_cat for e in entries]
    
    async def _ensure_monitor_roles_and_overwrites(self, guild: discord.Guild) -> None:
        """Create missing 'Monitor | channel' roles and set channel overwrites. Members role gets no view by default."""
        mr = self._monitor_roles_config()
        if not mr:
            return
        members_role_id = mr.get("members_role_id")
        members_role = guild.get_role(int(members_role_id)) if members_role_id else None
        categories_cfg = mr.get("categories") or []
        for cat_cfg in categories_cfg:
            cat_id = cat_cfg.get("id") if isinstance(cat_cfg, dict) else cat_cfg
            if not cat_id:
                continue
            excluded_ids: Set[int] = set()
            if isinstance(cat_cfg, dict):
                for cid in cat_cfg.get("excluded_channel_ids") or []:
                    excluded_ids.add(int(cid))
            category = guild.get_channel(int(cat_id))
            if not category or not isinstance(category, discord.CategoryChannel):
                continue
            for ch in category.text_channels:
                if ch.id in excluded_ids:
                    continue
                role_name = f"Monitor | {ch.name}"
                role = discord.utils.get(guild.roles, name=role_name)
                if not role:
                    try:
                        role = await guild.create_role(
                            name=role_name,
                            permissions=discord.Permissions.none(),
                            reason="Monitor role for channel subscription",
                        )
                        print(f"{Colors.GREEN}[MonitorRoles] Created role: {role.name}{Colors.RESET}")
                    except Exception as e:
                        print(f"{Colors.RED}[MonitorRoles] Failed to create role {role_name}: {e}{Colors.RESET}")
                        continue
                try:
                    await ch.set_permissions(guild.default_role, view_channel=False)
                    if members_role:
                        await ch.set_permissions(members_role, view_channel=False)
                    await ch.set_permissions(role, view_channel=True, read_message_history=True)
                except Exception as e:
                    print(f"{Colors.RED}[MonitorRoles] Failed to set overwrites for #{ch.name}: {e}{Colors.RESET}")
    
    def _first_image_from_message(self, message: discord.Message) -> Optional[str]:
        """Extract first image URL from message attachments or embeds."""
        return _first_image_from_message_impl(message)


class MonitorRoleView(discord.ui.View):
    """Persistent view: buttons toggle roles so members can show/hide monitor channels."""

    def __init__(self, pinger: RSMentionPinger, entries: List[_MonitorEntry], **kwargs):
        super().__init__(timeout=None, **kwargs)
        self.pinger = pinger
        for role_id, label, emoji in entries:
            btn = discord.ui.Button(
                label=label[:80],
                style=discord.ButtonStyle.primary,
                custom_id=f"monitor_toggle:{role_id}",
                emoji=emoji if emoji is not None else None,
            )
            btn.callback = self._make_callback(role_id, label)
            self.add_item(btn)

    def _make_callback(self, role_id: int, label: str):
        async def callback(interaction: discord.Interaction):
            await self._toggle_role(interaction, role_id, label)
        return callback

    async def _toggle_role(self, interaction: discord.Interaction, role_id: int, label: str):
        if not interaction.guild or not interaction.user:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        member = interaction.guild.get_member(interaction.user.id)
        if not member:
            await interaction.response.send_message("Could not find you as a member.", ephemeral=True)
            return
        role = interaction.guild.get_role(role_id)
        if not role:
            await interaction.response.send_message(f"Role for **{label}** no longer exists.", ephemeral=True)
            return
        try:
            if role in member.roles:
                await member.remove_roles(role, reason="Monitor role toggle (unsubscribe)")
                await interaction.response.send_message(f"Unsubscribed from **{label}**. Channel hidden.", ephemeral=True)
            else:
                await member.add_roles(role, reason="Monitor role toggle (subscribe)")
                await interaction.response.send_message(f"Subscribed to **{label}**. Channel visible.", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message("I don't have permission to manage that role.", ephemeral=True)
        except Exception as e:
            print(f"{Colors.RED}[MonitorRoles] Toggle error: {e}{Colors.RESET}")
            await interaction.response.send_message("Something went wrong. Try again or ask an admin.", ephemeral=True)


def _first_image_from_message_impl(message: discord.Message) -> Optional[str]:
    """Extract first image URL from message attachments or embeds."""
    for att in message.attachments:
        if (att.content_type and att.content_type.startswith("image/")) or (
            att.filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
        ):
            return att.url
    for e in message.embeds:
        if e.image and e.image.url:
            return e.image.url
        if e.type == "image" and e.url:
            return e.url
    return None


class _RSMentionPingerImpl:
    """Placeholder: methods below belong to RSMentionPinger (see next fix)."""

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
        print(f"{Colors.CYAN}[Mention] Role mention detected: {', '.join(mentioned_roles)} in <#{message.channel.id}>{Colors.RESET}")
        
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
        
        print(f"{Colors.CYAN}[DM] Sending mention DMs to {len(unique_users)} user(s) from <#{message.channel.id}>{Colors.RESET}")
        for user in unique_users:
            try:
                dm_text = dm_template.format(
                    author_mention=message.author.mention,
                    jump_url=message.jump_url
                )
                await user.send(dm_text)
                print(f"{Colors.GREEN}✅ Sent mention DM to {user} <@{user.id}>{Colors.RESET}")
            except discord.Forbidden:
                print(f"{Colors.YELLOW}⚠️ Couldn't DM {user} <@{user.id}>: User has DMs disabled{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ Couldn't DM {user} <@{user.id}>: {e}{Colors.RESET}")
    
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
                print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET} {Colors.BOLD}{guild.name}{Colors.RESET} Guild-ID: {guild_id}")
                
                # Log channel
                log_channel_id = self.config.get("log_channel_id")
                if log_channel_id:
                    log_channel = guild.get_channel(log_channel_id)
                    if log_channel:
                        print(f"{Colors.GREEN}📝 Log Channel:{Colors.RESET} {Colors.BOLD}{log_channel.name}{Colors.RESET} <#{log_channel_id}>")
                    else:
                        print(f"{Colors.YELLOW}⚠️  Log Channel:{Colors.RESET} Not found <#{log_channel_id}>")
                
                # Watched roles
                watched_role_ids = self.config.get("watched_role_ids", [])
                if watched_role_ids:
                    print(f"{Colors.GREEN}👀 Watched Roles:{Colors.RESET} {len(watched_role_ids)} role(s)")
                    for role_id in watched_role_ids[:5]:  # Show first 5
                        role = guild.get_role(role_id)
                        if role:
                            print(f"   • {Colors.BOLD}{role.name}{Colors.RESET} <@&{role_id}>")
                        else:
                            print(f"   • {Colors.RED}❌ Not found{Colors.RESET} <@&{role_id}>")
                    if len(watched_role_ids) > 5:
                        print(f"   ... and {len(watched_role_ids) - 5} more (use !configinfo to see all)")
                
                # Excluded categories
                excluded_category_ids = self.config.get("excluded_category_ids", [])
                if excluded_category_ids:
                    print(f"{Colors.GREEN}🚫 Excluded Categories:{Colors.RESET} {len(excluded_category_ids)} category/categories")
                    for cat_id in excluded_category_ids:
                        category = guild.get_channel(cat_id)
                        if category:
                            print(f"   • {Colors.BOLD}{category.name}{Colors.RESET} <#{cat_id}>")
                        else:
                            print(f"   • {Colors.RED}❌ Not found{Colors.RESET} <#{cat_id}>")
                
                # DM feature info
                dm_template = self.config.get("dm_message_template", "{author_mention} mentioned you {jump_url}")
                print(f"{Colors.GREEN}📩 DM Feature:{Colors.RESET} {Colors.BOLD}Enabled{Colors.RESET}")
                print(f"   Template: {dm_template}")
                print(f"   • Sends DM to users when they're mentioned")
                print(f"   • Skips DMs in excluded categories")
                print(f"   • Ignores bot mentions")
            else:
                print(f"{Colors.YELLOW}⚠️  Guild not found Guild-ID: {guild_id}{Colors.RESET}")
            
            # Register persistent monitor role views so buttons work after restart
            if guild and self._monitor_roles_config():
                entries = self._build_monitor_entries(guild)
                if entries:
                    for i in range(0, len(entries), MONITOR_BUTTONS_PER_VIEW):
                        chunk = entries[i : i + MONITOR_BUTTONS_PER_VIEW]
                        self.bot.add_view(MonitorRoleView(self, chunk))
                    print(f"{Colors.GREEN}[MonitorRoles] Registered {len(entries)} role button(s) in {(len(entries) + MONITOR_BUTTONS_PER_VIEW - 1) // MONITOR_BUTTONS_PER_VIEW} view(s){Colors.RESET}")
            
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
        
        @self.bot.command(name='postmonitorroles')
        @commands.has_permissions(manage_guild=True)
        async def post_monitor_roles(ctx):
            """Post or update monitor role picker messages (admin). Creates roles/overwrites if using categories."""
            mr = self._monitor_roles_config()
            if not mr:
                await ctx.send("❌ `monitor_roles` is not configured in config.json.", delete_after=10)
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                return
            guild = ctx.guild
            if not guild:
                await ctx.send("❌ Use this command in a server.", delete_after=10)
                return
            picker_channel_id = mr.get("picker_channel_id")
            picker_channel = guild.get_channel(picker_channel_id) if picker_channel_id else None
            if not picker_channel or not isinstance(picker_channel, discord.TextChannel):
                await ctx.send(f"❌ Picker channel not found (ID: {picker_channel_id}).", delete_after=10)
                try:
                    await ctx.message.delete()
                except Exception:
                    pass
                return
            await ctx.send("⏳ Setting up roles and overwrites (if using categories), then posting buttons…", delete_after=5)
            try:
                await ctx.message.delete()
            except Exception:
                pass
            try:
                await self._ensure_monitor_roles_and_overwrites(guild)
                by_category = self._build_monitor_entries_by_category(guild)
                if not by_category:
                    await picker_channel.send("No monitor entries found. Check `monitor_roles.categories` or `monitor_roles.entries` in config.")
                    return
                # Delete previous bot messages in picker channel to avoid duplicates
                try:
                    async for msg in picker_channel.history(limit=50):
                        if msg.author == self.bot.user:
                            await msg.delete()
                except Exception:
                    pass
                default_desc = "Choose the channels you want alerts from by clicking the buttons below. **ADD** or **REMOVE** roles to receive notifications when new items drop or restocks occur."
                desc = (mr.get("embed_description") or default_desc).strip()
                footer = mr.get("embed_footer") or "RS Monitor Roles"
                for cat_title, entries in by_category:
                    total_pages = (len(entries) + MONITOR_BUTTONS_PER_VIEW - 1) // MONITOR_BUTTONS_PER_VIEW
                    for i in range(0, len(entries), MONITOR_BUTTONS_PER_VIEW):
                        chunk = entries[i : i + MONITOR_BUTTONS_PER_VIEW]
                        view = MonitorRoleView(self, chunk)
                        title = f"Monitor Channels - {cat_title}"
                        if total_pages > 1:
                            page = i // MONITOR_BUTTONS_PER_VIEW + 1
                            title += f" (page {page}/{total_pages})"
                        embed = discord.Embed(
                            title=title,
                            description=desc,
                            color=self.get_embed_color(),
                        )
                        embed.set_footer(text=footer)
                        if guild.icon:
                            embed.set_thumbnail(url=guild.icon.url)
                        await picker_channel.send(embed=embed, view=view)
            except Exception as e:
                print(f"{Colors.RED}[MonitorRoles] postmonitorroles error: {e}{Colors.RESET}")
                await picker_channel.send(f"❌ Error: {e}")
    
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


# Attach methods that are defined later in the file (after MonitorRoleView)
for _m in ("_log_role_mention", "_dm_mentioned_users", "_setup_events", "run"):
    setattr(RSMentionPinger, _m, getattr(_RSMentionPingerImpl, _m))


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

