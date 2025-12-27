#!/usr/bin/env python3
"""
RS Vouch Bot Module
-------------------
Bot module for managing vouches (reputation system).
Can work standalone or attach to an existing bot instance.
All configuration in vouch_config.json, uses bot token from config.json.
"""

import os
import sys
import json
import asyncio
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timezone

import discord
from discord.ext import commands
from discord import app_commands

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class VouchView(discord.ui.View):
    """View for vouch messages with button to view all vouches"""
    
    def __init__(self, vouched_user: discord.User, vouch_module: 'RSVouchBot'):
        super().__init__(timeout=None)
        self.vouched_user = vouched_user
        self.vouch_module = vouch_module
    
    @discord.ui.button(label="View All Vouches", style=discord.ButtonStyle.primary)
    async def view_vouches(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.vouch_module._vouches_logic(interaction, self.vouched_user, should_delete=False)
        except Exception as e:
            print(f"{Colors.RED}[Vouch] Error in button: {e}{Colors.RESET}")
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ Failed to load vouches.", ephemeral=True)


class RSVouchBot:
    """Vouch bot module for reputation system"""
    
    def __init__(self, bot_instance: Optional[commands.Bot] = None):
        """
        Initialize vouch bot module
        
        Args:
            bot_instance: Optional existing bot instance to attach to.
                         If None, creates its own bot instance.
        """
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        self.vouch_config_path = self.base_path / "vouch_config.json"
        
        self.config: Dict[str, Any] = {}
        self.vouch_config: Dict[str, Any] = {}
        
        # JSON data storage
        self.vouches_json_path = self.base_path / "vouches.json"
        self.vouches_data: Dict[str, Any] = {
            "vouches": [],
            "migrated_at": None
        }
        
        # Cooldown tracking for vouch command
        self.vouch_cooldowns: Dict[int, float] = {}  # user_id -> last_use_timestamp
        
        # Load configurations
        self.load_config()
        self.load_vouch_config()
        self.load_vouches_data()
        
        # Use provided bot instance or create new one
        if bot_instance:
            self.bot = bot_instance
            self._is_shared_bot = True
        else:
            # Validate required config for standalone mode
            if not self.config.get("bot_token"):
                print(f"{Colors.RED}[Vouch] ERROR: 'bot_token' is required in config.json for standalone mode{Colors.RESET}")
                sys.exit(1)
            
            # Setup bot for standalone mode
            intents = discord.Intents.default()
            intents.members = True
            self.bot = commands.Bot(command_prefix="!", intents=intents)
            self._is_shared_bot = False
        
        self._setup_commands()
    
    def load_config(self):
        """Load main configuration from config.json"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            print(f"{Colors.GREEN}[Vouch] Configuration loaded from {self.config_path}{Colors.RESET}")
        except FileNotFoundError:
            print(f"{Colors.RED}[Vouch] ERROR: {self.config_path} not found{Colors.RESET}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"{Colors.RED}[Vouch] ERROR: Invalid JSON in {self.config_path}: {e}{Colors.RESET}")
            sys.exit(1)
    
    def load_vouch_config(self):
        """Load vouch-specific configuration from vouch_config.json"""
        try:
            with open(self.vouch_config_path, 'r', encoding='utf-8') as f:
                self.vouch_config = json.load(f)
            print(f"{Colors.GREEN}[Vouch] Vouch configuration loaded from {self.vouch_config_path}{Colors.RESET}")
            
            # Validate required config
            guild_id = self.vouch_config.get("guild_id")
            vouch_channel_id = self.vouch_config.get("vouch_channel_id")
            staff_role_id = self.vouch_config.get("staff_role_id")
            
            # Use main config guild_id if vouch config doesn't have it
            if not guild_id:
                guild_id = self.config.get("guild_id")
                if guild_id:
                    self.vouch_config["guild_id"] = guild_id
            
            if not guild_id:
                print(f"{Colors.YELLOW}[Vouch] WARNING: guild_id not set in vouch_config.json or config.json{Colors.RESET}")
            if not vouch_channel_id:
                print(f"{Colors.YELLOW}[Vouch] WARNING: vouch_channel_id not set in vouch_config.json{Colors.RESET}")
            if not staff_role_id:
                print(f"{Colors.YELLOW}[Vouch] WARNING: staff_role_id not set in vouch_config.json{Colors.RESET}")
        
        except FileNotFoundError:
            print(f"{Colors.RED}[Vouch] ERROR: {self.vouch_config_path} not found{Colors.RESET}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"{Colors.RED}[Vouch] ERROR: Invalid JSON in {self.vouch_config_path}: {e}{Colors.RESET}")
            sys.exit(1)
    
    def load_vouches_data(self):
        """Load vouches data from JSON file"""
        try:
            if self.vouches_json_path.exists():
                with open(self.vouches_json_path, 'r', encoding='utf-8') as f:
                    self.vouches_data = json.load(f)
                # Ensure vouches list exists
                if "vouches" not in self.vouches_data:
                    self.vouches_data["vouches"] = []
                print(f"{Colors.GREEN}[Vouch] Loaded {len(self.vouches_data['vouches'])} vouches from {self.vouches_json_path}{Colors.RESET}")
            else:
                # Initialize empty structure
                self.vouches_data = {
                    "vouches": [],
                    "migrated_at": datetime.now(timezone.utc).isoformat()
                }
                self.save_vouches_data()
                print(f"{Colors.YELLOW}[Vouch] Created new vouches file: {self.vouches_json_path}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Vouch] ERROR: Failed to load vouches data: {e}{Colors.RESET}")
            sys.exit(1)
    
    def save_vouches_data(self):
        """Save vouches data to JSON file"""
        try:
            with open(self.vouches_json_path, 'w', encoding='utf-8') as f:
                json.dump(self.vouches_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"{Colors.RED}[Vouch] ERROR: Failed to save vouches data: {e}{Colors.RESET}")
    
    def get_embed_color(self, rating: int) -> discord.Color:
        """Get embed color based on rating"""
        colors = self.vouch_config.get("embed_colors", {})
        
        if rating == 5:
            color_cfg = colors.get("five_stars", {"r": 46, "g": 204, "b": 113})
        elif rating >= 3:
            color_cfg = colors.get("three_to_four_stars", {"r": 241, "g": 196, "b": 15})
        else:
            color_cfg = colors.get("one_to_two_stars", {"r": 231, "g": 76, "b": 60})
        
        return discord.Color.from_rgb(
            color_cfg.get("r", 52),
            color_cfg.get("g", 152),
            color_cfg.get("b", 219)
        )
    
    def get_list_color(self) -> discord.Color:
        """Get color for vouches list embed"""
        color_cfg = self.vouch_config.get("embed_colors", {}).get("list_color", {"r": 52, "g": 152, "b": 219})
        return discord.Color.from_rgb(
            color_cfg.get("r", 52),
            color_cfg.get("g", 152),
            color_cfg.get("b", 219)
        )
    
    def _setup_commands(self):
        """Setup slash commands"""
        
        @self.bot.tree.command(name="rsvouch", description="Leave a vouch for someone")
        async def vouch_command(interaction: discord.Interaction, user: discord.User, rating: int, comment: str):
            # Check cooldown
            cooldown_seconds = self.vouch_config.get("cooldown_seconds", 15)
            user_id = interaction.user.id
            current_time = datetime.utcnow().timestamp()
            
            if user_id in self.vouch_cooldowns:
                time_since_last_use = current_time - self.vouch_cooldowns[user_id]
                if time_since_last_use < cooldown_seconds:
                    remaining = cooldown_seconds - time_since_last_use
                    await interaction.response.send_message(
                        f"❌ You're on cooldown. Try again in {remaining:.1f} seconds.",
                        ephemeral=True
                    )
                    return
            
            # Update cooldown
            self.vouch_cooldowns[user_id] = current_time
            
            if user.id == interaction.user.id:
                await interaction.response.send_message("❌ You cannot vouch for yourself.", ephemeral=True)
                return
            
            if rating < 1 or rating > 5:
                await interaction.response.send_message("❌ Rating must be between 1 and 5 stars.", ephemeral=True)
                return
            
            # Get config values
            vouch_channel_id = self.vouch_config.get("vouch_channel_id")
            guild_id = self.vouch_config.get("guild_id")
            
            if not vouch_channel_id:
                await interaction.response.send_message("❌ Vouch channel is not configured.", ephemeral=True)
                return
            
            if not guild_id:
                await interaction.response.send_message("❌ Guild ID is not configured.", ephemeral=True)
                return
            
            # Save vouch to JSON
            now = datetime.now(timezone.utc).isoformat()
            # Generate next ID (max existing ID + 1, or 1 if empty)
            next_id = max([v.get("id", 0) for v in self.vouches_data["vouches"]], default=0) + 1
            
            vouch_entry = {
                "id": next_id,
                "vouched_user_id": str(user.id),
                "voucher_user_id": str(interaction.user.id),
                "rating": rating,
                "comment": comment,
                "timestamp": now
            }
            self.vouches_data["vouches"].append(vouch_entry)
            self.save_vouches_data()
            
            # Create embed
            color = self.get_embed_color(rating)
            embed = discord.Embed(
                title="New Vouch Received",
                color=color,
                timestamp=datetime.utcnow()
            )
            embed.add_field(name="Vouch for:", value=user.name, inline=True)
            embed.add_field(name="Author:", value=interaction.user.name, inline=True)
            embed.add_field(name="Stars:", value=f"{'⭐' * rating} ({rating})", inline=True)
            embed.add_field(name="Comment:", value=comment, inline=False)
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.set_footer(text=self.vouch_config.get("footer_text", "Reselling Secrets Vouch System"))
            
            # Post to vouch channel
            channel = self.bot.get_channel(vouch_channel_id)
            if not channel:
                await interaction.response.send_message("❌ Vouch channel not found.", ephemeral=True)
                return
            
            public_msg = await channel.send(embed=embed, view=VouchView(user, self))
            message_link = f"https://discord.com/channels/{guild_id}/{vouch_channel_id}/{public_msg.id}"
            
            # Send DM to vouched user
            try:
                dm_embed = discord.Embed(
                    title="📬 You Received a New Vouch!",
                    description=f"**{interaction.user.name}** vouched for you [here]({message_link}).",
                    color=color,
                    timestamp=datetime.utcnow()
                )
                dm_embed.add_field(name="⭐ Rating", value=f"{rating}/5")
                dm_embed.add_field(name="💬 Comment", value=comment, inline=False)
                await user.send(embed=dm_embed)
            except discord.Forbidden:
                print(f"{Colors.YELLOW}[Vouch] Could not DM user {user}{Colors.RESET}")
            
            await interaction.response.send_message("✅ Successfully vouched for the user!", ephemeral=True)
        
        @self.bot.tree.command(name="rsvouches", description="View vouches for a user")
        async def vouches_command(interaction: discord.Interaction, user: discord.User = None):
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self._vouches_logic(interaction, user, should_delete=True)
        
        @self.bot.tree.command(name="rsremovevouch", description="Remove a vouch by ID (staff only)")
        async def removevouch_command(interaction: discord.Interaction, vouch_id: int):
            staff_role_id = self.vouch_config.get("staff_role_id")
            if not staff_role_id:
                await interaction.response.send_message("❌ Staff role is not configured.", ephemeral=True)
                return
            
            if not any(role.id == staff_role_id for role in interaction.user.roles):
                await interaction.response.send_message("❌ You do not have permission to use this command.", ephemeral=True)
                return
            
            # Find and remove vouch
            vouch_found = False
            for i, vouch in enumerate(self.vouches_data["vouches"]):
                if vouch.get("id") == vouch_id:
                    self.vouches_data["vouches"].pop(i)
                    self.save_vouches_data()
                    vouch_found = True
                    break
            
            if not vouch_found:
                await interaction.response.send_message("❌ Vouch not found.", ephemeral=True)
                return
            
            await interaction.response.send_message(f"✅ Vouch ID `{vouch_id}` has been removed.", ephemeral=True)
    
    async def _vouches_logic(self, interaction: discord.Interaction, user: discord.User = None, should_delete=True):
        """Internal logic for displaying vouches"""
        user = user or interaction.user
        guild_id = self.vouch_config.get("guild_id")
        
        if not guild_id:
            await interaction.channel.send("❌ Guild ID is not configured.")
            if should_delete:
                await interaction.delete_original_response()
            return
        
        # Get vouches for user from JSON
        user_id_str = str(user.id)
        results = []
        for vouch in self.vouches_data["vouches"]:
            if vouch.get("vouched_user_id") == user_id_str:
                results.append((
                    vouch.get("rating", 0),
                    vouch.get("comment", ""),
                    vouch.get("voucher_user_id", ""),
                    vouch.get("id", 0),
                    vouch.get("timestamp", "")
                ))
        
        if not results:
            await interaction.channel.send(f"❌ No vouches found for {user.name}.")
            if should_delete:
                await interaction.delete_original_response()
            return
        
        guild = self.bot.get_guild(guild_id)
        user_names = {}
        
        for row in results:
            uid = str(row[2])
            if uid not in user_names:
                try:
                    if guild:
                        member = await guild.fetch_member(int(uid))
                        user_names[uid] = member.name
                    else:
                        fetched_user = await self.bot.fetch_user(int(uid))
                        user_names[uid] = fetched_user.name
                except discord.NotFound:
                    user_names[uid] = f"User {uid}"
                except Exception as e:
                    print(f"{Colors.YELLOW}[Vouch] Could not fetch member {uid}: {e}{Colors.RESET}")
                    user_names[uid] = f"User {uid}"
        
        total_rating = sum(row[0] for row in results)
        avg_rating = total_rating / len(results)
        max_display = self.vouch_config.get("max_vouches_displayed", 10)
        
        embed = discord.Embed(
            title=f"📋 Vouches for {user}",
            description=f"⭐ Average Rating: **{avg_rating:.2f}/5**\n📦 Total Vouches: **{len(results)}**",
            color=self.get_list_color()
        )
        
        for row in results[:max_display]:
            rating, comment, voucher_user_id, vouch_id, timestamp = row
            name = user_names.get(str(voucher_user_id), f"User {voucher_user_id}")
            date_str = datetime.fromisoformat(timestamp).strftime("%b %d, %Y")
            embed.add_field(
                name=f"From {name} — {rating}/5 ⭐ (ID: {vouch_id})",
                value=f"{comment}\n*{date_str}*",
                inline=False
            )
        
        if len(results) > max_display:
            embed.set_footer(text=f"Showing {max_display} of {len(results)} vouches")
        
        await interaction.channel.send(embed=embed)
        if should_delete:
            await interaction.delete_original_response()
    
    def run(self):
        """Run the bot (only for standalone mode)"""
        if self._is_shared_bot:
            print(f"{Colors.RED}[Vouch] ERROR: Cannot run in standalone mode when using shared bot instance{Colors.RESET}")
            return
        
        token = self.config.get("bot_token")
        if not token:
            print(f"{Colors.RED}[Vouch] ERROR: bot_token not found in config.json{Colors.RESET}")
            sys.exit(1)
        
        try:
            self.bot.run(token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Vouch] Shutting down...{Colors.RESET}")
        finally:
            # Save data before shutdown
            self.save_vouches_data()
            print(f"{Colors.GREEN}[Vouch] Vouches data saved{Colors.RESET}")


def main():
    """Main entry point for standalone mode"""
    bot = RSVouchBot()
    bot.run()


if __name__ == "__main__":
    main()
