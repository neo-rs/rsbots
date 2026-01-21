"""
Bot Movement Tracker

Monitors RS Bots' activities in RS Server (read/write operations).
Stores per-bot movement data in separate JSON files.
"""

import json
import os
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from collections import defaultdict

import discord
from discord.ext import commands


class BotMovementTracker:
    """Tracks RS Bots' activities in RS Server and reports to test server channels."""
    
    def __init__(self, bot: commands.Bot, bots_dict: Dict[str, Dict], config: Dict[str, any], test_server_organizer=None):
        """
        Initialize BotMovementTracker.
        
        Args:
            bot: Discord bot instance
            bots_dict: BOTS dictionary from admin_bot.BOTS
            config: Configuration dictionary with rs_server_guild_id
            test_server_organizer: Optional TestServerOrganizer instance for sending reports
        """
        self.bot = bot
        self.bots_dict = bots_dict
        self.config = config
        self.rs_server_guild_id = config.get("rs_server_guild_id", 876528050081251379)
        self.tracking_enabled = config.get("bot_movement_tracking_enabled", True)
        self.test_server_organizer = test_server_organizer
        
        # Data directory
        self.data_dir = Path(__file__).parent / "whop_data" / "bot_movements"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Track bot user IDs (will be populated when bot is ready)
        self.bot_user_ids: Set[int] = set()
        self.bot_id_to_name: Dict[int, str] = {}
        
        # Cache for bot movements (loaded from files)
        self.movements_cache: Dict[str, List] = {}
        
        # Track last report time per bot (to avoid spam)
        self.last_report_time: Dict[str, datetime] = {}
        self.report_interval_seconds = 300  # Report every 5 minutes max per bot
    
    async def initialize_bot_ids(self):
        """Initialize bot user IDs by discovering bots in the RS Server guild.

        This intentionally does NOT read any local token files. Bot IDs are non-secret and should be
        derived from Discord state (guild members) to avoid storing tokens or duplicating sources.
        """
        if not self.tracking_enabled:
            return
        
        initialized_count = 0

        print(f"[BotMovementTracker] Fetching bot IDs from RS Server...")
        guild = self.bot.get_guild(self.rs_server_guild_id)
        if not guild:
            print(f"[BotMovementTracker] RS Server guild {self.rs_server_guild_id} not found")
            return

        try:
            # Build flexible matching patterns for each bot
            bot_patterns = {}
            for bot_key, bot_info in self.bots_dict.items():
                bot_name = bot_info.get("name", "").lower()
                bot_key_lower = bot_key.lower()
                # Create multiple matching patterns
                patterns = [
                    bot_name,  # Full name: "rs onboarding"
                    bot_name.replace(" ", ""),  # No spaces: "rsonboarding"
                    bot_key_lower,  # Key: "rsonboarding"
                    bot_name.replace("rs ", ""),  # Without "rs ": "onboarding"
                ]
                bot_patterns[bot_key] = patterns

            async for member in guild.fetch_members(limit=None):
                if member.bot:
                    # Try to match bot name to our BOTS dict
                    member_name = member.name.lower()
                    member_display = member.display_name.lower()

                    for bot_key, patterns in bot_patterns.items():
                        # Check if member name matches any pattern
                        for pattern in patterns:
                            if (
                                pattern
                                and (
                                    pattern in member_name
                                    or member_name in pattern
                                    or pattern in member_display
                                    or member_display in pattern
                                )
                            ):
                                self.bot_user_ids.add(member.id)
                                self.bot_id_to_name[member.id] = bot_key
                                initialized_count += 1
                                print(f"[BotMovementTracker] ✓ Matched bot: {member.name} (ID: {member.id}) -> {bot_key}")
                                break
                        else:
                            continue
                        break
        except Exception as e:
            print(f"[BotMovementTracker] Error fetching bot IDs from guild: {e}")
            import traceback
            print(f"[BotMovementTracker] Traceback: {traceback.format_exc()[:300]}")
        
        print(f"[BotMovementTracker] Initialized {initialized_count} bot ID(s) for tracking")
    
    def _get_bot_name_from_id(self, user_id: int) -> Optional[str]:
        """Get bot name from user ID."""
        return self.bot_id_to_name.get(user_id)
    
    def _is_tracked_bot(self, user_id: int) -> bool:
        """Check if user ID belongs to a tracked bot."""
        return user_id in self.bot_user_ids
    
    def _load_bot_movements(self, bot_name: str) -> List:
        """Load movements for a specific bot."""
        if bot_name in self.movements_cache:
            return self.movements_cache[bot_name]
        
        bot_file = self.data_dir / f"{bot_name}_movements.json"
        if bot_file.exists():
            try:
                with open(bot_file, 'r', encoding='utf-8') as f:
                    movements = json.load(f)
                    self.movements_cache[bot_name] = movements
                    return movements
            except (json.JSONDecodeError, IOError):
                pass
        
        self.movements_cache[bot_name] = []
        return []
    
    def _save_bot_movements(self, bot_name: str, movements: List):
        """Save movements for a specific bot."""
        bot_file = self.data_dir / f"{bot_name}_movements.json"
        
        # Keep only last 10000 movements per bot
        movements = movements[-10000:]
        
        try:
            with open(bot_file, 'w', encoding='utf-8') as f:
                json.dump(movements, f, indent=2, ensure_ascii=False)
            self.movements_cache[bot_name] = movements
        except IOError as e:
            print(f"[BotMovementTracker] Error saving movements for {bot_name}: {e}")
    
    def _record_movement(self, bot_name: str, action: str, message: discord.Message, 
                        additional_details: Dict = None):
        """Record a bot movement."""
        if not self.tracking_enabled:
            return
        
        # Only track messages in RS Server
        if message.guild and message.guild.id != self.rs_server_guild_id:
            return
        
        # Don't track RSAdminBot's own messages
        if message.author.id == self.bot.user.id:
            return
        
        movement = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,  # 'read', 'write', 'edit', 'delete'
            "channel_id": message.channel.id if message.channel else None,
            "channel_name": getattr(message.channel, 'name', 'unknown'),
            "guild_id": message.guild.id if message.guild else None,
            "guild_name": message.guild.name if message.guild else 'unknown',
            "message_id": message.id,
            "details": {
                "content_length": len(message.content) if message.content else 0,
                "has_embed": len(message.embeds) > 0,
                "attachment_count": len(message.attachments),
                **(additional_details or {})
            }
        }
        
        # Load existing movements
        movements = self._load_bot_movements(bot_name)
        movements.append(movement)
        
        # Save movements
        self._save_bot_movements(bot_name, movements)
        
        # Send activity report to test server channel (rate-limited, async)
        if self.test_server_organizer:
            asyncio.create_task(self._send_activity_report(bot_name, movement))
    
    async def track_message(self, message: discord.Message):
        """Track a message event (write operation)."""
        if not self.tracking_enabled:
            return
        
        # Only track in RS Server
        if not message.guild:
            return
        
        if message.guild.id != self.rs_server_guild_id:
            return
        
        # Check if message author is a tracked bot
        if message.author.bot:
            # Check if we know this bot ID
            if self._is_tracked_bot(message.author.id):
                bot_name = self._get_bot_name_from_id(message.author.id)
                if bot_name:
                    self._record_movement(bot_name, "write", message)
            else:
                # Try to match on-the-fly if not initialized yet
                # This helps catch bots that weren't matched during initialization
                member_name = message.author.name.lower()
                member_display = message.author.display_name.lower() if hasattr(message.author, 'display_name') else ""
                
                for bot_key, bot_info in self.bots_dict.items():
                    bot_name = bot_info.get("name", "").lower()
                    bot_key_lower = bot_key.lower()
                    patterns = [
                        bot_name,
                        bot_name.replace(" ", ""),
                        bot_key_lower,
                        bot_name.replace("rs ", ""),
                    ]
                    
                    for pattern in patterns:
                        if (pattern in member_name or member_name in pattern or
                            (member_display and (pattern in member_display or member_display in pattern))):
                            # Found a match - add to tracking
                            self.bot_user_ids.add(message.author.id)
                            self.bot_id_to_name[message.author.id] = bot_key
                            self._record_movement(bot_key, "write", message)
                            print(f"[BotMovementTracker] On-the-fly match: {message.author.name} ({message.author.id}) -> {bot_key}")
                            return
    
    async def track_message_edit(self, before: discord.Message, after: discord.Message):
        """Track a message edit event."""
        if not self.tracking_enabled:
            return
        
        # Only track in RS Server
        if not after.guild or after.guild.id != self.rs_server_guild_id:
            return
        
        # Check if message author is a tracked bot
        if after.author.bot and self._is_tracked_bot(after.author.id):
            bot_name = self._get_bot_name_from_id(after.author.id)
            if bot_name:
                details = {
                    "before_content_length": len(before.content) if before.content else 0,
                    "after_content_length": len(after.content) if after.content else 0,
                    "content_changed": before.content != after.content
                }
                self._record_movement(bot_name, "edit", after, details)
    
    async def track_message_delete(self, message: discord.Message):
        """Track a message delete event."""
        if not self.tracking_enabled:
            return
        
        # Only track in RS Server
        if not message.guild or message.guild.id != self.rs_server_guild_id:
            return
        
        # Check if message author is a tracked bot
        if message.author and message.author.bot and self._is_tracked_bot(message.author.id):
            bot_name = self._get_bot_name_from_id(message.author.id)
            if bot_name:
                self._record_movement(bot_name, "delete", message)
    
    def get_bot_movements(self, bot_name: str, limit: int = 100) -> List:
        """Get movement history for a specific bot."""
        movements = self._load_bot_movements(bot_name)
        return movements[-limit:] if limit else movements
    
    def get_bot_stats(self, bot_name: str) -> Dict:
        """Get statistics for a specific bot."""
        movements = self._load_bot_movements(bot_name)
        
        if not movements:
            return {
                "bot_name": bot_name,
                "total_movements": 0,
                "by_action": {},
                "by_channel": {}
            }
        
        by_action = defaultdict(int)
        by_channel = defaultdict(int)
        
        for movement in movements:
            action = movement.get("action", "unknown")
            channel_name = movement.get("channel_name", "unknown")
            by_action[action] += 1
            by_channel[channel_name] += 1
        
        return {
            "bot_name": bot_name,
            "total_movements": len(movements),
            "by_action": dict(by_action),
            "by_channel": dict(by_channel),
            "last_activity": movements[-1].get("timestamp") if movements else None
        }
    
    async def _send_activity_report(self, bot_name: str, movement: Dict):
        """Send bot activity report to test server channel (rate-limited)."""
        if not self.test_server_organizer:
            return
        
        # Rate limiting - only send report every N seconds per bot
        now = datetime.now(timezone.utc)
        last_report = self.last_report_time.get(bot_name)
        if last_report:
            time_since_last = (now - last_report).total_seconds()
            if time_since_last < self.report_interval_seconds:
                return  # Too soon, skip
        
        self.last_report_time[bot_name] = now
        
        try:
            # Get bot stats for comparison
            stats = self.get_bot_stats(bot_name)
            bot_display_name = self.bots_dict.get(bot_name, {}).get("name", bot_name)
            
            # Create embed
            embed = discord.Embed(
                title=f"🤖 {bot_display_name} Activity",
                color=discord.Color.blue(),
                timestamp=datetime.fromisoformat(movement["timestamp"].replace('Z', '+00:00'))
            )
            
            embed.add_field(
                name="Action",
                value=movement["action"].upper(),
                inline=True
            )
            
            embed.add_field(
                name="Channel",
                value=f"#{movement['channel_name']}",
                inline=True
            )
            
            embed.add_field(
                name="Total Movements",
                value=str(stats.get("total_movements", 0)),
                inline=True
            )
            
            # Add action breakdown
            by_action = stats.get("by_action", {})
            if by_action:
                action_text = "\n".join([f"**{k}**: {v}" for k, v in sorted(by_action.items(), key=lambda x: x[1], reverse=True)[:5]])
                embed.add_field(
                    name="Activity Breakdown",
                    value=action_text,
                    inline=False
                )
            
            # Send to per-bot journal/monitor channel in TestCenter
            await self.test_server_organizer.send_to_channel(str(bot_name), embed=embed)
            
        except Exception as e:
            # Don't spam errors - just log once
            if not hasattr(self, '_report_error_logged'):
                print(f"[BotMovementTracker] Error sending activity report: {e}")
                self._report_error_logged = True

