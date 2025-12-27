"""
Test Server Organizer

Auto-creates categories and channels in TEST SERVER for RSAdminBot organization.
"""

import json
from pathlib import Path
from typing import Dict, Optional
import hashlib

import discord
from discord.ext import commands


class TestServerOrganizer:
    """Organizes test server with categories and channels for monitoring."""
    
    def __init__(self, bot: commands.Bot, config: Dict[str, any], bots_dict: Dict[str, Dict]):
        """
        Initialize TestServerOrganizer.
        
        Args:
            bot: Discord bot instance
            config: Configuration dictionary with test_server_guild_id
            bots_dict: BOTS dictionary from admin_bot.BOTS
        """
        self.bot = bot
        self.config = config
        self.bots_dict = bots_dict
        self.test_server_guild_id = config.get("test_server_guild_id", 1451275225512546497)
        
        # Data directory
        self.data_dir = Path(__file__).parent / "whop_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Channels file
        self.channels_file = self.data_dir / "test_server_channels.json"
        self.channels_data = self._load_channels_data()
    
    def _load_channels_data(self) -> Dict:
        """Load created channels data."""
        if self.channels_file.exists():
            try:
                with open(self.channels_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {
            "category_id": None,
            "channels": {}
        }
    
    def _save_channels_data(self):
        """Save channels data."""
        try:
            with open(self.channels_file, 'w', encoding='utf-8') as f:
                json.dump(self.channels_data, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"[TestServerOrganizer] Error saving channels data: {e}")

    def _ensure_meta(self):
        if "meta" not in self.channels_data or not isinstance(self.channels_data.get("meta"), dict):
            self.channels_data["meta"] = {}

    def get_meta(self, key: str, default=None):
        self._ensure_meta()
        return self.channels_data["meta"].get(key, default)

    def set_meta(self, key: str, value) -> None:
        self._ensure_meta()
        self.channels_data["meta"][key] = value
        self._save_channels_data()

    @staticmethod
    def _sha256_text(text: str) -> str:
        return hashlib.sha256((text or "").encode("utf-8", errors="replace")).hexdigest()
    
    async def setup_monitoring_channels(self) -> Dict:
        """
        Setup monitoring categories and channels in test server.
        
        Returns:
            Dictionary with created channel IDs
        """
        guild = self.bot.get_guild(self.test_server_guild_id)
        if not guild:
            return {"error": f"Test server guild {self.test_server_guild_id} not found"}
        
        result = {
            "category_id": None,
            "channels": {}
        }
        
        # Check if category already exists (by ID first, then by name to prevent duplicates)
        category_id = self.channels_data.get("category_id")
        category = None
        
        if category_id:
            category = guild.get_channel(category_id)
        
        # If category not found by ID, check by name to prevent duplicates
        if not category:
            for existing_category in guild.categories:
                if existing_category.name == "RS Bot Monitoring":
                    category = existing_category
                    # Update stored ID to match existing category
                    self.channels_data["category_id"] = category.id
                    result["category_id"] = category.id
                    break
        
        # Create category if it doesn't exist
        if not category:
            try:
                category = await guild.create_category("RS Bot Monitoring")
                result["category_id"] = category.id
                self.channels_data["category_id"] = category.id
            except discord.Forbidden:
                return {"error": "Missing permissions to create category"}
            except Exception as e:
                return {"error": f"Failed to create category: {e}"}
        
        # Create main channels
        main_channels = {
            "whop_logs": "Whop Logs",
            "bot_activities": "Bot Activities",
            "commands": "RSAdminBot Commands"
        }
        
        for channel_key, channel_name in main_channels.items():
            # Check if channel already exists
            existing_channel_id = self.channels_data.get("channels", {}).get(channel_key)
            if existing_channel_id:
                existing_channel = guild.get_channel(existing_channel_id)
                if existing_channel:
                    result["channels"][channel_key] = existing_channel_id
                    continue
            
            # Create channel
            try:
                channel = await category.create_text_channel(channel_name)
                result["channels"][channel_key] = channel.id
                if "channels" not in self.channels_data:
                    self.channels_data["channels"] = {}
                self.channels_data["channels"][channel_key] = channel.id
            except discord.Forbidden:
                result["error"] = f"Missing permissions to create channel: {channel_name}"
            except Exception as e:
                result["error"] = f"Failed to create channel {channel_name}: {e}"
        
        # Create per-bot activity channels
        for bot_key, bot_info in self.bots_dict.items():
            bot_display_name = bot_info.get("name", bot_key)
            channel_name = f"{bot_display_name} Activity"
            
            # Check if channel already exists
            existing_channel_id = self.channels_data.get("channels", {}).get(f"{bot_key}_activity")
            if existing_channel_id:
                existing_channel = guild.get_channel(existing_channel_id)
                if existing_channel:
                    result["channels"][f"{bot_key}_activity"] = existing_channel_id
                    continue
            
            # Create channel
            try:
                channel = await category.create_text_channel(channel_name)
                result["channels"][f"{bot_key}_activity"] = channel.id
                if "channels" not in self.channels_data:
                    self.channels_data["channels"] = {}
                self.channels_data["channels"][f"{bot_key}_activity"] = channel.id
            except discord.Forbidden:
                if "error" not in result:
                    result["error"] = f"Missing permissions to create bot channels"
            except Exception as e:
                if "error" not in result:
                    result["error"] = f"Failed to create bot channels: {e}"
        
        # Save channels data
        self._save_channels_data()
        
        return result
    
    def get_channel_id(self, channel_key: str) -> Optional[int]:
        """Get channel ID by key."""
        return self.channels_data.get("channels", {}).get(channel_key)
    
    async def send_to_channel(self, channel_key: str, content: str = None, embed: discord.Embed = None):
        """Send message to a monitoring channel."""
        channel_id = self.get_channel_id(channel_key)
        if not channel_id:
            return False
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return False
        
        try:
            await channel.send(content=content, embed=embed)
            return True
        except Exception as e:
            print(f"[TestServerOrganizer] Error sending to {channel_key}: {e}")
            return False

