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

        # Avoid hardcoded guild IDs. Require explicit config.
        gid = None
        try:
            if isinstance(config, dict):
                raw = config.get("test_server_guild_id")
                if raw is not None and str(raw).strip():
                    gid = int(raw)
        except Exception:
            gid = None
        self.test_server_guild_id = gid
        
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
        Setup monitoring channels in test server.

        Current policy (commands-only):
        - Do NOT create categories
        - Do NOT create per-bot channels
        - Only ensure the Commands index channel exists (idempotent)
        
        Returns:
            Dictionary with created channel IDs
        """
        # Config gate
        cfg = self.config.get("commands_index") if isinstance(self.config, dict) else {}
        if isinstance(cfg, dict) and cfg.get("enabled") is False:
            return {"skipped": True, "reason": "commands_index.enabled=false"}

        if not self.test_server_guild_id:
            return {"skipped": True, "reason": "missing test_server_guild_id"}

        guild = self.bot.get_guild(self.test_server_guild_id)
        if not guild:
            return {"error": f"Test server guild {self.test_server_guild_id} not found"}
        
        result = {
            "category_id": None,
            "channels": {}
        }

        # Only ensure commands channel
        channel_key = "commands"
        # Discord normalizes channel names (spaces -> hyphens, lowercase). We search both forms.
        channel_display = "RSAdminBot Commands"
        channel_slug = "rsadminbot-commands"

        existing_channel_id = self.channels_data.get("channels", {}).get(channel_key)
        if existing_channel_id:
            existing = guild.get_channel(existing_channel_id)
            if existing:
                result["channels"][channel_key] = existing_channel_id
                return result

        # Search by name to avoid duplicates
        found = None
        for ch in guild.text_channels:
            if ch.name in (channel_display, channel_slug):
                found = ch
                break

        if found is None:
            try:
                found = await guild.create_text_channel(channel_slug)
            except discord.Forbidden:
                return {"error": "Missing permissions to create commands channel"}
            except Exception as e:
                return {"error": f"Failed to create commands channel: {e}"}

        result["channels"][channel_key] = found.id
        self.channels_data.setdefault("channels", {})[channel_key] = found.id
        
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

