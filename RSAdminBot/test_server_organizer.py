"""
Test Server Organizer

Auto-creates categories and channels in TEST SERVER for RSAdminBot organization.
"""

import json
from pathlib import Path
from typing import Dict, Optional, List
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
        key = str(channel_key or "").strip()
        if not key:
            return None
        for bucket in ("channels", "monitor_channels", "journal_channels"):
            data = self.channels_data.get(bucket, {})
            if isinstance(data, dict) and key in data:
                return data.get(key)
        return None
    
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
    
    async def ensure_monitor_category_and_bot_channels(self, rs_bot_keys: List[str]) -> Dict[str, int]:
        """
        Returns mapping {bot_key: channel_id} for per-bot monitor channels in test server.
        Creates category + channels only in test server, idempotent.
        
        Args:
            rs_bot_keys: List of bot keys (e.g., ["rsforwarder", "rsonboarding", ...])
            
        Returns:
            Dict mapping bot_key to channel_id
        """
        # Config gate
        cfg = self.config.get("monitor_channels") if isinstance(self.config, dict) else {}
        if not isinstance(cfg, dict) or not cfg.get("enabled"):
            return {}
        
        # Hard guard: only create in test server
        test_guild_id = cfg.get("test_server_guild_id")
        if not test_guild_id:
            return {}
        
        test_guild_id = int(test_guild_id)
        
        guild = self.bot.get_guild(test_guild_id)
        if not guild:
            return {}
        
        # Double-check we're in the right guild
        if guild.id != test_guild_id:
            return {}
        
        category_name = cfg.get("category_name", "RS Bots Terminal Logs")
        channel_prefix = cfg.get("channel_prefix", "bot-")
        
        result = {}
        
        # Ensure category exists - use discord.utils.get
        category_id = self.channels_data.get("monitor_category_id")
        category = None
        
        if category_id:
            category = guild.get_channel(category_id)
        
        if not category:
            # Search for existing category by name using discord.utils.get
            category = discord.utils.get(guild.categories, name=category_name)
            if category:
                category_id = category.id
                self.channels_data["monitor_category_id"] = category_id
                self._save_channels_data()
        
        if not category:
            # Create category
            try:
                category = await guild.create_category(category_name, reason="RSAdminBot monitor channels")
                category_id = category.id
                self.channels_data["monitor_category_id"] = category_id
                self._save_channels_data()
            except discord.Forbidden:
                print(f"[TestServerOrganizer] Missing permission to create category: {category_name}")
                return {}
            except Exception as e:
                print(f"[TestServerOrganizer] Error creating category: {e}")
                return {}
        
        # Ensure per-bot channels exist
        if "monitor_channels" not in self.channels_data:
            self.channels_data["monitor_channels"] = {}
        
        for bot_key in rs_bot_keys:
            channel_name = f"{channel_prefix}{bot_key}".lower()
            
            # Check if we already have this channel ID
            existing_channel_id = self.channels_data["monitor_channels"].get(bot_key)
            if existing_channel_id:
                existing_channel = guild.get_channel(existing_channel_id)
                if existing_channel:
                    result[bot_key] = existing_channel_id
                    continue
            
            # Search for existing channel by name in category using discord.utils.get
            found = discord.utils.get(guild.text_channels, name=channel_name, category=category)
            
            if not found:
                # Create channel in category
                try:
                    found = await guild.create_text_channel(channel_name, category=category, reason="RSAdminBot per-bot monitor channel")
                except discord.Forbidden:
                    print(f"[TestServerOrganizer] Missing permission to create channel: {channel_name}")
                    continue
                except Exception as e:
                    print(f"[TestServerOrganizer] Error creating channel {channel_name}: {e}")
                    continue
            
            if found:
                result[bot_key] = found.id
                self.channels_data["monitor_channels"][bot_key] = found.id
                self._save_channels_data()
        
        return result

    async def ensure_journal_channels_in_category(self, rs_bot_keys: List[str]) -> Dict[str, int]:
        """Ensure per-bot journal channels exist inside a configured test-server category.

        Hard rules:
        - Test server only (guild must match configured test_server_guild_id)
        - Do not create categories here (category must already exist)
        - Idempotent: reuse by stored IDs or existing channel names to avoid duplicates
        """
        cfg = self.config.get("journal_live") if isinstance(self.config, dict) else {}
        if not isinstance(cfg, dict) or not cfg.get("enabled"):
            return {}

        if not self.test_server_guild_id:
            return {}

        guild = self.bot.get_guild(self.test_server_guild_id)
        if not guild or guild.id != self.test_server_guild_id:
            return {}

        category_id = cfg.get("category_id")
        if not category_id:
            return {}
        try:
            category_id = int(category_id)
        except Exception:
            return {}

        category = guild.get_channel(category_id)
        if not category or not isinstance(category, discord.CategoryChannel):
            # Do not create categories; fail quietly.
            return {}

        channel_prefix = str(cfg.get("channel_prefix") or "journal-")

        if "journal_channels" not in self.channels_data:
            self.channels_data["journal_channels"] = {}

        result: Dict[str, int] = {}
        for bot_key in rs_bot_keys:
            channel_name = f"{channel_prefix}{bot_key}".lower()

            # 1) Stored ID
            existing_id = self.channels_data["journal_channels"].get(bot_key)
            if existing_id:
                ch = guild.get_channel(int(existing_id))
                if ch and isinstance(ch, discord.TextChannel):
                    result[bot_key] = ch.id
                    continue

            # 2) Search by name within the category
            found = discord.utils.get(guild.text_channels, name=channel_name, category=category)
            # 3) Same name anywhere in guild (channel was moved out of the journal category)
            if not found:
                found = discord.utils.get(guild.text_channels, name=channel_name)
            if not found:
                try:
                    found = await guild.create_text_channel(
                        channel_name,
                        category=category,
                        reason="RSAdminBot per-bot journal channel (test server only)",
                    )
                except discord.Forbidden:
                    continue
                except Exception:
                    continue

            result[bot_key] = found.id
            self.channels_data["journal_channels"][bot_key] = found.id
            self._save_channels_data()

        # Optional second journal channel for MWDiscumBot: fetchall/fetchsync lines only (D2D stays on journal-discumbot).
        if isinstance(cfg, dict) and cfg.get("discumbot_split_fetch_journal"):
            if "discumbot" in result:
                fetch_key = "discumbot_fetch"
                fetch_name = f"{channel_prefix}discumbot-fetch".lower()

                have_fetch = False
                existing_fetch_id = self.channels_data["journal_channels"].get(fetch_key)
                if existing_fetch_id:
                    ch = guild.get_channel(int(existing_fetch_id))
                    if ch and isinstance(ch, discord.TextChannel):
                        result[fetch_key] = ch.id
                        have_fetch = True

                if not have_fetch:
                    found = discord.utils.get(guild.text_channels, name=fetch_name, category=category)
                    if not found:
                        found = discord.utils.get(guild.text_channels, name=fetch_name)
                    if not found:
                        try:
                            found = await guild.create_text_channel(
                                fetch_name,
                                category=category,
                                reason="RSAdminBot MWDiscumBot fetch journal (test server only)",
                            )
                        except discord.Forbidden:
                            found = None
                        except Exception:
                            found = None

                    if found:
                        result[fetch_key] = found.id
                        self.channels_data["journal_channels"][fetch_key] = found.id
                        self._save_channels_data()

        # MWDataManagerBot: optional per-stream journals (matches MWDataManagerBot stdout `stream=...` lines).
        if isinstance(cfg, dict) and cfg.get("datamanager_split_stream_journals") and "datamanagerbot" in result:
            raw_streams = cfg.get("datamanager_journal_streams")
            if not isinstance(raw_streams, list):
                raw_streams = []
            for stream_slug in raw_streams:
                s = str(stream_slug or "").strip().lower()
                if not s or not all(c.isalnum() or c in "-_" for c in s):
                    continue
                s = s.replace("-", "_")
                map_key = f"datamanagerbot_{s}"
                ch_slug = s.replace("_", "-")
                channel_name = f"{channel_prefix}datamanagerbot-{ch_slug}".lower()

                have = False
                existing_id = self.channels_data["journal_channels"].get(map_key)
                if existing_id:
                    ch = guild.get_channel(int(existing_id))
                    if ch and isinstance(ch, discord.TextChannel):
                        result[map_key] = ch.id
                        have = True

                if not have:
                    found = discord.utils.get(guild.text_channels, name=channel_name, category=category)
                    if not found:
                        found = discord.utils.get(guild.text_channels, name=channel_name)
                    if not found:
                        try:
                            found = await guild.create_text_channel(
                                channel_name,
                                category=category,
                                reason="RSAdminBot MWDataManagerBot stream journal (test server only)",
                            )
                        except discord.Forbidden:
                            found = None
                        except Exception:
                            found = None

                    if found:
                        result[map_key] = found.id
                        self.channels_data["journal_channels"][map_key] = found.id
                        self._save_channels_data()

        # Instorebotforwarder: optional per-watched-source journals (RSAdmin maps source_channel_id -> slug).
        if isinstance(cfg, dict) and cfg.get("instorebotforwarder_split_source_journals") and "instorebotforwarder" in result:
            raw_sources = cfg.get("instorebotforwarder_journal_sources")
            if not isinstance(raw_sources, list):
                raw_sources = []
            for stream_slug in raw_sources:
                s = str(stream_slug or "").strip().lower()
                if not s or not all(c.isalnum() or c in "-_" for c in s):
                    continue
                s = s.replace("-", "_")
                map_key = f"instorebotforwarder_{s}"
                ch_slug = s.replace("_", "-")
                channel_name = f"{channel_prefix}instorebotforwarder-{ch_slug}".lower()

                have = False
                existing_id = self.channels_data["journal_channels"].get(map_key)
                if existing_id:
                    ch = guild.get_channel(int(existing_id))
                    if ch and isinstance(ch, discord.TextChannel):
                        result[map_key] = ch.id
                        have = True

                if not have:
                    found = discord.utils.get(guild.text_channels, name=channel_name, category=category)
                    if not found:
                        found = discord.utils.get(guild.text_channels, name=channel_name)
                    if not found:
                        try:
                            found = await guild.create_text_channel(
                                channel_name,
                                category=category,
                                reason="RSAdminBot Instorebotforwarder source journal (test server only)",
                            )
                        except discord.Forbidden:
                            found = None
                        except Exception:
                            found = None

                    if found:
                        result[map_key] = found.id
                        self.channels_data["journal_channels"][map_key] = found.id
                        self._save_channels_data()

        # RSCheckerbot: optional per-flow journals (matches stdout `[RSCheckerbot][FLOW]` from rschecker_journal).
        if isinstance(cfg, dict) and cfg.get("rscheckerbot_split_flow_journals") and "rscheckerbot" in result:
            raw_flows = cfg.get("rscheckerbot_journal_flows")
            if not isinstance(raw_flows, list):
                raw_flows = []
            for flow_slug in raw_flows:
                s = str(flow_slug or "").strip().lower()
                if not s or not all(c.isalnum() or c in "-_" for c in s):
                    continue
                s = s.replace("-", "_")
                map_key = f"rscheckerbot_{s}"
                ch_slug = s.replace("_", "-")
                channel_name = f"{channel_prefix}rscheckerbot-{ch_slug}".lower()

                have = False
                existing_id = self.channels_data["journal_channels"].get(map_key)
                if existing_id:
                    ch = guild.get_channel(int(existing_id))
                    if ch and isinstance(ch, discord.TextChannel):
                        result[map_key] = ch.id
                        have = True

                if not have:
                    found = discord.utils.get(guild.text_channels, name=channel_name, category=category)
                    if not found:
                        found = discord.utils.get(guild.text_channels, name=channel_name)
                    if not found:
                        try:
                            found = await guild.create_text_channel(
                                channel_name,
                                category=category,
                                reason="RSAdminBot RSCheckerbot flow journal (test server only)",
                            )
                        except discord.Forbidden:
                            found = None
                        except Exception:
                            found = None

                    if found:
                        result[map_key] = found.id
                        self.channels_data["journal_channels"][map_key] = found.id
                        self._save_channels_data()

        return result

