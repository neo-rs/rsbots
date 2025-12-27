#!/usr/bin/env python3
"""
Standalone History Scanner for RSuccessBot
Scans Discord message history to recover all points data and update JSON
"""
import json
import sys
import asyncio
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

import discord
from discord.ext import commands

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

class HistoryScanner:
    """Scanner to extract points from Discord message history"""
    
    def __init__(self, config_path: Path, json_path: Path):
        self.config_path = config_path
        self.json_path = json_path
        self.config: Dict[str, Any] = {}
        self.json_data: Dict[str, Any] = {}
        
        # Load config
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        # Load existing JSON data
        if json_path.exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                self.json_data = json.load(f)
        else:
            self.json_data = {
                "points": {},
                "image_hashes": {},
                "point_movements": [],
                "migrated_at": datetime.now().isoformat()
            }
        
        # Setup bot
        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        
        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self.setup_events()
    
    def setup_events(self):
        """Setup bot events"""
        
        @self.bot.event
        async def on_ready():
            print(f"{Colors.GREEN}[Scanner] Logged in as {self.bot.user}{Colors.RESET}")
            print(f"{Colors.CYAN}[Scanner] Starting history scan...{Colors.RESET}")
            await self.scan_all_channels()
            await self.bot.close()
    
    async def scan_all_channels(self):
        """Scan all success channels for points data"""
        guild_id = self.config.get("guild_id")
        success_channel_ids = self.config.get("success_channel_ids", [])
        
        if not guild_id:
            print(f"{Colors.RED}[Scanner] ERROR: guild_id not configured{Colors.RESET}")
            return
        
        guild = self.bot.get_guild(guild_id)
        if not guild:
            print(f"{Colors.RED}[Scanner] ERROR: Could not find guild {guild_id}{Colors.RESET}")
            return
        
        points_count: Dict[int, Dict[str, Any]] = {}
        total_messages = 0
        bot_messages_found = 0
        
        print(f"{Colors.CYAN}[Scanner] Scanning {len(success_channel_ids)} channel(s)...{Colors.RESET}")
        
        for channel_idx, channel_id in enumerate(success_channel_ids, 1):
            channel = guild.get_channel(channel_id)
            if not channel or not hasattr(channel, 'history'):
                print(f"{Colors.YELLOW}[Scanner] WARN: Channel {channel_id} not found or not accessible{Colors.RESET}")
                continue
            
            print(f"{Colors.BLUE}[Scanner] [{channel_idx}/{len(success_channel_ids)}] Scanning channel: {channel.name} ({channel_id})...{Colors.RESET}")
            
            try:
                async for message in channel.history(limit=None, oldest_first=True):
                    total_messages += 1
                    
                    # Check for bot award messages
                    if message.author.bot:
                        text_to_check = ""
                        
                        # Extract text from embeds
                        if message.embeds:
                            for embed in message.embeds:
                                desc = embed.description or ""
                                text_to_check += desc + " "
                                if "congratulations" in desc.lower() and "point" in desc.lower():
                                    bot_messages_found += 1
                        
                        # Add message content
                        text_to_check += message.content or ""
                        
                        # Check if this is a points award message
                        if "point" in text_to_check.lower() and (
                            "awarded" in text_to_check.lower() or 
                            "congratulations" in text_to_check.lower() or 
                            "success" in text_to_check.lower()
                        ):
                            # Extract user mentions
                            mention_matches = re.findall(r'<@!?(\d+)>', text_to_check)
                            for user_id_str in mention_matches:
                                try:
                                    user_id = int(user_id_str)
                                    member = guild.get_member(user_id)
                                    username = member.display_name if member else f"User_{user_id}"
                                    
                                    if user_id not in points_count:
                                        points_count[user_id] = {
                                            "name": username,
                                            "points": 0,
                                            "first_seen": message.created_at.isoformat(),
                                            "last_seen": message.created_at.isoformat()
                                        }
                                    
                                    points_count[user_id]["points"] += 1
                                    points_count[user_id]["last_seen"] = message.created_at.isoformat()
                                    if member:
                                        points_count[user_id]["name"] = member.display_name
                                    
                                except (ValueError, AttributeError) as e:
                                    continue
                    
                    # Progress update every 500 messages with detailed stats
                    if total_messages % 500 == 0:
                        progress_pct = min(100, (total_messages / max(1, total_messages)) * 100)
                        print(f"{Colors.CYAN}[Scanner] Progress: {total_messages:,} messages | {len(points_count)} users found | Channel: {channel.name}{Colors.RESET}")
                        await asyncio.sleep(0.5)  # Rate limit protection
                    
            except discord.Forbidden:
                print(f"{Colors.RED}[Scanner] ERROR: No permission to read channel {channel_id}{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}[Scanner] ERROR: Error scanning channel {channel_id}: {e}{Colors.RESET}")
        
        print(f"\n{Colors.GREEN}[Scanner] Scan complete!{Colors.RESET}")
        print(f"{Colors.CYAN}[Scanner]   - Channels scanned: {len(success_channel_ids)}{Colors.RESET}")
        print(f"{Colors.CYAN}[Scanner]   - Total messages scanned: {total_messages:,}{Colors.RESET}")
        print(f"{Colors.CYAN}[Scanner]   - Bot award messages found: {bot_messages_found:,}{Colors.RESET}")
        print(f"{Colors.CYAN}[Scanner]   - Users with points: {len(points_count)}{Colors.RESET}")
        
        # Show top 5 users
        if points_count:
            sorted_users = sorted(points_count.items(), key=lambda x: x[1]["points"], reverse=True)
            print(f"\n{Colors.YELLOW}[Scanner] Top 5 Users Found:{Colors.RESET}")
            for rank, (user_id, data) in enumerate(sorted_users[:5], 1):
                print(f"  {rank}. {data['name']} (ID: {user_id}): {data['points']} points")
        
        # Merge with existing data
        print(f"\n{Colors.BLUE}[Scanner] Merging with existing data...{Colors.RESET}")
        self.merge_scan_results(points_count)
        
        # Save updated JSON
        self.save_json_data()
        
        # Generate history file
        self.generate_history_file(points_count, total_messages, bot_messages_found)
        
        print(f"\n{Colors.GREEN}[Scanner] All done! Data saved to {self.json_path}{Colors.RESET}")
    
    def merge_scan_results(self, scanned_points: Dict[int, Dict[str, Any]]):
        """Merge scanned points with existing JSON data"""
        existing_points = self.json_data.get("points", {})
        updates = 0
        additions = 0
        
        for user_id, scan_data in scanned_points.items():
            user_id_str = str(user_id)
            scanned_points_value = scan_data["points"]
            scanned_timestamp = scan_data.get("last_seen", datetime.now().isoformat())
            
            if user_id_str in existing_points:
                existing_entry = existing_points[user_id_str]
                existing_points_value = existing_entry.get("points", 0) if isinstance(existing_entry, dict) else existing_entry
                existing_timestamp = existing_entry.get("last_updated", "") if isinstance(existing_entry, dict) else ""
                
                # Use scanned data if it's higher or more recent
                if scanned_points_value > existing_points_value:
                    self.json_data["points"][user_id_str] = {
                        "points": scanned_points_value,
                        "last_updated": scanned_timestamp,
                        "source": "history_scan",
                        "name": scan_data.get("name", f"User_{user_id}")
                    }
                    updates += 1
                    print(f"  [UPDATE] User {user_id_str}: {existing_points_value} -> {scanned_points_value} points")
                elif scanned_timestamp > existing_timestamp:
                    # Keep existing points but update timestamp
                    self.json_data["points"][user_id_str]["last_updated"] = scanned_timestamp
                    updates += 1
            else:
                # New user
                self.json_data["points"][user_id_str] = {
                    "points": scanned_points_value,
                    "last_updated": scanned_timestamp,
                    "source": "history_scan",
                    "name": scan_data.get("name", f"User_{user_id}")
                }
                additions += 1
                print(f"  [ADD] User {user_id_str}: {scanned_points_value} points")
        
        print(f"\n{Colors.CYAN}[Scanner] Merge complete: {updates} updated, {additions} added{Colors.RESET}")
    
    def save_json_data(self):
        """Save JSON data to file"""
        self.json_data["last_scan"] = datetime.now().isoformat()
        self.json_data["recovery_sources"] = self.json_data.get("recovery_sources", []) + ["history_scan"]
        
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(self.json_data, f, indent=2, ensure_ascii=False)
        
        print(f"{Colors.GREEN}[Scanner] JSON data saved{Colors.RESET}")
    
    def generate_history_file(self, points_count: Dict[int, Dict[str, Any]], total_messages: int, bot_messages_found: int):
        """Generate points_history.txt file"""
        history_file = self.json_path.parent / "points_history.txt"
        
        sorted_users = sorted(points_count.items(), key=lambda x: x[1]["points"], reverse=True)
        
        with open(history_file, 'w', encoding='utf-8') as f:
            f.write("=== POINTS HISTORY FROM MESSAGE SCAN ===\n\n")
            f.write(f"Total messages scanned: {total_messages}\n")
            f.write(f"Total users with points: {len(points_count)}\n\n")
            
            for rank, (user_id, data) in enumerate(sorted_users, start=1):
                f.write(f"{rank}. {data['name']} (ID: {user_id}) - {data['points']} points\n")
        
        print(f"{Colors.GREEN}[Scanner] History file saved to {history_file}{Colors.RESET}")

def main():
    """Main entry point"""
    base_path = Path(__file__).parent
    config_path = base_path / "config.json"
    json_path = base_path / "success_points.json"
    
    if not config_path.exists():
        print(f"{Colors.RED}[Scanner] ERROR: config.json not found at {config_path}{Colors.RESET}")
        sys.exit(1)
    
    bot_token = None
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
        bot_token = config.get("bot_token")
    
    if not bot_token:
        print(f"{Colors.RED}[Scanner] ERROR: bot_token not found in config.json{Colors.RESET}")
        sys.exit(1)
    
    print(f"{Colors.CYAN}[Scanner] Initializing History Scanner...{Colors.RESET}")
    scanner = HistoryScanner(config_path, json_path)
    
    try:
        scanner.bot.run(bot_token)
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[Scanner] Interrupted by user{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}[Scanner] ERROR: {e}{Colors.RESET}")
        sys.exit(1)

if __name__ == "__main__":
    main()

