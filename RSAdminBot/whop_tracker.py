"""
Whop Logs Scanner & Membership Tracker

Scans whop-logs channel (1076440941814091787) to extract:
- Membership Status
- Access Pass
- Renewals
- Cancellations
- Membership duration

Stores data in JSON files only (CANONICAL_RULES compliant).
"""

import os
import json
import re
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import discord
from discord.ext import commands

# Colors for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class WhopTracker:
    """Tracks whop membership logs and lifecycle."""
    
    def __init__(self, bot: commands.Bot, config: Dict[str, any]):
        """
        Initialize WhopTracker.
        
        Args:
            bot: Discord bot instance
            config: Configuration dictionary with whop_logs_channel_id, rs_server_guild_id
        """
        self.bot = bot
        self.config = config
        self.whop_logs_channel_id = config.get("whop_logs_channel_id", 1076440941814091787)
        self.rs_server_guild_id = config.get("rs_server_guild_id", 876528050081251379)
        
        # Data directory
        self.data_dir = Path(__file__).parent / "whop_data"
        self.data_dir.mkdir(exist_ok=True)
        
        # JSON storage path
        self.json_path = self.data_dir / "whop_history.json"
        
        # Scan history file
        self.scan_history_path = self.data_dir / "whop_scan_history.json"
    
    def _load_json(self) -> dict:
        """Load whop_history.json, return default structure if missing"""
        if not self.json_path.exists():
            return {
                "membership_events": [],
                "membership_timeline": []
            }
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Ensure required keys exist
                if "membership_events" not in data:
                    data["membership_events"] = []
                if "membership_timeline" not in data:
                    data["membership_timeline"] = []
                return data
        except (json.JSONDecodeError, IOError) as e:
            print(f"[WARN] Failed to load whop_history.json: {e}")
            return {
                "membership_events": [],
                "membership_timeline": []
            }
    
    def _save_json(self, data: dict) -> None:
        """Save data to whop_history.json"""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            # Use atomic write (write to temp file, then replace)
            tmp_path = self.json_path.with_suffix(self.json_path.suffix + ".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tmp_path.replace(self.json_path)
        except Exception as e:
            print(f"[ERROR] Failed to save whop_history.json: {e}")
    
    def _parse_whop_message(self, message: discord.Message) -> Optional[Dict]:
        """Parse whop log message to extract membership data from embeds or plain text."""
        # Try to parse from embed fields first (modern Whop format)
        if message.embeds:
            for embed in message.embeds:
                parsed = self._parse_from_embed(embed, message)
                if parsed:
                    return parsed
        
        # Fallback to plain text parsing (legacy format)
        content = message.content or ""
        
        # Check if message contains whop data
        if not any(keyword in content for keyword in ["Key", "Access Pass", "Membership Status", "Discord ID"]):
            return None
        
        # Parse format: Label on one line, value on next line
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        
        def get_value_after(label: str) -> Optional[str]:
            for i, line in enumerate(lines):
                if label in line and i + 1 < len(lines):
                    return lines[i + 1]
            return None
        
        # Extract Discord ID
        discord_id_value = get_value_after("Discord ID")
        if not discord_id_value:
            return None
        
        # Extract numeric Discord ID (skip if "No Discord")
        if "no discord" in discord_id_value.lower():
            return None
        
        discord_id_match = re.search(r'(\d{17,19})', discord_id_value)
        if not discord_id_match:
            return None
        
        discord_id = discord_id_match.group(1)
        
        # Determine event type from membership status
        membership_status = get_value_after("Membership Status") or ""
        event_type = self._determine_event_type(membership_status, content)
        
        return {
            "discord_id": discord_id,
            "discord_username": get_value_after("Discord Username"),
            "whop_key": get_value_after("Key"),
            "access_pass": get_value_after("Access Pass"),
            "name": get_value_after("Name"),
            "email": get_value_after("Email"),
            "membership_status": membership_status,
            "event_type": event_type,
            "message_id": message.id,
            "timestamp": message.created_at.isoformat()
        }
    
    def _parse_from_embed(self, embed: discord.Embed, message: discord.Message) -> Optional[Dict]:
        """Parse membership data from Discord embed fields."""
        # Collect all field values into a dict
        field_values = {}
        full_text = ""
        
        for field in embed.fields:
            name = field.name or ""
            value = field.value or ""
            field_values[name.lower()] = value
            full_text += f"{name} {value}\n"
        
        # Also check embed description and title
        if embed.description:
            full_text += embed.description + "\n"
        if embed.title:
            full_text += embed.title + "\n"
        
        # Look for Discord ID in fields
        discord_id_value = None
        for key in ["discord id", "discord_id", "discord user id"]:
            if key in field_values:
                discord_id_value = field_values[key]
                break
        
        # Also check description/content for Discord ID
        if not discord_id_value:
            content_text = full_text
            discord_id_match = re.search(r'Discord ID[:\s]+([^\n]+)', content_text, re.IGNORECASE)
            if discord_id_match:
                discord_id_value = discord_id_match.group(1).strip()
        
        if not discord_id_value:
            return None
        
        # Skip if "No Discord"
        if "no discord" in discord_id_value.lower():
            return None
        
        # Extract numeric Discord ID
        discord_id_match = re.search(r'(\d{17,19})', discord_id_value)
        if not discord_id_match:
            return None
        
        discord_id = discord_id_match.group(1)
        
        # Extract other fields
        def get_field(key_aliases: list) -> str:
            for key in key_aliases:
                if key in field_values:
                    val = field_values[key].strip()
                    # Remove markdown formatting
                    val = re.sub(r'[`*_]', '', val)
                    return val
            return ""
        
        # Also try parsing from full text if not in fields
        def get_from_text(label: str) -> str:
            match = re.search(rf'{re.escape(label)}[:\s]+([^\n]+)', full_text, re.IGNORECASE)
            if match:
                val = match.group(1).strip()
                val = re.sub(r'[`*_]', '', val)
                return val
            return ""
        
        whop_key = get_field(["key"]) or get_from_text("Key")
        access_pass = get_field(["access pass", "access_pass"]) or get_from_text("Access Pass")
        name = get_field(["name"]) or get_from_text("Name")
        email = get_field(["email"]) or get_from_text("Email")
        membership_status = get_field(["membership status", "membership_status", "status"]) or get_from_text("Membership Status")
        discord_username = get_field(["discord username", "discord_username"]) or get_from_text("Discord Username")
        
        # Determine event type
        event_type = self._determine_event_type(membership_status, full_text)
        
        return {
            "discord_id": discord_id,
            "discord_username": discord_username,
            "whop_key": whop_key,
            "access_pass": access_pass,
            "name": name,
            "email": email,
            "membership_status": membership_status,
            "event_type": event_type,
            "message_id": message.id,
            "timestamp": message.created_at.isoformat()
        }
    
    def _determine_event_type(self, status: str, content: str) -> str:
        """Determine event type from membership status and content."""
        status_lower = status.lower()
        content_lower = content.lower()
        
        if "renewal" in content_lower or "renew" in content_lower:
            return "renewal"
        elif "cancel" in status_lower or "cancel" in content_lower:
            return "cancellation"
        elif "completed" in status_lower:
            return "completed"
        else:
            return "new"
    
    def _store_membership_event(self, event_data: Dict):
        """Store membership event in JSON file."""
        data = self._load_json()
        events = data["membership_events"]
        
        # Check if message_id already exists (uniqueness check)
        message_id = event_data.get("message_id")
        if message_id:
            if any(e.get("message_id") == message_id for e in events):
                return  # Already exists
        
        # Auto-increment id (max existing id + 1, or start at 1)
        next_id = 1
        if events:
            next_id = max(e.get("id", 0) for e in events) + 1
        
        # Add created_at if not present
        created_at = event_data.get("created_at")
        if not created_at:
            created_at = datetime.now(timezone.utc).isoformat()
        
        # Create event record
        event_record = {
            "id": next_id,
            "discord_id": event_data.get("discord_id"),
            "discord_username": event_data.get("discord_username"),
            "whop_key": event_data.get("whop_key"),
            "access_pass": event_data.get("access_pass"),
            "name": event_data.get("name"),
            "email": event_data.get("email"),
            "membership_status": event_data.get("membership_status"),
            "event_type": event_data.get("event_type"),
            "message_id": message_id,
            "timestamp": event_data.get("timestamp"),
            "created_at": created_at
        }
        
        events.append(event_record)
        data["membership_events"] = events
        self._save_json(data)
    
    def _update_membership_timeline(self):
        """Update membership timeline for duration tracking."""
        data = self._load_json()
        events = data.get("membership_events", [])
        timeline = []
        
        # Group events by user
        events_by_user = defaultdict(list)
        for event in events:
            discord_id = event.get("discord_id")
            if discord_id:
                events_by_user[discord_id].append((
                    event.get("event_type"),
                    event.get("timestamp")
                ))
        
        # Sort events by timestamp for each user
        for discord_id in events_by_user:
            events_by_user[discord_id].sort(key=lambda x: x[1] or "")
        
        # Build timeline
        next_timeline_id = 1
        if data.get("membership_timeline"):
            existing_ids = [t.get("id", 0) for t in data["membership_timeline"] if t.get("id")]
            if existing_ids:
                next_timeline_id = max(existing_ids) + 1
        
        for discord_id, events_list in events_by_user.items():
            # Find start (new) and end (cancellation/completed) events
            started_at = None
            ended_at = None
            status = "active"
            
            for event_type, timestamp in events_list:
                if event_type == "new" and not started_at:
                    started_at = timestamp
                elif event_type in ["cancellation", "completed"]:
                    ended_at = timestamp
                    status = "cancelled" if event_type == "cancellation" else "completed"
            
            if started_at:
                duration_days = None
                if ended_at:
                    try:
                        start_dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                        end_dt = datetime.fromisoformat(ended_at.replace('Z', '+00:00'))
                        duration_days = (end_dt - start_dt).days
                    except (ValueError, AttributeError):
                        pass
                
                timeline_entry = {
                    "id": next_timeline_id,
                    "discord_id": discord_id,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "duration_days": duration_days,
                    "status": status,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                timeline.append(timeline_entry)
                next_timeline_id += 1
        
        data["membership_timeline"] = timeline
        self._save_json(data)
    
    async def scan_whop_logs(self, limit: int = 2000, lookback_days: int = 30, progress_callback=None) -> Dict:
        """
        Scan whop-logs channel for membership events.
        
        Args:
            limit: Maximum number of messages to scan
            lookback_days: How many days back to scan
            progress_callback: Optional async callback(progress_dict) for progress updates
        
        Returns:
            Dictionary with scan results
        """
        import time
        start_time = time.time()
        
        guild = self.bot.get_guild(self.rs_server_guild_id)
        if not guild:
            return {"error": f"RS Server guild {self.rs_server_guild_id} not found"}
        
        channel = self.bot.get_channel(self.whop_logs_channel_id)
        if not channel:
            return {"error": f"Whop logs channel {self.whop_logs_channel_id} not found"}
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        events_found = []
        messages_scanned = 0
        last_progress_update = start_time
        
        # Terminal output
        print(f"{Colors.CYAN}[WhopTracker] Starting scan: limit={limit}, lookback_days={lookback_days}{Colors.RESET}")
        
        async for message in channel.history(limit=limit, after=cutoff_time):
            messages_scanned += 1
            
            # Parse message content
            event_data = self._parse_whop_message(message)
            if event_data:
                events_found.append(event_data)
                # Store in database
                self._store_membership_event(event_data)
            
            # Progress updates every 100 messages or every 2 seconds
            current_time = time.time()
            if progress_callback and (messages_scanned % 100 == 0 or (current_time - last_progress_update) >= 2):
                elapsed = current_time - start_time
                rate = messages_scanned / elapsed if elapsed > 0 else 0
                remaining = limit - messages_scanned
                eta_seconds = int(remaining / rate) if rate > 0 else 0
                
                progress_pct = int((messages_scanned / limit) * 100) if limit > 0 else 0
                bar_length = 30
                filled = int(bar_length * messages_scanned / limit) if limit > 0 else 0
                bar = '=' * filled + '-' * (bar_length - filled)
                
                # Terminal progress output (like rsync_sync.py)
                print(f"\r{Colors.CYAN}[WhopTracker] [{bar}] {progress_pct}% ({messages_scanned}/{limit}) ETA: {eta_seconds}s | Events: {len(events_found)}{Colors.RESET}", end='', flush=True)
                
                progress_dict = {
                    "messages_scanned": messages_scanned,
                    "limit": limit,
                    "events_found": len(events_found),
                    "progress_pct": progress_pct,
                    "bar": bar,
                    "eta_seconds": eta_seconds,
                    "rate": rate
                }
                await progress_callback(progress_dict)
                last_progress_update = current_time
        
        # Final terminal output
        print(f"\r{Colors.GREEN}[WhopTracker] Scan complete: {messages_scanned} messages, {len(events_found)} events found{Colors.RESET}")
        
        # Update membership timeline
        self._update_membership_timeline()
        
        # Save scan history
        scan_info = {
            "scan_date": datetime.now(timezone.utc).isoformat(),
            "messages_scanned": messages_scanned,
            "events_found": len(events_found),
            "limit": limit,
            "lookback_days": lookback_days
        }
        
        # Load existing scan history
        scan_history = []
        if self.scan_history_path.exists():
            try:
                with open(self.scan_history_path, 'r', encoding='utf-8') as f:
                    scan_history = json.load(f)
            except (json.JSONDecodeError, IOError):
                scan_history = []
        
        scan_history.append(scan_info)
        
        # Keep only last 100 scans
        scan_history = scan_history[-100:]
        
        with open(self.scan_history_path, 'w', encoding='utf-8') as f:
            json.dump(scan_history, f, indent=2, ensure_ascii=False)
        
        return scan_info
    
    def get_membership_stats(self) -> Dict:
        """Get membership statistics."""
        data = self._load_json()
        events = data.get("membership_events", [])
        timeline = data.get("membership_timeline", [])
        
        stats = {}
        
        # Total members (distinct discord_ids)
        unique_members = set(e.get("discord_id") for e in events if e.get("discord_id"))
        stats["total_members"] = len(unique_members)
        
        # New members
        stats["new_members"] = sum(1 for e in events if e.get("event_type") == "new")
        
        # Renewals
        stats["renewals"] = sum(1 for e in events if e.get("event_type") == "renewal")
        
        # Cancellations
        stats["cancellations"] = sum(1 for e in events if e.get("event_type") == "cancellation")
        
        # Average duration
        durations = [t.get("duration_days") for t in timeline if t.get("duration_days") is not None]
        if durations:
            avg_duration = sum(durations) / len(durations)
            stats["avg_duration_days"] = round(avg_duration, 2)
        else:
            stats["avg_duration_days"] = None
        
        # Active memberships
        stats["active_memberships"] = sum(1 for t in timeline if t.get("status") == "active")
        
        return stats
    
    def get_user_history(self, discord_id: str) -> Dict:
        """Get membership history for a specific user."""
        data = self._load_json()
        events = data.get("membership_events", [])
        timeline = data.get("membership_timeline", [])
        
        # Filter events by discord_id
        user_events = [e for e in events if e.get("discord_id") == discord_id]
        user_events.sort(key=lambda x: x.get("timestamp") or "")
        
        # Filter timeline by discord_id
        user_timeline = [t for t in timeline if t.get("discord_id") == discord_id]
        user_timeline.sort(key=lambda x: x.get("started_at") or "", reverse=True)
        
        # Remove id fields from timeline for response (keep structure consistent)
        timeline_response = []
        for t in user_timeline:
            timeline_response.append({
                "started_at": t.get("started_at"),
                "ended_at": t.get("ended_at"),
                "duration_days": t.get("duration_days"),
                "status": t.get("status")
            })
        
        return {
            "discord_id": discord_id,
            "events": user_events,
            "timeline": timeline_response,
            "total_events": len(user_events),
            "total_periods": len(user_timeline)
        }

