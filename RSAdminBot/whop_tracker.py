"""
Whop Logs Scanner & Membership Tracker

Scans whop-logs channel (1076440941814091787) to extract:
- Membership Status
- Access Pass
- Renewals
- Cancellations
- Membership duration

Stores data in SQLite + JSON files.
"""

import os
import json
import sqlite3
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
        
        # Database path
        self.db_path = self.data_dir / "whop_history.db"
        
        # Scan history file
        self.scan_history_path = self.data_dir / "whop_scan_history.json"
        
        # Initialize database
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database for membership history."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # Membership events table
        c.execute("""
            CREATE TABLE IF NOT EXISTS membership_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT NOT NULL,
                discord_username TEXT,
                whop_key TEXT,
                access_pass TEXT,
                name TEXT,
                email TEXT,
                membership_status TEXT,
                event_type TEXT,
                message_id INTEGER UNIQUE,
                timestamp TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Membership timeline (for duration tracking)
        c.execute("""
            CREATE TABLE IF NOT EXISTS membership_timeline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_days INTEGER,
                status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Indexes
        c.execute("CREATE INDEX IF NOT EXISTS idx_discord_id ON membership_events(discord_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON membership_events(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_event_type ON membership_events(event_type)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_timeline_discord_id ON membership_timeline(discord_id)")
        
        conn.commit()
        conn.close()
    
    def _parse_whop_message(self, message: discord.Message) -> Optional[Dict]:
        """Parse whop log message to extract membership data."""
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
        
        # Extract numeric Discord ID
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
        """Store membership event in database."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        try:
            c.execute("""
                INSERT OR IGNORE INTO membership_events 
                (discord_id, discord_username, whop_key, access_pass, name, email, 
                 membership_status, event_type, message_id, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event_data.get("discord_id"),
                event_data.get("discord_username"),
                event_data.get("whop_key"),
                event_data.get("access_pass"),
                event_data.get("name"),
                event_data.get("email"),
                event_data.get("membership_status"),
                event_data.get("event_type"),
                event_data.get("message_id"),
                event_data.get("timestamp")
            ))
            conn.commit()
        except sqlite3.IntegrityError:
            pass  # Already exists
        finally:
            conn.close()
    
    def _update_membership_timeline(self):
        """Update membership timeline for duration tracking."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # Get all events grouped by user, ordered by timestamp
        c.execute("""
            SELECT discord_id, event_type, timestamp
            FROM membership_events
            ORDER BY discord_id, timestamp ASC
        """)
        
        events_by_user = defaultdict(list)
        for discord_id, event_type, timestamp in c.fetchall():
            events_by_user[discord_id].append((event_type, timestamp))
        
        # Build timeline
        for discord_id, events in events_by_user.items():
            # Find start (new) and end (cancellation/completed) events
            started_at = None
            ended_at = None
            status = "active"
            
            for event_type, timestamp in events:
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
                
                # Store/update timeline
                c.execute("""
                    INSERT OR REPLACE INTO membership_timeline
                    (discord_id, started_at, ended_at, duration_days, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    discord_id,
                    started_at,
                    ended_at,
                    duration_days,
                    status
                ))
        
        conn.commit()
        conn.close()
    
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
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        stats = {}
        
        # Total members
        c.execute("SELECT COUNT(DISTINCT discord_id) FROM membership_events")
        stats["total_members"] = c.fetchone()[0]
        
        # New members
        c.execute("SELECT COUNT(*) FROM membership_events WHERE event_type = 'new'")
        stats["new_members"] = c.fetchone()[0]
        
        # Renewals
        c.execute("SELECT COUNT(*) FROM membership_events WHERE event_type = 'renewal'")
        stats["renewals"] = c.fetchone()[0]
        
        # Cancellations
        c.execute("SELECT COUNT(*) FROM membership_events WHERE event_type = 'cancellation'")
        stats["cancellations"] = c.fetchone()[0]
        
        # Average duration
        c.execute("SELECT AVG(duration_days) FROM membership_timeline WHERE duration_days IS NOT NULL")
        avg_duration = c.fetchone()[0]
        stats["avg_duration_days"] = round(avg_duration, 2) if avg_duration else None
        
        # Active memberships
        c.execute("SELECT COUNT(*) FROM membership_timeline WHERE status = 'active'")
        stats["active_memberships"] = c.fetchone()[0]
        
        conn.close()
        return stats
    
    def get_user_history(self, discord_id: str) -> Dict:
        """Get membership history for a specific user."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # Get all events for user
        c.execute("""
            SELECT discord_id, discord_username, whop_key, access_pass, name, email,
                   membership_status, event_type, message_id, timestamp
            FROM membership_events
            WHERE discord_id = ?
            ORDER BY timestamp ASC
        """, (discord_id,))
        
        events = []
        for row in c.fetchall():
            events.append({
                "discord_id": row[0],
                "discord_username": row[1],
                "whop_key": row[2],
                "access_pass": row[3],
                "name": row[4],
                "email": row[5],
                "membership_status": row[6],
                "event_type": row[7],
                "message_id": row[8],
                "timestamp": row[9]
            })
        
        # Get timeline
        c.execute("""
            SELECT started_at, ended_at, duration_days, status
            FROM membership_timeline
            WHERE discord_id = ?
            ORDER BY started_at DESC
        """, (discord_id,))
        
        timeline = []
        for row in c.fetchall():
            timeline.append({
                "started_at": row[0],
                "ended_at": row[1],
                "duration_days": row[2],
                "status": row[3]
            })
        
        conn.close()
        
        return {
            "discord_id": discord_id,
            "events": events,
            "timeline": timeline,
            "total_events": len(events),
            "total_periods": len(timeline)
        }

