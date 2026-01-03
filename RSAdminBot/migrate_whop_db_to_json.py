#!/usr/bin/env python3
"""
Migration Script: Whop Database to JSON
----------------------------------------
One-time script to migrate RSAdminBot/whop_data/whop_history.db to JSON format.

This script:
1. Reads all data from whop_history.db (SQLite)
2. Exports to whop_history.json (JSON)
3. Preserves all data with no data loss

Run this script BEFORE updating whop_tracker.py to use JSON storage.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone

# Get script directory
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "whop_data"
DB_PATH = DATA_DIR / "whop_history.db"
JSON_PATH = DATA_DIR / "whop_history.json"

def migrate_database_to_json():
    """Migrate SQLite database to JSON format"""
    
    if not DB_PATH.exists():
        print(f"❌ Database file not found: {DB_PATH}")
        print("   No migration needed - database doesn't exist")
        return False
    
    print(f"📦 Migrating database: {DB_PATH}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Export membership_events
        c.execute("""
            SELECT id, discord_id, discord_username, whop_key, access_pass, name, email,
                   membership_status, event_type, message_id, timestamp, created_at
            FROM membership_events
            ORDER BY id ASC
        """)
        
        events = []
        for row in c.fetchall():
            events.append({
                "id": row[0],
                "discord_id": row[1],
                "discord_username": row[2],
                "whop_key": row[3],
                "access_pass": row[4],
                "name": row[5],
                "email": row[6],
                "membership_status": row[7],
                "event_type": row[8],
                "message_id": row[9],
                "timestamp": row[10],
                "created_at": row[11]
            })
        
        print(f"   ✓ Exported {len(events)} membership events")
        
        # Export membership_timeline
        c.execute("""
            SELECT id, discord_id, started_at, ended_at, duration_days, status, created_at
            FROM membership_timeline
            ORDER BY id ASC
        """)
        
        timeline = []
        for row in c.fetchall():
            timeline.append({
                "id": row[0],
                "discord_id": row[1],
                "started_at": row[2],
                "ended_at": row[3],
                "duration_days": row[4],
                "status": row[5],
                "created_at": row[6]
            })
        
        print(f"   ✓ Exported {len(timeline)} timeline entries")
        
        conn.close()
        
        # Create JSON structure
        json_data = {
            "membership_events": events,
            "membership_timeline": timeline,
            "migrated_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Write to JSON file
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Migration complete: {JSON_PATH}")
        print(f"   Events: {len(events)}, Timeline entries: {len(timeline)}")
        
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return False
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Whop Database to JSON Migration")
    print("=" * 60)
    print()
    
    success = migrate_database_to_json()
    
    if success:
        print()
        print("Next steps:")
        print("1. Verify JSON file contains all data")
        print("2. Update whop_tracker.py to use JSON storage")
        print("3. Test !whopscan, !whopstats, !whophistory commands")
        print("4. Delete whop_history.db after verification")
    else:
        print()
        print("Migration failed - check errors above")
    
    print()

