#!/usr/bin/env python3
"""
Database to JSON Migration Script
Migrates data from SQLite databases to JSON files for RS bots
Follows CANONICAL_RULES.md - one source of truth (JSON only)
"""
import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime

def migrate_rssuccessbot(db_path: Path, json_path: Path):
    """Migrate RSuccessBot database to JSON"""
    if not db_path.exists():
        print(f"⚠️  Database not found: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Migrate points
        cursor.execute("SELECT user_id, points, last_updated FROM points")
        points_data = {}
        for row in cursor.fetchall():
            user_id, points, last_updated = row
            points_data[str(user_id)] = {
                "points": points,
                "last_updated": last_updated or datetime.now().isoformat()
            }
        
        # Migrate image_hashes
        cursor.execute("SELECT hash, user_id, created_at FROM image_hashes")
        image_hashes = {}
        for row in cursor.fetchall():
            hash_val, user_id, created_at = row
            image_hashes[hash_val] = {
                "user_id": user_id,
                "created_at": created_at or datetime.now().isoformat()
            }
        
        # Migrate point_movements
        cursor.execute("SELECT user_id, change_amount, old_balance, new_balance, reason, admin_user_id, created_at FROM point_movements ORDER BY created_at")
        point_movements = []
        for row in cursor.fetchall():
            user_id, change_amount, old_balance, new_balance, reason, admin_user_id, created_at = row
            point_movements.append({
                "user_id": user_id,
                "change_amount": change_amount,
                "old_balance": old_balance,
                "new_balance": new_balance,
                "reason": reason,
                "admin_user_id": admin_user_id,
                "created_at": created_at or datetime.now().isoformat()
            })
        
        conn.close()
        
        # Save to JSON
        json_data = {
            "points": points_data,
            "image_hashes": image_hashes,
            "point_movements": point_movements,
            "migrated_at": datetime.now().isoformat()
        }
        
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Migrated RSuccessBot:")
        print(f"   - {len(points_data)} users with points")
        print(f"   - {len(image_hashes)} image hashes")
        print(f"   - {len(point_movements)} point movements")
        print(f"   → Saved to: {json_path}")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

def migrate_rscheckerbot(db_path: Path, json_path: Path):
    """Migrate RSCheckerbot database to JSON"""
    if not db_path.exists():
        print(f"⚠️  Database not found: {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Migrate invites
        cursor.execute("SELECT invite_code, lead_id, email, utm_data, created_at, used_at, discord_user_id, discord_username FROM invites")
        invites_data = {}
        for row in cursor.fetchall():
            invite_code, lead_id, email, utm_data, created_at, used_at, discord_user_id, discord_username = row
            invites_data[invite_code] = {
                "lead_id": lead_id,
                "email": email,
                "utm_data": utm_data,
                "created_at": created_at or datetime.now().isoformat(),
                "used_at": used_at,
                "discord_user_id": discord_user_id,
                "discord_username": discord_username
            }
        
        conn.close()
        
        # Save to JSON
        json_data = {
            "invites": invites_data,
            "migrated_at": datetime.now().isoformat()
        }
        
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Migrated RSCheckerbot:")
        print(f"   - {len(invites_data)} invites")
        print(f"   → Saved to: {json_path}")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

if __name__ == "__main__":
    base_path = Path(__file__).parent.parent
    
    print("🔄 Database to JSON Migration")
    print("=" * 60)
    
    # RSuccessBot
    rssuccess_db = base_path / "RSuccessBot" / "success_points.db"
    rssuccess_json = base_path / "RSuccessBot" / "success_points.json"
    migrate_rssuccessbot(rssuccess_db, rssuccess_json)
    
    # RSCheckerbot
    rschecker_db = base_path / "RSCheckerbot" / "invites.db"
    rschecker_json = base_path / "RSCheckerbot" / "invites.json"
    migrate_rscheckerbot(rschecker_db, rschecker_json)
    
    print("\n✅ Migration complete!")

