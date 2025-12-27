#!/usr/bin/env python3
"""
Recover Points Data from Multiple Sources
1. Original database (RSAdminBot/original_files/Success-Bot/Success-Bot/success_points.db)
2. Remote points_history.txt file
3. Merge and export to JSON
"""
import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def recover_from_database(db_path: Path):
    """Recover points from original database"""
    if not db_path.exists():
        print(f"⚠️  Database not found: {db_path}")
        return {}
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Get points
        cursor.execute("SELECT user_id, points FROM points")
        points_data = {}
        for row in cursor.fetchall():
            user_id, points = row
            points_data[str(user_id)] = {
                "points": points,
                "last_updated": datetime.now().isoformat(),
                "source": "original_database"
            }
        
        # Get image hashes
        cursor.execute("SELECT hash FROM image_hashes")
        image_hashes = {}
        for row in cursor.fetchall():
            hash_val = row[0]
            image_hashes[hash_val] = {
                "user_id": None,  # Original doesn't track user_id
                "created_at": datetime.now().isoformat(),
                "source": "original_database"
            }
        
        conn.close()
        
        print(f"[OK] Recovered from database:")
        print(f"   - {len(points_data)} users with points")
        print(f"   - {len(image_hashes)} image hashes")
        
        return {
            "points": points_data,
            "image_hashes": image_hashes
        }
    except Exception as e:
        print(f"❌ Database recovery failed: {e}")
        return {}

def recover_from_history_file(history_path: Path):
    """Recover points from points_history.txt"""
    if not history_path.exists():
        print(f"[WARN] History file not found: {history_path}")
        return {}
    
    try:
        points_data = {}
        import re
        with open(history_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Parse format: "Username (ID: 123456789) - 123 points"
            # Pattern: (ID: <digits>) - <digits> points
            pattern = r'\(ID:\s*(\d+)\)\s*-\s*(\d+)\s+points'
            matches = re.findall(pattern, content)
            
            for user_id, points_str in matches:
                try:
                    user_id = str(user_id)
                    points = int(points_str)
                    if user_id and points >= 0:
                        # Keep highest if duplicate
                        if user_id in points_data:
                            points_data[user_id] = max(points_data[user_id], points)
                        else:
                            points_data[user_id] = points
                except ValueError:
                    continue
        
        print(f"[OK] Recovered from history file:")
        print(f"   - {len(points_data)} users with points")
        
        return {
            "points": {uid: {
                "points": pts,
                "last_updated": datetime.now().isoformat(),
                "source": "history_file"
            } for uid, pts in points_data.items()},
            "image_hashes": {}
        }
    except Exception as e:
        print(f"❌ History file recovery failed: {e}")
        return {}

def merge_data(*data_sources):
    """Merge multiple data sources, keeping highest points per user"""
    merged_points = {}
    merged_hashes = {}
    
    for source in data_sources:
        if not source:
            continue
        
        # Merge points (keep highest value)
        for user_id, user_data in source.get("points", {}).items():
            if isinstance(user_data, dict):
                points = user_data.get("points", 0)
            else:
                points = user_data
            
            if user_id in merged_points:
                existing_points = merged_points[user_id].get("points", 0) if isinstance(merged_points[user_id], dict) else merged_points[user_id]
                if points > existing_points:
                    merged_points[user_id] = {
                        "points": points,
                        "last_updated": user_data.get("last_updated", datetime.now().isoformat()) if isinstance(user_data, dict) else datetime.now().isoformat(),
                        "source": user_data.get("source", "merged") if isinstance(user_data, dict) else "merged"
                    }
            else:
                merged_points[user_id] = {
                    "points": points if isinstance(user_data, dict) else user_data,
                    "last_updated": user_data.get("last_updated", datetime.now().isoformat()) if isinstance(user_data, dict) else datetime.now().isoformat(),
                    "source": user_data.get("source", "recovered") if isinstance(user_data, dict) else "recovered"
                }
        
        # Merge image hashes
        for hash_val, hash_data in source.get("image_hashes", {}).items():
            if hash_val not in merged_hashes:
                merged_hashes[hash_val] = hash_data
    
    return {
        "points": merged_points,
        "image_hashes": merged_hashes
    }

if __name__ == "__main__":
    base_path = Path(__file__).parent.parent
    
    print("[RECOVER] Recovering Points Data")
    print("=" * 60)
    
    # Source 1: Original database
    original_db = base_path / "RSAdminBot" / "original_files" / "Success-Bot" / "Success-Bot" / "success_points.db"
    db_data = recover_from_database(original_db)
    
    # Source 2: History file
    history_file = base_path / "points_history.txt"
    history_data = recover_from_history_file(history_file)
    
    # Merge all sources
    print("\n[MERGE] Merging data sources...")
    merged = merge_data(db_data, history_data)
    
    # Save to JSON
    output_file = base_path / "RSuccessBot" / "success_points.json"
    json_data = {
        "points": merged["points"],
        "image_hashes": merged["image_hashes"],
        "point_movements": [],  # Empty for now, will be populated as bot runs
        "migrated_at": datetime.now().isoformat(),
        "recovery_sources": ["original_database", "history_file"]
    }
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[OK] Recovery complete!")
    print(f"   - {len(merged['points'])} users with points")
    print(f"   - {len(merged['image_hashes'])} image hashes")
    print(f"   -> Saved to: {output_file}")
    
    # Show top 10 users
    sorted_users = sorted(merged["points"].items(), key=lambda x: x[1].get("points", 0) if isinstance(x[1], dict) else x[1], reverse=True)
    print(f"\n[TOP 10] Top 10 Users:")
    for rank, (user_id, user_data) in enumerate(sorted_users[:10], 1):
        points = user_data.get("points", 0) if isinstance(user_data, dict) else user_data
        print(f"   {rank}. User {user_id}: {points} points")

