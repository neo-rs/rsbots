#!/usr/bin/env python3
"""
JSON query script for Success-Bot
Useful for checking points and JSON storage status
"""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Get JSON path
base_path = Path(__file__).parent
json_path = base_path / "success_points.json"

def check_json():
    if not json_path.exists():
        print(f"[ERROR] JSON file '{json_path}' not found.")
        print(f"Expected location: {json_path}")
        return
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        points = data.get("points", {})
        image_hashes = data.get("image_hashes", {})
        
        if points:
            print("Top 20 users with points:")
            print("-" * 50)
            # Sort by points
            sorted_users = sorted(points.items(), key=lambda x: x[1].get("points", 0), reverse=True)
            for user_id, user_data in sorted_users[:20]:
                user_points = user_data.get("points", 0)
                print(f"User ID: {user_id}, Points: {user_points}")
        else:
            print("No users with points found.")
        
        # Get totals
        total_users = len(points)
        total_points = sum(user_data.get("points", 0) for user_data in points.values())
        
        print(f"\nTotal users: {total_users}")
        print(f"Total points: {total_points}")
        print(f"Image hashes stored: {len(image_hashes)}")
        
        # Get recent activity (last 24 hours)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        recent_updates = sum(
            1 for user_data in points.values()
            if user_data.get("last_updated", "") >= cutoff
        )
        print(f"Users updated in last 24h: {recent_updates}")
        
        # Show migration info
        if "migrated_at" in data:
            print(f"\nMigrated at: {data['migrated_at']}")
        
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON: {e}")
    except Exception as e:
        print(f"[ERROR] Error reading JSON: {e}")

if __name__ == "__main__":
    check_json()

