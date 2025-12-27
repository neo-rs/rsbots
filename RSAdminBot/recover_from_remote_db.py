#!/usr/bin/env python3
"""
Recover Recent Points Data from Remote Database
Extracts data from remote SQLite database and merges with local JSON
"""
import json
import sys
from pathlib import Path
from datetime import datetime
import subprocess
import tempfile
import os

def get_remote_database(ssh_key_path: Path, remote_user: str, remote_host: str, remote_db_path: str):
    """Download remote database to temp file"""
    try:
        # Create temp file
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        
        # Download via SCP
        cmd = [
            "scp",
            "-i", str(ssh_key_path),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            f"{remote_user}@{remote_host}:{remote_db_path}",
            temp_db.name
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            return temp_db.name
        else:
            print(f"[WARN] Could not download remote database: {result.stderr}")
            os.unlink(temp_db.name)
            return None
    except Exception as e:
        print(f"[WARN] Error downloading remote database: {e}")
        if 'temp_db' in locals():
            try:
                os.unlink(temp_db.name)
            except:
                pass
        return None

def extract_database_data(db_path: str):
    """Extract all data from SQLite database"""
    try:
        import sqlite3
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get points
        cursor.execute("SELECT user_id, points, last_updated FROM points")
        points_data = {}
        for row in cursor.fetchall():
            user_id, points, last_updated = row
            points_data[str(user_id)] = {
                "points": points,
                "last_updated": last_updated or datetime.now().isoformat(),
                "source": "remote_database"
            }
        
        # Get image hashes
        cursor.execute("SELECT hash, user_id, created_at FROM image_hashes")
        image_hashes = {}
        for row in cursor.fetchall():
            hash_val, user_id, created_at = row
            image_hashes[hash_val] = {
                "user_id": user_id,
                "created_at": created_at or datetime.now().isoformat(),
                "source": "remote_database"
            }
        
        # Get point movements
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
        
        # Get latest timestamp
        cursor.execute("SELECT MAX(last_updated) FROM points")
        max_ts_row = cursor.fetchone()
        max_timestamp = max_ts_row[0] if max_ts_row and max_ts_row[0] else None
        
        conn.close()
        
        return {
            "points": points_data,
            "image_hashes": image_hashes,
            "point_movements": point_movements,
            "latest_timestamp": max_timestamp
        }
    except Exception as e:
        print(f"[ERROR] Failed to extract database data: {e}")
        return None

def merge_data(local_data: dict, remote_data: dict, cutoff_timestamp: str = None):
    """Merge local and remote data, keeping most recent"""
    merged_points = {}
    all_user_ids = set(local_data.get("points", {}).keys()) | set(remote_data.get("points", {}).keys())
    
    updates = 0
    additions = 0
    
    for user_id in all_user_ids:
        local_entry = local_data.get("points", {}).get(user_id, {})
        remote_entry = remote_data.get("points", {}).get(user_id, {})
        
        # Get timestamps
        local_ts = local_entry.get("last_updated", "") if isinstance(local_entry, dict) else ""
        remote_ts = remote_entry.get("last_updated", "") if isinstance(remote_entry, dict) else ""
        
        # If cutoff timestamp provided, only use remote if it's newer
        if cutoff_timestamp and remote_ts:
            if remote_ts <= cutoff_timestamp:
                # Remote data is older, skip it
                if local_entry:
                    merged_points[user_id] = local_entry
                continue
        
        # Keep the most recent entry
        if remote_ts and local_ts:
            if remote_ts > local_ts:
                merged_points[user_id] = remote_entry
                updates += 1
            else:
                merged_points[user_id] = local_entry
        elif remote_ts:
            merged_points[user_id] = remote_entry
            additions += 1
        elif local_ts:
            merged_points[user_id] = local_entry
        else:
            # No timestamp, keep the one with higher points
            local_points = local_entry.get("points", 0) if isinstance(local_entry, dict) else (local_entry if isinstance(local_entry, int) else 0)
            remote_points = remote_entry.get("points", 0) if isinstance(remote_entry, dict) else (remote_entry if isinstance(remote_entry, int) else 0)
            if remote_points > local_points:
                merged_points[user_id] = remote_entry
                updates += 1
            else:
                merged_points[user_id] = local_entry
    
    return merged_points, updates, additions

if __name__ == "__main__":
    base_path = Path(__file__).parent.parent
    
    print("[RECOVER] Recovering Recent Data from Remote Database")
    print("=" * 60)
    
    # Load local data
    local_file = base_path / "RSuccessBot" / "success_points.json"
    if not local_file.exists():
        print(f"[ERROR] Local file not found: {local_file}")
        sys.exit(1)
    
    with open(local_file, 'r', encoding='utf-8') as f:
        local_data = json.load(f)
    
    # Get latest local timestamp
    local_timestamps = [u.get("last_updated", "") for u in local_data.get("points", {}).values() if isinstance(u, dict) and u.get("last_updated")]
    local_latest = max(local_timestamps) if local_timestamps else None
    
    print(f"[LOCAL] Loaded {len(local_data.get('points', {}))} users")
    if local_latest:
        print(f"[LOCAL] Latest timestamp: {local_latest}")
    
    # Get remote database
    oraclekeys_path = base_path / "oraclekeys"
    servers_json = oraclekeys_path / "servers.json"
    
    if not servers_json.exists():
        print(f"[ERROR] servers.json not found")
        sys.exit(1)
    
    with open(servers_json, 'r') as f:
        servers = json.load(f)
    
    if not servers:
        print(f"[ERROR] No servers configured")
        sys.exit(1)
    
    server = servers[0]
    remote_user = server.get("user", "rsadmin")
    remote_host = server.get("host", "")
    ssh_key = server.get("key")
    
    if not remote_host:
        print(f"[ERROR] Server host not configured")
        sys.exit(1)
    
    ssh_key_path = oraclekeys_path / ssh_key
    if not ssh_key_path.exists():
        print(f"[ERROR] SSH key not found: {ssh_key_path}")
        sys.exit(1)
    
    remote_db_path = "/home/rsadmin/bots/mirror-world/RSuccessBot/success_points.db"
    
    print(f"\n[REMOTE] Downloading database from {remote_host}...")
    temp_db = get_remote_database(ssh_key_path, remote_user, remote_host, remote_db_path)
    
    if not temp_db:
        print("[WARN] Could not download remote database. Using local data only.")
        sys.exit(0)
    
    print(f"[REMOTE] Extracting data from database...")
    remote_data = extract_database_data(temp_db)
    
    # Clean up temp file
    try:
        os.unlink(temp_db)
    except:
        pass
    
    if not remote_data:
        print("[WARN] Could not extract remote data. Using local data only.")
        sys.exit(0)
    
    print(f"[REMOTE] Found {len(remote_data.get('points', {}))} users")
    if remote_data.get("latest_timestamp"):
        print(f"[REMOTE] Latest timestamp: {remote_data['latest_timestamp']}")
    
    # Merge data (only use remote data newer than local cutoff)
    print(f"\n[MERGE] Merging data (only using remote data newer than local)...")
    merged_points, updates, additions = merge_data(local_data, remote_data, cutoff_timestamp=local_latest)
    
    # Merge image hashes (keep all unique)
    merged_hashes = {**local_data.get("image_hashes", {}), **remote_data.get("image_hashes", {})}
    
    # Merge point movements (keep all unique)
    all_movements = local_data.get("point_movements", []) + remote_data.get("point_movements", [])
    seen_movements = set()
    merged_movements = []
    for movement in all_movements:
        key = (movement.get("user_id"), movement.get("created_at"), movement.get("change_amount"))
        if key not in seen_movements:
            merged_movements.append(movement)
            seen_movements.add(key)
    merged_movements.sort(key=lambda x: x.get("created_at", ""))
    
    # Create merged data structure
    merged_data = {
        "points": merged_points,
        "image_hashes": merged_hashes,
        "point_movements": merged_movements,
        "migrated_at": local_data.get("migrated_at", datetime.now().isoformat()),
        "last_merged": datetime.now().isoformat(),
        "recovery_sources": local_data.get("recovery_sources", []) + ["remote_database"]
    }
    
    # Save merged data
    with open(local_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[OK] Merge complete!")
    print(f"   - {len(merged_points)} users with points")
    print(f"   - {updates} users updated with newer data")
    print(f"   - {additions} new users added")
    print(f"   - {len(merged_hashes)} image hashes")
    print(f"   - {len(merged_movements)} point movements")
    print(f"   -> Saved to: {local_file}")
    
    # Show latest timestamp
    timestamps = [u.get("last_updated", "") for u in merged_points.values() if isinstance(u, dict) and u.get("last_updated")]
    if timestamps:
        timestamps.sort(reverse=True)
        print(f"\n[INFO] Latest timestamp: {timestamps[0]}")

