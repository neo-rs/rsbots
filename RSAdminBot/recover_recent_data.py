#!/usr/bin/env python3
"""
Recover Recent Points Data from Remote Server
Merges remote data with local data, keeping the most recent values
"""
import json
import sys
from pathlib import Path
from datetime import datetime
import subprocess
import shlex

def get_remote_json_data(ssh_key_path: Path, remote_user: str, remote_host: str, remote_path: str):
    """Fetch JSON data from remote server"""
    try:
        cmd = [
            "ssh",
            "-i", str(ssh_key_path),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            f"{remote_user}@{remote_host}",
            f"cat {remote_path}"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, encoding='utf-8', errors='replace')
        
        if result.returncode == 0 and result.stdout:
            return json.loads(result.stdout)
        else:
            print(f"[WARN] Could not fetch remote data: {result.stderr}")
            return None
    except Exception as e:
        print(f"[WARN] Error fetching remote data: {e}")
        return None

def merge_points_data(local_data: dict, remote_data: dict):
    """Merge local and remote points data, keeping most recent"""
    merged = {}
    all_user_ids = set(local_data.get("points", {}).keys()) | set(remote_data.get("points", {}).keys())
    
    for user_id in all_user_ids:
        local_entry = local_data.get("points", {}).get(user_id, {})
        remote_entry = remote_data.get("points", {}).get(user_id, {})
        
        # Get timestamps
        local_ts = local_entry.get("last_updated", "") if isinstance(local_entry, dict) else ""
        remote_ts = remote_entry.get("last_updated", "") if isinstance(remote_entry, dict) else ""
        
        # Keep the most recent entry
        if remote_ts and local_ts:
            if remote_ts > local_ts:
                merged[user_id] = remote_entry
                print(f"  [UPDATE] User {user_id}: Using remote data (remote: {remote_ts}, local: {local_ts})")
            else:
                merged[user_id] = local_entry
        elif remote_ts:
            merged[user_id] = remote_entry
            print(f"  [ADD] User {user_id}: Added from remote ({remote_ts})")
        elif local_ts:
            merged[user_id] = local_entry
        else:
            # No timestamp, keep the one with higher points
            local_points = local_entry.get("points", 0) if isinstance(local_entry, dict) else (local_entry if isinstance(local_entry, int) else 0)
            remote_points = remote_entry.get("points", 0) if isinstance(remote_entry, dict) else (remote_entry if isinstance(remote_entry, int) else 0)
            if remote_points > local_points:
                merged[user_id] = remote_entry
            else:
                merged[user_id] = local_entry
    
    return merged

def merge_image_hashes(local_data: dict, remote_data: dict):
    """Merge image hashes, keeping all unique"""
    merged = {}
    all_hashes = set(local_data.get("image_hashes", {}).keys()) | set(remote_data.get("image_hashes", {}).keys())
    
    for hash_val in all_hashes:
        local_entry = local_data.get("image_hashes", {}).get(hash_val)
        remote_entry = remote_data.get("image_hashes", {}).get(hash_val)
        
        # Prefer remote if it has user_id, otherwise keep local
        if remote_entry and isinstance(remote_entry, dict) and remote_entry.get("user_id"):
            merged[hash_val] = remote_entry
        elif local_entry:
            merged[hash_val] = local_entry
        elif remote_entry:
            merged[hash_val] = remote_entry
    
    return merged

def merge_point_movements(local_data: dict, remote_data: dict):
    """Merge point movements, keeping all unique entries"""
    merged = []
    seen = set()
    
    # Add local movements
    for movement in local_data.get("point_movements", []):
        key = (movement.get("user_id"), movement.get("created_at"), movement.get("change_amount"))
        if key not in seen:
            merged.append(movement)
            seen.add(key)
    
    # Add remote movements
    for movement in remote_data.get("point_movements", []):
        key = (movement.get("user_id"), movement.get("created_at"), movement.get("change_amount"))
        if key not in seen:
            merged.append(movement)
            seen.add(key)
    
    # Sort by created_at
    merged.sort(key=lambda x: x.get("created_at", ""))
    
    return merged

if __name__ == "__main__":
    base_path = Path(__file__).parent.parent
    
    print("[RECOVER] Recovering Recent Data from Remote Server")
    print("=" * 60)
    
    # Load local data
    local_file = base_path / "RSuccessBot" / "success_points.json"
    if not local_file.exists():
        print(f"[ERROR] Local file not found: {local_file}")
        sys.exit(1)
    
    with open(local_file, 'r', encoding='utf-8') as f:
        local_data = json.load(f)
    
    print(f"[LOCAL] Loaded {len(local_data.get('points', {}))} users")
    
    # Get remote data
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
    
    remote_path = "/home/rsadmin/bots/mirror-world/RSuccessBot/success_points.json"
    
    print(f"[REMOTE] Fetching data from {remote_host}...")
    remote_data = get_remote_json_data(ssh_key_path, remote_user, remote_host, remote_path)
    
    if not remote_data:
        print("[WARN] No remote data found or could not fetch. Using local data only.")
        sys.exit(0)
    
    print(f"[REMOTE] Loaded {len(remote_data.get('points', {}))} users")
    
    # Merge data
    print("\n[MERGE] Merging local and remote data...")
    merged_points = merge_points_data(local_data, remote_data)
    merged_hashes = merge_image_hashes(local_data, remote_data)
    merged_movements = merge_point_movements(local_data, remote_data)
    
    # Create merged data structure
    merged_data = {
        "points": merged_points,
        "image_hashes": merged_hashes,
        "point_movements": merged_movements,
        "migrated_at": local_data.get("migrated_at", datetime.now().isoformat()),
        "last_merged": datetime.now().isoformat(),
        "recovery_sources": local_data.get("recovery_sources", []) + ["remote_server"]
    }
    
    # Save merged data
    with open(local_file, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[OK] Merge complete!")
    print(f"   - {len(merged_points)} users with points")
    print(f"   - {len(merged_hashes)} image hashes")
    print(f"   - {len(merged_movements)} point movements")
    print(f"   -> Saved to: {local_file}")
    
    # Show latest timestamp
    timestamps = [u.get("last_updated", "") for u in merged_points.values() if isinstance(u, dict) and u.get("last_updated")]
    if timestamps:
        timestamps.sort(reverse=True)
        print(f"\n[INFO] Latest timestamp: {timestamps[0]}")

