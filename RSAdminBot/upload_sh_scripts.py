#!/usr/bin/env python3
"""
Upload missing .sh scripts and startup_sequences folder to remote server
"""
import os
import subprocess
import json
import shlex
from pathlib import Path
import sys

# Canonical Oracle server config (CANONICAL_RULES.md)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_oracle_servers, pick_oracle_server, resolve_oracle_ssh_key_path

# Load config
config_path = Path(__file__).parent / "config.json"
try:
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"Error: config.json not found at {config_path}")
    exit(1)
except json.JSONDecodeError:
    print(f"Error: Invalid JSON in config.json at {config_path}")
    exit(1)

server_name = str(config.get("ssh_server_name") or "").strip()
if not server_name:
    print("Error: Missing ssh_server_name in RSAdminBot/config.json (must match oraclekeys/servers.json entry name)")
    raise SystemExit(1)

servers, _servers_path = load_oracle_servers(_REPO_ROOT)
entry = pick_oracle_server(servers, server_name)

host = str(entry.get("host") or "").strip()
user = str(entry.get("user") or "").strip() or "rsadmin"
key_value = str(entry.get("key") or "").strip()
port_val = entry.get("port", 22)
ssh_options_str = str(entry.get("ssh_options") or "").strip()
try:
    port = int(port_val) if port_val is not None else 22
except Exception:
    port = 22

if not host or not key_value:
    print("Error: servers.json entry missing host or key")
    raise SystemExit(1)

local_key_path = resolve_oracle_ssh_key_path(key_value, _REPO_ROOT)
if not local_key_path.exists():
    print(f"Error: SSH key file not found at {local_key_path}")
    raise SystemExit(1)

remote_base_path = f"/home/{user}/bots/mirror-world/RSAdminBot"

# Parse ssh_options_str into a list of arguments
parsed_ssh_options = shlex.split(ssh_options_str) if ssh_options_str else []

# List of .sh scripts to upload
sh_scripts = [
    "manage_rsadminbot.sh",
    "manage_rs_bots.sh",
    "manage_mirror_bots.sh",
    "sync_bot.sh",
    "scan_bot.sh"
]

print(f"Uploading {len(sh_scripts)} .sh script(s) to {user}@{host}:{remote_base_path}")

for script_name in sh_scripts:
    local_script_path = Path(__file__).parent / script_name
    remote_script_path = f"{remote_base_path}/{script_name}"

    if not local_script_path.exists():
        print(f"Warning: Local script {script_name} not found, skipping.")
        continue

    # SCP command to upload script
    scp_cmd = [
        "scp",
        "-i", str(local_key_path),
        "-P", str(port),
    ] + parsed_ssh_options + [
        str(local_script_path),
        f"{user}@{host}:{remote_script_path}"
    ]

    try:
        print(f"Uploading {script_name}...")
        result = subprocess.run(scp_cmd, capture_output=True, text=True, check=True, encoding='utf-8')
        print(f"[OK] {script_name} uploaded successfully")

        # Chmod command to make script executable
        chmod_cmd = [
            "ssh",
            "-i", str(local_key_path),
            "-p", str(port),
        ] + parsed_ssh_options + [
            f"{user}@{host}",
            f"chmod +x {remote_script_path}"
        ]
        subprocess.run(chmod_cmd, capture_output=True, text=True, check=True, encoding='utf-8')
        print(f"[OK] {script_name} made executable")

    except subprocess.CalledProcessError as e:
        print(f"Failed to upload or chmod {script_name}:")
        print(f"   STDOUT: {e.stdout.strip()}")
        print(f"   STDERR: {e.stderr.strip()}")
    except Exception as e:
        print(f"An unexpected error occurred for {script_name}: {e}")

# Upload startup_sequences folder
local_startup_path = Path(__file__).parent / "startup_sequences"
remote_startup_path = f"{remote_base_path}/startup_sequences"

if local_startup_path.is_dir():
    print(f"\nUploading startup_sequences folder to {user}@{host}:{remote_startup_path}")
    scp_cmd_dir = [
        "scp",
        "-r",  # Recursive for directories
        "-i", str(local_key_path),
        "-P", str(port),
    ] + parsed_ssh_options + [
        str(local_startup_path),
        f"{user}@{host}:{remote_base_path}/"  # Note the trailing slash to copy contents into remote_base_path
    ]
    try:
        print(f"Uploading startup_sequences...")
        result = subprocess.run(scp_cmd_dir, capture_output=True, text=True, check=True, encoding='utf-8')
        print(f"[OK] startup_sequences folder uploaded successfully")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload startup_sequences folder:")
        print(f"   STDOUT: {e.stdout.strip()}")
        print(f"   STDERR: {e.stderr.strip()}")
    except Exception as e:
        print(f"An unexpected error occurred for startup_sequences folder: {e}")
else:
    print(f"Warning: Local startup_sequences folder not found at {local_startup_path}, skipping.")

print("\n[OK] Upload complete!")

