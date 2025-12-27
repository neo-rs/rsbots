#!/usr/bin/env python3
"""
Upload missing .sh scripts and startup_sequences folder to remote server
"""
import os
import subprocess
import json
import shlex
from pathlib import Path

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

ssh_config = config.get("ssh_server", {})
host = ssh_config.get("host")
user = ssh_config.get("user")
key = ssh_config.get("key")
port = ssh_config.get("port", 22)
ssh_options_str = ssh_config.get("ssh_options", "")  # Get as string

if not all([host, user, key]):
    print("Error: SSH server configuration (host, user, key) is incomplete in config.json")
    exit(1)

local_key_path = Path(__file__).parent / key
if not local_key_path.exists():
    print(f"Error: SSH key file not found at {local_key_path}")
    exit(1)

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

