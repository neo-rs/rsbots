#!/usr/bin/env python3
"""
Script to test SSH connection and start all bots using .sh scripts
"""
import json
import subprocess
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
with open(config_path, 'r', encoding='utf-8') as f:
    config = json.load(f)

server_name = str(config.get("ssh_server_name") or "").strip()
if not server_name:
    print("ERROR: Missing ssh_server_name in RSAdminBot/config.json (must match oraclekeys/servers.json entry name)")
    raise SystemExit(1)

servers, _servers_path = load_oracle_servers(_REPO_ROOT)
entry = pick_oracle_server(servers, server_name)

host = str(entry.get("host") or "").strip()
user = str(entry.get("user") or "").strip() or "rsadmin"
key_value = str(entry.get("key") or "").strip()
ssh_options_str = str(entry.get("ssh_options") or "").strip()
port_val = entry.get("port", 22)
try:
    port = int(port_val) if port_val is not None else 22
except Exception:
    port = 22

if not host or not key_value:
    print("ERROR: servers.json entry missing host or key")
    raise SystemExit(1)

local_key_path = resolve_oracle_ssh_key_path(key_value, _REPO_ROOT)
if not local_key_path.exists():
    print(f"ERROR: SSH key not found: {local_key_path}")
    raise SystemExit(1)

remote_base_path = f"/home/{user}/bots/mirror-world/RSAdminBot"
parsed_ssh_options = shlex.split(ssh_options_str) if ssh_options_str else []

def execute_ssh_command(cmd: str, timeout: int = 60):
    """Execute SSH command and return (success, stdout, stderr)"""
    ssh_cmd = [
        "ssh",
        "-i", str(local_key_path),
        "-p", str(port),
    ] + parsed_ssh_options + [
        f"{user}@{host}",
        cmd
    ]
    
    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding='utf-8'
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, None, "Command timed out"
    except Exception as e:
        return False, None, str(e)

# Test SSH connection
print("=" * 60)
print("Testing SSH Connection...")
print("=" * 60)
test_success, test_output, test_error = execute_ssh_command("echo 'SSH connection test successful'", timeout=10)
if test_success:
    print(f"[OK] SSH connection successful")
    print(f"   Output: {test_output.strip()}")
else:
    print(f"[ERROR] SSH connection failed")
    print(f"   Error: {test_error}")
    exit(1)

print()

# Start all bots
scripts_to_run = [
    ("manage_rsadminbot.sh", "start", "rsadminbot"),
    ("manage_rs_bots.sh", "start", "all"),
    ("manage_mirror_bots.sh", "start", "all"),
]

print("=" * 60)
print("Starting All Bots...")
print("=" * 60)

for script_name, action, bot_arg in scripts_to_run:
    script_path = f"{remote_base_path}/{script_name}"
    cmd = f"bash {shlex.quote(script_path)} {action} {shlex.quote(bot_arg)}"
    
    print(f"\n[{script_name}] Starting {bot_arg}...")
    success, stdout, stderr = execute_ssh_command(cmd, timeout=120)
    
    if success:
        print(f"[OK] [{script_name}] Success")
        if stdout:
            # Show last few lines of output
            lines = stdout.strip().split('\n')
            for line in lines[-5:]:
                if line.strip():
                    # Remove Unicode characters that cause encoding issues
                    try:
                        print(f"   {line}")
                    except UnicodeEncodeError:
                        clean_line = line.encode('ascii', 'ignore').decode('ascii')
                        print(f"   {clean_line}")
    else:
        print(f"[ERROR] [{script_name}] Failed")
        if stderr:
            try:
                print(f"   Error: {stderr[:200]}")
            except UnicodeEncodeError:
                clean_stderr = stderr[:200].encode('ascii', 'ignore').decode('ascii')
                print(f"   Error: {clean_stderr}")
        if stdout:
            try:
                print(f"   Output: {stdout[:200]}")
            except UnicodeEncodeError:
                clean_stdout = stdout[:200].encode('ascii', 'ignore').decode('ascii')
                print(f"   Output: {clean_stdout}")

print()
print("=" * 60)
print("Checking Bot Status...")
print("=" * 60)

# Check status of all bots
status_scripts = [
    ("manage_rsadminbot.sh", "rsadminbot"),
    ("manage_rs_bots.sh", "all"),
    ("manage_mirror_bots.sh", "all"),
]

for script_name, bot_arg in status_scripts:
    script_path = f"{remote_base_path}/{script_name}"
    cmd = f"bash {shlex.quote(script_path)} status {shlex.quote(bot_arg)}"
    
    print(f"\n[{script_name}] Status for {bot_arg}...")
    success, stdout, stderr = execute_ssh_command(cmd, timeout=30)
    
    if success and stdout:
        lines = stdout.strip().split('\n')
        for line in lines:
            if line.strip():
                # Remove Unicode characters that cause encoding issues
                try:
                    print(f"   {line}")
                except UnicodeEncodeError:
                    clean_line = line.encode('ascii', 'ignore').decode('ascii')
                    print(f"   {clean_line}")
    elif stderr:
        try:
            print(f"   Error: {stderr[:200]}")
        except UnicodeEncodeError:
            clean_stderr = stderr[:200].encode('ascii', 'ignore').decode('ascii')
            print(f"   Error: {clean_stderr}")

print()
print("=" * 60)
print("Done!")
print("=" * 60)

