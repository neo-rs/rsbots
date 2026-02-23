#!/usr/bin/env python3
"""
Full setup: Sync WhopMembershipSync to Oracle and run on-server setup.

From your PC (with oraclekeys/servers.json and SSH key configured):
  1. Sync WhopMembershipSync code + config.json (not secrets) and systemd unit to Oracle
  2. Sync and run scripts/on_oracle_setup_whopmembershipsync.sh on the server
  3. Remind you to add config.secrets.json on Oracle if not present, then start service

Usage:
  python scripts/setup_whopmembershipsync_on_oracle.py
  python scripts/setup_whopmembershipsync_on_oracle.py --server-name "instance-enhance (rsadmin)"
  python scripts/setup_whopmembershipsync_on_oracle.py --sync-only   # copy files only; run on_oracle script manually
"""

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from subprocess import TimeoutExpired

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"

# Files to sync (do NOT sync config.secrets.json - add that on the server)
WHOP_SYNC_FILES = [
    "WhopMembershipSync/main.py",
    "WhopMembershipSync/whop_sheets_sync.py",
    "WhopMembershipSync/config.json",
    "WhopMembershipSync/requirements.txt",
    "WhopMembershipSync/ORACLE_SETUP_STATUS.md",
    "WhopMembershipSync/FLOW_DOCUMENTATION.md",
]
SYSTEMD_UNIT_LOCAL = "systemd/mirror-world-whopmembershipsync.service"
# Deploy unit into WhopMembershipSync/ so we don't require write access to repo/systemd on server
SYSTEMD_UNIT_REMOTE = "WhopMembershipSync/mirror-world-whopmembershipsync.service"
ON_ORACLE_SCRIPT = "scripts/on_oracle_setup_whopmembershipsync.sh"
# So Oracle RSAdminBot has mirror_bots including whopmembershipsync (journal channel + stream)
RSADMINBOT_CONFIG = "RSAdminBot/config.json"


def _load_servers():
    if not SERVERS_PATH.exists():
        raise FileNotFoundError(f"Missing {SERVERS_PATH}. Add Oracle server entry (name, user, host, key, remote_root).")
    return json.loads(SERVERS_PATH.read_text(encoding="utf-8") or "[]")


def _resolve_key_path(key_value: str) -> str:
    p = Path(key_value)
    if p.is_absolute() and p.exists():
        return str(p)
    candidate = REPO_ROOT / "oraclekeys" / key_value
    if candidate.exists():
        return str(candidate)
    if (REPO_ROOT / key_value).exists():
        return str(REPO_ROOT / key_value)
    return str(p)


def _pick_server(servers: list, server_name: str | None) -> dict:
    if not servers:
        raise ValueError("No servers in oraclekeys/servers.json")
    if server_name:
        for s in servers:
            if str(s.get("name", "")).strip() == server_name.strip():
                return s
        raise ValueError(f"Server not found: {server_name}")
    return servers[0]


def _ssh_cmd(entry: dict, cmd: str, timeout: int = 180) -> subprocess.CompletedProcess:
    key = _resolve_key_path(entry["key"])
    ssh_args = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        ssh_args.extend(shlex.split(opts))
    ssh_args.append(f"{entry['user']}@{entry['host']}")
    ssh_args.extend(["sh", "-c", cmd])
    return subprocess.run(ssh_args, capture_output=True, text=True, timeout=timeout)


def _scp_to(entry: dict, local: Path, remote: str, timeout: int = 90) -> subprocess.CompletedProcess:
    key = _resolve_key_path(entry["key"])
    scp_args = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        scp_args.extend(shlex.split(opts))
    scp_args.extend([str(local), f"{entry['user']}@{entry['host']}:{remote}"])
    return subprocess.run(scp_args, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    ap = argparse.ArgumentParser(description="Full setup: sync WhopMembershipSync to Oracle and run on-server setup")
    ap.add_argument("--server-name", default=None, help="Entry name from oraclekeys/servers.json")
    ap.add_argument("--sync-only", action="store_true", help="Only sync files; do not run on_oracle script")
    args = ap.parse_args()

    servers = _load_servers()
    entry = _pick_server(servers, args.server_name)
    rr = entry.get("remote_root") or "/home/rsadmin/bots/mirror-world"

    print("=== WhopMembershipSync full setup (local -> Oracle) ===\n")
    print(f"Server: {entry.get('name', entry.get('host'))}")
    print(f"Remote root: {rr}\n")

    # 0. Ensure remote directories exist (WhopMembershipSync may not exist yet)
    print("[0/4] Ensuring remote directories exist...")
    key = _resolve_key_path(entry["key"])
    ssh_mkdir = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        ssh_mkdir.extend(shlex.split(opts))
    ssh_mkdir.append(f"{entry['user']}@{entry['host']}")
    ssh_mkdir.extend(["mkdir", "-p", rr + "/WhopMembershipSync"])
    res = subprocess.run(ssh_mkdir, capture_output=True, text=True, timeout=30)
    if res.returncode != 0:
        print("  WARNING:", (res.stderr or res.stdout).strip()[:200])
    else:
        print("  OK")
    print()

    # 1. Sync WhopMembershipSync files
    print("[1/4] Syncing WhopMembershipSync files...")
    for rel in WHOP_SYNC_FILES:
        local = REPO_ROOT / rel
        if not local.exists():
            print(f"  SKIP (missing): {rel}")
            continue
        remote = f"{rr}/{rel}"
        res = _scp_to(entry, local, remote, timeout=90)
        if res.returncode != 0:
            print(f"  ERROR {rel}:", (res.stderr or res.stdout).strip()[:300])
            return 1
        print(f"  OK {rel}")
    print()

    # 2. Sync RSAdminBot config (so journal_live includes whopmembershipsync)
    print("[2/4] Syncing RSAdminBot/config.json (journal_live needs mirror_bots list)...")
    local_rsc = REPO_ROOT / RSADMINBOT_CONFIG
    if local_rsc.exists():
        res = _scp_to(entry, local_rsc, f"{rr}/{RSADMINBOT_CONFIG}", timeout=60)
        if res.returncode != 0:
            print("  ERROR:", (res.stderr or res.stdout).strip()[:300])
            return 1
        print("  OK")
    else:
        print("  SKIP (not found)")
    print()

    # 3. Sync systemd unit (into WhopMembershipSync/) and on-oracle script
    print("[3/4] Syncing systemd unit and on-Oracle script...")
    local_unit = REPO_ROOT / SYSTEMD_UNIT_LOCAL
    if not local_unit.exists():
        print(f"  ERROR: {SYSTEMD_UNIT_LOCAL} not found")
        return 1
    res = _scp_to(entry, local_unit, f"{rr}/{SYSTEMD_UNIT_REMOTE}", timeout=60)
    if res.returncode != 0:
        print(f"  ERROR {SYSTEMD_UNIT_LOCAL}:", (res.stderr or res.stdout).strip()[:300])
        return 1
    print(f"  OK {SYSTEMD_UNIT_LOCAL} -> {SYSTEMD_UNIT_REMOTE}")
    # Script must go to repo/scripts (ensure scripts dir exists)
    ssh_mkdir_scripts = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    if opts:
        ssh_mkdir_scripts.extend(shlex.split(opts))
    ssh_mkdir_scripts.append(f"{entry['user']}@{entry['host']}")
    ssh_mkdir_scripts.extend(["mkdir", "-p", f"{rr}/scripts"])
    subprocess.run(ssh_mkdir_scripts, capture_output=True, text=True, timeout=30)
    local_script = REPO_ROOT / ON_ORACLE_SCRIPT
    if not local_script.exists():
        print(f"  ERROR: {ON_ORACLE_SCRIPT} not found")
        return 1
    res = _scp_to(entry, local_script, f"{rr}/{ON_ORACLE_SCRIPT}", timeout=60)
    if res.returncode != 0:
        print(f"  ERROR {ON_ORACLE_SCRIPT}:", (res.stderr or res.stdout).strip()[:300])
        return 1
    print(f"  OK {ON_ORACLE_SCRIPT}")
    print()

    if args.sync_only:
        print("=== Sync complete (--sync-only) ===\n")
        print("Next: SSH to Oracle and run:")
        print(f"  bash {rr}/scripts/on_oracle_setup_whopmembershipsync.sh {rr}")
        print("\nOr run this script without --sync-only to run the on-Oracle script automatically.")
        return 0

    # 4. Run on-Oracle setup script (installs unit, enables, starts service, restarts RSAdminBot for journal)
    print("[4/4] Running on-Oracle setup script...")
    try:
        res = _ssh_cmd(entry, f"bash {rr}/scripts/on_oracle_setup_whopmembershipsync.sh {rr}", timeout=120)
        print(res.stdout or "")
        if res.stderr:
            print(res.stderr, file=sys.stderr)
        if res.returncode != 0:
            print("  WARNING: on-Oracle script exited with code", res.returncode)
        else:
            print("  Done.")
    except TimeoutExpired:
        print("  SSH timed out. Files are synced. Run manually on Oracle:")
        print(f"    bash {rr}/scripts/on_oracle_setup_whopmembershipsync.sh {rr}")

    print("\n=== Summary ===")
    print("1. Code and systemd unit are on Oracle.")
    print("2. Service is installed and enabled.")
    print("3. If config.secrets.json was missing on Oracle, add it then run:")
    print("     sudo systemctl start mirror-world-whopmembershipsync.service")
    print("   Check logs: journalctl -u mirror-world-whopmembershipsync.service -f")
    print("\nSee WhopMembershipSync/ORACLE_SETUP_STATUS.md for more commands.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
