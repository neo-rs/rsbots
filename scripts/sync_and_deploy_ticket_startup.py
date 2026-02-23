#!/usr/bin/env python3
"""
Sync local ticket-startup flow to Oracle and deploy.

Ensures Oracle matches local for:
- DailyScheduleReminder (reminder_bot.py, schedule_parser.py, config.json, requirements.txt)
- RSCheckerbot config (support_tickets.startup_messages.external_sender_enabled, etc.)

Steps:
1. Disable ticket_startup, clear pending (stop spam)
2. Restart DailyScheduleReminder
3. Sync DailyScheduleReminder files from local
4. Sync RSCheckerbot/config.json from local
5. Set ticket_startup.enabled=true, external_sender_enabled=true (ensure correct)
6. Restart DailyScheduleReminder and RSCheckerbot
7. Wait 15s, show journal for both
8. Verify local vs remote file hashes

Usage:
  python scripts/sync_and_deploy_ticket_startup.py
  python scripts/sync_and_deploy_ticket_startup.py --server-name "instance-enhance (rsadmin)"
  python scripts/sync_and_deploy_ticket_startup.py --skip-restart  # sync only, no restart
"""

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
import time
from subprocess import TimeoutExpired
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"

DAILYSCHEDULEREMINDER_FILES = [
    "DailyScheduleReminder/reminder_bot.py",
    "DailyScheduleReminder/schedule_parser.py",
    "DailyScheduleReminder/config.json",
    "DailyScheduleReminder/requirements.txt",
]
RSC_CONFIG = "RSCheckerbot/config.json"
SVC_DSR = "mirror-world-dailyschedulereminder.service"
SVC_RSC = "mirror-world-rscheckerbot.service"


def _load_servers():
    if not SERVERS_PATH.exists():
        raise FileNotFoundError(f"Missing {SERVERS_PATH}")
    return json.loads(SERVERS_PATH.read_text(encoding="utf-8") or "[]")


def _resolve_key_path(key_value: str) -> str:
    p = Path(key_value)
    if p.is_absolute() and p.exists():
        return str(p)
    candidate = REPO_ROOT / "oraclekeys" / key_value
    if candidate.exists():
        return str(candidate)
    return str(REPO_ROOT / key_value) if (REPO_ROOT / key_value).exists() else str(p)


def _pick_server(servers: list, server_name: str | None) -> dict:
    if not servers:
        raise ValueError("No servers in oraclekeys/servers.json")
    if server_name:
        for s in servers:
            if str(s.get("name", "")).strip() == server_name.strip():
                return s
        raise ValueError(f"Server not found: {server_name}")
    return servers[0]


def _ssh_cmd(entry: dict, cmd: str, timeout: int = 120) -> subprocess.CompletedProcess:
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


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description="Sync local ticket-startup flow to Oracle and deploy")
    ap.add_argument("--server-name", default=None)
    ap.add_argument("--skip-restart", action="store_true", help="Sync only, do not restart services")
    ap.add_argument("--sync-only", action="store_true", help="Sync files only (no SSH exec); use on_oracle_finish_ticket_startup.sh on server to finish")
    args = ap.parse_args()

    servers = _load_servers()
    entry = _pick_server(servers, args.server_name)
    rr = entry.get("remote_root") or "/home/rsadmin/bots/mirror-world"
    bot_dir = f"{rr}/DailyScheduleReminder"
    rsc_data = f"{rr}/RSCheckerbot/data"

    print("=== Sync and Deploy Ticket Startup (local -> Oracle) ===\n")

    # 1. Sync files first (SCP tends to complete; SSH exec may timeout)
    print("[1/8] Syncing DailyScheduleReminder files...")
    for rel in DAILYSCHEDULEREMINDER_FILES:
        local = REPO_ROOT / rel
        if not local.exists():
            print(f"  SKIP (missing): {rel}")
            continue
        remote = f"{rr}/{rel}"
        res = _scp_to(entry, local, remote, timeout=90)
        if res.returncode != 0:
            print(f"  ERROR {rel}:", (res.stderr or res.stdout).strip()[:200])
            return 1
        print(f"  OK {rel}")
    print()

    # 2. Sync RSCheckerbot/config.json
    print("[2/8] Syncing RSCheckerbot/config.json...")
    local_rsc = REPO_ROOT / RSC_CONFIG
    if local_rsc.exists():
        res = _scp_to(entry, local_rsc, f"{rr}/{RSC_CONFIG}", timeout=90)
        if res.returncode != 0:
            print("  ERROR:", (res.stderr or res.stdout).strip()[:200])
            return 1
        print("  OK\n")
    else:
        print("  SKIP (file not found)\n")

    if args.sync_only or args.skip_restart:
        print("=== Sync complete ===\n")
        if args.sync_only:
            print("Next: SSH to Oracle and run:")
            print("  cd /home/rsadmin/bots/mirror-world")
            print("  bash scripts/on_oracle_finish_ticket_startup.sh")
        return 0

    # 3. Copy and run on-server script (avoids SSH timeout with inline python)
    print("[3/8] Copying on_oracle_finish_ticket_startup.sh and running on Oracle...")
    on_server = REPO_ROOT / "scripts" / "on_oracle_finish_ticket_startup.sh"
    if not on_server.exists():
        print("  ERROR: scripts/on_oracle_finish_ticket_startup.sh not found")
        return 1
    res = _scp_to(entry, on_server, f"{rr}/scripts/on_oracle_finish_ticket_startup.sh", timeout=30)
    if res.returncode != 0:
        print("  ERROR:", (res.stderr or res.stdout).strip()[:200])
        return 1
    try:
        res = _ssh_cmd(entry, f"bash {rr}/scripts/on_oracle_finish_ticket_startup.sh", timeout=180)
        print(res.stdout or "")
        if res.stderr:
            print(res.stderr, file=sys.stderr)
        if res.returncode != 0:
            print("  WARNING: script exited with code", res.returncode)
        else:
            print("  Done.\n")
    except TimeoutExpired:
        print("  SSH timed out. Files are synced. Run manually on Oracle:")
        print("    bash /home/rsadmin/bots/mirror-world/scripts/on_oracle_finish_ticket_startup.sh")
        print()

    # 4. Verify hashes (journal already shown by on-server script)
    print("[4/4] Verifying local vs remote file hashes...")
    mismatches = []
    for rel in DAILYSCHEDULEREMINDER_FILES + [RSC_CONFIG]:
        local = REPO_ROOT / rel
        if not local.exists():
            continue
        local_hash = _file_sha256(local)
        remote_path = f"{rr}/{rel}"
        res = _ssh_cmd(entry, f"sha256sum {shlex.quote(remote_path)} 2>/dev/null | cut -d' ' -f1", timeout=30)
        remote_hash = (res.stdout or "").strip() if res.returncode == 0 else ""
        if remote_hash and remote_hash != local_hash:
            mismatches.append((rel, local_hash[:16], remote_hash[:16]))
        elif remote_hash:
            print(f"  OK {rel}")
        else:
            print(f"  ?  {rel} (remote read failed)")
    if mismatches:
        print("\n  MISMATCH:")
        for rel, lh, rh in mismatches:
            print(f"    {rel}: local {lh}... != remote {rh}...")
    else:
        print("  All synced files match.\n")

    print("=== Sync and deploy complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
