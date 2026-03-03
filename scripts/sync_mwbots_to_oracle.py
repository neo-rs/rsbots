#!/usr/bin/env python3
"""One-off: Sync local MW/oracle files to Oracle so server matches repo. Uses oraclekeys/servers.json."""

import json
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"

# Files from the commit to sync (paths relative to repo root)
SYNC_FILES = [
    "RSAdminBot/admin_bot.py",
    "RSAdminBot/botctl.sh",
    "RSAdminBot/config.json",
    "RSAdminBot/install_services.sh",
    "RSAdminBot/manage_mirror_bots.sh",
    "RSAdminBot/run_bot.sh",
    "oracle_tools_menu_mwbots.bat",
    "push_mwbots_py_only.bat",
    "scripts/deploy_dailyschedulereminder_fix.py",
    "scripts/download_oracle_snapshot_mwbots.py",
    "scripts/oracle_baseline_check_mwbots.py",
    "scripts/setup_whopmembershipsync_on_oracle.py",
    "scripts/sync_and_deploy_ticket_startup.py",
    "systemd/mirror-world-whopmembershipsync.service",
]


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
    return str(REPO_ROOT / key_value)


def _scp_to(entry: dict, local: Path, remote: str, timeout: int = 90) -> subprocess.CompletedProcess:
    key = _resolve_key_path(entry["key"])
    scp_args = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        scp_args.extend(shlex.split(opts))
    scp_args.extend([str(local), f"{entry['user']}@{entry['host']}:{remote}"])
    return subprocess.run(scp_args, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    servers = _load_servers()
    if not servers:
        print("No servers in oraclekeys/servers.json", file=sys.stderr)
        return 1
    entry = servers[0]
    rr = entry.get("remote_root") or "/home/rsadmin/bots/mirror-world"
    key = _resolve_key_path(entry["key"])
    opts = str(entry.get("ssh_options", "")).strip()

    print("=== Sync local -> Oracle (match repo) ===\n")
    print(f"Server: {entry.get('name', entry.get('host'))}")
    print(f"Remote root: {rr}\n")

    # Ensure remote dirs (systemd may be missing or unwritable; we'll try and fallback)
    ssh_base = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    if opts:
        ssh_base.extend(shlex.split(opts))
    ssh_base.append(f"{entry['user']}@{entry['host']}")
    for d in ["systemd", "scripts"]:
        subprocess.run(ssh_base + ["mkdir", "-p", f"{rr}/{d}"], capture_output=True, text=True, timeout=30)

    failed = []
    for rel in SYNC_FILES:
        local = REPO_ROOT / rel
        if not local.exists():
            print(f"  SKIP (missing): {rel}")
            continue
        remote = f"{rr}/{rel}"
        res = _scp_to(entry, local, remote, timeout=90)
        if res.returncode != 0:
            # If systemd unit fails (permission denied), put copy in WhopMembershipSync/
            if "systemd/" in rel and "Permission denied" in (res.stderr or res.stdout):
                alt = f"{rr}/WhopMembershipSync/mirror-world-whopmembershipsync.service"
                res2 = _scp_to(entry, local, alt, timeout=60)
                if res2.returncode == 0:
                    print(f"  OK {rel} (-> WhopMembershipSync/ copy)")
                else:
                    print(f"  ERROR {rel}: {(res.stderr or res.stdout).strip()[:200]}")
                    failed.append(rel)
            else:
                print(f"  ERROR {rel}: {(res.stderr or res.stdout).strip()[:200]}")
                failed.append(rel)
        else:
            print(f"  OK {rel}")

    if failed:
        print(f"\nFailed: {len(failed)} file(s). Fix and re-run.")
        return 1
    print("\nDone. Oracle server now matches local repo for these files.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
