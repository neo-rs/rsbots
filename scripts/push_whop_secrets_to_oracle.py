#!/usr/bin/env python3
"""Push WhopMembershipSync/config.secrets.json from local to Oracle and restart the service."""

import json
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"
SECRETS_LOCAL = REPO_ROOT / "WhopMembershipSync" / "config.secrets.json"


def _load_servers():
    if not SERVERS_PATH.exists():
        raise FileNotFoundError(f"Missing {SERVERS_PATH}")
    return json.loads(SERVERS_PATH.read_text(encoding="utf-8") or "[]")


def _resolve_key_path(key_value: str) -> str:
    p = Path(key_value)
    if p.is_absolute() and p.exists():
        return str(p)
    c = REPO_ROOT / "oraclekeys" / key_value
    if c.exists():
        return str(c)
    return str(REPO_ROOT / key_value)


def main() -> int:
    if not SECRETS_LOCAL.exists():
        print(f"ERROR: {SECRETS_LOCAL} not found. Create it from config.secrets.example.json with your Whop API key.")
        return 1
    servers = _load_servers()
    if not servers:
        print("ERROR: No servers in oraclekeys/servers.json")
        return 1
    entry = servers[0]
    rr = entry.get("remote_root") or "/home/rsadmin/bots/mirror-world"
    key = _resolve_key_path(entry["key"])
    scp_args = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        scp_args.extend(shlex.split(opts))
    remote = f"{entry['user']}@{entry['host']}:{rr}/WhopMembershipSync/config.secrets.json"
    scp_args.extend([str(SECRETS_LOCAL), remote])
    print("Pushing WhopMembershipSync/config.secrets.json to Oracle...")
    r = subprocess.run(scp_args, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        print("SCP failed:", (r.stderr or r.stdout).strip()[:300])
        return 1
    print("OK. Restarting mirror-world-whopmembershipsync.service...")
    ssh_args = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    if opts:
        ssh_args.extend(shlex.split(opts))
    ssh_args.append(f"{entry['user']}@{entry['host']}")
    ssh_args.append("sudo systemctl restart mirror-world-whopmembershipsync.service")
    r2 = subprocess.run(ssh_args, capture_output=True, text=True, timeout=30)
    if r2.returncode != 0:
        print("Restart failed:", (r2.stderr or r2.stdout).strip()[:300])
        return 1
    print("Done. Service restarted.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
