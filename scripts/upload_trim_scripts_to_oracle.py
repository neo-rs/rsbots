#!/usr/bin/env python3
"""
Upload RSAdminBot trim .sh scripts to the Oracle server.
Uses canonical SSH config: oraclekeys/servers.json + RSAdminBot/config.json ssh_server_name.
See CANONICAL_RULES.md (Ubuntu access and RS-bots deployment).
"""
import json
import os
import subprocess
import shlex
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mirror_world_config import load_oracle_servers, pick_oracle_server, resolve_oracle_ssh_key_path


def _ssh_cmd() -> list:
    if sys.platform == "win32":
        w = os.environ.get("WINDIR", "C:\\Windows")
        p = Path(w) / "System32" / "OpenSSH" / "ssh.exe"
        return [str(p)] if p.exists() else ["ssh"]
    return ["ssh"]


TRIM_SCRIPTS = [
    "RSAdminBot/scripts/trim_oracle_logs.sh",
    "RSAdminBot/scripts/install_trim_cron.sh",
]


def _scp_cmd() -> list:
    if sys.platform == "win32":
        w = os.environ.get("WINDIR", "C:\\Windows")
        p = Path(w) / "System32" / "OpenSSH" / "scp.exe"
        return [str(p)] if p.exists() else ["scp"]
    return ["scp"]


def main() -> int:
    config_path = REPO_ROOT / "RSAdminBot" / "config.json"
    if not config_path.exists():
        print("ERROR: RSAdminBot/config.json not found")
        return 1
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    server_name = (config.get("ssh_server_name") or "").strip()
    if not server_name:
        print("ERROR: Missing ssh_server_name in RSAdminBot/config.json (must match oraclekeys/servers.json)")
        return 1

    servers, _ = load_oracle_servers(REPO_ROOT)
    entry = pick_oracle_server(servers, server_name)
    host = (entry.get("host") or "").strip()
    user = (entry.get("user") or "").strip() or "rsadmin"
    key_value = (entry.get("key") or "").strip()
    ssh_opts_str = (entry.get("ssh_options") or "").strip()

    if not host or not key_value:
        print("ERROR: servers.json entry missing host or key")
        return 1

    key_path = resolve_oracle_ssh_key_path(key_value, REPO_ROOT)
    if not key_path.exists():
        print(f"ERROR: SSH key not found: {key_path}")
        return 1

    remote_dir = f"/home/{user}/bots/mirror-world/RSAdminBot/scripts"
    opts = shlex.split(ssh_opts_str) if ssh_opts_str else []
    ssh_cmd = _ssh_cmd() + ["-i", str(key_path)] + opts + [f"{user}@{host}", f"mkdir -p {remote_dir}"]
    if subprocess.run(ssh_cmd, cwd=str(REPO_ROOT)).returncode != 0:
        print("ERROR: Could not create remote scripts directory")
        return 1

    scp_base = _scp_cmd() + ["-i", str(key_path)] + opts

    for rel in TRIM_SCRIPTS:
        local = REPO_ROOT / rel
        if not local.exists():
            print(f"ERROR: Local file not found: {local}")
            return 1
        dest = f"{user}@{host}:{remote_dir}/"
        cmd = scp_base + [str(local), dest]
        print(f"Uploading {rel} -> {dest}")
        r = subprocess.run(cmd, cwd=str(REPO_ROOT))
        if r.returncode != 0:
            print(f"scp failed for {rel}")
            return 1
    print("Done. Trim scripts are on the Oracle server.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
