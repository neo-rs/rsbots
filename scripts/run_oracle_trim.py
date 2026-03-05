#!/usr/bin/env python3
"""
Run Oracle log trim once (and optionally install daily cron).
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


def main():
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

    remote_root = f"/home/{user}/bots/mirror-world"
    opts = shlex.split(ssh_opts_str) if ssh_opts_str else []

    def run_ssh(remote_cmd: str) -> int:
        cmd = (
            _ssh_cmd()
            + ["-i", str(key_path)]
            + opts
            + [f"{user}@{host}", f"bash -lc '{remote_cmd}'"]
        )
        r = subprocess.run(cmd, cwd=str(REPO_ROOT))
        return r.returncode

    print("Running trim script on Oracle once...")
    if run_ssh(f"cd {remote_root} && bash RSAdminBot/scripts/trim_oracle_logs.sh") != 0:
        print("SSH or trim failed.")
        return 1

    print("\nTrim done. Install daily cron (03:00) for automatic trim? [y/N] ", end="", flush=True)
    try:
        install = (input() or "n").strip().lower() == "y"
    except EOFError:
        install = False
    if install:
        print("Installing cron...")
        if run_ssh(f"cd {remote_root} && bash RSAdminBot/scripts/install_trim_cron.sh") != 0:
            print("Cron install failed.")
            return 1
        print("Cron installed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())