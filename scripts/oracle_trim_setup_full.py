#!/usr/bin/env python3
"""
One-shot Oracle log trim setup: ensure cron, run trim, install daily cron, verify sizes.
Streams all SSH output so you see live what runs on the server.
Uses canonical config: oraclekeys/servers.json + RSAdminBot/config.json ssh_server_name.
"""
import json
import os
import shlex
import subprocess
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


def main() -> int:
    config_path = REPO_ROOT / "RSAdminBot" / "config.json"
    if not config_path.exists():
        print("ERROR: RSAdminBot/config.json not found")
        return 1
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    server_name = (config.get("ssh_server_name") or "").strip()
    if not server_name:
        print("ERROR: Missing ssh_server_name in RSAdminBot/config.json")
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
        # Quote for remote shell: wrap in single quotes, escape any ' as '"'"'
        q = "'" + remote_cmd.replace("'", "'\"'\"'") + "'"
        cmd = (
            _ssh_cmd()
            + ["-i", str(key_path)]
            + opts
            + [f"{user}@{host}", f"bash -lc {q}"]
        )
        r = subprocess.run(cmd, cwd=str(REPO_ROOT))
        return r.returncode

    failed = False

    print("=== 1. Ensure cron is installed (sudo if needed) ===", flush=True)
    rc = run_ssh(
        "if ! command -v crontab >/dev/null 2>&1; then "
        "sudo apt-get update -qq && sudo apt-get install -y cron && sudo systemctl enable --now cron 2>/dev/null || true; "
        "fi; command -v crontab && echo cron_ok"
    )
    if rc != 0:
        print("WARNING: cron check/install had issues (continuing anyway)", flush=True)
    print()

    print("=== 2. Run trim script (cap log files to 30MB) ===", flush=True)
    rc = run_ssh(f"cd {remote_root} && bash RSAdminBot/scripts/trim_oracle_logs.sh")
    if rc != 0:
        failed = True
        print("FAILED: trim script", flush=True)
    print()

    print("=== 3. Install daily cron (03:00) ===", flush=True)
    rc = run_ssh(f"cd {remote_root} && bash RSAdminBot/scripts/install_trim_cron.sh")
    if rc != 0:
        failed = True
        print("FAILED: cron install", flush=True)
    print()

    print("=== 4. Verify log sizes and disk ===", flush=True)
    rc = run_ssh(
        "echo '--- Log dirs/files ---'; "
        "du -sh /home/rsadmin/bots/logs/rsadminbot 2>/dev/null || true; "
        "du -sh /home/rsadmin/bots/mirror-world/MWDataManagerBot/logs/decision_traces.jsonl 2>/dev/null || true; "
        "for f in /home/rsadmin/bots/mirror-world/logs/systemd_*.log; do [ -f \"$f\" ] && du -sh \"$f\"; done 2>/dev/null || true; "
        "echo '--- Disk (/) ---'; df -h / | tail -1"
    )
    if rc != 0:
        print("WARNING: verify step had issues", flush=True)
    print()

    if failed:
        print("One or more steps failed. Check output above.")
        return 1
    print("All done. Logs are capped; daily trim at 03:00 is installed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
