#!/usr/bin/env python3
"""
Deploy DailyScheduleReminder ticket-startup idempotency fix to Oracle.

1. Disable ticket_startup on Oracle and clear pending file (stop spam)
2. Restart DailyScheduleReminder
3. Upload reminder_bot.py
4. Re-enable ticket_startup
5. Restart DailyScheduleReminder
6. Wait and show journal

Usage:
  python scripts/deploy_dailyschedulereminder_fix.py
  python scripts/deploy_dailyschedulereminder_fix.py --server-name "instance-enhance (rsadmin)"
"""

import argparse
import json
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"


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


def _ssh_cmd(entry: dict, cmd: str, timeout: int = 60) -> subprocess.CompletedProcess:
    key = _resolve_key_path(entry["key"])
    ssh_args = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        ssh_args.extend(shlex.split(opts))
    ssh_args.append(f"{entry['user']}@{entry['host']}")
    ssh_args.extend(["sh", "-c", cmd])
    return subprocess.run(ssh_args, capture_output=True, text=True, timeout=timeout)


def _scp_to(entry: dict, local: Path, remote: str, timeout: int = 60) -> subprocess.CompletedProcess:
    key = _resolve_key_path(entry["key"])
    scp_args = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "")).strip()
    if opts:
        scp_args.extend(shlex.split(opts))
    scp_args.extend([str(local), f"{entry['user']}@{entry['host']}:{remote}"])
    return subprocess.run(scp_args, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    ap = argparse.ArgumentParser(description="Deploy DailyScheduleReminder fix to Oracle")
    ap.add_argument("--server-name", default=None)
    ap.add_argument("--finish", action="store_true", help="Skip steps 1-3 (disable/upload), only re-enable + restart + journal")
    args = ap.parse_args()

    servers = _load_servers()
    entry = _pick_server(servers, args.server_name)
    rr = entry.get("remote_root") or "/home/rsadmin/bots/mirror-world"
    bot_dir = f"{rr}/DailyScheduleReminder"
    rsc_data = f"{rr}/RSCheckerbot/data"
    svc = "mirror-world-dailyschedulereminder.service"

    print("=== Deploy DailyScheduleReminder fix to Oracle ===\n")

    if not args.finish:
        # 1. Disable ticket_startup and clear pending (stop spam)
        print("[1/6] Disabling ticket_startup and clearing pending file on Oracle...")
        disable_script = f"""
import json
from pathlib import Path

cfg = Path("{bot_dir}/config.json")
if cfg.exists():
    c = json.loads(cfg.read_text())
    c.setdefault("ticket_startup", {{}})["enabled"] = False
    cfg.write_text(json.dumps(c, indent=2))

pend = Path("{rsc_data}/pending_ticket_startup_messages.json")
if pend.exists():
    from datetime import datetime, timezone
    pend.write_text(json.dumps({{"pending": [], "updated_at_iso": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}}, indent=2))
print("OK")
"""
        res = _ssh_cmd(entry, f"cd {rr} && python3 -c {shlex.quote(disable_script)}", timeout=60)
        if res.returncode != 0:
            print(res.stderr or res.stdout)
            return 1
        print("  Done.\n")

        # 2. Restart service (picks up config change, stops sending)
        print("[2/6] Restarting DailyScheduleReminder...")
        res = _ssh_cmd(entry, f"sudo systemctl restart {svc}", timeout=45)
        if res.returncode != 0:
            print("  WARNING:", res.stderr or res.stdout)
        else:
            print("  Restarted.\n")

        # 3. Upload reminder_bot.py
        local_py = REPO_ROOT / "DailyScheduleReminder" / "reminder_bot.py"
        if not local_py.exists():
            print(f"ERROR: {local_py} not found")
            return 1
        print("[3/6] Uploading reminder_bot.py...")
        res = _scp_to(entry, local_py, f"{bot_dir}/reminder_bot.py", timeout=90)
        if res.returncode != 0:
            print(res.stderr or res.stdout)
            return 1
        print("  Uploaded.\n")

    # 4. Re-enable ticket_startup
    print("[4/6] Re-enabling ticket_startup...")
    enable_script = f'''import json
from pathlib import Path
cfg = Path("{bot_dir}/config.json")
c = json.loads(cfg.read_text())
c.setdefault("ticket_startup", {{}})["enabled"] = True
cfg.write_text(json.dumps(c, indent=2))
print("OK")
'''
    tmp_py = REPO_ROOT / "scripts" / "_enable_ticket_startup.py"
    tmp_py.write_text(enable_script)
    try:
        res = _scp_to(entry, tmp_py, "/tmp/enable_ticket_startup.py", timeout=30)
        if res.returncode != 0:
            print(res.stderr or res.stdout)
            return 1
        res = _ssh_cmd(entry, "python3 /tmp/enable_ticket_startup.py", timeout=30)
    finally:
        tmp_py.unlink(missing_ok=True)
    if res.returncode != 0:
        print(res.stderr or res.stdout)
        return 1
    print("  Done.\n")

    # 5. Restart again (picks up new code + enabled config)
    print("[5/6] Restarting DailyScheduleReminder...")
    res = _ssh_cmd(entry, f"sudo systemctl restart {svc}", timeout=45)
    if res.returncode != 0:
        print("  WARNING:", res.stderr or res.stdout)
    else:
        print("  Restarted.\n")

    # 6. Wait and show journal
    print("[6/6] Waiting 10s, then showing journal (last 25 lines)...")
    time.sleep(10)
    res = _ssh_cmd(entry, f"journalctl -u {svc} -n 25 --no-pager", timeout=30)
    print(res.stdout or res.stderr)
    if res.returncode != 0:
        print("  (journalctl may require sudo)")
    print("\n=== Deploy complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
