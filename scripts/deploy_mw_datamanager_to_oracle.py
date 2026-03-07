#!/usr/bin/env python3
"""
Deploy MWDataManagerBot to Oracle: push MWBots repo, update on server, restart, then scan.

Uses oraclekeys/servers.json (first entry) and resolves SSH key from oraclekeys/ or oraclekey/.
Run from repo root. Requires: SSH key at oraclekey/ssh-key-*.key or oraclekeys/, and
push_mwbots_py_only.bat (or MWBots repo) for the push step.

Usage:
  py -3 scripts/deploy_mw_datamanager_to_oracle.py
  py -3 scripts/deploy_mw_datamanager_to_oracle.py --no-push   # skip git push (code already pushed)
  py -3 scripts/deploy_mw_datamanager_to_oracle.py --no-scan   # skip scanner at the end
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"

CODE_ROOT = "/home/rsadmin/bots/mwbots-code"
LIVE_ROOT = "/home/rsadmin/bots/mirror-world"
BOT_FOLDER = "MWDataManagerBot"
SERVICE = "mirror-world-datamanagerbot.service"


def _resolve_key_path(key_value: str) -> str:
    p = Path(key_value)
    if p.is_absolute() and p.exists():
        return str(p)
    for folder in ("oraclekeys", "oraclekey"):
        candidate = REPO_ROOT / folder / key_value
        if candidate.exists():
            return str(candidate)
    if (REPO_ROOT / key_value).exists():
        return str(REPO_ROOT / key_value)
    return str(p)


def _load_server() -> dict:
    if not SERVERS_PATH.exists():
        raise FileNotFoundError(f"Missing {SERVERS_PATH}")
    data = json.loads(SERVERS_PATH.read_text(encoding="utf-8") or "[]")
    if not data:
        raise ValueError("No server entry in servers.json")
    return data[0]


def _run(cmd: list[str], timeout: int = 300, check: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(REPO_ROOT))
    if check and r.returncode != 0:
        print(r.stdout or "")
        print(r.stderr or "", file=sys.stderr)
        raise RuntimeError(f"Command failed with exit {r.returncode}")
    return r


def _bash_script() -> str:
    """Same logic as RSAdminBot._github_py_only_update for MWDataManagerBot."""
    return f"""
set -euo pipefail
CODE_ROOT={shlex.quote(CODE_ROOT)}
LIVE_ROOT={shlex.quote(LIVE_ROOT)}
BOT_FOLDER={shlex.quote(BOT_FOLDER)}

if [ ! -d "$CODE_ROOT/.git" ]; then
  echo "ERR=missing_code_root"
  exit 2
fi
if [ ! -d "$LIVE_ROOT" ]; then
  echo "ERR=missing_live_root"
  exit 2
fi

cd "$CODE_ROOT"
OLD="$(git rev-parse HEAD 2>/dev/null || echo '')"
git fetch origin
git pull --ff-only origin main
NEW="$(git rev-parse HEAD)"

TMP_ALL_LIST="/tmp/mw_tracked_$BOT_FOLDER.txt"
git ls-files "$BOT_FOLDER" 2>/dev/null > "$TMP_ALL_LIST" || true

TMP_PY_LIST="/tmp/mw_pyonly_$BOT_FOLDER.txt"
grep -E '\\.py$' "$TMP_ALL_LIST" > "$TMP_PY_LIST" || true
PY_COUNT="$(wc -l < "$TMP_PY_LIST" | tr -d ' ')"
if [ "$PY_COUNT" = "0" ]; then
  echo "ERR=no_python_files"
  exit 3
fi

TMP_SYNC_LIST="/tmp/mw_sync_$BOT_FOLDER.txt"
grep -E '(\\.py$|\\.md$|\\.json$|\\.txt$|(^|/)requirements\\.txt$)' "$TMP_ALL_LIST" | grep -v -E '(^|/)config\\.secrets\\.json$' > "$TMP_SYNC_LIST" || true
grep -v "^$BOT_FOLDER/config/settings\\.json$" "$TMP_SYNC_LIST" > "$TMP_SYNC_LIST.ex" 2>/dev/null && mv "$TMP_SYNC_LIST.ex" "$TMP_SYNC_LIST" || true
sort -u "$TMP_SYNC_LIST" -o "$TMP_SYNC_LIST"

TMP_SHARED_LIST="/tmp/mw_shared_$BOT_FOLDER.txt"
git ls-files "shared" 2>/dev/null | grep -E '(\\.py$|\\.md$|\\.json$|\\.txt$|(^|/)requirements\\.txt$)' | grep -v -E '(^|/)config\\.secrets\\.json$' > "$TMP_SHARED_LIST" || true
cat "$TMP_SYNC_LIST" "$TMP_SHARED_LIST" | sort -u > "$TMP_SYNC_LIST.merged"
mv "$TMP_SYNC_LIST.merged" "$TMP_SYNC_LIST"

TS="$(date +%Y%m%d_%H%M%S)"
SAFE_BOT="$(echo "$BOT_FOLDER" | tr '/' '_')"
BACKUP_DIR="$LIVE_ROOT/backups"
mkdir -p "$BACKUP_DIR"
BACKUP_TAR="$BACKUP_DIR/${{SAFE_BOT}}_preupdate_${{TS}}.tar.gz"
(cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar --ignore-failed-read -czf "$BACKUP_TAR" -T "$TMP_SYNC_LIST") || true

env -u TAR_OPTIONS /bin/tar -cf - -T "$TMP_SYNC_LIST" | (cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar -xf - --overwrite)

# Sync MWDiscumBot into mirror-world so DataManagerBot can import discum_command_bot.py (for /discum)
DISCUM_FOLDER="MWDiscumBot"
if [ -d "$CODE_ROOT/$DISCUM_FOLDER" ]; then
  TMP_D="$(mktemp -d)"
  git ls-files "$DISCUM_FOLDER" 2>/dev/null | grep -E '(\\.py$|\\.md$|\\.json$)' | grep -v -E '(^|/)config\\.secrets\\.json$' > "$TMP_D/list" || true
  if [ -s "$TMP_D/list" ]; then
    (cd "$CODE_ROOT" && env -u TAR_OPTIONS /bin/tar -cf - -T "$TMP_D/list") | (cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar -xf - --overwrite)
  fi
  rm -rf "$TMP_D"
fi

echo "OK=1"
echo "OLD=$OLD"
echo "NEW=$NEW"
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Deploy MWDataManagerBot to Oracle and optionally scan.")
    ap.add_argument("--no-push", action="store_true", help="Skip pushing MWBots to GitHub")
    ap.add_argument("--no-scan", action="store_true", help="Skip running scan_discord_registered_commands.py")
    ap.add_argument("--fetch-logs", action="store_true", help="SSH and print last 60 lines of DataManagerBot service log (then exit)")
    args = ap.parse_args()

    server = _load_server()
    user = server.get("user", "rsadmin")
    host = server.get("host", "")
    key = server.get("key", "")
    if not host or not key:
        print("ERROR: servers.json entry must have host and key", file=sys.stderr)
        return 1

    key_path = _resolve_key_path(key)
    if not Path(key_path).exists():
        print(f"ERROR: SSH key not found: {key_path}", file=sys.stderr)
        return 1

    ssh_opts = "-o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ConnectTimeout=60"
    ssh_base = ["ssh", "-i", key_path] + ssh_opts.split() + [f"{user}@{host}"]

    if args.fetch_logs:
        print("Fetching last 60 lines of mirror-world-datamanagerbot.service ...")
        r = subprocess.run(
            ssh_base + ["sudo", "journalctl", "-u", SERVICE, "-n", "60", "--no-pager"],
            capture_output=True,
            text=True,
            timeout=15,
            cwd=str(REPO_ROOT),
        )
        out = (r.stdout or "") + (r.stderr or "")
        print(out if out else "(no output)")
        return 0 if r.returncode == 0 else 1

    # 1. Push MWBots to GitHub
    if not args.no_push:
        print("[1/4] Pushing MWBots to GitHub ...")
        bat = REPO_ROOT / "push_mwbots_py_only.bat"
        if not bat.exists():
            print("WARN: push_mwbots_py_only.bat not found, skipping push.", file=sys.stderr)
        else:
            _run(["cmd", "/c", str(bat)], timeout=120)
        print("  Done.")
    else:
        print("[1/4] Skipping push (--no-push).")

    # 2. On Oracle: pull mwbots-code and sync MWDataManagerBot into mirror-world
    print("[2/4] On Oracle: git pull + sync MWDataManagerBot ...")
    script = _bash_script().replace("\r\n", "\n").replace("\r", "\n")
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".sh", delete=False) as f:
        f.write(script.encode("utf-8"))
        tmp_path = f.name
    try:
        with open(tmp_path, "rb") as stdin_file:
            r = subprocess.run(
                ssh_base + ["bash", "-s"],
                stdin=stdin_file,
                capture_output=True,
                text=False,
                timeout=180,
                cwd=str(REPO_ROOT),
            )
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    if r.returncode != 0:
        print((r.stdout or b"").decode("utf-8", errors="replace"))
        print((r.stderr or b"").decode("utf-8", errors="replace"), file=sys.stderr)
        print("ERROR: Remote update failed.", file=sys.stderr)
        return 1
    out = (r.stdout or b"").decode("utf-8", errors="replace")
    for line in out.splitlines():
        if line.startswith("OLD=") or line.startswith("NEW=") or line.startswith("OK="):
            print("  ", line)
    print("  Done.")

    # 3. Restart DataManagerBot service
    print("[3/4] Restarting DataManagerBot service ...")
    r = subprocess.run(
        ssh_base + ["sudo", "systemctl", "restart", SERVICE],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(REPO_ROOT),
    )
    if r.returncode != 0:
        print(r.stderr or r.stdout or "restart failed", file=sys.stderr)
        print("WARN: Restart may have failed (e.g. sudo prompt). Check server.", file=sys.stderr)
    else:
        print("  Done.")
    print("  Waiting 8s for bot to start and sync commands ...")
    time.sleep(8)

    # 4. Run scanner
    if not args.no_scan:
        print("[4/4] Scanning registered slash commands (datamanagerbot) ...")
        scan_script = REPO_ROOT / "scripts" / "scan_discord_registered_commands.py"
        r = subprocess.run(
            [sys.executable, str(scan_script), "--bots", "datamanagerbot"],
            cwd=str(REPO_ROOT),
            timeout=30,
        )
        if r.returncode != 0:
            print("WARN: Scanner exited with", r.returncode, "(e.g. no token in MWBots or snapshot)", file=sys.stderr)
    else:
        print("[4/4] Skipping scan (--no-scan).")

    print()
    print("Deploy complete. If /discum is still missing in Discord, ensure DataManagerBot token is correct and re-run scanner with tokens (e.g. from Oraclserver-files-mwbots snapshot).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
