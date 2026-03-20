#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mirror_world_config import load_oracle_servers, resolve_oracle_ssh_key_path


BOT_KEY_TO_FOLDER: Dict[str, str] = {
    # RS bots
    "rsadminbot": "RSAdminBot",
    "rsforwarder": "RSForwarder",
    "rsonboarding": "RSOnboarding",
    "rsmentionpinger": "RSMentionPinger",
    "rscheckerbot": "RSCheckerbot",
    "rssuccessbot": "RSuccessBot",
    "rspromobot": "RSPromoBot",
    "whopmembershipsync": "WhopMembershipSync",
    # Mirror-world bots
    "dailyschedulereminder": "DailyScheduleReminder",
    "datamanagerbot": "MWDataManagerBot",
    "discumbot": "MWDiscumBot",
    "instorebotforwarder": "Instorebotforwarder",
    "pingbot": "MWPingBot",
}


def _pick_server(servers: List[Dict[str, Any]], server_name: Optional[str]) -> Dict[str, Any]:
    if not servers:
        raise ValueError("No servers configured in oraclekeys/servers.json")
    if server_name and server_name.strip():
        name = server_name.strip()
        for s in servers:
            if str(s.get("name", "")).strip() == name:
                return s
        raise ValueError(f"Server name not found in oraclekeys/servers.json: {name}")
    return servers[0]


def _load_rsadmin_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "RSAdminBot" / "config.json"
    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("RSAdminBot/config.json is not a JSON object")
    return raw


def _build_ssh_cmd(*, user: str, host: str, key_path: Path, ssh_options: str, remote_cmd: str) -> List[str]:
    cmd = [
        "ssh",
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
    ]
    if ssh_options:
        cmd.extend(shlex.split(ssh_options))
    cmd.append(f"{user}@{host}")

    # Use a single remote command string; inside remote we run `bash -lc ...`.
    cmd.append(f"bash -lc {shlex.quote(remote_cmd)}")
    return cmd


def _update_snippet(*, code_root: str, live_root: str, bot_folder: str) -> str:
    """
    Sync tracked python-only files (plus a small safe set of md/json/txt/requirements)
    from the bot's code checkout into the live tree, excluding secrets/runtime data.
    """
    code_root_q = shlex.quote(code_root)
    live_root_q = shlex.quote(live_root)
    bot_folder_q = shlex.quote(bot_folder)

    # Note: this closely mirrors RSAdminBot's _github_py_only_update safety model:
    # - Never overwrite config.secrets.json
    # - Never overwrite member_history.json (server-owned)
    # - Allow only specific extensions
    # - Backup before overwrite (best-effort)
    return f"""
set -euo pipefail
CODE_ROOT={code_root_q}
LIVE_ROOT={live_root_q}
BOT_FOLDER={bot_folder_q}

if [ ! -d "$CODE_ROOT/.git" ]; then
  echo "ERR=missing_code_root"
  exit 2
fi
if [ ! -d "$LIVE_ROOT" ]; then
  echo "ERR=missing_live_root"
  exit 2
fi

cd "$CODE_ROOT"
#
# Ensure the checkout is clean before git pull.
# Without this, any untracked runtime artifacts (e.g. Playwright cache files)
# can cause `git pull --ff-only` to abort with:
#   "local changes ... would be overwritten by merge"
#   "untracked working tree files would be overwritten"
#
git reset --hard HEAD >/dev/null 2>&1 || true
git clean -fdx >/dev/null 2>&1 || true

OLD="$(git rev-parse HEAD 2>/dev/null || echo '')"
git fetch origin
git pull --ff-only origin main
NEW="$(git rev-parse HEAD)"

TMP_ALL_LIST="/tmp/mw_tracked_${{BOT_FOLDER}}.txt"
git ls-files "$BOT_FOLDER" 2>/dev/null > "$TMP_ALL_LIST" || true

TMP_PY_LIST="/tmp/mw_pyonly_${{BOT_FOLDER}}.txt"
grep -E "\\.py$" "$TMP_ALL_LIST" > "$TMP_PY_LIST" || true
PY_COUNT="$(wc -l < "$TMP_PY_LIST" | tr -d " ")"
if [ "$PY_COUNT" = "" ]; then PY_COUNT="0"; fi
if [ "$PY_COUNT" = "0" ]; then
  echo "ERR=no_python_files"
  exit 3
fi

TMP_SYNC_LIST="/tmp/mw_sync_${{BOT_FOLDER}}.txt"
grep -E "(\\.py$|\\.md$|\\.json$|\\.txt$|(^|/)requirements\\.txt$)" "$TMP_ALL_LIST" | \\
  grep -v -E "(^|/)config\\.secrets\\.json$" > "$TMP_SYNC_LIST" || true
grep -v -E "(^|/)tokens\\.env$" "$TMP_SYNC_LIST" > "$TMP_SYNC_LIST.ex" 2>/dev/null && mv "$TMP_SYNC_LIST.ex" "$TMP_SYNC_LIST" || true
grep -v -E "(^|/)member_history\\.json$" "$TMP_SYNC_LIST" > "$TMP_SYNC_LIST.ex" 2>/dev/null && mv "$TMP_SYNC_LIST.ex" "$TMP_SYNC_LIST" || true
sort -u "$TMP_SYNC_LIST" -o "$TMP_SYNC_LIST" || true

TMP_SHARED_LIST="/tmp/mw_shared_${{BOT_FOLDER}}.txt"
git ls-files "shared" 2>/dev/null | \\
  grep -E "(\\.py$|\\.md$|\\.json$|\\.txt$|(^|/)requirements\\.txt$)" | \\
  grep -v -E "(^|/)config\\.secrets\\.json$" > "$TMP_SHARED_LIST" || true

cat "$TMP_SYNC_LIST" "$TMP_SHARED_LIST" | sort -u > "${{TMP_SYNC_LIST}}.merged" || true
mv "${{TMP_SYNC_LIST}}.merged" "$TMP_SYNC_LIST"

SYNC_COUNT="$(wc -l < "$TMP_SYNC_LIST" | tr -d " ")"
if [ "$SYNC_COUNT" = "" ]; then SYNC_COUNT="0"; fi

# Backup (best-effort)
TS="$(date +%Y%m%d_%H%M%S)"
SAFE_BOT="$(echo "$BOT_FOLDER" | tr '/' '_')"
BACKUP_DIR="$LIVE_ROOT/backups"
mkdir -p "$BACKUP_DIR" || true
BACKUP_TAR="$BACKUP_DIR/${{SAFE_BOT}}_preupdate_${{TS}}.tar.gz"
(cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar --ignore-failed-read -czf "$BACKUP_TAR" -T "$TMP_SYNC_LIST") || true

# Sync (always overwrite tracked safe list)
env -u TAR_OPTIONS /bin/tar -cf - -T "$TMP_SYNC_LIST" | (cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar -xf - --overwrite)

echo "OK=1"
echo "OLD=$OLD"
echo "NEW=$NEW"
echo "PY_COUNT=$PY_COUNT"
echo "SYNC_COUNT=$SYNC_COUNT"
echo "CHANGED_BEGIN"
echo "(not computed by this lightweight runner)"
echo "CHANGED_END"
"""


def _restart_snippet(*, live_root: str, bot_key: str) -> str:
    live_root_q = shlex.quote(live_root)
    return f"""
set -euo pipefail
bash {live_root_q}/RSAdminBot/botctl.sh restart {shlex.quote(bot_key)}
"""


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Update MW/RS bots on Oracle from GitHub checkouts via SSH.")
    ap.add_argument("--group", choices=["mw", "rs"], required=True, help="Update group: mw=mirror_bots, rs=rs_bots + rsadminbot")
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json")
    ap.add_argument(
        "--bot",
        default=None,
        help="Single bot key to update (e.g. discumbot). If omitted, prompts you to choose.",
    )
    args = ap.parse_args(argv)

    rsadmin_cfg = _load_rsadmin_config()
    bot_groups = rsadmin_cfg.get("bot_groups") or {}
    code_checkouts = rsadmin_cfg.get("code_checkouts") or {}

    if not isinstance(bot_groups, dict):
        raise ValueError("RSAdminBot/config.json: bot_groups missing or not a dict")
    if not isinstance(code_checkouts, dict):
        raise ValueError("RSAdminBot/config.json: code_checkouts missing or not a dict")

    servers, _ = load_oracle_servers(REPO_ROOT)
    server = _pick_server(servers, args.server_name)

    user = str(server.get("user", "rsadmin"))
    host = str(server.get("host", "")).strip()
    key_val = str(server.get("key", "")).strip()
    ssh_options = str(server.get("ssh_options", "") or "")
    remote_root = str(server.get("remote_root") or server.get("live_root") or "/home/rsadmin/bots/mirror-world")

    if not host:
        raise ValueError("servers.json entry missing host")
    if not key_val:
        raise ValueError("servers.json entry missing key")

    key_path = resolve_oracle_ssh_key_path(key_val, REPO_ROOT)
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key not found: {key_path}")

    if args.group == "mw":
        bots = list(bot_groups.get("mirror_bots") or [])
        bots = [str(b).strip().lower() for b in bots if str(b).strip()]
        code_root = str(code_checkouts.get("mwbots_code_root") or "/home/rsadmin/bots/mwbots-code")
    else:
        rs_bots = list(bot_groups.get("rs_bots") or [])
        rs_bots = [str(b).strip().lower() for b in rs_bots if str(b).strip()]
        bots = ["rsadminbot"] + rs_bots
        code_root = str(code_checkouts.get("rsbots_code_root") or "/home/rsadmin/bots/rsbots-code")

    bots = [b for b in bots if b in BOT_KEY_TO_FOLDER]
    if not bots:
        raise ValueError(f"No bots found for group {args.group} after filtering")

    print(f"Server: {user}@{host}")
    print(f"Remote root: {remote_root}")
    print(f"Group: {args.group}")
    print(f"Available bots: {', '.join(bots)}")
    print()

    chosen: List[str]
    bot_arg = (args.bot or "").strip().lower()
    if bot_arg:
        if bot_arg not in bots:
            raise ValueError(f"Requested bot '{bot_arg}' not in available bots for group {args.group}")
        chosen = [bot_arg]
    else:
        # Interactive selection.
        print("Choose which bot(s) to update (numbers).")
        for i, b in enumerate(bots, start=1):
            print(f"  {i}. {b}")
        print("  0. Cancel")
        raw = input("Selection (e.g. 1 or 2,3 or 'all'): ").strip().lower()
        if raw in {"", "0", "cancel", "c"}:
            print("Cancelled.")
            return 0
        if raw in {"all", "a"}:
            chosen = bots
        else:
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            idxs: List[int] = []
            for p in parts:
                try:
                    idxs.append(int(p))
                except ValueError:
                    raise ValueError(f"Invalid selection token: {p!r}")
            # Map idx -> bot key (1-based)
            chosen = []
            for idx in idxs:
                if idx < 1 or idx > len(bots):
                    raise ValueError(f"Bot index out of range: {idx}")
                b = bots[idx - 1]
                if b not in chosen:
                    chosen.append(b)
        if not chosen:
            print("No bots selected. Cancelled.")
            return 0

    for bot_key in chosen:
        bot_folder = BOT_KEY_TO_FOLDER[bot_key]
        print(f"=== Updating {bot_key} (folder: {bot_folder}) ===")

        update_cmd = _update_snippet(code_root=code_root, live_root=remote_root, bot_folder=bot_folder)
        restart_cmd = _restart_snippet(live_root=remote_root, bot_key=bot_key)
        remote_cmd = f"{update_cmd}\n{restart_cmd}"

        ssh_cmd = _build_ssh_cmd(
            user=user,
            host=host,
            key_path=key_path,
            ssh_options=ssh_options,
            remote_cmd=remote_cmd,
        )

        res = subprocess.run(ssh_cmd, capture_output=True, text=True)
        if res.stdout:
            print(res.stdout.strip())
        if res.stderr:
            # Some commands print progress to stderr; keep short.
            stderr_clean = (res.stderr or "").strip()
            if stderr_clean:
                print(stderr_clean[:8000])
        if res.returncode != 0:
            print(f"\nERROR: Update failed for {bot_key} (exit {res.returncode})")
            return res.returncode

    print("\nDONE: Updates complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

