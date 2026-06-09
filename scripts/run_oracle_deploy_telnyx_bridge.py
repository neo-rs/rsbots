#!/usr/bin/env python3
"""Deploy telnyx_discord_sms_bridge to Oracle (code sync + install + restart).

Typical workflow:
  1) push_rsbots_py_only.bat   (commit + push code to GitHub)
  2) update_telnyx_bridge.bat  (sync this folder to Oracle and restart service)

Or one-shot from local without git push:
  py -3 scripts/run_oracle_deploy_telnyx_bridge.py --from-local
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mirror_world_config import load_oracle_servers, pick_oracle_server, resolve_oracle_ssh_key_path  # noqa: E402

BRIDGE_DIR = REPO_ROOT / "telnyx_discord_sms_bridge"
REMOTE_REL = "telnyx_discord_sms_bridge"
SKIP_NAMES = {".venv", "logs", "__pycache__", ".env"}


def _ssh(entry: dict, cmd: str, *, timeout: int = 300) -> subprocess.CompletedProcess:
    key = str(resolve_oracle_ssh_key_path(str(entry.get("key", "")), REPO_ROOT))
    args = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"]
    opts = str(entry.get("ssh_options", "") or "").strip()
    if opts:
        args.extend(shlex.split(opts))
    args.append(f'{entry["user"]}@{entry["host"]}')
    args.extend(["bash", "-lc", cmd])
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def _scp(entry: dict, local: Path, remote_path: str, *, timeout: int = 180) -> subprocess.CompletedProcess:
    key = str(resolve_oracle_ssh_key_path(str(entry.get("key", "")), REPO_ROOT))
    args = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "") or "").strip()
    if opts:
        args.extend(shlex.split(opts))
    args.extend([str(local), f'{entry["user"]}@{entry["host"]}:{remote_path}'])
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def _build_local_tar() -> Path:
    if not BRIDGE_DIR.is_dir():
        raise FileNotFoundError(BRIDGE_DIR)

    tmp = tempfile.NamedTemporaryFile(prefix="telnyx_bridge_", suffix=".tar.gz", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    def _filter(ti: tarfile.TarInfo) -> tarfile.TarInfo | None:
        parts = Path(ti.name).parts
        if any(part in SKIP_NAMES for part in parts):
            return None
        if ti.name.endswith(".pyc"):
            return None
        return ti

    with tarfile.open(tmp_path, "w:gz") as tar:
        tar.add(BRIDGE_DIR, arcname=REMOTE_REL, filter=_filter)
    return tmp_path


def _remote_sync_from_rsbots(entry: dict, remote_root: str) -> str:
    code_root = "/home/rsadmin/bots/rsbots-code"
    return f"""
set -euo pipefail
CODE_ROOT={shlex.quote(code_root)}
LIVE_ROOT={shlex.quote(remote_root)}
BOT_FOLDER={shlex.quote(REMOTE_REL)}

if [ ! -d "$CODE_ROOT/.git" ]; then
  echo "ERR=missing_rsbots_code_root"
  exit 2
fi

cd "$CODE_ROOT"
git fetch origin
git pull --ff-only origin main

TMP_LIST="/tmp/telnyx_bridge_sync.txt"
git ls-files "$BOT_FOLDER" | grep -E "(\\.py$|\\.md$|\\.json$|\\.txt$|\\.sh$|\\.bat$|\\.service$|requirements\\.txt$|\\.example$)" > "$TMP_LIST" || true
git ls-files "systemd/mirror-world-telnyx-discord-sms-bridge.service" >> "$TMP_LIST" || true
sort -u "$TMP_LIST" -o "$TMP_LIST"

COUNT="$(wc -l < "$TMP_LIST" | tr -d ' ')"
if [ "$COUNT" = "0" ]; then
  echo "ERR=no_tracked_files"
  exit 3
fi

env -u TAR_OPTIONS /bin/tar -cf - -T "$TMP_LIST" | (cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar -xf - --overwrite --no-same-owner --no-same-permissions)
echo "OK=sync_from_rsbots"
"""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Deploy Telnyx Discord SMS Bridge to Oracle.")
    ap.add_argument("--server-name", default=None)
    ap.add_argument(
        "--from-local",
        action="store_true",
        help="Upload telnyx_discord_sms_bridge from this workspace (skip rsbots-code git pull).",
    )
    ap.add_argument("--skip-install", action="store_true", help="Only sync files; do not run install_oracle.sh.")
    args = ap.parse_args(argv)

    servers, _ = load_oracle_servers(REPO_ROOT)
    entry = pick_oracle_server(servers, args.server_name) if args.server_name else servers[0]
    remote_root = str(entry.get("remote_root") or "/home/rsadmin/bots/mirror-world").rstrip("/")

    print(f"Server: {entry.get('user')}@{entry.get('host')}")
    print(f"Remote root: {remote_root}")

    if args.from_local:
        tar_path = _build_local_tar()
        remote_tar = f"/tmp/{tar_path.name}"
        print(f"Uploading local bundle: {tar_path}")
        scp_res = _scp(entry, tar_path, remote_tar, timeout=180)
        if scp_res.returncode != 0:
            print(scp_res.stderr or scp_res.stdout, file=sys.stderr)
            return scp_res.returncode or 1

        extract_cmd = f"""
set -euo pipefail
mkdir -p {shlex.quote(remote_root)}/systemd
mkdir -p {shlex.quote(remote_root)}
cd {shlex.quote(remote_root)}
tar -xzf {shlex.quote(remote_tar)}
rm -f {shlex.quote(remote_tar)}
echo OK=extract_local
"""
        # Ensure bridge-local deploy unit exists (install_oracle.sh fallback path).
        unit_local = BRIDGE_DIR / "deploy" / "mirror-world-telnyx-discord-sms-bridge.service"
        if not unit_local.is_file():
            root_unit = REPO_ROOT / "systemd" / "mirror-world-telnyx-discord-sms-bridge.service"
            if root_unit.is_file():
                unit_local.parent.mkdir(parents=True, exist_ok=True)
                unit_local.write_text(root_unit.read_text(encoding="utf-8"), encoding="utf-8")
        res = _ssh(entry, extract_cmd, timeout=120)
        print(res.stdout)
        if res.returncode != 0:
            print(res.stderr, file=sys.stderr)
            return res.returncode or 1
        tar_path.unlink(missing_ok=True)
    else:
        res = _ssh(entry, _remote_sync_from_rsbots(entry, remote_root), timeout=240)
        print(res.stdout)
        if res.returncode != 0:
            print(res.stderr, file=sys.stderr)
            return res.returncode or 1

    if args.skip_install:
        print("Skip install requested. Done.")
        return 0

    install_cmd = f"bash {shlex.quote(remote_root)}/{REMOTE_REL}/install_oracle.sh"
    res = _ssh(entry, install_cmd, timeout=300)
    print(res.stdout)
    if res.stderr.strip():
        print(res.stderr, file=sys.stderr)
    if res.returncode != 0:
        return res.returncode or 1

    verify_cmd = (
        "curl -sS http://127.0.0.1:8787/health; echo; "
        "curl -sS -o /dev/null -w public_health:%{http_code}\\n "
        "https://137.131.14.157.sslip.io/webhooks/telnyx"
    )
    res = _ssh(entry, verify_cmd, timeout=60)
    print(res.stdout)
    print("\nTelnyx webhook URL:")
    print("  https://137.131.14.157.sslip.io/webhooks/telnyx")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
