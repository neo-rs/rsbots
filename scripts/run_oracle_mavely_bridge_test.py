#!/usr/bin/env python3
"""
SCP patched affiliate rewriter + mavely_link_resolve + test script to Oracle, then run test_mavely_app_links remotely.

  py -3 scripts/run_oracle_mavely_bridge_test.py
  py -3 scripts/run_oracle_mavely_bridge_test.py --url "https://mavely.app.link/XXXX"
  py -3 scripts/run_oracle_mavely_bridge_test.py --skip-scp --url "https://mavely.app.link/XXXX"
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mirror_world_config import load_oracle_servers, pick_oracle_server, resolve_oracle_ssh_key_path  # noqa: E402


def _ssh(entry: dict, cmd: str, *, timeout: int) -> subprocess.CompletedProcess:
    key = str(resolve_oracle_ssh_key_path(str(entry.get("key", "")), REPO_ROOT))
    args = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "") or "").strip()
    if opts:
        args.extend(shlex.split(opts))
    args.append(f'{entry["user"]}@{entry["host"]}')
    # Non-login (-c only): avoids ~/.profile or similar changing cwd after our `cd` to remote_root.
    args.extend(["bash", "-c", cmd])
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def _scp(entry: dict, local: Path, remote_path: str, *, timeout: int) -> subprocess.CompletedProcess:
    key = str(resolve_oracle_ssh_key_path(str(entry.get("key", "")), REPO_ROOT))
    args = ["scp", "-i", key, "-o", "StrictHostKeyChecking=no"]
    opts = str(entry.get("ssh_options", "") or "").strip()
    if opts:
        args.extend(shlex.split(opts))
    args.extend([str(local), f'{entry["user"]}@{entry["host"]}:{remote_path}'])
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-name", default=None)
    ap.add_argument("--url", default="https://mavely.app.link/72tlN7jXH1b")
    ap.add_argument("--timeout-s", type=float, default=180.0, help="Passed to test_mavely_app_links.py on the server.")
    ap.add_argument("--skip-scp", action="store_true", help="Do not upload files (remote tree already updated).")
    args = ap.parse_args()

    servers, _ = load_oracle_servers(REPO_ROOT)
    entry = pick_oracle_server(servers, args.server_name) if args.server_name else servers[0]
    rr = str(entry.get("remote_root") or "/home/rsadmin/bots/mirror-world").rstrip("/")

    files = [
        (REPO_ROOT / "RSForwarder" / "affiliate_rewriter.py", f"{rr}/RSForwarder/affiliate_rewriter.py"),
        (REPO_ROOT / "RSForwarder" / "mavely_link_resolve.py", f"{rr}/RSForwarder/mavely_link_resolve.py"),
        (REPO_ROOT / "scripts" / "test_mavely_app_links.py", f"{rr}/scripts/test_mavely_app_links.py"),
    ]
    for loc, _ in files:
        if not loc.is_file():
            print("MISSING", loc, file=sys.stderr)
            return 2

    if not args.skip_scp:
        print("SCP ->", entry.get("name", entry.get("host")))
        for loc, rem in files:
            r = _scp(entry, loc, rem, timeout=120)
            if r.returncode != 0:
                print("scp failed:", rem, (r.stderr or r.stdout)[:500], file=sys.stderr)
                return r.returncode or 1
            print("  ok", rem)

    url = shlex.quote(args.url)
    timeout_arg = shlex.quote(str(float(args.timeout_s)))
    rr_q = shlex.quote(rr)
    # Oracle layout uses .venv but the interpreter may be `python3` only (no `python` symlink).
    # Avoid $VAR for repo path: some Windows OpenSSH clients expand $RR locally and empty it before the remote bash runs.
    vpy = shlex.quote(f"{rr}/.venv/bin/python")
    vpy3 = shlex.quote(f"{rr}/.venv/bin/python3")
    v2py = shlex.quote(f"{rr}/venv/bin/python")
    v2py3 = shlex.quote(f"{rr}/venv/bin/python3")
    remote_cmd = (
        f"cd {rr_q} || exit 1; "
        'PY=""; '
        f"for c in {vpy} {vpy3} {v2py} {v2py3}; do "
        'if [ -f "$c" ] && [ -x "$c" ]; then PY="$c"; break; fi; '
        "done; "
        'if [ -z "$PY" ] && command -v python3 >/dev/null 2>&1; then PY="$(command -v python3)"; fi; '
        'if [ -z "$PY" ]; then echo "ERROR: no venv python or python3 in PATH (cwd=$PWD)" >&2; '
        "ls -la .venv/bin 2>/dev/null || true; exit 127; fi; "
        'echo "Using: $PY"; '
        "export MAVELY_PLAYWRIGHT_NO_SANDBOX=1 MAVELY_BRIDGE_PLAYWRIGHT=1; "
        f'exec "$PY" {shlex.quote(f"{rr}/scripts/test_mavely_app_links.py")} {url} --timeout-s {timeout_arg}'
    )

    print("SSH smoke (timeout 300s)...")
    r = _ssh(entry, remote_cmd, timeout=300)
    out = (r.stdout or "").strip()
    err = (r.stderr or "").strip()
    if out:
        print(out)
    if err:
        print(err, file=sys.stderr)
    if r.returncode != 0:
        print("exit", r.returncode, file=sys.stderr)
    return r.returncode if r.returncode is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
