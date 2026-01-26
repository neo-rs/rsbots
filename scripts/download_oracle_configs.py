#!/usr/bin/env python3
"""
Download Oracle config files (config.json + config.secrets.json) safely.

Why:
- Prevent overwriting newer server-side configs when deploying from a stale local tree.
- Produce a timestamped local snapshot you can diff/review.

Output:
  <out_dir>/server_configs_<timestamp>/<BotName>/{config.json,config.secrets.json}

Notes:
- This tool downloads config.secrets.json too (contains secrets). The repo ignores:
  - Oraclserver-files/** and **/config.secrets.json
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from mirror_world_config import load_oracle_servers, pick_oracle_server, resolve_oracle_ssh_key_path


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_BOTS = [
    "RSAdminBot",
    "RSForwarder",
    "RSCheckerbot",
    "RSMentionPinger",
    "RSOnboarding",
    "RSuccessBot",
]


def _split_csv(raw: str) -> List[str]:
    out: List[str] = []
    for part in (raw or "").replace("\n", ",").split(","):
        p = (part or "").strip()
        if p:
            out.append(p)
    return out


def _scp_base(key_path: Path, ssh_options: str) -> List[str]:
    cmd: List[str] = ["scp", "-i", str(key_path), "-o", "StrictHostKeyChecking=no"]
    if ssh_options:
        cmd.extend(shlex.split(ssh_options))
    return cmd


def _run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True)


def _download_one(
    *,
    scp_base: List[str],
    user: str,
    host: str,
    remote_path: str,
    local_path: Path,
) -> tuple[bool, str]:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    src = f"{user}@{host}:{remote_path}"
    cmd = list(scp_base) + [src, str(local_path)]
    res = _run(cmd)
    if res.returncode == 0 and local_path.exists():
        return True, f"OK: {remote_path} -> {local_path}"
    msg = (res.stderr or res.stdout or "").strip()
    if not msg:
        msg = f"scp_failed_exit_{res.returncode}"
    # Common/expected: file doesn't exist (e.g. some bots might not have secrets yet).
    return False, f"SKIP: {remote_path} ({msg[:240]})"


def _iter_targets(bots: Iterable[str], include_secrets: bool) -> Iterable[tuple[str, str]]:
    for bot in bots:
        yield bot, "config.json"
        if include_secrets:
            yield bot, "config.secrets.json"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Download Oracle bot config files into a local timestamped snapshot folder.")
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json (default: first entry)")
    ap.add_argument(
        "--out-dir",
        default=str(REPO_ROOT / "Oraclserver-files"),
        help="Local output dir (default: Oraclserver-files)",
    )
    ap.add_argument(
        "--remote-root",
        default=None,
        help="Remote root override (defaults to servers.json remote_root/live_root, else /home/rsadmin/bots/mirror-world)",
    )
    ap.add_argument(
        "--bots",
        default=",".join(DEFAULT_BOTS),
        help=f"Comma-separated bot folders to fetch (default: {','.join(DEFAULT_BOTS)})",
    )
    ap.add_argument("--no-secrets", action="store_true", help="Do not download config.secrets.json files")
    args = ap.parse_args(argv)

    servers, _ = load_oracle_servers(REPO_ROOT)
    if not servers:
        print("ERROR: No servers configured in oraclekeys/servers.json", file=sys.stderr)
        return 2
    sname = str(args.server_name or "").strip() or str(servers[0].get("name", "")).strip()
    s = pick_oracle_server(servers, sname)

    user = str(s.get("user", "rsadmin"))
    host = str(s.get("host", "")).strip()
    key = resolve_oracle_ssh_key_path(str(s.get("key", "")), REPO_ROOT)
    ssh_options = str(s.get("ssh_options", "") or "")
    remote_root = str(args.remote_root or s.get("remote_root") or s.get("live_root") or "/home/rsadmin/bots/mirror-world")

    if not host:
        print("ERROR: servers.json entry missing host", file=sys.stderr)
        return 2
    if not key.exists():
        print(f"ERROR: SSH key not found: {key}", file=sys.stderr)
        return 2

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = out_dir / f"server_configs_{ts}"
    dest.mkdir(parents=True, exist_ok=True)

    bots = _split_csv(args.bots) or list(DEFAULT_BOTS)
    include_secrets = not bool(args.no_secrets)

    scp_base = _scp_base(key, ssh_options)

    print(f"Server:   {user}@{host}")
    print(f"Remote:   {remote_root}")
    print(f"Local:    {dest}")
    print(f"Bots:     {', '.join(bots)}")
    print(f"Secrets:  {'yes' if include_secrets else 'no'}")
    print()

    ok = 0
    skipped = 0
    for bot, filename in _iter_targets(bots, include_secrets):
        remote_path = f"{remote_root.rstrip('/')}/{bot}/{filename}"
        local_path = dest / bot / filename
        success, msg = _download_one(
            scp_base=scp_base,
            user=user,
            host=host,
            remote_path=remote_path,
            local_path=local_path,
        )
        print(msg)
        if success:
            ok += 1
        else:
            skipped += 1

    print()
    print(f"DONE: {dest}")
    print(f"Summary: ok={ok} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

