#!/usr/bin/env python3
"""
Download Oracle Full Snapshot (MWBots-focused)
---------------------------------------------
Creates a tar.gz snapshot on the Oracle server and downloads/extracts it into:
  Oraclserver-files-mwbots/server_full_snapshot_<timestamp>/

Scope:
- MW bot folders (if present on the server):
  - MWDataManagerBot
  - MWPingBot
  - MWDiscumBot
- systemd templates folder (repo-local)

Safety:
- Excludes secrets and key material.
- Excludes *.env and token files.
- Uses tar --ignore-failed-read so missing MW folders don't fail the snapshot.

Usage:
  python scripts/download_oracle_snapshot_mwbots.py
  python scripts/download_oracle_snapshot_mwbots.py --server-name "instance-enhance (rsadmin)"
  python scripts/download_oracle_snapshot_mwbots.py --out-dir Oraclserver-files-mwbots
"""

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"


INCLUDES_DEFAULT = [
    "MWDataManagerBot",
    "MWPingBot",
    "MWDiscumBot",
    "systemd",
]

EXCLUDES_DEFAULT = [
    "--exclude=config.secrets.json",
    "--exclude=rs-bot-tokens.txt",
    "--exclude=*.key",
    "--exclude=*.pem",
    "--exclude=*.ppk",
    "--exclude=*.env",
    "--exclude=.env",
    "--exclude=tokens.env",
    "--exclude=channel_map.json",
    "--exclude=source_channels.json",
    "--exclude=destination_channels.json",
    "--exclude=systemlogs.json",
    "--exclude=*.log",
    "--exclude=*.jsonl",
]


@dataclass
class ServerEntry:
    name: str
    user: str
    host: str
    key: str
    ssh_options: str = ""
    remote_root: str = "/home/rsadmin/bots/mirror-world"


def _load_servers() -> List[Dict[str, Any]]:
    if not SERVERS_PATH.exists():
        raise FileNotFoundError(f"Missing servers.json: {SERVERS_PATH}")
    return json.loads(SERVERS_PATH.read_text(encoding="utf-8") or "[]")


def _resolve_key_path(key_value: str) -> str:
    p = Path(key_value)
    if p.is_absolute() and p.exists():
        return str(p)
    candidate = REPO_ROOT / "oraclekeys" / key_value
    if candidate.exists():
        return str(candidate)
    candidate2 = REPO_ROOT / key_value
    if candidate2.exists():
        return str(candidate2)
    return str(p)


def _pick_server(servers: List[Dict[str, Any]], server_name: Optional[str]) -> Dict[str, Any]:
    if not servers:
        raise ValueError("No servers configured in oraclekeys/servers.json")
    if server_name:
        for s in servers:
            if str(s.get("name", "")).strip() == server_name.strip():
                return s
        raise ValueError(f"Server name not found in servers.json: {server_name}")
    return servers[0]


def _build_ssh_base(entry: ServerEntry) -> List[str]:
    cmd: List[str] = ["ssh", "-i", _resolve_key_path(entry.key), "-o", "StrictHostKeyChecking=no"]
    if entry.ssh_options:
        cmd.extend(shlex.split(entry.ssh_options))
    cmd.append(f"{entry.user}@{entry.host}")
    return cmd


def _build_scp_base(entry: ServerEntry) -> List[str]:
    cmd: List[str] = ["scp", "-i", _resolve_key_path(entry.key), "-o", "StrictHostKeyChecking=no"]
    if entry.ssh_options:
        cmd.extend(shlex.split(entry.ssh_options))
    return cmd


def _run(cmd: List[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _safe_extract(tar_path: Path, dest_dir: Path) -> None:
    with tarfile.open(tar_path, "r:gz") as tf:
        for member in tf.getmembers():
            name = member.name
            if name.startswith("/") or name.startswith("\\") or ".." in Path(name).parts:
                raise RuntimeError(f"Unsafe tar member path: {name}")
        tf.extractall(dest_dir)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json (defaults to first entry)")
    ap.add_argument("--out-dir", default=str(REPO_ROOT / "Oraclserver-files-mwbots"), help="Local output dir")
    ap.add_argument("--remote-root", default=None, help="Remote root override (default: from servers.json)")
    ap.add_argument("--keep-snapshots", type=int, default=1, help="Keep only newest N snapshot folders (default: 1)")
    ap.add_argument("--prune-only", action="store_true", help="Only prune old local snapshot folders (no SSH)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.prune_only:
        # simple prune: keep newest N by name
        dirs = sorted([p for p in out_dir.glob("server_full_snapshot_*") if p.is_dir()])
        keep_n = max(int(args.keep_snapshots or 1), 1)
        keep_set = set(dirs[-keep_n:]) if dirs else set()
        deleted = 0
        for p in dirs:
            if p in keep_set:
                continue
            shutil.rmtree(p, ignore_errors=True)
            deleted += 1
        print(f"[cleanup] deleted={deleted} keep={keep_n}")
        return 0

    servers = _load_servers()
    raw = _pick_server(servers, args.server_name)
    entry = ServerEntry(
        name=str(raw.get("name", "")),
        user=str(raw.get("user", "rsadmin")),
        host=str(raw.get("host", "")),
        key=str(raw.get("key", "")),
        ssh_options=str(raw.get("ssh_options", "")),
        remote_root=str(args.remote_root or raw.get("remote_root") or raw.get("live_root") or "/home/rsadmin/bots/mirror-world"),
    )
    if not entry.host:
        raise ValueError("servers.json entry missing host")
    if not entry.key:
        raise ValueError("servers.json entry missing key")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_dir = out_dir / f"server_full_snapshot_{ts}"
    snap_dir.mkdir(parents=True, exist_ok=True)

    remote_tar = f"/tmp/mwbots_full_snapshot_{ts}.tar.gz"
    includes = list(INCLUDES_DEFAULT)

    remote_cmd = (
        "set -euo pipefail; "
        f"cd {shlex.quote(entry.remote_root)}; "
        f"rm -f {shlex.quote(remote_tar)} || true; "
        f"tar --ignore-failed-read -czf {shlex.quote(remote_tar)} "
        + " ".join(EXCLUDES_DEFAULT)
        + " "
        + " ".join(shlex.quote(x) for x in includes)
        + f"; echo REMOTE_TAR={shlex.quote(remote_tar)}; ls -lh {shlex.quote(remote_tar)}"
    )

    print(f"[1/3] Building remote tar on {entry.user}@{entry.host} ...")
    ssh_cmd = _build_ssh_base(entry) + ["bash", "-lc", remote_cmd]
    res = _run(ssh_cmd, timeout=300)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"Remote snapshot build failed (exit {res.returncode})")
    print(res.stdout.strip())

    print("[2/3] Downloading tar via scp ...")
    local_tar = snap_dir / f"mwbots_full_snapshot_{ts}.tar.gz"
    scp_cmd = _build_scp_base(entry) + [f"{entry.user}@{entry.host}:{remote_tar}", str(local_tar)]
    res2 = _run(scp_cmd, timeout=300)
    if res2.returncode != 0:
        print(res2.stdout)
        print(res2.stderr)
        raise RuntimeError(f"SCP download failed (exit {res2.returncode})")

    print("[3/3] Extracting ...")
    _safe_extract(local_tar, snap_dir)

    print(f"DONE: {snap_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

