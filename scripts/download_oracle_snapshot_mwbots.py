#!/usr/bin/env python3
"""
Download Oracle Full Snapshot (MWBots-focused)
---------------------------------------------
Creates a tar.gz snapshot on the Oracle server and downloads/extracts it into:
  Oraclserver-files-mwbots/server_full_snapshot_<timestamp>/

Output dir is gitignored; use as a full local backup (includes config, channel_map, tokens).

Scope:
- MW bot folders (if present on the server):
  - DailyScheduleReminder, Instorebotforwarder, MWDataManagerBot, MWPingBot, MWDiscumBot,
  - WhopMembershipSync, systemd

Default: full backup (includes channel_map.json, tokens.env, config.secrets.json, etc.).
Use --no-secrets for code-only snapshot (excludes secrets and runtime data).

After a full backup (default), config files (channel_map.json, source_channels.json, etc.)
are copied from the snapshot into REPO_ROOT/MWBots/<bot>/config/ so local matches server.
Use --no-sync-config to skip that. Use --local-mwbots PATH to override the target.

Usage:
  python scripts/download_oracle_snapshot_mwbots.py
  python scripts/download_oracle_snapshot_mwbots.py --no-secrets
  python scripts/download_oracle_snapshot_mwbots.py --scp-timeout 1800   # if download times out (e.g. large backup)
  python scripts/download_oracle_snapshot_mwbots.py --out-dir Oraclserver-files-mwbots
  python scripts/download_oracle_snapshot_mwbots.py --no-sync-config     # do not update local MWBots config
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
    "DailyScheduleReminder",
    "Instorebotforwarder",
    "MWDataManagerBot",
    "MWPingBot",
    "MWDiscumBot",
    "WhopMembershipSync",
    "systemd",
]

# Full backup: only exclude paths that break Windows extract (long paths / caches)
EXCLUDES_FULL = [
    "--exclude=playwright_profile",
    "--exclude=*CacheStorage*",
    "--exclude=*Service Worker*",
    "--exclude=node_modules",
    "--exclude=.cache",
]

# Code-only snapshot: also exclude secrets and runtime data
EXCLUDES_NO_SECRETS = EXCLUDES_FULL + [
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

# Config files to copy from snapshot into local MWBots (so local matches server). tokens.env excluded for safety.
# fetchall_mappings.json: used only by MWDiscumBot (canonical); sync so local/server stay aligned.
CONFIG_FILES_TO_SYNC = [
    "channel_map.json",
    "source_channels.json",
    "destination_channels.json",
    "settings.json",
    "fetchall_mappings.json",
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
    for folder in ("oraclekeys", "oraclekey"):
        candidate = REPO_ROOT / folder / key_value
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
    # Canonical: absolute key path, stability options (CANONICAL_RULES.md)
    cmd: List[str] = [
        "scp",
        "-i", _resolve_key_path(entry.key),
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=60",
        "-o", "ConnectTimeout=60",
    ]
    if entry.ssh_options:
        cmd.extend(shlex.split(entry.ssh_options))
    return cmd


def _run(cmd: List[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _safe_extract(tar_path: Path, dest_dir: Path) -> None:
    # Windows path length limit; skip members that would exceed it when joined to dest_dir
    max_path = 259
    dest_str = str(dest_dir.resolve())
    skipped = 0
    with tarfile.open(tar_path, "r:gz") as tf:
        for member in tf.getmembers():
            name = member.name
            if name.startswith("/") or name.startswith("\\") or ".." in Path(name).parts:
                raise RuntimeError(f"Unsafe tar member path: {name}")
            target = os.path.normpath(os.path.join(dest_str, name))
            if len(target) > max_path:
                skipped += 1
                continue
            try:
                tf.extract(member, dest_dir)
            except OSError as e:
                if getattr(e, "winerror", None) == 3 or "path" in str(e).lower():
                    skipped += 1
                    continue
                raise
    if skipped:
        print(f"[extract] Skipped {skipped} member(s) (path too long or inaccessible on this OS)")


def _verify_snapshot(snap_dir: Path, full_backup: bool) -> None:
    """Check that key bot folders and files exist after extract."""
    checks = [
        ("MWDiscumBot", snap_dir / "MWDiscumBot"),
        ("MWDiscumBot/config", snap_dir / "MWDiscumBot" / "config"),
        ("MWDiscumBot/config/settings.json", snap_dir / "MWDiscumBot" / "config" / "settings.json"),
    ]
    if full_backup:
        checks.append(("MWDiscumBot/config/channel_map.json", snap_dir / "MWDiscumBot" / "config" / "channel_map.json"))
    missing = [label for label, path in checks if not path.exists()]
    if missing:
        print(f"[verify] Missing: {', '.join(missing)}")
    else:
        print(f"[verify] OK: MWDiscumBot + config present" + (" (incl. channel_map.json)" if full_backup else ""))


def _sync_config_to_local_mwbots(snap_dir: Path, local_mwbots_root: Path, full_backup: bool) -> None:
    """Copy config files from snapshot into local MWBots so local config matches server. Skips tokens.env."""
    if not full_backup:
        return
    if not local_mwbots_root.is_dir():
        print(f"[sync-config] Local MWBots not found at {local_mwbots_root}, skip.")
        return
    copied = 0
    for bot_name in INCLUDES_DEFAULT:
        snap_config = snap_dir / bot_name / "config"
        local_config = local_mwbots_root / bot_name / "config"
        if not snap_config.is_dir():
            continue
        local_config.mkdir(parents=True, exist_ok=True)
        for fname in CONFIG_FILES_TO_SYNC:
            src = snap_config / fname
            if not src.is_file():
                continue
            dst = local_config / fname
            try:
                shutil.copy2(src, dst)
                copied += 1
            except OSError as e:
                print(f"[sync-config] Copy failed {src.name} -> {local_config}: {e}")
    if copied:
        print(f"[sync-config] Copied {copied} config file(s) from snapshot -> {local_mwbots_root}")
    else:
        print(f"[sync-config] No config files copied (snapshot may be code-only or no matching files).")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json (defaults to first entry)")
    ap.add_argument("--out-dir", default=str(REPO_ROOT / "Oraclserver-files-mwbots"), help="Local output dir")
    ap.add_argument("--remote-root", default=None, help="Remote root override (default: from servers.json)")
    ap.add_argument("--keep-snapshots", type=int, default=1, help="Keep only newest N snapshot folders (default: 1)")
    ap.add_argument("--prune-only", action="store_true", help="Only prune old local snapshot folders (no SSH)")
    ap.add_argument("--no-secrets", action="store_true", help="Exclude secrets and runtime data (code-only snapshot)")
    ap.add_argument("--scp-timeout", type=int, default=900, help="SCP download timeout in seconds (default 900 for full backup)")
    ap.add_argument("--no-sync-config", action="store_true", help="Do not copy snapshot config into local MWBots (default: copy after full backup)")
    ap.add_argument("--local-mwbots", default=None, help="Local MWBots root (default: REPO_ROOT/MWBots)")
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
    excludes = EXCLUDES_NO_SECRETS if args.no_secrets else EXCLUDES_FULL
    mode = "code-only (no secrets)" if args.no_secrets else "full backup"

    remote_cmd = (
        "set -euo pipefail; "
        f"cd {shlex.quote(entry.remote_root)}; "
        f"rm -f {shlex.quote(remote_tar)} || true; "
        f"tar --ignore-failed-read -czf {shlex.quote(remote_tar)} "
        + " ".join(excludes)
        + " "
        + " ".join(shlex.quote(x) for x in includes)
        + f"; echo REMOTE_TAR={shlex.quote(remote_tar)}; ls -lh {shlex.quote(remote_tar)}"
    )

    print(f"[1/3] Building remote tar on {entry.user}@{entry.host} ({mode}) ...")
    ssh_cmd = _build_ssh_base(entry) + ["bash", "-c", remote_cmd]
    res = _run(ssh_cmd, timeout=300)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
        raise RuntimeError(f"Remote snapshot build failed (exit {res.returncode})")
    # Only print last lines (REMOTE_TAR= and ls) to avoid env dump from login shell
    for line in res.stdout.strip().splitlines():
        if line.startswith("REMOTE_TAR=") or line.strip().startswith("-"):
            print(line)

    print(f"[2/3] Downloading tar via scp (timeout={args.scp_timeout}s) ...")
    local_tar = snap_dir / f"mwbots_full_snapshot_{ts}.tar.gz"
    scp_cmd = _build_scp_base(entry) + [f"{entry.user}@{entry.host}:{remote_tar}", str(local_tar)]
    res2 = _run(scp_cmd, timeout=args.scp_timeout)
    if res2.returncode != 0:
        print(res2.stdout)
        print(res2.stderr)
        raise RuntimeError(f"SCP download failed (exit {res2.returncode})")

    print("[3/3] Extracting ...")
    _safe_extract(local_tar, snap_dir)

    _verify_snapshot(snap_dir, full_backup=not args.no_secrets)
    if not args.no_sync_config and not args.no_secrets:
        local_mwbots = Path(args.local_mwbots).resolve() if (args.local_mwbots and str(args.local_mwbots).strip()) else (REPO_ROOT / "MWBots")
        _sync_config_to_local_mwbots(snap_dir, local_mwbots, full_backup=True)
    print(f"DONE: {snap_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

