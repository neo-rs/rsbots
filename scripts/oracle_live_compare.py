#!/usr/bin/env python3
"""
Oracle Live Compare (read-only)
-------------------------------
Connects to the configured Oracle server via SSH and compares hashes of key files
against the local workspace.

This helps answer:
- "Is the server actually running the code/config I think it is?"
- "Does my downloaded snapshot match what is on the server?"
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVERS_PATH = REPO_ROOT / "oraclekeys" / "servers.json"


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


def _pick_server(servers: List[Dict[str, Any]], server_name: Optional[str]) -> Dict[str, Any]:
    if not servers:
        raise ValueError("No servers configured in oraclekeys/servers.json")
    if server_name:
        for s in servers:
            if str(s.get("name", "")).strip() == server_name.strip():
                return s
        raise ValueError(f"Server name not found in servers.json: {server_name}")
    return servers[0]


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


def _ssh_base(entry: ServerEntry) -> List[str]:
    cmd: List[str] = ["ssh", "-i", _resolve_key_path(entry.key), "-o", "StrictHostKeyChecking=no"]
    if entry.ssh_options:
        cmd.extend(shlex.split(entry.ssh_options))
    cmd.append(f"{entry.user}@{entry.host}")
    return cmd


def _run_ssh(entry: ServerEntry, bash_cmd: str, *, timeout: int = 60) -> subprocess.CompletedProcess:
    cmd = _ssh_base(entry) + ["bash", "-lc", bash_cmd]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def _git_show_bytes(rel: str) -> bytes | None:
    """Return git HEAD blob bytes for a path (or None)."""
    try:
        # Fast path: git show HEAD:<path>
        res = subprocess.run(
            ["git", "show", f"HEAD:{rel}"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            timeout=20,
        )
        if res.returncode != 0:
            return None
        return res.stdout
    except Exception:
        return None

def _git_eol_info(paths: list[str]) -> dict[str, str]:
    """Best-effort `git ls-files --eol` info per file."""
    out: dict[str, str] = {}
    try:
        res = subprocess.run(
            ["git", "ls-files", "--eol", *paths],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=20,
        )
        if res.returncode != 0:
            return out
        for ln in (res.stdout or "").splitlines():
            s = ln.strip()
            if not s:
                continue
            # Example:
            # i/lf    w/crlf  attr/                  RSCheckerbot/whop_brief.py
            parts = s.split()
            if parts:
                rel = parts[-1]
                out[rel] = " ".join(parts[:-1]).strip()
    except Exception:
        pass
    return out


def _latest_snapshot_dir(out_dir: Path) -> Path | None:
    try:
        snaps = sorted([p for p in out_dir.glob("server_full_snapshot_*") if p.is_dir()])
        return snaps[-1] if snaps else None
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json (defaults to first entry)")
    ap.add_argument(
        "--remote-root",
        default=None,
        help="Remote root override (default: servers.json remote_root or /home/rsadmin/bots/mirror-world)",
    )
    ap.add_argument(
        "--out-dir",
        default=str(REPO_ROOT / "Oraclserver-files"),
        help="Where snapshot folders live (default: Oraclserver-files). Used to compare live vs latest snapshot.",
    )
    ap.add_argument(
        "--snapshot-dir",
        default="",
        help="Explicit snapshot folder to compare against (default: newest in --out-dir).",
    )
    ap.add_argument(
        "--hash-mode",
        choices=["git", "worktree"],
        default="git",
        help="How to hash local files: git=hash HEAD content (stable across CRLF/LF), worktree=hash working tree bytes.",
    )
    args = ap.parse_args()

    raw = _pick_server(_load_servers(), args.server_name)
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

    rel_files = [
        "RSCheckerbot/main.py",
        "RSCheckerbot/whop_brief.py",
        "RSCheckerbot/staff_embeds.py",
        "RSCheckerbot/whop_webhook_handler.py",
        "RSCheckerbot/config.json",
    ]

    # Remote: git HEAD (best-effort) + sha256 + quick feature presence probes.
    remote_cmd = (
        "set -e; "
        + f"cd {shlex.quote(entry.remote_root)}"
        + " && echo REMOTE_ROOT=$(pwd)"
        + " && echo GIT_HEAD=$(git rev-parse HEAD 2>/dev/null || true)"
        + " && (python3 -c 'import pathlib; "
        + "t=pathlib.Path(\"RSCheckerbot/staff_embeds.py\").read_text(encoding=\"utf-8\"); "
        + "print(\"HAS_WHOP_DASHBOARD_LABEL=\" + str(\"Whop Dashboard\" in t)); "
        + "print(\"HAS_TOTAL_SPENT_KEY=\" + str(\"total_spent\" in t)); "
        + "print(\"HAS_DASHBOARD_URL_KEY=\" + str(\"dashboard_url\" in t))' 2>/dev/null || true)"
        + " && (sha256sum " + " ".join(shlex.quote(p) for p in rel_files) + " 2>/dev/null || true)"
    )
    res = _run_ssh(entry, remote_cmd, timeout=120)
    if res.returncode != 0:
        sys.stdout.write(res.stdout)
        sys.stderr.write(res.stderr)
        print("\nERROR: live compare failed (SSH/remote command).")
        return 2

    remote_lines = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip()]
    remote_hashes: Dict[str, str] = {}
    remote_meta: Dict[str, str] = {}
    for ln in remote_lines:
        if "=" in ln and ln.startswith(("REMOTE_ROOT=", "GIT_HEAD=", "HAS_")):
            k, v = ln.split("=", 1)
            remote_meta[k.strip()] = v.strip()
            continue
        # sha256sum format: "<hash>  <path>"
        parts = ln.split()
        if len(parts) >= 2 and len(parts[0]) == 64:
            remote_hashes[parts[-1]] = parts[0]

    # Local hashes
    local_hashes: Dict[str, str] = {}
    local_hash_source = "git:HEAD" if args.hash_mode == "git" else "worktree"
    for rel in rel_files:
        if args.hash_mode == "git":
            b = _git_show_bytes(rel)
            if b is not None:
                local_hashes[rel] = _sha256_bytes(b)
                continue
        # Fallback to working-tree bytes
        p = (REPO_ROOT / rel)
        if p.exists():
            local_hashes[rel] = _sha256_file(p)

    # Snapshot hashes (optional, best-effort)
    snapshot_hashes: Dict[str, str] = {}
    snapshot_dir = Path(args.snapshot_dir).resolve() if args.snapshot_dir else _latest_snapshot_dir(Path(args.out_dir).resolve())
    if snapshot_dir and snapshot_dir.exists():
        for rel in rel_files:
            sp = snapshot_dir / rel
            if sp.exists():
                snapshot_hashes[rel] = _sha256_file(sp)

    print("Oracle Live Compare (read-only)")
    print(f"server:      {entry.name} ({entry.user}@{entry.host})")
    print(f"remote_root: {remote_meta.get('REMOTE_ROOT', entry.remote_root)}")
    if remote_meta.get("GIT_HEAD"):
        print(f"remote git:  {remote_meta.get('GIT_HEAD')}")
    for k in ("HAS_WHOP_DASHBOARD_LABEL", "HAS_DASHBOARD_URL_KEY", "HAS_TOTAL_SPENT_KEY"):
        if k in remote_meta:
            print(f"{k}: {remote_meta[k]}")
    if snapshot_dir and snapshot_dir.exists():
        print(f"snapshot:    {snapshot_dir}")
    print(f"local_hash:  {local_hash_source}")
    print()

    any_diff = False
    eol_info = _git_eol_info(rel_files)
    for rel in rel_files:
        rh = remote_hashes.get(rel, "")
        lh = local_hashes.get(rel, "")
        sh = snapshot_hashes.get(rel, "")

        # Determine comparisons
        remote_vs_local = "MATCH" if rh and lh and rh == lh else ("MISSING_REMOTE" if not rh else ("MISSING_LOCAL" if not lh else "DIFF"))
        remote_vs_snapshot = ""
        if snapshot_dir and snapshot_dir.exists():
            remote_vs_snapshot = "MATCH" if rh and sh and rh == sh else ("MISSING_REMOTE" if not rh else ("MISSING_SNAPSHOT" if not sh else "DIFF"))

        if remote_vs_local != "MATCH" or (remote_vs_snapshot and remote_vs_snapshot != "MATCH"):
            any_diff = True

        if remote_vs_snapshot:
            extra = ""
            if remote_vs_local == "DIFF" and rel in eol_info:
                extra = f"  (eol: {eol_info.get(rel)})"
            print(f"- {rel}: live_vs_snapshot={remote_vs_snapshot}  live_vs_local={remote_vs_local}{extra}")
        else:
            extra = ""
            if remote_vs_local == "DIFF" and rel in eol_info:
                extra = f"  (eol: {eol_info.get(rel)})"
            print(f"- {rel}: live_vs_local={remote_vs_local}{extra}")
    print()
    if any_diff:
        print("NOTE:")
        print("- live_vs_snapshot=DIFF means your downloaded snapshot does NOT match current Oracle live files.")
        print("- live_vs_local=DIFF means Oracle live files do NOT match your local repo files.")
        print("If live==snapshot but local DIFF shows eol differences (i/lf vs w/crlf), that is usually just Windows line endings, not a deploy issue.")
    else:
        print("OK: Oracle live files match for the checked paths.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

