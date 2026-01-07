#!/usr/bin/env python3
"""
Build a safe deploy archive for the canonical server-side deploy flow:
  botctl.sh deploy_apply + deploy_unpack.sh

Scope (RS-only):
- RS bot folders: RSAdminBot, RSForwarder, RSCheckerbot, RSMentionPinger, RSOnboarding, RSuccessBot
- systemd unit templates
- shared helper files at repo root
- oraclekeys/servers.json (non-secret)

Safety:
- Excludes secrets and key material (config.secrets.json, *.key/*.pem/*.ppk)
- Excludes runtime/state artifacts (*.db/*.sqlite*, *.log, locks, __pycache__, venvs)
"""

from __future__ import annotations

import argparse
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]

RS_FOLDERS = [
    "RSAdminBot",
    "RSForwarder",
    "RSCheckerbot",
    "RSMentionPinger",
    "RSOnboarding",
    "RSuccessBot",
]

ROOT_FILES = [
    "mirror_world_config.py",
    "check_rs_bots_configs.py",
    "rsbots_manifest.py",
]

# Only include this canonical, non-secret file from oraclekeys/.
ORACLEKEYS_SERVER_LIST = Path("oraclekeys") / "servers.json"


def _is_excluded_file(path: Path) -> bool:
    name = path.name.lower()

    # Secrets / key material
    if name == "config.secrets.json":
        return True
    if name.endswith((".key", ".pem", ".ppk")):
        return True

    # Runtime/state artifacts
    if name.endswith((".db", ".sqlite", ".sqlite3", ".log", ".lock", ".migrated")):
        return True
    if name in {"points_history.txt"}:
        return True

    # Python build junk
    if name.endswith(".pyc"):
        return True

    return False


def _is_excluded_dir(path: Path) -> bool:
    part = path.name.lower()
    return part in {
        ".git",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".venv",
        "venv",
        "node_modules",
        "logs",
    }


def _iter_files(root: Path) -> Iterable[Path]:
    if root.is_file():
        yield root
        return
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        yield p


def _collect_includes() -> Tuple[List[Path], List[Path]]:
    include_roots: List[Path] = []
    include_single_files: List[Path] = []

    for d in RS_FOLDERS + ["systemd"]:
        p = (REPO_ROOT / d).resolve()
        if p.is_dir():
            include_roots.append(p)

    for f in ROOT_FILES:
        p = (REPO_ROOT / f).resolve()
        if p.is_file():
            include_single_files.append(p)

    # Optional helper script used by deploy_unpack (safe).
    p_opt = (REPO_ROOT / "scripts" / "rsbots_manifest.py").resolve()
    if p_opt.is_file():
        include_single_files.append(p_opt)

    # Canonical servers list (required for new SSH setup).
    servers_json = (REPO_ROOT / ORACLEKEYS_SERVER_LIST).resolve()
    if servers_json.is_file():
        include_single_files.append(servers_json)

    return include_roots, include_single_files


def build_archive(output_path: Path) -> Path:
    include_roots, include_files = _collect_includes()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    added: int = 0
    skipped: int = 0
    seen: Set[Path] = set()

    def add_file(tf: tarfile.TarFile, f: Path) -> None:
        nonlocal added, skipped
        f = f.resolve()
        if f in seen:
            return
        seen.add(f)

        rel = f.relative_to(REPO_ROOT)

        # Never include anything else from oraclekeys/ besides servers.json.
        if rel.parts and rel.parts[0] == "oraclekeys" and rel != ORACLEKEYS_SERVER_LIST:
            skipped += 1
            return

        # Skip excluded directories by checking all parents between file and repo root.
        cur = f.parent
        while True:
            if cur == REPO_ROOT:
                break
            if _is_excluded_dir(cur):
                skipped += 1
                return
            cur = cur.parent

        if _is_excluded_file(f):
            skipped += 1
            return

        tf.add(f, arcname=str(rel))
        added += 1

    with tarfile.open(output_path, "w:gz") as tf:
        for root in include_roots:
            for f in _iter_files(root):
                add_file(tf, f)
        for f in include_files:
            add_file(tf, f)

    print(f"[deploy-archive] Created: {output_path}")
    print(f"[deploy-archive] Files added: {added}, skipped: {skipped}")
    return output_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Mirror World RS-only deploy archive (safe, no secrets).")
    ap.add_argument(
        "--output",
        default="",
        help="Output archive path (defaults to dist/mirror-world_deploy_<timestamp>.tar.gz)",
    )
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output:
        out = Path(args.output).expanduser()
    else:
        out = REPO_ROOT / "dist" / f"mirror-world_deploy_{ts}.tar.gz"

    build_archive(out.resolve())


if __name__ == "__main__":
    main()


