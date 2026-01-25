from __future__ import annotations

"""
Oracle baseline check (MWBots-only)
----------------------------------
Compare local MW bot folders vs the latest downloaded Oracle snapshot.

This does NOT deploy anything. It only generates manifests + prints diffs.

It is intentionally separate from scripts/oracle_baseline_check.py (RS bots),
so MW workflows can evolve without touching RS workflows.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = ROOT / "Oraclserver-files-mwbots"
DEFAULT_LOCAL_MANIFEST = DEFAULT_OUT_DIR / "mwbots_manifest_local.json"
DEFAULT_SERVER_MANIFEST = DEFAULT_OUT_DIR / "mwbots_manifest_server.json"
DEFAULT_DIFF_OUT = DEFAULT_OUT_DIR / "mwbots_manifest_diff.json"

MW_BOTS = ["MWDataManagerBot", "MWPingBot", "MWDiscumBot"]


def _default_local_repo_root() -> Path:
    """
    Pick the most likely local source-of-truth for MW bots.

    In this workspace, MW bots live in a separate git repo at ./MWBots/,
    while the mirror-world root is RS-only and typically ignores MW bot folders.
    """
    candidate = ROOT / "MWBots"
    try:
        if candidate.is_dir() and (candidate / ".git").exists():
            if all((candidate / b).is_dir() for b in MW_BOTS):
                return candidate.resolve()
    except Exception:
        pass
    return ROOT


def _parse_snapshot_ts(name: str) -> Optional[datetime]:
    m = re.match(r"^server_full_snapshot_(\d{8}_\d{6})$", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
    except Exception:
        return None


def _latest_snapshot_dir(out_dir: Path) -> Path:
    snaps = [p for p in out_dir.glob("server_full_snapshot_*") if p.is_dir()]
    if not snaps:
        raise FileNotFoundError(f"No snapshot folders found in: {out_dir}")

    def key(p: Path) -> Tuple[int, float]:
        ts = _parse_snapshot_ts(p.name)
        if ts:
            return (1, ts.timestamp())
        return (0, p.stat().st_mtime)

    snaps.sort(key=key, reverse=True)
    return snaps[0]


def _run(cmd: list[str], *, capture_output: bool = True) -> None:
    if capture_output:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            sys.stdout.write(res.stdout)
            sys.stderr.write(res.stderr)
            raise SystemExit(res.returncode)
        return
    res2 = subprocess.run(cmd)
    if res2.returncode != 0:
        raise SystemExit(res2.returncode)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Baseline check: compare local MWBots vs latest downloaded Oracle snapshot.")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Where snapshot folders live (default: Oraclserver-files-mwbots)")
    ap.add_argument("--snapshot-dir", default="", help="Explicit snapshot folder (default: auto-pick latest in out-dir)")
    ap.add_argument("--download", action="store_true", help="Download a fresh snapshot first (runs scripts/download_oracle_snapshot_mwbots.py)")
    ap.add_argument("--server-name", default=None, help="Optional server name for download_oracle_snapshot_mwbots.py")
    ap.add_argument(
        "--local-root",
        default="",
        help="Local repo root to compare (default: auto-detect ./MWBots if present; otherwise workspace root).",
    )
    ap.add_argument("--local-manifest", default=str(DEFAULT_LOCAL_MANIFEST))
    ap.add_argument("--server-manifest", default=str(DEFAULT_SERVER_MANIFEST))
    ap.add_argument("--diff-out", default=str(DEFAULT_DIFF_OUT))
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    local_root = Path(args.local_root).resolve() if args.local_root else _default_local_repo_root()
    if not local_root.exists():
        raise FileNotFoundError(f"Local root not found: {local_root}")

    if args.download:
        cmd = [sys.executable, str(ROOT / "scripts" / "download_oracle_snapshot_mwbots.py")]
        if args.server_name:
            cmd += ["--server-name", str(args.server_name)]
        cmd += ["--out-dir", str(out_dir)]
        _run(cmd, capture_output=False)

    snapshot_dir = Path(args.snapshot_dir).resolve() if args.snapshot_dir else _latest_snapshot_dir(out_dir)
    if not snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot dir not found: {snapshot_dir}")

    local_manifest = Path(args.local_manifest).resolve()
    server_manifest = Path(args.server_manifest).resolve()
    diff_out = Path(args.diff_out).resolve()

    bots_arg = ",".join(MW_BOTS)
    _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "rsbots_manifest.py"),
            "--normalize-text-eol",
            "--repo-root",
            str(local_root),
            "--bots",
            bots_arg,
            "--out",
            str(local_manifest),
        ]
    )
    _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "rsbots_manifest.py"),
            "--normalize-text-eol",
            "--repo-root",
            str(snapshot_dir),
            "--bots",
            bots_arg,
            "--out",
            str(server_manifest),
        ]
    )

    import importlib.util

    lib_path = ROOT / "rsbots_manifest.py"
    spec = importlib.util.spec_from_file_location("rsbots_manifest_lib", lib_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load manifest library: {lib_path}")
    lib = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lib)  # type: ignore[attr-defined]

    local = json.loads(local_manifest.read_text(encoding="utf-8"))
    server = json.loads(server_manifest.read_text(encoding="utf-8"))
    diff = lib.compare_manifests(local, server)
    _write_json(diff_out, diff)

    folders = diff.get("folders") or {}
    changed_total = 0
    only_local_total = 0
    only_remote_total = 0
    missing_remote = 0

    for f in MW_BOTS:
        info = folders.get(f) or {}
        if info.get("missing_remote"):
            missing_remote += 1
        changed_total += len(info.get("changed") or [])
        only_local_total += len(info.get("only_local") or [])
        only_remote_total += len(info.get("only_remote") or [])

    print("Oracle baseline check (MWBots)")
    print(f"local:   {local_root}")
    print(f"snapshot: {snapshot_dir}")
    print(f"bots:     {', '.join(MW_BOTS)}")
    print()
    print(f"missing on server snapshot: {missing_remote}")
    print(f"changed:                {changed_total}")
    print(f"only local:             {only_local_total}")
    print(f"only server snapshot:   {only_remote_total}")
    print()
    print(f"diff json: {diff_out}")

    return 0 if (changed_total == 0 and only_local_total == 0 and only_remote_total == 0 and missing_remote == 0) else 2


if __name__ == "__main__":
    raise SystemExit(main())

