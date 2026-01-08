from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = ROOT / "Oraclserver-files"
DEFAULT_LOCAL_MANIFEST = DEFAULT_OUT_DIR / "rsbots_manifest_local.json"
DEFAULT_SERVER_MANIFEST = DEFAULT_OUT_DIR / "rsbots_manifest_server.json"
DEFAULT_DIFF_OUT = DEFAULT_OUT_DIR / "rsbots_manifest_diff.json"


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


def _run(cmd: list[str]) -> None:
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.stdout.write(res.stdout)
        sys.stderr.write(res.stderr)
        raise SystemExit(res.returncode)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Baseline check: compare local workspace vs latest downloaded Oracle snapshot "
            "(prevents Cursor edits against an outdated local tree)."
        )
    )
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Where snapshot folders live (default: Oraclserver-files)")
    ap.add_argument("--snapshot-dir", default="", help="Explicit snapshot folder (default: auto-pick latest in out-dir)")
    ap.add_argument("--download", action="store_true", help="Download a fresh snapshot first (runs scripts/download_oracle_snapshot.py)")
    ap.add_argument("--server-name", default=None, help="Optional server name for download_oracle_snapshot.py")
    ap.add_argument("--no-oracle-server-data", action="store_true", help="Pass-through to download_oracle_snapshot.py")
    ap.add_argument("--local-manifest", default=str(DEFAULT_LOCAL_MANIFEST), help="Output local manifest path")
    ap.add_argument("--server-manifest", default=str(DEFAULT_SERVER_MANIFEST), help="Output server manifest path")
    ap.add_argument("--diff-out", default=str(DEFAULT_DIFF_OUT), help="Output full diff JSON path")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.download:
        cmd = [sys.executable, str(ROOT / "scripts" / "download_oracle_snapshot.py")]
        if args.server_name:
            cmd += ["--server-name", str(args.server_name)]
        cmd += ["--out-dir", str(out_dir)]
        if args.no_oracle_server_data:
            cmd += ["--no-oracle-server-data"]
        _run(cmd)

    snapshot_dir = Path(args.snapshot_dir).resolve() if args.snapshot_dir else _latest_snapshot_dir(out_dir)
    if not snapshot_dir.exists():
        raise FileNotFoundError(f"Snapshot dir not found: {snapshot_dir}")

    local_manifest = Path(args.local_manifest).resolve()
    server_manifest = Path(args.server_manifest).resolve()
    diff_out = Path(args.diff_out).resolve()

    # Generate manifests with EOL normalization to avoid Windows (CRLF) vs Linux (LF) false mismatches.
    _run(
        [
            sys.executable,
            str(ROOT / "scripts" / "rsbots_manifest.py"),
            "--normalize-text-eol",
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
            "--out",
            str(server_manifest),
        ]
    )

    # Full diff (all included file types + systemd + root files)
    # IMPORTANT: load the top-level rsbots_manifest.py (canonical), not scripts/rsbots_manifest.py.
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

    # Python-only summary (same shape as scripts/compare_rsbots_python_only.py, but includes snapshot hint).
    def iter_py(manifest: dict) -> dict[tuple[str, str], str]:
        out: dict[tuple[str, str], str] = {}
        files = manifest.get("files") or {}
        for folder, mapping in files.items():
            if not isinstance(mapping, dict):
                continue
            if mapping.get("__missing__"):
                continue
            for rel, sha in mapping.items():
                if isinstance(rel, str) and rel.endswith(".py"):
                    out[(str(folder), str(rel))] = str(sha)
        return out

    lpy = iter_py(local)
    rpy = iter_py(server)
    only_local = sorted(set(lpy) - set(rpy))
    only_server = sorted(set(rpy) - set(lpy))
    changed = sorted([k for k in set(lpy) & set(rpy) if lpy[k] != rpy[k]])

    print("Oracle baseline check")
    print(f"snapshot:       {snapshot_dir}")
    print(f"local manifest: {local_manifest}")
    print(f"server manifest:{server_manifest}")
    print(f"diff json:      {diff_out}")
    print()
    print(f"only local .py:  {len(only_local)}")
    print(f"only server .py: {len(only_server)}")
    print(f"changed .py:     {len(changed)}")
    print()

    def show(title: str, items: list[tuple[str, str]]) -> None:
        if not items:
            return
        print(title)
        for folder, rel in items[:80]:
            print(f"- {folder}/{rel}")
        if len(items) > 80:
            print(f"(and {len(items) - 80} more)")
        print()

    show("CHANGED:", changed)
    show("ONLY LOCAL:", only_local)
    show("ONLY SERVER:", only_server)

    return 0 if (not only_local and not only_server and not changed) else 2


if __name__ == "__main__":
    raise SystemExit(main())

