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


def _run(cmd: list[str], *, capture_output: bool = True) -> None:
    """Run a subprocess command.

    By default we capture output to keep logs readable, but for long-running
    operations (like snapshot downloads + pruning) we stream output so the
    operator can see progress and cleanup warnings.
    """
    if capture_output:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            sys.stdout.write(res.stdout)
            sys.stderr.write(res.stderr)
            raise SystemExit(res.returncode)
        return

    # Stream output to console (no capture).
    res2 = subprocess.run(cmd)
    if res2.returncode != 0:
        raise SystemExit(res2.returncode)


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
        # Stream snapshot output so you can see pruning + warnings.
        _run(cmd, capture_output=False)

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

    # Critical non-.py drift summary (config + systemd).
    folders = diff.get("folders") or {}
    root_files = diff.get("root_files") or {}

    def _iter_rel_items(folder: str, info: dict) -> list[tuple[str, str]]:
        out2: list[tuple[str, str]] = []
        for key in ("changed", "only_local", "only_remote"):
            items = info.get(key) or []
            if not isinstance(items, list):
                continue
            for rel in items:
                if isinstance(rel, str) and rel:
                    out2.append((folder, rel))
        return out2

    non_py_items: list[tuple[str, str]] = []
    for folder, info in folders.items():
        if isinstance(info, dict):
            non_py_items.extend(_iter_rel_items(str(folder), info))

    root_items: list[tuple[str, str]] = []
    if isinstance(root_files, dict):
        for key in ("changed", "only_local", "only_remote"):
            items = root_files.get(key) or []
            if not isinstance(items, list):
                continue
            for rel in items:
                if isinstance(rel, str) and rel:
                    root_items.append(("(root)", rel))

    def _is_config_json(item: tuple[str, str]) -> bool:
        _, rel = item
        return rel.endswith("config.json")

    def _is_messages_json(item: tuple[str, str]) -> bool:
        _, rel = item
        return rel.endswith("messages.json")

    def _is_service(item: tuple[str, str]) -> bool:
        _, rel = item
        return rel.endswith(".service")

    def _is_other_json(item: tuple[str, str]) -> bool:
        _, rel = item
        return rel.endswith(".json") and (not rel.endswith("config.secrets.json")) and (not _is_config_json(item)) and (not _is_messages_json(item))

    config_json_drift = sorted([x for x in non_py_items if _is_config_json(x)])
    messages_json_drift = sorted([x for x in non_py_items if _is_messages_json(x)])
    service_drift = sorted([x for x in (non_py_items + root_items) if _is_service(x)])
    other_json_drift = sorted([x for x in non_py_items if _is_other_json(x)])

    critical_drift = sorted(set(config_json_drift + messages_json_drift + service_drift))

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
    print("Non-.py drift (high-signal)")
    print(f"config.json drift:   {len(config_json_drift)}")
    print(f"messages.json drift: {len(messages_json_drift)}")
    print(f"*.service drift:     {len(service_drift)}")
    print(f"other .json drift:   {len(other_json_drift)}")
    print(f"CRITICAL drift total:{len(critical_drift)}")
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
    show("CRITICAL (config/service) DRIFT:", critical_drift)

    if critical_drift:
        print("WARNING: Critical non-.py drift detected (config/service). See list above.")
        print()

    # Exit code remains python-only to preserve historical workflow behavior.
    return 0 if (not only_local and not only_server and not changed) else 2


if __name__ == "__main__":
    raise SystemExit(main())

