from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rsbots_manifest import (
    DEFAULT_RS_BOT_FOLDERS,
    compare_manifests,
)


def _run(cmd: List[str], cwd: Path | None = None) -> Tuple[int, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return p.returncode, (p.stdout or "")


def _ensure_oraclefiles_repo(dest: Path, repo_url: str) -> None:
    dest = dest.resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if (dest / ".git").exists():
        rc, out = _run(["git", "pull", "--ff-only"], cwd=dest)
        if rc != 0:
            raise RuntimeError(f"git pull failed in {dest}:\n{out[-2000:]}")
        # Make working tree deterministic on Windows: avoid CRLF rewriting that breaks byte hashes.
        _run(["git", "config", "core.autocrlf", "false"], cwd=dest)
        _run(["git", "config", "core.eol", "lf"], cwd=dest)
        _run(["git", "reset", "--hard"], cwd=dest)
        return

    if dest.exists() and any(dest.iterdir()):
        raise RuntimeError(f"oraclefiles_dir exists but is not a git repo: {dest}")

    rc, out = _run(["git", "clone", repo_url, str(dest)])
    if rc != 0:
        raise RuntimeError(f"git clone failed:\n{out[-2000:]}")
    # Make working tree deterministic on Windows: avoid CRLF rewriting that breaks byte hashes.
    _run(["git", "config", "core.autocrlf", "false"], cwd=dest)
    _run(["git", "config", "core.eol", "lf"], cwd=dest)
    _run(["git", "reset", "--hard"], cwd=dest)


def _summarize_diff(diff: Dict) -> Dict[str, Dict[str, int]]:
    folders = diff.get("folders", {}) or {}
    out: Dict[str, Dict[str, int]] = {}
    for folder in sorted(folders):
        d = folders[folder] or {}
        out[folder] = {
            "changed": len(d.get("changed") or []),
            "only_local": len(d.get("only_local") or []),
            "only_snapshot": len(d.get("only_remote") or []),
            "missing_local": 1 if d.get("missing_local") else 0,
            "missing_snapshot": 1 if d.get("missing_remote") else 0,
        }
    return out


def _should_skip_dir(name: str) -> bool:
    return name in {"__pycache__", ".git", ".venv", "venv"} or name.startswith(".staging-")


def _sha256_text_normalized(path: Path) -> str:
    """SHA256 over text content with EOL normalized to LF, so Windows CRLF doesn't cause false mismatches."""
    b = path.read_bytes()
    # Normalize EOLs: CRLF -> LF and lone CR -> LF
    b = b.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    import hashlib

    return hashlib.sha256(b).hexdigest()


def _generate_py_manifest_text_normalized(repo_root: Path, bot_folders: List[str]) -> Dict:
    repo_root = Path(repo_root).resolve()
    out: Dict = {
        "repo_root": str(repo_root),
        "bot_folders": bot_folders,
        "files": {},
    }

    for folder in bot_folders:
        base = repo_root / folder
        if not base.exists():
            out["files"][folder] = {"__missing__": True}
            continue
        files: Dict[str, str] = {}
        for p in base.rglob("*.py"):
            if p.is_dir():
                continue
            if any(_should_skip_dir(part) for part in p.parts):
                continue
            rel = p.relative_to(base).as_posix()
            # The live snapshot excludes original_files, so ignore it on both sides.
            if rel.startswith("original_files/"):
                continue
            # Local-only helper; not part of the Ubuntu snapshot comparison.
            if folder == "RSAdminBot" and rel == "compare_oraclefiles_snapshot.py":
                continue
            files[rel] = _sha256_text_normalized(p)
        out["files"][folder] = files

    return out


def main() -> int:
    repo_root = REPO_ROOT

    ap = argparse.ArgumentParser(
        description="Compare local mirror-world python files against the latest neo-rs/oraclefiles py_snapshot (SHA256)."
    )
    ap.add_argument(
        "--oraclefiles-dir",
        default=str(repo_root / ".tmp" / "oraclefiles"),
        help="Local path to a clone of neo-rs/oraclefiles (will be cloned/pulled). Default: .tmp/oraclefiles",
    )
    ap.add_argument(
        "--oraclefiles-repo",
        default="https://github.com/neo-rs/oraclefiles.git",
        help="Git URL for oraclefiles (public HTTPS recommended).",
    )
    ap.add_argument(
        "--folders",
        nargs="*",
        default=list(DEFAULT_RS_BOT_FOLDERS),
        help="Bot folders to compare (default: all RS bot folders).",
    )
    ap.add_argument(
        "--write-report",
        action="store_true",
        help="Write a markdown+json report into docs/ with timestamped filename.",
    )
    args = ap.parse_args()

    oracle_dir = Path(args.oraclefiles_dir).resolve()
    _ensure_oraclefiles_repo(oracle_dir, args.oraclefiles_repo)

    snapshot_root = oracle_dir / "py_snapshot"
    if not snapshot_root.exists():
        print(f"ERROR: oraclefiles repo has no py_snapshot/: {snapshot_root}", file=sys.stderr)
        return 2

    local_manifest = _generate_py_manifest_text_normalized(repo_root=repo_root, bot_folders=list(args.folders))
    snapshot_manifest = _generate_py_manifest_text_normalized(repo_root=snapshot_root, bot_folders=list(args.folders))

    diff = compare_manifests(local_manifest, snapshot_manifest)
    summary = _summarize_diff(diff)

    changed_any = False
    for folder, counts in summary.items():
        if counts["changed"] or counts["only_local"] or counts["only_snapshot"] or counts["missing_local"] or counts["missing_snapshot"]:
            changed_any = True
            break

    print("COMPARE: local mirror-world vs oraclefiles/py_snapshot (python-only)")
    print(f"local_repo_root={repo_root}")
    print(f"oraclefiles_dir={oracle_dir}")
    print(f"snapshot_root={snapshot_root}")
    print("")
    print("Per-folder counts:")
    for folder in sorted(summary):
        c = summary[folder]
        print(
            f"- {folder}: changed={c['changed']} only_local={c['only_local']} only_snapshot={c['only_snapshot']} "
            f"missing_local={c['missing_local']} missing_snapshot={c['missing_snapshot']}"
        )
    print("")

    if not changed_any:
        print("RESULT: OK (all compared *.py files match by SHA256).")
    else:
        print("RESULT: NOT IN SYNC (details below).")
        # print up to a small limit per folder for quick scanning
        folders = diff.get("folders", {}) or {}
        for folder in sorted(folders):
            d = folders[folder] or {}
            ch = d.get("changed") or []
            ol = d.get("only_local") or []
            os_ = d.get("only_remote") or []
            if not ch and not ol and not os_:
                continue
            print(f"\n[{folder}]")
            if ch:
                print("  changed (first 30):")
                for p in ch[:30]:
                    print(f"    - {p}")
            if ol:
                print("  only_local (first 30):")
                for p in ol[:30]:
                    print(f"    - {p}")
            if os_:
                print("  only_snapshot (first 30):")
                for p in os_[:30]:
                    print(f"    - {p}")

    if args.write_report:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        docs = repo_root / "docs"
        docs.mkdir(parents=True, exist_ok=True)
        base = docs / f"COMPARE_RSBOTS_VS_ORACLEFILES_{ts}"

        (base.with_suffix(".json")).write_text(json.dumps(diff, indent=2), encoding="utf-8")

        md_lines = [
            "# Compare: mirror-world vs oraclefiles py_snapshot (python-only)",
            "",
            f"- local_repo_root: `{repo_root}`",
            f"- oraclefiles_dir: `{oracle_dir}`",
            f"- snapshot_root: `{snapshot_root}`",
            f"- generated_utc: `{ts}`",
            "",
            "## Summary (counts per folder)",
            "",
        ]
        for folder in sorted(summary):
            c = summary[folder]
            md_lines.append(
                f"- `{folder}`: changed={c['changed']} only_local={c['only_local']} only_snapshot={c['only_snapshot']}"
            )
        md_lines.append("")
        md_lines.append("## Full diff")
        md_lines.append("")
        md_lines.append(f"See `{base.with_suffix('.json').name}` for the full diff JSON.")
        (base.with_suffix(".md")).write_text("\n".join(md_lines) + "\n", encoding="utf-8")

        print("")
        print(f"Report written: {base.with_suffix('.md')}")

    return 0 if not changed_any else 1


if __name__ == "__main__":
    raise SystemExit(main())


