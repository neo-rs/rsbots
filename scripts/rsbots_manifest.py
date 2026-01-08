from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


def _load_manifest_lib():
    """Load the canonical top-level rsbots_manifest.py from THIS workspace.

    We intentionally do not load rsbots_manifest.py from the target --repo-root, because
    snapshots may contain older versions of the library. We want one source of truth
    for hashing + include/exclude rules when comparing local vs server snapshots.
    """
    canonical_root = Path(__file__).resolve().parents[1]
    lib_path = canonical_root / "rsbots_manifest.py"
    spec = importlib.util.spec_from_file_location("rsbots_manifest_lib", lib_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load manifest library: {lib_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a hashed file manifest for RS bots (no secrets).")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]), help="Repo root directory")
    parser.add_argument(
        "--bots",
        default="",
        help="Comma-separated bot folders to include (default: RS bots)",
    )
    parser.add_argument(
        "--normalize-text-eol",
        action="store_true",
        help="Normalize CRLF/LF when hashing text files (prevents Windows vs Linux false mismatches).",
    )
    parser.add_argument("--out", default="", help="Write JSON to this file (default: stdout)")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    lib = _load_manifest_lib()
    default_bots = getattr(lib, "DEFAULT_RS_BOT_FOLDERS")
    generate_manifest = getattr(lib, "generate_manifest")

    bot_folders = [x.strip() for x in args.bots.split(",") if x.strip()] if args.bots else list(default_bots)
    manifest = generate_manifest(repo_root, bot_folders=bot_folders, normalize_text_eol=bool(args.normalize_text_eol))

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        print(str(out_path))
    else:
        print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()


