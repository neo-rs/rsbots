from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOCAL_MANIFEST = ROOT / "Oraclserver-files" / "rsbots_manifest_local.json"
SERVER_MANIFEST = ROOT / "Oraclserver-files" / "rsbots_manifest_server.json"


def load(p: Path) -> Dict:
    return json.loads(p.read_text(encoding="utf-8"))


def iter_py(manifest: Dict) -> Dict[Tuple[str, str], str]:
    out: Dict[Tuple[str, str], str] = {}
    files = manifest.get("files") or {}
    for folder, mapping in files.items():
        if not isinstance(mapping, dict):
            continue
        if mapping.get("__missing__"):
            continue
        for rel, sha in mapping.items():
            if rel.endswith(".py"):
                out[(folder, rel)] = sha
    return out


def main() -> int:
    if not LOCAL_MANIFEST.exists():
        raise SystemExit(f"Missing local manifest: {LOCAL_MANIFEST}")
    if not SERVER_MANIFEST.exists():
        raise SystemExit(f"Missing server manifest: {SERVER_MANIFEST}")

    local = iter_py(load(LOCAL_MANIFEST))
    server = iter_py(load(SERVER_MANIFEST))

    only_local = sorted(set(local) - set(server))
    only_server = sorted(set(server) - set(local))
    changed = sorted([k for k in set(local) & set(server) if local[k] != server[k]])

    print("Python-only manifest compare")
    print(f"local manifest:  {LOCAL_MANIFEST}")
    print(f"server manifest: {SERVER_MANIFEST}")
    print()
    print(f"only local .py:  {len(only_local)}")
    print(f"only server .py: {len(only_server)}")
    print(f"changed .py:     {len(changed)}")
    print()

    def show(title: str, items):
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


