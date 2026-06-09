#!/usr/bin/env python3
"""Clear cross-line thread state after Discord cards were deleted (run on Oracle)."""
from __future__ import annotations

import json
from pathlib import Path

KEYS = ("+15419202540|+18334882119", "+18334882119|+15419202540")
PATH = Path("data/conversations.json")


def main() -> None:
    data = json.loads(PATH.read_text(encoding="utf-8")) if PATH.exists() else {"threads": {}}
    threads = data.setdefault("threads", {})
    for key in KEYS:
        entry = dict(threads.get(key) or {})
        entry["message_id"] = None
        entry["lines"] = []
        threads[key] = entry
    PATH.parent.mkdir(parents=True, exist_ok=True)
    PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Reset {len(KEYS)} thread(s) in {PATH}")


if __name__ == "__main__":
    main()
