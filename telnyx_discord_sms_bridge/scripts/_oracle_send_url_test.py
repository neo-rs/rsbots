#!/usr/bin/env python3
"""Send URL test SMS via bridge /send (run on Oracle)."""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path.cwd() / ".env")

URL = "https://resellingsecrets.com/"
TESTS = [
    ("local_to_tollfree", "+15419202540", "+18334882119"),
    ("tollfree_to_local", "+18334882119", "+15419202540"),
]


def send(*, from_number: str, to_number: str, text: str) -> dict:
    key = os.getenv("BRIDGE_API_KEY", "").strip()
    if not key:
        raise RuntimeError("BRIDGE_API_KEY missing in .env")
    body = json.dumps({"to": to_number, "text": text, "from_number": from_number}).encode("utf-8")
    req = urllib.request.Request(
        "http://127.0.0.1:8787/send",
        data=body,
        headers={"Content-Type": "application/json", "X-Bridge-Key": key},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    ok = 0
    for label, from_number, to_number in TESTS:
        try:
            result = send(from_number=from_number, to_number=to_number, text=URL)
            status = result.get("status", "unknown")
            print(f"{label}: {status} from={from_number} to={to_number}")
            ok += 1
        except Exception as exc:
            print(f"{label}: FAILED {exc}", file=sys.stderr)
    return 0 if ok == len(TESTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
