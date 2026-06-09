#!/usr/bin/env python3
"""Send HELP from local line 2540 to toll-free 2119 (run on Oracle from bridge dir)."""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path.cwd() / ".env")

LOCAL = "+15419202540"
TOLL_FREE = "+18334882119"
TEXT = "HELP"


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
    try:
        result = send(from_number=LOCAL, to_number=TOLL_FREE, text=TEXT)
        print(f"sent: {result.get('status')} from={LOCAL} to={TOLL_FREE} text={TEXT}")
        return 0
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
