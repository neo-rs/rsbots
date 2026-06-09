#!/usr/bin/env python3
"""POST simulated Telnyx inbound webhooks for both lines (run on Oracle)."""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timezone

STAMP = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

# Use the two real Telnyx lines as each other's contact so Discord "Send message" works.
LOCAL = "+15419202540"
TOLL_FREE = "+18334882119"

TESTS = [
    {
        "label": "local_2540",
        "our_line": LOCAL,
        "remote": TOLL_FREE,
        "text": f"Fresh test — toll-free to local at {STAMP}",
    },
    {
        "label": "tollfree_2119",
        "our_line": TOLL_FREE,
        "remote": LOCAL,
        "text": f"Fresh test — local to toll-free at {STAMP}",
    },
]


def post_inbound(*, our_line: str, remote: str, text: str) -> tuple[int, str]:
    payload = {
        "data": {
            "event_type": "message.received",
            "id": "test-event",
            "payload": {
                "id": "test-message",
                "from": {"phone_number": remote},
                "to": [{"phone_number": our_line}],
                "text": text,
                "media": [],
            },
        }
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "http://127.0.0.1:8787/webhooks/telnyx",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def main() -> int:
    ok = 0
    for test in TESTS:
        try:
            status, body = post_inbound(
                our_line=test["our_line"],
                remote=test["remote"],
                text=test["text"],
            )
            print(f"{test['label']}: HTTP {status} {body}")
            ok += 1
        except Exception as exc:
            print(f"{test['label']}: FAILED {exc}", file=sys.stderr)
    return 0 if ok == len(TESTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
