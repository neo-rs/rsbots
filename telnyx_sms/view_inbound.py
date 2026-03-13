"""
Display inbound SMS stored by webhook_server.py (from inbound.json).
Run the webhook server and set its URL in Telnyx to receive messages; then run this to view them.
"""
from __future__ import annotations

import json
from pathlib import Path

INBOUND_PATH = Path(__file__).resolve().parent / "inbound.json"


def main() -> None:
    if not INBOUND_PATH.exists():
        print("No inbound messages yet.")
        print("To receive messages: run run_webhook.bat, set the webhook URL in Telnyx Messaging Profile, then have someone text your Telnyx number.")
        return
    try:
        entries = json.loads(INBOUND_PATH.read_text(encoding="utf-8"))
    except Exception:
        print("Could not read inbound.json")
        return
    if not entries:
        print("No inbound messages yet.")
        return
    print("────────────────────────────────────────────────────────────")
    print("  INBOUND MESSAGES (newest last)")
    print("────────────────────────────────────────────────────────────")
    for e in entries:
        ts = e.get("occurred_at", "")[:19].replace("T", " ")
        from_ = e.get("from", "—")
        text = (e.get("text") or "").replace("\n", " ")
        if len(text) > 50:
            text = text[:50] + "…"
        print(f"  {ts}  From: {from_}")
        print(f"         {text}")
        print()
    print("────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
