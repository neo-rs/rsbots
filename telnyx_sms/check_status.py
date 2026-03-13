"""
Check delivery status of sent messages via Telnyx API.
Uses sent_ids.json (filled when you send from send_sms.py) or a message ID you provide.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            if k and v and k not in os.environ:
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                os.environ[k] = v

import requests

TELNYX_API_BASE = "https://api.telnyx.com"
REQUEST_TIMEOUT = 30


def _get_api_key() -> str:
    key = os.environ.get("TELNYX_API_KEY", "").strip()
    if not key:
        print("Error: TELNYX_API_KEY not set in .env", file=sys.stderr)
        sys.exit(1)
    return key


def get_message_status(message_id: str) -> dict | None:
    api_key = _get_api_key()
    url = f"{TELNYX_API_BASE}/v2/messages/{message_id}"
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code != 200:
        print(f"Error: HTTP {resp.status_code} - {resp.text}", file=sys.stderr)
        return None
    return resp.json()


def _format_status_report(data: dict) -> str:
    d = data.get("data") or {}
    from_ = d.get("from") or {}
    to_list = d.get("to") or []
    to_first = to_list[0] if to_list else {}
    status = to_first.get("status", "—")
    from_num = from_.get("phone_number", "—")
    to_num = to_first.get("phone_number", "—")
    msg_id = d.get("id", "—")
    text = (d.get("text") or "")[:50]
    if len((d.get("text") or "")) > 50:
        text += "…"
    direction = d.get("direction", "—")
    return (
        "────────────────────────────────────────\n"
        "  DELIVERY STATUS\n"
        "────────────────────────────────────────\n"
        f"  ID:       {msg_id}\n"
        f"  From:     {from_num}\n"
        f"  To:       {to_num}\n"
        f"  Status:   {status}\n"
        f"  Direction: {direction}\n"
        f"  Text:     {text}\n"
        "────────────────────────────────────────"
    )


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    sent_path = script_dir / "sent_ids.json"
    ids_from_file = []
    if sent_path.exists():
        try:
            ids_from_file = json.loads(sent_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if args:
        message_id = args[0].strip()
    else:
        if ids_from_file:
            recent = list(reversed(ids_from_file))[:10]
            print("Recent sent messages (newest first):")
            for i, e in enumerate(recent, 1):
                print(f"  {i}. {e.get('id', '—')}  To: {e.get('to', '—')}  {e.get('text_preview', '')}")
            print()
        while True:
            message_id = input("Enter message ID (or 'q' to quit): ").strip()
            if not message_id or message_id.lower() == "q":
                return
            if "\\" in message_id or ".bat" in message_id.lower() or ".exe" in message_id.lower():
                print("That looks like a file path. Enter a number (1-10), a message ID, or 'all'.", file=sys.stderr)
                continue
            break
        if message_id.lower() == "all" and ids_from_file:
            message_id = None
        elif message_id.isdigit() and ids_from_file:
            idx = int(message_id)
            recent = list(reversed(ids_from_file))[:10]
            if 1 <= idx <= len(recent):
                message_id = recent[idx - 1].get("id", "")

    if not message_id:
        if not ids_from_file:
            print("No sent message IDs in sent_ids.json. Send a message first.", file=sys.stderr)
            return
        to_check = list(reversed(ids_from_file))[:5]
        for e in to_check:
            mid = e.get("id")
            if mid:
                result = get_message_status(mid)
                if result:
                    print(_format_status_report(result))
                    print()
    else:
        result = get_message_status(message_id)
        if result:
            print(_format_status_report(result))


if __name__ == "__main__":
    main()
