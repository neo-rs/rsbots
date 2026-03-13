"""
Telnyx SMS sender. Sends messages via Telnyx Messaging API.
All configuration from .env; no hardcoded values.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# Load .env from script directory
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    # Manual dotenv load to avoid extra dep if not needed
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            if k and v and k not in os.environ:
                # Remove surrounding quotes if present
                if (v.startswith('"') and v.endswith('"')) or (
                    v.startswith("'") and v.endswith("'")
                ):
                    v = v[1:-1]
                os.environ[k] = v

import requests

TELNYX_API_BASE = "https://api.telnyx.com"
MESSAGES_URL = f"{TELNYX_API_BASE}/v2/messages"
PHONE_NUMBERS_URL = f"{TELNYX_API_BASE}/v2/phone_numbers"
REQUEST_TIMEOUT = 30


def _normalize_phone(s: str) -> str:
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) == 10 and not s.strip().startswith("+"):
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return s.strip()


def _get_api_key() -> str:
    key = os.environ.get("TELNYX_API_KEY", "").strip()
    if not key:
        print("Error: TELNYX_API_KEY not set. Add it to .env in this folder.", file=sys.stderr)
        sys.exit(1)
    return key


def _get_from_number(api_key: str) -> str:
    from_num = os.environ.get("FROM_NUMBER", "").strip()
    if from_num:
        return from_num

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    resp = requests.get(
        PHONE_NUMBERS_URL,
        headers=headers,
        params={"page[size]": 100},
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code != 200:
        msg = f"Error: Could not fetch phone numbers (HTTP {resp.status_code}): {resp.text}"
        if resp.status_code == 401:
            msg += "\nCheck TELNYX_API_KEY in .env - copy it again from portal.telnyx.com (#/app/api-keys)"
        print(msg, file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    numbers = data.get("data", [])
    sms_numbers = [
        n for n in numbers
        if n.get("phone_number")
        and (n.get("messaging_profile_id") or n.get("connection_id"))
    ]
    if not sms_numbers:
        sms_numbers = [n for n in numbers if n.get("phone_number")]

    if not sms_numbers:
        print(
            "Error: No phone numbers found in your Telnyx account. "
            "Assign a number to a Messaging Profile at portal.telnyx.com",
            file=sys.stderr,
        )
        sys.exit(1)

    from_num = sms_numbers[0]["phone_number"]
    _save_from_number_to_env(from_num)
    return from_num


def _save_from_number_to_env(number: str) -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    lines = []
    found = False
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.strip().upper().startswith("FROM_NUMBER="):
                lines.append(f'FROM_NUMBER="{number}"')
                found = True
            else:
                lines.append(line)
    if not found:
        lines.append(f'FROM_NUMBER="{number}"')
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Saved FROM_NUMBER to .env: {number}")


def send_sms(to_number: str, text: str, from_number: str | None = None) -> dict:
    api_key = _get_api_key()
    from_num = from_number or _get_from_number(api_key)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "from": from_num,
        "to": to_number,
        "text": text,
    }

    resp = requests.post(
        MESSAGES_URL,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    if resp.status_code not in (200, 201):
        msg = f"Error: HTTP {resp.status_code}\n{resp.text}"
        if resp.status_code == 401:
            msg += "\nCheck TELNYX_API_KEY in .env - copy it again from portal.telnyx.com (#/app/api-keys)"
        if resp.status_code == 403:
            try:
                err = resp.json()
                for e in err.get("errors", []):
                    if e.get("code") == "10039":
                        msg += "\nTelnyx: your account can only send to pre-verified numbers. Add the destination in the portal or upgrade: https://telnyx.com/upgrade"
                        break
            except Exception:
                pass
        print(msg, file=sys.stderr)
        sys.exit(1)

    result = resp.json()
    _append_sent_id(result)
    return result


def _append_sent_id(result: dict) -> None:
    """Append sent message ID to sent_ids.json for status checks."""
    d = result.get("data") or {}
    msg_id = d.get("id")
    if not msg_id:
        return
    to_list = d.get("to") or []
    to_num = to_list[0].get("phone_number", "—") if to_list else "—"
    text = (d.get("text") or "")[:40]
    if len((d.get("text") or "")) > 40:
        text += "…"
    sent_at = d.get("received_at") or datetime.now(timezone.utc).isoformat()
    path = Path(__file__).resolve().parent / "sent_ids.json"
    entries = []
    if path.exists():
        try:
            entries = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            entries = []
    entries.append({"id": msg_id, "to": to_num, "text_preview": text, "sent_at": sent_at})
    path.write_text(json.dumps(entries[-100:], indent=2), encoding="utf-8")


def _format_send_report(result: dict) -> str:
    """Format Telnyx send response as a readable report."""
    d = result.get("data") or {}
    from_ = d.get("from") or {}
    to_list = d.get("to") or []
    to_first = to_list[0] if to_list else {}
    cost = d.get("cost") or {}
    from_num = from_.get("phone_number", "—")
    to_num = to_first.get("phone_number", "—")
    to_status = to_first.get("status", "—")
    msg_id = d.get("id", "—")
    text = (d.get("text") or "")
    text_preview = text[:60] + "…" if len(text) > 60 else text
    amount = cost.get("amount", "—")
    currency = cost.get("currency", "USD")
    return (
        "────────────────────────────────────────\n"
        "  SMS SENT\n"
        "────────────────────────────────────────\n"
        f"  From:     {from_num}\n"
        f"  To:       {to_num}\n"
        f"  Status:   {to_status}\n"
        f"  Message:  {text_preview}\n"
        f"  Cost:     {amount} {currency}\n"
        f"  ID:       {msg_id}\n"
        "────────────────────────────────────────"
    )


def main() -> None:
    args = [a for a in sys.argv[1:] if a not in ("--interactive", "-i")]
    interactive = "--interactive" in sys.argv or "-i" in sys.argv

    def run_once() -> bool:
        to_number = (os.environ.get("TO_NUMBER") or "").strip()
        if not to_number and len(args) > 0:
            to_number = args[0].strip()
        while True:
            if not to_number:
                to_number = input("Recipient phone number (E.164, e.g. +15551234567) or 'q' to quit: ").strip()
            if not to_number or to_number.lower() == "q":
                return False
            if "\\" in to_number or ".bat" in to_number.lower() or ".exe" in to_number.lower():
                print("Error: That looks like a file path. Enter a phone number (e.g. +15418649964).", file=sys.stderr)
                to_number = ""
                continue
            to_number = _normalize_phone(to_number)
            break

        text = (os.environ.get("MESSAGE") or "").strip()
        if len(args) > 1:
            text = args[1].strip()
        if not text:
            text = os.environ.get("DEFAULT_MESSAGE", "").strip()
        if not text:
            text = input("Message text: ").strip()
        if not text:
            print("Error: Message text is required. Set MESSAGE, DEFAULT_MESSAGE in .env, or enter at prompt.", file=sys.stderr)
            sys.exit(1)

        result = send_sms(to_number, text)
        print(_format_send_report(result))
        return True

    if interactive:
        while True:
            if not run_once():
                break
            if input("Send another? (y/n): ").strip().lower() != "y":
                break
            print()
    else:
        run_once()


if __name__ == "__main__":
    main()
