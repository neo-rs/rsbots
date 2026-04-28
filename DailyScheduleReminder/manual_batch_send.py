#!/usr/bin/env python3
"""
Send plain text to Discord channels using the same user token as DailyScheduleReminder (discum).

Modes:
  - Default / --payload: read JSON (see below).
  - --interactive: prompt for Channel ID and Message in the console (repeat until blank ID).

Payload file (JSON) — default: manual_send_payload.json in this folder.

Shapes:
  1) { "sends": [ { "channel_id": "...", "message": "..." }, ... ] }
  2) { "channel_ids": ["...", ...], "message": "..." }  — same body to each channel

Optional keys:
  - "allowed_mentions": e.g. {"parse": ["users", "roles"]} (global; per-send overrides)
  - "delay_seconds": number — pause after each successful channel (default 0)

Auth: DISCORD_USER_TOKEN or config.secrets.json (canonical: reminder_bot.load_token).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_BOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BOT_DIR.parent
for _p in (_REPO_ROOT, _BOT_DIR):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

import reminder_bot as _rb  # noqa: E402


def _normalize_sends(data: dict) -> list[dict]:
    """Return list of {channel_id, message, allowed_mentions?}."""
    sends_in = data.get("sends")
    if isinstance(sends_in, list) and sends_in:
        out = []
        for i, row in enumerate(sends_in):
            if not isinstance(row, dict):
                raise ValueError(f"sends[{i}] must be an object")
            cid = str(row.get("channel_id") or "").strip()
            msg = row.get("message")
            if not cid or not isinstance(msg, str) or not msg.strip():
                raise ValueError(f"sends[{i}] needs non-empty channel_id and message")
            entry = {"channel_id": cid, "message": msg}
            am = row.get("allowed_mentions")
            if am is not None:
                entry["allowed_mentions"] = am
            out.append(entry)
        return out
    ids = data.get("channel_ids")
    msg = data.get("message")
    if isinstance(ids, list) and ids and isinstance(msg, str) and msg.strip():
        out = []
        for cid in ids:
            s = str(cid).strip()
            if s:
                out.append({"channel_id": s, "message": msg})
        if not out:
            raise ValueError("channel_ids had no valid string ids")
        return out
    raise ValueError(
        'Expected either "sends": [{"channel_id","message"}, ...] '
        'or "channel_ids": [...] with "message".'
    )


def _send_rows(bot, rows: list[dict], global_am, delay_f: float) -> tuple[int, int]:
    """Send each row; return (success_count, total)."""
    ok = 0
    total = len(rows)
    for row in rows:
        cid = row["channel_id"]
        content = row["message"]
        am = row.get("allowed_mentions", global_am)
        parts = _rb._chunk_message(content)
        if not parts:
            print(f"Skip empty message for {cid}")
            continue
        failed = False
        for part in parts:
            resp = bot.sendMessage(cid, part, allowed_mentions=am)
            if not resp or getattr(resp, "status_code", None) != 200:
                print(f"FAIL {cid}: {getattr(resp, 'text', resp)}", file=sys.stderr)
                failed = True
                break
        if failed:
            continue
        print(f"OK   {cid}")
        ok += 1
        if delay_f > 0:
            time.sleep(delay_f)
    return ok, total


def _prompt_yes(question: str, default_yes: bool = True) -> bool:
    suffix = " [Y/n]: " if default_yes else " [y/N]: "
    raw = input(question + suffix).strip().lower()
    if not raw:
        return default_yes
    return raw in ("y", "yes")


def run_interactive(dry_run: bool) -> int:
    print("=" * 56)
    print("  Manual send (interactive) — DailyScheduleReminder token")
    print("=" * 56)
    print("Enter a Channel ID, then a Message (single line).")
    print("Leave Channel ID blank to exit.\n")

    bot = None
    if not dry_run:
        try:
            import discum
        except ImportError as e:
            print("discum is required:", e, file=sys.stderr)
            return 1
        token = _rb.load_token()
        bot = discum.Client(token=token, log={"console": False, "file": False})

    sent_ok = 0
    attempts = 0
    while True:
        try:
            cid = input("Channel ID: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nInterrupted.")
            break
        if not cid:
            print("Done.")
            break
        try:
            msg = input("Message: ")
        except (EOFError, KeyboardInterrupt):
            print("\nInterrupted.")
            break
        if not msg.strip():
            print("(Empty message — skipped.)\n")
            continue
        preview = msg.replace("\n", "\\n")
        if len(preview) > 100:
            preview = preview[:100] + "..."
        print(f"  -> #{cid}: {preview!r}")
        if not dry_run:
            if not _prompt_yes("Send this message?", default_yes=True):
                print("Skipped.\n")
                continue
        else:
            if not _prompt_yes("Dry-run only — simulate OK for this entry?", default_yes=True):
                print("Skipped.\n")
                continue

        attempts += 1
        row = {"channel_id": cid, "message": msg}
        if dry_run:
            print("[Dry run] Would send above.\n")
            sent_ok += 1
            continue

        assert bot is not None
        ok, _ = _send_rows(bot, [row], global_am=None, delay_f=0.0)
        sent_ok += ok
        print()

    if dry_run:
        print(f"Dry run finished. Confirmed {sent_ok} simulated send(s).")
        return 0
    print(f"Finished. Successful sends this session: {sent_ok} / {attempts} confirmed.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Send messages (JSON payload or interactive prompts).")
    ap.add_argument(
        "--payload",
        type=Path,
        default=_BOT_DIR / "manual_send_payload.json",
        help="Path to JSON payload (default: manual_send_payload.json next to this script)",
    )
    ap.add_argument("--dry-run", action="store_true", help="JSON: print only; interactive: no Discord calls.")
    ap.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for Channel ID and Message in the console (repeat until Channel ID is blank).",
    )
    args = ap.parse_args()

    if args.interactive:
        return run_interactive(dry_run=args.dry_run)

    path: Path = args.payload
    if not path.is_file():
        print(f"Payload file not found: {path}", file=sys.stderr)
        print("Copy manual_send_payload.example.json to manual_send_payload.json and edit.", file=sys.stderr)
        print("Or run: manual_batch_send.py --interactive", file=sys.stderr)
        return 1

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        print("Payload root must be a JSON object.", file=sys.stderr)
        return 1

    global_am = data.get("allowed_mentions")
    delay = data.get("delay_seconds")
    try:
        delay_f = float(delay) if delay is not None else 0.0
    except (TypeError, ValueError):
        print("delay_seconds must be a number.", file=sys.stderr)
        return 1
    if delay_f < 0:
        delay_f = 0.0

    try:
        rows = _normalize_sends(data)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1

    print(f"Planned {len(rows)} channel(s).")
    for i, row in enumerate(rows):
        preview = row["message"].replace("\n", "\\n")
        if len(preview) > 120:
            preview = preview[:120] + "..."
        print(f"  {i + 1}. #{row['channel_id']}: {preview!r}")

    if args.dry_run:
        print("Dry run: no messages sent.")
        return 0

    try:
        import discum
    except ImportError as e:
        print("discum is required (same as DailyScheduleReminder):", e, file=sys.stderr)
        return 1

    token = _rb.load_token()
    bot = discum.Client(token=token, log={"console": False, "file": False})

    ok, total = _send_rows(bot, rows, global_am, delay_f)
    print(f"Done. Sent to {ok}/{total} channel(s).")
    return 0 if ok == total else 2


if __name__ == "__main__":
    raise SystemExit(main())
