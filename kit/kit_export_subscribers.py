"""Export Kit (api.kit.com) subscribers to CSV: name, email, tags, state, created_at.

Uses GET /v4/subscribers (paginated) then GET /v4/subscribers/{id}/tags per subscriber.
Requires KIT_API_KEY in environment or kit.env in this folder.

Usage:
  py -3 kit_export_subscribers.py
  py -3 kit_export_subscribers.py --no-tags   (faster, no tag column)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

KIT_DIR = Path(__file__).resolve().parent
for env_path in [KIT_DIR / "kit.env", KIT_DIR.parent / "config" / "kit.env"]:
    if env_path.exists() and load_dotenv:
        load_dotenv(env_path)
        break

BASE_URL = "https://api.kit.com/v4"
DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def get_api_key() -> str:
    key = os.environ.get("KIT_API_KEY", "").strip()
    if not key:
        print("Set KIT_API_KEY in environment or in kit/kit.env", file=sys.stderr)
        sys.exit(1)
    return key


def check_kit_connection(api_key: str) -> tuple[bool, str]:
    try:
        with requests.Session() as session:
            resp = session.get(
                f"{BASE_URL}/subscribers",
                params={"email_address": "connection-check@example.invalid"},
                headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
                timeout=15,
            )
            if resp.status_code == 200:
                return True, "Connected."
            if resp.status_code in (401, 403):
                return False, "Invalid API key (check kit.env)."
            return True, "Connected."
    except requests.exceptions.RequestException as e:
        return False, f"Connection failed: {e}"


def fetch_all_subscribers(session: requests.Session, api_key: str) -> list[dict]:
    """Paginate GET /v4/subscribers; return list of subscriber dicts."""
    out: list[dict] = []
    after = None
    page = 0
    while True:
        page += 1
        params = {"per_page": 1000, "include_total_count": "true" if page == 1 else "false"}
        if after:
            params["after"] = after
        resp = session.get(
            f"{BASE_URL}/subscribers",
            params=params,
            headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
            timeout=60,
        )
        if resp.status_code != 200:
            return out
        data = resp.json()
        subs = data.get("subscribers") or []
        out.extend(subs)
        pagination = data.get("pagination") or {}
        if not pagination.get("has_next_page"):
            break
        after = pagination.get("end_cursor")
        if not after:
            break
    return out


def _progress_bar(current: int, total: int, width: int = 40) -> None:
    """Write a single-line progress bar to stdout (overwrites with \\r)."""
    if total <= 0:
        return
    pct = current / total
    filled = min(int(width * pct), width)
    if filled >= width:
        bar = "=" * width
    else:
        bar = "=" * filled + ">" + " " * (width - filled - 1)
    sys.stdout.write(f"\r  [{bar}] {current}/{total} ({100*pct:.1f}%)")
    sys.stdout.flush()


def fetch_subscriber_tags(session: requests.Session, api_key: str, subscriber_id: int) -> list[str]:
    """GET /v4/subscribers/{id}/tags; return list of tag names."""
    names: list[str] = []
    after = None
    while True:
        params = {"per_page": 1000}
        if after:
            params["after"] = after
        resp = session.get(
            f"{BASE_URL}/subscribers/{subscriber_id}/tags",
            params=params,
            headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
            timeout=30,
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        tags = data.get("tags") or []
        for t in tags:
            if isinstance(t, dict) and t.get("name"):
                names.append(str(t["name"]))
        pagination = data.get("pagination") or {}
        if not pagination.get("has_next_page"):
            break
        after = pagination.get("end_cursor")
        if not after:
            break
    return names


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Kit subscribers to CSV")
    parser.add_argument("--no-tags", action="store_true", help="Skip fetching tags (faster export)")
    parser.add_argument("--delay", type=float, default=0.05, help="Seconds between tag API calls (default 0.05)")
    args = parser.parse_args()

    api_key = get_api_key()
    print("Connecting to Kit API...", flush=True)
    ok, msg = check_kit_connection(api_key)
    if not ok:
        print(msg, file=sys.stderr)
        sys.exit(1)
    print(msg, flush=True)

    with requests.Session() as session:
        print("Fetching subscribers (paginated)...", flush=True)
        subscribers = fetch_all_subscribers(session, api_key)
    total = len(subscribers)
    print(f"Found {total} subscriber(s).", flush=True)
    if total == 0:
        print("No subscribers to export.")
        return

    # Optionally fetch tags for each subscriber
    include_tags = not args.no_tags
    if include_tags:
        print("Fetching tags for each subscriber...", flush=True)
        with requests.Session() as session:
            for i, sub in enumerate(subscribers):
                sid = sub.get("id")
                if sid is not None:
                    tags = fetch_subscriber_tags(session, api_key, int(sid))
                    sub["_tags"] = tags
                else:
                    sub["_tags"] = []
                _progress_bar(i + 1, total)
                time.sleep(args.delay)
        print()  # newline after progress bar
    else:
        for sub in subscribers:
            sub["_tags"] = []

    # Build CSV path in kit folder
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = KIT_DIR / f"subscribers_export_{stamp}.csv"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email", "tags", "state", "created_at"])
        for sub in subscribers:
            name = (sub.get("first_name") or "").strip()
            email = (sub.get("email_address") or "").strip()
            tags_str = "; ".join(sub.get("_tags") or [])
            state = (sub.get("state") or "").strip()
            created = (sub.get("created_at") or "").strip()
            writer.writerow([name, email, tags_str, state, created])

    print(f"Saved: {out_path}", flush=True)


if __name__ == "__main__":
    main()
