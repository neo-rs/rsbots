"""Unsubscribe emails from Kit (api.kit.com) via GET subscriber + POST unsubscribe.

Matches the workflow:
  1. GET https://api.kit.com/v4/subscribers?email_address=<email>
  2. POST https://api.kit.com/v4/subscribers/<id>/unsubscribe

Requires KIT_API_KEY in environment or in kit.env in this folder.

Usage:
  # Single email (run from repo root or kit/)
  py -3 kit/kit_unsubscribe.py user@example.com

  # From CSV (expects "Email" column; optional batching)
  py -3 kit/kit_unsubscribe.py path/to/file.csv
  py -3 kit/kit_unsubscribe.py kit/test_emails.csv --batch-size 2 --batch-interval 1000
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
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

# This folder (kit/) - script and kit.env live here
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
    """Verify API key and reachability. Returns (ok, message)."""
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


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def get_subscriber_by_email(session: requests.Session, api_key: str, email: str) -> dict | None:
    """GET /subscribers?email_address=<email>. Returns subscriber object or None."""
    email = normalize_email(email)
    if not email:
        return None
    url = f"{BASE_URL}/subscribers"
    resp = session.get(
        url,
        params={"email_address": email},
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        timeout=30,
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    subscribers = data.get("subscribers") or []
    return subscribers[0] if subscribers else None


def unsubscribe_subscriber(session: requests.Session, api_key: str, subscriber_id: str) -> bool:
    """POST /subscribers/<id>/unsubscribe. Returns True on success."""
    url = f"{BASE_URL}/subscribers/{subscriber_id}/unsubscribe"
    resp = session.post(
        url,
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        json={},
        timeout=30,
    )
    return resp.status_code in (200, 204)


def unsubscribe_email(api_key: str, email: str, verbose: bool = True) -> tuple[bool, str]:
    """Look up subscriber by email and unsubscribe. Returns (success, message)."""
    email = normalize_email(email)
    if not email:
        return False, "Empty email"
    with requests.Session() as session:
        if verbose:
            print("  -> Looking up subscriber...", flush=True)
        sub = get_subscriber_by_email(session, api_key, email)
        if not sub:
            return False, f"Subscriber not found: {email}"
        sid = sub.get("id")
        if not sid:
            return False, f"No id in subscriber for: {email}"
        if verbose:
            print("  -> Unsubscribing...", flush=True)
        ok = unsubscribe_subscriber(session, api_key, str(sid))
        return ok, "Unsubscribed" if ok else f"Unsubscribe failed for: {email}"


def emails_from_csv(path: Path) -> list[str]:
    """Read emails from CSV; look for 'Email' column (case-insensitive)."""
    emails = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return emails
        key = next((c for c in reader.fieldnames if c.strip().lower() == "email"), None)
        if not key:
            return emails
        for row in reader:
            val = (row.get(key) or "").strip()
            if val and "@" in val:
                emails.append(normalize_email(val))
    return list(dict.fromkeys(emails))  # dedupe order-preserving


def main() -> None:
    parser = argparse.ArgumentParser(description="Unsubscribe emails from Kit API")
    parser.add_argument("input", help="Single email or path to CSV with 'Email' column")
    parser.add_argument("--batch-size", type=int, default=2, help="Max requests per batch when using CSV (default 2)")
    parser.add_argument("--batch-interval", type=int, default=1000, help="Ms between batches (default 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Only list emails, do not unsubscribe")
    args = parser.parse_args()

    api_key = get_api_key()
    print("Connecting to Kit API...", flush=True)
    ok, msg = check_kit_connection(api_key)
    if not ok:
        print(msg, file=sys.stderr)
        sys.exit(1)
    print(msg, flush=True)

    # Single email vs CSV
    s = args.input.strip()
    if "@" in s and "\n" not in s and not Path(s).exists():
        emails = [normalize_email(s)]
    else:
        path = Path(s)
        if not path.is_absolute():
            # Try kit dir first, then cwd
            path = (KIT_DIR / path).resolve()
            if not path.exists():
                path = (Path.cwd() / s).resolve()
        if not path.exists():
            print(f"File not found: {path}", file=sys.stderr)
            sys.exit(1)
        emails = emails_from_csv(path)
        if not emails:
            if path.suffix.lower() != ".csv":
                print(f"Not a CSV file: {path.name}. Use a .csv file with an 'Email' column (e.g. test_emails.csv).", file=sys.stderr)
            else:
                print(f"No 'Email' column or no valid emails in {path.name}. Use a CSV with a column named Email.", file=sys.stderr)
            sys.exit(1)
        print(f"Loaded CSV: {path.name} -> {len(emails)} email(s)", flush=True)

    if args.dry_run:
        print(f"[dry-run] Would process {len(emails)} email(s):", flush=True)
        for j, e in enumerate(emails, 1):
            print(f"  {j}. {e}", flush=True)
        return

    total = len(emails)
    batch_size = max(1, args.batch_size)
    batch_interval_sec = args.batch_interval / 1000.0
    ok_count = fail_count = 0

    print(f"Starting: {total} email(s), batch size {batch_size}, interval {args.batch_interval} ms", flush=True)
    for i, email in enumerate(emails):
        n = i + 1
        pct = 100.0 * n / total if total else 0
        print(f"[{n}/{total}] ({pct:.1f}%) {email}", flush=True)
        success, msg = unsubscribe_email(api_key, email)
        if success:
            ok_count += 1
            print(f"  [OK] {msg}", flush=True)
        else:
            fail_count += 1
            print(f"  [FAIL] {msg}", flush=True)
        # Batching: wait after every batch_size requests
        if n % batch_size == 0 and n < total:
            print(f"  ... waiting {args.batch_interval} ms before next batch ...", flush=True)
            time.sleep(batch_interval_sec)

    print(f"Done: {ok_count} unsubscribed, {fail_count} failed.", flush=True)


if __name__ == "__main__":
    main()
