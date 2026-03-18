"""Update Kit subscriber tags: remove all current tags, add tag based on Status in CSV.
If subscriber not found, creates them (with Name from CSV) and adds the tag.
If subscriber exists but is cancelled/unsubscribed, sets state to active (re-subscribes) then updates tags.

Reads update_tag.csv (columns: Name, Email, Status).
Status -> tag IDs:
  active: [16819677], trialing: [16825287], churned: [16825530],
  canceling: [16824900], past_due: [16887341], expired: [16888765], left: [16889871]

Requires KIT_API_KEY in environment or kit.env in this folder.

Usage:
  py -3 kit_update_tags.py
  py -3 kit_update_tags.py update_tag.csv
  py -3 kit_update_tags.py --dry-run
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

# Status (lowercase) -> tag IDs to add (names are fetched from Kit API)
STATUS_TAG_IDS: dict[str, list[int]] = {
    "active": [16819677],
    "trialing": [16825287],
    "churned": [16825530],
    "canceling": [16824900],
    "past_due": [16887341],
    "expired": [16888765],
    "left": [16889871],
    "no": [17193272],
}


def fetch_tag_names(session: requests.Session, api_key: str) -> dict[int, str]:
    """GET /v4/tags (paginated). Return {tag_id: name} so we show actual Kit tag names."""
    out: dict[int, str] = {}
    after = None
    while True:
        params = {"per_page": 1000}
        if after:
            params["after"] = after
        resp = session.get(
            f"{BASE_URL}/tags",
            params=params,
            headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
            timeout=30,
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        for t in data.get("tags") or []:
            tid, name = t.get("id"), t.get("name")
            if tid is not None and name is not None:
                out[int(tid)] = str(name)
        pag = data.get("pagination") or {}
        if not pag.get("has_next_page"):
            break
        after = pag.get("end_cursor")
        if not after:
            break
    return out


def get_tag_name_for_status(tag_names: dict[int, str], status: str) -> str:
    """Resolve status to display name from API-fetched tag names, or fallback to status key."""
    tag_ids = STATUS_TAG_IDS.get(status, [])
    if not tag_ids:
        return status
    name = tag_names.get(tag_ids[0])
    return name if name is not None else status


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


def create_subscriber(session: requests.Session, api_key: str, email: str, first_name: str = "") -> tuple[dict | None, str]:
    """POST /v4/subscribers. Returns (subscriber_dict, "") on success or (None, error_message) on failure."""
    body = {"email_address": (email or "").strip().lower()}
    if (first_name or "").strip():
        body["first_name"] = (first_name or "").strip()[:255]
    try:
        resp = session.post(
            f"{BASE_URL}/subscribers",
            headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201, 202):
            data = resp.json()
            sub = data.get("subscriber")
            return (sub, "") if sub else (None, "No subscriber in response")
        try:
            err = resp.json()
            msgs = err.get("errors") or err.get("message") or [resp.text or f"HTTP {resp.status_code}"]
            if isinstance(msgs, list):
                msg = "; ".join(str(m) for m in msgs[:3])
            else:
                msg = str(msgs)
        except Exception:
            msg = resp.text or f"HTTP {resp.status_code}"
        return None, (msg[:80] if msg else "Unknown error")
    except requests.exceptions.RequestException as e:
        return None, str(e)[:80]


def get_subscriber_by_email(session: requests.Session, api_key: str, email: str) -> dict | None:
    resp = session.get(
        f"{BASE_URL}/subscribers",
        params={"email_address": (email or "").strip().lower(), "status": "all"},
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        timeout=30,
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    subs = data.get("subscribers") or []
    return subs[0] if subs else None


def fetch_subscriber_tags(session: requests.Session, api_key: str, subscriber_id: int) -> list[dict]:
    """Return list of tag dicts with id."""
    out: list[dict] = []
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
        out.extend(tags)
        pagination = data.get("pagination") or {}
        if not pagination.get("has_next_page"):
            break
        after = pagination.get("end_cursor")
        if not after:
            break
    return out


def remove_tag(session: requests.Session, api_key: str, tag_id: int, subscriber_id: int) -> bool:
    resp = session.delete(
        f"{BASE_URL}/tags/{tag_id}/subscribers/{subscriber_id}",
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        timeout=30,
    )
    return resp.status_code in (200, 204)


def add_tag(session: requests.Session, api_key: str, tag_id: int, subscriber_id: int) -> bool:
    resp = session.post(
        f"{BASE_URL}/tags/{tag_id}/subscribers/{subscriber_id}",
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        json={},
        timeout=30,
    )
    return resp.status_code in (200, 201)


def set_subscriber_active(session: requests.Session, api_key: str, subscriber_id: int, email: str) -> bool:
    """PUT /v4/subscribers/{id} with state=active to re-subscribe (change from cancelled to active)."""
    resp = session.put(
        f"{BASE_URL}/subscribers/{subscriber_id}",
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        json={"email_address": (email or "").strip().lower(), "state": "active"},
        timeout=30,
    )
    return resp.status_code in (200, 202)


def _total_from_response(data: dict) -> int:
    """Extract total_count from Kit list response (pagination or top-level)."""
    pag = data.get("pagination") or {}
    return int(pag.get("total_count") or data.get("total_count") or 0)


# Kit subscriber states (for breakdown report)
SUBSCRIBER_STATUSES = [
    ("active", "Active (confirmed)"),
    ("cancelled", "Cancelled / unsubscribed"),
    ("bounced", "Bounced"),
    ("complained", "Complained"),
    ("inactive", "Inactive"),
]


def get_subscriber_count_by_status(session: requests.Session, api_key: str, status: str) -> int:
    """GET /v4/subscribers with include_total_count and status filter."""
    resp = session.get(
        f"{BASE_URL}/subscribers",
        params={"per_page": 1, "include_total_count": "true", "status": status},
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        timeout=30,
    )
    if resp.status_code != 200:
        return 0
    return _total_from_response(resp.json())


def get_tag_subscriber_count(session: requests.Session, api_key: str, tag_id: int, status: str = "active") -> int:
    """GET /v4/tags/{tag_id}/subscribers with include_total_count. Use status=active to match dashboard."""
    resp = session.get(
        f"{BASE_URL}/tags/{tag_id}/subscribers",
        params={"per_page": 1, "include_total_count": "true", "status": status},
        headers={**DEFAULT_HEADERS, "X-Kit-Api-Key": api_key},
        timeout=30,
    )
    if resp.status_code != 200:
        return 0
    return _total_from_response(resp.json())


def load_csv(path: Path) -> list[tuple[str, str, str]]:
    """Return list of (email, status, name) from CSV with Email, Status, Name columns."""
    rows: list[tuple[str, str, str]] = []
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with path.open(newline="", encoding=enc) as f:
                reader = csv.DictReader(f)
                fn = reader.fieldnames or []
                key_email = next((c for c in fn if c.strip().lower() == "email"), None)
                key_status = next((c for c in fn if c.strip().lower() == "status"), None)
                key_name = next((c for c in fn if c.strip().lower() == "name"), None)
                if not key_email or not key_status:
                    return rows
                for r in reader:
                    email = (r.get(key_email) or "").strip()
                    status = (r.get(key_status) or "").strip().lower()
                    name = (r.get(key_name) or "").strip() if key_name else ""
                    if email and "@" in email:
                        rows.append((email, status, name))
                return rows
        except UnicodeDecodeError:
            continue
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Update Kit tags from CSV Status")
    parser.add_argument("input", nargs="?", default="update_tag.csv", help="CSV with Email, Status columns")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done, no API writes")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between API calls (default 0.5)")
    args = parser.parse_args()

    api_key = get_api_key()
    print("Connecting to Kit API...", flush=True)
    ok, msg = check_kit_connection(api_key)
    if not ok:
        print(msg, file=sys.stderr)
        sys.exit(1)
    print(msg, flush=True)

    # Fetch tag names from Kit API (id -> name) so logs and report show actual names
    with requests.Session() as session:
        tag_names = fetch_tag_names(session, api_key)
    if not tag_names:
        print("Warning: could not fetch tag names from API; will use status keys.", flush=True)

    path = Path(args.input)
    if not path.is_absolute():
        path = (KIT_DIR / path).resolve()
    if not path.exists():
        path = (Path.cwd() / args.input).resolve()
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    rows = load_csv(path)
    if not rows:
        print("No rows with Email and Status in CSV.", file=sys.stderr)
        sys.exit(1)
    print(f"Loaded {path.name}: {len(rows)} row(s)", flush=True)

    if args.dry_run:
        print("[dry-run] Would process:", flush=True)
        for i, (email, status, name) in enumerate(rows[:10]):
            tag_ids = STATUS_TAG_IDS.get(status, [])
            print(f"  {i+1}. {email} -> status={status!r} add tags={tag_ids}", flush=True)
        if len(rows) > 10:
            print(f"  ... and {len(rows)-10} more", flush=True)
        return

    ok_count = created_count = fail_count = skip_count = 0
    total = len(rows)
    failed_emails: list[tuple[str, str]] = []
    results: list[tuple[str, str, str, str, str]] = []
    print("Processing (lookup first, then create/re-subscribe/update tags as needed)...", flush=True)

    with requests.Session() as session:
        for i, (email, status, name) in enumerate(rows):
            n = i + 1
            pct = 100.0 * n / total if total else 0
            tag_name = get_tag_name_for_status(tag_names, status)
            target_tag_ids = set(STATUS_TAG_IDS.get(status, []))
            print(f"[{n}/{total}] ({pct:.1f}%) {email} -> {tag_name}", flush=True)

            # Step 1: lookup member
            sub = get_subscriber_by_email(session, api_key, email)
            time.sleep(args.delay)

            if not sub:
                # Not in Kit -> create + add tag
                print("  -> lookup: Not in Kit", flush=True)
                print("  -> creating new subscriber...", flush=True)
                sub, err = create_subscriber(session, api_key, email, name)
                time.sleep(args.delay)
                if not sub:
                    fail_count += 1
                    failed_emails.append((email, err))
                    results.append((name, email, status, "Fail", err[:200] if err else ""))
                    print(f"  [FAIL] {err}", flush=True)
                    time.sleep(2)
                    continue
                created_count += 1
                for tid in target_tag_ids:
                    add_tag(session, api_key, tid, int(sub["id"]))
                    time.sleep(args.delay)
                results.append((name, email, status, "Success", ""))
                print(f"  [OK] Created + tagged: {email} ({tag_name})", flush=True)
                continue

            # In Kit -> get state and current tags
            sid = int(sub["id"])
            state = (sub.get("state") or "").strip().lower()
            current_tags = fetch_subscriber_tags(session, api_key, sid)
            time.sleep(args.delay)
            current_tag_ids = {int(t["id"]) for t in current_tags if t.get("id") is not None}
            has_target_tag = bool(target_tag_ids & current_tag_ids)
            current_tag_names = ", ".join(tag_names.get(t["id"], str(t["id"])) for t in current_tags if t.get("id") is not None)

            print(f"  -> lookup: state={state}, tags=[{current_tag_names or 'none'}]", flush=True)

            # Skip if already active + already has target tag
            if state == "active" and has_target_tag:
                skip_count += 1
                results.append((name, email, status, "Skipped", "already correct"))
                print(f"  [SKIP] already active with {tag_name}", flush=True)
                continue

            # Re-subscribe if cancelled/unsubscribed
            if state != "active":
                print(f"  -> re-subscribing (was {state})...", flush=True)
                set_subscriber_active(session, api_key, sid, email)
                time.sleep(args.delay)
                print(f"  [OK] Re-subscribed", flush=True)

            # Update tags only if needed
            if not has_target_tag:
                if current_tag_ids:
                    print(f"  -> removing {len(current_tag_ids)} tag(s), adding {tag_name}...", flush=True)
                    for tid in current_tag_ids:
                        remove_tag(session, api_key, tid, sid)
                        time.sleep(args.delay)
                else:
                    print(f"  -> adding {tag_name}...", flush=True)
                for tid in target_tag_ids:
                    add_tag(session, api_key, tid, sid)
                    time.sleep(args.delay)
                print(f"  [OK] Tags updated: {tag_name}", flush=True)
            elif state != "active":
                print(f"  [OK] Done (re-subscribed, tag already correct)", flush=True)

            ok_count += 1
            results.append((name, email, status, "Success", ""))

    print("\n")
    print(f"Done: {ok_count} updated, {created_count} created+tagged, {skip_count} skipped, {fail_count} failed.", flush=True)
    if failed_emails:
        print(f"\nFailed (first 20):", flush=True)
        for email, err in failed_emails[:20]:
            print(f"  {email}: {err}", flush=True)
        if len(failed_emails) > 20:
            print(f"  ... and {len(failed_emails) - 20} more", flush=True)

    # Snapshot: write results CSV (Success/Fail per row) and failed-only CSV for retry
    if results:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        results_path = KIT_DIR / f"update_tag_results_{stamp}.csv"
        with results_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Name", "Email", "Status", "Result", "Error"])
            w.writerows(results)
        print(f"\nResults snapshot: {results_path.name}", flush=True)
        failed_rows = [(r[0], r[1], r[2]) for r in results if r[3] == "Fail"]
        if failed_rows:
            failed_path = KIT_DIR / "update_tag_failed.csv"
            with failed_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["Name", "Email", "Status"])
                w.writerows(failed_rows)
            print(f"Failed rows for retry: {failed_path.name} ({len(failed_rows)} row(s))", flush=True)

    # Live counts report: subscribers by status + tag counts (active only, matches dashboard)
    print("\n--- Live counts (Kit) ---", flush=True)
    try:
        with requests.Session() as session:
            print("Subscribers by status:", flush=True)
            total = 0
            for status_key, label in SUBSCRIBER_STATUSES:
                n = get_subscriber_count_by_status(session, api_key, status_key)
                total += n
                print(f"  {label}: {n}", flush=True)
            n_all = get_subscriber_count_by_status(session, api_key, "all")
            print(f"  Total (all): {n_all}", flush=True)
            print("Tag counts (active subscribers, matches dashboard):", flush=True)
            for status, tag_ids in STATUS_TAG_IDS.items():
                name = get_tag_name_for_status(tag_names, status)
                count = 0
                for tid in tag_ids:
                    count += get_tag_subscriber_count(session, api_key, tid, status="active")
                print(f"  {name}: {count}", flush=True)
    except Exception as e:
        print(f"  (Could not fetch counts: {e})", flush=True)


if __name__ == "__main__":
    main()
