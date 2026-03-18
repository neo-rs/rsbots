"""
Export GoHighLevel (GHL) contacts to CSV, with focus on invalid emails.

Fetches all contacts via GHL Contacts Search API, records email and validEmail,
and writes a CSV. Optionally filters to only contacts GHL considers invalid
(validEmail false/missing) so you can audit who you're being charged for
despite failed emails.

Credentials (do not commit):
  GHL_BEARER_TOKEN  - Private Integration Bearer (e.g. pit-...)
  GHL_LOCATION_ID   - Location/sub-account ID (optional if using JWT that has it)

Or pass via CLI: --bearer and --location-id.
Default location ID is GmTfoYNHLeHumMfDIset (override with GHL_LOCATION_ID or --location-id).

Usage:
  set GHL_BEARER_TOKEN=pit-...
  python kit/ghl_export_contacts.py

  python kit/ghl_export_contacts.py --invalid-only -o kit/ghl_invalid_emails.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

KIT_DIR = Path(__file__).resolve().parent
try:
    import requests
except ImportError:
    print("Install requests: pip install requests", file=sys.stderr)
    sys.exit(1)

BASE_URL = "https://services.leadconnectorhq.com"
SEARCH_URL = f"{BASE_URL}/contacts/search"
VERSION = "2021-07-28"
DELETE_CONTACT_URL = f"{BASE_URL}/contacts/{{contact_id}}"
VERIFY_EMAIL_URL = f"{BASE_URL}/email/verify"
PAGE_LIMIT = 100
CSV_OPEN_RETRIES = 3
CSV_OPEN_RETRY_DELAY = 2.0
VERIFY_DELAY_SEC = 0.3
# Throttle to avoid GHL rate limit / 400 after many pages
REQUEST_DELAY_SEC = 0.5
REQUEST_DELAY_FAST = 0.12  # --fast: shorter delay for testing
MAX_RETRIES = 4
RETRY_BACKOFF_SEC = 15
RETRY_BACKOFF_FAST = 5  # --fast: shorter backoff
# Checkpoint CSV every N contacts
CHECKPOINT_EVERY = 10_000
# GHL API only returns pages 1-100 (10k contacts); page 101+ returns 400. No retry.
DELETE_DELAY_SEC = 0.2  # delay between delete requests to avoid rate limit


def _headers(bearer: str) -> dict:
    return {
        "Authorization": f"Bearer {bearer}",
        "Version": VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def search_contacts(bearer: str, location_id: str, query: str = "", page: int = 1, after_id: str | None = None) -> dict:
    """POST /contacts/search. Paginate with page=1,2,... Optional after_id to try cursor (next batch after that contact)."""
    body = {
        "locationId": location_id,
        "pageLimit": PAGE_LIMIT,
        "query": query or "",
        "page": page,
    }
    if after_id:
        body["startAfterId"] = after_id  # try cursor; GHL may reject (400/422)
    for attempt in range(MAX_RETRIES):
        r = requests.post(SEARCH_URL, headers=_headers(bearer), json=body, timeout=60)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", RETRY_BACKOFF_SEC))
            print(f"  Rate limited (429); waiting {wait}s before retry {attempt + 1}/{MAX_RETRIES}...", flush=True)
            time.sleep(wait)
            continue
        if r.status_code in (502, 503) and attempt < MAX_RETRIES - 1:
            wait = RETRY_BACKOFF_SEC * (attempt + 1)
            print(f"  API {r.status_code}; waiting {wait}s before retry {attempt + 1}/{MAX_RETRIES}...", flush=True)
            time.sleep(wait)
            continue
        # 400: do not retry (often page cap or bad request); caller may save partial
        r.raise_for_status()
        return r.json()
    raise RuntimeError("search_contacts: max retries exceeded")


def delete_contact(bearer: str, contact_id: str) -> bool:
    """DELETE /contacts/:contactId. Returns True if deleted or 404 (already gone), False on failure after retries."""
    url = DELETE_CONTACT_URL.format(contact_id=contact_id)
    for attempt in range(MAX_RETRIES):
        r = requests.delete(url, headers=_headers(bearer), timeout=30)
        if r.status_code in (200, 204):
            return True
        if r.status_code == 404:
            return True  # already deleted
        if r.status_code == 429 and attempt < MAX_RETRIES - 1:
            wait = int(r.headers.get("Retry-After", RETRY_BACKOFF_SEC))
            time.sleep(wait)
            continue
        if r.status_code in (502, 503) and attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
            continue
        return False
    return False


def delete_contacts_from_ghl(bearer: str, contact_ids: list[str], delay_sec: float = DELETE_DELAY_SEC) -> tuple[int, int]:
    """Delete each contact from GHL. Returns (deleted_ok, failed)."""
    deleted, failed = 0, 0
    for i, cid in enumerate(contact_ids):
        if not (cid or "").strip():
            continue
        if delete_contact(bearer, cid):
            deleted += 1
        else:
            failed += 1
        if (i + 1) % 500 == 0:
            print(f"  Deleted {deleted} so far ({failed} failed)...", flush=True)
        time.sleep(delay_sec)
    return deleted, failed


def verify_contact_email(bearer: str, location_id: str, contact_id: str, email: str) -> str:
    """Call GHL POST /email/verify for a contact. Returns 'valid' | 'invalid' | 'skip'.
    skip = no email, API error, or unknown. GHL may charge per verification."""
    if not (email or "").strip():
        return "skip"
    try:
        r = requests.post(
            VERIFY_EMAIL_URL,
            headers=_headers(bearer),
            json={"locationId": location_id, "contactId": contact_id},
            timeout=30,
        )
        if r.status_code == 201:
            data = r.json() if r.text else {}
            if data.get("verified") is True or data.get("valid") is True:
                return "valid"
            if data.get("verified") is False or data.get("valid") is False:
                return "invalid"
            return "skip"
        if r.status_code in (400, 422):
            return "invalid"
        return "skip"
    except Exception:
        return "skip"


def _append_batch_to_csv(out_path: Path, fieldnames: list[str], rows: list[dict], file_exists: bool) -> tuple[Path, bool, bool]:
    """Append rows to CSV. On PermissionError retries then falls back to timestamped file.
    Returns (path_used, new_file_exists, used_fallback). Caller should use path_used for next round if used_fallback."""
    for attempt in range(CSV_OPEN_RETRIES):
        try:
            with open(out_path, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    w.writeheader()
                for row in rows:
                    w.writerow(row)
            return out_path, True, False
        except PermissionError:
            if attempt < CSV_OPEN_RETRIES - 1:
                time.sleep(CSV_OPEN_RETRY_DELAY)
                continue
            fallback = out_path.parent / (out_path.stem + "_" + time.strftime("%Y%m%d_%H%M%S") + out_path.suffix)
            print(f"  WARNING: {out_path} in use (close Excel etc.). Writing to {fallback} for this run.")
            with open(fallback, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                for row in rows:
                    w.writerow(row)
            return fallback, True, True
    return out_path, file_exists, False



def is_invalid_email(contact: dict) -> bool:
    """True if we treat this contact as invalid for export/delete.
    Only validEmail === True is treated as valid. False, None, or missing => invalid.
    Note: API returns None for many contacts (GHL UI shows 'Not Verified'). After the
    user clicks 'Verify Here' in GHL, validEmail can become True. So contacts with
    validEmail None are 'unverified' and may be verifiable - use export-only to audit
    and only run delete after you are sure (or use --dry-run first)."""
    valid = contact.get("validEmail")
    if valid is True:
        return False
    # validEmail is False, None, or missing => we treat as invalid
    return True


# Patterns for "safe to delete" - obvious placeholders/junk, not real people
SAFE_TO_DELETE_EMAIL_PATTERNS = (
    "deleted.com",   # e.g. 7e48ebb7071d3c23@deleted.com
    "unassigned",    # e.g. unassignedc0e541a24b6a43bde814c900@gmail.com
)
SAFE_TO_DELETE_NAME_SUBSTRINGS = (
    "deleted user",  # "Deleted User User" or "Deleted User"
)


def is_safe_to_delete(contact: dict) -> bool:
    """True only for contacts we're confident are safe to delete (junk/placeholders).
    Examples: @deleted.com, unassigned...@gmail.com, name 'Deleted User'.
    Never delete contacts that have no email but have a phone number (keep them)."""
    email = (contact.get("email") or "").strip()
    phone = (contact.get("phone") or "").strip()
    if not email and phone:
        return False  # blank email but has phone -> do not delete
    email_lower = email.lower()
    first = (contact.get("firstName") or "").strip()
    last = (contact.get("lastName") or "").strip()
    full_name = f"{first} {last}".strip().lower()

    for pat in SAFE_TO_DELETE_EMAIL_PATTERNS:
        if pat in email_lower:
            return True
    for sub in SAFE_TO_DELETE_NAME_SUBSTRINGS:
        if sub in full_name:
            return True
    return False


def extract_row(c: dict) -> dict:
    return {
        "id": c.get("id") or "",
        "email": c.get("email") or "",
        "validEmail": "true" if c.get("validEmail") is True else "false",
        "firstName": c.get("firstName") or "",
        "lastName": c.get("lastName") or "",
        "phone": c.get("phone") or "",
        "companyName": c.get("companyName") or "",
        "dateAdded": c.get("dateAdded") or "",
        "source": c.get("source") or "",
    }


def _write_csv(all_contacts: list[dict], invalid_only: bool, out_path: Path) -> int:
    """Write or update CSV with current contacts. Returns number of rows written."""
    if invalid_only:
        subset = [c for c in all_contacts if is_invalid_email(c)]
        rows = [extract_row(c) for c in subset]
    else:
        rows = [extract_row(c) for c in all_contacts]
    if not rows:
        return 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def _resume_file_path(out_path: Path) -> Path:
    """Resume state file next to the CSV: e.g. ghl_invalid_emails.csv -> ghl_invalid_emails_resume.json"""
    return out_path.with_suffix(out_path.suffix + "_resume.json")


def _write_resume(out_path: Path, last_contact: dict, last_page: int, total_fetched: int) -> None:
    """Save resume state so we can try to continue later from last contact id."""
    path = _resume_file_path(out_path)
    data = {
        "last_contact_id": last_contact.get("id") or "",
        "last_dateAdded": last_contact.get("dateAdded") or "",
        "last_page": last_page,
        "total_fetched": total_fetched,
        "output_path": str(out_path),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"  Resume state saved to {path} (use --resume-from {path} or --resume-from {out_path} to continue)")


def _read_resume_or_csv(path: Path) -> tuple[str, str, list[dict], Path]:
    """Read from resume JSON or CSV. Returns (last_contact_id, last_dateAdded, existing_rows, out_path). existing_rows are dicts with 'id' for dedupe."""
    path = path.resolve()
    if path.suffix.lower() == ".json" and "_resume" in path.name:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        out_path = Path(data.get("output_path", path.parent / "ghl_contacts.csv"))
        last_id = (data.get("last_contact_id") or "").strip()
        last_date = (data.get("last_dateAdded") or "").strip()
        existing = []
        if out_path.exists():
            with open(out_path, encoding="utf-8") as f:
                r = csv.DictReader(f)
                existing = list(r)
        return last_id, last_date, existing, out_path
    # CSV: read last row and all rows
    if not path.exists():
        raise FileNotFoundError(f"Resume path not found: {path}")
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
    if not rows:
        raise ValueError("CSV has no data rows; nothing to resume from.")
    last = rows[-1]
    last_id = (last.get("id") or "").strip()
    last_date = (last.get("dateAdded") or "").strip()
    return last_id, last_date, rows, path


def _run_resume(bearer: str, location_id: str, invalid_only: bool, resume_path: Path, out_path: Path | None) -> int:
    """Try to fetch next batch using last contact id (cursor). Append to CSV if API supports it."""
    last_id, last_date, existing_rows, resolved_out = _read_resume_or_csv(resume_path)
    if out_path is not None:
        resolved_out = out_path.resolve()
    if not last_id:
        print("No last_contact_id in resume data; cannot continue.")
        return 1
    existing_ids = {r.get("id") or "" for r in existing_rows}
    print(f"Resume: last id={last_id[:20]}..., existing rows={len(existing_rows)}. Trying cursor (startAfterId)...")

    # Try cursor-based fetch (page=1 with startAfterId)
    try:
        data = search_contacts(bearer, location_id, page=1, after_id=last_id)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code in (400, 422):
            print("GHL API rejected startAfterId (cursor). It does not support resume beyond 10,000 contacts.")
            print("Your CSV already has the first 10,000. To get more: use GHL dashboard bulk export, or export by tag/date.")
            return 0
        raise
    contacts = data.get("contacts") or []
    if not contacts:
        print("Cursor returned 0 contacts; you may already have all available.")
        return 0

    all_new: list[dict] = []
    cursor_id: str | None = last_id
    batch_num = 0
    while True:
        batch_num += 1
        data = search_contacts(bearer, location_id, page=1, after_id=cursor_id)
        batch = data.get("contacts") or []
        if not batch:
            break
        new_in_batch = [c for c in batch if (c.get("id") or "") not in existing_ids]
        all_new.extend(new_in_batch)
        for c in new_in_batch:
            if c.get("id"):
                existing_ids.add(c.get("id"))
        print(f"  Resume batch {batch_num}: +{len(new_in_batch)} new (total new this run: {len(all_new)})")
        if len(batch) < PAGE_LIMIT:
            break
        cursor_id = (batch[-1].get("id") or "").strip()
        if not cursor_id:
            break
        time.sleep(REQUEST_DELAY_SEC)
        if batch_num >= 100:
            print("  Reached 100 batches in resume run; stopping. Run --resume-from again to get more.")
            break

    if not all_new:
        print("No new contacts to add.")
        return 0

    # Convert existing_rows back to contact-like dicts for _write_csv, or append rows
    # We have existing_rows as CSV row dicts; we need to merge with all_new contacts then write
    def row_to_contact(r: dict) -> dict:
        return {
            "id": r.get("id"),
            "email": r.get("email"),
            "validEmail": r.get("validEmail") == "true",
            "firstName": r.get("firstName"),
            "lastName": r.get("lastName"),
            "phone": r.get("phone"),
            "companyName": r.get("companyName"),
            "dateAdded": r.get("dateAdded"),
            "source": r.get("source"),
        }
    existing_contacts = [row_to_contact(r) for r in existing_rows]
    merged = existing_contacts + all_new
    n = _write_csv(merged, invalid_only, resolved_out)
    last_contact = all_new[-1] if all_new else existing_contacts[-1]
    _write_resume(resolved_out, last_contact, batch_num, len(merged))
    print(f"Appended {len(all_new)} new contacts. Wrote {n} rows to {resolved_out}")
    return 0


def _fetch_batch(
    bearer: str,
    location_id: str,
    max_contacts: int | None = None,
) -> tuple[list[dict], bool]:
    """Fetch up to 10k contacts (pages 1-100). Returns (all_contacts, hit_10k_cap). hit_10k_cap True when we got 10k then 400."""
    all_contacts: list[dict] = []
    page = 1
    total_available = None
    hit_10k_cap = False

    while True:
        try:
            data = search_contacts(bearer, location_id, page=page)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and all_contacts:
                hit_10k_cap = len(all_contacts) >= CHECKPOINT_EVERY
                break
            raise
        contacts = data.get("contacts") or []
        if total_available is None:
            total_available = data.get("total")
        if not contacts and page == 1:
            break
        all_contacts.extend(contacts)
        print(f"Page {page}: fetched {len(contacts)} contacts (total so far: {len(all_contacts)}" + (f", API total: {total_available}" if total_available is not None and page == 1 else "") + ")")
        if len(contacts) < PAGE_LIMIT:
            break
        if max_contacts is not None and len(all_contacts) >= max_contacts:
            break
        page += 1
        time.sleep(REQUEST_DELAY_SEC)

    return all_contacts, hit_10k_cap


def run_export_then_delete(
    bearer: str,
    location_id: str,
    invalid_only: bool,
    out_path: Path,
    dry_run: bool = False,
    delete_only_safe: bool = True,
    verify_before_delete: bool = True,
) -> int:
    """Loop: fetch 10k -> write batch to CSV -> verify (if enabled) -> delete from GHL -> repeat.
    If dry_run True, only export to CSV and report what would be deleted; do not call delete API.
    If delete_only_safe True (default), only delete contacts that match safe patterns.
    If verify_before_delete True (default), call GHL verify API per contact; only delete if API says invalid."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print("DRY RUN: will export to CSV but NOT delete from GHL.\n")
    elif verify_before_delete:
        print("VERIFY then DELETE: calling GHL verify API per contact; only deleting if verified invalid.\n")
    elif delete_only_safe:
        print("SAFE-DELETE: only deleting obvious junk (@deleted.com, unassigned..., 'Deleted User'). Rest exported to CSV only.\n")
    all_rows_written = 0
    total_deleted = 0
    total_failed = 0
    round_num = 0
    file_exists = False
    fieldnames = list(extract_row({}).keys())  # id, email, validEmail, ...
    current_csv_path = out_path

    while True:
        round_num += 1
        print(f"\n--- Round {round_num}: fetch up to 10k contacts ---")
        all_contacts, hit_cap = _fetch_batch(bearer, location_id)
        if not all_contacts:
            print("No contacts returned; done.")
            break

        batch_to_export = [c for c in all_contacts if is_invalid_email(c)] if invalid_only else all_contacts
        if not batch_to_export:
            print("No contacts to export this round (none invalid).")
            if not hit_cap:
                break
            continue

        # Append batch to CSV (retry on permission error, then fallback to timestamped file)
        rows = [extract_row(c) for c in batch_to_export]
        current_csv_path, file_exists, used_fallback = _append_batch_to_csv(current_csv_path, fieldnames, rows, file_exists)
        if used_fallback:
            out_path = current_csv_path  # use fallback for rest of run
        all_rows_written += len(batch_to_export)
        print(f"  Wrote {len(batch_to_export)} rows to {current_csv_path} (total in CSV: {all_rows_written})")

        if delete_only_safe:
            safe_batch = [c for c in batch_to_export if is_safe_to_delete(c)]
            candidates_to_delete = [(c.get("id"), c.get("email") or "") for c in safe_batch if (c.get("id") or "").strip()]
            skipped = len(batch_to_export) - len(safe_batch)
            if skipped:
                print(f"  Of {len(batch_to_export)} invalid, {len(safe_batch)} are 'safe to delete'; {skipped} exported only (not deleted).")
            elif len(batch_to_export) > 0:
                print(f"  All {len(batch_to_export)} in this batch are 'safe to delete'; will verify then delete from GHL.")
        else:
            candidates_to_delete = [(c.get("id"), c.get("email") or "") for c in batch_to_export if (c.get("id") or "").strip()]

        if verify_before_delete and not dry_run and candidates_to_delete:
            verified_invalid = []
            for cid, email in candidates_to_delete:
                result = verify_contact_email(bearer, location_id, cid, email)
                if result == "invalid":
                    verified_invalid.append(cid)
                time.sleep(VERIFY_DELAY_SEC)
            ids_to_delete = verified_invalid
            if len(candidates_to_delete) > 0:
                print(f"  Verify: {len(verified_invalid)} confirmed invalid (will delete), {len(candidates_to_delete) - len(verified_invalid)} valid/skip (kept).")
        else:
            ids_to_delete = [cid for cid, _ in candidates_to_delete]

        if dry_run:
            print(f"  [DRY RUN] Would delete {len(ids_to_delete)} contacts from GHL (skipped).")
            total_deleted += len(ids_to_delete)
        else:
            if ids_to_delete:
                print(f"  Deleting {len(ids_to_delete)} contacts from GHL...")
                deleted, failed = delete_contacts_from_ghl(bearer, ids_to_delete)
                total_deleted += deleted
                total_failed += failed
                print(f"  Deleted {deleted} from GHL ({failed} failed). Running total: {total_deleted} deleted, {total_failed} failed.")
            else:
                print("  No contacts to delete this round (none matched safe-to-delete).")

        if not hit_cap:
            print("  No more pages (API cap or end of list); stopping.")
            break
        print("  Hit 10k cap; starting next round...")

    if dry_run:
        print(f"\nDone (dry run). Total exported to CSV: {all_rows_written}. Would have deleted from GHL: {total_deleted}.")
    else:
        print(f"\nDone. Total exported to CSV: {all_rows_written}. Total deleted from GHL: {total_deleted} ({total_failed} failed).")
    return 0 if total_failed == 0 else 1


def run(
    bearer: str,
    location_id: str,
    invalid_only: bool,
    out_path: Path,
    max_contacts: int | None = None,
) -> int:
    all_contacts, hit_cap = _fetch_batch(bearer, location_id, max_contacts=max_contacts)

    if not all_contacts:
        print("No rows to write.")
        return 0

    if hit_cap:
        _write_resume(out_path, all_contacts[-1], 100, len(all_contacts))
        print("  GHL API only allows pages 1-100 (10,000 contacts). Use --resume-from <csv or _resume.json> to try to fetch more.")

    n = _write_csv(all_contacts, invalid_only, out_path)
    if invalid_only:
        print(f"Contacts with invalid email (GHL still charges): {len([c for c in all_contacts if is_invalid_email(c)])} of {len(all_contacts)}")
    else:
        print(f"Total contacts: {len(all_contacts)} (invalid email: {sum(1 for c in all_contacts if is_invalid_email(c))})")
    print(f"Wrote {n} rows to {out_path}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Export GHL contacts to CSV (all or invalid-email only).")
    ap.add_argument("--bearer", default=os.environ.get("GHL_BEARER_TOKEN", "").strip(), help="Private Integration Bearer token")
    ap.add_argument("--location-id", default=os.environ.get("GHL_LOCATION_ID", "GmTfoYNHLeHumMfDIset").strip(), help="GHL location ID")
    ap.add_argument("--invalid-only", action="store_true", help="Only export contacts with invalid email (validEmail not true)")
    ap.add_argument("-o", "--output", type=Path, default=None, help="Output CSV path (default: kit/ghl_contacts.csv or kit/ghl_invalid_emails.csv)")
    ap.add_argument("--max-contacts", type=int, default=None, help="Stop after this many contacts (e.g. 100 for a quick sample)")
    ap.add_argument("--resume-from", type=Path, default=None, metavar="CSV_OR_JSON", help="Continue from last contact: pass CSV or ghl_*_resume.json from a previous run")
    ap.add_argument("--fast", action="store_true", help="Use shorter delays (for testing); may hit rate limits")
    ap.add_argument("--export-and-delete-invalid", action="store_true", help="Loop: fetch 10k invalid -> append CSV -> delete from GHL -> repeat until done")
    ap.add_argument("--dry-run", action="store_true", help="With --export-and-delete-invalid: export to CSV only, do NOT delete from GHL")
    ap.add_argument("--delete-all-invalid", action="store_true", help="With --export-and-delete-invalid: delete every invalid contact (default: only 'safe' ones)")
    ap.add_argument("--no-verify-before-delete", action="store_true", help="With --export-and-delete-invalid: skip GHL verify API; delete without verifying (faster, no verify cost)")
    args = ap.parse_args()

    if args.fast:
        global REQUEST_DELAY_SEC, RETRY_BACKOFF_SEC
        REQUEST_DELAY_SEC = REQUEST_DELAY_FAST
        RETRY_BACKOFF_SEC = RETRY_BACKOFF_FAST

    bearer = (args.bearer or "").strip()
    location_id = (args.location_id or "").strip()
    if not bearer:
        print("ERROR: Set GHL_BEARER_TOKEN or pass --bearer", file=sys.stderr)
        return 1
    if not location_id:
        print("ERROR: Set GHL_LOCATION_ID or pass --location-id", file=sys.stderr)
        return 1

    out_path = args.output
    if out_path is None:
        out_path = KIT_DIR / ("ghl_invalid_emails.csv" if args.invalid_only else "ghl_contacts.csv")

    if args.resume_from is not None:
        return _run_resume(bearer, location_id, args.invalid_only, args.resume_from, out_path)

    if args.export_and_delete_invalid:
        if not args.invalid_only:
            print("ERROR: --export-and-delete-invalid requires --invalid-only (we only delete invalid-email contacts).", file=sys.stderr)
            return 1
        return run_export_then_delete(
            bearer, location_id, True, out_path,
            dry_run=args.dry_run,
            delete_only_safe=not args.delete_all_invalid,
            verify_before_delete=not args.no_verify_before_delete,
        )

    return run(bearer, location_id, args.invalid_only, out_path, max_contacts=args.max_contacts)


if __name__ == "__main__":
    sys.exit(main())
