"""
Test Whop API: memberships count vs members count for Feb 2-9, 2026.

Compares:
- list_memberships(created_after, created_before) -> total memberships (should match dashboard)
- list_members filtered by joined_at -> unique members (what the bot currently reports)

Run from repo root: python scripts/test_whop_api_members_vs_memberships.py
"""
import asyncio
import json
import sys
from datetime import datetime, date, time, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RSC_DIR = REPO_ROOT / "RSCheckerbot"
if str(RSC_DIR) not in sys.path:
    sys.path.insert(0, str(RSC_DIR))

from whop_api_client import WhopAPIClient
from rschecker_utils import parse_dt_any


def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config() -> dict:
    cfg_path = RSC_DIR / "config.json"
    secrets_path = RSC_DIR / "config.secrets.json"
    cfg = {}
    if cfg_path.exists():
        with open(cfg_path) as f:
            cfg = json.load(f) or {}
    if secrets_path.exists():
        with open(secrets_path) as f:
            secrets = json.load(f) or {}
        cfg = _deep_merge(cfg, secrets)
    return cfg


async def main():
    cfg = load_config()
    wh = cfg.get("whop_api") or {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()

    if not api_key or not company_id:
        print("ERROR: Missing whop_api.api_key or whop_api.company_id in config.secrets.json")
        return 1

    client = WhopAPIClient(api_key, base_url, company_id)

    # Feb 2-9, 2026 (UTC)
    start_d = date(2026, 2, 2)
    end_d = date(2026, 2, 9)
    start_utc = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc)
    start_utc_iso = start_utc.isoformat().replace("+00:00", "Z")
    end_utc_iso = end_utc.isoformat().replace("+00:00", "Z")

    print("=== Whop API: Memberships vs Members (Feb 2-9, 2026) ===\n")
    print(f"Range: {start_d} to {end_d} (UTC)\n")

    # 1. list_memberships with created_after/created_before (matches dashboard "Memberships" filter)
    memberships_count = 0
    memberships_by_product: dict[str, int] = {}
    after = None
    per_page = 100
    max_pages = 50

    print("1. list_memberships(created_after, created_before) ...")
    while max_pages > 0:
        batch, page_info = await client.list_memberships(
            first=per_page,
            after=after,
            params={
                "created_after": start_utc_iso,
                "created_before": end_utc_iso,
                "order": "created_at",
                "direction": "asc",
            },
        )
        if not batch:
            break
        memberships_count += len(batch)
        for rec in batch:
            prod = (rec.get("product") or {})
            if isinstance(prod, dict):
                title = str(prod.get("title") or "").strip() or "Unknown"
            else:
                title = "Unknown"
            memberships_by_product[title] = memberships_by_product.get(title, 0) + 1

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break
        max_pages -= 1

    print(f"   Total memberships: {memberships_count}")
    for p, n in sorted(memberships_by_product.items(), key=lambda x: -x[1]):
        print(f"   - {p}: {n}")
    print()

    # 2. list_members ordered by joined_at desc, filter by joined_at in range
    members_seen: set[str] = set()
    after = None
    max_pages = 50
    stop = False

    print("2. list_members(order=joined_at desc) filtered by joined_at in range ...")
    while max_pages > 0 and not stop:
        batch, page_info = await client.list_members(
            first=per_page,
            after=after,
            params={"order": "joined_at", "direction": "desc"},
        )
        if not batch:
            break
        for rec in batch:
            mber_id = str(rec.get("id") or "").strip()
            joined_at_raw = str(rec.get("joined_at") or rec.get("created_at") or "").strip()
            dtj = parse_dt_any(joined_at_raw) if joined_at_raw else None
            if not isinstance(dtj, datetime):
                continue
            joined_d = dtj.astimezone(timezone.utc).date()
            if joined_d < start_d:
                stop = True
                break
            if joined_d > end_d:
                continue
            members_seen.add(mber_id)

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break
        max_pages -= 1

    print(f"   Unique members: {len(members_seen)}")
    print()

    print("--- Summary ---")
    print(f"  Memberships (dashboard-style): {memberships_count}")
    print(f"  Unique members (bot-style):    {len(members_seen)}")
    print(f"  Difference:                   {memberships_count - len(members_seen)}")
    if memberships_count >= 60:
        print("\n  [OK] API returns 60+ memberships (matches Whop dashboard).")
    else:
        print(f"\n  [WARN] API returned {memberships_count} memberships (dashboard shows 60).")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
