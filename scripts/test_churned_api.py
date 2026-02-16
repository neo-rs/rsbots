"""
Test script for Churned logic.

Mode 1 (default): list_members with most_recent_actions=[churned,left], usd_total_spent>0
  - Matches Whop dashboard, gave ~18 (Cho reported 14)

Mode 2 (--payments): Paid last calendar month, NOT this calendar month
  - Cho's literal "paid last month, not this month" - but gave 162 (too high)
"""
import asyncio
import json
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore

REPO_ROOT = Path(__file__).resolve().parents[1]
RSC_DIR = REPO_ROOT / "RSCheckerbot"
if str(RSC_DIR) not in sys.path:
    sys.path.insert(0, str(RSC_DIR))

from whop_api_client import WhopAPIClient


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
    use_payments = "--payments" in sys.argv
    cfg = load_config()
    wh = cfg.get("whop_api") or {}
    api_key = str(wh.get("api_key") or "").strip()
    company_id = str(wh.get("company_id") or "").strip()
    tz_name = str((cfg.get("reporting") or {}).get("timezone") or "America/New_York").strip()
    whop_cfg = cfg.get("whop_api") or {}

    start_d = date(2026, 2, 8)
    end_d = date(2026, 2, 14)
    min_spent = 1.0
    max_pages = 50
    per_page = 100

    if ZoneInfo:
        tz = ZoneInfo(tz_name)
        start_local = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=tz)
        end_local = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=tz)
    else:
        start_local = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc)
        end_local = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    start_utc_iso = start_utc.isoformat().replace("+00:00", "Z")
    end_utc_iso = end_utc.isoformat().replace("+00:00", "Z")

    # Calendar months: last month = Jan 1-31, this month = Feb 1 - report end
    curr_month_start = start_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = curr_month_start - timedelta(seconds=1)
    prev_month_start = prev_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_start_iso = prev_month_start.isoformat().replace("+00:00", "Z")
    prev_end_iso = prev_month_end.isoformat().replace("+00:00", "Z")
    curr_start_iso = curr_month_start.isoformat().replace("+00:00", "Z")

    client = WhopAPIClient(api_key, str(wh.get("base_url") or "https://api.whop.com/api/v1"), company_id)

    prod_ids_cfg = whop_cfg.get("metrics_report_product_ids")
    full_ids = [str(x).strip() for x in ((prod_ids_cfg or {}).get("full") or []) if str(x or "").strip().startswith("prod_")]
    lite_ids = [str(x).strip() for x in ((prod_ids_cfg or {}).get("lite") or []) if str(x or "").strip().startswith("prod_")]

    # Mode 1: list_members (matches Whop, ~18)
    if not use_payments:
        seen = set()
        params = {"order": "most_recent_action", "direction": "desc"}
        if full_ids:
            params["product_ids"] = full_ids
        for action in ("churned", "left"):
            p = dict(params)
            p["most_recent_actions[]"] = action
            after = None
            for _ in range(max_pages):
                batch, pi = await client.list_members(first=per_page, after=after, params=p)
                if not batch:
                    break
                for m in batch:
                    if isinstance(m, dict) and float(m.get("usd_total_spent") or 0) > 0:
                        mid = str(m.get("id") or "").strip()
                        if mid:
                            seen.add(mid)
                after = str(pi.get("end_cursor") or "")
                if not pi.get("has_next_page") or not after:
                    break
        print("=== Churned (list_members mode) ===")
        print("Logic: most_recent_action in [churned, left], usd_total_spent > 0\n")
        print("Reselling Secrets FULL - Churned: {}".format(len(seen)))
        return

    # Mode 2: payments-based
    all_pids = full_ids + lite_ids

    paid_st = {"paid", "succeeded", "success"}

    def _users_from_payments(payments, pids_f, pids_l):
        full_u, lite_u = set(), set()
        for p in payments:
            st = str(p.get("status") or p.get("substatus") or "").strip().lower()
            if st not in paid_st and "succeed" not in st:
                continue
            if float(p.get("usd_total") or p.get("total") or p.get("amount_after_fees") or 0) < min_spent:
                continue
            pid = str((p.get("product") or {}).get("id") or "").strip()
            if all_pids and pid not in all_pids:
                continue
            u = p.get("user") or p.get("member")
            uid = (u.get("id") if isinstance(u, dict) else str(u or "").strip()) if u else ""
            if not uid:
                continue
            uid = str(uid).strip()
            if pid in pids_l:
                lite_u.add(uid)
            elif pid in pids_f:
                full_u.add(uid)
        return (full_u, lite_u)

    # Previous period
    prev_payments = []
    after = None
    params_prev = {"created_after": prev_start_iso, "created_before": prev_end_iso, "order": "created_at", "direction": "asc"}
    for _ in range(max_pages):
        batch, pi = await client.list_payments(first=per_page, after=after, params=params_prev)
        if not batch:
            break
        prev_payments.extend(batch)
        after = str(pi.get("end_cursor") or "")
        if not pi.get("has_next_page") or not after:
            break

    # Report period
    curr_payments = []
    after = None
    params_curr = {"created_after": curr_start_iso, "created_before": end_utc_iso, "order": "created_at", "direction": "asc"}
    for _ in range(max_pages):
        batch, pi = await client.list_payments(first=per_page, after=after, params=params_curr)
        if not batch:
            break
        curr_payments.extend(batch)
        after = str(pi.get("end_cursor") or "")
        if not pi.get("has_next_page") or not after:
            break

    prev_full, prev_lite = _users_from_payments(prev_payments, full_ids, lite_ids)
    curr_full, curr_lite = _users_from_payments(curr_payments, full_ids, lite_ids)

    churned_full = prev_full - curr_full
    churned_lite = prev_lite - curr_lite

    print("=== Churned API Test: Paid in previous period, NOT in report period ===")
    print("Logic: users who paid in prev period but did NOT pay in report period\n")
    print("Report period: {} to {}".format(start_d, end_d))
    print("Last month (prev): {} to {}".format(prev_month_start.date(), prev_month_end.date()))
    print("This month (curr): {} to {} (report end)\n".format(curr_month_start.date(), end_utc.date()))
    print("--- DEBUG ---")
    print("  Prev period payments: {} | Unique FULL users: {} | Unique LITE users: {}".format(
        len(prev_payments), len(prev_full), len(prev_lite)))
    print("  Report period payments: {} | Unique FULL users: {} | Unique LITE users: {}".format(
        len(curr_payments), len(curr_full), len(curr_lite)))
    print()
    print("Reselling Secrets LITE")
    print("  Churned: {}".format(len(churned_lite)))
    print()
    print("Reselling Secrets FULL")
    print("  Churned: {}".format(len(churned_full)))
    print()
    print("Total Churned (FULL+LITE, deduped by product): FULL={}, LITE={}".format(len(churned_full), len(churned_lite)))

    base = "https://whop.com/dashboard/{}/users/".format(company_id)
    print("\n--- Sample Churned FULL user dashboard URLs (first 5) ---")
    for uid in list(churned_full)[:5]:
        print(base + uid + "/")


if __name__ == "__main__":
    asyncio.run(main())
