"""
Test script: New Paying = memberships whose FIRST successful payment was in the period (from $0 to >$1).

Includes both: new joiners who paid in period, AND old members who had $0 before and first paid in period.
"""
import asyncio
import json
import sys
from datetime import datetime, date, timezone
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


def _parse_dt(ts):
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            t = float(ts)
            if t > 1e11:
                t /= 1000
            return datetime.fromtimestamp(t, tz=timezone.utc)
        s = str(ts).strip()
        if not s:
            return None
        if "T" in s or "-" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except Exception:
        return None


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


def _is_lite_product(pid: str, lite_ids: list) -> bool:
    return pid in lite_ids


def _payment_membership_id(p: dict) -> str:
    """Extract membership id from payment object."""
    v = p.get("membership_id") or p.get("membership") or ""
    if isinstance(v, dict):
        return str(v.get("id") or v.get("membership_id") or "").strip()
    return str(v or "").strip()


async def main():
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

    client = WhopAPIClient(api_key, str(wh.get("base_url") or "https://api.whop.com/api/v1"), company_id)

    prod_ids_cfg = whop_cfg.get("metrics_report_product_ids")
    full_ids = [str(x).strip() for x in ((prod_ids_cfg or {}).get("full") or []) if str(x or "").strip().startswith("prod_")]
    lite_ids = [str(x).strip() for x in ((prod_ids_cfg or {}).get("lite") or []) if str(x or "").strip().startswith("prod_")]
    all_product_ids = full_ids + lite_ids

    paid_statuses = {"paid", "succeeded", "success"}

    # 0. Fetch payments BEFORE period -> users who had any prior payment (exclude from New Paying)
    payments_before_period = []
    after = None
    params_before = {"created_before": start_utc_iso, "order": "created_at", "direction": "desc"}
    for _ in range(max_pages):
        batch, pi = await client.list_payments(first=per_page, after=after, params=params_before)
        if not batch:
            break
        payments_before_period.extend(batch)
        after = str(pi.get("end_cursor") or "")
        if not pi.get("has_next_page") or not after:
            break
    user_ids_with_prior_payment = set()
    for p in payments_before_period:
        st = str(p.get("status") or p.get("substatus") or "").strip().lower()
        if st not in paid_statuses and "succeed" not in st:
            continue
        if float(p.get("usd_total") or p.get("total") or p.get("amount_after_fees") or 0) < min_spent:
            continue
        u = p.get("user") or p.get("member")
        uid = (u.get("id") if isinstance(u, dict) else str(u or "").strip()) if u else ""
        if uid:
            user_ids_with_prior_payment.add(str(uid).strip())
    print("--- DEBUG: prior payments ---")
    print("  Payments before period: {} | Users with prior payment: {}".format(
        len(payments_before_period), len(user_ids_with_prior_payment)))
    print()

    # 1. Fetch payments in period for our products
    payments_in_period = []
    after = None
    params = {
        "created_after": start_utc_iso,
        "created_before": end_utc_iso,
        "order": "created_at",
        "direction": "asc",
    }
    for _ in range(max_pages):
        batch, pi = await client.list_payments(first=per_page, after=after, params=params)
        if not batch:
            break
        payments_in_period.extend(batch)
        after = str(pi.get("end_cursor") or "")
        if not pi.get("has_next_page") or not after:
            break

    # DEBUG: raw payment stats
    print("--- DEBUG: list_payments ---")
    print("  Date range: {} to {}".format(start_utc_iso, end_utc_iso))
    print("  Total payments in period: {}".format(len(payments_in_period)))
    if payments_in_period:
        p0 = payments_in_period[0]
        print("  Sample payment keys: {}".format(list(p0.keys())))
        for j, px in enumerate(payments_in_period[:5]):
            st = px.get("status") or px.get("substatus")
            amt = px.get("usd_total") or px.get("total") or px.get("amount_after_fees")
            mid = (px.get("membership") or {}).get("id") or (px.get("membership") if isinstance(px.get("membership"), str) else "?")
            pid = (px.get("product") or {}).get("id") or (px.get("product") if isinstance(px.get("product"), str) else "?")
            created = px.get("created_at") or px.get("paid_at")
            print("    [{}] status={} amount={} membership={} product={} created={}".format(j, st, amt, mid, pid, created))
    else:
        print("  (no payments returned)")
    print()

    # 2. Filter to successful (paid) and amount > min_spent; collect unique (membership_id, product_id, user_id)
    #    Also build mid -> user_id for dashboard URLs
    to_check = {}  # membership_id -> (product_id, user_id)
    mid_to_user_id = {}  # membership_id -> user_id (for dashboard links)
    for p in payments_in_period:
        st = str(p.get("status") or p.get("substatus") or "").strip().lower()
        if st not in paid_statuses and "succeed" not in st:
            continue
        total = float(p.get("usd_total") or p.get("total") or p.get("amount_after_fees") or 0)
        if total < min_spent:
            continue
        mid = str((p.get("membership") or {}).get("id") or "").strip()
        pid = str((p.get("product") or {}).get("id") or "").strip()
        u = p.get("user") or p.get("member")
        uid = (u.get("id") if isinstance(u, dict) else str(u or "").strip()) if u else ""
        if mid and pid and (not all_product_ids or pid in all_product_ids):
            to_check[mid] = (pid, str(uid).strip() if uid else "")
            if mid and uid and mid not in mid_to_user_id:
                mid_to_user_id[mid] = str(uid).strip()

    print("--- DEBUG: after filters ---")
    print("  Product IDs we care about: {}".format(all_product_ids))
    print("  Unique (membership, product) to check: {}".format(len(to_check)))
    if to_check:
        sample = list(to_check.items())[:5]
        print("  Sample memberships to check: {}".format([(m, (p, u)) for m, (p, u) in sample]))
    print()

    # 3. For each membership: get full payment history, find first successful payment
    #    If first payment date is in period -> new paying (went from $0 to >$1 in period)
    max_check = 100
    new_paying_full = set()
    new_paying_lite = set()
    debug_first_few = []
    in_period_count = 0
    out_of_period_count = 0
    for i, (mid, item) in enumerate(to_check.items()):
        if i >= max_check:
            break
        product_id, user_id = item
        # Exclude users who had any payment before the period (true New Paying = first payment ever)
        if user_id and user_id in user_ids_with_prior_payment:
            continue
        pays = await client.get_payments_for_membership(mid)
        if not pays:
            continue
        # Filter to this membership (API may return company-wide; filter client-side)
        pays_for_mid = [p for p in (pays if isinstance(pays, list) else []) if isinstance(p, dict) and (_payment_membership_id(p) == mid or not _payment_membership_id(p))]
        if not pays_for_mid:
            pays_for_mid = [p for p in (pays if isinstance(pays, list) else []) if isinstance(p, dict)]
        # Find first successful payment for THIS membership (chronologically)
        success_pays = []
        for p in pays_for_mid:
            st = str(p.get("status") or p.get("substatus") or "").strip().lower()
            if st not in paid_statuses and "succeed" not in st:
                continue
            total = float(p.get("usd_total") or p.get("total") or p.get("amount_after_fees") or 0)
            if total < min_spent:
                continue
            ts = p.get("paid_at") or p.get("created_at")
            dt = _parse_dt(ts)
            if dt:
                success_pays.append((dt, p))
        if not success_pays:
            continue
        first_dt = min(dt for dt, _ in success_pays)
        in_period = start_utc <= first_dt <= end_utc
        if in_period:
            in_period_count += 1
        else:
            out_of_period_count += 1
        if len(debug_first_few) < 5:
            all_dates = sorted(dt.isoformat() for dt, _ in success_pays)
            debug_first_few.append({"mid": mid, "product_id": product_id, "first_dt": first_dt.isoformat(), "in_period": in_period, "all_success_dates": all_dates[:5]})
        if in_period:
            # Count by user_id (one user = one New Paying person; fallback to mid if no user_id)
            uid = user_id or mid
            if _is_lite_product(product_id, lite_ids):
                new_paying_lite.add(uid)
            else:
                new_paying_full.add(uid)
        await asyncio.sleep(0.1)

    print("--- DEBUG: first-payment checks ---")
    print("  Checked {} memberships: {} with first payment IN period, {} OUT of period".format(
        in_period_count + out_of_period_count, in_period_count, out_of_period_count))
    for d in debug_first_few:
        dates_str = ",".join(d.get("all_success_dates", [])[:3])
        print("  mid={} first={} in_period={} [success_dates: {}]".format(d["mid"], d["first_dt"], d["in_period"], dates_str))
    if not debug_first_few and to_check:
        print("  (no memberships checked - to_check empty or no success_pays)")
    print()

    print("=== New Paying API Test: First payment in period (from $0 to >${}) ===".format(int(min_spent)))
    print("Logic: users who had NO prior payment and whose FIRST successful payment (>{}) was in {}\n".format(min_spent, f"{start_d} to {end_d}"))
    print("Reselling Secrets LITE")
    print("  New Paying: {}\n".format(len(new_paying_lite)))
    print("Reselling Secrets FULL")
    print("  New Paying: {}\n".format(len(new_paying_full)))
    print("Total New Paying: {}".format(len(new_paying_full) + len(new_paying_lite)))

    # Dashboard URLs for new paying members (all_new contains user_ids or mids)
    base = "https://whop.com/dashboard/{}/users/".format(company_id)
    all_new = new_paying_full | new_paying_lite
    urls = []
    for uid_or_mid in sorted(all_new):
        # If it's a user_id (starts with user_), use as-is; else look up from mid
        if str(uid_or_mid).startswith("user_"):
            urls.append(base + uid_or_mid + "/")
        else:
            uid = mid_to_user_id.get(uid_or_mid)
            urls.append((base + uid + "/") if uid else "(no user_id: {})".format(uid_or_mid))
    print("\n--- Whop Dashboard URLs ({} members) ---".format(len(all_new)))
    for u in urls:
        print(u)


if __name__ == "__main__":
    asyncio.run(main())
