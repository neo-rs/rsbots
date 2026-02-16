"""
Run METRICS report logic for Feb 2-9 (matching main.py run_whop_membership_report_for_user).
Prints the result without Discord/DM.
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
from rschecker_utils import usd_amount


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


def _norm_membership(rec: dict) -> dict:
    for key in ("membership", "data", "item", "record"):
        inner = rec.get(key)
        if isinstance(inner, dict) and any(k in inner for k in ("status", "created_at", "product")):
            return inner
    return rec


def _norm_bool(v) -> bool:
    if v is True:
        return True
    s = str(v or "").strip().lower()
    return s in {"true", "yes", "1"}


def _metrics_bucket(m: dict) -> str:
    st = str(m.get("status") or "").strip().lower()
    cape = m.get("cancel_at_period_end") is True or _norm_bool(m.get("cancel_at_period_end"))
    total_raw = m.get("total_spent") or m.get("total_spent_usd") or m.get("total_spend") or m.get("total_spend_usd")
    spent = float(usd_amount(total_raw))
    product_title = str((m.get("product") or {}).get("title") or "").lower() if isinstance(m.get("product"), dict) else ""
    is_lifetime = "lifetime" in product_title

    if st in {"canceled", "cancelled", "expired", "churned"} and spent > 0:
        return "churned"
    if st == "completed":
        return "completed"
    if cape and st == "active":
        return "canceling"
    if is_lifetime:
        return "other"
    if st == "active" and spent > 0:
        return "new_paying"
    if st == "trialing":
        return "new_trials"
    return "other"


def _is_lite(title: str) -> bool:
    low = str(title or "").strip().lower()
    return "lite" in low and "lifetime" not in low


def _membership_member_id(m: dict) -> str:
    mm = m.get("member")
    if isinstance(mm, str) and mm.strip().startswith("mber_"):
        return mm.strip()
    if isinstance(mm, dict):
        mid = str(mm.get("id") or mm.get("member_id") or "").strip()
        if mid.startswith("mber_"):
            return mid
    return str(m.get("member_id") or "").strip()


async def main():
    cfg = load_config()
    wh = cfg.get("whop_api") or {}
    api_key = str(wh.get("api_key") or "").strip()
    company_id = str(wh.get("company_id") or "").strip()
    tz_name = str((cfg.get("reporting") or {}).get("timezone") or "America/New_York").strip()
    whop_cfg = cfg.get("whop_api") or {}
    prefixes = [str(x).strip() for x in whop_cfg.get("joined_report_product_title_prefixes") or ["Reselling Secrets"]]
    if not prefixes:
        prefixes = ["Reselling Secrets"]

    start_d = date(2026, 2, 8)
    end_d = date(2026, 2, 14)
    max_pages = 50

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

    # Users-tab mode: list_members with product_ids (matches Whop Users tab: 26 FULL, 16 LITE)
    prod_ids_cfg = whop_cfg.get("metrics_report_product_ids")
    full_ids = [str(x).strip() for x in ((prod_ids_cfg or {}).get("full") or []) if str(x or "").strip().startswith("prod_")]
    lite_ids = [str(x).strip() for x in ((prod_ids_cfg or {}).get("lite") or []) if str(x or "").strip().startswith("prod_")]
    use_users_tab = bool(full_ids or lite_ids)

    def _member_action_bucket(m):
        action = str(m.get("most_recent_action") or "").strip().lower()
        spent = float(m.get("usd_total_spent") or 0)
        if action in {"churned", "left"}:
            return "churned" if spent > 0 else "other"
        if action == "canceling":
            return "canceling"
        if action in {"renewing", "paid_subscriber", "paid_once", "joined", "finished_split_pay"} and spent > 0:
            return "new_paying"
        if action == "trialing":
            return "new_trials"
        return "other"

    async def _fetch_canceling():
        out = []
        after = None
        for _ in range(max_pages):
            batch, pi = await client.list_memberships(
                first=100, after=after,
                params={
                    "statuses[]": "canceling",
                    "created_after": start_utc_iso,
                    "created_before": end_utc_iso,
                    "order": "created_at",
                    "direction": "desc",
                },
            )
            if not batch:
                break
            for rec in batch:
                m = _norm_membership(rec)
                pt = str((m.get("product") or {}).get("title") or "")
                if prefixes and not any(pt.lower().startswith(p.lower()) for p in prefixes):
                    continue
                out.append(m)
            after = str(pi.get("end_cursor") or "")
            if not pi.get("has_next_page") or not after:
                break
        full_n = sum(1 for m in out if not _is_lite(str((m.get("product") or {}).get("title") or "")))
        lite_n = len(out) - full_n
        return (full_n, lite_n)

    all_product_ids = full_ids + lite_ids

    def _spent_from_membership(m):
        raw = (
            m.get("total_spent") or m.get("total_spent_usd") or m.get("total_spend") or m.get("total_spend_usd")
            or (m.get("member") or {}).get("usd_total_spent") if isinstance(m.get("member"), dict) else None
            or (m.get("user") or {}).get("usd_total_spent") if isinstance(m.get("user"), dict) else None
        )
        return float(usd_amount(raw))

    async def _fetch_new_paying():
        out = []
        after = None
        params = {"statuses[]": "active", "order": "created_at", "direction": "desc"}
        if all_product_ids:
            params["product_ids"] = all_product_ids
        for _ in range(max_pages):
            batch, pi = await client.list_memberships(first=100, after=after, params=params)
            if not batch:
                break
            for rec in batch:
                m = _norm_membership(rec)
                if not all_product_ids:
                    pt = str((m.get("product") or {}).get("title") or "")
                    if prefixes and not any(pt.lower().startswith(p.lower()) for p in prefixes):
                        continue
                if _spent_from_membership(m) <= 0:
                    continue
                out.append(m)
            after = str(pi.get("end_cursor") or "")
            if not pi.get("has_next_page") or not after:
                break
        full_n = sum(1 for m in out if not _is_lite(str((m.get("product") or {}).get("title") or "")))
        return (full_n, len(out) - full_n)

    async def _fetch_churned():
        out = []
        base_params = {"order": "canceled_at", "direction": "desc"}
        if all_product_ids:
            base_params["product_ids"] = all_product_ids
        for st in ("canceled", "expired"):
            after = None
            params = dict(base_params)
            params["statuses[]"] = st
            for _ in range(max_pages):
                batch, pi = await client.list_memberships(first=100, after=after, params=params)
                if not batch:
                    break
                for rec in batch:
                    m = _norm_membership(rec)
                    if not all_product_ids:
                        pt = str((m.get("product") or {}).get("title") or "")
                        if prefixes and not any(pt.lower().startswith(p.lower()) for p in prefixes):
                            continue
                    if _spent_from_membership(m) <= 0:
                        continue
                    out.append(m)
                after = str(pi.get("end_cursor") or "")
                if not pi.get("has_next_page") or not after:
                    break
        seen = set()
        deduped = []
        for m in out:
            mid = str(m.get("id") or m.get("membership_id") or "").strip()
            if mid and mid not in seen:
                seen.add(mid)
                deduped.append(m)
        full_n = sum(1 for m in deduped if not _is_lite(str((m.get("product") or {}).get("title") or "")))
        return (full_n, len(deduped) - full_n)

    def _parse_dt_any(ts):
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

    async def _count_new_paying_via_payments(cli, start_iso, end_iso, start_dt, end_dt, full_ids, lite_ids, all_pids, min_spent, max_pg):
        paid_st = {"paid", "succeeded", "success"}
        # Fetch payments before period -> users who had prior payment (exclude from New Paying)
        payments_before = []
        after_b = None
        params_before = {"created_before": start_iso, "order": "created_at", "direction": "desc"}
        for _ in range(min(max_pg, 50)):
            batch, pi = await cli.list_payments(first=100, after=after_b, params=params_before)
            if not batch:
                break
            payments_before.extend(batch)
            after_b = str(pi.get("end_cursor") or "")
            if not pi.get("has_next_page") or not after_b:
                break
        user_ids_with_prior = set()
        for p in payments_before:
            st = str(p.get("status") or p.get("substatus") or "").strip().lower()
            if st not in paid_st and "succeed" not in st:
                continue
            if float(p.get("usd_total") or p.get("total") or p.get("amount_after_fees") or 0) < min_spent:
                continue
            u = p.get("user") or p.get("member")
            uid = (u.get("id") if isinstance(u, dict) else str(u or "").strip()) if u else ""
            if uid:
                user_ids_with_prior.add(str(uid).strip())
        payments_in_period = []
        after = None
        params = {"created_after": start_iso, "created_before": end_iso, "order": "created_at", "direction": "asc"}
        for _ in range(min(max_pg, 30)):
            batch, pi = await cli.list_payments(first=100, after=after, params=params)
            if not batch:
                break
            payments_in_period.extend(batch)
            after = str(pi.get("end_cursor") or "")
            if not pi.get("has_next_page") or not after:
                break
        to_check = {}
        for p in payments_in_period:
            st = str(p.get("status") or p.get("substatus") or "").strip().lower()
            if st not in paid_st and "succeed" not in st:
                continue
            total = float(p.get("usd_total") or p.get("total") or p.get("amount_after_fees") or 0)
            if total < min_spent:
                continue
            mid = str((p.get("membership") or {}).get("id") or "").strip()
            pid = str((p.get("product") or {}).get("id") or "").strip()
            u = p.get("user") or p.get("member")
            uid = (u.get("id") if isinstance(u, dict) else str(u or "").strip()) if u else ""
            if mid and pid and (pid in all_pids if all_pids else True):
                to_check[mid] = (pid, str(uid).strip() if uid else "")
        full_set = set()
        lite_set = set()
        for i, (mid, item) in enumerate(to_check.items()):
            if i >= 150:
                break
            product_id, user_id = item
            if user_id and user_id in user_ids_with_prior:
                continue
            pays = await cli.get_payments_for_membership(mid)
            # Filter to this membership (API may return company-wide; filter client-side)
            def _pay_mid(p):
                v = p.get("membership_id") or p.get("membership") or ""
                if isinstance(v, dict):
                    return str(v.get("id") or v.get("membership_id") or "").strip()
                return str(v or "").strip()
            pays_for_mid = [p for p in (pays or []) if isinstance(p, dict) and (_pay_mid(p) == mid or not _pay_mid(p))]
            if not pays_for_mid:
                pays_for_mid = [p for p in (pays or []) if isinstance(p, dict)]
            success_pays = []
            for pay in pays_for_mid:
                st = str(pay.get("status") or pay.get("substatus") or "").strip().lower()
                if st not in paid_st and "succeed" not in st:
                    continue
                if float(pay.get("usd_total") or pay.get("total") or pay.get("amount_after_fees") or 0) < min_spent:
                    continue
                ts = pay.get("paid_at") or pay.get("created_at")
                dt = _parse_dt_any(ts)
                if dt:
                    success_pays.append(dt)
            if not success_pays:
                continue
            first_dt = min(success_pays)
            if start_dt <= first_dt <= end_dt:
                uid = user_id or mid
                if product_id in lite_ids:
                    lite_set.add(uid)
                else:
                    full_set.add(uid)
            await asyncio.sleep(0.05)
        return (len(full_set), len(lite_set))

    canceling_full_n, canceling_lite_n = await _fetch_canceling()

    if use_users_tab:
        base_params = {"created_after": start_utc_iso, "created_before": end_utc_iso, "order": "created_at", "direction": "asc"}

        async def _fetch_members_for_product(pid):
            out = []
            after = None
            for _ in range(max_pages):
                params = dict(base_params)
                params["product_ids"] = [pid]
                batch, pi = await client.list_members(first=100, after=after, params=params)
                if not batch:
                    break
                out.extend(batch)
                after = str(pi.get("end_cursor") or "")
                if not pi.get("has_next_page") or not after:
                    break
            return out

        async def _merge_members_by_id(product_ids):
            seen = set()
            merged = []
            for pid in product_ids:
                for m in await _fetch_members_for_product(pid):
                    mid = str(m.get("id") or "").strip()
                    if mid and mid not in seen:
                        seen.add(mid)
                        merged.append(m)
            return merged

        full_members = await _merge_members_by_id(full_ids) if full_ids else []
        lite_members = await _merge_members_by_id(lite_ids) if lite_ids else []

        buck_full = {"new_paying": 0, "new_trials": 0, "canceling": 0, "churned": 0, "other": 0}
        buck_lite = {"new_paying": 0, "new_trials": 0, "canceling": 0, "churned": 0, "other": 0}
        for m in full_members:
            b = _member_action_bucket(m)
            buck_full[b] = buck_full.get(b, 0) + 1
        for m in lite_members:
            b = _member_action_bucket(m)
            buck_lite[b] = buck_lite.get(b, 0) + 1

        # New Paying: from Reselling Secrets pool, memberships whose first payment was in period ($0 -> >$1)
        min_new_paying = float(whop_cfg.get("metrics_report_new_paying_min_spent", 1.0))
        new_paying_full_n, new_paying_lite_n = await _count_new_paying_via_payments(
            client, start_utc_iso, end_utc_iso, start_utc, end_utc, full_ids, lite_ids, all_product_ids, min_new_paying, max_pages
        )

        async def _count_churned_members(product_ids):
            """Churned = list_members with most_recent_actions=[churned, left], usd_total_spent>0 (matches Whop)."""
            seen = set()
            params = {"order": "most_recent_action", "direction": "desc"}
            if product_ids:
                params["product_ids"] = product_ids
            for action in ("churned", "left"):
                p = dict(params)
                p["most_recent_actions[]"] = action
                after = None
                for _ in range(max_pages):
                    batch, pi = await client.list_members(first=100, after=after, params=p)
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
            return len(seen)

        churned_full_n = await _count_churned_members(full_ids) if full_ids else 0
        churned_lite_n = await _count_churned_members(lite_ids) if lite_ids else 0

        full_n = len(full_members)
        lite_n = len(lite_members)
        trialing_full_n = buck_full.get("new_trials", 0)
        trialing_lite_n = buck_lite.get("new_trials", 0)
    else:
        all_memberships = []
        after = None
        for _ in range(max_pages):
            batch, pi = await client.list_memberships(
                first=100, after=after,
                params={"created_after": start_utc_iso, "created_before": end_utc_iso, "order": "created_at", "direction": "asc"},
            )
            if not batch:
                break
            for rec in batch:
                m = _norm_membership(rec)
                pt = str((m.get("product") or {}).get("title") or "")
                if prefixes and not any(pt.lower().startswith(p.lower()) for p in prefixes):
                    continue
                all_memberships.append(m)
            after = str(pi.get("end_cursor") or "")
            if not pi.get("has_next_page") or not after:
                break

        lite_ms = [m for m in all_memberships if _is_lite(str((m.get("product") or {}).get("title") or ""))]
        full_ms = [m for m in all_memberships if not _is_lite(str((m.get("product") or {}).get("title") or ""))]
        buck_lite = {}
        buck_full = {}
        for m in lite_ms:
            k = _metrics_bucket(m)
            buck_lite[k] = buck_lite.get(k, 0) + 1
        for m in full_ms:
            k = _metrics_bucket(m)
            buck_full[k] = buck_full.get(k, 0) + 1

        trialing_full = [m for m in full_ms if str(m.get("status") or "").lower() == "trialing"]
        trialing_lite = [m for m in lite_ms if str(m.get("status") or "").lower() == "trialing"]
        trialing_full_n = len(trialing_full)
        trialing_lite_n = len(trialing_lite)

        new_paying_full_n, new_paying_lite_n = await _fetch_new_paying()
        churned_full_n, churned_lite_n = await _fetch_churned()

        full_n = len(full_ms)
        lite_n = len(lite_ms)

    include_lite = bool(whop_cfg.get("metrics_report_include_lite", True))
    mode = "Whop Users" if use_users_tab else "Whop Memberships"
    print("=== METRICS Report: Feb 8–14, 2026 (America/New_York) [{}] ===\n".format(mode))
    if include_lite:
        print("Reselling Secrets LITE ({})".format(lite_n))
        print("  New Members: {}".format(lite_n))
        print("  New Paying: {}".format(new_paying_lite_n))
        print("  New Trials: {}".format(trialing_lite_n))
        print("  Members set to cancel: {}".format(canceling_lite_n))
        print("  Churned: {}".format(churned_lite_n))
        print("  Total: {}\n".format(lite_n))
    print("Reselling Secrets FULL ({})".format(full_n))
    print("  New Members: {}".format(full_n))
    print("  New Paying: {}".format(new_paying_full_n))
    print("  New Trials: {}".format(trialing_full_n))
    print("  Members set to cancel: {}".format(canceling_full_n))
    print("  Churned: {}".format(churned_full_n))
    print("  Total: {}".format(full_n))


if __name__ == "__main__":
    asyncio.run(main())
