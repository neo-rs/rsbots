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

    if st in {"canceled", "cancelled", "expired", "churned"}:
        return "churned"
    if st == "completed":
        return "completed"
    if cape and st in {"active", "trialing"}:
        return "canceling"
    if is_lifetime:
        return "other_lifetime"
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

    start_d = date(2026, 2, 2)
    end_d = date(2026, 2, 9)
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
    window_start = (start_utc - timedelta(days=7)).isoformat().replace("+00:00", "Z")
    window_end = (end_utc + timedelta(days=31)).isoformat().replace("+00:00", "Z")

    client = WhopAPIClient(api_key, str(wh.get("base_url") or "https://api.whop.com/api/v1"), company_id)

    joined_member_ids = set()
    after = None
    for _ in range(max_pages):
        batch, page_info = await client.list_members(
            first=100, after=after,
            params={"joined_after": start_utc_iso, "joined_before": end_utc_iso, "order": "joined_at", "direction": "asc"},
        )
        if not batch:
            break
        for rec in batch:
            mid = str(rec.get("id") or rec.get("member_id") or "").strip()
            if mid.startswith("mber_"):
                joined_member_ids.add(mid)
        after = str(page_info.get("end_cursor") or "")
        if not page_info.get("has_next_page") or not after:
            break

    all_memberships = []
    after = None
    for _ in range(max_pages):
        batch, page_info = await client.list_memberships(
            first=100, after=after,
            params={"created_after": window_start, "created_before": window_end, "order": "created_at", "direction": "asc"},
        )
        if not batch:
            break
        for rec in batch:
            m = _norm_membership(rec)
            mber_id = _membership_member_id(m)
            if mber_id and mber_id not in joined_member_ids:
                continue
            pt = str((m.get("product") or {}).get("title") or "")
            if prefixes and not any(pt.lower().startswith(p.lower()) for p in prefixes):
                continue
            all_memberships.append(m)
        after = str(page_info.get("end_cursor") or "")
        if not page_info.get("has_next_page") or not after:
            break

    lite_ms = [m for m in all_memberships if _is_lite(str((m.get("product") or {}).get("title") or ""))]
    full_ms = [m for m in all_memberships if not _is_lite(str((m.get("product") or {}).get("title") or ""))]

    def buckets(ms):
        b = {}
        for m in ms:
            k = _metrics_bucket(m)
            b[k] = b.get(k, 0) + 1
        return b

    buck_lite = buckets(lite_ms)
    buck_full = buckets(full_ms)
    churned_lite = buck_lite.get("churned", 0)
    churned_full = buck_full.get("churned", 0)
    churn_lite = (churned_lite / len(lite_ms) * 100) if lite_ms else 0
    churn_full = (churned_full / len(full_ms) * 100) if full_ms else 0

    lite_member_ids = {_membership_member_id(m) for m in lite_ms if _membership_member_id(m)}
    full_member_ids = {_membership_member_id(m) for m in full_ms if _membership_member_id(m)}

    print("=== METRICS Report: Feb 2–9, 2026 (joined_at filter, America/New_York) ===\n")
    print(f"New Members (unique joined in range): {len(joined_member_ids)}\n")
    print("Reselling Secrets LITE ({})".format(len(lite_ms)))
    print("  New Members: {}".format(len(lite_member_ids)))
    print("  New Paying: {}".format(buck_lite.get("new_paying", 0)))
    print("  New Trials: {}".format(buck_lite.get("new_trials", 0)))
    print("  Members set to cancel: {}".format(buck_lite.get("canceling", 0)))
    print("  Churned: {}".format(buck_lite.get("churned", 0)))
    print("  Completed (1-time): {}".format(buck_lite.get("completed", 0)))
    print("  Other (Lifetime): {}".format(buck_lite.get("other_lifetime", 0)))
    print("  Other: {}".format(buck_lite.get("other", 0)))
    print("  Total: {}".format(len(lite_ms)))
    print("  Churn: {:.2f}%\n".format(churn_lite))
    print("Reselling Secrets FULL ({})".format(len(full_ms)))
    print("  New Members: {}".format(len(full_member_ids)))
    print("  New Paying: {}".format(buck_full.get("new_paying", 0)))
    print("  New Trials: {}".format(buck_full.get("new_trials", 0)))
    print("  Members set to cancel: {}".format(buck_full.get("canceling", 0)))
    print("  Churned: {}".format(buck_full.get("churned", 0)))
    print("  Completed (1-time): {}".format(buck_full.get("completed", 0)))
    print("  Other (Lifetime): {}".format(buck_full.get("other_lifetime", 0)))
    print("  Other: {}".format(buck_full.get("other", 0)))
    print("  Total: {}".format(len(full_ms)))
    print("  Churn: {:.2f}%\n".format(churn_full))
    total_all = len(all_memberships)
    churned_all = sum(1 for m in all_memberships if _metrics_bucket(m) == "churned")
    overall_churn = (churned_all / total_all * 100) if total_all else 0
    print("Overall: New Members={}, Total memberships={}, Churn %={:.2f}%".format(
        len(joined_member_ids), total_all, overall_churn))


if __name__ == "__main__":
    asyncio.run(main())
