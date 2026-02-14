"""
Test METRICS report data sources: Whop API vs events JSON files.

Compares:
- Whop API list_memberships (created_after/before) with UTC vs America/New_York
- whop_membership_logs_events.json - by_email events (Membership Activated, Payment Succeeded, etc.)
- whop_logs_events.json - by_email events (Membership was purchased, etc.)

Run from repo root: python scripts/test_metrics_report_sources.py
"""
import asyncio
import json
import sys
from datetime import datetime, date, time, timezone
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
from rschecker_utils import parse_dt_any, usd_amount


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


def _metrics_bucket(m: dict) -> str:
    st = str(m.get("status") or "").strip().lower()
    cape = m.get("cancel_at_period_end") is True or str(m.get("cancel_at_period_end") or "").strip().lower() in {"true", "yes", "1"}
    total_raw = m.get("total_spent") or m.get("total_spent_usd") or m.get("total_spend") or m.get("total_spend_usd")
    spent = float(usd_amount(total_raw))
    product_title = str((m.get("product") or {}).get("title") or "") if isinstance(m.get("product"), dict) else ""
    is_lifetime = "lifetime" in product_title.lower()

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


async def main():
    cfg = load_config()
    wh = cfg.get("whop_api") or {}
    api_key = str(wh.get("api_key") or "").strip()
    company_id = str(wh.get("company_id") or "").strip()
    tz_name = str((cfg.get("reporting") or {}).get("timezone") or "America/New_York").strip()
    whop_cfg = cfg.get("whop_api") or {}
    prefixes = [str(x).strip() for x in whop_cfg.get("joined_report_product_title_prefixes") or ["Reselling Secrets"]] if isinstance(whop_cfg.get("joined_report_product_title_prefixes"), list) else ["Reselling Secrets"]

    start_d = date(2026, 2, 2)
    end_d = date(2026, 2, 9)

    print("=== METRICS Report Data Source Test ===\n")
    print(f"Date range: {start_d} to {end_d}")
    print(f"Product filter: {prefixes}")
    print()

    # Whop dashboard URL uses: 1770008400 to 1770699599 (Unix)
    # 1770008400 = Feb 2 05:00 UTC = Feb 2 00:00 Eastern
    # 1770699599 = Feb 10 04:59:59 UTC = Feb 9 23:59:59 Eastern
    if ZoneInfo:
        tz = ZoneInfo(tz_name)
        start_local = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=tz)
        end_local = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=tz)
        start_utc_et = start_local.astimezone(timezone.utc)
        end_utc_et = end_local.astimezone(timezone.utc)
        start_iso_et = start_utc_et.isoformat().replace("+00:00", "Z")
        end_iso_et = end_utc_et.isoformat().replace("+00:00", "Z")
        print(f"Whop dashboard likely uses: {tz_name}")
        print(f"  Start: {start_local} ({tz_name}) = {start_utc_et} (UTC)")
        print(f"  End:   {end_local} ({tz_name}) = {end_utc_et} (UTC)")
        print()
    else:
        start_iso_et = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        end_iso_et = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    start_utc = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc)
    start_iso_utc = start_utc.isoformat().replace("+00:00", "Z")
    end_iso_utc = end_utc.isoformat().replace("+00:00", "Z")

    # --- Whop API ---
    if api_key and company_id:
        client = WhopAPIClient(api_key, str(wh.get("base_url") or "https://api.whop.com/api/v1"), company_id)

        # Test list_members with joined_after/joined_before (Date joined filter)
        print("--- Whop API list_members (joined_after/joined_before) ---")
        try:
            batch, _ = await client.list_members(
                first=5, after=None,
                params={"joined_after": start_iso_et, "joined_before": end_iso_et, "order": "joined_at", "direction": "asc"},
            )
            print(f"  joined_after/before: API accepted, got {len(batch)} members (first 5)")
        except Exception as ex:
            print(f"  joined_after/before: {ex}")
            try:
                batch, _ = await client.list_members(first=5, params={"order": "joined_at", "direction": "desc"})
                print(f"  Fallback: will filter client-side, got sample {len(batch)} members")
            except Exception as ex2:
                print(f"  Fallback failed: {ex2}")
        print()

        for label, start_iso, end_iso in [
            ("UTC created_at", start_iso_utc, end_iso_utc),
            (f"{tz_name} created_at", start_iso_et, end_iso_et),
        ]:
            print(f"--- Whop API list_memberships ({label}) ---")
            all_ms = []
            after = None
            for _ in range(50):
                batch, pi = await client.list_memberships(
                    first=100, after=after,
                    params={"created_after": start_iso, "created_before": end_iso, "order": "created_at", "direction": "asc"},
                )
                if not batch:
                    break
                for rec in batch:
                    m = _norm_membership(rec)
                    pt = str((m.get("product") or {}).get("title") or "")
                    if prefixes and not any(pt.lower().startswith(p.lower()) for p in prefixes):
                        continue
                    all_ms.append(m)
                after = str(pi.get("end_cursor") or "")
                if not pi.get("has_next_page") or not after:
                    break

            full_ms = [m for m in all_ms if not _is_lite(str((m.get("product") or {}).get("title") or ""))]
            lite_ms = [m for m in all_ms if _is_lite(str((m.get("product") or {}).get("title") or ""))]

            buck = {}
            for m in full_ms:
                b = _metrics_bucket(m)
                buck[b] = buck.get(b, 0) + 1

            print(f"  FULL total: {len(full_ms)}")
            print(f"  LITE total: {len(lite_ms)}")
            print(f"  FULL buckets: new_paying={buck.get('new_paying',0)} new_trials={buck.get('new_trials',0)} canceling={buck.get('canceling',0)} churned={buck.get('churned',0)} completed={buck.get('completed',0)} other_lifetime={buck.get('other_lifetime',0)} other={buck.get('other',0)}")
            print()
    else:
        print("(Skipping Whop API - no api_key/company_id)")
        print()

    # --- whop_membership_logs_events.json ---
    ml_path = RSC_DIR / "data" / "whop_membership_logs_events.json"
    if ml_path.exists():
        data = json.loads(ml_path.read_text(encoding="utf-8", errors="replace"))
        by_email = data.get("by_email") or {}
        print("--- whop_membership_logs_events.json ---")
        print(f"  Unique emails: {len(by_email)}")

        # Count events in Feb 2-9 with product=Reselling Secrets (not Lite), status=trialing
        trialing_in_range = []
        for email, rec in by_email.items():
            events = rec.get("events") or {}
            for tkey, evt in events.items():
                created = evt.get("created_at_iso") or ""
                dt = parse_dt_any(created)
                if not dt:
                    continue
                d = dt.date()
                if d < start_d or d > end_d:
                    continue
                prod = str(evt.get("fields", {}).get("product") or evt.get("product") or "").strip()
                if "Lite" in prod and "Lifetime" not in prod:
                    continue
                if "Reselling Secrets" not in prod:
                    continue
                st = str(evt.get("status") or "").strip().lower()
                if st == "trialing":
                    trialing_in_range.append({"email": email, "title": tkey, "created": created, "product": prod})

        print(f"  Events (FULL product, status=trialing) with created_at in range: {len(trialing_in_range)}")
        if trialing_in_range:
            for x in trialing_in_range[:5]:
                print(f"    - {x['email'][:30]}... {x['title'][:40]} {x['created'][:10]}")
            if len(trialing_in_range) > 5:
                print(f"    ... +{len(trialing_in_range)-5} more")
        print()
    else:
        print("(whop_membership_logs_events.json not found)")
        print()

    # --- whop_logs_events.json ---
    wl_path = RSC_DIR / "data" / "whop_logs_events.json"
    if wl_path.exists():
        data = json.loads(wl_path.read_text(encoding="utf-8", errors="replace"))
        by_email = data.get("by_email") or {}
        print("--- whop_logs_events.json ---")
        print(f"  Unique emails: {len(by_email)}")

        purchased_in_range = []
        for email, rec in by_email.items():
            events = rec.get("events") or {}
            for tkey, evt in events.items():
                created = evt.get("created_at_iso") or ""
                dt = parse_dt_any(created)
                if not dt:
                    continue
                d = dt.date()
                if d < start_d or d > end_d:
                    continue
                if "membership was purchased" in tkey.lower() or "membership was generated" in tkey.lower():
                    purchased_in_range.append({"email": email, "title": tkey, "created": created, "status": evt.get("membership_status")})

        print(f"  Events (purchased/generated) with created_at in range: {len(purchased_in_range)}")
        if purchased_in_range:
            for x in purchased_in_range[:5]:
                print(f"    - {x['email'][:30]}... {x['title'][:50]} {x['created'][:10]}")
        print()

    print("--- Logic Summary ---")
    print("  New Paying: status=active AND total_spent > 0")
    print("  New Trials: status=trialing (membership created in date range)")
    print("  Members set to cancel: cancel_at_period_end=true, status in (active,trialing)")
    print("  Churned: status in (canceled,expired,churned)")
    print("  Completed: status=completed (1-time ended)")
    print("  Other (Lifetime): product title contains 'lifetime'")
    print("  Other: catch-all")
    print()
    print("  NOTE: Whop API filters by membership.created_at (when membership was created).")
    print("  A membership 'created' in range = joined in range. If someone appears as New Trial")
    print("  but 'joined way before', it could mean: (a) they upgraded - new FULL membership")
    print("  created in range, or (b) timezone mismatch - use America/New_York to match dashboard.")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
