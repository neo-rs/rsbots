#!/usr/bin/env python3
"""
Whop API Probe (local-only)
---------------------------
Small standalone script to confirm what Whop API returns, using your existing
`config.json` + `config.secrets.json` (no Discord bot startup).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import discord

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from whop_api_client import WhopAPIClient
from rschecker_utils import extract_discord_id_from_whop_member_record
from rschecker_utils import access_roles_plain, coerce_role_ids, fmt_date_any, usd_amount
from staff_embeds import build_case_minimal_embed, build_member_status_detailed_embed
from whop_webhook_handler import _extract_email_from_embed as _extract_email_from_native_embed
from whop_webhook_handler import _extract_discord_id_from_embed as _extract_discord_id_from_native_embed


BASE_DIR = Path(__file__).resolve().parent
_PROBE_STAFFCARDS_DEDUPE_FILE = BASE_DIR / ".probe_staffcards_sent.json"


def _load_json_file(p: Path) -> dict:
    try:
        txt = p.read_text(encoding="utf-8").strip()
        if not txt:
            return {}
        data = json.loads(txt)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json_file(p: Path, data: dict) -> None:
    try:
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return


def _deep_merge(a: dict, b: dict) -> dict:
    """Merge b into a (dict-only)."""
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)  # type: ignore[arg-type]
        else:
            out[k] = v
    return out


def load_config() -> dict:
    cfg = _load_json_file(BASE_DIR / "config.json")
    secrets = _load_json_file(BASE_DIR / "config.secrets.json")
    return _deep_merge(cfg, secrets)


def _parse_user_day(s: str) -> Optional[date]:
    ss = str(s or "").strip()
    if not ss:
        return None
    for fmt in ("%Y-%m-%d", "%m-%d-%y", "%m-%d-%Y", "%m/%d/%y", "%m/%d/%Y"):
        with suppress(Exception):
            return datetime.strptime(ss, fmt).date()
    for fmt in ("%m/%d", "%m-%d"):
        with suppress(Exception):
            d0 = datetime.strptime(ss, fmt).date()
            now0 = datetime.now(timezone.utc).date()
            return date(now0.year, d0.month, d0.day)
    return None


def _dt_local_range(start_d: date, end_d: date, tz_name: str) -> tuple[datetime, datetime, bool]:
    tz = timezone.utc
    ok = False
    if str(tz_name or "").strip().upper() == "UTC":
        ok = True
    elif ZoneInfo is not None:
        try:
            tz = ZoneInfo(tz_name)
            ok = True
        except Exception:
            ok = False
    start_local = datetime.combine(start_d, time(0, 0, 0), tzinfo=tz)
    end_local = datetime.combine(end_d, time(23, 59, 59), tzinfo=tz)
    return start_local, end_local, ok


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_bool(v: object) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _extract_email(m: dict) -> str:
    for path in (
        ("user", "email"),
        ("member", "user", "email"),
        ("member", "email"),
        ("email",),
    ):
        cur: Any = m
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur.get(k)
            else:
                ok = False
                break
        if ok:
            s = str(cur or "").strip()
            if "@" in s:
                return s
    return ""


def _extract_user_id(m: dict) -> str:
    u = m.get("user")
    if isinstance(u, dict):
        uid = str(u.get("id") or "").strip()
        if uid:
            return uid
    for k in ("user_id", "userId"):
        uid = str(m.get(k) or "").strip()
        if uid:
            return uid
    return ""


def _extract_product_title(m: dict) -> str:
    p = m.get("product")
    if isinstance(p, dict):
        t = str(p.get("title") or "").strip()
        if t:
            return t
    ap = m.get("access_pass")
    if isinstance(ap, dict):
        t = str(ap.get("title") or "").strip()
        if t:
            return t
    return ""


def _extract_member_id(m: dict) -> str:
    mm = m.get("member")
    if isinstance(mm, str) and mm.strip().startswith("mber_"):
        return mm.strip()
    if isinstance(mm, dict):
        mid = str(mm.get("id") or mm.get("member_id") or "").strip()
        if mid.startswith("mber_"):
            return mid
    mid2 = str(m.get("member_id") or "").strip()
    return mid2 if mid2.startswith("mber_") else ""


def _extract_total_spend_raw(obj: object) -> object:
    if not isinstance(obj, dict):
        return ""
    stats = obj.get("stats") if isinstance(obj.get("stats"), dict) else {}
    return (
        obj.get("usd_total_spent")
        or obj.get("usd_total_spent_cents")
        or obj.get("total_spent")
        or obj.get("total_spent_usd")
        or obj.get("total_spend")
        or obj.get("total_spend_usd")
        or obj.get("total_spend_cents")
        or obj.get("total_spent_cents")
        or obj.get("platform_spend_usd")
        or obj.get("platform_spend")
        or (stats.get("total_spent") if isinstance(stats, dict) else "")
        or (stats.get("total_spend") if isinstance(stats, dict) else "")
        or (stats.get("total_spend_cents") if isinstance(stats, dict) else "")
        or (stats.get("total_spent_cents") if isinstance(stats, dict) else "")
        or ((obj.get("user") or {}).get("total_spent") if isinstance(obj.get("user"), dict) else "")
        or ((obj.get("user") or {}).get("total_spend") if isinstance(obj.get("user"), dict) else "")
    )


def _fmt_usd_amt(amt: float) -> str:
    try:
        return f"${float(amt):,.2f}"
    except Exception:
        return "N/A"


def _usd_from_obj(obj: object, *, usd_keys: tuple[str, ...], cents_keys: tuple[str, ...]) -> tuple[float, bool]:
    """Return (amount_usd, found_any_field) using explicit *_cents keys when present."""
    if not isinstance(obj, dict):
        return (0.0, False)

    def _from_dict(d: dict) -> tuple[float, bool]:
        for k in cents_keys:
            if k in d:
                v = d.get(k)
                if v is None or str(v).strip() == "":
                    continue
                return (usd_amount(v) / 100.0, True)
        for k in usd_keys:
            if k in d:
                v = d.get(k)
                if v is None or str(v).strip() == "":
                    continue
                return (usd_amount(v), True)
        return (0.0, False)

    amt, found = _from_dict(obj)
    if found:
        return (amt, True)

    stats = obj.get("stats") if isinstance(obj.get("stats"), dict) else {}
    if isinstance(stats, dict) and stats:
        amt2, found2 = _from_dict(stats)
        if found2:
            return (amt2, True)

    user = obj.get("user") if isinstance(obj.get("user"), dict) else {}
    if isinstance(user, dict) and user:
        amt3, found3 = _from_dict(user)
        if found3:
            return (amt3, True)

    return (0.0, False)


def _total_spend_usd(obj: object) -> tuple[float, bool]:
    return _usd_from_obj(
        obj,
        usd_keys=("usd_total_spent", "total_spent_usd", "total_spend_usd", "total_spent", "total_spend", "platform_spend_usd", "platform_spend"),
        cents_keys=("usd_total_spent_cents", "total_spend_cents", "total_spent_cents"),
    )


def _extract_member_id_from_manage_url(url: str) -> str:
    s = str(url or "").strip()
    if "mber_" not in s:
        return ""
    i = s.find("mber_")
    if i < 0:
        return ""
    j = i
    while j < len(s) and (s[j].isalnum() or s[j] in "_-"):
        j += 1
    cand = s[i:j].strip()
    return cand if cand.startswith("mber_") else ""


async def _best_payment_for_membership(client: WhopAPIClient, membership_id: str) -> dict:
    """Return the most recent payment dict we can associate to membership_id (best-effort)."""
    mid = str(membership_id or "").strip()
    if not mid:
        return {}
    # Try Whop API client helper first (it may or may not be filtered server-side).
    pays: list[dict] = []
    with suppress(Exception):
        pays = await client.get_payments_for_membership(mid)  # type: ignore[assignment]
    if not isinstance(pays, list):
        pays = []

    # Filter if payment object includes membership id.
    def _payment_mid(p: dict) -> str:
        v = p.get("membership_id") or p.get("membership") or ""
        if isinstance(v, dict):
            return str(v.get("id") or v.get("membership_id") or "").strip()
        return str(v or "").strip()

    filtered = [p for p in pays if isinstance(p, dict) and (_payment_mid(p) == mid or not _payment_mid(p))]
    pool = filtered if filtered else [p for p in pays if isinstance(p, dict)]
    if not pool:
        return {}

    # Sort by created_at desc.
    def _ts(p: dict) -> str:
        return str(p.get("paid_at") or p.get("created_at") or "").strip()

    with suppress(Exception):
        pool.sort(key=_ts, reverse=True)
    return pool[0] if isinstance(pool[0], dict) else {}


async def _whop_brief_api_only(client: WhopAPIClient, membership_id: str) -> dict:
    """Build whop_brief using Whop API only (no Discord logs parsing)."""
    mid = str(membership_id or "").strip()
    if not mid:
        return {}

    mship = await client.get_membership_by_id(mid)
    if not isinstance(mship, dict) or not mship:
        return {}

    # Product / status
    product_title = "N/A"
    if isinstance(mship.get("product"), dict):
        product_title = str(mship["product"].get("title") or "").strip() or "N/A"
    status = str(mship.get("status") or "").strip() or "N/A"
    cape = True if mship.get("cancel_at_period_end") is True else (False if mship.get("cancel_at_period_end") is False else None)
    cancel_at_period_end = "yes" if cape is True else ("no" if cape is False else "N/A")

    # Renewal fields
    renewal_start_iso = str(mship.get("renewal_period_start") or "").strip()
    renewal_end_iso = str(mship.get("renewal_period_end") or "").strip()
    renewal_start = fmt_date_any(renewal_start_iso) if renewal_start_iso else "N/A"
    renewal_end = fmt_date_any(renewal_end_iso) if renewal_end_iso else "N/A"
    renewal_window = f"{renewal_start} → {renewal_end}" if (renewal_start != "N/A" and renewal_end != "N/A") else "N/A"

    remaining_days = "N/A"
    if renewal_end_iso:
        dt_end = _parse_dt_any(renewal_end_iso)
        if isinstance(dt_end, datetime):
            delta = (dt_end - datetime.now(timezone.utc)).total_seconds()
            remaining_days = str(max(0, int((delta / 86400.0) + 0.999)))

    # Links
    manage_url = str(mship.get("manage_url") or "").strip()
    manage_url_s = manage_url if manage_url else "N/A"

    # Dashboard URL: based on membership.user.id (user_...)
    dash = "N/A"
    u = mship.get("user")
    user_id = ""
    if isinstance(u, dict):
        user_id = str(u.get("id") or "").strip()
    elif isinstance(u, str):
        user_id = u.strip()
    if user_id and getattr(client, "company_id", ""):
        dash = f"https://whop.com/dashboard/{str(client.company_id).strip()}/users/{user_id}/"

    # Total spend: prefer /members/{mber_}.usd_total_spent if available.
    mber_id = ""
    if isinstance(mship.get("member"), dict):
        mber_id = str(mship["member"].get("id") or "").strip()
    if not mber_id and manage_url:
        mber_id = _extract_member_id_from_manage_url(manage_url)
    mrec = await client.get_member_by_id(mber_id) if mber_id else None

    mem_amt, mem_found = _total_spend_usd(mship)
    user_amt, user_found = _total_spend_usd(mrec) if isinstance(mrec, dict) else (0.0, False)
    if user_found and (not mem_found or float(user_amt) >= float(mem_amt)):
        total_spent = _fmt_usd_amt(float(user_amt))
    elif mem_found:
        total_spent = _fmt_usd_amt(float(mem_amt))
    else:
        total_spent = "N/A"

    # Payments: best-effort.
    pay = await _best_payment_for_membership(client, mid)
    last_success_paid_at = "N/A"
    last_payment_failure = "N/A"
    last_payment_method = "N/A"
    last_payment_type = "N/A"
    if isinstance(pay, dict) and pay:
        paid_at = str(pay.get("paid_at") or pay.get("created_at") or "").strip()
        if paid_at:
            last_success_paid_at = paid_at
        failure_msg = str(pay.get("failure_message") or "").strip()
        if failure_msg:
            last_payment_failure = failure_msg
        pm = str(pay.get("payment_method") or pay.get("method") or "").strip()
        if pm:
            last_payment_method = pm
        pt = str(pay.get("type") or pay.get("payment_type") or "").strip()
        if pt:
            last_payment_type = pt

    # Trial days / pricing (best-effort from plan)
    trial_days = "N/A"
    pricing = "N/A"
    plan_is_renewal = "N/A"
    plan = mship.get("plan") if isinstance(mship.get("plan"), dict) else {}
    if isinstance(plan, dict) and plan:
        td = plan.get("trial_days") or plan.get("trial_period_days")
        if str(td or "").strip():
            trial_days = str(td).strip()
        price = plan.get("price") or plan.get("pricing")
        if str(price or "").strip():
            pricing = str(price).strip()
        ir = plan.get("is_renewal") or plan.get("plan_is_renewal")
        if isinstance(ir, bool):
            plan_is_renewal = "true" if ir else "false"

    # Checkout link (often not available via API)
    checkout_url = "N/A"
    for k in ("checkout_url", "checkout", "purchase_link", "purchase_url"):
        v = str(mship.get(k) or "").strip()
        if v:
            checkout_url = v
            break

    return {
        "status": status,
        "product": product_title,
        "membership_id": mid,
        "member_since": fmt_date_any(str(mship.get("created_at") or "").strip()) if str(mship.get("created_at") or "").strip() else "N/A",
        "trial_end": fmt_date_any(str(mship.get("trial_end") or mship.get("trial_ends_at") or mship.get("trial_end_at") or "").strip())
        if str(mship.get("trial_end") or mship.get("trial_ends_at") or mship.get("trial_end_at") or "").strip()
        else "N/A",
        "trial_days": trial_days,
        "plan_is_renewal": plan_is_renewal,
        "promo": "N/A",
        "pricing": pricing,
        "renewal_start": renewal_start,
        "renewal_end": renewal_end,
        "renewal_window": renewal_window,
        "remaining_days": remaining_days,
        "dashboard_url": dash,
        "manage_url": manage_url_s,
        "checkout_url": checkout_url,
        "total_spent": total_spent,
        "cancel_at_period_end": cancel_at_period_end,
        "is_first_membership": "N/A",
        "last_success_paid_at": last_success_paid_at,
        "last_payment_failure": last_payment_failure,
        "last_payment_method": last_payment_method,
        "last_payment_type": last_payment_type,
    }


@dataclass
class JoinedRow:
    membership_id: str
    user_id: str
    email: str
    product: str
    status: str
    cancel_at_period_end: bool
    created_at: str
    date_joined: str


async def _probe_joined(args: argparse.Namespace) -> int:
    cfg = load_config()
    wh = cfg.get("whop_api") if isinstance(cfg, dict) else {}
    wh = wh if isinstance(wh, dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    if not api_key or not company_id:
        print("Missing `whop_api.api_key` or `whop_api.company_id` in config.")
        return 2

    tz_name = str(args.tz or "America/New_York").strip() or "America/New_York"
    start_d = _parse_user_day(args.start) or datetime.now(timezone.utc).date()
    end_d = _parse_user_day(args.end) or start_d
    if end_d < start_d:
        start_d, end_d = end_d, start_d

    start_local, end_local, tz_ok = _dt_local_range(start_d, end_d, tz_name)
    start_utc_iso = _isoz(start_local)
    # Inclusive end: add almost a full day so created_before includes end day.
    end_utc_iso = _isoz(end_local)

    prefixes: list[str] = [p.strip() for p in (args.product_prefix or []) if str(p).strip()]
    allowed_statuses: set[str] = {s.strip().lower() for s in (args.status or []) if str(s).strip()}

    client = WhopAPIClient(api_key, base_url, company_id)
    rows: list[JoinedRow] = []
    pages = 0
    after: str | None = None
    while pages < int(args.max_pages):
        batch, page_info = await client.list_memberships(
            first=int(args.per_page),
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
        pages += 1
        for m in batch:
            if not isinstance(m, dict):
                continue
            st = str(m.get("status") or "").strip().lower() or "unknown"
            if allowed_statuses and st not in allowed_statuses:
                continue
            if st == "drafted" and args.exclude_drafted:
                continue
            prod = _extract_product_title(m)
            if prefixes:
                low = prod.lower()
                if not any(low.startswith(p.lower()) for p in prefixes):
                    continue
            mid = str(m.get("id") or "").strip() or str(m.get("membership_id") or "").strip()
            rows.append(
                JoinedRow(
                    membership_id=mid,
                    user_id=_extract_user_id(m),
                    email=_extract_email(m),
                    product=prod,
                    status=st,
                    cancel_at_period_end=_norm_bool(m.get("cancel_at_period_end")),
                    created_at=str(m.get("created_at") or "").strip(),
                    date_joined=str(m.get("date_joined") or m.get("date_joined_at") or "").strip(),
                )
            )
        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break

    # Dedupe like the `.checker syncsummary` report (user+product).
    dedup: dict[tuple[str, str], JoinedRow] = {}
    prio = {"active": 1, "trialing": 2, "canceling": 3, "canceled": 4, "completed": 5, "expired": 6}
    for r in rows:
        k = (r.user_id or r.email or r.membership_id, r.product)
        best = dedup.get(k)
        if not best:
            dedup[k] = r
            continue
        if prio.get(r.status, 99) < prio.get(best.status, 99):
            dedup[k] = r
            continue
        if prio.get(r.status, 99) == prio.get(best.status, 99):
            bdt = _parse_dt_any(best.created_at) if best.created_at else None
            rdt = _parse_dt_any(r.created_at) if r.created_at else None
            if bdt and rdt and rdt < bdt:
                dedup[k] = r

    # Print summary
    total = len(dedup)
    status_counts: dict[str, int] = {}
    set_to_cancel = 0
    date_joined_present = 0
    for r in dedup.values():
        status_counts[r.status] = int(status_counts.get(r.status, 0)) + 1
        if r.cancel_at_period_end and r.status in {"active", "trialing"}:
            set_to_cancel += 1
        if r.date_joined:
            date_joined_present += 1

    churn = (float(status_counts.get("canceled", 0) + status_counts.get("completed", 0)) / float(total) * 100.0) if total else 0.0

    print("=== Whop API Probe: Joined (range) ===")
    print(f"Timezone: {tz_name}")
    if not tz_ok:
        print("WARNING: Timezone could not be resolved; using UTC boundaries.")
        print("         If you want America/New_York boundaries on Windows, install tzdata: `pip install tzdata`")
    print(f"Range: {start_d.isoformat()} -> {end_d.isoformat()}")
    print(f"created_after: {start_utc_iso}")
    print(f"created_before: {end_utc_iso}")
    print(f"Raw API records (after filters): {len(rows)}")
    print(f"Deduped user+product rows: {total}")
    print("Counts:")
    for k in sorted(status_counts.keys()):
        print(f"- {k}: {status_counts[k]}")
    print(f"- set_to_cancel (active/trialing + cancel_at_period_end): {set_to_cancel}")
    print(f"- date_joined populated (deduped): {date_joined_present}/{total}")
    print(f"Churn% (canceled+completed / total): {churn:.2f}%")

    # By product
    prod_counts: dict[str, int] = {}
    for r in dedup.values():
        prod_counts[r.product or "Unknown"] = int(prod_counts.get(r.product or "Unknown", 0)) + 1
    print("By product:")
    for p, n in sorted(prod_counts.items(), key=lambda kv: kv[1], reverse=True):
        print(f"- {p}: {n}")

    # Sample rows
    print("\nSample (deduped):")
    sample = list(dedup.values())[: int(args.show)]
    for r in sample:
        print(
            f"- {r.created_at} | {r.status:10s} | cape={str(r.cancel_at_period_end).lower():5s} | {r.product} | {r.email} | {r.membership_id}"
        )

    # Also compute Whop "Users view" style aggregation (dedupe by user_id).
    users: dict[str, dict] = {}
    for r in rows:
        ukey = (r.user_id or r.email or r.membership_id).strip().lower()
        if not ukey:
            continue
        u = users.get(ukey)
        if not isinstance(u, dict):
            u = {"user_id": r.user_id, "email": r.email, "products": set(), "items": []}
            users[ukey] = u
        if r.product:
            u["products"].add(r.product)
        u["items"].append(
            {
                "status": r.status,
                "cape": bool(r.cancel_at_period_end),
                "product": r.product,
                "created_at": r.created_at,
            }
        )

    prio2 = {"active": 1, "trialing": 2, "pending": 3, "canceling": 4, "past_due": 5, "unpaid": 5, "canceled": 20, "completed": 21, "expired": 22}

    def _is_lite(t: str) -> bool:
        return "(lite)" in str(t or "").lower()

    buckets = Counter()
    canceling_users = 0
    for u in users.values():
        items = u.get("items") if isinstance(u.get("items"), list) else []
        prods = list(u.get("products") or [])
        has_paid = any(not _is_lite(p) for p in prods) if prods else False
        paid_items = [it for it in items if not _is_lite(str(it.get("product") or ""))]
        pool = paid_items if paid_items else items
        best = None
        for it in pool:
            st = str(it.get("status") or "").lower()
            if best is None or prio2.get(st, 99) < prio2.get(str(best.get("status") or "").lower(), 99):
                best = it
        if not isinstance(best, dict):
            continue
        best_status = str(best.get("status") or "").lower()
        any_cape = any(bool(it.get("cape")) for it in pool if isinstance(it, dict))
        ended = best_status in {"canceled", "completed", "expired"}
        past_due = best_status in {"past_due", "unpaid"}
        b = "joined"
        if ended:
            b = "churned" if has_paid else "left"
        elif past_due:
            b = "past_due"
        elif any_cape and best_status in {"active", "trialing", "pending"}:
            b = "canceling"
        elif best_status in {"trialing", "pending"}:
            b = "trialing"
        else:
            b = "joined"
        buckets[b] += 1
        if b == "canceling":
            canceling_users += 1

    print("\n=== Users-view aggregation (dedupe by user_id/email) ===")
    print(f"users: {len(users)}")
    print(dict(buckets))

    return 0


async def _probe_compare_csv(args: argparse.Namespace) -> int:
    csv_path = Path(str(args.csv or "").strip())
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 2

    import csv as _csv
    from collections import Counter as _Counter

    rows = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        r = _csv.DictReader(f)
        for row in r:
            rows.append(row)

    # Whop export status buckets from date columns.
    buckets = _Counter()
    user_ids = set()
    member_ids = set()
    membership_ids = set()
    for row in rows:
        user_ids.add((row.get("User ID") or "").strip())
        member_ids.add((row.get("Member ID") or "").strip())
        mids = (row.get("Membership IDs") or "").strip()
        if mids:
            for m in mids.split(","):
                m = m.strip()
                if m:
                    membership_ids.add(m)
        churned = (row.get("Churned date") or "").strip()
        left = (row.get("Left date") or "").strip()
        canceling = (row.get("Canceling date") or "").strip()
        past_due = (row.get("Past due date") or "").strip()
        trial_end = (row.get("Trial end date") or "").strip()
        if churned:
            st = "churned"
        elif left:
            st = "left"
        elif canceling:
            st = "canceling"
        elif past_due:
            st = "past_due"
        elif trial_end:
            st = "trialing"
        else:
            st = "joined"
        buckets[st] += 1

    # Clean empties
    user_ids.discard("")
    member_ids.discard("")

    print("=== CSV export ===")
    print(f"rows: {len(rows)}")
    print(f"unique user_id: {len(user_ids)}")
    print(f"unique member_id: {len(member_ids)}")
    print(f"unique membership_id: {len(membership_ids)}")
    print("buckets:", dict(buckets))

    # Compare to API using:
    # - memberships in range -> member_id mapping (for product filter)
    # - members in range (Whop dashboard "Users") using joined_at + most_recent_action
    client, wh = _init_client_from_local_config()
    if not client:
        print("Missing `whop_api.api_key` or `whop_api.company_id` in config.")
        return 2

    tz_name = str(args.tz or "America/New_York").strip() or "America/New_York"
    start_d = _parse_user_day(args.start) or datetime.now(timezone.utc).date()
    end_d = _parse_user_day(args.end) or start_d
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    start_local, end_local, tz_ok = _dt_local_range(start_d, end_d, tz_name)
    start_utc_iso = _isoz(start_local)
    end_utc_iso = _isoz(end_local)

    prefixes: list[str] = [p.strip() for p in (args.product_prefix or []) if str(p).strip()]

    # 1) Memberships in range -> member mapping (for product filter + membership IDs)
    mber_map: dict[str, dict] = {}
    after: str | None = None
    pages_m = 0
    while pages_m < int(args.max_pages):
        batch, page_info = await client.list_memberships(
            first=int(args.per_page),
            after=after,
            params={"created_after": start_utc_iso, "created_before": end_utc_iso, "order": "created_at", "direction": "asc"},
        )
        if not batch:
            break
        pages_m += 1
        for m0 in batch:
            if not isinstance(m0, dict):
                continue
            st = str(m0.get("status") or "").strip().lower() or "unknown"
            if st == "drafted" and args.exclude_drafted:
                continue
            prod = _extract_product_title(m0)
            if prefixes:
                low = prod.lower()
                if not any(low.startswith(p.lower()) for p in prefixes):
                    continue
            mm = m0.get("member")
            mber_id = ""
            if isinstance(mm, dict):
                mber_id = str(mm.get("id") or "").strip()
            elif isinstance(mm, str):
                mber_id = mm.strip()
            if not mber_id:
                continue
            mid = str(m0.get("id") or "").strip() or str(m0.get("membership_id") or "").strip()
            rec = mber_map.get(mber_id)
            if not isinstance(rec, dict):
                rec = {"membership_ids": set(), "products": set()}
                mber_map[mber_id] = rec
            if mid:
                rec["membership_ids"].add(mid)
            if prod:
                rec["products"].add(prod)

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break

    # 2) Members in range -> buckets (Whop UI status)
    api_buckets = _Counter()
    api_member_ids = set()
    after = None
    pages_u = 0
    stop = False
    while pages_u < int(args.max_pages) and not stop:
        batch, page_info = await client.list_members(first=int(args.per_page), after=after, params={"order": "joined_at", "direction": "desc"})
        if not batch:
            break
        pages_u += 1
        for m in batch:
            if not isinstance(m, dict):
                continue
            mber_id = str(m.get("id") or "").strip()
            if not mber_id:
                continue
            joined_at = str(m.get("joined_at") or m.get("created_at") or "").strip()
            dtj = _parse_dt_any(joined_at)
            if not dtj:
                continue
            local_day = dtj.astimezone(start_local.tzinfo).date()  # type: ignore[arg-type]
            if local_day < start_d:
                stop = True
                break
            if local_day > end_d:
                continue
            # Keep members even if we couldn't map membership IDs (export may show blanks).
            api_member_ids.add(mber_id)
            status = str(m.get("status") or "").strip().lower() or "unknown"
            action = str(m.get("most_recent_action") or "").strip().lower()
            bucket = action if action in {"joined", "trialing", "canceling", "churned", "left", "past_due"} else status
            if bucket not in {"joined", "trialing", "canceling", "churned", "left", "past_due"}:
                bucket = "joined" if status == "joined" else ("left" if status == "left" else "joined")
            api_buckets[bucket] += 1

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break

    print("\n=== API (members + membership mapping) ===")
    print(f"tz_ok: {tz_ok}")
    print(f"memberships_pages: {pages_m}")
    print(f"members_pages: {pages_u}")
    print(f"unique members (range): {len(api_member_ids)}")
    print("buckets:", dict(api_buckets))

    # Compare membership_id coverage
    api_mids = set()
    for rec in mber_map.values():
        for mid in (rec.get("membership_ids") or set()):
            api_mids.add(mid)
    missing_in_api = sorted([m for m in membership_ids if m and m not in api_mids])[:50]
    extra_in_api = sorted([m for m in api_mids if m and m not in membership_ids])[:50]
    print(f"\nmembership_ids missing in API (first 50): {missing_in_api}")
    print(f"membership_ids extra in API (first 50): {extra_in_api}")
    return 0


async def _probe_joined_summary(args: argparse.Namespace) -> int:
    client, _wh = _init_client_from_local_config()
    if not client:
        print("Missing `whop_api.api_key` or `whop_api.company_id` in config.")
        return 2

    tz_name = str(args.tz or "America/New_York").strip() or "America/New_York"
    start_d = _parse_user_day(args.start) or datetime.now(timezone.utc).date()
    end_d = _parse_user_day(args.end) or start_d
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    start_local, end_local, tz_ok = _dt_local_range(start_d, end_d, tz_name)
    start_utc_iso = _isoz(start_local)
    end_utc_iso = _isoz(end_local)

    prefixes: list[str] = [p.strip() for p in (args.product_prefix or []) if str(p).strip()]

    # Membership mapping (optional, only for product breakdown).
    mber_map: dict[str, dict] = {}
    after: str | None = None
    pages_m = 0
    while pages_m < int(args.max_pages):
        batch, page_info = await client.list_memberships(
            first=int(args.per_page),
            after=after,
            params={"created_after": start_utc_iso, "created_before": end_utc_iso, "order": "created_at", "direction": "asc"},
        )
        if not batch:
            break
        pages_m += 1
        for m0 in batch:
            if not isinstance(m0, dict):
                continue
            st = str(m0.get("status") or "").strip().lower() or "unknown"
            if st == "drafted" and args.exclude_drafted:
                continue
            prod = _extract_product_title(m0)
            if prefixes:
                low = prod.lower()
                if not any(low.startswith(p.lower()) for p in prefixes):
                    continue
            mm = m0.get("member")
            mber_id = ""
            if isinstance(mm, dict):
                mber_id = str(mm.get("id") or "").strip()
            elif isinstance(mm, str):
                mber_id = mm.strip()
            if not mber_id:
                continue
            mid = str(m0.get("id") or "").strip() or str(m0.get("membership_id") or "").strip()
            rec = mber_map.get(mber_id)
            if not isinstance(rec, dict):
                rec = {"membership_ids": set(), "products": set()}
                mber_map[mber_id] = rec
            if mid:
                rec["membership_ids"].add(mid)
            if prod:
                rec["products"].add(prod)

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break

    # Members in range (joined_at desc; stop once before start)
    buckets = Counter()
    total = 0
    product_counts: dict[str, int] = {}
    unknown_products = 0
    unknown_members: list[dict] = []

    after = None
    pages_u = 0
    stop = False
    while pages_u < int(args.max_pages) and not stop:
        batch, page_info = await client.list_members(first=int(args.per_page), after=after, params={"order": "joined_at", "direction": "desc"})
        if not batch:
            break
        pages_u += 1
        for m in batch:
            if not isinstance(m, dict):
                continue
            mber_id = str(m.get("id") or "").strip()
            if not mber_id:
                continue
            joined_at = str(m.get("joined_at") or m.get("created_at") or "").strip()
            dtj = _parse_dt_any(joined_at)
            if not dtj:
                continue
            local_day = dtj.astimezone(start_local.tzinfo).date()  # type: ignore[arg-type]
            if local_day < start_d:
                stop = True
                break
            if local_day > end_d:
                continue

            status = str(m.get("status") or "").strip().lower() or "unknown"
            action = str(m.get("most_recent_action") or "").strip().lower()
            bucket = action if action in {"joined", "trialing", "canceling", "churned", "left", "past_due"} else status
            if bucket not in {"joined", "trialing", "canceling", "churned", "left", "past_due"}:
                bucket = "joined" if status == "joined" else ("left" if status == "left" else "joined")
            buckets[bucket] += 1
            total += 1

            rec = mber_map.get(mber_id)
            if isinstance(rec, dict) and rec.get("products"):
                for p in rec.get("products") or set():
                    ps = str(p or "").strip()
                    if ps:
                        product_counts[ps] = int(product_counts.get(ps, 0)) + 1
            else:
                unknown_products += 1
                u = m.get("user") if isinstance(m.get("user"), dict) else {}
                unknown_members.append(
                    {
                        "member_id": mber_id,
                        "user_id": str(u.get("id") or "").strip(),
                        "email": str(u.get("email") or "").strip(),
                        "name": str(u.get("name") or "").strip(),
                        "username": str(u.get("username") or "").strip(),
                        "status": str(m.get("status") or "").strip(),
                        "most_recent_action": str(m.get("most_recent_action") or "").strip(),
                    }
                )

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break

    churn_pct = (float(buckets.get("churned", 0)) / float(total) * 100.0) if total else 0.0

    print("=== Whop Joined Summary (probe) ===")
    print(f"tz_ok: {tz_ok} ({tz_name})")
    print(f"range: {start_d.isoformat()} -> {end_d.isoformat()}")
    print(f"users (range): {total}")
    for k in ("joined", "trialing", "canceling", "churned", "left", "past_due"):
        print(f"- {k}: {int(buckets.get(k, 0))}")
    print(f"- churn_pct: {churn_pct:.2f}%")
    if product_counts:
        print("by product:")
        for p, n in sorted(product_counts.items(), key=lambda kv: kv[1], reverse=True):
            print(f"- {p}: {n}")
    if unknown_products:
        print(f"unknown_products_rows: {unknown_products}")
        # Print the rows so you can identify them immediately.
        print("unknown_products (up to 10):")
        for r in unknown_members[:10]:
            nm = str(r.get("name") or r.get("username") or "").strip() or "(no name)"
            em = str(r.get("email") or "").strip() or "(no email)"
            print(f"- {nm} | {em} | member_id={r.get('member_id')} | action={r.get('most_recent_action')} status={r.get('status')}")

        # Optional deeper resolution: fetch all memberships for the user and list product titles.
        if bool(getattr(args, "resolve_unknown", False)):
            for r in unknown_members[:10]:
                uid = str(r.get("user_id") or "").strip()
                if not uid:
                    continue
                try:
                    ms = await client.get_user_memberships(uid)
                except Exception:
                    ms = []
                prods = []
                for mship in (ms or []):
                    if not isinstance(mship, dict):
                        continue
                    p = mship.get("product")
                    if isinstance(p, dict):
                        t = str(p.get("title") or "").strip()
                        if t:
                            prods.append(t)
                prods = sorted(set(prods))
                if prods:
                    print(f"  memberships for {uid}: {', '.join(prods)}")
    return 0


async def _probe_canceling(args: argparse.Namespace) -> int:
    cfg = load_config()
    wh = cfg.get("whop_api") if isinstance(cfg, dict) else {}
    wh = wh if isinstance(wh, dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    if not api_key or not company_id:
        print("Missing `whop_api.api_key` or `whop_api.company_id` in config.")
        return 2

    client = WhopAPIClient(api_key, base_url, company_id)
    per_page = int(args.per_page)
    max_pages = int(args.max_pages)
    max_rows = int(args.limit)
    email_filter = str(getattr(args, "email", "") or "").strip().lower()
    skip_remaining_gt = int(getattr(args, "skip_remaining_gt", 0) or 0)
    skip_remaining_gt = max(0, min(skip_remaining_gt, 3650))
    skip_keywords = [str(x or "").strip().lower() for x in (getattr(args, "skip_keyword", []) or []) if str(x or "").strip()]
    skip_keywords = sorted(set(skip_keywords))

    out: list[dict] = []
    after: str | None = None
    pages = 0
    while pages < max_pages and len(out) < max_rows:
        batch, page_info = await client.list_memberships(
            first=per_page,
            after=after,
            params={"statuses[]": "canceling", "order": "canceled_at", "direction": "asc"},
        )
        if not batch:
            break
        pages += 1
        for m in batch:
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id") or "").strip() or str(m.get("membership_id") or "").strip()
            status = str(m.get("status") or "").strip().lower()
            email = _extract_email(m)
            product = _extract_product_title(m)
            cape = _norm_bool(m.get("cancel_at_period_end"))
            created_at = str(m.get("created_at") or "").strip()
            mber_id = _extract_member_id(m)

            mrec = await client.get_member_by_id(mber_id) if mber_id else None
            did = 0
            if isinstance(mrec, dict) and mrec:
                raw = extract_discord_id_from_whop_member_record(mrec)
                if str(raw or "").strip().isdigit():
                    did = int(str(raw).strip())
                if not email:
                    u = mrec.get("user")
                    if isinstance(u, dict):
                        email = str(u.get("email") or "").strip()

            # Fetch full membership for renewal window + better fields.
            mfull = None
            if mid:
                with suppress(Exception):
                    mfull = await client.get_membership_by_id(mid)
            m_use = mfull if isinstance(mfull, dict) and mfull else m

            renewal_end_iso = str((m_use.get("renewal_period_end") if isinstance(m_use, dict) else "") or "").strip()
            remaining_days = ""
            dt_end = _parse_dt_any(renewal_end_iso) if renewal_end_iso else None
            if isinstance(dt_end, datetime):
                delta_s = (dt_end - datetime.now(timezone.utc)).total_seconds()
                remaining_days = str(max(0, int((delta_s / 86400.0) + 0.999)))

            if skip_remaining_gt > 0 and remaining_days.isdigit() and int(remaining_days) > skip_remaining_gt:
                continue

            latest_pay = await _best_payment_for_membership(client, mid) if (skip_keywords and mid) else {}
            if skip_keywords and isinstance(latest_pay, dict) and latest_pay:
                txt = " ".join(
                    [
                        str(latest_pay.get("status") or ""),
                        str(latest_pay.get("substatus") or ""),
                        str(latest_pay.get("billing_reason") or ""),
                        str(latest_pay.get("failure_message") or ""),
                    ]
                ).lower()
                if any(k in txt for k in skip_keywords):
                    continue

            if email_filter:
                if email_filter not in str(email or "").strip().lower():
                    continue

            membership_total_raw = _extract_total_spend_raw(m_use)
            member_total_raw = _extract_total_spend_raw(mrec) if isinstance(mrec, dict) else ""
            mem_amt, mem_found = _total_spend_usd(m_use)
            user_amt, user_found = _total_spend_usd(mrec) if isinstance(mrec, dict) else (0.0, False)
            if user_found and (not mem_found or float(user_amt) >= float(mem_amt)):
                total_spend = _fmt_usd_amt(float(user_amt))
            elif mem_found:
                total_spend = _fmt_usd_amt(float(mem_amt))
            else:
                total_spend = "N/A"

            out.append(
                {
                    "membership_id": mid,
                    "status": status,
                    "cancel_at_period_end": cape,
                    "created_at": created_at,
                    "email": email,
                    "product": product,
                    "discord_id": did,
                    "renewal_period_end": renewal_end_iso,
                    "remaining_days": remaining_days,
                    "latest_payment_status": str((latest_pay or {}).get("status") or "") if isinstance(latest_pay, dict) else "",
                    "total_spend_membership_raw": str(membership_total_raw),
                    "total_spend_member_raw": str(member_total_raw),
                    "total_spend_used": total_spend,
                }
            )
            if len(out) >= max_rows:
                break

        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break

    print("=== Whop API Probe: Canceling memberships ===")
    print(f"Rows: {len(out)} (pages={pages})")
    for r in out[: int(args.show)]:
        print(
            f"- {r.get('status'):9s} | cape={str(r.get('cancel_at_period_end')).lower():5s} | rem_days={str(r.get('remaining_days') or '-'):>3s} | pay={str(r.get('latest_payment_status') or '-'):>12s} | {r.get('product')} | {r.get('email')} | did={r.get('discord_id') or '-'} | spend={r.get('total_spend_used')} | mid={r.get('membership_id')}"
        )
    print("\nNote: `total_spend_used` prefers the member dashboard total when higher than membership payload.")
    return 0


def _init_client_from_local_config() -> tuple[WhopAPIClient | None, dict]:
    cfg = load_config()
    wh = cfg.get("whop_api") if isinstance(cfg, dict) else {}
    wh = wh if isinstance(wh, dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    if not api_key or not company_id:
        return (None, {})
    return (WhopAPIClient(api_key, base_url, company_id), wh)


def _parse_kv_params(kvs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in (kvs or []):
        s = str(item or "").strip()
        if not s or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


async def _probe_raw(args: argparse.Namespace) -> int:
    client, wh = _init_client_from_local_config()
    if not client:
        print("Missing `whop_api.api_key` or `whop_api.company_id` in config.")
        return 2

    endpoint = str(args.endpoint or "").strip()
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint

    params = _parse_kv_params(args.param or [])
    # Default company_id unless explicitly provided.
    if "company_id" not in params and isinstance(wh, dict) and str(wh.get("company_id") or "").strip():
        params["company_id"] = str(wh.get("company_id") or "").strip()

    print("=== Whop API Probe: RAW GET ===")
    print(f"endpoint: {endpoint}")
    if params:
        print(f"params: {params}")

    try:
        data = await client._request("GET", endpoint, params=params)  # type: ignore[attr-defined]
    except Exception as ex:
        print(f"ERROR: {ex}")
        return 1

    # Print shape + sample
    if isinstance(data, dict):
        keys = list(data.keys())
        print(f"top-level keys: {keys}")
        d = data.get("data")
        if isinstance(d, list):
            print(f"data: list (len={len(d)})")
            if d:
                print("first item keys:", list(d[0].keys()) if isinstance(d[0], dict) else type(d[0]))
        elif isinstance(d, dict):
            print("data: dict keys:", list(d.keys()))
        else:
            print("data:", type(d).__name__)
    else:
        print("response type:", type(data).__name__)

    if args.out:
        try:
            Path(args.out).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"saved: {args.out}")
        except Exception as ex:
            print(f"failed to write {args.out}: {ex}")
            return 1

    return 0


async def _probe_resolve_discord(args: argparse.Namespace) -> int:
    """Resolve Discord ID by scanning native whop-logs cards for an email."""
    cfg = load_config()
    token = str(cfg.get("bot_token") or "").strip()
    if not token:
        print("Missing bot_token in config.secrets.json")
        return 2

    try:
        guild_id = int(str(getattr(args, "guild_id", "") or cfg.get("guild_id") or 0).strip())
    except Exception:
        guild_id = 0
    if not guild_id:
        print("Missing guild_id.")
        return 2

    inv = cfg.get("invite_tracking") if isinstance(cfg, dict) else {}
    inv = inv if isinstance(inv, dict) else {}
    try:
        default_whop_logs = int(str(inv.get("whop_logs_channel_id") or 0).strip())
    except Exception:
        default_whop_logs = 0

    try:
        channel_id = int(str(getattr(args, "channel_id", "") or default_whop_logs or 0).strip())
    except Exception:
        channel_id = default_whop_logs
    if not channel_id:
        print("Missing whop-logs channel id (invite_tracking.whop_logs_channel_id).")
        return 2

    email_q = str(getattr(args, "email", "") or "").strip().lower()
    if not email_q or "@" not in email_q:
        print("Missing/invalid --email.")
        return 2

    hist_lim = int(getattr(args, "limit", 250) or 250)
    hist_lim = max(10, min(hist_lim, 500))
    show = int(getattr(args, "show", 3) or 3)
    show = max(1, min(show, 25))

    intents = discord.Intents.none()
    intents.guilds = True
    bot = discord.Client(intents=intents)

    @bot.event
    async def on_ready():
        g = bot.get_guild(guild_id)
        if g is None:
            with suppress(Exception):
                g = await bot.fetch_guild(guild_id)
        ch = bot.get_channel(channel_id)
        if ch is None:
            with suppress(Exception):
                ch = await bot.fetch_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            print(f"channel not found or not text: {channel_id}")
            with suppress(Exception):
                await bot.close()
            return

        found = 0
        scanned = 0
        samples: list[str] = []
        async for msg in ch.history(limit=hist_lim):
            scanned += 1
            e0 = msg.embeds[0] if msg.embeds else None
            if not isinstance(e0, discord.Embed):
                continue
            em = str(_extract_email_from_native_embed(e0) or "").strip().lower()
            if em and len(samples) < 10:
                samples.append(em)
            if not em or em != email_q:
                continue
            did = str(_extract_discord_id_from_native_embed(e0) or "").strip()
            title0 = str(getattr(e0, "title", "") or "").strip()
            print(f"match: email={em} did={did or '—'} title={title0 or '(no title)'} jump={str(getattr(msg,'jump_url','') or '')}")
            found += 1
            if found >= show:
                break

        if found <= 0:
            print(f"no matches found in last {hist_lim} messages (scanned={scanned}).")
            if samples:
                print("sample extracted emails (up to 10):")
                for s in samples:
                    print(f"- {s}")
            else:
                print("note: no emails could be extracted from embeds in this window (parser mismatch or cards are not embeds).")

        with suppress(Exception):
            await bot.close()

    async with bot:
        await bot.start(token)
    return 0


def _looks_like_dispute(payment: dict) -> bool:
    if not isinstance(payment, dict):
        return False
    if payment.get("dispute_alerted_at") or payment.get("disputed_at") or payment.get("chargeback_at"):
        return True
    status = str(payment.get("status") or "").lower()
    substatus = str(payment.get("substatus") or "").lower()
    billing_reason = str(payment.get("billing_reason") or "").lower()
    txt = " ".join([status, substatus, billing_reason])
    return any(w in txt for w in ("dispute", "chargeback"))


def _looks_like_resolution_needed(payment: dict) -> bool:
    if not isinstance(payment, dict):
        return False
    if _looks_like_dispute(payment):
        return True
    status = str(payment.get("status") or "").lower()
    substatus = str(payment.get("substatus") or "").lower()
    billing_reason = str(payment.get("billing_reason") or "").lower()
    txt = " ".join([status, substatus, billing_reason])
    if any(w in txt for w in ("failed", "past_due", "unpaid", "billing_issue", "canceled", "cancelled", "refunded")):
        return True
    if payment.get("failure_message") or payment.get("failure_code"):
        return True
    if payment.get("refunded_at") or str(payment.get("refunded_amount") or "").strip():
        return True
    return False


async def _probe_alerts(args: argparse.Namespace) -> int:
    client, wh = _init_client_from_local_config()
    if not client:
        print("Missing `whop_api.api_key` or `whop_api.company_id` in config.")
        return 2

    company_id = str(wh.get("company_id") or "").strip()
    max_pages = max(1, min(int(args.max_pages), 50))
    first = max(1, min(int(args.first), 200))
    after: str | None = None

    alerts: list[dict] = []
    pages = 0
    while pages < max_pages and len(alerts) < int(args.limit):
        q: dict[str, object] = {"company_id": company_id}
        # Many Whop endpoints accept cursor pagination; try it if supported.
        q["first"] = first
        if after:
            q["after"] = after
        try:
            resp = await client._request("GET", "/payments", params=q)  # type: ignore[attr-defined]
        except Exception as ex:
            print(f"ERROR calling /payments: {ex}")
            return 1
        pages += 1
        data = resp.get("data") if isinstance(resp, dict) else None
        if not isinstance(data, list) or not data:
            break
        for p in data:
            if not isinstance(p, dict):
                continue
            kind = ""
            if _looks_like_dispute(p):
                kind = "dispute"
            elif _looks_like_resolution_needed(p):
                kind = "resolution"
            else:
                continue
            alerts.append({"kind": kind, "payment": p})
            if len(alerts) >= int(args.limit):
                break
        page_info = resp.get("page_info") if isinstance(resp, dict) else None
        if isinstance(page_info, dict) and page_info.get("has_next_page") and page_info.get("end_cursor"):
            after = str(page_info.get("end_cursor") or "")
            continue
        break

    disputes = [a for a in alerts if a.get("kind") == "dispute"]
    resolutions = [a for a in alerts if a.get("kind") == "resolution"]
    print("=== Whop API Probe: Dispute/Resolution signals (from /payments) ===")
    print(f"pages_scanned: {pages}")
    print(f"alerts_found: {len(alerts)} (dispute={len(disputes)} resolution={len(resolutions)})")

    def _pid(p: dict) -> str:
        return str(p.get("id") or p.get("payment_id") or "").strip()

    def _mid(p: dict) -> str:
        v = p.get("membership_id") or p.get("membership") or ""
        if isinstance(v, dict):
            return str(v.get("id") or v.get("membership_id") or "").strip()
        return str(v or "").strip()

    def _status(p: dict) -> str:
        return str(p.get("status") or "").strip().lower()

    show = int(args.show)
    for a in alerts[:show]:
        p = a.get("payment") if isinstance(a.get("payment"), dict) else {}
        kind = str(a.get("kind") or "")
        print(f"- {kind:10s} | status={_status(p):10s} | mid={_mid(p) or '—'} | pay={_pid(p) or '—'}")
    if args.out:
        try:
            Path(args.out).write_text(json.dumps(alerts, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"saved: {args.out}")
        except Exception as ex:
            print(f"failed to write {args.out}: {ex}")
            return 1
    return 0


async def _probe_staffcards(args: argparse.Namespace) -> int:
    cfg = load_config()
    token = str(cfg.get("bot_token") or "").strip()
    if not token:
        print("Missing bot_token in config.secrets.json")
        return 2

    wh = cfg.get("whop_api") if isinstance(cfg, dict) else {}
    wh = wh if isinstance(wh, dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    if not api_key or not company_id:
        print("Missing whop_api.api_key or whop_api.company_id in config/secrets.")
        return 2

    try:
        guild_id = int(str(args.guild_id or cfg.get("guild_id") or 0).strip())
    except Exception:
        guild_id = 0
    if not guild_id:
        print("Missing guild_id.")
        return 2

    # Default channel: dm_sequence.member_status_logs_channel_id (source + dest unless overridden)
    dm = cfg.get("dm_sequence") if isinstance(cfg, dict) else {}
    dm = dm if isinstance(dm, dict) else {}
    try:
        default_ch = int(str(dm.get("member_status_logs_channel_id") or 0).strip())
    except Exception:
        default_ch = 0
    try:
        source_channel_id = int(str(getattr(args, "source_channel_id", "") or args.channel_id or default_ch or 0).strip())
    except Exception:
        source_channel_id = 0

    # Destination channel: default to source channel; optionally override via config whop_enrichment smoketest channel id.
    wh_en = cfg.get("whop_enrichment") if isinstance(cfg, dict) else {}
    wh_en = wh_en if isinstance(wh_en, dict) else {}
    try:
        default_dest = int(str(wh_en.get("startup_native_smoketest_output_channel_id") or 0).strip())
    except Exception:
        default_dest = 0
    try:
        dest_channel_id = int(str(getattr(args, "dest_channel_id", "") or (default_dest or source_channel_id or 0)).strip())
    except Exception:
        dest_channel_id = source_channel_id

    if (not source_channel_id) and bool(args.history):
        print("Missing source_channel_id (and no default in config).")
        return 2
    if not dest_channel_id and bool(args.post):
        print("Missing dest_channel_id (and no default in config).")
        return 2

    # Optional: dedupe across runs (prevents accidental duplicates in the output channel).
    force = bool(getattr(args, "force", False))
    dedupe = not force
    dedupe_key_prefix = f"{source_channel_id}:"
    dedupe_state = _load_json_file(_PROBE_STAFFCARDS_DEDUPE_FILE)
    if not isinstance(dedupe_state, dict):
        dedupe_state = {}
    sent_map = dedupe_state.get(str(dest_channel_id)) if isinstance(dedupe_state.get(str(dest_channel_id)), dict) else {}
    if not isinstance(sent_map, dict):
        sent_map = {}

    # Member history fallback: discord_id -> last_membership_id
    hist_db = _load_json_file(BASE_DIR / "member_history.json")
    if not isinstance(hist_db, dict):
        hist_db = {}

    def _mid_from_history(did: int) -> str:
        try:
            rec = hist_db.get(str(int(did))) if did else None
        except Exception:
            rec = None
        if not isinstance(rec, dict):
            return ""
        wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
        if not isinstance(wh, dict):
            wh = {}
        mid0 = str(wh.get("last_membership_id") or wh.get("last_whop_key") or "").strip()
        return mid0

    # Role IDs for Current Roles
    try:
        role_trigger = int(str(dm.get("role_trigger") or 0).strip())
    except Exception:
        role_trigger = 0
    try:
        welcome_role_id = int(str(dm.get("welcome_role_id") or 0).strip())
    except Exception:
        welcome_role_id = 0
    try:
        role_cancel_a = int(str(dm.get("role_cancel_a") or 0).strip())
    except Exception:
        role_cancel_a = 0
    try:
        role_cancel_b = int(str(dm.get("role_cancel_b") or 0).strip())
    except Exception:
        role_cancel_b = 0
    relevant_roles = coerce_role_ids(role_trigger, welcome_role_id, role_cancel_a, role_cancel_b)

    def _extract_discord_id(*parts: object) -> int:
        blob = " ".join(str(p or "") for p in parts)
        m = re.search(r"\b(\d{17,19})\b", blob)
        return int(m.group(1)) if m else 0

    def _extract_whop_membership_id(*parts: object) -> str:
        blob = " ".join(str(p or "") for p in parts)
        m = re.search(r"\b(mem_[A-Za-z0-9]+)\b", blob)
        if m:
            return m.group(1)
        # Many staff cards use the Whop "key" format (R-...) as membership identifier.
        m2 = re.search(r"\b(R-[A-Za-z0-9-]{8,}W)\b", blob)
        if m2:
            return m2.group(1)
        m3 = re.search(r"\b(R-[A-Za-z0-9-]{8,})\b", blob)
        return m3.group(1) if m3 else ""

    def _extract_whop_user_id_from_dashboard(*parts: object) -> str:
        blob = " ".join(str(p or "") for p in parts)
        m = re.search(r"/users/(user_[A-Za-z0-9]+)/", blob)
        return m.group(1) if m else ""

    def _infer_kind_from_title(t: str) -> str:
        low = str(t or "").lower()
        if "payment failed" in low or "billing issue" in low or "access risk" in low:
            return "payment_failed"
        if "cancellation scheduled" in low or "set to cancel" in low or "canceling" in low:
            return "cancellation_scheduled"
        if "member joined" in low:
            return "member_joined"
        if "member left" in low:
            return "member_left"
        if "access ended" in low or "deactivated" in low:
            return "deactivated"
        return "active"

    def _infer_color(kind: str) -> int:
        k = str(kind or "").lower()
        if k == "payment_failed":
            return 0xED4245
        if k == "cancellation_scheduled":
            return 0xFEE75C
        if k == "deactivated":
            return 0xFEE75C
        return 0x5865F2

    async def _best_membership_id_for_user(client0: WhopAPIClient, user_id: str) -> str:
        uid = str(user_id or "").strip()
        if not uid:
            return ""
        try:
            ms = await client0.get_user_memberships(uid)
        except Exception:
            ms = []
        pool = [m for m in (ms or []) if isinstance(m, dict)]
        if not pool:
            return ""
        prio = {"past_due": 1, "unpaid": 1, "trialing": 2, "active": 3, "canceling": 4, "pending": 5, "canceled": 20, "cancelled": 20, "completed": 21, "expired": 22}

        def _status(m: dict) -> str:
            return str(m.get("status") or "").strip().lower()

        def _ts(m: dict) -> str:
            return str(m.get("created_at") or "").strip()

        pool.sort(key=lambda m: (prio.get(_status(m), 99), _ts(m)), reverse=False)
        mid = str(pool[0].get("id") or pool[0].get("membership_id") or "").strip()
        return mid

    client = WhopAPIClient(api_key, base_url, company_id)

    intents = discord.Intents.none()
    intents.guilds = True
    intents.members = True

    bot = discord.Client(intents=intents)

    @bot.event
    async def on_ready():
        g = bot.get_guild(guild_id)
        if g is None:
            with suppress(Exception):
                g = await bot.fetch_guild(guild_id)

        src_ch = bot.get_channel(source_channel_id) if source_channel_id else None
        if src_ch is None and source_channel_id:
            with suppress(Exception):
                src_ch = await bot.fetch_channel(source_channel_id)

        out_ch = bot.get_channel(dest_channel_id) if dest_channel_id else None
        if out_ch is None and dest_channel_id:
            with suppress(Exception):
                out_ch = await bot.fetch_channel(dest_channel_id)

        # Case channels (optional; by name in the same guild as dest channel if available).
        post_cases = bool(getattr(args, "post_cases", False))
        payment_case_name = str(getattr(args, "payment_case_channel_name", "payment-failure") or "payment-failure").strip().lower()
        cancel_case_name = str(getattr(args, "cancel_case_channel_name", "member-cancelation") or "member-cancelation").strip().lower()
        case_payment = None
        case_cancel = None
        try:
            out_guild = getattr(out_ch, "guild", None)
        except Exception:
            out_guild = None
        if post_cases and isinstance(out_guild, discord.Guild):
            for ch0 in (out_guild.text_channels or []):
                if not isinstance(ch0, discord.TextChannel):
                    continue
                nm = str(getattr(ch0, "name", "") or "").strip().lower()
                if nm == payment_case_name:
                    case_payment = ch0
                elif nm == cancel_case_name:
                    case_cancel = ch0

        delay_ms = int(getattr(args, "delay_ms", 800) or 0)
        delay_ms = max(0, min(delay_ms, 5000))

        built = 0
        posted = 0
        posted_cases = 0

        # Build worklist (either from member_history or from channel history)
        work: list[dict] = []

        if bool(args.history):
            if not isinstance(src_ch, discord.TextChannel):
                print(f"source_channel_id not found or not text: {source_channel_id}")
                await bot.close()
                return
            hist_lim = int(getattr(args, "history_limit", 50) or 50)
            hist_lim = max(1, min(hist_lim, 500))
            async for msg in src_ch.history(limit=hist_lim):
                e0 = msg.embeds[0] if msg.embeds else None
                if not isinstance(e0, discord.Embed):
                    continue
                title0 = str(getattr(e0, "title", "") or "").strip()
                desc0 = str(getattr(e0, "description", "") or "").strip()
                fields0 = getattr(e0, "fields", None) or []
                blob_fields = " ".join([f"{getattr(f,'name','')}: {getattr(f,'value','')}" for f in fields0])
                did = _extract_discord_id(title0, desc0, blob_fields, str(getattr(msg, "content", "") or ""))
                if not did:
                    continue
                mid = _extract_whop_membership_id(title0, desc0, blob_fields)
                user_id = _extract_whop_user_id_from_dashboard(title0, desc0, blob_fields)
                kind0 = _infer_kind_from_title(title0)
                work.append(
                    {
                        "did": did,
                        "membership_id": mid,
                        "whop_user_id": user_id,
                        "orig_title": title0 or "(no title)",
                        "orig_jump": str(getattr(msg, "jump_url", "") or "").strip(),
                        "kind": kind0,
                        "source_message_id": int(getattr(msg, "id", 0) or 0),
                    }
                )
                if len(work) >= int(args.limit):
                    break
        else:
            hist = _load_json_file(BASE_DIR / "member_history.json")
            if isinstance(hist, dict):
                for did_s, rec in hist.items():
                    if not str(did_s).strip().isdigit():
                        continue
                    whp = (rec.get("whop") if isinstance(rec, dict) else None) if isinstance(rec, dict) else None
                    whp = whp if isinstance(whp, dict) else {}
                    mid = str(whp.get("last_membership_id") or whp.get("last_whop_key") or "").strip()
                    if not mid:
                        continue
                    work.append({"did": int(did_s), "membership_id": mid, "whop_user_id": "", "orig_title": "", "orig_jump": "", "kind": ""})
                    if len(work) >= int(args.limit):
                        break

        if not work:
            print("No work items found (history empty or member_history has no mids).")
            with suppress(Exception):
                await bot.close()
            return

        for item in work:
            did = int(item.get("did") or 0)
            mid = str(item.get("membership_id") or "").strip()
            user_id = str(item.get("whop_user_id") or "").strip()
            orig_title = str(item.get("orig_title") or "").strip()
            jump = str(item.get("orig_jump") or "").strip()
            kind = str(item.get("kind") or "").strip().lower()
            source_msg_id = int(item.get("source_message_id") or 0)

            if dedupe and source_msg_id:
                k = f"{dedupe_key_prefix}{source_msg_id}"
                if k in sent_map:
                    continue

            # Resolve member (for correct embed header + roles)
            try:
                member = g.get_member(did) if g else None
                if member is None and g is not None:
                    member = await g.fetch_member(did)
            except Exception:
                member = None
            if member is None:
                continue

            # If no membership id, try to infer from dashboard user id.
            if (not mid) and user_id:
                with suppress(Exception):
                    mid = await _best_membership_id_for_user(client, user_id)

            # Final fallback: use local member_history for this Discord ID.
            if not mid:
                mid = _mid_from_history(did)

            # API-only whop brief (best-effort)
            whop_brief: dict = {}
            if mid:
                with suppress(Exception):
                    whop_brief = await _whop_brief_api_only(client, mid)

            # If we didn't have an original kind (member_history mode), infer from whop status flags.
            if not kind:
                st = str(whop_brief.get("status") or "").strip().lower()
                cape = str(whop_brief.get("cancel_at_period_end") or "").strip().lower() == "yes"
                if st in {"past_due", "unpaid"}:
                    kind = "payment_failed"
                elif cape:
                    kind = "cancellation_scheduled"
                elif st in {"canceled", "cancelled", "completed", "expired"}:
                    kind = "deactivated"
                else:
                    kind = "active"

            color = _infer_color(kind)
            access = access_roles_plain(member, relevant_roles)
            member_kv = [("event", "whop.api.probe")]

            embed = build_member_status_detailed_embed(
                title=f"[API PROBE] {orig_title or 'Member Status'}",
                member=member,
                access_roles=access,
                color=color,
                discord_kv=None,
                member_kv=member_kv,
                whop_brief=whop_brief if whop_brief else {},
                event_kind=("payment_failed" if kind == "payment_failed" else ("cancellation_scheduled" if kind == "cancellation_scheduled" else ("deactivated" if kind == "deactivated" else "active"))),
                force_whop_core_fields=False,
            )
            built += 1

            if bool(args.post) and isinstance(out_ch, discord.abc.Messageable):
                with suppress(Exception):
                    await out_ch.send(content=member.mention, embed=embed, allowed_mentions=discord.AllowedMentions.none())
                    posted += 1
                    if dedupe and source_msg_id:
                        sent_map[f"{dedupe_key_prefix}{source_msg_id}"] = datetime.now(timezone.utc).isoformat()

                if post_cases and kind in {"payment_failed", "cancellation_scheduled"}:
                    try:
                        if kind == "payment_failed" and isinstance(case_payment, discord.TextChannel):
                            mini = build_case_minimal_embed(
                                title=f"[API PROBE] {orig_title or 'Payment Failed'}",
                                member=member,
                                access_roles=access,
                                whop_brief=whop_brief,
                                color=0xED4245,
                                event_kind="payment_failed",
                            )
                            await case_payment.send(embed=mini, allowed_mentions=discord.AllowedMentions.none())
                            posted_cases += 1
                        elif kind == "cancellation_scheduled" and isinstance(case_cancel, discord.TextChannel):
                            mini = build_case_minimal_embed(
                                title=f"[API PROBE] {orig_title or 'Cancellation Scheduled'}",
                                member=member,
                                access_roles=access,
                                whop_brief=whop_brief,
                                color=0xFEE75C,
                                event_kind="cancellation_scheduled",
                            )
                            await case_cancel.send(embed=mini, allowed_mentions=discord.AllowedMentions.none())
                            posted_cases += 1
                    except Exception:
                        pass

            if delay_ms:
                await asyncio.sleep(float(delay_ms) / 1000.0)

        if dedupe:
            dedupe_state[str(dest_channel_id)] = sent_map
            _save_json_file(_PROBE_STAFFCARDS_DEDUPE_FILE, dedupe_state)

        print(
            f"done. built={built} posted={posted} posted_cases={posted_cases} "
            f"source_channel_id={source_channel_id} dest_channel_id={dest_channel_id}"
        )
        with suppress(Exception):
            await bot.close()

    # Use async context manager for cleaner shutdown (avoids aiohttp connector warnings).
    async with bot:
        await bot.start(token)
    return 0


def _parse_dt_any(ts: object) -> Optional[datetime]:
    try:
        if ts is None or ts == "":
            return None
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        s = str(ts).strip()
        if not s:
            return None
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        return None
    except Exception:
        return None


def main() -> int:
    p = argparse.ArgumentParser(description="Whop API probe using local config + secrets (no Discord bot).")
    sub = p.add_subparsers(dest="mode", required=True)

    pj = sub.add_parser("joined", help="Probe joined range using created_after/created_before on memberships (Joined at).")
    pj.add_argument("--start", required=True, help="Start date (e.g. 01-26-26)")
    pj.add_argument("--end", required=False, default="", help="End date (e.g. 01-30-26). If omitted, uses start.")
    pj.add_argument("--tz", default="America/New_York", help="Timezone for day boundaries.")
    pj.add_argument("--product-prefix", action="append", default=[], help="Product title prefix filter (repeatable).")
    pj.add_argument("--status", action="append", default=[], help="Allow only this status (repeatable).")
    pj.add_argument("--exclude-drafted", action="store_true", default=True, help="Exclude drafted attempts.")
    pj.add_argument("--max-pages", type=int, default=50)
    pj.add_argument("--per-page", type=int, default=100)
    pj.add_argument("--show", type=int, default=20)

    pc = sub.add_parser("canceling", help="Probe canceling memberships and show spend/discord enrichment.")
    pc.add_argument("--max-pages", type=int, default=10)
    pc.add_argument("--per-page", type=int, default=100)
    pc.add_argument("--limit", type=int, default=50)
    pc.add_argument("--show", type=int, default=20)
    pc.add_argument("--email", default="", help="Filter by email substring (case-insensitive).")
    pc.add_argument("--skip-remaining-gt", type=int, default=0, help="Skip rows with remaining days > N (helps remove stale canceling rows).")
    pc.add_argument("--skip-keyword", action="append", default=[], help="Skip rows if latest payment contains keyword (repeatable).")

    pr = sub.add_parser("raw", help="Raw GET any Whop endpoint (debug/confirm fields).")
    pr.add_argument("--endpoint", required=True, help="Endpoint path like /payments or /disputes (leading / optional).")
    pr.add_argument("--param", action="append", default=[], help="Query param key=value (repeatable).")
    pr.add_argument("--out", default="", help="Optional output JSON file path.")

    pres = sub.add_parser("resolve-discord", help="Scan whop-logs and resolve Discord ID by email.")
    pres.add_argument("--email", required=True, help="Exact email address to match.")
    pres.add_argument("--limit", type=int, default=250, help="How many recent whop-logs messages to scan.")
    pres.add_argument("--show", type=int, default=3, help="How many matches to print.")
    pres.add_argument("--guild-id", default="", help="Override guild id (defaults to config guild_id).")
    pres.add_argument("--channel-id", default="", help="Override whop-logs channel id (defaults to invite_tracking.whop_logs_channel_id).")

    pa = sub.add_parser("alerts", help="Scan /payments and print dispute/resolution-like signals.")
    pa.add_argument("--max-pages", type=int, default=5)
    pa.add_argument("--first", type=int, default=100)
    pa.add_argument("--limit", type=int, default=50)
    pa.add_argument("--show", type=int, default=20)
    pa.add_argument("--out", default="", help="Optional output JSON file path.")

    px = sub.add_parser("compare-csv", help="Compare Whop Users CSV export against API-based aggregation.")
    px.add_argument("--csv", required=True, help="Path to Whop Users export CSV.")
    px.add_argument("--start", required=True, help="Start date (e.g. 01-26-26)")
    px.add_argument("--end", required=False, default="", help="End date (e.g. 01-30-26). If omitted, uses start.")
    px.add_argument("--tz", default="America/New_York", help="Timezone for day boundaries.")
    px.add_argument("--product-prefix", action="append", default=[], help="Product title prefix filter (repeatable).")
    px.add_argument("--exclude-drafted", action="store_true", default=True, help="Exclude drafted attempts.")
    px.add_argument("--max-pages", type=int, default=50)
    px.add_argument("--per-page", type=int, default=100)

    psum = sub.add_parser("joined-summary", help="Print a Whop Joined Summary using /members + membership mapping.")
    psum.add_argument("--start", required=True, help="Start date (e.g. 01-26-26)")
    psum.add_argument("--end", required=False, default="", help="End date (e.g. 01-30-26). If omitted, uses start.")
    psum.add_argument("--tz", default="America/New_York", help="Timezone for day boundaries.")
    psum.add_argument("--product-prefix", action="append", default=[], help="Product title prefix filter (repeatable).")
    psum.add_argument("--exclude-drafted", action="store_true", default=True, help="Exclude drafted attempts.")
    psum.add_argument("--resolve-unknown", action="store_true", default=False, help="For unknown product rows, fetch user memberships and list product titles.")
    psum.add_argument("--max-pages", type=int, default=50)
    psum.add_argument("--per-page", type=int, default=100)

    psc = sub.add_parser("staffcards", help="Post member-status style embeds built with Whop API only (no logs).")
    psc.add_argument("--guild-id", default="", help="Discord guild ID (defaults to config guild_id).")
    psc.add_argument("--channel-id", default="", help="(Legacy) Destination channel ID (defaults to dm_sequence.member_status_logs_channel_id).")
    psc.add_argument("--source-channel-id", default="", help="Source channel to read history from when --history is set.")
    psc.add_argument("--dest-channel-id", default="", help="Destination channel to post rebuilt embeds to (defaults to smoketest output channel or source).")
    psc.add_argument("--limit", type=int, default=5, help="How many members from member_history to test.")
    psc.add_argument("--post", action="store_true", default=False, help="Actually post to Discord (otherwise prints).")
    psc.add_argument("--history", action="store_true", default=False, help="Use member-status-logs message history as the input set (recommended).")
    psc.add_argument("--history-limit", type=int, default=50, help="How many messages to scan in the source channel when --history is used.")
    psc.add_argument("--post-cases", action="store_true", default=False, help="Also post minimal case embeds to #payment-failure/#member-cancelation (by name) in the dest guild.")
    psc.add_argument("--payment-case-channel-name", default="payment-failure", help="Case channel name for payment failures.")
    psc.add_argument("--cancel-case-channel-name", default="member-cancelation", help="Case channel name for cancellation scheduled.")
    psc.add_argument("--delay-ms", type=int, default=800, help="Delay between posts (ms) so you can watch 1-by-1.")
    psc.add_argument("--force", action="store_true", default=False, help="Disable dedupe and repost even if already posted before.")

    args = p.parse_args()
    if args.mode == "joined":
        if not args.end:
            args.end = args.start
        return asyncio.run(_probe_joined(args))
    if args.mode == "canceling":
        return asyncio.run(_probe_canceling(args))
    if args.mode == "raw":
        return asyncio.run(_probe_raw(args))
    if args.mode == "resolve-discord":
        return asyncio.run(_probe_resolve_discord(args))
    if args.mode == "alerts":
        return asyncio.run(_probe_alerts(args))
    if args.mode == "compare-csv":
        if not args.end:
            args.end = args.start
        return asyncio.run(_probe_compare_csv(args))
    if args.mode == "joined-summary":
        if not args.end:
            args.end = args.start
        return asyncio.run(_probe_joined_summary(args))
    if args.mode == "staffcards":
        return asyncio.run(_probe_staffcards(args))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

