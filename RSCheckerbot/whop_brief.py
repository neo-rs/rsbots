from __future__ import annotations

import logging
import json
import re
from contextlib import suppress
from datetime import datetime, timezone
from math import ceil
from pathlib import Path

from whop_api_client import WhopAPIClient
from rschecker_utils import extract_discord_id_from_whop_member_record
from rschecker_utils import fmt_date_any as _fmt_date_any, parse_dt_any as _parse_dt_any, usd_amount

log = logging.getLogger("rs-checker")

_BASE_DIR = Path(__file__).resolve().parent
_MEMBER_HISTORY_PATH = _BASE_DIR / "member_history.json"
_MH_CACHE: dict | None = None
_MH_CACHE_MTIME: float = 0.0
_MH_CACHE_AT: float = 0.0


def _now_ts() -> float:
    try:
        return float(datetime.now(timezone.utc).timestamp())
    except Exception:
        return 0.0


def _load_membership_baseline_cached(*, max_age_seconds: int = 30) -> dict:
    """Load only the membership baseline slices from member_history.json (cached)."""
    global _MH_CACHE, _MH_CACHE_MTIME, _MH_CACHE_AT
    now = _now_ts()
    if _MH_CACHE is not None and _MH_CACHE_AT and (now - _MH_CACHE_AT) < float(max_age_seconds):
        return _MH_CACHE
    mtime = 0.0
    try:
        mtime = float(_MEMBER_HISTORY_PATH.stat().st_mtime)
    except Exception:
        mtime = 0.0
    if _MH_CACHE is not None and mtime and _MH_CACHE_MTIME and mtime == _MH_CACHE_MTIME:
        _MH_CACHE_AT = now
        return _MH_CACHE
    data: dict = {}
    try:
        raw = json.loads(_MEMBER_HISTORY_PATH.read_text(encoding="utf-8") or "{}")
        if isinstance(raw, dict):
            data = raw
    except Exception:
        data = {}
    # Keep only what we need (reduces memory churn).
    out = {
        "whop_users": data.get("whop_users") if isinstance(data.get("whop_users"), dict) else {},
        "whop_user_index": data.get("whop_user_index") if isinstance(data.get("whop_user_index"), dict) else {},
        "whop_membership_index": data.get("whop_membership_index") if isinstance(data.get("whop_membership_index"), dict) else {},
    }
    _MH_CACHE = out
    _MH_CACHE_MTIME = mtime
    _MH_CACHE_AT = now
    return out


def _extract_user_id_from_dashboard_url(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    # Markdown link [Open](https://...)
    m = re.search(r"\((https?://[^)]+)\)", s)
    if m:
        s = m.group(1).strip()
    m2 = re.search(r"/users/(user_[A-Za-z0-9]+)/", s)
    return m2.group(1) if m2 else ""


def _best_membership_log_entry(wh_rec: dict, *, membership_id: str = "") -> dict:
    """Pick the most relevant membership_logs_latest entry (preferring matching membership_id)."""
    if not isinstance(wh_rec, dict):
        return {}
    wh = wh_rec.get("whop") if isinstance(wh_rec.get("whop"), dict) else {}
    latest = wh.get("membership_logs_latest") if isinstance(wh.get("membership_logs_latest"), dict) else {}
    if not isinstance(latest, dict) or not latest:
        return {}
    mid = str(membership_id or "").strip()
    if mid:
        best_m: dict | None = None
        best_k_m = ""
        for v in latest.values():
            if not (isinstance(v, dict) and str(v.get("membership_id") or "").strip() == mid):
                continue
            k = str(v.get("recorded_at") or v.get("created_at") or "")
            if (best_m is None) or (k > best_k_m):
                best_m = v
                best_k_m = k
        if isinstance(best_m, dict):
            return best_m
    # Otherwise pick newest by recorded_at then created_at.
    best: dict | None = None
    best_k = ""
    for v in latest.values():
        if not isinstance(v, dict):
            continue
        k = str(v.get("recorded_at") or v.get("created_at") or "")
        if (best is None) or (k > best_k):
            best = v
            best_k = k
    return best if isinstance(best, dict) else {}


def _merged_membership_log_fields(wh_rec: dict, *, membership_id: str = "") -> dict[str, str]:
    """Merge fields across membership_logs_latest newest-first (PII-safe, best-effort)."""
    if not isinstance(wh_rec, dict):
        return {}
    wh = wh_rec.get("whop") if isinstance(wh_rec.get("whop"), dict) else {}
    latest = wh.get("membership_logs_latest") if isinstance(wh.get("membership_logs_latest"), dict) else {}
    if not isinstance(latest, dict) or not latest:
        return {}
    mid = str(membership_id or "").strip()
    rows: list[dict] = []
    for v in latest.values():
        if not isinstance(v, dict):
            continue
        if mid:
            v_mid = str(v.get("membership_id") or "").strip()
            if v_mid and v_mid != mid:
                continue
        rows.append(v)

    def _k(v: dict) -> str:
        return str(v.get("recorded_at") or v.get("created_at") or "")

    rows.sort(key=_k, reverse=True)
    out: dict[str, str] = {}
    for v in rows:
        f = v.get("fields") if isinstance(v.get("fields"), dict) else {}
        if not isinstance(f, dict):
            continue
        for kk, vv in f.items():
            k2 = str(kk or "").strip().lower()
            if not k2:
                continue
            if k2 in out:
                continue
            s = str(vv or "").strip()
            if not s:
                continue
            out[k2] = s
    return out


def enrich_whop_brief_from_membership_logs(brief: dict | None, *, membership_id: str = "") -> dict:
    """Fill missing fields in a whop_brief from membership-log baseline (no extra API calls)."""
    b = brief if isinstance(brief, dict) else {}
    mid = str(membership_id or b.get("membership_id") or "").strip()
    uid = str(b.get("whop_user_id") or "").strip()
    if not uid:
        uid = _extract_user_id_from_dashboard_url(str(b.get("dashboard_url") or ""))
    # Pull baseline slices
    base = _load_membership_baseline_cached()
    wh_users = base.get("whop_users") if isinstance(base.get("whop_users"), dict) else {}
    wh_idx = base.get("whop_user_index") if isinstance(base.get("whop_user_index"), dict) else {}
    mem_idx = base.get("whop_membership_index") if isinstance(base.get("whop_membership_index"), dict) else {}

    key = ""
    if uid and uid in wh_users:
        key = uid
    if not key and mid and isinstance(mem_idx, dict) and mid in mem_idx:
        key = str(mem_idx.get(mid) or "").strip()
    # Fallback: find by last_membership_id (linear scan; used only if index missing)
    if not key and mid:
        try:
            for k0, rec0 in wh_users.items():
                if not isinstance(rec0, dict):
                    continue
                wh0 = rec0.get("whop") if isinstance(rec0.get("whop"), dict) else {}
                if str(wh0.get("last_membership_id") or "").strip() == mid:
                    key = str(k0)
                    break
        except Exception:
            key = ""

    # Fallback: if we have a username from prior brief, use username index
    if not key:
        uname = str(b.get("username") or b.get("user_name") or "").strip().lower()
        uname = re.sub(r"[^a-z0-9_.-]+", "", uname)
        if uname and uname in wh_idx:
            key = str(wh_idx.get(uname) or "").strip()

    rec = wh_users.get(key) if key and isinstance(wh_users, dict) else None
    if not isinstance(rec, dict):
        return b

    fields = _merged_membership_log_fields(rec, membership_id=mid)
    if not fields:
        return b

    def _get(*names: str) -> str:
        for nm in names:
            v = fields.get(str(nm).strip().lower())
            if v is None:
                continue
            s = str(v or "").strip()
            if s:
                return s
        return ""

    # Always attach stable IDs when missing.
    if not str(b.get("membership_id") or "").strip():
        b["membership_id"] = _get("membership id") or mid
    if not str(b.get("whop_user_id") or "").strip():
        b["whop_user_id"] = _get("whop user id") or uid
    if not str(b.get("username") or "").strip():
        b["username"] = _get("username")

    # Fill staff-facing fields if missing/blank.
    if (not str(b.get("status") or "").strip()) or str(b.get("status") or "").strip() == "—":
        b["status"] = _get("status") or b.get("status") or ""
    if (not str(b.get("product") or "").strip()) or str(b.get("product") or "").strip() == "—":
        b["product"] = _get("product", "membership") or b.get("product") or ""
    if not str(b.get("total_spent") or "").strip():
        b["total_spent"] = _get("total spent")
    if not str(b.get("trial_days") or "").strip():
        b["trial_days"] = _get("trial days")
    if not str(b.get("plan_is_renewal") or "").strip():
        b["plan_is_renewal"] = _get("plan is renewal")
    if not str(b.get("pricing") or "").strip():
        b["pricing"] = _get("pricing")
    if not str(b.get("is_first_membership") or "").strip():
        b["is_first_membership"] = _get("first membership")
    if not str(b.get("cancel_at_period_end") or "").strip():
        b["cancel_at_period_end"] = _get("cancel at period end")

    dash = _get("dashboard")
    if dash and not str(b.get("dashboard_url") or "").strip():
        b["dashboard_url"] = dash

    # Renewal window: "start → end"
    win = _get("renewal window", "renewal")
    if win and (not str(b.get("renewal_end_iso") or "").strip()):
        parts = [p.strip() for p in win.split("→")]
        if len(parts) == 2:
            start_iso = parts[0].strip()
            end_iso = parts[1].strip()
            if start_iso:
                b["renewal_start"] = _fmt_date_any(start_iso)
            if end_iso:
                b["renewal_end_iso"] = end_iso
                b["renewal_end"] = _fmt_date_any(end_iso)
                dt_end = _parse_dt_any(end_iso)
                if dt_end:
                    delta = (dt_end - datetime.now(timezone.utc)).total_seconds()
                    b["remaining_days"] = max(0, int(ceil(delta / 86400.0)))

    return b


def _coerce_money_amount(v: object) -> float | None:
    """Best-effort float conversion for Whop 'total spent' style values."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        # Strip common currency symbols/commas.
        s = s.replace("$", "").replace(",", "")
        return float(s)
    except Exception:
        return None


def _fmt_usd_amt(amt: float) -> str:
    try:
        return f"${float(amt):,.2f}"
    except Exception:
        return ""


def _money_from_obj(obj: object, *, usd_keys: tuple[str, ...], cents_keys: tuple[str, ...]) -> tuple[float | None, bool]:
    """Return (amount_usd, found_any_field) using explicit *_cents keys when present."""
    if not isinstance(obj, dict):
        return (None, False)

    def _from_dict(d: dict) -> tuple[float | None, bool]:
        for k in cents_keys:
            if k in d:
                v = d.get(k)
                if v is None or str(v).strip() == "":
                    continue
                return (float(usd_amount(v)) / 100.0, True)
        for k in usd_keys:
            if k in d:
                v = d.get(k)
                if v is None or str(v).strip() == "":
                    continue
                return (float(usd_amount(v)), True)
        return (None, False)

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
    return (None, False)


def _extract_discord_username_from_whop_member_record(rec: dict) -> str:
    """Best-effort Discord username from Whop member record connections."""
    if not isinstance(rec, dict):
        return ""

    def _walk(obj: object, depth: int) -> str:
        if depth > 6:
            return ""
        if isinstance(obj, dict):
            try:
                prov = str(obj.get("provider") or obj.get("service") or "").strip().lower()
                if prov == "discord":
                    for k in ("username", "handle", "name", "display_name", "user_name"):
                        v = str(obj.get(k) or "").strip()
                        if v:
                            return v
            except Exception:
                pass
            for _k, v in obj.items():
                out = _walk(v, depth + 1)
                if out:
                    return out
        elif isinstance(obj, list):
            for it in obj:
                out = _walk(it, depth + 1)
                if out:
                    return out
        return ""

    return _walk(rec, 0)


def _extract_whop_user_id(membership: dict) -> str:
    """Best-effort Whop user_id (user_...) extraction for building dashboard URLs."""
    if not isinstance(membership, dict):
        return ""

    # Prefer membership.user (if present)
    u = membership.get("user")
    if isinstance(u, str):
        s = u.strip()
        if s:
            return s
    if isinstance(u, dict):
        s = str(u.get("id") or u.get("user_id") or "").strip()
        if s:
            return s

    # Some payloads nest user under member
    m = membership.get("member")
    if isinstance(m, dict):
        u2 = m.get("user")
        if isinstance(u2, str):
            s = u2.strip()
            if s:
                return s
        if isinstance(u2, dict):
            s = str(u2.get("id") or u2.get("user_id") or "").strip()
            if s:
                return s

    return ""


async def fetch_whop_brief(
    client: WhopAPIClient | None,
    membership_id: str,
    *,
    enable_enrichment: bool = True,
) -> dict:
    """Fetch a minimal Whop summary for staff (no internal IDs)."""
    mid = (membership_id or "").strip()
    if not mid:
        return {}

    # If API is disabled/unavailable, still provide baseline-derived fields so staff/tickets aren't blank.
    if (not client) or (not enable_enrichment):
        return enrich_whop_brief_from_membership_logs({"membership_id": mid}, membership_id=mid)

    membership = None
    with suppress(Exception):
        membership = await client.get_membership_by_id(mid)
    if not isinstance(membership, dict):
        return enrich_whop_brief_from_membership_logs({"membership_id": mid}, membership_id=mid)

    product_title = "—"
    if isinstance(membership.get("product"), dict):
        product_title = str(membership["product"].get("title") or "").strip() or "—"

    renewal_end_iso = str(membership.get("renewal_period_end") or "").strip()
    renewal_end_dt = _parse_dt_any(renewal_end_iso) if renewal_end_iso else None
    remaining_days: int | None = None
    if renewal_end_dt:
        delta = (renewal_end_dt - datetime.now(timezone.utc)).total_seconds()
        remaining_days = max(0, int(ceil(delta / 86400.0)))

    manage_url_raw = str(membership.get("manage_url") or "").strip()

    brief = {
        "membership_id": str(membership.get("id") or mid).strip(),
        "status": str(membership.get("status") or "").strip() or "—",
        "product": product_title,
        "user_name": "",
        "email": "",
        "member_since": _fmt_date_any(membership.get("created_at")),
        "trial_end": _fmt_date_any(membership.get("trial_end") or membership.get("trial_ends_at") or membership.get("trial_end_at")),
        "renewal_start": _fmt_date_any(membership.get("renewal_period_start")),
        "renewal_end": _fmt_date_any(membership.get("renewal_period_end")),
        "renewal_end_iso": renewal_end_iso or "",
        "remaining_days": remaining_days if isinstance(remaining_days, int) else "",
        # We intentionally do NOT surface billing-manage URLs in staff cards (use Dashboard).
        "manage_url": "",
        "dashboard_url": "",
        "cancel_at_period_end": "yes" if membership.get("cancel_at_period_end") is True else ("no" if membership.get("cancel_at_period_end") is False else "—"),
        "plan_is_renewal": "true"
        if (
            membership.get("plan_is_renewal") is True
            or membership.get("is_renewal") is True
            or ((membership.get("plan") or {}).get("is_renewal") is True if isinstance(membership.get("plan"), dict) else False)
        )
        else (
            "false"
            if (
                membership.get("plan_is_renewal") is False
                or membership.get("is_renewal") is False
                or ((membership.get("plan") or {}).get("is_renewal") is False if isinstance(membership.get("plan"), dict) else False)
            )
            else "—"
        ),
        "is_first_membership": "true" if membership.get("is_first_membership") is True else ("false" if membership.get("is_first_membership") is False else "—"),
        "last_payment_method": "—",
        "last_payment_type": "—",
        "last_payment_failure": "",
        "last_success_paid_at_iso": "",
        "last_success_paid_at": "—",
        "total_spent": "",
        "mrr": "",
        "customer_since": "",
        "connected_discord": "",
        "cancellation_reason": "",
        # Internal IDs (not shown in staff embeds; used for caching/linking)
        "whop_user_id": "",
        "whop_member_id": "",
    }

    # User identity (email/name) from membership payload (staff-only).
    try:
        u = membership.get("user")
        if isinstance(u, dict):
            brief["user_name"] = str(u.get("name") or u.get("username") or "").strip()
            brief["email"] = str(u.get("email") or "").strip()
    except Exception:
        pass
    # Cancellation reason (membership payload)
    try:
        reason_raw = str(membership.get("cancel_option") or membership.get("cancellation_reason") or "").strip()
        canceled_at_raw = str(membership.get("canceled_at") or "").strip()
        reason_disp = ""
        if reason_raw:
            reason_disp = reason_raw.replace("_", " ").strip().title()
        when_disp = ""
        dt = _parse_dt_any(canceled_at_raw) if canceled_at_raw else None
        if dt:
            when_disp = dt.astimezone(timezone.utc).strftime("%b %d, %Y - %I:%M %p").replace(" 0", " ")
        if reason_disp:
            # Match set-to-cancel: reason + product + timestamp.
            parts = [reason_disp, product_title]
            if when_disp:
                parts.append(when_disp)
            brief["cancellation_reason"] = "\n".join([p for p in parts if p and p != "—"])
    except Exception:
        pass

    # Dashboard URL (staff-facing): https://whop.com/dashboard/<biz>/users/<user_id>/
    user_id = _extract_whop_user_id(membership)
    brief["whop_user_id"] = user_id
    if user_id and getattr(client, "company_id", ""):
        dash = f"https://whop.com/dashboard/{str(client.company_id).strip()}/users/{user_id}/"
        brief["dashboard_url"] = f"[Open]({dash})"

    # Member record enrichment (total spend, mrr, customer since, connected discord)
    mber_id = ""
    try:
        mm = membership.get("member")
        if isinstance(mm, dict):
            mber_id = str(mm.get("id") or "").strip()
        elif isinstance(mm, str):
            mber_id = mm.strip()
    except Exception:
        mber_id = ""
    if (not mber_id) and "mber_" in manage_url_raw:
        i = manage_url_raw.find("mber_")
        if i >= 0:
            j = i
            while j < len(manage_url_raw) and (manage_url_raw[j].isalnum() or manage_url_raw[j] in "_-"):
                j += 1
            cand = manage_url_raw[i:j].strip()
            if cand.startswith("mber_"):
                mber_id = cand
    if mber_id:
        brief["whop_member_id"] = mber_id
        mrec = None
        with suppress(Exception):
            mrec = await client.get_member_by_id(mber_id)
        if isinstance(mrec, dict) and mrec:
            # total spend / mrr (prefer explicit cents keys)
            amt, found = _money_from_obj(
                mrec,
                usd_keys=("usd_total_spent", "total_spent_usd", "total_spend_usd", "total_spent", "total_spend", "platform_spend_usd", "platform_spend"),
                cents_keys=("usd_total_spent_cents", "total_spend_cents", "total_spent_cents"),
            )
            if found and amt is not None:
                brief["total_spent"] = _fmt_usd_amt(float(amt))
            mrr_amt, mrr_found = _money_from_obj(mrec, usd_keys=("mrr_usd", "mrr"), cents_keys=("mrr_cents",))
            if mrr_found and mrr_amt is not None:
                brief["mrr"] = _fmt_usd_amt(float(mrr_amt))
            # customer since
            cust_raw = str(mrec.get("created_at") or "").strip()
            if not cust_raw and isinstance(mrec.get("user"), dict):
                cust_raw = str(mrec["user"].get("created_at") or mrec["user"].get("createdAt") or "").strip()
            if cust_raw:
                brief["customer_since"] = _fmt_date_any(cust_raw)
            # connected discord
            did = extract_discord_id_from_whop_member_record(mrec)
            du = _extract_discord_username_from_whop_member_record(mrec)
            if du and did:
                brief["connected_discord"] = f"{du} ({did})"
            elif did:
                brief["connected_discord"] = str(did)
            elif du:
                brief["connected_discord"] = du

    return enrich_whop_brief_from_membership_logs(brief, membership_id=mid)

