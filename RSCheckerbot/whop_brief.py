from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime, timezone
from math import ceil

from whop_api_client import WhopAPIClient
from rschecker_utils import extract_discord_id_from_whop_member_record
from rschecker_utils import fmt_date_any as _fmt_date_any, parse_dt_any as _parse_dt_any, usd_amount

log = logging.getLogger("rs-checker")


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
    if not client or not enable_enrichment:
        return {}
    mid = (membership_id or "").strip()
    if not mid:
        return {}

    membership = None
    with suppress(Exception):
        membership = await client.get_membership_by_id(mid)
    if not isinstance(membership, dict):
        return {}

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

    return brief

