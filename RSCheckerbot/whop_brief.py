from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime, timezone
from math import ceil

from whop_api_client import WhopAPIClient
from rschecker_utils import fmt_money, fmt_date_any as _fmt_date_any, parse_dt_any as _parse_dt_any

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
    manage_url = f"[Open]({manage_url_raw})" if manage_url_raw else ""

    brief = {
        "status": str(membership.get("status") or "").strip() or "—",
        "product": product_title,
        "member_since": _fmt_date_any(membership.get("created_at")),
        "trial_end": _fmt_date_any(membership.get("trial_end") or membership.get("trial_ends_at") or membership.get("trial_end_at")),
        "renewal_start": _fmt_date_any(membership.get("renewal_period_start")),
        "renewal_end": _fmt_date_any(membership.get("renewal_period_end")),
        "renewal_end_iso": renewal_end_iso or "",
        "remaining_days": remaining_days if isinstance(remaining_days, int) else "",
        "manage_url": manage_url,
        "dashboard_url": "",
        "cancel_at_period_end": "yes" if membership.get("cancel_at_period_end") is True else ("no" if membership.get("cancel_at_period_end") is False else "—"),
        "is_first_membership": "true" if membership.get("is_first_membership") is True else ("false" if membership.get("is_first_membership") is False else "—"),
        "last_payment_method": "—",
        "last_payment_type": "—",
        "last_payment_failure": "",
        "last_success_paid_at_iso": "",
        "last_success_paid_at": "—",
        "total_spent": "",
        # Internal IDs (not shown in staff embeds; used for caching/linking)
        "whop_user_id": "",
        "whop_member_id": "",
    }
    try:
        # Best-effort total_spent from membership payload only (no extra API calls).
        total_raw = (
            membership.get("total_spent")
            or membership.get("total_spent_usd")
            or membership.get("total_spend")
            or membership.get("total_spend_usd")
        )
        total_usd = _coerce_money_amount(total_raw)
        if total_usd is not None:
            brief["total_spent"] = fmt_money(total_usd, "usd")
    except Exception:
        pass

    # Dashboard URL (staff-facing): https://whop.com/dashboard/<biz>/users/<user_id>/
    user_id = _extract_whop_user_id(membership)
    brief["whop_user_id"] = user_id
    if user_id and getattr(client, "company_id", ""):
        dash = f"https://whop.com/dashboard/{str(client.company_id).strip()}/users/{user_id}/"
        brief["dashboard_url"] = f"[Open]({dash})"

    return brief

