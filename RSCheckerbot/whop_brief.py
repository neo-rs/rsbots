from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime, timezone

from whop_api_client import WhopAPIClient

log = logging.getLogger("rs-checker")


def _fmt_date_any(ts_str: str | int | float | None) -> str:
    """Human-friendly date like 'January 8, 2026' (best-effort)."""
    try:
        if ts_str is None:
            return "—"
        if isinstance(ts_str, (int, float)):
            dt = datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
        else:
            s = str(ts_str).strip()
            if not s:
                return "—"
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        out = dt.astimezone(timezone.utc).strftime("%B %d, %Y")
        return out.replace(" 0", " ")
    except Exception:
        return "—"


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

    brief = {
        "status": str(membership.get("status") or "").strip() or "—",
        "product": product_title,
        "member_since": _fmt_date_any(membership.get("created_at")),
        "trial_end": _fmt_date_any(membership.get("trial_end") or membership.get("trial_ends_at") or membership.get("trial_end_at")),
        "renewal_start": _fmt_date_any(membership.get("renewal_period_start")),
        "renewal_end": _fmt_date_any(membership.get("renewal_period_end")),
        "cancel_at_period_end": "yes" if membership.get("cancel_at_period_end") is True else ("no" if membership.get("cancel_at_period_end") is False else "—"),
        "is_first_membership": "true" if membership.get("is_first_membership") is True else ("false" if membership.get("is_first_membership") is False else "—"),
        "last_payment_method": "—",
        "last_payment_type": "—",
        "last_payment_failure": "",
    }

    payments = []
    try:
        payments = await client.get_payments_for_membership(mid)
    except Exception as e:
        log.warning(f"[WhopBrief] get_payments_for_membership failed for {mid}: {e}")
        payments = []

    if payments and isinstance(payments, list) and isinstance(payments[0], dict):
        p0 = payments[0]
        failure_msg = str(p0.get("failure_message") or "").strip()
        card_brand = str(p0.get("card_brand") or "").strip()
        card_last4 = str(p0.get("card_last4") or "").strip()
        pm_type = (
            p0.get("payment_method_type")
            or p0.get("payment_type")
            or p0.get("type")
            or p0.get("method")
        )
        if card_brand and card_last4:
            brief["last_payment_method"] = f"{card_brand.upper()} ****{card_last4}"
        if pm_type:
            brief["last_payment_type"] = str(pm_type).strip()
        if failure_msg:
            brief["last_payment_failure"] = failure_msg[:140]

    return brief

