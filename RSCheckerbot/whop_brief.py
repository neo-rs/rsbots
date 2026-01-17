from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime, timezone
from math import ceil

from whop_api_client import WhopAPIClient
from rschecker_utils import fmt_money

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

def _parse_dt_any(ts_str: str | int | float | None) -> datetime | None:
    """Parse ISO/unix-ish timestamps into UTC datetime (best-effort)."""
    if ts_str is None or ts_str == "":
        return None
    try:
        if isinstance(ts_str, (int, float)):
            return datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
        s = str(ts_str).strip()
        if not s:
            return None
        if "T" in s or "-" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except Exception:
        return None


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


def _extract_total_spent_usd(member_rec: dict) -> float | None:
    """Extract a lifetime total-spent value (USD) from /members/{mber_...} response (best-effort)."""
    if not isinstance(member_rec, dict):
        return None

    # Search both top-level and the nested user dict.
    user = member_rec.get("user") if isinstance(member_rec.get("user"), dict) else {}
    candidates: list[tuple[dict, str, bool]] = [
        (user, "total_spent_usd", False),
        (user, "usd_total_spent", False),
        (user, "total_spent", False),
        (user, "lifetime_spend_usd", False),
        (user, "lifetime_spend", False),
        (user, "total_spent_cents", True),
        (user, "total_spend_cents", True),
        (member_rec, "total_spent_usd", False),
        (member_rec, "usd_total_spent", False),
        (member_rec, "total_spent", False),
        (member_rec, "lifetime_spend_usd", False),
        (member_rec, "lifetime_spend", False),
        (member_rec, "total_spent_cents", True),
        (member_rec, "total_spend_cents", True),
    ]
    for d, key, is_cents in candidates:
        if not isinstance(d, dict) or key not in d:
            continue
        amt = _coerce_money_amount(d.get(key))
        if amt is None:
            continue
        return (amt / 100.0) if is_cents else amt

    # Some APIs put it under user.stats
    stats = user.get("stats") if isinstance(user.get("stats"), dict) else {}
    for key, is_cents in (("total_spent", False), ("total_spent_cents", True), ("lifetime_spend", False)):
        if key not in stats:
            continue
        amt = _coerce_money_amount(stats.get(key))
        if amt is None:
            continue
        return (amt / 100.0) if is_cents else amt

    return None


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
        "cancel_at_period_end": "yes" if membership.get("cancel_at_period_end") is True else ("no" if membership.get("cancel_at_period_end") is False else "—"),
        "is_first_membership": "true" if membership.get("is_first_membership") is True else ("false" if membership.get("is_first_membership") is False else "—"),
        "last_payment_method": "—",
        "last_payment_type": "—",
        "last_payment_failure": "",
        "last_success_paid_at_iso": "",
        "last_success_paid_at": "—",
        "total_spent": "",
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

    # Most recent successful payment timestamp (for staff visibility + fallback entitlement logic).
    try:
        for p in (payments or []):
            if not isinstance(p, dict):
                continue
            st = str(p.get("status") or "").strip().lower()
            if st not in {"succeeded", "paid", "successful", "success"}:
                continue
            ts = p.get("paid_at") or p.get("created_at") or ""
            dt = _parse_dt_any(ts)
            if dt:
                iso = dt.isoformat().replace("+00:00", "Z")
                brief["last_success_paid_at_iso"] = iso
                brief["last_success_paid_at"] = _fmt_date_any(iso)
                break
    except Exception:
        pass

    # Total spent (lifetime, best-effort from member record).
    # Fallback: sum succeeded payments for this membership (labeled clearly).
    try:
        whop_member_id = ""
        if isinstance(membership.get("member"), dict):
            whop_member_id = str(membership["member"].get("id") or "").strip()
        if whop_member_id:
            rec = await client.get_member_by_id(whop_member_id)
            if isinstance(rec, dict):
                total_usd = _extract_total_spent_usd(rec)
                if total_usd is not None:
                    brief["total_spent"] = fmt_money(total_usd, "usd")
        if not brief.get("total_spent"):
            total = 0.0
            saw_success = False
            for p in (payments or []):
                if not isinstance(p, dict):
                    continue
                st = str(p.get("status") or "").strip().lower()
                if st not in {"succeeded", "paid", "successful", "success"}:
                    continue
                saw_success = True
                amt = p.get("usd_total") or p.get("total") or p.get("subtotal") or p.get("amount_after_fees")
                a = _coerce_money_amount(amt)
                if a is not None:
                    total += a
            if saw_success:
                brief["total_spent"] = f"{fmt_money(total, 'usd')} (membership)"
    except Exception:
        pass

    return brief

