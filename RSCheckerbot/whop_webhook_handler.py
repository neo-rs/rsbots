#!/usr/bin/env python3
"""
Whop Webhook Handler
--------------------
Handles webhook messages from Whop workflows posted to Discord channel.
Monitors Discord channel for Whop webhook messages and processes them.

Canonical Owner: This module owns Whop webhook processing logic.
"""

import json
import re
import logging
import time
import discord
from pathlib import Path
from datetime import datetime, timezone
from contextlib import suppress

log = logging.getLogger("rs-checker")

# whop-logs history lookup throttles (avoid Discord 429s)
_WHOP_LOGS_LOOKUP_CACHE: dict[str, tuple[str, float]] = {}
_WHOP_LOGS_LOOKUP_LAST: dict[str, float] = {}

# Canonical shared helpers (single source of truth)
from rschecker_utils import load_json as _load_json
from rschecker_utils import save_json as _save_json
from rschecker_utils import fmt_money, usd_amount
from rschecker_utils import access_roles_plain, coerce_role_ids
from rschecker_utils import fmt_date_any as _fmt_date_any
from rschecker_utils import parse_dt_any as _parse_dt_any
from staff_embeds import build_case_minimal_embed, build_member_status_detailed_embed
from whop_brief import fetch_whop_brief
from whop_native_membership_cache import record_summary as _record_native_summary_by_mid
from staff_channels import PAYMENT_FAILURE_CHANNEL_NAME, MEMBER_CANCELLATION_CHANNEL_NAME
from staff_alerts_store import (
    load_staff_alerts,
    save_staff_alerts,
    should_post_alert,
    record_alert_post,
    should_post_and_record_alert,
)

# Import Whop API client (required; do not silently disable modules)
from whop_api_client import WhopAPIClient, WhopAPIError

# Configuration (initialized from main)
WHOP_WEBHOOK_CHANNEL_ID = None
WHOP_LOGS_CHANNEL_ID = None
ROLE_TRIGGER = None
WELCOME_ROLE_ID = None
ROLE_CANCEL_A = None
ROLE_CANCEL_B = None
LIFETIME_ROLE_IDS: set[int] = set()

# Logging functions (initialized from main - canonical ownership)
_log_other = None
_log_member_status = None
_fmt_user = None
_record_member_whop_summary = None
_record_whop_event = None

# Identity tracking and trial abuse detection
MEMBER_STATUS_LOGS_CHANNEL_ID = None

# Expected roles config (loaded from config.json)
EXPECTED_ROLES = {}

# Whop API client (initialized from main)
_whop_api_client = None
_whop_api_config = {}

# File paths for JSON storage (canonical: JSON-only, no SQLite)
BASE_DIR = Path(__file__).resolve().parent
IDENTITY_CACHE_FILE = BASE_DIR / "whop_identity_cache.json"
TRIAL_CACHE_FILE = BASE_DIR / "trial_history.json"
IDENTITY_CONFLICTS_FILE = BASE_DIR / "identity_conflicts.jsonl"
STAFF_ALERTS_FILE = BASE_DIR / "staff_alerts.json"
PAYMENT_CACHE_FILE = BASE_DIR / "payment_cache.json"

from rschecker_utils import extract_discord_id_from_whop_member_record


def _norm_email(s: str) -> str:
    """Normalize email address for consistent storage/lookup"""
    return (s or "").strip().lower()



#
# Dispute/Resolution reporting was removed from the runtime bot.
# If you need to inspect disputes/chargebacks, use `whop_api_probe.py alerts` or `raw`.
#


def _access_roles_compact(member: discord.Member) -> str:
    """Access-relevant roles only (no mentions)."""
    relevant = coerce_role_ids(ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B)
    return access_roles_plain(member, relevant)

def _has_lifetime_role(member: discord.Member) -> bool:
    if not LIFETIME_ROLE_IDS:
        return False
    try:
        role_ids = {r.id for r in (member.roles or [])}
        return bool(role_ids.intersection(LIFETIME_ROLE_IDS))
    except Exception:
        return False


def _membership_id_from_event(member: discord.Member, event_data: dict) -> str:
    """Prefer membership_id from webhook payload (no cache fallback)."""
    mid = _safe_get(event_data, "membership_id", "membership.id", default="").strip()
    if mid == "—":
        mid = ""
    return (mid or "").strip()


def _pick_first(*vals: object) -> str:
    for v in vals:
        s = str(v or "").strip()
        if s and s != "—":
            return s
    return ""


def _parse_bullet_kv(text: str) -> dict[str, str]:
    """Parse bullet lines like '• Key: Value' into a dict."""
    out: dict[str, str] = {}
    if not text:
        return out
    for raw in str(text).splitlines():
        line = raw.strip()
        if not line:
            continue
        line = line.lstrip("•").strip()
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        key = str(k or "").strip().lower()
        val = str(v or "").strip()
        if key and val:
            out[key] = val
    return out


def _flatten_field_kv(fields_data: dict) -> dict[str, str]:
    """Flatten embed fields (Identity/Membership/Plan/Actions) into a single key map."""
    out: dict[str, str] = {}
    for _name, value in (fields_data or {}).items():
        try:
            if isinstance(value, str):
                out.update(_parse_bullet_kv(value))
        except Exception:
            continue
    return out


def _extract_native_kv_from_embed(embed: discord.Embed) -> tuple[dict[str, str], dict[str, str]]:
    """Return (fields_data, merged_kv) for native Whop cards.

    - fields_data: normalized mapping of field-name -> field-value (lowercased)
    - merged_kv: union of direct field pairs + bullet KV parsed from multi-line fields
    """
    fields_data: dict[str, str] = {}
    try:
        for field in (getattr(embed, "fields", None) or []):
            try:
                k = str(getattr(field, "name", "") or "").strip().lower()
                v = str(getattr(field, "value", "") or "").strip()
            except Exception:
                continue
            if k:
                fields_data[k] = v
    except Exception:
        fields_data = {}

    # Direct KV: Whop sometimes places key data as field names (e.g. "Total Spent").
    direct_kv = {k: v for k, v in (fields_data or {}).items() if str(v or "").strip()}
    bullet_kv = _flatten_field_kv(fields_data)
    # Many Whop cards put the actual data in the embed description (not fields).
    desc_kv: dict[str, str] = {}
    try:
        desc_kv = _parse_bullet_kv(str(getattr(embed, "description", "") or ""))
    except Exception:
        desc_kv = {}
    merged_kv = {**direct_kv, **bullet_kv, **desc_kv}
    return (fields_data, merged_kv)


def _parse_renewal_window(raw: str) -> tuple[str, str]:
    """Parse 'start → end' or 'start -> end' into (start, end)."""
    s = str(raw or "").strip()
    if not s:
        return ("", "")
    if "→" in s:
        a, b = s.split("→", 1)
    elif "->" in s:
        a, b = s.split("->", 1)
    else:
        return ("", "")
    return (str(a).strip(), str(b).strip())


def _promo_from_pricing(pricing: str) -> str:
    """Return 'yes' if pricing indicates a promo (<60 USD baseline), else 'no'/' '."""
    s = str(pricing or "").strip()
    if not s:
        return ""
    # Prefer arrow format: "25 → 60 usd" or "0 -> 60 usd"
    arrow = "→" if "→" in s else ("->" if "->" in s else "")
    if arrow:
        left, right = s.split(arrow, 1)
        nums_l = re.findall(r"(-?\\d+(?:\\.\\d+)?)", left)
        nums_r = re.findall(r"(-?\\d+(?:\\.\\d+)?)", right)
        try:
            before = float(nums_l[0]) if nums_l else None
        except Exception:
            before = None
        try:
            after = float(nums_r[0]) if nums_r else None
        except Exception:
            after = None
        # Promo rule: if final price is around 60 and initial price is <60, it's a promo.
        if (after is not None) and (before is not None):
            if after >= 59.5 and before < 59.5:
                return "yes"
            return "no"
    # Fallback: if we only have one numeric price, treat <60 as promo.
    nums = re.findall(r"(-?\\d+(?:\\.\\d+)?)", s)
    if nums:
        try:
            v = float(nums[0])
            return "yes" if v < 59.5 else "no"
        except Exception:
            return ""
    return ""


def _build_whop_summary_from_native_kv(extra_kv: dict) -> dict:
    """Build a Whop summary using native card text as source-of-truth (no formatting/verification)."""
    extra = extra_kv if isinstance(extra_kv, dict) else {}

    def _get(*keys: str) -> str:
        for k in keys:
            ks = str(k or "").strip().lower()
            if not ks:
                continue
            v = str(extra.get(ks) or "").strip()
            if v:
                return v
            v2 = str(extra.get(ks.replace("_", " ")) or "").strip()
            if v2:
                return v2
        return ""

    status = _get("status", "membership_status", "membership status")
    product = _get("product", "plan", "product title")
    # Native cards vary between "Total Spent" and "Total Spend".
    total_spent = _get("total_spent", "total spent", "total spend")
    renewal_window = _get("renewal_window", "renewal window", "renewal")
    # Some cards output a bare arrow (e.g. "→") when window is unknown.
    if renewal_window.strip() in {"→", "->"}:
        renewal_window = ""
    pricing = _get("pricing", "price")
    promo = _promo_from_pricing(pricing)
    plan_is_renewal = _get("plan_is_renewal", "plan is renewal")
    trial_days = _get("trial_days", "trial days")
    # Native cards often label this as "Whop Dashboard".
    dashboard_url = _get("dashboard_url", "dashboard", "whop dashboard", "whop_dashboard")
    manage_url = _get("manage_url", "manage")
    checkout_url = _get("checkout_url", "checkout", "purchase link")
    is_first_membership = _get("is_first_membership", "first membership")
    last_payment_failure = _get("last_payment_failure", "failure reason", "failure message")

    # Keep these as raw strings exactly as Whop posted them (no parsing/formatting).
    renewal_start = ""
    renewal_end = ""
    renewal_window_human = ""
    remaining_days = ""
    if renewal_window and ("→" in renewal_window or "->" in renewal_window):
        ws, we = _parse_renewal_window(renewal_window)
        renewal_start = ws
        renewal_end = we
        # Humanize timestamps and compute window length (days) when possible.
        try:
            ds = _parse_dt_any(ws)
            de = _parse_dt_any(we)
        except Exception:
            ds = None
            de = None
        if ds and de:
            with suppress(Exception):
                renewal_start = _fmt_date_any(ds.isoformat().replace("+00:00", "Z"))
            with suppress(Exception):
                renewal_end = _fmt_date_any(de.isoformat().replace("+00:00", "Z"))
            # Requested: remaining_days = renewal_end - renewal_start (window length)
            try:
                days = int(max(0.0, (de - ds).total_seconds()) / 86400.0 + 0.00001)
                remaining_days = str(days)
            except Exception:
                remaining_days = ""
        # Keep a human-readable window string too (for staff view).
        if renewal_start and renewal_end:
            renewal_window_human = f"{renewal_start} → {renewal_end}"

    return {
        "status": status,
        "product": product,
        "total_spent": total_spent,
        "renewal_window": renewal_window_human or renewal_window,
        "renewal_start": renewal_start,
        "renewal_end": renewal_end,
        "remaining_days": remaining_days,
        "promo": promo,
        "pricing": pricing,
        "plan_is_renewal": plan_is_renewal,
        "trial_days": trial_days,
        "dashboard_url": dashboard_url,
        "manage_url": manage_url,
        "checkout_url": checkout_url,
        "is_first_membership": is_first_membership,
        "last_payment_failure": last_payment_failure,
    }


def extract_native_whop_card_debug(message: discord.Message) -> dict:
    """Best-effort extractor for native Whop embed cards (debug/testing only).

    Returns a small dict with the parsed Whop summary and key identifiers. This is used for
    startup smoke tests and should never raise.
    """
    try:
        if not message or not getattr(message, "embeds", None):
            return {}
        embed = message.embeds[0]

        fields_data, merged_kv = _extract_native_kv_from_embed(embed)

        # Discord ID best-effort (field, then description)
        discord_id_str = ""
        if "discord id" in fields_data:
            discord_id_str = re.sub(r"<@!?(\d+)>", r"\1", str(fields_data.get("discord id") or "")).strip()
        if not discord_id_str:
            desc = str(getattr(embed, "description", "") or "")
            m = re.search(r"Discord ID[:\s]+(\d{17,19})", desc, re.IGNORECASE)
            if m:
                discord_id_str = m.group(1)
        did_match = re.search(r"(\d{17,19})", str(discord_id_str or ""))
        did = did_match.group(1) if did_match else ""

        # Useful pairing keys
        email = str(merged_kv.get("email") or "").strip()
        membership_id = str(merged_kv.get("membership id") or merged_kv.get("membership_id") or merged_kv.get("membership") or "").strip()
        if membership_id.startswith("`") and membership_id.endswith("`"):
            membership_id = membership_id.strip("`").strip()

        summary = _build_whop_summary_from_native_kv(merged_kv)
        return {
            "message_id": int(getattr(message, "id", 0) or 0),
            "channel_id": int(getattr(getattr(message, "channel", None), "id", 0) or 0),
            "created_at": getattr(message, "created_at", None).isoformat() if getattr(message, "created_at", None) else "",
            "title": str(getattr(embed, "title", "") or "").strip(),
            "discord_id": did,
            "email": email,
            "membership_id": membership_id,
            "summary": summary if isinstance(summary, dict) else {},
            "kv_keys": sorted(list({str(k) for k in (merged_kv or {}).keys() if str(k or "").strip()}))[:50],
        }
    except Exception:
        return {}


def _build_whop_summary(event_data: dict, *, extra_kv: dict | None = None) -> dict:
    """Build a staff-safe Whop summary from webhook data (no cache lookups)."""
    extra = extra_kv if isinstance(extra_kv, dict) else {}

    def _val(*keys: str) -> str:
        for key in keys:
            v = _safe_get(event_data, key, default="").strip()
            if v:
                return v
            k_low = key.lower()
            v2 = str(extra.get(k_low) or "").strip()
            if v2:
                return v2
            k_spaced = k_low.replace("_", " ")
            v3 = str(extra.get(k_spaced) or "").strip()
            if v3:
                return v3
        return ""

    status = _val("membership.status", "status", "membership_status")
    product = _val("product.title", "product", "plan", "product_name")
    total_spent_raw = _val("total_spent", "total_spent_usd", "total_spent_cents", "total_spend", "total_spend_usd", "total_spend_cents")
    total_spent_val = ""
    if total_spent_raw:
        amt = usd_amount(total_spent_raw)
        total_spent_val = fmt_money(amt, "usd") if amt > 0 else str(total_spent_raw).strip()

    renewal_start_raw = _val("renewal_period_start", "renewal_start", "renewal_start_at")
    renewal_end_raw = _val("renewal_period_end", "renewal_end", "access_ends_at", "trial_end", "trial_ends_at")
    window_raw = _val("renewal_window", "renewal window")
    if window_raw and (not renewal_start_raw or not renewal_end_raw):
        ws, we = _parse_renewal_window(window_raw)
        renewal_start_raw = renewal_start_raw or ws
        renewal_end_raw = renewal_end_raw or we

    renewal_end_iso = ""
    renewal_end_fmt = ""
    remaining_days: int | str = ""
    try:
        dt_end = _parse_dt_any(renewal_end_raw)
        if dt_end:
            renewal_end_iso = dt_end.isoformat().replace("+00:00", "Z")
            renewal_end_fmt = _fmt_date_any(renewal_end_iso)
            delta = (dt_end - datetime.now(timezone.utc)).total_seconds()
            remaining_days = max(0, int((delta / 86400.0) + 0.999))
    except Exception:
        pass

    trial_days = _val("trial_days", "trial_period_days", "trial_days_remaining")
    pricing = _val("pricing", "price", "plan_price")
    promo = _promo_from_pricing(pricing)
    plan_is_renewal = _val("plan_is_renewal", "plan is renewal", "plan_is_renewal?")
    first_membership = _val("first_membership", "first membership", "is_first_membership")
    manage_url = _val("manage_url", "manage", "billing_manage")
    dashboard_url = _val("dashboard_url", "dashboard")
    cancel_at_period_end = _val("cancel_at_period_end", "cancel at period end", "cancel_at_period", "cancel_at_period_end?")
    is_first_membership = _val("is_first_membership", "first membership", "first_membership")
    last_payment_failure = _val("failure_reason", "last_payment_failure", "payment_failure", "failure message")

    return {
        "status": status,
        "product": product,
        "member_since": _val("created_at", "member_since"),
        "trial_end": _val("trial_end", "trial_ends_at", "trial_end_at"),
        "trial_days": trial_days,
        "renewal_window": window_raw,
        "renewal_start": _fmt_date_any(renewal_start_raw) if renewal_start_raw else "",
        "renewal_end": renewal_end_fmt or (_fmt_date_any(renewal_end_raw) if renewal_end_raw else ""),
        "renewal_end_iso": renewal_end_iso,
        "remaining_days": remaining_days,
        "manage_url": manage_url,
        "dashboard_url": dashboard_url,
        "checkout_url": _val("checkout_url", "checkout", "purchase link"),
        "cancel_at_period_end": cancel_at_period_end,
        "is_first_membership": is_first_membership or first_membership,
        "plan_is_renewal": plan_is_renewal,
        "promo": promo,
        "pricing": pricing,
        "last_payment_method": "",
        "last_payment_type": "",
        "last_payment_failure": last_payment_failure,
        "last_success_paid_at_iso": "",
        "last_success_paid_at": "—",
        "total_spent": total_spent_val,
    }


def _summary_to_event_fields(summary: dict) -> dict[str, str]:
    if not isinstance(summary, dict):
        return {}
    return {
        "product": str(summary.get("product") or "").strip(),
        "status": str(summary.get("status") or "").strip(),
        "trial_days": str(summary.get("trial_days") or "").strip(),
        "promo": str(summary.get("promo") or "").strip(),
        "pricing": str(summary.get("pricing") or "").strip(),
        "total_spent": str(summary.get("total_spent") or "").strip(),
        "cancel_at_period_end": str(summary.get("cancel_at_period_end") or "").strip(),
        "renewal_window": str(summary.get("renewal_window") or "").strip(),
        "dashboard_url": str(summary.get("dashboard_url") or "").strip(),
        "manage_url": str(summary.get("manage_url") or "").strip(),
        "checkout_url": str(summary.get("checkout_url") or "").strip(),
        "renewal_period_start": str(summary.get("renewal_start") or "").strip(),
        "renewal_period_end": str(summary.get("renewal_end") or "").strip(),
        "renewal_end_iso": str(summary.get("renewal_end_iso") or "").strip(),
    }


def _event_reason_from_data(event_data: dict, extra_kv: dict | None = None) -> str:
    extra = extra_kv if isinstance(extra_kv, dict) else {}
    return _pick_first(
        _safe_get(event_data, "failure_reason", "cancellation_reason", "reason", default="").strip(),
        str(extra.get("failure reason") or "").strip(),
        str(extra.get("cancellation reason") or "").strip(),
        str(extra.get("reason") or "").strip(),
    )


async def _record_whop_event_if_possible(event: dict) -> None:
    if not _record_whop_event:
        return
    try:
        await _record_whop_event(event)
    except Exception:
        return


def _record_whop_summary_if_possible(*, member_id: int, summary: dict, event_data: dict) -> None:
    if not _record_member_whop_summary:
        return
    if not isinstance(summary, dict) or not summary:
        return
    try:
        membership_id = _safe_get(event_data or {}, "membership_id", "membership.id", default="").strip()
        whop_key = _safe_get(event_data or {}, "whop_key", "key", default="").strip()
        event_type = str((event_data or {}).get("event_type") or "").strip()
        _record_member_whop_summary(
            int(member_id),
            summary,
            event_type=event_type,
            membership_id=membership_id,
            whop_key=whop_key,
        )
    except Exception:
        return

async def _whop_brief_from_event(member: discord.Member, event_data: dict) -> dict:
    # Prefer whop summary derived from webhook/member-logs payloads.
    summary = {}
    try:
        if isinstance(event_data, dict):
            summary = event_data.get("_whop_summary") or {}
    except Exception:
        summary = {}
    if not isinstance(summary, dict):
        summary = {}
    if not summary and isinstance(event_data, dict):
        try:
            summary = _build_whop_summary(event_data)
        except Exception:
            summary = {}

    def _blank(v: object) -> bool:
        s = str(v or "").strip()
        return (not s) or s == "—" or s.lower() == "n/a"

    def _merge_missing(base: dict, extra: dict) -> dict:
        out = dict(base or {})
        for k, v in (extra or {}).items():
            if k not in out or _blank(out.get(k)):
                out[k] = v
        return out

    # Optional: enrich via Whop API for webhook-driven cards (controlled by config).
    # This is how we match the richer set-to-cancel fields across all staff cards.
    if _cards_use_api() and _whop_api_client:
        mid = ""
        try:
            mid = _safe_get(event_data or {}, "membership_id", "membership.id", default="").strip()
        except Exception:
            mid = ""
        if not mid:
            try:
                mid = _safe_get(event_data or {}, "whop_key", "key", default="").strip()
            except Exception:
                mid = ""
        if mid:
            with suppress(Exception):
                api_brief = await fetch_whop_brief(_whop_api_client, mid, enable_enrichment=True)
                if isinstance(api_brief, dict) and api_brief:
                    summary = _merge_missing(summary, api_brief)

    if summary:
        _record_whop_summary_if_possible(member_id=int(member.id), summary=summary, event_data=event_data)
    return summary


def _extract_discord_id_from_embed(embed: discord.Embed) -> str:
    """Best-effort extract Discord ID from an embed."""
    try:
        fields_data, merged_kv = _extract_native_kv_from_embed(embed)
    except Exception:
        fields_data, merged_kv = ({}, {})
    did = str(
        merged_kv.get("discord id")
        or merged_kv.get("discord_id")
        or merged_kv.get("discordid")
        or merged_kv.get("discord user id")
        or merged_kv.get("discord userid")
        or ""
    ).strip()
    if not did:
        did = str(fields_data.get("discord id") or "").strip()
    if did:
        did = re.sub(r"<@!?(\d+)>", r"\1", did).strip()
    m = re.search(r"(\d{17,19})", did or "")
    if m:
        return m.group(1)

    # Heuristic fallback: any KV whose key contains "discord" and whose value contains a snowflake.
    try:
        for k, v in (merged_kv or {}).items():
            if "discord" not in str(k or "").lower():
                continue
            raw = re.sub(r"<@!?(\d+)>", r"\1", str(v or "")).strip()
            m2 = re.search(r"(\d{17,19})", raw or "")
            if m2:
                return m2.group(1)
    except Exception:
        pass
    return ""


def _extract_email_from_embed(embed: discord.Embed) -> str:
    try:
        _fields_data, merged_kv = _extract_native_kv_from_embed(embed)
    except Exception:
        merged_kv = {}
    # Common explicit keys
    for k in ("email", "email address", "customer email", "member email", "user email"):
        email = str(merged_kv.get(k) or "").strip()
        if "@" in email:
            return _norm_email(email)

    # Heuristic: any KV key containing "email"
    try:
        for k, v in (merged_kv or {}).items():
            if "email" not in str(k or "").lower():
                continue
            email = str(v or "").strip()
            if "@" in email:
                return _norm_email(email)
    except Exception:
        pass

    # Last resort: scan values for an email-like substring.
    try:
        for v in (merged_kv or {}).values():
            s = str(v or "").strip()
            if "@" not in s:
                continue
            m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", s, flags=re.I)
            if m:
                return _norm_email(m.group(1))
    except Exception:
        pass
    return ""


def _extract_key_from_embed(embed: discord.Embed) -> str:
    try:
        _fields_data, merged_kv = _extract_native_kv_from_embed(embed)
    except Exception:
        merged_kv = {}
    key = str(merged_kv.get("key") or merged_kv.get("whop_key") or "").strip()
    return key


async def _resolve_discord_id_from_whop_logs(
    guild: discord.Guild | None,
    *,
    email: str,
    membership_id_hint: str,
    whop_key: str,
    limit: int = 50,
) -> str:
    """Search whop-logs channel for a matching email/key and return Discord ID."""
    # Rate-limit protection: do not repeatedly scan channel history for the same key.
    global _WHOP_LOGS_LOOKUP_CACHE, _WHOP_LOGS_LOOKUP_LAST
    if not guild:
        return ""
    if not WHOP_LOGS_CHANNEL_ID:
        return ""
    ch = guild.get_channel(int(WHOP_LOGS_CHANNEL_ID))
    if not isinstance(ch, discord.TextChannel):
        return ""
    email_n = _norm_email(email) if email else ""
    mid = str(membership_id_hint or "").strip()
    key = str(whop_key or "").strip()
    if not (email_n or mid or key):
        return ""

    now = time.time()
    cache_key_email = f"email:{email_n}" if email_n else ""
    cache_key_key = f"key:{key}" if key else ""

    # Return cached results.
    try:
        if cache_key_email and cache_key_email in _WHOP_LOGS_LOOKUP_CACHE:
            did, ts = _WHOP_LOGS_LOOKUP_CACHE[cache_key_email]
            if did and (now - float(ts)) < 6 * 3600:
                return str(did)
        if cache_key_key and cache_key_key in _WHOP_LOGS_LOOKUP_CACHE:
            did, ts = _WHOP_LOGS_LOOKUP_CACHE[cache_key_key]
            if did and (now - float(ts)) < 6 * 3600:
                return str(did)
    except Exception:
        pass

    # Throttle repeated history scans.
    try:
        last = float(_WHOP_LOGS_LOOKUP_LAST.get(cache_key_email or cache_key_key or "global", 0.0))
        if (now - last) < 60.0:
            return ""
        _WHOP_LOGS_LOOKUP_LAST[cache_key_email or cache_key_key or "global"] = now
    except Exception:
        pass

    try:
        # This scan is throttled + cached; allow a deeper window so older members still resolve.
        async for m in ch.history(limit=int(max(10, min(250, int(limit))))):
            if not m.embeds:
                continue
            e0 = m.embeds[0]
            try:
                e_email = _extract_email_from_embed(e0)
                e_key = _extract_key_from_embed(e0)
            except Exception:
                e_email = ""
                e_key = ""
            # Prefer email match; fall back to key match when available.
            if email_n and e_email and email_n == e_email:
                did = _extract_discord_id_from_embed(e0)
                if did:
                    with suppress(Exception):
                        if cache_key_email:
                            _WHOP_LOGS_LOOKUP_CACHE[cache_key_email] = (did, now)
                        if cache_key_key:
                            _WHOP_LOGS_LOOKUP_CACHE[cache_key_key] = (did, now)
                    return did
            if key and e_key and key == e_key:
                did = _extract_discord_id_from_embed(e0)
                if did:
                    with suppress(Exception):
                        if cache_key_email:
                            _WHOP_LOGS_LOOKUP_CACHE[cache_key_email] = (did, now)
                        if cache_key_key:
                            _WHOP_LOGS_LOOKUP_CACHE[cache_key_key] = (did, now)
                    return did
            # Some logs include membership ids/keys in description; try raw text match as last resort.
            if mid:
                raw = (str(getattr(e0, "description", "") or "") + " " + " ".join([str(getattr(f, "value", "") or "") for f in (getattr(e0, "fields", None) or [])])).lower()
                if mid.lower() in raw:
                    did = _extract_discord_id_from_embed(e0)
                    if did:
                        with suppress(Exception):
                            if cache_key_email:
                                _WHOP_LOGS_LOOKUP_CACHE[cache_key_email] = (did, now)
                            if cache_key_key:
                                _WHOP_LOGS_LOOKUP_CACHE[cache_key_key] = (did, now)
                        return did
    except Exception:
        return ""
    return ""


# Public wrapper (used by startup scans) - keeps lookup logic single-sourced.
async def resolve_discord_id_from_whop_logs(
    guild: discord.Guild | None,
    *,
    email: str,
    membership_id_hint: str = "",
    whop_key: str = "",
    limit: int = 50,
) -> str:
    """Resolve Discord ID by searching `whop-logs` for the matching email/key."""
    return await _resolve_discord_id_from_whop_logs(
        guild,
        email=email,
        membership_id_hint=membership_id_hint,
        whop_key=whop_key,
        limit=limit,
    )


def _cards_use_api() -> bool:
    """Whether webhook-driven staff cards are allowed to call Whop API."""
    try:
        return bool((_whop_api_config or {}).get("cards_use_api", False))
    except Exception:
        return False

def _cache_identity(email: str, discord_id: str, discord_username: str = "") -> None:
    """Cache email -> discord_id mapping for future enrichment"""
    email = _norm_email(email)
    if not email or not discord_id:
        return
    db = _load_json(IDENTITY_CACHE_FILE)
    db[email] = {
        "discord_id": str(discord_id),
        "discord_username": (discord_username or "").strip(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }
    _save_json(IDENTITY_CACHE_FILE, db)

def _lookup_identity(email: str) -> dict | None:
    """Look up cached identity mapping by email"""
    email = _norm_email(email)
    if not email:
        return None
    db = _load_json(IDENTITY_CACHE_FILE)
    return db.get(email)

def _load_whop_history() -> dict:
    """Load whop_history.json from RSAdminBot/whop_data/ directory.
    
    Path is resolved from config or uses default relative path.
    Returns empty dict if file doesn't exist or is invalid.
    """
    try:
        # Default path (relative to RSCheckerbot folder)
        default_path = BASE_DIR.parent / "RSAdminBot" / "whop_data" / "whop_history.json"
        whop_history_path = default_path
        
        # Try to load config to get custom path (if available)
        try:
            config_path = BASE_DIR / "config.json"
            if config_path.exists():
                config_data = _load_json(config_path)
                custom_path = config_data.get("paths", {}).get("whop_history")
                if custom_path:
                    # Resolve relative to BASE_DIR
                    whop_history_path = (BASE_DIR / custom_path).resolve()
        except Exception:
            pass  # Use default path if config loading fails
        
        if not whop_history_path.exists():
            return {}
        
        data = _load_json(whop_history_path)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log.warning(f"Failed to load whop_history.json: {e}")
        return {}

def _build_identity_cache_from_history(whop_history: dict) -> dict:
    """Build identity cache dictionary from whop_history membership events.
    
    Args:
        whop_history: Dictionary with 'membership_events' key containing list of events
    
    Returns:
        Dictionary mapping email (normalized) to {discord_id, discord_username, last_seen, source}
    """
    cache = {}
    events = whop_history.get("membership_events", [])
    
    for event in events:
        email = event.get("email", "").strip()
        discord_id = event.get("discord_id", "").strip()
        discord_username = event.get("discord_username", "").strip()
        timestamp = event.get("timestamp") or event.get("created_at")
        
        if not email or not discord_id:
            continue
        
        email_norm = _norm_email(email)
        if not email_norm:
            continue
        
        # Parse timestamp to ISO format for last_seen
        last_seen_iso = timestamp if timestamp else datetime.now(timezone.utc).isoformat()
        
        cache[email_norm] = {
            "discord_id": str(discord_id),
            "discord_username": discord_username,
            "last_seen": last_seen_iso,
            "source": "whop_history"
        }
    
    return cache

def _backfill_identity_cache() -> None:
    """Backfill identity cache from whop_history.json.
    
    Merge rules:
    - If email not present → add
    - If email present and discord_id matches → update metadata
    - If email present and discord_id differs → log conflict, do NOT overwrite
    """
    try:
        whop_history = _load_whop_history()
        if not whop_history:
            log.info("whop_history.json not found or empty, skipping identity backfill")
            return
        
        history_cache = _build_identity_cache_from_history(whop_history)
        if not history_cache:
            log.info("No identity mappings found in whop_history.json")
            return
        
        # Load existing cache
        existing_cache = _load_json(IDENTITY_CACHE_FILE)
        
        added_count = 0
        updated_count = 0
        conflict_count = 0
        
        # Log conflicts to file
        conflicts_log = []
        
        for email, history_entry in history_cache.items():
            existing_entry = existing_cache.get(email)
            
            if not existing_entry:
                # New entry - add it
                existing_cache[email] = history_entry
                added_count += 1
            else:
                # Entry exists - check discord_id
                existing_id = str(existing_entry.get("discord_id", "")).strip()
                history_id = str(history_entry.get("discord_id", "")).strip()
                
                if existing_id == history_id:
                    # IDs match - update metadata (last_seen, username if newer)
                    existing_cache[email]["last_seen"] = history_entry["last_seen"]
                    if history_entry.get("discord_username"):
                        existing_cache[email]["discord_username"] = history_entry["discord_username"]
                    updated_count += 1
                else:
                    # IDs differ - log conflict, do NOT overwrite
                    conflict_count += 1
                    conflicts_log.append({
                        "email": email,
                        "existing_discord_id": existing_id,
                        "history_discord_id": history_id,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
        
        # Save merged cache
        _save_json(IDENTITY_CACHE_FILE, existing_cache)
        
        # Log conflicts if any
        if conflicts_log:
            try:
                with open(IDENTITY_CONFLICTS_FILE, "a", encoding="utf-8") as f:
                    for conflict in conflicts_log:
                        f.write(json.dumps(conflict, ensure_ascii=False) + "\n")
            except Exception as e:
                log.warning(f"Failed to write identity conflicts log: {e}")
        
        log.info(f"Identity backfill complete: {added_count} added, {updated_count} updated, {conflict_count} conflicts")
    except Exception as e:
        log.error(f"Identity backfill failed: {e}", exc_info=True)

def _record_trial_event(email: str, discord_id: str, membership_id: str, trial_days: str, is_first_membership: str, event_type: str) -> dict:
    """
    Store trial activity and detect suspicious patterns.
    Returns dict with 'suspicious', 'reason', 'key', 'count' fields.
    """
    email_n = _norm_email(email)
    key = f"{email_n}|{discord_id or 'no_discord'}"

    db = _load_json(TRIAL_CACHE_FILE)
    rec = db.get(key, {"email": email_n, "discord_id": discord_id or "", "events": []})

    rec["events"].append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "membership_id": membership_id or "",
        "trial_days": str(trial_days or ""),
        "is_first_membership": str(is_first_membership or ""),
    })

    # Keep last 50 events per identity to avoid bloat
    rec["events"] = rec["events"][-50:]
    db[key] = rec
    _save_json(TRIAL_CACHE_FILE, db)

    # Suspicion logic:
    # 1) If is_first_membership == false AND trial_days > 0 => strong repeat-trial signal
    # 2) If we see multiple trial activations historically => weak repeat-trial signal
    suspicious = False
    reason = ""
    try:
        td = int(str(trial_days or "0"))
    except ValueError:
        td = 0

    if str(is_first_membership).lower() == "false" and td > 0:
        suspicious = True
        reason = "Trial started but is_first_membership=false (repeat trial likely)"
    else:
        # count trial-type events
        trial_events = [e for e in rec["events"] if str(e.get("trial_days","0")).isdigit() and int(e.get("trial_days","0")) > 0]
        if len(trial_events) >= 2:
            suspicious = True
            reason = f"Multiple trial events seen ({len(trial_events)})"

    return {"suspicious": suspicious, "reason": reason, "key": key, "count": len(rec["events"])}


def _fmt_discord_ts(ts_str: str | None, style: str = "D") -> str:
    """Format timestamp string as Discord timestamp (human-readable)
    
    Args:
        ts_str: ISO timestamp string or Unix timestamp string
        style: Discord timestamp style ('D' = short date, 'F' = full date, 'R' = relative)
    
    Returns:
        Discord timestamp string like <t:1234567890:D> or "—" if invalid
    """
    if not ts_str:
        return "—"
    try:
        # Try parsing as ISO timestamp
        if "T" in str(ts_str) or "-" in str(ts_str):
            dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            unix_ts = int(dt.timestamp())
        else:
            # Assume Unix timestamp (string or int)
            unix_ts = int(float(str(ts_str)))
        return f"<t:{unix_ts}:{style}>"
    except (ValueError, TypeError, AttributeError):
        return "—"


async def _resolve_member_safe(guild: discord.Guild, discord_id: int | None, force_fetch: bool = False) -> discord.Member | None:
    """Safely resolve a member with rate-limit protection.
    
    Args:
        guild: Discord guild to resolve member in
        discord_id: Discord user ID to resolve
        force_fetch: If True, always try fetch_member (bypasses cache check)
    
    Returns:
        discord.Member if found, None otherwise
    """
    if not discord_id or not guild:
        return None
    
    # Try fast path first (cached member)
    member = guild.get_member(discord_id)
    if member:
        return member
    
    # Only fetch if explicitly requested or for critical events
    # (This prevents API spam - fetch_member is expensive)
    if force_fetch:
        try:
            member = await guild.fetch_member(discord_id)
            return member
        except (discord.NotFound, discord.HTTPException):
            return None
    
    return None

def _safe_get(event_data: dict, *keys: str, default: str = "—") -> str:
    """Safely get nested dict value using dot notation keys (e.g., 'user.username', 'membership.status')
    
    Args:
        event_data: Event data dictionary
        keys: Variable number of key paths to try (e.g., 'user.username', 'username')
        default: Default value if all keys fail
    
    Returns:
        Value as string, or default
    """
    for key_path in keys:
        parts = key_path.split(".")
        value = event_data
        try:
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                if value is None:
                    break
            if value is not None and value != "":
                return str(value)
        except (AttributeError, TypeError, KeyError):
            continue
    return default


def initialize(
    webhook_channel_id,
    whop_logs_channel_id,
    role_trigger,
    welcome_role_id,
    role_cancel_a,
    role_cancel_b,
    log_other_func,
    log_member_status_func,
    fmt_user_func,
    member_status_logs_channel_id=None,
    record_member_whop_summary_func=None,
    record_whop_event_func=None,
    whop_api_key=None,
    whop_api_config=None,
    lifetime_role_ids=None,
):
    """
    Initialize handler with configuration and logging functions.
    
    Args:
        webhook_channel_id: Channel ID where Whop workflow webhooks are posted
        whop_logs_channel_id: Channel ID where Whop native integration posts
        role_trigger: Cleanup/trigger role ID
        welcome_role_id: Welcome role ID
        role_cancel_a: Member role ID
        role_cancel_b: Welcome role ID (same as welcome_role_id)
        log_other_func: Function to log to other channel (canonical owner)
        log_member_status_func: Function to log to member status channel (canonical owner)
        fmt_user_func: Function to format user display (canonical owner)
        member_status_logs_channel_id: Channel ID for member status logs (lookup requests, trial alerts)
        record_member_whop_summary_func: Function to persist a Whop summary into member_history
        record_whop_event_func: Function to persist normalized Whop event records
        whop_api_key: Whop API key (optional, from config.secrets.json)
        whop_api_config: Whop API configuration dict (optional, from config.json)
    """
    global WHOP_WEBHOOK_CHANNEL_ID, WHOP_LOGS_CHANNEL_ID
    global ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B
    global LIFETIME_ROLE_IDS
    global _log_other, _log_member_status, _fmt_user, _record_member_whop_summary, _record_whop_event
    global MEMBER_STATUS_LOGS_CHANNEL_ID, EXPECTED_ROLES, _whop_api_client, _whop_api_config
    
    WHOP_WEBHOOK_CHANNEL_ID = webhook_channel_id
    WHOP_LOGS_CHANNEL_ID = whop_logs_channel_id
    ROLE_TRIGGER = role_trigger
    WELCOME_ROLE_ID = welcome_role_id
    ROLE_CANCEL_A = role_cancel_a
    ROLE_CANCEL_B = role_cancel_b
    try:
        ids = set()
        for x in (lifetime_role_ids or []):
            if str(x).strip().isdigit():
                ids.add(int(str(x).strip()))
        LIFETIME_ROLE_IDS = ids
    except Exception:
        LIFETIME_ROLE_IDS = set()
    _log_other = log_other_func
    _log_member_status = log_member_status_func
    _fmt_user = fmt_user_func
    _record_member_whop_summary = record_member_whop_summary_func
    _record_whop_event = record_whop_event_func
    MEMBER_STATUS_LOGS_CHANNEL_ID = member_status_logs_channel_id
    
    # Load expected roles config
    try:
        config_path = BASE_DIR / "config.json"
        if config_path.exists():
            config_data = _load_json(config_path)
            EXPECTED_ROLES = config_data.get("whop_webhook", {}).get("expected_roles", {})
        else:
            EXPECTED_ROLES = {}
    except Exception as e:
        log.warning(f"Failed to load expected roles config: {e}")
        EXPECTED_ROLES = {}
    
    # Backfill identity cache from whop_history.json
    _backfill_identity_cache()
    
    # Initialize Whop API client if key provided
    _whop_api_config = whop_api_config or {}
    if whop_api_key and WhopAPIClient:
        try:
            # Check if key is placeholder
            from mirror_world_config import is_placeholder_secret
            if not is_placeholder_secret(whop_api_key):
                base_url = _whop_api_config.get("base_url", "https://api.whop.com/api/v1")
                company_id = _whop_api_config.get("company_id", "")
                _whop_api_client = WhopAPIClient(whop_api_key, base_url, company_id)
                log.info("Whop API client initialized")
            else:
                _whop_api_client = None
                log.info("Whop API client disabled (placeholder key)")
        except Exception as e:
            _whop_api_client = None
            log.warning(f"Failed to initialize Whop API client: {e}")
    else:
        _whop_api_client = None
        if not WhopAPIClient:
            log.info("Whop API client disabled (module not available)")
        else:
            log.info("Whop API client disabled (no API key)")
    
    log.info(f"Whop webhook handler initialized")
    log.info(f"Monitoring webhook channel {webhook_channel_id} and logs channel {whop_logs_channel_id}")


async def handle_whop_webhook_message(message: discord.Message, *, backfill_only: bool = False):
    """
    Handle messages from Whop webhook in Discord channel.
    
    Supports two formats:
    1. Workflow webhooks (EVENT_DATA JSON in description)
    2. Native Whop integration messages (embed fields)
    
    Canonical owner for Whop webhook message processing.
    """
    try:
        # Check if message has embeds
        if not message.embeds:
            return
        
        embed = message.embeds[0]
        description = embed.description or ""
        title = embed.title or ""
        
        if not backfill_only:
            log.info(f"Whop message detected: {title}")
        
        # Try to extract EVENT_DATA from description (workflow format)
        json_match = re.search(r'EVENT_DATA:(\{.*\})', description)
        
        if json_match:
            # Workflow webhook format
            if backfill_only:
                return
            await _handle_workflow_webhook(message, embed, json_match)
        else:
            # Native Whop integration format
            await _handle_native_whop_message(message, embed, backfill_only=backfill_only)
        
    except Exception as e:
        log.error(f"Error handling webhook message: {e}", exc_info=True)
        if _log_other and not backfill_only:
            await _log_other(f"❌ **Whop Webhook Error:** {e}")


async def _handle_workflow_webhook(message: discord.Message, embed: discord.Embed, json_match: re.Match):
    """Handle workflow webhook format (EVENT_DATA JSON)"""
    try:
        # Parse event data
        json_string = json_match.group(1)
        event_data = json.loads(json_string)
        
        event_type = event_data.get('event_type', '').strip()
        discord_user_id = event_data.get('discord_user_id', '').strip()
        email = event_data.get('email', '').strip()
        
        # Check if EVENT_DATA is empty (all fields are empty strings)
        has_data = any(v and v.strip() for k, v in event_data.items() if k != 'event_type' or v.strip())
        
        if not event_type:
            log.warning(f"Whop workflow webhook has no event_type: {json_string}")
            if _log_other:
                await _log_other(f"⚠️ **Whop Webhook:** Received webhook with empty event_type. Check Whop workflow variables.")
            return
        
        if not has_data:
            log.warning(f"Whop workflow webhook has empty EVENT_DATA fields: {json_string}")
            if _log_other:
                await _log_other(
                    f"⚠️ **Whop Webhook Error:** EVENT_DATA fields are empty!\n"
                    f"**Event Type:** `{event_type}`\n"
                    f"**Issue:** Whop workflow variables not populated. Check workflow configuration.\n"
                    f"**Message ID:** {message.id}"
                )
            return
        
        log.info(f"Processing Whop workflow event: {event_type} for user {discord_user_id}")
        
        # Trial abuse tracking (workflow path)
        trial_days = event_data.get("trial_period_days", "") or event_data.get("trial_days", "")
        is_first = event_data.get("is_first_membership", "")
        membership_id_val = event_data.get("membership_id", "")

        # Build a staff-safe summary from webhook data (used for embeds + history).
        try:
            summary = _build_whop_summary(event_data)
            if summary:
                event_data["_whop_summary"] = summary
        except Exception:
            pass

        # Record event ledger entry (even if Discord ID is missing).
        try:
            summary = event_data.get("_whop_summary") if isinstance(event_data, dict) else {}
            fields = _summary_to_event_fields(summary if isinstance(summary, dict) else {})
            reason = _event_reason_from_data(event_data)
            event = {
                "event_id": f"discord:{message.id}",
                "source": "whop_discord_webhook",
                "event_type": str(event_type or "").strip(),
                "occurred_at": message.created_at.isoformat() if message.created_at else datetime.now(timezone.utc).isoformat(),
                "membership_id": str(membership_id_val or _membership_id_from_event(None, event_data)).strip(),
                "user_id": str(_safe_get(event_data, "user_id", "user.id", default="") or "").strip(),
                "member_id": str(_safe_get(event_data, "member_id", "member.id", default="") or "").strip(),
                "discord_id": str(discord_user_id or "").strip(),
                "email": str(email or "").strip(),
                "reason": reason,
                "source_discord": {
                    "channel_id": getattr(message.channel, "id", ""),
                    "message_id": message.id,
                    "jump_url": getattr(message, "jump_url", ""),
                },
            }
            event.update(fields)
            await _record_whop_event_if_possible(event)
        except Exception:
            pass

        # Consider trial tracking for activation/pending events
        if event_type in ("membership.activated.pending", "membership.activated", "payment.succeeded.activation", "payment.succeeded.renewal"):
            info = _record_trial_event(
                email=email,
                discord_id=discord_user_id,
                membership_id=membership_id_val,
                trial_days=trial_days,
                is_first_membership=is_first,
                event_type=event_type,
            )
            # Alert logic: only alert when actionable
            # - If discord_id is empty: only alert on strong signal (is_first=false && trial_days>0)
            # - If discord_id exists: alert on any suspicious pattern
            should_alert = info.get("suspicious", False)
            if not discord_user_id:
                # When discord_id missing, only alert on strong signal (not weak "multiple trials" signal)
                try:
                    td = int(str(trial_days or "0"))
                    is_strong_signal = (str(is_first).lower() == "false" and td > 0)
                    should_alert = should_alert and is_strong_signal
                except ValueError:
                    should_alert = False
            
            if should_alert and _log_member_status:
                guild = message.guild if message.guild else None
                mem: discord.Member | None = None
                if discord_user_id and guild:
                    try:
                        user_id_int = int(str(discord_user_id).strip())
                        mem = await _resolve_member_safe(guild, user_id_int, force_fetch=True)
                    except Exception:
                        mem = None

                if mem:
                    whop_brief = await _whop_brief_from_event(mem, event_data)
                    access = _access_roles_compact(mem)
                    detailed = build_member_status_detailed_embed(
                        title="🚩 Trial Abuse Signal",
                        member=mem,
                        access_roles=access,
                        color=0xED4245,
                        discord_kv=[
                            ("event", str(event_type or "").strip() or "whop_workflow"),
                            ("reason", info.get("reason", "Unknown")),
                            ("email", _norm_email(email) if email else "—"),
                        ],
                        whop_brief=whop_brief,
                    )
                    await _log_member_status("", embed=detailed)
                else:
                    # Fallback (no resolved member): still avoid legacy field names.
                    embed = discord.Embed(
                        title="🚩 Trial Abuse Signal",
                        color=0xED4245,
                        timestamp=datetime.now(timezone.utc),
                    )
                    embed.add_field(
                        name="Member Info",
                        value=f"discord_id `{str(discord_user_id or 'N/A')}`",
                        inline=False,
                    )
                    embed.add_field(
                        name="Discord Info",
                        value=f"event `{str(event_type or '').strip()}`",
                        inline=False,
                    )
                    embed.add_field(
                        name="Payment Info",
                        value=f"reason `{info.get('reason','Unknown')}`",
                        inline=False,
                    )
                    await _log_member_status("", embed=embed)
        
        if not discord_user_id:
            if _log_other:
                await _log_other(
                    f"⚠️ **Whop Webhook:** No discord_user_id in event.\n"
                    f"**Event Type:** `{event_type}`\n"
                    f"**Email:** {email if email else 'N/A'}\n"
                    f"**Message ID:** {message.id}"
                )

            return
        
        # Get guild and member
        guild = message.guild
        try:
            did_int = int(str(discord_user_id).strip())
        except ValueError:
            log.error(f"Invalid discord_user_id format: {discord_user_id}")
            if _log_other:
                await _log_other(f"❌ **Whop Webhook Error:** Invalid discord_user_id format: `{discord_user_id}`")
            return
        
        member = await _resolve_member_safe(guild, did_int, force_fetch=True)
        
        if not member:
            if _log_other:
                await _log_other(
                    f"⚠️ **Whop Webhook:** Member not found in guild.\n"
                    f"**Discord ID:** `{discord_user_id}`\n"
                    f"**Event Type:** `{event_type}`\n"
                    f"**Email:** {email if email else 'N/A'}"
                )
            return
        
        # Route to handler based on event type
        if event_type == 'membership.activated':
            await handle_membership_activated(member, event_data)
        elif event_type == 'membership.activated.pending':
            await handle_membership_activated_pending(member, event_data)
        elif event_type == 'membership.deactivated':
            await handle_membership_deactivated(member, event_data)
        elif event_type == 'membership.deactivated.payment_failure':
            await handle_membership_deactivated_payment_failure(member, event_data)
        elif event_type == 'payment.succeeded.renewal':
            await handle_payment_renewal(member, event_data)
        elif event_type == 'payment.succeeded.activation':
            await handle_payment_activation(member, event_data)
        elif event_type == 'payment.failed':
            await handle_payment_failed(member, event_data)
        elif event_type == 'payment.refunded':
            await handle_payment_refunded(member, event_data)
        elif event_type == 'waitlist.entry_approved':
            await handle_waitlist_approved(member, event_data)
        else:
            if _log_other:
                await _log_other(f"ℹ️ **Whop Webhook:** Unhandled event type: {event_type}")
    except json.JSONDecodeError as e:
        log.error(f"JSON decode error in workflow webhook: {e}")
        if _log_other:
            await _log_other(f"❌ **Whop Webhook Error:** Failed to parse JSON: {e}")


async def _handle_native_whop_message(message: discord.Message, embed: discord.Embed, *, backfill_only: bool = False):
    """
    Handle native Whop integration messages (embed fields format).
    """
    try:
        title = embed.title or ""
        description = embed.description or ""
        content = message.content or ""
        
        # Extract data from embed fields (primary source)
        fields_data, merged_kv = _extract_native_kv_from_embed(embed)
        
        # Also parse message content as fallback (for messages without embeds)
        content_data = _parse_whop_content(content)
        
        # Merge: embed fields take precedence, content as fallback
        # Include merged_kv so description-parsed bullets (membership id, email, total spent, links)
        # are available to the rest of the handler.
        parsed_data = {**content_data, **fields_data, **merged_kv}
        # For native Whop cards, treat the posted card text as the source of truth.
        summary_from_native = _build_whop_summary_from_native_kv(merged_kv)

        # Extract membership status and event type (usable even when Discord ID is missing)
        membership_status = parsed_data.get("membership_status", "") or fields_data.get("membership status", "")
        event_type = _determine_event_type_from_message(title, description, content, membership_status)

        # Email can come from parsed content/fields; use best-effort and never crash on missing.
        email_value = (
            parsed_data.get("email")
            or fields_data.get("membership status", {}).get("email", "") if isinstance(fields_data.get("membership status"), dict) else ""
            or fields_data.get("email")
            or fields_data.get("Email")
            or ""
        )

        # Attempt best-effort membership id hint (do NOT log; only for internal correlation)
        membership_id_hint = ""
        try:
            mid_candidate = str(
                parsed_data.get("membership_id")
                or parsed_data.get("membership id")
                or fields_data.get("membership id")
                or ""
            ).strip()
            if mid_candidate.startswith(("mem_", "R-")):
                membership_id_hint = mid_candidate
            if not membership_id_hint:
                whop_key = str(parsed_data.get("whop_key") or parsed_data.get("key") or "").strip()
                if whop_key.startswith(("mem_", "R-")):
                    membership_id_hint = whop_key
        except Exception:
            membership_id_hint = ""

        # Extract Discord ID (best-effort; may be missing on native cards)
        discord_id_str = None

        # Record event ledger entry (even if Discord ID is missing).
        try:
            fields = _summary_to_event_fields(summary_from_native)
            reason = _event_reason_from_data(parsed_data, flat_kv)
            event = {
                "event_id": f"discord:{message.id}",
                "source": "whop_discord_logs",
                "event_type": str(event_type or "").strip(),
                "occurred_at": message.created_at.isoformat() if message.created_at else datetime.now(timezone.utc).isoformat(),
                "membership_id": str(membership_id_hint or parsed_data.get("membership_id") or parsed_data.get("membership id") or "").strip(),
                "user_id": str(parsed_data.get("user_id") or parsed_data.get("user id") or "").strip(),
                "member_id": str(parsed_data.get("member_id") or parsed_data.get("member id") or "").strip(),
                "discord_id": str(discord_id_str or "").strip(),
                "email": str(email_value or "").strip(),
                "reason": reason,
                "source_discord": {
                    "channel_id": getattr(message.channel, "id", ""),
                    "message_id": message.id,
                    "jump_url": getattr(message, "jump_url", ""),
                },
            }
            event.update(fields)
            await _record_whop_event_if_possible(event)
        except Exception:
            pass
        
        # Try embed fields first
        if "discord id" in fields_data:
            discord_id_str = fields_data["discord id"]
            discord_id_str = re.sub(r'<@!?(\d+)>', r'\1', discord_id_str).strip()
        elif "Discord ID" in [f.name for f in embed.fields]:
            for field in embed.fields:
                if field.name == "Discord ID":
                    discord_id_str = re.sub(r'<@!?(\d+)>', r'\1', field.value).strip()
                    break
        
        # Try content parsing
        if not discord_id_str and content_data.get("discord_id"):
            discord_id_str = content_data["discord_id"]
        
        # Try description
        if not discord_id_str:
            desc_match = re.search(r'Discord ID[:\s]+(\d+)', description, re.IGNORECASE)
            if desc_match:
                discord_id_str = desc_match.group(1)
        
        if not discord_id_str or discord_id_str == "No Discord":
            if not backfill_only:
                log.info(f"Native Whop message has no Discord ID: {title}")
            resolved_id = ""

            # Best-effort #1: resolve Discord ID from cached identity (email -> discord_id).
            if not resolved_id:
                try:
                    email_n = _norm_email(str(email_value or "").strip())
                    cached = _lookup_identity(email_n) if email_n else None
                    cached_id = str((cached or {}).get("discord_id") or "").strip() if isinstance(cached, dict) else ""
                except Exception:
                    cached_id = ""
                if cached_id and cached_id.isdigit():
                    resolved_id = cached_id

            # Best-effort #2: resolve from whop-logs channel by matching email/key (no Whop API).
            if not resolved_id:
                try:
                    whop_key = str(parsed_data.get("whop_key") or parsed_data.get("key") or "").strip()
                except Exception:
                    whop_key = ""
                try:
                    resolved_id = await _resolve_discord_id_from_whop_logs(
                        message.guild if message.guild else None,
                        email=str(email_value or "").strip(),
                        membership_id_hint=str(membership_id_hint or "").strip(),
                        whop_key=whop_key,
                        limit=50,
                    )
                except Exception:
                    resolved_id = ""

            if resolved_id and str(resolved_id).isdigit():
                discord_id_str = str(resolved_id).strip()
            else:
                # We cannot link to a Discord ID, but we CAN cache the native summary by membership_id
                # so member-status-logs can reuse it later when a Discord user is linked.
                if membership_id_hint and isinstance(summary_from_native, dict) and summary_from_native:
                    _record_native_summary_by_mid(
                        str(membership_id_hint).strip(),
                        summary_from_native,
                        source_message_id=int(message.id),
                    )
                return
        
        # Extract numeric Discord ID
        discord_id_match = re.search(r'(\d{17,19})', discord_id_str)
        if not discord_id_match:
            log.warning(f"Could not extract valid Discord ID from: {discord_id_str}")
            return
        
        discord_id = discord_id_match.group(1)
        discord_user_id = int(discord_id)
        
        # Cache identity mapping for enrichment (email -> discord_id)
        discord_username_value = (
            parsed_data.get("discord_username")
            or fields_data.get("discord username")
            or ""
        )
        _cache_identity(email_value, str(discord_user_id), str(discord_username_value))

        # Always persist the native-card summary as the staff-safe source of truth.
        # This lets Discord-only cards (join/leave) show Total Spent / Dashboard / Renewal Window
        # as soon as we've seen at least one native Whop card for that Discord ID.
        try:
            event_stub = {
                "event_type": str(event_type or "").strip(),
                "membership_id": str(membership_id_hint or "").strip(),
                "whop_key": str(parsed_data.get("whop_key") or parsed_data.get("key") or "").strip(),
            }
            _record_whop_summary_if_possible(member_id=int(discord_user_id), summary=summary_from_native, event_data=event_stub)
            if membership_id_hint and isinstance(summary_from_native, dict) and summary_from_native:
                _record_native_summary_by_mid(
                    str(membership_id_hint).strip(),
                    summary_from_native,
                    source_message_id=int(message.id),
                )
        except Exception:
            pass

        if backfill_only:
            return

        # Get guild and member
        guild = message.guild
        member = guild.get_member(discord_user_id) if guild else None
        if (not member) and guild:
            with suppress(Exception):
                member = await guild.fetch_member(discord_user_id)
        
        if not member:
            # Still process summary storage even if member not found, but also post a minimal staff card
            # so events don't silently disappear.
            if _log_other:
                await _log_other(f"⚠️ **Whop Native:** Member `{discord_user_id}` not found in guild (posting fallback card)")
            if _log_member_status:
                e = discord.Embed(
                    title=str(title or "Whop Native Event").strip()[:256],
                    description=str(description or "").strip()[:3500] or "—",
                    color=0xFEE75C,
                    timestamp=datetime.now(timezone.utc),
                )
                with suppress(Exception):
                    if email_value:
                        e.add_field(name="Email", value=str(email_value)[:1024], inline=False)
                with suppress(Exception):
                    e.add_field(name="Discord ID", value=f"`{discord_user_id}`", inline=False)
                with suppress(Exception):
                    if membership_id_hint:
                        e.add_field(name="Membership ID", value=f"`{membership_id_hint}`", inline=False)
                with suppress(Exception):
                    dash = str(summary_from_native.get("dashboard_url") or "").strip() if isinstance(summary_from_native, dict) else ""
                    if dash:
                        e.add_field(name="Whop Dashboard", value=dash[:1024], inline=False)
                with suppress(Exception):
                    e.set_footer(text="RSCheckerbot • Member Status Tracking")
                with suppress(Exception):
                    await _log_member_status("", embed=e)
            member = None

        # Process role changes if member found
        if member:
            title_l = title.lower()
            desc_l = description.lower()

            # Membership Activated (Pending) / Activation PENDING
            if ("activation pending" in title_l) or ("activation pending" in desc_l) or ("activated (pending)" in title_l) or ("activated (pending)" in desc_l):
                event_data = {
                    "event_type": "membership.activated.pending",
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                }
                if membership_id_hint:
                    event_data["membership_id"] = membership_id_hint
                if isinstance(summary_from_native, dict) and summary_from_native:
                    event_data["_whop_summary"] = summary_from_native
                await handle_membership_activated_pending(member, event_data)
                return

            # Billing Issue (Access Risk)
            if ("billing issue" in title_l) or ("billing issue" in desc_l) or ("access risk" in title_l) or ("access risk" in desc_l):
                event_data = {
                    "event_type": "membership.deactivated.billing_issue",
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                }
                if membership_id_hint:
                    event_data["membership_id"] = membership_id_hint
                if isinstance(summary_from_native, dict) and summary_from_native:
                    event_data["_whop_summary"] = summary_from_native
                await handle_membership_billing_issue_access_risk(member, event_data)
                return

            # Check for payment failed
            if "payment failed" in title_l or "payment failed" in desc_l:
                event_data = {
                    "event_type": "payment.failed",
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                    "amount": "N/A",
                    "failure_reason": "Payment failed",
                }
                if isinstance(summary_from_native, dict) and summary_from_native:
                    event_data["_whop_summary"] = summary_from_native
                await handle_payment_failed(member, event_data)

            # Check for payment received / succeeded (native cards)
            elif "payment received" in title_l or "payment received" in desc_l or "payment succeeded" in title_l or "payment succeeded" in desc_l:
                is_renewal = ("renewal" in title_l) or ("renewal" in desc_l) or ("renewal" in (message.content or "").lower())
                evt = "payment.succeeded.renewal" if is_renewal else "payment.succeeded.activation"
                event_data = {
                    "event_type": evt,
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                }
                if membership_id_hint:
                    event_data["membership_id"] = membership_id_hint
                if isinstance(summary_from_native, dict) and summary_from_native:
                    event_data["_whop_summary"] = summary_from_native
                if is_renewal:
                    await handle_payment_renewal(member, event_data)
                else:
                    await handle_payment_activation(member, event_data)

            # Check for cancel action
            elif ("performing cancel" in desc_l) or ("removeallroles" in desc_l) or ("performing cancel" in title_l) or ("removeallroles" in title_l):
                event_data = {
                    "event_type": "membership.deactivated",
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                    "cancellation_reason": "Whop native cancel action",
                }
                if isinstance(summary_from_native, dict) and summary_from_native:
                    event_data["_whop_summary"] = summary_from_native
                await handle_membership_deactivated(member, event_data)

            # Check for membership status changes
            elif "membership update" in title_l:
                if "past due" in membership_status.lower():
                    if _log_member_status:
                        await _log_member_status(f"⚠️ **Whop Native:** {_fmt_user(member)} - Membership Past Due")
                elif "active" in membership_status.lower():
                    event_data = {
                        "event_type": "membership.activated",
                        "discord_user_id": str(discord_user_id),
                        "email": email_value,
                        "status": "active",
                    }
                    if isinstance(summary_from_native, dict) and summary_from_native:
                        event_data["_whop_summary"] = summary_from_native
                    await handle_membership_activated(member, event_data)

            else:
                log.info(f"Processed native Whop message: {title}")
        else:
            log.info(f"Native Whop message (member not in guild): {title}")
            
    except (ValueError, KeyError) as e:
        log.error(f"Error parsing native Whop message: {e}", exc_info=True)
        if _log_other:
            await _log_other(f"❌ **Whop Native Error:** Failed to parse message: {e}")


def _parse_whop_content(content: str) -> dict:
    """
    Parse Whop message content (text format, like whop_tracker.py).
    Handles format: Label on one line, value on next line.
    """
    if not content:
        return {}
    
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    
    def get_value_after(label: str) -> str:
        for i, line in enumerate(lines):
            if label in line and i + 1 < len(lines):
                return lines[i + 1]
        return ""
    
    discord_id_value = get_value_after("Discord ID")
    discord_id_match = re.search(r'(\d{17,19})', discord_id_value) if discord_id_value else None
    
    return {
        "discord_id": discord_id_match.group(1) if discord_id_match else "",
        "discord_username": get_value_after("Discord Username"),
        "whop_key": get_value_after("Key"),
        "access_pass": get_value_after("Access Pass"),
        "name": get_value_after("Name"),
        "email": get_value_after("Email"),
        "membership_status": get_value_after("Membership Status")
    }


def _determine_event_type_from_message(title: str, description: str, content: str, membership_status: str) -> str:
    """
    Determine event type from message (matching whop_tracker.py logic).
    Returns: 'new', 'renewal', 'cancellation', 'completed', or 'payment_failed'
    """
    title_lower = title.lower()
    desc_lower = description.lower()
    content_lower = content.lower()
    status_lower = membership_status.lower()
    
    if "payment failed" in title_lower or "payment failed" in desc_lower:
        return "payment_failed"
    elif "renewal" in content_lower or "renew" in content_lower or "renewal" in desc_lower:
        return "renewal"
    elif "cancel" in status_lower or "cancel" in content_lower or "cancel" in desc_lower or "removeallroles" in desc_lower:
        return "cancellation"
    elif "completed" in status_lower:
        return "completed"
    else:
        return "new"


# API verification and enrichment functions
async def _verify_webhook_with_api(member: discord.Member, event_data: dict, event_type: str):
    """
    Verify webhook data against Whop API.
    
    Args:
        member: Discord member
        event_data: Webhook event data
        event_type: Event type string (e.g., "membership.activated")
    """
    # Webhook-driven cards should not depend on Whop API (scans/sync own API usage).
    if not bool((_whop_api_config or {}).get("cards_use_api", False)):
        return
    if not _whop_api_client:
        return
    if not _whop_api_config.get("enable_verification", True):
        return
    
    try:
        # Determine expected status from event type
        expected_status = None
        if "activated" in event_type.lower():
            expected_status = "active"
        elif "deactivated" in event_type.lower() or "canceled" in event_type.lower() or "cancellation" in event_type.lower():
            expected_status = "canceled"
        elif "payment" in event_type.lower() and "succeeded" in event_type.lower():
            expected_status = "active"
        
        if expected_status:
            membership_id = _safe_get(event_data, "membership_id", "membership.id", default="").strip()
            if membership_id == "—":
                membership_id = ""
            if not membership_id:
                return  # Can't verify without membership_id

            verification = await _whop_api_client.verify_membership_status(membership_id, expected_status)
            
            if not verification["matches"]:
                # Log discrepancy
                if _log_other:
                    await _log_other(
                        f"⚠️ **API Verification Mismatch** for {_fmt_user(member)}\n"
                        f"   Webhook says: `{expected_status}`\n"
                        f"   API says: `{verification['actual_status'] or 'N/A'}`\n"
                        f"   Event: `{event_type}`"
                    )
    except Exception as e:
        log.error(f"API verification failed for {member.id}: {e}")


# Event handlers - canonical owners for their respective event types
async def handle_membership_activated(member: discord.Member, event_data: dict):
    """Handle new active membership - assign Cleanup role and log with support card embed"""
    guild = member.guild
    
    cleanup_role = guild.get_role(ROLE_TRIGGER)
    
    if cleanup_role and cleanup_role not in member.roles:
        await member.add_roles(cleanup_role, reason="Whop: Membership activated")
        log.info(f"Assigned cleanup role to {member} for membership activation")
    
    if _log_member_status:
        whop_brief = await _whop_brief_from_event(member, event_data)
        access = _access_roles_compact(member)
        detailed = build_member_status_detailed_embed(
            title="✅ Membership Activated",
            member=member,
            access_roles=access,
            color=0x57F287,
            event_kind="active",
            discord_kv=[
                ("event", "membership.activated"),
                ("roles_added", cleanup_role.name if cleanup_role else "—"),
            ],
            whop_brief=whop_brief,
        )
        await _log_member_status("", embed=detailed)
    
    # Verify with API after processing
    await _verify_webhook_with_api(member, event_data, "membership.activated")


async def handle_membership_activated_pending(member: discord.Member, event_data: dict):
    """Handle pending membership activation - log with support card embed"""
    if _log_member_status:
        whop_brief = await _whop_brief_from_event(member, event_data)
        access = _access_roles_compact(member)
        detailed = build_member_status_detailed_embed(
            title="⏳ Membership Activated (Pending)",
            member=member,
            access_roles=access,
            color=0xFEE75C,
            event_kind="active",
            discord_kv=[
                ("event", "membership.activated.pending"),
            ],
            whop_brief=whop_brief,
        )
        await _log_member_status("", embed=detailed)
    
    # Verify with API after processing
    await _verify_webhook_with_api(member, event_data, "membership.activated.pending")


async def handle_membership_billing_issue_access_risk(member: discord.Member, event_data: dict):
    """Handle billing issue risk card (staff alert; no role removals)."""
    await handle_membership_deactivated(
        member,
        event_data,
        case_channel_name=PAYMENT_FAILURE_CHANNEL_NAME,
        title_override="⚠️ Billing Issue (Access Risk)",
        color_override=0xFEE75C,
    )


async def handle_membership_deactivated(
    member: discord.Member,
    event_data: dict,
    *,
    case_channel_name: str | None = None,
    title_override: str | None = None,
    color_override: int | None = None,
):
    """Handle membership deactivation - log + alert only (no role removals).

    Role removals are owned by the periodic/startup Whop sync in RSCheckerbot/main.py.
    This handler must fail-closed: if membership_id is missing/uncertain or API checks fail,
    leave roles untouched and alert staff for verification.
    """
    guild = member.guild
    
    member_role = guild.get_role(ROLE_CANCEL_A)
    welcome_role = guild.get_role(ROLE_CANCEL_B)

    # Decide destination channel early (used for kind + alerts).
    dest = case_channel_name or MEMBER_CANCELLATION_CHANNEL_NAME

    # If Whop reports "deactivated/canceled" but cancel_at_period_end=true, access continues
    # until the end of the current billing period. In that case, do not remove roles early.
    still_entitled: bool | None = None
    entitlement_checked = False
    entitlement_error = ""
    lifetime_protected = _has_lifetime_role(member)
    membership_id = ""
    membership_id_source = ""
    try:
        membership_id = _safe_get(event_data or {}, "membership_id", "membership.id", default="").strip()
        if membership_id == "—":
            membership_id = ""
        if membership_id:
            membership_id_source = "event"
    except Exception:
        membership_id = ""
        membership_id_source = ""

    if _cards_use_api() and membership_id and _whop_api_client:
        try:
            m = await _whop_api_client.get_membership_by_id(membership_id)
            entitled, _until_dt, _why = await _whop_api_client.is_entitled_until_end(
                membership_id,
                m if isinstance(m, dict) else None,
                cache_path=str(PAYMENT_CACHE_FILE),
                monthly_days=30,
                grace_days=3,
                now=datetime.now(timezone.utc),
            )
            still_entitled = bool(entitled)
            entitlement_checked = True
        except Exception as e:
            still_entitled = None
            entitlement_checked = False
            entitlement_error = str(e)[:180]
    else:
        # Fail-closed: without a membership_id or API client, we cannot safely conclude entitlement.
        still_entitled = None
    
    roles_would_remove: list[discord.Role] = []
    if (not lifetime_protected) and (still_entitled is False):
        if member_role and member_role in member.roles:
            roles_would_remove.append(member_role)
        if welcome_role and welcome_role in member.roles:
            roles_would_remove.append(welcome_role)
    
    if _log_member_status:
        whop_brief = await _whop_brief_from_event(member, event_data)
        access = _access_roles_compact(member)
        # still_entitled:
        # - True  => cancellation scheduled (access continues)
        # - False => deactivated and not entitled
        # - None  => unverified (missing membership_id / API unavailable / API error)
        if title_override:
            title = title_override
        elif lifetime_protected:
            title = "Lifetime Access — No Role Removal"
        elif still_entitled is True:
            title = "⚠️ Cancellation Scheduled"
        elif still_entitled is False:
            title = "🟧 Membership Deactivated"
        else:
            title = "🟧 Membership Deactivated (Unverified)"
        color = int(color_override) if isinstance(color_override, int) else 0xFEE75C
        kind = (
            "payment_failed"
            if ("payment failed" in title.lower() or dest == PAYMENT_FAILURE_CHANNEL_NAME)
            else ("cancellation_scheduled" if still_entitled is True else "deactivated")
        )
        detailed = build_member_status_detailed_embed(
            title=title,
            member=member,
            access_roles=access,
            color=color,
            event_kind=kind,
            discord_kv=[
                ("event", "membership.deactivated"),
                ("role_removal", "disabled (sync-only)"),
                ("roles_would_remove", ", ".join([r.name for r in roles_would_remove]) if roles_would_remove else "—"),
                ("lifetime_role_protected", "true" if lifetime_protected else "false"),
                ("membership_id_source", membership_id_source or "—"),
                ("entitlement_checked", "true" if entitlement_checked else "false"),
                ("entitlement_error", entitlement_error or "—"),
            ],
            whop_brief=whop_brief,
        )
        await _log_member_status("", embed=detailed)

        # Minimal alert -> dedicated case channel (defaults to member-cancelation)
        minimal = build_case_minimal_embed(
            title=title,
            member=member,
            access_roles=access,
            whop_brief=whop_brief,
            color=color,
            event_kind=kind,
        )
        issue_key = f"whop.deactivated:{_membership_id_from_event(member, event_data)}"
        if await should_post_and_record_alert(
            STAFF_ALERTS_FILE,
            discord_id=member.id,
            issue_key=issue_key,
            cooldown_hours=6.0,
        ):
            await _log_member_status("", embed=minimal, channel_name=dest)
    
    # Verify with API after processing
    await _verify_webhook_with_api(member, event_data, "membership.deactivated")


async def handle_membership_deactivated_payment_failure(member: discord.Member, event_data: dict):
    """Handle payment failure deactivation"""
    await handle_membership_deactivated(
        member,
        event_data,
        case_channel_name=PAYMENT_FAILURE_CHANNEL_NAME,
        title_override="❌ Payment Failed — Action Needed",
        color_override=0xED4245,
    )


async def handle_payment_renewal(member: discord.Member, event_data: dict):
    """Handle payment renewal - ensure Member role is assigned and log with support card embed"""
    guild = member.guild
    member_role = guild.get_role(ROLE_CANCEL_A)
    
    if member_role and member_role not in member.roles:
        await member.add_roles(member_role, reason="Whop: Payment renewal")
        log.info(f"Assigned Member role to {member} for payment renewal")
    
    if _log_member_status:
        whop_brief = await _whop_brief_from_event(member, event_data)
        access = _access_roles_compact(member)
        detailed = build_member_status_detailed_embed(
            title="✅ Payment Renewed",
            member=member,
            access_roles=access,
            color=0x57F287,
            event_kind="active",
            discord_kv=[("event", "payment.succeeded.renewal")],
            whop_brief=whop_brief,
        )
        await _log_member_status("", embed=detailed)
    
    # Verify with API after processing
    await _verify_webhook_with_api(member, event_data, "payment.succeeded.renewal")


async def handle_payment_activation(member: discord.Member, event_data: dict):
    """Handle first payment - assign Member role and log with support card embed"""
    guild = member.guild
    member_role = guild.get_role(ROLE_CANCEL_A)
    
    if member_role and member_role not in member.roles:
        await member.add_roles(member_role, reason="Whop: Payment activation")
        log.info(f"Assigned Member role to {member} for payment activation")
    
    if _log_member_status:
        whop_brief = await _whop_brief_from_event(member, event_data)
        access = _access_roles_compact(member)
        detailed = build_member_status_detailed_embed(
            title="✅ Payment Activated",
            member=member,
            access_roles=access,
            color=0x57F287,
            event_kind="active",
            discord_kv=[
                ("event", "payment.succeeded.activation"),
                ("roles_added", member_role.name if member_role else "—"),
            ],
            whop_brief=whop_brief,
        )
        await _log_member_status("", embed=detailed)
    
    # Verify with API after processing
    await _verify_webhook_with_api(member, event_data, "payment.succeeded.activation")


async def handle_payment_failed(member: discord.Member, event_data: dict):
    """Handle payment failure - log with support card embed"""
    if _log_member_status:
        whop_brief = await _whop_brief_from_event(member, event_data)
        access = _access_roles_compact(member)
        detailed = build_member_status_detailed_embed(
            title="❌ Payment Failed — Action Needed",
            member=member,
            access_roles=access,
            color=0xED4245,
            event_kind="payment_failed",
            discord_kv=[("event", "payment.failed")],
            whop_brief=whop_brief,
        )
        await _log_member_status("", embed=detailed)

        minimal = build_case_minimal_embed(
            title="❌ Payment Failed — Action Needed",
            member=member,
            access_roles=access,
            whop_brief=whop_brief,
            color=0xED4245,
            event_kind="payment_failed",
        )
        issue_key = f"whop.payment_failed:{_membership_id_from_event(member, event_data)}"
        if await should_post_and_record_alert(
            STAFF_ALERTS_FILE,
            discord_id=member.id,
            issue_key=issue_key,
            cooldown_hours=2.0,
        ):
            await _log_member_status("", embed=minimal, channel_name=PAYMENT_FAILURE_CHANNEL_NAME)


async def handle_payment_refunded(member: discord.Member, event_data: dict):
    """Handle payment refund - treat as deactivation (log-only; no removals)."""
    await handle_membership_deactivated(member, event_data)


async def handle_waitlist_approved(member: discord.Member, event_data: dict):
    """Handle waitlist approval - same as membership activated"""
    await handle_membership_activated(member, event_data)

