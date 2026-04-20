"""Canonical Whop Standard Webhook → staff ``kind`` / title rules for RSCheckerbot.

Single source of truth for:
- event type normalization (underscore UI vs dot API),
- first-pass ``kind`` from ``(event_type, payload)``,
- membership diff → ``kind`` fallback (``classify_whop_membership_state_change``),
- staff embed title / color / layout key for a ``kind``.

Imported by ``main.py`` (live bot) and ``scripts/audit_whop_webhook_flow.py`` (local audit).
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _deep_get(obj: Any, path: str) -> Any:
    cur = obj
    for part in (path or "").split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def normalize_whop_std_event_type(evt: str) -> str:
    """Normalize Whop webhook ``type`` to canonical dot format (see main.py history)."""
    e = str(evt or "").strip().lower()
    if not e:
        return ""
    if "." in e:
        return e
    if "_" not in e:
        return e
    mapping = {
        "payment_created": "payment.created",
        "payment_succeeded": "payment.succeeded",
        "payment_failed": "payment.failed",
        "payment_pending": "payment.pending",
        "refund_created": "refund.created",
        "refund_updated": "refund.updated",
        "dispute_created": "dispute.created",
        "dispute_updated": "dispute.updated",
        "setup_intent_requires_action": "setup_intent.requires_action",
        "setup_intent_succeeded": "setup_intent.succeeded",
        "setup_intent_canceled": "setup_intent.canceled",
        "invoice_created": "invoice.created",
        "invoice_paid": "invoice.paid",
        "invoice_past_due": "invoice.past_due",
        "invoice_voided": "invoice.voided",
        "withdrawal_created": "withdrawal.created",
        "withdrawal_updated": "withdrawal.updated",
        "payout_method_created": "payout_method.created",
        "payoutmethod_created": "payout_method.created",
        "verification_succeeded": "verification.succeeded",
        "membership_activated": "membership.activated",
        "membership_deactivated": "membership.deactivated",
        "membership_cancel_at_period_end_changed": "membership.cancel_at_period_end_changed",
        "entry_created": "entry.created",
        "entry_approved": "entry.approved",
        "entry_denied": "entry.denied",
        "entry_deleted": "entry.deleted",
        "course_lesson_interaction_completed": "course_lesson_interaction.completed",
        "courselessoninteraction_completed": "course_lesson_interaction.completed",
    }
    return mapping.get(e, e)


def whop_membership_status_bucket(status: str) -> str:
    s = str(status or "").strip().lower()
    if s in {"past_due", "unpaid"}:
        return "payment_failed"
    if s in {"canceled", "cancelled", "completed", "expired"}:
        return "deactivated"
    if s in {"trialing", "pending"}:
        return "trialing"
    return "active"


def classify_whop_membership_state_change(prev: Optional[dict], cur: dict) -> str:
    """Membership snapshot diff → staff ``kind`` (same rules as legacy ``_classify_whop_change``)."""
    cur_status = str(cur.get("status") or "").strip().lower()
    cur_bucket = whop_membership_status_bucket(cur_status)
    cur_cape = bool(cur.get("cancel_at_period_end") is True)

    if not isinstance(prev, dict) or not prev:
        if cur_bucket == "payment_failed":
            return "payment_failed"
        if cur_bucket == "deactivated":
            return "deactivated"
        if cur_cape and cur_bucket in {"active", "trialing"}:
            return "cancellation_scheduled"
        if cur_bucket == "trialing":
            return "membership_joined"
        return "membership_activated"

    prev_status = str(prev.get("status") or "").strip().lower()
    prev_bucket = whop_membership_status_bucket(prev_status)
    prev_cape = bool(prev.get("cancel_at_period_end") is True)

    if prev_bucket in {"payment_failed"} and cur_bucket in {"active", "trialing"}:
        return "access_restored"
    if cur_bucket == "payment_failed" and prev_bucket != "payment_failed":
        return "payment_failed"
    if cur_bucket == "payment_failed" and prev_bucket == "payment_failed":
        if str(cur.get("updated_at") or "") and str(cur.get("updated_at") or "") != str(prev.get("updated_at") or ""):
            return "payment_failed"
    if cur_bucket == "deactivated" and prev_bucket != "deactivated":
        return "deactivated"
    if (not prev_cape) and cur_cape and cur_bucket in {"active", "trialing"}:
        return "cancellation_scheduled"
    if prev_cape and (not cur_cape) and prev_bucket in {"active", "trialing"}:
        return "cancellation_removed"
    if cur_bucket in {"active", "trialing"} and prev_bucket in {"active", "trialing"}:
        cur_end = str(cur.get("renewal_period_end") or cur.get("renewal_end") or "").strip()
        prev_end = str(prev.get("renewal_period_end") or prev.get("renewal_end") or "").strip()
        if cur_end and cur_end != prev_end:
            return "payment_succeeded"
    return ""


def infer_staff_kind_from_std_webhook(evt: str, payload: dict) -> str:
    """First-pass staff ``kind`` from normalized event type + payload (live bot order)."""
    evt_l = normalize_whop_std_event_type(evt)
    if evt_l == "payment.created":
        return "payment_created"
    if evt_l == "payment.pending":
        return "payment_pending"
    if evt_l == "setup_intent.requires_action":
        return "setup_intent_requires_action"
    if evt_l == "setup_intent.succeeded":
        return "setup_intent_succeeded"
    if evt_l == "setup_intent.canceled":
        return "setup_intent_canceled"
    if evt_l == "entry.created":
        return "entry_created"
    if evt_l == "entry.approved":
        return "entry_approved"
    if evt_l == "entry.denied":
        return "entry_denied"
    if evt_l == "entry.deleted":
        return "entry_deleted"
    if evt_l == "course_lesson_interaction.completed":
        return "course_lesson_completed"
    if evt_l == "invoice.created":
        return "invoice_created"
    if evt_l == "invoice.paid":
        return "invoice_paid"
    if evt_l == "invoice.past_due":
        return "invoice_past_due"
    if evt_l == "invoice.voided":
        return "invoice_voided"
    if evt_l == "refund.created":
        return "refund_created"
    if evt_l == "refund.updated":
        return "refund_updated"
    if evt_l == "dispute.created":
        return "dispute_created"
    if evt_l == "dispute.updated":
        return "dispute_updated"
    if "payment" in evt_l and "failed" in evt_l:
        return "payment_failed"
    if "payment" in evt_l and ("succeeded" in evt_l or "paid" in evt_l):
        return "payment_succeeded"
    if "membership" in evt_l and any(x in evt_l for x in ("deactivated", "canceled", "cancelled", "expired", "ended")):
        return "deactivated"
    if evt_l == "membership.cancel_at_period_end_changed":
        cape = bool(_deep_get(payload, "data.cancel_at_period_end") is True)
        return "cancellation_scheduled" if cape else "cancellation_removed"
    if "cancel" in evt_l and any(x in evt_l for x in ("removed", "unscheduled", "resumed")):
        return "cancellation_removed"
    if "membership" in evt_l and any(x in evt_l for x in ("created", "activated", "purchased", "generated", "started")):
        return "membership_activated"
    return ""


def membership_cur_snapshot_from_std_payload(payload: dict) -> dict:
    """Build a ``cur``-shaped dict from webhook ``data`` only (audit helper; live bot uses API brief)."""
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    renewal_end = str(
        data.get("renewal_period_end")
        or data.get("renewal_end")
        or _deep_get(data, "membership.renewal_period_end")
        or ""
    ).strip()
    return {
        "status": str(data.get("status") or "").strip().lower(),
        "cancel_at_period_end": bool(data.get("cancel_at_period_end") is True),
        "renewal_period_end": renewal_end,
        "renewal_end": renewal_end,
        "updated_at": str(data.get("updated_at") or "").strip(),
    }


def title_color_layout_for_whop_staff_kind(kind: str) -> tuple[str, int, str]:
    """Staff embed title, color, and layout key for ``kind`` (same literals as legacy ``_title_for_event``)."""
    k = str(kind or "").strip().lower()
    if k == "payment_created":
        return ("🧾 Payment Created", 0x5865F2, "payment_created")
    if k == "payment_pending":
        return ("⏳ Payment Pending", 0xFEE75C, "payment_pending")
    if k == "setup_intent_requires_action":
        return ("⚠️ Setup Intent — Requires Action", 0xED4245, "setup_intent")
    if k == "setup_intent_succeeded":
        return ("✅ Setup Intent Succeeded", 0x57F287, "setup_intent")
    if k == "setup_intent_canceled":
        return ("🟨 Setup Intent Canceled", 0xFEE75C, "setup_intent")
    if k == "entry_created":
        return ("📩 Entry Created", 0x5865F2, "entry")
    if k == "entry_approved":
        return ("✅ Entry Approved", 0x57F287, "entry")
    if k == "entry_denied":
        return ("⛔ Entry Denied", 0xED4245, "entry")
    if k == "entry_deleted":
        return ("🗑️ Entry Deleted", 0xFEE75C, "entry")
    if k == "course_lesson_completed":
        return ("📚 Lesson Completed", 0x5865F2, "course")
    if k == "invoice_created":
        return ("🧾 Invoice Created", 0x5865F2, "invoice")
    if k == "invoice_paid":
        return ("✅ Invoice Paid", 0x57F287, "invoice")
    if k == "invoice_past_due":
        return ("⚠️ Invoice Past Due", 0xED4245, "invoice")
    if k == "invoice_voided":
        return ("🗑️ Invoice Voided", 0xFEE75C, "invoice")
    if k == "payment_failed":
        return ("❌ Payment Failed — Action Needed", 0xED4245, "payment_failed")
    if k == "payment_succeeded":
        return ("✅ Payment Succeeded", 0x57F287, "active")
    if k == "refund_created":
        return ("↩️ Refund Created", 0xFEE75C, "refund_created")
    if k == "refund_updated":
        return ("↩️ Refund Updated", 0xFEE75C, "refund_updated")
    if k == "dispute_created":
        return ("⚠️ Dispute Created", 0xED4245, "dispute")
    if k == "dispute_updated":
        return ("⚠️ Dispute Updated", 0xED4245, "dispute")
    if k == "cancellation_scheduled":
        return ("⚠️ Cancellation Scheduled", 0xFEE75C, "cancellation_scheduled")
    if k == "cancellation_removed":
        return ("✅ Cancellation Removed", 0x57F287, "active")
    if k == "deactivated":
        return ("🟧 Membership Deactivated", 0xFEE75C, "deactivated")
    if k == "access_restored":
        return ("✅ Access Restored", 0x57F287, "active")
    if k == "membership_joined":
        return ("👋 Member Joined", 0x5865F2, "active")
    if k == "membership_activated":
        return ("✅ Membership Activated", 0x57F287, "active")
    return ("✅ Membership Activated", 0x57F287, "active")
