"""Locked + atomic ingest for `#member-status-logs` → `data/member_status_logs_events.json`.

This store is server-owned runtime data (NOT synced). It is intended to provide a
deterministic, replayable ledger of staff cards posted into the member-status-logs
channel, keyed per Discord user id and per Discord message id.

Design goals:
- One source of truth for the member-status-logs ledger (this module).
- No PII persistence (no emails, names).
- Stable, append/upsert-by-message_id semantics (edits update the same message_id entry).
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path

import discord

from rschecker_utils import load_json, save_json

_MSL_EVENTS_LOCK = asyncio.Lock()


def _iso(dt: object) -> str:
    try:
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    return ""


def _ensure_shape(raw: object, *, source_channel_id: int, source_channel_name: str) -> dict:
    d = raw if isinstance(raw, dict) else {}
    meta = d.get("meta") if isinstance(d.get("meta"), dict) else {}
    by_did = d.get("by_discord_id") if isinstance(d.get("by_discord_id"), dict) else {}
    meta.setdefault("version", 1)
    meta["source_channel_id"] = int(source_channel_id or 0)
    meta["source_channel_name"] = str(source_channel_name or "").strip() or "member-status-logs"
    meta.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    meta.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    meta.setdefault("unique_members", 0)
    meta.setdefault("total_cards", 0)
    return {"meta": meta, "by_discord_id": by_did}


def _safe_brief(brief: dict | None) -> dict:
    b = brief if isinstance(brief, dict) else {}
    out: dict = {}
    # Keep only staff-safe, non-PII fields that help replay ticket logic.
    for k in (
        "membership_id",
        "product",
        "status",
        "trial_days",
        "pricing",
        "total_spent",
        "remaining_days",
        "renewal_end",
        "renewal_end_iso",
        "dashboard_url",
        "cancel_at_period_end",
        "connected_discord",
        "plan_is_renewal",
        "is_first_membership",
        "customer_since",
    ):
        v = b.get(k)
        if v is None:
            continue
        out[str(k)] = str(v)[:512]
    return out


def _member_header_update(header: dict, *, now_iso: str, kind: str, title: str, membership_id: str, status: str, product: str) -> dict:
    h = header if isinstance(header, dict) else {}
    h["last_seen_at"] = now_iso
    if membership_id:
        h["membership_id"] = membership_id[:128]
    if status:
        h["status"] = status[:64]
    if product:
        h["product"] = product[:128]
    if kind:
        h["last_kind"] = kind[:64]
    if title:
        h["last_title"] = title[:256]
    return h


def _load(path: Path, *, source_channel_id: int, source_channel_name: str) -> dict:
    return _ensure_shape(load_json(path), source_channel_id=source_channel_id, source_channel_name=source_channel_name)


def _save(path: Path, store: dict) -> None:
    try:
        meta = store.get("meta") if isinstance(store.get("meta"), dict) else {}
        meta["updated_at"] = datetime.now(timezone.utc).isoformat()
        store["meta"] = meta
    except Exception:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    save_json(path, store)


async def upsert_member_status_logs_message(
    *,
    events_path: Path,
    configured_channel_id: int,
    message: discord.Message,
    kind: str,
    discord_id: int | None,
    whop_brief: dict | None,
    source_name: str = "member-status-logs",
) -> bool:
    """Upsert one member-status-logs staff card into `member_status_logs_events.json`.

    Returns True if the file changed.
    """
    if int(configured_channel_id or 0) <= 0:
        return False
    if int(getattr(getattr(message, "channel", None), "id", 0) or 0) != int(configured_channel_id):
        return False
    if not getattr(message, "embeds", None):
        return False
    e0 = message.embeds[0]
    if not isinstance(e0, discord.Embed):
        return False
    did = int(discord_id or 0)
    if did <= 0:
        return False
    mid = int(getattr(message, "id", 0) or 0)
    if mid <= 0:
        return False

    title = str(getattr(e0, "title", "") or "").strip() or "(no title)"
    created_iso = ""
    with suppress(Exception):
        created_iso = _iso(getattr(message, "created_at", None))
    jump = str(getattr(message, "jump_url", "") or "").strip()
    k = str(kind or "").strip().lower() or "unknown"
    brief = _safe_brief(whop_brief)

    membership_id = str(brief.get("membership_id") or "").strip()
    status = str(brief.get("status") or "").strip()
    product = str(brief.get("product") or "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()

    async with _MSL_EVENTS_LOCK:
        store = _load(events_path, source_channel_id=int(configured_channel_id), source_channel_name=source_name)
        by_did = store.get("by_discord_id") if isinstance(store.get("by_discord_id"), dict) else {}
        rec = by_did.get(str(did))
        if not isinstance(rec, dict):
            rec = {"header": {}, "cards": {}}
        header = rec.get("header") if isinstance(rec.get("header"), dict) else {}
        cards = rec.get("cards") if isinstance(rec.get("cards"), dict) else {}

        before = cards.get(str(mid))
        entry = {
            "message_id": int(mid),
            "jump_url": jump[:400],
            "created_at_iso": (created_iso or now_iso),
            "observed_at_iso": now_iso,
            "kind": k[:64],
            "title": title[:256],
            "whop_brief": brief,
        }
        cards[str(mid)] = entry
        rec["cards"] = cards
        rec["header"] = _member_header_update(
            header,
            now_iso=now_iso,
            kind=k,
            title=title,
            membership_id=membership_id,
            status=status,
            product=product,
        )
        by_did[str(did)] = rec
        store["by_discord_id"] = by_did

        # Update meta counters (best-effort; tolerate drift)
        try:
            meta = store.get("meta") if isinstance(store.get("meta"), dict) else {}
            meta["unique_members"] = int(len(by_did))
            if before is None:
                meta["total_cards"] = int(meta.get("total_cards") or 0) + 1
            store["meta"] = meta
        except Exception:
            pass

        changed = before != entry
        if changed:
            _save(events_path, store)
        return changed

