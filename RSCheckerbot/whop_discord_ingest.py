"""Live + locked ingest for Whop native `#whop-logs` → `data/whop_logs_events.json`.

Canonical file shape matches `_save_events_by_email` in `main.py` (meta + by_email).
Discord ID resolution helpers read the same store (asyncio-locked for consistency with writes).
"""
from __future__ import annotations

import asyncio
import re
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path

import discord

from rschecker_utils import load_json, save_json
from whop_webhook_handler import _extract_discord_id_from_embed, _extract_email_from_embed

_WHOP_LOG_EVENTS_LOCK = asyncio.Lock()
_MEM_ID_RE = re.compile(r"\b(mem_[A-Za-z0-9]+)\b", re.I)


def _membership_hint_from_whop_logs_key_field(key_raw: str) -> str:
    """Return `mem_...` or Whop `R-...` key from native #whop-logs `key` field, or ""."""
    s = str(key_raw or "").strip()
    if not s:
        return ""
    m = _MEM_ID_RE.search(s)
    if m:
        return str(m.group(1) or "").strip()[:128]
    tok = s.split()[0].strip()
    if tok.startswith("R-") and len(tok) >= 10:
        return tok[:128]
    return ""


def membership_hints_by_discord_id_from_events_file(events_path: Path) -> dict[str, str]:
    """Single pass over `by_email`: numeric Discord id → best membership key for API lookups.

    Picks the newest event by `created_at_iso` (lexicographic ISO works for UTC offsets used here).
    On timestamp ties, prefers `mem_` over `R-` keys.
    """
    by_email = _load_by_email(events_path)
    best_hint: dict[str, str] = {}
    best_iso: dict[str, str] = {}
    for rec in (by_email or {}).values():
        if not isinstance(rec, dict):
            continue
        did = str(rec.get("discord_id") or "").strip()
        if not did.isdigit():
            continue
        evs = rec.get("events")
        if not isinstance(evs, dict):
            continue
        for ev in evs.values():
            if not isinstance(ev, dict):
                continue
            hint = _membership_hint_from_whop_logs_key_field(str(ev.get("key") or ""))
            if not hint:
                continue
            iso = str(ev.get("created_at_iso") or "")
            old_h = best_hint.get(did, "")
            old_i = best_iso.get(did, "")
            if not old_h:
                best_hint[did] = hint
                best_iso[did] = iso
                continue
            if iso > old_i:
                best_hint[did] = hint
                best_iso[did] = iso
            elif iso == old_i and hint.startswith("mem_") and not old_h.startswith("mem_"):
                best_hint[did] = hint
                best_iso[did] = iso
    return best_hint


def _norm_title_key(title: str) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip().lower())[:80] or "unknown"


def _load_by_email(path: Path) -> dict[str, dict]:
    raw = load_json(path)
    if not isinstance(raw, dict):
        return {}
    be = raw.get("by_email")
    return be if isinstance(be, dict) else {}


def _save_by_email(path: Path, by_email: dict, *, source_name: str, channel_id: int) -> None:
    result = {
        "meta": {
            "source_channel_id": channel_id,
            "source_channel_name": source_name,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "unique_emails": len(by_email),
        },
        "by_email": by_email,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    save_json(path, result)


def merge_whop_logs_embed_into_by_email(
    by_email: dict,
    *,
    embed: discord.Embed,
    title: str,
    message_id: int,
    jump_url: str,
    created_at_iso: str,
) -> bool:
    """Same semantics as `main._apply_whop_logs_event_to_by_email`. Returns True if merged."""
    email = str(_extract_email_from_embed(embed) or "").strip().lower()
    if not email or "@" not in email:
        return False
    did = str(_extract_discord_id_from_embed(embed) or "").strip()
    key_val = ""
    access_pass = ""
    mstatus = ""
    with suppress(Exception):
        for f in (getattr(embed, "fields", None) or []):
            n = str(getattr(f, "name", "") or "").strip().lower()
            v = str(getattr(f, "value", "") or "").strip()
            if n == "key":
                key_val = v
            elif n in {"access pass", "access_pass"}:
                access_pass = v
            elif n in {"membership status", "membership_status", "status"}:
                mstatus = v
    evt = {
        "created_at_iso": created_at_iso,
        "message_id": int(message_id or 0),
        "jump_url": (jump_url or "")[:400],
        "title": (title or "")[:256],
        "membership_status": (mstatus or "")[:64],
        "access_pass": (access_pass or "")[:128],
        "key": (key_val or "")[:128],
    }
    if email not in by_email:
        by_email[email] = {"discord_id": did or "", "events": {}}
    rec = by_email[email]
    if did:
        rec["discord_id"] = did
    tkey = _norm_title_key(title)
    rec.setdefault("events", {})[tkey] = evt
    return True


async def append_whop_logs_discord_message(
    *,
    events_path: Path,
    configured_channel_id: int,
    message: discord.Message,
    source_name: str = "whop-logs",
) -> bool:
    """Upsert one `#whop-logs` message into `whop_logs_events.json`. Returns True if file changed."""
    if int(configured_channel_id or 0) <= 0:
        return False
    if int(getattr(getattr(message, "channel", None), "id", 0) or 0) != int(configured_channel_id):
        return False
    if not message.embeds:
        return False
    e0 = message.embeds[0]
    if not isinstance(e0, discord.Embed):
        return False
    title = str(getattr(e0, "title", "") or "").strip() or "(no title)"
    jump = str(getattr(message, "jump_url", "") or "").strip()
    created_iso = ""
    with suppress(Exception):
        if getattr(message, "created_at", None):
            created_iso = message.created_at.astimezone(timezone.utc).isoformat()  # type: ignore[union-attr]
    mid = int(getattr(message, "id", 0) or 0)

    async with _WHOP_LOG_EVENTS_LOCK:
        by_email = _load_by_email(events_path)
        changed = merge_whop_logs_embed_into_by_email(
            by_email,
            embed=e0,
            title=title,
            message_id=mid,
            jump_url=jump,
            created_at_iso=created_iso,
        )
        if changed:
            _save_by_email(events_path, by_email, source_name=source_name, channel_id=int(configured_channel_id))
        return changed


async def lookup_discord_id_for_email(events_path: Path, email: str) -> str:
    """Return numeric Discord user id string, or ""."""
    em = str(email or "").strip().lower()
    if not em or "@" not in em:
        return ""
    async with _WHOP_LOG_EVENTS_LOCK:
        by_email = _load_by_email(events_path)
        rec = by_email.get(em) if isinstance(by_email, dict) else None
        if not isinstance(rec, dict):
            return ""
        did = str(rec.get("discord_id") or "").strip()
        return did if did.isdigit() else ""


def lookup_discord_id_for_email_sync(events_path: Path, email: str) -> str:
    """Sync read (no lock). Prefer `lookup_discord_id_for_email` from async code when possible."""
    em = str(email or "").strip().lower()
    if not em or "@" not in em:
        return ""
    by_email = _load_by_email(events_path)
    rec = by_email.get(em) if isinstance(by_email, dict) else None
    if not isinstance(rec, dict):
        return ""
    did = str(rec.get("discord_id") or "").strip()
    return did if did.isdigit() else ""
