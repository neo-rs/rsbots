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
import sys
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
from rschecker_utils import access_roles_plain, coerce_role_ids, fmt_date_any, usd_amount, save_json
from staff_embeds import build_case_minimal_embed, build_member_status_detailed_embed
from whop_webhook_handler import _extract_email_from_embed as _extract_email_from_native_embed
from whop_webhook_handler import _extract_discord_id_from_embed as _extract_discord_id_from_native_embed


BASE_DIR = Path(__file__).resolve().parent
_PROBE_STAFFCARDS_DEDUPE_FILE = BASE_DIR / ".probe_staffcards_sent.json"
_MEMBER_HISTORY_FILE = BASE_DIR / "member_history.json"
_WHOP_IDENTITY_CACHE_FILE = BASE_DIR / "whop_identity_cache.json"
_PROBE_WHOPLOGS_STATE_FILE = BASE_DIR / ".probe_whoplogs_baseline_state.json"


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


def _mid_from_member_history(did: int) -> str:
    raw = _load_json_file(_MEMBER_HISTORY_FILE)
    if not isinstance(raw, dict):
        return ""
    rec = raw.get(str(int(did))) if did else None
    if not isinstance(rec, dict):
        return ""
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    if not isinstance(wh, dict):
        return ""
    return str(wh.get("last_membership_id") or wh.get("last_whop_key") or "").strip()


def _linked_discord_id_from_identity_cache(email: str) -> int:
    """Best-effort: email -> discord_id cache built from native Whop cards."""
    em = str(email or "").strip().lower()
    if not em or "@" not in em:
        return 0
    raw = _load_json_file(_WHOP_IDENTITY_CACHE_FILE)
    if not isinstance(raw, dict):
        return 0
    rec = raw.get(em)
    if not isinstance(rec, dict):
        return 0
    did = str(rec.get("discord_id") or "").strip()
    return int(did) if did.isdigit() else 0


def _email_from_identity_cache_by_discord_id(discord_id: int) -> str:
    """Best-effort reverse lookup: discord_id -> email from whop_identity_cache.json."""
    did = int(discord_id or 0)
    if did <= 0:
        return ""
    raw = _load_json_file(_WHOP_IDENTITY_CACHE_FILE)
    if not isinstance(raw, dict):
        return ""
    for em, rec in raw.items():
        if not isinstance(rec, dict):
            continue
        v = str(rec.get("discord_id") or "").strip()
        if v.isdigit() and int(v) == did and ("@" in str(em or "")):
            return str(em).strip().lower()
    return ""


def _ensure_member_history_whop_shape(rec: dict) -> dict:
    if not isinstance(rec, dict):
        rec = {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    if not isinstance(wh, dict):
        wh = {}
    rec["whop"] = wh
    return rec


def _update_member_history_from_whop_log_hit(
    *,
    discord_id: int,
    title: str,
    created_at_iso: str,
    message_id: int,
    jump_url: str,
    whop_key: str,
    membership_status: str,
    access_pass: str,
    source_channel_id: int,
) -> bool:
    """Write a minimal, non-bloated per-title record into member_history.json (no PII)."""
    did = int(discord_id or 0)
    if did <= 0:
        return False
    db = _load_json_file(_MEMBER_HISTORY_FILE)
    if not isinstance(db, dict):
        db = {}
    rec = db.get(str(did), {})
    rec = _ensure_member_history_whop_shape(rec if isinstance(rec, dict) else {})
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    if not isinstance(wh, dict):
        wh = {}

    # Keep membership identifiers up-to-date (both mem_... and R-... can be used as membership keys in this project).
    key0 = str(whop_key or "").strip()
    if key0.startswith(("mem_", "R-")):
        wh["last_whop_key"] = key0
        wh["last_membership_id"] = key0

    # Per-title latest record (no arrays; no bloat).
    tkey = re.sub(r"\s+", " ", str(title or "").strip().lower())[:80] or "unknown"
    latest = wh.get("native_whop_logs_latest") if isinstance(wh.get("native_whop_logs_latest"), dict) else {}
    if not isinstance(latest, dict):
        latest = {}
    latest[tkey] = {
        "title": str(title or "").strip()[:256],
        "created_at": str(created_at_iso or "").strip()[:64],
        "message_id": int(message_id or 0),
        "jump_url": str(jump_url or "").strip()[:300],
        "key": str(whop_key or "").strip()[:128],
        "membership_status": str(membership_status or "").strip()[:64],
        "access_pass": str(access_pass or "").strip()[:128],
        "source_channel_id": int(source_channel_id or 0),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    # Cap titles per user to avoid bloat.
    try:
        if len(latest) > 25:
            # Keep most recent 25 by recorded_at.
            items = list(latest.items())
            items.sort(key=lambda kv: str((kv[1] or {}).get("recorded_at") or ""), reverse=True)
            latest = dict(items[:25])
    except Exception:
        pass

    wh["native_whop_logs_latest"] = latest
    rec["whop"] = wh
    db[str(did)] = rec
    try:
        save_json(Path(_MEMBER_HISTORY_FILE), db)
    except Exception:
        _save_json_file(Path(_MEMBER_HISTORY_FILE), db)
    return True


def _load_probe_state(p: Path) -> dict:
    raw = _load_json_file(p)
    return raw if isinstance(raw, dict) else {}


def _save_probe_state(p: Path, data: dict) -> None:
    try:
        if not isinstance(data, dict):
            data = {}
        save_json(p, data)
    except Exception:
        _save_json_file(p, data if isinstance(data, dict) else {})


async def _probe_whoplogs_baseline(args: argparse.Namespace) -> int:
    """Scan a whop-logs channel and write a baseline into member_history.json (no PII).

    This is designed to be run repeatedly in batches:
    - First run scans newest -> older messages.
    - It records a resume cursor (oldest scanned message id) into a state file.
    - Subsequent runs can pass --resume to continue scanning older history.
    """
    cfg = load_config()
    token = str(cfg.get("bot_token") or "").strip()
    if not token:
        print("Missing bot_token in config.secrets.json")
        return 2

    try:
        channel_id = int(str(getattr(args, "channel_id", "") or "").strip())
    except Exception:
        channel_id = 0
    if not channel_id:
        print("Missing --channel-id.")
        return 2

    limit = int(getattr(args, "limit", 5000) or 5000)
    limit = max(50, min(limit, 20000))
    run_until_done = bool(getattr(args, "run_until_done", False))
    batch_delay_s = float(getattr(args, "batch_delay_seconds", 1.0) or 1.0)
    batch_delay_s = max(0.0, min(batch_delay_s, 10.0))
    max_batches = int(getattr(args, "max_batches", 0) or 0)
    max_batches = max(0, min(max_batches, 1000000))
    interactive = bool(getattr(args, "interactive", False))
    checkpoint_every = int(getattr(args, "checkpoint_every", 0) or 0)
    checkpoint_every = max(0, min(checkpoint_every, 50000))

    state_path = Path(str(getattr(args, "state_file", "") or "").strip() or str(_PROBE_WHOPLOGS_STATE_FILE))
    state = _load_probe_state(state_path)

    before_id_raw = str(getattr(args, "before_message_id", "") or "").strip()
    resume = bool(getattr(args, "resume", False))
    if (not before_id_raw) and resume:
        before_id_raw = str(state.get("before_message_id") or "").strip()

    before_obj = discord.Object(id=int(before_id_raw)) if before_id_raw.isdigit() else None

    do_record = bool(getattr(args, "record_member_history", False))
    confirm = str(getattr(args, "confirm", "") or "").strip().lower()
    if do_record and confirm != "confirm":
        print("Confirmation required to write member_history.json. Use: --record-member-history --confirm confirm")
        return 2

    intents = discord.Intents.none()
    intents.guilds = True
    intents.messages = True
    intents.message_content = False
    bot = discord.Client(intents=intents)

    @bot.event
    async def on_ready():
        nonlocal state
        progress_every = int(getattr(args, "progress_every", 200) or 200)
        progress_every = max(0, min(progress_every, 5000))
        bar_w = int(getattr(args, "bar_width", 24) or 24)
        bar_w = max(10, min(bar_w, 60))

        def _short_iso(ts: str) -> str:
            s = str(ts or "").strip()
            # Keep it compact: YYYY-MM-DD HH:MM:SS (UTC)
            if "T" in s:
                s = s.replace("T", " ")
            if s.endswith("+00:00"):
                s = s[:-6] + "Z"
            return s[:19] + ("Z" if s.endswith("Z") else "")

        def _progress_line(*, scanned: int, extracted: int, unique_ids: int, newest: str, oldest: str) -> str:
            if limit <= 0:
                pct = 0
            else:
                pct = int((float(scanned) / float(limit)) * 100.0)
                pct = max(0, min(pct, 100))
            filled = int((pct / 100.0) * bar_w)
            bar = "[" + ("=" * filled) + ("-" * (bar_w - filled)) + "]"
            return (
                f"\r{bar} {pct:3d}% "
                f"scanned={scanned}/{limit} embeds={extracted} unique={unique_ids} "
                f"newest={_short_iso(newest) if newest else '-'} oldest={_short_iso(oldest) if oldest else '-'}"
            )

        print("=== Whop Logs Baseline Scan ===")
        print(f"channel_id: {channel_id}")
        print(f"limit: {limit}")
        print(f"record_member_history: {bool(do_record)}")
        print(f"state_file: {str(state_path)}")
        if progress_every:
            print(f"progress_every: {progress_every}  (live bar)")
        # Channel creation timestamp from snowflake (UTC)
        try:
            ms = (int(channel_id) >> 22) + 1420070400000
            created_dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            print(f"channel_created_utc: {created_dt.isoformat()}")
        except Exception:
            pass
        if before_obj:
            print(f"before_message_id: {int(before_obj.id)}")
        elif resume:
            print("resume: requested, but no saved cursor found (starting from newest).")
        elif interactive:
            # If we have a saved cursor, offer to resume.
            saved = str(state.get("before_message_id") or "").strip()
            if saved.isdigit():
                try:
                    ans = await asyncio.to_thread(input, f"Resume from last saved cursor ({saved})? [Y/n] ")
                except Exception:
                    ans = "y"
                if str(ans or "").strip().lower() not in {"n", "no"}:
                    nonlocal_before = discord.Object(id=int(saved))
                    # Rebind outer before_obj via closure mutation is awkward; store in state and read below.
                    state["_resume_override_before_id"] = str(saved)
                    print(f"resume_selected_before_message_id: {saved}")
        if run_until_done:
            print(f"run_until_done: True (batch_delay_seconds={batch_delay_s} max_batches={max_batches or '∞'})")
        if interactive:
            print("interactive: True (will prompt between batches)")
        if checkpoint_every:
            print(f"checkpoint_every: {checkpoint_every} (write resume cursor periodically)")

        ch = bot.get_channel(channel_id)
        if ch is None:
            with suppress(Exception):
                ch = await bot.fetch_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            print("channel not found or not text.")
            with suppress(Exception):
                await bot.close()
            return

        scanned = 0
        extracted = 0
        updated = 0
        unique_dids: set[int] = set()
        newest_iso = ""
        oldest_iso = ""
        oldest_msg_id = 0

        async def _scan_batch(before: discord.Object | None) -> tuple[int, int, int, str, str, int]:
            """Return (scanned, extracted, updated, newest_iso, oldest_iso, oldest_msg_id)."""
            _scanned = 0
            _extracted = 0
            _updated = 0
            _newest = ""
            _oldest = ""
            _oldest_id = 0

            async for msg in ch.history(limit=limit, before=before):
                _scanned += 1
                _oldest_id = int(getattr(msg, "id", 0) or 0) or _oldest_id
                try:
                    ts = (getattr(msg, "created_at", None) or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
                    if not _newest:
                        _newest = ts
                    _oldest = ts
                except Exception:
                    pass

                if progress_every and (_scanned == 1 or (_scanned % progress_every) == 0 or _scanned == limit):
                    sys.stdout.write(
                        _progress_line(
                            scanned=_scanned,
                            extracted=extracted + _extracted,
                            unique_ids=len(unique_dids),
                            newest=_newest or newest_iso,
                            oldest=_oldest or oldest_iso,
                        )
                    )
                    sys.stdout.flush()
                if checkpoint_every and _oldest_id and (_scanned % checkpoint_every) == 0:
                    # Save an in-progress checkpoint so a crash/timeout can resume.
                    state["before_message_id"] = str(_oldest_id)
                    state["last_scan_in_progress_at"] = datetime.now(timezone.utc).isoformat()
                    state["last_scan_in_progress_oldest_utc"] = str(_oldest or "")
                    _save_probe_state(state_path, state)

                e0 = msg.embeds[0] if msg.embeds else None
                if not isinstance(e0, discord.Embed):
                    continue

                did_txt = str(_extract_discord_id_from_native_embed(e0) or "").strip()
                if not did_txt.isdigit():
                    continue
                did = int(did_txt)
                if did <= 0:
                    continue
                _extracted += 1
                unique_dids.add(did)

                title = str(getattr(e0, "title", "") or "").strip() or "(no title)"
                jump = str(getattr(msg, "jump_url", "") or "").strip()

                key_val = ""
                access_pass = ""
                mstatus = ""
                with suppress(Exception):
                    for f in (getattr(e0, "fields", None) or []):
                        n = str(getattr(f, "name", "") or "").strip().lower()
                        v = str(getattr(f, "value", "") or "").strip()
                        if n == "key":
                            key_val = v
                        elif n in {"access pass", "access_pass"}:
                            access_pass = v
                        elif n in {"membership status", "membership_status", "status"}:
                            mstatus = v

                if do_record:
                    ok = _update_member_history_from_whop_log_hit(
                        discord_id=int(did),
                        title=title,
                        created_at_iso=str(getattr(msg, "created_at", None) or "").strip(),
                        message_id=int(getattr(msg, "id", 0) or 0),
                        jump_url=jump,
                        whop_key=key_val,
                        membership_status=mstatus,
                        access_pass=access_pass,
                        source_channel_id=int(channel_id),
                    )
                    if ok:
                        _updated += 1

            return (_scanned, _extracted, _updated, _newest, _oldest, _oldest_id)

        # If interactive resume selected, use it.
        before_cur = None
        try:
            v = str(state.get("_resume_override_before_id") or "").strip()
            if v.isdigit():
                before_cur = discord.Object(id=int(v))
        except Exception:
            before_cur = None
        if before_cur is None:
            before_cur = before_obj
        batches = 0
        while True:
            batches += 1
            if max_batches and batches > max_batches:
                break
            try:
                b_scanned, b_extracted, b_updated, b_newest, b_oldest, b_oldest_id = await _scan_batch(before_cur)
            except Exception as ex:
                # Save cursor if we have one, then stop.
                if oldest_msg_id:
                    state["before_message_id"] = str(oldest_msg_id)
                    state["last_scan_error_at"] = datetime.now(timezone.utc).isoformat()
                    state["last_scan_error"] = str(ex)[:300]
                    _save_probe_state(state_path, state)
                print(f"\nERROR: scan failed (batch={batches}) err={str(ex)[:200]}")
                break
            scanned += int(b_scanned)
            extracted += int(b_extracted)
            updated += int(b_updated)
            if b_newest and not newest_iso:
                newest_iso = b_newest
            if b_oldest:
                oldest_iso = b_oldest
            if b_oldest_id:
                oldest_msg_id = b_oldest_id

            # Stop if this batch hit the end of available history.
            if b_scanned < limit:
                break
            if not run_until_done:
                break
            # Continue scanning older messages.
            if oldest_msg_id <= 0:
                break
            if interactive:
                try:
                    ans = await asyncio.to_thread(
                        input,
                        f"\nContinue scanning older than {oldest_msg_id} (oldest_utc={_short_iso(oldest_iso) if oldest_iso else '-'})? [Y/n] ",
                    )
                except Exception:
                    ans = "y"
                if str(ans or "").strip().lower() in {"n", "no"}:
                    break
            before_cur = discord.Object(id=int(oldest_msg_id))
            if batch_delay_s:
                await asyncio.sleep(batch_delay_s)

        if progress_every:
            # Finish the live line cleanly
            sys.stdout.write("\n")
            sys.stdout.flush()

        print(f"messages_scanned: {scanned}")
        print(f"embeds_with_discord_id: {extracted}")
        print(f"unique_discord_ids: {len(unique_dids)}")
        if newest_iso and oldest_iso:
            print(f"scan_window_newest_utc: {newest_iso}")
            print(f"scan_window_oldest_utc: {oldest_iso}")
        if do_record:
            print(f"member_history_updates: {updated}")

        # Save resume cursor (oldest message id we reached in this batch).
        if oldest_msg_id > 0:
            state["before_message_id"] = str(oldest_msg_id)
            state["last_scan_completed_at"] = datetime.now(timezone.utc).isoformat()
            state["last_scan_summary"] = {
                "messages_scanned": scanned,
                "unique_discord_ids": len(unique_dids),
                "embeds_with_discord_id": extracted,
                "scan_window_newest_utc": newest_iso,
                "scan_window_oldest_utc": oldest_iso,
                "channel_id": int(channel_id),
                "record_member_history": bool(do_record),
                "run_until_done": bool(run_until_done),
                "batches": int(batches),
            }
            state.pop("_resume_override_before_id", None)
            _save_probe_state(state_path, state)
            print(f"saved_resume_before_message_id: {oldest_msg_id}")

        with suppress(Exception):
            await bot.close()

    async with bot:
        await bot.start(token)
    return 0


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


async def _probe_nowhop_debug(args: argparse.Namespace) -> int:
    """Debug how 'no whop link' is determined for specific Discord IDs.

    This mirrors RSCheckerbot/support_tickets.py scan decision points:
    - If no membership_id is recorded locally for the Discord ID -> ticket uses Discord-only fallback.
    - If membership_id exists -> fetch API-only brief and show connected-discord extraction.
    """
    dids = []
    for x in (getattr(args, "discord_id", []) or []):
        s = str(x or "").strip()
        if s.isdigit():
            dids.append(int(s))
    dids = sorted(set([d for d in dids if d > 0]))
    if not dids:
        print("Missing --discord-id (repeatable).")
        return 2

    client, wh = _init_client_from_local_config()
    api_ready = bool(client and isinstance(wh, dict))
    scan_members = bool(getattr(args, "scan_members", False))
    scan_discord_logs = bool(getattr(args, "scan_discord_logs", False))
    scan_whop_logs = bool(getattr(args, "scan_whop_logs", False))
    record_member_history = bool(getattr(args, "record_member_history", False))
    max_pages = int(getattr(args, "members_max_pages", 10) or 10)
    per_page = int(getattr(args, "members_per_page", 100) or 100)
    max_pages = max(1, min(max_pages, 200))
    per_page = max(10, min(per_page, 200))

    print("=== no_whop_link debug (local) ===")
    print(f"discord_ids: {', '.join(str(d) for d in dids)}")
    print(f"member_history_file: {str(_MEMBER_HISTORY_FILE)} (exists={_MEMBER_HISTORY_FILE.exists()})")
    print(f"identity_cache_file: {str(_WHOP_IDENTITY_CACHE_FILE)} (exists={_WHOP_IDENTITY_CACHE_FILE.exists()})")
    print(f"whop_api_ready: {api_ready}")
    if not api_ready:
        print("NOTE: Whop API not available locally (missing whop_api.api_key or whop_api.company_id).")
        print("      We can still explain why tickets are discord-only fallback when membership_id is missing.")

    found_in_members: dict[int, dict] = {}
    scan_stats = {"pages": 0, "scanned": 0, "discord_ids_found": 0}

    if scan_members and api_ready and client:
        print("\n=== scan: Whop /members (looking for connected Discord IDs) ===")
        after: str | None = None
        pages = 0
        scanned = 0
        did_found = 0
        while pages < max_pages and len(found_in_members) < len(dids):
            batch, page_info = await client.list_members(first=per_page, after=after, params={"order": "joined_at", "direction": "desc"})
            pages += 1
            if not batch:
                break
            for rec in batch:
                if not isinstance(rec, dict):
                    continue
                scanned += 1
                raw = extract_discord_id_from_whop_member_record(rec)
                if str(raw or "").strip().isdigit():
                    did_found += 1
                    d0 = int(str(raw).strip())
                    if d0 in dids and d0 not in found_in_members:
                        found_in_members[d0] = rec
                if len(found_in_members) >= len(dids):
                    break
            after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
            has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
            if (not has_next) or (not after):
                break
        scan_stats = {"pages": pages, "scanned": scanned, "discord_ids_found": did_found}
        print(f"pages_scanned: {pages} (max_pages={max_pages})")
        print(f"members_scanned: {scanned}")
        print(f"members_with_discord_connection_field: {did_found}")
        print(f"targets_found: {len(found_in_members)}/{len(dids)}")

    # Discord-side truth: member-status-logs embeds include a "Connected Discord" field when Whop is linked.
    latest_log: dict[int, dict] = {}
    if scan_discord_logs:
        cfg = load_config()
        token = str(cfg.get("bot_token") or "").strip()
        if not token:
            print("\n=== scan: Discord member-status-logs ===")
            print("Missing bot_token in config.secrets.json (cannot scan Discord logs).")
        else:
            st = cfg.get("support_tickets") if isinstance(cfg, dict) else {}
            dm = cfg.get("dm_sequence") if isinstance(cfg, dict) else {}
            st = st if isinstance(st, dict) else {}
            dm = dm if isinstance(dm, dict) else {}
            try:
                guild_id = int(str(getattr(args, "guild_id", "") or st.get("guild_id") or cfg.get("guild_id") or 0).strip())
            except Exception:
                guild_id = 0
            try:
                ch_id = int(str(getattr(args, "channel_id", "") or dm.get("member_status_logs_channel_id") or 0).strip())
            except Exception:
                ch_id = 0
            hist_lim = int(getattr(args, "history_limit", 800) or 800)
            hist_lim = max(50, min(hist_lim, 5000))
            if not guild_id or not ch_id:
                print("\n=== scan: Discord member-status-logs ===")
                print(f"Missing guild_id or channel_id (guild_id={guild_id} channel_id={ch_id}).")
            else:
                intents = discord.Intents.none()
                intents.guilds = True
                intents.messages = True
                intents.message_content = False
                bot = discord.Client(intents=intents)

                @bot.event
                async def on_ready():
                    print("\n=== scan: Discord member-status-logs ===")
                    print(f"guild_id: {guild_id}")
                    print(f"channel_id: {ch_id}")
                    print(f"history_limit: {hist_lim}")
                    ch = bot.get_channel(ch_id)
                    if ch is None:
                        with suppress(Exception):
                            ch = await bot.fetch_channel(ch_id)
                    if not isinstance(ch, discord.TextChannel):
                        print("channel not found or not text.")
                        with suppress(Exception):
                            await bot.close()
                        return

                    scanned = 0
                    matches = 0
                    async for msg in ch.history(limit=hist_lim):
                        scanned += 1
                        e0 = msg.embeds[0] if msg.embeds else None
                        if not isinstance(e0, discord.Embed):
                            continue
                        # Extract Discord ID and Connected Discord from fields (best-effort).
                        did0 = 0
                        connected = ""
                        membership_id = ""
                        for f in (getattr(e0, "fields", None) or []):
                            n = str(getattr(f, "name", "") or "").strip().lower()
                            v = str(getattr(f, "value", "") or "").strip()
                            if n == "discord id":
                                m = re.search(r"\b(\d{17,19})\b", v)
                                if m:
                                    did0 = int(m.group(1))
                            elif n in {"connected discord", "connected_discord"}:
                                connected = v
                            elif n in {"membership id", "membership_id"}:
                                membership_id = v
                        if did0 and did0 in dids:
                            latest_log[did0] = {
                                "message_id": int(getattr(msg, "id", 0) or 0),
                                "jump_url": str(getattr(msg, "jump_url", "") or ""),
                                "title": str(getattr(e0, "title", "") or ""),
                                "connected_discord": connected,
                                "membership_id": membership_id,
                            }
                            matches += 1
                            if len(latest_log) >= len(dids):
                                # We are scanning newest-first; once we have all, stop.
                                break
                    print(f"messages_scanned: {scanned}")
                    print(f"matched_targets: {len(latest_log)}/{len(dids)} (hits={matches})")
                    with suppress(Exception):
                        await bot.close()

                async with bot:
                    await bot.start(token)

    # Discord-side truth: whop-logs native cards may include contact labels / connected discord info.
    latest_whop_logs: dict[int, dict] = {}
    if scan_whop_logs:
        cfg = load_config()
        token = str(cfg.get("bot_token") or "").strip()
        if not token:
            print("\n=== scan: Discord whop-logs ===")
            print("Missing bot_token in config.secrets.json (cannot scan whop-logs).")
        else:
            try:
                whop_logs_ch_id = int(str(getattr(args, "whop_logs_channel_id", "") or 0).strip())
            except Exception:
                whop_logs_ch_id = 0
            hist_lim = int(getattr(args, "whop_logs_history_limit", 2000) or 2000)
            hist_lim = max(50, min(hist_lim, 20000))
            before_id_raw = str(getattr(args, "whop_logs_before_message_id", "") or "").strip()
            before_obj = discord.Object(id=int(before_id_raw)) if before_id_raw.isdigit() else None
            if not whop_logs_ch_id:
                print("\n=== scan: Discord whop-logs ===")
                print("Missing --whop-logs-channel-id.")
            else:
                intents = discord.Intents.none()
                intents.guilds = True
                intents.messages = True
                intents.message_content = False
                bot = discord.Client(intents=intents)

                def _blob_from_embed(e: discord.Embed) -> str:
                    parts = [str(getattr(e, "title", "") or ""), str(getattr(e, "description", "") or "")]
                    with suppress(Exception):
                        ft = str(getattr(getattr(e, "footer", None), "text", "") or "")
                        if ft:
                            parts.append(ft)
                    with suppress(Exception):
                        for f in (getattr(e, "fields", None) or []):
                            parts.append(str(getattr(f, "name", "") or ""))
                            parts.append(str(getattr(f, "value", "") or ""))
                    return " ".join([p for p in parts if str(p or "").strip()])

                @bot.event
                async def on_ready():
                    print("\n=== scan: Discord whop-logs ===")
                    print(f"channel_id: {whop_logs_ch_id}")
                    print(f"history_limit: {hist_lim}")
                    if before_obj:
                        print(f"before_message_id: {int(before_obj.id)}")
                    ch = bot.get_channel(whop_logs_ch_id)
                    if ch is None:
                        with suppress(Exception):
                            ch = await bot.fetch_channel(whop_logs_ch_id)
                    if not isinstance(ch, discord.TextChannel):
                        print("channel not found or not text.")
                        with suppress(Exception):
                            await bot.close()
                        return

                    scanned = 0
                    hits = 0
                    via_email_hits = 0
                    did_fields_hits = 0
                    regex_hits = 0
                    newest_iso = ""
                    oldest_iso = ""

                    did_strs = {str(d) for d in dids}

                    async for msg in ch.history(limit=hist_lim, before=before_obj):
                        scanned += 1
                        try:
                            ts = (getattr(msg, "created_at", None) or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
                            if not newest_iso:
                                newest_iso = ts
                            oldest_iso = ts
                        except Exception:
                            pass
                        e0 = msg.embeds[0] if msg.embeds else None
                        if not isinstance(e0, discord.Embed):
                            continue
                        blob = _blob_from_embed(e0)
                        # Native helper extractors (email + discord id, when present)
                        email = str(_extract_email_from_native_embed(e0) or "").strip().lower()
                        did_txt = str(_extract_discord_id_from_native_embed(e0) or "").strip()
                        did0 = int(did_txt) if did_txt.isdigit() else 0

                        # Extra fields we want to record (non-PII): Key, Access Pass, Membership Status.
                        key_val = ""
                        access_pass = ""
                        mstatus = ""
                        with suppress(Exception):
                            for f in (getattr(e0, "fields", None) or []):
                                n = str(getattr(f, "name", "") or "").strip().lower()
                                v = str(getattr(f, "value", "") or "").strip()
                                if n == "key":
                                    key_val = v
                                elif n in {"access pass", "access_pass"}:
                                    access_pass = v
                                elif n in {"membership status", "membership_status", "status"}:
                                    mstatus = v

                        matched = False
                        matched_did = 0
                        match_kind = ""

                        if did0 and did0 in dids:
                            matched = True
                            matched_did = int(did0)
                            match_kind = "native_discord_id"
                            did_fields_hits += 1
                        else:
                            # Fallback: look for exact target IDs in the embed text.
                            for s in did_strs:
                                if s and s in blob:
                                    matched = True
                                    matched_did = int(s)
                                    match_kind = "regex_in_embed"
                                    regex_hits += 1
                                    break
                        if (not matched) and email:
                            cached_did = _linked_discord_id_from_identity_cache(email)
                            if cached_did in dids:
                                matched = True
                                matched_did = int(cached_did)
                                match_kind = "identity_cache_email"
                                via_email_hits += 1

                        if not matched or matched_did <= 0:
                            continue

                        # Record newest match for this discord id (we're scanning newest-first).
                        if matched_did not in latest_whop_logs:
                            latest_whop_logs[matched_did] = {
                                "message_id": int(getattr(msg, "id", 0) or 0),
                                "jump_url": str(getattr(msg, "jump_url", "") or ""),
                                "title": str(getattr(e0, "title", "") or ""),
                                "created_at": str(getattr(msg, "created_at", None) or "").strip(),
                                "email": email,
                                "extracted_discord_id": str(did_txt or ""),
                                "match_kind": match_kind,
                                "key": key_val,
                                "access_pass": access_pass,
                                "membership_status": mstatus,
                            }
                            hits += 1
                            if len(latest_whop_logs) >= len(dids):
                                break

                    print(f"messages_scanned: {scanned}")
                    if newest_iso and oldest_iso:
                        print(f"scan_window_newest_utc: {newest_iso}")
                        print(f"scan_window_oldest_utc: {oldest_iso}")
                    print(f"matched_targets: {len(latest_whop_logs)}/{len(dids)} (hits={hits})")
                    print(f"match_breakdown: native_discord_id={did_fields_hits} regex_in_embed={regex_hits} identity_cache_email={via_email_hits}")
                    with suppress(Exception):
                        await bot.close()

                async with bot:
                    await bot.start(token)

    for did in dids:
        mid = _mid_from_member_history(int(did))
        print("\n---")
        print(f"discord_id: {did}")
        cache_email = _email_from_identity_cache_by_discord_id(int(did))
        print(f"identity_cache.email_for_discord_id: {cache_email or '-'}")
        if scan_discord_logs:
            info = latest_log.get(int(did)) if isinstance(latest_log, dict) else None
            if isinstance(info, dict) and info:
                cd = str(info.get("connected_discord") or "").strip()
                print("discord_logs.match: YES")
                print(f"discord_logs.title: {str(info.get('title') or '')}")
                print(f"discord_logs.jump: {str(info.get('jump_url') or '')}")
                print(f"discord_logs.connected_discord: {cd or '-'}")
                print(f"discord_logs.membership_id: {str(info.get('membership_id') or '-')}")
                m = re.search(r"\b(\d{17,19})\b", cd)
                cd_id = int(m.group(1)) if m else 0
                if cd_id == int(did):
                    print("discord_logs.decision: CONNECTED (Connected Discord matches this discord_id)")
                elif cd_id > 0:
                    print(f"discord_logs.decision: MISMATCH (Connected Discord={cd_id} != discord_id={did})")
                elif cd:
                    print("discord_logs.decision: UNPARSEABLE (Connected Discord present but not an ID)")
                else:
                    print("discord_logs.decision: NOT LINKED (no Connected Discord field/value)")
            else:
                print("discord_logs.match: NO (no recent member-status embed found for this discord_id in scan window)")
        if scan_whop_logs:
            info = latest_whop_logs.get(int(did)) if isinstance(latest_whop_logs, dict) else None
            if isinstance(info, dict) and info:
                print("whop_logs.match: YES")
                print(f"whop_logs.match_kind: {str(info.get('match_kind') or '')}")
                print(f"whop_logs.title: {str(info.get('title') or '')}")
                print(f"whop_logs.jump: {str(info.get('jump_url') or '')}")
                print(f"whop_logs.email: {str(info.get('email') or '-')}")
                edid = str(info.get("extracted_discord_id") or "").strip()
                if edid:
                    print(f"whop_logs.extracted_discord_id: {edid}")
                if str(info.get("key") or "").strip():
                    print(f"whop_logs.key: {str(info.get('key') or '')}")
                if str(info.get("membership_status") or "").strip():
                    print(f"whop_logs.membership_status: {str(info.get('membership_status') or '')}")
                if str(info.get("access_pass") or "").strip():
                    print(f"whop_logs.access_pass: {str(info.get('access_pass') or '')}")
                if record_member_history:
                    ok = _update_member_history_from_whop_log_hit(
                        discord_id=int(did),
                        title=str(info.get("title") or ""),
                        created_at_iso=str(info.get("created_at") or "").strip(),
                        message_id=int(info.get("message_id") or 0),
                        jump_url=str(info.get("jump_url") or ""),
                        whop_key=str(info.get("key") or ""),
                        membership_status=str(info.get("membership_status") or ""),
                        access_pass=str(info.get("access_pass") or ""),
                        source_channel_id=int(getattr(args, "whop_logs_channel_id", 0) or 0),
                    )
                    print(f"member_history.recorded_from_whop_logs: {'YES' if ok else 'NO'}")
            else:
                print("whop_logs.match: NO (no matching whop-logs embed found in scan window)")
        if scan_members and api_ready:
            recm = found_in_members.get(int(did))
            if isinstance(recm, dict) and recm:
                print("whop_members_scan.match: YES (Discord is connected in Whop member record)")
                print(f"whop_member_id: {str(recm.get('id') or recm.get('member_id') or '')}")
                print(f"whop_status: {str(recm.get('status') or '')}")
                print(f"whop_most_recent_action: {str(recm.get('most_recent_action') or '')}")
            else:
                # Only claim "not connected" if the API actually exposes any discord IDs at all in this scan.
                if int(scan_stats.get("discord_ids_found") or 0) > 0:
                    print("whop_members_scan.match: NO (not seen in scanned members with Discord connections)")
                else:
                    print("whop_members_scan.match: INCONCLUSIVE (API scan did not expose any Discord connection fields)")
        print(f"member_history.last_membership_id: {mid or '—'}")
        print(f"member_history.last_membership_id.len: {len(mid)}")
        print(f"member_history.last_membership_id.repr: {repr(mid)}")
        if not mid:
            print("decision: OPEN no_whop_link (discord-only fallback) because no membership_id recorded yet.")
            # Optional: if we can resolve email via identity cache, attempt to locate the Whop user and derive membership_id.
            if cache_email and api_ready and client:
                # Scan /members for this email to get Whop user_id, then fetch memberships.
                print("attempt: resolve membership via identity_cache email -> /members -> /memberships")
                user_id = ""
                member_id = ""
                after: str | None = None
                pages = 0
                scanned = 0
                while pages < max_pages:
                    batch, page_info = await client.list_members(first=per_page, after=after, params={"order": "joined_at", "direction": "desc"})
                    pages += 1
                    if not batch:
                        break
                    for rec in batch:
                        if not isinstance(rec, dict):
                            continue
                        scanned += 1
                        u = rec.get("user") if isinstance(rec.get("user"), dict) else {}
                        em = str(u.get("email") or "").strip().lower() if isinstance(u, dict) else ""
                        if em and em == cache_email:
                            user_id = str(u.get("id") or u.get("user_id") or "").strip()
                            member_id = str(rec.get("id") or rec.get("member_id") or "").strip()
                            break
                    after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
                    has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
                    if user_id or (not has_next) or (not after):
                        break
                print(f"scan_email.pages_scanned: {pages}")
                print(f"scan_email.members_scanned: {scanned}")
                print(f"scan_email.whop_user_id: {user_id or '—'}")
                print(f"scan_email.whop_member_id: {member_id or '—'}")
                if user_id:
                    ms = []
                    with suppress(Exception):
                        ms = await client.get_user_memberships(user_id)
                    ms = ms if isinstance(ms, list) else []
                    mids = [str(m.get("id") or m.get("membership_id") or "").strip() for m in ms if isinstance(m, dict)]
                    mids = [m for m in mids if m]
                    print(f"user_memberships.count: {len(ms)}")
                    print(f"user_memberships.membership_ids.sample: {mids[:5]}")
                    if mids:
                        # Fetch the newest-looking membership and check connected Discord via member record.
                        m0 = str(mids[0])
                        brief = {}
                        with suppress(Exception):
                            brief = await _whop_brief_api_only(client, m0)
                        conn = str((brief or {}).get("connected_discord") or "").strip() if isinstance(brief, dict) else ""
                        print(f"api.membership_id_used: {m0}")
                        print(f"api.connected_discord: {conn or '—'}")
            continue
        if not api_ready or not client:
            print("decision: membership_id exists, but cannot probe Whop API locally (missing credentials).")
            continue
        brief = {}
        with suppress(Exception):
            brief = await _whop_brief_api_only(client, str(mid))
        if not isinstance(brief, dict) or not brief:
            print("api: failed to fetch membership (brief empty)")
            print("decision: cannot confirm linkage via API (would skip/avoid low-quality ticket in production scan).")
            continue
        conn = str(brief.get("connected_discord") or "").strip()
        email = str(brief.get("email") or "").strip()
        cached_did = _linked_discord_id_from_identity_cache(email) if email else 0
        print(f"api.status: {str(brief.get('status') or '')}")
        print(f"api.product: {str(brief.get('product') or '')}")
        print(f"api.connected_discord: {conn or '—'}")
        if email:
            print(f"api.email: {email}")
            print(f"identity_cache.discord_id_for_email: {cached_did or '—'}")
        # Determine "linked" vs "not linked"
        m = re.search(r"\b(\d{17,19})\b", conn)
        conn_id = int(m.group(1)) if m else 0
        if conn_id == int(did):
            print("decision: LINKED (connected_discord matches this discord_id) -> no ticket.")
        elif conn_id > 0 and conn_id != int(did):
            print(f"decision: MISMATCH (connected_discord={conn_id} != discord_id={did}) -> OPEN no_whop_link.")
        else:
            print("decision: NOT LINKED (no connected_discord in API) -> OPEN no_whop_link.")

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

    pnow = sub.add_parser("nowhop-debug", help="Debug no_whop_link decision for specific Discord IDs (local + optional API).")
    pnow.add_argument("--discord-id", action="append", default=[], help="Discord user id (repeatable).")
    pnow.add_argument("--scan-members", action="store_true", default=False, help="Scan Whop /members pages and try to match connected Discord IDs.")
    pnow.add_argument("--members-max-pages", type=int, default=10, help="Max pages to scan from /members (each page is members-per-page).")
    pnow.add_argument("--members-per-page", type=int, default=100, help="Page size for /members (10-200).")
    pnow.add_argument("--scan-discord-logs", action="store_true", default=False, help="Scan Discord member-status-logs for Connected Discord field (definitive).")
    pnow.add_argument("--guild-id", default="", help="Discord guild id for scan-discord-logs (defaults to support_tickets.guild_id).")
    pnow.add_argument("--channel-id", default="", help="Discord channel id for scan-discord-logs (defaults to dm_sequence.member_status_logs_channel_id).")
    pnow.add_argument("--history-limit", type=int, default=800, help="How many recent messages to scan in member-status-logs (50-5000).")
    pnow.add_argument("--scan-whop-logs", action="store_true", default=False, help="Scan Discord whop-logs channel for Discord linkage (native cards).")
    pnow.add_argument("--whop-logs-channel-id", dest="whop_logs_channel_id", default="", help="Discord channel id for whop-logs scan.")
    pnow.add_argument("--whop-logs-history-limit", type=int, default=2000, help="How many recent messages to scan in whop-logs (50-20000).")
    pnow.add_argument("--whop-logs-before-message-id", default="", help="Scan whop-logs messages before this message id (snowflake) to reach older history.")
    pnow.add_argument("--record-member-history", action="store_true", default=False, help="Record latest per-title whop-logs hits into member_history.json (no PII).")

    pbl = sub.add_parser("whoplogs-baseline", help="Scan a whop-logs channel and record a per-user baseline into member_history.json.")
    pbl.add_argument("--channel-id", required=True, help="Discord whop-logs channel id to scan.")
    pbl.add_argument("--limit", type=int, default=5000, help="How many messages to scan this run (50-20000).")
    pbl.add_argument("--before-message-id", default="", help="Scan messages before this message id (to go further back).")
    pbl.add_argument("--resume", action="store_true", default=False, help="Resume using saved cursor from state file.")
    pbl.add_argument("--state-file", default="", help="State file path (default: RSCheckerbot/.probe_whoplogs_baseline_state.json).")
    pbl.add_argument("--record-member-history", action="store_true", default=False, help="Actually write updates into member_history.json (no PII).")
    pbl.add_argument("--confirm", default="", help="Must be exactly 'confirm' when using --record-member-history.")
    pbl.add_argument("--progress-every", type=int, default=200, help="Update the live progress bar every N messages (0 disables).")
    pbl.add_argument("--bar-width", type=int, default=24, help="Width of the live progress bar (10-60).")
    pbl.add_argument("--run-until-done", action="store_true", default=False, help="Keep scanning older history in chunks until the channel history is exhausted.")
    pbl.add_argument("--batch-delay-seconds", type=float, default=1.0, help="Delay between chunks when --run-until-done is enabled (0-10).")
    pbl.add_argument("--max-batches", type=int, default=0, help="Optional safety cap on number of chunks (0 = unlimited).")
    pbl.add_argument("--interactive", action="store_true", default=False, help="Prompt to resume and/or continue between chunks (safe for flaky connections).")
    pbl.add_argument("--checkpoint-every", type=int, default=0, help="Write resume cursor every N messages within a chunk (0 disables).")

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
    if args.mode == "nowhop-debug":
        return asyncio.run(_probe_nowhop_debug(args))
    if args.mode == "whoplogs-baseline":
        return asyncio.run(_probe_whoplogs_baseline(args))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

