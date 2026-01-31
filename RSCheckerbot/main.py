import os
import sys
import json
import asyncio
import re
import csv
import io
import time
import hashlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import logging
from typing import Dict, Optional
from contextlib import suppress
from collections import deque

# Logger must exist early (used by early config/webhook wiring).
log = logging.getLogger("rs-checker")
try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]
try:
    import msvcrt  # type: ignore
except Exception:  # pragma: no cover
    msvcrt = None  # type: ignore[assignment]
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Always resolve bot-local files relative to this directory (do not depend on cwd).
BASE_DIR = Path(__file__).resolve().parent

# Startup sequencing: ensure heavy scans finish before sync begins.
_STARTUP_SCANS_DONE: asyncio.Event = asyncio.Event()

# Email -> Discord ID cache (populated by parsing native whop-logs cards).
WHOP_IDENTITY_CACHE_FILE = BASE_DIR / "whop_identity_cache.json"

# -----------------------------
# Progress formatting (Discord + terminal)
# -----------------------------
def _progress_bar(done: int, total: int, *, width: int) -> str:
    try:
        total_i = int(total)
    except Exception:
        total_i = 0
    try:
        done_i = int(done)
    except Exception:
        done_i = 0
    try:
        w = int(width)
    except Exception:
        w = 26
    w = max(10, min(w, 60))
    if total_i <= 0:
        filled = 0
        pct = 0
    else:
        pct = int(round((done_i / max(1, total_i)) * 100))
        filled = int(round((done_i / max(1, total_i)) * w))
    filled = max(0, min(filled, w))
    return "[" + ("—" * filled) + (" " * (w - filled)) + f"] {pct}% ({done_i}/{total_i})"


def _progress_text(*, label: str, step: tuple[int, int], done: int, total: int, stats: dict[str, object], stage: str) -> str:
    try:
        width = int(LOG_CONTROLS.get("progress_bar_width", 26))
    except Exception:
        width = 26
    s1 = f"Fetchall: {str(label or '').strip()} ({int(step[0])}/{int(step[1])})"
    s2 = _progress_bar(int(done), int(total), width=width)
    parts: list[str] = []
    for k, v in (stats or {}).items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    parts.append(f"stage={stage}")
    s3 = " ".join(parts)[:1900]
    return f"{s1}\n{s2}\n{s3}"

# -----------------------------
# Single-instance guard (prevents duplicate events when multiple processes start)
# -----------------------------
_INSTANCE_LOCK_FH: io.TextIOWrapper | None = None


def _acquire_single_instance_lock() -> bool:
    """Best-effort cross-platform single-instance lock using a lock file."""
    global _INSTANCE_LOCK_FH
    lock_path = BASE_DIR / ".rscheckerbot.lock"
    fh: io.TextIOWrapper | None = None
    existing = ""
    try:
        fh = open(lock_path, "a+", encoding="utf-8")
        with suppress(Exception):
            fh.seek(0)
            existing = (fh.read() or "").strip()
            fh.seek(0)
    except Exception:
        fh = None

    if not fh:
        return True  # cannot lock -> do not block startup

    try:
        if msvcrt is not None:
            # Lock 1 byte at start of file (Windows).
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        elif fcntl is not None:
            # Unix advisory lock.
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[arg-type]
        else:
            # Unknown platform; skip lock.
            _INSTANCE_LOCK_FH = fh
            return True
    except Exception:
        with suppress(Exception):
            fh.close()
        msg = (existing[:200] + ("…" if len(existing) > 200 else "")) if existing else "—"
        try:
            log.error(f"[Boot] Another RSCheckerbot instance is already running (lock busy). lock_file={lock_path} existing={msg}")
        except Exception:
            pass
        return False

    # Write pid metadata (best-effort) and keep handle open for duration of process.
    with suppress(Exception):
        fh.seek(0)
        fh.truncate(0)
        fh.write(json.dumps({"pid": os.getpid(), "started_at": datetime.now(timezone.utc).isoformat()}))
        fh.flush()
    _INSTANCE_LOCK_FH = fh
    return True

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

import discord
from discord.ext import commands, tasks
from aiohttp import web
import aiohttp

from rschecker_utils import (
    load_json,
    save_json,
    append_jsonl,
    iter_jsonl,
    roles_plain,
    access_roles_plain,
    coerce_role_ids,
    usd_amount,
    extract_discord_id_from_whop_member_record,
    fmt_date_any as _fmt_date_any,
    parse_dt_any as _parse_dt_any,
)
from staff_embeds import (
    apply_member_header as _apply_member_header,
    build_case_minimal_embed as _build_case_minimal_embed,
    build_member_status_detailed_embed as _build_member_status_detailed_embed,
)
from whop_brief import fetch_whop_brief
from whop_native_membership_cache import get_summary as _get_native_summary_by_mid
from staff_channels import (
    PAYMENT_FAILURE_CHANNEL_NAME,
    MEMBER_CANCELLATION_CHANNEL_NAME,
    STAFF_ALERTS_CATEGORY_ID,
)
from staff_alerts_store import (
    load_staff_alerts,
    save_staff_alerts,
    should_post_alert,
    record_alert_post,
    should_post_and_record_alert,
)

# Shared channel helpers (canonical for ticket-like channels)
from ticket_channels import slug_channel_name as _slug_channel_name
from ticket_channels import ensure_ticket_like_channel as _ensure_whop_case_channel

# Support CRM tickets (Neo)
import support_tickets

# Import Whop webhook handler
from whop_webhook_handler import (
    initialize as init_whop_handler,
    handle_whop_webhook_message,
    extract_native_whop_card_debug,
    resolve_discord_id_from_whop_logs as _resolve_discord_id_from_whop_logs,
)

# Import Whop API client
from whop_api_client import WhopAPIClient, WhopAPIError
from shared.whop_webhook_utils import verify_standard_webhook

# Reporting store (runtime JSON; persisted only for member-status-logs output)
try:
    from reporting_store import (
        load_store as _report_load_store,
        save_store as _report_save_store,
        prune_store as _report_prune_store,
        record_member_status_post as _report_record_member_status_post,
        week_keys_between as _report_week_keys_between,
        summarize_counts as _report_summarize_counts,
    )
except Exception:
    _report_load_store = None
    _report_save_store = None
    _report_prune_store = None
    _report_record_member_status_post = None
    _report_week_keys_between = None
    _report_summarize_counts = None

# -----------------------------
# RSCheckerbot Rules
# -----------------------------
# - No role mentions ever (<@&...> is forbidden)
# - Only mention users (@user) and channels (#channel)
# - Commands must only trigger with .checker or bot mention
# - DM sequence must be toggleable via commands
# -----------------------------

# -----------------------------
# Load Configuration
# -----------------------------
def load_config():
    config, _, secrets_path = load_config_with_secrets(BASE_DIR)
    if not secrets_path.exists():
        raise RuntimeError(f"Missing server-only secrets file: {secrets_path}")
    if not config.get("bot_token"):
        raise RuntimeError(f"bot_token must be set in {secrets_path}")
    return config

config = load_config()

# -----------------------------
# Reporting config (validated early; must not crash bot)
# -----------------------------
def _load_reporting_config(cfg: dict) -> dict:
    """Parse reporting config with safe defaults (never raises)."""
    base = cfg.get("reporting") if isinstance(cfg, dict) else None
    base = base if isinstance(base, dict) else {}

    def _as_bool(v: object) -> bool:
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        return s in {"1", "true", "yes", "y", "on"}

    def _as_int(v: object) -> int | None:
        try:
            s = str(v).strip()
            if not s:
                return None
            return int(s)
        except Exception:
            return None

    def _as_str(v: object) -> str:
        return str(v or "").strip()

    enabled = _as_bool(base.get("enabled", False))
    dm_user_id = _as_int(base.get("dm_user_id"))
    tz = _as_str(base.get("timezone") or "America/New_York") or "America/New_York"
    report_time = _as_str(base.get("report_time_local") or "09:00") or "09:00"
    weekly_day = _as_str(base.get("weekly_day_local") or "mon").lower() or "mon"
    retention_weeks = _as_int(base.get("retention_weeks")) or 26
    reminder_days = base.get("reminder_days_before_cancel")
    scan_log_channel_id = _as_int(base.get("scan_log_channel_id"))
    scan_log_webhook_url = _as_str(base.get("scan_log_webhook_url"))
    scan_log_each_member = _as_bool(base.get("scan_log_each_member", False))
    scan_log_include_raw_dates = _as_bool(base.get("scan_log_include_raw_dates", False))
    scan_log_max_members = _as_int(base.get("scan_log_max_members")) or 0
    scan_log_progress_every = _as_int(base.get("scan_log_progress_every")) or 50

    # Optional mirrors / startup snapshots (must never crash).
    cancel_out_gid = _as_int(base.get("cancel_reminders_output_guild_id")) or 0
    cancel_out_name = _as_str(base.get("cancel_reminders_output_channel_name"))
    startup_canceling_enabled = _as_bool(base.get("startup_canceling_snapshot_enabled", False))
    startup_canceling_max_pages = _as_int(base.get("startup_canceling_snapshot_max_pages")) or 0
    startup_canceling_per_page = _as_int(base.get("startup_canceling_snapshot_per_page")) or 0
    startup_canceling_max_rows = _as_int(base.get("startup_canceling_snapshot_max_rows")) or 0
    try:
        startup_canceling_min_spent = float(base.get("startup_canceling_snapshot_min_total_spent_usd") or 0.0)
    except Exception:
        startup_canceling_min_spent = 0.0

    # Startup canceling snapshot diagnostics/output controls (optional).
    startup_canceling_log_each = _as_bool(base.get("startup_canceling_snapshot_log_each_member", False))
    startup_canceling_one_per_member = _as_bool(base.get("startup_canceling_snapshot_one_message_per_member", False))
    startup_canceling_clear = _as_bool(base.get("startup_canceling_snapshot_clear_channel", False))
    startup_canceling_clear_limit = _as_int(base.get("startup_canceling_snapshot_clear_limit")) or 0
    startup_canceling_clear_limit = max(0, min(int(startup_canceling_clear_limit or 0), 500))
    startup_canceling_fill_total = _as_bool(base.get("startup_canceling_snapshot_fill_total_spend_via_payments", False))
    startup_canceling_attach_csv = _as_bool(base.get("startup_canceling_snapshot_attach_csv", False))
    startup_canceling_pay_sleep_ms = _as_int(base.get("startup_canceling_snapshot_payments_sleep_ms")) or 0
    startup_canceling_pay_sleep_ms = max(0, min(int(startup_canceling_pay_sleep_ms or 0), 2000))

    # Optional filters (reduce noisy/incorrect canceling rows)
    startup_canceling_skip_remaining_gt = _as_int(base.get("startup_canceling_snapshot_skip_if_remaining_days_gt")) or 0
    startup_canceling_skip_remaining_gt = max(0, min(int(startup_canceling_skip_remaining_gt or 0), 3650))
    skip_keywords_raw = base.get("startup_canceling_snapshot_skip_payment_status_keywords")
    if not isinstance(skip_keywords_raw, list):
        skip_keywords_raw = []
    startup_canceling_skip_payment_keywords = [
        str(x or "").strip().lower()
        for x in skip_keywords_raw
        if str(x or "").strip()
    ]
    startup_canceling_skip_payment_keywords = sorted(set(startup_canceling_skip_payment_keywords))
    startup_canceling_payment_check_limit = _as_int(base.get("startup_canceling_snapshot_payment_check_limit")) or 0
    startup_canceling_payment_check_limit = max(0, min(int(startup_canceling_payment_check_limit or 0), 200))

    exclude_prod_raw = base.get("startup_canceling_snapshot_exclude_product_title_keywords")
    if not isinstance(exclude_prod_raw, list):
        exclude_prod_raw = []
    startup_canceling_exclude_product_keywords = [
        str(x or "").strip().lower()
        for x in exclude_prod_raw
        if str(x or "").strip()
    ]
    startup_canceling_exclude_product_keywords = sorted(set(startup_canceling_exclude_product_keywords))

    # Optional mirror targets for canceling snapshot (by channel id).
    mirror_ids_raw = base.get("startup_canceling_snapshot_mirror_channel_ids")
    if not isinstance(mirror_ids_raw, list):
        mirror_ids_raw = []
    mirror_ids: list[int] = []
    for x in mirror_ids_raw:
        xi = _as_int(x)
        if isinstance(xi, int) and xi > 0:
            mirror_ids.append(int(xi))
    mirror_ids = list(dict.fromkeys(mirror_ids))  # preserve order, unique

    mirror_clear = _as_bool(base.get("startup_canceling_snapshot_mirror_clear_channel", False))
    mirror_clear_limit = _as_int(base.get("startup_canceling_snapshot_mirror_clear_limit")) or 0
    mirror_clear_limit = max(0, min(int(mirror_clear_limit or 0), 500))

    if not isinstance(reminder_days, list):
        reminder_days = [7, 3, 1]
    cleaned_days: list[int] = []
    for x in reminder_days:
        xi = _as_int(x)
        if isinstance(xi, int) and 0 <= xi <= 365:
            cleaned_days.append(int(xi))
    if not cleaned_days:
        cleaned_days = [7, 3, 1]
    cleaned_days = sorted(set(cleaned_days), reverse=True)

    # Basic validation; on invalid config, disable reporting but keep bot running.
    if enabled and (dm_user_id is None or dm_user_id <= 0):
        print("[Config] reporting.enabled=true but reporting.dm_user_id is missing/invalid; disabling reporting.")
        enabled = False
    if retention_weeks < 4 or retention_weeks > 260:
        print("[Config] reporting.retention_weeks out of range (4..260); using 26.")
        retention_weeks = 26
    if weekly_day not in {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}:
        print("[Config] reporting.weekly_day_local invalid; using 'mon'.")
        weekly_day = "mon"
    if scan_log_progress_every <= 0:
        scan_log_progress_every = 50

    return {
        "enabled": bool(enabled),
        "dm_user_id": dm_user_id or 0,
        "timezone": tz,
        "report_time_local": report_time,
        "weekly_day_local": weekly_day,
        "retention_weeks": int(retention_weeks),
        "reminder_days_before_cancel": cleaned_days,
        "cancel_reminders_output_guild_id": int(cancel_out_gid or 0),
        "cancel_reminders_output_channel_name": cancel_out_name,
        "startup_canceling_snapshot_enabled": bool(startup_canceling_enabled),
        "startup_canceling_snapshot_max_pages": max(0, int(startup_canceling_max_pages or 0)),
        "startup_canceling_snapshot_per_page": max(0, min(200, int(startup_canceling_per_page or 0))),
        "startup_canceling_snapshot_max_rows": max(0, min(200, int(startup_canceling_max_rows or 0))),
        "startup_canceling_snapshot_min_total_spent_usd": float(max(0.0, startup_canceling_min_spent)),
        "startup_canceling_snapshot_log_each_member": bool(startup_canceling_log_each),
        "startup_canceling_snapshot_one_message_per_member": bool(startup_canceling_one_per_member),
        "startup_canceling_snapshot_clear_channel": bool(startup_canceling_clear),
        "startup_canceling_snapshot_clear_limit": int(startup_canceling_clear_limit),
        "startup_canceling_snapshot_fill_total_spend_via_payments": bool(startup_canceling_fill_total),
        "startup_canceling_snapshot_payments_sleep_ms": int(startup_canceling_pay_sleep_ms),
        "startup_canceling_snapshot_attach_csv": bool(startup_canceling_attach_csv),
        "startup_canceling_snapshot_skip_if_remaining_days_gt": int(startup_canceling_skip_remaining_gt),
        "startup_canceling_snapshot_skip_payment_status_keywords": startup_canceling_skip_payment_keywords,
        "startup_canceling_snapshot_payment_check_limit": int(startup_canceling_payment_check_limit),
        "startup_canceling_snapshot_exclude_product_title_keywords": startup_canceling_exclude_product_keywords,
        "startup_canceling_snapshot_mirror_channel_ids": mirror_ids,
        "startup_canceling_snapshot_mirror_clear_channel": bool(mirror_clear),
        "startup_canceling_snapshot_mirror_clear_limit": int(mirror_clear_limit),
        "scan_log_channel_id": scan_log_channel_id or 0,
        "scan_log_webhook_url": scan_log_webhook_url,
        "scan_log_each_member": bool(scan_log_each_member),
        "scan_log_include_raw_dates": bool(scan_log_include_raw_dates),
        "scan_log_max_members": int(scan_log_max_members),
        "scan_log_progress_every": int(scan_log_progress_every),
    }

REPORTING_CONFIG = _load_reporting_config(config)
_REPORTING_STORE: dict | None = None
_REPORTING_STORE_LOCK: asyncio.Lock = asyncio.Lock()
_SCAN_LOG_WEBHOOK_SESSION: aiohttp.ClientSession | None = None


def _tz_now() -> datetime:
    tz_name = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
    if ZoneInfo is None:
        return datetime.now(timezone.utc)
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


async def _get_scan_log_webhook_session() -> aiohttp.ClientSession:
    global _SCAN_LOG_WEBHOOK_SESSION
    if _SCAN_LOG_WEBHOOK_SESSION and not _SCAN_LOG_WEBHOOK_SESSION.closed:
        return _SCAN_LOG_WEBHOOK_SESSION
    timeout = aiohttp.ClientTimeout(total=10)
    _SCAN_LOG_WEBHOOK_SESSION = aiohttp.ClientSession(timeout=timeout)
    return _SCAN_LOG_WEBHOOK_SESSION


async def _post_scan_log_webhook(text: str) -> None:
    url = str(REPORTING_CONFIG.get("scan_log_webhook_url") or "").strip()
    msg = str(text or "").strip()
    if not url or not msg:
        return
    payload = {"content": msg[:1900], "allowed_mentions": {"parse": []}}
    try:
        session = await _get_scan_log_webhook_session()
        async with session.post(url, json=payload) as resp:
            if not (200 <= resp.status < 300):
                log.warning("[ReportScan] scan log webhook post failed (status=%s)", resp.status)
    except Exception as e:
        log.warning("[ReportScan] scan log webhook post failed: %s", e)


async def _report_scan_log_message(text: str) -> None:
    """Optional scan log output to a configured channel or webhook."""
    msg = str(text or "").strip()
    if not msg:
        return
    log.info("[ReportScan] %s", msg)
    try:
        ch_id = int(REPORTING_CONFIG.get("scan_log_channel_id") or 0)
    except Exception:
        ch_id = 0
    if ch_id:
        ch = bot.get_channel(ch_id)
        if isinstance(ch, discord.TextChannel):
            with suppress(Exception):
                await ch.send(msg[:1900], allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))
    if REPORTING_CONFIG.get("scan_log_webhook_url"):
        asyncio.create_task(_post_scan_log_webhook(msg))


async def _backfill_recent_native_whop_cards() -> None:
    """Populate member_history from recent native Whop log cards (no staff spam).

    This lets Discord-only cards (join/leave) show Total Spent / Dashboard / Status / Renewal Window
    once we've seen any native Whop card for that Discord ID.
    """
    try:
        limit = int(WHOP_NATIVE_BACKFILL_LIMIT or 0)
    except Exception:
        limit = 0
    if limit <= 0:
        return

    try:
        max_days = int(WHOP_NATIVE_BACKFILL_MAX_DAYS or 0)
    except Exception:
        max_days = 0

    now = datetime.now(timezone.utc)
    seen_ids: set[int] = set()

    channel_ids: list[int] = []
    if str(WHOP_WEBHOOK_CHANNEL_ID or "").strip().isdigit():
        channel_ids.append(int(WHOP_WEBHOOK_CHANNEL_ID))
    if str(WHOP_LOGS_CHANNEL_ID or "").strip().isdigit():
        channel_ids.append(int(WHOP_LOGS_CHANNEL_ID))
    if not channel_ids:
        return

    for cid in channel_ids:
        ch = bot.get_channel(int(cid))
        if not isinstance(ch, discord.TextChannel):
            continue

        processed = 0
        async for m in ch.history(limit=limit):
            if max_days and getattr(m, "created_at", None):
                try:
                    if (now - m.created_at).days > int(max_days):
                        break
                except Exception:
                    pass

            did: int | None = None
            try:
                if m.embeds:
                    emb = m.embeds[0]
                    for f in (emb.fields or []):
                        if str(getattr(f, "name", "") or "").strip().lower() == "discord id":
                            mm = re.search(r"(\d{17,19})", str(getattr(f, "value", "") or ""))
                            if mm:
                                did = int(mm.group(1))
                            break
            except Exception:
                did = None

            if did and did in seen_ids:
                continue

            with suppress(Exception):
                await handle_whop_webhook_message(m, backfill_only=True)

            processed += 1
            if did:
                seen_ids.add(int(did))

            # Yield occasionally so startup remains responsive.
            if processed and (processed % 200 == 0):
                await asyncio.sleep(0)


async def _startup_native_whop_smoketest() -> None:
    """On startup, replay the last N native Whop log cards into the Neo test server.

    This uses the same staff embed builder used in `member-status-logs` so you can verify
    Total Spent / Whop Dashboard parsing without changing the live channel history.
    """
    if not WHOP_STARTUP_NATIVE_SMOKETEST_ENABLED:
        return

    # Default to the configured webhook channel; also use the configured logs channel as a secondary
    # source (it often includes Discord ID/username when the webhook channel does not).
    primary_id = int(WHOP_STARTUP_NATIVE_SMOKETEST_SOURCE_CHANNEL_ID or (WHOP_WEBHOOK_CHANNEL_ID or 0) or 0)
    secondary_id = int(WHOP_LOGS_CHANNEL_ID or 0)
    if not primary_id:
        log.info("[BOOT][WhopSmoke] disabled (missing primary source channel id)")
        return
    log.info(
        "[BOOT][WhopSmoke] enabled count=%s source_channel_id=%s output_guild_id=%s output_channel_id=%s output_channel_name=%s",
        int(WHOP_STARTUP_NATIVE_SMOKETEST_COUNT or 3),
        primary_id,
        int(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_GUILD_ID or 0),
        int(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_ID or 0),
        str(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_NAME or "").strip(),
    )

    async def _get_text_channel(cid: int) -> discord.TextChannel | None:
        ch = bot.get_channel(int(cid))
        if isinstance(ch, discord.TextChannel):
            return ch
        with suppress(Exception):
            fetched = await bot.fetch_channel(int(cid))
            return fetched if isinstance(fetched, discord.TextChannel) else None
        return None

    async def _pick_writable_text_channel(guild: discord.Guild) -> discord.TextChannel | None:
        """Best-effort writable channel selection (no pings)."""
        if not guild:
            return None
        me = guild.me or guild.get_member(int(getattr(bot.user, "id", 0) or 0))
        # Prefer system channel if writable.
        sys_ch = getattr(guild, "system_channel", None)
        if isinstance(sys_ch, discord.TextChannel) and me and sys_ch.permissions_for(me).send_messages:
            return sys_ch
        # Else pick first writable text channel.
        for ch in (guild.text_channels or []):
            if not isinstance(ch, discord.TextChannel):
                continue
            if me and ch.permissions_for(me).send_messages:
                return ch
        return None

    async def _resolve_output_channel() -> discord.TextChannel | None:
        # 1) Explicit channel id (works across guilds).
        if int(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_ID or 0):
            return await _get_text_channel(int(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_ID))

        # 2) Target guild + channel name (Neo test).
        gid = int(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_GUILD_ID or 0)
        g = bot.get_guild(gid) if gid else None
        if gid and not g:
            log.warning("[BOOT][WhopSmoke] output guild not found in cache (bot may not be in this server): %s", gid)
        if g:
            name = str(WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_NAME or "").strip().lower()
            if name:
                for ch in (g.text_channels or []):
                    if isinstance(ch, discord.TextChannel) and str(ch.name).lower() == name:
                        return ch
                # Create channel if allowed.
                me = g.me or g.get_member(int(getattr(bot.user, "id", 0) or 0))
                if me and getattr(me.guild_permissions, "manage_channels", False):
                    with suppress(Exception):
                        created = await g.create_text_channel(name=name, reason="RSCheckerbot: startup Whop smoke test output")
                        if isinstance(created, discord.TextChannel):
                            return created
            # Fallback: any writable channel in the target guild.
            ch_any = await _pick_writable_text_channel(g)
            if ch_any:
                return ch_any

        # 3) Final fallback: current guild's member status logs.
        if int(MEMBER_STATUS_LOGS_CHANNEL_ID or 0):
            return await _get_text_channel(int(MEMBER_STATUS_LOGS_CHANNEL_ID))
        return None

    src = await _get_text_channel(primary_id)
    out = await _resolve_output_channel()
    if not src or not out:
        log.warning("[BOOT][WhopSmoke] disabled (channel not found) src=%s out=%s", primary_id, int(getattr(out, "id", 0) or 0))
        return

    # Secondary source (best-effort)
    src2: discord.TextChannel | None = None
    if secondary_id and int(secondary_id) != int(primary_id):
        src2 = await _get_text_channel(int(secondary_id))
        if not src2:
            log.warning("[BOOT][WhopSmoke] secondary channel not found: %s", secondary_id)

    count = int(WHOP_STARTUP_NATIVE_SMOKETEST_COUNT or 3)
    main_guild = bot.get_guild(GUILD_ID) if GUILD_ID else None
    out_guild = getattr(out, "guild", None)
    if isinstance(out_guild, discord.Guild):
        # Ensure case channels exist in the Neo test server (best-effort).
        with suppress(Exception):
            await _ensure_alert_channels(out_guild)
    out_payment_ch = _find_text_channel_by_name(out_guild, PAYMENT_FAILURE_CHANNEL_NAME) if isinstance(out_guild, discord.Guild) else None
    out_cancel_ch = _find_text_channel_by_name(out_guild, MEMBER_CANCELLATION_CHANNEL_NAME) if isinstance(out_guild, discord.Guild) else None

    async def _resolve_member(guild: discord.Guild | None, user_id: int) -> discord.Member | None:
        if not guild or not user_id:
            return None
        m = guild.get_member(int(user_id))
        if m:
            return m
        with suppress(Exception):
            return await guild.fetch_member(int(user_id))
        return None

    def _blank(v: object) -> bool:
        s = str(v or "").strip()
        return (not s) or s == "—"

    def _merge_briefs(a: dict | None, b: dict | None) -> dict:
        aa = a if isinstance(a, dict) else {}
        bb = b if isinstance(b, dict) else {}
        out: dict = {}
        for k in set(list(aa.keys()) + list(bb.keys())):
            va = aa.get(k)
            vb = bb.get(k)
            out[k] = va if not _blank(va) else vb
        return out

    def _title_key(t: str) -> str:
        return re.sub(r"\\s+", " ", str(t or "").strip().lower())

    def _norm_email(s: str) -> str:
        return str(s or "").strip().lower()

    def _event_guess(title: str) -> str:
        t = str(title or "").strip().lower()
        if "cancellation scheduled" in t:
            return "cancellation_scheduled"
        if "set to cancel" in t:
            return "cancellation_scheduled"
        if ("cancel" in t and "scheduled" in t) or ("canceling" in t and "cancels" in t):
            return "cancellation_scheduled"
        if "payment failed" in t:
            return "payment.failed"
        if "billing issue" in t or "access risk" in t:
            return "payment.failed"
        if "payment succeeded" in t or "payment received" in t:
            return "payment.succeeded"
        if "activated" in t and "pending" in t:
            return "membership.activated.pending"
        if "activated" in t:
            return "membership.activated"
        if "member joined" in t:
            return "member.joined"
        if "member left" in t:
            return "member.left"
        return "whop.native"

    # Preload a small window from the secondary channel so we can pair events.
    secondary_pool: list[dict] = []
    if src2:
        limit = max(10, min(80, count * 20))
        async for m2 in src2.history(limit=limit):
            info2 = extract_native_whop_card_debug(m2) or {}
            secondary_pool.append(
                {
                    "msg": m2,
                    "info": info2,
                    "title": _title_key(str(info2.get("title") or "")),
                    "email": _norm_email(str(info2.get("email") or "")),
                    "membership_id": str(info2.get("membership_id") or "").strip(),
                    "discord_id": str(info2.get("discord_id") or "").strip(),
                    "created_at": getattr(m2, "created_at", None),
                }
            )

    def _best_pair(msg: discord.Message, info: dict) -> dict | None:
        if not secondary_pool:
            return None
        t1 = _title_key(str(info.get("title") or ""))
        email1 = _norm_email(str(info.get("email") or ""))
        mid1 = str(info.get("membership_id") or "").strip()
        ts1 = getattr(msg, "created_at", None)
        best = None
        best_score = None
        for cand in secondary_pool:
            # Prefer hard keys: membership_id match, then email match.
            mid2 = str(cand.get("membership_id") or "").strip()
            email2 = str(cand.get("email") or "").strip()
            hard = 0
            if mid1 and mid2 and mid1 == mid2:
                hard = 2
            elif email1 and email2 and email1 == email2:
                hard = 1
            else:
                # Fallback: weak title similarity.
                t2 = str(cand.get("title") or "")
                if t1 and t2 and not (t1 == t2 or t1 in t2 or t2 in t1):
                    continue
            dt = None
            try:
                if ts1 and cand.get("created_at"):
                    dt = abs((ts1 - cand["created_at"]).total_seconds())
            except Exception:
                dt = None
            # Allow a wider window here because the two sources can lag slightly.
            if dt is None or dt > 900:
                continue
            # Score: prefer hard match, then closer timestamp.
            score = (0 if hard == 2 else (10 if hard == 1 else 30)) + float(dt or 0.0) / 60.0
            if best_score is None or score < best_score:
                best = cand
                best_score = score
        return best

    # Build embeds using the same builder as member-status-logs.
    embeds: list[discord.Embed] = []
    found = 0
    async for m in src.history(limit=count):
        found += 1
        info = extract_native_whop_card_debug(m) or {}
        summary1 = info.get("summary") if isinstance(info.get("summary"), dict) else {}
        pair = _best_pair(m, info)
        info2 = (pair or {}).get("info") if isinstance((pair or {}).get("info"), dict) else {}
        summary2 = info2.get("summary") if isinstance(info2.get("summary"), dict) else {}
        summary = _merge_briefs(summary1, summary2)
        # If we have a membership_id, fill missing staff fields via Whop API to match production cards better.
        mid = str(info.get("membership_id") or "").strip() or str(info2.get("membership_id") or "").strip()
        if mid and whop_api_client and bool(WHOP_API_CONFIG.get("enable_enrichment", True)):
            api_brief = await _fetch_whop_brief_by_membership_id(mid)
            if isinstance(api_brief, dict) and api_brief:
                summary = _merge_briefs(summary, api_brief)
        title = str(info.get("title") or "").strip() or "Whop card"
        jump = str(getattr(m, "jump_url", "") or "").strip()

        did_s = str(info.get("discord_id") or "").strip() or str(info2.get("discord_id") or "").strip()
        try:
            did = int(did_s) if did_s.isdigit() else 0
        except Exception:
            did = 0

        member = await _resolve_member(main_guild, did) if did else None
        event_name = _event_guess(title)
        if member:
            relevant = coerce_role_ids(ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B)
            access = access_roles_plain(member, relevant)
            e = _build_member_status_detailed_embed(
                title=f"[SMOKE] {title}",
                member=member,
                access_roles=access,
                color=0x5865F2,
                discord_kv=[
                    ("event", event_name),
                    ("source_primary_channel_id", str(src.id)),
                    ("source_primary_message_id", str(m.id)),
                    ("source_primary_jump_url", jump),
                    ("source_secondary_channel_id", str(getattr(src2, "id", "") or "") if src2 else ""),
                    ("source_secondary_message_id", str(getattr((pair or {}).get("msg"), "id", "") or "") if pair else ""),
                ],
                whop_brief=summary,
            )
        else:
            # Fallback: no member resolvable in the main guild (or no Discord ID on the card).
            e = discord.Embed(
                title=f"[SMOKE] {title}",
                color=0xFEE75C,
                timestamp=datetime.now(timezone.utc),
            )
            e.add_field(name="Source", value=f"{src.name} (`{src.id}`)", inline=False)
            e.add_field(name="Message", value=f"`{m.id}`" + (f"\n{jump}" if jump else ""), inline=False)
            e.add_field(name="Discord ID", value=(f"`{did_s}`" if did_s else "—"), inline=True)
            e.add_field(name="Total Spent", value=str((summary or {}).get("total_spent") or "—"), inline=True)
            e.add_field(name="Whop Dashboard", value=str((summary or {}).get("dashboard_url") or "—"), inline=True)
            e.set_footer(text="RSCheckerbot • Startup Whop Smoke Test")

        embeds.append(e)

        # Also post minimal "case" embeds into the Neo test server's case channels (best-effort).
        if member and isinstance(out_guild, discord.Guild):
            try:
                if event_name == "payment.failed" and isinstance(out_payment_ch, discord.TextChannel):
                    mini = _build_case_minimal_embed(
                        title=f"[SMOKE] {title}",
                        member=member,
                        access_roles=access_roles_plain(member, coerce_role_ids(ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B)),
                        whop_brief=summary,
                        color=0xED4245,
                        event_kind="payment_failed",
                    )
                    with suppress(Exception):
                        await out_payment_ch.send(embed=mini, allowed_mentions=discord.AllowedMentions.none())
                elif event_name == "cancellation_scheduled" and isinstance(out_cancel_ch, discord.TextChannel):
                    mini = _build_case_minimal_embed(
                        title=f"[SMOKE] {title}",
                        member=member,
                        access_roles=access_roles_plain(member, coerce_role_ids(ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B)),
                        whop_brief=summary,
                        color=0xFEE75C,
                        event_kind="cancellation_scheduled",
                    )
                    with suppress(Exception):
                        await out_cancel_ch.send(embed=mini, allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                pass

    header = (
        f"RSCheckerbot startup Whop smoke test: last {count} message(s)\n"
        f"primary: {src.name} (`{src.id}`)\n"
        f"secondary: {getattr(src2, 'name', '—')} (`{getattr(src2, 'id', '—')}`)\n"
        f"dest: {getattr(out.guild, 'name', 'unknown')} / #{out.name} (`{out.id}`)\n"
        f"case_channels: #{PAYMENT_FAILURE_CHANNEL_NAME}, #{MEMBER_CANCELLATION_CHANNEL_NAME}"
    )
    if found == 0:
        header += "\n(no messages found in source history)"

    def _chunks(items: list[discord.Embed], size: int = 10) -> list[list[discord.Embed]]:
        out_chunks: list[list[discord.Embed]] = []
        i = 0
        while i < len(items):
            out_chunks.append(items[i : i + size])
            i += size
        return out_chunks

    chunks = _chunks(embeds, 10) if embeds else [[]]
    try:
        sent_any: discord.Message | None = None
        for idx, chunk in enumerate(chunks):
            content = header if idx == 0 else f"[SMOKE] continued {idx + 1}/{len(chunks)}"
            sent_any = await out.send(content=content[:1900], embeds=chunk, allowed_mentions=discord.AllowedMentions.none())
        log.info(
            "[BOOT][WhopSmoke] posted %s embed(s) to %s/#%s (%s) msg_id=%s",
            len(embeds),
            getattr(out.guild, "id", ""),
            getattr(out, "name", ""),
            getattr(out, "id", ""),
            getattr(sent_any, "id", "") if sent_any else "",
        )
    except Exception as e:
        log.warning(
            "[BOOT][WhopSmoke] FAILED to post to %s/#%s (%s): %s",
            getattr(out.guild, "id", ""),
            getattr(out, "name", ""),
            getattr(out, "id", ""),
            str(e)[:240],
        )

    # Optional: mirror a sample of existing staff cards (member joined/left/payment cards etc).
    if WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_ENABLED and int(MEMBER_STATUS_LOGS_CHANNEL_ID or 0):
        staff_src: discord.TextChannel | None = None
        with suppress(Exception):
            staff_src = await _get_text_channel(int(MEMBER_STATUS_LOGS_CHANNEL_ID))
        if isinstance(staff_src, discord.TextChannel):
            hist_lim = int(WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_HISTORY_LIMIT or 200)
            hist_lim = max(10, min(hist_lim, 1000))
            max_unique = int(WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_MAX_UNIQUE_TITLES or 25)
            max_unique = max(0, min(max_unique, 100))

            seen_titles: set[str] = set()
            samples: list[dict] = []
            async for sm in staff_src.history(limit=hist_lim):
                if not sm.embeds:
                    continue
                e0 = sm.embeds[0]
                t0 = _title_key(str(getattr(e0, "title", "") or ""))
                if not t0 or t0 in seen_titles:
                    continue
                seen_titles.add(t0)
                samples.append({"embed": e0, "jump": str(getattr(sm, "jump_url", "") or "").strip()})
                if max_unique and len(samples) >= max_unique:
                    break

            if samples:
                with suppress(Exception):
                    await out.send(
                        content=(
                            f"[SMOKE] Staff card samples (mirror)\n"
                            f"source: {getattr(staff_src.guild, 'name', 'unknown')} / #{staff_src.name} (`{staff_src.id}`)\n"
                            f"unique_titles: {len(samples)} (history_limit={hist_lim})"
                        )[:1900],
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                for s in samples:
                    jump = str(s.get("jump") or "").strip()
                    emb = s.get("embed") if isinstance(s.get("embed"), discord.Embed) else None
                    if not emb:
                        continue
                    content = f"[SMOKE] {jump}" if jump else "[SMOKE]"
                    with suppress(Exception):
                        await out.send(content=content[:1900], embed=emb, allowed_mentions=discord.AllowedMentions.none())


async def _best_payment_for_membership(mid: str, *, limit: int = 0) -> dict:
    """Return the most recent payment we can associate to membership_id (best-effort)."""
    mem_id = str(mid or "").strip()
    if not mem_id or not whop_api_client:
        return {}
    try:
        pays = await whop_api_client.get_payments_for_membership(mem_id)
    except Exception:
        pays = []
    pool = [p for p in (pays or []) if isinstance(p, dict)]
    if not pool:
        return {}

    def _payment_mid(p: dict) -> str:
        v = p.get("membership_id") or p.get("membership") or ""
        if isinstance(v, dict):
            return str(v.get("id") or v.get("membership_id") or "").strip()
        return str(v or "").strip()

    # Prefer payments that explicitly reference this membership id.
    filtered = [p for p in pool if _payment_mid(p) == mem_id]
    if not filtered:
        return {}
    pool2 = filtered

    # Sort by paid_at/created_at desc.
    def _ts(p: dict) -> str:
        return str(p.get("paid_at") or p.get("created_at") or "").strip()

    with suppress(Exception):
        pool2.sort(key=_ts, reverse=True)
    if int(limit or 0) > 0 and len(pool2) > int(limit):
        pool2 = pool2[: int(limit)]
    return pool2[0] if pool2 and isinstance(pool2[0], dict) else {}


async def _startup_canceling_members_snapshot() -> None:
    """On startup, query Whop API for memberships that are canceling/cancel_at_period_end and post a snapshot to Neo."""
    try:
        if not REPORTING_CONFIG.get("startup_canceling_snapshot_enabled"):
            return
        if not whop_api_client:
            return

        # Output channel (Neo): reuse the same config used for cancellation reminders mirror.
        try:
            out_gid = int(REPORTING_CONFIG.get("cancel_reminders_output_guild_id") or 0)
        except Exception:
            out_gid = 0
        out_name = str(REPORTING_CONFIG.get("cancel_reminders_output_channel_name") or "").strip()
        if not out_gid or not out_name:
            return

        g = bot.get_guild(int(out_gid))
        if not g:
            log.warning("[BOOT][Canceling] output guild not found: %s", out_gid)
            return

        ch = await _get_or_create_text_channel(g, name=out_name, category_id=STAFF_ALERTS_CATEGORY_ID)
        if not isinstance(ch, discord.TextChannel):
            log.warning("[BOOT][Canceling] output channel not found/creatable: %s", out_name)
            return

        # Optional mirror channels (e.g. main guild case channel).
        mirror_chs: list[discord.TextChannel] = []
        try:
            mirror_ids = REPORTING_CONFIG.get("startup_canceling_snapshot_mirror_channel_ids")
        except Exception:
            mirror_ids = []
        if not isinstance(mirror_ids, list):
            mirror_ids = []
        for cid in mirror_ids:
            try:
                cid_i = int(cid)
            except Exception:
                continue
            if cid_i <= 0 or cid_i == int(ch.id):
                continue
            mch = bot.get_channel(cid_i)
            if mch is None:
                with suppress(Exception):
                    mch = await bot.fetch_channel(cid_i)
            if isinstance(mch, discord.TextChannel):
                mirror_chs.append(mch)

        max_pages = int(REPORTING_CONFIG.get("startup_canceling_snapshot_max_pages") or 0) or 3
        per_page = int(REPORTING_CONFIG.get("startup_canceling_snapshot_per_page") or 0) or 100
        max_rows = int(REPORTING_CONFIG.get("startup_canceling_snapshot_max_rows") or 0) or 50
        min_spent = float(REPORTING_CONFIG.get("startup_canceling_snapshot_min_total_spent_usd") or 0.0)
        log_each = bool(REPORTING_CONFIG.get("startup_canceling_snapshot_log_each_member", False))
        per_member_msgs = bool(REPORTING_CONFIG.get("startup_canceling_snapshot_one_message_per_member", False))
        clear_first = bool(REPORTING_CONFIG.get("startup_canceling_snapshot_clear_channel", False))
        try:
            clear_limit = int(REPORTING_CONFIG.get("startup_canceling_snapshot_clear_limit", 200))
        except Exception:
            clear_limit = 200
        clear_limit = max(0, min(clear_limit, 500))
        mirror_clear_first = bool(REPORTING_CONFIG.get("startup_canceling_snapshot_mirror_clear_channel", False))
        try:
            mirror_clear_limit = int(REPORTING_CONFIG.get("startup_canceling_snapshot_mirror_clear_limit", clear_limit))
        except Exception:
            mirror_clear_limit = clear_limit
        mirror_clear_limit = max(0, min(mirror_clear_limit, 500))
        fill_total_via_payments = bool(REPORTING_CONFIG.get("startup_canceling_snapshot_fill_total_spend_via_payments", False))
        attach_csv = bool(REPORTING_CONFIG.get("startup_canceling_snapshot_attach_csv", False))
        try:
            pay_sleep_ms = int(REPORTING_CONFIG.get("startup_canceling_snapshot_payments_sleep_ms", 120))
        except Exception:
            pay_sleep_ms = 120
        pay_sleep_ms = max(0, min(pay_sleep_ms, 2000))

        # Optional filters: reduce noisy/incorrect "canceling" rows.
        try:
            skip_remaining_gt = int(REPORTING_CONFIG.get("startup_canceling_snapshot_skip_if_remaining_days_gt") or 0)
        except Exception:
            skip_remaining_gt = 0
        skip_remaining_gt = max(0, min(skip_remaining_gt, 3650))
        skip_keywords = REPORTING_CONFIG.get("startup_canceling_snapshot_skip_payment_status_keywords")
        if not isinstance(skip_keywords, list):
            skip_keywords = []
        skip_keywords_norm = sorted({str(x or "").strip().lower() for x in skip_keywords if str(x or "").strip()})
        try:
            pay_check_limit = int(REPORTING_CONFIG.get("startup_canceling_snapshot_payment_check_limit") or 0)
        except Exception:
            pay_check_limit = 0
        pay_check_limit = max(0, min(pay_check_limit, 200))

        exclude_prod = REPORTING_CONFIG.get("startup_canceling_snapshot_exclude_product_title_keywords")
        if not isinstance(exclude_prod, list):
            exclude_prod = []
        exclude_prod_norm = sorted({str(x or "").strip().lower() for x in exclude_prod if str(x or "").strip()})

        # Prefer Whop's official "canceling" status filter (matches dashboard UI).
        log.info("[BOOT][Canceling] scanning Whop memberships (statuses=canceling pages=%s per_page=%s)", max_pages, per_page)
        rows: list[dict] = []
        now_utc = datetime.now(timezone.utc)

        # Best-effort enrichment caches (avoid repeat API hits per membership/member).
        membership_cache: dict[str, dict] = {}
        member_cache: dict[str, dict] = {}
        src_guild = bot.get_guild(int(GUILD_ID)) if str(GUILD_ID or "").strip().isdigit() else None

        def _extract_member_id(m: dict) -> str:
            """Extract mber_... from membership/member objects (best-effort)."""
            if not isinstance(m, dict):
                return ""
            mm = m.get("member")
            if isinstance(mm, str) and mm.strip().startswith("mber_"):
                return mm.strip()
            if isinstance(mm, dict):
                mid = str(mm.get("id") or mm.get("member_id") or "").strip()
                if mid.startswith("mber_"):
                    return mid
            mid2 = str(m.get("member_id") or "").strip()
            if mid2.startswith("mber_"):
                return mid2
            return ""

        def _fmt_usd_amt(amt: float) -> str:
            try:
                return f"${float(amt):,.2f}"
            except Exception:
                return "—"

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

            # Direct fields first
            amt, found = _from_dict(obj)
            if found:
                return (amt, True)

            # stats.* fields
            stats = obj.get("stats") if isinstance(obj.get("stats"), dict) else {}
            if isinstance(stats, dict) and stats:
                amt2, found2 = _from_dict(stats)
                if found2:
                    return (amt2, True)

            # user.* fields (some endpoints nest under user)
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

        def _mrr_usd(obj: object) -> tuple[float, bool]:
            return _usd_from_obj(
                obj,
                usd_keys=("mrr_usd", "mrr"),
                cents_keys=("mrr_cents",),
            )

        async def _total_spend_from_payments(mid: str) -> str:
            """Compute total spend from successful payments (best-effort)."""
            mem_id = str(mid or "").strip()
            if not mem_id:
                return ""
            try:
                pays = await whop_api_client.get_payments_for_membership(mem_id)
            except Exception:
                pays = []
            total = 0.0
            for p in (pays or []):
                if not isinstance(p, dict):
                    continue
                st = str(p.get("status") or "").strip().lower()
                if st not in {"succeeded", "paid", "successful", "success"}:
                    continue
                # Prefer explicit *_cents keys when present.
                cents_raw = (
                    p.get("amount_cents")
                    or p.get("paid_amount_cents")
                    or p.get("total_cents")
                    or p.get("amount_usd_cents")
                    or p.get("paid_amount_usd_cents")
                    or p.get("total_usd_cents")
                )
                usd_raw = (
                    p.get("amount_usd")
                    or p.get("paid_amount_usd")
                    or p.get("total_usd")
                    or p.get("amount")
                    or p.get("paid_amount")
                    or p.get("total")
                )
                try:
                    if cents_raw is not None and str(cents_raw).strip() != "":
                        total += float(usd_amount(cents_raw) / 100.0)
                    elif usd_raw is not None and str(usd_raw).strip() != "":
                        total += float(usd_amount(usd_raw))
                except Exception:
                    continue
            return _fmt_usd_amt(total) if total >= 0 else ""

        after: str | None = None
        scanned_pages = 0

        # Local identity cache: email -> discord_id (fast + no Discord history scan).
        identity_cache: dict = {}
        with suppress(Exception):
            identity_cache = load_json(BASE_DIR / "whop_identity_cache.json")
        if not isinstance(identity_cache, dict):
            identity_cache = {}

        # Build a quick email->discord_id index from whop-logs (one history scan, rate-limit friendly).
        email_to_did: dict[str, int] = {}
        try:
            lim = int(WHOP_API_CONFIG.get("logs_lookup_limit", 50))
        except Exception:
            lim = 50
        lim = max(10, min(lim, 250))
        try:
            import re as _re

            cid = int(WHOP_LOGS_CHANNEL_ID)
            ch_logs = bot.get_channel(cid)
            if ch_logs is None:
                ch_logs = await bot.fetch_channel(cid)
            if isinstance(ch_logs, discord.TextChannel):
                async for m in ch_logs.history(limit=lim):
                    if not m.embeds:
                        continue
                    e0 = m.embeds[0]
                    fields = getattr(e0, "fields", None) or []
                    blob = " ".join(
                        [
                            str(getattr(e0, "title", "") or ""),
                            str(getattr(e0, "description", "") or ""),
                        ]
                        + [f"{getattr(f,'name','')}: {getattr(f,'value','')}" for f in fields]
                    )
                    mm = _re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", blob)
                    did_m = _re.search(r"(\d{17,19})", blob)
                    if mm and did_m:
                        em = mm.group(1).strip().lower()
                        did_i = int(did_m.group(1))
                        if em and did_i:
                            email_to_did[em] = did_i
            else:
                log.warning("[BOOT][Canceling] cannot read whop-logs history (channel not text): %s", str(type(ch_logs)))
        except Exception as ex:
            log.warning("[BOOT][Canceling] cannot read whop-logs history: %s", str(ex)[:240])

        # Build a quick membership_id/email -> total_spent index from whop-membership-logs (last N cards).
        mid_to_spend: dict[str, str] = {}
        email_to_spend: dict[str, str] = {}
        try:
            import re as _re

            cid = int(WHOP_WEBHOOK_CHANNEL_ID)
            ch_memlogs = bot.get_channel(cid)
            if ch_memlogs is None:
                ch_memlogs = await bot.fetch_channel(cid)
            if isinstance(ch_memlogs, discord.TextChannel):
                async for m in ch_memlogs.history(limit=lim):
                    if not m.embeds:
                        continue
                    e0 = m.embeds[0]
                    fields = getattr(e0, "fields", None) or []
                    blob = " ".join(
                        [
                            str(getattr(e0, "title", "") or ""),
                            str(getattr(e0, "description", "") or ""),
                        ]
                        + [f"{getattr(f,'name','')}: {getattr(f,'value','')}" for f in fields]
                    )
                    mm = _re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", blob)
                    mid_m = _re.search(r"(mem_[A-Za-z0-9]+)", blob)
                    spent_m = _re.search(r"total\s+spen[dt]\s*:\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", blob, _re.IGNORECASE)

                    em = mm.group(1).strip().lower() if mm else ""
                    mid_s = mid_m.group(1).strip() if mid_m else ""
                    spent_s = ""
                    if spent_m:
                        try:
                            spent_s = f"${float(spent_m.group(1).replace(',', '')):.2f}"
                        except Exception:
                            spent_s = ""
                    if spent_s:
                        if mid_s:
                            mid_to_spend[mid_s] = spent_s
                        if em:
                            email_to_spend[em] = spent_s
            else:
                log.warning("[BOOT][Canceling] cannot read whop-membership-logs history (channel not text): %s", str(type(ch_memlogs)))
        except Exception as ex:
            log.warning("[BOOT][Canceling] cannot read whop-membership-logs history: %s", str(ex)[:240])

        log.info(
            "[BOOT][Canceling] index whop_logs emails=%s memlogs mids=%s memlogs emails=%s",
            len(email_to_did),
            len(mid_to_spend),
            len(email_to_spend),
        )

        async def _maybe_clear_channel(target: discord.TextChannel, *, do_clear: bool, limit_n: int) -> None:
            if not do_clear or not bot.user or limit_n <= 0:
                return
            try:
                deleted = 0
                async for m in target.history(limit=limit_n):
                    if int(getattr(m.author, "id", 0) or 0) != int(bot.user.id):
                        continue
                    with suppress(Exception):
                        await m.delete()
                        deleted += 1
                if deleted:
                    log.info("[BOOT][Canceling] cleared %s bot message(s) in #%s", deleted, str(getattr(target, "name", "")))
            except Exception as ex:
                log.warning("[BOOT][Canceling] clear channel failed: %s", str(ex)[:240])

        # Optional: clear recent bot messages in the snapshot channels (keep channels tidy).
        await _maybe_clear_channel(ch, do_clear=clear_first, limit_n=clear_limit)
        for mch in mirror_chs:
            await _maybe_clear_channel(mch, do_clear=mirror_clear_first, limit_n=mirror_clear_limit)

        # Live progress message in primary snapshot channel (edited as we scan/post).
        progress_msg: discord.Message | None = None
        try:
            txt = _progress_text(
                label="Canceling Snapshot",
                step=(1, 1),
                done=0,
                total=max_rows,
                stats={"pages": f"{scanned_pages}/{max_pages}", "rows": 0, "errors": 0},
                stage="start",
            )
            progress_msg = await ch.send(content=txt, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            progress_msg = None

        while scanned_pages < max_pages and len(rows) < max_rows:
            batch, page_info = await whop_api_client.list_memberships(
                first=per_page,
                after=after,
                params={"statuses[]": "canceling", "order": "canceled_at", "direction": "asc"},
            )
            if not batch:
                break
            log.info("[BOOT][Canceling] page=%s got=%s has_next=%s", scanned_pages + 1, len(batch), bool(page_info.get("has_next_page") if isinstance(page_info, dict) else False))

            for rec in batch:
                if not isinstance(rec, dict):
                    continue
                mship = _whop_report_normalize_membership(rec)
                if not isinstance(mship, dict):
                    continue

                status_l = str(mship.get("status") or "").strip().lower()
                cape = (mship.get("cancel_at_period_end") is True) or _whop_report_norm_bool(mship.get("cancel_at_period_end"))

                mid = _whop_report_membership_id(mship)
                mship_full: dict | None = None
                if mid:
                    if mid not in membership_cache:
                        membership_cache[mid] = (await whop_api_client.get_membership_by_id(mid)) or {}
                    mship_full = membership_cache.get(mid) if isinstance(membership_cache.get(mid), dict) and membership_cache.get(mid) else None

                # Use fuller membership object when available to fill missing totals/IDs.
                m_use = mship_full if isinstance(mship_full, dict) and mship_full else mship

                # Best-effort resolve Discord ID and email:
                did = _whop_report_extract_discord_id(m_use) or 0
                email = _whop_report_extract_email(m_use)
                did_src = "membership" if did else ""

                # Member record (used for Whop dashboard-style fields: total spend, mrr, customer_since, connected discord).
                mber_id = _extract_member_id(m_use)
                mrec: dict | None = None
                if mber_id:
                    if mber_id not in member_cache:
                        member_cache[mber_id] = (await whop_api_client.get_member_by_id(mber_id)) or {}
                    mrec = member_cache.get(mber_id) if isinstance(member_cache.get(mber_id), dict) and member_cache.get(mber_id) else None

                # Preferred fallback: identity cache by email.
                if (not did) and email:
                    rec0 = identity_cache.get(str(email).strip().lower() or "")
                    if isinstance(rec0, dict):
                        did_s = str(rec0.get("discord_id") or "").strip()
                        if did_s.isdigit():
                            did = int(did_s)
                            did_src = "identity_cache"

                # If missing Discord ID, try member record via mber_... (often has connections).
                if (not did) and isinstance(mrec, dict) and mrec:
                    raw = extract_discord_id_from_whop_member_record(mrec)
                    if str(raw or "").strip().isdigit():
                        did = int(str(raw).strip())
                        did_src = "member_api"
                if (not email) and isinstance(mrec, dict) and mrec:
                    # member record usually has user.email
                    u = mrec.get("user")
                    if isinstance(u, dict):
                        email = str(u.get("email") or "").strip()

                # Final fallback (no API): use whop-logs email->discord_id index.
                if (not did) and email:
                    did2 = email_to_did.get(str(email).strip().lower() or "")
                    if isinstance(did2, int) and did2:
                        did = int(did2)
                        did_src = "whop_logs"

                # Total spend: use explicit *_cents fields when present; otherwise treat as USD.
                mem_spend_usd, mem_spend_found = _total_spend_usd(m_use)
                user_spend_usd, user_spend_found = _total_spend_usd(mrec) if isinstance(mrec, dict) and mrec else (0.0, False)

                total_spent_s = ""
                spend_src = ""
                if user_spend_found and (not mem_spend_found or float(user_spend_usd) >= float(mem_spend_usd)):
                    total_spent_s = _fmt_usd_amt(float(user_spend_usd))
                    spend_src = "member_record"
                elif mem_spend_found:
                    total_spent_s = _fmt_usd_amt(float(mem_spend_usd))
                    spend_src = "membership"

                # Preferred fallback: native membership cache keyed by membership_id.
                if (not total_spent_s) and mid:
                    with suppress(Exception):
                        nsum = _get_native_summary_by_mid(mid)
                        if isinstance(nsum, dict) and nsum:
                            n_spent = str(nsum.get("total_spent") or "").strip()
                            if n_spent and re.search(r"\d", n_spent):
                                total_spent_s = _fmt_usd_amt(float(usd_amount(n_spent)))
                                spend_src = "native_cache"
                # Use memlogs mapping if available (fast, no extra API).
                if (not total_spent_s) and mid and mid in mid_to_spend:
                    total_spent_s = str(mid_to_spend.get(mid) or "").strip()
                    if total_spent_s:
                        spend_src = "whop_membership_logs"
                if (not total_spent_s) and email:
                    ekey = str(email).strip().lower()
                    if ekey in email_to_spend:
                        total_spent_s = str(email_to_spend.get(ekey) or "").strip()
                        if total_spent_s:
                            spend_src = "whop_membership_logs"
                if (not total_spent_s) and fill_total_via_payments and mid:
                    # Slow but accurate fallback (startup scan only).
                    try:
                        total_spent_s = await _total_spend_from_payments(mid)
                    except Exception:
                        total_spent_s = ""
                    if pay_sleep_ms:
                        await asyncio.sleep(float(pay_sleep_ms) / 1000.0)
                    if total_spent_s:
                        spend_src = "payments"

                # Whop dashboard-style fields (best-effort).
                mrr_s = "—"
                mrr_usd, mrr_found = _mrr_usd(m_use)
                if not mrr_found and isinstance(mrec, dict) and mrec:
                    mrr_usd, mrr_found = _mrr_usd(mrec)
                if mrr_found:
                    mrr_s = _fmt_usd_amt(float(mrr_usd))

                # Customer since: prefer member/user created date; fall back to membership created_at/date_joined.
                cust_raw = ""
                if isinstance(mrec, dict) and mrec:
                    cust_raw = str(mrec.get("created_at") or "").strip()
                    if not cust_raw and isinstance(mrec.get("user"), dict):
                        cust_raw = str(mrec["user"].get("created_at") or mrec["user"].get("createdAt") or "").strip()
                if not cust_raw:
                    cust_raw = str(m_use.get("date_joined") or m_use.get("created_at") or "").strip()
                cust_since = _fmt_date_any(cust_raw) if cust_raw else "—"

                # Connected Discord username (best-effort): identity cache, then member record.
                discord_user = "—"
                if email:
                    rec0 = identity_cache.get(str(email).strip().lower() or "")
                    if isinstance(rec0, dict):
                        du = str(rec0.get("discord_username") or "").strip()
                        if du:
                            # Often "name (<@id>)" - keep just the handle.
                            discord_user = du.split(" (", 1)[0].strip() or du

                # Cancellation reason (best-effort; matches Whop dashboard).
                cancel_opt = str(m_use.get("cancel_option") or "").strip()
                cancel_reason_free = str(m_use.get("cancellation_reason") or "").strip()
                cancel_reason = cancel_opt or cancel_reason_free
                if cancel_reason:
                    cancel_reason = cancel_reason.replace("_", " ").strip().title()
                else:
                    cancel_reason = "—"
                # Prefer "scheduled/canceled at" timestamps if available.
                cancel_when_dt = _whop_report_pick_dt(
                    m_use,
                    [
                        "canceled_at",
                        "cancelled_at",
                        "cancel_at",
                        "cancel_at_period_end_at",
                        "cancellation_scheduled_at",
                        "updated_at",
                    ],
                )
                canceled_at_disp = "—"
                if isinstance(cancel_when_dt, datetime):
                    tz2 = timezone.utc
                    tz_name2 = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
                    if ZoneInfo is not None:
                        with suppress(Exception):
                            tz2 = ZoneInfo(tz_name2)
                    dtl = cancel_when_dt.astimezone(tz2)
                    canceled_at_disp = dtl.strftime("%b %d, %Y - %I:%M %p").replace(" 0", " ")

                # If still unknown, prefer showing "—" rather than a misleading $0.00.
                if not total_spent_s:
                    total_spent_s = "—"
                    spend_src = "unknown"

                # Build brief from membership (for dashboard/manage links). Override total_spent with enriched value if we found it.
                brief = _whop_report_brief_from_membership(m_use, api_client=whop_api_client)
                if total_spent_s:
                    brief["total_spent"] = total_spent_s

                # Joined/renewal window (staff-display string)
                renewal_window = ""
                rs = str(brief.get("renewal_start") or "").strip()
                re = str(brief.get("renewal_end") or "").strip()
                if rs and re and rs != "—" and re != "—":
                    renewal_window = f"{rs} → {re}"

                # Checkout URL (best-effort; often present on native webhook cards, sometimes on API membership).
                checkout_url = ""
                with suppress(Exception):
                    checkout_url = str(
                        m_use.get("checkout_url")
                        or m_use.get("checkout")
                        or m_use.get("purchase_link")
                        or m_use.get("purchase_url")
                        or ""
                    ).strip()

                spent = usd_amount(brief.get("total_spent"))
                if float(spent) < float(min_spent):
                    continue

                end_dt = _access_end_dt_from_membership(m_use)
                # Skip rows that are already ended/expired (no remaining days / no entitlement).
                if (not isinstance(end_dt, datetime)) or (end_dt <= now_utc):
                    continue

                # Skip extremely long "canceling" windows (often stale/incorrect rows).
                if skip_remaining_gt > 0:
                    try:
                        delta_s = float((end_dt - now_utc).total_seconds())
                        rem_days = max(0, int((delta_s / 86400.0) + 0.999))
                    except Exception:
                        rem_days = 0
                    if rem_days > int(skip_remaining_gt):
                        continue

                # Skip if recent payment looks like dispute/resolution (best-effort).
                if skip_keywords_norm and pay_check_limit > 0 and mid:
                    pay = await _best_payment_for_membership(mid, limit=pay_check_limit)
                    if isinstance(pay, dict) and pay:
                        txt = " ".join(
                            [
                                str(pay.get("status") or ""),
                                str(pay.get("substatus") or ""),
                                str(pay.get("billing_reason") or ""),
                                str(pay.get("failure_message") or ""),
                            ]
                        ).lower()
                        if any(k in txt for k in skip_keywords_norm):
                            continue
                # Best-effort user + product columns (match Whop UI list).
                user_name = ""
                try:
                    u = m_use.get("user")
                    if isinstance(u, dict):
                        user_name = str(u.get("name") or u.get("username") or "").strip()
                    elif isinstance(u, str):
                        user_name = u.strip()
                except Exception:
                    user_name = ""
                if not user_name:
                    user_name = str(email.split("@", 1)[0] if str(email or "").strip() else "").strip()
                product_title = ""
                try:
                    prod = m_use.get("product")
                    if isinstance(prod, dict):
                        product_title = str(prod.get("title") or "").strip()
                except Exception:
                    product_title = ""
                if not product_title:
                    try:
                        ap = m_use.get("access_pass")
                        if isinstance(ap, dict):
                            product_title = str(ap.get("title") or "").strip()
                    except Exception:
                        product_title = ""

                # Optional: exclude Lifetime (or other configured keywords) from set-to-cancel.
                if exclude_prod_norm:
                    prod_check = (product_title or str(brief.get("product") or "")).strip().lower()
                    if prod_check and any(k in prod_check for k in exclude_prod_norm):
                        continue

                # Joined timestamps (best-effort)
                first_joined_dt = _whop_report_pick_dt(m_use, ["member", "created_at", "createdAt", "created_on", "date_joined"])
                joined_dt = _whop_report_pick_dt(m_use, ["date_joined", "date_joined_at", "created_at", "createdAt", "created_on"])
                trial_end_dt = _whop_report_pick_dt(m_use, ["trial_end", "trial_end_at", "trial_ends_at", "trial_end_on"])

                rows.append(
                    {
                        "discord_id": int(did) if did else 0,
                        "discord_username": str(discord_user or "").strip(),
                        "email": str(email or "").strip(),
                        "membership_id": str(mid or "").strip(),
                        "status": status_l or "—",
                        "cancel_at_period_end": bool(cape),
                        "ends_at": end_dt,
                        "total_spent": str((brief.get("total_spent") or total_spent_s) or "").strip(),
                        "mrr": str(mrr_s or "").strip(),
                        "customer_since": str(cust_since or "").strip(),
                        "cancel_reason": str(cancel_reason or "").strip(),
                        "canceled_at": str(canceled_at_disp or "").strip(),
                        "user_name": user_name,
                        "product_title": product_title,
                        "first_joined_at": first_joined_dt,
                        "joined_at": joined_dt,
                        "trial_end_at": trial_end_dt,
                        "dashboard_url": str(brief.get("dashboard_url") or "").strip(),
                        "manage_url": str(brief.get("manage_url") or "").strip(),
                        "renewal_window": renewal_window,
                        "remaining_days": str(brief.get("remaining_days") or "").strip(),
                        "checkout_url": checkout_url,
                    }
                )
                if len(rows) >= max_rows:
                    break

                if log_each:
                    log.info(
                        "[BOOT][Canceling][row] email=%s did=%s did_src=%s discord_user=%s mid=%s mber=%s status=%s cape=%s end=%s spend=%s spend_src=%s mrr=%s customer_since=%s cancel_reason=%s canceled_at=%s source=%s",
                        str(email or ""),
                        str(did or ""),
                        str(did_src or ""),
                        str(discord_user or ""),
                        str(mid or ""),
                        str(mber_id or ""),
                        str(status_l or ""),
                        "true" if bool(cape) else "false",
                        str(end_dt.isoformat() if isinstance(end_dt, datetime) else ""),
                        str((brief.get("total_spent") or total_spent_s or "").strip()),
                        str(spend_src or ""),
                        str(mrr_s or ""),
                        str(cust_since or ""),
                        str(cancel_reason or ""),
                        str(canceled_at_disp or ""),
                        "api" if mship_full else "list",
                    )

            scanned_pages += 1
            after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
            has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
            if not has_next or not after:
                break

            if progress_msg:
                with suppress(Exception):
                    txt = _progress_text(
                        label="Canceling Snapshot",
                        step=(1, 1),
                        done=min(len(rows), max_rows),
                        total=max_rows,
                        stats={"pages": f"{scanned_pages}/{max_pages}", "rows": len(rows), "errors": 0},
                        stage="scan",
                    )
                    await progress_msg.edit(content=txt)

        # Sort by soonest end date.
        def _sort_key(r: dict) -> float:
            dt = r.get("ends_at")
            if isinstance(dt, datetime):
                return float(dt.timestamp())
            return 9e18

        rows.sort(key=_sort_key)

        # Render (embed layout aligned to Whop table columns).
        now = datetime.now(timezone.utc)

        def _fmt_delta(dt: datetime | None, *, prefix: str) -> str:
            if not isinstance(dt, datetime):
                return "—"
            delta = (dt - now).total_seconds()
            if delta <= 0:
                return "—"
            days = int(delta // 86400)
            hours = int((delta % 86400) // 3600)
            if days > 0:
                return f"{prefix} in {days}d {hours}h"
            return f"{prefix} in {hours}h"

        def _ts_rel(dt: datetime | None) -> str:
            if not isinstance(dt, datetime):
                return "—"
            return f"<t:{int(dt.timestamp())}:R>"

        # Header embed
        header = discord.Embed(
            title="Canceling (Whop) — Snapshot",
            description=f"Count: **{len(rows[:max_rows])}** • Source: Whop API `/memberships` filter `statuses[]=canceling`",
            color=0xFEE75C,
            timestamp=datetime.now(timezone.utc),
        )
        header.set_footer(text="RSCheckerbot • Whop API")
        embeds: list[discord.Embed] = []

        # Optional CSV attachment
        file_obj: discord.File | None = None
        if attach_csv and rows:
            try:
                buf = io.StringIO()
                w = csv.DictWriter(
                    buf,
                    fieldnames=[
                        "user",
                        "email",
                        "discord_id",
                        "membership_id",
                        "status",
                        "cancel_at_period_end",
                        "ends_at",
                        "total_spend",
                        "dashboard_url",
                        "manage_url",
                    ],
                )
                w.writeheader()
                for r in rows[:max_rows]:
                    ends = r.get("ends_at")
                    ends_s = ends.isoformat() if isinstance(ends, datetime) else ""
                    w.writerow(
                        {
                            "user": str(r.get("user_name") or ""),
                            "email": str(r.get("email") or ""),
                            "discord_id": str(r.get("discord_id") or ""),
                            "membership_id": str(r.get("membership_id") or ""),
                            "status": str(r.get("status") or ""),
                            "cancel_at_period_end": "true" if bool(r.get("cancel_at_period_end")) else "false",
                            "ends_at": ends_s,
                            "total_spend": str(r.get("total_spent") or ""),
                            "dashboard_url": str(r.get("dashboard_url") or ""),
                            "manage_url": str(r.get("manage_url") or ""),
                        }
                    )
                data = buf.getvalue().encode("utf-8")
                file_obj = discord.File(fp=io.BytesIO(data), filename="canceling-snapshot.csv")
            except Exception:
                file_obj = None

        # Send header (with optional file) first to all snapshot destinations.
        header_bytes: bytes | None = None
        if file_obj:
            with suppress(Exception):
                # Read underlying buffer so we can re-attach for mirrors.
                fp = getattr(file_obj, "fp", None)
                if fp and hasattr(fp, "getvalue"):
                    header_bytes = fp.getvalue()
        for dst in [ch] + mirror_chs:
            with suppress(Exception):
                if header_bytes:
                    f2 = discord.File(fp=io.BytesIO(header_bytes), filename="canceling-snapshot.csv")
                    await dst.send(embed=header, file=f2, allowed_mentions=discord.AllowedMentions.none())
                else:
                    await dst.send(embed=header, allowed_mentions=discord.AllowedMentions.none())

        # If configured, post one embed per member and stop (avoid bundled mega-embeds).
        if per_member_msgs:
            sent_msgs = 0
            last_err: Exception | None = None
            for r in rows[:max_rows]:
                user_name = str(r.get("user_name") or "").strip() or "Member"
                email_s = str(r.get("email") or "").strip() or "—"
                product_s = str(r.get("product_title") or "").strip() or "—"
                spent_s = str(r.get("total_spent") or "").strip() or "—"
                did = int(r.get("discord_id") or 0)
                mid = str(r.get("membership_id") or "").strip() or "—"
                status_l = str(r.get("status") or "").strip().lower() or "—"
                cape = bool(r.get("cancel_at_period_end"))
                trial_end = r.get("trial_end_at") if isinstance(r.get("trial_end_at"), datetime) else None
                end_dt = r.get("ends_at") if isinstance(r.get("ends_at"), datetime) else None

                # Display status: cancels for canceling/cape, else trial ends for trialing.
                if cape or status_l == "canceling":
                    status_disp = _fmt_delta(end_dt, prefix="Cancels") if end_dt else "Cancels —"
                elif status_l in {"trialing", "trial", "pending"} or trial_end:
                    status_disp = _fmt_delta(trial_end or end_dt, prefix="Trial ends")
                else:
                    status_disp = status_l or "—"

                dash_url = str(r.get("dashboard_url") or "").strip()
                manage_url = str(r.get("manage_url") or "").strip()
                discord_user = str(r.get("discord_username") or "").strip() or "—"
                mrr_s = str(r.get("mrr") or "").strip() or "—"
                cust_since = str(r.get("customer_since") or "").strip() or "—"
                cancel_reason = str(r.get("cancel_reason") or "").strip() or "—"
                canceled_at = str(r.get("canceled_at") or "").strip() or "—"

                # Match member-status-logs format: reuse the same staff embed builder when Discord member is resolvable.
                member_obj: discord.Member | None = None
                if src_guild and did:
                    with suppress(Exception):
                        member_obj = src_guild.get_member(int(did))
                    if member_obj is None:
                        with suppress(Exception):
                            member_obj = await src_guild.fetch_member(int(did))

                if member_obj is not None:
                    relevant = coerce_role_ids(ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B)
                    access = access_roles_plain(member_obj, relevant)

                    whop_brief = {
                        "status": (status_l or "—"),
                        "product": (product_s or "—"),
                        "membership_id": (mid if mid and mid != "—" else ""),
                        "total_spent": spent_s,
                        "remaining_days": (str(r.get("remaining_days") or "").strip() or ""),
                        "renewal_end": _fmt_date_any(end_dt) if isinstance(end_dt, datetime) else "",
                        "renewal_window": (str(r.get("renewal_window") or "").strip() or ""),
                        "dashboard_url": (dash_url or ""),
                        "manage_url": (manage_url or ""),
                        "cancel_at_period_end": "yes" if cape else "no",
                        "checkout_url": (str(r.get("checkout_url") or "").strip() or ""),
                        # Set-to-cancel style extras:
                        "mrr": (str(mrr_s or "").strip() if str(mrr_s or "").strip() and str(mrr_s).strip() != "—" else ""),
                        "customer_since": (str(cust_since or "").strip() if str(cust_since or "").strip() and str(cust_since).strip() != "—" else ""),
                        "connected_discord": (
                            f"{discord_user} ({did})" if (str(discord_user or '').strip() and str(discord_user).strip() != '—' and did) else ""
                        ),
                        "cancellation_reason": (
                            f"{cancel_reason}\n{product_s}\n{canceled_at}"
                            if (str(cancel_reason or '').strip() and str(cancel_reason).strip() != '—')
                            else ""
                        ),
                    }
                    title = "⚠️ Cancellation Scheduled" if cape else "⚠️ Canceling (Whop)"
                    e = _build_member_status_detailed_embed(
                        title=title,
                        member=member_obj,
                        access_roles=access,
                        color=0xFEE75C,
                        discord_kv=[("discord_username", discord_user)],
                        member_kv=[("event", "whop.snapshot.canceling"), ("membership_id", mid)],
                        whop_brief=whop_brief,
                        event_kind="cancellation_scheduled" if cape else "deactivated",
                    )
                else:
                    # Fallback: keep a compact embed (no Discord member available).
                    e = discord.Embed(
                        title=f"Canceling — {user_name}"[:256],
                        color=0xFEE75C,
                        timestamp=datetime.now(timezone.utc),
                    )
                    # In fallback mode the user often isn't in-guild; avoid mentions/links that show as @unknown-user
                    # or open external tabs. Keep it plain + include Discord ID separately.
                    if did:
                        link_name = (discord_user if discord_user and discord_user != "—" else user_name) or "Member"
                        e.add_field(name="Member", value=link_name[:1024], inline=True)
                    e.add_field(name="Email", value=email_s[:1024], inline=False)
                    e.add_field(name="Discord ID", value=(f"`{did}`" if did else "—"), inline=True)
                    e.add_field(name="Membership", value=product_s[:1024], inline=True)
                    e.add_field(name="Status", value=str(status_disp)[:1024], inline=True)
                    e.add_field(name="Total Spent (lifetime)", value=spent_s[:1024], inline=True)
                    if dash_url:
                        e.add_field(name="Whop Dashboard", value=dash_url[:1024], inline=False)
                    e.set_footer(text="RSCheckerbot • Whop API")

                # Snapshot-only extras are now carried via whop_brief so all cards share the same layout.

                try:
                    # Keep member identity inside the embed (no content mention -> avoids @unknown-user).
                    for dst in [ch] + mirror_chs:
                        # Main guild: clickable mention in message content (silent, no ping).
                        in_main = bool(int(getattr(getattr(dst, "guild", None), "id", 0) or 0) == int(GUILD_ID or 0))
                        content = f"<@{did}>" if (in_main and did) else ""
                        allow = discord.AllowedMentions(users=True, roles=False, everyone=False) if in_main else discord.AllowedMentions.none()
                        try:
                            await dst.send(content=content, embed=e, allowed_mentions=allow, silent=bool(in_main))
                        except TypeError:
                            await dst.send(content=content, embed=e, allowed_mentions=allow)
                    sent_msgs += 1
                except Exception as ex:
                    last_err = ex
                    break

            # Mark progress complete (best-effort).
            if progress_msg:
                with suppress(Exception):
                    txt = _progress_text(
                        label="Canceling Snapshot",
                        step=(1, 1),
                        done=min(len(rows), max_rows),
                        total=max_rows,
                        stats={"pages": f"{scanned_pages}/{max_pages}", "rows": len(rows), "msgs": sent_msgs, "errors": 0 if not last_err else 1},
                        stage="complete" if sent_msgs else "failed",
                    )
                    await progress_msg.edit(content=txt)

            if sent_msgs:
                log.info(
                    "[BOOT][Canceling] posted snapshot rows=%s msgs=%s to %s/#%s (%s)",
                    len(rows),
                    sent_msgs,
                    getattr(g, "id", ""),
                    getattr(ch, "name", ""),
                    getattr(ch, "id", ""),
                )
            else:
                log.warning(
                    "[BOOT][Canceling] FAILED to post snapshot (rows=%s) to %s/#%s (%s): %s",
                    len(rows),
                    getattr(g, "id", ""),
                    getattr(ch, "name", ""),
                    getattr(ch, "id", ""),
                    str(last_err)[:240] if last_err else "unknown error",
                )
            return

        def _embed_size(e: discord.Embed) -> int:
            try:
                n = len(str(e.title or "")) + len(str(e.description or ""))
                for f in (e.fields or []):
                    n += len(str(getattr(f, "name", "") or "")) + len(str(getattr(f, "value", "") or ""))
                return int(n)
            except Exception:
                return 0

        # Discord hard limit is 6000 chars per embed; keep a large safety margin.
        MAX_EMBED_SIZE = 4800

        def _new_page() -> discord.Embed:
            p = discord.Embed(
                title="Canceling Members (details)",
                color=0xFEE75C,
                timestamp=datetime.now(timezone.utc),
            )
            p.set_footer(text="RSCheckerbot • Whop API")
            return p

        page = _new_page()

        for r in rows[:max_rows]:
            user_name = str(r.get("user_name") or "").strip() or "—"
            email_s = str(r.get("email") or "").strip() or "—"
            product_s = str(r.get("product_title") or "").strip() or "—"
            spent_s = str(r.get("total_spent") or "").strip() or "—"
            did = int(r.get("discord_id") or 0)
            mid = str(r.get("membership_id") or "").strip() or "—"
            status_l = str(r.get("status") or "").strip().lower() or "—"
            cape = bool(r.get("cancel_at_period_end"))

            trial_end = r.get("trial_end_at") if isinstance(r.get("trial_end_at"), datetime) else None
            end_dt = r.get("ends_at") if isinstance(r.get("ends_at"), datetime) else None
            # Whop-style status column:
            # - If Whop says cancel_at_period_end / canceling: show "Cancels …" even if trial fields exist.
            if cape or status_l == "canceling":
                status_disp = _fmt_delta(end_dt, prefix="Cancels") if end_dt else "Cancels —"
            elif status_l in {"trialing", "trial", "pending"} or trial_end:
                status_disp = _fmt_delta(trial_end or end_dt, prefix="Trial ends")
            else:
                status_disp = (status_l or "—")

            first_joined = r.get("first_joined_at") if isinstance(r.get("first_joined_at"), datetime) else None
            joined_at = r.get("joined_at") if isinstance(r.get("joined_at"), datetime) else None

            contact = []
            if email_s and email_s != "—":
                contact.append("email")
            if did:
                contact.append("discord")
            contact_s = " / ".join(contact) if contact else "—"

            val = (
                f"Email: {email_s}\n"
                f"Product: {product_s}\n"
                f"Status: {status_disp}\n"
                f"Total spend: {spent_s}\n"
                f"Contact: {contact_s}\n"
                f"First joined at: {_ts_rel(first_joined)}\n"
                f"Joined at: {_ts_rel(joined_at)}\n"
                f"Discord ID: {f'`{did}`' if did else '—'}\n"
                f"Membership ID: {f'`{mid}`' if mid and mid != '—' else '—'}"
            )
            field_name = f"User: **{user_name}**"[:160]
            field_val = val[:520]

            # Add field, but ensure we don't exceed size limit.
            page.add_field(name=field_name, value=field_val, inline=False)
            if _embed_size(page) > MAX_EMBED_SIZE or len(page.fields) >= 25:
                # Remove and start a new page.
                with suppress(Exception):
                    page.remove_field(len(page.fields) - 1)
                embeds.append(page)
                page = _new_page()
                page.add_field(name=field_name, value=field_val, inline=False)

        if page.fields:
            embeds.append(page)

        if progress_msg:
            with suppress(Exception):
                txt = _progress_text(
                    label="Canceling Snapshot",
                    step=(1, 1),
                    done=min(len(rows), max_rows),
                    total=max_rows,
                    stats={"pages": f"{scanned_pages}/{max_pages}", "rows": len(rows), "errors": 0},
                    stage="post",
                )
                await progress_msg.edit(content=txt)

        # Legacy: compact paged embeds (kept for backwards compatibility).
        sent_msgs = 0
        last_err: Exception | None = None
        embeds.append(header)
        for e in embeds:
            try:
                await ch.send(embed=e, allowed_mentions=discord.AllowedMentions.none())
                sent_msgs += 1
            except Exception as ex:
                last_err = ex
                break

        # Mark progress complete (best-effort).
        if progress_msg:
            with suppress(Exception):
                txt = _progress_text(
                    label="Canceling Snapshot",
                    step=(1, 1),
                    done=min(len(rows), max_rows),
                    total=max_rows,
                    stats={"pages": f"{scanned_pages}/{max_pages}", "rows": len(rows), "msgs": sent_msgs, "errors": 0 if not last_err else 1},
                    stage="complete" if sent_msgs else "failed",
                )
                await progress_msg.edit(content=txt)

        if sent_msgs:
            log.info(
                "[BOOT][Canceling] posted snapshot rows=%s msgs=%s to %s/#%s (%s)",
                len(rows),
                sent_msgs,
                getattr(g, "id", ""),
                getattr(ch, "name", ""),
                getattr(ch, "id", ""),
            )
        else:
            log.warning(
                "[BOOT][Canceling] FAILED to post snapshot (rows=%s) to %s/#%s (%s): %s",
                len(rows),
                getattr(g, "id", ""),
                getattr(ch, "name", ""),
                getattr(ch, "id", ""),
                str(last_err)[:240] if last_err else "unknown error",
            )
    except Exception:
        log.exception("[BOOT][Canceling] snapshot failed")


async def _run_startup_scans() -> None:
    """Run startup scans in a single canonical sequence (anti-rate-limit)."""
    # Small delay so on_ready completes and caches settle.
    try:
        delay_s = float(LOG_CONTROLS.get("startup_scans_delay_seconds", 2.0))
    except Exception:
        delay_s = 2.0
    delay_s = max(0.0, min(delay_s, 10.0))
    if delay_s:
        await asyncio.sleep(delay_s)

    try:
        _STARTUP_SCANS_DONE.clear()
        # Run in order (cheap -> expensive).
        with suppress(Exception):
            await _startup_canceling_members_snapshot()

        with suppress(Exception):
            if WHOP_STARTUP_NATIVE_SMOKETEST_ENABLED:
                await _startup_native_whop_smoketest()

        with suppress(Exception):
            if WHOP_NATIVE_BACKFILL_LIMIT > 0:
                # Backfill can be heavy; keep it last.
                await _backfill_recent_native_whop_cards()
    finally:
        with suppress(Exception):
            _STARTUP_SCANS_DONE.set()


async def _start_whop_sync_job_after_startup() -> None:
    """Start the Whop membership sync job after a short delay to avoid startup pileups."""
    if not whop_api_client:
        return
    if not WHOP_API_CONFIG.get("enable_sync", True):
        return
    # If we're routing output to a different guild (Neo test mode), still allow sync,
    # but delay it so startup scans can complete first.
    # Wait for startup scans to complete (plus a small delay).
    try:
        delay_s = float(WHOP_API_CONFIG.get("startup_sync_delay_seconds", 20))
    except Exception:
        delay_s = 20.0
    delay_s = max(0.0, min(delay_s, 300.0))
    with suppress(Exception):
        await asyncio.wait_for(_STARTUP_SCANS_DONE.wait(), timeout=300.0)
    if delay_s:
        await asyncio.sleep(delay_s)

    try:
        sync_interval = WHOP_API_CONFIG.get("sync_interval_hours", 6)
        if not sync_whop_memberships.is_running():
            sync_whop_memberships.change_interval(hours=sync_interval)
            sync_whop_memberships.start()
            log.info(f"[Whop Sync] Membership sync job started (every {sync_interval} hours)")
    except Exception as e:
        log.warning(f"[Whop Sync] Failed to start sync job: {e}")


def _parse_hhmm(s: str) -> tuple[int, int]:
    try:
        txt = str(s or "").strip()
        if ":" not in txt:
            return (9, 0)
        hh, mm = txt.split(":", 1)
        h = max(0, min(int(hh), 23))
        m = max(0, min(int(mm), 59))
        return (h, m)
    except Exception:
        return (9, 0)


def _weekday_idx(name: str) -> int:
    m = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    return int(m.get(str(name or "").strip().lower(), 0))


async def _dm_user(user_id: int, *, embed: discord.Embed, content: str = "") -> bool:
    """DM a user ID (best-effort)."""
    try:
        uid = int(user_id)
    except Exception:
        return False
    try:
        user = bot.get_user(uid)
        if user is None:
            user = await bot.fetch_user(uid)
        if user is None:
            return False
        await user.send(content=content or None, embed=embed)
        return True
    except Exception as e:
        log.warning(f"[Reporting] Failed to DM user {user_id}: {e}")
        return False


def _load_reporting_store_sync() -> dict:
    global _REPORTING_STORE
    if _REPORTING_STORE is None:
        if _report_load_store:
            _REPORTING_STORE = _report_load_store(BASE_DIR, retention_weeks=int(REPORTING_CONFIG.get("retention_weeks", 26)))
        else:
            _REPORTING_STORE = {"meta": {}, "weeks": {}, "members": {}, "unlinked": {}}
    return _REPORTING_STORE


async def _save_reporting_store(store: dict) -> None:
    global _REPORTING_STORE
    _REPORTING_STORE = store
    if _report_save_store:
        _report_save_store(BASE_DIR, store)


def _normalize_whop_event(event: dict) -> dict:
    if not isinstance(event, dict):
        return {}
    ev = dict(event)
    ev.setdefault("event_id", "")
    ev.setdefault("source", "")
    ev.setdefault("event_type", "unknown")
    ev.setdefault("occurred_at", "")
    for key in (
        "membership_id",
        "user_id",
        "member_id",
        "discord_id",
        "email",
        "product",
        "status",
        "trial_days",
        "pricing",
        "total_spent",
        "cancel_at_period_end",
        "renewal_period_start",
        "renewal_period_end",
        "renewal_end_iso",
        "reason",
    ):
        if ev.get(key) is None:
            ev[key] = ""
    if not isinstance(ev.get("source_discord"), dict):
        ev["source_discord"] = {}
    return ev


def _whop_event_seen(event_id: str) -> bool:
    eid = str(event_id or "").strip()
    if not eid:
        return False
    if eid in _WHOP_EVENT_DEDUPE_IDS:
        return True
    _WHOP_EVENT_DEDUPE_IDS.add(eid)
    _WHOP_EVENT_DEDUPE_QUEUE.append(eid)
    if WHOP_EVENTS_DEDUPE_MAX > 0 and len(_WHOP_EVENT_DEDUPE_QUEUE) > WHOP_EVENTS_DEDUPE_MAX:
        old = _WHOP_EVENT_DEDUPE_QUEUE.popleft()
        _WHOP_EVENT_DEDUPE_IDS.discard(old)
    return False


def _load_whop_event_dedupe_cache() -> None:
    if WHOP_EVENTS_DEDUPE_MAX <= 0:
        return
    rows = iter_jsonl(WHOP_EVENTS_FILE)
    tail = rows[-WHOP_EVENTS_DEDUPE_MAX:] if len(rows) > WHOP_EVENTS_DEDUPE_MAX else rows
    for rec in tail:
        eid = str(rec.get("event_id") or "").strip()
        if not eid:
            continue
        _WHOP_EVENT_DEDUPE_IDS.add(eid)
        _WHOP_EVENT_DEDUPE_QUEUE.append(eid)


async def _record_whop_event(event: dict) -> bool:
    if not WHOP_EVENTS_ENABLED:
        return False
    ev = _normalize_whop_event(event)
    if not ev:
        return False
    event_id = str(ev.get("event_id") or "").strip()
    if not event_id:
        source = str(ev.get("source") or "event").strip()
        event_id = f"{source}:{int(time.time() * 1000)}"
        ev["event_id"] = event_id
    if _whop_event_seen(event_id):
        return False
    try:
        await append_jsonl(WHOP_EVENTS_FILE, ev)
    except Exception as e:
        log.warning(f"[WhopEvents] Failed to append event {event_id}: {e}")
        return False
    return True


def _whop_event_from_webhook_payload(payload: dict, *, event_id: str, occurred_at: datetime) -> dict:
    data = payload.get("data") if isinstance(payload, dict) else {}
    data = data if isinstance(data, dict) else {}
    event_type = str(payload.get("type") or payload.get("event_type") or payload.get("event") or "").strip()
    membership = data.get("membership") if isinstance(data.get("membership"), dict) else None
    if not membership:
        membership = data
    membership = _whop_report_normalize_membership(membership) if isinstance(membership, dict) else {}
    membership_id = str(
        data.get("membership_id")
        or membership.get("id")
        or membership.get("membership_id")
        or ""
    ).strip()
    user_id = str(data.get("user_id") or "").strip()
    if not user_id and isinstance(membership.get("user"), dict):
        user_id = str(membership["user"].get("id") or "").strip()
    member_id = str(data.get("member_id") or "").strip()
    if not member_id and isinstance(membership.get("member"), dict):
        member_id = str(membership["member"].get("id") or "").strip()
    email = ""
    if isinstance(membership.get("member"), dict):
        email = str(membership["member"].get("email") or "").strip()
    product = ""
    if isinstance(membership.get("product"), dict):
        product = str(membership["product"].get("title") or "").strip()
    status = str(membership.get("status") or data.get("status") or "").strip()
    total_spent = str(
        membership.get("total_spent")
        or membership.get("total_spent_usd")
        or data.get("total_spent")
        or ""
    ).strip()
    cancel_at_period_end = str(membership.get("cancel_at_period_end") or data.get("cancel_at_period_end") or "").strip()
    renewal_start = str(membership.get("renewal_period_start") or data.get("renewal_period_start") or "").strip()
    renewal_end = str(membership.get("renewal_period_end") or data.get("renewal_period_end") or "").strip()
    reason = str(
        data.get("failure_reason")
        or data.get("cancellation_reason")
        or data.get("reason")
        or ""
    ).strip()
    return {
        "event_id": event_id,
        "source": "whop_official_webhook",
        "event_type": event_type or "unknown",
        "occurred_at": occurred_at.isoformat(),
        "membership_id": membership_id,
        "user_id": user_id,
        "member_id": member_id,
        "discord_id": "",
        "email": email,
        "product": product,
        "status": status,
        "trial_days": str(membership.get("trial_days") or ""),
        "pricing": str(membership.get("pricing") or ""),
        "total_spent": total_spent,
        "cancel_at_period_end": cancel_at_period_end,
        "renewal_period_start": renewal_start,
        "renewal_period_end": renewal_end,
        "renewal_end_iso": str(membership.get("renewal_end_iso") or ""),
        "reason": reason,
        "source_discord": {},
    }

def _to_int(v: object) -> int | None:
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _extract_reporting_from_member_status_embed(
    sent_embed: discord.Embed,
    *,
    fallback_ts: int,
) -> tuple[int, str, int | None, dict]:
    """Extract (ts, kind, discord_id, whop_brief) from a member-status-logs embed."""
    # Timestamp
    ts_i = int(fallback_ts)
    try:
        if getattr(sent_embed, "timestamp", None):
            ts_i = int(sent_embed.timestamp.replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        pass

    title = str(getattr(sent_embed, "title", "") or "").strip()
    desc = str(getattr(sent_embed, "description", "") or "").strip()

    discord_id: int | None = None
    whop_brief: dict = {}

    # Fields
    try:
        for f in (getattr(sent_embed, "fields", None) or []):
            n = str(getattr(f, "name", "") or "").strip()
            v = str(getattr(f, "value", "") or "").strip()
            if not n:
                continue
            ln = n.lower()
            if ln == "discord id" and discord_id is None:
                m = re.search(r"(\d{17,19})", v)
                if m:
                    discord_id = int(m.group(1))
            elif ln == "total spent":
                whop_brief["total_spent"] = v
            elif ln in {"membership", "product"}:
                whop_brief["product"] = v
            elif ln == "status":
                whop_brief["status"] = v
            elif ln in {"whop dashboard", "dashboard"}:
                whop_brief["dashboard_url"] = v
            elif ln in {"cancel at period end", "cancel_at_period_end"}:
                whop_brief["cancel_at_period_end"] = v
            elif ln in {"access ends on", "next billing date", "renewal end", "renewal_end"}:
                # Best-effort: store ISO for reminders
                whop_brief["renewal_end"] = v
                m_ts = re.search(r"<t:(\d+):[A-Za-z]>", v)
                if m_ts:
                    whop_brief["renewal_end_iso"] = datetime.fromtimestamp(int(m_ts.group(1)), tz=timezone.utc).isoformat()
                else:
                    # Fall back to common "Month D, YYYY" format
                    try:
                        dt = datetime.strptime(v.replace(" 0", " "), "%B %d, %Y").replace(tzinfo=timezone.utc)
                        whop_brief["renewal_end_iso"] = dt.isoformat()
                    except Exception:
                        pass
    except Exception:
        pass

    # Fallback discord id from description (e.g., "... (1234567890)")
    if discord_id is None and desc:
        m = re.search(r"\((\d{17,19})\)", desc)
        if m:
            discord_id = int(m.group(1))

    # Kind inference from title/description (ONLY from member-status-logs outputs)
    kind = "unknown"
    t_low = title.lower()
    d_low = desc.lower()
    if "payment/onboarding complete" in d_low:
        kind = "member_role_added"
    elif "membership activated (pending)" in t_low or "activated (pending)" in t_low:
        kind = "membership_activated_pending"
    elif "cancellation scheduled" in t_low:
        kind = "cancellation_scheduled"
    elif "payment failed" in t_low:
        kind = "payment_failed"
    elif "membership deactivated" in t_low or "deactivated" in t_low:
        kind = "deactivated"
    elif "member joined" in t_low:
        kind = "member_joined"
    else:
        # Fallback: if status says trialing, treat as a trial signal.
        st = str(whop_brief.get("status") or "").strip().lower()
        if st == "trialing":
            kind = "trialing"

    return (ts_i, kind, discord_id, whop_brief)


@tasks.loop(seconds=60)
async def reporting_loop() -> None:
    """Weekly report + daily reminders (DM Neo)."""
    try:
        if not bot.is_ready():
            return
        if not REPORTING_CONFIG.get("enabled"):
            return
        if not MEMBER_STATUS_LOGS_CHANNEL_ID:
            return
        now_local = _tz_now()
        hh, mm = _parse_hhmm(str(REPORTING_CONFIG.get("report_time_local") or "09:00"))
        if now_local.hour != hh or now_local.minute != mm:
            return

        dm_uid = int(REPORTING_CONFIG.get("dm_user_id") or 0)
        if not dm_uid:
            return

        weekly_day = _weekday_idx(str(REPORTING_CONFIG.get("weekly_day_local") or "mon"))
        today_local = now_local.date().isoformat()
        today_wd = int(now_local.weekday())

        async with _REPORTING_STORE_LOCK:
            store = _load_reporting_store_sync()
            meta = store.get("meta") if isinstance(store.get("meta"), dict) else {}
            store["meta"] = meta

            # Daily reminders (once per day)
            last_rem = str(meta.get("last_daily_reminder_date") or "")
            should_daily = last_rem != today_local
            if should_daily:
                meta["last_daily_reminder_date"] = today_local
                await _save_reporting_store(store)

        # Run daily reminders outside lock (will re-lock for per-member reminder writes)
        if should_daily:
            await _run_daily_cancel_reminders(dm_uid)

        # Weekly report (once per date, only on weekly day)
        if today_wd == weekly_day:
            async with _REPORTING_STORE_LOCK:
                store = _load_reporting_store_sync()
                meta = store.get("meta") if isinstance(store.get("meta"), dict) else {}
                store["meta"] = meta
                last_weekly = str(meta.get("last_weekly_report_date") or "")
                should_weekly = last_weekly != today_local
                if should_weekly:
                    meta["last_weekly_report_date"] = today_local
                    await _save_reporting_store(store)
            if should_weekly:
                await _run_weekly_report(dm_uid, now_local=now_local)
    except Exception as e:
        log.warning(f"[Reporting] reporting_loop error: {e}")


async def _run_weekly_report(dm_uid: int, *, now_local: datetime) -> None:
    # Range: last 7 days ending now (UTC)
    end_utc = now_local.astimezone(timezone.utc)
    start_utc = end_utc - timedelta(days=7)
    e = await _build_report_embed(start_utc, end_utc, title_prefix="RS Weekly Report")
    await _dm_user(dm_uid, embed=e)


async def _build_report_embed(start_utc: datetime, end_utc: datetime, *, title_prefix: str = "RS Report") -> discord.Embed:
    async with _REPORTING_STORE_LOCK:
        store = _load_reporting_store_sync()
        if _report_prune_store:
            store = _report_prune_store(store, retention_weeks=int(REPORTING_CONFIG.get("retention_weeks", 26)))
            await _save_reporting_store(store)

    counts: dict[str, int] = {}
    if _report_week_keys_between and _report_summarize_counts:
        keys = _report_week_keys_between(int(start_utc.timestamp()), int(end_utc.timestamp()))
        async with _REPORTING_STORE_LOCK:
            store = _load_reporting_store_sync()
            counts = _report_summarize_counts(store, keys)

    title = f"{title_prefix} ({start_utc.date().isoformat()} → {end_utc.date().isoformat()})"
    tz_name = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
    e = discord.Embed(
        title=title,
        description=f"Timezone: `{tz_name}` • Deduped per member per reporting period",
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )

    e.add_field(name="New Members", value=str(counts.get("new_members", 0)), inline=False)
    e.add_field(name="New Trials", value=str(counts.get("new_trials", 0)), inline=False)
    e.add_field(name="Payment Failed", value=str(counts.get("payment_failed", 0)), inline=False)
    e.add_field(name="Cancellation Scheduled", value=str(counts.get("cancellation_scheduled", 0)), inline=False)

    e.set_footer(text="RSCheckerbot • Reporting")
    return e


async def _run_daily_cancel_reminders(dm_uid: int) -> None:
    now_local = _tz_now()
    today = now_local.date()
    days = REPORTING_CONFIG.get("reminder_days_before_cancel") or [7, 3, 1]
    try:
        days = [int(x) for x in days]
    except Exception:
        days = [7, 3, 1]

    rows: list[str] = []
    to_mark: list[tuple[int, int]] = []  # (discord_id, day)

    async with _REPORTING_STORE_LOCK:
        store = _load_reporting_store_sync()
        members = store.get("members") if isinstance(store.get("members"), dict) else {}
        for did_s, rec in members.items():
            if not isinstance(rec, dict):
                continue
            did = _to_int(did_s)
            if not did:
                continue
            if rec.get("churned_at"):
                continue
            end_ts = _to_int(rec.get("cancel_scheduled_end_ts"))
            if not end_ts:
                continue
            end_dt_local = datetime.fromtimestamp(int(end_ts), tz=timezone.utc).astimezone(now_local.tzinfo or timezone.utc)
            delta_days = (end_dt_local.date() - today).days
            if delta_days not in days:
                continue
            rem = rec.get("reminders") if isinstance(rec.get("reminders"), dict) else {}
            last = str(rem.get(str(delta_days)) or "")
            if last == today.isoformat():
                continue

            # Build a clickable profile link without pings
            rows.append(f"- <@{did}> ends <t:{int(end_ts)}:D> (in {delta_days}d)")
            to_mark.append((int(did), int(delta_days)))

        # Mark reminders as sent (persist)
        if to_mark:
            for did, day in to_mark:
                rec = members.get(str(did))
                if not isinstance(rec, dict):
                    continue
                rem = rec.get("reminders")
                if not isinstance(rem, dict):
                    rem = {}
                rem[str(day)] = today.isoformat()
                rec["reminders"] = rem
                members[str(did)] = rec
            store["members"] = members
            if _report_save_store:
                _report_save_store(BASE_DIR, store)
            global _REPORTING_STORE
            _REPORTING_STORE = store

    if not rows:
        return

    e = discord.Embed(
        title="Cancellation Reminders",
        description="\n".join(rows)[:4000],
        color=0xFEE75C,
        timestamp=datetime.now(timezone.utc),
    )
    e.set_footer(text="RSCheckerbot • Reporting")
    await _dm_user(dm_uid, embed=e)

    # Optional: mirror reminders into a test channel (no pings).
    try:
        out_gid = int(REPORTING_CONFIG.get("cancel_reminders_output_guild_id") or 0)
    except Exception:
        out_gid = 0
    out_name = str(REPORTING_CONFIG.get("cancel_reminders_output_channel_name") or "").strip()
    if out_gid and out_name:
        g = bot.get_guild(int(out_gid))
        if g:
            ch = _find_text_channel_by_name(g, out_name)
            if ch is None:
                me = g.me or g.get_member(int(getattr(bot.user, "id", 0) or 0))
                if me and getattr(me.guild_permissions, "manage_channels", False):
                    with suppress(Exception):
                        ch = await g.create_text_channel(name=out_name, reason="RSCheckerbot: cancellation reminders mirror")
            if isinstance(ch, discord.TextChannel):
                # Rebuild description without mentions to avoid pings in test server.
                safe_lines = []
                for raw in rows:
                    m = re.search(r"<@(\d{17,19})>", str(raw))
                    if m:
                        did = m.group(1)
                        safe_lines.append(f"- user_id `{did}` ends {raw.split('ends', 1)[-1].strip()}")
                    else:
                        safe_lines.append(str(raw))
                e2 = discord.Embed(
                    title="Cancellation Reminders (mirror)",
                    description="\n".join(safe_lines)[:4000],
                    color=0xFEE75C,
                    timestamp=datetime.now(timezone.utc),
                )
                e2.set_footer(text="RSCheckerbot • Reporting")
                with suppress(Exception):
                    await ch.send(embed=e2, allowed_mentions=discord.AllowedMentions.none())

# -----------------------------
# ENV / CONSTANTS
# -----------------------------
TOKEN = config.get("bot_token")
GUILD_ID = config.get("guild_id")

# Whop API Config
WHOP_API_CONFIG = config.get("whop_api", {})
WHOP_API_KEY = WHOP_API_CONFIG.get("api_key", "")
WHOP_WEBHOOK_SECRET = str(WHOP_API_CONFIG.get("webhook_secret") or WHOP_API_CONFIG.get("webhook_key") or "").strip()
WHOP_WEBHOOK_VERIFY = bool(WHOP_API_CONFIG.get("webhook_verify", True))
try:
    WHOP_WEBHOOK_TOLERANCE_SECONDS = int(WHOP_API_CONFIG.get("webhook_tolerance_seconds") or 300)
except Exception:
    WHOP_WEBHOOK_TOLERANCE_SECONDS = 300

# Support env-configured secret (systemd Environment=WHOP_WEBHOOK_SECRET=...),
# while still allowing config.secrets.json to be the primary source of truth.
if not WHOP_WEBHOOK_SECRET:
    WHOP_WEBHOOK_SECRET = str(os.getenv("WHOP_WEBHOOK_SECRET", "") or "").strip()

# Safety: don't silently reject every webhook due to missing secret.
# If you want verification ON (recommended), set whop_api.webhook_secret in config.secrets.json.
if WHOP_WEBHOOK_VERIFY and not WHOP_WEBHOOK_SECRET:
    log.warning("[WhopWebhook] webhook_verify enabled but webhook_secret is empty; DISABLING verification until configured")
    WHOP_WEBHOOK_VERIFY = False
log.info(
    "[WhopWebhook] verify=%s tolerance_s=%s secret_configured=%s",
    ("on" if WHOP_WEBHOOK_VERIFY else "off"),
    str(WHOP_WEBHOOK_TOLERANCE_SECONDS),
    ("yes" if bool(WHOP_WEBHOOK_SECRET) else "no"),
)
WHOP_EVENTS_FILE = BASE_DIR / "whop_events.jsonl"
WHOP_EVENTS_ENABLED = bool(WHOP_API_CONFIG.get("event_ledger_enabled", True))
try:
    WHOP_EVENTS_DEDUPE_MAX = int(WHOP_API_CONFIG.get("event_dedupe_max") or 2000)
except Exception:
    WHOP_EVENTS_DEDUPE_MAX = 2000
WHOP_EVENTS_DEDUPE_MAX = max(0, WHOP_EVENTS_DEDUPE_MAX)

# Whop movement logs (webhook receipts / detections) to Neo.
try:
    WHOP_MOVEMENT_LOG_ENABLED = bool(WHOP_API_CONFIG.get("movement_log_enabled", True))
except Exception:
    WHOP_MOVEMENT_LOG_ENABLED = True
try:
    WHOP_MOVEMENT_LOG_OUTPUT_GUILD_ID = int(WHOP_API_CONFIG.get("movement_log_output_guild_id") or 0)
except Exception:
    WHOP_MOVEMENT_LOG_OUTPUT_GUILD_ID = 0
try:
    WHOP_MOVEMENT_LOG_OUTPUT_CHANNEL_ID = int(WHOP_API_CONFIG.get("movement_log_output_channel_id") or 0)
except Exception:
    WHOP_MOVEMENT_LOG_OUTPUT_CHANNEL_ID = 0
try:
    WHOP_MOVEMENT_LOG_OUTPUT_CHANNEL_NAME = str(WHOP_API_CONFIG.get("movement_log_output_channel_name") or "").strip()
except Exception:
    WHOP_MOVEMENT_LOG_OUTPUT_CHANNEL_NAME = ""
try:
    WHOP_MOVEMENT_LOG_WEBHOOK_URL = str(WHOP_API_CONFIG.get("movement_log_webhook_url") or "").strip()
except Exception:
    WHOP_MOVEMENT_LOG_WEBHOOK_URL = ""

WHOP_UNLINKED_NOTE = str(
    WHOP_API_CONFIG.get("unlinked_note")
    or "Discord not linked. Ask the member to connect Discord in Whop: Dashboard -> Connected accounts -> Discord."
).strip()

WHOP_NOT_IN_GUILD_NOTE = str(
    WHOP_API_CONFIG.get("not_in_guild_note")
    or "Discord is linked in Whop, but the user is not in this Discord server. Ask them to join the server with the linked Discord account (or rejoin if they left)."
).strip()

# Dispute / resolution per-case channels (main guild only).
try:
    DISPUTE_CASE_CATEGORY_ID = int(WHOP_API_CONFIG.get("dispute_case_category_id") or 0)
except Exception:
    DISPUTE_CASE_CATEGORY_ID = 0
try:
    RESOLUTION_CASE_CATEGORY_ID = int(WHOP_API_CONFIG.get("resolution_case_category_id") or 0)
except Exception:
    RESOLUTION_CASE_CATEGORY_ID = 0
_WHOP_EVENT_DEDUPE_IDS: set[str] = set()
_WHOP_EVENT_DEDUPE_QUEUE: deque[str] = deque()

# Invite Tracking Config
INVITE_CONFIG = config.get("invite_tracking", {})
HTTP_SERVER_PORT = INVITE_CONFIG.get("http_server_port", 8080)
INVITE_CHANNEL_ID = INVITE_CONFIG.get("invite_channel_id")
FALLBACK_INVITE = INVITE_CONFIG.get("fallback_invite", "")
WHOP_LOGS_CHANNEL_ID = INVITE_CONFIG.get("whop_logs_channel_id")
WHOP_WEBHOOK_CHANNEL_ID = INVITE_CONFIG.get("whop_webhook_channel_id")
DISCORD_WEBHOOK_URL = INVITE_CONFIG.get("discord_webhook_url", "")
GHL_API_KEY = INVITE_CONFIG.get("ghl_api_key", "")
GHL_LOCATION_ID = INVITE_CONFIG.get("ghl_location_id", "")
GHL_CF_DISCORD_USERNAME = INVITE_CONFIG.get("ghl_cf_discord_username", "")
GHL_CF_DISCORD_ID = INVITE_CONFIG.get("ghl_cf_discord_id", "")

# DM Sequence Config
DM_CONFIG = config.get("dm_sequence", {})
ROLE_TO_ASSIGN = DM_CONFIG.get("role_to_assign")
ROLE_TRIGGER = DM_CONFIG.get("role_trigger", ROLE_TO_ASSIGN)
WELCOME_ROLE_ID = DM_CONFIG.get("welcome_role_id")  # Role that triggers RSOnboarding ticket creation
ROLE_CANCEL_A = DM_CONFIG.get("role_cancel_a")
ROLE_CANCEL_B = DM_CONFIG.get("role_cancel_b")
FORMER_MEMBER_ROLE = DM_CONFIG.get("former_member_role")
FORMER_MEMBER_DELAY_SECONDS = DM_CONFIG.get("former_member_delay_seconds", 60)
LIFETIME_ROLE_IDS: set[int] = set()
try:
    for _rid in (DM_CONFIG.get("lifetime_role_ids") or []):
        if str(_rid).strip().isdigit():
            LIFETIME_ROLE_IDS.add(int(str(_rid).strip()))
except Exception:
    LIFETIME_ROLE_IDS = set()
ROLES_TO_CHECK = set(DM_CONFIG.get("roles_to_check", []))

# For history/access tracking, only keep numeric role IDs.
ACCESS_ROLE_IDS: set[int] = set()
try:
    for _rid in (ROLES_TO_CHECK or []):
        if str(_rid).strip().isdigit():
            ACCESS_ROLE_IDS.add(int(str(_rid).strip()))
except Exception:
    ACCESS_ROLE_IDS = set()

# History event filtering: only track changes for these role IDs (keeps member_history.json high-signal).
HISTORY_RELEVANT_ROLE_IDS: set[int] = set(ACCESS_ROLE_IDS)
try:
    for _rid in (ROLE_CANCEL_A, ROLE_CANCEL_B, WELCOME_ROLE_ID, ROLE_TRIGGER, FORMER_MEMBER_ROLE):
        if str(_rid).strip().isdigit():
            HISTORY_RELEVANT_ROLE_IDS.add(int(str(_rid).strip()))
except Exception:
    pass
SEND_SPACING_SECONDS = DM_CONFIG.get("send_spacing_seconds", 30.0)
DAY_GAP_HOURS = DM_CONFIG.get("day_gap_hours", 24)
DAY7B_DELAY_MIN = DM_CONFIG.get("day7b_delay_min", 30)
TEST_INTERVAL_SECONDS = DM_CONFIG.get("test_interval_seconds", 10.0)
LOG_FIRST_CHANNEL_ID = DM_CONFIG.get("log_first_channel_id")
LOG_OTHER_CHANNEL_ID = DM_CONFIG.get("log_other_channel_id")
MEMBER_STATUS_LOGS_CHANNEL_ID = DM_CONFIG.get("member_status_logs_channel_id")

# Future Member audit config (Discord-only role audit helper)
FUTURE_MEMBER_AUDIT_CONFIG = config.get("future_member_audit", {}) if isinstance(config, dict) else {}
FUTURE_MEMBER_AUDIT_EXCLUDE_ROLE_IDS: set[int] = set()
try:
    for _rid in (FUTURE_MEMBER_AUDIT_CONFIG.get("exclude_role_ids") or []):
        if str(_rid).strip().isdigit():
            FUTURE_MEMBER_AUDIT_EXCLUDE_ROLE_IDS.add(int(str(_rid).strip()))
except Exception:
    FUTURE_MEMBER_AUDIT_EXCLUDE_ROLE_IDS = set()

# Logging controls (spam reduction + correlation)
LOG_CONTROLS = config.get("log_controls", {}) if isinstance(config, dict) else {}
try:
    BOOT_POST_MIN_HOURS = float(LOG_CONTROLS.get("boot_post_min_hours", 6))
except Exception:
    BOOT_POST_MIN_HOURS = 6.0
try:
    ROLE_UPDATE_BATCH_SECONDS = float(LOG_CONTROLS.get("role_update_batch_seconds", 2))
except Exception:
    ROLE_UPDATE_BATCH_SECONDS = 2.0
try:
    CID_TTL_MINUTES = float(LOG_CONTROLS.get("cid_ttl_minutes", 10))
except Exception:
    CID_TTL_MINUTES = 10.0
VERBOSE_ROLE_LISTS = bool(LOG_CONTROLS.get("verbose_role_lists", False))

# Optional output routing (send staff logs to a different guild, e.g. Neo Test Server).
try:
    OUTPUT_GUILD_ID = int(LOG_CONTROLS.get("output_guild_id") or 0)
except Exception:
    OUTPUT_GUILD_ID = 0
OUTPUT_LOG_OTHER_CHANNEL_NAME = str(LOG_CONTROLS.get("output_log_other_channel_name") or "").strip()
OUTPUT_MEMBER_STATUS_CHANNEL_NAME = str(LOG_CONTROLS.get("output_member_status_channel_name") or "").strip()
OUTPUT_WHOP_CHANNEL_NAME = str(LOG_CONTROLS.get("output_whop_channel_name") or "").strip()

# Member history controls (bounded storage; human-auditable)
MEMBER_HISTORY_CONFIG = config.get("member_history", {}) if isinstance(config, dict) else {}
try:
    MEMBER_HISTORY_MAX_EVENTS_PER_USER = int(MEMBER_HISTORY_CONFIG.get("max_events_per_user", 50))
except Exception:
    MEMBER_HISTORY_MAX_EVENTS_PER_USER = 50
MEMBER_HISTORY_MAX_EVENTS_PER_USER = max(0, MEMBER_HISTORY_MAX_EVENTS_PER_USER)

# Whop enrichment controls (post-then-edit behavior)
WHOP_ENRICHMENT_CONFIG = config.get("whop_enrichment", {}) if isinstance(config, dict) else {}
try:
    WHOP_LINK_TIMEOUT_SECONDS = int(WHOP_ENRICHMENT_CONFIG.get("link_timeout_seconds", 90))
except Exception:
    WHOP_LINK_TIMEOUT_SECONDS = 90
WHOP_LINK_TIMEOUT_SECONDS = max(5, WHOP_LINK_TIMEOUT_SECONDS)
try:
    _retry = WHOP_ENRICHMENT_CONFIG.get("link_retry_seconds", [2, 5, 10, 20, 30, 60])
    WHOP_LINK_RETRY_SECONDS = [int(x) for x in (_retry or []) if int(x) > 0]
except Exception:
    WHOP_LINK_RETRY_SECONDS = [2, 5, 10, 20, 30, 60]
if not WHOP_LINK_RETRY_SECONDS:
    WHOP_LINK_RETRY_SECONDS = [5, 15, 30]
try:
    PAYMENT_RESUMED_RECENT_HOURS = float(WHOP_ENRICHMENT_CONFIG.get("payment_resumed_recent_hours", 6))
except Exception:
    PAYMENT_RESUMED_RECENT_HOURS = 6.0
PAYMENT_RESUMED_RECENT_HOURS = max(0.5, PAYMENT_RESUMED_RECENT_HOURS)

# Native Whop log backfill (Discord message history -> member_history summaries)
try:
    WHOP_NATIVE_BACKFILL_LIMIT = int(WHOP_ENRICHMENT_CONFIG.get("native_backfill_limit", 2000))
except Exception:
    WHOP_NATIVE_BACKFILL_LIMIT = 2000
WHOP_NATIVE_BACKFILL_LIMIT = max(0, WHOP_NATIVE_BACKFILL_LIMIT)
try:
    WHOP_NATIVE_BACKFILL_MAX_DAYS = int(WHOP_ENRICHMENT_CONFIG.get("native_backfill_max_days", 30))
except Exception:
    WHOP_NATIVE_BACKFILL_MAX_DAYS = 30
WHOP_NATIVE_BACKFILL_MAX_DAYS = max(0, WHOP_NATIVE_BACKFILL_MAX_DAYS)

# Startup native Whop log smoke test (post parsed summary of last N cards).
_smoke_enabled_raw = WHOP_ENRICHMENT_CONFIG.get("startup_native_smoketest_enabled", False)
WHOP_STARTUP_NATIVE_SMOKETEST_ENABLED = bool(_smoke_enabled_raw) if isinstance(_smoke_enabled_raw, bool) else (str(_smoke_enabled_raw).strip().lower() in {"1", "true", "yes", "on"})
try:
    WHOP_STARTUP_NATIVE_SMOKETEST_COUNT = int(WHOP_ENRICHMENT_CONFIG.get("startup_native_smoketest_count", 3))
except Exception:
    WHOP_STARTUP_NATIVE_SMOKETEST_COUNT = 3
WHOP_STARTUP_NATIVE_SMOKETEST_COUNT = max(1, min(50, WHOP_STARTUP_NATIVE_SMOKETEST_COUNT))
try:
    WHOP_STARTUP_NATIVE_SMOKETEST_SOURCE_CHANNEL_ID = int(WHOP_ENRICHMENT_CONFIG.get("startup_native_smoketest_source_channel_id") or 0)
except Exception:
    WHOP_STARTUP_NATIVE_SMOKETEST_SOURCE_CHANNEL_ID = 0
try:
    WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_ID = int(WHOP_ENRICHMENT_CONFIG.get("startup_native_smoketest_output_channel_id") or 0)
except Exception:
    WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_ID = 0
try:
    WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_GUILD_ID = int(WHOP_ENRICHMENT_CONFIG.get("startup_native_smoketest_output_guild_id") or 0)
except Exception:
    WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_GUILD_ID = 0
WHOP_STARTUP_NATIVE_SMOKETEST_OUTPUT_CHANNEL_NAME = str(WHOP_ENRICHMENT_CONFIG.get("startup_native_smoketest_output_channel_name") or "").strip()

# Optional: mirror a sample of existing member-status embeds into the smoketest output channel
_smoke_staff_raw = WHOP_ENRICHMENT_CONFIG.get("startup_smoketest_mirror_staff_samples_enabled", False)
WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_ENABLED = (
    bool(_smoke_staff_raw)
    if isinstance(_smoke_staff_raw, bool)
    else (str(_smoke_staff_raw).strip().lower() in {"1", "true", "yes", "on"})
)
try:
    WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_HISTORY_LIMIT = int(
        WHOP_ENRICHMENT_CONFIG.get("startup_smoketest_mirror_staff_samples_history_limit", 200)
    )
except Exception:
    WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_HISTORY_LIMIT = 200
WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_HISTORY_LIMIT = max(
    10, min(1000, WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_HISTORY_LIMIT)
)
try:
    WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_MAX_UNIQUE_TITLES = int(
        WHOP_ENRICHMENT_CONFIG.get("startup_smoketest_mirror_staff_samples_max_unique_titles", 25)
    )
except Exception:
    WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_MAX_UNIQUE_TITLES = 25
WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_MAX_UNIQUE_TITLES = max(
    0, min(100, WHOP_STARTUP_SMOKETEST_MIRROR_STAFF_SAMPLES_MAX_UNIQUE_TITLES)
)

# Files
QUEUE_FILE = BASE_DIR / "queue.json"
REGISTRY_FILE = BASE_DIR / "registry.json"
INVITES_FILE = BASE_DIR / "invites.json"
MESSAGES_FILE = BASE_DIR / "messages.json"
SETTINGS_FILE = BASE_DIR / "settings.json"
MEMBER_HISTORY_FILE = BASE_DIR / "member_history.json"
WHOP_WEBHOOK_RAW_LOG_FILE = BASE_DIR / "whop_webhook_raw_payloads.json"
BOOT_STATE_FILE = BASE_DIR / "boot_state.json"
PAYMENT_CACHE_FILE = BASE_DIR / "payment_cache.json"
WHOP_API_EVENTS_STATE_FILE = BASE_DIR / "whop_api_events_state.json"

# Message order keys
DAY_KEYS = ["day_1", "day_2", "day_3", "day_4", "day_5", "day_6", "day_7a", "day_7b"]

# UTM links
UTM_LINKS = config.get("utm_links", {})

# Load messages from JSON (fallback to Python modules if JSON doesn't exist)
def load_messages():
    """Load messages from JSON file (single source of truth; no Python-module fallback)."""
    if not MESSAGES_FILE.exists():
        raise RuntimeError(f"Missing {MESSAGES_FILE} (required).")
    try:
        with open(MESSAGES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load {MESSAGES_FILE}: {e}") from e

    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid {MESSAGES_FILE}: expected JSON object at top-level.")

    days = data.get("days")
    if not isinstance(days, dict):
        raise RuntimeError(f"Invalid {MESSAGES_FILE}: expected 'days' object.")

    missing = [k for k in DAY_KEYS if k not in days]
    if missing:
        raise RuntimeError(f"Invalid {MESSAGES_FILE}: missing day key(s): {', '.join(missing)}")

    for k in DAY_KEYS:
        day = days.get(k)
        if not isinstance(day, dict):
            raise RuntimeError(f"Invalid {MESSAGES_FILE}: days['{k}'] must be an object.")
        desc = day.get("description", "")
        if not isinstance(desc, str) or not desc.strip():
            raise RuntimeError(f"Invalid {MESSAGES_FILE}: days['{k}'].description must be a non-empty string.")

    return data

messages_data = load_messages()

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------
# Discord client
# -----------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.invites = True

_base_prefix = commands.when_mentioned_or(".checker ")

def _command_prefix(bot_obj: commands.Bot, message: discord.Message):
    # Only allow `!` commands inside support ticket channels.
    prefixes = list(_base_prefix(bot_obj, message))
    try:
        ch_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        if ch_id and support_tickets.is_ticket_channel(ch_id):
            prefixes.append("!")
    except Exception:
        pass
    return prefixes

bot = commands.Bot(command_prefix=_command_prefix, intents=intents)

# Make bot instance accessible for message editor
class RSCheckerBot:
    """Wrapper to make bot instance accessible to message editor"""
    def __init__(self):
        self.config = config
        self.messages = messages_data
    
    def save_messages(self):
        """Save messages to JSON file"""
        global messages_data
        try:
            with open(MESSAGES_FILE, "w", encoding="utf-8") as f:
                json.dump(self.messages, f, indent=2, ensure_ascii=False)
            messages_data = self.messages  # Update global
            log.info("Messages saved to messages.json")
        except Exception as e:
            log.error(f"Failed to save messages: {e}")

bot_instance = RSCheckerBot()

# -----------------------------
# Invite Tracking JSON
# -----------------------------
def load_invites():
    """Load invites from JSON file"""
    invites_path = Path(INVITES_FILE)
    if invites_path.exists():
        try:
            with open(invites_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("invites", {})
        except Exception as e:
            log.error(f"Failed to load invites: {e}")
            return {}
    return {}

def save_invites(invites_data: dict):
    """Save invites to JSON file"""
    invites_path = Path(INVITES_FILE)
    try:
        json_data = {
            "invites": invites_data,
            "last_updated": datetime.now().isoformat()
        }
        with open(invites_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"Failed to save invites: {e}")

# Load invites on startup
invites_data = load_invites()

# -----------------------------
# State
# -----------------------------
queue_state: Dict[str, Dict[str, str]] = {}
registry: Dict[str, Dict[str, str]] = {}
last_send_at: Optional[datetime] = None
pending_checks: set[int] = set()
pending_former_checks: set[int] = set()
invite_tracking: Dict[str, str] = {}  # invite_code -> lead_id
invite_usage_cache: Dict[str, int] = {}  # invite_code -> last known uses

# Whop API client (initialized from config)
whop_api_client = None

# Correlation IDs (short-lived, for searchability across related events)
cid_cache: Dict[int, Dict[str, str]] = {}  # user_id -> {"cid": str, "expires_at": iso}

# Role update batching (reduce spam from rapid add/remove sequences)
pending_role_updates: Dict[int, Dict[str, object]] = {}  # user_id -> {"added": set[int], "removed": set[int], "member": discord.Member, "task": asyncio.Task}

# Suppress member-update log spam for known automated role changes (startup/sync).
# user_id -> monotonic expiry timestamp
_member_update_log_suppress: Dict[int, float] = {}

def _suppress_member_update_logs(user_id: int, *, seconds: float = 180.0) -> None:
    try:
        uid = int(user_id)
    except Exception:
        return
    try:
        ttl = float(seconds)
    except Exception:
        ttl = 180.0
    ttl = max(5.0, ttl)
    _member_update_log_suppress[uid] = time.monotonic() + ttl

def _member_update_logs_suppressed(user_id: int) -> bool:
    try:
        uid = int(user_id)
    except Exception:
        return False
    exp = _member_update_log_suppress.get(uid)
    if not exp:
        return False
    try:
        if time.monotonic() < float(exp):
            return True
    except Exception:
        return False
    with suppress(Exception):
        _member_update_log_suppress.pop(uid, None)
    return False

# -----------------------------
# Utils: persistence/time
# -----------------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)

def save_all():
    save_json(QUEUE_FILE, queue_state)
    save_json(REGISTRY_FILE, registry)

def _cid_for(user_id: int) -> str:
    """Return a short-lived correlation ID for a user (helps support search one token)."""
    now = _now()
    rec = cid_cache.get(int(user_id))
    if rec:
        try:
            exp = datetime.fromisoformat(str(rec.get("expires_at", "")).replace("Z", "+00:00"))
            if exp > now and rec.get("cid"):
                return str(rec["cid"])
        except Exception:
            pass
    cid = f"CID-{int(user_id)}-{now.strftime('%m%d%H%M%S')}"
    cid_cache[int(user_id)] = {"cid": cid, "expires_at": (now + timedelta(minutes=CID_TTL_MINUTES)).isoformat()}
    return cid

def _should_post_boot() -> bool:
    """Throttle noisy boot posts to Discord."""
    try:
        if BOOT_POST_MIN_HOURS <= 0:
            return True
        state = load_json(BOOT_STATE_FILE)
        last_iso = str(state.get("last_boot_post_iso") or "").strip()
        if last_iso:
            try:
                last_dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
                if (_now() - last_dt) < timedelta(hours=BOOT_POST_MIN_HOURS):
                    return False
            except Exception:
                pass
        state["last_boot_post_iso"] = _now().isoformat()
        save_json(BOOT_STATE_FILE, state)
        return True
    except Exception:
        return True

def _should_post_sync_report() -> bool:
    """Throttle sync DM reports to avoid spam."""
    try:
        if BOOT_POST_MIN_HOURS <= 0:
            return True
        state = load_json(BOOT_STATE_FILE)
        last_iso = str(state.get("last_sync_report_iso") or "").strip()
        if last_iso:
            try:
                last_dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
                if (_now() - last_dt) < timedelta(hours=BOOT_POST_MIN_HOURS):
                    return False
            except Exception:
                pass
        state["last_sync_report_iso"] = _now().isoformat()
        save_json(BOOT_STATE_FILE, state)
        return True
    except Exception:
        return True

async def _flush_role_update(user_id: int) -> None:
    """Flush a batched role update after a short debounce window."""
    await asyncio.sleep(max(0.2, ROLE_UPDATE_BATCH_SECONDS))
    rec = pending_role_updates.pop(int(user_id), None)
    if not rec:
        return
    member = rec.get("member")
    if not isinstance(member, discord.Member):
        return
    added: set[int] = rec.get("added") or set()
    removed: set[int] = rec.get("removed") or set()
    if not added and not removed:
        return

    cid = _cid_for(member.id)

    def _role_name(rid: int) -> str:
        role = member.guild.get_role(int(rid)) if member.guild else None
        return str(role.name) if role else str(rid)

    added_list = [_role_name(rid) for rid in sorted(set(added))] if added else []
    removed_list = [_role_name(rid) for rid in sorted(set(removed))] if removed else []

    if len(added_list) == 1 and not removed_list:
        desc = f"{member.mention} was given the {added_list[0]} role"
    elif len(removed_list) == 1 and not added_list:
        desc = f"{member.mention} was removed from the {removed_list[0]} role"
    else:
        desc = f"{member.mention} roles updated"

    e = _make_dyno_embed(
        member=member,
        description=desc,
        footer=f"ID: {member.id} • CID: {cid}",
    )
    if removed_list and (len(removed_list) > 1 or added_list):
        e.add_field(name="Removed", value=(", ".join(removed_list)[:1024] or "—"), inline=False)
    if added_list and (len(added_list) > 1 or removed_list):
        e.add_field(name="Added", value=(", ".join(added_list)[:1024] or "—"), inline=False)
    await log_role_event(embed=e)

def _save_raw_webhook_payload(payload: dict, headers: dict = None):
    """Save raw webhook payload to JSON file for inspection"""
    try:
        # Load existing payloads
        if WHOP_WEBHOOK_RAW_LOG_FILE.exists():
            with open(WHOP_WEBHOOK_RAW_LOG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {"payloads": []}
        
        # Add new payload with timestamp
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
            "headers": dict(headers) if headers else {}
        }
        data["payloads"].append(entry)
        
        # Keep last 1000 entries to avoid file bloat
        data["payloads"] = data["payloads"][-1000:]
        
        # Save to file
        with open(WHOP_WEBHOOK_RAW_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved raw webhook payload to {WHOP_WEBHOOK_RAW_LOG_FILE.name}")
    except Exception as e:
        log.error(f"Failed to save raw webhook payload: {e}")

# -----------------------------
# Member History Helpers
# -----------------------------
def _load_member_history() -> dict:
    """Load member history from JSON file"""
    try:
        if not MEMBER_HISTORY_FILE.exists() or MEMBER_HISTORY_FILE.stat().st_size == 0:
            return {}
        return json.loads(MEMBER_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_member_history(db: dict) -> None:
    """Save member history to JSON file"""
    try:
        if not isinstance(db, dict):
            return
        # Atomic write (tmp + replace) to avoid truncated JSON on crash.
        save_json(MEMBER_HISTORY_FILE, db)
    except Exception:
        pass


STAFF_ALERTS_FILE = BASE_DIR / "staff_alerts.json"

def _history_now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def _history_access_role_ids(member: discord.Member | None) -> list[int]:
    """Access role IDs present on a member (based on dm_sequence.roles_to_check)."""
    if not isinstance(member, discord.Member):
        return []
    try:
        role_ids = {int(r.id) for r in (member.roles or [])}
        return sorted(role_ids.intersection(ACCESS_ROLE_IDS))
    except Exception:
        return []

def _history_role_snapshot(member: discord.Member | None) -> tuple[list[int], list[str]]:
    """Best-effort role snapshot (excluding @everyone)."""
    if not isinstance(member, discord.Member):
        return ([], [])
    ids: list[int] = []
    names: list[str] = []
    try:
        for r in (member.roles or []):
            try:
                if getattr(r, "is_default", None) and r.is_default():
                    continue
            except Exception:
                # Fallback: @everyone is typically guild id
                if member.guild and int(r.id) == int(member.guild.id):
                    continue
            ids.append(int(r.id))
            names.append(str(r.name))
    except Exception:
        return ([], [])
    return (ids, names)

def _history_identity_snapshot(member: discord.Member | None) -> dict:
    if not isinstance(member, discord.Member):
        return {}
    try:
        return {
            "last_known_username": str(getattr(member, "name", "") or "").strip(),
            "last_known_display_name": str(getattr(member, "display_name", "") or "").strip(),
            "last_known_discriminator": str(getattr(member, "discriminator", "") or "").strip(),
        }
    except Exception:
        return {}

def _ensure_member_history_shape(rec: dict, *, now: int) -> dict:
    """Normalize a per-user history record in-place (non-destructive)."""
    if not isinstance(rec, dict):
        rec = {}

    # Legacy fields (keep for backwards compatibility)
    if "first_join_ts" not in rec:
        rec["first_join_ts"] = None
    if "last_join_ts" not in rec:
        rec["last_join_ts"] = None
    if "last_leave_ts" not in rec:
        rec["last_leave_ts"] = None
    try:
        rec["join_count"] = int(rec.get("join_count", 0) or 0)
    except Exception:
        rec["join_count"] = 0

    # New structured fields
    if not isinstance(rec.get("identity"), dict):
        rec["identity"] = {}
    if not isinstance(rec.get("discord"), dict):
        rec["discord"] = {}
    if not isinstance(rec.get("access"), dict):
        rec["access"] = {}
    if not isinstance(rec.get("events"), list):
        rec["events"] = []

    # Defaults for access tracking
    acc = rec["access"]
    if "ever_had_access_role" not in acc:
        acc["ever_had_access_role"] = False
    if "first_access_ts" not in acc:
        acc["first_access_ts"] = None
    if "last_access_ts" not in acc:
        acc["last_access_ts"] = None
    if "ever_had_member_role" not in acc:
        acc["ever_had_member_role"] = False
    if "first_member_role_ts" not in acc:
        acc["first_member_role_ts"] = None
    if "last_member_role_ts" not in acc:
        acc["last_member_role_ts"] = None

    # Keep a compact last snapshot
    disc = rec["discord"]
    if "last_snapshot_ts" not in disc:
        disc["last_snapshot_ts"] = None
    if "last_roles" not in disc:
        disc["last_roles"] = []
    if "last_role_names" not in disc:
        disc["last_role_names"] = []

    return rec

def _history_update_identity_and_snapshot(rec: dict, *, member: discord.Member | None, now: int) -> None:
    if not isinstance(rec, dict):
        return
    rec = _ensure_member_history_shape(rec, now=now)
    ident = rec.get("identity") if isinstance(rec.get("identity"), dict) else {}
    snap = _history_identity_snapshot(member)
    if snap:
        ident.update({k: v for k, v in snap.items() if v})
        ident["updated_at"] = datetime.now(timezone.utc).isoformat()
        rec["identity"] = ident

    if isinstance(member, discord.Member):
        role_ids, role_names = _history_role_snapshot(member)
        disc = rec.get("discord") if isinstance(rec.get("discord"), dict) else {}
        disc["last_snapshot_ts"] = now
        disc["last_roles"] = role_ids
        disc["last_role_names"] = role_names
        rec["discord"] = disc

        # Update access timelines (roles_to_check) and member role timelines (ROLE_CANCEL_A)
        access_ids = _history_access_role_ids(member)
        acc = rec.get("access") if isinstance(rec.get("access"), dict) else {}
        if access_ids:
            acc["ever_had_access_role"] = True
            if not acc.get("first_access_ts"):
                acc["first_access_ts"] = now
            acc["last_access_ts"] = now
        try:
            has_member_role = bool(ROLE_CANCEL_A and int(ROLE_CANCEL_A) in {r.id for r in (member.roles or [])})
        except Exception:
            has_member_role = False
        if has_member_role:
            acc["ever_had_member_role"] = True
            if not acc.get("first_member_role_ts"):
                acc["first_member_role_ts"] = now
            acc["last_member_role_ts"] = now
        rec["access"] = acc

def _history_append_event(
    rec: dict,
    *,
    kind: str,
    now: int,
    roles_added: list[int] | None = None,
    roles_removed: list[int] | None = None,
    note: str = "",
    max_events: int | None = None,
    access_role_ids: list[int] | None = None,
) -> None:
    if not isinstance(rec, dict):
        return
    rec = _ensure_member_history_shape(rec, now=now)
    events = rec.get("events")
    if not isinstance(events, list):
        events = []

    ev = {
        "ts": int(now),
        "kind": str(kind or "").strip().lower() or "event",
        "roles_added": sorted({int(x) for x in (roles_added or []) if str(x).strip().isdigit()}),
        "roles_removed": sorted({int(x) for x in (roles_removed or []) if str(x).strip().isdigit()}),
        "access_roles": sorted({int(x) for x in (access_role_ids or []) if str(x).strip().isdigit()}),
        "note": str(note or "").strip(),
    }

    # Deduplicate immediate duplicates (role updates can fire rapidly)
    try:
        if events:
            last = events[-1] if isinstance(events[-1], dict) else {}
            if (
                isinstance(last, dict)
                and last.get("kind") == ev["kind"]
                and last.get("roles_added") == ev["roles_added"]
                and last.get("roles_removed") == ev["roles_removed"]
                and abs(int(ev["ts"]) - int(last.get("ts") or 0)) <= 2
            ):
                rec["events"] = events
                return
    except Exception:
        pass

    events.append(ev)
    cap = MEMBER_HISTORY_MAX_EVENTS_PER_USER if max_events is None else int(max_events)
    cap = max(0, cap)
    if cap and len(events) > cap:
        events = events[-cap:]
    rec["events"] = events

def _touch_join(discord_id: int, member: discord.Member | None = None) -> dict:
    """Record member join event, return history record (in-place; bounded)."""
    now = _history_now_ts()
    db = _load_member_history()
    key = str(discord_id)
    rec = db.get(key, {})
    rec = _ensure_member_history_shape(rec, now=now)

    rec["last_join_ts"] = now
    rec["join_count"] = int(rec.get("join_count", 0) or 0) + 1
    if rec.get("first_join_ts") is None:
        rec["first_join_ts"] = now

    _history_update_identity_and_snapshot(rec, member=member, now=now)
    _history_append_event(rec, kind="join", now=now, access_role_ids=_history_access_role_ids(member))

    db[key] = rec
    _save_member_history(db)
    return rec

def _touch_leave(discord_id: int, member: discord.Member | None = None) -> dict:
    """Record member leave event, return history record (in-place; bounded)."""
    now = _history_now_ts()
    db = _load_member_history()
    key = str(discord_id)
    rec = db.get(key, {})
    rec = _ensure_member_history_shape(rec, now=now)

    # User left but we never tracked a join (edge case): keep legacy fields as None.
    rec["last_leave_ts"] = now

    _history_update_identity_and_snapshot(rec, member=member, now=now)
    _history_append_event(rec, kind="leave", now=now, access_role_ids=_history_access_role_ids(member))

    db[key] = rec
    _save_member_history(db)
    return rec

def _touch_role_change(
    member: discord.Member,
    *,
    roles_added: set[int] | None = None,
    roles_removed: set[int] | None = None,
    note: str = "",
) -> dict:
    """Record a role change event (filtered by caller); updates access timelines."""
    now = _history_now_ts()
    db = _load_member_history()
    key = str(int(member.id))
    rec = db.get(key, {})
    rec = _ensure_member_history_shape(rec, now=now)

    _history_update_identity_and_snapshot(rec, member=member, now=now)
    _history_append_event(
        rec,
        kind="role_change",
        now=now,
        roles_added=sorted({int(x) for x in (roles_added or set())}),
        roles_removed=sorted({int(x) for x in (roles_removed or set())}),
        note=note,
        access_role_ids=_history_access_role_ids(member),
    )

    db[key] = rec
    _save_member_history(db)
    return rec

def _fmt_ts(ts: int | None, style: str = "D") -> str:
    """Format timestamp as Discord timestamp (human-readable)
    
    Styles:
    - 'D' = Short date: Aug 11, 2025
    - 'F' = Full date: August 11, 2025 4:40 PM
    - 'R' = Relative: 3 months ago
    """
    if not ts:
        return "—"
    try:
        return f"<t:{int(ts)}:{style}>"
    except Exception:
        return "—"


def _fmt_discord_ts_any(ts_str: str | int | float | None, style: str = "D") -> str:
    """Format ISO or unix timestamp into a Discord timestamp string."""
    if ts_str is None or ts_str == "":
        return "—"
    try:
        s = str(ts_str).strip()
        if not s:
            return "—"
        # ISO-ish path
        if "T" in s or "-" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return f"<t:{int(dt.timestamp())}:{style}>"
        return f"<t:{int(float(s))}:{style}>"
    except Exception:
        return "—"

def _has_lifetime_role(member: discord.Member) -> bool:
    """True if member has any configured lifetime role IDs."""
    if not LIFETIME_ROLE_IDS:
        return False
    try:
        role_ids = {r.id for r in (member.roles or [])}
        return bool(role_ids.intersection(LIFETIME_ROLE_IDS))
    except Exception:
        return False

def _access_end_dt_from_membership(membership_data: dict | None) -> datetime | None:
    """Compute the best-effort access end datetime for a Whop membership object.

    Primary source (matches staff embeds): renewal_period_end.
    """
    if not isinstance(membership_data, dict):
        return None
    return _parse_dt_any(
        membership_data.get("renewal_period_end")
        or membership_data.get("access_ends_at")
        or membership_data.get("trial_end")
        or membership_data.get("trial_ends_at")
        or membership_data.get("trial_end_at")
    )


async def _fetch_whop_brief_by_membership_id(membership_id: str) -> dict:
    """Fetch a minimal Whop summary for staff (no internal IDs)."""
    global whop_api_client
    mid = (membership_id or "").strip()
    if not mid:
        return {}
    brief = await fetch_whop_brief(
        whop_api_client,
        mid,
        enable_enrichment=bool(WHOP_API_CONFIG.get("enable_enrichment", True)),
    )
    if not isinstance(brief, dict) or not brief:
        return {}

    # IMPORTANT: Whop Company API does not expose connected accounts (Discord) in /members or /users.
    # The Whop dashboard UI shows this, but the API payloads do not.
    #
    # We therefore treat the native Whop log cards in `whop-logs` as source-of-truth for Discord ID
    # and opportunistically enrich `connected_discord` for *all* staff card paths.
    try:
        connected_disp = str(brief.get("connected_discord") or "").strip()
    except Exception:
        connected_disp = ""
    if not connected_disp:
        try:
            email_hint = str(brief.get("email") or "").strip()
        except Exception:
            email_hint = ""
        if email_hint:
            # 1) Fast path: local identity cache (no Discord API calls).
            try:
                email_n = str(email_hint).strip().lower()
            except Exception:
                email_n = ""
            if email_n:
                with suppress(Exception):
                    db = load_json(WHOP_IDENTITY_CACHE_FILE)
                    if isinstance(db, dict):
                        rec = db.get(email_n) if isinstance(db.get(email_n), dict) else None
                        did_s = str((rec or {}).get("discord_id") or "").strip() if isinstance(rec, dict) else ""
                        if did_s.isdigit():
                            brief["connected_discord"] = did_s
                            return brief

            try:
                lim = int(WHOP_API_CONFIG.get("logs_lookup_limit") or 50)
            except Exception:
                lim = 50
            lim = max(10, min(lim, 250))
            with suppress(Exception):
                # Prefer the guild that actually contains the configured whop-logs channel.
                g: discord.Guild | None = None
                for gid0 in (int(GUILD_ID or 0), int(OUTPUT_GUILD_ID or 0)):
                    if not gid0:
                        continue
                    gg = bot.get_guild(int(gid0))
                    if not isinstance(gg, discord.Guild):
                        continue
                    try:
                        ch0 = gg.get_channel(int(WHOP_LOGS_CHANNEL_ID or 0)) if WHOP_LOGS_CHANNEL_ID else None
                    except Exception:
                        ch0 = None
                    if isinstance(ch0, discord.TextChannel):
                        g = gg
                        break
                if g is None:
                    g = bot.get_guild(int(GUILD_ID)) if int(GUILD_ID or 0) else None
                resolved = await _resolve_discord_id_from_whop_logs(
                    g,
                    email=email_hint,
                    membership_id_hint=mid,
                    whop_key="",
                    limit=lim,
                )
                if str(resolved or "").strip().isdigit():
                    brief["connected_discord"] = str(resolved).strip()

    return brief


def _whop_placeholder_brief(state: str) -> dict:
    """Return a placeholder Whop brief with consistent keys for staff embeds."""
    st = str(state or "").strip().lower()
    # Never surface user-visible placeholders like "Not linked yet" / "Linking…".
    # Unknown = "—" (staff-friendly, consistent across cards).
    dash = "—"
    spent = "—"
    status = "—"
    return {
        "status": status,
        "product": "—",
        "member_since": "—",
        "renewal_start": "—",
        "renewal_end": "—",
        "remaining_days": "—",
        "dashboard_url": dash,
        "manage_url": "",
        "total_spent": spent,
        "last_success_paid_at": "—",
        "last_payment_failure": "",
        "cancel_at_period_end": "—",
        "is_first_membership": "—",
        "last_payment_method": "—",
        "last_payment_type": "—",
        # Internal IDs (unused in embeds)
        "whop_user_id": "",
        "whop_member_id": "",
        "last_success_paid_at_iso": "",
        "renewal_end_iso": "",
    }


async def _resolve_whop_brief_for_discord_id(discord_id: int) -> tuple[str, dict]:
    """Resolve (membership_id, whop_brief) for a Discord ID (best-effort)."""
    # Prefer the last webhook-derived summary from member_history.
    summary = _whop_summary_for_member(int(discord_id))
    if isinstance(summary, dict) and summary:
        return ("", summary)
    # Fallback: use the last membership_id from member_history for API lookup.
    mid = str(_membership_id_from_history(int(discord_id)) or "").strip()
    if not mid:
        return ("", {})
    brief = await _fetch_whop_brief_by_membership_id(mid)
    return (mid, brief if isinstance(brief, dict) else {})


async def _edit_staff_message(msg: discord.Message, *, embed: discord.Embed) -> None:
    """Best-effort edit (user mentions allowed; no role/everyone)."""
    if not msg:
        return
    try:
        await msg.edit(embed=embed, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))
    except TypeError:
        # Some discord.py versions don't accept allowed_mentions on edit.
        with suppress(Exception):
            await msg.edit(embed=embed)
    except Exception:
        return


async def _retry_whop_enrich_and_edit(
    *,
    discord_id: int,
    messages: list[discord.Message],
    make_embed,
    make_fallback_embed,
    timeout_seconds: int,
    retry_seconds: list[int],
) -> None:
    """Retry Whop enrichment for a short window, then edit messages."""
    start = _now()
    # Try immediately
    _mid, brief = await _resolve_whop_brief_for_discord_id(discord_id)
    if isinstance(brief, dict) and brief:
        e = make_embed(brief)
        for m in (messages or []):
            await _edit_staff_message(m, embed=e)
        return

    for d in (retry_seconds or []):
        if (_now() - start).total_seconds() >= float(timeout_seconds or 0):
            break
        await asyncio.sleep(max(1, int(d)))
        _mid2, brief2 = await _resolve_whop_brief_for_discord_id(discord_id)
        if isinstance(brief2, dict) and brief2:
            e2 = make_embed(brief2)
            for m in (messages or []):
                await _edit_staff_message(m, embed=e2)
            return

    # Timeout: mark as unlinked
    ef = make_fallback_embed()
    for m in (messages or []):
        await _edit_staff_message(m, embed=ef)

def get_member_history(discord_id: int) -> dict:
    """Get member history record (exposed for whop_webhook_handler and support cards)"""
    db = _load_member_history()
    return db.get(str(discord_id), {})


def record_member_whop_summary(
    discord_id: int,
    summary: dict,
    *,
    event_type: str = "",
    membership_id: str = "",
    whop_key: str = "",
) -> None:
    """Persist a staff-safe Whop summary into member_history (no PII)."""
    try:
        did = int(discord_id)
    except Exception:
        return
    try:
        now = _history_now_ts()
        db = _load_member_history()
        key = str(did)
        rec = db.get(key, {})
        rec = _ensure_member_history_shape(rec, now=now)
        wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
        if isinstance(summary, dict) and summary:
            # Ensure we never persist PII like email/name into member_history.json.
            safe = dict(summary)
            for k in (
                "email",
                "user_name",
                "name",
                "username",
                "whop_user_id",
                "whop_member_id",
                "manage_url",
            ):
                with suppress(Exception):
                    safe.pop(k, None)
            wh["last_summary"] = safe
            st = str(summary.get("status") or "").strip().lower()
            if st:
                wh["last_status"] = st
        if event_type:
            wh["last_event_type"] = str(event_type).strip()
        wh["last_event_ts"] = int(now)
        if membership_id and str(membership_id).strip().startswith(("mem_", "R-")):
            wh["last_membership_id"] = str(membership_id).strip()
        if whop_key and str(whop_key).strip().startswith(("mem_", "R-")):
            wh["last_whop_key"] = str(whop_key).strip()
        rec["whop"] = wh
        db[key] = rec
        _save_member_history(db)
    except Exception:
        return


def _whop_summary_for_member(discord_id: int) -> dict:
    try:
        hist = get_member_history(int(discord_id)) or {}
        wh = hist.get("whop") if isinstance(hist, dict) else None
        if isinstance(wh, dict) and isinstance(wh.get("last_summary"), dict):
            return wh.get("last_summary") or {}
        # If we don't have a last_summary but we do have a membership_id,
        # fetch the most recent parsed native Whop summary by membership_id.
        mid = _membership_id_from_history(int(discord_id))
        if mid:
            cached = _get_native_summary_by_mid(mid)
            if isinstance(cached, dict) and cached:
                return cached
        # Fallback: if we only have a timeline/status (from whop_history backfill),
        # expose at least status so staff cards aren't blank.
        if isinstance(wh, dict):
            st = str(wh.get("last_status") or "").strip().lower()
            if st:
                return {"status": st}
    except Exception:
        return {}
    return {}


def _membership_id_from_history(discord_id: int) -> str:
    try:
        hist = get_member_history(int(discord_id)) or {}
        wh = hist.get("whop") if isinstance(hist, dict) else None
        if isinstance(wh, dict):
            mid = str(wh.get("last_membership_id") or wh.get("last_whop_key") or "").strip()
            if mid.startswith(("mem_", "R-")):
                return mid
    except Exception:
        return ""
    return ""

def _backfill_whop_timeline_from_whop_history() -> None:
    """Backfill Whop lifecycle timeline from whop_history.json.
    
    Stores Whop events in member_history[discord_id]["whop"] sub-object.
    Non-destructive: only adds/updates "whop" timeline, never overwrites Discord join/leave fields.
    """
    try:
        # Load path from config
        default_path = BASE_DIR.parent / "RSAdminBot" / "whop_data" / "whop_history.json"
        whop_history_path = default_path
        
        try:
            config_path = BASE_DIR / "config.json"
            if config_path.exists():
                with open(config_path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                custom_path = config_data.get("paths", {}).get("whop_history")
                if custom_path:
                    whop_history_path = (BASE_DIR / custom_path).resolve()
        except Exception:
            pass  # Use default path if config loading fails
        
        if not whop_history_path.exists():
            log.info("whop_history.json not found, skipping Whop timeline backfill")
            return
        
        # Load whop_history.json
        try:
            with open(whop_history_path, "r", encoding="utf-8") as f:
                whop_history = json.load(f)
        except Exception as e:
            log.warning(f"Failed to load whop_history.json: {e}")
            return
        
        events = whop_history.get("membership_events", [])
        if not events:
            log.info("No membership events in whop_history.json")
            return
        
        # Load existing member_history
        member_history = _load_member_history()
        backfilled_count = 0
        
        for event in events:
            discord_id_str = event.get("discord_id", "").strip()
            if not discord_id_str:
                continue
            
            try:
                discord_id = int(discord_id_str)
            except (ValueError, TypeError):
                continue
            
            # Parse timestamp
            timestamp_str = event.get("timestamp") or event.get("created_at")
            if not timestamp_str:
                continue
            
            try:
                # Parse ISO timestamp to Unix timestamp
                if "T" in str(timestamp_str):
                    dt = datetime.fromisoformat(str(timestamp_str).replace("Z", "+00:00"))
                    event_ts = int(dt.timestamp())
                else:
                    event_ts = int(float(str(timestamp_str)))
            except (ValueError, TypeError, AttributeError):
                continue
            
            # Get or create member record
            key = str(discord_id)
            rec = member_history.get(key, {})
            # Ensure join/leave/access fields always exist, even when this record is created via Whop backfill.
            rec = _ensure_member_history_shape(rec, now=event_ts)

            # Get or create whop timeline sub-object
            if not isinstance(rec.get("whop"), dict):
                rec["whop"] = {}

            member_history[key] = rec
            whop_timeline = rec["whop"]
            
            # Get status (normalize to lowercase)
            status = (event.get("membership_status", "") or "").strip().lower()
            event_type = (event.get("event_type", "") or "").strip().lower()
            
            # Update timeline fields
            # first_seen_ts: min of all timestamps (only set if missing or new event is earlier)
            if "first_seen_ts" not in whop_timeline or whop_timeline["first_seen_ts"] is None or event_ts < whop_timeline["first_seen_ts"]:
                whop_timeline["first_seen_ts"] = event_ts
            
            # last_seen_ts: max of all timestamps
            if "last_seen_ts" not in whop_timeline or whop_timeline["last_seen_ts"] is None or event_ts > whop_timeline["last_seen_ts"]:
                whop_timeline["last_seen_ts"] = event_ts
            
            # last_active_ts: max timestamp where status is "active" or "trialing"
            if status in ("active", "trialing"):
                if "last_active_ts" not in whop_timeline or whop_timeline["last_active_ts"] is None or event_ts > whop_timeline["last_active_ts"]:
                    whop_timeline["last_active_ts"] = event_ts
            
            # last_canceled_ts: max timestamp where status is "canceled" or event_type is "cancellation"
            if status == "canceled" or event_type == "cancellation":
                if "last_canceled_ts" not in whop_timeline or whop_timeline["last_canceled_ts"] is None or event_ts > whop_timeline["last_canceled_ts"]:
                    whop_timeline["last_canceled_ts"] = event_ts
            
            # last_status: most recent status (normalize to lowercase)
            if status:
                whop_timeline["last_status"] = status
            
            # Membership identifiers:
            # - Whop history/workflows often include a humanish "R-..." key (commonly stored as whop_key).
            # - Some payloads include an API-style "mem_..." id as membership_id.
            # Both have been observed to work with /memberships/{id}, so keep both if available.
            whop_key = str(event.get("whop_key") or "").strip()
            if whop_key:
                whop_timeline["last_whop_key"] = whop_key
                if whop_key.startswith(("mem_", "R-")):
                    whop_timeline["last_membership_id"] = whop_key

            membership_id = str(event.get("membership_id") or "").strip()
            if membership_id.startswith(("mem_", "R-")):
                whop_timeline["last_membership_id"] = membership_id
            
            # last_user_id: from event if available (though whop_history.json doesn't seem to have this)
            # Keeping for future compatibility
            
            backfilled_count += 1
        
        # Save merged history (non-destructive: only whop sub-object was modified)
        _save_member_history(member_history)
        
        log.info(f"Whop timeline backfill complete: {backfilled_count} events processed, {len([k for k, v in member_history.items() if v.get('whop')])} members with Whop timeline")
    except Exception as e:
        log.error(f"Whop timeline backfill failed: {e}", exc_info=True)

def _access_roles_plain(member: discord.Member) -> str:
    """Return a compact list of access-relevant role names (no mentions).

    This intentionally filters out "noise" roles so support can quickly see access state.
    """
    relevant_ids = coerce_role_ids(ROLE_CANCEL_A, ROLE_CANCEL_B, WELCOME_ROLE_ID, ROLE_TRIGGER, FORMER_MEMBER_ROLE)
    try:
        relevant_ids.update({int(x) for x in ROLES_TO_CHECK if str(x).strip().isdigit()})
    except Exception:
        pass
    return access_roles_plain(member, relevant_ids)

def load_settings() -> dict:
    """Load settings from JSON file.

    IMPORTANT: Default to DISABLED if missing/bad to avoid DM sequence re-enabling
    after restarts when settings.json is absent/corrupt on the server.
    """
    if not SETTINGS_FILE.exists():
        return {"dm_sequence_enabled": False}
    try:
        data = load_json(SETTINGS_FILE)
        return {"dm_sequence_enabled": data.get("dm_sequence_enabled", False)}
    except Exception:
        return {"dm_sequence_enabled": False}

def save_settings(settings: dict) -> None:
    """Save settings to JSON file"""
    save_json(SETTINGS_FILE, settings)

# -----------------------------
# Logging to Discord channels
# -----------------------------
def _fmt_user(member: discord.abc.User) -> str:
    # Keep it Dyno-like: simple display + ID (no heavy markdown).
    return f"{member.display_name} ({member.id})"

def _fmt_role(role_id: int, guild: discord.Guild) -> str:
    """Format role as name (ID) or just ID if not found."""
    role = guild.get_role(role_id) if guild else None
    if role:
        return f"**{role.name}** (`{role_id}`)"
    return f"`{role_id}`"

def _fmt_role_list(role_ids: set, guild: discord.Guild) -> str:
    """Format list of roles as names. Excludes @everyone role to prevent mentions."""
    roles = []
    for rid in role_ids:
        # Filter out @everyone role (ID equals guild ID) to prevent mentions
        if guild and rid == guild.id:
            continue
        role = guild.get_role(rid) if guild else None
        if role:
            roles.append(str(role.name))
        else:
            roles.append(f"`{rid}`")
    return ", ".join(roles) if roles else "—"

def m_user(member: discord.Member) -> str:
    """Format member as mentionable user (@user)"""
    return member.mention

def m_channel(channel: discord.abc.GuildChannel) -> str:
    """Format channel as mentionable (#channel)"""
    return channel.mention

def t_role(role_id: int, guild: discord.Guild) -> str:
    """Format role as plain text (no mention) - alias for _fmt_role for clarity"""
    return _fmt_role(role_id, guild)

def _make_dyno_embed(
    *,
    member: discord.abc.User | None,
    description: str,
    footer: str = "",
    color: int = 0x5865F2,
    timestamp: datetime | None = None,
) -> discord.Embed:
    """Build a compact, Dyno-like embed for log channels."""
    e = discord.Embed(
        description=str(description or "").strip() or "—",
        color=int(color) if isinstance(color, int) else 0x5865F2,
        timestamp=timestamp or datetime.now(timezone.utc),
    )
    if member is not None:
        with suppress(Exception):
            _apply_member_header(e, member)
    if footer:
        with suppress(Exception):
            e.set_footer(text=str(footer)[:2048])
    return e

async def log_first(msg: str | None = None, *, embed: discord.Embed | None = None):
    ch = bot.get_channel(LOG_FIRST_CHANNEL_ID)
    if ch:
        with suppress(Exception):
            e = embed
            if e is None:
                e = discord.Embed(
                    description=str(msg or "").strip() or "—",
                    color=0x5865F2,
                    timestamp=datetime.now(timezone.utc),
                )
                # Prefer the runtime channel name (no hardcoded labels).
                nm = str(getattr(ch, "name", "") or "").strip()
                e.set_footer(text=f"RSCheckerbot • {nm}" if nm else "RSCheckerbot")
            await ch.send(embed=e, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))

async def log_other(msg: str | None = None, *, embed: discord.Embed | None = None):
    guild = _output_guild()
    if not guild:
        return

    ch: discord.TextChannel | None = None
    # If output guild is the main guild, prefer configured channel ID.
    if (not OUTPUT_GUILD_ID) or int(getattr(guild, "id", 0) or 0) == int(GUILD_ID or 0):
        base = bot.get_channel(LOG_OTHER_CHANNEL_ID) if LOG_OTHER_CHANNEL_ID else None
        ch = base if isinstance(base, discord.TextChannel) else None
    # Otherwise (or if missing), use channel name override.
    if ch is None:
        name = OUTPUT_LOG_OTHER_CHANNEL_NAME or "bot-logs"
        ch = await _get_or_create_text_channel(guild, name=name)
    if not ch:
        return

    with suppress(Exception):
        e = embed
        if e is None:
            e = discord.Embed(
                description=str(msg or "").strip() or "—",
                color=0x5865F2,
                timestamp=datetime.now(timezone.utc),
            )
            nm = str(getattr(ch, "name", "") or "").strip()
            e.set_footer(text=f"RSCheckerbot • {nm}" if nm else "RSCheckerbot")
        allow = discord.AllowedMentions.none() if int(getattr(guild, "id", 0) or 0) != int(GUILD_ID or 0) else discord.AllowedMentions(users=True, roles=False, everyone=False)
        await ch.send(embed=e, allowed_mentions=allow)

async def log_role_event(message: str | None = None, *, embed: discord.Embed | None = None):
    await log_other(message, embed=embed)

async def log_whop(msg: str):
    """Log to Whop logs channel (for subscription data from Whop system)"""
    guild = _output_guild()
    if not guild:
        return
    ch: discord.TextChannel | None = None
    if (not OUTPUT_GUILD_ID) or int(getattr(guild, "id", 0) or 0) == int(GUILD_ID or 0):
        base = bot.get_channel(WHOP_LOGS_CHANNEL_ID) if WHOP_LOGS_CHANNEL_ID else None
        ch = base if isinstance(base, discord.TextChannel) else None
    if ch is None:
        name = OUTPUT_WHOP_CHANNEL_NAME or "whop-logs"
        ch = await _get_or_create_text_channel(guild, name=name)
    if not ch:
        return
    with suppress(Exception):
        await ch.send(str(msg or "")[:1900], allowed_mentions=discord.AllowedMentions.none())

def _find_text_channel_by_name(guild: discord.Guild, name: str) -> discord.TextChannel | None:
    nm = (name or "").strip().lower()
    if not guild or not nm:
        return None
    for ch in guild.text_channels:
        if str(ch.name or "").lower() == nm:
            return ch
    return None


def _output_guild() -> discord.Guild | None:
    """Guild used for bot output/logging (defaults to main guild)."""
    gid = int(OUTPUT_GUILD_ID or 0) or int(GUILD_ID or 0)
    return bot.get_guild(int(gid)) if gid else None


_STAFF_SEND_DEDUPE: dict[str, float] = {}


def _staff_send_dedupe_key(*, channel_id: int, content: str, embed: discord.Embed) -> str:
    """Stable-ish key to prevent duplicate staff posts (short window).

    We intentionally avoid embed timestamps; we key off semantic fields.
    """
    try:
        title = str(getattr(embed, "title", "") or "").strip()
    except Exception:
        title = ""
    try:
        desc = str(getattr(embed, "description", "") or "").strip()
    except Exception:
        desc = ""
    try:
        footer = str(getattr(getattr(embed, "footer", None), "text", "") or "").strip()
    except Exception:
        footer = ""

    # Prefer identity/status fields to avoid suppressing different events.
    important_names = {
        "discord id",
        "membership id",
        "status",
        "membership",
        "email",
        "member (whop)",
        "whop key",
        "key",
    }
    fields_out: list[str] = []
    try:
        for f in (getattr(embed, "fields", None) or []):
            try:
                nm = str(getattr(f, "name", "") or "").strip()
                val = str(getattr(f, "value", "") or "").strip()
            except Exception:
                continue
            if not nm or not val:
                continue
            if nm.strip().lower() in important_names:
                fields_out.append(f"{nm}:{val}")
    except Exception:
        pass

    blob = "\n".join(
        [
            f"ch={int(channel_id)}",
            f"title={title}",
            f"footer={footer}",
            f"content={str(content or '').strip()}",
            f"desc={desc[:300]}",
            *fields_out[:20],
        ]
    )
    return hashlib.sha1(blob.encode("utf-8", errors="ignore")).hexdigest()


async def _get_or_create_text_channel(
    guild: discord.Guild,
    *,
    name: str,
    category_id: int | None = None,
) -> discord.TextChannel | None:
    nm = str(name or "").strip()
    if not guild or not nm:
        return None
    existing = _find_text_channel_by_name(guild, nm)
    if existing:
        return existing
    me = guild.me or guild.get_member(int(getattr(bot.user, "id", 0) or 0))
    if not me or not getattr(me.guild_permissions, "manage_channels", False):
        return None
    category = None
    if category_id:
        base = guild.get_channel(int(category_id))
        category = base if isinstance(base, discord.CategoryChannel) else None
    with suppress(Exception):
        created = await guild.create_text_channel(name=nm, category=category, reason="RSCheckerbot: auto-create output channel")
        return created if isinstance(created, discord.TextChannel) else None
    return None


async def _ensure_alert_channels(guild: discord.Guild) -> None:
    """Ensure the two staff alert channels exist and live under the staff alerts category."""
    if not guild:
        return
    category = guild.get_channel(STAFF_ALERTS_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        category = None

    for name in (PAYMENT_FAILURE_CHANNEL_NAME, MEMBER_CANCELLATION_CHANNEL_NAME):
        existing = _find_text_channel_by_name(guild, name)
        if existing:
            # Best-effort: ensure it is inside the category for visibility/organization.
            if category and getattr(existing, "category_id", None) != category.id:
                with suppress(Exception):
                    await existing.edit(category=category, reason="RSCheckerbot: move staff alert channel into category")
            continue
        try:
            await guild.create_text_channel(name=name, category=category, reason="RSCheckerbot: staff alert channel")
        except Exception as e:
            # Fallback: category permissions can block creation even if the bot can create channels in general.
            log.warning(f"[Alerts] Failed to create #{name} in category; retrying without category: {e}")
            with suppress(Exception):
                await guild.create_text_channel(name=name, reason="RSCheckerbot: staff alert channel (fallback)")


async def log_member_status(msg: str, embed: discord.Embed = None, *, channel_name: str | None = None):
    """Log staff embeds. Defaults to member status logs channel, but can route by channel name."""
    guild = _output_guild()
    if not guild:
        return

    ch: discord.TextChannel | None = None
    is_member_status_target = False
    if channel_name:
        ch = _find_text_channel_by_name(guild, channel_name)
        if ch is None:
            log.warning(f"[Log] Requested channel '{channel_name}' not found; falling back to member_status_logs_channel_id")
    else:
        is_member_status_target = True

    # Member status default: resolve by channel name in the output guild, not by ID from the main guild.
    if ch is None and is_member_status_target:
        name = OUTPUT_MEMBER_STATUS_CHANNEL_NAME or "member-status-logs"
        ch = await _get_or_create_text_channel(guild, name=name, category_id=STAFF_ALERTS_CATEGORY_ID)

    # If a case channel is requested, best-effort ensure they exist in the output guild.
    if ch is None and channel_name and channel_name in (PAYMENT_FAILURE_CHANNEL_NAME, MEMBER_CANCELLATION_CHANNEL_NAME):
        with suppress(Exception):
            await _ensure_alert_channels(guild)
        ch = _find_text_channel_by_name(guild, channel_name)

    if not ch:
        return

    async def _maybe_capture_for_reporting(sent_embed: discord.Embed, *, is_member_status: bool) -> None:
        """Persist only member-status-logs output into the reporting store (bounded)."""
        if not REPORTING_CONFIG.get("enabled"):
            return
        if not is_member_status:
            return
        if not (_report_load_store and _report_save_store and _report_prune_store and _report_record_member_status_post):
            return

        ts_i, kind, discord_id, whop_brief = _extract_reporting_from_member_status_embed(
            sent_embed,
            fallback_ts=int(datetime.now(timezone.utc).timestamp()),
        )

        # Load, record, prune, save (bounded)
        try:
            async with _REPORTING_STORE_LOCK:
                global _REPORTING_STORE
                if _REPORTING_STORE is None:
                    _REPORTING_STORE = _report_load_store(BASE_DIR, retention_weeks=int(REPORTING_CONFIG.get("retention_weeks", 26)))
                _REPORTING_STORE = _report_record_member_status_post(
                    _REPORTING_STORE,
                    ts=ts_i,
                    event_kind=kind,
                    discord_id=discord_id,
                    email="",
                    whop_brief=whop_brief or None,
                )
                _REPORTING_STORE = _report_prune_store(
                    _REPORTING_STORE,
                    retention_weeks=int(REPORTING_CONFIG.get("retention_weeks", 26)),
                )
                _report_save_store(BASE_DIR, _REPORTING_STORE)
        except Exception as e:
            log.warning(f"[Reporting] failed to persist member-status entry: {e}")

    try:
        # Use provided embed or create default one
        embed_was_none = embed is None
        if embed_was_none:
            embed = discord.Embed(
                description=msg,
                color=0x5865F2,  # Discord blurple color
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text="RSCheckerbot • Member Status Tracking")

        # Ensure member is always clickable by putting the user mention in message content.
        # Embed mentions are unreliable across clients. We also keep Neo output non-mention to avoid "unknown-user".
        content = ""
        in_main_guild = int(getattr(guild, "id", 0) or 0) == int(GUILD_ID or 0)
        if (not embed_was_none) and in_main_guild:
            content = str(msg or "").strip()
            if not content and isinstance(embed, discord.Embed):
                with suppress(Exception):
                    for f in (embed.fields or []):
                        if str(getattr(f, "name", "") or "").strip().lower() == "discord id":
                            m = re.search(r"(\d{17,19})", str(getattr(f, "value", "") or ""))
                            if m:
                                content = f"<@{m.group(1)}>"
                                break

        # Allow user mentions only in main guild, but send "silent" so staff cards don't ping.
        allow = (
            discord.AllowedMentions.none()
            if not in_main_guild
            else discord.AllowedMentions(users=True, roles=False, everyone=False)
        )

        # Dedupe: avoid duplicate staff cards (webhook retries / race conditions).
        try:
            ttl = int(LOG_CONTROLS.get("staff_embed_dedupe_seconds", 45))
        except Exception:
            ttl = 45
        ttl = max(0, min(ttl, 600))
        if ttl > 0 and isinstance(embed, discord.Embed):
            now = time.time()
            key = _staff_send_dedupe_key(channel_id=int(getattr(ch, "id", 0) or 0), content=(content or ""), embed=embed)
            # Prune old entries (cheap).
            if _STAFF_SEND_DEDUPE:
                try:
                    cutoff = now - float(ttl)
                    stale = [k for k, ts in _STAFF_SEND_DEDUPE.items() if float(ts) < cutoff]
                    for k in stale[:2000]:
                        _STAFF_SEND_DEDUPE.pop(k, None)
                except Exception:
                    pass
            prev = _STAFF_SEND_DEDUPE.get(key)
            if prev and (now - float(prev)) < float(ttl):
                return None
            # Reserve key immediately to avoid races; if send fails, remove reservation.
            _STAFF_SEND_DEDUPE[key] = now
        try:
            sent = await ch.send(content=(content or ""), embed=embed, allowed_mentions=allow, silent=bool(in_main_guild))
        except TypeError:
            # Backwards compatibility (older discord.py)
            sent = await ch.send(content=(content or ""), embed=embed, allowed_mentions=allow)
        try:
            await _maybe_capture_for_reporting(embed, is_member_status=is_member_status_target)
        except Exception:
            pass
        return sent
    except Exception:
        # If we reserved a dedupe key but failed to send, clear it so a retry can post.
        with suppress(Exception):
            if "key" in locals():
                now0 = locals().get("now", None)
                if now0 is not None and _STAFF_SEND_DEDUPE.get(locals()["key"]) == now0:
                    _STAFF_SEND_DEDUPE.pop(locals()["key"], None)
        return None

# -----------------------------
# Registry helpers
# -----------------------------
def has_sequence_before(user_id: int) -> bool:
    return str(user_id) in registry

def mark_started(user_id: int):
    uid = str(user_id)
    if uid not in registry:
        registry[uid] = {
            "started_at": _now().isoformat(),
            "completed": False,
        }
        save_json(REGISTRY_FILE, registry)

def mark_cancelled(user_id: int, reason: str):
    uid = str(user_id)
    if uid not in registry:
        registry[uid] = {"started_at": _now().isoformat()}
    registry[uid]["completed"] = True
    registry[uid]["cancel_reason"] = reason
    queue_state.pop(uid, None)
    save_all()

def mark_finished(user_id: int):
    uid = str(user_id)
    if uid not in registry:
        registry[uid] = {"started_at": _now().isoformat()}
    registry[uid]["completed"] = True
    registry[uid]["cancel_reason"] = "finished"
    queue_state.pop(uid, None)
    save_all()

# -----------------------------
# Queue helpers
# -----------------------------
def enqueue_first_day(user_id: int):
    settings = load_settings()
    if not settings.get("dm_sequence_enabled", True):
        return  # DM sequence disabled, don't enqueue
    queue_state[str(user_id)] = {
        "current_day": "day_1",
        "next_send": _now().isoformat().replace("+00:00", "Z"),
    }
    save_json(QUEUE_FILE, queue_state)
    mark_started(user_id)

def schedule_next(user_id: int, current_day: str):
    if current_day not in DAY_KEYS:
        mark_cancelled(user_id, "internal_error_bad_day")
        return
    if current_day == "day_7b":
        mark_finished(user_id)
        return

    idx = DAY_KEYS.index(current_day)
    next_day = DAY_KEYS[idx + 1]
    delay = timedelta(minutes=DAY7B_DELAY_MIN) if next_day == "day_7b" else timedelta(hours=DAY_GAP_HOURS)
    next_time = _now() + delay

    queue_state[str(user_id)] = {
        "current_day": next_day,
        "next_send": next_time.isoformat().replace("+00:00", "Z"),
    }
    save_json(QUEUE_FILE, queue_state)

def is_due(next_send_iso: str) -> bool:
    try:
        nxt = datetime.fromisoformat(next_send_iso.replace("Z", "+00:00"))
        return _now() >= nxt
    except Exception:
        return True

# -----------------------------
# Role checks
# -----------------------------
def has_cancel_role(member: discord.Member) -> bool:
    role_ids = {r.id for r in member.roles}
    return (ROLE_CANCEL_A in role_ids) or (ROLE_CANCEL_B in role_ids)

def has_trigger_role(member: discord.Member) -> bool:
    return any(r.id == ROLE_TRIGGER for r in member.roles)

def has_member_role(member: discord.Member) -> bool:
    return any(r.id == ROLE_CANCEL_A for r in member.roles)

def has_former_member_role(member: discord.Member) -> bool:
    return any(r.id == FORMER_MEMBER_ROLE for r in member.roles)

# -----------------------------
# Message loader/sender
# -----------------------------
async def send_day(member: discord.Member, day_key: str):
    settings = load_settings()
    if not settings.get("dm_sequence_enabled", True):
        return  # DM sequence disabled, don't send
    global last_send_at

    if last_send_at:
        delta = (_now() - last_send_at).total_seconds()
        if delta < SEND_SPACING_SECONDS:
            await asyncio.sleep(SEND_SPACING_SECONDS - delta)

    if has_cancel_role(member):
        cancel_roles = []
        if ROLE_CANCEL_A and any(r.id == ROLE_CANCEL_A for r in member.roles):
            cancel_roles.append(_fmt_role(ROLE_CANCEL_A, member.guild))
        if ROLE_CANCEL_B and any(r.id == ROLE_CANCEL_B for r in member.roles):
            cancel_roles.append(_fmt_role(ROLE_CANCEL_B, member.guild))
        cancel_info = ", ".join(cancel_roles) if cancel_roles else "cancel role"
        mark_cancelled(member.id, "cancel_role_present_pre_send")
        await log_other(f"🛑 Cancelled pre-send for {_fmt_user(member)} — {cancel_info} present (DM not sent)")
        return

    join_url = UTM_LINKS.get(day_key)
    if not join_url:
        mark_cancelled(member.id, "missing_utm")
        await log_other(f"❌ Missing UTM for `{day_key}` on {_fmt_user(member)} — sequence cancelled")
        return

    try:
        # Build embed from JSON (single source of truth)
        from view import get_dm_view
        day_data = (messages_data.get("days") or {}).get(day_key) or {}

        banner_url = day_data.get("banner_url") or messages_data.get("banner_url")
        footer_url = day_data.get("footer_url") or messages_data.get("footer_url")
        main_image_url = day_data.get("main_image_url")
        description = str(day_data.get("description", "")).format(join_url=join_url)

        banner_embed = discord.Embed()
        if banner_url:
            banner_embed.set_image(url=banner_url)

        content_embed = discord.Embed(description=description)
        if main_image_url:
            content_embed.set_image(url=main_image_url)
        elif footer_url:
            content_embed.set_image(url=footer_url)

        view = get_dm_view(day_number=day_key, join_url=join_url)
        embeds = [banner_embed, content_embed]
    except Exception as e:
        mark_cancelled(member.id, "embed_build_error")
        await log_other(f"❌ build_embed error `{day_key}` for {_fmt_user(member)}: `{e}` — sequence cancelled")
        return

    try:
        await member.send(embeds=embeds, view=view)
        last_send_at = _now()
        sent_embed = _make_dyno_embed(
            member=member,
            description=f"{member.mention} {day_key} sent",
            footer=f"ID: {member.id}",
            color=0x57F287,
        )
        if day_key == "day_1":
            await log_first(embed=sent_embed)
        else:
            await log_other(embed=sent_embed)
    except discord.Forbidden:
        mark_cancelled(member.id, "dm_forbidden")
        await log_other(f"🚫 DM forbidden for {_fmt_user(member)} — sequence cancelled (user blocked DMs)")
    except Exception as e:
        await log_other(f"⚠️ Failed to send **{day_key}** to {_fmt_user(member)}: `{e}`")

# -----------------------------
# Invite Tracking Functions
# -----------------------------
async def create_single_use_invite(email: str, lead_id: str, utm_data: dict = None) -> Optional[str]:
    """Create a single-use Discord invite link and store it in JSON."""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            log.error("Guild not found")
            return FALLBACK_INVITE if FALLBACK_INVITE else None

        channel = guild.get_channel(INVITE_CHANNEL_ID) if INVITE_CHANNEL_ID else None
        if not channel:
            log.error("Invite channel not found")
            return FALLBACK_INVITE if FALLBACK_INVITE else None

        invite = await channel.create_invite(
            max_uses=1,
            max_age=604800,  # 7 days
            unique=True
        )

        # Store in JSON
        global invites_data
        invites_data[invite.code] = {
            "lead_id": lead_id,
            "email": email,
            "utm_data": utm_data,
            "created_at": _now().isoformat(),
            "used_at": None,
            "discord_user_id": None,
            "discord_username": None
        }
        save_invites(invites_data)

        invite_tracking[invite.code] = lead_id
        log.info(f"Created invite {invite.code} for lead {lead_id}")
        return invite.url

    except Exception as e:
        log.error(f"Failed to create invite: {e}")
        return FALLBACK_INVITE if FALLBACK_INVITE else None

async def update_ghl_contact(lead_id: str, discord_user_id: str, discord_username: str, tag: str = "Discord Joined"):
    """Update HighLevel contact with Discord information."""
    if not GHL_API_KEY or not GHL_LOCATION_ID:
        log.warning("GHL API credentials not configured, skipping update")
        return

    try:
        url = f"https://services.leadconnectorhq.com/contacts/{lead_id}"
        headers = {
            "Authorization": f"Bearer {GHL_API_KEY}",
            "Version": "2021-07-28",
            "Content-Type": "application/json"
        }

        update_data = {
            "tags": [tag]
        }

        if GHL_CF_DISCORD_USERNAME:
            update_data["customFields"] = [
                {
                    "id": GHL_CF_DISCORD_USERNAME,
                    "value": discord_username
                }
            ]
        if GHL_CF_DISCORD_ID:
            if "customFields" not in update_data:
                update_data["customFields"] = []
            update_data["customFields"].append({
                "id": GHL_CF_DISCORD_ID,
                "value": str(discord_user_id)
            })

        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=headers, json=update_data) as resp:
                if resp.status == 200:
                    log.info(f"Updated GHL contact {lead_id} with Discord info")
                else:
                    log.error(f"GHL update failed: {resp.status} - {await resp.text()}")

    except Exception as e:
        log.error(f"Failed to update GHL: {e}")

async def track_invite_usage(invite_code: str, member: discord.Member):
    """Track when an invite is used and update GHL."""
    global invites_data
    
    if invite_code in invites_data:
        invite_entry = invites_data[invite_code]
        used_at = invite_entry.get("used_at")
        
        # Only process if not already tracked
        if used_at is None:
            invite_entry["used_at"] = _now().isoformat()
            invite_entry["discord_user_id"] = str(member.id)
            invite_entry["discord_username"] = str(member)
            save_invites(invites_data)

            lead_id = invite_entry.get("lead_id")
            await update_ghl_contact(lead_id, str(member.id), str(member), "Discord Joined")
            log.info(f"Tracked invite usage: {invite_code} -> {member} (lead: {lead_id})")
        else:
            log.debug(f"Invite {invite_code} already tracked")
    else:
        log.warning(f"Unknown invite code used: {invite_code}")

# -----------------------------
# HTTP Server for Invite Creation
# -----------------------------
async def handle_create_invite(request):
    """Handle POST request to create an invite."""
    try:
        data = await request.json()
        email = data.get("email")
        lead_id = data.get("lead_id")
        utm_data = data.get("utm_data", {})

        if not email or not lead_id:
            return web.json_response({"error": "email and lead_id required"}, status=400)

        invite_url = await create_single_use_invite(email, lead_id, utm_data)
        if not invite_url:
            return web.json_response({"error": "Failed to create invite"}, status=500)

        if GHL_API_KEY and GHL_LOCATION_ID:
            try:
                url = f"https://services.leadconnectorhq.com/contacts/{lead_id}/tags"
                headers = {
                    "Authorization": f"Bearer {GHL_API_KEY}",
                    "Version": "2021-07-28",
                    "Content-Type": "application/json"
                }
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json={"tags": ["Discord Invited"]}) as resp:
                        if resp.status != 200:
                            log.warning(f"Failed to tag GHL contact: {resp.status}")
            except Exception as e:
                log.error(f"Failed to tag GHL: {e}")

        return web.json_response({"invite_url": invite_url})

    except Exception as e:
        log.error(f"Error handling invite request: {e}")
        return web.json_response({"error": str(e)}, status=500)


def _deep_get(obj: object, path: str) -> object:
    """Best-effort nested dict access using dot paths."""
    cur = obj
    for part in str(path or "").split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _first_str(*vals: object) -> str:
    for v in vals:
        s = str(v or "").strip()
        if s and s != "—":
            return s
    return ""


def _first_prefixed(prefix: str, *vals: object) -> str:
    p = str(prefix or "").strip()
    if not p:
        return ""
    for v in vals:
        s = str(v or "").strip()
        if s and s.startswith(p):
            return s
    return ""


def _whop_std_event_type(payload: dict) -> str:
    # Whop webhooks commonly use `type` with a dotted name (e.g. "payment.created").
    return _first_str(
        payload.get("type"),
        payload.get("event_type"),
        payload.get("event"),
        _deep_get(payload, "data.type"),
        _deep_get(payload, "data.event_type"),
    ).lower()


def _whop_std_membership_id(payload: dict) -> str:
    # Try common shapes for membership id under `data`.
    cand = _first_str(
        payload.get("membership_id"),
        _deep_get(payload, "membership.id"),
        _deep_get(payload, "membership_id"),
        _deep_get(payload, "data.membership_id"),
        _deep_get(payload, "data.membership.id"),
        _deep_get(payload, "data.membership"),
        _deep_get(payload, "data.object.membership_id"),
        _deep_get(payload, "data.object.membership.id"),
        _deep_get(payload, "data.object.membership"),
        _deep_get(payload, "data.payment.membership_id"),
        _deep_get(payload, "data.payment.membership.id"),
        _deep_get(payload, "data.payment.membership"),
        _deep_get(payload, "data.data.membership_id"),
        _deep_get(payload, "data.data.membership.id"),
        _deep_get(payload, "data.data.membership"),
    )
    if cand:
        return str(cand).strip()

    # Fallback: scan payload for a mem_... token anywhere.
    try:
        def _walk(obj: object) -> str:
            if isinstance(obj, dict):
                for _k, v in obj.items():
                    out = _walk(v)
                    if out:
                        return out
            elif isinstance(obj, list):
                for it in obj:
                    out = _walk(it)
                    if out:
                        return out
            else:
                s = str(obj or "").strip()
                if s and "mem_" in s:
                    m = re.search(r"\b(mem_[A-Za-z0-9]+)\b", s)
                    if m:
                        return m.group(1)
            return ""

        return _walk(payload)
    except Exception:
        return ""


def _whop_std_payment_id(payload: dict) -> str:
    # Whop: payment.* events use data.id = pay_...
    return _first_prefixed(
        "pay_",
        _deep_get(payload, "data.payment.id"),
        _deep_get(payload, "payment.id"),
        payload.get("payment_id"),
        _deep_get(payload, "data.id"),
        _deep_get(payload, "data.payment.id"),
        _deep_get(payload, "payment.id"),
        payload.get("payment_id"),
    )


def _whop_std_refund_id(payload: dict) -> str:
    return _first_prefixed(
        "rfnd_",
        _deep_get(payload, "data.id"),
        _deep_get(payload, "data.refund.id"),
        _deep_get(payload, "refund.id"),
        payload.get("refund_id"),
    )


def _whop_std_dispute_id(payload: dict) -> str:
    return _first_prefixed(
        "dspt_",
        _deep_get(payload, "data.id"),
        _deep_get(payload, "data.dispute.id"),
        _deep_get(payload, "dispute.id"),
        payload.get("dispute_id"),
    )


def _whop_std_user_id(payload: dict) -> str:
    return _first_prefixed(
        "user_",
        _deep_get(payload, "data.user.id"),
        _deep_get(payload, "data.user"),
        _deep_get(payload, "user.id"),
        _deep_get(payload, "user"),
        payload.get("user_id"),
    )


def _whop_std_member_id(payload: dict) -> str:
    # Whop: setup_intent.* includes data.member.id = mber_...
    return _first_prefixed(
        "mber_",
        _deep_get(payload, "data.member.id"),
        _deep_get(payload, "data.member"),
        payload.get("member_id"),
    )


def _pick_best_membership_id_for_user(memberships: list[dict]) -> str:
    """Pick the most relevant membership id from a Whop user memberships list."""
    if not isinstance(memberships, list) or not memberships:
        return ""
    pri = {
        "active": 0,
        "trialing": 1,
        "past_due": 2,
        "unpaid": 2,
        "canceling": 3,
        "canceled": 4,
        "cancelled": 4,
        "completed": 4,
        "expired": 5,
        "ended": 5,
        "drafted": 9,
    }

    def _score(m: dict) -> tuple[int, str]:
        st = str(m.get("status") or "").strip().lower()
        p = pri.get(st, 8)
        ts = str(m.get("updated_at") or m.get("created_at") or "").strip()
        return (p, ts)

    best: dict | None = None
    for m in memberships:
        if not isinstance(m, dict):
            continue
        mid = str(m.get("id") or m.get("membership_id") or "").strip()
        if not mid.startswith("mem_"):
            continue
        if best is None:
            best = m
            continue
        try:
            # Prefer lower priority, then later timestamp.
            if _score(m)[0] < _score(best)[0]:
                best = m
            elif _score(m)[0] == _score(best)[0] and _score(m)[1] > _score(best)[1]:
                best = m
        except Exception:
            best = m
    if not isinstance(best, dict):
        return ""
    return str(best.get("id") or best.get("membership_id") or "").strip()


def _normalize_whop_std_event_type(evt: str) -> str:
    """Normalize Whop webhook `type` to the canonical dot format used in docs.

    Whop docs/examples use dot-separated types (e.g. `invoice.created`), while the UI
    can display underscore variants (e.g. `invoice_created`). To avoid missed routing
    when Whop delivers underscore-style types, normalize known variants here.
    """
    e = str(evt or "").strip().lower()
    if not e:
        return ""
    if "." in e:
        return e
    if "_" not in e:
        return e
    mapping = {
        # Payments
        "payment_created": "payment.created",
        "payment_succeeded": "payment.succeeded",
        "payment_failed": "payment.failed",
        "payment_pending": "payment.pending",
        # Refunds / disputes
        "refund_created": "refund.created",
        "refund_updated": "refund.updated",
        "dispute_created": "dispute.created",
        "dispute_updated": "dispute.updated",
        # Setup intents
        "setup_intent_requires_action": "setup_intent.requires_action",
        "setup_intent_succeeded": "setup_intent.succeeded",
        "setup_intent_canceled": "setup_intent.canceled",
        # Invoices
        "invoice_created": "invoice.created",
        "invoice_paid": "invoice.paid",
        "invoice_past_due": "invoice.past_due",
        "invoice_voided": "invoice.voided",
        # Withdrawals / payout methods / verification
        "withdrawal_created": "withdrawal.created",
        "withdrawal_updated": "withdrawal.updated",
        "payout_method_created": "payout_method.created",
        "payoutmethod_created": "payout_method.created",
        "verification_succeeded": "verification.succeeded",
        # Memberships
        "membership_activated": "membership.activated",
        "membership_deactivated": "membership.deactivated",
        "membership_cancel_at_period_end_changed": "membership.cancel_at_period_end_changed",
        # Entries / courses
        "entry_created": "entry.created",
        "entry_approved": "entry.approved",
        "entry_denied": "entry.denied",
        "entry_deleted": "entry.deleted",
        "course_lesson_interaction_completed": "course_lesson_interaction.completed",
        "courselessoninteraction_completed": "course_lesson_interaction.completed",
    }
    return mapping.get(e, e)


async def _resolve_membership_id_for_std_webhook(evt: str, payload: dict) -> tuple[str, dict]:
    """Resolve membership_id for webhook types that don't include `membership` directly."""
    mid = _whop_std_membership_id(payload)
    ctx: dict = {
        "payment_id": "",
        "refund_id": "",
        "dispute_id": "",
        "user_id": "",
        "member_id": "",
    }
    if mid:
        return (mid, ctx)
    evt_l = _normalize_whop_std_event_type(evt)
    ctx["payment_id"] = str(_whop_std_payment_id(payload) or "").strip()
    ctx["refund_id"] = str(_whop_std_refund_id(payload) or "").strip() if evt_l.startswith("refund.") else ""
    ctx["dispute_id"] = str(_whop_std_dispute_id(payload) or "").strip() if evt_l.startswith("dispute.") else ""
    ctx["user_id"] = str(_whop_std_user_id(payload) or "").strip()
    ctx["member_id"] = str(_whop_std_member_id(payload) or "").strip() if evt_l.startswith("setup_intent.") else ""

    # 1) payment/refund/dispute: resolve via payment.id
    pay_obj: dict | None = None
    pay_id = str(ctx.get("payment_id") or "").strip()
    if evt_l.startswith("refund.") and not pay_id:
        pay_id = _first_prefixed("pay_", _deep_get(payload, "data.payment.id"), _deep_get(payload, "data.payment"))
        ctx["payment_id"] = str(pay_id or "").strip()
    if evt_l.startswith("dispute.") and not pay_id:
        pay_id = _first_prefixed("pay_", _deep_get(payload, "data.payment.id"), _deep_get(payload, "data.payment"))
        ctx["payment_id"] = str(pay_id or "").strip()
        if not pay_id:
            # dispute.updated often omits payment; fetch dispute to find payment id.
            dspt = str(ctx.get("dispute_id") or "").strip()
            if dspt and hasattr(whop_api_client, "get_dispute_by_id"):
                with suppress(Exception):
                    ds = await whop_api_client.get_dispute_by_id(dspt)  # type: ignore[attr-defined]
                    if isinstance(ds, dict):
                        pay_id = _first_prefixed("pay_", ds.get("payment_id"), _deep_get(ds, "payment.id"), _deep_get(ds, "payment"))
                        if pay_id:
                            ctx["payment_id"] = str(pay_id).strip()

    if pay_id and hasattr(whop_api_client, "get_payment_by_id"):
        with suppress(Exception):
            pay_obj = await whop_api_client.get_payment_by_id(str(pay_id))  # type: ignore[attr-defined]
    if isinstance(pay_obj, dict) and pay_obj:
        mid = _first_str(
            pay_obj.get("membership_id"),
            _deep_get(pay_obj, "membership.id"),
            _deep_get(pay_obj, "membership"),
        )
        if mid and str(mid).strip().startswith("mem_"):
            return (str(mid).strip(), ctx)

    # 2) setup_intent: resolve member -> user -> memberships -> best membership
    if evt_l.startswith("setup_intent.") and str(ctx.get("member_id") or "").strip().startswith("mber_"):
        mber_id = str(ctx.get("member_id") or "").strip()
        if hasattr(whop_api_client, "get_member_by_id"):
            with suppress(Exception):
                mber = await whop_api_client.get_member_by_id(mber_id)
                if isinstance(mber, dict):
                    uid = _first_prefixed("user_", mber.get("user_id"), _deep_get(mber, "user.id"), _deep_get(mber, "user"))
                    if uid:
                        ctx["user_id"] = str(uid).strip()

    # 3) user-centric events (entries / course): resolve via user.id -> memberships
    uid = str(ctx.get("user_id") or "").strip()
    if uid.startswith("user_"):
        with suppress(Exception):
            candidates = await whop_api_client.get_user_memberships(uid)
            best_mid = _pick_best_membership_id_for_user(candidates or [])
            if best_mid:
                return (best_mid, ctx)

    return ("", ctx)


async def _process_whop_standard_webhook(payload: dict, *, headers: dict) -> None:
    """Process Whop standard webhooks (real-time) into staff cards + movement logs."""
    if not isinstance(payload, dict) or not payload:
        return
    if not whop_api_client or not bot.is_ready():
        return

    evt = _normalize_whop_std_event_type(_whop_std_event_type(payload))
    wh_id = str((headers or {}).get("webhook-id") or payload.get("id") or "").strip()
    mid = _whop_std_membership_id(payload)
    if not mid:
        mid, ctx = await _resolve_membership_id_for_std_webhook(evt, payload)
        if mid:
            with suppress(Exception):
                await _whop_api_events_log(
                    f"[Whop Webhook] resolved mid={mid} type={evt or '—'} pay={str(ctx.get('payment_id') or '—')} dspt={str(ctx.get('dispute_id') or '—')}"
                )

    # Always log receipt (movement logs in Neo).
    with suppress(Exception):
        # Keep it intentionally simple for easy scanning.
        await _whop_api_events_log(f"[Whop Webhook] detected type={evt or '—'} mid={mid or '—'} id={wh_id or '—'}")

    # Many webhook types (withdrawals, payout methods, invoices, etc.) have no membership context.
    # We still log them above; staff-card output only happens when we can resolve a membership.
    if not mid:
        return

    now = datetime.now(timezone.utc)
    guild = bot.get_guild(GUILD_ID) if GUILD_ID else None
    if not isinstance(guild, discord.Guild):
        return

    async with _WHOP_API_EVENTS_LOCK:
        state = _load_whop_api_events_state()
        memberships = state.get("memberships") if isinstance(state.get("memberships"), dict) else {}
        sent = state.get("sent") if isinstance(state.get("sent"), dict) else {}
        cases = state.get("cases") if isinstance(state.get("cases"), dict) else {}
        if not isinstance(memberships, dict):
            memberships = {}
        if not isinstance(sent, dict):
            sent = {}
        if not isinstance(cases, dict):
            cases = {}

        # Dedupe: webhook deliveries are unique by webhook-id.
        if wh_id:
            dkey = f"whop_webhook:{wh_id}"
            if dkey in sent:
                return
            sent[dkey] = now.isoformat().replace("+00:00", "Z")

        # Fetch authoritative state via API (totals + dashboard). Discord linkage is enriched via whop-logs fallback.
        brief = await _fetch_whop_brief_by_membership_id(mid)
        if not isinstance(brief, dict) or not brief:
            return

        mid2 = str(brief.get("membership_id") or mid).strip() or mid
        renewal_end_iso = str(brief.get("renewal_end_iso") or "").strip()
        cur = {
            "status": str(brief.get("status") or "").strip().lower(),
            "cancel_at_period_end": (str(brief.get("cancel_at_period_end") or "").strip().lower() == "yes"),
            "renewal_period_end": _first_str(
                _deep_get(payload, "data.renewal_period_end"),
                _deep_get(payload, "data.membership.renewal_period_end"),
                renewal_end_iso,
            ),
            "updated_at": now.isoformat().replace("+00:00", "Z"),
        }
        prev = memberships.get(mid2) if isinstance(memberships.get(mid2), dict) else None
        # Prefer explicit webhook type to decide "kind" (real-time triggers),
        # then fall back to diff-based classification.
        kind = ""
        evt_l = _normalize_whop_std_event_type(evt)
        if evt_l == "payment.created":
            kind = "payment_created"
        elif evt_l == "payment.pending":
            kind = "payment_pending"
        elif evt_l == "setup_intent.requires_action":
            kind = "setup_intent_requires_action"
        elif evt_l == "setup_intent.succeeded":
            kind = "setup_intent_succeeded"
        elif evt_l == "setup_intent.canceled":
            kind = "setup_intent_canceled"
        elif evt_l == "entry.created":
            kind = "entry_created"
        elif evt_l == "entry.approved":
            kind = "entry_approved"
        elif evt_l == "entry.denied":
            kind = "entry_denied"
        elif evt_l == "entry.deleted":
            kind = "entry_deleted"
        elif evt_l == "course_lesson_interaction.completed":
            kind = "course_lesson_completed"
        elif evt_l == "invoice.created":
            kind = "invoice_created"
        elif evt_l == "invoice.paid":
            kind = "invoice_paid"
        elif evt_l == "invoice.past_due":
            kind = "invoice_past_due"
        elif evt_l == "invoice.voided":
            kind = "invoice_voided"
        elif evt_l == "refund.created":
            kind = "refund_created"
        elif evt_l == "refund.updated":
            kind = "refund_updated"
        elif evt_l == "dispute.created":
            kind = "dispute_created"
        elif evt_l == "dispute.updated":
            kind = "dispute_updated"
        elif "payment" in evt_l and "failed" in evt_l:
            kind = "payment_failed"
        elif "payment" in evt_l and ("succeeded" in evt_l or "paid" in evt_l):
            kind = "payment_succeeded"
        elif "membership" in evt_l and any(x in evt_l for x in ("deactivated", "canceled", "cancelled", "expired", "ended")):
            kind = "deactivated"
        elif evt_l == "membership.cancel_at_period_end_changed":
            cape = bool(_deep_get(payload, "data.cancel_at_period_end") is True)
            kind = "cancellation_scheduled" if cape else "cancellation_removed"
        elif "cancel" in evt_l and any(x in evt_l for x in ("removed", "unscheduled", "resumed")):
            kind = "cancellation_removed"
        elif "membership" in evt_l and any(x in evt_l for x in ("created", "activated", "purchased", "generated", "started")):
            # We'll still rely on status bucket to choose joined vs activated title.
            kind = "membership_activated"

        if not kind:
            kind = _classify_whop_change(prev, cur)
        memberships[mid2] = cur

        if not kind:
            state["memberships"] = memberships
            state["sent"] = sent
            state["cases"] = cases
            _save_whop_api_events_state(state)
            return

        title, color, embed_kind = _title_for_event(kind)
        connected_disp = str((brief or {}).get("connected_discord") or "").strip()
        did = _extract_discord_id_from_connected(connected_disp)

        member_obj: discord.Member | None = None
        if did:
            member_obj = guild.get_member(int(did))
            if member_obj is None:
                with suppress(Exception):
                    member_obj = await guild.fetch_member(int(did))

        # Persist brief for later role-driven cards.
        if did and isinstance(brief, dict) and brief:
            with suppress(Exception):
                record_member_whop_summary(int(did), brief, event_type=f"whop.webhook.{evt or kind}", membership_id=str(mid2))

        if member_obj is None:
            # Important: "not linked" should mean "Whop has no Discord connection",
            # not "Discord member not found in the guild".
            if connected_disp:
                title2 = f"{title} (Discord linked, not in server)"
                note = WHOP_NOT_IN_GUILD_NOTE
                discord_value = connected_disp
            else:
                title2 = f"{title} (Discord not linked)"
                note = WHOP_UNLINKED_NOTE
                discord_value = "Not linked"
            e_unlinked = _linked_hint_embed(title=title2, color=color, brief=brief, note=note, discord_value=discord_value)
            await log_member_status("", embed=e_unlinked)
            with suppress(Exception):
                await _whop_api_events_log(f"[Whop Webhook][detected] kind={kind} linked=no mid={mid2} type={evt or '—'}")
            with suppress(Exception):
                issue_override = ""
                case_key_override = ""
                extra_topic = ""
                extra_fields: list[tuple[str, str]] = []
                always_post = False
                if evt_l.startswith("dispute."):
                    dspt = str(_deep_get(payload, "data.id") or "").strip()
                    if dspt.startswith("dspt_"):
                        dstatus = str(_deep_get(payload, "data.status") or "").strip()
                        dreason = str(_deep_get(payload, "data.reason") or "").strip()
                        dby = str(_deep_get(payload, "data.needs_response_by") or "").strip()
                        damt = str(_deep_get(payload, "data.amount") or "").strip()
                        dcur = str(_deep_get(payload, "data.currency") or "").strip()
                        issue_override = _bucket_for_dispute_status(dstatus)
                        case_key_override = f"rschecker_whop_case:dspt={dspt}"
                        extra_topic = f"dspt={dspt}\nstatus={dstatus or '—'}\nreason={dreason or '—'}"
                        always_post = True
                        extra_fields = [
                            ("Dispute ID", dspt),
                            ("Dispute status", dstatus),
                            ("Reason", dreason),
                            ("Needs response by", dby),
                            ("Amount", (f"{damt} {dcur}".strip() if (damt or dcur) else "")),
                            ("Event", evt_l),
                        ]
                await _maybe_open_dispute_resolution_case(
                    guild=guild,
                    mid=mid2,
                    updated_at=str(cur.get("updated_at") or ""),
                    brief=brief,
                    cases=cases,
                    did=did,
                    member_obj=None,
                    issue_override=issue_override,
                    case_key_override=case_key_override,
                    extra_topic=extra_topic,
                    always_post=always_post,
                    extra_fields=extra_fields,
                )
        else:
            relevant = coerce_role_ids(ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B)
            access = access_roles_plain(member_obj, relevant)
            detailed = _build_member_status_detailed_embed(
                title=title,
                member=member_obj,
                access_roles=access,
                color=color,
                event_kind=embed_kind,
                discord_kv=[("event", f"whop.webhook.{evt or kind}")],
                member_kv=[("membership_id", mid2)],
                whop_brief=brief,
            )
            await log_member_status("", embed=detailed)

            # Case channels: real-time webhook-only (no startup/sync/backfill noise).
            try:
                if kind == "payment_failed":
                    mini = _build_case_minimal_embed(
                        title=title,
                        member=member_obj,
                        access_roles=access,
                        whop_brief=brief,
                        color=0xED4245,
                        event_kind="payment_failed",
                    )
                    await log_member_status("", embed=mini, channel_name=PAYMENT_FAILURE_CHANNEL_NAME)
                elif kind == "cancellation_scheduled":
                    mini = _build_case_minimal_embed(
                        title=title,
                        member=member_obj,
                        access_roles=access,
                        whop_brief=brief,
                        color=0xFEE75C,
                        event_kind="cancellation_scheduled",
                    )
                    await log_member_status("", embed=mini, channel_name=MEMBER_CANCELLATION_CHANNEL_NAME)
            except Exception:
                pass
            with suppress(Exception):
                await _whop_api_events_log(
                    f"[Whop Webhook][detected] kind={kind} linked=yes did={member_obj.id} mid={mid2} type={evt or '—'}"
                )
            with suppress(Exception):
                issue_override = ""
                case_key_override = ""
                extra_topic = ""
                extra_fields: list[tuple[str, str]] = []
                always_post = False
                if evt_l.startswith("dispute."):
                    dspt = str(_deep_get(payload, "data.id") or "").strip()
                    if dspt.startswith("dspt_"):
                        dstatus = str(_deep_get(payload, "data.status") or "").strip()
                        dreason = str(_deep_get(payload, "data.reason") or "").strip()
                        dby = str(_deep_get(payload, "data.needs_response_by") or "").strip()
                        damt = str(_deep_get(payload, "data.amount") or "").strip()
                        dcur = str(_deep_get(payload, "data.currency") or "").strip()
                        issue_override = _bucket_for_dispute_status(dstatus)
                        case_key_override = f"rschecker_whop_case:dspt={dspt}"
                        extra_topic = f"dspt={dspt}\nstatus={dstatus or '—'}\nreason={dreason or '—'}"
                        always_post = True
                        extra_fields = [
                            ("Dispute ID", dspt),
                            ("Dispute status", dstatus),
                            ("Reason", dreason),
                            ("Needs response by", dby),
                            ("Amount", (f"{damt} {dcur}".strip() if (damt or dcur) else "")),
                            ("Event", evt_l),
                        ]
                await _maybe_open_dispute_resolution_case(
                    guild=guild,
                    mid=mid2,
                    updated_at=str(cur.get("updated_at") or ""),
                    brief=brief,
                    cases=cases,
                    did=member_obj.id,
                    member_obj=member_obj,
                    issue_override=issue_override,
                    case_key_override=case_key_override,
                    extra_topic=extra_topic,
                    always_post=always_post,
                    extra_fields=extra_fields,
                )

        state["memberships"] = memberships
        state["sent"] = sent
        state["cases"] = cases
        _save_whop_api_events_state(state)


async def handle_whop_webhook_receiver(request):
    """Receive Whop webhook payloads, log them, and forward to Discord"""
    try:
        raw_body = await request.read()
        headers = {k.lower(): v for k, v in dict(request.headers).items()}

        ok, reason = verify_standard_webhook(
            headers,
            raw_body,
            secret=WHOP_WEBHOOK_SECRET,
            tolerance_seconds=WHOP_WEBHOOK_TOLERANCE_SECONDS,
            verify=WHOP_WEBHOOK_VERIFY,
        )
        if not ok:
            log.warning(f"[WhopWebhook] Signature verification failed: {reason}")
            return web.Response(text=f"Invalid webhook signature ({reason})", status=401)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            log.warning("[WhopWebhook] Invalid JSON payload")
            return web.Response(text="Invalid JSON payload", status=400)

        # Log the raw payload
        _save_raw_webhook_payload(payload, headers)
        log.info(f"Received Whop webhook payload (saved to {WHOP_WEBHOOK_RAW_LOG_FILE.name})")

        # Record event ledger entry
        try:
            wh_id = str(headers.get("webhook-id") or payload.get("id") or "").strip()
            wh_ts = str(headers.get("webhook-timestamp") or "").strip()
            try:
                ts_i = int(float(wh_ts)) if wh_ts else int(time.time())
            except Exception:
                ts_i = int(time.time())
            occurred_at = datetime.fromtimestamp(ts_i, tz=timezone.utc)
            if wh_id:
                event = _whop_event_from_webhook_payload(payload, event_id=wh_id, occurred_at=occurred_at)
                await _record_whop_event(event)
        except Exception as e:
            log.warning(f"[WhopWebhook] Failed to record event: {e}")

        # Process the webhook directly (real-time staff cards + movement logs).
        # This replaces the legacy "forward raw JSON to a Discord webhook" behavior which caused blanks.
        with suppress(Exception):
            await _process_whop_standard_webhook(payload, headers=headers)
        return web.Response(text="OK", status=200)
    
    except Exception as e:
        log.error(f"Error handling webhook receiver: {e}", exc_info=True)
        return web.Response(text=f"Error: {str(e)}", status=500)

async def init_http_server():
    """Initialize the HTTP server for invite creation and Whop webhook receiver."""
    app = web.Application()
    app.router.add_post("/create-invite", handle_create_invite)
    app.router.add_post("/whop-webhook", handle_whop_webhook_receiver)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_SERVER_PORT)
    try:
        await site.start()
    except OSError as e:
        # Common on local dev: another bot already bound the port.
        log.warning(f"[HTTP Server] Failed to bind port {HTTP_SERVER_PORT}: {e}")
        with suppress(Exception):
            await runner.cleanup()
        return
    log.info(f"HTTP server started on port {HTTP_SERVER_PORT}")
    log.info(f"Whop webhook receiver: http://0.0.0.0:{HTTP_SERVER_PORT}/whop-webhook")

# -----------------------------
# Cross-bot communication checks
# -----------------------------
async def check_onboarding_ticket(user_id: int, guild: discord.Guild, delay_seconds: int = 5):
    """Check if RSOnboarding created a ticket after Welcome role was assigned."""
    await asyncio.sleep(delay_seconds)  # Give RSOnboarding time to process
    
    try:
        member = guild.get_member(user_id)
        if not member:
            return
        
        # Check if user has Welcome role but no Member role (should have ticket)
        welcome_role = guild.get_role(WELCOME_ROLE_ID)
        member_role = guild.get_role(ROLE_CANCEL_A)  # Member role
        has_welcome = welcome_role and welcome_role in member.roles
        has_member = member_role and member_role in member.roles
        
        if has_welcome and not has_member:
            # User has Welcome but not Member - ticket should exist
            await log_role_event(
                f"🔍 **Onboarding Check** for {_fmt_user(member)}\n"
                f"   ✅ Has {_fmt_role(WELCOME_ROLE_ID, guild)}\n"
                f"   ❌ Missing {_fmt_role(ROLE_CANCEL_A, guild)}\n"
                f"   📋 Expected: RSOnboarding should create ticket"
            )
    except Exception as e:
        log.error(f"Error checking onboarding ticket for {user_id}: {e}")

# -----------------------------
# 60-second fallback checker
# -----------------------------
async def check_and_assign_role(member: discord.Member, *, silent: bool = False):
    if member.bot or member.id in pending_checks:
        return
    pending_checks.add(member.id)
    try:
        await asyncio.sleep(60)

        guild = member.guild
        current_roles = {r.id for r in member.roles}
        has_any = any(role.id in ROLES_TO_CHECK for role in member.roles)
        
        # Get which checked roles the user has vs missing
        user_has_checked = [r.id for r in member.roles if r.id in ROLES_TO_CHECK]
        user_missing_checked = [rid for rid in ROLES_TO_CHECK if rid not in current_roles]
        
        if not has_any:
            trigger_role = guild.get_role(ROLE_TO_ASSIGN)
            if trigger_role is None:
                if silent:
                    log.warning(f"[RoleCheck] Trigger role missing for {member} ({member.id})")
                else:
                    err = _make_dyno_embed(
                        member=member,
                        description=f"{member.mention} trigger role missing",
                        footer=f"ID: {member.id}",
                        color=0xED4245,
                    )
                    err.add_field(name="Missing role", value=_fmt_role(ROLE_TO_ASSIGN, guild)[:1024], inline=False)
                    await log_role_event(embed=err)
                return
            try:
                roles_to_add = [trigger_role]
                roles_to_add_names = [trigger_role.name]
                
                if silent:
                    _suppress_member_update_logs(member.id, seconds=300.0)
                await member.add_roles(*roles_to_add, reason="No valid roles after 60s")

                assigned = ", ".join([str(x) for x in roles_to_add_names if str(x).strip()]) or "—"
                if not silent:
                    e = _make_dyno_embed(
                        member=member,
                        description=f"{member.mention} was given the {assigned} role",
                        footer=f"ID: {member.id}",
                        color=0x57F287,
                    )
                    e.add_field(name="Reason", value="No checked roles after 60s", inline=False)
                    e.add_field(name="Checked roles", value=str(len(ROLES_TO_CHECK)), inline=True)
                    await log_role_event(embed=e)
                
                if (not silent) and (not has_sequence_before(member.id)):
                    enqueue_first_day(member.id)
                    enq = _make_dyno_embed(
                        member=member,
                        description=f"{member.mention} queued for day_1 (60s fallback)",
                        footer=f"ID: {member.id}",
                        color=0x5865F2,
                    )
                    await log_first(embed=enq)
            except Exception as e:
                if silent:
                    log.warning(f"[RoleCheck] Failed to assign roles to {member} ({member.id}): {e}")
                else:
                    await log_role_event(f"⚠️ **Failed to assign roles** to {_fmt_user(member)}\n   ❌ Error: `{e}`")
        else:
            # User has checked roles - keep log compact (avoid dumping full role lists).
            user_has_names = _fmt_role_list(set(user_has_checked), guild)
            if not silent:
                sk = _make_dyno_embed(
                    member=member,
                    description=f"{member.mention} has checked roles; no trigger role needed",
                    footer=f"ID: {member.id}",
                    color=0x5865F2,
                )
                sk.add_field(name="Has", value=user_has_names[:1024] or "—", inline=False)
                sk.add_field(name="Checked roles", value=str(len(ROLES_TO_CHECK)), inline=True)
                await log_role_event(embed=sk)
    finally:
        pending_checks.discard(member.id)

async def delayed_assign_former_member(member: discord.Member):
    if member.bot or member.id in pending_former_checks:
        return
    pending_former_checks.add(member.id)
    try:
        await asyncio.sleep(FORMER_MEMBER_DELAY_SECONDS)

        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return
        refreshed = guild.get_member(member.id)
        if not refreshed:
            return
        if _has_lifetime_role(refreshed):
            e = _make_dyno_embed(
                member=refreshed,
                description=f"{refreshed.mention} has Lifetime access; skipping Former Member/extra role assignment",
                footer=f"ID: {refreshed.id}",
                color=0x57F287,
            )
            await log_role_event(embed=e)
            return

        if has_member_role(refreshed):
            e = _make_dyno_embed(
                member=refreshed,
                description=f"{refreshed.mention} regained the member role; not marking as Former Member",
                footer=f"ID: {refreshed.id}",
                color=0x57F287,
            )
            await log_role_event(embed=e)
            return

        if not has_former_member_role(refreshed):
            role = guild.get_role(FORMER_MEMBER_ROLE)
            if role is None:
                err = _make_dyno_embed(
                    member=refreshed,
                    description=f"{refreshed.mention} former-member role missing",
                    footer=f"ID: {refreshed.id}",
                    color=0xED4245,
                )
                err.add_field(name="Missing role", value=_fmt_role(FORMER_MEMBER_ROLE, guild)[:1024], inline=False)
                await log_role_event(embed=err)
            else:
                try:
                    await refreshed.add_roles(role, reason="Lost member role; mark as former member")
                    e = _make_dyno_embed(
                        member=refreshed,
                        description=f"{refreshed.mention} was given the {role.name} role",
                        footer=f"ID: {refreshed.id}",
                        color=0xFEE75C,
                    )
                    e.add_field(name="Reason", value="Member role not regained within grace period", inline=False)
                    await log_role_event(embed=e)
                except Exception as e:
                    await log_role_event(f"⚠️ **Failed to assign Former Member role** to {_fmt_user(refreshed)}\n   ❌ Error: `{e}`")

        extra_role = guild.get_role(1224748748920328384)
        if extra_role and extra_role not in refreshed.roles:
            try:
                await refreshed.add_roles(extra_role, reason="Lost member role; add extra role")
                e = _make_dyno_embed(
                    member=refreshed,
                    description=f"{refreshed.mention} was given the {extra_role.name} role",
                    footer=f"ID: {refreshed.id}",
                    color=0xFEE75C,
                )
                await log_role_event(embed=e)
            except Exception as e:
                await log_role_event(f"⚠️ **Failed to assign extra role** to {_fmt_user(refreshed)}\n   ❌ Error: `{e}`")
    finally:
        pending_former_checks.discard(member.id)

# -----------------------------
# Scheduler loop
# -----------------------------
@tasks.loop(seconds=10)
async def scheduler_loop():
    try:
        if not bot.is_ready():
            return
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return

        for uid, payload in list(queue_state.items()):
            try:
                day_key = payload.get("current_day")
                next_send = payload.get("next_send", "")

                if not day_key or not is_due(next_send):
                    continue

                member = guild.get_member(int(uid))
                if not member:
                    mark_cancelled(int(uid), "left_guild")
                    await log_other(f"👋 User `{uid}` left guild — sequence cancelled")
                    continue

                if has_cancel_role(member):
                    cancel_roles = []
                    if ROLE_CANCEL_A and any(r.id == ROLE_CANCEL_A for r in member.roles):
                        cancel_roles.append(_fmt_role(ROLE_CANCEL_A, guild))
                    if ROLE_CANCEL_B and any(r.id == ROLE_CANCEL_B for r in member.roles):
                        cancel_roles.append(_fmt_role(ROLE_CANCEL_B, guild))
                    cancel_info = ", ".join(cancel_roles) if cancel_roles else "cancel role"
                    mark_cancelled(member.id, "cancel_role_present")
                    await log_other(f"🛑 Cancelled for {_fmt_user(member)} — {cancel_info} present (during scheduler)")
                    continue

                await send_day(member, day_key)

                # user may have been popped inside send_day (forbidden/cancel/finish)
                if str(member.id) in queue_state:
                    prev = day_key
                    schedule_next(member.id, day_key)

                    # schedule_next may have finished & popped; guard read
                    nxt = queue_state.get(str(member.id))
                    if nxt:
                        target_ch = log_other if prev != "day_1" else log_first
                        next_send_iso = nxt.get("next_send", "")
                        next_dt = _parse_dt_any(next_send_iso)
                        when = _fmt_discord_ts_any(next_send_iso, "F")
                        sched_embed = _make_dyno_embed(
                            member=member,
                            description=f"{member.mention} {nxt.get('current_day', 'next').strip()} scheduled for {when}",
                            footer=f"ID: {member.id}",
                            color=0x5865F2,
                            timestamp=next_dt or datetime.now(timezone.utc),
                        )
                        await target_ch(embed=sched_embed)
            except Exception as e:
                await log_other(f"⚠️ scheduler_loop user error for uid `{uid}`: `{e}`")
    except Exception as e:
        await log_other(f"❌ scheduler_loop tick error: `{e}`")

@scheduler_loop.error
async def scheduler_loop_error(error):
    await log_other(f"🔁 scheduler_loop crashed: `{error}` — restarting in 5s")
    with suppress(Exception):
        scheduler_loop.cancel()
    await asyncio.sleep(5)
    with suppress(Exception):
        scheduler_loop.start()

# -----------------------------
# Whop membership sync job
# -----------------------------
@tasks.loop(hours=6)  # Default interval, changed in on_ready() from config
async def sync_whop_memberships():
    """Periodically sync Discord roles with Whop membership status"""
    global whop_api_client
    
    if not whop_api_client:
        return  # API client not available
    
    if not bot.is_ready():
        return
    
    # Check if sync is enabled
    if not WHOP_API_CONFIG.get("enable_sync", True):
        return
    
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return
    
    log.info("Starting Whop membership sync...")
    synced_count = 0
    error_count = 0
    relinked_count = 0
    would_remove_count = 0
    auto_healed_count = 0
    checked_with_mid = 0
    cancel_scheduled_count = 0
    status_counts: dict[str, int] = {}
    actual_status_counts: dict[str, int] = {}
    missing_membership_data = 0
    report_rows: list[dict[str, str]] = []
    
    # Get all members with Member role
    member_role = guild.get_role(ROLE_CANCEL_A)
    if not member_role:
        log.warning("Member role not found, skipping sync")
        return
    
    members_to_check = [m for m in guild.members if member_role in m.roles]
    log.info(f"Checking {len(members_to_check)} members with Member role...")

    # Optional: live progress in Neo (single message edited periodically).
    progress_msg: discord.Message | None = None
    last_progress_edit = 0.0
    mirror_ch: discord.TextChannel | None = None
    try:
        mirror_enabled = bool(WHOP_API_CONFIG.get("sync_summary_enabled", False))
    except Exception:
        mirror_enabled = False
    try:
        mirror_gid = int(WHOP_API_CONFIG.get("sync_summary_output_guild_id") or 0) if mirror_enabled else 0
    except Exception:
        mirror_gid = 0
    mirror_name = str(WHOP_API_CONFIG.get("sync_summary_output_channel_name") or "").strip()
    if mirror_gid and mirror_name:
        g2 = bot.get_guild(int(mirror_gid))
        if g2:
            mirror_ch = await _get_or_create_text_channel(g2, name=mirror_name, category_id=STAFF_ALERTS_CATEGORY_ID)
            if isinstance(mirror_ch, discord.TextChannel):
                # Reuse an existing progress message (prevents spam across restarts/reruns).
                try:
                    me_id = int(getattr(bot.user, "id", 0) or 0)
                except Exception:
                    me_id = 0

                def _is_progress_message(m: discord.Message) -> bool:
                    try:
                        if not m or not me_id:
                            return False
                        if int(getattr(getattr(m, "author", None), "id", 0) or 0) != me_id:
                            return False
                        c = str(getattr(m, "content", "") or "").strip()
                        return c.startswith("Fetchall: Whop Sync")
                    except Exception:
                        return False

                with suppress(Exception):
                    async for m0 in mirror_ch.history(limit=25):
                        if _is_progress_message(m0):
                            progress_msg = m0
                            break

                txt = _progress_text(
                    label="Whop Sync",
                    step=(1, 1),
                    done=0,
                    total=len(members_to_check),
                    stats={"linked": 0, "errors": 0, "would_remove": 0, "removed": 0, "relinked": 0},
                    stage="start",
                )
                try:
                    if progress_msg:
                        # Always clear embeds; we only want ONE clean progress line.
                        await progress_msg.edit(content=txt, embed=None)
                    else:
                        progress_msg = await mirror_ch.send(content=txt, allowed_mentions=discord.AllowedMentions.none())
                    last_progress_edit = time.time()
                except Exception:
                    progress_msg = None

    auto_heal_enabled = bool(WHOP_API_CONFIG.get("auto_heal_add_members_role", False))
    # Safety defaults:
    # - Role removals are dangerous; default to disabled unless explicitly enabled.
    # - Sync logging can be noisy; default to silent unless explicitly disabled.
    enforce_removals = bool(WHOP_API_CONFIG.get("enforce_role_removals", False))
    sync_silent = bool(WHOP_API_CONFIG.get("sync_silent", True))
    try:
        auto_heal_min_spent = float(WHOP_API_CONFIG.get("auto_heal_min_total_spent_usd", 1.0))
    except Exception:
        auto_heal_min_spent = 1.0

    async def _find_entitled_membership_for_same_user(
        *,
        membership_data: dict | None,
        membership_id: str,
        require_active: bool,
        now_dt: datetime,
    ) -> tuple[str, dict]:
        """Find an entitled membership for the same Whop user (handles multi-membership users).

        Returns (membership_id, membership_dict) or ("", {}).
        """
        if not isinstance(membership_data, dict):
            return ("", {})
        mid = str(membership_id or "").strip()
        if not mid:
            return ("", {})

        product_title = ""
        if isinstance(membership_data.get("product"), dict):
            product_title = str(membership_data["product"].get("title") or "").strip()

        whop_user_id = ""
        u = membership_data.get("user")
        if isinstance(u, str):
            whop_user_id = u.strip()
        elif isinstance(u, dict):
            whop_user_id = str(u.get("id") or u.get("user_id") or "").strip()
        if not whop_user_id:
            btmp = await _fetch_whop_brief_by_membership_id(mid)
            whop_user_id = str((btmp or {}).get("whop_user_id") or "").strip()
        if not whop_user_id:
            return ("", {})

        candidates = await whop_api_client.get_user_memberships(whop_user_id)
        # Prefer active, then trialing (unless require_active).
        status_sets = (("active",),) if require_active else (("active",), ("trialing",))
        for desired in status_sets:
            for m2 in (candidates or []):
                if not isinstance(m2, dict):
                    continue
                # Safety: some /memberships listings can be broader than intended.
                # Only consider memberships that clearly belong to the same Whop user.
                u2_id = ""
                u2 = m2.get("user")
                if isinstance(u2, str):
                    u2_id = u2.strip()
                elif isinstance(u2, dict):
                    u2_id = str(u2.get("id") or u2.get("user_id") or "").strip()
                if not u2_id:
                    m2m = m2.get("member")
                    if isinstance(m2m, dict):
                        u3 = m2m.get("user")
                        if isinstance(u3, str):
                            u2_id = u3.strip()
                        elif isinstance(u3, dict):
                            u2_id = str(u3.get("id") or u3.get("user_id") or "").strip()
                if not u2_id or u2_id != whop_user_id:
                    continue
                mid2 = str(m2.get("id") or m2.get("membership_id") or "").strip()
                if not mid2 or mid2 == mid:
                    continue
                st2 = str(m2.get("status") or "").strip().lower()
                if st2 not in desired:
                    continue
                if product_title and isinstance(m2.get("product"), dict):
                    t2 = str(m2["product"].get("title") or "").strip()
                    if t2 and t2 != product_title:
                        continue
                entitled2, _until2, _why2 = await whop_api_client.is_entitled_until_end(
                    mid2,
                    m2,
                    cache_path=str(PAYMENT_CACHE_FILE),
                    monthly_days=30,
                    grace_days=3,
                    now=now_dt,
                )
                if entitled2:
                    return (mid2, m2)
        return ("", {})
    
    for idx, member in enumerate(members_to_check, start=1):
        try:
            # Check membership status via API (membership_id-based; avoids mismatched users)
            membership_id = _membership_id_from_history(member.id)
            if not membership_id:
                continue
            checked_with_mid += 1
            verification = await whop_api_client.verify_membership_status(membership_id, "active")

            # Cancellation scheduled signal (active/trialing but cancel_at_period_end=true).
            # This does NOT DM users; it's staff-only visibility (case channel + member status logs).
            membership_data = verification.get("membership_data") if isinstance(verification, dict) else None
            row_action = "none"
            row_status = ""
            row_actual_status = ""
            row_cape = ""
            row_renew_end = ""
            if isinstance(membership_data, dict):
                row_status = str(membership_data.get("status") or "").strip().lower()
                row_cape = "true" if (membership_data.get("cancel_at_period_end") is True) else "false"
                row_renew_end = str(membership_data.get("renewal_period_end") or "").strip()
            if isinstance(membership_data, dict):
                status_now = str(membership_data.get("status") or "").strip().lower()
                status_counts[status_now or "unknown"] = int(status_counts.get(status_now or "unknown", 0)) + 1
                cape_now = membership_data.get("cancel_at_period_end")
                if cape_now is True and status_now in ("active", "trialing"):
                    cancel_scheduled_count += 1
                    if not row_action or row_action == "none":
                        row_action = "set_to_cancel"
                    # Sync job is report-only for cancellation scheduling; real-time cards come from Whop webhooks.
                    report_rows.append(
                        {
                            "discord_id": str(member.id),
                            "membership_id": str(membership_id),
                            "status": row_status or status_now,
                            "actual_status": "",
                            "cancel_at_period_end": row_cape,
                            "renewal_period_end": row_renew_end,
                            "action": row_action,
                        }
                    )
                    continue
            
            if not verification["matches"]:
                actual_status = verification["actual_status"]
                st = str(actual_status or "").strip().lower() or "unknown"
                actual_status_counts[st] = int(actual_status_counts.get(st, 0)) + 1
                row_actual_status = st
                
                # If API says canceled but user has Member role, remove it
                if actual_status in ("canceled", "completed", "past_due", "unpaid"):
                    # Lifetime members keep access indefinitely.
                    if _has_lifetime_role(member):
                        continue

                    # Fail-closed: only attempt "relink to active membership" when we can strongly
                    # verify the membership belongs to this Discord ID.
                    link_verified = False

                    # Guardrail: ensure the cached membership_id actually belongs to this Discord ID.
                    # If it doesn't, do not remove roles (prevents accidental removals from stale/bad links).
                    try:
                        mobj = membership_data if isinstance(membership_data, dict) else None
                        whop_member_id = ""
                        if isinstance(mobj, dict):
                            mref = mobj.get("member")
                            if isinstance(mref, dict):
                                whop_member_id = str(mref.get("id") or mref.get("member_id") or "").strip()
                            elif isinstance(mref, str):
                                whop_member_id = mref.strip()
                            if not whop_member_id:
                                whop_member_id = str(mobj.get("member_id") or "").strip()
                        if whop_member_id and whop_member_id.startswith("mber_"):
                            rec = await whop_api_client.get_member_by_id(whop_member_id)
                            did = extract_discord_id_from_whop_member_record(rec) if isinstance(rec, dict) else ""
                            if did and did.isdigit() and int(did) != int(member.id):
                                if not sync_silent:
                                    await log_other(
                                        f"⚠️ **Whop Sync skipped removal (mismatch)** {_fmt_user(member)}\n"
                                        f"   membership_id: `{membership_id}`\n"
                                        f"   whop_member_id: `{whop_member_id}`\n"
                                        f"   whop_discord_id: `{did}`"
                                    )
                                continue
                            if did and did.isdigit() and int(did) == int(member.id):
                                link_verified = True
                    except Exception:
                        pass

                    # Guardrail #2: user can have multiple memberships; the cached membership_id might be an old canceled one.
                    # If the Whop user currently has an ACTIVE/TRIALING entitled membership for the same product, keep access and relink.
                    try:
                        if not link_verified:
                            raise RuntimeError("link_not_verified")
                        now_dt = _now()
                        mid2, m2 = await _find_entitled_membership_for_same_user(
                            membership_data=membership_data if isinstance(membership_data, dict) else None,
                            membership_id=membership_id,
                            require_active=False,
                            now_dt=now_dt,
                        )
                        if mid2 and isinstance(m2, dict):
                            st2 = str(m2.get("status") or "").strip().lower()
                            record_member_whop_summary(
                                member.id,
                                {},
                                event_type="sync.relink_active_membership",
                                membership_id=mid2,
                            )
                            relinked_count += 1
                            row_action = "relinked_keep_access"
                            if not sync_silent:
                                await log_other(
                                    f"✅ **Whop Sync kept access (newer active membership)** {_fmt_user(member)}\n"
                                    f"   old_membership_id: `{membership_id}` (status `{actual_status}`)\n"
                                    f"   new_membership_id: `{mid2}` (status `{st2}`)"
                                )
                            continue
                    except Exception:
                        pass

                    entitled, _until_dt, _reason = await whop_api_client.is_entitled_until_end(
                        membership_id,
                        membership_data if isinstance(membership_data, dict) else None,
                        cache_path=str(PAYMENT_CACHE_FILE),
                        monthly_days=30,
                        grace_days=3,
                        now=_now(),
                    )
                    if entitled:
                        continue
                    if not enforce_removals:
                        would_remove_count += 1
                        row_action = "would_remove_role"
                        if not sync_silent:
                            await log_other(
                                f"⚠️ **Whop Sync would remove role (enforcement disabled)** {_fmt_user(member)}\n"
                                f"   API Status: `{actual_status}`\n"
                                f"   membership_id: `{membership_id}`\n"
                                f"   Removed: {_fmt_role(ROLE_CANCEL_A, guild)}"
                            )
                        continue
                    await member.remove_roles(
                        member_role, 
                        reason=f"Whop sync: Status is {actual_status}"
                    )
                    if not sync_silent:
                        await log_other(
                            f"🔄 **Sync Removed Role:** {_fmt_user(member)}\n"
                            f"   API Status: `{actual_status}`\n"
                            f"   Removed: {_fmt_role(ROLE_CANCEL_A, guild)}"
                        )
                    synced_count += 1
                    row_action = "removed_role"
        except Exception as e:
            error_count += 1
            log.error(f"Error syncing member {member.id}: {e}")
            continue

        if membership_data is None:
            missing_membership_data += 1

        # Record one audit row per checked membership.
        report_rows.append(
            {
                "discord_id": str(member.id),
                "membership_id": str(membership_id),
                "status": row_status,
                "actual_status": row_actual_status,
                "cancel_at_period_end": row_cape,
                "renewal_period_end": row_renew_end,
                "action": row_action,
            }
        )

        # Periodic progress (console + Neo progress message).
        if idx % 50 == 0 or idx == len(members_to_check):
            log.info(
                "[Whop Sync] progress %s/%s (linked=%s errors=%s would_remove=%s removed=%s relinked=%s)",
                idx,
                len(members_to_check),
                checked_with_mid,
                error_count,
                would_remove_count,
                synced_count,
                relinked_count,
            )
            if progress_msg and mirror_ch:
                now_s = time.time()
                if (now_s - last_progress_edit) >= 10.0:
                    last_progress_edit = now_s
                    with suppress(Exception):
                        txt = _progress_text(
                            label="Whop Sync",
                            step=(1, 1),
                            done=idx,
                            total=len(members_to_check),
                            stats={
                                "linked": checked_with_mid,
                                "errors": error_count,
                                "would_remove": would_remove_count,
                                "removed": synced_count,
                                "relinked": relinked_count,
                            },
                            stage="scan",
                        )
                        await progress_msg.edit(content=txt, embed=None)

    # Auto-heal: add Member role for users missing it, when Whop shows active+entitled access and paid (>$1).
    if auto_heal_enabled:
        healed = 0
        try:
            hist_db = _load_member_history()
            ids = [k for k in (hist_db.keys() if isinstance(hist_db, dict) else []) if str(k).isdigit()]
        except Exception:
            ids = []

        # Compute duplicate membership_ids once; skip them (fail-closed).
        dup_mids: set[str] = set()
        try:
            if isinstance(hist_db, dict):
                counts: dict[str, int] = {}
                for _did, _rec in hist_db.items():
                    if not isinstance(_rec, dict):
                        continue
                    wh = _rec.get("whop") if isinstance(_rec.get("whop"), dict) else None
                    _mid = str((wh or {}).get("last_membership_id") or "").strip()
                    if not _mid:
                        continue
                    counts[_mid] = counts.get(_mid, 0) + 1
                dup_mids = {m for m, c in counts.items() if c > 1}
        except Exception:
            dup_mids = set()

        for did in ids:
            try:
                uid = int(str(did))
            except Exception:
                continue
            mbr = guild.get_member(uid)
            if not mbr or mbr.bot:
                continue
            if member_role in mbr.roles:
                continue

            # Resolve current best membership for this Discord ID.
            mid = str(_membership_id_from_history(uid) or "").strip()
            if not mid:
                continue
            if dup_mids and mid in dup_mids:
                continue
            try:
                mdata = await whop_api_client.get_membership_by_id(mid)
            except Exception:
                mdata = None
            if not isinstance(mdata, dict):
                continue

            now_dt = _now()
            # Prefer active entitled memberships.
            mid_active, m_active = await _find_entitled_membership_for_same_user(
                membership_data=mdata,
                membership_id=mid,
                require_active=True,
                now_dt=now_dt,
            )
            if not mid_active or not isinstance(m_active, dict):
                # Fallback: if the cached membership itself is active+entitled, allow it.
                st0 = str(mdata.get("status") or "").strip().lower()
                ent0, _until0, _why0 = await whop_api_client.is_entitled_until_end(
                    mid,
                    mdata,
                    cache_path=str(PAYMENT_CACHE_FILE),
                    monthly_days=30,
                    grace_days=3,
                    now=now_dt,
                )
                if st0 == "active" and ent0:
                    mid_active, m_active = (mid, mdata)
                else:
                    continue

            end_dt = whop_api_client.access_end_dt_from_membership(m_active)
            if not end_dt or end_dt <= now_dt:
                continue

            # Check paid threshold using Whop brief (total_spent best-effort).
            brief = _whop_summary_for_member(mbr.id)
            if not (isinstance(brief, dict) and brief):
                brief = await _fetch_whop_brief_by_membership_id(mid_active)
            spent = float(usd_amount((brief or {}).get("total_spent")))
            if spent <= float(auto_heal_min_spent):
                continue

            # Only add if Whop says active (no trial-only auto-heal).
            st_final = str(m_active.get("status") or "").strip().lower()
            if st_final != "active":
                continue

            try:
                # Prevent startup/sync-driven role adds from spamming member-status channels.
                _suppress_member_update_logs(mbr.id, seconds=300.0)
                await mbr.add_roles(member_role, reason="Whop auto-heal: active+entitled membership")
                record_member_whop_summary(
                    mbr.id,
                    {},
                    event_type="sync.auto_heal_add_member_role",
                    membership_id=mid_active,
                )
                healed += 1
                if not sync_silent:
                    await log_other(
                        f"✅ **Auto-heal added Members role** {_fmt_user(mbr)}\n"
                        f"   membership_id: `{mid_active}`\n"
                        f"   total_spent: `${spent:.2f}`"
                    )
            except Exception:
                continue

        if healed and (not sync_silent):
            await log_other(f"✅ **Whop Auto-heal complete** — added Members role for {healed} user(s)")
        auto_healed_count = healed

    # Optional: DM a sync summary report (throttled).
    if (not sync_silent) and bool(WHOP_API_CONFIG.get("startup_scan_dm_report", False)) and _should_post_sync_report():
        dm_uid = int(REPORTING_CONFIG.get("dm_user_id") or 0)
        if dm_uid:
            try:
                e = discord.Embed(
                    title="RSCheckerbot • Whop Startup Scan",
                    description="Summary of the startup/6-hour Whop sync sweep (audit-first).",
                    color=0x5865F2,
                    timestamp=datetime.now(timezone.utc),
                )
                e.add_field(name="Members checked", value=str(len(members_to_check)), inline=True)
                e.add_field(name="Relinked to active membership", value=str(relinked_count), inline=True)
                e.add_field(name="Auto-heal added Members", value=str(auto_healed_count), inline=True)
                e.add_field(name="Would remove (enforcement disabled)", value=str(would_remove_count), inline=True)
                e.add_field(name="Removed", value=str(synced_count), inline=True)
                await _dm_user(dm_uid, embed=e)
            except Exception:
                pass
    
    log.info(f"Sync complete: {synced_count} roles updated, {error_count} errors")
    if (not sync_silent) and (synced_count > 0 or error_count > 0):
        await log_other(
            f"🔄 **Whop Sync Complete**\n"
            f"   Members checked: {len(members_to_check)}\n"
            f"   Roles updated: {synced_count}\n"
            f"   Errors: {error_count}"
        )

    # Optional: mirror an accurate sync summary to Neo Test Server (does not affect main guild output).
    if mirror_ch:
        # Status rollups are from THIS sync pass.
        def _c(d: dict[str, int], *keys: str) -> int:
            return int(sum(int(d.get(k, 0) or 0) for k in keys))

        users = int(checked_with_mid or 0) or int(len(members_to_check) or 0)
        active = _c(status_counts, "active")
        trialing = _c(status_counts, "trialing")
        canceled = _c(status_counts, "canceled") + _c(actual_status_counts, "canceled", "completed")
        past_due = _c(status_counts, "past_due", "unpaid") + _c(actual_status_counts, "past_due", "unpaid")
        churn_pct = (float(canceled) / float(users) * 100.0) if users else 0.0

        e = discord.Embed(
            title="Whop Sync Summary (mirror)",
            description="Latest Whop sync sweep (members with Member role; membership_id-linked where available).",
            color=0x5865F2,
            timestamp=datetime.now(timezone.utc),
        )
        e.add_field(name="Members with Member role", value=str(len(members_to_check)), inline=True)
        e.add_field(name="Linked memberships", value=str(checked_with_mid), inline=True)
        e.add_field(name="Errors", value=str(error_count), inline=True)
        e.add_field(name="Active", value=str(active), inline=True)
        e.add_field(name="Trialing", value=str(trialing), inline=True)
        e.add_field(name="Set to cancel", value=str(cancel_scheduled_count), inline=True)
        e.add_field(name="Canceled / completed", value=str(canceled), inline=True)
        e.add_field(name="Past due / unpaid", value=str(past_due), inline=True)
        e.add_field(name="Churn", value=f"{churn_pct:.2f}%", inline=True)
        e.add_field(name="Role updates", value=str(synced_count), inline=True)
        e.add_field(name="Would remove (enforcement disabled)", value=str(would_remove_count), inline=True)
        e.add_field(name="Relinked", value=str(relinked_count), inline=True)
        e.add_field(name="Auto-healed", value=str(auto_healed_count), inline=True)
        e.add_field(name="Missing membership_data", value=str(missing_membership_data), inline=True)
        e.set_footer(text="RSCheckerbot • Whop Sync")

        # Optional downloadable report attachment (send exactly once: file OR embed-only fallback).
        try:
            attach = bool(WHOP_API_CONFIG.get("sync_summary_attach_report", False))
        except Exception:
            attach = False
        try:
            fmt = str(WHOP_API_CONFIG.get("sync_summary_report_format") or "csv").strip().lower()
        except Exception:
            fmt = "csv"

        sent_msg: discord.Message | None = None
        if attach and report_rows and fmt == "csv":
            try:
                buf = io.StringIO()
                w = csv.DictWriter(
                    buf,
                    fieldnames=[
                        "discord_id",
                        "membership_id",
                        "status",
                        "actual_status",
                        "cancel_at_period_end",
                        "renewal_period_end",
                        "action",
                    ],
                )
                w.writeheader()
                for r in report_rows:
                    w.writerow({k: str(r.get(k, "") or "") for k in w.fieldnames})
                data = buf.getvalue().encode("utf-8")
                f = discord.File(fp=io.BytesIO(data), filename="whop-sync-report.csv")
                sent_msg = await mirror_ch.send(embed=e, file=f, allowed_mentions=discord.AllowedMentions.none())
            except Exception as ex:
                log.warning("[Whop Sync] mirror send with file failed: %s", str(ex)[:240])
                sent_msg = None

        if sent_msg is None:
            with suppress(Exception):
                sent_msg = await mirror_ch.send(embed=e, allowed_mentions=discord.AllowedMentions.none())

        # Finalize progress message (content only; never embed).
        with suppress(Exception):
            if progress_msg:
                txt = _progress_text(
                    label="Whop Sync",
                    step=(1, 1),
                    done=len(members_to_check),
                    total=len(members_to_check),
                    stats={"linked": checked_with_mid, "errors": error_count},
                    stage="complete",
                )
                await progress_msg.edit(content=txt, embed=None)


# -----------------------------
# Whop webhook utilities (movement logs + dispute/resolution cases)
# -----------------------------
_WHOP_API_EVENTS_LOCK: asyncio.Lock = asyncio.Lock()
_WHOP_API_EVENTS_LOG_CH: discord.TextChannel | None = None


def _load_whop_api_events_state() -> dict:
    st = load_json(WHOP_API_EVENTS_STATE_FILE)
    return st if isinstance(st, dict) else {}


def _save_whop_api_events_state(st: dict) -> None:
    try:
        save_json(WHOP_API_EVENTS_STATE_FILE, st if isinstance(st, dict) else {})
    except Exception:
        return


def _extract_discord_id_from_connected(s: str) -> int:
    try:
        m = re.search(r"\b(\d{17,19})\b", str(s or ""))
        return int(m.group(1)) if m else 0
    except Exception:
        return 0


def _payment_issue_bucket_from_payment(p: dict) -> str:
    """Return 'dispute' | 'resolution' | '' for a payment record (best-effort)."""
    if not isinstance(p, dict) or not p:
        return ""
    txt = " ".join(
        [
            str(p.get("status") or ""),
            str(p.get("substatus") or ""),
            str(p.get("billing_reason") or ""),
            str(p.get("failure_message") or ""),
            str(p.get("reason") or ""),
            str(p.get("note") or ""),
        ]
    ).strip()
    low = txt.lower()
    # Dispute-ish
    if any(k in low for k in ("dispute", "disputed", "chargeback", "under review", "under_review")):
        return "dispute"
    # Resolution-ish
    if any(k in low for k in ("resolution", "resolved", "won", "lost")):
        return "resolution"
    return ""


def _bucket_for_dispute_status(status: str) -> str:
    s = str(status or "").strip().lower()
    if not s:
        return "dispute"
    if any(k in s for k in ("won", "lost", "resolved", "closed", "settled", "completed")):
        return "resolution"
    return "dispute"


def _payment_id_any(p: dict) -> str:
    try:
        pid = str(p.get("id") or p.get("payment_id") or p.get("payment") or "").strip()
        if isinstance(p.get("payment"), dict):
            pid = str(p["payment"].get("id") or p["payment"].get("payment_id") or "").strip() or pid
        return pid
    except Exception:
        return ""


async def _maybe_open_dispute_resolution_case(
    *,
    guild: discord.Guild,
    mid: str,
    updated_at: str,
    brief: dict,
    cases: dict,
    did: int = 0,
    member_obj: discord.Member | None = None,
    issue_override: str = "",
    case_key_override: str = "",
    pay_override: dict | None = None,
    extra_topic: str = "",
    always_post: bool = False,
    extra_fields: list[tuple[str, str]] | None = None,
) -> None:
    """Open a per-case channel for dispute/resolution payments (best-effort)."""
    if not isinstance(cases, dict):
        return
    if int(DISPUTE_CASE_CATEGORY_ID or 0) <= 0 and int(RESOLUTION_CASE_CATEGORY_ID or 0) <= 0:
        return
    mid_s = str(mid or "").strip()
    if not mid_s:
        return

    pay = pay_override if isinstance(pay_override, dict) else None
    if pay is None:
        pay = await _best_payment_for_membership(mid_s, limit=25)
    issue = str(issue_override or "").strip().lower() or (_payment_issue_bucket_from_payment(pay) if isinstance(pay, dict) else "")
    if issue not in {"dispute", "resolution"}:
        return

    cat_id = int(DISPUTE_CASE_CATEGORY_ID) if issue == "dispute" else int(RESOLUTION_CASE_CATEGORY_ID)
    if cat_id <= 0:
        return
    pid = _payment_id_any(pay) if isinstance(pay, dict) else ""
    key = str(case_key_override or "").strip() or f"rschecker_whop_case:{issue}:mid={mid_s}:pid={pid or updated_at or 'unknown'}"

    # Resolve Discord ID if missing
    did_i = int(did or 0)
    if did_i <= 0:
        did_i = _extract_discord_id_from_connected(str((brief or {}).get("connected_discord") or ""))

    suffix = (pid[-6:] if pid else mid_s[-6:]).lower()
    ch_name = f"{issue}-{suffix}"
    topic = (
        f"rschecker_whop_case issue={issue}\n"
        f"mid={mid_s}\n"
        f"pid={pid or '—'}\n"
        f"did={did_i or '—'}\n"
        f"email={str((brief or {}).get('email') or '—').strip()}\n"
        f"product={str((brief or {}).get('product') or '—').strip()}\n"
    )
    if extra_topic and str(extra_topic).strip():
        topic = (topic + "\n" + str(extra_topic).strip()).strip()
    case_ch = await _ensure_whop_case_channel(
        guild=guild,
        category_id=cat_id,
        case_key=key,
        channel_name=ch_name,
        topic=topic,
    )
    if not isinstance(case_ch, discord.TextChannel):
        return

    first_seen = key not in cases
    cases[key] = int(case_ch.id)

    # Starter / update card (silent)
    if (first_seen or always_post):
        with suppress(Exception):
            ecase = discord.Embed(
                title=("⚠️ Dispute Case" if issue == "dispute" else "🟡 Resolution Case"),
                color=(0xED4245 if issue == "dispute" else 0xFEE75C),
                timestamp=datetime.now(timezone.utc),
            )
            mname = str(getattr(member_obj, "display_name", "") or "").strip() if member_obj else ""
            if not mname:
                mname = str((brief or {}).get("user_name") or "").strip() or "—"
            ecase.add_field(name="Member", value=mname[:1024], inline=True)
            ecase.add_field(
                name="Discord ID",
                value=(f"`{int(member_obj.id)}`" if member_obj else (f"`{did_i}`" if did_i else "—")),
                inline=True,
            )
            ecase.add_field(name="Membership ID", value=str(mid_s)[:1024], inline=False)
            ecase.add_field(name="Membership", value=str((brief or {}).get("product") or "—")[:1024], inline=True)
            ecase.add_field(name="Status", value=str((brief or {}).get("status") or "—")[:1024], inline=True)
            pay_txt = " ".join(
                [
                    str((pay or {}).get("status") or ""),
                    str((pay or {}).get("substatus") or ""),
                    str((pay or {}).get("billing_reason") or ""),
                    str((pay or {}).get("failure_message") or ""),
                ]
            ).strip()
            if pay_txt:
                ecase.add_field(name="Payment", value=pay_txt[:1024], inline=False)
            dash = str((brief or {}).get("dashboard_url") or "").strip()
            if dash and dash != "—":
                ecase.add_field(name="Whop Dashboard", value=dash[:1024], inline=False)
            if isinstance(extra_fields, list):
                for k, v in extra_fields:
                    kk = str(k or "").strip()
                    vv = str(v or "").strip()
                    if kk and vv:
                        ecase.add_field(name=kk[:256], value=vv[:1024], inline=False)
            ecase.set_footer(text="RSCheckerbot • Whop API")
            # Only mention when the member is in-guild (avoids @unknown-user).
            mention = f"<@{int(member_obj.id)}>" if member_obj else ""
            await case_ch.send(
                content=mention,
                embed=ecase,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                silent=True,
            )
    if first_seen:
        with suppress(Exception):
            await _whop_api_events_log(f"[Whop Case] opened issue={issue} mid={mid_s} ch=#{case_ch.name} ({case_ch.id})")


async def _whop_api_events_log(msg: str) -> None:
    """Best-effort Whop movement/webhook receipt log (Neo).

    - Uses embeds (minimal, no pings).
    - When sending via Discord webhook, overrides username to avoid confusing names (e.g. "Captain Hook").
    """
    global _WHOP_API_EVENTS_LOG_CH
    if not WHOP_MOVEMENT_LOG_ENABLED:
        return
    if not bot.is_ready():
        return
    if not msg or not str(msg).strip():
        return
    raw = str(msg).strip()

    def _kv_pairs(s: str) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            for m in re.finditer(r"(?<!\w)([a-zA-Z_]+)=([^\s]+)", s):
                k = str(m.group(1) or "").strip()
                v = str(m.group(2) or "").strip()
                if k and v:
                    out[k] = v
        except Exception:
            return {}
        return out

    def _build_embed(s: str) -> discord.Embed:
        kv = _kv_pairs(s)
        kind = str(kv.get("kind") or "").strip()
        evt = str(kv.get("type") or kv.get("event") or "").strip()
        linked = str(kv.get("linked") or "").strip().lower()
        did = str(kv.get("did") or "").strip()
        mid = str(kv.get("mid") or kv.get("membership_id") or "").strip()
        issue = str(kv.get("issue") or "").strip()

        title = "Whop movement"
        if s.startswith("[Whop Webhook][detected]"):
            title = "Whop detected"
        elif s.startswith("[Whop Webhook]"):
            title = "Whop webhook received"
        elif s.startswith("[Whop Case]"):
            title = "Whop case update"

        low = f"{kind} {evt} {s}".lower()
        color = 0x5865F2
        if any(x in low for x in ("payment_failed", "past_due", "unpaid", "dispute", "chargeback", "invoice_past_due")):
            color = 0xED4245
        elif any(x in low for x in ("cancellation", "cancel", "canceling", "cancelled", "canceled", "deactivated", "expired")):
            color = 0xFEE75C
        elif any(x in low for x in ("succeeded", "paid", "access_restored", "payment_resumed", "restored")):
            color = 0x57F287

        e = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
        if evt:
            e.add_field(name="Event", value=evt[:1024], inline=True)
        if kind:
            e.add_field(name="Kind", value=kind[:1024], inline=True)
        if issue:
            e.add_field(name="Issue", value=issue[:1024], inline=True)
        if mid:
            e.add_field(name="Membership ID", value=mid[:1024], inline=False)
        if did and did.isdigit():
            e.add_field(name="Discord ID", value=f"`{did}`", inline=True)
        elif linked:
            e.add_field(name="Linked", value=("yes" if linked == "yes" else "no"), inline=True)

        # If the message references a channel id "(123...)", surface it.
        ch_id = ""
        with suppress(Exception):
            m2 = re.search(r"\((\d{10,20})\)", s)
            if m2:
                ch_id = str(m2.group(1) or "").strip()
        if ch_id:
            e.add_field(name="Channel ID", value=f"`{ch_id}`", inline=True)

        # Footer for consistency.
        e.set_footer(text="RSCheckerbot • Whop movement")
        return e

    embed: discord.Embed | None = None
    with suppress(Exception):
        embed = _build_embed(raw)

    # Optional: send via Discord webhook (best-effort; avoids channel-perms issues).
    if WHOP_MOVEMENT_LOG_WEBHOOK_URL:
        try:
            payload: dict = {
                "username": "RSCheckerbot",
                "allowed_mentions": {"parse": []},
            }
            if embed:
                payload["content"] = ""
                payload["embeds"] = [embed.to_dict()]
            else:
                payload["content"] = raw[:1900]
            async with aiohttp.ClientSession() as session:
                await session.post(
                    WHOP_MOVEMENT_LOG_WEBHOOK_URL,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=8),
                )
            return
        except Exception:
            # Fall back to channel send below.
            pass
    cid = int(WHOP_MOVEMENT_LOG_OUTPUT_CHANNEL_ID or 0)
    ch = _WHOP_API_EVENTS_LOG_CH
    if cid > 0:
        if not isinstance(ch, discord.TextChannel) or int(getattr(ch, "id", 0) or 0) != cid:
            ch2 = bot.get_channel(cid)
            if isinstance(ch2, discord.TextChannel):
                ch = ch2
            else:
                with suppress(Exception):
                    fetched = await bot.fetch_channel(cid)
                    ch = fetched if isinstance(fetched, discord.TextChannel) else None
            _WHOP_API_EVENTS_LOG_CH = ch if isinstance(ch, discord.TextChannel) else None
    else:
        # Name-based: create/find a dedicated channel in the configured guild (Neo).
        gid = int(WHOP_MOVEMENT_LOG_OUTPUT_GUILD_ID or 0)
        name = str(WHOP_MOVEMENT_LOG_OUTPUT_CHANNEL_NAME or "").strip() or "whop-movement-logs"
        g = bot.get_guild(gid) if gid else None
        if isinstance(g, discord.Guild):
            if not isinstance(ch, discord.TextChannel) or int(getattr(ch, "guild", None).id) != int(g.id) or str(getattr(ch, "name", "") or "") != name:
                with suppress(Exception):
                    ch = await _get_or_create_text_channel(g, name=name, category_id=None)
                _WHOP_API_EVENTS_LOG_CH = ch if isinstance(ch, discord.TextChannel) else None
    if not isinstance(ch, discord.TextChannel):
        return
    with suppress(Exception):
        if embed:
            await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none(), silent=True)
        else:
            await ch.send(content=raw[:1900], allowed_mentions=discord.AllowedMentions.none(), silent=True)


def _linked_hint_embed(*, title: str, color: int, brief: dict, note: str, discord_value: str = "") -> discord.Embed:
    """Staff-only embed for Whop events where no Discord member is resolved."""
    e = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    user_name = str((brief or {}).get("user_name") or "").strip() or "—"
    email = str((brief or {}).get("email") or "").strip() or "—"
    product = str((brief or {}).get("product") or "").strip() or "—"
    status = str((brief or {}).get("status") or "").strip() or "—"
    spent = str((brief or {}).get("total_spent") or "").strip() or "—"
    dash = str((brief or {}).get("dashboard_url") or "").strip() or "—"
    renew = str((brief or {}).get("renewal_window") or "").strip() or "—"
    discord_disp = str(discord_value or "").strip() or str((brief or {}).get("connected_discord") or "").strip()
    e.add_field(name="Member (Whop)", value=user_name[:1024], inline=True)
    e.add_field(name="Email", value=email[:1024], inline=True)
    # Use Whop-linked display if we have it; otherwise show "Not linked".
    e.add_field(name="Discord", value=(discord_disp[:1024] if discord_disp else "Not linked"), inline=True)
    e.add_field(name="Membership", value=product[:1024], inline=True)
    e.add_field(name="Status", value=status[:1024], inline=True)
    e.add_field(name="Total Spent (lifetime)", value=spent[:1024], inline=True)
    e.add_field(name="Whop Dashboard", value=dash[:1024], inline=False)
    if renew and renew != "—":
        e.add_field(name="Renewal Window", value=renew[:1024], inline=False)
    if note:
        e.add_field(name="Action", value=str(note)[:1024], inline=False)
    e.set_footer(text="RSCheckerbot • Whop API")
    return e


def _status_bucket(status: str) -> str:
    s = str(status or "").strip().lower()
    if s in {"past_due", "unpaid"}:
        return "payment_failed"
    if s in {"canceled", "cancelled", "completed", "expired"}:
        return "deactivated"
    if s in {"trialing", "pending"}:
        return "trialing"
    return "active"


def _classify_whop_change(prev: dict | None, cur: dict) -> str:
    """Return event kind for a membership update (best-effort)."""
    cur_status = str(cur.get("status") or "").strip().lower()
    cur_bucket = _status_bucket(cur_status)
    cur_cape = bool(cur.get("cancel_at_period_end") is True)

    if not isinstance(prev, dict) or not prev:
        # First time seeing this membership in our poller.
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
    prev_bucket = _status_bucket(prev_status)
    prev_cape = bool(prev.get("cancel_at_period_end") is True)

    if prev_bucket in {"payment_failed"} and cur_bucket in {"active", "trialing"}:
        return "access_restored"
    if cur_bucket == "payment_failed" and prev_bucket != "payment_failed":
        return "payment_failed"
    # Repeated payment failures: Whop can emit multiple "payment failed" movements while status remains past_due/unpaid.
    # Use updated_at watermark (stored in state) to treat each update as a movement.
    if cur_bucket == "payment_failed" and prev_bucket == "payment_failed":
        if str(cur.get("updated_at") or "") and str(cur.get("updated_at") or "") != str(prev.get("updated_at") or ""):
            return "payment_failed"
    if cur_bucket == "deactivated" and prev_bucket != "deactivated":
        return "deactivated"
    if (not prev_cape) and cur_cape and cur_bucket in {"active", "trialing"}:
        return "cancellation_scheduled"
    if prev_cape and (not cur_cape) and prev_bucket in {"active", "trialing"}:
        return "cancellation_removed"
    # Payment succeeded / renewal: status may stay active but renewal window advances.
    if cur_bucket in {"active", "trialing"} and prev_bucket in {"active", "trialing"}:
        cur_end = str(cur.get("renewal_period_end") or cur.get("renewal_end") or "").strip()
        prev_end = str(prev.get("renewal_period_end") or prev.get("renewal_end") or "").strip()
        if cur_end and cur_end != prev_end:
            return "payment_succeeded"
    # Generic membership update: log-only (when enabled) will capture this as movement.
    return ""


def _title_for_event(kind: str) -> tuple[str, int, str]:
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


#
# NOTE: Removed the API poll "movement watcher" and the startup dispute/resolution sweep.
# Webhooks are canonical for real-time detection, and dispute/resolution case channels are webhook-driven.
#


@sync_whop_memberships.error
async def sync_whop_memberships_error(error):
    await log_other(f"❌ Sync job error: `{error}`")
    log.error(f"Sync job error: {error}", exc_info=True)

# -----------------------------
# Events
# -----------------------------
@bot.event
async def on_ready():
    global queue_state, registry, invite_usage_cache, whop_api_client
    startup_notes: list[str] = []
    startup_kv: list[tuple[str, object]] = []
    post_startup_report = _should_post_boot()
    
    # Comprehensive startup logging
    log.info("="*60)
    log.info("  🔍 RS Checker Bot")
    log.info("="*60)
    log.info(f"[Bot] Ready as {bot.user} (ID: {bot.user.id})")
    
    queue_state = load_json(QUEUE_FILE)
    registry = load_json(REGISTRY_FILE)

    # Support tickets (Neo): initialize after config load so ticket commands + sweeper can run.
    try:
        tz_name = str((REPORTING_CONFIG or {}).get("timezone") or "UTC").strip() or "UTC"
    except Exception:
        tz_name = "UTC"

    def _is_whop_linked(did: int) -> bool:
        try:
            return bool(_membership_id_from_history(int(did)))
        except Exception:
            return False

    with suppress(Exception):
        support_tickets.initialize(
            bot=bot,
            config=config,
            log_func=log_other,
            is_whop_linked=_is_whop_linked,
            timezone_name=tz_name,
        )
    
    # Backfill Whop timeline from whop_history.json (before initializing whop handler)
    _backfill_whop_timeline_from_whop_history()
    _load_whop_event_dedupe_cache()
    if WHOP_EVENTS_ENABLED:
        try:
            WHOP_EVENTS_FILE.touch(exist_ok=True)
        except Exception as e:
            log.warning(f"[WhopEvents] Failed to create ledger file: {e}")
    guild = bot.get_guild(GUILD_ID)
    if guild:
        log.info(f"[Bot] Connected to: {guild.name}")

        # Ensure staff alert channels exist (only when writing outputs to this guild).
        if not OUTPUT_GUILD_ID or int(OUTPUT_GUILD_ID) == int(GUILD_ID):
            await _ensure_alert_channels(guild)
        
        # Display config information
        log.info("")
        log.info("[Config] Configuration Information:")
        log.info("-"*60)
        log.info(f"🏠 Guild: {guild.name} (ID: {GUILD_ID})")
        
        # DM Sequence Config
        if ROLE_TRIGGER:
            trigger_role = guild.get_role(ROLE_TRIGGER)
            if trigger_role:
                log.info(f"🎯 Trigger Role: {trigger_role.name} (ID: {ROLE_TRIGGER})")
            else:
                log.warning(f"⚠️  Trigger Role: Not found (ID: {ROLE_TRIGGER})")
        
        if WELCOME_ROLE_ID:
            welcome_role = guild.get_role(WELCOME_ROLE_ID)
            if welcome_role:
                log.info(f"👋 Welcome Role: {welcome_role.name} (ID: {WELCOME_ROLE_ID})")
            else:
                log.warning(f"⚠️  Welcome Role: Not found (ID: {WELCOME_ROLE_ID})")
        
        if ROLES_TO_CHECK:
            log.info(f"🔍 Roles to Check: {len(ROLES_TO_CHECK)} role(s)")
            for role_id in list(ROLES_TO_CHECK)[:3]:
                role = guild.get_role(role_id)
                if role:
                    log.info(f"   • {role.name} (ID: {role_id})")
                else:
                    log.warning(f"   • ❌ Not found (ID: {role_id})")
            if len(ROLES_TO_CHECK) > 3:
                log.info(f"   ... and {len(ROLES_TO_CHECK) - 3} more")
        
        # Channels
        if LOG_FIRST_CHANNEL_ID:
            log_channel = guild.get_channel(LOG_FIRST_CHANNEL_ID)
            if log_channel:
                log.info(f"📝 Log First Channel: {log_channel.name} (ID: {LOG_FIRST_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Log First Channel: Not found (ID: {LOG_FIRST_CHANNEL_ID})")
        
        if LOG_OTHER_CHANNEL_ID:
            log_channel = guild.get_channel(LOG_OTHER_CHANNEL_ID)
            if log_channel:
                log.info(f"📝 Log Other Channel: {log_channel.name} (ID: {LOG_OTHER_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Log Other Channel: Not found (ID: {LOG_OTHER_CHANNEL_ID})")
        
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            status_channel = guild.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if status_channel:
                log.info(f"📊 Member Status Channel: {status_channel.name} (ID: {MEMBER_STATUS_LOGS_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Member Status Channel: Not found (ID: {MEMBER_STATUS_LOGS_CHANNEL_ID})")
        
        # Invite Tracking
        if INVITE_CHANNEL_ID:
            invite_channel = guild.get_channel(INVITE_CHANNEL_ID)
            if invite_channel:
                log.info(f"🔗 Invite Channel: {invite_channel.name} (ID: {INVITE_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Invite Channel: Not found (ID: {INVITE_CHANNEL_ID})")
        
        log.info("-"*60)
        
        # Initialize invite usage cache
        try:
            invites = await guild.invites()
            for invite in invites:
                invite_usage_cache[invite.code] = invite.uses
            log.info(f"[Invites] Cached {len(invite_usage_cache)} invites")
        except Exception as e:
            log.error(f"[Invites] Error caching invites: {e}")

        # Also cache invites for the support-tickets guild (Neo) so invite-delta detection works there too.
        try:
            st_gid = int((config.get("support_tickets") or {}).get("guild_id") or 0) if isinstance(config, dict) else 0
        except Exception:
            st_gid = 0
        if st_gid and int(st_gid) != int(GUILD_ID):
            g2 = bot.get_guild(int(st_gid))
            if g2:
                try:
                    inv2 = await g2.invites()
                    for inv in inv2:
                        invite_usage_cache[inv.code] = inv.uses
                    log.info(f"[Invites] Cached {len(inv2)} invites for support_tickets guild ({st_gid})")
                except Exception as e:
                    log.error(f"[Invites] Error caching invites for support_tickets guild ({st_gid}): {e}")
    else:
        log.warning(f"⚠️  Guild not found (ID: {GUILD_ID})")

    for uid, payload in queue_state.items():
        iso = payload.get("next_send")
        if not iso or is_due(iso):
            payload["next_send"] = (_now() + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    save_json(QUEUE_FILE, queue_state)

    # Queue and Registry Status
    queue_count = len(queue_state)
    registry_count = len(registry)
    log.info(f"[Queue] Active entries: {queue_count}")
    log.info(f"[Registry] Registered users: {registry_count}")
    if post_startup_report:
        startup_kv.append(("queue_entries", queue_count))
        startup_kv.append(("registry_users", registry_count))
    
    if not scheduler_loop.is_running():
        scheduler_loop.start()
        log.info("[Scheduler] Started and state restored")

    if post_startup_report:
        startup_notes.append("Scheduler started and state restored.")
    
    # Cleanup old data on startup
    cleanup_old_data()
    cleanup_old_invites()
    log.info("[Cleanup] Old data cleanup completed")

    if guild:
        scheduled = 0
        for m in guild.members:
            if not m.bot and not any(r.id in ROLES_TO_CHECK for r in m.roles):
                asyncio.create_task(check_and_assign_role(m, silent=True))
                scheduled += 1
        if scheduled:
            log.info(f"[Boot Check] Scheduled fallback role checks for {scheduled} member(s)")
            if post_startup_report:
                startup_kv.append(("boot_check_scheduled", scheduled))

    # Start HTTP server for invite tracking
    asyncio.create_task(init_http_server())
    log.info(f"[HTTP Server] Started on port {HTTP_SERVER_PORT}")
    
    # Initialize Whop API client if key provided
    if WHOP_API_KEY and WhopAPIClient:
        try:
            if not is_placeholder_secret(WHOP_API_KEY):
                base_url = WHOP_API_CONFIG.get("base_url", "https://api.whop.com/api/v1")
                company_id = WHOP_API_CONFIG.get("company_id", "")
                whop_api_client = WhopAPIClient(WHOP_API_KEY, base_url, company_id)
                log.info("[Whop API] Client initialized")
            else:
                whop_api_client = None
                log.info("[Whop API] Client disabled (placeholder key)")
        except Exception as e:
            whop_api_client = None
            log.warning(f"[Whop API] Failed to initialize: {e}")
    else:
        whop_api_client = None
        if not WhopAPIClient:
            log.info("[Whop API] Client disabled (module not available)")
        else:
            log.info("[Whop API] Client disabled (no API key)")
    
    # Startup scans can be expensive; run them sequentially in one canonical routine.
    # NOTE: We do NOT rely on Discord whop-* channels when API/webhook mode is enabled.
    asyncio.create_task(_run_startup_scans())
    
    # Schedule periodic cleanup (every 24 hours)
    @tasks.loop(hours=24)
    async def periodic_cleanup():
        cleanup_old_data()
        cleanup_old_invites()
    
    periodic_cleanup.start()
    log.info("[Cleanup] Periodic cleanup scheduled (every 24 hours)")
    
    # Start Whop membership sync job AFTER startup scans (anti-rate-limit).
    if whop_api_client and WHOP_API_CONFIG.get("enable_sync", True):
        asyncio.create_task(_start_whop_sync_job_after_startup())

    # Start reporting loop (weekly report + daily reminders) if enabled
    if REPORTING_CONFIG.get("enabled"):
        if not reporting_loop.is_running():
            reporting_loop.start()
            log.info("[Reporting] Reporting loop started")

    # One single startup report (anti-spam): file health + cache poisoning detection.
    if post_startup_report and LOG_OTHER_CHANNEL_ID:
        try:
            def _as_hours(delta: timedelta) -> float:
                try:
                    return float(delta.total_seconds() / 3600.0)
                except Exception:
                    return 0.0

            # Stale-file threshold (hours)
            try:
                stale_hours = float(LOG_CONTROLS.get("startup_stale_hours", 24))
            except Exception:
                stale_hours = 24.0
            stale_hours = max(0.0, stale_hours)

            # Collect runtime file health
            runtime_files: list[tuple[str, Path]] = [
                ("member_history.json", MEMBER_HISTORY_FILE),
                ("payment_cache.json", PAYMENT_CACHE_FILE),
                ("staff_alerts.json", STAFF_ALERTS_FILE),
                ("reporting_store.json", BASE_DIR / "reporting_store.json"),
            ]
            stale: list[str] = []
            tmp_leftovers: list[str] = []
            for label, p in runtime_files:
                try:
                    tmp = Path(str(p) + ".tmp")
                    if tmp.exists():
                        tmp_leftovers.append(tmp.name)
                except Exception:
                    pass
                try:
                    if not p.exists():
                        continue
                    age = _now() - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                    if stale_hours > 0 and age > timedelta(hours=stale_hours):
                        stale.append(f"{label} ({_as_hours(age):.1f}h)")
                except Exception:
                    continue

            e = discord.Embed(
                title="RSCheckerbot • Startup Health",
                description="Startup completed. This is the only startup message (anti-spam).",
                color=0x5865F2,
                timestamp=_now(),
            )

            # High-signal facts
            if startup_notes:
                e.add_field(name="Startup", value=("\n".join(f"- {x}" for x in startup_notes)[:1024] or "—"), inline=False)

            if stale:
                e.add_field(name=f"Stale files (> {stale_hours:.0f}h)", value=(", ".join(stale)[:1024] or "—"), inline=False)
            if tmp_leftovers:
                e.add_field(name="Tmp leftovers", value=(", ".join(tmp_leftovers)[:1024] or "—"), inline=False)

            # Add any collected key/values (small)
            if startup_kv:
                lines = []
                for k, v in startup_kv:
                    ks = str(k or "").strip()
                    if not ks:
                        continue
                    lines.append(f"{ks}: {v}")
                if lines:
                    e.add_field(name="Notes", value=("\n".join(lines)[:1024] or "—"), inline=False)

            await log_other(embed=e)
        except Exception:
            pass


@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id == GUILD_ID and not member.bot:
        # Discord can emit duplicate join events; dedupe per user for a short window.
        global _RECENT_MEMBER_JOINED
        try:
            _RECENT_MEMBER_JOINED
        except Exception:
            _RECENT_MEMBER_JOINED = {}  # type: ignore[var-annotated]
        try:
            now_ts = int(time.time())
            prev_ts = int((_RECENT_MEMBER_JOINED or {}).get(str(member.id)) or 0)
            if prev_ts and (now_ts - prev_ts) < 60:
                return
            _RECENT_MEMBER_JOINED[str(member.id)] = now_ts
            if len(_RECENT_MEMBER_JOINED) > 5000:
                items = sorted(_RECENT_MEMBER_JOINED.items(), key=lambda kv: int(kv[1] or 0), reverse=True)[:2000]
                _RECENT_MEMBER_JOINED = dict(items)  # type: ignore[assignment]
        except Exception:
            pass

        async def _upsert_member_join_card(embed: discord.Embed) -> discord.Message | None:
            """Prevent duplicate join cards by editing an existing recent one if present."""
            try:
                base = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID) if MEMBER_STATUS_LOGS_CHANNEL_ID else None
                ch = base if isinstance(base, discord.TextChannel) else None
                if not ch:
                    return None
                me = bot.user
                if not me:
                    return None
                now = datetime.now(timezone.utc)
                # Look back a small window for the same user+event (multi-process safety).
                matches: list[discord.Message] = []
                async for m in ch.history(limit=50):
                    try:
                        if int(getattr(m.author, "id", 0) or 0) != int(getattr(me, "id", 0) or 0):
                            continue
                        if (now - m.created_at).total_seconds() > 300:
                            break
                        if not m.embeds:
                            continue
                        e0 = m.embeds[0]
                        t = str(getattr(e0, "title", "") or "")
                        if "member joined" not in t.lower():
                            continue
                        # Match by Discord ID field if present.
                        did = ""
                        try:
                            for f in (getattr(e0, "fields", None) or []):
                                if str(getattr(f, "name", "") or "").strip().lower() == "discord id":
                                    did = re.sub(r"[^\d]", "", str(getattr(f, "value", "") or ""))
                                    break
                        except Exception:
                            did = ""
                        if did and did.isdigit() and int(did) == int(member.id):
                            matches.append(m)
                    except Exception:
                        continue
                if matches:
                    # History yields newest-first; edit the most recent and delete any extras.
                    keep = matches[0]
                    with suppress(Exception):
                        await keep.edit(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                    for extra in matches[1:]:
                        with suppress(Exception):
                            await extra.delete()
                    return keep
            except Exception:
                return None
            return None

        # Track join event FIRST (before other logic)
        rec = _touch_join(member.id, member)

        # Determine join method via invite usage deltas (best-effort).
        used_invite_code = None
        used_invite_inviter = None
        used_invite_inviter_name = None
        used_invite_inviter_id = None
        join_method_lines: list[str] = []
        try:
            invites = await member.guild.invites()
            for invite in invites:
                previous_uses = invite_usage_cache.get(invite.code, 0)
                if invite.uses > previous_uses and used_invite_code is None:
                    used_invite_code = invite.code
                    used_invite_inviter = invite.inviter
                    used_invite_inviter_name = invite.inviter.name if invite.inviter else None
                    used_invite_inviter_id = invite.inviter.id if invite.inviter else None
                # Keep cache updated
                invite_usage_cache[invite.code] = invite.uses
        except Exception as e:
            join_method_lines.append(f"• Invite tracking: error ({str(e)[:120]})")

        if used_invite_code:
            join_method_lines.append(f"• Invite code: `{used_invite_code}`")
            if used_invite_inviter_name and used_invite_inviter_id:
                join_method_lines.append(f"• Invited by: {used_invite_inviter_name} (`{used_invite_inviter_id}`)")
            invite_entry = invites_data.get(used_invite_code) or {}
            is_tracked = bool(invite_entry) and invite_entry.get("used_at") is None
            join_method_lines.append(f"• Tracked invite: {'yes' if is_tracked else 'no'}")
            # Support tickets: invite-based Free Pass intake (Neo-only; support_tickets enforces guild match).
            with suppress(Exception):
                asyncio.create_task(
                    support_tickets.handle_free_pass_join_if_needed(
                        member=member,
                        tracked_one_time_invite=bool(is_tracked),
                    )
                )
            if is_tracked:
                join_method_lines.append("• Source: One-time invite")
                lead_id = invite_entry.get("lead_id") or ""
                if lead_id:
                    join_method_lines.append(f"• Lead ID: `{lead_id}`")
            else:
                join_method_lines.append("• Source: Untracked/permanent or external")
        else:
            # Pure output (no explanations)
            if not join_method_lines:
                join_method_lines.append("• Invite code: —")
                join_method_lines.append("• Invited by: —")
                join_method_lines.append("• Tracked invite: —")
                join_method_lines.append("• Source: —")
        
        # Optional: Log join to member-status-logs
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if ch:
                inviter_s = "—"
                if used_invite_inviter_name and used_invite_inviter_id:
                    inviter_s = f"{used_invite_inviter_name} ({used_invite_inviter_id})"
                tracked_s = "—"
                source_s = "—"
                if used_invite_code:
                    invite_entry = invites_data.get(used_invite_code) or {}
                    is_tracked = bool(invite_entry) and invite_entry.get("used_at") is None
                    tracked_s = "yes" if is_tracked else "no"
                    source_s = "one_time_invite" if is_tracked else "untracked_or_external"
                access = _access_roles_plain(member)

                acc = rec.get("access") if isinstance(rec.get("access"), dict) else {}

                base_member_kv = [
                    ("account_created", member.created_at.strftime("%b %d, %Y")),
                    ("first_joined", _fmt_ts(rec.get("first_join_ts"), "D")),
                    ("join_count", rec.get("join_count", 1)),
                    ("returning_member", "true" if rec.get("join_count", 0) > 1 else "false"),
                    ("ever_had_member_role", "yes" if acc.get("ever_had_member_role") is True else "no"),
                ]
                base_discord_kv = [
                    ("invite_code", used_invite_code or "—"),
                    ("invited_by", inviter_s),
                    ("tracked_invite", tracked_s),
                    ("source", source_s),
                ]
                # Helpful for staff triage; only shows when present (blanks are hidden).
                mid_hint = _membership_id_from_history(member.id)
                if mid_hint:
                    base_discord_kv.append(("membership_id", mid_hint))

                # Attempt immediate Whop enrichment; otherwise post placeholder then edit.
                _mid_now, brief_now = await _resolve_whop_brief_for_discord_id(member.id)
                if isinstance(brief_now, dict) and brief_now:
                    detailed = _build_member_status_detailed_embed(
                        title="👋 Member Joined",
                        member=member,
                        access_roles=access,
                        color=0x57F287,
                        event_kind="active",
                        member_kv=base_member_kv,
                        discord_kv=base_discord_kv,
                        whop_brief=brief_now,
                    )
                    # Upsert to avoid duplicates (e.g. multiple instances online).
                    existing = await _upsert_member_join_card(detailed)
                    if not existing:
                        await log_member_status("", embed=detailed)
                else:
                    pending: dict = {}
                    pending_embed = _build_member_status_detailed_embed(
                        title="👋 Member Joined",
                        member=member,
                        access_roles=access,
                        color=0x57F287,
                        event_kind="active",
                        member_kv=base_member_kv,
                        discord_kv=base_discord_kv,
                        whop_brief=pending,
                    )
                    # Upsert to avoid duplicates (e.g. multiple instances online).
                    msg = await _upsert_member_join_card(pending_embed)
                    if not msg:
                        msg = await log_member_status("", embed=pending_embed)

                    def _final(brief: dict) -> discord.Embed:
                        return _build_member_status_detailed_embed(
                            title="👋 Member Joined",
                            member=member,
                            access_roles=access,
                            color=0x57F287,
                            event_kind="active",
                            member_kv=base_member_kv,
                            discord_kv=base_discord_kv,
                            whop_brief=brief,
                        )

                    def _fallback() -> discord.Embed:
                        return _build_member_status_detailed_embed(
                            title="👋 Member Joined",
                            member=member,
                            access_roles=access,
                            color=0x57F287,
                            event_kind="active",
                            member_kv=base_member_kv,
                            discord_kv=base_discord_kv,
                            whop_brief={},
                        )

                    if msg:
                        asyncio.create_task(
                            _retry_whop_enrich_and_edit(
                                discord_id=member.id,
                                messages=[msg],
                                make_embed=_final,
                                make_fallback_embed=_fallback,
                                timeout_seconds=WHOP_LINK_TIMEOUT_SECONDS,
                                retry_seconds=WHOP_LINK_RETRY_SECONDS,
                            )
                        )
        
        guild = member.guild
        current_roles = {r.id for r in member.roles}
        current_role_names = _fmt_role_list(current_roles, guild) if current_roles else "—"
        
        # Check if they already have Welcome role
        has_welcome = WELCOME_ROLE_ID and WELCOME_ROLE_ID in current_roles
        welcome_status = f"✅ Has {_fmt_role(WELCOME_ROLE_ID, guild)}" if has_welcome else f"❌ Missing {_fmt_role(WELCOME_ROLE_ID, guild)}"
        
        # Check which checked roles they have vs missing
        user_has_checked = [r.id for r in member.roles if r.id in ROLES_TO_CHECK]
        user_missing_checked = [rid for rid in ROLES_TO_CHECK if rid not in current_roles]
        has_any_checked = len(user_has_checked) > 0
        
        checked_note = "Has checked roles" if has_any_checked else "No checked roles"
        join_embed = _make_dyno_embed(
            member=member,
            description=f"{member.mention} joined",
            footer=f"ID: {member.id}",
            color=0x5865F2,
        )
        try:
            # Returning member signal: join_count>1 OR we have a recorded leave timestamp.
            join_count = int(rec.get("join_count", 0) or 0)
        except Exception:
            join_count = 0
        last_leave_ts = rec.get("last_leave_ts")
        acc = rec.get("access") if isinstance(rec.get("access"), dict) else {}
        returning = bool(join_count > 1 or last_leave_ts)

        join_embed.add_field(name="Returning", value=("Yes" if returning else "No"), inline=True)
        join_embed.add_field(name="Joins", value=(str(join_count) if join_count else "—"), inline=True)
        join_embed.add_field(
            name="Last left",
            value=(_fmt_ts(last_leave_ts, "R") if last_leave_ts else "—"),
            inline=True,
        )
        join_embed.add_field(
            name="History",
            value=(
                f"First joined: {_fmt_ts(rec.get('first_join_ts'), 'D')}\n"
                f"Ever had Member: {'yes' if acc.get('ever_had_member_role') is True else 'no'}"
            )[:1024],
            inline=False,
        )
        join_embed.add_field(name="Welcome", value=("Yes" if has_welcome else "No"), inline=True)
        join_embed.add_field(name="Checked roles", value=checked_note, inline=True)
        join_embed.add_field(name="Next", value="Will verify again in 60s", inline=False)
        if current_role_names and current_role_names != "—":
            join_embed.add_field(name="Current roles", value=current_role_names[:1024], inline=False)
        # This is a join log (not a role event). Route to the join-logs channel (log_first).
        await log_first(embed=join_embed)
        asyncio.create_task(check_and_assign_role(member))

        # If we detected a tracked invite, mark it used (non-destructive; persists metadata for audit).
        try:
            if used_invite_code:
                invite_entry = invites_data.get(used_invite_code)
                if invite_entry and invite_entry.get("used_at") is None:
                    await track_invite_usage(used_invite_code, member)
        except Exception as e:
            log.error(f"❌ Error updating tracked invite usage for {member} ({member.id}): {e}")

@bot.event
async def on_member_remove(member: discord.Member):
    """Track member leave events and log to member-status-logs"""
    # Discord can emit duplicate leave events; dedupe per user for a short window.
    global _RECENT_MEMBER_LEFT
    try:
        _RECENT_MEMBER_LEFT
    except Exception:
        _RECENT_MEMBER_LEFT = {}  # type: ignore[var-annotated]

    try:
        now_ts = int(time.time())
        prev_ts = int((_RECENT_MEMBER_LEFT or {}).get(str(member.id)) or 0)
        if prev_ts and (now_ts - prev_ts) < 60:
            return
        _RECENT_MEMBER_LEFT[str(member.id)] = now_ts
        # Bound size (best-effort)
        if len(_RECENT_MEMBER_LEFT) > 5000:
            # keep most recent ~2000
            items = sorted(_RECENT_MEMBER_LEFT.items(), key=lambda kv: int(kv[1] or 0), reverse=True)[:2000]
            _RECENT_MEMBER_LEFT = {k: v for k, v in items}  # type: ignore[var-annotated]
    except Exception:
        pass

    if member.guild.id == GUILD_ID and not member.bot:
        rec = _touch_leave(member.id, member)
        
        # Log to member-status-logs channel
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if ch:
                access = _access_roles_plain(member)

                # No Whop API calls for join/leave logs; rely on member_history + native-card cache only.
                mid = _membership_id_from_history(member.id)
                whop_brief = _whop_summary_for_member(member.id) or {}
                acc = rec.get("access") if isinstance(rec.get("access"), dict) else {}
                detailed = _build_member_status_detailed_embed(
                    title="🚪 Member Left",
                    member=member,
                    access_roles=access,
                    color=0xFAA61A,
                    event_kind="active",
                    member_kv=[
                        ("left_at", _fmt_ts(rec.get("last_leave_ts"), "D") if rec.get("last_leave_ts") else "—"),
                        ("first_joined", _fmt_ts(rec.get("first_join_ts"), "D") if rec.get("first_join_ts") else "—"),
                        ("join_count", rec.get("join_count") or "—"),
                        ("ever_had_member_role", "yes" if acc.get("ever_had_member_role") is True else "no"),
                        ("first_access", _fmt_ts(acc.get("first_access_ts"), "D") if acc.get("first_access_ts") else "—"),
                        ("last_access", _fmt_ts(acc.get("last_access_ts"), "D") if acc.get("last_access_ts") else "—"),
                    ],
                    discord_kv=[
                        ("access_roles_at_leave", access),
                        ("membership_id", mid) if mid else ("membership_id", "—"),
                    ],
                    whop_brief=whop_brief,
                )
                await log_member_status("", embed=detailed)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    before_roles = {r.id for r in before.roles}
    after_roles = {r.id for r in after.roles}
    suppress_logs = _member_update_logs_suppressed(after.id)
    
    # Detect all role changes (added and removed)
    roles_added = after_roles - before_roles
    roles_removed = before_roles - after_roles

    # Persist a bounded, high-signal role-change timeline for staff triage.
    try:
        relevant_added = {rid for rid in roles_added if int(rid) in HISTORY_RELEVANT_ROLE_IDS}
        relevant_removed = {rid for rid in roles_removed if int(rid) in HISTORY_RELEVANT_ROLE_IDS}
        if relevant_added or relevant_removed:
            _touch_role_change(after, roles_added=relevant_added, roles_removed=relevant_removed, note="member_update")
    except Exception:
        pass
    
    # Log general role changes if any occurred (but skip if we'll log them specifically below)
    if roles_added or roles_removed:
        # Skip logging here if it's a specific case we handle below (we'll log it there with more detail)
        is_specific_case = (
            (ROLE_CANCEL_A in after_roles or ROLE_CANCEL_B in after_roles) and str(after.id) in queue_state or
            (ROLE_TRIGGER not in before_roles and ROLE_TRIGGER in after_roles) or
            (any(r.id in ROLES_TO_CHECK for r in before.roles) and not any(r.id in ROLES_TO_CHECK for r in after.roles)) or
            ((ROLE_CANCEL_A in before_roles) and (ROLE_CANCEL_A not in after_roles)) or
            ((ROLE_CANCEL_A not in before_roles) and (ROLE_CANCEL_A in after_roles))
        )
        
        if not is_specific_case:
            # General role change - batch to reduce spam (rapid sequences often produce multiple events)
            uid = int(after.id)
            rec = pending_role_updates.get(uid)
            if not rec:
                rec = {"added": set(), "removed": set(), "member": after, "task": None}
                pending_role_updates[uid] = rec
            try:
                rec["added"].update(set(roles_added))
                rec["removed"].update(set(roles_removed))
                # Cancel out roles that were both added and removed within the batch window
                both = rec["added"].intersection(rec["removed"])
                if both:
                    rec["added"].difference_update(both)
                    rec["removed"].difference_update(both)
            except Exception:
                pass
            if not rec.get("task"):
                rec["task"] = asyncio.create_task(_flush_role_update(uid))

    if (ROLE_CANCEL_A in after_roles or ROLE_CANCEL_B in after_roles) and str(after.id) in queue_state:
        cancel_roles = []
        if ROLE_CANCEL_A in after_roles:
            cancel_roles.append(_fmt_role(ROLE_CANCEL_A, after.guild))
        if ROLE_CANCEL_B in after_roles:
            cancel_roles.append(_fmt_role(ROLE_CANCEL_B, after.guild))
        cancel_info = ", ".join(cancel_roles) if cancel_roles else "cancel role"
        
        # Check if Member role was just added (user completed onboarding/payment)
        member_role_just_added = ROLE_CANCEL_A not in before_roles and ROLE_CANCEL_A in after_roles
        if member_role_just_added:
            await log_member_status(
                f"✅ **Payment/Onboarding Complete**\n"
                f"**User:** {_fmt_user(after)}\n"
                f"**Roles Added:** {cancel_info}\n"
                f"**Action:** DM sequence cancelled — user now has full access"
            )
        
        mark_cancelled(after.id, "cancel_role_added")
        await log_other(f"🛑 Cancelled for {_fmt_user(after)} — {cancel_info} was added (role update)")
        return

    if ROLE_TRIGGER not in before_roles and ROLE_TRIGGER in after_roles:
        guild = after.guild
        if suppress_logs:
            # Startup/sync-driven role changes should not enqueue DM sequences or spam channels.
            return
        
        if has_sequence_before(after.id):
            await log_other(f"⏭️ Skipped DM sequence for {_fmt_user(after)} — sequence previously run")
            return
        
        enqueue_first_day(after.id)
        
        enq = _make_dyno_embed(
            member=after,
            description=f"{after.mention} queued for day_1 (trigger role added)",
            footer=f"ID: {after.id}",
            color=0x5865F2,
        )
        await log_first(embed=enq)
        
        return

    had_checked = any(r.id in ROLES_TO_CHECK for r in before.roles)
    has_checked_now = any(r.id in ROLES_TO_CHECK for r in after.roles)
    if had_checked and not has_checked_now:
        # Get which checked roles were lost
        before_checked = [r.id for r in before.roles if r.id in ROLES_TO_CHECK]
        after_checked = [r.id for r in after.roles if r.id in ROLES_TO_CHECK]
        lost_checked = [rid for rid in before_checked if rid not in after_checked]
        
        # Show all role changes in this update
        all_removed_in_update = before_roles - after_roles
        all_added_in_update = after_roles - before_roles
        all_removed_names = _fmt_role_list(all_removed_in_update, after.guild)
        all_added_names = _fmt_role_list(all_added_in_update, after.guild) if all_added_in_update else None
        
        lost_checked_names = _fmt_role_list(set(lost_checked), after.guild)

        cid = _cid_for(after.id)
        lost_embed = _make_dyno_embed(
            member=after,
            description=f"{after.mention} has none of the checked roles",
            footer=f"ID: {after.id} • CID: {cid}",
            color=0xFEE75C,
        )
        if all_removed_names and all_removed_names != "—":
            lost_embed.add_field(name="Removed", value=all_removed_names[:1024], inline=False)
        if all_added_names and all_added_names != "—":
            lost_embed.add_field(name="Added", value=all_added_names[:1024], inline=False)
        if lost_checked_names and lost_checked_names != "—":
            lost_embed.add_field(name="Lost checked roles", value=lost_checked_names[:1024], inline=False)
        lost_embed.add_field(name="Next", value="Will check again in 60s", inline=False)
        await log_role_event(embed=lost_embed)
        asyncio.create_task(check_and_assign_role(after))

    if (ROLE_CANCEL_A in before_roles) and (ROLE_CANCEL_A not in after_roles):
        # Sync-driven removals (if enabled) can create noisy staff cards; suppress when flagged.
        if _has_lifetime_role(after):
            role_obj = after.guild.get_role(ROLE_CANCEL_A) if after.guild else None
            role_name = str(role_obj.name) if role_obj else "Member"
            cid = _cid_for(after.id)
            e = _make_dyno_embed(
                member=after,
                description=f"{after.mention} was removed from the {role_name} role but has Lifetime access; skipping auto actions",
                footer=f"ID: {after.id} • CID: {cid}",
                color=0x57F287,
            )
            if not suppress_logs:
                await log_role_event(embed=e)
            return
        # Show all roles removed in this update (not just Member role)
        all_removed_in_update = before_roles - after_roles
        all_added_in_update = after_roles - before_roles
        removed_names = _fmt_role_list(all_removed_in_update, after.guild)
        added_names = _fmt_role_list(all_added_in_update, after.guild) if all_added_in_update else None
        
        # Note: we no longer generate Whop payment/cancellation cards from Discord role changes.
        # Those cards are webhook-driven (real-time) to avoid backfill/sync noise in case channels.
        
        role_obj = after.guild.get_role(ROLE_CANCEL_A) if after.guild else None
        role_name = str(role_obj.name) if role_obj else "Member"
        cid = _cid_for(after.id)
        removed_embed = _make_dyno_embed(
            member=after,
            description=f"{after.mention} was removed from the {role_name} role",
            footer=f"ID: {after.id} • CID: {cid}",
            color=0xFEE75C,
        )
        if removed_names and removed_names != "—":
            removed_embed.add_field(name="Removed", value=removed_names[:1024], inline=False)
        if added_names and added_names != "—":
            removed_embed.add_field(name="Added", value=added_names[:1024], inline=False)
        removed_embed.add_field(
            name="Next",
            value=f"Will mark as Former Member in {FORMER_MEMBER_DELAY_SECONDS}s if not regained",
            inline=False,
        )
        if not suppress_logs:
            await log_role_event(embed=removed_embed)
        
        # If Member role was removed and user has active DM sequence, cancel it
        if str(after.id) in queue_state:
            mark_cancelled(after.id, "member_role_removed_payment")
            if not suppress_logs:
                await log_other(f"🛑 Cancelled DM sequence for {_fmt_user(after)} — Member role removed (likely payment cancellation)")
        
        asyncio.create_task(delayed_assign_former_member(after))

    if (ROLE_CANCEL_A not in before_roles) and (ROLE_CANCEL_A in after_roles):
        # Show all role changes when Member role is regained
        all_removed_in_update = before_roles - after_roles
        all_added_in_update = after_roles - before_roles
        removed_names = _fmt_role_list(all_removed_in_update, after.guild) if all_removed_in_update else None
        added_names = _fmt_role_list(all_added_in_update, after.guild)
        
        # Check if this looks like payment reactivation (Member role regained)
        # If Member role was the ONLY role added, likely payment reactivation
        only_member_added = len(all_added_in_update) == 1
        if only_member_added:
            if suppress_logs:
                return
            access = _access_roles_plain(after)
            issue_key = "payment_resumed"
            if await should_post_and_record_alert(
                STAFF_ALERTS_FILE,
                discord_id=after.id,
                issue_key=issue_key,
                cooldown_hours=2.0,
            ):
                hist = get_member_history(after.id) or {}
                acc = hist.get("access") if isinstance(hist.get("access"), dict) else {}

                def _is_recent_success(brief: dict) -> bool:
                    if not isinstance(brief, dict):
                        return False
                    st = str(brief.get("status") or "").strip().lower()
                    if st not in {"active", "trialing"}:
                        return False
                    dt = _parse_dt_any(brief.get("last_success_paid_at_iso") or "")
                    if not dt:
                        return False
                    return (_now() - dt) <= timedelta(hours=float(PAYMENT_RESUMED_RECENT_HOURS))

                def _title_for(brief: dict) -> str:
                    if not isinstance(brief, dict):
                        return "⚠️ Member role added (Whop not linked)"
                    st = str(brief.get("status") or "").strip().lower()
                    if st in {"active", "trialing"}:
                        return "✅ Payment Resumed" if _is_recent_success(brief) else "✅ Access Restored"
                    if st in {"past_due", "unpaid"}:
                        return "⚠️ Member role added (payment past due)"
                    if st in {"canceled", "cancelled", "expired", "inactive"}:
                        return "⚠️ Member role added (Whop canceled)"
                    return "⚠️ Member role added (Whop status unclear)"

                base_member_kv = [
                    ("first_joined", _fmt_ts(hist.get("first_join_ts"), "D") if hist.get("first_join_ts") else "—"),
                    ("join_count", hist.get("join_count") or "—"),
                    ("ever_had_member_role", "yes" if acc.get("ever_had_member_role") is True else "no"),
                    ("first_access", _fmt_ts(acc.get("first_access_ts"), "D") if acc.get("first_access_ts") else "—"),
                    ("last_access", _fmt_ts(acc.get("last_access_ts"), "D") if acc.get("last_access_ts") else "—"),
                ]
                base_discord_kv = [
                    ("roles_added", added_names),
                    ("roles_removed", removed_names or "—"),
                    ("reason", "member_role_regained"),
                ]

                # Try immediate Whop enrichment; otherwise post placeholder then edit.
                _mid_now, whop_brief_now = await _resolve_whop_brief_for_discord_id(after.id)
                if isinstance(whop_brief_now, dict) and whop_brief_now:
                    final_title = _title_for(whop_brief_now)
                    detailed = _build_member_status_detailed_embed(
                        title=final_title,
                        member=after,
                        access_roles=access,
                        color=0x57F287,
                        event_kind="active",
                        member_kv=base_member_kv,
                        discord_kv=base_discord_kv + [("event", "payment.resumed" if final_title == "✅ Payment Resumed" else "access.restored")],
                        whop_brief=whop_brief_now,
                    )
                    await log_member_status("", embed=detailed)

                    # No case-channel posting here; webhook events own the payment-failure channel.
                else:
                    pending: dict = {}
                    pending_title = "✅ Access Restored"

                    pending_detailed = _build_member_status_detailed_embed(
                        title=pending_title,
                        member=after,
                        access_roles=access,
                        color=0x57F287,
                        event_kind="active",
                        member_kv=base_member_kv,
                        discord_kv=base_discord_kv + [("event", "access.restored")],
                        whop_brief=pending,
                    )
                    msg_detailed = await log_member_status("", embed=pending_detailed)

                    def _final_detailed(brief: dict) -> discord.Embed:
                        # helper for retry loop (sync)
                        final_title2 = _title_for(brief)
                        return _build_member_status_detailed_embed(
                            title=final_title2,
                            member=after,
                            access_roles=access,
                            color=0x57F287,
                            event_kind="active",
                            member_kv=base_member_kv,
                            discord_kv=base_discord_kv + [("event", "payment.resumed" if final_title2 == "✅ Payment Resumed" else "access.restored")],
                            whop_brief=brief,
                        )

                    def _fallback_detailed() -> discord.Embed:
                        return _build_member_status_detailed_embed(
                            title=pending_title,
                            member=after,
                            access_roles=access,
                            color=0x57F287,
                            event_kind="active",
                            member_kv=base_member_kv,
                            discord_kv=base_discord_kv + [("event", "access.restored")],
                            whop_brief={},
                        )

                    if msg_detailed:
                        asyncio.create_task(
                            _retry_whop_enrich_and_edit(
                                discord_id=after.id,
                                messages=[msg_detailed],
                                make_embed=_final_detailed,
                                make_fallback_embed=_fallback_detailed,
                                timeout_seconds=WHOP_LINK_TIMEOUT_SECONDS,
                                retry_seconds=WHOP_LINK_RETRY_SECONDS,
                            )
                        )
        
        if has_former_member_role(after):
            role = after.guild.get_role(FORMER_MEMBER_ROLE)
            if role:
                with suppress(Exception):
                    await after.remove_roles(role, reason="Regained member role; remove former-member marker")
                    cid = _cid_for(after.id)
                    e = _make_dyno_embed(
                        member=after,
                        description=f"{after.mention} was removed from the {role.name} role",
                        footer=f"ID: {after.id} • CID: {cid}",
                        color=0x57F287,
                    )
                    if added_names and added_names != "—":
                        e.add_field(name="Added", value=added_names[:1024], inline=False)
                    if removed_names and removed_names != "—":
                        e.add_field(name="Removed", value=str(removed_names)[:1024], inline=False)
                    e.add_field(name="Reason", value="Regained member role", inline=False)
                    await log_role_event(embed=e)

@bot.event
async def on_message(message: discord.Message):
    """
    Handle incoming messages.
    Processes Whop webhook messages from configured channels (workflow webhooks and native integration).
    """
    # Process commands first (canonical pattern)
    await bot.process_commands(message)
    
    # Prevent self-loops
    if bot.user and message.author and message.author.id == bot.user.id:
        return

    # Support tickets: track last_activity for non-bot messages (always, even when webhooks are enabled).
    with suppress(Exception):
        await support_tickets.record_activity_from_message(message)

    # Webhooks are canonical: do not rely on Discord whop-* channels.
    if bool(str(WHOP_WEBHOOK_SECRET or "").strip()):
        return

    # Check if this is a Whop message (from either channel).
    # Channel ID is the source of truth; Whop app messages may not be flagged as bot/webhook.
    if (WHOP_WEBHOOK_CHANNEL_ID and message.channel.id == WHOP_WEBHOOK_CHANNEL_ID):
        await handle_whop_webhook_message(message)
        return

    if (WHOP_LOGS_CHANNEL_ID and message.channel.id == WHOP_LOGS_CHANNEL_ID):
        await handle_whop_webhook_message(message)
        return
    
    # Message processing continues here if needed for other handlers
    # (Currently no other message handlers, but this preserves extensibility)

# -----------------------------
# Data Cleanup Functions
# -----------------------------
def cleanup_old_data():
    """Clean up old completed entries from queue.json and registry.json"""
    global queue_state, registry
    
    # Clean queue: remove entries older than 30 days that are completed
    cleaned_queue = {}
    cutoff_date = _now() - timedelta(days=30)
    
    for uid, payload in queue_state.items():
        try:
            next_send = payload.get("next_send", "")
            if next_send:
                next_dt = datetime.fromisoformat(next_send.replace("Z", "+00:00"))
                # Keep if not old or if still active
                if next_dt > cutoff_date:
                    cleaned_queue[uid] = payload
        except Exception:
            # Keep entry if we can't parse date
            cleaned_queue[uid] = payload
    
    removed_queue = len(queue_state) - len(cleaned_queue)
    queue_state = cleaned_queue
    
    # Clean registry: remove completed entries older than 90 days
    cleaned_registry = {}
    registry_cutoff = _now() - timedelta(days=90)
    
    for uid, data in registry.items():
        try:
            started_at = data.get("started_at", "")
            if started_at:
                started_dt = datetime.fromisoformat(started_at.replace("+00:00", ""))
                completed = data.get("completed", False)
                # Keep if recent OR if not completed
                if not completed or started_dt > registry_cutoff:
                    cleaned_registry[uid] = data
        except Exception:
            # Keep entry if we can't parse date
            cleaned_registry[uid] = data
    
    removed_registry = len(registry) - len(cleaned_registry)
    registry = cleaned_registry
    
    if removed_queue > 0 or removed_registry > 0:
        save_all()
        log.info(f"Cleaned up {removed_queue} old queue entries and {removed_registry} old registry entries")

def cleanup_old_invites():
    """Clean up old invite JSON entries (older than 90 days and used)"""
    try:
        global invites_data
        cutoff_date = (_now() - timedelta(days=90)).isoformat()
        
        # Remove old used invites
        removed = 0
        invites_to_remove = []
        
        for invite_code, invite_entry in invites_data.items():
            used_at = invite_entry.get("used_at")
            if used_at and used_at < cutoff_date:
                invites_to_remove.append(invite_code)
        
        for invite_code in invites_to_remove:
            del invites_data[invite_code]
            removed += 1
        
        if removed > 0:
            save_invites(invites_data)
            log.info(f"Cleaned up {removed} old invite entries")
    except Exception as e:
        log.error(f"Error cleaning invites: {e}")

# -----------------------------
# Admin commands
# -----------------------------
@bot.command(name="editmessages", aliases=["editmessage", "checker-edit", "cedit", "checker-messages"])
@commands.has_permissions(administrator=True)
async def edit_messages(ctx):
    """Edit DM messages via embedded interface"""
    try:
        # Update messages_data before loading editor
        bot_instance.messages = messages_data
        from message_editor import MessageEditorView
        view = MessageEditorView(bot_instance)
        embed = view.get_main_embed()
        await ctx.send(embed=embed, view=view)
        try:
            await ctx.message.delete()
        except Exception:
            pass
    except ImportError as e:
        await ctx.send(f"❌ Failed to import message editor: {e}", delete_after=10)
    except Exception as e:
        await ctx.send(f"❌ Error: {e}", delete_after=10)

@bot.command(name="reloadmessages", aliases=["checker-reload", "creload"])
@commands.has_permissions(administrator=True)
async def reload_messages(ctx):
    """Reload messages from JSON file"""
    global messages_data
    try:
        messages_data = load_messages()
        bot_instance.messages = messages_data
        await ctx.send("✅ Messages reloaded from messages.json!", delete_after=5)
    except Exception as e:
        await ctx.send(f"❌ Failed to reload messages.json: {e}", delete_after=15)
    try:
        await ctx.message.delete()
    except Exception:
        pass

@bot.command(name="cleanup")
@commands.has_permissions(administrator=True)
async def cleanup_data(ctx):
    """Manually trigger data cleanup"""
    cleanup_old_data()
    cleanup_old_invites()
    await ctx.send("✅ Data cleanup completed!", delete_after=5)
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name="transcript")
@support_tickets.staff_check_for_ctx()
@commands.guild_only()
async def ticket_transcript(ctx: commands.Context):
    """Export transcript for a support ticket channel and close it."""
    ch_id = int(getattr(getattr(ctx, "channel", None), "id", 0) or 0)
    rec = await support_tickets.get_ticket_record_for_channel_id(ch_id)
    if not isinstance(rec, dict) or str(rec.get("status") or "").strip().upper() != "OPEN":
        await ctx.send("❌ This command only works inside an OPEN ticket channel.", delete_after=12)
        with suppress(Exception):
            await ctx.message.delete()
        return
    with suppress(Exception):
        await ctx.send("⏳ Exporting transcript and closing…", delete_after=10)
    await support_tickets.close_ticket_by_channel_id(
        ch_id,
        close_reason="manual_transcript",
        do_transcript=True,
        delete_channel=True,
    )


@bot.command(name="close")
@support_tickets.staff_check_for_ctx()
@commands.guild_only()
async def ticket_close(ctx: commands.Context):
    """Close a support ticket channel (defaults to transcript + delete)."""
    ch_id = int(getattr(getattr(ctx, "channel", None), "id", 0) or 0)
    rec = await support_tickets.get_ticket_record_for_channel_id(ch_id)
    if not isinstance(rec, dict) or str(rec.get("status") or "").strip().upper() != "OPEN":
        await ctx.send("❌ This command only works inside an OPEN ticket channel.", delete_after=12)
        with suppress(Exception):
            await ctx.message.delete()
        return
    with suppress(Exception):
        await ctx.send("⏳ Closing ticket…", delete_after=10)
    await support_tickets.close_ticket_by_channel_id(
        ch_id,
        close_reason="manual_close",
        do_transcript=True,
        delete_channel=True,
    )


def _parse_date_ymd(s: str) -> datetime | None:
    try:
        txt = str(s or "").strip()
        if not txt:
            return None
        d = datetime.strptime(txt, "%Y-%m-%d").date()
        # Interpret dates in reporting timezone, then convert to UTC later.
        if ZoneInfo is None:
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        tz_name = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
        tz = ZoneInfo(tz_name)
        return datetime(d.year, d.month, d.day, tzinfo=tz)
    except Exception:
        return None


def _whop_report_norm_bool(v: object) -> bool:
    s = str(v or "").strip().lower()
    return s in {"true", "yes", "1", "y"}


def _whop_report_normalize_membership(rec: dict) -> dict:
    if not isinstance(rec, dict):
        return {}
    for key in ("membership", "data", "item", "record"):
        inner = rec.get(key)
        if isinstance(inner, dict):
            if any(k in inner for k in ("status", "created_at", "renewal_period_end", "id")):
                return inner
    return rec


def _whop_report_membership_id(membership: dict) -> str:
    if not isinstance(membership, dict):
        return ""
    def _find_mem_id(obj: object, *, depth: int) -> str:
        if depth > 5:
            return ""
        if isinstance(obj, str):
            m = re.search(r"(mem_[A-Za-z0-9]+|R-[A-Za-z0-9-]+)", obj)
            return m.group(1) if m else ""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(k, str) and k.lower() in {"id", "membership_id", "membershipid", "whop_key", "key"}:
                    if isinstance(v, (int, float)):
                        s = str(v).strip()
                        if s:
                            return s
                    if isinstance(v, str):
                        m = re.search(r"(mem_[A-Za-z0-9]+|R-[A-Za-z0-9-]+)", v)
                        if m:
                            return m.group(1)
                found = _find_mem_id(v, depth=depth + 1)
                if found:
                    return found
        if isinstance(obj, list):
            for it in obj:
                found = _find_mem_id(it, depth=depth + 1)
                if found:
                    return found
        return ""

    for key in ("id", "membership_id", "membershipId", "membership", "whop_key", "key"):
        val = membership.get(key)
        if isinstance(val, (int, float)):
            s = str(val).strip()
            if s:
                return s
        if isinstance(val, str) and val.strip():
            m = re.search(r"(mem_[A-Za-z0-9]+|R-[A-Za-z0-9-]+)", val)
            return m.group(1) if m else val.strip()
        if isinstance(val, dict):
            inner_id = str(val.get("id") or val.get("membership_id") or "").strip()
            if inner_id:
                return inner_id
    return _find_mem_id(membership, depth=0)


def _whop_report_extract_email(membership: dict) -> str:
    if not isinstance(membership, dict):
        return ""
    for key in ("email", "user_email"):
        val = membership.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    user = membership.get("user")
    if isinstance(user, dict):
        em = str(user.get("email") or "").strip()
        if em:
            return em
    member = membership.get("member")
    if isinstance(member, dict):
        em = str(member.get("email") or "").strip()
        if em:
            return em
    return ""


def _whop_report_extract_discord_id(membership: dict) -> int | None:
    raw = extract_discord_id_from_whop_member_record(membership) if isinstance(membership, dict) else ""
    return int(raw) if str(raw or "").strip().isdigit() else None


def _whop_report_extract_user_id(membership: dict) -> str:
    if not isinstance(membership, dict):
        return ""
    u = membership.get("user")
    if isinstance(u, str):
        return u.strip()
    if isinstance(u, dict):
        return str(u.get("id") or u.get("user_id") or "").strip()
    m = membership.get("member")
    if isinstance(m, dict):
        u2 = m.get("user")
        if isinstance(u2, str):
            return u2.strip()
        if isinstance(u2, dict):
            return str(u2.get("id") or u2.get("user_id") or "").strip()
    return ""


def _whop_report_pick_dt(membership: dict, keys: list[str]) -> datetime | None:
    if not isinstance(membership, dict):
        return None
    for k in keys:
        v = membership.get(k)
        if isinstance(v, dict):
            sec = v.get("seconds") or v.get("_seconds") or v.get("epoch_seconds") or v.get("unix")
            nanos = v.get("nanos") or v.get("nanoseconds") or v.get("_nanoseconds")
            if sec is not None:
                with suppress(Exception):
                    base = float(sec)
                    frac = float(nanos or 0) / 1_000_000_000.0
                    return datetime.fromtimestamp(base + frac, tz=timezone.utc)
            for sk in (
                "created_at",
                "created",
                "created_on",
                "updated_at",
                "updated",
                "timestamp",
                "time",
                "date",
                "epoch",
                "seconds",
                "unix",
                "iso",
                "iso8601",
                "activated_at",
                "activated",
                "start",
                "end",
            ):
                dt = _parse_dt_any(v.get(sk))
                if dt:
                    return dt
        dt = _parse_dt_any(v)
        if dt:
            return dt
    return None


def _whop_report_day_key(dt: datetime, scan_tz: timezone | ZoneInfo) -> str:
    return dt.astimezone(scan_tz).date().isoformat()


def _whop_report_brief_from_membership(membership: dict, *, api_client: WhopAPIClient | None) -> dict:
    if not isinstance(membership, dict):
        return {}
    status = str(membership.get("status") or "").strip()
    product = ""
    if isinstance(membership.get("product"), dict):
        product = str(membership["product"].get("title") or "").strip()
    renewal_end_iso = str(membership.get("renewal_period_end") or "").strip()
    renewal_end_dt = _parse_dt_any(renewal_end_iso) if renewal_end_iso else None
    remaining_days: int | str = ""
    if renewal_end_dt:
        delta = (renewal_end_dt - datetime.now(timezone.utc)).total_seconds()
        remaining_days = max(0, int((delta / 86400.0) + 0.999))
    total_raw = (
        membership.get("total_spent")
        or membership.get("total_spent_usd")
        or membership.get("total_spend")
        or membership.get("total_spend_usd")
    )
    total_spent = ""
    if str(total_raw or "").strip():
        amt = usd_amount(total_raw)
        total_spent = f"${amt:.2f}" if amt else str(total_raw).strip()
    manage_url_raw = str(membership.get("manage_url") or "").strip()
    manage_url = f"[Open]({manage_url_raw})" if manage_url_raw else ""
    user_id = _whop_report_extract_user_id(membership)
    dash = ""
    if user_id and api_client and getattr(api_client, "company_id", ""):
        dash = f"[Open](https://whop.com/dashboard/{str(api_client.company_id).strip()}/users/{user_id}/)"
    trial_days = (
        membership.get("trial_days")
        or membership.get("trial_period_days")
        or ((membership.get("plan") or {}).get("trial_days") if isinstance(membership.get("plan"), dict) else None)
    )
    plan_is_renewal = (
        membership.get("plan_is_renewal")
        or membership.get("is_renewal")
        or ((membership.get("plan") or {}).get("is_renewal") if isinstance(membership.get("plan"), dict) else None)
    )
    pricing = (
        membership.get("pricing")
        or ((membership.get("plan") or {}).get("price") if isinstance(membership.get("plan"), dict) else None)
    )
    return {
        "status": status or "—",
        "product": product or "—",
        "member_since": _fmt_date_any(membership.get("created_at")),
        "trial_end": _fmt_date_any(membership.get("trial_end") or membership.get("trial_ends_at") or membership.get("trial_end_at")),
        "trial_days": str(trial_days).strip() if str(trial_days or "").strip() else "",
        "plan_is_renewal": str(plan_is_renewal).strip() if str(plan_is_renewal or "").strip() else "",
        "pricing": str(pricing).strip() if str(pricing or "").strip() else "",
        "renewal_start": _fmt_date_any(membership.get("renewal_period_start")),
        "renewal_end": _fmt_date_any(membership.get("renewal_period_end")),
        "renewal_end_iso": renewal_end_iso or "",
        "remaining_days": remaining_days,
        "manage_url": manage_url,
        "dashboard_url": dash,
        "cancel_at_period_end": "yes" if membership.get("cancel_at_period_end") is True else ("no" if membership.get("cancel_at_period_end") is False else "—"),
        "is_first_membership": "true" if membership.get("is_first_membership") is True else ("false" if membership.get("is_first_membership") is False else "—"),
        "total_spent": total_spent,
        "last_payment_failure": str(membership.get("last_payment_failure") or "").strip(),
    }


def _whop_report_compute_events(
    membership: dict,
    *,
    start_utc: datetime,
    end_utc: datetime,
    api_client: WhopAPIClient | None,
) -> tuple[list[tuple[str, datetime]], dict, dict]:
    status_l = str(membership.get("status") or "").strip().lower()
    created_dt = _whop_report_pick_dt(membership, ["created_at", "createdAt", "created_on", "created", "started_at", "starts_at", "start_at", "member", "user", "timestamps", "dates"])
    activated_dt = _whop_report_pick_dt(membership, ["activated_at", "activatedAt", "current_period_start", "current_period_start_at", "starts_at", "started_at", "member", "user", "timestamps", "dates"])
    updated_dt = _whop_report_pick_dt(membership, ["updated_at", "updatedAt", "updated_on", "member", "user", "timestamps", "dates"])
    trial_end_dt = _whop_report_pick_dt(membership, ["trial_end", "trial_end_at", "trial_ends_at", "trial_end_on", "member", "user", "timestamps", "dates"])
    failure_dt = _whop_report_pick_dt(membership, ["last_payment_failure", "last_payment_failed_at", "payment_failed_at", "last_failed_payment_at", "member", "user", "timestamps", "dates"])
    cancel_dt = _whop_report_pick_dt(membership, ["cancel_at", "cancel_at_period_end_at", "cancellation_scheduled_at", "cancelled_at", "canceled_at", "updated_at", "member", "user", "timestamps", "dates"])

    trial_days_raw = (
        membership.get("trial_days")
        or membership.get("trial_period_days")
        or ((membership.get("plan") or {}).get("trial_days") if isinstance(membership.get("plan"), dict) else None)
    )
    try:
        trial_days = int(str(trial_days_raw).strip())
    except Exception:
        trial_days = 0

    is_trial = bool(
        status_l in {"trialing", "trial", "pending"}
        or trial_end_dt is not None
        or int(trial_days) > 0
    )

    brief = _whop_report_brief_from_membership(membership, api_client=api_client)
    spent = usd_amount(brief.get("total_spent"))

    def _in_range(dt: datetime | None) -> bool:
        return bool(dt and start_utc <= dt <= end_utc)

    buckets: list[tuple[str, datetime]] = []
    if is_trial:
        trial_dt = created_dt or activated_dt
        if _in_range(trial_dt):
            buckets.append(("new_trial", trial_dt))
    else:
        member_dt = activated_dt or created_dt
        if _in_range(member_dt):
            buckets.append(("new_member", member_dt))

    failure_dt = failure_dt or (updated_dt if status_l in {"past_due", "unpaid", "payment_failed"} else None)
    if _in_range(failure_dt):
        buckets.append(("payment_failed", failure_dt))

    if _whop_report_norm_bool(membership.get("cancel_at_period_end")) and status_l in {"active", "trialing"} and float(spent) > 1.0:
        if _in_range(cancel_dt):
            buckets.append(("cancellation_scheduled", cancel_dt))

    info = {
        "status_l": status_l,
        "created_dt": created_dt,
        "activated_dt": activated_dt,
        "updated_dt": updated_dt,
        "trial_end_dt": trial_end_dt,
        "failure_dt": failure_dt,
        "cancel_dt": cancel_dt,
        "trial_days": trial_days,
        "is_trial": is_trial,
        "spent": spent,
        "cancel_at_period_end": membership.get("cancel_at_period_end"),
    }
    return (buckets, brief, info)


def _whop_report_collect_date_fields(membership: dict) -> list[str]:
    if not isinstance(membership, dict):
        return []
    keys = [
        "created_at",
        "createdAt",
        "created_on",
        "created",
        "activated_at",
        "activatedAt",
        "updated_at",
        "updatedAt",
        "trial_end",
        "trial_end_at",
        "trial_ends_at",
        "renewal_period_start",
        "renewal_period_end",
        "cancel_at",
        "cancel_at_period_end_at",
        "cancellation_scheduled_at",
        "canceled_at",
        "cancelled_at",
        "last_payment_failure",
        "last_payment_failed_at",
        "payment_failed_at",
    ]
    found: list[str] = []
    for k in keys:
        v = membership.get(k)
        if v not in (None, "", {}):
            found.append(f"{k}={v}")
    for nested_key in ("member", "user", "timestamps", "dates"):
        nested = membership.get(nested_key)
        if isinstance(nested, dict):
            for k in keys:
                v = nested.get(k)
                if v not in (None, "", {}):
                    found.append(f"{nested_key}.{k}={v}")
    return found


async def _whop_report_find_membership_for_discord_id(discord_id: int, *, max_pages: int = 8) -> dict:
    if not whop_api_client:
        return {}
    after: str | None = None
    first = 100
    scanned = 0
    while scanned < max_pages:
        batch, page_info = await whop_api_client.list_memberships(first=first, after=after)
        if not batch:
            break
        for rec in batch:
            if not isinstance(rec, dict):
                continue
            membership = _whop_report_normalize_membership(rec)
            did = _whop_report_extract_discord_id(membership)
            if did and int(did) == int(discord_id):
                return membership
        scanned += 1
        after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
        has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
        if not has_next or not after:
            break
    return {}

#
# NOTE: `.checker report` (and its interactive UI) was removed (canonical cleanup).
#


async def _report_scan_whop(ctx, start: str, end: str, *, sample_csv: bool = False) -> None:
    """One-time scan of Whop event ledger to rebuild reporting_store.json and output report + CSV."""

    if not REPORTING_CONFIG.get("enabled"):
        await ctx.send("❌ Reporting is disabled in config.", delete_after=15)
        return

    if not (_report_load_store and _report_record_member_status_post and _report_prune_store and _report_save_store):
        await ctx.send("❌ Reporting store module is not available.", delete_after=20)
        return

    if not WHOP_EVENTS_ENABLED:
        await ctx.send("❌ Whop event ledger is disabled in config.", delete_after=20)
        return

    if not WHOP_EVENTS_FILE.exists():
        try:
            WHOP_EVENTS_FILE.touch(exist_ok=True)
        except Exception as e:
            await ctx.send(f"❌ Whop event ledger file not found ({e}).", delete_after=20)
            return

    # Mountain Time (boss): use America/Denver for day boundaries during dedupe.
    scan_tz = None
    if ZoneInfo is not None:
        with suppress(Exception):
            scan_tz = ZoneInfo("America/Denver")
    if scan_tz is None:
        scan_tz = timezone.utc

    def _parse_day_local(s: str) -> datetime | None:
        try:
            d = datetime.strptime(str(s or "").strip(), "%Y-%m-%d").date()
            return datetime(d.year, d.month, d.day, tzinfo=scan_tz)
        except Exception:
            return None

    start_local = _parse_day_local(start)
    end_local = _parse_day_local(end)
    if start_local is None or end_local is None:
        await ctx.send("❌ Bad date(s). Use YYYY-MM-DD YYYY-MM-DD, e.g. `2026-01-01 2026-01-31`.", delete_after=20)
        return

    end_local = end_local.replace(hour=23, minute=59, second=59)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    quiet_here = bool(MEMBER_STATUS_LOGS_CHANNEL_ID and getattr(ctx, "channel", None) and int(getattr(ctx.channel, "id", 0)) == int(MEMBER_STATUS_LOGS_CHANNEL_ID))
    status_msg = None
    scan_label = "Whop Events"
    if quiet_here:
        with suppress(Exception):
            status_msg = await ctx.author.send(
                f"🔍 **Scanning {scan_label} for report...**\n"
                f"```\nRange (MT): {start_local.date().isoformat()} → {end_local.date().isoformat()}\nInitializing...\n```"
            )
    else:
        status_msg = await ctx.send(
            f"🔍 **Scanning {scan_label} for report...**\n"
            f"```\nRange (MT): {start_local.date().isoformat()} → {end_local.date().isoformat()}\nInitializing...\n```"
        )

    scan_log_each_member = bool(REPORTING_CONFIG.get("scan_log_each_member"))
    scan_log_include_raw = bool(REPORTING_CONFIG.get("scan_log_include_raw_dates"))
    scan_log_max_members = int(REPORTING_CONFIG.get("scan_log_max_members") or 0)
    scan_log_progress_every = int(REPORTING_CONFIG.get("scan_log_progress_every") or 50)
    scan_log_count = 0

    log.info(
        "[ReportScan] start range_mt=%s→%s events_file=%s each_member=%s max_members=%s progress_every=%s",
        start_local.date().isoformat(),
        end_local.date().isoformat(),
        WHOP_EVENTS_FILE.name,
        scan_log_each_member,
        scan_log_max_members,
        scan_log_progress_every,
    )
    await _report_scan_log_message(
        f"🔍 Whop scan started (MT {start_local.date().isoformat()} → {end_local.date().isoformat()})"
    )

    started_at = time.time()
    last_edit = 0.0
    scanned = 0
    included = 0
    dupes = 0

    seen: set[tuple[str, str, str]] = set()
    seen_event_ids: set[str] = set()

    def _rate() -> float:
        dt = max(1e-6, time.time() - started_at)
        return float(scanned) / dt

    async def _progress(stage: str, *, force: bool = False) -> None:
        nonlocal last_edit
        now = time.time()
        if not status_msg:
            return
        if not force and (now - last_edit) < 2.0:
            return
        last_edit = now
        rate = _rate()
        await status_msg.edit(
            content=(
                f"🔍 **Scanning Whop Events for report...**\n"
                f"```\n"
                f"Stage: {stage}\n"
                f"Events scanned: {scanned}\n"
                f"Included (deduped): {included}\n"
                f"Dupes skipped: {dupes}\n"
                f"Rate: {rate:.1f} events/s\n"
                f"```"
            )[:1900]
        )

    retention = int(REPORTING_CONFIG.get("retention_weeks", 26))
    store = _report_load_store(BASE_DIR, retention_weeks=retention)
    if isinstance(store.get("meta"), dict):
        store["meta"]["version"] = int(store["meta"].get("version") or 1)
        store["meta"]["scan_source"] = "report.scan.whop.events"
        store["meta"]["scan_range_mt"] = f"{start_local.date().isoformat()}→{end_local.date().isoformat()}"
    store["weeks"] = {}
    store["members"] = {}
    store["unlinked"] = {}

    csv_rows: list[dict] = []
    totals = {"new_members": 0, "new_trials": 0, "payment_failed": 0, "cancellation_scheduled": 0}

    def _bucket_for_event(ev: dict) -> str:
        t = str(ev.get("event_type") or "").lower()
        status_l = str(ev.get("status") or "").lower()
        trial_days = str(ev.get("trial_days") or "").strip()
        cancel_flag = str(ev.get("cancel_at_period_end") or "").strip().lower()

        if "payment.failed" in t or "payment_failed" in t or "deactivated.payment_failure" in t:
            return "payment_failed"
        if "membership.activated.pending" in t or "trial" in t:
            return "new_trial"
        if "membership.activated" in t or "payment.succeeded.activation" in t:
            return "new_member"
        if "payment.succeeded" in t and status_l in {"active", "trialing"} and trial_days == "":
            return "new_member"
        if cancel_flag in {"true", "yes", "1"} and status_l in {"active", "trialing"}:
            return "cancellation_scheduled"
        return ""

    try:
        await _progress("read ledger", force=True)
        events = iter_jsonl(WHOP_EVENTS_FILE)
        for idx, ev in enumerate(events):
            scanned = idx + 1
            if not isinstance(ev, dict):
                continue
            event_id = str(ev.get("event_id") or "").strip()
            if event_id:
                if event_id in seen_event_ids:
                    dupes += 1
                    continue
                seen_event_ids.add(event_id)

            ev_dt = _parse_dt_any(ev.get("occurred_at"))
            if not ev_dt or not (start_utc <= ev_dt <= end_utc):
                continue

            bucket = _bucket_for_event(ev)
            if not bucket:
                continue

            membership_id = str(ev.get("membership_id") or "").strip()
            email = str(ev.get("email") or "").strip()
            discord_id_raw = str(ev.get("discord_id") or "").strip()
            discord_id = int(discord_id_raw) if discord_id_raw.isdigit() else None

            ident = membership_id or discord_id_raw or email
            if not ident:
                continue

            day_key = _whop_report_day_key(ev_dt, scan_tz)
            key = (bucket, ident, day_key)
            if key in seen:
                dupes += 1
                continue
            seen.add(key)

            event_kind = "unknown"
            if bucket == "payment_failed":
                event_kind = "payment_failed"
            elif bucket == "new_trial":
                event_kind = "trialing"
            elif bucket == "new_member":
                event_kind = "member_role_added"
            elif bucket == "cancellation_scheduled":
                event_kind = "cancellation_scheduled"

            whop_brief = {
                "product": str(ev.get("product") or ""),
                "status": str(ev.get("status") or ""),
                "total_spent": str(ev.get("total_spent") or ""),
                "cancel_at_period_end": str(ev.get("cancel_at_period_end") or ""),
                "renewal_end_iso": str(ev.get("renewal_end_iso") or ev.get("renewal_period_end") or ""),
                "dashboard_url": "",
            }

            store = _report_record_member_status_post(
                store,
                ts=int(ev_dt.astimezone(timezone.utc).timestamp()),
                event_kind=event_kind,
                discord_id=discord_id,
                email=email or "",
                whop_brief=whop_brief,
            )
            included += 1

            if bucket == "payment_failed":
                totals["payment_failed"] += 1
            elif bucket == "new_trial":
                totals["new_trials"] += 1
            elif bucket == "new_member":
                totals["new_members"] += 1
            elif bucket == "cancellation_scheduled":
                totals["cancellation_scheduled"] += 1

            source = ev.get("source_discord") if isinstance(ev.get("source_discord"), dict) else {}
            csv_rows.append(
                {
                    "day_mt": day_key,
                    "event_bucket": bucket,
                    "membership_id": membership_id,
                    "discord_id": str(discord_id or ""),
                    "email": email,
                    "product": str(ev.get("product") or ""),
                    "status": str(ev.get("status") or ""),
                    "total_spent": str(ev.get("total_spent") or ""),
                    "cancel_at_period_end": str(ev.get("cancel_at_period_end") or ""),
                    "renewal_end_iso": str(ev.get("renewal_end_iso") or ev.get("renewal_period_end") or ""),
                    "dashboard_url": "",
                    "source_channel_id": str(source.get("channel_id") or ""),
                    "source_message_id": str(source.get("message_id") or ""),
                    "source_jump_url": str(source.get("jump_url") or ""),
                    "event_type": str(ev.get("event_type") or ""),
                }
            )

            if scan_log_each_member and (scan_log_max_members <= 0 or scan_log_count < scan_log_max_members):
                scan_log_count += 1
                occurred_txt = ev_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                detail = (
                    "event_id={event_id} type={event_type} occurred={occurred} bucket={bucket} "
                    "membership_id={membership_id} discord_id={discord_id} email={email} status={status} product={product}"
                ).format(
                    event_id=event_id or "—",
                    event_type=str(ev.get("event_type") or "—"),
                    occurred=occurred_txt,
                    bucket=bucket or "—",
                    membership_id=membership_id or "—",
                    discord_id=discord_id_raw or "—",
                    email=email or "—",
                    status=str(ev.get("status") or "—"),
                    product=str(ev.get("product") or "—"),
                )
                log.info("[ReportScan] %s", detail)
                if REPORTING_CONFIG.get("scan_log_webhook_url"):
                    asyncio.create_task(_post_scan_log_webhook(f"[ReportScan] {detail}"))
                if scan_log_include_raw:
                    raw_text = json.dumps(ev, ensure_ascii=False)[:1000]
                    log.info("[ReportScan] raw_event=%s", raw_text)
                    if REPORTING_CONFIG.get("scan_log_webhook_url"):
                        asyncio.create_task(_post_scan_log_webhook(f"[ReportScan] raw_event={raw_text}"))

            if scan_log_progress_every > 0 and (idx + 1) % scan_log_progress_every == 0:
                await _report_scan_log_message(
                    f"⏳ Scan progress: scanned {idx + 1}, included {included}, dupes {dupes}"
                )
                with suppress(Exception):
                    await _progress(f"event {idx + 1}")
    except Exception as e:
        with suppress(Exception):
            if status_msg:
                await status_msg.edit(content=f"❌ Scan failed: `{e}`")
        await _report_scan_log_message(f"❌ Whop scan failed: {e}")
        return

    await _report_scan_log_message(
        f"✅ Whop scan finished: scanned {scanned}, included {included}, dupes {dupes}, rate {_rate():.1f} events/s"
    )

    # Prune store (best-effort: saving can fail on misconfigured server permissions)
    store = _report_prune_store(store, retention_weeks=retention)
    save_warning = ""
    if not sample_csv:
        try:
            async with _REPORTING_STORE_LOCK:
                global _REPORTING_STORE
                _REPORTING_STORE = store
                _report_save_store(BASE_DIR, store)
        except PermissionError as e:
            save_warning = str(e)[:240]
        except Exception:
            save_warning = "Failed to save reporting_store.json (unknown error)"

    # Optional: anonymized sample output (no real IDs/URLs/emails).
    if sample_csv:
        mem_map: dict[str, str] = {}
        did_map: dict[str, str] = {}
        email_map: dict[str, str] = {}

        def _map(d: dict, key: str, mp: dict[str, str], prefix: str) -> str:
            raw = str(d.get(key) or "").strip()
            if not raw:
                return ""
            if raw not in mp:
                mp[raw] = f"{prefix}{len(mp) + 1:04d}"
            return mp[raw]

        anon_rows: list[dict] = []
        for r in (csv_rows or [])[:200]:
            rr = dict(r)
            rr["membership_id"] = _map(rr, "membership_id", mem_map, "mem_SAMPLE_")
            rr["discord_id"] = _map(rr, "discord_id", did_map, "discord_SAMPLE_")
            rr["email"] = _map(rr, "email", email_map, "email_SAMPLE_")
            rr["dashboard_url"] = ""
            rr["source_channel_id"] = ""
            rr["source_message_id"] = ""
            rr["source_jump_url"] = ""
            anon_rows.append(rr)
        csv_rows = anon_rows

    # Build CSV attachment
    csv_buf = io.StringIO()
    fieldnames = [
        "day_mt",
        "event_bucket",
        "membership_id",
        "discord_id",
        "email",
        "product",
        "status",
        "total_spent",
        "cancel_at_period_end",
        "renewal_end_iso",
        "dashboard_url",
        "source_channel_id",
        "source_message_id",
        "source_jump_url",
        "event_type",
    ]
    w = csv.DictWriter(csv_buf, fieldnames=fieldnames)
    w.writeheader()
    for r in (csv_rows or []):
        with suppress(Exception):
            w.writerow(r)
    csv_bytes = csv_buf.getvalue().encode("utf-8", errors="ignore")

    fname = (
        f"rs_whop_report_SAMPLE_{start_local.date().isoformat()}_{end_local.date().isoformat()}.csv"
        if sample_csv
        else f"rs_whop_report_{start_local.date().isoformat()}_{end_local.date().isoformat()}.csv"
    )

    # Report embed (from scan totals)
    warn_note = ""
    if scanned == 0:
        warn_note = "\n⚠️ No events found in the ledger. Confirm whop-member-logs/webhooks are being recorded."
    elif included == 0:
        warn_note = "\n⚠️ Events found, but none matched the date range. Verify occurred_at timestamps."

    e = discord.Embed(
        title=f"RS Whop Scan Report ({start_local.date().isoformat()} → {end_local.date().isoformat()})",
        description=(
            "Source: Whop Event Ledger (deduped per member per day/event) • Timezone: `America/Denver`"
            + f"\nScanned: {scanned} events • Included: {included} • Dupes: {dupes}"
            + (" • Output: anonymized sample CSV" if sample_csv else "")
            + warn_note
            + (f"\n⚠️ Store not saved: {save_warning}" if save_warning else "")
        ),
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    e.add_field(name="New Members", value=str(totals["new_members"]), inline=False)
    e.add_field(name="New Trials", value=str(totals["new_trials"]), inline=False)
    e.add_field(name="Payment Failed", value=str(totals["payment_failed"]), inline=False)
    e.add_field(name="Cancellation Scheduled", value=str(totals["cancellation_scheduled"]), inline=False)
    e.set_footer(text="RSCheckerbot • Reporting")

    file_obj = discord.File(fp=io.BytesIO(csv_bytes), filename=fname)

    # DM Neo (configured) and the invoker, but avoid duplicate send when invoker is Neo.
    dm_uid = int(REPORTING_CONFIG.get("dm_user_id") or 0)
    targets: list[int] = []
    if dm_uid:
        targets.append(dm_uid)
    if ctx.author and getattr(ctx.author, "id", None):
        targets.append(int(ctx.author.id))
    targets = list(dict.fromkeys([int(x) for x in targets if int(x) > 0]))

    invoker_id = int(getattr(ctx.author, "id", 0) or 0) if ctx.author else 0
    invoker_dm_failed = False
    for uid in targets:
        try:
            user = bot.get_user(uid) or await bot.fetch_user(uid)
            if not user:
                continue
            # New file object per send
            f = discord.File(fp=io.BytesIO(csv_bytes), filename=fname)
            await user.send(embed=e, file=f)
        except Exception:
            # Most common: user has DMs closed. Tell the invoker explicitly so it doesn't look like "no CSV".
            if invoker_id and int(uid) == int(invoker_id):
                invoker_dm_failed = True

    with suppress(Exception):
        if status_msg:
            if invoker_dm_failed:
                await status_msg.edit(
                    content=(
                        "✅ Scan complete. Report attempted via DM (with CSV).\n"
                        "⚠️ Could not DM the invoker (DMs likely closed)."
                    )[:1900]
                )
            else:
                await status_msg.edit(content="✅ Scan complete. Report sent via DM (with CSV).")
    if invoker_dm_failed:
        with suppress(Exception):
            await ctx.send(
                "⚠️ I couldn’t DM you the report/CSV (DMs likely closed). "
                "Enable DMs for this server (User Settings → Privacy & Safety → Server DMs), then rerun the scan.",
                delete_after=35,
            )
    await _report_scan_log_message(
        f"✅ **Whop scan complete**\nScanned: {scanned} • Included: {included}"
    )
    return


async def _report_debug_whop(ctx, *, target: str, start: str = "", end: str = "") -> None:
    """Debug a Whop membership record and show parsed report fields."""
    if not REPORTING_CONFIG.get("enabled"):
        await ctx.send("❌ Reporting is disabled in config.", delete_after=15)
        return
    if not whop_api_client or not getattr(whop_api_client, "list_memberships", None):
        await ctx.send("❌ Whop API is not configured for reporting.", delete_after=20)
        return

    target_s = str(target or "").strip()
    if not target_s:
        await ctx.send("❌ Debug usage: `.checker report debug <discord_id|membership_id> [YYYY-MM-DD YYYY-MM-DD]`", delete_after=20)
        return

    start_dt = _parse_date_ymd(start) if start else None
    end_dt = _parse_date_ymd(end) if end else None
    now_local = _tz_now()
    if start_dt is None and end_dt is None:
        end_utc = now_local.astimezone(timezone.utc)
        start_utc = end_utc - timedelta(days=30)
    else:
        if start_dt is None and end_dt is not None:
            start_dt, end_dt = end_dt, None
        if start_dt is None:
            start_dt = now_local
        if end_dt is None:
            end_dt = now_local
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
        start_utc = start_dt.astimezone(timezone.utc)
        end_utc = end_dt.astimezone(timezone.utc)

    membership_id = ""
    discord_id: int | None = None
    if target_s.startswith("mem_") or target_s.startswith("R-"):
        membership_id = target_s
    elif target_s.isdigit() and 17 <= len(target_s) <= 19:
        discord_id = int(target_s)
        membership_id = str(_membership_id_from_history(discord_id) or "").strip()
    else:
        membership_id = target_s

    membership: dict = {}
    if membership_id:
        membership = await whop_api_client.get_membership_by_id(membership_id) or {}
    if (not membership) and discord_id:
        try:
            max_pages = int(WHOP_API_CONFIG.get("report_debug_max_pages") or 8)
        except Exception:
            max_pages = 8
        if max_pages <= 0:
            max_pages = 8
        membership = await _whop_report_find_membership_for_discord_id(discord_id, max_pages=max_pages)
        if membership:
            membership_id = _whop_report_membership_id(membership)

    if not membership:
        await ctx.send("❌ Debug: membership not found. Try a membership_id (`mem_...`) or check member_history link.", delete_after=25)
        return

    membership = _whop_report_normalize_membership(membership)
    membership_id = _whop_report_membership_id(membership) or membership_id
    discord_id = _whop_report_extract_discord_id(membership) or discord_id
    email = _whop_report_extract_email(membership)

    buckets, brief, info = _whop_report_compute_events(
        membership,
        start_utc=start_utc,
        end_utc=end_utc,
        api_client=whop_api_client,
    )

    def _fmt_dt(dt: datetime | None) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if dt else "—"

    dates_block = "\n".join(
        [
            f"created: {_fmt_dt(info.get('created_dt'))}",
            f"activated: {_fmt_dt(info.get('activated_dt'))}",
            f"updated: {_fmt_dt(info.get('updated_dt'))}",
            f"trial_end: {_fmt_dt(info.get('trial_end_dt'))}",
            f"payment_failed: {_fmt_dt(info.get('failure_dt'))}",
            f"cancel_at: {_fmt_dt(info.get('cancel_dt'))}",
        ]
    )[:1024]

    raw_dates = _whop_report_collect_date_fields(membership)
    raw_block = ("\n".join(raw_dates)[:1024]) if raw_dates else "—"

    bucket_lines = [f"{b} @ {_fmt_dt(dt)}" for b, dt in buckets] if buckets else ["—"]

    product = str((membership.get("product") or {}).get("title") or "").strip() if isinstance(membership.get("product"), dict) else ""
    status_l = str(info.get("status_l") or "").strip() or "—"

    e = discord.Embed(
        title="RS Whop Debug",
        description=f"Target: `{target_s}`\nRange: {start_utc.date().isoformat()} → {end_utc.date().isoformat()}",
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    e.add_field(name="Membership ID", value=membership_id or "—", inline=True)
    e.add_field(name="Discord ID", value=str(discord_id or "—"), inline=True)
    e.add_field(name="Email", value=email or "—", inline=True)
    e.add_field(name="Status", value=status_l, inline=True)
    e.add_field(name="Product", value=product or "—", inline=True)
    e.add_field(name="Trial Days", value=str(info.get("trial_days") or "0"), inline=True)
    e.add_field(name="Is Trial", value="yes" if info.get("is_trial") else "no", inline=True)
    e.add_field(name="Cancel At Period End", value=str(info.get("cancel_at_period_end") or "—"), inline=True)
    e.add_field(name="Total Spent", value=str(brief.get("total_spent") or "—"), inline=True)
    e.add_field(name="Parsed Dates", value=dates_block or "—", inline=False)
    e.add_field(name="Raw Date Fields", value=raw_block, inline=False)
    e.add_field(name="Computed Buckets", value="\n".join(bucket_lines)[:1024], inline=False)
    e.set_footer(text="RSCheckerbot • Reporting Debug")

    await _dm_user(int(ctx.author.id), embed=e)
    await ctx.send("✅ Debug report sent via DM.", delete_after=20)


async def _report_scan_member_status(ctx, start: str, end: str) -> None:
    """One-time scan: member-status-logs history into reporting_store.json (then use `.checker report`)."""

    if not REPORTING_CONFIG.get("enabled"):
        await ctx.send("❌ Reporting is disabled in config.", delete_after=15)
        return

    if not (MEMBER_STATUS_LOGS_CHANNEL_ID and _report_record_member_status_post and _report_prune_store and _report_save_store):
        await ctx.send("❌ Reporting store is not available or member-status-logs channel is not set.", delete_after=20)
        return

    ch_raw = bot.get_channel(int(MEMBER_STATUS_LOGS_CHANNEL_ID))
    if not isinstance(ch_raw, discord.TextChannel):
        await ctx.send("❌ member-status-logs channel not found / not a text channel.", delete_after=20)
        return

    start_dt = _parse_date_ymd(start) if start else None
    end_dt = _parse_date_ymd(end) if end else None
    now_local = _tz_now()
    if start_dt is None or end_dt is None:
        await ctx.send("❌ Provide start+end dates (YYYY-MM-DD YYYY-MM-DD).", delete_after=20)
        return
    end_dt = end_dt.replace(hour=23, minute=59, second=59)

    start_utc = start_dt.astimezone(timezone.utc)
    end_utc = end_dt.astimezone(timezone.utc)

    quiet_here = bool(MEMBER_STATUS_LOGS_CHANNEL_ID and getattr(ctx, "channel", None) and int(getattr(ctx.channel, "id", 0)) == int(MEMBER_STATUS_LOGS_CHANNEL_ID))
    status_msg = None
    if quiet_here:
        with suppress(Exception):
            status_msg = await ctx.author.send(
                f"⏳ Backfill started for <#{MEMBER_STATUS_LOGS_CHANNEL_ID}> "
                f"({start_utc.date().isoformat()} → {end_utc.date().isoformat()})."
            )
    else:
        status_msg = await ctx.send(
            f"⏳ Backfill started for <#{MEMBER_STATUS_LOGS_CHANNEL_ID}> "
            f"({start_utc.date().isoformat()} → {end_utc.date().isoformat()}).",
            delete_after=3600,
        )

    # Fresh store for the scanned window (one-time rebuild)
    retention = int(REPORTING_CONFIG.get("retention_weeks", 26))
    store: dict = {
        "meta": {"version": 1, "retention_weeks": retention, "backfill_range": f"{start_utc.date()}→{end_utc.date()}"},
        "weeks": {},
        "members": {},
        "unlinked": {},
    }

    scanned = 0
    captured = 0
    last_edit = datetime.now(timezone.utc)

    async for m in ch_raw.history(after=start_utc, before=(end_utc + timedelta(seconds=1)), limit=None, oldest_first=True):
        scanned += 1
        # Only backfill RSCheckerbot outputs (avoid user chatter / other bots)
        if not bot.user or m.author.id != bot.user.id:
            continue
        if not m.embeds:
            continue
        emb = m.embeds[0]
        fallback_ts = int(m.created_at.replace(tzinfo=timezone.utc).timestamp())
        ts_i, kind, discord_id, whop_brief = _extract_reporting_from_member_status_embed(emb, fallback_ts=fallback_ts)

        store = _report_record_member_status_post(
            store,
            ts=ts_i,
            event_kind=kind,
            discord_id=discord_id,
            email="",
            whop_brief=whop_brief or None,
        )
        captured += 1

        # Update progress occasionally
        if (captured % 200) == 0:
            now = datetime.now(timezone.utc)
            if (now - last_edit).total_seconds() >= 10:
                last_edit = now
                with suppress(Exception):
                    if status_msg:
                        await status_msg.edit(content=f"⏳ Backfill in progress… scanned {scanned}, captured {captured}")

    store = _report_prune_store(store, retention_weeks=retention)

    save_warning = ""
    async with _REPORTING_STORE_LOCK:
        global _REPORTING_STORE
        _REPORTING_STORE = store
        try:
            _report_save_store(BASE_DIR, store)
        except PermissionError as e:
            save_warning = str(e)[:240]
        except Exception:
            save_warning = "Failed to save reporting_store.json (unknown error)"

    with suppress(Exception):
        if status_msg:
            warn = f"\n⚠️ Store not saved: {save_warning}" if save_warning else ""
            await status_msg.edit(content=f"✅ Backfill complete — scanned {scanned}, captured {captured}. Building report…{warn}")

    # DM report (Neo + invoker). Avoid channel spam (especially in member-status-logs).
    try:
        e = await _build_report_embed(start_utc, end_utc, title_prefix="RS Backfill Report (member-status-logs)")
        dm_uid = int(REPORTING_CONFIG.get("dm_user_id") or 0)
        targets: list[int] = []
        if dm_uid:
            targets.append(dm_uid)
        if ctx.author and getattr(ctx.author, "id", None):
            targets.append(int(ctx.author.id))
        targets = list(dict.fromkeys([int(x) for x in targets if int(x) > 0]))
        for uid in targets:
            await _dm_user(uid, embed=e)
    except Exception:
        pass

    if not quiet_here:
        await ctx.send(
            f"✅ Backfill complete — scanned {scanned}, captured {captured}. Report sent via DM.",
            delete_after=25,
        )
    return


@bot.command(name="purgecases", aliases=["purgecasechannels", "deletecases", "deletecasechannels"])
@commands.has_permissions(administrator=True)
async def purge_case_channels(ctx, confirm: str = ""):
    """Delete legacy per-user payment case channels under the configured category.

    Usage:
      .checker purgecases confirm
    """
    LEGACY_CATEGORY_ID = 1458533733681598654
    if str(confirm or "").strip().lower() != "confirm":
        await ctx.send("❌ Confirmation required. Use: `.checker purgecases confirm`", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        await ctx.send("❌ Guild not found / bot not ready.", delete_after=10)
        with suppress(Exception):
            await ctx.message.delete()
        return

    category = guild.get_channel(LEGACY_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        await ctx.send("❌ Legacy case category not found or not a category.", delete_after=12)
        with suppress(Exception):
            await ctx.message.delete()
        return

    deleted = 0
    skipped = 0
    failed = 0

    for ch in list(category.channels):
        if not isinstance(ch, discord.TextChannel):
            skipped += 1
            continue
        nm = str(ch.name or "")
        topic = str(ch.topic or "")
        # Category is dedicated to legacy case channels; be permissive in matching.
        is_legacy = ("rschecker_payment_case" in topic) or nm.startswith("pay-")
        if not is_legacy:
            skipped += 1
            continue
        try:
            await ch.delete(reason="RSCheckerbot: purge legacy payment case channels")
            deleted += 1
        except Exception:
            failed += 1

    await ctx.send(f"✅ purgecases complete — deleted: {deleted}, skipped: {skipped}, failed: {failed}", delete_after=20)
    with suppress(Exception):
        await ctx.message.delete()


class _FutureMemberAuditView(discord.ui.View):
    def __init__(
        self,
        *,
        invoker_user_id: int,
        guild_id: int,
        member_role_id: int,
        future_role_id: int,
        exclude_role_ids: list[int],
        candidate_ids: list[int],
        totals: dict,
    ):
        super().__init__(timeout=15 * 60)
        self.invoker_user_id = int(invoker_user_id)
        self.guild_id = int(guild_id)
        self.member_role_id = int(member_role_id)
        self.future_role_id = int(future_role_id)
        self.exclude_role_ids = [int(x) for x in (exclude_role_ids or []) if int(x) > 0]
        self.candidate_ids = [int(x) for x in (candidate_ids or []) if int(x) > 0]
        self.totals = totals if isinstance(totals, dict) else {}
        self.message: discord.Message | None = None
        self._applied = False

    def _is_allowed_clicker(self, interaction: discord.Interaction) -> bool:
        try:
            if int(interaction.user.id) == int(self.invoker_user_id):
                return True
            perms = getattr(interaction.user, "guild_permissions", None)
            return bool(perms and perms.administrator)
        except Exception:
            return False

    async def _deny(self, interaction: discord.Interaction) -> None:
        with suppress(Exception):
            await interaction.response.send_message("❌ Not allowed (invoker/admins only).", ephemeral=True)

    async def _disable(self, *, cancelled: bool = False) -> None:
        for child in self.children:
            child.disabled = True
        if self.message:
            with suppress(Exception):
                if cancelled:
                    e = self.message.embeds[0] if self.message.embeds else discord.Embed(color=0x5865F2)
                    e = e.copy()
                    e.title = (str(e.title or "").strip() + " (cancelled)").strip()
                    await self.message.edit(embed=e, view=self)
                else:
                    await self.message.edit(view=self)

    async def on_timeout(self) -> None:
        await self._disable(cancelled=True)

    @discord.ui.button(label="Confirm apply", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not self._is_allowed_clicker(interaction):
            await self._deny(interaction)
            return
        if self._applied:
            with suppress(Exception):
                await interaction.response.send_message("Already applied.", ephemeral=True)
            return

        await interaction.response.defer()
        await self._disable()
        self._applied = True

        guild = bot.get_guild(int(self.guild_id))
        if not guild:
            with suppress(Exception):
                await interaction.followup.send("❌ Guild not found / bot not ready.", ephemeral=True)
            return
        future_role = guild.get_role(int(self.future_role_id))
        member_role = guild.get_role(int(self.member_role_id))
        if not future_role or not member_role:
            with suppress(Exception):
                await interaction.followup.send("❌ Required role(s) not found. Check config.", ephemeral=True)
            return

        # Permission + hierarchy checks (fail-fast).
        me = getattr(guild, "me", None) or getattr(guild, "self_member", None) or guild.get_member(getattr(bot.user, "id", 0))
        if not isinstance(me, discord.Member):
            with suppress(Exception):
                await interaction.followup.send("❌ Cannot resolve bot member in guild.", ephemeral=True)
            return
        if not getattr(me.guild_permissions, "manage_roles", False):
            with suppress(Exception):
                await interaction.followup.send("❌ Missing permission: Manage Roles.", ephemeral=True)
            return
        try:
            if future_role >= me.top_role:
                with suppress(Exception):
                    await interaction.followup.send(
                        "❌ Bot cannot assign Future Member role (role hierarchy). Move the bot role above it.",
                        ephemeral=True,
                    )
                return
        except Exception:
            pass

        def _is_staff(m: discord.Member) -> bool:
            try:
                p = m.guild_permissions
                if bool(p.administrator or p.manage_guild or p.manage_roles):
                    return True
            except Exception:
                pass
            try:
                if self.exclude_role_ids:
                    rids = {int(r.id) for r in (m.roles or [])}
                    if any(int(x) in rids for x in self.exclude_role_ids):
                        return True
            except Exception:
                pass
            return False

        added = 0
        skipped_now_has_member = 0
        skipped_already_future = 0
        skipped_staff = 0
        skipped_staff_role = 0
        skipped_missing_member = 0
        failed = 0

        started = time.time()
        last_edit = 0.0

        async def _edit_progress(stage: str, processed: int, *, force: bool = False) -> None:
            nonlocal last_edit
            if not self.message:
                return
            now = time.time()
            if not force and (now - last_edit) < 2.0:
                return
            last_edit = now
            e = discord.Embed(
                title="Future Member Audit — applying",
                description="Bulk role update in progress.",
                color=0xED4245,
                timestamp=datetime.now(timezone.utc),
            )
            e.add_field(name="Stage", value=str(stage)[:256], inline=False)
            e.add_field(name="Processed", value=str(processed), inline=True)
            e.add_field(name="Added", value=str(added), inline=True)
            e.add_field(name="Failed", value=str(failed), inline=True)
            e.add_field(name="Skipped (now has Member)", value=str(skipped_now_has_member), inline=True)
            e.add_field(name="Skipped (already Future)", value=str(skipped_already_future), inline=True)
            e.add_field(name="Skipped (staff/admin)", value=str(skipped_staff), inline=True)
            e.add_field(name="Skipped (staff role)", value=str(skipped_staff_role), inline=True)
            e.add_field(name="Skipped (member not found)", value=str(skipped_missing_member), inline=True)
            e.set_footer(text=f"RSCheckerbot • futurememberaudit • {int(now - started)}s elapsed")
            with suppress(Exception):
                await self.message.edit(embed=e, view=self)

        await _edit_progress("starting", 0, force=True)

        processed = 0
        for uid in list(self.candidate_ids):
            processed += 1
            m = guild.get_member(int(uid))
            if not isinstance(m, discord.Member):
                with suppress(Exception):
                    m = await guild.fetch_member(int(uid))
            if not isinstance(m, discord.Member):
                skipped_missing_member += 1
                await _edit_progress("apply", processed)
                continue
            if m.bot:
                skipped_staff += 1
                skipped_staff_role += 1
                await _edit_progress("apply", processed)
                continue
            if _is_staff(m):
                skipped_staff += 1
                # Track staff-role skips separately when applicable
                try:
                    if self.exclude_role_ids:
                        rids = {int(r.id) for r in (m.roles or [])}
                        if any(int(x) in rids for x in self.exclude_role_ids):
                            skipped_staff_role += 1
                except Exception:
                    pass
                await _edit_progress("apply", processed)
                continue
            role_ids = {r.id for r in (m.roles or [])}
            if int(self.member_role_id) in role_ids:
                skipped_now_has_member += 1
                await _edit_progress("apply", processed)
                continue
            if int(self.future_role_id) in role_ids:
                skipped_already_future += 1
                await _edit_progress("apply", processed)
                continue
            try:
                await m.add_roles(future_role, reason="RSCheckerbot: futurememberaudit (missing Member role)")
                added += 1
            except Exception:
                failed += 1
            await _edit_progress("apply", processed)
            await asyncio.sleep(max(0.2, ROLE_UPDATE_BATCH_SECONDS))

        await _edit_progress("complete", processed, force=True)
        done = discord.Embed(
            title="Future Member Audit — complete",
            description="Bulk role update finished.",
            color=0x57F287 if failed == 0 else 0xFEE75C,
            timestamp=datetime.now(timezone.utc),
        )
        done.add_field(name="Candidates (initial)", value=str(len(self.candidate_ids)), inline=True)
        done.add_field(name="Added", value=str(added), inline=True)
        done.add_field(name="Failed", value=str(failed), inline=True)
        done.add_field(name="Skipped (now has Member)", value=str(skipped_now_has_member), inline=True)
        done.add_field(name="Skipped (already Future)", value=str(skipped_already_future), inline=True)
        done.add_field(name="Skipped (staff/admin)", value=str(skipped_staff), inline=True)
        done.add_field(name="Skipped (staff role)", value=str(skipped_staff_role), inline=True)
        done.add_field(name="Skipped (member not found)", value=str(skipped_missing_member), inline=True)
        done.set_footer(text="RSCheckerbot • futurememberaudit")
        with suppress(Exception):
            if self.message:
                await self.message.edit(embed=done, view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not self._is_allowed_clicker(interaction):
            await self._deny(interaction)
            return
        await interaction.response.defer()
        await self._disable(cancelled=True)


@bot.command(name="futurememberaudit", aliases=["futureaudit", "auditfuture"])
@commands.has_permissions(administrator=True)
async def future_member_audit(ctx):
    """Scan Discord members missing the Member role and (after confirmation) add Future Member role."""
    # Restrict to the configured primary guild.
    if ctx.guild and GUILD_ID and int(ctx.guild.id) != int(GUILD_ID):
        await ctx.send("❌ This command is only allowed in the main server.", delete_after=12)
        with suppress(Exception):
            await ctx.message.delete()
        return

    guild = bot.get_guild(GUILD_ID) if GUILD_ID else None
    if not guild:
        await ctx.send("❌ Guild not found / bot not ready.", delete_after=10)
        with suppress(Exception):
            await ctx.message.delete()
        return

    # Resolve role IDs from config (no hardcoding).
    try:
        member_role_id = int(str(ROLE_CANCEL_A or "").strip())
        future_role_id = int(str(ROLE_TO_ASSIGN or "").strip())
    except Exception:
        member_role_id = 0
        future_role_id = 0
    if not member_role_id or not future_role_id:
        await ctx.send("❌ Missing role IDs in config (dm_sequence.role_cancel_a / role_to_assign).", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    member_role = guild.get_role(member_role_id)
    future_role = guild.get_role(future_role_id)
    if not member_role or not future_role:
        await ctx.send("❌ Required role(s) not found in guild. Check role IDs in config.", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    exclude_role_ids = sorted(list(FUTURE_MEMBER_AUDIT_EXCLUDE_ROLE_IDS))

    def _is_staff(m: discord.Member) -> bool:
        try:
            p = m.guild_permissions
            if bool(p.administrator or p.manage_guild or p.manage_roles):
                return True
        except Exception:
            pass
        try:
            if exclude_role_ids:
                rids = {int(r.id) for r in (m.roles or [])}
                if any(int(x) in rids for x in exclude_role_ids):
                    return True
        except Exception:
            pass
        return False

    def _plain_user(m: discord.Member) -> str:
        # Plain-text username (used alongside a clickable mention in samples).
        try:
            uname = str(getattr(m, "name", "") or "").strip()
        except Exception:
            uname = ""
        if not uname:
            try:
                uname = str(getattr(m, "display_name", "") or "").strip()
            except Exception:
                uname = ""
        uname = uname.replace("\n", " ").strip()
        if not uname:
            uname = f"user_{int(m.id)}"
        return f"@{uname}"

    # Prefer posting preview + confirmation in member-status-logs.
    status_ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID) if MEMBER_STATUS_LOGS_CHANNEL_ID else None
    if not isinstance(status_ch, discord.TextChannel):
        await ctx.send("❌ member-status-logs channel not found / not configured.", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    # Let the invoker know we’re running, but avoid channel spam.
    with suppress(Exception):
        await ctx.send(
            "🔍 Scanning members… I will post the preview + confirmation in member-status-logs.",
            delete_after=15,
        )

    scanned = 0
    bots_skipped = 0
    staff_skipped = 0
    staff_role_skipped = 0
    has_member = 0
    missing_member = 0
    already_future = 0
    candidates: list[int] = []
    sample_lines: list[str] = []

    # Fetch members for correctness (cache can be partial on large guilds).
    try:
        async for m in guild.fetch_members(limit=None):
            scanned += 1
            if not isinstance(m, discord.Member):
                continue
            if m.bot:
                bots_skipped += 1
                continue
            if _is_staff(m):
                staff_skipped += 1
                try:
                    if exclude_role_ids:
                        rids = {int(r.id) for r in (m.roles or [])}
                        if any(int(x) in rids for x in exclude_role_ids):
                            staff_role_skipped += 1
                except Exception:
                    pass
                continue
            role_ids = {r.id for r in (m.roles or [])}
            if member_role_id in role_ids:
                has_member += 1
                continue
            missing_member += 1
            if future_role_id in role_ids:
                already_future += 1
                continue
            candidates.append(int(m.id))
            if len(sample_lines) < 20:
                # Clickable mention (will ping) + plain username for readability.
                disp = str(getattr(m, "display_name", "") or "").strip() or str(getattr(m, "name", "") or "").strip() or f"user_{int(m.id)}"
                disp = disp.replace("\n", " ").strip()
                sample_lines.append(f"- {m.mention} {disp} ({_plain_user(m)}) • `{m.id}`")
    except Exception as e:
        await ctx.send(f"❌ Scan failed: {e}", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    totals = {
        "scanned": scanned,
        "bots_skipped": bots_skipped,
        "staff_skipped": staff_skipped,
        "missing_member": missing_member,
        "already_future": already_future,
        "candidates": len(candidates),
    }

    e = discord.Embed(
        title="Future Member Audit — preview",
        description=(
            "This will add the Future Member role to members who are missing the Member role.\n"
            "No changes happen until Confirm."
        ),
        color=0xFEE75C if candidates else 0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    e.add_field(name="Member role", value=_fmt_role(member_role_id, guild), inline=False)
    e.add_field(name="Future Member role", value=_fmt_role(future_role_id, guild), inline=False)
    e.add_field(name="Scanned", value=str(scanned), inline=True)
    e.add_field(name="Has Member role", value=str(has_member), inline=True)
    e.add_field(name="Skipped (bots)", value=str(bots_skipped), inline=True)
    e.add_field(name="Skipped (staff/admin)", value=str(staff_skipped), inline=True)
    e.add_field(name="Skipped (staff role)", value=str(staff_role_skipped), inline=True)
    e.add_field(name="Missing Member role", value=str(missing_member), inline=True)
    e.add_field(name="Already has Future role", value=str(already_future), inline=True)
    e.add_field(name="Will add Future role to", value=str(len(candidates)), inline=True)
    sample_txt = "\n".join(sample_lines) if sample_lines else "—"
    if len(candidates) > len(sample_lines):
        sample_txt += f"\n… and {len(candidates) - len(sample_lines)} more"
    e.add_field(name="Sample", value=sample_txt[:1024], inline=False)
    e.set_footer(text="RSCheckerbot • futurememberaudit")

    view = _FutureMemberAuditView(
        invoker_user_id=int(getattr(ctx.author, "id", 0) or 0),
        guild_id=int(guild.id),
        member_role_id=int(member_role_id),
        future_role_id=int(future_role_id),
        exclude_role_ids=exclude_role_ids,
        candidate_ids=candidates,
        totals=totals,
    )
    try:
        msg = await status_ch.send(embed=e, view=view, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))
        view.message = msg
    except Exception as ex:
        await ctx.send(f"❌ Failed to post preview in member-status-logs: {ex}", delete_after=15)

    with suppress(Exception):
        await ctx.message.delete()


@bot.command(name="dmenable")
@commands.has_permissions(administrator=True)
async def dm_enable(ctx):
    """Enable DM sequence"""
    settings = load_settings()
    settings["dm_sequence_enabled"] = True
    save_settings(settings)
    await ctx.send("✅ DM sequence enabled", delete_after=5)
    try:
        await ctx.message.delete()
    except Exception:
        pass

@bot.command(name="dmdisable")
@commands.has_permissions(administrator=True)
async def dm_disable(ctx):
    """Disable DM sequence"""
    settings = load_settings()
    settings["dm_sequence_enabled"] = False
    save_settings(settings)
    await ctx.send("⛔ DM sequence disabled", delete_after=5)
    try:
        await ctx.message.delete()
    except Exception:
        pass

@bot.command(name="dmstatus")
@commands.has_permissions(administrator=True)
async def dm_status(ctx):
    """Show DM sequence status"""
    settings = load_settings()
    status = "ENABLED" if settings.get("dm_sequence_enabled", True) else "DISABLED"
    emoji = "✅" if settings.get("dm_sequence_enabled", True) else "⛔"
    await ctx.send(f"{emoji} DM sequence: **{status}**", delete_after=10)
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name="whois", aliases=["whof"])
@commands.has_permissions(administrator=True)
async def whois_member(ctx, member: discord.Member):
    """Whop API-first lookup for a Discord user.

    Usage:
      .checker whois @user
    """
    try:
        embed = discord.Embed(
            title="🔎 Whop Lookup",
            description=f"Lookup for {member.mention}",
            color=0x2B2D31,
            timestamp=datetime.now(timezone.utc),
        )
        _apply_member_header(embed, member)
        access = _access_roles_plain(member)
        embed.add_field(name="Member", value=member.mention, inline=True)
        embed.add_field(name="Access", value=access or "—", inline=True)

        brief = _whop_summary_for_member(member.id)
        if not (isinstance(brief, dict) and brief):
            mid = _membership_id_from_history(member.id)
            brief = await _fetch_whop_brief_by_membership_id(mid) if mid else {}
        lines: list[str] = []
        if brief:
            if brief.get("product") and brief["product"] != "—":
                lines.append(str(brief["product"]))
            if brief.get("status") and brief["status"] != "—":
                lines.append(f"Status {brief['status']}")
            if brief.get("member_since") and brief["member_since"] != "—":
                lines.append(f"Member since {brief['member_since']}")
            if brief.get("renewal_start") != "—" and brief.get("renewal_end") != "—":
                lines.append(f"Renewal {brief['renewal_start']} to {brief['renewal_end']}")
            if brief.get("cancel_at_period_end") == "yes":
                lines.append("Cancels at period end")
            if brief.get("last_payment_method") and brief["last_payment_method"] != "—":
                lines.append(f"Method {brief['last_payment_method']}")
            if brief.get("last_payment_type") and brief["last_payment_type"] != "—":
                lines.append(f"Type {brief['last_payment_type']}")
            if brief.get("last_payment_failure"):
                lines.append(f"Last failure: {brief['last_payment_failure']}")
        else:
            lines.append("No Whop membership link found for this Discord user.")
        summary = "\n".join([str(x) for x in lines if str(x).strip()])[:1024] or "—"
        embed.add_field(name="Summary", value=summary, inline=False)

        await ctx.send(embed=embed, delete_after=30)
    except Exception as e:
        await ctx.send(f"❌ whois error: {e}", delete_after=10)
    finally:
        with suppress(Exception):
            await ctx.message.delete()


@bot.command(name="whopmembership", aliases=["whopmember", "whopmem"])
@commands.has_permissions(administrator=True)
async def whop_membership_lookup(ctx, membership_id: str):
    """Direct Whop membership lookup by membership_id.

    Usage:
      .checker whopmembership mem_...
    """
    global whop_api_client
    if not whop_api_client:
        await ctx.send("❌ Whop API client is not initialized.", delete_after=10)
        return

    mid = (membership_id or "").strip()
    if not mid:
        await ctx.send("❌ Provide a membership_id.", delete_after=10)
        return

    brief = await _fetch_whop_brief_by_membership_id(mid)
    if not brief:
        await ctx.send("❌ Whop lookup failed (no data returned).", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    embed = discord.Embed(
        title="🔎 Whop Membership",
        description="Membership lookup complete.",
        color=0x2B2D31,
        timestamp=datetime.now(timezone.utc),
    )
    lines: list[str] = []
    if brief.get("product") and brief["product"] != "—":
        lines.append(str(brief["product"]))
    if brief.get("status") and brief["status"] != "—":
        lines.append(f"Status {brief['status']}")
    if brief.get("member_since") and brief["member_since"] != "—":
        lines.append(f"Member since {brief['member_since']}")
    if brief.get("renewal_start") != "—" and brief.get("renewal_end") != "—":
        lines.append(f"Renewal {brief['renewal_start']} to {brief['renewal_end']}")
    if brief.get("cancel_at_period_end") == "yes":
        lines.append("Cancels at period end")
    if brief.get("last_payment_method") and brief["last_payment_method"] != "—":
        lines.append(f"Method {brief['last_payment_method']}")
    if brief.get("last_payment_type") and brief["last_payment_type"] != "—":
        lines.append(f"Type {brief['last_payment_type']}")
    if brief.get("last_payment_failure"):
        lines.append(f"Last failure: {brief['last_payment_failure']}")

    summary = "\n".join([str(x) for x in lines if str(x).strip()])[:1024] or "—"
    embed.add_field(name="Summary", value=summary, inline=False)
    await ctx.send(embed=embed, delete_after=30)
    with suppress(Exception):
        await ctx.message.delete()

@bot.command(name="syncsummary", aliases=["whopsync", "whopsyncsummary", "sync-report"])
@commands.has_permissions(administrator=True)
async def whop_sync_summary_dm(ctx, start: str = "", end: str = ""):
    """DM a boss-friendly report to the invoker.

    - With NO dates: re-send the latest **Whop Sync Summary (mirror)** + CSV.
    - With dates: generate a **Whop memberships joined report** filtered by join/created date
      (from Whop API `created_after/created_before`) and DM an embed + CSV.

    Date formats accepted:
    - YYYY-MM-DD
    - MM-DD-YY / MM-DD-YYYY
    - MM/DD/YY / MM/DD/YYYY
    - MM/DD or MM-DD (assumes current year)
    """
    # Build label
    def _parse_user_day(s: str) -> date | None:
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

    start_d = _parse_user_day(start)
    end_d = _parse_user_day(end)
    if start_d and not end_d:
        end_d = start_d
    if (not start_d) and end_d:
        start_d = end_d
    has_user_range = bool(start_d or end_d)
    if not start_d:
        start_d = datetime.now(timezone.utc).date()
    if not end_d:
        end_d = start_d

    label = f"{start_d.strftime('%m-%d-%y')}" if start_d == end_d else f"{start_d.strftime('%m-%d-%y')}-{end_d.strftime('%m-%d-%y')}"

    # ------------------------------------------------------------------
    # Mode 1: date range => Whop "joined" report (filter by created_after/before)
    # ------------------------------------------------------------------
    if has_user_range:
        if not whop_api_client:
            await ctx.send("❌ Whop API client is not initialized.", delete_after=20)
            with suppress(Exception):
                await ctx.message.delete()
            return

        # Filters to match Whop dashboard "Users" view (Joined at).
        try:
            require_dj = bool(WHOP_API_CONFIG.get("joined_report_require_date_joined", True))
        except Exception:
            require_dj = False
        statuses_cfg = WHOP_API_CONFIG.get("joined_report_statuses")
        allowed_statuses: set[str] = set()
        if isinstance(statuses_cfg, list):
            allowed_statuses = {str(x).strip().lower() for x in statuses_cfg if str(x).strip()}
        if not allowed_statuses:
            # Safe default excludes draft attempts.
            allowed_statuses = {"active", "trialing", "canceling", "completed", "canceled", "expired"}
        prefixes_cfg = WHOP_API_CONFIG.get("joined_report_product_title_prefixes")
        product_prefixes: list[str] = []
        if isinstance(prefixes_cfg, list):
            product_prefixes = [str(x).strip() for x in prefixes_cfg if str(x).strip()]
        try:
            max_pages = int(WHOP_API_CONFIG.get("joined_report_max_pages", 50))
        except Exception:
            max_pages = 50
        max_pages = max(1, min(max_pages, 200))

        tz = timezone.utc
        tz_name = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
        if ZoneInfo is not None:
            with suppress(Exception):
                tz = ZoneInfo(tz_name)
        start_local = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=tz)
        end_local = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=tz)
        start_utc_iso = start_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        end_utc_iso = end_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

        # Pull memberships in range (server-side filter) to map member_id -> products/memberships.
        # Then pull members in range (Whop dashboard "Users" view) and join via member_id.
        mber_to_memberships: dict[str, dict] = {}

        def _extract_member_id_from_membership(mship: dict) -> str:
            if not isinstance(mship, dict):
                return ""
            mm = mship.get("member")
            if isinstance(mm, str) and mm.strip().startswith("mber_"):
                return mm.strip()
            if isinstance(mm, dict):
                mid0 = str(mm.get("id") or mm.get("member_id") or "").strip()
                if mid0.startswith("mber_"):
                    return mid0
            mid2 = str(mship.get("member_id") or "").strip()
            return mid2 if mid2.startswith("mber_") else ""

        after: str | None = None
        pages = 0
        per_page = 100
        while pages < max_pages:
            batch, page_info = await whop_api_client.list_memberships(
                first=per_page,
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
            for rec in batch:
                if not isinstance(rec, dict):
                    continue
                m = _whop_report_normalize_membership(rec)
                if not isinstance(m, dict):
                    continue
                mid = _whop_report_membership_id(m)
                st = str(m.get("status") or "").strip().lower() or "unknown"
                if st not in allowed_statuses:
                    continue
                cape = (m.get("cancel_at_period_end") is True) or _whop_report_norm_bool(m.get("cancel_at_period_end"))

                email_s = _whop_report_extract_email(m)
                user_id = _whop_report_extract_user_id(m)
                created_at = str(m.get("created_at") or "").strip()
                # Whop UI "Joined at" matches membership created_at in most cases.
                created_dt = _parse_dt_any(created_at) if created_at else None
                date_joined = str(m.get("date_joined") or m.get("date_joined_at") or "").strip()
                dj_dt = _parse_dt_any(date_joined) if date_joined else None
                joined_dt = dj_dt if isinstance(dj_dt, datetime) else created_dt
                if require_dj and not isinstance(dj_dt, datetime):
                    continue
                if isinstance(joined_dt, datetime):
                    joined_d = joined_dt.astimezone(tz).date()
                    if joined_d < start_d or joined_d > end_d:
                        continue
                renewal_end = str(m.get("renewal_period_end") or "").strip()
                product_title = ""
                if isinstance(m.get("product"), dict):
                    product_title = str(m["product"].get("title") or "").strip()
                if product_prefixes:
                    low = product_title.lower()
                    if not any(low.startswith(p.lower()) for p in product_prefixes):
                        continue

                mber_id = _extract_member_id_from_membership(m)
                if not mber_id:
                    continue
                rec0 = mber_to_memberships.get(mber_id)
                if not isinstance(rec0, dict):
                    rec0 = {"products": set(), "membership_ids": set()}
                    mber_to_memberships[mber_id] = rec0
                with suppress(Exception):
                    if product_title:
                        rec0["products"].add(str(product_title))
                    if mid:
                        rec0["membership_ids"].add(str(mid))

            after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
            has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
            if not has_next or not after:
                break

        # Pull members in range (descending by joined_at; stop once we're before start boundary).
        user_rows: list[dict] = []
        after = None
        pages = 0
        stop = False
        while pages < max_pages and not stop:
            batch, page_info = await whop_api_client.list_members(
                first=per_page,
                after=after,
                params={"order": "joined_at", "direction": "desc"},
            )
            if not batch:
                break
            pages += 1
            for rec in batch:
                if not isinstance(rec, dict):
                    continue
                mber_id = str(rec.get("id") or "").strip()
                joined_at_raw = str(rec.get("joined_at") or rec.get("created_at") or "").strip()
                dtj = _parse_dt_any(joined_at_raw) if joined_at_raw else None
                if not isinstance(dtj, datetime):
                    continue
                joined_d = dtj.astimezone(tz).date()
                if joined_d < start_d:
                    stop = True
                    break
                if joined_d > end_d:
                    continue

                # Membership mapping (best-effort). Some Whop "Users" rows may not have membership IDs in the export;
                # keep them in the report to match the dashboard, but product/membership columns may be blank.
                mm = mber_to_memberships.get(mber_id)

                # Bucket: prefer most_recent_action (Whop UI status), fall back to status.
                status = str(rec.get("status") or "").strip().lower() or "unknown"
                action = str(rec.get("most_recent_action") or "").strip().lower()
                bucket = action if action in {"joined", "trialing", "canceling", "churned", "left", "past_due"} else status
                if bucket not in {"joined", "trialing", "canceling", "churned", "left", "past_due"}:
                    bucket = "joined" if status == "joined" else ("left" if status == "left" else "joined")

                u = rec.get("user") if isinstance(rec.get("user"), dict) else {}
                user_id = str(u.get("id") or "").strip()
                email_s = str(u.get("email") or "").strip()
                name_s = str(u.get("name") or "").strip()
                username_s = str(u.get("username") or "").strip()
                spent = rec.get("usd_total_spent")
                spent_s = f"${float(spent or 0.0):.2f}"

                products = []
                mids = []
                if isinstance(mm, dict):
                    products = sorted([str(x).strip() for x in (mm.get("products") or set()) if str(x).strip()])
                    mids = sorted([str(x).strip() for x in (mm.get("membership_ids") or set()) if str(x).strip()])

                user_rows.append(
                    {
                        "member_id": mber_id,
                        "user_id": user_id,
                        "email": email_s,
                        "name": name_s,
                        "username": username_s,
                        "status_bucket": bucket,
                        "most_recent_action": action,
                        "products": ", ".join(products),
                        "membership_ids": ", ".join(mids),
                        "joined_at": dtj.isoformat().replace("+00:00", "Z"),
                        "total_spent": spent_s,
                    }
                )

            after = str(page_info.get("end_cursor") or "") if isinstance(page_info, dict) else ""
            has_next = bool(page_info.get("has_next_page")) if isinstance(page_info, dict) else False
            if not has_next or not after:
                break

        total_users = len(user_rows)
        counts: dict[str, int] = {}
        for r in user_rows:
            k = str(r.get("status_bucket") or "unknown").strip().lower() or "unknown"
            counts[k] = int(counts.get(k, 0)) + 1

        joined = int(counts.get("joined", 0))
        trialing = int(counts.get("trialing", 0))
        canceling = int(counts.get("canceling", 0))
        churned = int(counts.get("churned", 0))
        left = int(counts.get("left", 0))
        past_due = int(counts.get("past_due", 0))

        churn_pct = (float(churned) / float(total_users) * 100.0) if total_users else 0.0

        # Per-product breakdown (unique users; based on membership mapping).
        prod_counts: dict[str, int] = {}
        unknown_products = 0
        for r in user_rows:
            prods = [p.strip() for p in str(r.get("products") or "").split(",") if p.strip()]
            if not prods:
                unknown_products += 1
            for p in prods:
                prod_counts[p] = int(prod_counts.get(p, 0)) + 1
        prod_lines: list[str] = []
        for p, n in sorted(prod_counts.items(), key=lambda kv: kv[1], reverse=True)[:8]:
            prod_lines.append(f"**{p}** — {n}")
        extra = max(0, len(prod_counts) - 8)
        if extra:
            prod_lines.append(f"_… +{extra} more product(s)_")
        if unknown_products:
            prod_lines.append(f"**(Unknown product)** — {unknown_products}")

        e = discord.Embed(
            title=f"Whop Joined Summary — {label}",
            description=f"Range: `{start_d.isoformat()} → {end_d.isoformat()}`",
            color=0x5865F2,
            timestamp=datetime.now(timezone.utc),
        )
        e.add_field(name="Users (range)", value=str(total_users), inline=True)
        e.add_field(name="Joined", value=str(joined), inline=True)
        e.add_field(name="Trialing", value=str(trialing), inline=True)
        e.add_field(name="Canceling", value=str(canceling), inline=True)
        e.add_field(name="Churned", value=str(churned), inline=True)
        e.add_field(name="Left", value=str(left), inline=True)
        e.add_field(name="Past due", value=str(past_due), inline=True)
        e.add_field(name="Churn %", value=f"{churn_pct:.2f}%", inline=True)
        if prod_lines:
            e.add_field(name="By product", value="\n".join(prod_lines)[:1024], inline=False)
        e.set_footer(text="RSCheckerbot • Whop API")

        # CSV attachment (DM-only)
        buf = io.StringIO()
        fieldnames = [
            "member_id",
            "user_id",
            "email",
            "name",
            "username",
            "status_bucket",
            "most_recent_action",
            "products",
            "membership_ids",
            "total_spent",
            "joined_at",
        ]
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        for r in user_rows:
            with suppress(Exception):
                w.writerow({k: str(r.get(k, "") or "") for k in fieldnames})
        data = buf.getvalue().encode("utf-8")
        fname = f"whop-joined-report_{label}.csv".replace("/", "-")
        file_obj = discord.File(fp=io.BytesIO(data), filename=fname)

        try:
            await ctx.author.send(embed=e, file=file_obj)
        except Exception:
            await ctx.send("❌ I couldn't DM you (your DMs are likely closed).", delete_after=20)
            with suppress(Exception):
                await ctx.message.delete()
            return

        await ctx.send("✅ Sent Whop joined report via DM.", delete_after=10)
        with suppress(Exception):
            await ctx.message.delete()
        return

    # Resolve mirror channel (where the bot posts the summary + CSV).
    try:
        mirror_enabled = bool(WHOP_API_CONFIG.get("sync_summary_enabled", False))
    except Exception:
        mirror_enabled = False
    try:
        mirror_gid = int(WHOP_API_CONFIG.get("sync_summary_output_guild_id") or 0) if mirror_enabled else 0
    except Exception:
        mirror_gid = 0
    mirror_name = str(WHOP_API_CONFIG.get("sync_summary_output_channel_name") or "").strip()
    if not (mirror_gid and mirror_name):
        await ctx.send("❌ Sync summary mirror is not configured in `whop_api.*`.", delete_after=20)
        with suppress(Exception):
            await ctx.message.delete()
        return

    g2 = bot.get_guild(int(mirror_gid))
    if not g2:
        await ctx.send("❌ Mirror guild not found / bot not ready.", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    ch = await _get_or_create_text_channel(g2, name=mirror_name, category_id=STAFF_ALERTS_CATEGORY_ID)
    if not isinstance(ch, discord.TextChannel):
        await ctx.send("❌ Mirror channel not found / not a text channel.", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    # Find the most recent summary message (prefer one with CSV attachment).
    found: discord.Message | None = None
    async for m in ch.history(limit=50):
        if bot.user and int(getattr(m.author, "id", 0) or 0) != int(bot.user.id):
            continue
        if not m.embeds:
            continue
        t = str((m.embeds[0].title or "")).strip()
        if "Whop Sync Summary" not in t:
            continue
        found = m
        # Prefer the one with the csv attached.
        if any(str(a.filename or "").lower().endswith(".csv") for a in (m.attachments or [])):
            break

    if not found or not found.embeds:
        await ctx.send("❌ Could not find a recent Whop Sync Summary message to DM.", delete_after=20)
        with suppress(Exception):
            await ctx.message.delete()
        return

    # Clone embed and relabel for boss-friendly reporting.
    base = found.embeds[0]
    e = discord.Embed.from_dict(base.to_dict())
    e.title = f"Whop Sync Summary — {label}"

    file_obj: discord.File | None = None
    att = None
    for a in (found.attachments or []):
        if str(a.filename or "").lower().endswith(".csv"):
            att = a
            break
    if att is not None:
        try:
            data = await att.read()
            fname = f"whop-sync-report_{label}.csv".replace("/", "-")
            file_obj = discord.File(fp=io.BytesIO(data), filename=fname)
        except Exception:
            file_obj = None

    # DM-only output.
    try:
        if file_obj:
            await ctx.author.send(embed=e, file=file_obj)
        else:
            await ctx.author.send(embed=e)
    except Exception:
        await ctx.send("❌ I couldn't DM you (your DMs are likely closed).", delete_after=20)
        with suppress(Exception):
            await ctx.message.delete()
        return

    await ctx.send("✅ Sent Whop Sync Summary via DM.", delete_after=10)
    with suppress(Exception):
        await ctx.message.delete()


@bot.command(name="canceling", aliases=["cancelling", "set-to-cancel", "settocancel"])
@commands.has_permissions(administrator=True)
async def run_canceling_snapshot(ctx) -> None:
    """Manually run the Whop canceling snapshot (same as startup) into configured destinations."""
    with suppress(Exception):
        await ctx.message.delete()
    try:
        if not whop_api_client:
            await ctx.send("❌ Whop API client is not initialized.", delete_after=15)
            return
        # Run the same snapshot logic (respects reporting.* config including clear_channel).
        await _startup_canceling_members_snapshot()
        await ctx.send("✅ Posted canceling snapshot (Neo + any configured mirrors).", delete_after=10)
    except Exception:
        log.exception("[Canceling] manual snapshot failed")
        await ctx.send("❌ Canceling snapshot failed (see logs).", delete_after=20)

@bot.command(name="start")
@commands.has_permissions(administrator=True)
async def start_sequence(ctx, member: discord.Member):
    if not has_trigger_role(member):
        await ctx.reply("❗ User does not have the trigger role; sequence only starts after that role is added.")
        return
    if has_sequence_before(member.id):
        await ctx.reply("User already had sequence before; not starting again.")
        return
    enqueue_first_day(member.id)
    await ctx.reply(f"Queued day_1 for {m_user(member)} now.")
    e = _make_dyno_embed(
        member=member,
        description=f"{member.mention} queued for day_1 (admin)",
        footer=f"ID: {member.id}",
        color=0x5865F2,
    )
    await log_first(embed=e)

@bot.command(name="cancel")
@commands.has_permissions(administrator=True)
async def cancel_sequence(ctx, member: discord.Member):
    if str(member.id) not in queue_state:
        await ctx.reply("User not in active queue.")
        return
    mark_cancelled(member.id, "admin_cancel")
    await ctx.reply(f"Cancelled sequence for {m_user(member)}.")
    e = _make_dyno_embed(
        member=member,
        description=f"{member.mention} sequence cancelled (admin)",
        footer=f"ID: {member.id}",
        color=0xFEE75C,
    )
    await log_other(embed=e)

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--check-config", action="store_true", help="Validate config + secrets and exit (no Discord connection).")
    args = parser.parse_args()

    if args.check_config:
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")
        if errors:
            print("[ConfigCheck] FAILED")
            for e in errors:
                print(f"- {e}")
            raise SystemExit(2)
        print("[ConfigCheck] OK")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        raise SystemExit(0)

    if not TOKEN:
        raise RuntimeError("bot_token must be set in config.secrets.json (server-only)")

    # Prevent duplicate event spam when multiple processes start.
    if not _acquire_single_instance_lock():
        raise SystemExit(0)
    bot.run(TOKEN)

