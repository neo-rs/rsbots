import os
import sys
import json
import asyncio
import re
import csv
import io
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
from typing import Dict, Optional
from contextlib import suppress
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

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

import discord
from discord.ext import commands, tasks
from aiohttp import web
import aiohttp

from rschecker_utils import (
    load_json,
    save_json,
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

# Import Whop webhook handler
from whop_webhook_handler import (
    initialize as init_whop_handler,
    handle_whop_webhook_message,
)

# Import Whop API client
from whop_api_client import WhopAPIClient, WhopAPIError

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

    return {
        "enabled": bool(enabled),
        "dm_user_id": dm_user_id or 0,
        "timezone": tz,
        "report_time_local": report_time,
        "weekly_day_local": weekly_day,
        "retention_weeks": int(retention_weeks),
        "reminder_days_before_cancel": cleaned_days,
    }

REPORTING_CONFIG = _load_reporting_config(config)
_REPORTING_STORE: dict | None = None
_REPORTING_STORE_LOCK: asyncio.Lock = asyncio.Lock()


def _tz_now() -> datetime:
    tz_name = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
    if ZoneInfo is None:
        return datetime.now(timezone.utc)
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.now(timezone.utc)


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

# -----------------------------
# ENV / CONSTANTS
# -----------------------------
TOKEN = config.get("bot_token")
GUILD_ID = config.get("guild_id")

# Whop API Config
WHOP_API_CONFIG = config.get("whop_api", {})
WHOP_API_KEY = WHOP_API_CONFIG.get("api_key", "")
WHOP_RESOLUTION_CATEGORY_ID = WHOP_API_CONFIG.get("resolution_category_id")
WHOP_DISPUTE_CHANNEL_NAME = WHOP_API_CONFIG.get("dispute_channel_name", "dispute-fighter")
WHOP_RESOLUTION_CHANNEL_NAME = WHOP_API_CONFIG.get("resolution_channel_name", "resolution-center")
WHOP_SUPPORT_PING_ROLE_ID = WHOP_API_CONFIG.get("support_ping_role_id", "")
WHOP_SUPPORT_PING_ROLE_NAME = WHOP_API_CONFIG.get("support_ping_role_name", "")

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
log = logging.getLogger("rs-checker")

# -----------------------------
# Discord client
# -----------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.invites = True

bot = commands.Bot(command_prefix=commands.when_mentioned_or(".checker "), intents=intents)

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

# Channels for Whop resolution/dispute reporting (created/located at runtime)
WHOP_DISPUTE_CHANNEL_ID: int | None = None
WHOP_RESOLUTION_CHANNEL_ID: int | None = None

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
    return await fetch_whop_brief(
        whop_api_client,
        mid,
        enable_enrichment=bool(WHOP_API_CONFIG.get("enable_enrichment", True)),
    )


def _whop_placeholder_brief(state: str) -> dict:
    """Return a placeholder Whop brief with consistent keys for staff embeds."""
    st = str(state or "").strip().lower()
    if st == "pending":
        dash = "Linking…"
        spent = "Linking…"
        status = "—"
    else:
        dash = "Not linked yet"
        spent = "Not linked yet"
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
            wh["last_summary"] = summary
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
            await ch.send(embed=e, allowed_mentions=discord.AllowedMentions.none())

async def log_other(msg: str | None = None, *, embed: discord.Embed | None = None):
    ch = bot.get_channel(LOG_OTHER_CHANNEL_ID)
    if ch:
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
            await ch.send(embed=e, allowed_mentions=discord.AllowedMentions.none())

async def log_role_event(message: str | None = None, *, embed: discord.Embed | None = None):
    await log_other(message, embed=embed)

async def log_whop(msg: str):
    """Log to Whop logs channel (for subscription data from Whop system)"""
    if WHOP_LOGS_CHANNEL_ID:
        ch = bot.get_channel(WHOP_LOGS_CHANNEL_ID)
        if ch:
            with suppress(Exception):
                await ch.send(msg)

def _find_text_channel_by_name(guild: discord.Guild, name: str) -> discord.TextChannel | None:
    nm = (name or "").strip().lower()
    if not guild or not nm:
        return None
    for ch in guild.text_channels:
        if str(ch.name or "").lower() == nm:
            return ch
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
    guild = bot.get_guild(GUILD_ID) if GUILD_ID else None
    if not guild:
        return

    ch: discord.TextChannel | None = None
    if channel_name:
        ch = _find_text_channel_by_name(guild, channel_name)
        if ch is None:
            log.warning(f"[Log] Requested channel '{channel_name}' not found; falling back to member_status_logs_channel_id")
    if ch is None and MEMBER_STATUS_LOGS_CHANNEL_ID:
        base = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
        ch = base if isinstance(base, discord.TextChannel) else None

    if not ch:
        return

    async def _maybe_capture_for_reporting(sent_embed: discord.Embed, *, sent_channel_id: int) -> None:
        """Persist only member-status-logs output into the reporting store (bounded)."""
        if not REPORTING_CONFIG.get("enabled"):
            return
        if not MEMBER_STATUS_LOGS_CHANNEL_ID or int(sent_channel_id) != int(MEMBER_STATUS_LOGS_CHANNEL_ID):
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
        if embed is None:
            embed = discord.Embed(
                description=msg,
                color=0x5865F2,  # Discord blurple color
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text="RSCheckerbot • Member Status Tracking")
        # Allow user mentions for clickable member references (no role/everyone mentions).
        sent = await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))
        try:
            await _maybe_capture_for_reporting(embed, sent_channel_id=int(ch.id))
        except Exception:
            pass
        return sent
    except Exception:
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

async def handle_whop_webhook_receiver(request):
    """Receive Whop webhook payloads, log them, and forward to Discord"""
    try:
        # Get raw payload
        if request.content_type == 'application/json':
            payload = await request.json()
        else:
            # Try form data
            form_data = await request.post()
            payload = dict(form_data)
        
        # Get headers (for debugging)
        headers = dict(request.headers)
        
        # Log the raw payload
        _save_raw_webhook_payload(payload, headers)
        log.info(f"Received Whop webhook payload (saved to {WHOP_WEBHOOK_RAW_LOG_FILE.name})")
        
        # Forward to Discord webhook if URL is configured
        if not DISCORD_WEBHOOK_URL:
            log.warning("Discord webhook URL not configured - payload logged but not forwarded")
            return web.Response(text="OK (logged, not forwarded - no webhook URL)", status=200)
        
        # Forward to Discord webhook
        async with aiohttp.ClientSession() as session:
            async with session.post(
                DISCORD_WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as resp:
                if resp.status == 200 or resp.status == 204:
                    log.info(f"Forwarded webhook to Discord successfully (status: {resp.status})")
                    return web.Response(text="OK", status=200)
                else:
                    error_text = await resp.text()
                    log.error(f"Failed to forward webhook to Discord (status: {resp.status}): {error_text}")
                    return web.Response(text=f"Forward failed: {error_text}", status=500)
    
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
    await site.start()
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
    
    # Get all members with Member role
    member_role = guild.get_role(ROLE_CANCEL_A)
    if not member_role:
        log.warning("Member role not found, skipping sync")
        return
    
    members_to_check = [m for m in guild.members if member_role in m.roles]
    log.info(f"Checking {len(members_to_check)} members with Member role...")

    auto_heal_enabled = bool(WHOP_API_CONFIG.get("auto_heal_add_members_role", False))
    # Safety defaults:
    # - Role removals are dangerous; default to disabled unless explicitly enabled.
    # - Sync logging can be noisy; default to silent unless explicitly disabled.
    enforce_removals = bool(WHOP_API_CONFIG.get("enforce_role_removals", False))
    sync_silent = bool(WHOP_API_CONFIG.get("sync_silent", True))
    post_cancel_cards = bool(WHOP_API_CONFIG.get("post_cancellation_scheduled_cards", False)) and (not sync_silent)
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
    
    for member in members_to_check:
        try:
            # Check membership status via API (membership_id-based; avoids mismatched users)
            membership_id = _membership_id_from_history(member.id)
            if not membership_id:
                continue
            verification = await whop_api_client.verify_membership_status(membership_id, "active")

            # Cancellation scheduled signal (active/trialing but cancel_at_period_end=true).
            # This does NOT DM users; it's staff-only visibility (case channel + member status logs).
            membership_data = verification.get("membership_data") if isinstance(verification, dict) else None
            if isinstance(membership_data, dict):
                status_now = str(membership_data.get("status") or "").strip().lower()
                cape_now = membership_data.get("cancel_at_period_end")
                if cape_now is True and status_now in ("active", "trialing"):
                    if not post_cancel_cards:
                        # Startup/6h sync is allowed to stay silent (report-only mode).
                        continue
                    # Only emit this alert once per membership per renewal_end per day (prevents spam).
                    renewal_end_key = str(membership_data.get("renewal_period_end") or "").strip()
                    issue_key = f"cancel_scheduled:{membership_id}:{renewal_end_key}"

                    access = _access_roles_plain(member)
                    whop_brief = _whop_summary_for_member(member.id)
                    if not (isinstance(whop_brief, dict) and whop_brief):
                        whop_brief = await _fetch_whop_brief_by_membership_id(membership_id)
                    # Noise control: only log Cancellation Scheduled for paying members (>$1) and active status.
                    # (Trials/$0 are extremely noisy and do not match reporting requirements.)
                    if status_now != "active" or float(usd_amount(whop_brief.get("total_spent"))) <= 1.0:
                        continue
                    if not await should_post_and_record_alert(
                        STAFF_ALERTS_FILE,
                        discord_id=member.id,
                        issue_key=issue_key,
                        cooldown_hours=24.0,
                    ):
                        continue

                    # Detailed card -> member-status-logs
                    hist = get_member_history(member.id) or {}
                    acc = hist.get("access") if isinstance(hist.get("access"), dict) else {}
                    detailed = _build_member_status_detailed_embed(
                        title="⚠️ Cancellation Scheduled",
                        member=member,
                        access_roles=access,
                        color=0xFEE75C,
                        event_kind="cancellation_scheduled",
                        member_kv=[
                            ("first_joined", _fmt_ts(hist.get("first_join_ts"), "D") if hist.get("first_join_ts") else "—"),
                            ("join_count", hist.get("join_count") or "—"),
                            ("ever_had_member_role", "yes" if acc.get("ever_had_member_role") is True else "no"),
                            ("first_access", _fmt_ts(acc.get("first_access_ts"), "D") if acc.get("first_access_ts") else "—"),
                            ("last_access", _fmt_ts(acc.get("last_access_ts"), "D") if acc.get("last_access_ts") else "—"),
                        ],
                        discord_kv=[
                            ("reason", "cancel_at_period_end=true"),
                        ],
                        whop_brief=whop_brief,
                    )
                    await log_member_status("", embed=detailed)

                    # Minimal card -> member-cancelation
                    minimal = _build_case_minimal_embed(
                        title="⚠️ Cancellation Scheduled",
                        member=member,
                        access_roles=access,
                        whop_brief=whop_brief,
                        color=0xFEE75C,
                        event_kind="cancellation_scheduled",
                    )
                    await log_member_status("", embed=minimal, channel_name=MEMBER_CANCELLATION_CHANNEL_NAME)
            
            if not verification["matches"]:
                actual_status = verification["actual_status"]
                
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
        except Exception as e:
            error_count += 1
            log.error(f"Error syncing member {member.id}: {e}")

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
                e.add_field(name="Cancellation Scheduled cards", value="on" if post_cancel_cards else "off", inline=True)
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
    
    # Backfill Whop timeline from whop_history.json (before initializing whop handler)
    _backfill_whop_timeline_from_whop_history()
    # Remove deprecated link cache file (no longer used).
    try:
        link_path = BASE_DIR / "whop_discord_link.json"
        if link_path.exists():
            link_path.unlink()
            if post_startup_report:
                startup_kv.append(("removed_whop_discord_link_json", "yes"))
    except Exception as e:
        if post_startup_report:
            startup_kv.append(("whop_discord_link_remove_error", str(e)[:120]))
    
    guild = bot.get_guild(GUILD_ID)
    if guild:
        log.info(f"[Bot] Connected to: {guild.name}")

        # Ensure the two staff alert channels exist (payment-failure + member-cancelation).
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
        
        # Ensure Whop dispute/resolution channels exist (optional, config-driven)
        await _ensure_whop_resolution_channels(guild)
        
        log.info("-"*60)
        
        # Initialize invite usage cache
        try:
            invites = await guild.invites()
            for invite in invites:
                invite_usage_cache[invite.code] = invite.uses
            log.info(f"[Invites] Cached {len(invite_usage_cache)} invites")
        except Exception as e:
            log.error(f"[Invites] Error caching invites: {e}")
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
    
    # Initialize Whop webhook handler
    if WHOP_WEBHOOK_CHANNEL_ID or WHOP_LOGS_CHANNEL_ID:
        init_whop_handler(
            webhook_channel_id=WHOP_WEBHOOK_CHANNEL_ID,
            whop_logs_channel_id=WHOP_LOGS_CHANNEL_ID,
            role_trigger=ROLE_TRIGGER,
            welcome_role_id=WELCOME_ROLE_ID,
            role_cancel_a=ROLE_CANCEL_A,
            role_cancel_b=ROLE_CANCEL_B,
            lifetime_role_ids=sorted(list(LIFETIME_ROLE_IDS)),
            log_other_func=log_other,
            log_member_status_func=log_member_status,
            fmt_user_func=_fmt_user,
            member_status_logs_channel_id=MEMBER_STATUS_LOGS_CHANNEL_ID,
            record_member_whop_summary_func=record_member_whop_summary,
            whop_api_key=WHOP_API_KEY,
            whop_api_config=WHOP_API_CONFIG,
            dispute_channel_id=WHOP_DISPUTE_CHANNEL_ID,
            resolution_channel_id=WHOP_RESOLUTION_CHANNEL_ID,
            support_ping_role_id=WHOP_SUPPORT_PING_ROLE_ID,
            support_ping_role_name=WHOP_SUPPORT_PING_ROLE_NAME,
        )
        channels = []
        if WHOP_WEBHOOK_CHANNEL_ID:
            channels.append(f"webhook channel {WHOP_WEBHOOK_CHANNEL_ID}")
        if WHOP_LOGS_CHANNEL_ID:
            channels.append(f"logs channel {WHOP_LOGS_CHANNEL_ID}")
        log.info(f"[Whop] Webhook handler initialized for {', '.join(channels)}")
    else:
        log.warning("[Whop] Webhook/logs channel IDs not configured - webhook handler disabled")
    
    # Schedule periodic cleanup (every 24 hours)
    @tasks.loop(hours=24)
    async def periodic_cleanup():
        cleanup_old_data()
        cleanup_old_invites()
    
    periodic_cleanup.start()
    log.info("[Cleanup] Periodic cleanup scheduled (every 24 hours)")
    
    # Start Whop membership sync job if enabled
    if whop_api_client and WHOP_API_CONFIG.get("enable_sync", True):
        sync_interval = WHOP_API_CONFIG.get("sync_interval_hours", 6)
        if not sync_whop_memberships.is_running():
            sync_whop_memberships.change_interval(hours=sync_interval)
            sync_whop_memberships.start()
            log.info(f"[Whop Sync] Membership sync job started (every {sync_interval} hours)")

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


async def _ensure_whop_resolution_channels(guild: discord.Guild) -> None:
    """Ensure dispute/resolution channels exist under configured category (best-effort)."""
    global WHOP_DISPUTE_CHANNEL_ID, WHOP_RESOLUTION_CHANNEL_ID

    if not WHOP_API_CONFIG.get("enable_resolution_reporting", True):
        return
    if not guild:
        return
    if not WHOP_RESOLUTION_CATEGORY_ID:
        return
    try:
        category_id_int = int(str(WHOP_RESOLUTION_CATEGORY_ID).strip())
    except Exception:
        return

    category = guild.get_channel(category_id_int)
    if not isinstance(category, discord.CategoryChannel):
        log.warning(f"[Whop Resolution] Category not found (ID: {category_id_int})")
        return

    async def ensure_channel(name: str) -> discord.TextChannel | None:
        if not name:
            return None
        # Look for existing channel with same name under category
        for ch in category.channels:
            if isinstance(ch, discord.TextChannel) and ch.name == name:
                return ch
        # Create it
        try:
            return await guild.create_text_channel(
                name=name,
                category=category,
                reason="RSCheckerbot: Whop resolution/dispute reporting channel",
            )
        except Exception as e:
            log.warning(f"[Whop Resolution] Failed to create channel '{name}': {e}")
            return None

    dispute_ch = await ensure_channel(str(WHOP_DISPUTE_CHANNEL_NAME).strip())
    resolution_ch = await ensure_channel(str(WHOP_RESOLUTION_CHANNEL_NAME).strip())

    WHOP_DISPUTE_CHANNEL_ID = dispute_ch.id if dispute_ch else None
    WHOP_RESOLUTION_CHANNEL_ID = resolution_ch.id if resolution_ch else None

    if dispute_ch:
        log.info(f"[Whop Resolution] Dispute channel ready: {dispute_ch.name} (ID: {dispute_ch.id})")
    if resolution_ch:
        log.info(f"[Whop Resolution] Resolution channel ready: {resolution_ch.name} (ID: {resolution_ch.id})")
    else:
        if not whop_api_client:
            log.info("[Whop Sync] Sync job disabled (no API client)")
        else:
            log.info("[Whop Sync] Sync job disabled (enable_sync=false)")
    
    log.info("="*60)
    log.info("")

@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id == GUILD_ID and not member.bot:
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
                    await log_member_status("", embed=detailed)
                else:
                    pending = _whop_placeholder_brief("pending")
                    pending_embed = _build_member_status_detailed_embed(
                        title="👋 Member Joined",
                        member=member,
                        access_roles=access,
                        color=0x57F287,
                        event_kind="active",
                        member_kv=base_member_kv,
                        discord_kv=base_discord_kv + [("whop_link", "Linking…")],
                        whop_brief=pending,
                    )
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
                        unlinked = _whop_placeholder_brief("unlinked")
                        note = f"Not linked yet (joined via {source_s})" if source_s and source_s != "—" else "Not linked yet (joined via invite)"
                        return _build_member_status_detailed_embed(
                            title="👋 Member Joined",
                            member=member,
                            access_roles=access,
                            color=0x57F287,
                            event_kind="active",
                            member_kv=base_member_kv,
                            discord_kv=base_discord_kv + [("whop_link", note)],
                            whop_brief=unlinked,
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
    if member.guild.id == GUILD_ID and not member.bot:
        rec = _touch_leave(member.id, member)
        
        # Log to member-status-logs channel
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if ch:
                access = _access_roles_plain(member)

                mid = ""
                whop_brief = _whop_summary_for_member(member.id)
                if not (isinstance(whop_brief, dict) and whop_brief):
                    mid = _membership_id_from_history(member.id)
                    whop_brief = await _fetch_whop_brief_by_membership_id(mid) if mid else {}
                if not (isinstance(whop_brief, dict) and whop_brief):
                    whop_brief = _whop_placeholder_brief("pending" if mid else "unlinked")
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
        
        # Check if this looks like a subscription/payment cancellation
        # If Member role was the ONLY role removed (or only with Welcome), likely payment-related
        only_member_removed = len(all_removed_in_update) == 1 or (len(all_removed_in_update) == 2 and ROLE_CANCEL_B in all_removed_in_update)
        if only_member_removed:
            # Determine whether this is a payment failure (past_due/unpaid) vs cancellation.
            whop_brief = _whop_summary_for_member(after.id)
            if not (isinstance(whop_brief, dict) and whop_brief):
                whop_mid = _membership_id_from_history(after.id)
                whop_brief = await _fetch_whop_brief_by_membership_id(whop_mid) if whop_mid else {}
            whop_status = str((whop_brief.get("status") or "")).strip().lower()
            is_payment_failed = whop_status in ("past_due", "unpaid") or bool(whop_brief.get("last_payment_failure"))
            event_kind = "payment_failed" if is_payment_failed else "deactivated"
            access = _access_roles_plain(after)
            issue_key = f"payment_failed:{whop_status}" if is_payment_failed else "payment_cancellation_detected"
            if await should_post_and_record_alert(
                STAFF_ALERTS_FILE,
                discord_id=after.id,
                issue_key=issue_key,
                cooldown_hours=6.0,
            ):
                dest = PAYMENT_FAILURE_CHANNEL_NAME if is_payment_failed else MEMBER_CANCELLATION_CHANNEL_NAME
                title = "💳 Payment Failed — Action Needed" if is_payment_failed else "💳 Payment Cancellation Detected"
                color = 0xED4245 if is_payment_failed else 0xFF0000

                # Detailed card -> member-status-logs
                hist = get_member_history(after.id) or {}
                acc = hist.get("access") if isinstance(hist.get("access"), dict) else {}
                detailed = _build_member_status_detailed_embed(
                    title=title,
                    member=after,
                    access_roles=access,
                    color=color,
                    event_kind=event_kind,
                    member_kv=[
                        ("account_created", after.created_at.strftime("%b %d, %Y")),
                        ("first_joined", _fmt_ts(hist.get("first_join_ts"), "D") if hist.get("first_join_ts") else "—"),
                        ("join_count", hist.get("join_count") or "—"),
                        ("ever_had_member_role", "yes" if acc.get("ever_had_member_role") is True else "no"),
                        ("first_access", _fmt_ts(acc.get("first_access_ts"), "D") if acc.get("first_access_ts") else "—"),
                        ("last_access", _fmt_ts(acc.get("last_access_ts"), "D") if acc.get("last_access_ts") else "—"),
                    ],
                    discord_kv=[
                        ("roles_removed", removed_names),
                        ("reason", "member_role_removed"),
                    ],
                    whop_brief=whop_brief,
                )
                await log_member_status("", embed=detailed)

                # Minimal card -> case channel
                minimal = _build_case_minimal_embed(
                    title=title,
                    member=after,
                    access_roles=access,
                    whop_brief=whop_brief,
                    color=color,
                    event_kind=event_kind,
                )
                await log_member_status("", embed=minimal, channel_name=dest)
        
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

                    minimal = _build_case_minimal_embed(
                        title=final_title,
                        member=after,
                        access_roles=access,
                        whop_brief=whop_brief_now,
                        color=0x57F287,
                        event_kind="active",
                    )
                    await log_member_status("", embed=minimal, channel_name=PAYMENT_FAILURE_CHANNEL_NAME)
                else:
                    pending = _whop_placeholder_brief("pending")
                    pending_title = "✅ Access Restored"

                    pending_detailed = _build_member_status_detailed_embed(
                        title=pending_title,
                        member=after,
                        access_roles=access,
                        color=0x57F287,
                        event_kind="active",
                        member_kv=base_member_kv,
                        discord_kv=base_discord_kv + [("whop_link", "Linking…"), ("event", "access.restored")],
                        whop_brief=pending,
                    )
                    msg_detailed = await log_member_status("", embed=pending_detailed)

                    pending_min = _build_case_minimal_embed(
                        title=pending_title,
                        member=after,
                        access_roles=access,
                        whop_brief=pending,
                        color=0x57F287,
                        event_kind="active",
                    )
                    msg_min = await log_member_status("", embed=pending_min, channel_name=PAYMENT_FAILURE_CHANNEL_NAME)

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
                        unlinked = _whop_placeholder_brief("unlinked")
                        return _build_member_status_detailed_embed(
                            title=pending_title,
                            member=after,
                            access_roles=access,
                            color=0x57F287,
                            event_kind="active",
                            member_kv=base_member_kv,
                            discord_kv=base_discord_kv + [("whop_link", "Not linked yet"), ("event", "access.restored")],
                            whop_brief=unlinked,
                        )

                    def _final_min(brief: dict) -> discord.Embed:
                        final_title2 = _title_for(brief)
                        return _build_case_minimal_embed(
                            title=final_title2,
                            member=after,
                            access_roles=access,
                            whop_brief=brief,
                            color=0x57F287,
                            event_kind="active",
                        )

                    def _fallback_min() -> discord.Embed:
                        unlinked = _whop_placeholder_brief("unlinked")
                        return _build_case_minimal_embed(
                            title=pending_title,
                            member=after,
                            access_roles=access,
                            whop_brief=unlinked,
                            color=0x57F287,
                            event_kind="active",
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
                    if msg_min:
                        asyncio.create_task(
                            _retry_whop_enrich_and_edit(
                                discord_id=after.id,
                                messages=[msg_min],
                                make_embed=_final_min,
                                make_fallback_embed=_fallback_min,
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

    # Check if this is a Whop message (from either channel).
    # IMPORTANT: workflow posts can be webhook messages (author.bot == False), so we gate by channel ID
    # and require either bot-author OR webhook_id to avoid parsing random user chatter.
    is_bot_or_webhook = bool(getattr(message.author, "bot", False)) or (getattr(message, "webhook_id", None) is not None)
    if is_bot_or_webhook:
        # Check workflow webhook channel
        if (WHOP_WEBHOOK_CHANNEL_ID and message.channel.id == WHOP_WEBHOOK_CHANNEL_ID):
            await handle_whop_webhook_message(message)
            return

        # Check native Whop logs channel
        if (WHOP_LOGS_CHANNEL_ID and message.channel.id == WHOP_LOGS_CHANNEL_ID):
            # Channel ID is the source of truth (do not gate on bot/app name).
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
    for key in ("id", "membership_id", "membershipId", "membership", "whop_key", "key"):
        val = membership.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, dict):
            inner_id = str(val.get("id") or val.get("membership_id") or "").strip()
            if inner_id:
                return inner_id
    return ""


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
    page = 1
    per_page = 100
    while page <= max_pages:
        batch = await whop_api_client.list_memberships(page=page, per_page=per_page)
        if not batch:
            break
        for rec in batch:
            if not isinstance(rec, dict):
                continue
            membership = _whop_report_normalize_membership(rec)
            did = _whop_report_extract_discord_id(membership)
            if did and int(did) == int(discord_id):
                return membership
        if len(batch) < per_page:
            break
        page += 1
    return {}

def _report_mode_label(mode: str) -> str:
    m = str(mode or "").strip().lower()
    if m == "scan_whop":
        return "Scan Whop API + CSV"
    if m == "scan_memberstatus":
        return "Scan member-status logs"
    return "Manual report (DM only)"


class _ReportDatesModal(discord.ui.Modal):
    def __init__(self, view: "_ReportOptionsView"):
        super().__init__(title="Report Date Range")
        self._view = view
        self.start_input = discord.ui.TextInput(
            label="Start date (YYYY-MM-DD)",
            required=False,
            default=str(view.start or ""),
            placeholder="2026-01-01",
        )
        self.end_input = discord.ui.TextInput(
            label="End date (YYYY-MM-DD)",
            required=False,
            default=str(view.end or ""),
            placeholder="2026-01-07",
        )
        self.add_item(self.start_input)
        self.add_item(self.end_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        self._view.start = str(self.start_input.value or "").strip()
        self._view.end = str(self.end_input.value or "").strip()
        await self._view.refresh_message()
        with suppress(Exception):
            await interaction.response.send_message("✅ Report dates updated.", ephemeral=True)


class _ReportTypeSelect(discord.ui.Select):
    def __init__(self, view: "_ReportOptionsView"):
        options = [
            discord.SelectOption(label="Manual report (DM)", value="manual"),
            discord.SelectOption(label="Scan Whop API + CSV", value="scan_whop"),
            discord.SelectOption(label="Scan member-status logs", value="scan_memberstatus"),
        ]
        super().__init__(placeholder="Report type", min_values=1, max_values=1, options=options)
        self._view = view

    async def callback(self, interaction: discord.Interaction) -> None:
        self._view.mode = str(self.values[0] or "manual")
        self._view.sample_csv = False if self._view.mode != "scan_whop" else self._view.sample_csv
        self._view.sync_buttons()
        await interaction.response.edit_message(embed=self._view.build_embed(), view=self._view)


class _ReportOptionsView(discord.ui.View):
    def __init__(self, ctx: commands.Context):
        try:
            timeout_s = int(REPORTING_CONFIG.get("interactive_timeout_sec") or 900)
        except Exception:
            timeout_s = 900
        if timeout_s <= 0:
            timeout_s = 900
        super().__init__(timeout=timeout_s)
        self.ctx = ctx
        self.author_id = int(getattr(ctx.author, "id", 0) or 0)
        self.mode = "manual"
        self.start = ""
        self.end = ""
        self.sample_csv = False
        self.message: discord.Message | None = None

        self.type_select = _ReportTypeSelect(self)
        self.add_item(self.type_select)

        self.sample_button = discord.ui.Button(
            label="Sample CSV: Off",
            style=discord.ButtonStyle.secondary,
        )
        self.sample_button.callback = self._toggle_sample  # type: ignore[assignment]
        self.add_item(self.sample_button)

        self.dates_button = discord.ui.Button(
            label="Set dates",
            style=discord.ButtonStyle.primary,
        )
        self.dates_button.callback = self._set_dates  # type: ignore[assignment]
        self.add_item(self.dates_button)

        self.run_button = discord.ui.Button(
            label="Run report",
            style=discord.ButtonStyle.success,
        )
        self.run_button.callback = self._run_report  # type: ignore[assignment]
        self.add_item(self.run_button)

        self.cancel_button = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.danger,
        )
        self.cancel_button.callback = self._cancel  # type: ignore[assignment]
        self.add_item(self.cancel_button)

        self.sync_buttons()

    def sync_buttons(self) -> None:
        if getattr(self, "sample_button", None):
            self.sample_button.label = f"Sample CSV: {'On' if self.sample_csv else 'Off'}"
            self.sample_button.disabled = self.mode != "scan_whop"

    def build_embed(self) -> discord.Embed:
        tz_name = str(REPORTING_CONFIG.get("timezone") or "UTC").strip() or "UTC"
        mode_label = _report_mode_label(self.mode)
        if not self.start and not self.end:
            range_label = "Last 7 days (auto)"
        elif self.start and not self.end:
            range_label = f"{self.start} → now"
        elif not self.start and self.end:
            range_label = f"{self.end} → now"
        else:
            range_label = f"{self.start} → {self.end}"

        if self.mode != "manual" and (not self.start or not self.end):
            range_label = f"{range_label} (required for scan)"

        desc = (
            "Pick a report type and date range.\n"
            f"Type: {mode_label}\n"
            f"Date range: {range_label}\n"
            f"Sample CSV: {'yes' if self.sample_csv else 'no'}\n"
            f"Timezone: {tz_name}"
        )
        return discord.Embed(
            title="RSCheckerbot Report Builder",
            description=desc,
            color=0x5865F2,
            timestamp=datetime.now(timezone.utc),
        )

    async def refresh_message(self) -> None:
        self.sync_buttons()
        if self.message:
            await self.message.edit(embed=self.build_embed(), view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        uid = int(getattr(interaction.user, "id", 0) or 0)
        if uid and self.author_id and uid != self.author_id:
            await interaction.response.send_message("❌ Only the report requester can use this.", ephemeral=True)
            return False
        return True

    async def _toggle_sample(self, interaction: discord.Interaction) -> None:
        if self.mode != "scan_whop":
            await interaction.response.send_message("Sample CSV is only for Whop API scans.", ephemeral=True)
            return
        self.sample_csv = not self.sample_csv
        self.sync_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _set_dates(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(_ReportDatesModal(self))

    async def _run_report(self, interaction: discord.Interaction) -> None:
        if self.mode != "manual" and (not self.start or not self.end):
            await interaction.response.send_message("❌ Start and end dates are required for scans.", ephemeral=True)
            return
        await interaction.response.defer()
        await self._disable()
        tokens: list[str] = []
        if self.mode == "manual":
            if self.start:
                tokens.append(self.start)
            if self.end:
                tokens.append(self.end)
        elif self.mode == "scan_whop":
            tokens = ["scan", "whop", self.start, self.end, "confirm"]
            if self.sample_csv:
                tokens.append("sample")
        elif self.mode == "scan_memberstatus":
            tokens = ["scan", "memberstatus", self.start, self.end, "confirm"]
        await _run_report_with_tokens(self.ctx, tokens)

    async def _cancel(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        await self._disable(cancelled=True)

    async def _disable(self, *, cancelled: bool = False) -> None:
        for child in self.children:
            child.disabled = True
        if self.message:
            if cancelled:
                embed = self.build_embed()
                embed.title = "RSCheckerbot Report Builder (cancelled)"
                await self.message.edit(embed=embed, view=self)
            else:
                await self.message.edit(view=self)


async def _start_report_interactive(ctx: commands.Context) -> None:
    view = _ReportOptionsView(ctx)
    msg = await ctx.send(embed=view.build_embed(), view=view)
    view.message = msg


async def _run_report_with_tokens(ctx: commands.Context, tokens: list[str]) -> None:
    try:
        if tokens and tokens[0].lower() == "debug":
            target = tokens[1] if len(tokens) >= 2 else ""
            start_s = tokens[2] if len(tokens) >= 3 else ""
            end_s = tokens[3] if len(tokens) >= 4 else ""
            await _report_debug_whop(ctx, target=target, start=start_s, end=end_s)
            return
        if tokens and tokens[0].lower() == "scan":
            if len(tokens) not in {5, 6}:
                await ctx.send(
                    "❌ Scan usage: `.checker report scan whop YYYY-MM-DD YYYY-MM-DD confirm` "
                    "or `.checker report scan memberstatus YYYY-MM-DD YYYY-MM-DD confirm`",
                    delete_after=25,
                )
                return

            source = tokens[1].lower()
            start_s = tokens[2]
            end_s = tokens[3]
            confirm = tokens[4].lower()
            sample_csv = False
            if len(tokens) == 6:
                sample_csv = tokens[5].lower() in {"sample", "samplecsv", "anonymize", "anon"}
            if confirm != "confirm":
                await ctx.send("❌ Confirmation required. Add `confirm` at the end.", delete_after=20)
                return

            if source in {"whop", "whoplogs", "whop-log", "whop-logs"}:
                await _report_scan_whop(ctx, start=start_s, end=end_s, sample_csv=sample_csv)
                return
            if source in {"memberstatus", "member-status", "status", "memberstatuslogs", "member-status-logs"}:
                await _report_scan_member_status(ctx, start=start_s, end=end_s)
                return

            await ctx.send("❌ Unknown scan source. Use `whop` or `memberstatus`.", delete_after=20)
            return

        start = tokens[0] if len(tokens) >= 1 else ""
        end = tokens[1] if len(tokens) >= 2 else ""
        start_dt = _parse_date_ymd(start) if start else None
        end_dt = _parse_date_ymd(end) if end else None

        now_local = _tz_now()
        if start_dt is None and end_dt is None:
            end_utc = now_local.astimezone(timezone.utc)
            start_utc = end_utc - timedelta(days=7)
        else:
            # If only one date is provided, treat it as start, end = now.
            if start_dt is None and end_dt is not None:
                start_dt, end_dt = end_dt, None
            if start_dt is None:
                start_dt = now_local
            if end_dt is None:
                end_dt = now_local
            # end is inclusive by date; bump to end-of-day local
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
            start_utc = start_dt.astimezone(timezone.utc)
            end_utc = end_dt.astimezone(timezone.utc)

        e = await _build_report_embed(start_utc, end_utc, title_prefix="RS Manual Report")

        # DM Neo (configured) and the invoker, but avoid duplicate DM when invoker is Neo.
        dm_uid = int(REPORTING_CONFIG.get("dm_user_id") or 0)
        targets: list[int] = []
        if dm_uid:
            targets.append(dm_uid)
        if ctx.author and getattr(ctx.author, "id", None):
            targets.append(int(ctx.author.id))
        targets = list(dict.fromkeys([int(x) for x in targets if int(x) > 0]))
        for uid in targets:
            await _dm_user(uid, embed=e)

        await ctx.send(
            "✅ Report sent (DM). If you need to rebuild data + get a downloadable CSV, use: "
            "`.checker report scan whop YYYY-MM-DD YYYY-MM-DD confirm`",
            delete_after=25,
        )
    except Exception as ex:
        if isinstance(ex, PermissionError):
            store_path = BASE_DIR / "reporting_store.json"
            tmp_path = store_path.with_suffix(store_path.suffix + ".tmp")
            await ctx.send(
                "❌ Report error: permission denied writing the reporting store.\n"
                f"- store: `{store_path}`\n"
                f"- temp: `{tmp_path}`\n"
                "Fix: ensure the bot service user can write to the RSCheckerbot folder "
                "(common cause: stale root-owned `.tmp` file).",
                delete_after=30,
            )
        else:
            await ctx.send(f"❌ Report error: {ex}", delete_after=15)
    finally:
        with suppress(Exception):
            await ctx.message.delete()


@bot.command(name="report", aliases=["reports"])
@commands.has_permissions(administrator=True)
async def checker_report(ctx, arg1: str = "", arg2: str = "", arg3: str = "", arg4: str = "", arg5: str = "", arg6: str = ""):
    """Generate a report for a date range and DM it (Neo + invoker).

    Usage:
      .checker report
      .checker report 2026-01-01
      .checker report 2026-01-01 2026-01-07
      .checker report scan whop 2026-01-01 2026-01-31 confirm
      .checker report scan memberstatus 2026-01-01 2026-01-31 confirm
      .checker report debug <discord_id|membership_id> [YYYY-MM-DD YYYY-MM-DD]

    Tip: run without arguments to open the interactive picker.
    """
    tokens = [str(x).strip() for x in [arg1, arg2, arg3, arg4, arg5, arg6] if str(x or "").strip()]
    if not tokens:
        await _start_report_interactive(ctx)
        with suppress(Exception):
            await ctx.message.delete()
        return
    await _run_report_with_tokens(ctx, tokens)


async def _report_scan_whop(ctx, start: str, end: str, *, sample_csv: bool = False) -> None:
    """One-time scan of Whop API memberships to rebuild reporting_store.json and output report + CSV."""

    if not REPORTING_CONFIG.get("enabled"):
        await ctx.send("❌ Reporting is disabled in config.", delete_after=15)
        return

    if not (_report_load_store and _report_record_member_status_post and _report_prune_store and _report_save_store):
        await ctx.send("❌ Reporting store module is not available.", delete_after=20)
        return

    if not whop_api_client or not getattr(whop_api_client, "list_memberships", None):
        await ctx.send("❌ Whop API is not configured for reporting.", delete_after=20)
        return

    # Mountain Time (boss): use America/Denver for day boundaries during dedupe.
    scan_tz = None
    if ZoneInfo is not None:
        with suppress(Exception):
            scan_tz = ZoneInfo("America/Denver")
    if scan_tz is None:
        scan_tz = timezone.utc

    # Parse YYYY-MM-DD in MT
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
    scan_label = "Whop API"
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

    started_at = time.time()
    last_edit = 0.0
    scanned = 0
    included = 0
    dupes = 0
    api_calls = 0

    # Dedup: (bucket, identity, day_key)
    seen: set[tuple[str, str, str]] = set()

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
                f"🔍 **Scanning Whop API for report...**\n"
                f"```\n"
                f"Stage: {stage}\n"
                f"Memberships scanned: {scanned}\n"
                f"Included (deduped): {included}\n"
                f"Dupes skipped: {dupes}\n"
                f"Whop API calls: {api_calls}\n"
                f"Rate: {rate:.1f} memberships/s\n"
                f"```"
            )[:1900]
        )

    # Fresh store (overwrite)
    retention = int(REPORTING_CONFIG.get("retention_weeks", 26))
    store = _report_load_store(BASE_DIR, retention_weeks=retention)
    if isinstance(store.get("meta"), dict):
        store["meta"]["version"] = int(store["meta"].get("version") or 1)
        store["meta"]["scan_source"] = "report.scan.whop.api"
        store["meta"]["scan_range_mt"] = f"{start_local.date().isoformat()}→{end_local.date().isoformat()}"
    store["weeks"] = {}
    store["members"] = {}
    store["unlinked"] = {}

    # Detailed CSV rows (only included/deduped events)
    csv_rows: list[dict] = []
    totals = {"new_members": 0, "new_trials": 0, "payment_failed": 0, "cancellation_scheduled": 0}

    try:
        per_page = int(WHOP_API_CONFIG.get("report_page_size") or WHOP_API_CONFIG.get("page_size") or 100)
    except Exception:
        per_page = 100
    if per_page <= 0:
        per_page = 100
    page = 1
    seen_memberships: set[str] = set()

    try:
        await _progress("page 1", force=True)
        while True:
            api_calls += 1
            batch = await whop_api_client.list_memberships(page=page, per_page=per_page)
            if not batch:
                break
            new_on_page = 0
            for membership in batch:
                scanned += 1
                if (scanned % 50) == 0:
                    with suppress(Exception):
                        await _progress(f"page {page}")

                if not isinstance(membership, dict):
                    continue
                membership = _whop_report_normalize_membership(membership)

                membership_id = _whop_report_membership_id(membership)
                if membership_id:
                    if membership_id in seen_memberships:
                        continue
                    seen_memberships.add(membership_id)
                    new_on_page += 1

                buckets, brief, info = _whop_report_compute_events(
                    membership,
                    start_utc=start_utc,
                    end_utc=end_utc,
                    api_client=whop_api_client,
                )

                if not buckets:
                    continue

                status_l = str(info.get("status_l") or "").strip().lower()
                discord_id = _whop_report_extract_discord_id(membership)
                email = _whop_report_extract_email(membership)
                if not email and not discord_id:
                    email = membership_id

                ident = (membership_id or "").strip() or (str(discord_id or "").strip()) or (email or "")
                if not ident:
                    continue

                event_type = f"api.membership.{status_l}" if status_l else "api.membership"

                for bucket, ev_dt in buckets:
                    if not ev_dt:
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
                        event_kind = "trialing" if status_l == "trialing" else "membership_activated_pending"
                    elif bucket == "new_member":
                        event_kind = "member_role_added"
                    elif bucket == "cancellation_scheduled":
                        event_kind = "cancellation_scheduled"

                    store = _report_record_member_status_post(
                        store,
                        ts=int(ev_dt.astimezone(timezone.utc).timestamp()),
                        event_kind=event_kind,
                        discord_id=discord_id,
                        email=email or "",
                        whop_brief=brief if isinstance(brief, dict) and brief else None,
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

                    csv_rows.append(
                        {
                            "day_mt": day_key,
                            "event_bucket": bucket,
                            "membership_id": membership_id,
                            "discord_id": str(discord_id or ""),
                            "email": email or "",
                            "product": str((brief or {}).get("product") or ""),
                            "status": str((brief or {}).get("status") or ""),
                            "total_spent": str((brief or {}).get("total_spent") or ""),
                            "cancel_at_period_end": str((brief or {}).get("cancel_at_period_end") or ""),
                            "renewal_end_iso": str((brief or {}).get("renewal_end_iso") or ""),
                            "dashboard_url": str((brief or {}).get("dashboard_url") or ""),
                            "source_channel_id": "",
                            "source_message_id": "",
                            "source_jump_url": "",
                            "event_type": event_type,
                        }
                    )

            if new_on_page == 0 and page > 1:
                break
            if len(batch) < int(per_page):
                break
            page += 1
    except Exception as e:
        with suppress(Exception):
            await status_msg.edit(content=f"❌ Scan failed: `{e}`")
        return

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
        warn_note = "\n⚠️ No memberships returned by the API. Check `whop_api.company_id` and API scopes."
    elif included == 0:
        warn_note = "\n⚠️ Memberships returned, but no events matched the date range. Verify date fields."

    e = discord.Embed(
        title=f"RS Whop Scan Report ({start_local.date().isoformat()} → {end_local.date().isoformat()})",
        description=(
            "Source: Whop API (deduped per membership per day/event) • Timezone: `America/Denver`"
            + f"\nScanned: {scanned} memberships • Included: {included} • API calls: {api_calls}"
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

    for uid in targets:
        with suppress(Exception):
            user = bot.get_user(uid) or await bot.fetch_user(uid)
            if not user:
                continue
            # New file object per send
            f = discord.File(fp=io.BytesIO(csv_bytes), filename=fname)
            await user.send(embed=e, file=f)

    with suppress(Exception):
        if status_msg:
            await status_msg.edit(content="✅ Scan complete. Report sent via DM (with CSV).")
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

@bot.command(name="test")
@commands.has_permissions(administrator=True)
async def test_sequence(ctx, member: discord.Member):
    await ctx.reply(f"Starting test sequence for {m_user(member)}...")
    for day_key in DAY_KEYS:
        try:
            join_url = UTM_LINKS[day_key]

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
            await member.send(embeds=embeds, view=view)
            log.info(f"[TEST] Sent {day_key} to {member} ({member.id})")
            if day_key == "day_1":
                e = _make_dyno_embed(
                    member=member,
                    description=f"{member.mention} {day_key} sent (test)",
                    footer=f"ID: {member.id}",
                    color=0x57F287,
                )
                await log_first(embed=e)
            else:
                e = _make_dyno_embed(
                    member=member,
                    description=f"{member.mention} {day_key} sent (test)",
                    footer=f"ID: {member.id}",
                    color=0x57F287,
                )
                await log_other(embed=e)
        except Exception as e:
            await log_other(f"🧪❌ TEST failed `{day_key}` for {_fmt_user(member)}: `{e}`")
        await asyncio.sleep(TEST_INTERVAL_SECONDS)
    await ctx.send(f"✅ Test sequence complete for {m_user(member)}.")

@bot.command(name="relocate")
@commands.has_permissions(administrator=True)
async def relocate_sequence(ctx, member: discord.Member, day: str):
    d = day.strip().lower()
    if d.isdigit():
        idx = int(d) - 1
        day_key = DAY_KEYS[idx] if 0 <= idx < len(DAY_KEYS) else None
    elif d in {"7a", "7b"}:
        day_key = f"day_{d}"
    elif d.startswith("day_") and d in DAY_KEYS:
        day_key = d
    else:
        day_key = None

    if not day_key:
        await ctx.reply("Invalid day. Use 1–6, 7a, 7b, or day_1..day_7b.")
        return

    queue_state[str(member.id)] = {
        "current_day": day_key,
        "next_send": (_now() + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
    }
    save_json(QUEUE_FILE, queue_state)
    await ctx.reply(f"Relocated {m_user(member)} to **{day_key}**, will send in ~5s.")
    e = _make_dyno_embed(
        member=member,
        description=f"{member.mention} relocated to {day_key}",
        footer=f"ID: {member.id}",
        color=0x5865F2,
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
    bot.run(TOKEN)

