from __future__ import annotations

import io
import re
import uuid
import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

import discord
from discord.ext import commands

from rschecker_utils import load_json as _load_json
from rschecker_utils import save_json as _save_json
from rschecker_utils import fmt_date_any as _fmt_date_any
from rschecker_utils import parse_dt_any as _parse_dt_any
from rschecker_utils import access_roles_plain as _access_roles_plain
from rschecker_utils import roles_plain as _roles_plain
from rschecker_utils import extract_discord_id_from_whop_member_record as _extract_discord_id_from_whop_member_record
from staff_embeds import build_member_status_detailed_embed as _build_member_status_detailed_embed
from ticket_channels import ensure_ticket_like_channel as _ensure_ticket_like_channel
from ticket_channels import slug_channel_name as _slug_channel_name
from whop_brief import fetch_whop_brief as _fetch_whop_brief
from whop_brief import enrich_whop_brief_from_membership_logs as _enrich_whop_brief_from_membership_logs


BASE_DIR = Path(__file__).resolve().parent
INDEX_PATH = BASE_DIR / "data" / "tickets_index.json"
MIGRATIONS_STATE_PATH = BASE_DIR / "data" / "support_tickets_migrations.json"
MEMBER_HISTORY_PATH = BASE_DIR / "member_history.json"
WHOP_IDENTITY_CACHE_PATH = BASE_DIR / "whop_identity_cache.json"
MEMBER_LOOKUP_PANEL_STATE_PATH = BASE_DIR / "data" / "support_member_lookup_panel.json"

_INDEX_LOCK: asyncio.Lock = asyncio.Lock()
_MH_DB_CACHE: dict | None = None
_MH_DB_CACHE_AT: float = 0.0
_MH_DB_CACHE_MTIME: float = 0.0


@dataclass(frozen=True)
class SupportTicketConfig:
    guild_id: int
    staff_role_ids: list[int]
    legacy_staff_role_ids: list[int]
    admin_role_ids: list[int]
    include_ticket_owner_in_channel: bool
    cancellation_category_id: int
    billing_category_id: int
    free_pass_category_id: int
    member_welcome_category_id: int
    member_welcome_category_name: str
    no_whop_link_category_id: int
    no_whop_link_category_name: str
    transcript_category_id: int
    cancellation_transcript_channel_id: int
    billing_transcript_channel_id: int
    free_pass_transcript_channel_id: int
    what_you_missed_channel_id: int
    preview_limit: int
    auto_delete_enabled: bool
    inactivity_seconds: int
    check_interval_seconds: int
    delete_on_whop_linked: bool
    dedupe_enabled: bool
    cooldown_free_pass_seconds: int
    cooldown_free_pass_welcome_seconds: int
    cooldown_billing_seconds: int
    cooldown_cancellation_seconds: int
    cooldown_member_welcome_seconds: int
    startup_enabled: bool
    startup_delay_seconds: int
    startup_recent_history_limit: int
    startup_templates: dict[str, str]
    header_templates: dict[str, str]
    audit_enabled: bool
    audit_channel_id: int
    audit_channel_name: str
    audit_include_transcript_category: bool
    billing_role_id: int
    cancellation_role_id: int
    free_pass_no_whop_role_id: int
    no_whop_link_role_id: int
    no_whop_remove_role_ids_on_add: list[int]
    no_whop_link_enabled: bool
    no_whop_link_scan_interval_seconds: int
    no_whop_link_members_role_id: int
    no_whop_link_exclude_role_ids: list[int]
    no_whop_link_log_to_member_status_logs: bool
    no_whop_link_use_whop_api: bool
    no_whop_link_max_pages: int
    no_whop_link_per_page: int
    no_whop_link_cooldown_seconds: int
    whop_unlinked_note: str
    member_status_logs_channel_id: int
    resolution_followup_enabled: bool
    resolution_followup_auto_close_after_seconds: int
    resolution_followup_templates: dict[str, str]
    member_lookup_enabled: bool
    member_lookup_channel_id: int
    member_lookup_max_native_logs: int
    member_lookup_panel_title: str
    member_lookup_panel_description: str
    whop_logs_channel_id: int
    whop_membership_logs_channel_id: int


_BOT: commands.Bot | None = None
_CFG: SupportTicketConfig | None = None
_LOG_FUNC = None  # async callable(str) -> None
_IS_WHOP_LINKED = None  # callable(discord_id:int) -> bool
_TZ_NAME = "UTC"
_CONTROLS_VIEW: "SupportTicketControlsView | None" = None
_MEMBER_LOOKUP_VIEW: "MemberLookupPanelView | None" = None
_WHOP_API_CLIENT = None  # optional WhopAPIClient injected by main.py
_RUN_MEMBERSHIP_REPORT_CALLBACK = None  # async (user, start_str, end_str) -> (success, message)
_LOOKUP_LIVE_CACHE: dict[int, dict] = {}
_LOOKUP_LIVE_CACHE_AT: dict[int, float] = {}


def _as_int(v: object) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0


def _as_bool(v: object) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _parse_iso(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(s or "").replace("Z", "+00:00"))
    except Exception:
        return None


def _day_bucket_local(dt: datetime) -> str:
    # Keep it simple: use configured tz if available, else UTC.
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        tz = ZoneInfo(str(_TZ_NAME or "UTC").strip() or "UTC")
        return dt.astimezone(tz).date().isoformat()
    except Exception:
        return dt.astimezone(timezone.utc).date().isoformat()


def _ensure_cfg_loaded() -> bool:
    return (_BOT is not None) and (_CFG is not None)


async def _log(msg: str) -> None:
    global _LOG_FUNC
    if not msg:
        return
    fn = _LOG_FUNC
    if fn:
        with suppress(Exception):
            await fn(str(msg)[:1800])


def initialize(
    *,
    bot: commands.Bot,
    config: dict,
    log_func=None,
    is_whop_linked=None,
    timezone_name: str = "UTC",
    whop_api_client=None,
    run_membership_report_callback=None,
) -> None:
    """Initialize the support ticket subsystem.

    This is called from RSCheckerbot/main.py after config load.
    run_membership_report_callback: async (user, start_str, end_str) -> (success, message)
    """
    global _BOT, _CFG, _LOG_FUNC, _IS_WHOP_LINKED, _TZ_NAME, _WHOP_API_CLIENT, _RUN_MEMBERSHIP_REPORT_CALLBACK
    _BOT = bot
    _RUN_MEMBERSHIP_REPORT_CALLBACK = run_membership_report_callback
    _LOG_FUNC = log_func
    _IS_WHOP_LINKED = is_whop_linked
    _TZ_NAME = str(timezone_name or "UTC").strip() or "UTC"
    _WHOP_API_CLIENT = whop_api_client

    root = config if isinstance(config, dict) else {}
    st = root.get("support_tickets") if isinstance(root.get("support_tickets"), dict) else {}

    perms = st.get("permissions") if isinstance(st.get("permissions"), dict) else {}
    cats = st.get("ticket_categories") if isinstance(st.get("ticket_categories"), dict) else {}
    tx = st.get("transcripts") if isinstance(st.get("transcripts"), dict) else {}
    fp = st.get("free_pass") if isinstance(st.get("free_pass"), dict) else {}
    fp_ad = fp.get("auto_delete") if isinstance(fp.get("auto_delete"), dict) else {}
    dd = st.get("dedupe") if isinstance(st.get("dedupe"), dict) else {}
    sm = st.get("startup_messages") if isinstance(st.get("startup_messages"), dict) else {}
    sm_templates = sm.get("templates") if isinstance(sm.get("templates"), dict) else {}
    hm = st.get("header_messages") if isinstance(st.get("header_messages"), dict) else {}
    hm_templates = hm.get("templates") if isinstance(hm.get("templates"), dict) else {}
    rf = st.get("resolution_followup") if isinstance(st.get("resolution_followup"), dict) else {}
    rf_templates = rf.get("templates") if isinstance(rf.get("templates"), dict) else {}
    al = st.get("audit_logs") if isinstance(st.get("audit_logs"), dict) else {}
    tr = st.get("ticket_roles") if isinstance(st.get("ticket_roles"), dict) else {}
    nw = st.get("no_whop_link") if isinstance(st.get("no_whop_link"), dict) else {}
    ml = st.get("member_lookup") if isinstance(st.get("member_lookup"), dict) else {}
    wh_api = root.get("whop_api") if isinstance(root.get("whop_api"), dict) else {}
    dm = root.get("dm_sequence") if isinstance(root.get("dm_sequence"), dict) else {}
    inv = root.get("invite_tracking") if isinstance(root.get("invite_tracking"), dict) else {}

    def _int_list(obj: object) -> list[int]:
        out: list[int] = []
        for x in (obj or []):
            v = _as_int(x)
            if v > 0:
                out.append(v)
        return sorted(list(dict.fromkeys(out)))

    _CFG = SupportTicketConfig(
        guild_id=_as_int(st.get("guild_id")),
        staff_role_ids=_int_list(perms.get("staff_role_ids")),
        legacy_staff_role_ids=_int_list(perms.get("legacy_staff_role_ids")),
        admin_role_ids=_int_list(perms.get("admin_role_ids")),
        include_ticket_owner_in_channel=_as_bool(perms.get("include_ticket_owner_in_channel", True)),
        cancellation_category_id=_as_int(cats.get("cancellation_category_id")),
        billing_category_id=_as_int(cats.get("billing_category_id")),
        free_pass_category_id=_as_int(cats.get("free_pass_category_id")),
        member_welcome_category_id=_as_int(cats.get("member_welcome_category_id")),
        member_welcome_category_name=str(cats.get("member_welcome_category_name") or "member-welcome").strip() or "member-welcome",
        no_whop_link_category_id=_as_int(cats.get("no_whop_link_category_id")),
        no_whop_link_category_name=str(cats.get("no_whop_link_category_name") or "no-whop-link").strip() or "no-whop-link",
        transcript_category_id=_as_int(tx.get("transcript_category_id")),
        cancellation_transcript_channel_id=_as_int(tx.get("cancellation_transcript_channel_id")),
        billing_transcript_channel_id=_as_int(tx.get("billing_transcript_channel_id")),
        free_pass_transcript_channel_id=_as_int(tx.get("free_pass_transcript_channel_id")),
        what_you_missed_channel_id=_as_int(fp.get("what_you_missed_channel_id")),
        preview_limit=max(1, min(10, _as_int(fp.get("preview_limit")) or 3)),
        auto_delete_enabled=_as_bool(fp_ad.get("enabled")),
        inactivity_seconds=max(60, _as_int(fp_ad.get("inactivity_seconds")) or 86400),
        check_interval_seconds=max(30, _as_int(fp_ad.get("check_interval_seconds")) or 600),
        delete_on_whop_linked=_as_bool(fp_ad.get("delete_on_whop_linked")),
        dedupe_enabled=_as_bool(dd.get("enabled")),
        cooldown_free_pass_seconds=max(0, _as_int((dd.get("free_pass") or {}).get("cooldown_seconds")) or 86400),
        cooldown_free_pass_welcome_seconds=max(0, _as_int((dd.get("free_pass_welcome") or {}).get("cooldown_seconds")) or 86400),
        cooldown_billing_seconds=max(0, _as_int((dd.get("billing") or {}).get("cooldown_seconds")) or 21600),
        cooldown_cancellation_seconds=max(0, _as_int((dd.get("cancellation") or {}).get("cooldown_seconds")) or 86400),
        cooldown_member_welcome_seconds=max(0, _as_int((dd.get("member_welcome") or {}).get("cooldown_seconds")) or 86400),
        startup_enabled=_as_bool(sm.get("enabled")),
        startup_delay_seconds=max(5, _as_int(sm.get("delay_seconds")) or 300),
        startup_recent_history_limit=max(10, min(200, _as_int(sm.get("recent_history_limit")) or 50)),
        startup_templates={str(k).strip().lower(): str(v) for k, v in (sm_templates or {}).items() if str(k or "").strip()},
        header_templates={str(k).strip().lower(): str(v) for k, v in (hm_templates or {}).items() if str(k or "").strip()},
        audit_enabled=_as_bool(al.get("enabled")),
        audit_channel_id=_as_int(al.get("channel_id")),
        audit_channel_name=str(al.get("channel_name") or "tickets-logs").strip() or "tickets-logs",
        audit_include_transcript_category=_as_bool(al.get("include_transcript_category")),
        billing_role_id=_as_int(tr.get("billing_role_id")),
        cancellation_role_id=_as_int(tr.get("cancellation_role_id")),
        free_pass_no_whop_role_id=_as_int(tr.get("free_pass_no_whop_role_id")),
        no_whop_link_role_id=_as_int(tr.get("no_whop_link_role_id")),
        no_whop_remove_role_ids_on_add=_int_list(tr.get("remove_role_ids_on_no_whop") or tr.get("no_whop_remove_role_ids_on_add") or []),
        no_whop_link_enabled=_as_bool(nw.get("enabled")),
        no_whop_link_scan_interval_seconds=max(60, _as_int(nw.get("scan_interval_seconds")) or 21600),
        no_whop_link_members_role_id=_as_int(nw.get("members_role_id")),
        no_whop_link_exclude_role_ids=_int_list(nw.get("exclude_role_ids")),
        no_whop_link_log_to_member_status_logs=_as_bool(nw.get("log_to_member_status_logs")),
        no_whop_link_use_whop_api=_as_bool(nw.get("use_whop_api")),
        no_whop_link_max_pages=max(1, min(200, _as_int(nw.get("max_pages")) or 50)),
        no_whop_link_per_page=max(10, min(200, _as_int(nw.get("per_page")) or 100)),
        no_whop_link_cooldown_seconds=max(0, _as_int(nw.get("cooldown_seconds")) or 86400),
        whop_unlinked_note=str(wh_api.get("unlinked_note") or "").strip(),
        member_status_logs_channel_id=_as_int(dm.get("member_status_logs_channel_id")),
        resolution_followup_enabled=_as_bool(rf.get("enabled")),
        resolution_followup_auto_close_after_seconds=max(0, _as_int(rf.get("auto_close_after_seconds")) or 1800),
        resolution_followup_templates={str(k).strip().lower(): str(v) for k, v in (rf_templates or {}).items() if str(k or "").strip()},
        member_lookup_enabled=_as_bool(ml.get("enabled", False)),
        member_lookup_channel_id=_as_int(ml.get("channel_id")),
        member_lookup_max_native_logs=max(0, min(10, _as_int(ml.get("max_native_logs")) or 5)),
        member_lookup_panel_title=str(ml.get("panel_title") or "Ticket Tool").strip() or "Ticket Tool",
        member_lookup_panel_description=str(
            ml.get("panel_description") or "Use the buttons below to look up a member’s Discord + Whop baseline history."
        ).strip()
        or "Use the buttons below to look up a member’s Discord + Whop baseline history.",
        whop_logs_channel_id=_as_int(inv.get("whop_logs_channel_id")),
        whop_membership_logs_channel_id=_as_int(inv.get("whop_webhook_channel_id")),
    )

    # Register persistent view so buttons survive restarts.
    global _CONTROLS_VIEW
    if _BOT and _CONTROLS_VIEW is None:
        with suppress(Exception):
            _CONTROLS_VIEW = SupportTicketControlsView()
            _BOT.add_view(_CONTROLS_VIEW)
    global _MEMBER_LOOKUP_VIEW
    if _BOT and _MEMBER_LOOKUP_VIEW is None:
        with suppress(Exception):
            _MEMBER_LOOKUP_VIEW = MemberLookupPanelView()
            _BOT.add_view(_MEMBER_LOOKUP_VIEW)


def _member_lookup_state_load() -> dict:
    raw = _load_json(MEMBER_LOOKUP_PANEL_STATE_PATH)
    return raw if isinstance(raw, dict) else {}


def _member_lookup_state_save(state: dict) -> None:
    if not isinstance(state, dict):
        state = {}
    _save_json(MEMBER_LOOKUP_PANEL_STATE_PATH, state)


def _short_iso(ts: str) -> str:
    dt = _parse_dt_any(ts)
    if not dt:
        return "—"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _member_history_get(discord_id: int) -> dict:
    did = int(discord_id or 0)
    if did <= 0:
        return {}
    db = _member_history_db_cached()
    rec = db.get(str(did)) if isinstance(db, dict) else None
    return rec if isinstance(rec, dict) else {}


def _member_history_db_cached(*, ttl_seconds: int = 15) -> dict:
    """Load member_history.json once per short window (it can be large)."""
    global _MH_DB_CACHE, _MH_DB_CACHE_AT, _MH_DB_CACHE_MTIME
    try:
        ttl = max(1, min(int(ttl_seconds or 15), 120))
    except Exception:
        ttl = 15
    now = datetime.now(timezone.utc).timestamp()
    try:
        mtime = float(MEMBER_HISTORY_PATH.stat().st_mtime) if MEMBER_HISTORY_PATH.exists() else 0.0
    except Exception:
        mtime = 0.0
    if _MH_DB_CACHE is not None and (now - float(_MH_DB_CACHE_AT)) <= float(ttl) and float(mtime) == float(_MH_DB_CACHE_MTIME):
        return _MH_DB_CACHE if isinstance(_MH_DB_CACHE, dict) else {}
    raw = _load_json(MEMBER_HISTORY_PATH)
    _MH_DB_CACHE = raw if isinstance(raw, dict) else {}
    _MH_DB_CACHE_AT = float(now)
    _MH_DB_CACHE_MTIME = float(mtime)
    return _MH_DB_CACHE if isinstance(_MH_DB_CACHE, dict) else {}


def _snowflake_created_at_utc(snowflake_id: int) -> datetime | None:
    """Best-effort Discord account creation time from snowflake."""
    try:
        sid = int(snowflake_id or 0)
        if sid <= 0:
            return None
        ms = (sid >> 22) + 1420070400000
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def _whop_member_type(product: str, *, has_membership: bool) -> str:
    """Human-readable member type label for lookup."""
    if not has_membership:
        return "Direct Invite / No Whop"
    p = str(product or "").strip()
    if not p:
        return "Whop (unknown product)"
    if "(lite" in p.lower() or "lite" in p.lower():
        return "Reselling Secrets (Lite)"
    return "Reselling Secrets"


def _merged_membership_logs_fields(*, db: dict, whop_user_id: str, membership_id: str) -> dict[str, str]:
    """Merge fields across whop_users[*].whop.membership_logs_latest (newest-first) for a membership_id."""
    if not isinstance(db, dict):
        return {}
    user_id = str(whop_user_id or "").strip()
    if not user_id:
        return {}
    wu = db.get("whop_users") if isinstance(db.get("whop_users"), dict) else {}
    rec = wu.get(user_id) if isinstance(wu, dict) else None
    if not isinstance(rec, dict):
        return {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    latest = wh.get("membership_logs_latest") if isinstance(wh.get("membership_logs_latest"), dict) else {}
    if not isinstance(latest, dict) or not latest:
        return {}
    mid = str(membership_id or "").strip()
    rows: list[dict] = []
    for v in latest.values():
        if not isinstance(v, dict):
            continue
        if mid:
            v_mid = str(v.get("membership_id") or "").strip()
            if v_mid and v_mid != mid:
                continue
        rows.append(v)

    def _k(v: dict) -> str:
        return str(v.get("recorded_at") or v.get("created_at") or "")

    rows.sort(key=_k, reverse=True)
    out: dict[str, str] = {}
    for row in rows:
        f = row.get("fields") if isinstance(row.get("fields"), dict) else {}
        if not isinstance(f, dict):
            continue
        for kk, vv in f.items():
            k2 = str(kk or "").strip().lower()
            if not k2 or k2 in out:
                continue
            s = str(vv or "").strip()
            if not s:
                continue
            out[k2[:64]] = s[:1500]
    return out


def _membership_logs_rows(*, db: dict, whop_user_id: str) -> list[dict]:
    """Return membership log rows sorted newest->oldest for a whop_user."""
    user_id = str(whop_user_id or "").strip()
    if not user_id or not isinstance(db, dict):
        return []
    wu = db.get("whop_users") if isinstance(db.get("whop_users"), dict) else {}
    rec = wu.get(user_id) if isinstance(wu, dict) else None
    if not isinstance(rec, dict):
        return []
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    latest = wh.get("membership_logs_latest") if isinstance(wh.get("membership_logs_latest"), dict) else {}
    if not isinstance(latest, dict) or not latest:
        return []
    rows: list[dict] = [v for v in latest.values() if isinstance(v, dict)]

    def _ts(row: dict) -> float:
        dt = _parse_dt_any(row.get("recorded_at") or row.get("created_at") or "")
        return float(dt.timestamp()) if dt else 0.0

    rows.sort(key=_ts, reverse=True)
    return rows


def _fmt_ts_any(v: object) -> str:
    dt = _parse_dt_any(v)
    if not dt:
        return "—"
    return dt.astimezone(timezone.utc).strftime("%b %d, %Y")


def _fmt_unix_ts(v: object) -> str:
    try:
        ts = int(v) if v is not None else 0
    except Exception:
        ts = 0
    if ts <= 0:
        return "—"
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d, %Y")
    except Exception:
        return "—"


def _embed_desc_lines(title: str, lines: list[str], *, max_chars: int = 3500) -> discord.Embed:
    e = discord.Embed(title=title, color=0x5865F2)
    out: list[str] = []
    n = 0
    for ln in (lines or []):
        s = str(ln or "").rstrip()
        if not s:
            continue
        if n + len(s) + 1 > int(max_chars):
            break
        out.append(s)
        n += len(s) + 1
    e.description = ("\n".join(out) if out else "—")
    return e


def _open_tickets_lines_for_user(*, user_id: int) -> list[str]:
    """Human-readable open tickets list for member lookup."""
    uid = int(user_id or 0)
    if uid <= 0:
        return []
    lines: list[str] = []
    try:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            if _as_int(rec.get("user_id")) != uid:
                continue
            ttype = str(rec.get("ticket_type") or "").strip().lower() or "unknown"
            ch_id = _as_int(rec.get("channel_id"))
            created = str(rec.get("created_at_iso") or "").strip()
            last_act = str(rec.get("last_activity_at_iso") or "").strip()
            age = _short_iso(created) if created else "—"
            last_s = _short_iso(last_act) if last_act else "—"
            lines.append(f"- **{ttype}**: <#{int(ch_id)}> (created {age}, last activity {last_s})"[:1200])
    except Exception:
        return []
    return lines[:10]


def _whop_user_id_for_lookup(*, db: dict, brief: dict, membership_id: str) -> str:
    if not isinstance(db, dict):
        return ""
    b = brief if isinstance(brief, dict) else {}
    u = str(b.get("whop_user_id") or "").strip()
    if u.startswith("user_"):
        return u
    mid = str(membership_id or "").strip()
    idx = db.get("whop_membership_index") if isinstance(db.get("whop_membership_index"), dict) else {}
    if mid and isinstance(idx, dict):
        u2 = str(idx.get(mid) or "").strip()
        if u2.startswith("user_"):
            return u2
    un = str(b.get("username") or "").strip().lower()
    uidx = db.get("whop_user_index") if isinstance(db.get("whop_user_index"), dict) else {}
    if un and isinstance(uidx, dict):
        u3 = str(uidx.get(un) or "").strip()
        if u3.startswith("user_"):
            return u3
    return ""


def _derive_last_seen_utc(rec: dict) -> str:
    """Pick best last-seen value from whop timeline + baselines."""
    if not isinstance(rec, dict):
        return "—"
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    # Prefer explicit timeline last_seen_ts
    ts = wh.get("last_seen_ts")
    if ts:
        s = _fmt_unix_ts(ts)
        if s != "—":
            return s
    # Fallback: last_event_ts
    ts2 = wh.get("last_event_ts")
    if ts2:
        s = _fmt_ts_any(ts2)
        if s != "—":
            return s
    # Fallback: newest member_status/native log row created_at
    best: datetime | None = None
    for rows in (_member_status_latest_rows(rec), _native_logs_latest_rows(rec)):
        for row in (rows or [])[:3]:
            dt = _parse_dt_any(row.get("created_at") or row.get("recorded_at") or "")
            if dt and (best is None or dt > best):
                best = dt
    return best.astimezone(timezone.utc).strftime("%b %d, %Y") if best else "—"


def _build_member_lookup_result_embeds(*, guild: discord.Guild, did: int) -> list[discord.Embed]:
    """Detailed, support-friendly multi-embed lookup output."""
    did_i = int(did or 0)
    member = guild.get_member(did_i) if did_i > 0 else None
    db = _member_history_db_cached()
    rec = db.get(str(did_i)) if isinstance(db, dict) else {}
    rec = rec if isinstance(rec, dict) else {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}

    # Basic identity
    display = str(getattr(member, "display_name", "") or getattr(member, "name", "") or "—") if isinstance(member, discord.Member) else "—"
    mention = member.mention if isinstance(member, discord.Member) else f"`{did_i}`"

    def _matches_did(s: object) -> bool:
        try:
            m = re.search(r"\b(\d{17,19})\b", str(s or ""))
            return bool(m and m.group(1).isdigit() and int(m.group(1)) == int(did_i))
        except Exception:
            return False

    # Derive linkage signal (Connected Discord)
    connected_discord = ""
    with suppress(Exception):
        ms_map = wh.get("member_status_logs_latest") if isinstance(wh.get("member_status_logs_latest"), dict) else {}
        if isinstance(ms_map, dict):
            for row in ms_map.values():
                if not isinstance(row, dict):
                    continue
                cd0 = str(row.get("connected_discord") or "").strip()
                if cd0 and cd0 != "—":
                    connected_discord = cd0
                    break

    # Overview embed
    ov_lines: list[str] = [
        f"**Member**: {mention}",
        f"**Display Name**: {display}",
        f"**Discord ID**: `{did_i}`",
    ]
    if isinstance(member, discord.Member):
        created_dt = _snowflake_created_at_utc(did_i)
        ov_lines.append(f"**Discord Account Created**: {created_dt.strftime('%b %d, %Y') if created_dt else '—'}")
        with suppress(Exception):
            if getattr(member, "joined_at", None):
                ov_lines.append(f"**First Joined**: {member.joined_at.astimezone(timezone.utc).strftime('%b %d, %Y')}")  # type: ignore[union-attr]

    open_lines = _open_tickets_lines_for_user(user_id=did_i)
    if open_lines:
        ov_lines.append("")
        ov_lines.append("**Open Tickets**:")
        ov_lines.extend(open_lines)

    # Quick Whop linkage status (used for nowhop cleanup)
    ov_lines.append("")
    ov_lines.append("**Whop Link Status**:")
    if connected_discord:
        ov_lines.append(f"- Connected Discord: `{connected_discord}`")
        ov_lines.append(f"- Linked: `{'yes' if _matches_did(connected_discord) else 'no'}`")
    else:
        ov_lines.append("- Connected Discord: —")
        ov_lines.append("- Linked: `no`")

    emb_overview = _embed_desc_lines("Member Lookup (1/5) — Overview", ov_lines)

    # Discord details embed
    disc_lines: list[str] = []
    if isinstance(member, discord.Member):
        disc_lines.append(f"**Current Roles**: {_roles_plain(member) or '—'}")
    # Stored history
    acc = rec.get("access") if isinstance(rec.get("access"), dict) else {}
    disc = rec.get("discord") if isinstance(rec.get("discord"), dict) else {}
    disc_lines.append("")
    disc_lines.append("**Access Timeline (from member_history.json)**:")
    disc_lines.append(f"- Ever Had Access Role: `{bool(acc.get('ever_had_access_role'))}`")
    disc_lines.append(f"- First Access: {_fmt_unix_ts(acc.get('first_access_ts'))}")
    disc_lines.append(f"- Last Access: {_fmt_unix_ts(acc.get('last_access_ts'))}")
    disc_lines.append(f"- Ever Had Member Role: `{bool(acc.get('ever_had_member_role'))}`")
    disc_lines.append(f"- First Member Role: {_fmt_unix_ts(acc.get('first_member_role_ts'))}")
    disc_lines.append(f"- Last Member Role: {_fmt_unix_ts(acc.get('last_member_role_ts'))}")
    disc_lines.append("")
    disc_lines.append("**Join/Leave (legacy fields)**:")
    disc_lines.append(f"- Join Count: `{int(rec.get('join_count') or 0)}`")
    disc_lines.append(f"- First Join: {_fmt_unix_ts(rec.get('first_join_ts'))}")
    disc_lines.append(f"- Last Join: {_fmt_unix_ts(rec.get('last_join_ts'))}")
    disc_lines.append(f"- Last Leave: {_fmt_unix_ts(rec.get('last_leave_ts'))}")
    if isinstance(disc.get("last_role_names"), list) and disc.get("last_role_names"):
        rn = ", ".join([str(x) for x in (disc.get("last_role_names") or []) if str(x).strip()][:40])
        if rn:
            disc_lines.append("")
            disc_lines.append("**Last Snapshot Roles (stored)**:")
            disc_lines.append(f"- {rn[:1800]}")
    emb_discord = _embed_desc_lines("Member Lookup (2/5) — Discord", disc_lines)

    # Whop summary embed (baseline-enriched)
    # Pick the best membership key we know about (prefer mem_ over R-).
    cands: list[str] = []
    with suppress(Exception):
        cands.append(str(wh.get("last_membership_id") or "").strip())
        cands.append(str(wh.get("last_whop_key") or "").strip())
    with suppress(Exception):
        for row in _member_status_latest_rows(rec)[:15]:
            if isinstance(row, dict):
                cands.append(str(row.get("membership_id") or "").strip())
    with suppress(Exception):
        for row in _native_logs_latest_rows(rec)[:25]:
            if isinstance(row, dict):
                cands.append(str(row.get("key") or "").strip())
    cands = [x for x in cands if x and x != "—"]
    last_mid = ""
    for x in cands:
        if str(x).startswith("mem_"):
            last_mid = str(x)
            break
    if not last_mid:
        for x in cands:
            if str(x).startswith("R-"):
                last_mid = str(x)
                break
    if not last_mid and cands:
        last_mid = str(cands[0])
    last_status = str(wh.get("last_status") or "").strip()
    base_summary = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
    base_brief = dict(base_summary) if isinstance(base_summary, dict) else {}
    if last_mid:
        base_brief.setdefault("membership_id", last_mid)
    if last_status and not str(base_brief.get("status") or "").strip():
        base_brief["status"] = last_status
    brief = _enrich_whop_brief_from_membership_logs(base_brief, membership_id=str(last_mid or "").strip())
    brief = brief if isinstance(brief, dict) else base_brief

    # Derived info
    product = str(brief.get("product") or "").strip()
    access_pass = ""
    with suppress(Exception):
        latest = wh.get("native_whop_logs_latest") if isinstance(wh.get("native_whop_logs_latest"), dict) else {}
        if isinstance(latest, dict):
            for v in sorted(list(latest.values()), key=lambda r: str((r or {}).get("recorded_at") or ""), reverse=True):
                if not isinstance(v, dict):
                    continue
                ap = str(v.get("access_pass") or "").strip()
                if ap:
                    access_pass = ap
                    break
    # If product is blank, try to fill from member-status baseline.
    if not product:
        with suppress(Exception):
            for row in _member_status_latest_rows(rec)[:10]:
                if not isinstance(row, dict):
                    continue
                p2 = str(row.get("product") or "").strip()
                if p2 and p2 != "—":
                    product = p2
                    break

    member_type = _whop_member_type(product, has_membership=bool(last_mid))
    if (member_type == "Whop (unknown product)") and access_pass:
        if "free pass" in access_pass.lower():
            member_type = f"Free Pass ({access_pass})"
        elif access_pass:
            member_type = access_pass

    member_since = str(brief.get("customer_since") or brief.get("member_since") or "").strip()
    if not member_since and isinstance(member, discord.Member):
        with suppress(Exception):
            if getattr(member, "joined_at", None):
                member_since = member.joined_at.astimezone(timezone.utc).strftime("%b %d, %Y")  # type: ignore[union-attr]

    last_seen = _derive_last_seen_utc(rec)
    wh_lines: list[str] = [
        f"**Current Member Type**: {member_type}",
        f"**Member Since**: {member_since or '—'}",
        f"**Last Seen**: {last_seen}",
        "",
        f"**Membership ID**: `{last_mid}`" if last_mid else "**Membership ID**: —",
    ]
    whop_user_id = _whop_user_id_for_lookup(db=db, brief=brief, membership_id=last_mid)
    if whop_user_id:
        wh_lines.append(f"**Whop User ID**: `{whop_user_id}`")
    if str(brief.get("status") or "").strip():
        wh_lines.append(f"**Status**: {_whop_status_label(str(brief.get('status') or ''))}")
    if product:
        wh_lines.append(f"**Membership / Product**: {product}")
    if access_pass:
        wh_lines.append(f"**Access Pass**: {access_pass}")
    if str(brief.get("total_spent") or "").strip():
        wh_lines.append(f"**Total Spent (lifetime)**: {str(brief.get('total_spent') or '').strip()}")
    if str(brief.get("remaining_days") or "").strip() and str(brief.get("remaining_days") or "").strip() != "—":
        wh_lines.append(f"**Remaining Days**: {str(brief.get('remaining_days') or '').strip()}")
    if str(brief.get("renewal_end") or "").strip() and str(brief.get("renewal_end") or "").strip() != "—":
        wh_lines.append(f"**Access Ends On**: {str(brief.get('renewal_end') or '').strip()}")
    if str(brief.get("next_billing_date") or "").strip() and str(brief.get("next_billing_date") or "").strip() != "—":
        wh_lines.append(f"**Next Billing Date**: {str(brief.get('next_billing_date') or '').strip()}")
    if str(brief.get("cancel_at_period_end") or "").strip():
        wh_lines.append(f"**Cancel At Period End**: {str(brief.get('cancel_at_period_end') or '').strip()}")
    dash = str(brief.get("dashboard_url") or "").strip()
    if dash:
        wh_lines.append(f"**Whop Dashboard**: {_embed_link('Open', dash)}")

    # Whop timeline fields (from whop_history backfill)
    wh_lines.append("")
    wh_lines.append("**Whop Timeline (from whop_history backfill, if available)**:")
    wh_lines.append(f"- First Seen: {_fmt_unix_ts(wh.get('first_seen_ts'))}")
    wh_lines.append(f"- Last Seen: {_fmt_unix_ts(wh.get('last_seen_ts'))}")
    wh_lines.append(f"- Last Active: {_fmt_unix_ts(wh.get('last_active_ts'))}")
    wh_lines.append(f"- Last Canceled: {_fmt_unix_ts(wh.get('last_canceled_ts'))}")

    # Pull key fields from membership-log baseline to fill gaps for support.
    merged_fields: dict[str, str] = {}
    with suppress(Exception):
        if whop_user_id:
            merged_fields = _merged_membership_logs_fields(db=db, whop_user_id=whop_user_id, membership_id=last_mid)
    if merged_fields:
        wh_lines.append("")
        wh_lines.append("**Whop Membership Log Fields (baseline)**:")
        for k in (
            "connected discord",
            "status",
            "membership",
            "product",
            "plan",
            "total spent",
            "total spent (lifetime)",
            "next billing date",
            "renewal window",
            "pricing",
            "manage",
            "checkout",
        ):
            v = str(merged_fields.get(k) or "").strip()
            if not v:
                continue
            label = k.title()
            if k in {"manage", "checkout"}:
                wh_lines.append(f"- {label}: {_embed_link('Open', v)}")
            else:
                wh_lines.append(f"- {label}: {v}")

    # Explicit linkage line on the Whop page too.
    if not any("Connected Discord" in x for x in wh_lines):
        wh_lines.insert(0, f"**Connected Discord**: `{connected_discord}`" if connected_discord else "**Connected Discord**: —")

    emb_whop = _embed_desc_lines("Member Lookup (3/5) — Whop", wh_lines)

    # History embed (member-status + whop logs + membership logs)
    hist_lines: list[str] = []
    ms_rows = _member_status_latest_rows(rec)
    if ms_rows:
        hist_lines.append("**Member Status Cards (latest per kind)**:")
        for row in ms_rows[:10]:
            title = str(row.get("title") or row.get("kind") or "").strip() or "—"
            created = _short_iso(str(row.get("created_at") or ""))
            st = str(row.get("status") or "").strip()
            prod = str(row.get("product") or "").strip()
            cd = str(row.get("connected_discord") or "").strip()
            parts = [created, title]
            if st:
                parts.append(f"status={st}")
            if prod:
                parts.append(f"product={prod}")
            if cd and cd != "—":
                parts.append(f"connected_discord={cd}")
            hist_lines.append("- " + " • ".join([p for p in parts if p])[:1800])
        hist_lines.append("")

    wl_rows = _native_logs_latest_rows(rec)
    if wl_rows:
        hist_lines.append("**Whop Logs (latest titles)**:")
        for row in wl_rows[:10]:
            title = str(row.get("title") or "").strip() or "—"
            created = _short_iso(str(row.get("created_at") or ""))
            ap = str(row.get("access_pass") or "").strip()
            k = str(row.get("key") or "").strip()
            bits = [created, title]
            if ap:
                bits.append(f"access_pass={ap}")
            if k:
                bits.append(f"key={k}")
            hist_lines.append("- " + " • ".join(bits)[:1800])
        hist_lines.append("")

    if whop_user_id:
        rows = _membership_logs_rows(db=db, whop_user_id=whop_user_id)
        if rows:
            hist_lines.append("**Whop Membership Logs (latest cards)**:")
            for r0 in rows[:10]:
                title = str(r0.get("title") or "").strip() or "—"
                created = _short_iso(str(r0.get("created_at") or r0.get("recorded_at") or ""))
                fields = r0.get("fields") if isinstance(r0.get("fields"), dict) else {}
                st2 = str(fields.get("status") or fields.get("membership status") or "").strip()
                mb = str(fields.get("membership") or fields.get("product") or "").strip()
                spent = str(fields.get("total spent") or fields.get("total spent (lifetime)") or "").strip()
                nb = str(fields.get("next billing date") or "").strip()
                rw = str(fields.get("renewal window") or "").strip()
                seg = [created, title]
                if st2:
                    seg.append(f"status={st2}")
                if mb:
                    seg.append(f"membership={mb}")
                if spent:
                    seg.append(f"spent={spent}")
                if nb:
                    seg.append(f"next={nb}")
                if rw:
                    seg.append(f"renewal={rw}")
                hist_lines.append("- " + " • ".join(seg)[:1800])

    emb_hist = _embed_desc_lines("Member Lookup (4/5) — History", (hist_lines if hist_lines else ["—"]))

    # Events embed (Discord-side audit trail from member_history)
    ev_lines: list[str] = []
    events = rec.get("events") if isinstance(rec.get("events"), list) else []
    if events:
        ev_lines.append("**Recent Discord Events (member_history)**:")

        def _role_name(rid: int) -> str:
            rr = guild.get_role(int(rid)) if guild else None
            return str(getattr(rr, "name", "") or f"role:{int(rid)}")

        for ev in list(events)[-25:][::-1]:
            if not isinstance(ev, dict):
                continue
            ts = _fmt_unix_ts(ev.get("ts"))
            kind = str(ev.get("kind") or "").strip() or "event"
            note = str(ev.get("note") or "").strip()
            added = [int(x) for x in (ev.get("roles_added") or []) if str(x).strip().isdigit()]
            removed = [int(x) for x in (ev.get("roles_removed") or []) if str(x).strip().isdigit()]
            parts = [f"{ts} • {kind}"]
            if note:
                parts.append(f"note={note}")
            if added:
                parts.append("added=" + ", ".join(_role_name(x) for x in added[:8]))
            if removed:
                parts.append("removed=" + ", ".join(_role_name(x) for x in removed[:8]))
            ev_lines.append("- " + " • ".join(parts)[:1800])
    emb_events = _embed_desc_lines("Member Lookup (5/5) — Events", (ev_lines if ev_lines else ["—"]))

    return [emb_overview, emb_discord, emb_whop, emb_hist, emb_events]


def _truthy(v: object) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _lookup_snowflake_created_at_utc(did: int) -> str:
    dt = _snowflake_created_at_utc(int(did or 0))
    return dt.astimezone(timezone.utc).strftime("%b %d, %Y") if dt else "—"


def _lookup_member_activity_kv(*, rec: dict, member: discord.Member) -> list[tuple[str, object]]:
    """MemberActivity block for lookup cards (best-effort; hide blanks)."""
    acc = rec.get("access") if isinstance(rec.get("access"), dict) else {}
    disc = rec.get("discord") if isinstance(rec.get("discord"), dict) else {}
    out: list[tuple[str, object]] = []
    out.append(("account_created", _lookup_snowflake_created_at_utc(int(member.id))))
    with suppress(Exception):
        if getattr(member, "joined_at", None):
            out.append(("first_joined", member.joined_at.astimezone(timezone.utc).strftime("%b %d, %Y")))  # type: ignore[union-attr]
    # Stored counts
    jc = int(rec.get("join_count") or 0) if isinstance(rec, dict) else 0
    if jc > 0:
        out.append(("join_count", jc))
        out.append(("returning_member", "true" if jc > 1 else "false"))
    # Leave
    out.append(("left_at", _fmt_unix_ts(rec.get("last_leave_ts"))))
    # Access / Members timeline
    out.append(("ever_had_member_role", "true" if bool(acc.get("ever_had_member_role")) else "false"))
    out.append(("first_access", _fmt_unix_ts(acc.get("first_access_ts"))))
    out.append(("last_access", _fmt_unix_ts(acc.get("last_access_ts"))))
    # Invite fields (if present in history)
    for k in ("invite_code", "tracked_invite", "source"):
        v = disc.get(k) if isinstance(disc, dict) else ""
        if str(v or "").strip():
            out.append((k, str(v).strip()))
    # Whop link status (display only; not used for logic)
    linked = False
    try:
        fn = _IS_WHOP_LINKED
        if fn:
            linked = bool(fn(int(member.id)))
    except Exception:
        linked = False
    out.append(("whop_link", "linked" if linked else "not linked"))
    return out


def _lookup_best_membership_id(*, rec: dict) -> str:
    """Pick best membership token we know (prefer mem_ over R-)."""
    if not isinstance(rec, dict):
        return ""
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    cands: list[str] = []
    with suppress(Exception):
        cands.append(str(wh.get("last_membership_id") or "").strip())
        cands.append(str(wh.get("last_whop_key") or "").strip())
    with suppress(Exception):
        for row in _member_status_latest_rows(rec)[:15]:
            if isinstance(row, dict):
                cands.append(str(row.get("membership_id") or "").strip())
    with suppress(Exception):
        for row in _native_logs_latest_rows(rec)[:25]:
            if isinstance(row, dict):
                cands.append(str(row.get("key") or "").strip())
    cands = [x for x in cands if x and x != "—"]
    for x in cands:
        if str(x).startswith("mem_"):
            return str(x)
    for x in cands:
        if str(x).startswith("R-"):
            return str(x)
    return str(cands[0]) if cands else ""


def _lookup_best_whop_brief(*, db: dict, rec: dict) -> dict:
    """Best-effort Whop brief for lookup cards (baseline-enriched)."""
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    base_summary = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
    brief0 = dict(base_summary) if isinstance(base_summary, dict) else {}
    mid = _lookup_best_membership_id(rec=rec)
    if mid:
        brief0.setdefault("membership_id", mid)
    # Enrich from whop-membership-logs baseline when possible (no extra API calls).
    with suppress(Exception):
        brief0 = _enrich_whop_brief_from_membership_logs(brief0, membership_id=str(mid or "").strip())
    return brief0 if isinstance(brief0, dict) else {}


def _lookup_pick_source_row(*, db: dict, rec: dict, category: str) -> tuple[str, dict]:
    """Return (source_kind, row_dict) for the most relevant last card."""
    cat = str(category or "").strip().lower()
    ms_rows = _member_status_latest_rows(rec)
    ms_kinds: set[str] = set()
    ms_keywords: tuple[str, ...] = ()
    ml_keywords: tuple[str, ...] = ()
    wl_keywords: tuple[str, ...] = ()
    if cat == "payment":
        ms_kinds = {"payment_failed", "payment_succeeded", "payment_pending", "invoice_paid", "invoice_past_due", "setup_intent_requires_action"}
        ms_keywords = ("payment", "invoice")
        ml_keywords = ("payment", "invoice", "setup intent")
        wl_keywords = ("payment", "purchased", "membership update")
    elif cat == "membership":
        ms_kinds = {"membership_activated_pending", "membership_activated", "member_joined", "trialing", "deactivated", "member_role_added"}
        ms_keywords = ("membership", "activated", "joined", "deactivated")
        ml_keywords = ("membership", "activated", "pending", "deactivated")
        wl_keywords = ("membership", "purchased", "membership update")
    elif cat == "cancellation":
        ms_kinds = {"cancellation_scheduled", "cancellation_removed", "deactivated"}
        ms_keywords = ("cancellation", "cancel", "deactivated")
        ml_keywords = ("cancel", "cancellation")
        wl_keywords = ("cancel", "cancellation")
    elif cat == "dispute":
        ms_kinds = {"dispute_created", "dispute_updated", "refund_created", "refund_updated"}
        ms_keywords = ("dispute", "refund", "chargeback")
        ml_keywords = ("dispute", "refund", "chargeback")
        wl_keywords = ("dispute", "refund", "chargeback")

    # 1) member-status-logs baseline (best when available)
    for row in (ms_rows or [])[:25]:
        if not isinstance(row, dict):
            continue
        k = str(row.get("kind") or "").strip().lower()
        t = str(row.get("title") or "").strip().lower()
        if (k and k in ms_kinds) or (ms_keywords and any(x in t for x in ms_keywords)):
            return ("member_status_logs", row)

    # 2) whop-membership-logs baseline (needs whop_user_id)
    brief = _lookup_best_whop_brief(db=db, rec=rec)
    mid = str(brief.get("membership_id") or "").strip()
    whop_uid = _whop_user_id_for_lookup(db=db, brief=brief, membership_id=mid)
    if whop_uid:
        rows = _membership_logs_rows(db=db, whop_user_id=whop_uid)
        for r0 in (rows or [])[:30]:
            title = str(r0.get("title") or "").strip().lower()
            if ml_keywords and any(x in title for x in ml_keywords):
                return ("whop_membership_logs", r0)

    # 3) native whop-logs baseline
    wl = _native_logs_latest_rows(rec)
    for r1 in (wl or [])[:30]:
        title = str(r1.get("title") or "").strip().lower()
        if wl_keywords and any(x in title for x in wl_keywords):
            return ("whop_logs", r1)

    return ("synthetic", {})


def _lookup_title_color_for(*, category: str, brief: dict) -> tuple[str, int, str]:
    """Return (title, color, event_kind) for the lookup card."""
    cat = str(category or "").strip().lower()
    st = str((brief or {}).get("status") or "").strip().lower()
    cap = str((brief or {}).get("cancel_at_period_end") or "").strip().lower()
    if cat == "payment":
        if st in {"past_due", "unpaid"}:
            return ("❌ Payment Failed — Action Needed", 0xED4245, "payment_failed")
        # Always distinct: Payment = blue (even when active).
        return ("✅ Payment Activated", 0x5865F2, "payment_succeeded")
    if cat == "membership":
        if st in {"deactivated", "canceled", "cancelled", "expired"}:
            return ("🟧 Membership Deactivated", 0xF59E0B, "deactivated")
        if st in {"trialing", "pending"}:
            return ("⏳ Membership Activated (Pending)", 0x57F287, "membership_activated_pending")
        return ("✅ Membership Activated", 0x57F287, "active")
    if cat == "cancellation":
        if cap in {"yes", "true", "1"}:
            return ("⚠️ Cancellation Scheduled", 0xFEE75C, "cancellation_scheduled")
        if st in {"canceled", "cancelled", "deactivated"}:
            return ("🟧 Membership Deactivated", 0xF59E0B, "deactivated")
        # Always distinct: Cancellation = yellow when "removed"/active.
        return ("✅ Cancellation Removed", 0xFEE75C, "active")
    if cat == "dispute":
        return ("⚠️ Dispute", 0xED4245, "dispute")
    return ("Member Lookup", 0x5865F2, "active")


def _lookup_brief_from_source_row(*, source_kind: str, row: dict, base_brief: dict) -> tuple[dict, str]:
    """Overlay the chosen source row onto base_brief. Return (brief, source_line)."""
    brief = dict(base_brief) if isinstance(base_brief, dict) else {}
    sk = str(source_kind or "").strip()
    r = row if isinstance(row, dict) else {}
    created = _short_iso(str(r.get("created_at") or r.get("recorded_at") or ""))
    jump = str(r.get("jump_url") or "").strip()
    source_line = f"{sk} • {created}"
    if jump:
        source_line += f" • {_embed_link('Open', jump)}"

    if sk == "member_status_logs":
        for k in (
            "membership_id",
            "status",
            "product",
            "remaining_days",
            "renewal_end",
            "renewal_end_iso",
            "total_spent",
            "dashboard_url",
            "cancel_at_period_end",
            "connected_discord",
        ):
            v = str(r.get(k) or "").strip()
            if v:
                brief[k] = v
    elif sk == "whop_logs":
        # native whop-logs baseline row
        k0 = str(r.get("key") or "").strip()
        if k0:
            brief["membership_id"] = k0
        st0 = str(r.get("membership_status") or "").strip()
        if st0:
            brief["status"] = st0
        ap = str(r.get("access_pass") or "").strip()
        if ap and (not str(brief.get("product") or "").strip()):
            brief["product"] = ap
    elif sk == "whop_membership_logs":
        # baseline fields already merged in whop_brief; add anything obviously present
        fields = r.get("fields") if isinstance(r.get("fields"), dict) else {}
        if isinstance(fields, dict):
            mid = str(r.get("membership_id") or fields.get("membership id") or "").strip()
            if mid:
                brief["membership_id"] = mid
            if str(fields.get("status") or "").strip():
                brief["status"] = str(fields.get("status") or "").strip()
            # Prefer product/membership label when present
            prod = str(fields.get("product") or fields.get("membership") or "").strip()
            if prod:
                brief["product"] = prod
            spent = str(fields.get("total spent") or fields.get("total spent (lifetime)") or "").strip()
            if spent and (not str(brief.get("total_spent") or "").strip()):
                brief["total_spent"] = spent
            dash = str(fields.get("dashboard") or "").strip()
            if dash and (not str(brief.get("dashboard_url") or "").strip()):
                brief["dashboard_url"] = dash
            rw = str(fields.get("renewal window") or "").strip()
            if rw and (not str(brief.get("renewal_window") or "").strip()):
                brief["renewal_window"] = rw
            cd = str(fields.get("connected discord") or fields.get("connected_discord") or "").strip()
            if cd and (not str(brief.get("connected_discord") or "").strip()):
                brief["connected_discord"] = cd

    return (brief, source_line)


def _lookup_cache_get(did: int, *, ttl_seconds: int = 90) -> dict | None:
    try:
        did_i = int(did or 0)
    except Exception:
        return None
    if did_i <= 0:
        return None
    try:
        now = datetime.now(timezone.utc).timestamp()
        at = float(_LOOKUP_LIVE_CACHE_AT.get(did_i) or 0.0)
    except Exception:
        now, at = 0.0, 0.0
    if at and (now - at) <= float(max(10, min(int(ttl_seconds or 90), 900))):
        v = _LOOKUP_LIVE_CACHE.get(did_i)
        return v if isinstance(v, dict) else None
    return None


def _lookup_cache_set(did: int, data: dict) -> None:
    try:
        did_i = int(did or 0)
    except Exception:
        return
    if did_i <= 0 or not isinstance(data, dict):
        return
    _LOOKUP_LIVE_CACHE[did_i] = data
    _LOOKUP_LIVE_CACHE_AT[did_i] = datetime.now(timezone.utc).timestamp()


def _infer_memberstatus_kind_from_title(title: str) -> str:
    """Best-effort kind inference from a member-status-logs embed title."""
    t = str(title or "").strip().lower()
    if not t:
        return "unknown"
    if "payment created" in t:
        return "payment_created"
    if "payment pending" in t:
        return "payment_pending"
    if "setup intent" in t:
        return "setup_intent"
    if "invoice " in t:
        return "invoice"
    if "refund" in t:
        return "refund"
    if "dispute" in t:
        return "dispute"
    if "payment failed" in t or "billing issue" in t or "access risk" in t:
        return "payment_failed"
    if "payment succeeded" in t or "payment renewed" in t or "payment activated" in t or "access restored" in t:
        return "payment_succeeded"
    if "cancellation removed" in t:
        return "cancellation_removed"
    if "cancellation scheduled" in t or "set to cancel" in t or "canceling" in t:
        return "cancellation_scheduled"
    if "member joined" in t:
        return "member_joined"
    if "member left" in t:
        return "member_left"
    if "activated (pending)" in t or "activation (pending)" in t:
        return "membership_activated_pending"
    if "membership activated" in t:
        return "membership_activated"
    if "deactivated" in t or "access ended" in t:
        return "deactivated"
    return "unknown"


def _brief_from_member_status_embed(e: discord.Embed) -> dict:
    """Extract a whop_brief-ish dict from a RSCheckerbot member-status-logs embed."""
    if not isinstance(e, discord.Embed):
        return {}
    out: dict[str, object] = {}
    with suppress(Exception):
        for f in (getattr(e, "fields", None) or []):
            n = str(getattr(f, "name", "") or "").strip().lower()
            v = str(getattr(f, "value", "") or "").strip()
            if not n or not v:
                continue
            if n in {"membership id", "membership_id"}:
                out["membership_id"] = v.strip("`")
            elif n in {"membership", "product"}:
                out["product"] = v
            elif n == "status":
                out["status"] = v
            elif n.startswith("total spent"):
                out["total_spent"] = v
            elif n in {"whop dashboard", "dashboard"}:
                out["dashboard_url"] = _embed_link("Open", v)
            elif n in {"renewal window", "renewal_window"}:
                out["renewal_window"] = v
            elif n in {"remaining days", "remaining_days"}:
                out["remaining_days"] = v
            elif n in {"next billing date", "access ends on", "renewal end", "renewal_end"}:
                out["renewal_end"] = v
            elif n in {"connected discord", "connected_discord"}:
                out["connected_discord"] = v
            elif n in {"cancel at period end", "cancel_at_period_end"}:
                out["cancel_at_period_end"] = v
            elif n in {"customer since", "member since", "customer_since", "member_since"}:
                out["customer_since"] = v
    return out


async def _scan_member_status_logs_for_did(*, guild: discord.Guild, did: int, limit: int) -> list[dict]:
    """Scan member-status-logs channel for embeds matching this Discord ID (newest-first)."""
    cfg = _cfg()
    if not cfg or not _BOT:
        return []
    cid = int(cfg.member_status_logs_channel_id or 0)
    if cid <= 0:
        return []
    try:
        lim = max(50, min(int(limit or 500), 2000))
    except Exception:
        lim = 500
    ch0 = _BOT.get_channel(int(cid))
    if ch0 is None:
        with suppress(Exception):
            ch0 = await _BOT.fetch_channel(int(cid))
    if not isinstance(ch0, discord.TextChannel):
        return []
    rows: list[dict] = []
    did_s = str(int(did))

    def _embed_has_did(e: discord.Embed) -> bool:
        with suppress(Exception):
            for f in (getattr(e, "fields", None) or []):
                if str(getattr(f, "name", "") or "").strip().lower() == "discord id":
                    return did_s in str(getattr(f, "value", "") or "")
        return False

    async for m in ch0.history(limit=lim):
        if not m.embeds:
            continue
        e0 = m.embeds[0]
        if not isinstance(e0, discord.Embed):
            continue
        if not _embed_has_did(e0):
            continue
        title = str(getattr(e0, "title", "") or "").strip()
        kind = _infer_memberstatus_kind_from_title(title)
        brief = _brief_from_member_status_embed(e0)
        rows.append(
            {
                "kind": kind,
                "title": title,
                "created_at": (m.created_at.astimezone(timezone.utc).isoformat() if getattr(m, "created_at", None) else ""),
                "jump_url": str(getattr(m, "jump_url", "") or "").strip(),
                "brief": brief,
                "source": "member_status_logs",
            }
        )
        if len(rows) >= 25:
            break
    return rows


def _parse_whop_bullets(text: str) -> dict[str, str]:
    """Parse bullet lines like '• Key: Value' into a dict (lookup-only)."""
    out: dict[str, str] = {}
    s = str(text or "")
    for raw in s.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        m = re.match(r"^[•*\-]\s*([^:]{1,64})\s*:\s*(.+?)\s*$", ln)
        if not m:
            continue
        k = re.sub(r"\s+", " ", str(m.group(1) or "").strip().lower())
        v = str(m.group(2) or "").strip()
        if not k or not v:
            continue
        out[k[:64]] = v[:2000]
    return out


def _brief_from_whop_logs_embed(embed: discord.Embed) -> dict:
    """Extract a whop_brief-ish dict from a native `#whop-logs` embed."""
    b: dict[str, object] = {}
    email = ""
    name = ""
    did = ""
    duser = ""
    key = ""
    access_pass = ""
    status = ""
    dash = ""
    def _extract_url_any(raw: str) -> str:
        s = str(raw or "").strip()
        if not s:
            return ""
        m = re.search(r"\((https?://[^)]+)\)", s)
        if m:
            return str(m.group(1)).strip()
        m2 = re.search(r"(https?://\S+)", s)
        return str(m2.group(1)).strip().rstrip(").,") if m2 else ""
    try:
        for f in (getattr(embed, "fields", None) or []):
            n = str(getattr(f, "name", "") or "").strip().lower()
            v = str(getattr(f, "value", "") or "").strip()
            if n == "email":
                email = v
            elif n in {"name", "member (whop)", "member"}:
                name = v
            elif n == "discord id":
                did = v
            elif n == "discord username":
                duser = v
            elif n == "key":
                key = v
            elif n in {"access pass", "access_pass"}:
                access_pass = v
            elif n in {"membership status", "membership_status", "status"}:
                status = v
            elif n in {"logs", "view detailed logs"}:
                dash = _extract_url_any(v)
    except Exception:
        pass
    if key:
        b["membership_id"] = key
    if status:
        b["status"] = status
    if access_pass:
        # Most useful label for support when product isn't known.
        b["product"] = access_pass
    if did:
        b["connected_discord"] = did
    if dash:
        # Not always the Whop dashboard, but gives staff a clickable reference.
        b["dashboard_url"] = _embed_link("Open", dash)
    if name:
        b["user_name"] = name
    if email:
        b["email"] = email
    if duser:
        b["discord_username"] = duser
    return b


def _brief_from_membership_logs_embed(embed: discord.Embed) -> dict:
    """Extract a whop_brief-ish dict from a `#whop-membership-logs` embed."""
    b: dict[str, object] = {}
    blob = ""
    with suppress(Exception):
        blob = str(getattr(embed, "description", "") or "")
    fields_blob = ""
    with suppress(Exception):
        fields_blob = "\n".join([str(getattr(f, "value", "") or "") for f in (getattr(embed, "fields", None) or [])])
    kv = _parse_whop_bullets(blob + "\n" + fields_blob)
    # Some cards put important info in the title (e.g. "Payment Succeeded").
    with suppress(Exception):
        b["__title"] = str(getattr(embed, "title", "") or "").strip()

    def _get(*keys: str) -> str:
        for k in keys:
            v = str(kv.get(str(k).strip().lower()) or "").strip()
            if v:
                return v
        return ""

    mid = _get("membership id", "membership_id")
    if mid:
        b["membership_id"] = mid
    st = _get("status", "membership status")
    if st:
        b["status"] = st
    prod = _get("product", "membership", "plan / product", "plan", "membership")
    if prod:
        b["product"] = prod
    spent = _get("total spent", "total spent (lifetime)", "total spent:")  # key variants already normalized
    if spent:
        b["total_spent"] = spent
    rw = _get("renewal window")
    if rw:
        b["renewal_window"] = rw
    td = _get("trial days", "trial_days")
    if td:
        b["trial_days"] = td
    pir = _get("plan is renewal", "plan_is_renewal")
    if pir:
        b["plan_is_renewal"] = pir
    pricing = _get("pricing")
    if pricing:
        b["pricing"] = pricing
    dash = _get("dashboard")
    if dash:
        b["dashboard_url"] = _embed_link("Open", dash)
    manage = _get("manage")
    if manage:
        b["manage_url"] = _embed_link("Open", manage)
    checkout = _get("checkout", "purchase link")
    if checkout:
        b["checkout_url"] = _embed_link("Open", checkout)
    cd = _get("connected discord", "connected_discord")
    if cd:
        b["connected_discord"] = cd
    # Helpful identity keys (for matching / display)
    uname = _get("username")
    if uname:
        b["username"] = uname
    nm = _get("name")
    if nm:
        b["user_name"] = nm
    em = _get("email")
    if em:
        b["email"] = em
    return b


async def _build_member_lookup_category_embeds(*, guild: discord.Guild, did: int, category: str) -> list[discord.Embed]:
    """Build a single 'last card' style embed for the chosen category, with baseline fallback."""
    did_i = int(did or 0)
    member = guild.get_member(did_i) if did_i > 0 else None

    db = _member_history_db_cached()
    rec = db.get(str(did_i)) if isinstance(db, dict) else {}
    rec = rec if isinstance(rec, dict) else {}

    base_brief = _lookup_best_whop_brief(db=db, rec=rec) if rec else {}

    def _has_any_whop_value(b: dict) -> bool:
        if not isinstance(b, dict):
            return False
        for k in ("membership_id", "status", "product", "total_spent", "dashboard_url", "renewal_window", "renewal_end", "remaining_days"):
            if str(b.get(k) or "").strip() and str(b.get(k) or "").strip() != "—":
                return True
        return False

    # Always prefer the latest matching member-status-logs card (it represents the real staff card).
    # Cache it so switching buttons is instant.
    live = _lookup_cache_get(did_i, ttl_seconds=120) or {}
    if not isinstance(live, dict):
        live = {}
    if "member_status" not in live:
        cfg = _cfg()
        # Reuse max_native_logs as a cap for live scans (keeps this configurable).
        scan_lim = 200 * int((cfg.member_lookup_max_native_logs or 5) if cfg else 5)
        with suppress(Exception):
            ms_rows = await _scan_member_status_logs_for_did(guild=guild, did=did_i, limit=scan_lim)
            live["member_status"] = ms_rows
            _lookup_cache_set(did_i, live)

    # If baseline is missing (or member isn't in server), also live scan whop-logs + whop-membership-logs
    # so we can fill fields when member-status cards are sparse.
    if (not live.get("whop_logs") and not live.get("membership_logs")) and (
        (not _has_any_whop_value(base_brief)) or (not isinstance(member, discord.Member))
    ):
        cfg = _cfg()
        logs_cid = int(cfg.whop_logs_channel_id or 0) if cfg else 0
        mem_cid = int(cfg.whop_membership_logs_channel_id or 0) if cfg else 0

        async def _get_text_channel(cid: int) -> discord.TextChannel | None:
            if cid <= 0:
                return None
            ch0 = guild.get_channel(int(cid))
            if not isinstance(ch0, discord.TextChannel):
                with suppress(Exception):
                    if _BOT:
                        ch0 = await _BOT.fetch_channel(int(cid))
            return ch0 if isinstance(ch0, discord.TextChannel) else None

        logs_ch = await _get_text_channel(logs_cid)
        mem_ch = await _get_text_channel(mem_cid)

        whop_logs_hits: list[dict] = []
        mem_logs_hits: list[dict] = []
        email_hint = ""
        name_hint = str(getattr(member, "display_name", "") or getattr(member, "name", "") or "").strip() if isinstance(member, discord.Member) else ""
        # 1) Find the latest whop-logs card for this Discord ID (gives us email for matching).
        if isinstance(logs_ch, discord.TextChannel):
            try:
                async for m in logs_ch.history(limit=250):
                    if not m.embeds:
                        continue
                    e0 = m.embeds[0]
                    if not isinstance(e0, discord.Embed):
                        continue
                    # Match Discord ID field.
                    did_ok = False
                    with suppress(Exception):
                        for f in (getattr(e0, "fields", None) or []):
                            if str(getattr(f, "name", "") or "").strip().lower() == "discord id":
                                if re.search(rf"\b{int(did_i)}\b", str(getattr(f, "value", "") or "")):
                                    did_ok = True
                                    break
                    if not did_ok:
                        continue
                    b0 = _brief_from_whop_logs_embed(e0)
                    email_hint = str(b0.get("email") or "").strip()
                    whop_logs_hits.append(
                        {
                            "title": str(getattr(e0, "title", "") or "").strip(),
                            "created_at": (m.created_at.astimezone(timezone.utc).isoformat() if getattr(m, "created_at", None) else ""),
                            "jump_url": str(getattr(m, "jump_url", "") or "").strip(),
                            "brief": b0,
                            "kind": "whop_logs",
                        }
                    )
                    if len(whop_logs_hits) >= 6:
                        break
            except Exception:
                pass

        # 2) Find latest membership-logs cards by email (best) or by name (fallback).
        if isinstance(mem_ch, discord.TextChannel) and (email_hint or name_hint):
            try:
                email_l = str(email_hint).strip().lower()
                name_l = str(name_hint).strip().lower()
                async for m in mem_ch.history(limit=350):
                    if not m.embeds:
                        continue
                    e0 = m.embeds[0]
                    if not isinstance(e0, discord.Embed):
                        continue
                    fields_txt = ""
                    with suppress(Exception):
                        fields_txt = "\n".join([str(getattr(f, "value", "") or "") for f in (getattr(e0, "fields", None) or [])])
                    blob = (str(getattr(e0, "title", "") or "") + "\n" + str(getattr(e0, "description", "") or "") + "\n" + fields_txt).lower()
                    if email_l and email_l in blob:
                        pass
                    elif name_l and name_l in blob:
                        pass
                    else:
                        continue
                    b0 = _brief_from_membership_logs_embed(e0)
                    mem_logs_hits.append(
                        {
                            "title": str(getattr(e0, "title", "") or "").strip(),
                            "created_at": (m.created_at.astimezone(timezone.utc).isoformat() if getattr(m, "created_at", None) else ""),
                            "jump_url": str(getattr(m, "jump_url", "") or "").strip(),
                            "brief": b0,
                            "kind": "whop_membership_logs",
                        }
                    )
                    if len(mem_logs_hits) >= 10:
                        break
            except Exception:
                pass

        live = {"whop_logs": whop_logs_hits, "membership_logs": mem_logs_hits, "email": email_hint, "name": name_hint}
        _lookup_cache_set(did_i, live)

    def _pick_live_row(cat: str) -> tuple[str, dict]:
        """Pick the most relevant live row for this category."""
        c = str(cat or "").strip().lower()
        ms_rows = [r for r in (live.get("member_status") or []) if isinstance(r, dict)]
        mem_rows = [r for r in (live.get("membership_logs") or []) if isinstance(r, dict)]
        wl_rows = [r for r in (live.get("whop_logs") or []) if isinstance(r, dict)]
        # Prefer membership logs, then whop logs.
        if c == "payment":
            keys = ("payment", "invoice", "setup intent")
        elif c == "membership":
            keys = ("membership", "activated", "pending", "trial")
        elif c == "cancellation":
            keys = ("cancel", "cancellation", "deactivated", "ended", "expired")
        elif c == "dispute":
            keys = ("dispute", "refund", "chargeback")
        else:
            keys = ()

        # Prefer real staff cards first (member-status-logs).
        if ms_rows:
            want: set[str] = set()
            if c == "payment":
                want = {"payment_failed", "payment_succeeded", "payment_created", "payment_pending", "invoice", "setup_intent"}
            elif c == "membership":
                want = {"membership_activated_pending", "membership_activated", "member_joined", "trialing", "deactivated"}
            elif c == "cancellation":
                want = {"cancellation_scheduled", "cancellation_removed", "deactivated"}
            elif c == "dispute":
                want = {"dispute", "refund"}
            for r in ms_rows:
                if str(r.get("kind") or "").strip().lower() in want:
                    return ("member_status_logs", r)

        for r in mem_rows:
            t = str(r.get("title") or "").strip().lower()
            if keys and any(k in t for k in keys):
                return ("whop_membership_logs", r)
        for r in wl_rows:
            t = str(r.get("title") or "").strip().lower()
            if keys and any(k in t for k in keys):
                return ("whop_logs", r)
        # If there is no matching row, do NOT reuse a different category's rows.
        if c in {"payment", "membership", "cancellation", "dispute"}:
            return ("synthetic", {"title": f"No recent {c} cards found", "created_at": "", "jump_url": "", "brief": {}, "kind": "synthetic"})
        # Fallback: newest membership log if exists, else newest whop log.
        return ("whop_membership_logs", mem_rows[0]) if mem_rows else (("whop_logs", wl_rows[0]) if wl_rows else ("synthetic", {}))

    # Choose a source row.
    # Pick the most relevant source for this category.
    # Prefer live member-status rows, then member_history-derived rows, then other live logs.
    source_kind, row = _pick_live_row(str(category or ""))
    if source_kind == "synthetic" and rec:
        source_kind, row = _lookup_pick_source_row(db=db, rec=rec, category=category)

    # Merge base brief with source row's parsed brief (live rows include brief already).
    brief2 = dict(base_brief) if isinstance(base_brief, dict) else {}
    source_line = "synthetic"
    if isinstance(row, dict) and isinstance(row.get("brief"), dict):
        brief2.update(row.get("brief") or {})
        source_line = f"{str(row.get('kind') or source_kind)} • {_short_iso(str(row.get('created_at') or ''))}"
        if str(row.get("jump_url") or "").strip():
            source_line += f" • {_embed_link('Open', str(row.get('jump_url') or '').strip())}"
    else:
        brief2, source_line = _lookup_brief_from_source_row(source_kind=source_kind, row=row if isinstance(row, dict) else {}, base_brief=brief2)

    # Always ensure Connected Discord reflects the lookup Discord ID.
    brief2.setdefault("connected_discord", str(did_i))

    # Final enrichment pass (fills blanks from whop-membership-logs baseline when possible).
    with suppress(Exception):
        brief2 = _enrich_whop_brief_from_membership_logs(brief2, membership_id=str(brief2.get("membership_id") or "").strip())

    title, color, event_kind = _lookup_title_color_for(category=category, brief=brief2)
    # If we found a real member-status card, use its title (this is what staff expects).
    src_title = ""
    with suppress(Exception):
        if isinstance(row, dict):
            src_title = str(row.get("title") or "").strip()
            if source_kind == "member_status_logs" and src_title:
                title = src_title[:240]

    # Build embed:
    # - if member exists in guild: full Member Status Tracking layout
    # - otherwise: compact lookup embed using whop-logs/membership-logs data
    if isinstance(member, discord.Member):
        access_roles = _roles_plain(member) or "—"
        member_kv = _lookup_member_activity_kv(rec=rec, member=member)
        emb = _build_member_status_detailed_embed(
            title=title,
            member=member,
            access_roles=access_roles,
            color=int(color),
            member_kv=member_kv,
            discord_kv=[],
            whop_brief=(brief2 if isinstance(brief2, dict) else {}),
            event_kind=event_kind,
            force_whop_core_fields=True,
        )
    else:
        emb = discord.Embed(title=title, color=int(color), timestamp=_now_utc())
        # Basic identity (no Member object available)
        wh_name = str((brief2 or {}).get("user_name") or (live.get("name") if isinstance(live, dict) else "") or "—").strip()  # type: ignore[union-attr]
        email0 = str((brief2 or {}).get("email") or (live.get("email") if isinstance(live, dict) else "") or "—").strip()  # type: ignore[union-attr]
        emb.add_field(name="Member (Whop)", value=wh_name[:1024] if wh_name else "—", inline=True)
        emb.add_field(name="Discord ID", value=f"`{did_i}`", inline=True)
        emb.add_field(name="In Server", value="no", inline=True)
        if email0 and email0 != "—":
            emb.add_field(name="Email", value=email0[:1024], inline=True)
        # Whop core
        mid0 = str((brief2 or {}).get("membership_id") or "").strip()
        if mid0:
            emb.add_field(name="Membership ID", value=f"`{mid0[:128]}`", inline=True)
        st0 = str((brief2 or {}).get("status") or "").strip()
        if st0:
            emb.add_field(name="Status", value=st0[:1024], inline=True)
        prod0 = str((brief2 or {}).get("product") or "").strip()
        if prod0:
            emb.add_field(name="Membership", value=prod0[:1024], inline=True)
        spent0 = str((brief2 or {}).get("total_spent") or "").strip()
        if spent0:
            emb.add_field(name="Total Spent (lifetime)", value=spent0[:1024], inline=True)
        renew0 = str((brief2 or {}).get("renewal_window") or "").strip()
        if renew0:
            emb.add_field(name="Renewal Window", value=renew0[:1024], inline=False)
        dash0 = str((brief2 or {}).get("dashboard_url") or "").strip()
        if dash0:
            emb.add_field(name="Whop Dashboard", value=str(dash0)[:1024], inline=False)
        emb.set_footer(text="RSCheckerbot • Member Lookup")

    # Add a compact source line so staff can click through when available.
    with suppress(Exception):
        emb.add_field(name="Source", value=str(source_line or "synthetic")[:1024], inline=False)
    if src_title:
        with suppress(Exception):
            emb.add_field(name="Last Card Title", value=str(src_title)[:1024], inline=False)
    return [emb]


def _build_member_lookup_picker_embed(*, guild: discord.Guild, did: int) -> discord.Embed:
    did_i = int(did or 0)
    member = guild.get_member(did_i) if did_i > 0 else None
    mention = member.mention if isinstance(member, discord.Member) else f"<@{did_i}>"
    roles_txt = _roles_plain(member) if isinstance(member, discord.Member) else ""
    lines: list[str] = [
        f"**Member**: {mention}",
        f"**Discord ID**: `{did_i}`",
        f"**Current Roles**: {roles_txt or '—'}",
        f"**In Server**: `{'yes' if isinstance(member, discord.Member) else 'no'}`",
        "",
        "**Pick what you want to view:**",
        "- Payment",
        "- Membership",
        "- Cancellation",
        "- Dispute",
    ]
    open_lines = _open_tickets_lines_for_user(user_id=did_i)
    if open_lines:
        lines.append("")
        lines.append("**Open Tickets:**")
        lines.extend(open_lines)
    e = _embed_desc_lines("Member Lookup — Select View", lines)
    e.color = 0x2B2D31
    return e


def _native_logs_latest_rows(rec: dict) -> list[dict]:
    """Return native_whop_logs_latest entries sorted newest->oldest (best-effort)."""
    if not isinstance(rec, dict):
        return []
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    latest = wh.get("native_whop_logs_latest") if isinstance(wh.get("native_whop_logs_latest"), dict) else {}
    rows: list[dict] = []
    if isinstance(latest, dict):
        for v in latest.values():
            if isinstance(v, dict):
                rows.append(v)

    def _ts(row: dict) -> float:
        dt = _parse_dt_any(row.get("created_at") or row.get("recorded_at") or "")
        if not dt:
            return 0.0
        return float(dt.timestamp())

    rows.sort(key=_ts, reverse=True)
    return rows


def _member_status_latest_rows(rec: dict) -> list[dict]:
    """Return member_status_logs_latest entries sorted newest->oldest (best-effort)."""
    if not isinstance(rec, dict):
        return []
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    latest = wh.get("member_status_logs_latest") if isinstance(wh.get("member_status_logs_latest"), dict) else {}
    rows: list[dict] = []
    if isinstance(latest, dict):
        for v in latest.values():
            if isinstance(v, dict):
                rows.append(v)

    def _ts(row: dict) -> float:
        dt = _parse_dt_any(row.get("created_at") or row.get("recorded_at") or "")
        if not dt:
            return 0.0
        return float(dt.timestamp())

    rows.sort(key=_ts, reverse=True)
    return rows


def _build_member_lookup_result_embed(*, guild: discord.Guild, did: int) -> discord.Embed:
    # Back-compat wrapper (some callers expect a single embed).
    embs = _build_member_lookup_result_embeds(guild=guild, did=int(did or 0))
    return embs[0] if embs else discord.Embed(title="Member Lookup", description="—", color=0x5865F2)


async def ensure_member_lookup_panel() -> None:
    """Ensure the staff lookup panel exists in the configured channel (no channel spam)."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or (not bool(cfg.member_lookup_enabled)):
        return
    ch_id = int(cfg.member_lookup_channel_id or 0)
    if ch_id <= 0:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return
    ch = guild.get_channel(int(ch_id))
    if not isinstance(ch, discord.TextChannel):
        return

    state = _member_lookup_state_load()
    mid = _as_int(state.get("message_id"))
    view = _MEMBER_LOOKUP_VIEW or MemberLookupPanelView()
    emb = discord.Embed(title=str(cfg.member_lookup_panel_title or "Ticket Tool"), color=0x2B2D31)
    emb.description = str(cfg.member_lookup_panel_description or "").strip()[:4000]

    sent = None
    if mid > 0:
        try:
            msg = await ch.fetch_message(int(mid))
            await msg.edit(embed=emb, view=view)
            sent = msg
        except Exception:
            sent = None
    if not sent:
        try:
            sent = await ch.send(embed=emb, view=view, allowed_mentions=discord.AllowedMentions.none(), silent=True)
        except Exception:
            sent = None
    if sent:
        _member_lookup_state_save(
            {
                "channel_id": int(ch.id),
                "message_id": int(sent.id),
                "updated_at_iso": _now_iso(),
            }
        )

    # Best-effort: ensure ticket index file is writable (avoid silent failures later).
    try:
        _index_save(_index_load())
    except Exception as e:
        # Keep the bot running, but surface why tickets might not open.
        with suppress(Exception):
            asyncio.create_task(_log(f"❌ support_tickets: failed to init tickets_index.json (OneDrive lock?) err={str(e)[:220]}"))

    # One-time startup migrations (run after bot is ready).
    if _BOT:
        with suppress(Exception):
            asyncio.create_task(_run_post_ready_migrations())


def _migrations_load() -> dict:
    raw = _load_json(MIGRATIONS_STATE_PATH)
    return raw if isinstance(raw, dict) else {}


def _migrations_save(db: dict) -> None:
    _save_json(MIGRATIONS_STATE_PATH, db if isinstance(db, dict) else {})


async def _run_post_ready_migrations() -> None:
    """Run one-time migrations after the bot is ready."""
    if not _BOT:
        return
    with suppress(Exception):
        await _BOT.wait_until_ready()
    with suppress(Exception):
        await migrate_ticket_channel_staff_overwrites()
    with suppress(Exception):
        await migrate_ticket_channel_owner_overwrites()
    with suppress(Exception):
        await migrate_strip_total_spent_from_ticket_headers()


async def migrate_strip_total_spent_from_ticket_headers() -> None:
    """One-time migration: remove Total Spent fields from existing ticket header embeds (edit in place)."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return

    mig_key = "ticket_header:strip_total_spent_v1"
    try:
        dbm = _migrations_load()
        done = str((dbm.get("done") or {}).get(mig_key) or "").strip() if isinstance(dbm, dict) else ""
        if done:
            return
    except Exception:
        dbm = {}

    # Gather open tickets with header_message_id (avoid scanning channel histories).
    targets: list[tuple[str, int, int]] = []  # (ticket_type, channel_id, header_message_id)
    async with _INDEX_LOCK:
        idx = _index_load()
        for _tid, rec in _ticket_iter(idx):
            if not _ticket_is_open(rec):
                continue
            ttype = str(rec.get("ticket_type") or "").strip().lower()
            if ttype not in {"cancellation", "billing", "member_welcome", "no_whop_link"}:
                continue
            ch_id = _as_int(rec.get("channel_id"))
            mid = _as_int(rec.get("header_message_id"))
            if ch_id > 0 and mid > 0:
                targets.append((ttype, int(ch_id), int(mid)))

    bot_id = int(getattr(getattr(_BOT, "user", None), "id", 0) or 0)
    view = _CONTROLS_VIEW or SupportTicketControlsView()
    scanned = 0
    edited = 0
    skipped = 0
    failed = 0

    def _strip(e: discord.Embed) -> tuple[discord.Embed, bool]:
        # Use dict roundtrip to preserve everything (author/footer/thumbnail/etc).
        try:
            d = e.to_dict()
        except Exception:
            return (e, False)
        fields = d.get("fields") if isinstance(d, dict) else None
        if not isinstance(fields, list) or not fields:
            return (e, False)
        kept: list[dict] = []
        removed_any = False
        for f in fields:
            if not isinstance(f, dict):
                continue
            nm = str(f.get("name") or "").strip()
            if nm.strip().lower().startswith("total spent"):
                removed_any = True
                continue
            kept.append(f)
        if not removed_any:
            return (e, False)
        d["fields"] = kept
        try:
            return (discord.Embed.from_dict(d), True)
        except Exception:
            return (e, False)

    for _ttype, ch_id, mid in targets:
        scanned += 1
        try:
            ch = guild.get_channel(int(ch_id))
            if not isinstance(ch, discord.TextChannel):
                skipped += 1
                continue
            msg = await ch.fetch_message(int(mid))
            if int(getattr(getattr(msg, "author", None), "id", 0) or 0) != bot_id:
                skipped += 1
                continue
            if not getattr(msg, "embeds", None):
                skipped += 1
                continue
            e0 = msg.embeds[0]
            if not isinstance(e0, discord.Embed):
                skipped += 1
                continue
            e2, changed = _strip(e0)
            if not changed:
                skipped += 1
                continue
            with suppress(Exception):
                await msg.edit(embed=e2, view=view, allowed_mentions=discord.AllowedMentions.none())
            edited += 1
        except Exception:
            failed += 1

    # Mark migration done
    try:
        out = dbm if isinstance(dbm, dict) else {}
        if not isinstance(out.get("done"), dict):
            out["done"] = {}
        out["done"][mig_key] = _now_iso()
        out.setdefault("stats", {})[mig_key] = {
            "scanned": scanned,
            "edited": edited,
            "skipped": skipped,
            "failed": failed,
        }
        _migrations_save(out)
    except Exception:
        pass

    with suppress(Exception):
        await _log(f"🧾 support_tickets: migration strip_total_spent scanned={scanned} edited={edited} skipped={skipped} failed={failed}")


def _topic_is_support_ticket(topic: object) -> bool:
    try:
        return "rschecker_support_ticket" in str(topic or "").lower()
    except Exception:
        return False


def _ticket_owner_id_from_topic(topic: object) -> int:
    try:
        m = re.search(r"(?im)^\s*user_id\s*=\s*(\d{17,19})\s*$", str(topic or ""))
        return int(m.group(1)) if m else 0
    except Exception:
        return 0


async def migrate_ticket_channel_owner_overwrites() -> None:
    """Best-effort migration to align owner access with config.

    This is restart-safe, keyed by the direction:
    - `owner_overwrites:disable_owner` when owner visibility is disabled
    - `owner_overwrites:enable_owner` when owner visibility is enabled
    """
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return

    enable_owner = bool(cfg.include_ticket_owner_in_channel)
    mig_key = "owner_overwrites:enable_owner" if enable_owner else "owner_overwrites:disable_owner"
    try:
        db = _migrations_load()
        done = str((db.get("done") or {}).get(mig_key) or "").strip()
        if done:
            return
    except Exception:
        db = {}

    cat_ids = [
        int(cfg.billing_category_id or 0),
        int(cfg.free_pass_category_id or 0),
        int(cfg.cancellation_category_id or 0),
        int(cfg.member_welcome_category_id or 0),
        int(cfg.no_whop_link_category_id or 0) if bool(getattr(cfg, "no_whop_link_enabled", False)) else 0,
    ]
    # no_whop_link category may be configured by name (id=0); resolve it ONLY when enabled.
    if bool(getattr(cfg, "no_whop_link_enabled", False)):
        with suppress(Exception):
            nw_id = await _get_or_create_no_whop_link_category_id(guild=guild)
            if int(nw_id or 0) > 0:
                cat_ids.append(int(nw_id))
    # member_welcome category may be configured by name (id=0); resolve it best-effort.
    with suppress(Exception):
        mw_id = await _get_or_create_member_welcome_category_id(guild=guild)
        if int(mw_id or 0) > 0:
            cat_ids.append(int(mw_id))
    cat_ids = [int(x) for x in cat_ids if int(x) > 0]

    scanned = 0
    removed = 0
    added = 0
    header_scrubbed = 0
    header_restored = 0
    failed = 0

    for cid in cat_ids:
        cat = guild.get_channel(int(cid))
        if not isinstance(cat, discord.CategoryChannel):
            continue
        for ch in list(getattr(cat, "channels", []) or []):
            if not isinstance(ch, discord.TextChannel):
                continue
            if not _topic_is_support_ticket(getattr(ch, "topic", "") or ""):
                continue
            scanned += 1
            try:
                uid = _ticket_owner_id_from_topic(getattr(ch, "topic", "") or "")
                if uid > 0:
                    mobj = guild.get_member(int(uid))
                    if not isinstance(mobj, discord.Member):
                        with suppress(Exception):
                            mobj = await guild.fetch_member(int(uid))
                    if isinstance(mobj, discord.Member):
                        has_ow = False
                        with suppress(Exception):
                            has_ow = bool(mobj in (getattr(ch, "overwrites", {}) or {}))
                        if enable_owner:
                            if not has_ow:
                                await ch.set_permissions(
                                    mobj,
                                    view_channel=True,
                                    send_messages=True,
                                    read_message_history=True,
                                    attach_files=True,
                                    embed_links=True,
                                    add_reactions=True,
                                    reason="RSCheckerbot: enable ticket owner visibility",
                                )
                                added += 1
                        else:
                            if has_ow:
                                await ch.set_permissions(mobj, overwrite=None, reason="RSCheckerbot: disable ticket owner visibility")
                                removed += 1

                # Scrub header message content (remove old pings) if we know the header id.
                rec0 = await get_ticket_record_for_channel_id(int(ch.id))
                mid0 = int((rec0 or {}).get("header_message_id") or 0) if isinstance(rec0, dict) else 0
                if mid0 > 0:
                    with suppress(Exception):
                        msg = await ch.fetch_message(int(mid0))
                        if msg:
                            content = str(getattr(msg, "content", "") or "").strip()
                            if enable_owner:
                                ping = _ticket_ping_content(owner_id=int(uid), mention_owner=True, mention_staff=True).strip()
                                if ping and content != ping:
                                    await msg.edit(content=ping)
                                    header_restored += 1
                            else:
                                if content:
                                    await msg.edit(content="")
                                    header_scrubbed += 1
            except Exception:
                failed += 1

    if enable_owner:
        await _log(f"🧩 support_tickets: owner_overwrites_enable scanned={scanned} owner_overwrites_added={added} header_restored={header_restored} failed={failed}")
    else:
        await _log(f"🧩 support_tickets: owner_overwrites_disable scanned={scanned} owner_overwrites_removed={removed} header_scrubbed={header_scrubbed} failed={failed}")
    try:
        db2 = _migrations_load()
        done_map = db2.get("done") if isinstance(db2.get("done"), dict) else {}
        if not isinstance(done_map, dict):
            done_map = {}
        done_map[mig_key] = _now_iso()
        db2["done"] = done_map
        _migrations_save(db2)
    except Exception:
        return


async def migrate_ticket_channel_staff_overwrites() -> None:
    """One-time best-effort: remove legacy staff-role overwrites from existing ticket channels.

    This is config-driven:
    - `support_tickets.permissions.legacy_staff_role_ids`: role overwrites to remove
    - `support_tickets.permissions.staff_role_ids`: role overwrites to ensure exist
    """
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg:
        return
    legacy = [int(x) for x in (cfg.legacy_staff_role_ids or []) if int(x) > 0]
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return

    # Restart-safe: run once per config shape (ensures staff can view channels).
    mig_key = f"staff_overwrites:{','.join([str(x) for x in legacy])}->{','.join([str(x) for x in (cfg.staff_role_ids or [])])}"
    try:
        db = _migrations_load()
        done = str((db.get("done") or {}).get(mig_key) or "").strip()
        if done:
            return
    except Exception:
        db = {}

    cat_ids = [
        int(cfg.billing_category_id or 0),
        int(cfg.free_pass_category_id or 0),
        int(cfg.cancellation_category_id or 0),
        int(cfg.member_welcome_category_id or 0),
        int(cfg.no_whop_link_category_id or 0) if bool(getattr(cfg, "no_whop_link_enabled", False)) else 0,
    ]
    if bool(getattr(cfg, "no_whop_link_enabled", False)):
        with suppress(Exception):
            nw_id = await _get_or_create_no_whop_link_category_id(guild=guild)
            if int(nw_id or 0) > 0:
                cat_ids.append(int(nw_id))
    with suppress(Exception):
        mw_id = await _get_or_create_member_welcome_category_id(guild=guild)
        if int(mw_id or 0) > 0:
            cat_ids.append(int(mw_id))
    cat_ids = [int(x) for x in cat_ids if int(x) > 0]
    if not cat_ids:
        return

    scanned = 0
    touched = 0
    removed = 0
    failed = 0

    for cid in cat_ids:
        cat = guild.get_channel(int(cid))
        if not isinstance(cat, discord.CategoryChannel):
            continue
        for ch in list(getattr(cat, "channels", []) or []):
            if not isinstance(ch, discord.TextChannel):
                continue
            if not _topic_is_support_ticket(getattr(ch, "topic", "") or ""):
                continue
            scanned += 1
            try:
                did_any = False
                for rid in legacy:
                    role = guild.get_role(int(rid))
                    if not role:
                        continue
                    has_ow = False
                    with suppress(Exception):
                        has_ow = bool(role in (getattr(ch, "overwrites", {}) or {}))
                    if not has_ow:
                        continue
                    await ch.set_permissions(role, overwrite=None, reason="RSCheckerbot: migrate staff role overwrites")
                    removed += 1
                    did_any = True
                # Ensure current staff roles can view channel (idempotent).
                await _ensure_staff_roles_can_view_channel(guild=guild, channel=ch, staff_role_ids=cfg.staff_role_ids)
                if did_any:
                    touched += 1
            except Exception:
                failed += 1

    await _log(f"🧩 support_tickets: staff_overwrites_migrate scanned={scanned} channels_changed={touched} overwrites_removed={removed} failed={failed}")
    try:
        db2 = _migrations_load()
        done_map = db2.get("done") if isinstance(db2.get("done"), dict) else {}
        if not isinstance(done_map, dict):
            done_map = {}
        done_map[mig_key] = _now_iso()
        db2["done"] = done_map
        _migrations_save(db2)
    except Exception:
        return


def _cfg() -> SupportTicketConfig | None:
    return _CFG


def _index_load() -> dict:
    raw = _load_json(INDEX_PATH)
    if not isinstance(raw, dict):
        raw = {}
    tickets = raw.get("tickets")
    if not isinstance(tickets, dict):
        raw["tickets"] = {}
    raw.setdefault("version", 1)
    return raw


def _index_save(db: dict) -> None:
    _save_json(INDEX_PATH, db if isinstance(db, dict) else {"version": 1, "tickets": {}})


def _ticket_iter(db: dict) -> list[tuple[str, dict]]:
    tickets = db.get("tickets") if isinstance(db.get("tickets"), dict) else {}
    out: list[tuple[str, dict]] = []
    for tid, rec in tickets.items():
        if not isinstance(rec, dict):
            continue
        out.append((str(tid), rec))
    return out


def _startup_template(ticket_type: str) -> str:
    cfg = _cfg()
    if not cfg:
        return ""
    key = str(ticket_type or "").strip().lower()
    tmpl = (cfg.startup_templates or {}).get(key) if isinstance(cfg.startup_templates, dict) else ""
    return str(tmpl or "").strip()


def _header_template(ticket_type: str) -> str:
    cfg = _cfg()
    if not cfg:
        return ""
    key = str(ticket_type or "").strip().lower()
    hm = cfg.header_templates if isinstance(cfg.header_templates, dict) else {}
    tmpl = hm.get(key) or hm.get("default") or ""
    return str(tmpl or "").strip()


def _resolution_followup_template(ticket_type: str) -> str:
    cfg = _cfg()
    if not cfg:
        return ""
    key = str(ticket_type or "").strip().lower()
    tmpl = (cfg.resolution_followup_templates or {}).get(key) if isinstance(cfg.resolution_followup_templates, dict) else ""
    return str(tmpl or "").strip()


async def _startup_has_human_activity_since_creation(
    *,
    ch: discord.TextChannel,
    created_at: datetime,
    limit: int,
) -> bool:
    """Return True if any non-bot message exists since creation."""
    with suppress(Exception):
        async for m in ch.history(limit=int(limit), oldest_first=False):
            if not m:
                continue
            try:
                if (m.created_at or _now_utc()) < created_at:
                    # history is newest-first, so once we cross creation time we can stop
                    break
            except Exception:
                pass
            if getattr(getattr(m, "author", None), "bot", False):
                continue
            return True
    return False


def _chunk_message_content(content: str, *, max_len: int = 1990) -> list[str]:
    """Split long startup content into safe chunks (prefers newline boundaries)."""
    s = str(content or "")
    if not s:
        return []
    n = max(200, int(max_len or 1990))
    if len(s) <= n:
        return [s]
    out: list[str] = []
    cur = s
    while cur:
        if len(cur) <= n:
            out.append(cur)
            break
        cut = cur.rfind("\n\n", 0, n)
        if cut < 0:
            cut = cur.rfind("\n", 0, n)
        if cut < 0:
            cut = n
        part = cur[:cut].rstrip()
        if part:
            out.append(part)
        cur = cur[cut:].lstrip("\n")
    return out


async def sweep_startup_messages() -> None:
    """Pattern A: stateless sweeper loop for 5-minute startup acknowledgement."""
    if not _ensure_cfg_loaded():
        return
    cfg = _cfg()
    if not cfg or not cfg.startup_enabled or not _BOT:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not guild:
        return

    now = _now_utc()

    # Copy candidates under lock to avoid holding lock across awaits.
    candidates: list[tuple[str, dict]] = []
    async with _INDEX_LOCK:
        db = _index_load()
        for tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            # already sent or skipped
            if str(rec.get("startup_sent_at_iso") or "").strip():
                continue
            if str(rec.get("startup_skipped_at_iso") or "").strip():
                continue
            candidates.append((tid, dict(rec)))

    for tid, rec in candidates:
        created_dt = _parse_iso(str(rec.get("created_at_iso") or "")) or now
        if (now - created_dt) < timedelta(seconds=int(cfg.startup_delay_seconds)):
            continue

        ch_id = _as_int(rec.get("channel_id"))
        uid = _as_int(rec.get("user_id"))
        ttype = str(rec.get("ticket_type") or "").strip().lower()

        ch = guild.get_channel(int(ch_id)) if ch_id else None
        if not isinstance(ch, discord.TextChannel):
            # Channel missing -> close record
            async with _INDEX_LOCK:
                db2 = _index_load()
                found = _ticket_by_channel_id(db2, int(ch_id))
                if found:
                    tid2, rec2 = found
                    if _ticket_is_open(rec2):
                        rec2["status"] = "CLOSED"
                        rec2["close_reason"] = "channel_missing"
                        rec2["closed_at_iso"] = _now_iso()
                        db2["tickets"][tid2] = rec2  # type: ignore[index]
                        _index_save(db2)
            continue

        # Guard: for most tickets we skip if any human spoke since creation.
        # For `no_whop_link`, we DO send (the guide is the point of the ticket).
        if ttype != "no_whop_link":
            spoke = await _startup_has_human_activity_since_creation(
                ch=ch,
                created_at=created_dt,
                limit=int(cfg.startup_recent_history_limit),
            )
            if spoke:
                async with _INDEX_LOCK:
                    db2 = _index_load()
                    found = _ticket_by_channel_id(db2, int(ch_id))
                    if found:
                        tid2, rec2 = found
                        if _ticket_is_open(rec2) and (not str(rec2.get("startup_sent_at_iso") or "").strip()):
                            rec2["startup_skipped_at_iso"] = _now_iso()
                            db2["tickets"][tid2] = rec2  # type: ignore[index]
                            _index_save(db2)
                continue

        tmpl = _startup_template(ttype)
        if not tmpl:
            continue
        mention = f"<@{int(uid)}>" if uid else ""
        staff_mention = _support_ping_role_mention()
        content = (
            tmpl.replace("{mention}", mention)
            .replace("{staff}", staff_mention)
            .replace("{staff_mention}", staff_mention)
            .strip()
        )
        if not content:
            continue

        ok = True
        try:
            allow = discord.AllowedMentions(
                users=True,
                roles=bool(staff_mention and ("{staff" in tmpl or "<@&" in content)),
                everyone=False,
            )
            for part in _chunk_message_content(content, max_len=1990):
                await ch.send(content=part, allowed_mentions=allow)
        except Exception as ex:
            ok = False
            await _log(f"❌ support_tickets: startup_message_send_failed type={ttype} ch={int(ch.id)} err={str(ex)[:180]}")

        if ok:
            async with _INDEX_LOCK:
                db2 = _index_load()
                found = _ticket_by_channel_id(db2, int(ch_id))
                if found:
                    tid2, rec2 = found
                    if _ticket_is_open(rec2) and (not str(rec2.get("startup_sent_at_iso") or "").strip()):
                        rec2["startup_sent_at_iso"] = _now_iso()
                        db2["tickets"][tid2] = rec2  # type: ignore[index]
                        _index_save(db2)


def _ticket_by_channel_id(db: dict, channel_id: int) -> tuple[str, dict] | None:
    for tid, rec in _ticket_iter(db):
        try:
            if _as_int(rec.get("channel_id")) == int(channel_id):
                return (tid, rec)
        except Exception:
            continue
    return None


def _ticket_is_open(rec: dict) -> bool:
    return str(rec.get("status") or "").strip().upper() == "OPEN"


def _cooldown_seconds_for(ticket_type: str) -> int:
    c = _cfg()
    if not c:
        return 0
    t = str(ticket_type or "").strip().lower()
    if t == "free_pass":
        return int(c.cooldown_free_pass_seconds)
    if t == "free_pass_welcome":
        return int(c.cooldown_free_pass_welcome_seconds)
    if t == "billing":
        return int(c.cooldown_billing_seconds)
    if t == "no_whop_link":
        return int(c.no_whop_link_cooldown_seconds)
    if t == "member_welcome":
        return int(c.cooldown_member_welcome_seconds)
    return int(c.cooldown_cancellation_seconds)


def _make_ticket_id() -> str:
    return uuid.uuid4().hex[:12]


def _ticket_channel_name(ticket_type: str, member: discord.abc.User) -> str:
    t = str(ticket_type or "").strip().lower()
    prefix = "ticket"
    if t == "cancellation":
        prefix = "cancel"
    elif t == "billing":
        prefix = "billing"
    elif t == "free_pass":
        prefix = "freepass"
    elif t == "free_pass_welcome":
        prefix = "freepass"
    elif t == "member_welcome":
        prefix = "welcome"
    elif t == "no_whop_link":
        prefix = "nowhop"

    uname = str(getattr(member, "display_name", "") or getattr(member, "name", "") or "user")
    uname = _slug_channel_name(uname, max_len=20) or "user"
    last4 = str(int(getattr(member, "id", 0) or 0))[-4:] if str(getattr(member, "id", "")).isdigit() else "0000"
    return f"{prefix}-{uname}-{last4}"


def _ticket_topic(*, ticket_id: str, ticket_type: str, user_id: int, fingerprint: str) -> str:
    tid = str(ticket_id or "").strip()
    fp = str(fingerprint or "").strip()
    return (
        "rschecker_support_ticket\n"
        f"ticket_id={tid}\n"
        f"ticket_type={str(ticket_type or '').strip()}\n"
        f"user_id={int(user_id)}\n"
        f"fingerprint={fp}\n"
    ).strip()


def _ticket_case_key(*, ticket_id: str) -> str:
    return f"rschecker_support_ticket:{str(ticket_id or '').strip()}"


def _support_ping_role_mention() -> str:
    """Single support ping role mention (first staff_role_id)."""
    cfg = _cfg()
    if not cfg:
        return ""
    rid = 0
    with suppress(Exception):
        rid = next((int(x) for x in (cfg.staff_role_ids or []) if int(x) > 0), 0)
    return f"<@&{int(rid)}>" if int(rid or 0) > 0 else ""


def _ticket_ping_content(*, owner_id: int, mention_owner: bool, mention_staff: bool) -> str:
    """Ticket header ping content (member + support role).

    Note: mentions are independent from channel permissions. We can ping the member even if they are not added to
    the channel (used for no_whop_link while testing).
    """
    uid = int(owner_id or 0)
    owner_mention = f"<@{uid}>" if (uid and bool(mention_owner)) else ""
    role_mention = _support_ping_role_mention() if bool(mention_staff) else ""
    return " ".join([x for x in [owner_mention, role_mention] if str(x or "").strip()])


def _ticket_header_content(*, ticket_type: str, owner_id: int, mention_owner: bool, mention_staff: bool) -> str:
    """Ticket header message content (human-friendly, template-driven)."""
    tmpl = _header_template(ticket_type)
    if not tmpl:
        # Fallback to old behavior.
        return _ticket_ping_content(owner_id=int(owner_id), mention_owner=bool(mention_owner), mention_staff=bool(mention_staff))
    uid = int(owner_id or 0)
    member_txt = f"<@{uid}>" if (uid and bool(mention_owner)) else ""
    staff_txt = _support_ping_role_mention() if bool(mention_staff) else ""
    # Allow either placeholder spelling.
    out = str(tmpl).replace("{member}", member_txt).replace("{mention}", member_txt)
    out = out.replace("{staff}", staff_txt).replace("{staff_mention}", staff_txt)
    return out.strip()


async def _ensure_ticket_header_message(
    *,
    ticket_type: str,
    channel: discord.TextChannel,
    owner_id: int,
    include_owner: bool,
    mention_owner: bool = False,
    mention_staff: bool = False,
    preview_embed: discord.Embed,
    header_message_id: int = 0,
) -> int:
    """Ensure a single 'header' message exists (ping + embed + buttons), and keep it updated."""
    if not _ensure_cfg_loaded() or not _BOT or not isinstance(channel, discord.TextChannel):
        return 0

    view = _CONTROLS_VIEW or SupportTicketControlsView()
    ping_owner = bool(mention_owner) or bool(include_owner)
    ping_staff = bool(mention_staff) or bool(include_owner)
    content = _ticket_header_content(ticket_type=str(ticket_type or ""), owner_id=int(owner_id), mention_owner=ping_owner, mention_staff=ping_staff)
    bot_id = int(getattr(getattr(_BOT, "user", None), "id", 0) or 0)
    allow_roles = "<@&" in str(content or "")
    allow_users = "<@" in str(content or "")
    allowed_mentions = discord.AllowedMentions(users=bool(allow_users), roles=bool(allow_roles), everyone=False)

    # Prefer editing the stored message id.
    mid = int(header_message_id or 0)
    if mid > 0:
        try:
            msg = await channel.fetch_message(int(mid))
        except Exception:
            msg = None
        if msg is not None and int(getattr(getattr(msg, "author", None), "id", 0) or 0) == bot_id:
            with suppress(Exception):
                await msg.edit(content=content, embed=preview_embed, view=view, allowed_mentions=allowed_mentions)
                return int(getattr(msg, "id", 0) or 0)

    # Fallback: find an early bot-authored embed message in channel history.
    embed_msg: discord.Message | None = None
    ping_only_msg: discord.Message | None = None
    try:
        async for m in channel.history(limit=30, oldest_first=True):
            if int(getattr(getattr(m, "author", None), "id", 0) or 0) != bot_id:
                continue
            has_embed = bool(getattr(m, "embeds", None) or [])
            if has_embed and embed_msg is None:
                embed_msg = m
            if (not has_embed) and (not getattr(m, "attachments", None)) and content and str(getattr(m, "content", "") or "").strip() == content:
                ping_only_msg = m
    except Exception:
        embed_msg = None
        ping_only_msg = None

    if embed_msg is not None:
        with suppress(Exception):
            await embed_msg.edit(content=content, embed=preview_embed, view=view, allowed_mentions=allowed_mentions)
        # Best-effort cleanup: delete the separate ping-only message if it exists.
        if ping_only_msg is not None and int(getattr(ping_only_msg, "id", 0) or 0) != int(getattr(embed_msg, "id", 0) or 0):
            with suppress(Exception):
                await ping_only_msg.delete()
        return int(getattr(embed_msg, "id", 0) or 0)

    # Nothing to edit: send a fresh header.
    try:
        sent = await channel.send(
            content=content,
            embed=preview_embed,
            view=view,
            allowed_mentions=allowed_mentions,
        )
        return int(getattr(sent, "id", 0) or 0)
    except Exception:
        return 0


async def _ensure_staff_roles_can_view_channel(
    *,
    guild: discord.Guild,
    channel: discord.TextChannel,
    staff_role_ids: list[int],
) -> None:
    """Best-effort: ensure staff roles in config can view the ticket channel."""
    if not isinstance(guild, discord.Guild) or not isinstance(channel, discord.TextChannel):
        return
    for rid in list(dict.fromkeys([int(x) for x in (staff_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if not role:
            await _log(f"⚠️ support_tickets: staff_role_id not found in guild (role_id={rid})")
            continue
        with suppress(Exception):
            perms = channel.permissions_for(role)
            if bool(getattr(perms, "view_channel", False)):
                continue
        with suppress(Exception):
            await channel.set_permissions(
                role,
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                reason="RSCheckerbot: ensure support role access",
            )


def _ticket_category_ids_for_audit() -> set[int]:
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return set()
    ids = {
        int(cfg.cancellation_category_id or 0),
        int(cfg.billing_category_id or 0),
        int(cfg.free_pass_category_id or 0),
        int(cfg.member_welcome_category_id or 0),
    }
    if bool(cfg.audit_include_transcript_category):
        ids.add(int(cfg.transcript_category_id or 0))
    return {int(x) for x in ids if int(x) > 0}


def _is_ticket_category_channel_for_audit(ch: discord.abc.GuildChannel | None) -> bool:
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return False
    if not ch:
        return False
    cat_ids = _ticket_category_ids_for_audit()
    if not cat_ids:
        return False
    # Category channel itself
    if isinstance(ch, discord.CategoryChannel):
        return int(ch.id) in cat_ids
    # Text/other channels: match by category_id
    cid = int(getattr(ch, "category_id", 0) or 0)
    return cid in cat_ids


async def _get_or_create_audit_channel(*, guild: discord.Guild) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled or not _BOT:
        return None
    if not isinstance(guild, discord.Guild):
        return None

    cid = int(cfg.audit_channel_id or 0)
    if cid > 0:
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            return ch
        with suppress(Exception):
            fetched = await _BOT.fetch_channel(cid)
            return fetched if isinstance(fetched, discord.TextChannel) else None

    name = str(cfg.audit_channel_name or "tickets-logs").strip() or "tickets-logs"
    with suppress(Exception):
        for ch in list(getattr(guild, "text_channels", []) or []):
            if isinstance(ch, discord.TextChannel) and str(getattr(ch, "name", "") or "") == name:
                return ch

    me = getattr(guild, "me", None) or guild.get_member(int(getattr(getattr(_BOT, "user", None), "id", 0) or 0))
    if not (me and getattr(me, "guild_permissions", None) and bool(getattr(me.guild_permissions, "manage_channels", False))):
        return None
    with suppress(Exception):
        created = await guild.create_text_channel(name=name, reason="RSCheckerbot: ticket audit logs")
        return created if isinstance(created, discord.TextChannel) else None
    return None


def _clip(s: object, n: int) -> str:
    out = str(s or "")
    out = out.replace("```", "`\u200b``")  # avoid breaking code blocks
    return (out[:n] + "…") if len(out) > n else out


async def _audit_send(
    *,
    guild_id: int,
    embed: discord.Embed,
) -> None:
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    g = _BOT.get_guild(int(guild_id or 0))
    if not isinstance(g, discord.Guild):
        return
    ch = await _get_or_create_audit_channel(guild=g)
    if not isinstance(ch, discord.TextChannel):
        return
    with suppress(Exception):
        await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none(), silent=True)


async def _audit_ticket_deduped(
    *,
    ticket_type: str,
    owner: discord.Member,
    existing_channel: discord.TextChannel,
    ticket_id: str,
    fingerprint: str,
    reference_jump_url: str = "",
) -> None:
    """Log a dedupe hit to tickets-logs (do not spam the ticket channel)."""
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    try:
        g = getattr(existing_channel, "guild", None)
        gid = int(getattr(g, "id", 0) or 0)
    except Exception:
        gid = int(cfg.guild_id or 0)
    if gid <= 0:
        gid = int(cfg.guild_id or 0)
    e = discord.Embed(title="Ticket Deduped", color=0x5865F2, timestamp=_now_utc())
    e.add_field(name="Type", value=str(ticket_type or "—")[:1024], inline=True)
    e.add_field(name="Member", value=f"{getattr(owner, 'mention', '')} (`{int(owner.id)}`)", inline=True)
    e.add_field(name="Existing Ticket", value=f"<#{int(existing_channel.id)}>", inline=True)
    if ticket_id:
        e.add_field(name="Ticket ID", value=f"`{str(ticket_id)[:128]}`", inline=True)
    if fingerprint:
        e.add_field(name="Fingerprint", value=f"`{str(fingerprint)[:256]}`", inline=False)
    if reference_jump_url:
        e.add_field(name="Source", value=_embed_link("View Full Log", str(reference_jump_url)), inline=False)
    await _audit_send(guild_id=gid, embed=e)


async def _audit_ticket_resolved(
    *,
    ticket_type: str,
    owner: discord.Member,
    ticket_channel: discord.TextChannel,
    resolution_event: str,
    reference_jump_url: str = "",
) -> None:
    """Log a resolution signal to tickets-logs (with ticket reference)."""
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    try:
        g = getattr(ticket_channel, "guild", None)
        gid = int(getattr(g, "id", 0) or 0)
    except Exception:
        gid = int(cfg.guild_id or 0)
    if gid <= 0:
        gid = int(cfg.guild_id or 0)
    e = discord.Embed(title="Ticket Resolved (Follow-up Posted)", color=0x57F287, timestamp=_now_utc())
    e.add_field(name="Type", value=str(ticket_type or "—")[:1024], inline=True)
    e.add_field(name="Member", value=f"{getattr(owner, 'mention', '')} (`{int(owner.id)}`)", inline=True)
    e.add_field(name="Ticket", value=f"<#{int(ticket_channel.id)}>", inline=True)
    if resolution_event:
        e.add_field(name="Trigger", value=str(resolution_event)[:1024], inline=True)
    if reference_jump_url:
        e.add_field(name="Source", value=_embed_link("View Full Log", str(reference_jump_url)), inline=False)
    await _audit_send(guild_id=gid, embed=e)


async def _audit_ticket_suppressed_cooldown(
    *,
    ticket_type: str,
    owner: discord.Member,
    last_ticket_id: str,
    last_channel_id: int,
    last_channel_name: str,
    last_closed_at_iso: str,
    cooldown_seconds: int,
    fingerprint: str,
    reference_jump_url: str = "",
) -> None:
    """Log a cooldown suppression to tickets-logs (prevents spam/reopen loops)."""
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    gid = int(cfg.guild_id or 0)
    if gid <= 0:
        return
    e = discord.Embed(title="Ticket Suppressed (Cooldown)", color=0xFEE75C, timestamp=_now_utc())
    e.add_field(name="Type", value=str(ticket_type or "—")[:1024], inline=True)
    e.add_field(name="Member", value=f"{getattr(owner, 'mention', '')} (`{int(owner.id)}`)", inline=True)
    if last_ticket_id:
        e.add_field(name="Last Ticket ID", value=f"`{str(last_ticket_id)[:128]}`", inline=True)
    if last_channel_id:
        ch_label = f"#{str(last_channel_name or '').strip()}" if str(last_channel_name or "").strip() else "unknown"
        e.add_field(name="Last Ticket", value=f"{ch_label} (`{int(last_channel_id)}`)", inline=False)
    if last_closed_at_iso:
        e.add_field(name="Last Closed At", value=str(last_closed_at_iso)[:1024], inline=False)
    e.add_field(name="Cooldown", value=f"`{int(cooldown_seconds)}`s", inline=True)
    if fingerprint:
        e.add_field(name="Fingerprint", value=f"`{str(fingerprint)[:256]}`", inline=False)
    if reference_jump_url:
        e.add_field(name="Source", value=_embed_link("View Full Log", str(reference_jump_url)), inline=False)
    await _audit_send(guild_id=gid, embed=e)


async def audit_message_create(message: discord.Message) -> None:
    """Audit: message sent in ticket categories."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    if not message or not getattr(message, "guild", None):
        return
    if int(message.guild.id) != int(cfg.guild_id):
        return
    ch = getattr(message, "channel", None)
    if not isinstance(ch, discord.TextChannel):
        return
    # Avoid logging the log channel itself
    if int(getattr(ch, "id", 0) or 0) == int(cfg.audit_channel_id or 0):
        return
    if not _is_ticket_category_channel_for_audit(ch):
        return

    author = getattr(message, "author", None)
    author_id = int(getattr(author, "id", 0) or 0)
    author_mention = str(getattr(author, "mention", "") or f"`{author_id}`")
    e = discord.Embed(title="Message Sent", color=0x5865F2, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"<#{int(ch.id)}>", inline=True)
    e.add_field(name="Author", value=f"{author_mention} (`{author_id}`)", inline=True)
    e.add_field(name="Message ID", value=f"`{int(getattr(message, 'id', 0) or 0)}`", inline=True)
    content = _clip(getattr(message, "content", "") or "", 900).strip()
    if content:
        e.add_field(name="Content", value=content[:1024], inline=False)
    atts = list(getattr(message, "attachments", []) or [])
    if atts:
        names = ", ".join(_clip(getattr(a, "filename", "") or "file", 64) for a in atts[:6])
        e.add_field(name="Attachments", value=_clip(names, 1024) or "—", inline=False)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_message_delete(message: discord.Message) -> None:
    """Audit: cached message delete in ticket categories."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    if not message or not getattr(message, "guild", None):
        return
    if int(message.guild.id) != int(cfg.guild_id):
        return
    ch = getattr(message, "channel", None)
    if not isinstance(ch, discord.TextChannel):
        return
    if int(getattr(ch, "id", 0) or 0) == int(cfg.audit_channel_id or 0):
        return
    if not _is_ticket_category_channel_for_audit(ch):
        return

    author = getattr(message, "author", None)
    author_id = int(getattr(author, "id", 0) or 0)
    author_mention = str(getattr(author, "mention", "") or f"`{author_id}`")
    e = discord.Embed(title="Message Deleted", color=0xED4245, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"<#{int(ch.id)}>", inline=True)
    e.add_field(name="Author", value=f"{author_mention} (`{author_id}`)", inline=True)
    e.add_field(name="Message ID", value=f"`{int(getattr(message, 'id', 0) or 0)}`", inline=True)
    content = _clip(getattr(message, "content", "") or "", 900).strip()
    if content:
        e.add_field(name="Content", value=content[:1024], inline=False)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_raw_message_delete(payload: object) -> None:
    """Audit: raw message delete (content may be unavailable)."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    gid = int(getattr(payload, "guild_id", 0) or 0)
    if not gid or int(gid) != int(cfg.guild_id):
        return
    ch_id = int(getattr(payload, "channel_id", 0) or 0)
    msg_id = int(getattr(payload, "message_id", 0) or 0)
    g = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(g, discord.Guild):
        return
    ch = g.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, discord.TextChannel):
        return
    if int(getattr(ch, "id", 0) or 0) == int(cfg.audit_channel_id or 0):
        return
    if not _is_ticket_category_channel_for_audit(ch):
        return
    e = discord.Embed(title="Message Deleted", color=0xED4245, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"<#{int(ch.id)}>", inline=True)
    e.add_field(name="Message ID", value=f"`{int(msg_id)}`", inline=True)
    e.add_field(name="Note", value="Message content not cached.", inline=True)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_message_edit(before: discord.Message, after: discord.Message) -> None:
    """Audit: cached message edit in ticket categories."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    msg = after or before
    if not msg or not getattr(msg, "guild", None):
        return
    if int(msg.guild.id) != int(cfg.guild_id):
        return
    ch = getattr(msg, "channel", None)
    if not isinstance(ch, discord.TextChannel):
        return
    if int(getattr(ch, "id", 0) or 0) == int(cfg.audit_channel_id or 0):
        return
    if not _is_ticket_category_channel_for_audit(ch):
        return
    btxt = _clip(getattr(before, "content", "") or "", 700).strip()
    atxt = _clip(getattr(after, "content", "") or "", 700).strip()
    if btxt == atxt:
        return
    author = getattr(msg, "author", None)
    author_id = int(getattr(author, "id", 0) or 0)
    author_mention = str(getattr(author, "mention", "") or f"`{author_id}`")
    e = discord.Embed(title="Message Edited", color=0xFEE75C, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"<#{int(ch.id)}>", inline=True)
    e.add_field(name="Author", value=f"{author_mention} (`{author_id}`)", inline=True)
    e.add_field(name="Message ID", value=f"`{int(getattr(msg, 'id', 0) or 0)}`", inline=True)
    if btxt:
        e.add_field(name="Before", value=btxt[:1024], inline=False)
    if atxt:
        e.add_field(name="After", value=atxt[:1024], inline=False)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_raw_message_edit(payload: object) -> None:
    """Audit: raw message edit (content may be unavailable)."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    gid = int(getattr(payload, "guild_id", 0) or 0)
    if not gid or int(gid) != int(cfg.guild_id):
        return
    ch_id = int(getattr(payload, "channel_id", 0) or 0)
    msg_id = int(getattr(payload, "message_id", 0) or 0)
    g = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(g, discord.Guild):
        return
    ch = g.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, discord.TextChannel):
        return
    if int(getattr(ch, "id", 0) or 0) == int(cfg.audit_channel_id or 0):
        return
    if not _is_ticket_category_channel_for_audit(ch):
        return
    e = discord.Embed(title="Message Edited", color=0xFEE75C, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"<#{int(ch.id)}>", inline=True)
    e.add_field(name="Message ID", value=f"`{int(msg_id)}`", inline=True)
    e.add_field(name="Note", value="Edit payload received (content not cached).", inline=True)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_channel_create(channel: discord.abc.GuildChannel) -> None:
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    if not channel or int(getattr(getattr(channel, "guild", None), "id", 0) or 0) != int(cfg.guild_id):
        return
    if not _is_ticket_category_channel_for_audit(channel):
        return
    e = discord.Embed(title="Channel Created", color=0x57F287, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"<#{int(getattr(channel, 'id', 0) or 0)}>", inline=True)
    e.add_field(name="ID", value=f"`{int(getattr(channel, 'id', 0) or 0)}`", inline=True)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_channel_delete(channel: discord.abc.GuildChannel) -> None:
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    if not channel or int(getattr(getattr(channel, "guild", None), "id", 0) or 0) != int(cfg.guild_id):
        return
    if not _is_ticket_category_channel_for_audit(channel):
        return
    name = str(getattr(channel, "name", "") or "—")
    e = discord.Embed(title="Channel Deleted", color=0xED4245, timestamp=_now_utc())
    e.add_field(name="Channel", value=f"#{_clip(name, 90)}", inline=True)
    e.add_field(name="ID", value=f"`{int(getattr(channel, 'id', 0) or 0)}`", inline=True)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


async def audit_channel_update(before: discord.abc.GuildChannel, after: discord.abc.GuildChannel) -> None:
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.audit_enabled:
        return
    ch = after or before
    if not ch or int(getattr(getattr(ch, "guild", None), "id", 0) or 0) != int(cfg.guild_id):
        return
    if not (_is_ticket_category_channel_for_audit(before) or _is_ticket_category_channel_for_audit(after)):
        return
    bname = str(getattr(before, "name", "") or "")
    aname = str(getattr(after, "name", "") or "")
    bcat = int(getattr(before, "category_id", 0) or 0)
    acat = int(getattr(after, "category_id", 0) or 0)
    if bname == aname and bcat == acat:
        return
    e = discord.Embed(title="Channel Updated", color=0xFEE75C, timestamp=_now_utc())
    e.add_field(name="ID", value=f"`{int(getattr(ch, 'id', 0) or 0)}`", inline=True)
    if bname != aname and aname:
        e.add_field(name="Name", value=f"#{_clip(bname or '—', 70)} → #{_clip(aname, 70)}", inline=False)
    if bcat != acat:
        e.add_field(name="Category", value=f"`{bcat}` → `{acat}`", inline=False)
    await _audit_send(guild_id=int(cfg.guild_id), embed=e)


def _build_overwrites(
    *,
    guild: discord.Guild,
    owner: discord.Member,
    staff_role_ids: list[int],
    admin_role_ids: list[int],
    include_owner: bool,
) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {}
    overwrites[guild.default_role] = discord.PermissionOverwrite(view_channel=False)

    me = getattr(guild, "me", None) or guild.get_member(int(getattr(getattr(_BOT, "user", None), "id", 0) or 0))
    if isinstance(me, discord.Member):
        overwrites[me] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            manage_messages=True,
            embed_links=True,
            attach_files=True,
        )

    # Staff roles
    for rid in list(dict.fromkeys([int(x) for x in (staff_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    # Admin roles (optional; still no manage perms by default)
    for rid in list(dict.fromkeys([int(x) for x in (admin_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)

    # Ticket owner (optional; can be disabled during setup/testing)
    if include_owner:
        overwrites[owner] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
        )
    return overwrites


def _is_staff_member(member: discord.Member) -> bool:
    cfg = _cfg()
    if not cfg:
        return False
    try:
        perms = getattr(member, "guild_permissions", None)
        if perms and bool(getattr(perms, "administrator", False)):
            return True
    except Exception:
        pass
    try:
        rids = {int(r.id) for r in (member.roles or [])}
    except Exception:
        rids = set()
    if any(int(x) in rids for x in (cfg.admin_role_ids or [])):
        return True
    if any(int(x) in rids for x in (cfg.staff_role_ids or [])):
        return True
    return False


def _ticket_role_id_for_type(ticket_type: str) -> int:
    cfg = _cfg()
    if not cfg:
        return 0
    t = str(ticket_type or "").strip().lower()
    if t == "billing":
        return int(cfg.billing_role_id or 0)
    if t == "cancellation":
        return int(cfg.cancellation_role_id or 0)
    if t == "free_pass":
        return int(cfg.free_pass_no_whop_role_id or 0)
    if t == "no_whop_link":
        return int(cfg.no_whop_link_role_id or 0)
    return 0


async def _set_ticket_role_for_member(*, guild: discord.Guild, member: discord.Member, ticket_type: str, add: bool) -> None:
    """Auto-add/remove the per-ticket role for the owner."""
    if not isinstance(guild, discord.Guild) or not isinstance(member, discord.Member):
        return
    rid = int(_ticket_role_id_for_type(ticket_type) or 0)
    if rid <= 0:
        return
    role = guild.get_role(int(rid))
    if not role:
        await _log(f"⚠️ support_tickets: role not found for type={ticket_type} role_id={rid}")
        return

    me = getattr(guild, "me", None) or guild.get_member(int(getattr(getattr(_BOT, "user", None), "id", 0) or 0))
    if isinstance(me, discord.Member):
        with suppress(Exception):
            if not bool(getattr(me.guild_permissions, "manage_roles", False)):
                await _log(f"❌ support_tickets: bot lacks manage_roles (cannot {'add' if add else 'remove'} role_id={rid})")
                return
            if getattr(me, "top_role", None) and getattr(me.top_role, "position", 0) <= getattr(role, "position", 0):
                await _log(
                    f"❌ support_tickets: role hierarchy prevents {'add' if add else 'remove'} role_id={rid} (bot_top={me.top_role.position} role_pos={role.position})"
                )
                return
    has_it = False
    with suppress(Exception):
        has_it = any(int(getattr(r, "id", 0) or 0) == int(rid) for r in (member.roles or []))
    if add:
        # Special case: whenever we're adding the "no-whop" role (free-pass-no-whop or no-whop-link),
        # ensure any configured "remove on no-whop add" roles are removed (ex: legacy billing-fail role).
        cfg = _cfg()
        nr = int(getattr(cfg, "no_whop_link_role_id", 0) or 0) if cfg else 0
        fr = int(getattr(cfg, "free_pass_no_whop_role_id", 0) or 0) if cfg else 0
        is_no_whop_add = int(role.id) in {int(nr), int(fr)} or str(ticket_type or "").strip().lower() == "no_whop_link"
        if is_no_whop_add:
            remove_ids: set[int] = set()
            with suppress(Exception):
                if cfg and int(getattr(cfg, "billing_role_id", 0) or 0) > 0:
                    remove_ids.add(int(getattr(cfg, "billing_role_id", 0) or 0))
            with suppress(Exception):
                for x in (getattr(cfg, "no_whop_remove_role_ids_on_add", None) or []) if cfg else []:
                    rid2 = int(x or 0)
                    if rid2 > 0:
                        remove_ids.add(int(rid2))
            # Never remove the role we are about to add.
            remove_ids.discard(int(role.id))
            if remove_ids:
                try:
                    member_role_ids = {int(getattr(r, "id", 0) or 0) for r in (member.roles or [])}
                except Exception:
                    member_role_ids = set()
                for rid2 in sorted(list(remove_ids)):
                    if rid2 not in member_role_ids:
                        continue
                    r2 = guild.get_role(int(rid2))
                    if not r2:
                        continue
                    try:
                        await member.remove_roles(r2, reason="RSCheckerbot: remove legacy role when adding no-whop role")
                    except Exception as ex:
                        await _log(
                            f"⚠️ support_tickets: failed to remove role_id={rid2} from user_id={int(member.id)} "
                            f"(hierarchy/perms?) err={str(ex)[:180]}"
                        )
        if has_it:
            return
        try:
            await member.add_roles(role, reason=f"RSCheckerbot: open {ticket_type} ticket")
        except Exception as ex:
            await _log(f"❌ support_tickets: failed to add role_id={rid} to user_id={int(member.id)} ({str(ex)[:200]})")
    else:
        if not has_it:
            return
        try:
            await member.remove_roles(role, reason=f"RSCheckerbot: close {ticket_type} ticket")
        except Exception as ex:
            await _log(f"❌ support_tickets: failed to remove role_id={rid} from user_id={int(member.id)} ({str(ex)[:200]})")


async def post_resolution_followup_and_remove_role(
    *,
    discord_id: int,
    ticket_type: str,
    resolution_event: str,
    reference_jump_url: str = "",
) -> bool:
    """Mark an open ticket as resolved, remove its role, and post follow-up inside the ticket channel."""
    if not _ensure_cfg_loaded() or not _BOT:
        return False
    cfg = _cfg()
    if not cfg or not cfg.resolution_followup_enabled:
        return False
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return False

    uid = int(discord_id or 0)
    if uid <= 0:
        return False
    ttype = str(ticket_type or "").strip().lower()
    if ttype not in {"billing", "cancellation", "free_pass", "no_whop_link"}:
        return False

    # Find the open ticket channel (copy under lock).
    ch_id = 0
    already_sent = False
    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_find_open(db, ticket_type=ttype, user_id=uid, fingerprint="")
        if not found:
            return False
        tid, rec = found
        ch_id = _as_int(rec.get("channel_id"))
        already_sent = bool(str(rec.get("resolved_followup_sent_at_iso") or "").strip())
        if already_sent:
            return True

        # Throttle retries (prevents spam if multiple resolve signals arrive).
        now_iso = _now_iso()
        last_try = _parse_iso(str(rec.get("resolved_followup_last_attempt_at_iso") or "").strip() or "")
        if last_try and (_now_utc() - last_try) < timedelta(seconds=300):
            return True

        # Mark resolved + record an attempt (we'll mark "sent" only after successful post).
        if not str(rec.get("resolved_at_iso") or "").strip():
            rec["resolved_at_iso"] = now_iso
        rec["resolved_event"] = str(resolution_event or "")[:200]
        rec["resolved_followup_last_attempt_at_iso"] = now_iso
        try:
            rec["resolved_followup_attempts"] = int(rec.get("resolved_followup_attempts") or 0) + 1
        except Exception:
            rec["resolved_followup_attempts"] = 1
        # keep latest source reference
        if reference_jump_url:
            rec["reference_jump_url"] = str(reference_jump_url or "")
        db["tickets"][tid] = rec  # type: ignore[index]
        _index_save(db)

    ch = guild.get_channel(int(ch_id)) if ch_id else None
    if not isinstance(ch, discord.TextChannel):
        return False

    owner = guild.get_member(uid)
    if not isinstance(owner, discord.Member):
        with suppress(Exception):
            owner = await guild.fetch_member(uid)
    if not isinstance(owner, discord.Member):
        return False

    # Remove the ticket role now (resolved state).
    with suppress(Exception):
        await _set_ticket_role_for_member(guild=guild, member=owner, ticket_type=ttype, add=False)

    # Follow-up embed + buttons in-ticket.
    tmpl = _resolution_followup_template(ttype)
    desc = tmpl.strip() if tmpl else "Update: this ticket appears resolved. If you still have concerns, reply here — otherwise Support can close."
    e = discord.Embed(title="Update", description=desc[:4096], color=0x57F287)
    if resolution_event:
        e.add_field(name="Trigger", value=str(resolution_event)[:1024], inline=True)
    if reference_jump_url:
        e.add_field(name="Source", value=_embed_link("View Full Log", str(reference_jump_url)), inline=True)
    view = _CONTROLS_VIEW or SupportTicketControlsView()
    if ttype == "no_whop_link":
        # No staff ping; member may not have channel access.
        with suppress(Exception):
            await ch.send(
                embed=e,
                view=view,
                allowed_mentions=discord.AllowedMentions.none(),
                silent=True,
            )
    else:
        role_mention = _support_ping_role_mention()
        ping = " ".join([x for x in [f"<@{uid}>", role_mention] if str(x or "").strip()])
        with suppress(Exception):
            await ch.send(
                content=ping,
                embed=e,
                view=view,
                allowed_mentions=discord.AllowedMentions(users=True, roles=True, everyone=False),
            )

    # Mark "sent" after posting succeeds (best-effort).
    async with _INDEX_LOCK:
        db2 = _index_load()
        found2 = _ticket_by_channel_id(db2, int(ch.id))
        if found2:
            tid2, rec2 = found2
            if _ticket_is_open(rec2) and (not str(rec2.get("resolved_followup_sent_at_iso") or "").strip()):
                rec2["resolved_followup_sent_at_iso"] = _now_iso()
                db2["tickets"][tid2] = rec2  # type: ignore[index]
                _index_save(db2)

    with suppress(Exception):
        await _audit_ticket_resolved(
            ticket_type=ttype,
            owner=owner,
            ticket_channel=ch,
            resolution_event=resolution_event,
            reference_jump_url=reference_jump_url,
        )
    return True


async def reconcile_open_ticket_roles() -> None:
    """One-time best-effort: ensure legacy open tickets have correct ticket-roles applied."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return

    # Copy under lock to avoid holding lock across awaits.
    tickets: list[tuple[int, str, str]] = []  # (user_id, ticket_type, resolved_followup_sent_at_iso)
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            uid = _as_int(rec.get("user_id"))
            ttype = str(rec.get("ticket_type") or "").strip().lower()
            if uid <= 0 or ttype not in {"billing", "cancellation", "free_pass", "no_whop_link"}:
                continue
            r_iso = str(rec.get("resolved_followup_sent_at_iso") or "").strip()
            tickets.append((uid, ttype, r_iso))

    processed = 0
    fetched = 0
    for uid, ttype, r_iso in tickets:
        processed += 1
        m = guild.get_member(int(uid))
        if not isinstance(m, discord.Member):
            with suppress(Exception):
                m = await guild.fetch_member(int(uid))
        if isinstance(m, discord.Member):
            fetched += 1
        else:
            continue
        # If we already posted a resolved follow-up, ensure role is removed; otherwise ensure role is present.
        should_add = not bool(r_iso)
        with suppress(Exception):
            await _set_ticket_role_for_member(guild=guild, member=m, ticket_type=ttype, add=should_add)

    with suppress(Exception):
        await _log(f"🧩 support_tickets: role_reconcile processed={processed} fetched={fetched}")


def is_ticket_channel(channel_id: int) -> bool:
    """Fast check used by main.on_message (best-effort)."""
    if int(channel_id or 0) <= 0:
        return False
    try:
        db = _index_load()
        found = _ticket_by_channel_id(db, int(channel_id))
        return bool(found and _ticket_is_open(found[1]))
    except Exception:
        return False


async def has_open_ticket_for_user(*, ticket_type: str, user_id: int) -> bool:
    """Check if a user has an OPEN ticket of a given type (best-effort)."""
    t = str(ticket_type or "").strip().lower()
    uid = int(user_id or 0)
    if not t or uid <= 0:
        return False
    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_find_open(db, ticket_type=t, user_id=uid, fingerprint="")
        return bool(found and _ticket_is_open(found[1]))


async def get_open_cancellation_ticket_channel_id(user_id: int) -> int:
    """Return the channel ID of the user's open cancellation ticket, or 0 if none."""
    uid = int(user_id or 0)
    if uid <= 0:
        return 0
    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_find_open(db, ticket_type="cancellation", user_id=uid, fingerprint="")
        if not found:
            return 0
        rec = found[1]
        if not _ticket_is_open(rec):
            return 0
        return int(rec.get("channel_id") or 0)


async def record_activity_from_message(message: discord.Message) -> None:
    """Update last_activity_at for ticket channels (non-bot messages only)."""
    if not _ensure_cfg_loaded():
        return
    if not message or not getattr(message, "channel", None):
        return
    if not message.guild:
        return
    if not message.author or getattr(message.author, "bot", False):
        return

    cid = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
    if cid <= 0:
        return

    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_by_channel_id(db, cid)
        if not found:
            return
        tid, rec = found
        if not _ticket_is_open(rec):
            return
        rec["last_activity_at_iso"] = (message.created_at or _now_utc()).astimezone(timezone.utc).isoformat()
        db["tickets"][tid] = rec  # type: ignore[index]
        _index_save(db)


def _ticket_find_open(
    db: dict,
    *,
    ticket_type: str,
    user_id: int,
    fingerprint: str,
) -> tuple[str, dict] | None:
    t = str(ticket_type or "").strip().lower()
    fp = str(fingerprint or "").strip()
    uid = int(user_id or 0)
    for tid, rec in _ticket_iter(db):
        if not _ticket_is_open(rec):
            continue
        if str(rec.get("ticket_type") or "").strip().lower() != t:
            continue
        try:
            if int(rec.get("user_id") or 0) == uid:
                return (tid, rec)
        except Exception:
            pass
        if fp and str(rec.get("fingerprint") or "").strip() == fp:
            return (tid, rec)
    return None


async def _open_or_update_ticket(
    *,
    ticket_type: str,
    owner: discord.Member,
    fingerprint: str,
    category_id: int,
    preview_embed: discord.Embed,
    reference_jump_url: str = "",
    whop_dashboard_url: str = "",
    extra_sends: list[tuple[str, discord.Embed | None]] | None = None,
    extra_record_fields: dict | None = None,
) -> discord.TextChannel | None:
    if not _ensure_cfg_loaded():
        return None
    cfg = _cfg()
    if not cfg or not _BOT:
        return None
    if not isinstance(owner, discord.Member):
        return None
    if int(category_id or 0) <= 0:
        await _log(f"⚠️ support_tickets: category_id is not configured for type={ticket_type}")
        return None

    guild = _BOT.get_guild(int(cfg.guild_id))
    if not guild:
        await _log(f"⚠️ support_tickets: guild not found (guild_id={cfg.guild_id})")
        return None

    # Preflight: category existence + bot perms (this is the #1 reason tickets "stop" after role/category changes).
    try:
        cat = guild.get_channel(int(category_id))
    except Exception:
        cat = None
    if not isinstance(cat, discord.CategoryChannel):
        await _log(f"❌ support_tickets: category not found type={ticket_type} category_id={int(category_id)}")
    else:
        me = getattr(guild, "me", None) or guild.get_member(int(getattr(getattr(_BOT, "user", None), "id", 0) or 0))
        if isinstance(me, discord.Member):
            with suppress(Exception):
                p = cat.permissions_for(me)
                if not bool(getattr(p, "view_channel", False)):
                    await _log(
                        f"❌ support_tickets: bot cannot view category type={ticket_type} category_id={int(category_id)}"
                    )
                if not bool(getattr(p, "manage_channels", False)):
                    await _log(
                        f"❌ support_tickets: bot cannot manage_channels in category type={ticket_type} category_id={int(category_id)}"
                    )
                if not bool(getattr(p, "manage_permissions", False)):
                    await _log(
                        f"⚠️ support_tickets: bot cannot manage_permissions in category type={ticket_type} category_id={int(category_id)}"
                    )

    # Dedupe / cooldown: single open ticket per (type,user) or fingerprint.
    # no_whop_link: ping both member + staff on the header message.
    is_nowhop = str(ticket_type or "").strip().lower() == "no_whop_link"
    ping_member = bool(is_nowhop)
    ping_staff = bool(is_nowhop)

    async with _INDEX_LOCK:
        db = _index_load()
        existing = _ticket_find_open(db, ticket_type=ticket_type, user_id=int(owner.id), fingerprint=fingerprint)
        if existing and cfg.dedupe_enabled:
            _tid, rec = existing
            ch_id = _as_int(rec.get("channel_id"))
            existing_ticket_id = str(rec.get("ticket_id") or _tid or "").strip()
            header_mid = _as_int(rec.get("header_message_id") or 0)
            ch = guild.get_channel(int(ch_id)) if ch_id else None
            if isinstance(ch, discord.TextChannel):
                # Ensure the correct per-ticket role is applied even on dedupe.
                with suppress(Exception):
                    await _set_ticket_role_for_member(guild=guild, member=owner, ticket_type=ticket_type, add=True)
                # Keep the ticket header up-to-date (single message: pings + embed + buttons).
                with suppress(Exception):
                    new_mid = await _ensure_ticket_header_message(
                        ticket_type=str(ticket_type or ""),
                        channel=ch,
                        owner_id=int(owner.id),
                        include_owner=bool(cfg.include_ticket_owner_in_channel),
                        mention_owner=bool(ping_member),
                        mention_staff=bool(ping_staff),
                        preview_embed=preview_embed,
                        header_message_id=int(header_mid),
                    )
                    if new_mid:
                        rec["header_message_id"] = int(new_mid)
                        rec["channel_name"] = str(getattr(ch, "name", "") or "")
                        if reference_jump_url:
                            rec["reference_jump_url"] = str(reference_jump_url or "")
                        if whop_dashboard_url:
                            rec["whop_dashboard_url"] = str(whop_dashboard_url or "")
                        db["tickets"][_tid] = rec  # type: ignore[index]
                        _index_save(db)
                # Log dedupe to tickets-logs instead of posting inside the ticket.
                with suppress(Exception):
                    await _audit_ticket_deduped(
                        ticket_type=ticket_type,
                        owner=owner,
                        existing_channel=ch,
                        ticket_id=existing_ticket_id,
                        fingerprint=fingerprint,
                        reference_jump_url=reference_jump_url,
                    )
                # Important: do NOT bump last_activity for bot messages.
                return ch

        # Cooldown: if a ticket was recently CLOSED, do not re-open another one immediately.
        cd_s = int(_cooldown_seconds_for(ticket_type) or 0) if cfg.dedupe_enabled else 0
        if cd_s > 0:
            fp = str(fingerprint or "").strip()
            last_ts: datetime | None = None
            last_tid = ""
            last_rec: dict | None = None
            for tid0, rec0 in _ticket_iter(db):
                if str(rec0.get("ticket_type") or "").strip().lower() != str(ticket_type or "").strip().lower():
                    continue
                uid0 = _as_int(rec0.get("user_id"))
                if uid0 != int(owner.id):
                    if not (fp and str(rec0.get("fingerprint") or "").strip() == fp):
                        continue
                closed_iso = str(rec0.get("closed_at_iso") or "").strip()
                if not closed_iso:
                    continue
                dt0 = _parse_iso(closed_iso)
                if not dt0:
                    continue
                if (last_ts is None) or (dt0 > last_ts):
                    last_ts = dt0
                    last_tid = str(tid0 or "")
                    last_rec = rec0
            if last_ts and last_rec and (_now_utc() - last_ts) < timedelta(seconds=int(cd_s)):
                with suppress(Exception):
                    await _audit_ticket_suppressed_cooldown(
                        ticket_type=ticket_type,
                        owner=owner,
                        last_ticket_id=str(last_rec.get("ticket_id") or last_tid or "").strip(),
                        last_channel_id=int(_as_int(last_rec.get("channel_id"))),
                        last_channel_name=str(last_rec.get("channel_name") or "").strip(),
                        last_closed_at_iso=str(last_rec.get("closed_at_iso") or "").strip(),
                        cooldown_seconds=int(cd_s),
                        fingerprint=fingerprint,
                        reference_jump_url=reference_jump_url,
                    )
                # Return a truthy sentinel so callers don't log this as a "failed open".
                return discord.Object(id=int(_as_int(last_rec.get("channel_id")) or owner.id))

        ticket_id = _make_ticket_id()
        case_key = _ticket_case_key(ticket_id=ticket_id)
        ch_name = _ticket_channel_name(ticket_type, owner)
        topic = _ticket_topic(ticket_id=ticket_id, ticket_type=ticket_type, user_id=int(owner.id), fingerprint=fingerprint)
        overwrites = _build_overwrites(
            guild=guild,
            owner=owner,
            staff_role_ids=cfg.staff_role_ids,
            admin_role_ids=cfg.admin_role_ids,
            include_owner=bool(cfg.include_ticket_owner_in_channel),
        )

        ch = await _ensure_ticket_like_channel(
            guild=guild,
            category_id=int(category_id),
            case_key=case_key,
            channel_name=ch_name,
            topic=topic,
            overwrites=overwrites,
            apply_overwrites_if_found=True,
            reason="RSCheckerbot: open support ticket",
        )
        if not isinstance(ch, discord.TextChannel):
            await _log(f"❌ support_tickets: failed to create ticket channel type={ticket_type} user_id={owner.id}")
            return None

        # Safety: ensure configured staff roles can view the channel (especially if a channel was found pre-existing).
        with suppress(Exception):
            await _ensure_staff_roles_can_view_channel(guild=guild, channel=ch, staff_role_ids=cfg.staff_role_ids)

        # Auto-assign per-ticket role to the owner (billing/cancellation/no-whop).
        with suppress(Exception):
            await _set_ticket_role_for_member(guild=guild, member=owner, ticket_type=ticket_type, add=True)

        # Header (single message: pings + embed + buttons)
        header_mid = 0
        with suppress(Exception):
            header_mid = int(
                await _ensure_ticket_header_message(
                    ticket_type=str(ticket_type or ""),
                    channel=ch,
                    owner_id=int(owner.id),
                    include_owner=bool(cfg.include_ticket_owner_in_channel),
                    mention_owner=bool(ping_member),
                    mention_staff=bool(ping_staff),
                    preview_embed=preview_embed,
                    header_message_id=0,
                )
                or 0
            )
        for content, emb in (extra_sends or []):
            with suppress(Exception):
                if emb is None:
                    await ch.send(str(content or ""), silent=True)
                else:
                    await ch.send(content=str(content or ""), embed=emb, silent=True)

        rec = {
            "ticket_id": ticket_id,
            "ticket_type": str(ticket_type),
            "user_id": int(owner.id),
            "channel_id": int(ch.id),
            "channel_name": str(getattr(ch, "name", "") or ""),
            "guild_id": int(getattr(getattr(ch, "guild", None), "id", 0) or int(cfg.guild_id)),
            "header_message_id": int(header_mid or 0),
            "created_at_iso": _now_iso(),
            "last_activity_at_iso": _now_iso(),
            "status": "OPEN",
            "fingerprint": str(fingerprint or ""),
            "reference_jump_url": str(reference_jump_url or ""),
            "whop_dashboard_url": str(whop_dashboard_url or ""),
            "close_reason": "",
            "closed_at_iso": "",
            "startup_sent_at_iso": "",
            "startup_skipped_at_iso": "",
            "resolved_at_iso": "",
            "resolved_event": "",
            "resolved_followup_sent_at_iso": "",
            "resolved_followup_last_attempt_at_iso": "",
            "resolved_followup_attempts": 0,
        }
        if isinstance(extra_record_fields, dict):
            for k, v in extra_record_fields.items():
                ks = str(k or "").strip()
                if ks:
                    rec[ks] = v

        db["tickets"][ticket_id] = rec  # type: ignore[index]
        _index_save(db)
        return ch


def _embed_link(label: str, url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return "—"
    # Avoid nested markdown links like: [Open]([Open](https://...))
    m = re.search(r"\((https?://[^)]+)\)", raw)
    if m:
        raw = m.group(1).strip()
    m2 = re.search(r"(https?://\S+)", raw)
    u = (m2.group(1) if m2 else raw).strip()
    u = u.rstrip(").,")
    if not u.startswith(("http://", "https://")):
        return "—"
    return f"[{label}]({u})"


class SupportTicketControlsView(discord.ui.View):
    """Persistent buttons for ticket controls."""

    def __init__(self):
        super().__init__(timeout=None)

    def _allowed(self, user: discord.abc.User | discord.Member) -> bool:
        try:
            if isinstance(user, discord.Member):
                return _is_staff_member(user)
        except Exception:
            return False
        return False

    async def _deny(self, interaction: discord.Interaction) -> None:
        with suppress(Exception):
            await interaction.response.send_message("❌ Not allowed (staff only).", ephemeral=True)

    @discord.ui.button(
        label="Transcript & Close",
        style=discord.ButtonStyle.primary,
        custom_id="rsticket:transcript",
    )
    async def transcript(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not self._allowed(interaction.user):
            await self._deny(interaction)
            return
        with suppress(Exception):
            await interaction.response.send_message("⏳ Exporting transcript and closing…", ephemeral=True)
        ch_id = int(getattr(getattr(interaction, "channel", None), "id", 0) or 0)
        if ch_id:
            await close_ticket_by_channel_id(
                ch_id,
                close_reason="manual_transcript",
                do_transcript=True,
                delete_channel=True,
            )

    @discord.ui.button(
        label="Close",
        style=discord.ButtonStyle.danger,
        custom_id="rsticket:close",
    )
    async def close(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not self._allowed(interaction.user):
            await self._deny(interaction)
            return
        with suppress(Exception):
            await interaction.response.send_message("⏳ Closing ticket…", ephemeral=True)
        ch_id = int(getattr(getattr(interaction, "channel", None), "id", 0) or 0)
        if ch_id:
            await close_ticket_by_channel_id(
                ch_id,
                close_reason="manual_close",
                do_transcript=True,
                delete_channel=True,
            )


class _MembershipReportModal(discord.ui.Modal, title="Membership Report"):
    """User-friendly date range for Whop memberships joined report."""

    start_date = discord.ui.TextInput(
        label="Start date",
        placeholder="e.g. 2/2/26 or 2026-02-02",
        required=True,
        max_length=32,
    )
    end_date = discord.ui.TextInput(
        label="End date",
        placeholder="e.g. 2/9/26 or 2026-02-09",
        required=False,
        max_length=32,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not _ensure_cfg_loaded() or not _BOT:
            with suppress(Exception):
                await interaction.response.send_message("❌ Bot not ready.", ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member) or not _is_staff_member(interaction.user):
            with suppress(Exception):
                await interaction.response.send_message("❌ Not allowed (staff only).", ephemeral=True)
            return
        cb = _RUN_MEMBERSHIP_REPORT_CALLBACK
        if not cb or not callable(cb):
            with suppress(Exception):
                await interaction.response.send_message("❌ Membership report is not configured.", ephemeral=True)
            return
        with suppress(Exception):
            await interaction.response.defer(ephemeral=True)
        try:
            ok, msg = await cb(interaction.user, str(self.start_date.value or "").strip(), str(self.end_date.value or "").strip())
        except Exception as e:
            ok, msg = False, str(e)[:200]
        if ok:
            with suppress(Exception):
                await interaction.followup.send(f"✅ {msg}", ephemeral=True)
        else:
            with suppress(Exception):
                await interaction.followup.send(f"❌ {msg}", ephemeral=True)


class _MemberLookupModal(discord.ui.Modal, title="Lookup Member"):
    query = discord.ui.TextInput(
        label="Discord ID or @mention",
        placeholder="e.g. 1195796809705066627 or <@1195796809705066627>",
        required=True,
        max_length=64,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not _ensure_cfg_loaded() or not _BOT:
            with suppress(Exception):
                await interaction.response.send_message("❌ Bot not ready.", ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member) or not _is_staff_member(interaction.user):
            with suppress(Exception):
                await interaction.response.send_message("❌ Not allowed (staff only).", ephemeral=True)
            return
        cfg = _cfg()
        if not cfg:
            with suppress(Exception):
                await interaction.response.send_message("❌ Config not loaded.", ephemeral=True)
            return
        guild = _BOT.get_guild(int(cfg.guild_id))
        if not isinstance(guild, discord.Guild):
            with suppress(Exception):
                await interaction.response.send_message("❌ Guild not found.", ephemeral=True)
            return

        raw = str(self.query.value or "").strip()
        m = re.search(r"\b(\d{17,19})\b", raw)
        if not m:
            with suppress(Exception):
                await interaction.response.send_message("❌ Please paste a valid Discord ID (17-19 digits).", ephemeral=True)
            return
        did = int(m.group(1))
        picker = _build_member_lookup_picker_embed(guild=guild, did=did)
        view = MemberLookupResultView(did=did)
        with suppress(Exception):
            await interaction.response.send_message(
                embed=picker,
                view=view,
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )


class MemberLookupResultView(discord.ui.View):
    """Ephemeral per-lookup view: staff chooses which last-card to render."""

    def __init__(self, *, did: int):
        super().__init__(timeout=600)
        self.did = int(did or 0)
        self._render_lock = asyncio.Lock()
        self._render_cache: dict[str, list[discord.Embed]] = {}

    def _allowed(self, user: discord.abc.User | discord.Member) -> bool:
        try:
            return bool(isinstance(user, discord.Member) and _is_staff_member(user))
        except Exception:
            return False

    async def _deny(self, interaction: discord.Interaction) -> None:
        with suppress(Exception):
            await interaction.response.send_message("❌ Not allowed (staff only).", ephemeral=True)

    async def _render(self, interaction: discord.Interaction, *, category: str) -> None:
        if not _ensure_cfg_loaded() or not _BOT:
            with suppress(Exception):
                await interaction.response.send_message("❌ Bot not ready.", ephemeral=True)
            return
        if not self._allowed(interaction.user):
            await self._deny(interaction)
            return
        cfg = _cfg()
        if not cfg:
            with suppress(Exception):
                await interaction.response.send_message("❌ Config not loaded.", ephemeral=True)
            return
        guild = _BOT.get_guild(int(cfg.guild_id))
        if not isinstance(guild, discord.Guild):
            with suppress(Exception):
                await interaction.response.send_message("❌ Guild not found.", ephemeral=True)
            return

        # ACK ASAP to avoid "Unknown interaction" when rate-limited / slow.
        # After deferring, we must use edit_original_response() instead of response.edit_message().
        with suppress(Exception):
            if not interaction.response.is_done():
                await interaction.response.defer()

        cat = str(category or "").strip().lower()
        async with self._render_lock:
            embs = self._render_cache.get(cat)
            if embs is None:
                embs = await _build_member_lookup_category_embeds(guild=guild, did=int(self.did), category=cat)
                self._render_cache[cat] = embs

        # Update the same ephemeral message so staff can switch between views.
        try:
            await interaction.edit_original_response(embeds=embs[:10], view=self)
        except TypeError:
            # Older discord.py: may not support embeds= on edit reliably.
            with suppress(Exception):
                await interaction.edit_original_response(embed=(embs[0] if embs else None), view=self)
        except discord.NotFound:
            # Interaction/message expired or deleted; nothing we can do.
            return

    @discord.ui.button(label="Payment", style=discord.ButtonStyle.primary)
    async def payment(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self._render(interaction, category="payment")

    @discord.ui.button(label="Membership", style=discord.ButtonStyle.success)
    async def membership(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self._render(interaction, category="membership")

    @discord.ui.button(label="Cancellation", style=discord.ButtonStyle.secondary)
    async def cancellation(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self._render(interaction, category="cancellation")

    @discord.ui.button(label="Dispute", style=discord.ButtonStyle.danger)
    async def dispute(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self._render(interaction, category="dispute")


class MemberLookupPanelView(discord.ui.View):
    """Persistent staff panel for member lookups (ephemeral results)."""

    def __init__(self):
        super().__init__(timeout=None)

    def _allowed(self, user: discord.abc.User | discord.Member) -> bool:
        try:
            if isinstance(user, discord.Member):
                return _is_staff_member(user)
        except Exception:
            return False
        return False

    async def _deny(self, interaction: discord.Interaction) -> None:
        with suppress(Exception):
            await interaction.response.send_message("❌ Not allowed (staff only).", ephemeral=True)

    @discord.ui.button(
        label="Lookup Member",
        style=discord.ButtonStyle.primary,
        custom_id="rsmember:lookup",
    )
    async def lookup(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not self._allowed(interaction.user):
            await self._deny(interaction)
            return
        with suppress(Exception):
            await interaction.response.send_modal(_MemberLookupModal())

    @discord.ui.button(
        label="Membership Report",
        style=discord.ButtonStyle.secondary,
        custom_id="rsmember:report",
    )
    async def membership_report(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not self._allowed(interaction.user):
            await self._deny(interaction)
            return
        with suppress(Exception):
            await interaction.response.send_modal(_MembershipReportModal())


def _format_discord_id(uid: int) -> str:
    return f"`{int(uid)}`" if int(uid) > 0 else "—"


def _norm_whop_status_key(s: str) -> str:
    raw = str(s or "").strip().lower()
    if not raw:
        return "unknown"
    raw = raw.replace(" ", "_")
    if raw in {"pastdue"}:
        raw = "past_due"
    if raw in {"cancelled"}:
        raw = "canceled"
    if raw in {"canceling", "cancelling"}:
        raw = "canceling"
    if raw in {"deactivated", "inactive"}:
        raw = "deactivated"
    return raw


def _whop_status_label(key: str) -> str:
    k = _norm_whop_status_key(key)
    if k == "past_due":
        return "Past Due"
    if k == "canceled":
        return "Canceled"
    if k == "canceling":
        return "Cancelling"
    if k == "trialing":
        return "Trialing"
    if k == "active":
        return "Active"
    if k == "deactivated":
        return "Deactivated"
    return (key or "Unknown").strip() or "Unknown"


def _whop_date_any(b: dict) -> str:
    if not isinstance(b, dict):
        return ""
    # Prefer the human-friendly field captured from member-status-logs.
    s = str(b.get("renewal_end") or "").strip()
    if s and s != "—":
        return s
    iso = str(b.get("renewal_end_iso") or "").strip()
    if iso:
        return _fmt_date_any(iso)
    return ""


def _whop_status_display(*, ticket_type: str, whop_brief: dict | None, status_override: str = "") -> str:
    b = whop_brief if isinstance(whop_brief, dict) else {}
    raw_status = str(status_override or b.get("status") or "").strip()
    key = _norm_whop_status_key(raw_status)

    # Cancellation scheduled can appear as "active" + cancel_at_period_end=yes.
    cap = str(b.get("cancel_at_period_end") or "").strip().lower()
    if ticket_type == "cancellation" and key == "active" and cap in {"yes", "true", "1"}:
        key = "canceling"

    label = _whop_status_label(key)
    d = _whop_date_any(b)
    if d:
        # Keep generic "(date)" per your spec.
        return f"{label} ({d})"
    return label


def build_cancellation_preview_embed(
    *,
    member: discord.Member,
    whop_brief: dict | None,
    cancellation_reason: str = "",
    reference_jump_url: str = "",
) -> discord.Embed:
    b = whop_brief if isinstance(whop_brief, dict) else {}
    e = discord.Embed(title="Cancellation", color=0xFEE75C)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    e.add_field(name="Membership", value=str(b.get("product") or "—")[:1024], inline=True)
    e.add_field(name="Whop Status", value=_whop_status_display(ticket_type="cancellation", whop_brief=b), inline=True)

    days = str(b.get("remaining_days") or "").strip()
    end = str(b.get("renewal_end") or "").strip()
    if days and days != "—":
        e.add_field(name="Remaining Days", value=days[:1024], inline=True)
    if end and end != "—":
        e.add_field(name="Access Ends On", value=end[:1024], inline=True)

    cap_raw = str(b.get("cancel_at_period_end") or "").strip()
    if cap_raw and cap_raw != "—":
        e.add_field(name="Cancel At Period End", value=cap_raw[:1024], inline=True)

    # Cancellation reason values from member-status-logs can contain extra lines (membership + timestamp).
    # Keep only the actual reason (first non-empty line) for a clean card.
    reason_raw = str(cancellation_reason or b.get("cancellation_reason") or "").strip()
    reason_line = ""
    for ln in reason_raw.splitlines():
        s = str(ln or "").strip()
        if s:
            reason_line = s
            break
    if reason_line:
        e.add_field(name="Cancellation Reason", value=f"```\n{reason_line[:300]}\n```", inline=False)
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", str(b.get("dashboard_url") or "")), inline=True)
    e.add_field(name="Message Reference", value=_embed_link("View Full Log", reference_jump_url), inline=True)
    return e


def build_billing_preview_embed(
    *,
    member: discord.Member,
    event_type: str,
    status: str,
    whop_brief: dict | None = None,
    reference_jump_url: str = "",
) -> discord.Embed:
    b = whop_brief if isinstance(whop_brief, dict) else {}
    e = discord.Embed(title="Billing", color=0xED4245)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    e.add_field(name="Membership", value=str(b.get("product") or "—")[:1024], inline=True)

    st_disp = _whop_status_display(ticket_type="billing", whop_brief=b, status_override=str(status or ""))
    e.add_field(name="Whop Status", value=str(st_disp or "—")[:1024], inline=True)

    # Core billing context (same shapes as member-status-logs cards)
    days = str(b.get("remaining_days") or "").strip()
    if days and days != "—":
        e.add_field(name="Remaining Days", value=days[:1024], inline=True)
    end = str(b.get("renewal_end") or "").strip()
    if end and end != "—":
        e.add_field(name="Next Billing Date", value=end[:1024], inline=True)
    cap_raw = str(b.get("cancel_at_period_end") or "").strip()
    if cap_raw and cap_raw != "—":
        e.add_field(name="Cancel At Period End", value=cap_raw[:1024], inline=True)

    # Optional: show a human-readable issue label (avoid confusing raw event strings).
    ev = str(event_type or "").strip().lower()
    issue = ""
    if "payment.failed" in ev or "payment_failed" in ev:
        issue = "Payment Failed"
    elif "past_due" in ev or "invoice.past_due" in ev:
        issue = "Past Due"
    elif ev:
        issue = ev.replace("_", " ").replace(".", " ").strip().title()
    if issue:
        e.add_field(name="Issue", value=issue[:1024], inline=True)

    dash = str(b.get("dashboard_url") or "").strip()
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", dash), inline=True)
    with suppress(Exception):
        roles_txt = _access_roles_plain(
            member,
            {int(r.id) for r in (member.roles or []) if int(getattr(r, "id", 0) or 0) != int(member.guild.default_role.id)},
        )
        if roles_txt and roles_txt != "—":
            e.add_field(name="Current Roles", value=str(roles_txt)[:1024], inline=False)
    e.add_field(name="Message Reference", value=_embed_link("View Full Log", reference_jump_url), inline=True)
    return e


def build_free_pass_header_embed(
    *,
    member: discord.Member,
    what_you_missed_jump_url: str,
    reference_jump_url: str = "",
    whop_dashboard_url: str = "",
) -> discord.Embed:
    e = discord.Embed(title="Free Pass", color=0x5865F2)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    e.add_field(name="Whop Status", value="Not linked", inline=True)
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", str(whop_dashboard_url or "")), inline=True)
    e.add_field(name="Message Reference", value=_embed_link("View Full Log", str(reference_jump_url or "")), inline=True)
    return e


def build_free_pass_welcome_header_embed(
    *,
    member: discord.Member,
    whop_brief: dict | None,
    reference_jump_url: str = "",
) -> discord.Embed:
    """Ticket header for Whop Free Pass (Lite) members (linked via Whop)."""
    b = whop_brief if isinstance(whop_brief, dict) else {}
    e = discord.Embed(title="Free Pass Welcome", color=0x57F287)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    prod = str(b.get("product") or "").strip() or "—"
    e.add_field(name="Membership", value=prod[:1024], inline=True)
    st_disp = _whop_status_display(ticket_type="free_pass_welcome", whop_brief=b, status_override=str(b.get("status") or ""))
    e.add_field(name="Whop Status", value=str(st_disp or "—")[:1024], inline=True)
    dash = str(b.get("dashboard_url") or "").strip()
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", dash), inline=True)
    e.add_field(name="Message Reference", value=_embed_link("View Full Log", str(reference_jump_url or "")), inline=True)
    return e


def build_member_welcome_preview_embed(
    *,
    member: discord.Member,
    whop_brief: dict | None,
    reference_jump_url: str = "",
) -> discord.Embed:
    """Ticket header for first-time paid members (full access)."""
    b = whop_brief if isinstance(whop_brief, dict) else {}
    e = discord.Embed(title="Member Welcome", color=0x57F287)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    prod = str(b.get("product") or "").strip() or "—"
    e.add_field(name="Membership", value=prod[:1024], inline=True)
    st_disp = _whop_status_display(ticket_type="member_welcome", whop_brief=b, status_override=str(b.get("status") or ""))
    e.add_field(name="Whop Status", value=str(st_disp or "—")[:1024], inline=True)
    dash = str(b.get("dashboard_url") or "").strip()
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", dash), inline=True)
    e.add_field(name="Message Reference", value=_embed_link("View Full Log", str(reference_jump_url or "")), inline=True)
    return e


def build_no_whop_link_preview_embed(
    *,
    member: discord.Member,
    whop_brief: dict | None = None,
    reference_jump_url: str = "",
) -> discord.Embed:
    """Ticket header for members who have Members role but Whop Discord isn't connected (or doesn't match)."""
    b = whop_brief if isinstance(whop_brief, dict) else {}
    e = discord.Embed(title="No Whop Link", color=0xFEE75C)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    mid = str(b.get("membership_id") or "").strip()
    if mid:
        e.add_field(name="Membership ID", value=f"`{mid[:128]}`", inline=True)
    # Member since (Whop side when available; fallback to Discord join date).
    ms = str(b.get("member_since") or b.get("customer_since") or "").strip()
    if not ms:
        with suppress(Exception):
            if getattr(member, "joined_at", None):
                ms = _fmt_date_any(member.joined_at.astimezone(timezone.utc).isoformat())  # type: ignore[union-attr]
    if ms:
        e.add_field(name="Member Since", value=str(ms)[:1024], inline=True)
    # Current roles (all roles, no mentions)
    with suppress(Exception):
        role_ids = {int(r.id) for r in (member.roles or []) if int(getattr(r, "id", 0) or 0) != int(member.guild.default_role.id)}
        roles_txt = _access_roles_plain(member, role_ids)
        if roles_txt and roles_txt != "—":
            e.add_field(name="Current Roles", value=str(roles_txt)[:1024], inline=False)
    if not str(b.get("status") or "").strip():
        # Default for discord-only fallback cases
        b = {**b, "status": "Not linked"}
    e.add_field(name="Whop Status", value=_whop_status_display(ticket_type="no_whop_link", whop_brief=b)[:1024] or "—", inline=True)
    prod = str(b.get("product") or "").strip()
    if prod:
        e.add_field(name="Membership", value=prod[:1024], inline=True)
    conn = str(b.get("connected_discord") or "").strip()
    if conn:
        e.add_field(name="Connected Discord", value=conn[:1024], inline=False)
    dash = str(b.get("dashboard_url") or "").strip()
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", dash), inline=True)
    e.add_field(name="Message Reference", value=_embed_link("View Full Log", str(reference_jump_url or "")), inline=True)
    if not mid:
        e.add_field(name="Whop", value="not linked (no membership_id recorded yet)", inline=True)
    return e


async def _get_or_create_no_whop_link_category_id(*, guild: discord.Guild) -> int:
    cfg = _cfg()
    if not cfg or not isinstance(guild, discord.Guild) or not _BOT:
        return 0
    cid = int(cfg.no_whop_link_category_id or 0)
    if cid > 0:
        ch = guild.get_channel(cid)
        return int(ch.id) if isinstance(ch, discord.CategoryChannel) else 0

    name = str(cfg.no_whop_link_category_name or "no-whop-link").strip() or "no-whop-link"
    for cat in list(getattr(guild, "categories", []) or []):
        if isinstance(cat, discord.CategoryChannel) and str(getattr(cat, "name", "") or "").strip().lower() == name.lower():
            return int(cat.id)

    me = getattr(guild, "me", None) or guild.get_member(int(getattr(getattr(_BOT, "user", None), "id", 0) or 0))
    if not (me and getattr(me, "guild_permissions", None) and bool(getattr(me.guild_permissions, "manage_channels", False))):
        await _log("❌ support_tickets: cannot create no-whop-link category (missing manage_channels)")
        return 0

    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {}
    overwrites[guild.default_role] = discord.PermissionOverwrite(view_channel=False)
    if isinstance(me, discord.Member):
        overwrites[me] = discord.PermissionOverwrite(view_channel=True, manage_channels=True, manage_permissions=True, read_message_history=True)
    for rid in list(dict.fromkeys([int(x) for x in (cfg.staff_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True)
    for rid in list(dict.fromkeys([int(x) for x in (cfg.admin_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if role and role not in overwrites:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True)

    try:
        created = await guild.create_category(name=name, overwrites=overwrites, reason="RSCheckerbot: create no-whop-link ticket category")
        return int(created.id) if isinstance(created, discord.CategoryChannel) else 0
    except Exception as ex:
        await _log(f"❌ support_tickets: failed to create no-whop-link category ({str(ex)[:200]})")
        return 0


async def _get_or_create_member_welcome_category_id(*, guild: discord.Guild) -> int:
    cfg = _cfg()
    if not cfg or not isinstance(guild, discord.Guild) or not _BOT:
        return 0
    cid = int(cfg.member_welcome_category_id or 0)
    if cid > 0:
        ch = guild.get_channel(cid)
        return int(ch.id) if isinstance(ch, discord.CategoryChannel) else 0

    name = str(cfg.member_welcome_category_name or "member-welcome").strip() or "member-welcome"
    for cat in list(getattr(guild, "categories", []) or []):
        if isinstance(cat, discord.CategoryChannel) and str(getattr(cat, "name", "") or "").strip().lower() == name.lower():
            return int(cat.id)

    me = getattr(guild, "me", None) or guild.get_member(int(getattr(getattr(_BOT, "user", None), "id", 0) or 0))
    if not (me and getattr(me, "guild_permissions", None) and bool(getattr(me.guild_permissions, "manage_channels", False))):
        await _log("❌ support_tickets: cannot create member-welcome category (missing manage_channels)")
        return 0

    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {}
    overwrites[guild.default_role] = discord.PermissionOverwrite(view_channel=False)
    if isinstance(me, discord.Member):
        overwrites[me] = discord.PermissionOverwrite(view_channel=True, manage_channels=True, manage_permissions=True, read_message_history=True)
    for rid in list(dict.fromkeys([int(x) for x in (cfg.staff_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True)
    for rid in list(dict.fromkeys([int(x) for x in (cfg.admin_role_ids or []) if int(x) > 0])):
        role = guild.get_role(int(rid))
        if role and role not in overwrites:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, read_message_history=True)

    try:
        created = await guild.create_category(name=name, overwrites=overwrites, reason="RSCheckerbot: create member-welcome ticket category")
        return int(created.id) if isinstance(created, discord.CategoryChannel) else 0
    except Exception as ex:
        await _log(f"❌ support_tickets: failed to create member-welcome category ({str(ex)[:200]})")
        return 0


async def open_cancellation_ticket(
    *,
    member: discord.Member,
    whop_brief: dict | None,
    cancellation_reason: str = "",
    fingerprint: str,
    reference_jump_url: str = "",
) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg:
        return None
    embed = build_cancellation_preview_embed(
        member=member,
        whop_brief=whop_brief,
        cancellation_reason=cancellation_reason,
        reference_jump_url=reference_jump_url,
    )
    return await _open_or_update_ticket(
        ticket_type="cancellation",
        owner=member,
        fingerprint=fingerprint,
        category_id=int(cfg.cancellation_category_id),
        preview_embed=embed,
        reference_jump_url=reference_jump_url,
        whop_dashboard_url=str((whop_brief or {}).get("dashboard_url") or "") if isinstance(whop_brief, dict) else "",
    )


def _in_current_month(dt: datetime) -> bool:
    if not isinstance(dt, datetime):
        return False
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(str(_TZ_NAME or "UTC").strip() or "UTC")
        now_local = _now_utc().astimezone(tz)
        d_local = dt.astimezone(tz)
        return (now_local.year == d_local.year) and (now_local.month == d_local.month)
    except Exception:
        now = _now_utc()
        d = dt.astimezone(timezone.utc)
        return (now.year == d.year) and (now.month == d.month)


async def open_billing_ticket(
    *,
    member: discord.Member,
    event_type: str,
    status: str,
    whop_brief: dict | None = None,
    fingerprint: str,
    occurred_at: datetime,
    reference_jump_url: str = "",
) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg:
        return None
    if not _in_current_month(occurred_at or _now_utc()):
        return None
    embed = build_billing_preview_embed(
        member=member,
        event_type=event_type,
        status=status,
        whop_brief=whop_brief,
        reference_jump_url=reference_jump_url,
    )
    return await _open_or_update_ticket(
        ticket_type="billing",
        owner=member,
        fingerprint=fingerprint,
        category_id=int(cfg.billing_category_id),
        preview_embed=embed,
        reference_jump_url=reference_jump_url,
    )


def _guide_text() -> str:
    # Exact format requested (no extra blank lines; links in <> to avoid previews).
    return (
        "## Welcome to **Reselling Secrets 🚀\n"
        "### How to Access Reselling Secrets (7-Day Trial)\n"
        "### 1️⃣ Join & Claim Access\n"
        "<https://whop.com/profits-pass/profits-pass/>\n"
        "* Complete checkout\n"
        "* Connect Discord when prompted\n"
        "* Access is granted automatically\n"
        "### 2️⃣ Not Seeing the Server?\n"
        "<https://whop.com/account/connected-accounts/>\n"
        "* Make sure Discord is connected\n"
        "* Confirm you’re logged into the correct Discord account\n"
        "### 3️⃣ Discord Verification (If Needed)\n"
        "* Discord → ⚙️ Settings → My Account\n"
        "* Verify Email\n"
        "* Click the link sent to your inbox\n"
        "### 4️⃣ Still Stuck?\n"
        "<https://whop.com/@me/settings/memberships/>\n"
        "* Move membership to the correct Whop account"
    )


async def _build_what_you_missed_preview_embed() -> tuple[discord.Embed | None, str]:
    cfg = _cfg()
    if not cfg or not _BOT:
        return (None, "")
    g = _BOT.get_guild(int(cfg.guild_id))
    if not g:
        return (None, "")
    ch = g.get_channel(int(cfg.what_you_missed_channel_id)) if cfg.what_you_missed_channel_id else None
    if not isinstance(ch, discord.TextChannel):
        return (None, "")

    # Only 1 sample message (most recent post with content/embed/attachment).
    src: discord.Message | None = None
    with suppress(Exception):
        async for m in ch.history(limit=max(10, int(cfg.preview_limit or 1))):
            if not m:
                continue
            has_content = bool(str(m.content or "").strip())
            has_embeds = bool(getattr(m, "embeds", None))
            has_files = bool(getattr(m, "attachments", None))
            if has_content or has_embeds or has_files:
                src = m
                break

    if not src:
        return (None, "")

    source_jump = str(getattr(src, "jump_url", "") or "")

    # Build a preview embed that looks like the original post (best-effort).
    preview: discord.Embed | None = None
    with suppress(Exception):
        if src.embeds:
            d = src.embeds[0].to_dict()
            # Remove noisy footer/timestamp so it matches RSCheckerbot-style cards.
            d.pop("footer", None)
            d.pop("timestamp", None)
            preview = discord.Embed.from_dict(d)
    if preview is None:
        txt = str(src.content or "").strip()
        preview = discord.Embed(description=(txt[:3500] if txt else "—"), color=0x5865F2)

    # Ensure we always have a clear header label.
    with suppress(Exception):
        if not str(getattr(preview, "title", "") or "").strip():
            first_line = str(src.content or "").strip().splitlines()[0].strip() if str(src.content or "").strip() else ""
            if first_line:
                preview.title = first_line[:256]
            else:
                preview.title = "Latest from #what-you-missed"

    # Attach image from attachments if needed (only if embed lacks one).
    has_img = False
    with suppress(Exception):
        has_img = bool(getattr(getattr(preview, "image", None), "url", None))
    if not has_img:
        img_url = ""
        with suppress(Exception):
            for a in (src.attachments or []):
                ctype = str(getattr(a, "content_type", "") or "").lower()
                if "image" in ctype or str(a.filename or "").lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                    img_url = str(getattr(a, "url", "") or "")
                    break
        if img_url:
            with suppress(Exception):
                preview.set_image(url=img_url)

    # Include a clean link (no pings; no extra footer).
    with suppress(Exception):
        preview.add_field(name="View Post", value=_embed_link("Open", source_jump), inline=False)

    return (preview, source_jump)


async def open_free_pass_ticket(
    *,
    member: discord.Member,
    fingerprint: str,
    reference_jump_url: str = "",
) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg:
        return None

    preview_embed, source_jump = await _build_what_you_missed_preview_embed()
    header = build_free_pass_header_embed(member=member, what_you_missed_jump_url=source_jump, reference_jump_url=str(reference_jump_url or ""))
    extra: list[tuple[str, discord.Embed | None]] = []
    extra.append((_guide_text(), None))
    # Put the preview LAST (cleaner; matches requested layout).
    if preview_embed:
        extra.append(("**Latest from #what-you-missed**", preview_embed))

    return await _open_or_update_ticket(
        ticket_type="free_pass",
        owner=member,
        fingerprint=fingerprint,
        category_id=int(cfg.free_pass_category_id),
        preview_embed=header,
        extra_sends=extra,
        extra_record_fields={"what_you_missed_jump_url": str(source_jump or "")},
        reference_jump_url=str(reference_jump_url or ""),
    )


async def open_free_pass_welcome_ticket(
    *,
    member: discord.Member,
    whop_brief: dict | None,
    fingerprint: str,
    reference_jump_url: str = "",
) -> discord.TextChannel | None:
    """Welcome ticket for Whop Free Pass (Lite) members (should NOT apply no-whop role)."""
    cfg = _cfg()
    if not cfg:
        return None
    header = build_free_pass_welcome_header_embed(member=member, whop_brief=whop_brief, reference_jump_url=str(reference_jump_url or ""))
    return await _open_or_update_ticket(
        ticket_type="free_pass_welcome",
        owner=member,
        fingerprint=fingerprint,
        category_id=int(cfg.free_pass_category_id),
        preview_embed=header,
        reference_jump_url=str(reference_jump_url or ""),
        whop_dashboard_url=str((whop_brief or {}).get("dashboard_url") or "") if isinstance(whop_brief, dict) else "",
    )


async def open_member_welcome_ticket(
    *,
    member: discord.Member,
    whop_brief: dict | None,
    fingerprint: str,
    reference_jump_url: str = "",
) -> discord.TextChannel | None:
    """First-time paid-member welcome ticket (separate category)."""
    cfg = _cfg()
    if not cfg or not _BOT:
        return None
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return None
    cat_id = await _get_or_create_member_welcome_category_id(guild=guild)
    if int(cat_id or 0) <= 0:
        return None
    header = build_member_welcome_preview_embed(member=member, whop_brief=whop_brief, reference_jump_url=str(reference_jump_url or ""))
    return await _open_or_update_ticket(
        ticket_type="member_welcome",
        owner=member,
        fingerprint=fingerprint,
        category_id=int(cat_id),
        preview_embed=header,
        reference_jump_url=str(reference_jump_url or ""),
        whop_dashboard_url=str((whop_brief or {}).get("dashboard_url") or "") if isinstance(whop_brief, dict) else "",
    )


async def open_no_whop_link_ticket(
    *,
    member: discord.Member,
    fingerprint: str,
    whop_brief: dict | None = None,
    reference_jump_url: str = "",
    note: str = "",
) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg or not _BOT:
        return None
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return None
    cat_id = await _get_or_create_no_whop_link_category_id(guild=guild)
    if int(cat_id or 0) <= 0:
        return None
    embed = build_no_whop_link_preview_embed(member=member, whop_brief=whop_brief, reference_jump_url=str(reference_jump_url or ""))
    return await _open_or_update_ticket(
        ticket_type="no_whop_link",
        owner=member,
        fingerprint=fingerprint,
        category_id=int(cat_id),
        preview_embed=embed,
        reference_jump_url=reference_jump_url,
        whop_dashboard_url=str((whop_brief or {}).get("dashboard_url") or "") if isinstance(whop_brief, dict) else "",
        extra_record_fields={"no_whop_link_note": str(note or "").strip()},
    )


def _scan_state_get() -> dict:
    db = _migrations_load()
    st = db.get("scan_state") if isinstance(db.get("scan_state"), dict) else {}
    return st if isinstance(st, dict) else {}


def _scan_state_set(key: str, value: dict) -> None:
    db = _migrations_load()
    st = db.get("scan_state") if isinstance(db.get("scan_state"), dict) else {}
    if not isinstance(st, dict):
        st = {}
    st[str(key)] = value if isinstance(value, dict) else {}
    db["scan_state"] = st
    _migrations_save(db)


def _membership_id_from_member_history(discord_id: int) -> str:
    """Best-effort: return last_membership_id/whop_key for a Discord user from member_history.json."""
    did = int(discord_id or 0)
    if did <= 0:
        return ""
    raw = _load_json(MEMBER_HISTORY_PATH)
    if not isinstance(raw, dict):
        return ""
    rec = raw.get(str(did))
    if not isinstance(rec, dict):
        return ""
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    if not isinstance(wh, dict):
        return ""
    mid = str(wh.get("last_membership_id") or wh.get("last_whop_key") or "").strip()
    return mid


def _linked_discord_id_from_identity_cache(email: str) -> int:
    """Best-effort: resolve linked Discord ID by email from whop_identity_cache.json (built from native whop cards)."""
    em = str(email or "").strip().lower()
    if not em or "@" not in em:
        return 0
    raw = _load_json(WHOP_IDENTITY_CACHE_PATH)
    if not isinstance(raw, dict):
        return 0
    rec = raw.get(em)
    if not isinstance(rec, dict):
        return 0
    did = str(rec.get("discord_id") or "").strip()
    return int(did) if did.isdigit() else 0


async def sweep_no_whop_link_scan(*, force: bool = False) -> str:
    """Periodic scan: open no_whop_link tickets for Members-role users whose Whop membership has no Discord connection (or mismatch)."""
    if not _ensure_cfg_loaded() or not _BOT:
        return ""
    cfg = _cfg()
    if not cfg or not cfg.no_whop_link_enabled:
        return ""
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return ""

    rid = int(cfg.no_whop_link_members_role_id or 0)
    if rid <= 0:
        await _log("⚠️ support_tickets: no_whop_link scan skipped (members_role_id not configured)")
        return ""
    role = guild.get_role(int(rid))
    if not isinstance(role, discord.Role):
        await _log(f"⚠️ support_tickets: no_whop_link scan skipped (members_role_id not found: {rid})")
        return ""
    client_ok = bool(_WHOP_API_CLIENT) and bool(cfg.no_whop_link_use_whop_api)

    # Throttle by persisted scan state (restart-safe).
    now = _now_utc()
    state = _scan_state_get()
    rec = state.get("no_whop_link") if isinstance(state.get("no_whop_link"), dict) else {}
    last_iso = str((rec or {}).get("last_scan_at_iso") or "").strip()
    last_dt = _parse_iso(last_iso) if last_iso else None
    if (not bool(force)) and last_dt and (now - last_dt) < timedelta(seconds=int(cfg.no_whop_link_scan_interval_seconds)):
        return ""
    _scan_state_set("no_whop_link", {"last_scan_at_iso": _now_iso(), "last_summary": "running"})

    # Exclude staff/providers/admins from no_whop_link tickets (config-driven + safe defaults).
    exclude_role_ids = set(int(x) for x in (cfg.no_whop_link_exclude_role_ids or []) if int(x) > 0)
    exclude_role_ids |= set(int(x) for x in (cfg.staff_role_ids or []) if int(x) > 0)
    exclude_role_ids |= set(int(x) for x in (cfg.admin_role_ids or []) if int(x) > 0)

    members: list[discord.Member] = []
    for m in (guild.members or []):
        if not isinstance(m, discord.Member) or getattr(m, "bot", False):
            continue
        if role not in (m.roles or []):
            continue
        # Skip administrators / staff and any explicitly excluded roles.
        with suppress(Exception):
            perms = getattr(m, "guild_permissions", None)
            if perms and bool(getattr(perms, "administrator", False)):
                continue
        try:
            rids = {int(r.id) for r in (m.roles or [])}
        except Exception:
            rids = set()
        if exclude_role_ids and (rids & exclude_role_ids):
            continue
        members.append(m)
    opened = 0
    already_open = 0
    suppressed = 0
    skipped_linked = 0
    skipped_no_mid = 0
    skipped_no_client = 0
    failed = 0

    # Safeguard: if we cannot resolve linkage signals reliably, abort rather than spamming tickets.
    # We'll compute a rough coverage ratio while scanning.
    checked = 0
    resolved_linked = 0
    resolved_unlinked = 0

    for m in members:
        uid = int(getattr(m, "id", 0) or 0)
        if uid <= 0:
            continue
        mid = _membership_id_from_member_history(int(uid))
        if not str(mid or "").strip():
            # Fallback: open a discord-only ticket (roles + join date).
            skipped_no_mid += 1
            if await has_open_ticket_for_user(ticket_type="no_whop_link", user_id=int(uid)):
                already_open += 1
                continue
            fp = f"{int(uid)}|no_whop_link|no_mid"
            try:
                ch0 = await open_no_whop_link_ticket(member=m, fingerprint=fp, whop_brief=None, note=str(cfg.whop_unlinked_note or "").strip())
                if isinstance(ch0, discord.TextChannel):
                    opened += 1
                elif ch0:
                    suppressed += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
            continue

        # Fetch Whop brief (includes dashboard + total spend + connected discord when available).
        if not client_ok:
            skipped_no_client += 1
            continue
        try:
            brief = await _fetch_whop_brief(_WHOP_API_CLIENT, str(mid), enable_enrichment=True)
        except Exception:
            brief = {}
        if not isinstance(brief, dict) or not brief:
            failed += 1
            continue
        checked += 1

        connected_disp = str(brief.get("connected_discord") or "").strip()
        did_in_whop = ""
        with suppress(Exception):
            m_did = re.search(r"\b(\d{17,19})\b", connected_disp)
            if m_did:
                did_in_whop = m_did.group(1)
        # Fallback: identity cache by email when API doesn't expose connections.
        if (not did_in_whop) and str(brief.get("email") or "").strip():
            with suppress(Exception):
                did2 = _linked_discord_id_from_identity_cache(str(brief.get("email") or ""))
                if did2 > 0:
                    did_in_whop = str(int(did2))

        # If Whop shows a Discord connection matching this member, skip.
        if did_in_whop and did_in_whop.isdigit() and int(did_in_whop) == int(uid):
            skipped_linked += 1
            resolved_linked += 1
            continue
        resolved_unlinked += 1

        # Early abort if it looks like we can't resolve any links (prevents spam when API/cache doesn't expose connections).
        if checked >= 25 and resolved_linked <= 0:
            await _log("⚠️ support_tickets: no_whop_link scan aborted (0 linked resolutions after 25 checks; connection fields likely unavailable)")
            break

        if await has_open_ticket_for_user(ticket_type="no_whop_link", user_id=int(uid)):
            already_open += 1
            continue

        note = ""
        if did_in_whop and did_in_whop.isdigit() and int(did_in_whop) != int(uid):
            note = f"Whop has Discord connected to a different account: `{did_in_whop}`"
        else:
            note = str(cfg.whop_unlinked_note or "").strip()

        fp = f"{int(uid)}|no_whop_link|{str(mid).strip()}"
        try:
            ch = await open_no_whop_link_ticket(member=m, fingerprint=fp, whop_brief=brief, note=note)
            if isinstance(ch, discord.TextChannel):
                opened += 1
                # Optional: also post to member-status-logs for staff visibility (no pings).
                if bool(cfg.no_whop_link_log_to_member_status_logs) and int(cfg.member_status_logs_channel_id or 0) > 0:
                    out_ch = guild.get_channel(int(cfg.member_status_logs_channel_id))
                    if isinstance(out_ch, discord.TextChannel):
                        with suppress(Exception):
                            access = _access_roles_plain(m, {int(r.id) for r in (m.roles or [])})
                            emb = _build_member_status_detailed_embed(
                                title="🧾 No Whop Link (Members role)",
                                member=m,
                                access_roles=access,
                                color=0xFEE75C,
                                discord_kv=[("event", "support_tickets.no_whop_link_scan"), ("ticket", f"<#{int(ch.id)}>")],
                                whop_brief=brief,
                                event_kind="active",
                            )
                            await out_ch.send(embed=emb, allowed_mentions=discord.AllowedMentions.none(), silent=True)
            elif ch:
                suppressed += 1
            else:
                failed += 1
        except Exception:
            failed += 1

    summary = (
        f"members={len(members)} opened={opened} already_open={already_open} "
        f"linked_ok={skipped_linked} no_mid={skipped_no_mid} no_client={skipped_no_client} suppressed={suppressed} failed={failed}"
    )
    await _log(f"🧾 support_tickets: no_whop_link scan {summary}")
    _scan_state_set("no_whop_link", {"last_scan_at_iso": _now_iso(), "last_summary": summary})
    return summary


def _ticket_type_from_topic(topic: object) -> str:
    try:
        m = re.search(r"(?im)^\s*ticket_type\s*=\s*([A-Za-z0-9_]+)\s*$", str(topic or ""))
        return str(m.group(1) or "").strip().lower() if m else ""
    except Exception:
        return ""


async def purge_no_whop_link_open_tickets(
    *,
    do_transcript: bool = True,
    delete_channel: bool = True,
    max_delete: int = 0,
) -> dict:
    """Delete ONLY open `no_whop_link` ticket channels (safe: by ticket index/topic)."""
    if not _ensure_cfg_loaded() or not _BOT:
        return {"deleted": 0, "skipped": 0, "failed": 0}
    cfg = _cfg()
    if not cfg:
        return {"deleted": 0, "skipped": 0, "failed": 0}
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return {"deleted": 0, "skipped": 0, "failed": 0}

    # Gather open no_whop_link tickets from index (no awaits under lock).
    targets: list[int] = []
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            if str(rec.get("ticket_type") or "").strip().lower() != "no_whop_link":
                continue
            cid = _as_int(rec.get("channel_id"))
            if cid > 0:
                targets.append(int(cid))

    # Also include orphaned channels in the no-whop category that still have the marker topic.
    with suppress(Exception):
        cat_id = int(cfg.no_whop_link_category_id or 0)
        if cat_id <= 0:
            cat_id = int(await _get_or_create_no_whop_link_category_id(guild=guild) or 0)
        cat = guild.get_channel(int(cat_id)) if cat_id > 0 else None
        if isinstance(cat, discord.CategoryChannel):
            for ch in list(getattr(cat, "channels", []) or []):
                if not isinstance(ch, discord.TextChannel):
                    continue
                if int(ch.id) in set(targets):
                    continue
                top = getattr(ch, "topic", "") or ""
                if _topic_is_support_ticket(top) and _ticket_type_from_topic(top) == "no_whop_link":
                    targets.append(int(ch.id))

    # Dedup targets, then purge.
    seen: set[int] = set()
    ordered: list[int] = []
    for cid in targets:
        if int(cid) <= 0 or int(cid) in seen:
            continue
        seen.add(int(cid))
        ordered.append(int(cid))

    deleted = 0
    skipped = 0
    failed = 0
    for cid in ordered:
        if int(max_delete or 0) > 0 and deleted >= int(max_delete):
            break
        try:
            ch = guild.get_channel(int(cid))
            if not isinstance(ch, discord.TextChannel):
                skipped += 1
                continue
            # Hard safety: require marker + exact ticket_type.
            top = getattr(ch, "topic", "") or ""
            if (not _topic_is_support_ticket(top)) or (_ticket_type_from_topic(top) != "no_whop_link"):
                skipped += 1
                continue
            await close_ticket_by_channel_id(
                int(cid),
                close_reason="purge_no_whop_link",
                do_transcript=bool(do_transcript),
                delete_channel=bool(delete_channel),
            )
            deleted += 1
        except Exception:
            failed += 1

    await _log(f"🧹 support_tickets: purged no_whop_link tickets deleted={deleted} skipped={skipped} failed={failed}")
    return {"deleted": int(deleted), "skipped": int(skipped), "failed": int(failed)}


async def remove_billing_role_from_no_whop_members(*, billing_role_id: int = 0) -> dict:
    """One-time helper: remove Billing role from members who have an OPEN `no_whop_link` ticket.

    This is ticket-index driven (no role heuristics) to avoid affecting users who do not currently have
    an OPEN No-Whop-Link ticket.
    """
    if not _ensure_cfg_loaded() or not _BOT:
        return {"removed": 0, "skipped": 0, "failed": 0}
    cfg = _cfg()
    if not cfg:
        return {"removed": 0, "skipped": 0, "failed": 0}
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return {"removed": 0, "skipped": 0, "failed": 0}

    bid = int(billing_role_id or 0) or int(cfg.billing_role_id or 0)
    if bid <= 0:
        await _log("⚠️ support_tickets: fix_no_whop_roles skipped (billing_role_id not configured)")
        return {"removed": 0, "skipped": 0, "failed": 0}
    bill_role = guild.get_role(int(bid))
    if not isinstance(bill_role, discord.Role):
        await _log(f"⚠️ support_tickets: fix_no_whop_roles skipped (billing role not found: {bid})")
        return {"removed": 0, "skipped": 0, "failed": 0}

    # Build target user_ids from the ticket index (OPEN no_whop_link tickets only).
    target_uids: set[int] = set()
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            if str(rec.get("ticket_type") or "").strip().lower() != "no_whop_link":
                continue
            uid = _as_int(rec.get("user_id"))
            if uid > 0:
                target_uids.add(int(uid))
    if not target_uids:
        await _log("ℹ️ support_tickets: fix_no_whop_roles no targets (no OPEN no_whop_link tickets)")
        return {"removed": 0, "skipped": 0, "failed": 0}

    removed = 0
    skipped = 0
    failed = 0

    for uid in sorted(list(target_uids)):
        m = guild.get_member(int(uid))
        if not isinstance(m, discord.Member):
            with suppress(Exception):
                m = await guild.fetch_member(int(uid))
        if not isinstance(m, discord.Member):
            skipped += 1
            continue
        try:
            rids = {int(r.id) for r in (m.roles or [])}
        except Exception:
            skipped += 1
            continue
        if int(bid) not in rids:
            skipped += 1
            continue
        try:
            await m.remove_roles(
                bill_role,
                reason="RSCheckerbot: one-time cleanup (OPEN no_whop_link ticket: remove Billing role)",
            )
            removed += 1
        except Exception:
            failed += 1

    await _log(f"🧹 support_tickets: fix_no_whop_roles removed={removed} skipped={skipped} failed={failed} billing_role_id={bid} targets={len(target_uids)}")
    return {"removed": int(removed), "skipped": int(skipped), "failed": int(failed)}


async def handle_free_pass_join_if_needed(
    *,
    member: discord.Member,
    tracked_one_time_invite: bool,
) -> None:
    """Free Pass intake hook (invite-based)."""
    if not _ensure_cfg_loaded():
        return
    if not tracked_one_time_invite:
        return
    # Must be in the ticket guild
    cfg = _cfg()
    if not cfg or not _BOT:
        return
    if int(getattr(getattr(member, "guild", None), "id", 0) or 0) != int(cfg.guild_id):
        return

    # Skip if Whop already linked (best-effort)
    linked = False
    fn = _IS_WHOP_LINKED
    if fn:
        with suppress(Exception):
            linked = bool(fn(int(member.id)))
    if linked:
        return

    fp = f"{int(member.id)}|freepass|{_day_bucket_local(_now_utc())}"
    await open_free_pass_ticket(member=member, fingerprint=fp)


def _transcript_channel_id_for(ticket_type: str) -> int:
    cfg = _cfg()
    if not cfg:
        return 0
    t = str(ticket_type or "").strip().lower()
    if t == "cancellation":
        return int(cfg.cancellation_transcript_channel_id)
    if t == "billing":
        return int(cfg.billing_transcript_channel_id)
    return int(cfg.free_pass_transcript_channel_id)


async def _get_or_create_transcript_channel(*, guild: discord.Guild, ticket_type: str) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg:
        return None

    # Prefer explicit channel IDs.
    ch_id = _transcript_channel_id_for(ticket_type)
    if ch_id:
        base = guild.get_channel(int(ch_id))
        return base if isinstance(base, discord.TextChannel) else None

    # Fallback: create/find channel under transcript category by name.
    cat_id = int(cfg.transcript_category_id or 0)
    if cat_id <= 0:
        return None
    cat = guild.get_channel(int(cat_id))
    if not isinstance(cat, discord.CategoryChannel):
        return None

    suffix = str(ticket_type or "").strip().lower()
    if suffix not in {"cancellation", "billing", "free_pass"}:
        suffix = "tickets"
    nm = _slug_channel_name(f"transcripts-{suffix}", max_len=90)

    for ch in list(cat.channels):
        if isinstance(ch, discord.TextChannel) and str(ch.name or "").lower() == str(nm).lower():
            return ch
    with suppress(Exception):
        created = await guild.create_text_channel(name=nm, category=cat, reason="RSCheckerbot: create transcript channel")
        return created if isinstance(created, discord.TextChannel) else None
    return None


async def export_transcript_for_channel_id(channel_id: int, *, close_reason: str) -> bool:
    if not _ensure_cfg_loaded():
        return False
    cfg = _cfg()
    if not cfg or not _BOT:
        return False
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not guild:
        return False
    ch = guild.get_channel(int(channel_id))
    if not isinstance(ch, discord.TextChannel):
        return False

    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_by_channel_id(db, int(channel_id))
        if not found:
            return False
        tid, rec = found
        if not _ticket_is_open(rec):
            return False
        ticket_type = str(rec.get("ticket_type") or "").strip().lower()
        user_id = _as_int(rec.get("user_id"))
        created_at_iso = str(rec.get("created_at_iso") or "")
        ref_url = str(rec.get("reference_jump_url") or "")
        wym_url = str(rec.get("what_you_missed_jump_url") or "")

    tx_ch = await _get_or_create_transcript_channel(guild=guild, ticket_type=ticket_type)
    if not isinstance(tx_ch, discord.TextChannel):
        await _log(f"⚠️ support_tickets: transcript channel not configured for type={ticket_type}")
        return False

    # Build transcript
    lines: list[str] = []
    lines.append(f"ticket_type={ticket_type}")
    lines.append(f"user_id={user_id}")
    lines.append(f"channel_id={int(channel_id)}")
    lines.append(f"created_at={created_at_iso}")
    lines.append("")

    with suppress(Exception):
        async for m in ch.history(limit=None, oldest_first=True):
            try:
                ts = (m.created_at or _now_utc()).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
                author = str(getattr(getattr(m, "author", None), "display_name", "") or "Unknown")
                aid = int(getattr(getattr(m, "author", None), "id", 0) or 0)
                content = str(m.content or "").rstrip()
                lines.append(f"[{ts}] {author} ({aid}): {content}")
                # embeds (titles + fields)
                for e in (m.embeds or []):
                    et = str(getattr(e, "title", "") or "").strip()
                    if et:
                        lines.append(f"  [embed] title: {et}")
                    ed = str(getattr(e, "description", "") or "").strip()
                    if ed:
                        lines.append(f"  [embed] description: {ed}")
                    for f in (getattr(e, "fields", None) or []):
                        fn = str(getattr(f, "name", "") or "").strip()
                        fv = str(getattr(f, "value", "") or "").strip()
                        if fn or fv:
                            lines.append(f"  [embed] {fn}: {fv}")
                # attachments
                for a in (m.attachments or []):
                    url = str(getattr(a, "url", "") or "")
                    if url:
                        lines.append(f"  [attachment] {url}")
            except Exception:
                continue

    closed_at_iso = _now_iso()
    body = ("\n".join(lines)).encode("utf-8", errors="replace")
    filename = f"transcript_{ticket_type}_{user_id}_{str(channel_id)}.txt"
    file = discord.File(io.BytesIO(body), filename=filename)

    # Summary embed (in transcript channel)
    # Prefer display name when present; keep mention for clickability.
    member_label = f"<@{user_id}>" if user_id else "—"
    member_name = ""
    with suppress(Exception):
        mobj = guild.get_member(int(user_id)) if user_id else None
        if mobj:
            member_name = str(getattr(mobj, "display_name", "") or "").strip()
    e = discord.Embed(
        title=f"Transcript — {ticket_type} — {(member_name or str(user_id or 'unknown'))}",
        color=0x5865F2,
        timestamp=_now_utc(),
    )
    e.add_field(name="Member", value=member_label, inline=False)
    e.add_field(name="Discord ID", value=_format_discord_id(user_id), inline=False)
    e.add_field(name="Ticket Type", value=str(ticket_type), inline=False)
    ch_url = f"https://discord.com/channels/{int(guild.id)}/{int(ch.id)}"
    e.add_field(name="Ticket Channel", value=_embed_link("Open", ch_url), inline=False)
    e.add_field(name="Created At", value=str(created_at_iso or "—")[:1024], inline=False)
    e.add_field(name="Closed At", value=str(closed_at_iso)[:1024], inline=False)
    ref_lines: list[str] = []
    if ref_url:
        ref_lines.append(_embed_link("View Full Log", ref_url))
    if ticket_type == "free_pass" and wym_url:
        ref_lines.append(_embed_link("Open What-You-Missed", wym_url))
    if ref_lines:
        e.add_field(name="Reference Links", value="\n".join(ref_lines)[:1024], inline=False)
    e.add_field(name="Close Reason", value=str(close_reason or "—")[:1024], inline=False)
    e.set_footer(text="RSCheckerbot • Support")

    try:
        await tx_ch.send(embed=e, file=file, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        return False

    # Mark closed in index (channel deletion handled by caller)
    async with _INDEX_LOCK:
        db = _index_load()
        found2 = _ticket_by_channel_id(db, int(channel_id))
        if found2:
            tid2, rec2 = found2
            rec2["status"] = "CLOSED"
            rec2["close_reason"] = str(close_reason or "")
            rec2["closed_at_iso"] = closed_at_iso
            db["tickets"][tid2] = rec2  # type: ignore[index]
            _index_save(db)
    return True


async def close_ticket_by_channel_id(
    channel_id: int,
    *,
    close_reason: str,
    do_transcript: bool = True,
    delete_channel: bool = True,
) -> bool:
    if not _ensure_cfg_loaded():
        return False
    cfg = _cfg()
    if not cfg or not _BOT:
        return False
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not guild:
        return False

    # Capture ticket metadata up-front (for role removal / fallback close).
    ticket_type = ""
    ticket_user_id = 0
    async with _INDEX_LOCK:
        db0 = _index_load()
        found0 = _ticket_by_channel_id(db0, int(channel_id))
        if found0:
            _tid0, rec0 = found0
            ticket_type = str(rec0.get("ticket_type") or "").strip().lower()
            ticket_user_id = _as_int(rec0.get("user_id"))

    ch = guild.get_channel(int(channel_id))
    if not isinstance(ch, discord.TextChannel):
        # Channel gone; mark closed
        async with _INDEX_LOCK:
            db = _index_load()
            found = _ticket_by_channel_id(db, int(channel_id))
            if found:
                tid, rec = found
                if _ticket_is_open(rec):
                    rec["status"] = "CLOSED"
                    rec["close_reason"] = str(close_reason or "channel_missing")
                    rec["closed_at_iso"] = _now_iso()
                    db["tickets"][tid] = rec  # type: ignore[index]
                    _index_save(db)
        # Auto-remove the role tied to this ticket type.
        if ticket_user_id and ticket_type:
            mobj = guild.get_member(int(ticket_user_id))
            if not isinstance(mobj, discord.Member):
                with suppress(Exception):
                    mobj = await guild.fetch_member(int(ticket_user_id))
            if isinstance(mobj, discord.Member):
                with suppress(Exception):
                    await _set_ticket_role_for_member(guild=guild, member=mobj, ticket_type=ticket_type, add=False)
        return True

    if do_transcript:
        ok = await export_transcript_for_channel_id(int(channel_id), close_reason=close_reason)
        if not ok:
            await _log(f"❌ support_tickets: transcript failed; refusing to delete ticket channel_id={channel_id}")
            return False
    else:
        # No transcript requested: still mark closed (keeps index + role state consistent).
        async with _INDEX_LOCK:
            db = _index_load()
            found = _ticket_by_channel_id(db, int(channel_id))
            if found:
                tid, rec = found
                if _ticket_is_open(rec):
                    rec["status"] = "CLOSED"
                    rec["close_reason"] = str(close_reason or "")
                    rec["closed_at_iso"] = _now_iso()
                    db["tickets"][tid] = rec  # type: ignore[index]
                    _index_save(db)

    # Auto-remove the role tied to this ticket type (remove only that role).
    if ticket_user_id and ticket_type:
        mobj = guild.get_member(int(ticket_user_id))
        if not isinstance(mobj, discord.Member):
            with suppress(Exception):
                mobj = await guild.fetch_member(int(ticket_user_id))
        if isinstance(mobj, discord.Member):
            with suppress(Exception):
                await _set_ticket_role_for_member(guild=guild, member=mobj, ticket_type=ticket_type, add=False)

    if delete_channel:
        with suppress(Exception):
            await ch.delete(reason=f"RSCheckerbot: close ticket ({close_reason})")
    return True


async def close_open_ticket_for_user(
    discord_id: int,
    *,
    ticket_type: str,
    close_reason: str,
    do_transcript: bool = True,
    delete_channel: bool = True,
) -> bool:
    """Close the current OPEN ticket of a given type for a user (best-effort)."""
    if not _ensure_cfg_loaded():
        return False
    uid = int(discord_id or 0)
    if uid <= 0:
        return False
    t = str(ticket_type or "").strip().lower()
    if not t:
        return False

    # Never hold the index lock while awaiting close_ticket_by_channel_id (it also needs the lock).
    ch_id = 0
    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_find_open(db, ticket_type=t, user_id=uid, fingerprint="")
        if found:
            _tid, rec = found
            if _ticket_is_open(rec):
                ch_id = _as_int(rec.get("channel_id"))
    if not ch_id:
        return False
    return bool(
        await close_ticket_by_channel_id(
            int(ch_id),
            close_reason=str(close_reason or ""),
            do_transcript=bool(do_transcript),
            delete_channel=bool(delete_channel),
        )
    )


async def close_free_pass_if_whop_linked(
    discord_id: int,
    *,
    resolution_event: str = "whop_linked",
    reference_jump_url: str = "",
) -> None:
    """Resolve Free Pass only when Whop-linked (best-effort)."""
    if not _ensure_cfg_loaded():
        return
    uid = int(discord_id or 0)
    if uid <= 0:
        return
    # Only proceed when we can confirm linkage (prevents false "resolved" on noisy cards).
    if not _IS_WHOP_LINKED:
        return
    linked = False
    with suppress(Exception):
        linked = bool(_IS_WHOP_LINKED(int(uid)))
    if not linked:
        return
    # Never hold the index lock while awaiting close_ticket_by_channel_id (it also needs the lock).
    ch_id = 0
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            if str(rec.get("ticket_type") or "").strip().lower() != "free_pass":
                continue
            if _as_int(rec.get("user_id")) != uid:
                continue
            ch_id = _as_int(rec.get("channel_id"))
            break
    if not ch_id:
        return
    # Prefer follow-up + role removal, then let the sweeper auto-close after grace.
    with suppress(Exception):
        await post_resolution_followup_and_remove_role(
            discord_id=int(uid),
            ticket_type="free_pass",
            resolution_event=str(resolution_event or "whop_linked"),
            reference_jump_url=str(reference_jump_url or ""),
        )
    return


async def close_no_whop_link_if_linked(
    discord_id: int,
    *,
    resolution_event: str = "whop_linked",
    reference_jump_url: str = "",
) -> None:
    """Close no_whop_link ticket when linkage is confirmed (triggered from member-status-logs)."""
    if not _ensure_cfg_loaded():
        return
    uid = int(discord_id or 0)
    if uid <= 0:
        return
    cfg = _cfg()
    if not cfg or not _BOT:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return

    # Find open no_whop_link ticket channel id under lock.
    ch_id = 0
    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_find_open(db, ticket_type="no_whop_link", user_id=uid, fingerprint="")
        if found:
            _tid, rec = found
            ch_id = _as_int(rec.get("channel_id"))
    if not ch_id:
        return

    # Close (this also removes the no-whop role). We still post a small update into transcript.
    with suppress(Exception):
        await post_resolution_followup_and_remove_role(
            discord_id=int(uid),
            ticket_type="no_whop_link",
            resolution_event=str(resolution_event or "whop_linked"),
            reference_jump_url=str(reference_jump_url or ""),
        )
    with suppress(Exception):
        await close_ticket_by_channel_id(int(ch_id), close_reason=str(resolution_event or "whop_linked"), do_transcript=True, delete_channel=True)
    return


async def sweep_no_whop_link_cleanup(*, force: bool = False) -> str:
    """Cleanup no_whop_link: close tickets + remove No Whop role once Whop is linked."""
    if not _ensure_cfg_loaded() or not _BOT:
        return "skipped:not_ready"
    cfg = _cfg()
    if not cfg:
        return "skipped:no_cfg"
    if not _IS_WHOP_LINKED:
        return "skipped:no_is_whop_linked"

    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return "skipped:no_guild"

    # Throttle: run at most once per 10 minutes unless forced.
    state = _scan_state_get()
    rec = state.get("no_whop_link_cleanup") if isinstance(state.get("no_whop_link_cleanup"), dict) else {}
    last_iso = str((rec or {}).get("last_run_at_iso") or "").strip()
    last_dt = _parse_iso(last_iso) if last_iso else None
    now = _now_utc()
    if (not bool(force)) and last_dt and (now - last_dt) < timedelta(seconds=600):
        return "skipped:throttle"
    _scan_state_set("no_whop_link_cleanup", {"last_run_at_iso": _now_iso(), "last_summary": "running"})

    # 1) Close OPEN no_whop_link tickets whose owner is now linked.
    to_close: list[tuple[int, int]] = []  # (channel_id, user_id)
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, r in _ticket_iter(db):
            if not _ticket_is_open(r):
                continue
            if str(r.get("ticket_type") or "").strip().lower() != "no_whop_link":
                continue
            ch_id = _as_int(r.get("channel_id"))
            uid = _as_int(r.get("user_id"))
            if ch_id and uid:
                to_close.append((ch_id, uid))

    closed = 0
    skipped = 0
    failed = 0
    for ch_id, uid in to_close:
        linked = False
        with suppress(Exception):
            linked = bool(_IS_WHOP_LINKED(int(uid)))
        if not linked:
            skipped += 1
            continue
        try:
            ok = await close_ticket_by_channel_id(
                int(ch_id),
                close_reason="whop_linked",
                do_transcript=True,
                delete_channel=True,
            )
            if ok:
                closed += 1
            else:
                failed += 1
        except Exception:
            failed += 1

    # 2) Remove stale No Whop role from members who are now linked (even if no ticket exists).
    role_removed = 0
    role_skipped = 0
    rid = int(cfg.no_whop_link_role_id or 0)
    role = guild.get_role(int(rid)) if rid else None
    if isinstance(role, discord.Role):
        for m in list(getattr(role, "members", []) or []):
            if not isinstance(m, discord.Member):
                continue
            linked = False
            with suppress(Exception):
                linked = bool(_IS_WHOP_LINKED(int(m.id)))
            if not linked:
                role_skipped += 1
                continue
            with suppress(Exception):
                await m.remove_roles(role, reason="RSCheckerbot: Whop linked; remove No Whop role")
                role_removed += 1

    summary = f"closed={closed} skipped={skipped} failed={failed} role_removed={role_removed} role_skipped={role_skipped}"
    _scan_state_set("no_whop_link_cleanup", {"last_run_at_iso": _now_iso(), "last_summary": summary})
    with suppress(Exception):
        await _log(f"🧹 support_tickets: no_whop_link cleanup {summary}")
    return summary


async def sweep_free_pass_tickets() -> None:
    """Periodic sweeper: inactivity and whop-linked closure for Free Pass tickets."""
    if not _ensure_cfg_loaded():
        return
    # Always run startup-message sweeper (config-gated; restart-safe).
    with suppress(Exception):
        await sweep_startup_messages()
    # Always run no-whop-link scan (config-gated; restart-safe).
    with suppress(Exception):
        await sweep_no_whop_link_scan()
    # Always run no-whop-link cleanup (restart-safe): close resolved tickets + remove stale roles.
    with suppress(Exception):
        await sweep_no_whop_link_cleanup()
    # Always run resolved-ticket sweeper (config-gated; restart-safe).
    with suppress(Exception):
        await sweep_resolved_tickets()

    cfg = _cfg()
    if not cfg or not cfg.auto_delete_enabled:
        return

    now = _now_utc()
    # Copy candidates under lock (avoid holding lock across network calls)
    candidates: list[tuple[int, int, str]] = []  # (channel_id, user_id, last_activity_iso)
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            if str(rec.get("ticket_type") or "").strip().lower() != "free_pass":
                continue
            ch_id = _as_int(rec.get("channel_id"))
            uid = _as_int(rec.get("user_id"))
            last_iso = str(rec.get("last_activity_at_iso") or rec.get("created_at_iso") or "")
            if ch_id and uid:
                candidates.append((ch_id, uid, last_iso))

    for ch_id, uid, last_iso in candidates:
        # Condition B: Whop-linked
        if cfg.delete_on_whop_linked and _IS_WHOP_LINKED:
            linked = False
            with suppress(Exception):
                linked = bool(_IS_WHOP_LINKED(int(uid)))
            if linked:
                # Post follow-up + role removal; channel will be auto-closed by resolved sweeper.
                with suppress(Exception):
                    await post_resolution_followup_and_remove_role(
                        discord_id=int(uid),
                        ticket_type="free_pass",
                        resolution_event="whop_linked",
                    )
                continue

        # Condition A: inactivity
        last_dt = _parse_iso(last_iso) or now
        if (now - last_dt) >= timedelta(seconds=int(cfg.inactivity_seconds)):
            await close_ticket_by_channel_id(int(ch_id), close_reason="inactivity", do_transcript=True, delete_channel=True)

    # Note: sweep_resolved_tickets() is called near the top so it runs even when free-pass auto-delete is disabled.


async def sweep_resolved_tickets() -> None:
    """Auto-close tickets that have a resolved follow-up and no human replies after grace."""
    if not _ensure_cfg_loaded() or not _BOT:
        return
    cfg = _cfg()
    if not cfg or not cfg.resolution_followup_enabled:
        return
    delay_s = int(cfg.resolution_followup_auto_close_after_seconds or 0)
    if delay_s <= 0:
        return
    guild = _BOT.get_guild(int(cfg.guild_id))
    if not isinstance(guild, discord.Guild):
        return
    now = _now_utc()

    to_close: list[tuple[int, str]] = []  # (channel_id, close_reason)
    async with _INDEX_LOCK:
        db = _index_load()
        for _tid, rec in _ticket_iter(db):
            if not _ticket_is_open(rec):
                continue
            ttype = str(rec.get("ticket_type") or "").strip().lower()
            if ttype not in {"billing", "cancellation", "free_pass"}:
                continue
            sent_iso = str(rec.get("resolved_followup_sent_at_iso") or "").strip()
            if not sent_iso:
                continue
            sent_dt = _parse_iso(sent_iso) or None
            if not sent_dt:
                continue
            if (now - sent_dt) < timedelta(seconds=delay_s):
                continue
            last_iso = str(rec.get("last_activity_at_iso") or rec.get("created_at_iso") or "").strip()
            last_dt = _parse_iso(last_iso) or now
            # Only close if no human activity after the follow-up was posted.
            if last_dt > sent_dt:
                continue
            ch_id = _as_int(rec.get("channel_id"))
            ev = str(rec.get("resolved_event") or "").strip()
            if ch_id:
                to_close.append((int(ch_id), f"resolved:{ev or 'ok'}"))

    for ch_id, reason in to_close:
        with suppress(Exception):
            await close_ticket_by_channel_id(int(ch_id), close_reason=reason, do_transcript=True, delete_channel=True)


async def get_ticket_record_for_channel_id(channel_id: int) -> dict | None:
    async with _INDEX_LOCK:
        db = _index_load()
        found = _ticket_by_channel_id(db, int(channel_id))
        if not found:
            return None
        _tid, rec = found
        return dict(rec)


def staff_check_for_ctx() -> commands.Check:
    """discord.py commands check for ticket staff."""

    async def _check(ctx: commands.Context) -> bool:
        try:
            if not ctx or not ctx.guild or not ctx.author:
                return False
            if not isinstance(ctx.author, discord.Member):
                return False
            return _is_staff_member(ctx.author)
        except Exception:
            return False

    return commands.check(_check)

