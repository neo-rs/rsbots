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
from ticket_channels import ensure_ticket_like_channel as _ensure_ticket_like_channel
from ticket_channels import slug_channel_name as _slug_channel_name


BASE_DIR = Path(__file__).resolve().parent
INDEX_PATH = BASE_DIR / "data" / "tickets_index.json"

_INDEX_LOCK: asyncio.Lock = asyncio.Lock()


@dataclass(frozen=True)
class SupportTicketConfig:
    guild_id: int
    staff_role_ids: list[int]
    admin_role_ids: list[int]
    include_ticket_owner_in_channel: bool
    cancellation_category_id: int
    billing_category_id: int
    free_pass_category_id: int
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
    cooldown_billing_seconds: int
    cooldown_cancellation_seconds: int
    startup_enabled: bool
    startup_delay_seconds: int
    startup_recent_history_limit: int
    startup_templates: dict[str, str]
    audit_enabled: bool
    audit_channel_id: int
    audit_channel_name: str
    audit_include_transcript_category: bool
    billing_role_id: int
    cancellation_role_id: int
    free_pass_no_whop_role_id: int
    resolution_followup_enabled: bool
    resolution_followup_auto_close_after_seconds: int
    resolution_followup_templates: dict[str, str]


_BOT: commands.Bot | None = None
_CFG: SupportTicketConfig | None = None
_LOG_FUNC = None  # async callable(str) -> None
_IS_WHOP_LINKED = None  # callable(discord_id:int) -> bool
_TZ_NAME = "UTC"
_CONTROLS_VIEW: "SupportTicketControlsView | None" = None


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
) -> None:
    """Initialize the support ticket subsystem.

    This is called from RSCheckerbot/main.py after config load.
    """
    global _BOT, _CFG, _LOG_FUNC, _IS_WHOP_LINKED, _TZ_NAME
    _BOT = bot
    _LOG_FUNC = log_func
    _IS_WHOP_LINKED = is_whop_linked
    _TZ_NAME = str(timezone_name or "UTC").strip() or "UTC"

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
    rf = st.get("resolution_followup") if isinstance(st.get("resolution_followup"), dict) else {}
    rf_templates = rf.get("templates") if isinstance(rf.get("templates"), dict) else {}
    al = st.get("audit_logs") if isinstance(st.get("audit_logs"), dict) else {}
    tr = st.get("ticket_roles") if isinstance(st.get("ticket_roles"), dict) else {}

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
        admin_role_ids=_int_list(perms.get("admin_role_ids")),
        include_ticket_owner_in_channel=_as_bool(perms.get("include_ticket_owner_in_channel", True)),
        cancellation_category_id=_as_int(cats.get("cancellation_category_id")),
        billing_category_id=_as_int(cats.get("billing_category_id")),
        free_pass_category_id=_as_int(cats.get("free_pass_category_id")),
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
        cooldown_billing_seconds=max(0, _as_int((dd.get("billing") or {}).get("cooldown_seconds")) or 21600),
        cooldown_cancellation_seconds=max(0, _as_int((dd.get("cancellation") or {}).get("cooldown_seconds")) or 86400),
        startup_enabled=_as_bool(sm.get("enabled")),
        startup_delay_seconds=max(5, _as_int(sm.get("delay_seconds")) or 300),
        startup_recent_history_limit=max(10, min(200, _as_int(sm.get("recent_history_limit")) or 50)),
        startup_templates={str(k).strip().lower(): str(v) for k, v in (sm_templates or {}).items() if str(k or "").strip()},
        audit_enabled=_as_bool(al.get("enabled")),
        audit_channel_id=_as_int(al.get("channel_id")),
        audit_channel_name=str(al.get("channel_name") or "tickets-logs").strip() or "tickets-logs",
        audit_include_transcript_category=_as_bool(al.get("include_transcript_category")),
        billing_role_id=_as_int(tr.get("billing_role_id")),
        cancellation_role_id=_as_int(tr.get("cancellation_role_id")),
        free_pass_no_whop_role_id=_as_int(tr.get("free_pass_no_whop_role_id")),
        resolution_followup_enabled=_as_bool(rf.get("enabled")),
        resolution_followup_auto_close_after_seconds=max(0, _as_int(rf.get("auto_close_after_seconds")) or 1800),
        resolution_followup_templates={str(k).strip().lower(): str(v) for k, v in (rf_templates or {}).items() if str(k or "").strip()},
    )

    # Register persistent view so buttons survive restarts.
    global _CONTROLS_VIEW
    if _BOT and _CONTROLS_VIEW is None:
        with suppress(Exception):
            _CONTROLS_VIEW = SupportTicketControlsView()
            _BOT.add_view(_CONTROLS_VIEW)

    # Best-effort: ensure ticket index file is writable (avoid silent failures later).
    try:
        _index_save(_index_load())
    except Exception as e:
        # Keep the bot running, but surface why tickets might not open.
        with suppress(Exception):
            asyncio.create_task(_log(f"❌ support_tickets: failed to init tickets_index.json (OneDrive lock?) err={str(e)[:220]}"))


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

        # Guard: skip if any human spoke since creation.
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
        content = tmpl.replace("{mention}", mention).strip()
        if not content:
            continue

        ok = True
        try:
            await ch.send(
                content=content[:1900],
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
        except Exception:
            ok = False

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
    if t == "billing":
        return int(c.cooldown_billing_seconds)
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
    if ttype not in {"billing", "cancellation", "free_pass"}:
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

    # Follow-up embed + buttons in-ticket (ping member + support).
    tmpl = _resolution_followup_template(ttype)
    desc = tmpl.strip() if tmpl else "Update: this ticket appears resolved. If you still have concerns, reply here — otherwise Support can close."
    e = discord.Embed(title="Update", description=desc[:4096], color=0x57F287)
    if resolution_event:
        e.add_field(name="Trigger", value=str(resolution_event)[:1024], inline=True)
    if reference_jump_url:
        e.add_field(name="Source", value=_embed_link("View Full Log", str(reference_jump_url)), inline=True)
    view = _CONTROLS_VIEW or SupportTicketControlsView()
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
            if uid <= 0 or ttype not in {"billing", "cancellation", "free_pass"}:
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
    async with _INDEX_LOCK:
        db = _index_load()
        existing = _ticket_find_open(db, ticket_type=ticket_type, user_id=int(owner.id), fingerprint=fingerprint)
        if existing and cfg.dedupe_enabled:
            _tid, rec = existing
            ch_id = _as_int(rec.get("channel_id"))
            existing_ticket_id = str(rec.get("ticket_id") or _tid or "").strip()
            ch = guild.get_channel(int(ch_id)) if ch_id else None
            if isinstance(ch, discord.TextChannel):
                # Ensure the correct per-ticket role is applied even on dedupe.
                with suppress(Exception):
                    await _set_ticket_role_for_member(guild=guild, member=owner, ticket_type=ticket_type, add=True)
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

        # Initial messages (minimal)
        if cfg.include_ticket_owner_in_channel:
            with suppress(Exception):
                role_mention = _support_ping_role_mention()
                ping = " ".join([x for x in [f"<@{int(owner.id)}>", role_mention] if str(x or "").strip()])
                await ch.send(
                    content=ping,
                    allowed_mentions=discord.AllowedMentions(users=True, roles=True, everyone=False),
                )
        # Preview + controls (single message, buttons attached; no command text)
        with suppress(Exception):
            view = _CONTROLS_VIEW or SupportTicketControlsView()
            await ch.send(content="", embed=preview_embed, view=view, silent=True)
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

    # Only show Remaining Days / Access Ends On when they are both present and meaningful.
    st_key = _norm_whop_status_key(str(b.get("status") or ""))
    cap = str(b.get("cancel_at_period_end") or "").strip().lower()
    is_canceling = st_key == "canceling" or (st_key == "active" and cap in {"yes", "true", "1"})
    days = str(b.get("remaining_days") or "").strip()
    end = str(b.get("renewal_end") or "").strip()
    if is_canceling and days and days != "—" and end and end != "—":
        e.add_field(name="Remaining Days", value=days[:1024], inline=True)
        e.add_field(name="Access Ends On", value=end[:1024], inline=True)
    reason = str(cancellation_reason or b.get("cancellation_reason") or "").strip()
    if reason:
        e.add_field(name="Cancellation Reason", value=f"```\n{reason[:950]}\n```", inline=False)
    e.add_field(name="Whop Dashboard", value=_embed_link("Open", str(b.get("dashboard_url") or "")), inline=True)
    if reference_jump_url:
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
    with suppress(Exception):
        roles_txt = _access_roles_plain(member, {int(r.id) for r in (member.roles or [])})
        if roles_txt and roles_txt != "—":
            e.add_field(name="Current Roles", value=str(roles_txt)[:1024], inline=False)
    e.add_field(name="Event", value=str(event_type or "—")[:1024], inline=True)
    e.add_field(
        name="Whop Status",
        value=_whop_status_display(ticket_type="billing", whop_brief=b, status_override=str(status or ""))[:1024],
        inline=True,
    )
    if reference_jump_url:
        e.add_field(name="Message Reference", value=_embed_link("View Full Log", reference_jump_url), inline=True)
    return e


def build_free_pass_header_embed(
    *,
    member: discord.Member,
    what_you_missed_jump_url: str,
) -> discord.Embed:
    e = discord.Embed(title="Free Pass", color=0x5865F2)
    e.add_field(name="Member", value=str(getattr(member, "display_name", "") or str(member))[:1024], inline=True)
    e.add_field(name="Discord ID", value=_format_discord_id(int(member.id)), inline=True)
    e.add_field(name="Whop Status", value="Not linked", inline=True)
    return e


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
) -> discord.TextChannel | None:
    cfg = _cfg()
    if not cfg:
        return None

    preview_embed, source_jump = await _build_what_you_missed_preview_embed()
    header = build_free_pass_header_embed(member=member, what_you_missed_jump_url=source_jump)
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
    )


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


async def sweep_free_pass_tickets() -> None:
    """Periodic sweeper: inactivity and whop-linked closure for Free Pass tickets."""
    if not _ensure_cfg_loaded():
        return
    # Always run startup-message sweeper (config-gated; restart-safe).
    with suppress(Exception):
        await sweep_startup_messages()
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

    # Auto-close resolved tickets after grace (all types).
    with suppress(Exception):
        await sweep_resolved_tickets()


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

