"""Waitlist staff cards: correlate `#whop-logs` (new entry + identity) with `#whop-membership-logs` (lifecycle).

Canonical JSON: `data/waitlist_state.json` (email / Whop user correlation + lifecycle).
"""
from __future__ import annotations

import asyncio
import logging
import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Coroutine

import discord
from discord.ext import commands

from rschecker_utils import load_json as _load_json
from rschecker_utils import save_json as _save_json

log = logging.getLogger("rs-checker")

BASE_DIR = Path(__file__).resolve().parent
WAITLIST_STATE_PATH = BASE_DIR / "data" / "waitlist_state.json"
WAITLIST_DEDUPE_PATH = BASE_DIR / "data" / "waitlist_processed_messages.json"

_WAITLIST_LOCK = asyncio.Lock()
_DEDUPE_LOCK = asyncio.Lock()

_COUNT_CACHE: dict[str, Any] = {"at": 0.0, "counts": {}, "err": ""}

_DISCORD_ID_RE = re.compile(r"\b(\d{17,19})\b")
_USER_RE = re.compile(r"\b(user_[A-Za-z0-9]+)\b")
_PLAN_RE = re.compile(r"\b(plan_[A-Za-z0-9]+)\b")


@dataclass(frozen=True)
class WaitlistLoggingConfig:
    enabled: bool
    output_channel_id: int
    whop_logs_channel_id: int
    membership_logs_channel_id: int
    waitlist_role_id: int
    api_counts_enabled: bool
    api_counts_cache_seconds: float
    dedupe_max: int


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(s: object) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())[:2048]


def _field_map(embed: discord.Embed) -> dict[str, str]:
    out: dict[str, str] = {}
    for f in getattr(embed, "fields", None) or []:
        n = _norm(getattr(f, "name", "") or "").lower()
        v = _norm(getattr(f, "value", "") or "")
        if n:
            out[n] = v
    return out


def _blob(embed: discord.Embed) -> str:
    parts = [
        _norm(getattr(embed, "title", "") or ""),
        _norm(getattr(embed, "description", "") or ""),
    ]
    if embed.author:
        with suppress(Exception):
            parts.append(_norm(embed.author.name or ""))
    return "\n".join(parts).lower()


def is_whop_logs_new_waitlist_entry(embed: discord.Embed) -> bool:
    return "new waitlist entry" in _blob(embed)


def parse_whop_logs_waitlist_identity(embed: discord.Embed) -> dict[str, str]:
    """Extract plan, email, discord id/username from Whop Events card."""
    fm = _field_map(embed)
    email = ""
    plan = ""
    access_pass = ""
    discord_id = ""
    discord_username = ""
    for k, v in fm.items():
        kl = k.lower()
        if kl == "email" or "email" in kl:
            if "@" in v:
                email = v.strip().lower()
        elif kl == "plan" or kl.endswith(" plan"):
            plan = v
        elif "access pass" in kl:
            access_pass = v
        elif kl == "discord id" or kl.endswith("discord id"):
            m = _DISCORD_ID_RE.search(v)
            discord_id = m.group(1) if m else ""
            if "no discord" in v.lower():
                discord_id = ""
        elif kl == "discord username" or "username" in kl:
            if "no discord" not in v.lower():
                discord_username = v
    if not email:
        with suppress(Exception):
            from whop_webhook_handler import _extract_email_from_embed

            email = str(_extract_email_from_embed(embed) or "").strip().lower()
    return {
        "email": email,
        "plan": plan or "",
        "access_pass": access_pass or "",
        "discord_id": discord_id,
        "discord_username": discord_username or "",
    }


def classify_membership_waitlist_title(title: str) -> str:
    t = str(title or "").strip().lower()
    if t.startswith("waitlist approved"):
        return "approved"
    if t.startswith("waitlist denied"):
        return "denied"
    if "waitlist entry created" in t or t.startswith("waitlist entry created"):
        return "created"
    return ""


def extract_footer_debug_ids(embed: discord.Embed, desc: str) -> tuple[str, str]:
    blob = ""
    with suppress(Exception):
        blob = str(getattr(getattr(embed, "footer", None), "text", "") or "")
    blob = f"{blob}\n{desc or ''}"
    um = _USER_RE.search(blob)
    pm = _PLAN_RE.search(blob)
    return ((um.group(1) if um else "").strip(), (pm.group(1) if pm else "").strip())


def parse_membership_identity(embed: discord.Embed, desc: str) -> dict[str, str]:
    fm = _field_map(embed)
    email = ""
    username = ""
    name = ""
    for k, v in fm.items():
        kl = k.lower()
        if kl == "email" or ("email" in kl and "discord" not in kl):
            if "@" in v:
                email = v.strip().lower()
        elif kl == "username":
            username = v.strip()
        elif kl in {"name", "full name"}:
            name = v.strip()
    uid, pid = extract_footer_debug_ids(embed, desc)
    if not email:
        m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", desc or "", re.I)
        if m:
            email = m.group(1).strip().lower()
    return {
        "whop_user_id": uid,
        "plan_id": pid,
        "email": email,
        "username": username,
        "name": name,
    }


def _load_state() -> dict:
    raw = _load_json(WAITLIST_STATE_PATH)
    return raw if isinstance(raw, dict) else {}


def _save_state(db: dict) -> None:
    db = db if isinstance(db, dict) else {}
    db.setdefault("meta", {})
    if isinstance(db["meta"], dict):
        db["meta"]["updated_at"] = _iso_now()
    db.setdefault("by_whop_user", {})
    db.setdefault("by_email", {})
    WAITLIST_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _save_json(WAITLIST_STATE_PATH, db)


def _dedupe_load() -> dict:
    raw = _load_json(WAITLIST_DEDUPE_PATH)
    return raw if isinstance(raw, dict) else {}


def _dedupe_save(rec: dict) -> None:
    WAITLIST_DEDUPE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _save_json(WAITLIST_DEDUPE_PATH, rec)


async def _was_processed(channel_id: int, message_id: int, *, max_keys: int) -> bool:
    key = f"{int(channel_id)}:{int(message_id)}"
    async with _DEDUPE_LOCK:
        rec = _dedupe_load()
        keys: list[str] = list(rec.get("keys") or []) if isinstance(rec.get("keys"), list) else []
        if key in keys:
            return True
        keys.append(key)
        if len(keys) > int(max_keys):
            keys = keys[-int(max_keys) :]
        rec["keys"] = keys
        _dedupe_save(rec)
        return False


async def _resolve_footer_counts(
    fetch_counts: Callable[[], Coroutine[Any, Any, tuple[dict[str, int], str]]] | None,
    *,
    cache_seconds: float,
    api_enabled: bool,
) -> tuple[str, str]:
    """Return (footer_suffix, error snippet)."""
    if not api_enabled or fetch_counts is None:
        return ("Whop counts: disabled in config", "")
    now = datetime.now(timezone.utc).timestamp()
    try:
        ttl = float(cache_seconds)
    except Exception:
        ttl = 900.0
    if ttl <= 0:
        ttl = 900.0
    global _COUNT_CACHE
    if (now - float(_COUNT_CACHE.get("at") or 0)) < ttl and isinstance(_COUNT_CACHE.get("counts"), dict):
        c = _COUNT_CACHE["counts"]
        if all(str(k) in c for k in ("pending", "approved", "denied")):
            return (
                f"Whop waitlist entries — pending={c['pending']} approved={c['approved']} denied={c['denied']}",
                str(_COUNT_CACHE.get("err") or ""),
            )
    try:
        counts, err = await fetch_counts()
    except Exception as e:
        counts, err = {}, str(e)[:120]
    if isinstance(counts, dict) and counts:
        _COUNT_CACHE = {"at": now, "counts": counts, "err": err}
        return (
            f"Whop waitlist entries — pending={counts.get('pending', '—')} approved={counts.get('approved', '—')} denied={counts.get('denied', '—')}",
            err,
        )
    return ("Whop counts: unavailable (API)", err)


def _set_footer(embed: discord.Embed, base: str, counts_line: str, err: str) -> None:
    parts = ["RSCheckerbot", "Waitlist", base, counts_line]
    if err:
        parts.append(f"api_err={err[:80]}")
    embed.set_footer(text=" • ".join(p for p in parts if p)[:2048])


async def _send_waitlist_embed(
    bot: commands.Bot,
    guild_id: int,
    channel_id: int,
    embed: discord.Embed,
    *,
    content: str = "",
) -> None:
    if int(channel_id or 0) <= 0:
        return
    g = bot.get_guild(int(guild_id))
    if not isinstance(g, discord.Guild):
        with suppress(Exception):
            g = await bot.fetch_guild(int(guild_id))
    if not isinstance(g, discord.Guild):
        return
    ch = g.get_channel(int(channel_id))
    if ch is None:
        with suppress(Exception):
            ch = await bot.fetch_channel(int(channel_id))
    if not isinstance(ch, discord.TextChannel):
        return
    allow = discord.AllowedMentions.none()
    with suppress(Exception):
        await ch.send(content=content[:2000] if content else None, embed=embed, allowed_mentions=allow)


async def _apply_waitlist_role(
    guild: discord.Guild,
    member: discord.Member,
    role_id: int,
    *,
    add: bool,
) -> tuple[bool, str]:
    rid = int(role_id or 0)
    if rid <= 0:
        return (False, "no_role_configured")
    role = guild.get_role(rid)
    if not role:
        return (False, "role_not_found")
    me = guild.me
    if not isinstance(me, discord.Member) or not me.guild_permissions.manage_roles:
        return (False, "bot_missing_manage_roles")
    try:
        if me.top_role.position <= role.position:
            return (False, "role_hierarchy")
    except Exception:
        return (False, "role_hierarchy_check_failed")
    try:
        if add:
            if role not in member.roles:
                await member.add_roles(role, reason="RSCheckerbot: waitlist signup")
        else:
            if role in member.roles:
                await member.remove_roles(role, reason="RSCheckerbot: waitlist lifecycle")
        return (True, "ok")
    except Exception as e:
        return (False, str(e)[:120])


def _merge_state(
    db: dict,
    *,
    whop_user_id: str,
    email: str,
    plan: str,
    plan_id: str,
    discord_id: str,
    status: str,
) -> None:
    by_u = db.setdefault("by_whop_user", {})
    by_e = db.setdefault("by_email", {})
    wuid = str(whop_user_id or "").strip()
    em = str(email or "").strip().lower()
    rec: dict[str, Any] = {}
    if wuid.startswith("user_") and wuid in by_u and isinstance(by_u[wuid], dict):
        rec = dict(by_u[wuid])
    elif em and em in by_e and isinstance(by_e[em], dict):
        rec = dict(by_e[em])
    if em and "@" in em:
        rec["email"] = em
    if plan:
        rec["plan"] = plan[:512]
    if plan_id:
        rec["plan_id"] = plan_id[:128]
    did = str(discord_id or "").strip()
    if did.isdigit():
        rec["discord_id"] = did
    if wuid.startswith("user_"):
        rec["whop_user_id"] = wuid
    st = str(status or "").strip().lower()
    if st:
        rec["status"] = st
    rec["updated_at"] = _iso_now()
    if wuid.startswith("user_"):
        by_u[wuid] = rec
    if em and "@" in em:
        by_e[em] = rec


async def process_whop_logs_message(
    bot: commands.Bot,
    message: discord.Message,
    *,
    guild_id: int,
    cfg: WaitlistLoggingConfig,
    fetch_counts: Callable[[], Coroutine[Any, Any, tuple[dict[str, int], str]]] | None,
) -> None:
    if not cfg.enabled:
        return
    if int(message.channel.id) != int(cfg.whop_logs_channel_id):
        return
    if not message.embeds:
        return
    e0 = message.embeds[0]
    if not isinstance(e0, discord.Embed):
        return
    if not is_whop_logs_new_waitlist_entry(e0):
        return

    ids = parse_whop_logs_waitlist_identity(e0)
    email = ids.get("email") or ""
    if not email or "@" not in email:
        return
    if await _was_processed(message.channel.id, message.id, max_keys=cfg.dedupe_max):
        return

    counts_line, cerr = await _resolve_footer_counts(fetch_counts, cache_seconds=cfg.api_counts_cache_seconds, api_enabled=cfg.api_counts_enabled)

    async with _WAITLIST_LOCK:
        db = _load_state()
        _merge_state(
            db,
            whop_user_id="",
            email=email,
            plan=str(ids.get("plan") or ""),
            plan_id="",
            discord_id=str(ids.get("discord_id") or ""),
            status="pending",
        )
        _save_state(db)

    guild = bot.get_guild(int(guild_id))
    mem = None
    did_s = str(ids.get("discord_id") or "").strip()
    if guild and did_s.isdigit():
        mem = guild.get_member(int(did_s))
        if mem is None:
            with suppress(Exception):
                mem = await guild.fetch_member(int(did_s))

    role_note = "skipped (no Discord on card)"
    if cfg.waitlist_role_id and mem and did_s.isdigit():
        ok, why = await _apply_waitlist_role(guild, mem, cfg.waitlist_role_id, add=True)  # type: ignore[arg-type]
        role_note = "assigned waitlist role" if ok else f"role not applied ({why})"

    embed = discord.Embed(
        title="📋 Waitlist — New entry (Whop Events)",
        color=0x57F287,
        timestamp=datetime.now(timezone.utc),
    )
    if mem:
        with suppress(Exception):
            from staff_embeds import apply_member_header

            apply_member_header(embed, mem)
    embed.add_field(name="Plan", value=str(ids.get("plan") or "—")[:1024], inline=False)
    embed.add_field(name="Email", value=f"`{email}`", inline=True)
    embed.add_field(name="Discord ID", value=f"`{ids.get('discord_id') or '—'}`", inline=True)
    embed.add_field(name="Discord username (Whop)", value=str(ids.get("discord_username") or "—")[:1024], inline=False)
    embed.add_field(name="Source", value=str(message.jump_url or "")[:1024], inline=False)
    embed.add_field(name="Automation", value=_norm(role_note)[:1024], inline=False)
    _set_footer(embed, "event=new_waitlist_entry", counts_line, cerr)

    content = ""
    if mem:
        content = mem.mention
    await _send_waitlist_embed(bot, guild_id, cfg.output_channel_id, embed, content=content)


async def process_membership_logs_message(
    bot: commands.Bot,
    message: discord.Message,
    *,
    guild_id: int,
    cfg: WaitlistLoggingConfig,
    fetch_counts: Callable[[], Coroutine[Any, Any, tuple[dict[str, int], str]]] | None,
) -> None:
    if not cfg.enabled:
        return
    if int(message.channel.id) != int(cfg.membership_logs_channel_id):
        return
    if not message.embeds:
        return
    e0 = message.embeds[0]
    if not isinstance(e0, discord.Embed):
        return
    title = str(getattr(e0, "title", "") or "")
    kind = classify_membership_waitlist_title(title)
    if not kind:
        return
    if await _was_processed(message.channel.id, message.id, max_keys=cfg.dedupe_max):
        return

    desc = str(getattr(e0, "description", "") or "")
    mid = parse_membership_identity(e0, desc)
    wuid = str(mid.get("whop_user_id") or "").strip()
    email = str(mid.get("email") or "").strip().lower()

    counts_line, cerr = await _resolve_footer_counts(fetch_counts, cache_seconds=cfg.api_counts_cache_seconds, api_enabled=cfg.api_counts_enabled)

    async with _WAITLIST_LOCK:
        db = _load_state()
        if kind == "created":
            _merge_state(
                db,
                whop_user_id=wuid,
                email=email,
                plan="",
                plan_id=str(mid.get("plan_id") or ""),
                discord_id="",
                status="pending",
            )
        elif kind == "approved":
            _merge_state(
                db,
                whop_user_id=wuid,
                email=email,
                plan="",
                plan_id=str(mid.get("plan_id") or ""),
                discord_id="",
                status="approved",
            )
        elif kind == "denied":
            _merge_state(
                db,
                whop_user_id=wuid,
                email=email,
                plan="",
                plan_id=str(mid.get("plan_id") or ""),
                discord_id="",
                status="denied",
            )
        _save_state(db)

        rec: dict[str, Any] = {}
        if wuid.startswith("user_"):
            tmp = (db.get("by_whop_user") or {}).get(wuid) if isinstance(db.get("by_whop_user"), dict) else None
            if isinstance(tmp, dict):
                rec = tmp
        if not rec and email and isinstance(db.get("by_email"), dict):
            tmp = db["by_email"].get(email)
            if isinstance(tmp, dict):
                rec = tmp
        discord_target = str(rec.get("discord_id") or "").strip()

    guild = bot.get_guild(int(guild_id))
    mem = None
    if guild and discord_target.isdigit():
        mem = guild.get_member(int(discord_target))
        if mem is None:
            with suppress(Exception):
                mem = await guild.fetch_member(int(discord_target))

    role_note = "no Discord resolved; role unchanged"
    if kind in {"approved", "denied"} and cfg.waitlist_role_id and mem:
        ok, why = await _apply_waitlist_role(guild, mem, cfg.waitlist_role_id, add=False)  # type: ignore[arg-type]
        role_note = "removed waitlist role" if ok else f"role removal failed ({why})"

    color = 0x57F287
    if kind == "denied":
        color = 0xED4245
    elif kind == "approved":
        color = 0x57F287
    elif kind == "created":
        color = 0x5865F2

    title_out = f"📋 Waitlist — {kind.title()} (membership logs)"
    embed = discord.Embed(title=title_out, description=_norm(desc)[:4096] or None, color=color, timestamp=datetime.now(timezone.utc))
    if mem:
        with suppress(Exception):
            from staff_embeds import apply_member_header

            apply_member_header(embed, mem)
    embed.add_field(name="Whop user", value=f"`{wuid or '—'}`", inline=True)
    embed.add_field(name="Plan id", value=f"`{mid.get('plan_id') or '—'}`", inline=True)
    embed.add_field(name="Email (best-effort)", value=f"`{email or '—'}`", inline=False)
    embed.add_field(name="Username", value=str(mid.get("username") or "—")[:1024], inline=True)
    embed.add_field(name="Resolved Discord", value=f"`{discord_target or '—'}`", inline=True)
    embed.add_field(name="Automation", value=_norm(role_note)[:1024], inline=False)
    embed.add_field(name="Source", value=str(message.jump_url or "")[:1024], inline=False)
    _set_footer(embed, f"event=waitlist_{kind}", counts_line, cerr)

    content = mem.mention if mem else ""
    await _send_waitlist_embed(bot, guild_id, cfg.output_channel_id, embed, content=content)


def load_cfg_from_dict(raw: object) -> WaitlistLoggingConfig:
    if not isinstance(raw, dict):
        raw = {}
    return WaitlistLoggingConfig(
        enabled=bool(raw.get("enabled", False)),
        output_channel_id=max(0, int(raw.get("output_channel_id") or 0)),
        whop_logs_channel_id=max(0, int(raw.get("whop_logs_channel_id") or 0)),
        membership_logs_channel_id=max(0, int(raw.get("membership_logs_channel_id") or 0)),
        waitlist_role_id=max(0, int(raw.get("waitlist_role_id") or 0)),
        api_counts_enabled=bool(raw.get("api_counts_enabled", True)),
        api_counts_cache_seconds=max(60.0, float(raw.get("api_counts_cache_seconds") or 900.0)),
        dedupe_max=max(100, min(20000, int(raw.get("dedupe_max_keys") or 4000))),
    )
