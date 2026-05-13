"""
RS `review rs` channel blurbs: daily-reminder descriptions + schedule-style fallbacks.

Reminder parsing targets messages like:
  TOMORROW'S DAILY REMINDER - MM/DD
  Online Reminders: / Instore Reminders:
with lines containing <#channel_id> and optional " - description".

Channel-name time/drop parsing is aligned with DailyScheduleReminder/schedule_parser.py
(kept here so RSAdminBot does not depend on importing that package at runtime).
"""
from __future__ import annotations

import re
from datetime import datetime, time
from typing import Dict, Optional, Tuple

import discord

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[misc, assignment]

# --- schedule_parser-aligned (DailyScheduleReminder/schedule_parser.py) -----------------

TIME_PATTERN = re.compile(
    r"^\s*(\d{1,2})\s*(am|pm)\s*(-local|-est)?",
    re.IGNORECASE,
)
PIPE_SEP = re.compile(r"\s*[|\u2502\ufe31]\s*", re.UNICODE)


def parse_channel_name(name: str) -> tuple[Optional[datetime], Optional[str]]:
    """
    Parse a channel name into (next_scheduled_datetime_est, drop_name) or (None, None).
    Same rules as DailyScheduleReminder.schedule_parser.parse_channel_name.
    """
    if not name or not name.strip():
        return None, None

    raw = name.strip()
    raw = re.sub(r"^[^0-9a-zA-Z\-]+", "", raw)
    if not raw:
        return None, None
    event_part: Optional[str] = None
    time_part: Optional[str] = None

    pipe_parts = PIPE_SEP.split(raw, maxsplit=1)
    if len(pipe_parts) >= 2:
        time_part = pipe_parts[0].strip()
        event_part = pipe_parts[1].strip() if len(pipe_parts) > 1 else ""
    else:
        m = TIME_PATTERN.match(raw)
        if m:
            time_part = m.group(0).strip()
            event_part = raw[m.end() :].strip().lstrip("-")
        else:
            return None, None

    if not time_part:
        return None, None

    hm = re.match(r"(\d{1,2})\s*(am|pm)", time_part, re.IGNORECASE)
    if not hm:
        return None, None
    hour = int(hm.group(1))
    is_pm = hm.group(2).lower() == "pm"
    if hour == 12:
        hour_24 = 0 if not is_pm else 12
    else:
        hour_24 = hour + (12 if is_pm else 0)
    if hour_24 > 23:
        hour_24 = 0
    try:
        t = time(hour_24, 0, 0)
    except ValueError:
        return None, None

    if ZoneInfo is None:
        return None, None
    try:
        est = ZoneInfo("America/New_York")
    except Exception:
        return None, None

    today = datetime.now(est).date()
    scheduled_dt = datetime.combine(today, t, tzinfo=est)

    if not event_part:
        event_part = "Drop"
    event_clean = re.sub(r"^[\d\-\.]+\s*", "", event_part).strip() or event_part
    drop_name = event_clean.replace("-", " ").strip()
    if drop_name:
        drop_name = drop_name.title()
    else:
        drop_name = "Drop"

    return scheduled_dt, drop_name


def schedule_style_label(channel_name: str, *, max_len: int = 200) -> str:
    """Human line from channel name when daily reminder has no description for this channel."""
    dt, drop = parse_channel_name(channel_name)
    if dt is not None and drop:
        hour12 = int(dt.strftime("%I"))
        rest = dt.strftime(":%M %p EST")
        time_s = f"{hour12}{rest}"
        line = f"{time_s} — {drop}"
    else:
        raw = (channel_name or "").strip()
        raw = re.sub(r"^[^0-9a-zA-Z\-#|]+", "", raw)
        blob = PIPE_SEP.sub(" ", raw).replace("-", " ")
        parts = [p for p in re.split(r"\s+", blob.strip()) if p]
        line = " ".join(parts[:14]).strip() or "Channel"
    if max_len > 0 and len(line) > max_len:
        return line[: max_len - 1].rstrip() + "…"
    return line


# --- daily reminder body parsing ---------------------------------------------------------

_REMINDER_HEADER_RE = re.compile(
    r"(?is)TOMORROW'?S\s+DAILY\s+REMINDER\s*[-–]\s*(\d{1,2})/(\d{1,2})",
)
_INSTORE_SECTION_STOP = re.compile(
    r"(?is)^\s*(\*\*ALWAYS\b|Sneaker\s+Flipping:|Instore\s+Flipping:|Watch\s+our\b|\*\*LET\'?S\b|If\s+you\'?re\s+done\s+reading\b)",
)


def parse_reminder_header_mmdd(text: str) -> Optional[Tuple[int, int]]:
    m = _REMINDER_HEADER_RE.search(text or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _flatten_message_content(msg: discord.Message, *, max_embeds: int = 3) -> str:
    parts: list[str] = []
    c = str(getattr(msg, "content", None) or "").strip()
    if c:
        parts.append(c)
    try:
        for emb in (getattr(msg, "embeds", None) or [])[:max_embeds]:
            if emb is None:
                continue
            t = str(getattr(emb, "title", None) or "").strip()
            if t:
                parts.append(t)
            d = str(getattr(emb, "description", None) or "").strip()
            if d:
                parts.append(d)
    except Exception:
        pass
    return "\n".join(parts).strip()


def _trim_instore_tail(blob: str) -> str:
    lines_out: list[str] = []
    for ln in (blob or "").splitlines():
        s = ln.strip()
        if _INSTORE_SECTION_STOP.match(s):
            break
        lines_out.append(ln)
    return "\n".join(lines_out).strip()


def extract_reminder_blurbs(text: str) -> Dict[int, str]:
    """
    From Online Reminders + Instore Reminders sections only: first <#id> per line with optional
    trailing ' - description'. Instore lines overwrite online for the same id.
    """
    out: Dict[int, str] = {}
    t = text or ""
    mo = re.search(r"(?is)\*{0,2}\s*Online\s+Reminders\s*:\s*\*{0,2}", t)
    mi = re.search(r"(?is)\*{0,2}\s*Instore\s+Reminders\s*:\s*\*{0,2}", t)
    if mo and mi and mi.start() > mo.end():
        online_body = t[mo.end() : mi.start()]
        _accumulate_reminder_lines(online_body, out)
    if mi:
        rest = t[mi.end() :]
        instore_body = _trim_instore_tail(rest)
        _accumulate_reminder_lines(instore_body, out)
    return out


def _accumulate_reminder_lines(blob: str, out: Dict[int, str]) -> None:
    for ln in (blob or "").splitlines():
        cid, desc = _line_channel_blurb(ln)
        if cid is None:
            continue
        if desc:
            out[int(cid)] = desc


def _line_channel_blurb(line: str) -> Tuple[Optional[int], str]:
    s = (line or "").strip()
    if not s:
        return None, ""
    m = re.search(r"<#(\d+)>", s)
    if not m:
        return None, ""
    cid = int(m.group(1))
    tail = s[m.end() :].strip()
    m2 = re.match(r"^[-–]\s*(.+)$", tail)
    if m2:
        return cid, m2.group(1).strip()
    return cid, ""


async def fetch_today_reminder_blurbs(
    bot: discord.Client,
    *,
    reminder_channel_id: int,
    timezone_name: str,
    history_limit: int = 30,
) -> Dict[int, str]:
    """
    Newest-first scan of reminder_channel_id for a message whose header MM/DD matches
    today's calendar date in timezone_name. Returns channel_id -> description (non-empty only).
    """
    if ZoneInfo is None:
        return {}
    try:
        tz = ZoneInfo((timezone_name or "America/New_York").strip() or "America/New_York")
    except Exception:
        try:
            tz = ZoneInfo("America/New_York")
        except Exception:
            return {}

    now_local = datetime.now(tz)
    want_m, want_d = now_local.month, now_local.day

    ch = bot.get_channel(int(reminder_channel_id))
    if ch is None:
        try:
            ch = await bot.fetch_channel(int(reminder_channel_id))
        except Exception:
            return {}
    if not hasattr(ch, "history"):
        return {}

    try:
        async for msg in ch.history(limit=max(5, min(int(history_limit), 50)), oldest_first=False):
            blob = _flatten_message_content(msg)
            if not blob:
                continue
            u = blob.upper()
            if "TOMORROW" not in u or "DAILY REMINDER" not in u:
                continue
            parsed = parse_reminder_header_mmdd(blob)
            if not parsed:
                continue
            pm, pd = parsed
            if (pm, pd) != (want_m, want_d):
                continue
            return extract_reminder_blurbs(blob)
    except Exception:
        return {}
    return {}


def blurb_for_channel_id(
    channel_id: int,
    channel_name: str,
    reminder_blurbs: Dict[int, str],
    *,
    max_blurb_chars: int = 200,
) -> str:
    desc = (reminder_blurbs or {}).get(int(channel_id))
    if desc and str(desc).strip():
        d = str(desc).strip()
        if max_blurb_chars > 0 and len(d) > max_blurb_chars:
            return d[: max_blurb_chars - 1].rstrip() + "…"
        return d
    return schedule_style_label(channel_name, max_len=max_blurb_chars)
