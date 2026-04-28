"""
Parse DAILY SCHEDULE channel names into (scheduled_time_est, drop_name).
EST is the default; -local is treated as EST.
Drop name: humanized from event part (title case, no hyphens), excluding time and leading numbers/dates.
"""
import re
from datetime import datetime, time

def _get_est_tz():
    """America/New_York: use zoneinfo (Python 3.9+) or pytz. On Windows, pip install tzdata or pytz if needed."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:
        try:
            import pytz
            return pytz.timezone("America/New_York")
        except ImportError:
            raise ImportError(
                "Need timezone data: run  pip install tzdata  (or  pip install pytz )"
            ) from None

EST = _get_est_tz()

# Match leading time: 9pm-est, 10am-local, 12pm-est, 1pm (no suffix = EST)
TIME_PATTERN = re.compile(
    r"^\s*(\d{1,2})\s*(am|pm)\s*(-local|-est)?",
    re.IGNORECASE,
)
# Unicode pipe variants used in channel names: | │ (U+2502) ︱ (U+FE31)
PIPE_SEP = re.compile(r"\s*[|\u2502\ufe31]\s*", re.UNICODE)

def parse_channel_name(name: str) -> tuple[datetime | None, str | None]:
    """
    Parse a channel name into (next_scheduled_datetime_est, drop_name) or (None, None) if unparseable.
    Uses today's date for the time; callers can adjust for "next occurrence" if needed.
    Strips leading #, spaces, and emojis (e.g. 🟢🟡📅┃). Accepts pipe separators |, │, ︱.
    """
    if not name or not name.strip():
        return None, None

    raw = name.strip()
    # Strip leading #, spaces, emojis, symbols so we start at digit or letter (e.g. "🟢9pm-est︱walmart")
    raw = re.sub(r"^[^0-9a-zA-Z\-]+", "", raw)
    if not raw:
        return None, None
    event_part: str | None = None
    time_part: str | None = None

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

    # Parse hour and am/pm
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

    # Use today in EST
    today = datetime.now(EST).date()
    scheduled_dt = datetime.combine(today, t, tzinfo=EST)

    # Drop name: from event part, strip leading digits/dashes, then humanize (hyphen -> space, title)
    if not event_part:
        event_part = "Drop"
    # Remove leading numbers and hyphens (e.g. "2025-26-topps-basketball-tuesday" -> "topps-basketball-tuesday")
    event_clean = re.sub(r"^[\d\-\.]+\s*", "", event_part).strip() or event_part
    drop_name = event_clean.replace("-", " ").strip()
    if drop_name:
        drop_name = drop_name.title()
    else:
        drop_name = "Drop"

    return scheduled_dt, drop_name
