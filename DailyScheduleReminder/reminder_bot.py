"""
Minimal shared helpers for DailyScheduleReminder scripts.

This repo previously referenced `reminder_bot.load_token()` and `_chunk_message()` from
multiple scripts (manual senders, mirror forwarders, etc.). Keep the token loading and
message chunking logic in one canonical module.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable, List

_BOT_DIR = Path(__file__).resolve().parent


def load_token() -> str:
    """
    Return the Discord user token for DailyScheduleReminder.

    Precedence:
      1) env var DISCORD_USER_TOKEN
      2) DailyScheduleReminder/config.secrets.json {"token": "..."}
    """
    env = (os.environ.get("DISCORD_USER_TOKEN") or "").strip()
    if env:
        return env

    p = _BOT_DIR / "config.secrets.json"
    if p.is_file():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                t = str(data.get("token") or "").strip()
                if t:
                    return t
        except Exception:
            pass

    raise RuntimeError(
        "Missing Discord token. Set env var DISCORD_USER_TOKEN or create "
        "DailyScheduleReminder/config.secrets.json with {\"token\": \"...\"}."
    )


def _chunk_message(content: str, *, max_len: int = 1900) -> List[str]:
    """
    Split a long Discord message into <= max_len chunks.
    Keeps line boundaries where possible.
    """
    text = str(content or "")
    if not text.strip():
        return []

    out: List[str] = []
    buf: List[str] = []
    buf_len = 0

    def flush() -> None:
        nonlocal buf, buf_len
        if not buf:
            return
        out.append("".join(buf).rstrip())
        buf = []
        buf_len = 0

    # Prefer splitting on newlines for readability
    parts: Iterable[str] = text.splitlines(keepends=True)
    for part in parts:
        if len(part) > max_len:
            # Hard-split very long lines
            start = 0
            while start < len(part):
                seg = part[start : start + max_len]
                if buf_len + len(seg) > max_len:
                    flush()
                buf.append(seg)
                buf_len += len(seg)
                flush()
                start += max_len
            continue

        if buf_len + len(part) > max_len:
            flush()
        buf.append(part)
        buf_len += len(part)

    flush()
    # Discord requires at least 1 non-empty char
    return [s for s in out if s.strip()]

"""
Daily schedule reminder bot: posts to a reminder channel 30 minutes before each
drop in the DAILY SCHEDULE category. Uses Discum (user token).

Default (stay on, check every minute):  python reminder_bot.py
One-shot for cron:                      python reminder_bot.py --once
Test one channel:                       python reminder_bot.py --test <channel_id>
Listen for !reminder <#channel>:        python reminder_bot.py --listen
"""
import re
import sys
import threading
import time
from pathlib import Path

# Allow importing discum from sibling Discumraw
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import json
import discum
from schedule_parser import parse_channel_name, EST
from datetime import datetime, timedelta, timezone

BLUE_SIREN = "<a:blue_siren:1408979316536115260>"
PEPE_COOK = "<a:pepevbcook:1412558017089503253>"


def load_config():
    """Load config.json; defaults for missing keys."""
    path = Path(__file__).parent / "config.json"
    defaults = {
        "enabled": True,
        "category_id": "1313260017989713981",
        "reminder_channel_id": "1473485905602940959",
        "reminder_mins_before": 30,
        "mention_role_id": "876569612085518376",
        "command_channel_id": None,
    }
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            defaults.update(json.load(f))
    return defaults


def load_token():
    import os
    # Prefer env
    token = os.environ.get("DISCORD_USER_TOKEN")
    if token:
        return token
    # Then config.secrets.json in this folder
    secrets_path = Path(__file__).parent / "config.secrets.json"
    if secrets_path.exists():
        with open(secrets_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("token") or data.get("user_token")
    raise RuntimeError("Set DISCORD_USER_TOKEN or create DailyScheduleReminder/config.secrets.json with 'token'")


def _chunk_message(content: str, max_len: int = 1990):
    """Split long content into chunks (prefer newline boundaries)."""
    if not content or len(content) <= max_len:
        return [content] if content else []
    out = []
    cur = content
    while cur:
        if len(cur) <= max_len:
            out.append(cur)
            break
        cut = cur.rfind("\n\n", 0, max_len)
        if cut < 0:
            cut = cur.rfind("\n", 0, max_len)
        if cut < 0:
            cut = max_len
        part = cur[:cut].rstrip()
        if part:
            out.append(part)
        cur = cur[cut:].lstrip("\n")
    return out


def run_ticket_startup_send(bot) -> None:
    """
    If ticket_startup.enabled: read RSCheckerbot pending_ticket_startup_messages.json,
    send the startup (checking-in) message to each ticket channel as the Discord user,
    then mark startup_sent_at_iso in RSCheckerbot tickets_index.json.
    """
    cfg = load_config()
    ticket_cfg = cfg.get("ticket_startup")
    if not isinstance(ticket_cfg, dict) or not ticket_cfg.get("enabled"):
        return
    pending_path = _repo_root / "RSCheckerbot" / "data" / "pending_ticket_startup_messages.json"
    if not pending_path.exists():
        return
    try:
        with open(pending_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[TicketStartup] Failed to read pending file: {e}")
        return
    pending = data.get("pending")
    if not isinstance(pending, list) or not pending:
        return
    rsc_config_path = _repo_root / "RSCheckerbot" / "config.json"
    if not rsc_config_path.exists():
        print("[TicketStartup] RSCheckerbot config.json not found.")
        return
    try:
        with open(rsc_config_path, "r", encoding="utf-8") as f:
            rsc_config = json.load(f)
    except Exception as e:
        print(f"[TicketStartup] Failed to read RSCheckerbot config: {e}")
        return
    st = rsc_config.get("support_tickets") or {}
    sm = st.get("startup_messages") or {}
    templates = sm.get("templates") or {}
    perms = st.get("permissions") or {}
    staff_ids = perms.get("staff_role_ids") or []
    staff_mention = f"<@&{staff_ids[0]}>" if staff_ids else ""
    index_path = _repo_root / "RSCheckerbot" / "data" / "tickets_index.json"
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            idx = json.load(f)
    except Exception:
        idx = {}
    skip_tickets = idx.get("tickets") or {}
    for entry in pending:
        ticket_id = entry.get("ticket_id")
        channel_id = entry.get("channel_id")
        user_id = entry.get("user_id")
        ticket_type = str(entry.get("ticket_type") or "").strip().lower()
        if not ticket_id or not channel_id:
            continue
        rec = skip_tickets.get(ticket_id)
        if isinstance(rec, dict) and str(rec.get("startup_sent_at_iso") or "").strip():
            continue
        tmpl = templates.get(ticket_type) or templates.get("default") or ""
        if not tmpl:
            continue
        mention = f"<@{user_id}>" if user_id else ""
        content = (
            tmpl.replace("{mention}", mention)
            .replace("{staff}", staff_mention)
            .replace("{staff_mention}", staff_mention)
            .strip()
        )
        if not content:
            continue
        ok = True
        for part in _chunk_message(content):
            resp = bot.sendMessage(str(channel_id), part, allowed_mentions={"parse": ["users", "roles"]})
            if not resp or getattr(resp, "status_code", None) != 200:
                ok = False
                print(f"[TicketStartup] Send failed for channel {channel_id}: {getattr(resp, 'text', resp)}")
                break
        if not ok:
            continue
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                idx = json.load(f)
        except Exception as e:
            print(f"[TicketStartup] Failed to read index: {e}")
            continue
        tickets = idx.get("tickets")
        if isinstance(tickets, dict) and ticket_id in tickets:
            tickets[ticket_id]["startup_sent_at_iso"] = now_iso
            if ticket_id not in skip_tickets:
                skip_tickets[ticket_id] = {}
            skip_tickets[ticket_id]["startup_sent_at_iso"] = now_iso
            try:
                with open(index_path, "w", encoding="utf-8") as f:
                    json.dump(idx, f, indent=2, ensure_ascii=False)
            except Exception as e:
                print(f"[TicketStartup] Failed to write index: {e}")
        print(f"[TicketStartup] Sent startup message to channel {channel_id} (ticket {ticket_id})")


def get_schedule_channels_in_category(bot, guild_id: str, category_id: str):
    """Return list of channel dicts with parent_id == category_id (and type 0 = text)."""
    r = bot.getGuildChannels(guild_id)
    if not r or r.status_code != 200:
        return []
    channels = r.json()
    return [
        c for c in channels
        if str(c.get("parent_id")) == str(category_id)
        and c.get("type") == 0  # GUILD_TEXT
    ]


def _log_startup(bot, reminder_channel_id: str, category_id: str, channels: list, today: list, verbose: bool = True):
    """Print startup banner, login info, and what channels we're reading."""
    print("=" * 50)
    print("  DailyScheduleReminder")
    print("=" * 50)
    try:
        me = bot.info(with_analytics_token=False)
        if me and me.status_code == 200:
            d = me.json()
            username = d.get("username") or "?"
            discriminator = d.get("discriminator", "0")
            if discriminator == "0":
                print(f"  Logged in: {username}")
            else:
                print(f"  Logged in: {username}#{discriminator}")
        else:
            print("  Logged in (token valid).")
    except Exception:
        print("  Logged in (token valid).")
    print(f"  Reminder channel ID: {reminder_channel_id}")
    print(f"  Category ID:         {category_id}")
    print(f"  Channels in category: {len(channels)}")
    if channels:
        for ch in channels:
            name = ch.get("name") or "(no name)"
            ch_id = ch.get("id", "")
            scheduled_dt, drop_name = parse_channel_name(name)
            if scheduled_dt is not None:
                time_str = scheduled_dt.strftime("%I:%M %p EST")
                print(f"    - #{name}  ->  {time_str}  {drop_name!r}  (id={ch_id})")
            else:
                print(f"    - #{name}  (no parseable time, skipped)")
    else:
        print("    (none — check category_id or guild permissions)")
    now_est = datetime.now(EST)
    past = [(t, d, c) for t, d, c in today if t <= now_est]
    upcoming = [(t, d, c) for t, d, c in today if t > now_est]
    print(f"  Today's schedule: {len(past)} past, {len(upcoming)} upcoming")
    if upcoming:
        print("    Upcoming:")
        for sched_dt, drop_name, ch_id in upcoming:
            print(f"      - {sched_dt.strftime('%I:%M %p EST')}  {drop_name!r}")
    if past and not upcoming:
        print("    (all drops for today are in the past)")
    print("=" * 50)


def build_today_schedule(channels) -> list[tuple[datetime, str, str]]:
    """Return [(scheduled_dt_est, drop_name, channel_id)] for today, sorted by time."""
    out = []
    for ch in channels:
        name = ch.get("name") or ""
        ch_id = ch.get("id")
        if not ch_id:
            continue
        scheduled_dt, drop_name = parse_channel_name(name)
        if scheduled_dt is None:
            continue
        out.append((scheduled_dt, drop_name, str(ch_id)))
    out.sort(key=lambda x: x[0])
    return out


def format_reminder(
    scheduled_dt: datetime,
    drop_name: str,
    channel_id: str,
    is_first_of_day: bool,
) -> str:
    """Format one reminder block. channel_id is the drop channel (for the -# <#id> link)."""
    unix = int(scheduled_dt.timestamp())
    time_part = f"<t:{unix}:R> minutes until drop {BLUE_SIREN}"
    title_line = f"### {drop_name} {PEPE_COOK}"
    channel_ref = f"-# <#{channel_id}>"

    if is_first_of_day:
        return (
            f"## {time_part}\n"
            f"{title_line}\n"
            f"{channel_ref}"
        )
    return (
        "## UP NEXT\n"
        f"### {time_part}\n"
        f"{title_line}\n"
        f"{channel_ref}"
    )


def send_test_reminder_for_channel(bot, channel_id: str, reminder_channel_id: str, mention_role_id=None) -> bool:
    """
    Fetch channel name, parse it, and send one reminder message to reminder_channel_id.
    Works for any channel (e.g. outside the category). Returns True if sent.
    """
    ch_r = bot.getChannel(channel_id)
    if not ch_r or ch_r.status_code != 200:
        print(f"Could not get channel {channel_id}.")
        return False
    name = (ch_r.json() or {}).get("name") or ""
    scheduled_dt, drop_name = parse_channel_name(name)
    if scheduled_dt is None:
        print(f"Channel name not parseable for time/drop: {name!r}")
        return False
    message = format_reminder(scheduled_dt, drop_name, channel_id, is_first_of_day=True)
    message = "## 🧪 Test reminder\n\n" + message
    if mention_role_id:
        message += f"\n\n<@&{mention_role_id}>"
    allowed_mentions = {"parse": ["roles"]} if mention_role_id else None
    resp = bot.sendMessage(reminder_channel_id, message, allowed_mentions=allowed_mentions)
    if resp and getattr(resp, "status_code", None) == 200:
        print(f"Test reminder sent to channel {reminder_channel_id} for #{name}.")
        return True
    print("Send failed:", getattr(resp, "text", resp))
    return False


def run_test(channel_id: str):
    """Send one test reminder for the given channel (can be outside category)."""
    cfg = load_config()
    reminder_channel_id = str(cfg["reminder_channel_id"])
    mention_role_id = cfg.get("mention_role_id")
    token = load_token()
    bot = discum.Client(token=token, log={"console": False, "file": False})
    send_test_reminder_for_channel(bot, channel_id.strip(), reminder_channel_id, mention_role_id)


def run_listen():
    """Run gateway and listen for !reminder <#channel> or !reminder <channel_id> in command channel."""
    cfg = load_config()
    reminder_channel_id = str(cfg["reminder_channel_id"])
    command_channel_id = str(cfg.get("command_channel_id") or reminder_channel_id)
    mention_role_id = cfg.get("mention_role_id")
    token = load_token()
    bot = discum.Client(token=token, log={"console": False, "file": False})

    @bot.gateway.command
    def on_message(resp):
        if not resp.event.message:
            return
        m = resp.parsed.auto()
        channel_id_msg = m.get("channel_id")
        if str(channel_id_msg) != command_channel_id:
            return
        content = (m.get("content") or "").strip()
        if not content.lower().startswith("!reminder"):
            return
        # !reminder <#123...> or !reminder 123...
        match = re.search(r"!reminder\s+(?:<#!?(\d+)>|(\d+))", content, re.IGNORECASE)
        if not match:
            return
        target_channel_id = (match.group(1) or match.group(2) or "").strip()
        if not target_channel_id:
            return
        if send_test_reminder_for_channel(bot, target_channel_id, reminder_channel_id, mention_role_id):
            print(f"[Command] Test reminder sent for channel {target_channel_id}.")

    print(f"Listening for '!reminder <#channel>' in channel {command_channel_id}. Ctrl+C to stop.")
    bot.gateway.run(auto_reconnect=True)


def run_bot():
    cfg = load_config()
    if not cfg.get("enabled", True):
        print("DailyScheduleReminder is disabled in config; exiting.")
        return
    category_id = str(cfg["category_id"])
    reminder_channel_id = str(cfg["reminder_channel_id"])
    mins_before = int(cfg["reminder_mins_before"])
    mention_role_id = cfg.get("mention_role_id")

    token = load_token()
    bot = discum.Client(token=token, log={"console": False, "file": False})

    # Resolve guild_id from reminder channel
    ch_r = bot.getChannel(reminder_channel_id)
    if not ch_r or ch_r.status_code != 200:
        raise RuntimeError("Could not get reminder channel; check reminder_channel_id and token.")
    guild_id = ch_r.json().get("guild_id")
    if not guild_id:
        raise RuntimeError("Reminder channel has no guild_id.")
    guild_id = str(guild_id)

    channels = get_schedule_channels_in_category(bot, guild_id, category_id)
    today = build_today_schedule(channels)
    _log_startup(bot, reminder_channel_id, category_id, channels, today)

    if not today:
        print("No scheduled channels found. Either the category has no channels with a parseable time")
        print("(e.g. 9pm-est, 10am-local, 12pm-est) or the category_id is wrong. Exiting.")
        return

    now = datetime.now(EST)
    reminder_at = now + timedelta(minutes=mins_before)
    to_send = []
    for i, (scheduled_dt, drop_name, ch_id) in enumerate(today):
        if scheduled_dt <= now:
            continue
        target_reminder_time = scheduled_dt - timedelta(minutes=mins_before)
        if (target_reminder_time.hour, target_reminder_time.minute) == (reminder_at.hour, reminder_at.minute):
            is_first_of_day = (i == 0)  # first in today's sorted schedule
            to_send.append((scheduled_dt, drop_name, ch_id, is_first_of_day))

    if not to_send:
        next_drops = [(t, d, c) for t, d, c in today if t > now]
        if next_drops:
            times = ", ".join(t.strftime("%I:%M %p") for t, _, _ in next_drops[:3])
            print(f"No reminders due right now. Next drop(s) today: {times}. Exiting.")
        else:
            print("No reminders due; all of today's drops are in the past. Exiting.")
        return

    # Build one message: first drop of day uses first format; rest use UP NEXT
    blocks = []
    for scheduled_dt, drop_name, ch_id, is_first_of_day in to_send:
        blocks.append(format_reminder(scheduled_dt, drop_name, ch_id, is_first_of_day))
    message = "\n\n".join(blocks)
    if mention_role_id:
        message += f"\n\n<@&{mention_role_id}>"
    allowed_mentions = {"parse": ["roles"]} if mention_role_id else None
    resp = bot.sendMessage(reminder_channel_id, message, allowed_mentions=allowed_mentions)
    if resp and getattr(resp, "status_code", None) == 200:
        print("Reminder(s) sent. Done.")
    else:
        print("Failed to send:", getattr(resp, "text", resp))


def _daemon_loop(bot, guild_id: str, category_id: str, reminder_channel_id: str, mins_before: int, mention_role_id):
    """Background thread: check every minute and send scheduled reminders."""
    sent_key: set[tuple[datetime, str]] = set()
    while True:
        try:
            run_ticket_startup_send(bot)
            now = datetime.now(EST)
            if now.hour == 0 and now.minute == 0:
                sent_key.clear()
            channels = get_schedule_channels_in_category(bot, guild_id, category_id)
            today = build_today_schedule(channels)
            upcoming_count = sum(1 for t, _, _ in today if t > now)
            print(f"[{now.strftime('%H:%M')}] Re-read category: {len(channels)} channels, {len(today)} in schedule ({upcoming_count} upcoming)")
            reminder_at = now + timedelta(minutes=mins_before)
            to_send = []
            for i, (scheduled_dt, drop_name, ch_id) in enumerate(today):
                if scheduled_dt <= now:
                    continue
                if (scheduled_dt, ch_id) in sent_key:
                    continue
                target_reminder_time = scheduled_dt - timedelta(minutes=mins_before)
                if (target_reminder_time.hour, target_reminder_time.minute) == (reminder_at.hour, reminder_at.minute):
                    to_send.append((scheduled_dt, drop_name, ch_id, i == 0))
            if to_send:
                blocks = [
                    format_reminder(sched_dt, dname, cid, is_first)
                    for sched_dt, dname, cid, is_first in to_send
                ]
                message = "\n\n".join(blocks)
                if mention_role_id:
                    message += f"\n\n<@&{mention_role_id}>"
                allowed_mentions = {"parse": ["roles"]} if mention_role_id else None
                resp = bot.sendMessage(reminder_channel_id, message, allowed_mentions=allowed_mentions)
                if resp and getattr(resp, "status_code", None) == 200:
                    for sched_dt, _, cid, _ in to_send:
                        sent_key.add((sched_dt, cid))
                    print(f"[{now.strftime('%H:%M')}] Reminder(s) sent.")
                else:
                    print(f"[{now.strftime('%H:%M')}] Send failed: {getattr(resp, 'text', resp)}")
        except Exception as e:
            print(f"[Daemon] Error: {e}")
        time.sleep(60 - (datetime.now(EST).second + 0.001))


def run_daemon():
    """Run reminder check every minute and listen for !reminder in Discord (one process)."""
    cfg = load_config()
    if not cfg.get("enabled", True):
        print("DailyScheduleReminder is disabled in config; exiting.")
        return
    category_id = str(cfg["category_id"])
    reminder_channel_id = str(cfg["reminder_channel_id"])
    mins_before = int(cfg["reminder_mins_before"])
    mention_role_id = cfg.get("mention_role_id")
    command_channel_id = str(cfg.get("command_channel_id") or reminder_channel_id)

    token = load_token()
    bot = discum.Client(token=token, log={"console": False, "file": False})
    ch_r = bot.getChannel(reminder_channel_id)
    if not ch_r or ch_r.status_code != 200:
        raise RuntimeError("Could not get reminder channel; check reminder_channel_id and token.")
    guild_id = str(ch_r.json().get("guild_id") or "")

    channels = get_schedule_channels_in_category(bot, guild_id, category_id)
    today = build_today_schedule(channels)
    _log_startup(bot, reminder_channel_id, category_id, channels, today)
    print("Daemon: checking every minute. Commands: !reminder <#channel> or !reminder <channel_id>")
    print(f"Command channel: {command_channel_id}. Ctrl+C to stop.\n")

    @bot.gateway.command
    def on_message(resp):
        if not resp.event.message:
            return
        m = resp.parsed.auto()
        if str(m.get("channel_id")) != command_channel_id:
            return
        content = (m.get("content") or "").strip()
        if not content.lower().startswith("!reminder"):
            return
        match = re.search(r"!reminder\s+(?:<#!?(\d+)>|(\d+))", content, re.IGNORECASE)
        if not match:
            return
        target_channel_id = (match.group(1) or match.group(2) or "").strip()
        if not target_channel_id:
            return
        if send_test_reminder_for_channel(bot, target_channel_id, reminder_channel_id, mention_role_id):
            print(f"[Command] Test reminder sent for channel {target_channel_id}.")

    thread = threading.Thread(target=_daemon_loop, args=(bot, guild_id, category_id, reminder_channel_id, mins_before, mention_role_id), daemon=True)
    thread.start()
    bot.gateway.run(auto_reconnect=True)


if __name__ == "__main__":
    if "--once" in sys.argv:
        # One-shot for cron: check once and exit (no reminders due = exit)
        run_bot()
    elif "--listen" in sys.argv:
        run_listen()
    elif "--test" in sys.argv:
        idx = sys.argv.index("--test")
        if idx + 1 >= len(sys.argv):
            print("Usage: python reminder_bot.py --test <channel_id>")
            sys.exit(1)
        run_test(sys.argv[idx + 1])
    else:
        # Default: stay on, check every minute; new/moved channels picked up each run
        run_daemon()
