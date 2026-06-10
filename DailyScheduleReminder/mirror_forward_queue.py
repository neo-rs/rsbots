#!/usr/bin/env python3
"""
Pick a store (from m_lead_routes.json), paste a Mirror World start message link, then walk
newer messages in that channel: build !m lead / !m hdnation (same rules as mirror_message_to_m_lead.py),
POST each !m line to command_post_channel_id (or destination if unset), then optionally wait
for a monitor bot in that post channel (post_confirmation). The <#id> inside the line comes from
each route's destination_channel_id. React on the Mirror World source after send/confirm, then
optionally sleep (--delay seconds, or --delay-random-minutes / m_lead_routes forward_delay) before
the next message.

For !m lead (clearance / all-stores), poll until the Lead posted confirmation embed appears
(post_confirmation.timeout_seconds; 0 = no time limit). For !m hdnation, use
post_confirmation.timeout_seconds_hdnation. If the monitor reports a stock fetch failure
(matched failure_substrings), the script reacts with an X on the Mirror source message,
appends a line to mirror_forward_hdnation_failures.jsonl for a later retry run, and
continues to the next Mirror message without advancing the checkpoint.

When post_confirmation is enabled, monitor waits use a session window: monitor-bot messages after
our !m command until our next !m hdnation/lead (other staff commands are ignored). Tempo
maintenance/update banners extend the wait instead of counting as success or failure.

Consecutive duplicate guard (default on): skips when the previous channel message had the same
parsed UPC/TCIN/SKU (!m lead) or SKU (!m hdnation). A non-deal message in between resets the chain.
On dedupe skip, reacts with cross mark on the skipped Mirror message (unless --no-react).
Use --no-dedupe to turn off.

Checkpoint: mirror_forward_checkpoint.json stores last fully completed Mirror message per
Mirror channel (by_channel map). Interactive runs can resume from checkpoint or start from a
calendar date (same date for all stores in --all-stores). Use --no-checkpoint to disable writes.

--all-stores: run every route in forward_all_stores_order from m_lead_routes.json; each store
resumes from checkpoint or from --from-date (first message on/after that local calendar day).

Auth: same token chain as mirror_message_to_m_lead (DailyScheduleReminder + optional MWDiscumBot).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

_BOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BOT_DIR.parent
for _p in (_REPO_ROOT, _BOT_DIR):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

import mirror_message_to_m_lead as mm  # noqa: E402
from run_notify import (  # noqa: E402
    notify_batch_finished,
    notify_enabled_by_default,
    notify_run_error,
)
import reminder_bot as _rb  # noqa: E402

CHECKPOINT_PATH = Path(__file__).resolve().parent / "mirror_forward_checkpoint.json"
HDNATION_FAIL_LOG = Path(__file__).resolve().parent / "mirror_forward_hdnation_failures.jsonl"
DEDUPE_SKIP_EMOJI = "\u274c"  # cross mark (X) on dedupe skip
CHECKMARK_EMOJI = "\u2705"
# Sentinel channel id from _pick_store_interactive when user chooses "0. Run ALL stores".
ALL_STORES_MENU_SENTINEL = "__all_stores_menu__"
START_MODE_CHECKPOINT = "checkpoint"
START_MODE_DATE = "date"


@dataclass
class ForwardStartPlan:
    mode: str  # checkpoint | date
    start_date: date | None = None


def parse_calendar_date(text: str, *, today: date | None = None) -> date:
    """
    Parse calendar date only (local). Accepts:
      2026-03-15   03-15-26   03-15   03/15
    Month-day without year uses current year; if that date is still in the future, use last year.
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("Empty date.")
    today = today or date.today()
    iso = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if iso:
        y, mo, d = int(iso.group(1)), int(iso.group(2)), int(iso.group(3))
        return date(y, mo, d)
    mdy2 = re.fullmatch(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2})", s)
    if mdy2:
        mo, d, yy = int(mdy2.group(1)), int(mdy2.group(2)), int(mdy2.group(3))
        y = 2000 + yy if yy < 100 else yy
        return date(y, mo, d)
    md = re.fullmatch(r"(\d{1,2})[-/](\d{1,2})", s)
    if md:
        mo, d = int(md.group(1)), int(md.group(2))
        y = today.year
        candidate = date(y, mo, d)
        if candidate > today:
            candidate = date(y - 1, mo, d)
        return candidate
    raise ValueError(
        f"Unrecognized date {text!r}. Use YYYY-MM-DD, MM-DD-YY, MM-DD, or MM/DD."
    )


def _format_checkpoint_saved_at(iso_s: str) -> str:
    raw = (iso_s or "").strip()
    if not raw:
        return "—"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone().strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return raw[:16]


def _checkpoint_row_summary(entry: dict | None, channel_id: str) -> tuple[str, str, str]:
    """(deal_date_display, saved_display, message_id) for one store."""
    if not entry:
        return ("—", "—", "")
    mid = str(entry.get("last_completed_message_id") or "").strip()
    deal_d = mm.snowflake_to_local_date_str(mid) if mid.isdigit() else "—"
    saved = _format_checkpoint_saved_at(str(entry.get("updated_at_iso") or ""))
    return (deal_d, saved, mid)


def print_checkpoint_summary(
    ordered_rows: list[tuple[str, str, dict]],
    root: dict | None = None,
) -> None:
    """Print last completed forward per store (deal message date + when checkpoint was saved)."""
    root = root if root is not None else _load_checkpoint_root()
    print("\nCheckpoint summary (last fully completed forward per store):\n")
    print(f"  {'Store':<22}  {'Last deal (local)':<18}  {'Checkpoint saved':<18}  Message id")
    print(f"  {'-' * 22}  {'-' * 18}  {'-' * 18}  {'-' * 10}")
    for cid, lab, _r in ordered_rows:
        ent = _get_channel_checkpoint(root, str(cid))
        deal_d, saved, mid = _checkpoint_row_summary(ent, str(cid))
        mid_disp = mid if mid else "—"
        print(f"  {lab:<22}  {deal_d:<18}  {saved:<18}  {mid_disp}")
    print()


def _prompt_calendar_date() -> date:
    while True:
        raw = input(
            "Enter start date (YYYY-MM-DD, MM-DD-YY, MM-DD, or MM/DD): "
        ).strip()
        if not raw:
            print("Cancelled.")
            raise KeyboardInterrupt
        try:
            d = parse_calendar_date(raw)
            print(f"  -> {d.isoformat()} (local calendar day; first message on/after midnight)\n")
            return d
        except ValueError as e:
            print(f"  {e}")


def prompt_forward_start_plan(
    *,
    interactive: bool,
    cli_from_date: str,
    for_all_stores: bool,
) -> ForwardStartPlan | None:
    """
    Resolve checkpoint vs date start. CLI: --from-date forces date mode.
    Interactive: menu after checkpoint summary.
    """
    if (cli_from_date or "").strip():
        try:
            d = parse_calendar_date(cli_from_date)
            return ForwardStartPlan(mode=START_MODE_DATE, start_date=d)
        except ValueError as e:
            print(f"Invalid --from-date: {e}", file=sys.stderr)
            return None
    if not interactive:
        return ForwardStartPlan(mode=START_MODE_CHECKPOINT)
    print("How should this run start?\n")
    print("  1. Resume from checkpoint (next message after last completed forward)")
    print("  2. Start from a calendar date (first message on/after that day, all stores)")
    print()
    raw = input("Enter 1 or 2 [default 1]: ").strip()
    if not raw or raw == "1":
        return ForwardStartPlan(mode=START_MODE_CHECKPOINT)
    if raw == "2":
        try:
            d = _prompt_calendar_date()
            return ForwardStartPlan(mode=START_MODE_DATE, start_date=d)
        except KeyboardInterrupt:
            return None
    print("Invalid choice.", file=sys.stderr)
    return None


def _guild_id_from_checkpoint_entry(entry: dict | None) -> str:
    if not entry:
        return ""
    gid = str(entry.get("guild_id") or "").strip()
    if gid:
        return gid
    lj = str(entry.get("last_jump_url") or "").strip()
    if lj:
        try:
            gid, _, _ = mm.parse_jump_url(lj)
            return str(gid).strip()
        except ValueError:
            pass
    return ""


def _resolve_store_start_from_checkpoint(
    *,
    store_label: str,
    link_ch: str,
    entry: dict,
    token: str,
) -> tuple[dict, dict, str, str] | None:
    """(start_msg, channel, token, guild_id) or None if nothing to do."""
    last_id = str(entry.get("last_completed_message_id") or "").strip()
    if not last_id.isdigit():
        print(f"\n[{store_label}] invalid checkpoint; skipping.", file=sys.stderr)
        return None
    guild_id = _guild_id_from_checkpoint_entry(entry)
    if not guild_id:
        guild_id = mm.resolve_channel_guild_id(link_ch, token)
    if not guild_id:
        print(f"\n[{store_label}] checkpoint missing guild_id; skipping.", file=sys.stderr)
        return None
    try:
        _anchor, channel, _lab, token = mm.fetch_message_with_token_fallback(
            guild_id, link_ch, last_id
        )
    except RuntimeError as e:
        print(f"\n[{store_label}] cannot read channel: {e}", file=sys.stderr)
        return None
    try:
        nxt = mm.list_messages_after(link_ch, last_id, token, limit=50)
    except RuntimeError as e:
        print(f"\n[{store_label}] list after checkpoint: {e}", file=sys.stderr)
        return None
    if not nxt:
        print(f"\n[{store_label}] no newer messages after checkpoint; skipping.")
        return None
    start_msg = nxt[0]
    if not str(start_msg.get("id") or "").isdigit():
        print(f"\n[{store_label}] invalid resume message; skipping.", file=sys.stderr)
        return None
    return start_msg, channel, token, guild_id


def _resolve_store_start_from_date(
    *,
    store_label: str,
    link_ch: str,
    start_date: date,
    token: str,
    entry: dict | None,
) -> tuple[dict, dict, str, str] | None:
    """(start_msg, channel, token, guild_id) or None if nothing to do."""
    try:
        start_msg = mm.find_first_message_on_or_after_date(link_ch, start_date, token)
    except RuntimeError as e:
        print(f"\n[{store_label}] date search failed: {e}", file=sys.stderr)
        return None
    if not start_msg:
        print(f"\n[{store_label}] no messages on or after {start_date.isoformat()}; skipping.")
        return None
    start_mid = str(start_msg.get("id") or "")
    guild_id = _guild_id_from_checkpoint_entry(entry)
    if not guild_id:
        guild_id = mm.resolve_channel_guild_id(link_ch, token)
    if not guild_id:
        print(f"\n[{store_label}] could not resolve guild_id; skipping.", file=sys.stderr)
        return None
    try:
        start_msg, channel, _lab, token = mm.fetch_message_with_token_fallback(
            guild_id, link_ch, start_mid
        )
    except RuntimeError as e:
        print(f"\n[{store_label}] cannot load start message: {e}", file=sys.stderr)
        return None
    dt = mm.message_timestamp_local(start_msg)
    when = dt.strftime("%Y-%m-%d %H:%M") if dt else start_mid
    print(f"\n[{store_label}] date start {start_date.isoformat()}: first message {start_mid} ({when})")
    return start_msg, channel, token, guild_id


def _read_checkpoint_file_raw() -> dict | None:
    if not CHECKPOINT_PATH.is_file():
        return None
    try:
        data = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _normalize_checkpoint_root(data: dict | None) -> dict:
    """Single canonical on-disk shape: {\"by_channel\": { \"<mirror_channel_id>\": { ... }}}."""
    if not data or not isinstance(data, dict):
        return {"by_channel": {}}
    bc = data.get("by_channel")
    if isinstance(bc, dict):
        clean: dict[str, dict] = {}
        for k, v in bc.items():
            if str(k).isdigit() and isinstance(v, dict):
                clean[str(k)] = v
        return {"by_channel": clean}
    mid = str(data.get("mirror_channel_id") or "").strip()
    if mid.isdigit():
        entry = {k: v for k, v in data.items() if k != "by_channel"}
        return {"by_channel": {mid: entry}}
    return {"by_channel": {}}


def _load_checkpoint_root() -> dict:
    return _normalize_checkpoint_root(_read_checkpoint_file_raw())


def _get_channel_checkpoint(root: dict, mirror_channel_id: str) -> dict | None:
    bc = root.get("by_channel")
    if not isinstance(bc, dict):
        return None
    ent = bc.get(str(mirror_channel_id).strip())
    return ent if isinstance(ent, dict) else None


def _save_checkpoint_root(root: dict) -> None:
    """Write full checkpoint root; merges all by_channel keys. Uses temp file + replace to avoid partial writes."""
    text = json.dumps(root, indent=2, ensure_ascii=False) + "\n"
    parent = CHECKPOINT_PATH.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix="mirror_forward_checkpoint.",
            suffix=".tmp",
            dir=str(parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp_path, CHECKPOINT_PATH)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except OSError as e:
        print(f"  WARN: could not write checkpoint: {e}", file=sys.stderr)


def _save_checkpoint(
    *,
    guild_id: str,
    mirror_channel_id: str,
    last_completed_message_id: str,
    mirror_label: str,
    last_jump_url: str,
) -> None:
    root = _load_checkpoint_root()
    ch_id = str(mirror_channel_id).strip()
    root.setdefault("by_channel", {})
    assert isinstance(root["by_channel"], dict)
    root["by_channel"][ch_id] = {
        "guild_id": str(guild_id).strip(),
        "mirror_channel_id": ch_id,
        "last_completed_message_id": str(last_completed_message_id).strip(),
        "mirror_label": str(mirror_label).strip(),
        "last_jump_url": str(last_jump_url).strip(),
        "updated_at_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _save_checkpoint_root(root)


def _parse_random_delay_minutes_range(spec: str) -> tuple[float, float]:
    """Parse '1-3' or '1.5 - 2' into (lo, hi) minutes inclusive for random.uniform."""
    s = (spec or "").strip().replace(" ", "")
    if not s:
        raise ValueError("Empty delay range.")
    if "-" not in s:
        raise ValueError(f'Expected MIN-MAX minutes, got: {spec!r}')
    a_s, b_s = s.split("-", 1)
    lo_m = float(a_s)
    hi_m = float(b_s)
    if lo_m <= 0 or hi_m <= 0:
        raise ValueError("Random delay minutes must be positive.")
    lo, hi = (lo_m, hi_m) if lo_m <= hi_m else (hi_m, lo_m)
    return (lo, hi)


def _react_mirror_optional(
    channel_id: str,
    message_id: str,
    token: str,
    args: argparse.Namespace,
    emoji: str,
    *,
    label: str,
) -> bool:
    if args.dry_run or args.no_react:
        return True
    rr = mm.add_message_reaction(str(channel_id), str(message_id), token, emoji=emoji)
    if rr.status_code not in (204, 200):
        print(
            f"  {label} react HTTP {rr.status_code}: {(rr.text or '')[:160]}",
            file=sys.stderr,
        )
        return False
    return True


def _store_rows(routes: dict[str, dict]) -> list[tuple[str, str, dict]]:
    """(channel_id, display_label, route_dict) sorted by label."""
    rows: list[tuple[str, str, dict]] = []
    for cid, r in routes.items():
        if not isinstance(r, dict):
            continue
        lab = str(r.get("mirror_label") or r.get("m_lead_slug") or cid).strip()
        rows.append((cid, lab, r))
    rows.sort(key=lambda x: x[1].lower())
    return rows


def _match_store_key(rows: list[tuple[str, str, dict]], key: str) -> tuple[str, str, dict] | None:
    k = (key or "").strip().lower()
    if not k:
        return None
    for cid, lab, r in rows:
        if cid == key.strip():
            return (cid, lab, r)
        if lab.lower() == k:
            return (cid, lab, r)
        slug = str(r.get("m_lead_slug") or "").strip().lower()
        if slug and slug == k:
            return (cid, lab, r)
    return None


def _prompt_yes(prompt: str, default_yes: bool = True) -> bool:
    suffix = " [Y/n]: " if default_yes else " [y/N]: "
    raw = input(prompt + suffix).strip().lower()
    if not raw:
        return default_yes
    return raw in ("y", "yes")


def _reply_done_text(mlead_full: dict) -> str:
    t = str(mlead_full.get("forward_reply_done_text") or "done").strip()
    return t if t else "done"


def _want_reply_done(args: argparse.Namespace, mlead_full: dict) -> bool:
    if getattr(args, "no_reply_done", False):
        return False
    if getattr(args, "reply_done", False):
        return True
    v = mlead_full.get("forward_reply_done")
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)


def _is_menu_style_argv() -> bool:
    """
    True when the process was started without --store, --url, or --all-stores on the command line
    (interactive store menu or double-click batch with only delays, etc.).
    Seeding runs like: py mirror_forward_queue.py --store walmart --yes --url \"...\" are False.
    """
    low = {a.lower() for a in sys.argv[1:]}
    if "--store" in low or "--url" in low or "--all-stores" in low:
        return False
    return True


def _effective_reply_done_eof_menu(args: argparse.Namespace, mlead_full: dict) -> bool:
    """forward_reply_done / --reply-done only apply for menu-style invocations (see _is_menu_style_argv)."""
    return _want_reply_done(args, mlead_full) and _is_menu_style_argv()


def _post_mirror_done_reply_optional(
    *,
    guild_id: str,
    link_ch: str,
    mirror_message_id: str,
    token: str,
    args: argparse.Namespace,
    body: str,
) -> None:
    if args.dry_run:
        print(f"  dry-run: would reply {body!r} to Mirror message {mirror_message_id}")
        return
    r = mm.post_channel_message_reply(
        str(link_ch), str(guild_id), str(mirror_message_id), body, token
    )
    if r.status_code not in (200, 201):
        print(
            f"  WARN: Mirror 'done' reply HTTP {r.status_code}: {(r.text or '')[:160]}",
            file=sys.stderr,
        )
    else:
        print(f"  replied {body!r} to Mirror message")


def _retry_after_seconds(resp: object) -> float:
    ra = getattr(resp, "headers", {}).get("Retry-After") if resp is not None else None
    if ra is None:
        return 1.0
    try:
        return float(ra)
    except (TypeError, ValueError):
        return 1.0


def _send_chunks(
    dest_channel_id: str,
    line: str,
    token: str,
    *,
    dry_run: bool,
) -> tuple[bool, str | None, str]:
    """Returns (ok, last_posted_message_id_or_none, error_detail)."""
    parts = _rb._chunk_message(line)
    if not parts:
        return False, None, "empty content after chunking"
    last_mid: str | None = None
    if dry_run:
        return True, None, ""
    for part in parts:
        last_resp = None
        for _ in range(5):
            r = mm.post_channel_message(dest_channel_id, part, token)
            last_resp = r
            if r.status_code == 200:
                try:
                    j = r.json()
                    if isinstance(j, dict) and j.get("id"):
                        last_mid = str(j["id"])
                except Exception:
                    pass
                break
            if r.status_code == 429:
                time.sleep(_retry_after_seconds(r))
                continue
            return False, last_mid, (r.text or "")[:400]
        else:
            return False, last_mid, (getattr(last_resp, "text", None) or "")[:400]
    return True, last_mid, ""


def _parse_string_list(val: object) -> list[str]:
    if val is None:
        return []
    if isinstance(val, (list, tuple)):
        return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, str) and val.strip():
        return [p.strip() for p in val.split(",") if p.strip()]
    return []


def _parse_post_confirmation_settings(raw: object) -> dict | None:
    """Return normalized dict or None if waiting is disabled."""
    if not isinstance(raw, dict):
        return None
    aid = str(raw.get("author_user_id") or "").strip()
    if not aid:
        return None
    try:
        timeout_s = float(raw.get("timeout_seconds", 120))
    except (TypeError, ValueError):
        timeout_s = 120.0
    try:
        timeout_hd = float(raw.get("timeout_seconds_hdnation", 360))
    except (TypeError, ValueError):
        timeout_hd = 360.0
    try:
        poll_s = float(raw.get("poll_interval_seconds", 2))
    except (TypeError, ValueError):
        poll_s = 2.0
    sub = str(raw.get("text_substring") or "Lead posted").strip() or "Lead posted"
    success_extra = _parse_string_list(raw.get("success_substrings"))
    failure_subs = _parse_string_list(raw.get("failure_substrings"))
    maint_start = _parse_string_list(raw.get("maintenance_start_substrings"))
    maint_done = _parse_string_list(raw.get("maintenance_done_substrings"))
    try:
        maint_extend_s = float(raw.get("maintenance_extend_seconds", 600))
    except (TypeError, ValueError):
        maint_extend_s = 600.0
    try:
        maint_post_done_s = float(raw.get("maintenance_post_done_grace_seconds", 180))
    except (TypeError, ValueError):
        maint_post_done_s = 180.0
    return {
        "author_user_id": aid,
        "text_substring": sub,
        "timeout_seconds": max(0.0, timeout_s),
        "timeout_seconds_hdnation": max(0.0, timeout_hd),
        "poll_interval_seconds": max(0.4, poll_s),
        "success_substrings_extra": success_extra,
        "failure_substrings": failure_subs,
        "maintenance_start_substrings": maint_start,
        "maintenance_done_substrings": maint_done,
        "maintenance_extend_seconds": max(0.0, maint_extend_s),
        "maintenance_post_done_grace_seconds": max(30.0, maint_post_done_s),
    }


def _monitor_needles_for_command(cmd: str, pc: dict) -> tuple[list[str], list[str], float]:
    """(success_needles, failure_needles, timeout_seconds) for monitor wait."""
    if cmd == "hdnation":
        return mm.hdnation_monitor_needles_from_post_confirmation(pc)
    return mm.lead_monitor_needles_from_post_confirmation(pc)


def _append_hdnation_failure_record(
    *,
    mirror_channel_id: str,
    mirror_message_id: str,
    command_line: str,
    outcome: str,
    detail: str,
) -> None:
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mirror_channel_id": str(mirror_channel_id).strip(),
        "mirror_message_id": str(mirror_message_id).strip(),
        "command_line": command_line,
        "outcome": outcome,
        "detail": detail,
    }
    try:
        with open(HDNATION_FAIL_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"  WARN: could not append hdnation failure log: {e}", file=sys.stderr)


def _forward_all_stores_order(mlead_full: dict) -> list[str]:
    raw = mlead_full.get("forward_all_stores_order")
    if isinstance(raw, list) and raw:
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str) and raw.strip():
        return [p.strip() for p in raw.split(",") if p.strip()]
    return ["walmart", "target", "lowes", "homedepot", "hd-clearance stock"]


def _ordered_store_rows(
    rows: list[tuple[str, str, dict]], order_labels: list[str]
) -> list[tuple[str, str, dict]]:
    by_label: dict[str, tuple[str, str, dict]] = {}
    for cid, lab, r in rows:
        by_label[lab.lower()] = (cid, lab, r)
    out: list[tuple[str, str, dict]] = []
    seen_cids: set[str] = set()
    for key in order_labels:
        lk = key.strip().lower()
        row = by_label.get(lk)
        if row is not None and row[0] not in seen_cids:
            out.append(row)
            seen_cids.add(row[0])
    rest = [row for row in rows if row[0] not in seen_cids]
    rest.sort(key=lambda x: x[1].lower())
    out.extend(rest)
    return out


@dataclass
class ForwardBatchResult:
    ok_n: int = 0
    skip_n: int = 0
    dedupe_n: int = 0
    hdnation_monitor_fail_n: int = 0
    fail_n: int = 0
    messages_processed: int = 0
    aborted: bool = False


def _notify_forward_abort(args: argparse.Namespace, store_label: str, detail: str) -> None:
    notify_run_error(
        "Clearance all-stores",
        f"{store_label}: {detail}",
        enabled=getattr(args, "notify", True),
        dry_run=args.dry_run,
    )


def _run_forward_batch(
    *,
    start_msg: dict,
    link_ch: str,
    guild_id: str,
    channel: dict,
    token: str,
    route_for_build: dict,
    store_label: str,
    dest_command: str,
    post_ch: str,
    post_confirm: dict | None,
    args: argparse.Namespace,
    use_checkpoint: bool,
    max_messages: int,
    random_delay_minutes: tuple[float, float] | None,
    delay_f: float,
    slug_ov: str,
    skip_start_prompt: bool,
    reply_done_eof_menu: bool,
    reply_done_text: str,
) -> ForwardBatchResult:
    """
    Walk forward from start_msg in link_ch. max_messages <= 0 means no cap.
    If skip_start_prompt, do not print the pre-loop summary (used when --all-stores already printed).
    """
    res = ForwardBatchResult()
    route_cmd = mm.route_command(route_for_build)
    prev_msg_deal_key: str | None = None
    last_completed_for_reply: str | None = None
    command_author_id = mm.resolve_discord_user_id(token)

    if not skip_start_prompt and not args.yes and not args.dry_run:
        max_disp = "unlimited" if max_messages <= 0 else str(max_messages)
        print(
            f"\n[{store_label}] process up to {max_disp} message(s) from id {start_msg.get('id')}.\n"
            f"!m line destination (<#...>): {dest_command}\n"
            f"POST !m text to channel id: {post_ch}\n"
            f"Dry-run: {args.dry_run}   React: {not args.no_react}   "
            f"Dedupe: {not args.no_dedupe}   "
            f"Wait for monitor: {bool(post_confirm)}   "
            f"Checkpoint: {use_checkpoint}   "
            f"Reply-done (menu, end-of-queue only): {reply_done_eof_menu}\n"
            f"Between messages: "
            + (
                f"random {random_delay_minutes[0]:g}-{random_delay_minutes[1]:g} min\n"
                if random_delay_minutes
                else f"fixed {delay_f}s\n"
            )
        )
        if not _prompt_yes("Start?", default_yes=True):
            print("Cancelled.")
            res.aborted = True
            return res

    for msg in mm.iter_channel_forward_from_start(
        start_msg,
        str(link_ch),
        token,
        max_messages=max_messages,
    ):
        res.messages_processed += 1
        mid = str(msg.get("id") or "")
        deal_key = mm.message_dedupe_key(msg, route_for_build)
        if (
            deal_key is not None
            and not args.no_dedupe
            and prev_msg_deal_key is not None
            and deal_key == prev_msg_deal_key
        ):
            print(f"  skip id={mid}: dedupe (same product as previous message: {deal_key})")
            res.dedupe_n += 1
            if _react_mirror_optional(
                str(link_ch), mid, token, args, DEDUPE_SKIP_EMOJI, label="Dedupe skip"
            ):
                print("  reacted (duplicate skip)")
            continue

        try:
            line = mm.build_command_line_for_route(
                msg,
                route_for_build,
                channel=channel,
                dest_override=dest_command,
                source_slug_override=slug_ov,
            )
        except ValueError as e:
            print(f"  skip id={mid}: {e}")
            res.skip_n += 1
            prev_msg_deal_key = deal_key
            continue

        preview = line if len(line) <= 160 else line[:160] + "..."
        print(f"\n--- [{store_label}] id={mid} ---\n{preview}")

        ok_send, posted_mid, err_txt = _send_chunks(post_ch, line, token, dry_run=args.dry_run)
        if not ok_send:
            print(f"  SEND FAIL: {err_txt}", file=sys.stderr)
            res.fail_n += 1
            res.aborted = True
            _notify_forward_abort(args, store_label, f"SEND FAIL: {err_txt}")
            break

        print("  sent OK")

        confirm_ok = True
        if post_confirm and not args.dry_run:
            if not posted_mid:
                print(
                    "  CONFIRM FAIL: send response had no message id; cannot wait for monitor.",
                    file=sys.stderr,
                )
                res.fail_n += 1
                res.aborted = True
                _notify_forward_abort(args, store_label, "POST returned no message id")
                break
            succ_needles, fail_needles, mon_timeout = _monitor_needles_for_command(
                route_cmd, post_confirm
            )
            wait_label = "hdnation stock-check" if route_cmd == "hdnation" else "lead posted"
            if route_cmd == "hdnation":
                hdnation_sku = mm.parse_hdnation_sku_from_command_line(line)
                if mon_timeout <= 0:
                    wait_note = "poll until Home Depot Nationwide Stock Check (no time limit)"
                else:
                    wait_note = (
                        f"poll until stock-check (safety limit {mon_timeout:.0f}s)"
                    )
            elif mon_timeout <= 0:
                wait_note = f"poll until {wait_label} (no time limit)"
            else:
                wait_note = f"poll until {wait_label} (safety limit {mon_timeout:.0f}s)"
            print(
                f"  waiting: {wait_note} — monitor author {post_confirm['author_user_id']!r}, "
                f"session until our next !m"
            )
            if route_cmd == "hdnation":
                if not hdnation_sku:
                    print("  CONFIRM FAIL: could not parse SKU from !m hdnation line", file=sys.stderr)
                    res.fail_n += 1
                    res.aborted = True
                    _notify_forward_abort(args, store_label, "could not parse SKU from !m hdnation line")
                    break
                outcome, mon_detail = mm.wait_for_hdnation_stock_check(
                    post_ch,
                    posted_mid,
                    token,
                    author_user_id=post_confirm["author_user_id"],
                    success_needles=succ_needles,
                    failure_needles=fail_needles,
                    correlate_sku=hdnation_sku,
                    maintenance_start_needles=post_confirm.get("maintenance_start_substrings") or [],
                    maintenance_done_needles=post_confirm.get("maintenance_done_substrings") or [],
                    maintenance_extend_seconds=float(post_confirm.get("maintenance_extend_seconds") or 0.0),
                    maintenance_post_done_grace_seconds=float(
                        post_confirm.get("maintenance_post_done_grace_seconds") or 180.0
                    ),
                    timeout_seconds=mon_timeout,
                    poll_interval_seconds=post_confirm["poll_interval_seconds"],
                    command_author_user_id=command_author_id,
                    on_status=lambda msg: print(f"  {msg}"),
                )
            else:
                outcome, mon_detail = mm.wait_for_lead_posted(
                    post_ch,
                    posted_mid,
                    token,
                    author_user_id=post_confirm["author_user_id"],
                    success_needles=succ_needles,
                    failure_needles=fail_needles,
                    maintenance_start_needles=post_confirm.get("maintenance_start_substrings") or [],
                    maintenance_done_needles=post_confirm.get("maintenance_done_substrings") or [],
                    maintenance_extend_seconds=float(post_confirm.get("maintenance_extend_seconds") or 0.0),
                    maintenance_post_done_grace_seconds=float(
                        post_confirm.get("maintenance_post_done_grace_seconds") or 180.0
                    ),
                    timeout_seconds=mon_timeout,
                    poll_interval_seconds=post_confirm["poll_interval_seconds"],
                    command_author_user_id=command_author_id,
                    on_status=lambda msg: print(f"  {msg}"),
                )
            if outcome == "ok":
                print("  monitor confirmed")
            elif outcome == "fail":
                print(f"  MONITOR STOCK/FETCH FAIL: {mon_detail}", file=sys.stderr)
                res.hdnation_monitor_fail_n += 1
                _append_hdnation_failure_record(
                    mirror_channel_id=str(link_ch),
                    mirror_message_id=mid,
                    command_line=line,
                    outcome=outcome,
                    detail=mon_detail,
                )
                print("  logged to mirror_forward_hdnation_failures.jsonl (no checkpoint advance)")
                if _react_mirror_optional(
                    str(link_ch), mid, token, args, DEDUPE_SKIP_EMOJI, label="Monitor fail"
                ):
                    print("  reacted (monitor failure)")
                if random_delay_minutes is not None:
                    lo_m, hi_m = random_delay_minutes
                    pause_s = random.uniform(lo_m * 60.0, hi_m * 60.0)
                    print(f"  pausing {pause_s:.0f}s before next message…")
                    time.sleep(pause_s)
                elif delay_f > 0:
                    time.sleep(delay_f)
                continue
            else:
                confirm_ok = False
                confirm_note = mon_detail or outcome
                print(f"  CONFIRM FAIL: {confirm_note}", file=sys.stderr)
                res.fail_n += 1
                res.aborted = True
                _notify_forward_abort(args, store_label, confirm_note)
                break

        mirror_react_ok = True
        if not args.dry_run and not args.no_react and confirm_ok:
            mirror_react_ok = _react_mirror_optional(
                str(link_ch), mid, token, args, CHECKMARK_EMOJI, label="OK"
            )
            if mirror_react_ok:
                print("  reacted")
        elif args.no_react and confirm_ok:
            mirror_react_ok = True

        completed = not args.dry_run and confirm_ok and mirror_react_ok
        if use_checkpoint and completed:
            _save_checkpoint(
                guild_id=guild_id,
                mirror_channel_id=str(link_ch),
                last_completed_message_id=mid,
                mirror_label=store_label,
                last_jump_url=f"https://discord.com/channels/{guild_id}/{link_ch}/{mid}",
            )
        if completed:
            last_completed_for_reply = mid

        res.ok_n += 1
        prev_msg_deal_key = deal_key
        if random_delay_minutes is not None:
            lo_m, hi_m = random_delay_minutes
            pause_s = random.uniform(lo_m * 60.0, hi_m * 60.0)
            print(f"  pausing {pause_s:.0f}s (~{pause_s / 60.0:.2f} min) before next message…")
            time.sleep(pause_s)
        elif delay_f > 0:
            time.sleep(delay_f)

    if (
        reply_done_eof_menu
        and last_completed_for_reply
        and not res.aborted
    ):
        if args.dry_run:
            print(
                f"  dry-run: if no newer Mirror messages after id {last_completed_for_reply}, "
                f"would reply {reply_done_text!r} to that message"
            )
        else:
            try:
                tail = mm.list_messages_after(
                    str(link_ch), last_completed_for_reply, token, limit=1
                )
            except RuntimeError as e:
                print(f"  WARN: could not check for newer Mirror messages: {e}", file=sys.stderr)
                tail = None
            if tail is None:
                pass
            elif not tail:
                _post_mirror_done_reply_optional(
                    guild_id=guild_id,
                    link_ch=str(link_ch),
                    mirror_message_id=last_completed_for_reply,
                    token=token,
                    args=args,
                    body=reply_done_text,
                )
            else:
                print(
                    "  (no 'done' reply: newer Mirror messages exist after the last completed id)"
                )

    return res


def _pick_store_interactive(rows: list[tuple[str, str, dict]]) -> tuple[str, str, dict] | None:
    print("\nStores (from m_lead_routes.json):\n")
    print(
        "  0. Run ALL stores  (checkpoint or calendar date; order: forward_all_stores_order "
        "in m_lead_routes.json)"
    )
    for i, (cid, lab, _r) in enumerate(rows, start=1):
        print(f"  {i}. {lab}  (Mirror channel id {cid})")
    print()
    raw = input("Enter number (0 = all stores, or blank to cancel): ").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        print("Invalid number.", file=sys.stderr)
        return None
    if n == 0:
        return (ALL_STORES_MENU_SENTINEL, "", {})
    if n < 1 or n > len(rows):
        print("Out of range.", file=sys.stderr)
        return None
    cid, lab, r = rows[n - 1]
    return (cid, lab, r)


def _run_all_stores(
    args: argparse.Namespace,
    routes: dict[str, dict],
    mlead_full: dict,
    post_confirm: dict | None,
    rows: list[tuple[str, str, dict]],
    start_plan: ForwardStartPlan,
) -> int:
    """Run every route in order; each store starts from checkpoint or a shared calendar date."""
    if args.no_checkpoint:
        print("--all-stores requires checkpoints for saving progress (omit --no-checkpoint).", file=sys.stderr)
        return 2
    if (args.store or "").strip() or (args.url or "").strip():
        print("--all-stores cannot be used with --store or --url.", file=sys.stderr)
        return 2
    if (args.dest or "").strip():
        print("--all-stores cannot be used with --dest.", file=sys.stderr)
        return 2
    if not args.dry_run and not args.yes:
        print("--all-stores requires --yes unless --dry-run.", file=sys.stderr)
        return 2
    if not mm.load_fetch_token_chain():
        print(
            "No Discord token: configure DailyScheduleReminder/config.secrets.json.",
            file=sys.stderr,
        )
        return 1

    max_n = int(args.max)
    delay_f = max(0.0, float(args.delay))
    rd_str = (args.delay_random_minutes or "").strip()
    if not rd_str:
        jrd = mlead_full.get("forward_delay_random_minutes")
        if isinstance(jrd, (list, tuple)) and len(jrd) >= 2:
            rd_str = f"{jrd[0]}-{jrd[1]}"
        elif isinstance(jrd, str) and jrd.strip():
            rd_str = jrd.strip()
    random_delay_minutes: tuple[float, float] | None = None
    if rd_str:
        try:
            random_delay_minutes = _parse_random_delay_minutes_range(rd_str)
        except ValueError as e:
            print(
                f"Invalid --delay-random-minutes / forward_delay_random_minutes: {e}",
                file=sys.stderr,
            )
            return 1

    ordered = _ordered_store_rows(rows, _forward_all_stores_order(mlead_full))
    post_ch = (args.post_channel or "").strip() or str(
        mlead_full.get("command_post_channel_id") or ""
    ).strip()
    if not post_ch:
        post_ch = str(ordered[0][2].get("destination_channel_id") or "").strip()
    if not post_ch:
        print(
            "No command_post_channel_id in m_lead_routes.json (and no --post-channel); "
            "cannot determine where to POST !m lines.",
            file=sys.stderr,
        )
        return 1

    use_checkpoint = True
    slug_ov = (args.source_slug or "").strip()
    reply_done_eof_menu = _effective_reply_done_eof_menu(args, mlead_full)
    reply_done_text = _reply_done_text(mlead_full)
    labels = " → ".join(lab for _cid, lab, _r in ordered)
    max_disp = "unlimited" if max_n <= 0 else str(max_n)
    root0 = _load_checkpoint_root()
    print_checkpoint_summary(ordered, root0)
    if start_plan.mode == START_MODE_DATE and start_plan.start_date:
        print(
            f"\n--all-stores: start from date {start_plan.start_date.isoformat()} "
            f"(local day, every store).\nOrder: {labels}\n"
            f"Global --max: {max_disp} (every message walked, including skips/dedupes).\n"
            f"POST !m to channel id: {post_ch}\n"
        )
    else:
        print(
            f"\n--all-stores: resume from checkpoint.\nOrder: {labels}\n"
            f"Global --max: {max_disp} (every message walked, including skips/dedupes).\n"
            f"POST !m to channel id: {post_ch}\n"
        )
        without_cp: list[str] = []
        for cid, lab, _r in ordered:
            if not _get_channel_checkpoint(root0, str(cid)):
                without_cp.append(lab)
        if without_cp:
            print(
                f"Stores with no checkpoint (skipped in checkpoint mode): {', '.join(without_cp)}\n"
                "Tip: use start mode 2 (calendar date) to include stores without a checkpoint.\n"
            )

    budget: int | None = None if max_n <= 0 else max_n
    agg = ForwardBatchResult()
    chain = mm.load_fetch_token_chain()
    if not chain:
        print(
            "No Discord token: configure DailyScheduleReminder/config.secrets.json.",
            file=sys.stderr,
        )
        return 1
    token = chain[0][1]

    for sel in ordered:
        mw_channel_id, store_label, route_for_build = sel[0], sel[1], sel[2]
        if budget is not None and budget <= 0:
            print("\nGlobal --max exhausted; stopping.")
            break
        link_ch = str(mw_channel_id)
        root = _load_checkpoint_root()
        entry = _get_channel_checkpoint(root, link_ch)
        resolved: tuple[dict, dict, str, str] | None = None
        if start_plan.mode == START_MODE_DATE and start_plan.start_date:
            resolved = _resolve_store_start_from_date(
                store_label=store_label,
                link_ch=link_ch,
                start_date=start_plan.start_date,
                token=token,
                entry=entry,
            )
        else:
            if not entry:
                print(f"\n[{store_label}] no checkpoint for channel {mw_channel_id}; skipping.")
                continue
            resolved = _resolve_store_start_from_checkpoint(
                store_label=store_label,
                link_ch=link_ch,
                entry=entry,
                token=token,
            )
        if not resolved:
            continue
        start_msg, channel, token, guild_id = resolved
        dest_command = str(route_for_build.get("destination_channel_id") or "").strip()
        if not dest_command:
            print(
                f"\n[{store_label}] missing destination_channel_id on route; skipping.",
                file=sys.stderr,
            )
            continue
        cap = budget if budget is not None else 0
        batch = _run_forward_batch(
            start_msg=start_msg,
            link_ch=str(link_ch),
            guild_id=guild_id,
            channel=channel,
            token=token,
            route_for_build=route_for_build,
            store_label=store_label,
            dest_command=dest_command,
            post_ch=post_ch,
            post_confirm=post_confirm,
            args=args,
            use_checkpoint=use_checkpoint,
            max_messages=cap,
            random_delay_minutes=random_delay_minutes,
            delay_f=delay_f,
            slug_ov=slug_ov,
            skip_start_prompt=True,
            reply_done_eof_menu=reply_done_eof_menu,
            reply_done_text=reply_done_text,
        )
        if budget is not None:
            budget -= batch.messages_processed
        agg.ok_n += batch.ok_n
        agg.skip_n += batch.skip_n
        agg.dedupe_n += batch.dedupe_n
        agg.hdnation_monitor_fail_n += batch.hdnation_monitor_fail_n
        agg.fail_n += batch.fail_n
        if batch.aborted:
            if batch.fail_n:
                print(
                    f"\n  WARN: [{store_label}] stopped early (error). Continuing --all-stores "
                    f"with the next store (forwarded_ok so far={agg.ok_n}).",
                    file=sys.stderr,
                )
                continue
            return 0

    print(
        f"\nDone (--all-stores). forwarded_ok={agg.ok_n}  skipped_parse={agg.skip_n}  "
        f"skipped_dedupe={agg.dedupe_n}  hdnation_monitor_fail={agg.hdnation_monitor_fail_n}  "
        f"failed={agg.fail_n}  dry_run={args.dry_run}"
    )
    notify_batch_finished(
        "Clearance all-stores",
        (
            f"forwarded_ok={agg.ok_n}  failed={agg.fail_n}  "
            f"hdnation_monitor_fail={agg.hdnation_monitor_fail_n}  "
            f"skipped_dedupe={agg.dedupe_n}"
        ),
        had_errors=agg.fail_n > 0 or agg.hdnation_monitor_fail_n > 0,
        enabled=getattr(args, "notify", True),
        dry_run=args.dry_run,
    )
    return 0 if agg.fail_n == 0 else 2


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Forward Mirror World deal messages to RS: build !m line, send, react on success."
    )
    ap.add_argument(
        "--store",
        default="",
        help="Route key: mirror_label / m_lead_slug / Mirror channel id (non-interactive).",
    )
    ap.add_argument("--url", default="", help="Start message jump link (non-interactive).")
    ap.add_argument(
        "--max",
        type=int,
        default=0,
        help="Max messages to attempt including the start message (0 = no cap; default 0).",
    )
    ap.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Fixed seconds after each completed message before the next (ignored if random minutes set).",
    )
    ap.add_argument(
        "--delay-random-minutes",
        default="",
        metavar="MIN-MAX",
        help='Random minutes between completed messages, e.g. "1-3". Overrides --delay. '
        'If omitted, can use forward_delay_random_minutes in m_lead_routes.json.',
    )
    ap.add_argument("--dry-run", action="store_true", help="Parse and print only; no POST or reactions.")
    ap.add_argument("--no-react", action="store_true", help="Do not add check mark on Mirror messages.")
    ap.add_argument(
        "--reply-done",
        action="store_true",
        help="Menu-style runs only (no --store/--url/--all-stores on cmdline): when the batch ends "
        "and there are no newer Mirror messages after the last completed deal, POST one reply "
        "(forward_reply_done_text, default done). Ignored for CLI seeding. Or set forward_reply_done in JSON.",
    )
    ap.add_argument(
        "--no-reply-done",
        action="store_true",
        help="Disable forward_reply_done from m_lead_routes.json.",
    )
    ap.add_argument(
        "--dest",
        default="",
        help="Override the <#channel_id> inside the !m line only (not where the line is posted).",
    )
    ap.add_argument(
        "--post-channel",
        default="",
        help="Override command_post_channel_id: channel to POST the !m text (default from m_lead_routes.json).",
    )
    ap.add_argument(
        "--source-slug",
        default="",
        help="Override !m lead slug (passed to build_command_line_for_route).",
    )
    ap.add_argument(
        "--yes",
        action="store_true",
        help="Non-interactive: skip confirmation prompts (use with --store and --url).",
    )
    ap.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Do not skip when the previous message had the same product id (lead: UPC/TCIN/SKU; hdnation: SKU).",
    )
    ap.add_argument(
        "--no-wait-confirm",
        action="store_true",
        help="Do not wait for post_confirmation monitor message (react right after send).",
    )
    ap.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Do not read or write mirror_forward_checkpoint.json (no resume from last run).",
    )
    ap.add_argument(
        "--from-date",
        default="",
        metavar="DATE",
        help="Start from first Mirror message on/after this local calendar day (all stores or "
        "single store). Formats: YYYY-MM-DD, MM-DD-YY, MM-DD, MM/DD. Implies date mode; "
        "checkpoints still update on each success.",
    )
    ap.add_argument(
        "--all-stores",
        action="store_true",
        help="Run every route in forward_all_stores_order (m_lead_routes.json). Each store starts "
        "from checkpoint or --from-date. Per-store errors do not stop later stores. "
        "--max counts messages across all stores. Requires --yes unless --dry-run. "
        "Incompatible with --store, --url, --dest, and --no-checkpoint.",
    )
    ap.add_argument(
        "--notify",
        action=argparse.BooleanOptionalAction,
        default=notify_enabled_by_default(),
        help="Windows desktop alert on finish or hard error (default on Windows).",
    )
    args = ap.parse_args()

    routes = mm.load_routes()
    mlead_full = mm.load_m_lead_file()
    post_confirm = _parse_post_confirmation_settings(mlead_full.get("post_confirmation"))
    if args.no_wait_confirm:
        post_confirm = None
    rows = _store_rows(routes)
    if not rows:
        print("No routes in m_lead_routes.json.", file=sys.stderr)
        return 1

    if args.all_stores:
        plan = prompt_forward_start_plan(
            interactive=not args.yes and not (args.from_date or "").strip(),
            cli_from_date=(args.from_date or ""),
            for_all_stores=True,
        )
        if plan is None:
            return 2
        return _run_all_stores(args, routes, mlead_full, post_confirm, rows, plan)

    sel: tuple[str, str, dict] | None = None
    store_key = (args.store or "").strip()

    if store_key:
        sel = _match_store_key(rows, store_key)
        if not sel:
            print(f"No route matches --store {store_key!r}.", file=sys.stderr)
            return 1
    else:
        sel = _pick_store_interactive(rows)
        if not sel:
            print("Cancelled.")
            return 0
        if sel[0] == ALL_STORES_MENU_SENTINEL:
            ordered = _ordered_store_rows(rows, _forward_all_stores_order(mlead_full))
            print_checkpoint_summary(ordered)
            plan = prompt_forward_start_plan(
                interactive=True,
                cli_from_date="",
                for_all_stores=True,
            )
            if not plan:
                print("Cancelled.")
                return 0
            if not args.dry_run:
                if not _prompt_yes("Proceed with this all-stores run?", default_yes=False):
                    print("Cancelled.")
                    return 0
            args.yes = True
            args.all_stores = True
            return _run_all_stores(args, routes, mlead_full, post_confirm, rows, plan)

    mw_channel_id, store_label, _ = sel
    dest_override = (args.dest or "").strip()
    slug_ov = (args.source_slug or "").strip()
    use_checkpoint = not args.no_checkpoint

    if not mm.load_fetch_token_chain():
        print(
            "No Discord token: configure DailyScheduleReminder/config.secrets.json.",
            file=sys.stderr,
        )
        return 1

    max_n = int(args.max)
    delay_f = max(0.0, float(args.delay))
    rd_str = (args.delay_random_minutes or "").strip()
    if not rd_str:
        jrd = mlead_full.get("forward_delay_random_minutes")
        if isinstance(jrd, (list, tuple)) and len(jrd) >= 2:
            rd_str = f"{jrd[0]}-{jrd[1]}"
        elif isinstance(jrd, str) and jrd.strip():
            rd_str = jrd.strip()
    random_delay_minutes: tuple[float, float] | None = None
    if rd_str:
        try:
            random_delay_minutes = _parse_random_delay_minutes_range(rd_str)
        except ValueError as e:
            print(f"Invalid --delay-random-minutes / forward_delay_random_minutes: {e}", file=sys.stderr)
            return 1

    guild_id = ""
    link_ch = ""
    start_mid = ""
    start_msg: dict = {}
    channel: dict = {}
    token = ""
    url_in = (args.url or "").strip()

    if url_in:
        url = url_in
        try:
            guild_id, link_ch, start_mid = mm.parse_jump_url(url)
        except ValueError as e:
            print(e, file=sys.stderr)
            return 1
        if str(link_ch) != str(mw_channel_id):
            print(
                f"\n[WARN] Link channel id {link_ch} != selected store channel {mw_channel_id}.\n"
                f"        Selected store: {store_label}\n",
                file=sys.stderr,
            )
            if not args.yes:
                if not _prompt_yes("Continue anyway?", default_yes=False):
                    print("Cancelled.")
                    return 0
        try:
            start_msg, channel, _lab, token = mm.fetch_message_with_token_fallback(
                guild_id, link_ch, start_mid
            )
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            return 1
    else:
        print(f"\nSelected: {store_label}  (Mirror channel {mw_channel_id})\n")
        cp_root = _load_checkpoint_root() if use_checkpoint else {"by_channel": {}}
        r_cp = _get_channel_checkpoint(cp_root, str(mw_channel_id))
        deal_d, saved, mid = _checkpoint_row_summary(r_cp, str(mw_channel_id))
        print("Checkpoint for this store:")
        print(f"  last deal (local): {deal_d}   checkpoint saved: {saved}   message id: {mid or '—'}\n")

        chain = mm.load_fetch_token_chain()
        if not chain:
            print("No Discord token.", file=sys.stderr)
            return 1
        token = chain[0][1]
        link_ch = str(mw_channel_id)
        start_plan: ForwardStartPlan | None = None

        if (args.from_date or "").strip():
            try:
                d = parse_calendar_date(args.from_date)
                start_plan = ForwardStartPlan(mode=START_MODE_DATE, start_date=d)
            except ValueError as e:
                print(f"Invalid --from-date: {e}", file=sys.stderr)
                return 1
        elif args.yes:
            if r_cp:
                start_plan = ForwardStartPlan(mode=START_MODE_CHECKPOINT)
            else:
                print(
                    "ERROR: --yes with no saved checkpoint for this store — use --url, "
                    "--from-date, or run interactively.",
                    file=sys.stderr,
                )
                return 1
        elif not args.yes:
            print("How should this run start?\n")
            print("  1. Resume from checkpoint (next message after last completed)")
            print("  2. Start from a calendar date (first message on/after that day)")
            print("  3. Paste a Discord jump link")
            print()
            choice = input("Enter 1, 2, or 3 [default 1]: ").strip()
            if not choice or choice == "1":
                if r_cp:
                    start_plan = ForwardStartPlan(mode=START_MODE_CHECKPOINT)
                else:
                    print("No checkpoint saved; paste a jump link instead.\n")
                    choice = "3"
            if choice == "2":
                try:
                    d = _prompt_calendar_date()
                    start_plan = ForwardStartPlan(mode=START_MODE_DATE, start_date=d)
                except KeyboardInterrupt:
                    print("Cancelled.")
                    return 0
            if choice == "3" or start_plan is None:
                print("\nPaste the Discord jump link for the FIRST deal message to process.\n")
                url = input("Start message link: ").strip()
                if not url.strip():
                    print("Cancelled.")
                    return 0
                try:
                    guild_id, link_ch, start_mid = mm.parse_jump_url(url.strip())
                except ValueError as e:
                    print(e, file=sys.stderr)
                    return 1
                if str(link_ch) != str(mw_channel_id):
                    print(
                        f"\n[WARN] Link channel id {link_ch} != selected store channel {mw_channel_id}.\n"
                        f"        Selected store: {store_label}\n",
                        file=sys.stderr,
                    )
                    if not _prompt_yes("Continue anyway?", default_yes=False):
                        print("Cancelled.")
                        return 0
                try:
                    start_msg, channel, _lab, token = mm.fetch_message_with_token_fallback(
                        guild_id, link_ch, start_mid
                    )
                except RuntimeError as e:
                    print(str(e), file=sys.stderr)
                    return 1
                start_plan = None  # already resolved via url

        if start_plan is not None:
            resolved: tuple[dict, dict, str, str] | None = None
            if start_plan.mode == START_MODE_DATE and start_plan.start_date:
                resolved = _resolve_store_start_from_date(
                    store_label=store_label,
                    link_ch=link_ch,
                    start_date=start_plan.start_date,
                    token=token,
                    entry=r_cp,
                )
            elif start_plan.mode == START_MODE_CHECKPOINT and r_cp:
                resolved = _resolve_store_start_from_checkpoint(
                    store_label=store_label,
                    link_ch=link_ch,
                    entry=r_cp,
                    token=token,
                )
            if not resolved:
                return 0
            start_msg, channel, token, guild_id = resolved
            start_mid = str(start_msg.get("id") or "")
            url = f"https://discord.com/channels/{guild_id}/{link_ch}/{start_mid}"
        elif not start_msg:
            print("No start message resolved.", file=sys.stderr)
            return 1

    route_for_build = mm.route_for_channel(routes, str(link_ch))
    if route_for_build is None:
        print(
            f"No m_lead_routes.json entry for the link's channel_id={link_ch}.",
            file=sys.stderr,
        )
        return 1

    dest_command = dest_override
    if not dest_command and route_for_build:
        dest_command = str(route_for_build.get("destination_channel_id") or "").strip()
    if not dest_command:
        print("No destination_channel_id on route for the !m line; pass --dest.", file=sys.stderr)
        return 1

    post_ch = (args.post_channel or "").strip() or str(
        mlead_full.get("command_post_channel_id") or ""
    ).strip()
    if not post_ch:
        post_ch = dest_command

    reply_done_eof_menu = _effective_reply_done_eof_menu(args, mlead_full)
    reply_done_text = _reply_done_text(mlead_full)
    batch = _run_forward_batch(
        start_msg=start_msg,
        link_ch=str(link_ch),
        guild_id=guild_id,
        channel=channel,
        token=token,
        route_for_build=route_for_build,
        store_label=store_label,
        dest_command=dest_command,
        post_ch=post_ch,
        post_confirm=post_confirm,
        args=args,
        use_checkpoint=use_checkpoint,
        max_messages=max_n,
        random_delay_minutes=random_delay_minutes,
        delay_f=delay_f,
        slug_ov=slug_ov,
        skip_start_prompt=False,
        reply_done_eof_menu=reply_done_eof_menu,
        reply_done_text=reply_done_text,
    )
    if batch.aborted and batch.fail_n == 0:
        return 0

    print(
        f"\nDone. forwarded_ok={batch.ok_n}  skipped_parse={batch.skip_n}  "
        f"skipped_dedupe={batch.dedupe_n}  "
        f"hdnation_monitor_fail={batch.hdnation_monitor_fail_n}  failed={batch.fail_n}  "
        f"dry_run={args.dry_run}"
    )
    notify_batch_finished(
        f"Clearance forward ({store_label})",
        (
            f"forwarded_ok={batch.ok_n}  failed={batch.fail_n}  "
            f"hdnation_monitor_fail={batch.hdnation_monitor_fail_n}"
        ),
        had_errors=batch.fail_n > 0 or batch.hdnation_monitor_fail_n > 0,
        enabled=getattr(args, "notify", True),
        dry_run=args.dry_run,
    )
    return 0 if batch.fail_n == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
