#!/usr/bin/env python3
"""
Pick a store (from m_lead_routes.json), paste a Mirror World start message link, then walk
newer messages in that channel: build !m lead / !m hdnation (same rules as mirror_message_to_m_lead.py),
POST each !m line to command_post_channel_id (or destination if unset), then optionally wait
for a monitor bot in that post channel (post_confirmation). The <#id> inside the line comes from
each route's destination_channel_id. React on the Mirror World source after send/confirm, then
optionally sleep (--delay seconds, or --delay-random-minutes / m_lead_routes forward_delay) before
the next message.

For !m hdnation, the monitor can take several minutes; use post_confirmation.timeout_seconds_hdnation
and optional success_substrings / failure_substrings in m_lead_routes.json. If the monitor reports
a stock fetch failure (matched failure_substrings), the script reacts with an X on the Mirror source
message, appends a line to mirror_forward_hdnation_failures.jsonl for a later retry run, and
continues to the next Mirror message without advancing the checkpoint.

When post_confirmation is enabled, monitor waits use a session window: monitor-bot messages after
our !m command until the next !m hdnation/lead in the channel (covers Lead posted and multi-message
hdnation stock checks without reply chains).

Consecutive duplicate guard (default on): skips when the previous channel message had the same
parsed UPC/TCIN/SKU (!m lead) or SKU (!m hdnation). A non-deal message in between resets the chain.
On dedupe skip, reacts with cross mark on the skipped Mirror message (unless --no-react).
Use --no-dedupe to turn off.

Checkpoint: mirror_forward_checkpoint.json stores last fully completed Mirror message per
Mirror channel (by_channel map). Legacy single-object files are read and merged on save.
Interactive runs can resume from the next message after that. Use --no-checkpoint to disable.

Optional: when forward_reply_done / --reply-done is on, only for menu-style runs (no --store/--url
/--all-stores on the command line): after the batch ends cleanly, if there are no newer Mirror
messages after the last fully completed deal, POST a single reply (forward_reply_done_text, default
done) to that deal message. CLI seeding (--store + --url) never sends this reply.

--all-stores: run every route in forward_all_stores_order from m_lead_routes.json; each store
resumes only from its checkpoint, then continues to the next store (--max is global). A hard
failure in one store (e.g. network error, send fail) no longer stops the rest; the run continues
and final exit is still non-zero if any store had failures. Requires --yes unless --dry-run.
Interactive menu: choose 0 for the same all-stores run (confirm prompt).

Auth: same token chain as mirror_message_to_m_lead (DailyScheduleReminder + optional MWDiscumBot).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

_BOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BOT_DIR.parent
for _p in (_REPO_ROOT, _BOT_DIR):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

import mirror_message_to_m_lead as mm  # noqa: E402
import reminder_bot as _rb  # noqa: E402

CHECKPOINT_PATH = Path(__file__).resolve().parent / "mirror_forward_checkpoint.json"
HDNATION_FAIL_LOG = Path(__file__).resolve().parent / "mirror_forward_hdnation_failures.jsonl"
DEDUPE_SKIP_EMOJI = "\u274c"  # cross mark (X) on dedupe skip
CHECKMARK_EMOJI = "\u2705"
# Sentinel channel id from _pick_store_interactive when user chooses "0. Run ALL stores".
ALL_STORES_MENU_SENTINEL = "__all_stores_menu__"


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
    return {
        "author_user_id": aid,
        "text_substring": sub,
        "timeout_seconds": max(1.0, timeout_s),
        "timeout_seconds_hdnation": max(1.0, timeout_hd),
        "poll_interval_seconds": max(0.4, poll_s),
        "success_substrings_extra": success_extra,
        "failure_substrings": failure_subs,
        "maintenance_start_substrings": maint_start,
        "maintenance_done_substrings": maint_done,
        "maintenance_extend_seconds": max(0.0, maint_extend_s),
    }


def _monitor_needles_for_command(cmd: str, pc: dict) -> tuple[list[str], list[str], float]:
    """(success_needles, failure_needles, timeout_seconds) for wait_for_monitor_outcome."""
    primary = (pc.get("text_substring") or "Lead posted").strip().lower()
    seen: dict[str, None] = {}
    success: list[str] = []
    for s in [primary, *[x.strip().lower() for x in pc.get("success_substrings_extra") or []]]:
        if s and s not in seen:
            seen[s] = None
            success.append(s)
    timeout = float(pc["timeout_seconds"])
    if cmd == "hdnation":
        timeout = float(pc.get("timeout_seconds_hdnation") or pc["timeout_seconds"])
        for extra in (
            "home depot nationwide stock check",
            "lowest store price",
            "lowest online price",
            "powered by tempomonitors",
        ):
            if extra not in seen:
                seen[extra] = None
                success.append(extra)
        fails = pc.get("failure_substrings") or []
        if not fails:
            fails = [
                "could not fetch stock",
                "check you have entered the correct sku",
            ]
        fail_norm = [str(x).strip().lower() for x in fails if str(x).strip()]
        return success, fail_norm, max(1.0, timeout)
    return success, [], max(1.0, timeout)


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
                break
            succ_needles, fail_needles, mon_timeout = _monitor_needles_for_command(
                route_cmd, post_confirm
            )
            wait_label = "hdnation stock-check" if route_cmd == "hdnation" else "lead posted"
            print(
                f"  waiting for {wait_label} (author {post_confirm['author_user_id']!r}, "
                f"timeout {mon_timeout:.0f}s, session until next !m)…"
            )
            outcome, mon_detail = mm.wait_for_m_command_monitor_session(
                post_ch,
                posted_mid,
                token,
                author_user_id=post_confirm["author_user_id"],
                success_needles=succ_needles,
                failure_needles=fail_needles,
                maintenance_start_needles=post_confirm.get("maintenance_start_substrings") or [],
                maintenance_done_needles=post_confirm.get("maintenance_done_substrings") or [],
                maintenance_extend_seconds=float(post_confirm.get("maintenance_extend_seconds") or 0.0),
                timeout_seconds=mon_timeout,
                poll_interval_seconds=post_confirm["poll_interval_seconds"],
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
        "  0. Run ALL stores  (checkpoint resume per channel; order: forward_all_stores_order "
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
) -> int:
    """Run every route in order; each store resumes from checkpoint only."""
    if args.no_checkpoint:
        print("--all-stores requires checkpoints (omit --no-checkpoint).", file=sys.stderr)
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
    print(
        f"\n--all-stores: checkpoint resume only.\nOrder: {labels}\n"
        f"Global --max: {max_disp} (every message walked, including skips/dedupes).\n"
        f"POST !m to channel id: {post_ch}\n"
    )

    root0 = _load_checkpoint_root()
    with_cp: list[str] = []
    without_cp: list[str] = []
    for cid, lab, _r in ordered:
        if _get_channel_checkpoint(root0, str(cid)):
            with_cp.append(lab)
        else:
            without_cp.append(lab)
    if with_cp:
        print(f"Checkpoints found: {', '.join(with_cp)}")
    if without_cp:
        print(
            f"No checkpoint in mirror_forward_checkpoint.json for: {', '.join(without_cp)} "
            f"(those channels are skipped).\n"
            "Checkpoint = last fully completed Mirror forward from this script (not every !m in Discord). "
            "Older builds kept one global cursor in this file, so the last store you finished overwrote "
            "the others; today’s file merges by_channel and keeps every store you complete from here on.\n"
        )

    budget: int | None = None if max_n <= 0 else max_n
    agg = ForwardBatchResult()
    token = ""

    for sel in ordered:
        mw_channel_id, store_label, route_for_build = sel[0], sel[1], sel[2]
        if budget is not None and budget <= 0:
            print("\nGlobal --max exhausted; stopping.")
            break
        root = _load_checkpoint_root()
        entry = _get_channel_checkpoint(root, str(mw_channel_id))
        if not entry:
            print(f"\n[{store_label}] no checkpoint for channel {mw_channel_id}; skipping.")
            continue
        last_id = str(entry.get("last_completed_message_id") or "").strip()
        if not last_id.isdigit():
            print(f"\n[{store_label}] invalid checkpoint; skipping.", file=sys.stderr)
            continue
        guild_id = str(entry.get("guild_id") or "").strip()
        lj = str(entry.get("last_jump_url") or "").strip()
        if not guild_id and lj:
            try:
                guild_id, _, _ = mm.parse_jump_url(lj)
            except ValueError:
                guild_id = ""
        if not guild_id:
            print(f"\n[{store_label}] checkpoint missing guild_id; skipping.", file=sys.stderr)
            continue
        link_ch = str(mw_channel_id)
        resume_after = last_id
        try:
            _anchor, channel, _lab, token = mm.fetch_message_with_token_fallback(
                guild_id, link_ch, resume_after
            )
        except RuntimeError as e:
            print(f"\n[{store_label}] cannot read channel: {e}", file=sys.stderr)
            continue
        try:
            nxt = mm.list_messages_after(link_ch, resume_after, token, limit=50)
        except RuntimeError as e:
            print(f"\n[{store_label}] list after checkpoint: {e}", file=sys.stderr)
            continue
        if not nxt:
            print(f"\n[{store_label}] no newer messages after checkpoint; skipping.")
            continue
        start_msg = nxt[0]
        start_mid = str(start_msg.get("id") or "")
        if not start_mid.isdigit():
            print(f"\n[{store_label}] invalid resume message; skipping.", file=sys.stderr)
            continue
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
        "--all-stores",
        action="store_true",
        help="Run every route in forward_all_stores_order (m_lead_routes.json): each store resumes "
        "from its checkpoint only, then continues to the next store (per-store errors do not stop "
        "later stores). --max counts messages across all stores. Requires --yes unless --dry-run. "
        "Incompatible with --store, --url, --dest, and --no-checkpoint.",
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
        return _run_all_stores(args, routes, mlead_full, post_confirm, rows)

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
            if not args.dry_run:
                if not _prompt_yes(
                    "Run ALL stores in order (each channel resumes from its checkpoint only). "
                    "This can take a long time. Proceed?",
                    default_yes=False,
                ):
                    print("Cancelled.")
                    return 0
            args.yes = True
            args.all_stores = True
            return _run_all_stores(args, routes, mlead_full, post_confirm, rows)

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
        resume_next = False
        cp_root = _load_checkpoint_root() if use_checkpoint else None
        r_cp: dict | None = (
            _get_channel_checkpoint(cp_root, str(mw_channel_id)) if cp_root else None
        )
        last_id = str((r_cp or {}).get("last_completed_message_id") or "").strip()
        if r_cp and last_id.isdigit():
            lj = str(r_cp.get("last_jump_url") or "").strip()
            if args.yes:
                resume_next = True
                guild_id = str(r_cp.get("guild_id") or "").strip()
                if not guild_id and lj:
                    try:
                        guild_id, _, _ = mm.parse_jump_url(lj)
                    except ValueError:
                        guild_id = ""
                if not guild_id:
                    print(
                        "Checkpoint has no guild_id and no parseable last_jump_url; "
                        "paste a start link instead.\n",
                        file=sys.stderr,
                    )
                    resume_next = False
            else:
                upd = str(r_cp.get("updated_at_iso") or "").strip()
                print("Saved checkpoint for this store (last fully completed forward):")
                print(f"  message id: {last_id}")
                if lj:
                    print(f"  link: {lj}")
                if upd:
                    print(f"  saved at: {upd}")
                if _prompt_yes("Start from the next message after that one?", default_yes=True):
                    resume_next = True
                    guild_id = str(r_cp.get("guild_id") or "").strip()
                    if not guild_id and lj:
                        try:
                            guild_id, _, _ = mm.parse_jump_url(lj)
                        except ValueError:
                            guild_id = ""
                    if not guild_id:
                        print(
                            "Checkpoint has no guild_id and no parseable last_jump_url; "
                            "paste a start link instead.\n",
                            file=sys.stderr,
                        )
                        resume_next = False
        if resume_next:
            link_ch = str(mw_channel_id)
            resume_after = last_id
            try:
                _anchor, channel, _lab, token = mm.fetch_message_with_token_fallback(
                    guild_id, link_ch, resume_after
                )
            except RuntimeError as e:
                print(str(e), file=sys.stderr)
                return 1
            try:
                nxt = mm.list_messages_after(link_ch, resume_after, token, limit=50)
            except RuntimeError as e:
                print(str(e), file=sys.stderr)
                return 1
            if not nxt:
                print("No newer messages after the checkpoint; nothing to do.")
                return 0
            start_msg = nxt[0]
            start_mid = str(start_msg.get("id") or "")
            if not start_mid.isdigit():
                print("Invalid message in channel history after checkpoint.", file=sys.stderr)
                return 1
            url = f"https://discord.com/channels/{guild_id}/{link_ch}/{start_mid}"
            print(f"Resuming after id {resume_after}; first message this run: {start_mid}\n")
        else:
            print("Paste the Discord jump link for the FIRST deal message to process.\n")
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
    return 0 if batch.fail_n == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
