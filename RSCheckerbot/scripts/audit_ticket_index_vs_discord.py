"""Discord-backed audit: ticket categories vs tickets_index.json (local script).

This script logs in with the bot token and scans configured ticket categories:
- billing category
- cancellation category
- churn category (for moved cancellation tickets)

It compares Discord channels (topic markers) to `RSCheckerbot/data/tickets_index.json`.

Modes:
- scan: print findings only
- apply confirm: write safe repairs to tickets_index.json:
  - add missing index rows for channels that have a valid support-ticket topic (topic is the source of truth)
  - update channel_name + category_id + category_name fields for existing rows

Safety:
- Does NOT delete channels.
- Does NOT change CLOSED -> OPEN by default.
- Does NOT close tickets.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import discord

# Ensure repo root + RSCheckerbot are importable when running from `RSCheckerbot/scripts/`.
_RS_CHECKERBOT_DIR = Path(__file__).resolve().parents[1]
_REPO_ROOT = Path(__file__).resolve().parents[2]
for p in (str(_REPO_ROOT), str(_RS_CHECKERBOT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from mirror_world_config import load_config_with_secrets

import support_tickets


def _safe(s: object) -> str:
    return (str(s) if s is not None else "").encode("utf-8", "backslashreplace").decode("utf-8", "ignore")


def _as_int(v: object) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0


def _iso(dt: datetime | None) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _fmt_iso_human(iso: object) -> str:
    s = str(iso or "").strip()
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return s[:32]


_RE_KV = re.compile(r"(?im)^\\s*([A-Za-z_]+)\\s*=\\s*(.+?)\\s*$")


def _kv_from_topic(topic: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in _RE_KV.finditer(str(topic or "")):
        k = str(m.group(1) or "").strip().lower()
        v = str(m.group(2) or "").strip()
        if k and v and k not in out:
            out[k] = v
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit tickets_index.json vs Discord ticket categories")
    p.add_argument("mode", nargs="?", default="scan", help="scan | apply")
    p.add_argument("confirm", nargs="?", default="", help="confirm (required for apply)")
    p.add_argument("--guild-id", type=int, default=0, help="Override guild id (defaults to support_tickets.guild_id)")
    p.add_argument("--billing-category-id", type=int, default=0, help="Override billing category id")
    p.add_argument("--cancellation-category-id", type=int, default=0, help="Override cancellation category id")
    p.add_argument("--churn-category-id", type=int, default=0, help="Override churn category id")
    p.add_argument("--limit", type=int, default=0, help="Cap channels scanned per category (0=no cap)")
    p.add_argument("--write-report", type=str, default="", help="Optional report path (txt)")
    return p.parse_args()


async def _run() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    cfg, _, secrets_path = load_config_with_secrets(base_dir)

    token = str(cfg.get("bot_token") or "").strip()
    if not token:
        raise RuntimeError(f"Missing bot_token in {secrets_path}")

    args = _parse_args()
    mode = str(args.mode or "scan").strip().lower()
    applying = mode == "apply"
    if applying and str(args.confirm or "").strip().lower() != "confirm":
        raise RuntimeError("Refusing apply without explicit 'confirm'. Usage: ... apply confirm")

    st = cfg.get("support_tickets") if isinstance(cfg, dict) else {}
    st = st if isinstance(st, dict) else {}
    cats = st.get("ticket_categories") if isinstance(st.get("ticket_categories"), dict) else {}
    cc = st.get("cancellation_countdown") if isinstance(st.get("cancellation_countdown"), dict) else {}

    guild_id = int(args.guild_id or st.get("guild_id") or cfg.get("guild_id") or 0)
    billing_cat_id = int(args.billing_category_id or cats.get("billing_category_id") or 0)
    cancel_cat_id = int(args.cancellation_category_id or cats.get("cancellation_category_id") or 0)
    churn_cat_id = int(args.churn_category_id or cc.get("churn_category_id") or 0)
    per_cat_limit = int(args.limit or 0)
    per_cat_limit = max(0, min(per_cat_limit, 5000))

    if guild_id <= 0:
        raise RuntimeError("Missing guild_id (support_tickets.guild_id or config.guild_id).")
    if billing_cat_id <= 0 and cancel_cat_id <= 0 and churn_cat_id <= 0:
        raise RuntimeError("No category ids configured (billing/cancellation/churn).")

    intents = discord.Intents.none()
    intents.guilds = True
    client = discord.Client(intents=intents)

    report_lines: list[str] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    # Stats
    stats: dict[str, int] = {
        "scanned_channels": 0,
        "ticket_channels": 0,
        "missing_in_index": 0,
        "updated_in_index": 0,
        "skipped_apply": 0,
        "errors": 0,
    }

    @client.event
    async def on_ready():
        nonlocal report_lines
        print("=== ticket index audit (Discord-backed) ===")
        print(f"mode={mode}")
        print(f"guild_id={guild_id}")
        print(f"billing_category_id={billing_cat_id}")
        print(f"cancellation_category_id={cancel_cat_id}")
        print(f"churn_category_id={churn_cat_id}")
        if per_cat_limit:
            print(f"per_category_limit={per_cat_limit}")
        print("")

        g = client.get_guild(int(guild_id))
        if g is None:
            with suppress(Exception):
                g = await client.fetch_guild(int(guild_id))
        if not isinstance(g, discord.Guild):
            print("ERROR: guild not found.")
            stats["errors"] += 1
            with suppress(Exception):
                await client.close()
            return

        # Load index via canonical loader/saver
        db = support_tickets._index_load()
        if not isinstance(db, dict):
            db = {}
        tmap = db.get("tickets") if isinstance(db.get("tickets"), dict) else {}
        if not isinstance(tmap, dict):
            tmap = {}
            db["tickets"] = tmap

        # Invert: channel_id -> ticket_id(s)
        chan_to_tid: dict[int, str] = {}
        for tid, rec in tmap.items():
            if not isinstance(rec, dict):
                continue
            ch_id = _as_int(rec.get("channel_id") or 0)
            if ch_id > 0 and ch_id not in chan_to_tid:
                chan_to_tid[ch_id] = str(tid)

        def cat_obj(cid: int) -> discord.CategoryChannel | None:
            ch = g.get_channel(int(cid))
            return ch if isinstance(ch, discord.CategoryChannel) else None

        cats_to_scan: list[tuple[str, int]] = []
        if billing_cat_id > 0:
            cats_to_scan.append(("billing", billing_cat_id))
        if cancel_cat_id > 0:
            cats_to_scan.append(("cancellation", cancel_cat_id))
        if churn_cat_id > 0:
            cats_to_scan.append(("churn", churn_cat_id))

        report_lines.append("==============================================================================")
        report_lines.append("Ticket index audit (Discord-backed)")
        report_lines.append("==============================================================================")
        report_lines.append(f"generated_at_utc={_fmt_iso_human(now_iso)} ({now_iso})")
        report_lines.append(f"guild_id={guild_id}")
        report_lines.append(f"billing_category_id={billing_cat_id}")
        report_lines.append(f"cancellation_category_id={cancel_cat_id}")
        report_lines.append(f"churn_category_id={churn_cat_id}")
        report_lines.append("")

        for label, cid in cats_to_scan:
            cat = cat_obj(cid)
            if not cat:
                report_lines.append(f"[{label}] category not found: id={cid}")
                continue
            report_lines.append(f"[{label}] category: {cat.name} (id={cat.id}) channels={len(cat.channels)}")
            scanned_here = 0
            for ch in cat.channels:
                if per_cat_limit and scanned_here >= per_cat_limit:
                    break
                if not isinstance(ch, discord.TextChannel):
                    continue
                scanned_here += 1
                stats["scanned_channels"] += 1

                top = str(getattr(ch, "topic", "") or "")
                if not support_tickets._topic_is_support_ticket(top):
                    continue
                stats["ticket_channels"] += 1

                ttype = str(support_tickets._ticket_type_from_topic(top) or "").strip()
                uid = int(support_tickets._ticket_owner_id_from_topic(top) or 0)
                kv = _kv_from_topic(top)
                tid_topic = str(kv.get("ticket_id") or "").strip()
                fp = str(kv.get("fingerprint") or "").strip()
                if not tid_topic:
                    # fallback: if the topic doesn't include ticket_id, we can only map by channel_id
                    tid_topic = ""

                ch_id = int(getattr(ch, "id", 0) or 0)
                ch_name = str(getattr(ch, "name", "") or "")
                created_iso = _iso(getattr(ch, "created_at", None))  # type: ignore[arg-type]

                tid_idx = chan_to_tid.get(ch_id, "")
                rec_idx = tmap.get(tid_idx) if tid_idx else None

                if not isinstance(rec_idx, dict):
                    stats["missing_in_index"] += 1
                    report_lines.append(
                        f"  - MISSING index row: ch={ch_id} name={ch_name} topic_type={ttype} user_id={uid} "
                        f"ticket_id_in_topic={tid_topic or '—'}"
                    )
                    if applying:
                        # Create a new row: prefer ticket_id from topic; else derive deterministic id from channel_id
                        new_tid = tid_topic or f"discord_ch_{ch_id}"
                        if new_tid in tmap:
                            # If collision, suffix
                            new_tid = f"{new_tid}_{uid or 0}"
                        tmap[new_tid] = {
                            "ticket_id": new_tid,
                            "ticket_type": ttype or label,  # best-effort
                            "user_id": int(uid or 0),
                            "channel_id": int(ch_id),
                            "channel_name": ch_name,
                            "guild_id": int(guild_id),
                            "header_message_id": 0,
                            "created_at_iso": created_iso or now_iso,
                            "last_activity_at_iso": created_iso or now_iso,
                            "status": "OPEN",
                            "fingerprint": fp or f"{uid}|{ttype or label}|{(created_iso or now_iso)[:10]}",
                            "reference_jump_url": "",
                            "whop_dashboard_url": "",
                            "close_reason": "",
                            "closed_at_iso": "",
                            # Audit fields (new; safe to ignore by runtime)
                            "discord_category_id": int(getattr(cat, "id", 0) or 0),
                            "discord_category_name": str(getattr(cat, "name", "") or ""),
                        }
                        chan_to_tid[ch_id] = new_tid
                        stats["updated_in_index"] += 1
                    continue

                # Existing row: update channel/category info
                changed = False
                if str(rec_idx.get("channel_name") or "") != ch_name:
                    rec_idx["channel_name"] = ch_name
                    changed = True
                if int(rec_idx.get("channel_id") or 0) != int(ch_id):
                    rec_idx["channel_id"] = int(ch_id)
                    changed = True
                # Record category info for accuracy
                if int(rec_idx.get("discord_category_id") or 0) != int(getattr(cat, "id", 0) or 0):
                    rec_idx["discord_category_id"] = int(getattr(cat, "id", 0) or 0)
                    rec_idx["discord_category_name"] = str(getattr(cat, "name", "") or "")
                    changed = True

                if applying and changed:
                    tmap[str(tid_idx)] = rec_idx
                    stats["updated_in_index"] += 1

        # Save if applying
        if applying:
            db["tickets"] = tmap
            support_tickets._index_save(db)

        report_lines.append("")
        report_lines.append("SUMMARY")
        report_lines.append(f"- scanned_channels={stats['scanned_channels']}")
        report_lines.append(f"- ticket_channels_with_topic={stats['ticket_channels']}")
        report_lines.append(f"- missing_in_index={stats['missing_in_index']}")
        report_lines.append(f"- updated_in_index={stats['updated_in_index']}")
        report_lines.append(f"- errors={stats['errors']}")

        txt = "\n".join(report_lines)
        print(txt)
        out = str(args.write_report or "").strip()
        if out:
            p = Path(out).expanduser().resolve()
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(txt, encoding="utf-8")
            print("")
            print(f"wrote_report={_safe(p)}")

        with suppress(Exception):
            await client.close()

    async with client:
        await client.start(token)
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())

