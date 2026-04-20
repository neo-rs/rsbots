"""Analyze member-status ledger vs tickets_index (local-only).

Inputs:
- RSCheckerbot/data/member_status_logs_events.json
- RSCheckerbot/data/tickets_index.json

Outputs:
- Console report (UTF-8 safe)
- Optional text report file
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _safe(s: object) -> str:
    return (str(s) if s is not None else "").encode("utf-8", "backslashreplace").decode("utf-8", "ignore")


def _fmt_iso_human(iso: object) -> str:
    """Format ISO timestamp into a short UTC human string."""
    s = str(iso or "").strip()
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        # Example: 2026-04-20 11:51 UTC
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return s[:32]


def _load_json(path: Path) -> dict:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return {}
        return json.loads(path.read_text(encoding="utf-8") or "{}")
    except Exception:
        return {}


def _as_int(v: object) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0


def _ticket_is_open(t: dict) -> bool:
    return str(t.get("status") or "").strip().upper() == "OPEN"


def _churn_marked(t: dict) -> bool:
    # Best-effort: tickets_index does not always include category_id, so rely on persisted markers.
    if str(t.get("cancellation_moved_to_churn_at_iso") or "").strip():
        return True
    nm = str(t.get("channel_name") or "").strip().lower()
    if nm.startswith("churn-"):
        return True
    return False


def _latest_card_of_kind(by_did: dict[str, Any], uid: int, kind: str) -> dict | None:
    rec = by_did.get(str(uid), {})
    cards = rec.get("cards") if isinstance(rec, dict) else None
    if not isinstance(cards, dict):
        return None
    k0 = str(kind or "").strip().lower()
    best: dict | None = None
    best_iso = ""
    for c in cards.values():
        if not isinstance(c, dict):
            continue
        if str(c.get("kind") or "").strip().lower() != k0:
            continue
        iso = str(c.get("created_at_iso") or "")
        if (best is None) or (iso > best_iso):
            best = c
            best_iso = iso
    return best


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze member-status ledger vs tickets_index.json")
    p.add_argument("--msl", type=str, default="", help="Path to member_status_logs_events.json")
    p.add_argument("--tickets", type=str, default="", help="Path to tickets_index.json")
    p.add_argument("--out", type=str, default="", help="Optional output report path (txt)")
    p.add_argument("--limit", type=int, default=25, help="Max rows to print per section (default 25, max 200)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    limit = int(max(5, min(int(args.limit or 25), 200)))

    base = Path(__file__).resolve().parents[1] / "data"
    msl_path = Path(str(args.msl)).resolve() if str(args.msl).strip() else (base / "member_status_logs_events.json")
    tix_path = Path(str(args.tickets)).resolve() if str(args.tickets).strip() else (base / "tickets_index.json")

    msl = _load_json(msl_path)
    tix = _load_json(tix_path)

    by_did: dict[str, Any] = msl.get("by_discord_id") if isinstance(msl.get("by_discord_id"), dict) else {}
    tickets: dict[str, Any] = tix.get("tickets") if isinstance(tix.get("tickets"), dict) else {}

    per_user: dict[int, list[dict]] = {}
    for rec in tickets.values():
        if not isinstance(rec, dict):
            continue
        uid = _as_int(rec.get("user_id") or 0)
        if uid <= 0:
            continue
        per_user.setdefault(uid, []).append(rec)

    def open_types(uid: int) -> set[str]:
        out: set[str] = set()
        for t in per_user.get(uid, []):
            if _ticket_is_open(t):
                out.add(str(t.get("ticket_type") or "").strip())
        return out

    def ever_has_type(uid: int, ticket_type: str) -> bool:
        for t in per_user.get(uid, []):
            if str(t.get("ticket_type") or "").strip() == ticket_type:
                return True
        return False

    # Scan ledger signals
    kinds_count: dict[str, int] = {}
    users_with_kind: dict[str, set[int]] = {"cancellation_scheduled": set(), "deactivated": set(), "payment_failed": set()}
    for did_s, rec in by_did.items():
        did = _as_int(did_s)
        if did <= 0:
            continue
        cards = rec.get("cards") if isinstance(rec, dict) else None
        if not isinstance(cards, dict):
            continue
        for c in cards.values():
            if not isinstance(c, dict):
                continue
            k = str(c.get("kind") or "unknown").strip().lower() or "unknown"
            kinds_count[k] = kinds_count.get(k, 0) + 1
            if k in users_with_kind:
                users_with_kind[k].add(did)

    # Missing cancellation tickets for cancellation_scheduled
    cancel_users = users_with_kind["cancellation_scheduled"]
    missing_any_cancel = sorted([u for u in cancel_users if not ever_has_type(u, "cancellation")])
    missing_open_cancel = sorted([u for u in cancel_users if "cancellation" not in open_types(u)])

    # Churn alignment (local-only): deactivated users with OPEN cancellation ticket not churn-marked
    deact_users = users_with_kind["deactivated"]
    churn_should_move = []
    for uid in sorted(deact_users):
        for t in per_user.get(uid, []):
            if str(t.get("ticket_type") or "").strip() != "cancellation":
                continue
            if not _ticket_is_open(t):
                continue
            if not _churn_marked(t):
                churn_should_move.append((uid, t))

    # Countdown churn expectation (remaining_days == 0) but not churn marked
    countdown_should_move = []
    for uid, tlist in per_user.items():
        for t in tlist:
            if str(t.get("ticket_type") or "").strip() != "cancellation":
                continue
            if not _ticket_is_open(t):
                continue
            try:
                rem = int(t.get("cancellation_last_remaining_days") or -1)
            except Exception:
                rem = -1
            if rem == 0 and not _churn_marked(t):
                countdown_should_move.append((uid, t))

    # Report builder
    lines: list[str] = []
    now = datetime.now(timezone.utc).isoformat()
    meta = msl.get("meta") if isinstance(msl.get("meta"), dict) else {}
    lines.append("==============================================================================")
    lines.append("MSL ledger vs tickets_index (local analysis)")
    lines.append("==============================================================================")
    lines.append(f"generated_at_utc={_fmt_iso_human(now)} ({now})")
    lines.append(f"msl_path={_safe(msl_path)}")
    lines.append(f"tickets_path={_safe(tix_path)}")
    lines.append("")
    lines.append("1) LEDGER SUMMARY")
    lines.append(f"- unique_members={_safe(meta.get('unique_members'))}")
    lines.append(f"- total_cards={_safe(meta.get('total_cards'))}")
    lines.append("- top_kinds:")
    for k, v in sorted(kinds_count.items(), key=lambda kv: kv[1], reverse=True)[:12]:
        lines.append(f"  - {k}: {v}")
    lines.append("")
    lines.append("2) TICKET COVERAGE (signals from ledger)")
    lines.append(f"- cancellation_scheduled users={len(cancel_users)}")
    lines.append(f"  - missing ANY cancellation ticket={len(missing_any_cancel)}")
    lines.append(f"  - missing OPEN cancellation ticket={len(missing_open_cancel)}")
    lines.append(f"- deactivated users={len(deact_users)}")
    lines.append(f"- payment_failed users={len(users_with_kind['payment_failed'])}")
    lines.append("")

    def _ledger_header(uid: int) -> tuple[str, str, str]:
        rec = by_did.get(str(uid), {})
        header = rec.get("header") if isinstance(rec, dict) and isinstance(rec.get("header"), dict) else {}
        mid = str(header.get("membership_id") or "").strip()
        title = str(header.get("last_title") or "").strip()
        seen = str(header.get("last_seen_at") or "").strip()
        return (mid, title, seen)

    lines.append("3) MISSING cancellation tickets for cancellation_scheduled (ANY)")
    if not missing_any_cancel:
        lines.append("- none")
    else:
        for uid in missing_any_cancel[:limit]:
            c = _latest_card_of_kind(by_did, uid, "cancellation_scheduled") or {}
            brief = c.get("whop_brief") if isinstance(c.get("whop_brief"), dict) else {}
            mid = str(brief.get("membership_id") or "").strip()
            spent = str(brief.get("total_spent") or "").strip()
            prod = str(brief.get("product") or "").strip()
            st = str(brief.get("status") or "").strip()
            lines.append(
                f"- uid={uid} mid={_safe(mid)} title={_safe(c.get('title'))} "
                f"created_at={_fmt_iso_human(c.get('created_at_iso'))} spent={_safe(spent)} status={_safe(st)} product={_safe(prod)} "
                f"jump={_safe(c.get('jump_url'))}"
            )
        if len(missing_any_cancel) > limit:
            lines.append(f"- ... +{len(missing_any_cancel) - limit} more")
    lines.append("")

    lines.append("4) OPEN deactivated tickets missing churn markers (should move to churn category on replay)")
    if not churn_should_move:
        lines.append("- none")
    else:
        for uid, t in churn_should_move[:limit]:
            c = _latest_card_of_kind(by_did, uid, "deactivated") or {}
            brief = c.get("whop_brief") if isinstance(c.get("whop_brief"), dict) else {}
            mid = str(brief.get("membership_id") or "").strip()
            title = str(c.get("title") or "").strip()
            when = str(c.get("created_at_iso") or "").strip()
            jump = str(c.get("jump_url") or "").strip()
            lines.append(
                f"- uid={uid} mid={_safe(mid)} deactivated_at={_fmt_iso_human(when)} title={_safe(title)} jump={_safe(jump)} "
                f"ticket_id={_safe(t.get('ticket_id'))} ch={_safe(t.get('channel_id'))} name={_safe(t.get('channel_name'))}"
            )
        if len(churn_should_move) > limit:
            lines.append(f"- ... +{len(churn_should_move) - limit} more")
    lines.append("")

    lines.append("5) OPEN cancellation tickets with remaining_days==0 missing churn markers (countdown churn)")
    if not countdown_should_move:
        lines.append("- none")
    else:
        for uid, t in countdown_should_move[:limit]:
            lines.append(
                f"- uid={uid} ticket_id={_safe(t.get('ticket_id'))} ch={_safe(t.get('channel_id'))} "
                f"name={_safe(t.get('channel_name'))} rem_days={_safe(t.get('cancellation_last_remaining_days'))}"
            )
        if len(countdown_should_move) > limit:
            lines.append(f"- ... +{len(countdown_should_move) - limit} more")
    lines.append("")
    lines.append("6) NOTES")
    lines.append("- This report is local-only (JSON vs JSON). It does not query Discord for live categories.")
    lines.append("- Churn markers are inferred from tickets_index fields:")
    lines.append("  - cancellation_moved_to_churn_at_iso OR channel_name starts with 'churn-'")
    lines.append("- If a ticket is CLOSED, it is intentionally not considered for churn moves here.")

    report = "\n".join(lines)
    print(report)

    out = str(args.out or "").strip()
    if out:
        out_p = Path(out).expanduser().resolve()
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(report, encoding="utf-8")
        print("")
        print(f"wrote_report={_safe(out_p)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

