from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


# Runtime JSON store (server-owned; NEVER synced to GitHub)
STORE_FILENAME = "reporting_store.json"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_week_key_from_ts(ts: int) -> str:
    dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    y, w, _ = dt.isocalendar()
    return f"{y:04d}-W{w:02d}"


def _week_start_dt(week_key: str) -> Optional[datetime]:
    """Return Monday 00:00 UTC for a YYYY-Www key (best-effort)."""
    try:
        s = str(week_key or "").strip()
        if not s or "-W" not in s:
            return None
        y_s, w_s = s.split("-W", 1)
        y = int(y_s)
        w = int(w_s)
        dt = datetime.fromisocalendar(y, w, 1).replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _parse_dt_any(ts: object) -> Optional[datetime]:
    if ts is None or ts == "":
        return None
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        s = str(ts).strip()
        if not s:
            return None
        # ISO format
        if "T" in s or "-" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except Exception:
        return None


def _safe_int(v: object) -> Optional[int]:
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _ensure_store_shape(store: dict, *, retention_weeks: int) -> dict:
    if not isinstance(store, dict):
        store = {}
    meta = store.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("version", 1)
    meta["retention_weeks"] = int(retention_weeks)
    store["meta"] = meta

    if not isinstance(store.get("weeks"), dict):
        store["weeks"] = {}
    if not isinstance(store.get("members"), dict):
        store["members"] = {}
    if not isinstance(store.get("unlinked"), dict):
        store["unlinked"] = {}
    return store


def load_store(base_dir: Path, *, retention_weeks: int) -> dict:
    p = Path(base_dir) / STORE_FILENAME
    try:
        if not p.exists() or p.stat().st_size == 0:
            return _ensure_store_shape({}, retention_weeks=retention_weeks)
        raw = json.loads(p.read_text(encoding="utf-8") or "{}")
        return _ensure_store_shape(raw if isinstance(raw, dict) else {}, retention_weeks=retention_weeks)
    except Exception:
        return _ensure_store_shape({}, retention_weeks=retention_weeks)


def save_store(base_dir: Path, store: dict) -> None:
    p = Path(base_dir) / STORE_FILENAME
    p.parent.mkdir(parents=True, exist_ok=True)

    # Atomic write (same-folder temp -> replace)
    tmp = p.with_suffix(p.suffix + ".tmp")
    data = json.dumps(store, indent=2, ensure_ascii=False)
    tmp.write_text(data, encoding="utf-8")
    try:
        os.replace(tmp, p)
    except Exception:
        # Best-effort cleanup
        try:
            tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass


def prune_store(store: dict, *, retention_weeks: int, now: Optional[datetime] = None) -> dict:
    now_dt = now or _now_utc()
    cutoff = now_dt - timedelta(weeks=int(retention_weeks))
    cutoff_ts = int(cutoff.timestamp())

    store = _ensure_store_shape(store, retention_weeks=retention_weeks)

    # Weeks
    weeks = store.get("weeks") if isinstance(store.get("weeks"), dict) else {}
    pruned_weeks: dict[str, dict] = {}
    for wk, rec in weeks.items():
        ws = _week_start_dt(wk)
        if not ws:
            continue
        if ws >= cutoff:
            pruned_weeks[str(wk)] = rec if isinstance(rec, dict) else {}
    store["weeks"] = pruned_weeks

    # Members
    members = store.get("members") if isinstance(store.get("members"), dict) else {}
    pruned_members: dict[str, dict] = {}
    for did, rec in members.items():
        if not isinstance(rec, dict):
            continue
        last_seen = _safe_int(rec.get("last_seen_ts")) or 0
        if last_seen >= cutoff_ts:
            pruned_members[str(did)] = rec
    store["members"] = pruned_members

    # Unlinked (raw email keys)
    unlinked = store.get("unlinked") if isinstance(store.get("unlinked"), dict) else {}
    pruned_unlinked: dict[str, dict] = {}
    for email, rec in unlinked.items():
        if not isinstance(rec, dict):
            continue
        last_seen = _safe_int(rec.get("last_seen_ts")) or 0
        if last_seen >= cutoff_ts:
            pruned_unlinked[str(email)] = rec
    store["unlinked"] = pruned_unlinked

    return store


def _ensure_week_bucket(store: dict, week_key: str) -> dict:
    weeks = store.get("weeks")
    if not isinstance(weeks, dict):
        weeks = {}
        store["weeks"] = weeks
    rec = weeks.get(week_key)
    if not isinstance(rec, dict):
        rec = {}
        weeks[week_key] = rec
    counts = rec.get("counts")
    if not isinstance(counts, dict):
        counts = {}
        rec["counts"] = counts
    # Keep a place for totals (e.g., total_earned_usd) if/when computed.
    totals = rec.get("totals")
    if not isinstance(totals, dict):
        totals = {}
        rec["totals"] = totals
    return rec


def _bump(store: dict, *, week_key: str, metric: str, amount: int = 1) -> None:
    wk = _ensure_week_bucket(store, week_key)
    counts = wk.get("counts") if isinstance(wk.get("counts"), dict) else {}
    try:
        prev = int(counts.get(metric, 0) or 0)
    except Exception:
        prev = 0
    counts[str(metric)] = prev + int(amount)
    wk["counts"] = counts


def _member_rec(store: dict, discord_id: int) -> dict:
    members = store.get("members")
    if not isinstance(members, dict):
        members = {}
        store["members"] = members
    key = str(int(discord_id))
    rec = members.get(key)
    if not isinstance(rec, dict):
        rec = {}
        members[key] = rec
    rec.setdefault("flags", {})  # metric -> last_week_key counted
    return rec


def _unlinked_rec(store: dict, email: str) -> dict:
    unlinked = store.get("unlinked")
    if not isinstance(unlinked, dict):
        unlinked = {}
        store["unlinked"] = unlinked
    key = str(email or "").strip()
    rec = unlinked.get(key)
    if not isinstance(rec, dict):
        rec = {}
        unlinked[key] = rec
    rec.setdefault("flags", {})  # metric -> last_week_key counted
    return rec


def _flag_and_bump(flags: dict, *, metric: str, week_key: str, bump_fn) -> None:
    """Idempotent: bump only once per metric per week."""
    if not isinstance(flags, dict):
        return
    prev = str(flags.get(metric) or "")
    if prev == str(week_key):
        return
    flags[metric] = str(week_key)
    bump_fn()


def record_member_status_post(
    store: dict,
    *,
    ts: int,
    event_kind: str,
    discord_id: int | None = None,
    email: str = "",
    whop_brief: dict | None = None,
) -> dict:
    """Update the bounded store from a member-status-logs reference output.

    IMPORTANT: This is the ONLY persistence point. Call this only when RSCheckerbot
    has actually posted a reference entry into member-status-logs.
    """
    ts_i = int(ts)
    week_key = _iso_week_key_from_ts(ts_i)
    kind = str(event_kind or "").strip().lower() or "unknown"
    b = whop_brief if isinstance(whop_brief, dict) else {}

    # Ensure week bucket exists (even if we don't increment a metric).
    _ensure_week_bucket(store, week_key)

    if discord_id is not None:
        did = int(discord_id)
        rec = _member_rec(store, did)
        rec["last_seen_ts"] = ts_i
        rec["last_event_kind"] = kind

        # Keep minimal Whop state for reminders (derived from the reference embed input).
        if b:
            rec["whop"] = {
                "status": str(b.get("status") or "").strip(),
                "product": str(b.get("product") or "").strip(),
                "total_spent": str(b.get("total_spent") or "").strip(),
                "cancel_at_period_end": str(b.get("cancel_at_period_end") or "").strip(),
                "renewal_end_iso": str(b.get("renewal_end_iso") or "").strip(),
                "dashboard_url": str(b.get("dashboard_url") or "").strip(),
            }

        flags = rec.get("flags") if isinstance(rec.get("flags"), dict) else {}
        rec["flags"] = flags

        def bump(metric: str) -> None:
            _flag_and_bump(flags, metric=metric, week_key=week_key, bump_fn=lambda: _bump(store, week_key=week_key, metric=metric))

        # Metrics (weekly counts). These are derived ONLY from what we emitted.
        if kind in {"onboarding_completed", "member_granted", "member_role_added"}:
            # Count onboarding completion once (ever) and also once per week for reporting.
            if not rec.get("onboarding_completed_at"):
                rec["onboarding_completed_at"] = ts_i
            bump("new_members")

        if kind in {"trial", "trialing", "membership_trial", "membership_activated_pending"}:
            bump("new_trials")

        if kind in {"payment_failed"}:
            bump("payment_failed")

        if kind in {"cancellation_scheduled"}:
            bump("cancellation_scheduled")
            # For reminders: remember access end (renewal_end_iso) when present.
            iso = str(b.get("renewal_end_iso") or "").strip()
            dt = _parse_dt_any(iso) if iso else None
            if dt:
                rec["cancel_scheduled_end_ts"] = int(dt.timestamp())
                if not rec.get("cancel_scheduled_first_ts"):
                    rec["cancel_scheduled_first_ts"] = ts_i

        if kind in {"deactivated", "canceled", "cancelled"}:
            bump("cancelled_members")
            # Churn: previously scheduled cancel, now deactivated/payment_failed.
            if rec.get("cancel_scheduled_end_ts") and not rec.get("churned_at"):
                rec["churned_at"] = ts_i
                bump("churn")

        store.get("members", {})[str(did)] = rec
        return store

    # Unlinked: store under raw email key (as requested).
    email_s = str(email or "").strip()
    if email_s:
        recu = _unlinked_rec(store, email_s)
        recu["last_seen_ts"] = ts_i
        recu["last_event_kind"] = kind
        flagsu = recu.get("flags") if isinstance(recu.get("flags"), dict) else {}
        recu["flags"] = flagsu

        def bumpu(metric: str) -> None:
            _flag_and_bump(flagsu, metric=metric, week_key=week_key, bump_fn=lambda: _bump(store, week_key=week_key, metric=metric))

        if kind in {"payment_failed"}:
            bumpu("unlinked_payment_failed")
        if kind in {"deactivated", "canceled", "cancelled"}:
            bumpu("unlinked_cancelled_members")
        if kind in {"cancellation_scheduled"}:
            bumpu("unlinked_cancellation_scheduled")
        if kind in {"trial", "trialing", "membership_activated_pending"}:
            bumpu("unlinked_new_trials")

        store.get("unlinked", {})[email_s] = recu

    return store


def week_keys_between(start_ts: int, end_ts: int) -> list[str]:
    """Return ISO week keys touched by [start_ts, end_ts]."""
    a = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
    b = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)
    if b < a:
        a, b = b, a
    # Walk Mondays.
    cur = a - timedelta(days=a.weekday())
    keys: list[str] = []
    while cur <= b:
        y, w, _ = cur.isocalendar()
        keys.append(f"{y:04d}-W{w:02d}")
        cur += timedelta(days=7)
    return sorted(set(keys))


def summarize_counts(store: dict, week_keys: Iterable[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    weeks = store.get("weeks") if isinstance(store.get("weeks"), dict) else {}
    for wk in week_keys:
        rec = weeks.get(str(wk))
        if not isinstance(rec, dict):
            continue
        counts = rec.get("counts") if isinstance(rec.get("counts"), dict) else {}
        for k, v in counts.items():
            try:
                out[str(k)] = int(out.get(str(k), 0) or 0) + int(v or 0)
            except Exception:
                continue
    return out

