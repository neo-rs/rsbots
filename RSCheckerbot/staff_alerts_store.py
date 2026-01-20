from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path

def _parse_iso(dt_str: str) -> datetime | None:
    try:
        s = (dt_str or "").strip()
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def load_staff_alerts(path: Path) -> dict:
    try:
        p = Path(path)
        if not p.exists() or p.stat().st_size == 0:
            return {}
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_staff_alerts(path: Path, db: dict) -> None:
    try:
        p = Path(path)
        if not isinstance(db, dict):
            return
        # Atomic write (same-folder temp -> replace) to avoid truncated JSON on crash.
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(db, indent=2, ensure_ascii=False), encoding="utf-8")
        try:
            os.replace(tmp, p)
        except Exception:
            with suppress(Exception):
                tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
    except Exception:
        pass


def should_post_alert(db: dict, discord_id: int, issue_key: str, cooldown_hours: float = 6.0) -> bool:
    """Generic staff-alert dedupe (JSON-only persistence)."""
    try:
        uid = str(int(discord_id))
    except Exception:
        return True
    rec = db.get(uid) if isinstance(db, dict) else None
    if not isinstance(rec, dict):
        return True
    last = rec.get("last") if isinstance(rec.get("last"), dict) else {}
    last_iso = str(last.get(issue_key) or "")
    last_dt = _parse_iso(last_iso)
    if not last_dt:
        return True
    return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=cooldown_hours)


def record_alert_post(db: dict, discord_id: int, issue_key: str) -> None:
    try:
        uid = str(int(discord_id))
    except Exception:
        return
    rec = db.get(uid) if isinstance(db, dict) else None
    if not isinstance(rec, dict):
        rec = {}
        db[uid] = rec
    last = rec.get("last")
    if not isinstance(last, dict):
        last = {}
    last[issue_key] = datetime.now(timezone.utc).isoformat()
    rec["last"] = last


# Shared lock for staff_alerts.json (prevents load→await→save lost updates)
STAFF_ALERTS_LOCK: asyncio.Lock = asyncio.Lock()


async def should_post_and_record_alert(
    path: Path,
    *,
    discord_id: int,
    issue_key: str,
    cooldown_hours: float = 6.0,
) -> bool:
    """Atomically (within-process) check cooldown and record the alert post.

    This prevents the common race where multiple coroutines:
      1) load staff_alerts.json
      2) await network/Discord
      3) save, overwriting each other's updates
    """
    async with STAFF_ALERTS_LOCK:
        db = load_staff_alerts(path)
        if not should_post_alert(db, discord_id, issue_key, cooldown_hours=cooldown_hours):
            return False
        record_alert_post(db, discord_id, issue_key)
        save_staff_alerts(path, db)
        return True

