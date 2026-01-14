from __future__ import annotations

import json
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
        p.write_text(json.dumps(db, indent=2, ensure_ascii=False), encoding="utf-8")
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
    return (_now() - last_dt) >= timedelta(hours=cooldown_hours)


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
    last[issue_key] = _now().isoformat()
    rec["last"] = last

