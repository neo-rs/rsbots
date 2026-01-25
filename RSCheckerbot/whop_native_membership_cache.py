from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from rschecker_utils import load_json as _load_json
from rschecker_utils import save_json as _save_json

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "whop_native_membership_cache.json"


def _norm_mid(membership_id: str) -> str:
    mid = str(membership_id or "").strip()
    return mid if mid.startswith(("mem_", "R-")) else ""


def record_summary(
    membership_id: str,
    summary: dict,
    *,
    source_message_id: int | None = None,
) -> None:
    """Persist a staff-safe native Whop summary keyed by membership_id."""
    mid = _norm_mid(membership_id)
    if not mid:
        return
    if not isinstance(summary, dict) or not summary:
        return

    try:
        db = _load_json(CACHE_FILE)
        if not isinstance(db, dict):
            db = {}
        now = int(time.time())
        db[mid] = {
            "summary": summary,
            "updated_at": now,
            "source_message_id": int(source_message_id) if source_message_id else None,
        }
        _save_json(CACHE_FILE, db)
    except Exception:
        return


def get_summary(membership_id: str) -> dict:
    """Return cached summary for membership_id, or {} if missing."""
    mid = _norm_mid(membership_id)
    if not mid:
        return {}
    try:
        db: dict[str, Any] = _load_json(CACHE_FILE)
        rec = db.get(mid) if isinstance(db, dict) else None
        if isinstance(rec, dict) and isinstance(rec.get("summary"), dict):
            return rec.get("summary") or {}
    except Exception:
        return {}
    return {}

