"""
Local test for RSCheckerbot repeat-trial bypass.

What it proves (without needing Discord):
1) We can mark a user as "had trial before" in member_history (ever_trialing=true).
2) We can set the one-time bypass flag (repeat_trial_bypass_once=true).
3) RSCheckerbot/main.py consumes that flag exactly once via _consume_repeat_trial_bypass_once().

Usage (repo root):
  python scripts/test_trial_bypass_local.py --discord-id 1393361893061689504
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RSC_DIR = REPO_ROOT / "RSCheckerbot"
MEMBER_HISTORY = RSC_DIR / "member_history.json"


def _load_db() -> dict:
    if not MEMBER_HISTORY.exists():
        raise SystemExit(f"Missing {MEMBER_HISTORY}")
    return json.loads(MEMBER_HISTORY.read_text(encoding="utf-8"))


def _save_db(db: dict) -> None:
    tmp = MEMBER_HISTORY.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(db, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(MEMBER_HISTORY)


def ensure_repeat_trial_profile(discord_id: str) -> None:
    db = _load_db()
    rec = db.get(discord_id) or {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}

    # Ensure "had trial before" will evaluate True
    wh["ever_trialing"] = True

    # Ensure a $0-ish spend is present (this is what the old history-based guard reads)
    last = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
    if not isinstance(last, dict):
        last = {}
    last.setdefault("total_spent", "$0.00")
    wh["last_summary"] = last

    rec["whop"] = wh
    db[discord_id] = rec
    _save_db(db)


def set_bypass_flag(discord_id: str) -> None:
    db = _load_db()
    rec = db.get(discord_id) or {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    wh["repeat_trial_bypass_once"] = True
    wh["repeat_trial_bypass_set_ts"] = 0
    rec["whop"] = wh
    db[discord_id] = rec
    _save_db(db)


def get_flags(discord_id: str) -> dict:
    db = _load_db()
    rec = db.get(discord_id) or {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    last = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
    return {
        "ever_trialing": wh.get("ever_trialing"),
        "ever_had_trial_days": wh.get("ever_had_trial_days"),
        "total_spent": (last or {}).get("total_spent") if isinstance(last, dict) else None,
        "repeat_trial_bypass_once": wh.get("repeat_trial_bypass_once"),
        "repeat_trial_bypass_set_ts": wh.get("repeat_trial_bypass_set_ts"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--discord-id", required=True, type=str)
    args = ap.parse_args()

    did = str(args.discord_id).strip()
    if not did.isdigit():
        print("ERROR: --discord-id must be numeric", file=sys.stderr)
        return 2

    print("Updating local member_history to ensure repeat-trial profile…")
    ensure_repeat_trial_profile(did)
    print("Flags after profile:", get_flags(did))

    print("\nSetting one-time bypass flag (as !trialbypass would)…")
    set_bypass_flag(did)
    print("Flags after bypass set:", get_flags(did))

    # Import RSCheckerbot/main.py and call the real consume function
    sys.path.insert(0, str(RSC_DIR))
    import main as rs_main  # type: ignore

    print("\nCalling _consume_repeat_trial_bypass_once (1st time)…")
    first = bool(rs_main._consume_repeat_trial_bypass_once(int(did)))
    print("consume returned:", first)
    print("Flags after consume #1:", get_flags(did))

    print("\nCalling _consume_repeat_trial_bypass_once (2nd time)…")
    second = bool(rs_main._consume_repeat_trial_bypass_once(int(did)))
    print("consume returned:", second)
    print("Flags after consume #2:", get_flags(did))

    ok = first is True and second is False
    print("\nRESULT:", "OK (bypass consumed once)" if ok else "FAIL (unexpected consume behavior)")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

