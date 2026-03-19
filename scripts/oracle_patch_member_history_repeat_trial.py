"""
Patch RSCheckerbot/member_history.json on Oracle to force a repeat-trial scenario for a Discord ID.

Usage (run on Oracle):
  python3 scripts/oracle_patch_member_history_repeat_trial.py --discord-id 1393361893061689504 --ever-trialing 1
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--discord-id", required=True, type=str)
    ap.add_argument("--ever-trialing", default="1", type=str)
    args = ap.parse_args()

    did = str(args.discord_id).strip()
    if not did.isdigit():
        raise SystemExit("discord-id must be numeric")

    ever_trialing = str(args.ever_trialing).strip().lower() in {"1", "true", "yes", "on"}

    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "RSCheckerbot" / "member_history.json"
    if not path.exists():
        raise SystemExit(f"member_history.json not found at: {path}")

    db = json.loads(path.read_text(encoding="utf-8"))
    rec = db.get(did) or {}
    wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
    prev = (wh.get("ever_trialing"), wh.get("ever_had_trial_days"))

    if ever_trialing:
        wh["ever_trialing"] = True
    rec["whop"] = wh
    db[did] = rec

    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(db, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)

    now = (wh.get("ever_trialing"), wh.get("ever_had_trial_days"))
    last = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
    print(
        "UPDATED",
        did,
        "prev",
        prev,
        "now",
        now,
        "membership_id",
        str(last.get("membership_id") or wh.get("last_membership_id") or ""),
        "total_spent",
        str(last.get("total_spent") or ""),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

