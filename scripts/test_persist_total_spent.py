"""
Test that when the repeat-trial guard fetches a fresh brief, we persist it to member_history
(so last_summary.total_spent gets the API value, e.g. $30.00).

Uses a temp member_history.json and the real record_member_whop_summary from main.
Run from repo root: python scripts/test_persist_total_spent.py
"""
import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RSC_DIR = REPO_ROOT / "RSCheckerbot"
# Prefer RSCheckerbot so "import main" loads RSCheckerbot/main.py, not repo root main.py
if str(RSC_DIR) not in sys.path:
    sys.path.insert(0, str(RSC_DIR))

# Use a temp file so we don't touch real member_history.json
def run_test() -> int:
    with tempfile.TemporaryDirectory(prefix="rsc_test_mh_") as tmpdir:
        tmp_path = Path(tmpdir) / "member_history.json"
        # Start with one user: has ever_trialing, empty total_spent (stale)
        initial = {
            "731728830108270643": {
                "first_join_ts": None,
                "last_join_ts": None,
                "join_count": 0,
                "identity": {},
                "discord": {},
                "access": {},
                "events": [],
                "whop": {
                    "last_summary": {"total_spent": "", "status": "canceled", "product": "Reselling Secrets (Lite)"},
                    "ever_trialing": True,
                    "ever_had_trial_days": None,
                    "last_membership_id": "mem_RsTSPhi1qwYn1T",
                    "last_whop_key": "mem_RsTSPhi1qwYn1T",
                },
            }
        }
        tmp_path.write_text(json.dumps(initial, indent=2), encoding="utf-8")

        # Patch main to use our temp file, then call record_member_whop_summary
        import main as main_mod
        original_path = main_mod.MEMBER_HISTORY_FILE
        try:
            main_mod.MEMBER_HISTORY_FILE = tmp_path
            # Clear any in-memory cache so we read from file
            if hasattr(main_mod, "_MH_LINK_CACHE"):
                main_mod._MH_LINK_CACHE["db"] = None
                main_mod._MH_LINK_CACHE["at"] = 0.0
                main_mod._MH_LINK_CACHE["mtime"] = 0.0

            # This is the same brief shape we get from _fetch_whop_brief_by_membership_id (e.g. API returns $30)
            fresh_brief = {
                "total_spent": "$30.00",
                "status": "completed",
                "product": "Reselling Secrets (Lite)",
                "membership_id": "mem_RsTSPhi1qwYn1T",
                "renewal_window": "",
                "trial_days": "0",
            }
            main_mod.record_member_whop_summary(
                731728830108270643,
                fresh_brief,
                event_type="repeat_trial_refresh",
                membership_id="mem_RsTSPhi1qwYn1T",
                whop_key="mem_RsTSPhi1qwYn1T",
            )
        finally:
            main_mod.MEMBER_HISTORY_FILE = original_path

        # Read back and assert
        with open(tmp_path, encoding="utf-8") as f:
            db = json.load(f)
    rec = db.get("731728830108270643") or {}
    wh = rec.get("whop") or {}
    last = wh.get("last_summary") or {}
    total_spent = str(last.get("total_spent") or "").strip()
    if total_spent != "$30.00":
        print(f"FAIL: expected last_summary.total_spent '$30.00', got {total_spent!r}")
        return 1
    print("OK: record_member_whop_summary persisted total_spent = '$30.00' to member_history (last_summary)")
    return 0


if __name__ == "__main__":
    sys.exit(run_test())
