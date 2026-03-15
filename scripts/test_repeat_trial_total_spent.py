"""
Test that total_spent used by the repeat-trial guard is correctly fetched from the Whop API,
and that the "had trial before" check reads the right fields from member_history.

Usage (from repo root):
  python scripts/test_repeat_trial_total_spent.py --membership-id mem_xxxx
  python scripts/test_repeat_trial_total_spent.py --discord-id 731728830108270643
  python scripts/test_repeat_trial_total_spent.py --check-had-trial-before --discord-id 731728830108270643

--check-had-trial-before: Same logic as main._is_repeat_trial_no_payment (member_history + config).
  Prints ever_trialing, ever_had_trial_days, last_summary.total_spent, and whether guard would fire.
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RSC_DIR = REPO_ROOT / "RSCheckerbot"
if str(RSC_DIR) not in sys.path:
    sys.path.insert(0, str(RSC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config() -> dict:
    cfg_path = RSC_DIR / "config.json"
    secrets_path = RSC_DIR / "config.secrets.json"
    cfg = {}
    if cfg_path.exists():
        with open(cfg_path, encoding="utf-8") as f:
            cfg = json.load(f) or {}
    if secrets_path.exists():
        with open(secrets_path, encoding="utf-8") as f:
            secrets = json.load(f) or {}
        cfg = _deep_merge(cfg, secrets)
    return cfg


def membership_id_from_history(discord_id: int) -> str:
    """Same logic as main._membership_id_from_history (no bot deps)."""
    mh_path = RSC_DIR / "member_history.json"
    if not mh_path.exists():
        return ""
    try:
        with open(mh_path, encoding="utf-8") as f:
            db = json.load(f)
    except Exception:
        return ""
    rec = db.get(str(discord_id)) if isinstance(db, dict) else {}
    wh = rec.get("whop") if isinstance(rec, dict) else None
    if not isinstance(wh, dict):
        return ""
    cands = []
    cands.append(str(wh.get("last_membership_id") or "").strip())
    cands.append(str(wh.get("last_whop_key") or "").strip())
    for row in (wh.get("member_status_logs_latest") or {}).values():
        if isinstance(row, dict):
            cands.append(str(row.get("membership_id") or "").strip())
    for row in (wh.get("native_whop_logs_latest") or {}).values():
        if isinstance(row, dict):
            cands.append(str(row.get("key") or "").strip())
    cands = [x for x in cands if x and x != "—"]
    for x in cands:
        if str(x).startswith("mem_"):
            return str(x)
    for x in cands:
        if str(x).startswith("R-"):
            return str(x)
    return cands[0] if cands else ""


def load_member_history() -> dict:
    """Load member_history.json (same file main uses)."""
    mh_path = RSC_DIR / "member_history.json"
    if not mh_path.exists():
        return {}
    try:
        with open(mh_path, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def is_repeat_trial_no_payment(discord_id: int, cfg: dict) -> bool:
    """
    Same logic as main._is_repeat_trial_no_payment (no bot/discord deps).
    True => guard would remove Member role (had trial before + spend <= threshold).
    """
    from rschecker_utils import usd_amount
    try:
        guard = (cfg.get("whop_api") or {}).get("repeat_trial_guard") or {}
        if not isinstance(guard, dict) or not guard.get("enabled", False):
            return False
        max_spent = float(guard.get("max_total_spent_usd", 0) or 0)
    except Exception:
        return False
    db = load_member_history()
    hist = db.get(str(discord_id), {}) if isinstance(db, dict) else {}
    wh = hist.get("whop") if isinstance(hist.get("whop"), dict) else {}
    if not wh.get("ever_trialing") and not wh.get("ever_had_trial_days"):
        return False
    last = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
    spent_raw = str(last.get("total_spent") or "").strip()
    spent = float(usd_amount(spent_raw)) if spent_raw else 0.0
    return spent <= max_spent


def run_check_had_trial_before(discord_id: int) -> int:
    """Print what main reads for 'had trial before' and whether guard would fire."""
    cfg = load_config()
    db = load_member_history()
    hist = db.get(str(discord_id), {}) if isinstance(db, dict) else {}
    wh = hist.get("whop") if isinstance(hist.get("whop"), dict) else {}
    last = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}

    print(f"Discord ID: {discord_id}")
    print(f"In member_history: {'yes' if hist else 'no'}")
    if not hist:
        print("(No record -> guard treats as no prior trial; would NOT remove.)")
        return 0
    print()
    print("--- Same fields main._is_repeat_trial_no_payment reads ---")
    ever_trialing = wh.get("ever_trialing")
    ever_had_trial_days = wh.get("ever_had_trial_days")
    print(f"  ever_trialing:          {ever_trialing!r}")
    print(f"  ever_had_trial_days:    {ever_had_trial_days!r}")
    had_trial_before = bool(ever_trialing or ever_had_trial_days)
    print(f"  => 'Had trial before':  {had_trial_before}")
    print()
    spent_raw = str(last.get("total_spent") or "").strip()
    print(f"  last_summary.total_spent: {spent_raw!r}")
    guard = (cfg.get("whop_api") or {}).get("repeat_trial_guard") or {}
    max_spent = float(guard.get("max_total_spent_usd", 0) or 0)
    from rschecker_utils import usd_amount
    spent = float(usd_amount(spent_raw)) if spent_raw else 0.0
    print(f"  repeat_trial_guard.max_total_spent_usd: {max_spent}")
    print(f"  parsed spend (from history): {spent}")
    print()
    would_fire = is_repeat_trial_no_payment(discord_id, cfg)
    print(f"Would guard REMOVE Member role? {would_fire}  (True = repeat trial blocked)")
    print()
    if not had_trial_before:
        print("First-time trial: ever_trialing and ever_had_trial_days both falsy → guard does NOT run.")
    return 0


async def run(membership_id: str, verbose: bool) -> int:
    cfg = load_config()
    wh = cfg.get("whop_api") or {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()

    if not api_key or not company_id:
        print("ERROR: whop_api.api_key or company_id missing (use config.secrets.json)", file=sys.stderr)
        return 1

    from whop_api_client import WhopAPIClient
    from whop_brief import fetch_whop_brief
    from rschecker_utils import usd_amount

    client = WhopAPIClient(api_key, base_url, company_id)
    enable_enrichment = bool(wh.get("enable_enrichment", True))

    print(f"Membership ID: {membership_id}")
    print(f"API base: {base_url}  company_id: {company_id}")
    print()

    # Same path as repeat-trial guard: fetch_whop_brief -> total_spent
    brief = await fetch_whop_brief(client, membership_id, enable_enrichment=enable_enrichment)
    if not isinstance(brief, dict):
        print("ERROR: fetch_whop_brief returned non-dict")
        return 1

    total_spent_raw = brief.get("total_spent")
    parsed = float(usd_amount(total_spent_raw))
    print(f"brief['total_spent'] = {total_spent_raw!r}")
    print(f"usd_amount(brief['total_spent']) = {parsed}")
    print()

    # Guard logic: if parsed > max_total_spent_usd (0), do NOT remove
    guard = wh.get("repeat_trial_guard")
    if not isinstance(guard, dict):
        guard = {}
    max_val = guard.get("max_total_spent_usd", 0)
    try:
        max_spent = 0.0 if isinstance(max_val, dict) else float(max_val or 0)
    except (TypeError, ValueError):
        max_spent = 0.0
    would_remove = parsed <= max_spent
    print(f"repeat_trial_guard.max_total_spent_usd = {max_spent}")
    print(f"Would guard REMOVE Member role? {would_remove}  (False = allow access)")
    print()

    if verbose:
        # Fetch raw membership and member to see what API returns for spend
        membership = await client.get_membership_by_id(membership_id)
        if isinstance(membership, dict):
            mber = membership.get("member")
            mber_id = None
            if isinstance(mber, dict):
                mber_id = str(mber.get("id") or "").strip()
            elif isinstance(mber, str):
                mber_id = mber.strip()
            if mber_id:
                mrec = await client.get_member_by_id(mber_id)
                if isinstance(mrec, dict):
                    print("--- Raw member record keys containing 'spent' or 'total' ---")
                    for k, v in sorted(mrec.items()):
                        if "spent" in k.lower() or "total" in k.lower():
                            print(f"  {k}: {v}")
                    stats = mrec.get("stats")
                    if isinstance(stats, dict):
                        print("  stats.*:")
                        for k, v in sorted(stats.items()):
                            if "spent" in k.lower() or "total" in k.lower():
                                print(f"    {k}: {v}")
                    user = mrec.get("user")
                    if isinstance(user, dict):
                        for k, v in sorted(user.items()):
                            if "spent" in k.lower() or "total" in k.lower():
                                print(f"  user.{k}: {v}")
                else:
                    print("(get_member_by_id returned no dict)")
            else:
                print("(no member id in membership)")
        else:
            print("(get_membership_by_id returned no dict)")

    return 0


def main():
    ap = argparse.ArgumentParser(description="Test total_spent used by repeat-trial guard")
    ap.add_argument("--membership-id", type=str, help="Whop membership ID (mem_... or R-...)")
    ap.add_argument("--discord-id", type=int, help="Discord user ID (look up membership_id from member_history)")
    ap.add_argument("--check-had-trial-before", action="store_true", help="Test 'had trial before' from member_history (same logic as main)")
    ap.add_argument("--verbose", "-v", action="store_true", help="Print raw API member record spend keys")
    args = ap.parse_args()

    if args.check_had_trial_before:
        if args.discord_id is None:
            print("ERROR: --check-had-trial-before requires --discord-id", file=sys.stderr)
            return 1
        return run_check_had_trial_before(args.discord_id)

    mid = (args.membership_id or "").strip()
    if args.discord_id:
        from_mh = membership_id_from_history(int(args.discord_id))
        if mid and mid != from_mh:
            print(f"Using --membership-id {mid} (overrides discord-id lookup)")
        elif from_mh:
            mid = from_mh
            print(f"From member_history for discord_id {args.discord_id}: membership_id = {mid}")
        else:
            print(f"ERROR: No membership_id in member_history for discord_id {args.discord_id}", file=sys.stderr)
            return 1

    if not mid:
        print("ERROR: Provide --membership-id or --discord-id", file=sys.stderr)
        return 1

    return asyncio.run(run(mid, args.verbose))


if __name__ == "__main__":
    sys.exit(main())
