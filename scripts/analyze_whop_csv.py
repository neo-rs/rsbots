"""
Analyze whop-member-page.csv to derive correct metrics for Cho's numbers.
Cho: 7 new paying, 19 trials, 14 churned, 6 set to cancel.
Whop uses "Joined at" (membership join) for date filter.
"""
import csv
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CSV_PATH = REPO / "whop-member-page.csv"

def parse_dt(s):
    if not s or not str(s).strip():
        return None
    try:
        return datetime.strptime(str(s).strip()[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def main():
    rows = []
    with open(CSV_PATH, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    # Filter: Reselling Secrets FULL only (not Lite)
    full = [r for r in rows if "Reselling Secrets" in str(r.get("Whop title", "")) and "Lite" not in str(r.get("Whop title", ""))]
    # Date range: Feb 8-14, 2026 (Joined at)
    start = datetime(2026, 2, 8, 0, 0, 0)
    end = datetime(2026, 2, 14, 23, 59, 59)

    in_range = []
    for r in full:
        j = parse_dt(r.get("Joined at", ""))
        if j and start <= j <= end:
            in_range.append(r)

    print(f"Reselling Secrets FULL, Joined Feb 8-14: {len(in_range)} memberships\n")

    trialing = [r for r in in_range if str(r.get("Subscription status", "")).strip().lower() == "trialing"]
    active = [r for r in in_range if str(r.get("Subscription status", "")).strip().lower() == "active"]
    canceled = [r for r in in_range if str(r.get("Subscription status", "")).strip().lower() == "canceled"]
    completed = [r for r in in_range if str(r.get("Subscription status", "")).strip().lower() == "completed"]

    paying = [r for r in in_range if float(r.get("Total spend (in USD)", 0) or 0) > 0]
    new_paying = [r for r in active if float(r.get("Total spend (in USD)", 0) or 0) > 0]

    # "Set to cancel" = cancel_at_period_end - in Whop CSV this might be "canceling" status
    # The CSV has "Subscription status" - no "canceling". "canceled" = already ended.
    # Cho's "6 set to cancel" = paying members who chose to cancel (cancel_at_period_end).
    # Whop status "canceling" = set to cancel. Our CSV might only have completed/canceled/active/trialing.
    canceling = [r for r in in_range if str(r.get("Subscription status", "")).strip().lower() == "canceling"]

    # Churned = paying members who canceled/expired (Cho: paying only)
    churned = [r for r in canceled if float(r.get("Total spend (in USD)", 0) or 0) > 0]
    churned_completed = [r for r in completed if float(r.get("Total spend (in USD)", 0) or 0) > 0]
    # Actually "churned" = membership ended. completed with $0 = trial ended. canceled with $0 = trial canceled.
    # Cho: "Churned = paying members only"
    churned_paying = churned  # canceled + had spent

    print(f"By status in range:")
    print(f"  trialing: {len(trialing)}")
    print(f"  active: {len(active)}")
    print(f"  canceled: {len(canceled)}")
    print(f"  completed: {len(completed)}")
    print(f"  canceling: {len(canceling)}")
    print()
    print(f"New paying (active + Total spend > 0): {len(new_paying)}")
    for r in new_paying:
        print(f"  - {r.get('Email')} {r.get('Total spend (in USD)')} {r.get('Subscription status')}")
    print()
    print(f"Trials (status=trialing): {len(trialing)}")
    print()
    print(f"Churned (canceled + paying): {len(churned_paying)}")
    for r in churned_paying:
        print(f"  - {r.get('Email')} spend={r.get('Total spend (in USD)')} status={r.get('Subscription status')}")
    print()
    print(f"Set to cancel (status=canceling in range): {len(canceling)}")

    # Cho's numbers: 7 paying, 19 trials, 14 churned, 6 set to cancel
    # The "set to cancel" and "churned" might be GLOBAL (not date-filtered)
    # Let me also check - maybe Cho means:
    # - 19 trials = trialing in range ✓
    # - 7 new paying = converted to paying IN the range (first payment in range)
    # - 14 churned = ALL paying who churned (global)
    # - 6 set to cancel = ALL paying who set to cancel (global)

    print("\n--- If Churned and Set to cancel are GLOBAL (all RS FULL) ---")
    all_full = [r for r in rows if "Reselling Secrets" in str(r.get("Whop title", "")) and "Lite" not in str(r.get("Whop title", ""))]
    global_canceling = [r for r in all_full if str(r.get("Subscription status", "")).strip().lower() == "canceling" and float(r.get("Total spend (in USD)", 0) or 0) > 0]
    global_canceled_paying = [r for r in all_full if str(r.get("Subscription status", "")).strip().lower() == "canceled" and float(r.get("Total spend (in USD)", 0) or 0) > 0]
    global_expired_paying = [r for r in all_full if str(r.get("Subscription status", "")).strip().lower() == "expired" and float(r.get("Total spend (in USD)", 0) or 0) > 0]
    print(f"Global canceling (paying): {len(global_canceling)}")
    print(f"Global canceled (paying): {len(global_canceled_paying)}")
    print(f"Global expired (paying): {len(global_expired_paying)}")
    print(f"Churned total (canceled+expired paying): {len(global_canceled_paying) + len(global_expired_paying)}")

if __name__ == "__main__":
    main()
