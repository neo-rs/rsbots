#!/usr/bin/env python3
"""
Detailed breakdown of Active tab members that are NOT 'renewing'.
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_non_renewing_breakdown():
    """Break down the 68 Active tab members that are NOT 'renewing'."""
    
    config_file = Path(__file__).parent / "config.secrets.json"
    if not config_file.exists():
        print("ERROR: config.secrets.json not found")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        secrets = json.load(f)
    
    api_key = secrets.get("whop_api", {}).get("api_key", "").strip()
    company_id = "biz_s58kr1WWnL1bzH"
    main_product_id = "prod_RrcvGelB8tVgu"
    
    if not api_key:
        print("ERROR: Missing API key")
        return
    
    print("=" * 80)
    print("NON-RENEWING ACTIVE TAB MEMBERS BREAKDOWN")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Fetch all active tab statuses
    print("\nStep 1: Fetching memberships for Active tab statuses...")
    active_tab_statuses = ["active", "past_due", "completed", "unresolved", "drafted"]
    
    all_active_tab_memberships = []
    by_status = defaultdict(list)
    
    for status_filter in active_tab_statuses:
        after = None
        while True:
            try:
                batch, page_info = await whop_client.list_memberships(
                    first=100,
                    after=after,
                    params={
                        "product_ids": [main_product_id],
                        "statuses[]": [status_filter]
                    }
                )
                
                for mship in batch:
                    if isinstance(mship, dict):
                        all_active_tab_memberships.append(mship)
                        by_status[status_filter].append(mship)
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                break
    
    print(f"  Total memberships: {len(all_active_tab_memberships)}")
    for status, memberships in sorted(by_status.items()):
        print(f"    {status:15}: {len(memberships):4}")
    
    # Fetch renewing members
    print("\nStep 2: Fetching 'renewing' members...")
    renewing_emails = set()
    
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [main_product_id], "most_recent_actions[]": ["renewing"]}
            )
            
            for member in batch:
                if isinstance(member, dict):
                    user_obj = member.get("user") or {}
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                        if email:
                            renewing_emails.add(email)
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            break
    
    print(f"  Total 'renewing' emails: {len(renewing_emails)}")
    
    # Categorize Active tab members
    print("\nStep 3: Categorizing Active tab members...")
    
    active_tab_emails = {}
    for mship in all_active_tab_memberships:
        user_obj = mship.get("user") or {}
        if isinstance(user_obj, dict):
            email = str(user_obj.get("email") or "").strip().lower()
            if email:
                status = str(mship.get("status") or "").strip().lower()
                active_tab_emails[email] = status
    
    # Breakdown
    renewing_and_active = []
    renewing_and_past_due = []
    renewing_and_completed = []
    non_renewing_active = []
    non_renewing_past_due = []
    non_renewing_completed = []
    
    for email, status in active_tab_emails.items():
        is_renewing = email in renewing_emails
        
        if status == "active":
            if is_renewing:
                renewing_and_active.append(email)
            else:
                non_renewing_active.append(email)
        elif status == "past_due":
            if is_renewing:
                renewing_and_past_due.append(email)
            else:
                non_renewing_past_due.append(email)
        elif status == "completed":
            if is_renewing:
                renewing_and_completed.append(email)
            else:
                non_renewing_completed.append(email)
    
    print("\n" + "=" * 80)
    print("BREAKDOWN OF ACTIVE TAB MEMBERS")
    print("=" * 80)
    
    print(f"\nStatus='active' (actual paying):")
    print(f"  - Also 'renewing': {len(renewing_and_active)} (auto-renewing subscriptions)")
    print(f"  - NOT 'renewing': {len(non_renewing_active)} (manual renewals or different billing cycle)")
    
    print(f"\nStatus='past_due':")
    print(f"  - Also 'renewing': {len(renewing_and_past_due)}")
    print(f"  - NOT 'renewing': {len(non_renewing_past_due)}")
    
    print(f"\nStatus='completed':")
    print(f"  - Also 'renewing': {len(renewing_and_completed)}")
    print(f"  - NOT 'renewing': {len(non_renewing_completed)}")
    
    total_non_renewing = len(non_renewing_active) + len(non_renewing_past_due) + len(non_renewing_completed)
    print(f"\nTotal NOT 'renewing': {total_non_renewing}")
    print(f"  Breakdown:")
    print(f"    - active status: {len(non_renewing_active)}")
    print(f"    - past_due status: {len(non_renewing_past_due)}")
    print(f"    - completed status: {len(non_renewing_completed)}")
    
    # Fetch details for non-renewing active members
    if non_renewing_active:
        print(f"\nStep 4: Fetching details for {len(non_renewing_active)} non-renewing 'active' members...")
        
        # Get their membership details
        non_renewing_active_details = []
        for email in non_renewing_active[:10]:  # Sample first 10
            # Find their membership
            for mship in by_status["active"]:
                user_obj = mship.get("user") or {}
                if isinstance(user_obj, dict):
                    mship_email = str(user_obj.get("email") or "").strip().lower()
                    if mship_email == email:
                        # Check billing details
                        plan_obj = mship.get("plan") or {}
                        billing_period = str(plan_obj.get("billing_period") or "").strip().lower()
                        cancel_at_period_end = mship.get("cancel_at_period_end", False)
                        
                        non_renewing_active_details.append({
                            "email": email,
                            "billing_period": billing_period,
                            "cancel_at_period_end": cancel_at_period_end,
                            "membership_id": mship.get("id"),
                        })
                        break
        
        print(f"\n  Sample of non-renewing 'active' members (first 10):")
        for detail in non_renewing_active_details:
            print(f"    - {detail['email']}")
            print(f"      Billing period: {detail['billing_period'] or 'unknown'}")
            print(f"      Cancel at period end: {detail['cancel_at_period_end']}")
    
    print("\n" + "=" * 80)
    print("ACCURATE SUMMARY")
    print("=" * 80)
    print(f"\nActual PAYING members (status='active' only): {len(by_status['active'])}")
    print(f"  - Auto-renewing ('renewing'): {len(renewing_and_active)}")
    print(f"  - NOT auto-renewing: {len(non_renewing_active)}")
    print(f"\nActive tab total (includes past_due, completed): {len(active_tab_emails)}")
    print(f"  Terminal shows: 275")
    print(f"  Difference: {275 - len(active_tab_emails)} (likely timing or 1 member without email)")

if __name__ == "__main__":
    asyncio.run(test_non_renewing_breakdown())
