#!/usr/bin/env python3
"""
Test to verify active member counts and cross-check with renewing status.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_active_paying_count():
    """Test active members vs paying members (excluding trials and lifetime)."""
    
    config_file = Path(__file__).parent / "config.secrets.json"
    if not config_file.exists():
        print("ERROR: config.secrets.json not found")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        secrets = json.load(f)
    
    api_key = secrets.get("whop_api", {}).get("api_key", "").strip()
    company_id = "biz_s58kr1WWnL1bzH"
    
    # Product IDs
    main_product_id = "prod_RrcvGelB8tVgu"  # Reselling Secrets
    lite_product_id = "prod_U52ytqRZdCFak"  # Reselling Secrets Lite
    lifetime_product_id = "prod_76xygbFOv0aUM"  # Lifetime
    
    if not api_key:
        print("ERROR: Missing API key")
        return
    
    print("=" * 80)
    print("ACTIVE MEMBERS vs PAYING MEMBERS ANALYSIS")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Step 1: Fetch ALL active memberships (all products)
    print("\nStep 1: Fetching ALL active memberships (all products)...")
    all_active_memberships = []
    
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_memberships(
                first=100,
                after=after,
                params={"statuses[]": ["active"]}
            )
            
            for mship in batch:
                if isinstance(mship, dict):
                    all_active_memberships.append(mship)
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"  Total active memberships (all products): {len(all_active_memberships)}")
    
    # Step 2: Categorize by product
    print("\nStep 2: Categorizing by product...")
    
    main_product_active = []
    lite_product_active = []
    lifetime_product_active = []
    other_product_active = []
    
    for mship in all_active_memberships:
        product_obj = mship.get("product") or {}
        if isinstance(product_obj, dict):
            product_id = str(product_obj.get("id") or "").strip()
            
            if product_id == main_product_id:
                main_product_active.append(mship)
            elif product_id == lite_product_id:
                lite_product_active.append(mship)
            elif product_id == lifetime_product_id:
                lifetime_product_active.append(mship)
            else:
                other_product_active.append(mship)
    
    print(f"  - Reselling Secrets (main): {len(main_product_active)}")
    print(f"  - Reselling Secrets Lite: {len(lite_product_active)}")
    print(f"  - Lifetime: {len(lifetime_product_active)}")
    print(f"  - Other products: {len(other_product_active)}")
    
    # Step 3: Check for trialing status (active memberships can be trialing)
    print("\nStep 3: Checking for trialing memberships...")
    
    main_trialing = []
    main_paying = []
    
    for mship in main_product_active:
        status = str(mship.get("status") or "").strip().lower()
        if status == "trialing":
            main_trialing.append(mship)
        else:
            main_paying.append(mship)
    
    print(f"  Reselling Secrets (main):")
    print(f"    - Trialing: {len(main_trialing)}")
    print(f"    - Active (paying): {len(main_paying)}")
    
    # Step 4: Check cancel_at_period_end (canceling)
    print("\nStep 4: Checking cancel_at_period_end (canceling)...")
    
    main_canceling = []
    main_active_not_canceling = []
    
    for mship in main_paying:
        if mship.get("cancel_at_period_end") is True:
            main_canceling.append(mship)
        else:
            main_active_not_canceling.append(mship)
    
    print(f"  Reselling Secrets (main) - Active paying:")
    print(f"    - Canceling (cancel_at_period_end=true): {len(main_canceling)}")
    print(f"    - Active (not canceling): {len(main_active_not_canceling)}")
    
    # Step 5: Fetch "renewing" members from /members endpoint
    print("\nStep 5: Fetching 'renewing' members from /members endpoint...")
    
    renewing_members = []
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
                    renewing_members.append(member)
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
            print(f"ERROR: {e}")
            break
    
    print(f"  Total 'renewing' members: {len(renewing_members)}")
    print(f"  Unique emails: {len(renewing_emails)}")
    
    # Step 6: Cross-check active paying with renewing
    print("\nStep 6: Cross-checking active paying members with 'renewing' status...")
    
    active_paying_emails = set()
    for mship in main_active_not_canceling:
        user_obj = mship.get("user") or {}
        if isinstance(user_obj, dict):
            email = str(user_obj.get("email") or "").strip().lower()
            if email:
                active_paying_emails.add(email)
    
    overlap_renewing = active_paying_emails & renewing_emails
    only_active = active_paying_emails - renewing_emails
    only_renewing = renewing_emails - active_paying_emails
    
    print(f"  Active paying members (not canceling): {len(active_paying_emails)}")
    print(f"  'Renewing' members: {len(renewing_emails)}")
    print(f"  Overlap (in both): {len(overlap_renewing)}")
    print(f"  Only in active (not in renewing): {len(only_active)}")
    print(f"  Only in renewing (not in active): {len(only_renewing)}")
    
    # Step 7: Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nReselling Secrets (main product) - Active Members:")
    print(f"  - Total active: {len(main_product_active)}")
    print(f"  - Trialing: {len(main_trialing)}")
    print(f"  - Active (paying): {len(main_paying)}")
    print(f"    - Canceling: {len(main_canceling)}")
    print(f"    - Active (not canceling): {len(main_active_not_canceling)}")
    
    print(f"\nActual PAYING members (excluding trials, lifetime, canceling):")
    print(f"  - Active paying (not canceling): {len(main_active_not_canceling)}")
    
    print(f"\nCross-check with 'renewing' status:")
    print(f"  - Active paying members: {len(active_paying_emails)}")
    print(f"  - 'Renewing' members: {len(renewing_emails)}")
    print(f"  - Overlap: {len(overlap_renewing)}")
    
    if len(overlap_renewing) > 0:
        print(f"\n  WARNING: {len(overlap_renewing)} active paying members also appear in 'renewing' list!")
        print(f"  This suggests they're actively paying AND marked as 'renewing'")
    
    print(f"\n  Expected paying count (excluding trials, lifetime, canceling): {len(main_active_not_canceling)}")

if __name__ == "__main__":
    asyncio.run(test_active_paying_count())
