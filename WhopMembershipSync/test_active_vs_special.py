#!/usr/bin/env python3
"""
Test if active members are appearing in special status lists (churned, renewing, canceling).
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_active_vs_special():
    """Check if active members appear in special status lists."""
    
    config_file = Path(__file__).parent / "config.secrets.json"
    if not config_file.exists():
        print("ERROR: config.secrets.json not found")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        secrets = json.load(f)
    
    api_key = secrets.get("whop_api", {}).get("api_key", "").strip()
    company_id = "biz_s58kr1WWnL1bzH"
    product_id = "prod_RrcvGelB8tVgu"
    
    if not api_key:
        print("ERROR: Missing API key")
        return
    
    print("=" * 80)
    print("ACTIVE vs SPECIAL STATUS OVERLAP TEST")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Step 1: Fetch active memberships
    print("\nStep 1: Fetching active memberships...")
    active_emails = set()
    active_member_ids = set()
    
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_memberships(
                first=100,
                after=after,
                params={
                    "product_ids": [product_id],
                    "statuses[]": ["active"]
                }
            )
            
            for mship in batch:
                if isinstance(mship, dict):
                    user_obj = mship.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                    
                    member_obj = mship.get("member") or {}
                    member_id = ""
                    if isinstance(member_obj, dict):
                        member_id = str(member_obj.get("id") or "").strip()
                    
                    if email:
                        active_emails.add(email)
                    if member_id:
                        active_member_ids.add(member_id)
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"  Found {len(active_emails)} active members (by email)")
    print(f"  Found {len(active_member_ids)} active members (by member ID)")
    
    # Step 2: Check each special status
    special_statuses = ["canceling", "churned", "renewing"]
    
    for status_name in special_statuses:
        print(f"\nStep 2.{special_statuses.index(status_name) + 1}: Checking '{status_name}' members...")
        
        special_emails = set()
        special_member_ids = set()
        special_with_active_membership = []
        
        after = None
        for page in range(100):
            try:
                params = {"product_ids": [product_id]} if product_id else {}
                params["most_recent_actions[]"] = [status_name]
                
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=after,
                    params=params
                )
                
                for member in batch:
                    if not isinstance(member, dict):
                        continue
                    
                    member_id = str(member.get("id") or "").strip()
                    
                    user_obj = member.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                    
                    if email:
                        special_emails.add(email)
                    if member_id:
                        special_member_ids.add(member_id)
                    
                    # Check if this member has an active membership
                    memberships = member.get("memberships") or []
                    has_active = False
                    if isinstance(memberships, list):
                        for mship in memberships:
                            if isinstance(mship, dict):
                                mship_status = str(mship.get("status") or "").strip().lower()
                                mship_product_id = ""
                                product_obj = mship.get("product") or {}
                                if isinstance(product_obj, dict):
                                    mship_product_id = str(product_obj.get("id") or "").strip()
                                
                                if mship_product_id == product_id and mship_status in ["active", "trialing"]:
                                    has_active = True
                                    if email:
                                        special_with_active_membership.append({
                                            "email": email,
                                            "member_id": member_id,
                                            "status": status_name,
                                        })
                                    break
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                print(f"ERROR: {e}")
                break
        
        print(f"  Found {len(special_emails)} '{status_name}' members (by email)")
        print(f"  Found {len(special_member_ids)} '{status_name}' members (by member ID)")
        
        # Check overlaps
        email_overlap = active_emails & special_emails
        member_id_overlap = active_member_ids & special_member_ids
        
        print(f"  Overlap with active (by email): {len(email_overlap)}")
        print(f"  Overlap with active (by member ID): {len(member_id_overlap)}")
        print(f"  '{status_name}' members that have active membership: {len(special_with_active_membership)}")
        
        if len(email_overlap) > 0:
            print(f"\n  WARNING: {len(email_overlap)} active members also appear in '{status_name}' list!")
            print(f"  Sample (first 5):")
            for email in list(email_overlap)[:5]:
                print(f"    - {email}")
        
        if len(special_with_active_membership) > 0:
            print(f"\n  CRITICAL: {len(special_with_active_membership)} '{status_name}' members have active memberships!")
            print(f"  These should be EXCLUDED from '{status_name}' list but included as 'active'")
            print(f"  Sample (first 5):")
            for item in special_with_active_membership[:5]:
                print(f"    - {item['email']} (member_id: {item['member_id']})")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("\nIf any special status members have active memberships, they should be")
    print("excluded from the special list and handled as 'active' instead.")

if __name__ == "__main__":
    asyncio.run(test_active_vs_special())
