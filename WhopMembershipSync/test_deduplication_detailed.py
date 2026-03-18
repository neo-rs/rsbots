#!/usr/bin/env python3
"""
Detailed test to trace deduplication logic and find why active members are being lost.
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_deduplication_detailed():
    """Test deduplication logic in detail to find the bug."""
    
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
    print("DETAILED DEDUPLICATION TEST")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    status_priority = {
        "canceling": 1,
        "renewing": 2,
        "active": 3,
        "trialing": 4,
        "churned": 5,
        "expired": 6,
        "completed": 7,
        "past_due": 8,
        "unresolved": 9,
        "drafted": 10,
        "left": 11,
    }
    
    def get_status_priority(status: str) -> int:
        return status_priority.get(status.lower(), 999)
    
    # Step 1: Fetch ALL active memberships
    print("\nStep 1: Fetching active memberships from /memberships...")
    active_memberships = []
    active_emails = set()
    
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
                    
                    if email:
                        active_memberships.append({
                            "email": email,
                            "status": "active",
                            "priority": 3,
                            "source": "/memberships",
                        })
                        active_emails.add(email)
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"  Found {len(active_memberships)} active memberships")
    print(f"  Unique emails: {len(active_emails)}")
    
    # Step 2: Fetch "left" members
    print("\nStep 2: Fetching 'left' members from /members...")
    left_members = []
    left_emails = set()
    left_with_active = 0
    
    after = None
    for page in range(100):
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [product_id]} if product_id else {}
            )
            
            for member in batch:
                if not isinstance(member, dict):
                    continue
                
                status = str(member.get("status") or "").strip().lower()
                if status != "left":
                    continue
                
                user_obj = member.get("user") or {}
                email = ""
                if isinstance(user_obj, dict):
                    email = str(user_obj.get("email") or "").strip().lower()
                
                if not email:
                    continue
                
                # Check if member has active membership
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
                                left_with_active += 1
                                break
                
                if not has_active:
                    left_members.append({
                        "email": email,
                        "status": "left",
                        "priority": 11,
                        "source": "/members",
                    })
                    left_emails.add(email)
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"  Found {len(left_members)} 'left' members")
    print(f"  Unique emails: {len(left_emails)}")
    print(f"  'Left' members that have active membership (should be excluded): {left_with_active}")
    
    # Step 3: Check overlaps
    print("\nStep 3: Checking overlaps...")
    overlap = active_emails & left_emails
    print(f"  Emails in BOTH active and left lists: {len(overlap)}")
    
    if len(overlap) > 0:
        print(f"\n  WARNING: {len(overlap)} members appear in BOTH lists!")
        print(f"  Sample overlapping emails (first 10):")
        for email in list(overlap)[:10]:
            print(f"    - {email}")
    
    # Step 4: Simulate deduplication
    print("\nStep 4: Simulating deduplication...")
    
    # Combine all memberships
    all_items = active_memberships + left_members
    
    # Deduplicate
    member_status_map = {}
    replacements = []
    
    for item in all_items:
        email = item["email"]
        status = item["status"]
        priority = item["priority"]
        source = item["source"]
        
        existing = member_status_map.get(email)
        if existing:
            existing_status = existing["status"]
            existing_priority = get_status_priority(existing_status)
            existing_source = existing["source"]
            
            if priority < existing_priority:
                # Current has higher priority, replace
                replacements.append({
                    "email": email,
                    "old_status": existing_status,
                    "old_source": existing_source,
                    "new_status": status,
                    "new_source": source,
                })
                member_status_map[email] = {
                    "status": status,
                    "source": source,
                }
            elif priority == existing_priority:
                # Same priority - keep existing
                pass
            else:
                # Existing has higher priority - keep existing
                pass
        else:
            member_status_map[email] = {
                "status": status,
                "source": source,
            }
    
    # Count final statuses
    final_counts = defaultdict(int)
    for email, data in member_status_map.items():
        final_counts[data["status"]] += 1
    
    print(f"\n  Final counts after deduplication:")
    for status, count in sorted(final_counts.items(), key=lambda x: status_priority.get(x[0], 999)):
        print(f"    - {status}: {count}")
    
    print(f"\n  Replacements made: {len(replacements)}")
    
    # Check if any active members were replaced by left
    active_replaced_by_left = [
        r for r in replacements 
        if r["old_status"] == "active" and r["new_status"] == "left"
    ]
    
    if active_replaced_by_left:
        print(f"\n  ERROR: {len(active_replaced_by_left)} active members were replaced by 'left'!")
        print(f"  Sample (first 5):")
        for r in active_replaced_by_left[:5]:
            print(f"    - {r['email']}: active ({r['old_source']}) -> left ({r['new_source']})")
    else:
        print(f"\n  OK: No active members were replaced by 'left'")
    
    # Check if any left members were replaced by active
    left_replaced_by_active = [
        r for r in replacements 
        if r["old_status"] == "left" and r["new_status"] == "active"
    ]
    
    print(f"\n  'Left' members replaced by 'active': {len(left_replaced_by_active)}")
    
    # Step 5: Check what happened to the 210 missing active members
    print("\nStep 5: Analyzing missing active members...")
    final_active_emails = {
        email for email, data in member_status_map.items() 
        if data["status"] == "active"
    }
    
    missing_active = active_emails - final_active_emails
    print(f"  Missing active members: {len(missing_active)}")
    
    if len(missing_active) > 0:
        print(f"\n  Sample missing emails (first 10):")
        for email in list(missing_active)[:10]:
            final_status = member_status_map.get(email, {}).get("status", "NOT_FOUND")
            print(f"    - {email}: Final status = {final_status}")
    
    # Check if missing active members are in left list
    missing_in_left = missing_active & left_emails
    print(f"\n  Missing active members that are ALSO in 'left' list: {len(missing_in_left)}")
    
    if len(missing_in_left) > 0:
        print(f"  This suggests 'left' is overwriting 'active' during deduplication!")
        print(f"  Sample (first 5):")
        for email in list(missing_in_left)[:5]:
            print(f"    - {email}")

if __name__ == "__main__":
    asyncio.run(test_deduplication_detailed())
