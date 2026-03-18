#!/usr/bin/env python3
"""
Direct test script to compare Whop API active memberships vs our code logic.
"""

import asyncio
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_active_members():
    """Test fetching active memberships directly from Whop API."""
    
    # Load config
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
    
    print("=" * 60)
    print("TESTING ACTIVE MEMBERSHIPS")
    print("=" * 60)
    print(f"Product ID: {product_id}")
    print()
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Test 1: Fetch active memberships directly
    print("TEST 1: Fetching active memberships from /memberships endpoint...")
    print("-" * 60)
    
    active_memberships = []
    after = None
    page_count = 0
    
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
            
            page_count += 1
            print(f"  Page {page_count}: Fetched {len(batch)} memberships")
            
            for mship in batch:
                if isinstance(mship, dict):
                    mship_id = str(mship.get("id") or "").strip()
                    status = str(mship.get("status") or "").strip().lower()
                    cancel_at_period_end = mship.get("cancel_at_period_end", False)
                    
                    # Check if this would be marked as "canceling" by our code
                    would_be_canceling = cancel_at_period_end is True and status in ["active", "trialing"]
                    
                    user_obj = mship.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip()
                    
                    active_memberships.append({
                        "id": mship_id,
                        "status": status,
                        "cancel_at_period_end": cancel_at_period_end,
                        "would_be_canceling": would_be_canceling,
                        "email": email,
                    })
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"  ERROR: {e}")
            break
    
    print(f"\nTotal active memberships fetched: {len(active_memberships)}")
    
    # Count how many would be marked as "canceling"
    canceling_count = sum(1 for m in active_memberships if m["would_be_canceling"])
    true_active_count = len(active_memberships) - canceling_count
    
    print(f"  - Would be marked as 'canceling': {canceling_count}")
    print(f"  - Would remain as 'active': {true_active_count}")
    print()
    
    # Test 2: Fetch from /members endpoint with most_recent_actions=canceling
    print("TEST 2: Fetching canceling members from /members endpoint...")
    print("-" * 60)
    
    canceling_members = []
    canceling_after = None
    canceling_page = 0
    
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=canceling_after,
                params={
                    "product_ids": [product_id],
                    "most_recent_actions[]": ["canceling"]
                }
            )
            
            canceling_page += 1
            print(f"  Page {canceling_page}: Fetched {len(batch)} members")
            
            for member in batch:
                if isinstance(member, dict):
                    member_id = str(member.get("id") or "").strip()
                    action = str(member.get("most_recent_action") or "").strip().lower()
                    user_obj = member.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip()
                    
                    canceling_members.append({
                        "id": member_id,
                        "most_recent_action": action,
                        "email": email,
                    })
            
            if not page_info.get("has_next_page"):
                break
            canceling_after = page_info.get("end_cursor")
            if not canceling_after:
                break
        except Exception as e:
            print(f"  ERROR: {e}")
            break
    
    print(f"\nTotal canceling members from /members: {len(canceling_members)}")
    print()
    
    # Test 3: Check for duplicates/overlaps
    print("TEST 3: Checking for overlaps...")
    print("-" * 60)
    
    active_emails = {m["email"].lower() for m in active_memberships if m["email"]}
    canceling_emails = {m["email"].lower() for m in canceling_members if m["email"]}
    
    overlap = active_emails & canceling_emails
    print(f"  Active memberships with email: {len(active_emails)}")
    print(f"  Canceling members with email: {len(canceling_emails)}")
    print(f"  Overlap (same email in both): {len(overlap)}")
    
    if overlap:
        print(f"\n  WARNING: {len(overlap)} members appear in both active and canceling!")
        print("  Sample overlapping emails:")
        for email in list(overlap)[:5]:
            print(f"    - {email}")
    print()
    
    # Test 4: Fetch ALL memberships (no status filter) to see total
    print("TEST 4: Fetching ALL memberships (no status filter)...")
    print("-" * 60)
    
    all_memberships = []
    all_after = None
    all_page = 0
    
    while all_page < 10:  # Limit to first 10 pages for speed
        try:
            batch, page_info = await whop_client.list_memberships(
                first=100,
                after=all_after,
                params={"product_ids": [product_id]}
            )
            
            all_page += 1
            print(f"  Page {all_page}: Fetched {len(batch)} memberships")
            
            status_counts = {}
            for mship in batch:
                if isinstance(mship, dict):
                    status = str(mship.get("status") or "").strip().lower()
                    status_counts[status] = status_counts.get(status, 0) + 1
            
            all_memberships.extend(batch)
            
            if not page_info.get("has_next_page"):
                break
            all_after = page_info.get("end_cursor")
            if not all_after:
                break
        except Exception as e:
            print(f"  ERROR: {e}")
            break
    
    print(f"\nTotal memberships fetched (first 10 pages): {len(all_memberships)}")
    print("Status breakdown:")
    status_counts = {}
    for mship in all_memberships:
        if isinstance(mship, dict):
            status = str(mship.get("status") or "").strip().lower()
            status_counts[status] = status_counts.get(status, 0) + 1
    
    for status, count in sorted(status_counts.items()):
        print(f"  - {status}: {count}")
    print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Active memberships (from /memberships): {len(active_memberships)}")
    print(f"  - Would be 'canceling': {canceling_count}")
    print(f"  - Would be 'active': {true_active_count}")
    print(f"Canceling members (from /members): {len(canceling_members)}")
    print(f"Expected 'active' count in sheet: {true_active_count}")
    print()
    print("If sheet shows only 16 active users, possible issues:")
    print("  1. Deduplication removing active members")
    print("  2. Members without email/Discord ID being skipped")
    print("  3. 'left' members overriding active memberships")
    print("  4. Other statuses overriding 'active'")

if __name__ == "__main__":
    asyncio.run(test_active_members())
