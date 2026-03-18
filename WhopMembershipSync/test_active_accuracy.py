#!/usr/bin/env python3
"""
Test to find why we're missing 3 active members.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_active_accuracy():
    """Find missing active members."""
    
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
    print("FINDING MISSING ACTIVE MEMBERS")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Get all active memberships from API
    print("\nFetching all active memberships from API...")
    active_memberships = []
    active_emails = set()
    active_without_email = []
    
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
                    active_memberships.append(mship)
                    user_obj = mship.get("user") or {}
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                        if email:
                            active_emails.add(email)
                        else:
                            member_obj = mship.get("member") or {}
                            member_id = ""
                            if isinstance(member_obj, dict):
                                member_id = str(member_obj.get("id") or "").strip()
                            active_without_email.append({
                                "membership_id": mship.get("id"),
                                "member_id": member_id,
                            })
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"  Total active memberships: {len(active_memberships)}")
    print(f"  Active members with email: {len(active_emails)}")
    print(f"  Active members WITHOUT email: {len(active_without_email)}")
    
    if active_without_email:
        print(f"\n  Active memberships without email (these would be skipped):")
        for item in active_without_email[:10]:
            print(f"    - Membership ID: {item['membership_id']}, Member ID: {item['member_id']}")
    
    # Check if any active members appear in special status lists
    print("\nChecking if active members appear in special status lists...")
    
    # Check canceling
    canceling_emails = set()
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [product_id], "most_recent_actions[]": ["canceling"]}
            )
            for member in batch:
                if isinstance(member, dict):
                    user_obj = member.get("user") or {}
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                        if email:
                            canceling_emails.add(email)
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    
    overlap_canceling = active_emails & canceling_emails
    if overlap_canceling:
        print(f"  WARNING: {len(overlap_canceling)} active members also in canceling list!")
        print(f"  Sample: {list(overlap_canceling)[:5]}")
    
    # Check churned
    churned_emails = set()
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [product_id], "most_recent_actions[]": ["churned"]}
            )
            for member in batch:
                if isinstance(member, dict):
                    user_obj = member.get("user") or {}
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                        if email:
                            churned_emails.add(email)
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    
    overlap_churned = active_emails & churned_emails
    if overlap_churned:
        print(f"  WARNING: {len(overlap_churned)} active members also in churned list!")
        print(f"  Sample: {list(overlap_churned)[:5]}")
    
    # Check left
    left_emails = set()
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [product_id]}
            )
            for member in batch:
                if isinstance(member, dict):
                    status = str(member.get("status") or "").strip().lower()
                    if status == "left":
                        user_obj = member.get("user") or {}
                        if isinstance(user_obj, dict):
                            email = str(user_obj.get("email") or "").strip().lower()
                            if email:
                                left_emails.add(email)
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    
    overlap_left = active_emails & left_emails
    if overlap_left:
        print(f"  WARNING: {len(overlap_left)} active members also in left list!")
        print(f"  Sample: {list(overlap_left)[:5]}")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total active memberships: {len(active_memberships)}")
    print(f"Active members with email (will be included): {len(active_emails)}")
    print(f"Active members without email (will be skipped): {len(active_without_email)}")
    print(f"\nPotential conflicts:")
    print(f"  - Active in canceling: {len(overlap_canceling)}")
    print(f"  - Active in churned: {len(overlap_churned)}")
    print(f"  - Active in left: {len(overlap_left)}")

if __name__ == "__main__":
    asyncio.run(test_active_accuracy())
