#!/usr/bin/env python3
"""
Comprehensive test to compare Whop API direct results vs our code logic for each status.
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_all_statuses():
    """Test fetching each status separately and compare with our code logic."""
    
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
    
    print("=" * 80)
    print("COMPREHENSIVE STATUS TESTING")
    print("=" * 80)
    print(f"Product ID: {product_id}")
    print()
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    results = {}
    
    # Test 1: Statuses from /memberships endpoint
    print("=" * 80)
    print("TEST 1: Statuses from /memberships endpoint")
    print("=" * 80)
    
    statuses_to_test = ["trialing", "active", "past_due", "completed", "expired", "unresolved", "drafted"]
    
    for status_filter in statuses_to_test:
        print(f"\n--- Testing status: {status_filter} ---")
        print("-" * 80)
        
        direct_count = 0
        would_be_canceling = 0
        would_be_active = 0
        memberships_with_email = 0
        memberships_with_discord = 0
        memberships_no_id = 0
        
        after = None
        page = 0
        
        while True:
            try:
                batch, page_info = await whop_client.list_memberships(
                    first=100,
                    after=after,
                    params={
                        "product_ids": [product_id],
                        "statuses[]": [status_filter]
                    }
                )
                
                page += 1
                if page == 1:
                    print(f"  Fetched page {page}: {len(batch)} memberships")
                
                for mship in batch:
                    if not isinstance(mship, dict):
                        continue
                    
                    direct_count += 1
                    
                    # Check cancel_at_period_end
                    cancel_at_period_end = mship.get("cancel_at_period_end", False)
                    base_status = str(mship.get("status") or "").strip().lower()
                    
                    # Our code logic: would this be marked as "canceling"?
                    if cancel_at_period_end is True and base_status in ["active", "trialing"]:
                        would_be_canceling += 1
                        final_status = "canceling"
                    else:
                        final_status = base_status
                        if final_status == "active":
                            would_be_active += 1
                    
                    # Check for email/Discord ID
                    user_obj = mship.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip()
                    
                    member_obj = mship.get("member") or {}
                    member_id = None
                    if isinstance(member_obj, dict):
                        member_id = str(member_obj.get("id") or "").strip()
                    
                    # Simulate Discord ID extraction (simplified)
                    discord_id = ""
                    if member_id:
                        # In real code, we'd fetch member record, but for test we'll just check if ID exists
                        pass
                    
                    if email:
                        memberships_with_email += 1
                    if discord_id:
                        memberships_with_discord += 1
                    if not email and not discord_id:
                        memberships_no_id += 1
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                print(f"  ERROR: {e}")
                break
        
        print(f"\n  Direct API count: {direct_count}")
        print(f"  - Would be 'canceling': {would_be_canceling}")
        print(f"  - Would be '{status_filter}': {would_be_active if status_filter == 'active' else direct_count - would_be_canceling}")
        print(f"  - With email: {memberships_with_email}")
        print(f"  - With Discord ID: {memberships_with_discord}")
        print(f"  - NO email/Discord ID (would be skipped): {memberships_no_id}")
        
        results[f"/memberships:{status_filter}"] = {
            "direct_count": direct_count,
            "would_be_canceling": would_be_canceling,
            "would_be_status": direct_count - would_be_canceling if status_filter in ["active", "trialing"] else direct_count,
            "with_email": memberships_with_email,
            "no_id": memberships_no_id,
        }
    
    # Test 2: Special statuses from /members endpoint
    print("\n" + "=" * 80)
    print("TEST 2: Special statuses from /members endpoint")
    print("=" * 80)
    
    special_statuses = {
        "canceling": {"filter": "most_recent_actions[]", "value": "canceling"},
        "churned": {"filter": "most_recent_actions[]", "value": "churned"},
        "left": {"filter": "statuses[]", "value": "left"},
    }
    
    for status_name, config in special_statuses.items():
        print(f"\n--- Testing status: {status_name} ---")
        print("-" * 80)
        
        direct_count = 0
        members_with_email = 0
        members_no_id = 0
        members_with_active_membership = 0
        
        after = None
        page = 0
        
        while True:
            try:
                params = {"product_ids": [product_id]} if product_id else {}
                if config["filter"] == "most_recent_actions[]":
                    params["most_recent_actions[]"] = [config["value"]]
                else:
                    params["statuses[]"] = [config["value"]]
                
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=after,
                    params=params
                )
                
                page += 1
                if page == 1:
                    print(f"  Fetched page {page}: {len(batch)} members")
                
                for member in batch:
                    if not isinstance(member, dict):
                        continue
                    
                    direct_count += 1
                    
                    # Check for email
                    user_obj = member.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip()
                    
                    if email:
                        members_with_email += 1
                    else:
                        members_no_id += 1
                    
                    # Check if member has active membership (should be excluded)
                    memberships = member.get("memberships") or []
                    has_active = False
                    if isinstance(memberships, list):
                        for mship in memberships:
                            if isinstance(mship, dict):
                                mship_status = str(mship.get("status") or "").strip().lower()
                                if mship_status in ["active", "trialing"]:
                                    has_active = True
                                    break
                    
                    if has_active:
                        members_with_active_membership += 1
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                print(f"  ERROR: {e}")
                break
        
        print(f"\n  Direct API count: {direct_count}")
        print(f"  - With email: {members_with_email}")
        print(f"  - NO email/Discord ID (would be skipped): {members_no_id}")
        print(f"  - Have active membership (should be excluded): {members_with_active_membership}")
        print(f"  - Should be included: {direct_count - members_with_active_membership}")
        
        results[f"/members:{status_name}"] = {
            "direct_count": direct_count,
            "with_email": members_with_email,
            "no_id": members_no_id,
            "with_active_membership": members_with_active_membership,
            "should_include": direct_count - members_with_active_membership,
        }
    
    # Test 3: Simulate our code's deduplication logic
    print("\n" + "=" * 80)
    print("TEST 3: Simulating our code's deduplication")
    print("=" * 80)
    
    # Status priority from our code (updated - removed renewing)
    status_priority = {
        "canceling": 1,
        "active": 2,
        "trialing": 3,
        "churned": 4,
        "expired": 5,
        "completed": 6,
        "past_due": 7,
        "unresolved": 8,
        "drafted": 9,
        "left": 10,
    }
    
    def get_status_priority(status: str) -> int:
        return status_priority.get(status.lower(), 999)
    
    # Simulate: fetch all memberships and special members, then deduplicate
    print("\nSimulating full fetch and deduplication...")
    
    # Fetch all memberships
    all_memberships_sim = []
    for status_filter in statuses_to_test:
        after = None
        while True:
            try:
                batch, page_info = await whop_client.list_memberships(
                    first=100,
                    after=after,
                    params={
                        "product_ids": [product_id],
                        "statuses[]": [status_filter]
                    }
                )
                for mship in batch:
                    if isinstance(mship, dict):
                        base_status = str(mship.get("status") or "").strip().lower()
                        if base_status == "canceled":
                            continue
                        if mship.get("cancel_at_period_end") is True and base_status in ["active", "trialing"]:
                            final_status = "canceling"
                        else:
                            final_status = base_status
                        
                        user_obj = mship.get("user") or {}
                        email = ""
                        if isinstance(user_obj, dict):
                            email = str(user_obj.get("email") or "").strip()
                        
                        if email:
                            all_memberships_sim.append({
                                "email": email.lower(),
                                "status": final_status,
                                "priority": get_status_priority(final_status),
                            })
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception:
                break
    
    # Fetch special members (simplified - just count)
    special_counts = {}
    for status_name in ["canceling", "churned", "left"]:
        count = 0
        after = None
        while True:
            try:
                params = {"product_ids": [product_id]} if product_id else {}
                if status_name != "left":
                    params["most_recent_actions[]"] = [status_name]
                else:
                    params["statuses[]"] = ["left"]
                
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=after,
                    params=params
                )
                
                for member in batch:
                    if isinstance(member, dict):
                        user_obj = member.get("user") or {}
                        email = ""
                        if isinstance(user_obj, dict):
                            email = str(user_obj.get("email") or "").strip()
                        
                        if email:
                            # Check if has active membership (should exclude)
                            memberships = member.get("memberships") or []
                            has_active = False
                            if isinstance(memberships, list):
                                for mship in memberships:
                                    if isinstance(mship, dict):
                                        mship_status = str(mship.get("status") or "").strip().lower()
                                        if mship_status in ["active", "trialing"]:
                                            has_active = True
                                            break
                            
                            if not has_active:
                                count += 1
                                all_memberships_sim.append({
                                    "email": email.lower(),
                                    "status": status_name,
                                    "priority": get_status_priority(status_name),
                                })
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception:
                break
        
        special_counts[status_name] = count
    
    # Deduplicate
    member_status_map = {}
    for item in all_memberships_sim:
        email = item["email"]
        status = item["status"]
        priority = item["priority"]
        
        existing = member_status_map.get(email)
        if existing:
            existing_priority = get_status_priority(existing["status"])
            if priority < existing_priority:
                member_status_map[email] = {"status": status}
            elif priority == existing_priority:
                # Keep existing
                pass
        else:
            member_status_map[email] = {"status": status}
    
    # Count final statuses
    final_status_counts = defaultdict(int)
    for email, data in member_status_map.items():
        final_status_counts[data["status"]] += 1
    
    print("\nFinal deduplicated counts:")
    for status, count in sorted(final_status_counts.items(), key=lambda x: status_priority.get(x[0], 999)):
        print(f"  - {status}: {count}")
    
    # Summary comparison
    print("\n" + "=" * 80)
    print("SUMMARY COMPARISON")
    print("=" * 80)
    
    print("\nExpected from direct API calls:")
    print(f"  Active: {results.get('/memberships:active', {}).get('would_be_status', 0)}")
    print(f"  Canceling: {results.get('/members:canceling', {}).get('should_include', 0)}")
    print(f"  Left: {results.get('/members:left', {}).get('should_include', 0)}")
    print(f"  Churned: {results.get('/members:churned', {}).get('should_include', 0)}")
    
    print("\nAfter deduplication (simulated):")
    print(f"  Active: {final_status_counts.get('active', 0)}")
    print(f"  Canceling: {final_status_counts.get('canceling', 0)}")
    print(f"  Left: {final_status_counts.get('left', 0)}")
    print(f"  Churned: {final_status_counts.get('churned', 0)}")
    
    print("\n" + "=" * 80)
    print("DIFFERENCES:")
    print("=" * 80)
    
    expected_active = results.get('/memberships:active', {}).get('would_be_status', 0)
    actual_active = final_status_counts.get('active', 0)
    if expected_active != actual_active:
        print(f"  Active: Expected {expected_active}, Got {actual_active} (diff: {actual_active - expected_active})")
    
    expected_canceling = results.get('/members:canceling', {}).get('should_include', 0)
    actual_canceling = final_status_counts.get('canceling', 0)
    if expected_canceling != actual_canceling:
        print(f"  Canceling: Expected {expected_canceling}, Got {actual_canceling} (diff: {actual_canceling - expected_canceling})")

if __name__ == "__main__":
    asyncio.run(test_all_statuses())
