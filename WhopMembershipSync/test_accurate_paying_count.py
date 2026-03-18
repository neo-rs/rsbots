#!/usr/bin/env python3
"""
Accurate test to determine actual paying members and cross-check with renewing.
Simulates exact code logic including deduplication.
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_accurate_paying_count():
    """Accurately determine paying members using exact code logic."""
    
    config_file = Path(__file__).parent / "config.secrets.json"
    if not config_file.exists():
        print("ERROR: config.secrets.json not found")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        secrets = json.load(f)
    
    # Load config.json
    config_json = Path(__file__).parent / "config.json"
    with open(config_json, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    api_key = secrets.get("whop_api", {}).get("api_key", "").strip()
    company_id = "biz_s58kr1WWnL1bzH"
    main_product_id = "prod_RrcvGelB8tVgu"
    lifetime_product_id = "prod_76xygbFOv0aUM"
    
    if not api_key:
        print("ERROR: Missing API key")
        return
    
    print("=" * 80)
    print("ACCURATE PAYING MEMBERS COUNT (Simulating Code Logic)")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Step 1: Fetch all memberships (like code does)
    print("\nStep 1: Fetching all memberships from /memberships endpoint...")
    all_memberships = []
    statuses_to_fetch = ["trialing", "active", "past_due", "completed", "expired", "unresolved", "drafted"]
    
    for status_filter in statuses_to_fetch:
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
                        all_memberships.append(mship)
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                print(f"  ERROR fetching {status_filter}: {e}")
                break
    
    print(f"  Fetched {len(all_memberships)} total memberships")
    
    # Step 2: Build active member set (like code does)
    print("\nStep 2: Building active member set...")
    active_member_emails = set()
    active_member_ids = set()
    
    for mship in all_memberships:
        base_status = str(mship.get("status") or "").strip().lower()
        if base_status in ["active", "trialing"]:
            user_obj = mship.get("user") or {}
            if isinstance(user_obj, dict):
                email = str(user_obj.get("email") or "").strip().lower()
                if email:
                    active_member_emails.add(email)
            
            member_obj = mship.get("member") or {}
            if isinstance(member_obj, dict):
                member_id = str(member_obj.get("id") or "").strip()
                if member_id:
                    active_member_ids.add(member_id)
    
    print(f"  Active member emails: {len(active_member_emails)}")
    print(f"  Active member IDs: {len(active_member_ids)}")
    
    # Step 3: Fetch special status members (excluding those with active memberships)
    print("\nStep 3: Fetching special status members (excluding active)...")
    special_members = {"left": [], "churned": [], "canceling": []}
    excluded_from_special = 0
    
    for action_type in ["canceling", "churned"]:
        after = None
        for page in range(100):
            try:
                params = {"product_ids": [main_product_id]} if main_product_id else {}
                params["most_recent_actions[]"] = [action_type]
                
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=after,
                    params=params
                )
                
                for member in batch:
                    if not isinstance(member, dict):
                        continue
                    
                    member_id = str(member.get("id") or "").strip()
                    if not member_id:
                        continue
                    
                    # Check if already active (like code does)
                    user_obj = member.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                    
                    is_already_active = False
                    if email and email in active_member_emails:
                        is_already_active = True
                    elif member_id in active_member_ids:
                        is_already_active = True
                    
                    if is_already_active:
                        excluded_from_special += 1
                        continue
                    
                    # Verify product association
                    memberships = member.get("memberships") or []
                    has_product_membership = False
                    
                    if isinstance(memberships, list) and memberships:
                        for mship in memberships:
                            product_obj = mship.get("product") or {}
                            if isinstance(product_obj, dict):
                                mship_product_id = str(product_obj.get("id") or "").strip()
                                if mship_product_id == main_product_id:
                                    mship_status = str(mship.get("status") or "").strip().lower()
                                    if mship_status in ["active", "trialing"]:
                                        has_product_membership = False
                                        break
                                    has_product_membership = True
                                    break
                    
                    if not has_product_membership:
                        if main_product_id and not memberships:
                            has_product_membership = True
                        else:
                            continue
                    
                    if not has_product_membership:
                        continue
                    
                    special_members[action_type].append(member)
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                break
    
    # Fetch left members
    after = None
    for page in range(100):
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [main_product_id]} if main_product_id else {}
            )
            
            for member in batch:
                if not isinstance(member, dict):
                    continue
                
                member_id = str(member.get("id") or "").strip()
                if not member_id:
                    continue
                
                # Check if already active
                user_obj = member.get("user") or {}
                email = ""
                if isinstance(user_obj, dict):
                    email = str(user_obj.get("email") or "").strip().lower()
                
                is_already_active = False
                if email and email in active_member_emails:
                    is_already_active = True
                elif member_id in active_member_ids:
                    is_already_active = True
                
                if is_already_active:
                    excluded_from_special += 1
                    continue
                
                status = str(member.get("status") or "").strip().lower()
                if status == "left":
                    memberships = member.get("memberships") or []
                    has_product_membership = False
                    
                    if isinstance(memberships, list) and memberships:
                        for mship in memberships:
                            product_obj = mship.get("product") or {}
                            if isinstance(product_obj, dict):
                                mship_product_id = str(product_obj.get("id") or "").strip()
                                if mship_product_id == main_product_id:
                                    mship_status = str(mship.get("status") or "").strip().lower()
                                    if mship_status in ["active", "trialing"]:
                                        has_product_membership = False
                                        break
                                    has_product_membership = True
                                    break
                    
                    if not has_product_membership:
                        if main_product_id and not memberships:
                            has_product_membership = True
                        else:
                            continue
                    
                    if has_product_membership:
                        special_members["left"].append(member)
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            break
    
    print(f"  Found {len(special_members['left'])} 'left', {len(special_members['churned'])} 'churned', {len(special_members['canceling'])} 'canceling'")
    print(f"  Excluded {excluded_from_special} special members (already have active memberships)")
    
    # Step 4: Simulate deduplication (like code does)
    print("\nStep 4: Simulating deduplication...")
    
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
    
    member_status_map = {}
    
    # Process regular memberships
    for mship in all_memberships:
        if not isinstance(mship, dict):
            continue
        
        base_status = str(mship.get("status") or "").strip().lower()
        if base_status == "canceled":
            continue
        
        if mship.get("cancel_at_period_end") is True and base_status in ["active", "trialing"]:
            status = "canceling"
        else:
            status = base_status
        
        user_obj = mship.get("user") or {}
        email = ""
        if isinstance(user_obj, dict):
            email = str(user_obj.get("email") or "").strip().lower()
        
        if not email:
            continue
        
        member_key = email
        
        existing = member_status_map.get(member_key)
        current_priority = get_status_priority(status)
        
        if existing:
            existing_priority = get_status_priority(existing.get("status", ""))
            if existing.get("status", "").lower() == "left" and current_priority < 10:
                member_status_map[member_key] = {"status": status}
            elif current_priority < existing_priority:
                member_status_map[member_key] = {"status": status}
        else:
            member_status_map[member_key] = {"status": status}
    
    # Process special members
    for status_type, members_list in special_members.items():
        for member in members_list:
            if not isinstance(member, dict):
                continue
            
            user_obj = member.get("user") or {}
            email = ""
            if isinstance(user_obj, dict):
                email = str(user_obj.get("email") or "").strip().lower()
            
            if not email:
                continue
            
            member_key = email
            status = status_type
            
            existing = member_status_map.get(member_key)
            current_priority = get_status_priority(status)
            
            if existing:
                existing_priority = get_status_priority(existing.get("status", ""))
                if existing.get("status", "").lower() == "left" and current_priority < 10:
                    member_status_map[member_key] = {"status": status}
                elif current_priority < existing_priority:
                    member_status_map[member_key] = {"status": status}
            else:
                member_status_map[member_key] = {"status": status}
    
    # Count final statuses
    final_counts = defaultdict(int)
    for email, data in member_status_map.items():
        final_counts[data["status"]] += 1
    
    # Step 5: Get status mapping from config
    status_mapping = config.get("status_tabs", {}).get("status_mapping", {})
    
    # Map final statuses to tabs
    tab_counts = defaultdict(int)
    for status, count in final_counts.items():
        tab_name = status_mapping.get(status, status.capitalize())
        tab_counts[tab_name] += count
    
    print(f"\nFinal deduplicated counts by status:")
    for status, count in sorted(final_counts.items(), key=lambda x: status_priority.get(x[0], 999)):
        print(f"  {status:15}: {count:4}")
    
    print(f"\nFinal counts by tab (after status mapping):")
    for tab_name, count in sorted(tab_counts.items()):
        print(f"  {tab_name:15}: {count:4}")
    
    # Step 6: Cross-check with renewing
    print("\nStep 6: Cross-checking with 'renewing' status...")
    
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
            break
    
    print(f"  Total 'renewing' members: {len(renewing_members)}")
    print(f"  Unique 'renewing' emails: {len(renewing_emails)}")
    
    # Get active tab members (active, past_due, completed, unresolved, drafted)
    active_tab_emails = set()
    for email, data in member_status_map.items():
        status = data["status"]
        if status in ["active", "past_due", "completed", "unresolved", "drafted"]:
            active_tab_emails.add(email)
    
    print(f"\n  Active tab members (after deduplication): {len(active_tab_emails)}")
    
    # Cross-check
    overlap = active_tab_emails & renewing_emails
    only_active = active_tab_emails - renewing_emails
    only_renewing = renewing_emails - active_tab_emails
    
    print(f"\n  Cross-check results:")
    print(f"    - Active tab members: {len(active_tab_emails)}")
    print(f"    - 'Renewing' members: {len(renewing_emails)}")
    print(f"    - Overlap (in both): {len(overlap)}")
    print(f"    - Only in Active tab (not renewing): {len(only_active)}")
    print(f"    - Only in renewing (not in Active tab): {len(only_renewing)}")
    
    # Step 7: Breakdown of Active tab
    print("\nStep 7: Breakdown of Active tab members by status:")
    active_tab_by_status = defaultdict(int)
    for email, data in member_status_map.items():
        status = data["status"]
        if status in ["active", "past_due", "completed", "unresolved", "drafted"]:
            active_tab_by_status[status] += 1
    
    for status, count in sorted(active_tab_by_status.items()):
        print(f"  {status:15}: {count:4}")
    
    # Step 8: Actual paying members (active status only, excluding past_due, completed, etc.)
    actual_paying = final_counts.get("active", 0)
    print(f"\nStep 8: Actual PAYING members (status='active' only):")
    print(f"  Active status only: {actual_paying}")
    print(f"  (Excludes: past_due, completed, unresolved, drafted)")
    
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"\nActive tab count (includes active, past_due, completed, unresolved, drafted): {len(active_tab_emails)}")
    print(f"Actual paying members (status='active' only): {actual_paying}")
    print(f"Terminal shows: 275")
    print(f"\nDifference: {275 - len(active_tab_emails)}")
    
    if len(overlap) > 0:
        print(f"\n'Renewing' cross-check:")
        print(f"  - {len(overlap)} active tab members are also 'renewing' (auto-renewing subscriptions)")
        print(f"  - {len(only_active)} active tab members are NOT 'renewing' (manual renewals or different billing)")

if __name__ == "__main__":
    asyncio.run(test_accurate_paying_count())
