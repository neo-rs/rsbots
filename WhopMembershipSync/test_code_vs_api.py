#!/usr/bin/env python3
"""
Test comparing our actual code logic vs direct API calls for each status.
This simulates the exact flow our code uses.
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_code_vs_api():
    """Test our code's exact logic vs direct API calls."""
    
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
    print("CODE LOGIC vs DIRECT API COMPARISON")
    print("=" * 80)
    print(f"Product ID: {product_id}")
    print()
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Step 1: Fetch all memberships from /memberships endpoint (like our code does)
    print("Step 1: Fetching memberships from /memberships endpoint...")
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
                        "product_ids": [product_id],
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
    
    # Step 2: Build active member set (like our code does)
    print("\nStep 2: Building active member set...")
    active_member_emails = set()
    active_member_ids = set()
    
    for mship in all_memberships:
        if not isinstance(mship, dict):
            continue
        
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
    
    # Step 3: Fetch special status members (like our code does)
    print("\nStep 3: Fetching special status members from /members endpoint...")
    special_members = {"left": [], "churned": [], "canceling": []}
    excluded_from_special = 0
    
    # Fetch canceling and churned
    for action_type in ["canceling", "churned"]:
        action_after = None
        for page in range(100):
            try:
                params = {"product_ids": [product_id]} if product_id else {}
                params["most_recent_actions[]"] = [action_type]
                
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=action_after,
                    params=params
                )
                
                for member in batch:
                    if not isinstance(member, dict):
                        continue
                    
                    member_id = str(member.get("id") or "").strip()
                    if not member_id:
                        continue
                    
                    # Check if already active (like our code does)
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
                                if mship_product_id == product_id:
                                    mship_status = str(mship.get("status") or "").strip().lower()
                                    if mship_status in ["active", "trialing"]:
                                        has_product_membership = False
                                        break
                                    has_product_membership = True
                                    break
                    
                    if not has_product_membership:
                        if product_id and not memberships:
                            has_product_membership = True
                        else:
                            continue
                    
                    if not has_product_membership:
                        continue
                    
                    special_members[action_type].append(member)
                
                if not page_info.get("has_next_page"):
                    break
                action_after = page_info.get("end_cursor")
                if not action_after:
                    break
            except Exception as e:
                print(f"  ERROR fetching {action_type}: {e}")
                break
    
    # Fetch left members
    special_after = None
    for page in range(100):
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=special_after,
                params={"product_ids": [product_id]} if product_id else {}
            )
            
            for member in batch:
                if not isinstance(member, dict):
                    continue
                
                member_id = str(member.get("id") or "").strip()
                if not member_id:
                    continue
                
                # Check if already active (like our code does)
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
                                if mship_product_id == product_id:
                                    mship_status = str(mship.get("status") or "").strip().lower()
                                    if mship_status in ["active", "trialing"]:
                                        has_product_membership = False
                                        break
                                    has_product_membership = True
                                    break
                    
                    if not has_product_membership:
                        if product_id and not memberships:
                            has_product_membership = True
                        else:
                            continue
                    
                    if has_product_membership:
                        special_members["left"].append(member)
            
            if not page_info.get("has_next_page"):
                break
            special_after = page_info.get("end_cursor")
            if not special_after:
                break
        except Exception as e:
            print(f"  ERROR fetching left: {e}")
            break
    
    print(f"  Found {len(special_members['left'])} 'left', {len(special_members['churned'])} 'churned', {len(special_members['canceling'])} 'canceling'")
    print(f"  Excluded {excluded_from_special} special members (already have active memberships)")
    
    # Step 4: Process memberships and deduplicate (like our code does)
    print("\nStep 4: Processing and deduplicating...")
    
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
    skipped_no_id = 0
    
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
            skipped_no_id += 1
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
                skipped_no_id += 1
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
    
    print(f"  Skipped {skipped_no_id} memberships (no email/Discord ID)")
    
    # Step 5: Compare with direct API calls
    print("\n" + "=" * 80)
    print("COMPARISON: CODE LOGIC vs DIRECT API")
    print("=" * 80)
    
    # Direct API counts
    direct_counts = {}
    
    # Active
    active_count = 0
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_memberships(
                first=100,
                after=after,
                params={"product_ids": [product_id], "statuses[]": ["active"]}
            )
            active_count += len(batch)
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    direct_counts["active"] = active_count
    
    # Canceling
    canceling_count = 0
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [product_id], "most_recent_actions[]": ["canceling"]}
            )
            canceling_count += len(batch)
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    direct_counts["canceling"] = canceling_count
    
    # Churned
    churned_count = 0
    after = None
    while True:
        try:
            batch, page_info = await whop_client.list_members(
                first=100,
                after=after,
                params={"product_ids": [product_id], "most_recent_actions[]": ["churned"]}
            )
            churned_count += len(batch)
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    direct_counts["churned"] = churned_count
    
    # Left
    left_count = 0
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
                        left_count += 1
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception:
            break
    direct_counts["left"] = left_count
    
    print("\nStatus-by-Status Comparison:")
    print("-" * 80)
    
    for status in ["active", "canceling", "churned", "left", "trialing", "expired", "completed", "past_due"]:
        direct = direct_counts.get(status, 0)
        code = final_counts.get(status, 0)
        diff = code - direct
        
        if status == "active":
            # For active, direct count might include some that become canceling
            # So we need to account for that
            pass
        
        status_symbol = "OK" if abs(diff) <= 5 else "DIFF"
        print(f"  {status:12} | Direct API: {direct:5} | Code Logic: {code:5} | Diff: {diff:+5} {status_symbol}")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total members processed: {len(member_status_map)}")
    print(f"Skipped (no ID): {skipped_no_id}")
    print(f"Excluded from special (already active): {excluded_from_special}")

if __name__ == "__main__":
    asyncio.run(test_code_vs_api())
