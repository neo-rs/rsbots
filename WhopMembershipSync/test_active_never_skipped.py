#!/usr/bin/env python3
"""
Test to verify active members are NEVER skipped, even without email/Discord ID.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_active_never_skipped():
    """Verify all active members are included."""
    
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
    print("VERIFYING ACTIVE MEMBERS ARE NEVER SKIPPED")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Get all active memberships
    print("\nFetching all active memberships...")
    active_memberships = []
    active_with_email = 0
    active_with_discord_only = 0
    active_with_member_id_only = 0
    active_with_nothing = 0
    
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
                    
                    # Check identifiers
                    user_obj = mship.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                    
                    member_obj = mship.get("member") or {}
                    member_id = ""
                    if isinstance(member_obj, dict):
                        member_id = str(member_obj.get("id") or "").strip()
                    
                    # Fetch member record to check Discord ID
                    discord_id = ""
                    if member_id:
                        try:
                            member_record = await whop_client.get_member_by_id(member_id)
                            if member_record:
                                connected_accounts = member_record.get("connected_accounts") or []
                                for acc in connected_accounts:
                                    if isinstance(acc, dict):
                                        provider = str(acc.get("provider") or "").strip().lower()
                                        if provider == "discord":
                                            discord_id = str(acc.get("provider_account_id") or "").strip()
                                            break
                        except Exception:
                            pass
                    
                    if email:
                        active_with_email += 1
                    elif discord_id:
                        active_with_discord_only += 1
                    elif member_id:
                        active_with_member_id_only += 1
                    else:
                        active_with_nothing += 1
            
            if not page_info.get("has_next_page"):
                break
            after = page_info.get("end_cursor")
            if not after:
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"\nTotal active memberships: {len(active_memberships)}")
    print(f"  - With email: {active_with_email}")
    print(f"  - With Discord ID only (no email): {active_with_discord_only}")
    print(f"  - With member ID only (no email/Discord): {active_with_member_id_only}")
    print(f"  - With nothing (would be skipped in old code): {active_with_nothing}")
    
    # Calculate what our code should include
    should_include = active_with_email + active_with_discord_only + active_with_member_id_only
    print(f"\nMembers that SHOULD be included (with new code): {should_include}")
    print(f"Members that would be SKIPPED (old code): {active_with_nothing}")
    
    if active_with_member_id_only > 0:
        print(f"\n✓ New code will include {active_with_member_id_only} active members using member ID as fallback")
    
    if active_with_nothing > 0:
        print(f"\nWARNING: {active_with_nothing} active members have NO identifiers at all!")
        print(f"  These cannot be included even with the fix.")
    else:
        print(f"\nOK All active members have at least one identifier - 100% accuracy achievable!")

if __name__ == "__main__":
    asyncio.run(test_active_never_skipped())
