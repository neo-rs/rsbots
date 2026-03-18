#!/usr/bin/env python3
"""
Test to see exactly what statuses are included in the "Active" tab (275 count).
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

async def test_active_tab_breakdown():
    """Test what statuses map to 'Active' tab."""
    
    config_file = Path(__file__).parent / "config.secrets.json"
    if not config_file.exists():
        print("ERROR: config.secrets.json not found")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        secrets = json.load(f)
    
    # Load config.json for status mapping
    config_json = Path(__file__).parent / "config.json"
    with open(config_json, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    api_key = secrets.get("whop_api", {}).get("api_key", "").strip()
    company_id = "biz_s58kr1WWnL1bzH"
    product_id = "prod_RrcvGelB8tVgu"
    
    if not api_key:
        print("ERROR: Missing API key")
        return
    
    print("=" * 80)
    print("ACTIVE TAB BREAKDOWN ANALYSIS")
    print("=" * 80)
    
    # Get status mapping from config
    status_mapping = config.get("status_tabs", {}).get("status_mapping", {})
    print(f"\nStatus mapping from config.json:")
    for api_status, tab_name in status_mapping.items():
        print(f"  {api_status:15} -> {tab_name}")
    
    # Find which statuses map to "Active"
    statuses_to_active = [s for s, tab in status_mapping.items() if tab == "Active"]
    print(f"\nStatuses that map to 'Active' tab: {statuses_to_active}")
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # Fetch all memberships for each status that maps to "Active"
    print("\n" + "=" * 80)
    print("Fetching memberships by status...")
    print("=" * 80)
    
    status_counts = {}
    all_memberships_by_status = defaultdict(list)
    
    for status_filter in statuses_to_active:
        print(f"\nFetching {status_filter}...")
        count = 0
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
                        count += 1
                        all_memberships_by_status[status_filter].append(mship)
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                print(f"  ERROR: {e}")
                break
        
        status_counts[status_filter] = count
        print(f"  {status_filter:15}: {count:4} memberships")
    
    # Also check for canceling (active/trialing with cancel_at_period_end=true)
    print(f"\nChecking for 'canceling' (active/trialing with cancel_at_period_end=true)...")
    canceling_count = 0
    
    for status_filter in ["active", "trialing"]:
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
                        if mship.get("cancel_at_period_end") is True:
                            canceling_count += 1
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                break
    
    print(f"  canceling:       {canceling_count:4} memberships")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    total_active_tab = sum(status_counts.values())
    print(f"\nTotal memberships that would go to 'Active' tab: {total_active_tab}")
    print(f"\nBreakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status:15}: {count:4}")
    
    print(f"\nExpected 'Active' tab count: {total_active_tab}")
    print(f"Actual 'Active' tab count from terminal: 275")
    print(f"Difference: {275 - total_active_tab}")
    
    # Check if canceling members are being excluded from Active
    print(f"\nNote: 'Canceling' members ({canceling_count}) go to 'Canceling' tab, not 'Active'")
    print(f"So if canceling members were previously in 'Active', they're now correctly separated.")
    
    # Check for deduplication impact
    print(f"\nAfter deduplication (each member appears only once):")
    print(f"  The actual count may differ if members have multiple statuses")
    print(f"  (e.g., a member with both 'active' and 'past_due' would only appear once)")

if __name__ == "__main__":
    asyncio.run(test_active_tab_breakdown())
