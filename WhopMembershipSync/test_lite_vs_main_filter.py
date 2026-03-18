#!/usr/bin/env python3
"""
Test: Reselling Secrets Lite filtering - API vs our code.
Verifies that Lite tab contains ONLY Lite product memberships and identifies
members who have BOTH products.
"""

import asyncio
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError

MAIN_PRODUCT_ID = "prod_RrcvGelB8tVgu"   # Reselling Secrets
LITE_PRODUCT_ID = "prod_U52ytqRZdCFak"   # Reselling Secrets Lite


async def fetch_all_memberships_for_product(whop_client, product_id: str, product_label: str):
    """Fetch all memberships for one product (same as our code)."""
    all_memberships = []
    statuses = ["trialing", "active", "past_due", "completed", "expired", "unresolved", "drafted"]
    
    for status_filter in statuses:
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
                print(f"  ERROR {product_label} {status_filter}: {e}")
                break
    
    return all_memberships


async def main():
    config_file = Path(__file__).parent / "config.secrets.json"
    if not config_file.exists():
        print("ERROR: config.secrets.json not found")
        return
    
    with open(config_file, "r", encoding="utf-8") as f:
        secrets = json.load(f)
    
    api_key = (secrets.get("whop_api") or {}).get("api_key", "").strip()
    company_id = "biz_s58kr1WWnL1bzH"
    if not api_key:
        print("ERROR: Missing API key")
        return
    
    print("=" * 80)
    print("RESELLING SECRETS LITE FILTER TEST")
    print("API vs our code - who has Lite only vs who has BOTH products")
    print("=" * 80)
    
    whop_client = WhopAPIClient(api_key=api_key, company_id=company_id)
    
    # 1) Fetch memberships for MAIN product only (same as our code: product_ids=[main])
    print("\nStep 1: Fetching memberships for MAIN product only (product_ids=[prod_RrcvGelB8tVgu])...")
    main_memberships = await fetch_all_memberships_for_product(
        whop_client, MAIN_PRODUCT_ID, "Main"
    )
    print(f"  Total memberships returned for MAIN: {len(main_memberships)}")
    
    # 2) Fetch memberships for LITE product only (same as our code: product_ids=[lite])
    print("\nStep 2: Fetching memberships for LITE product only (product_ids=[prod_U52ytqRZdCFak])...")
    lite_memberships = await fetch_all_memberships_for_product(
        whop_client, LITE_PRODUCT_ID, "Lite"
    )
    print(f"  Total memberships returned for LITE: {len(lite_memberships)}")
    
    # 3) Verify each membership's product.id matches the requested product (API contract)
    print("\nStep 3: Verifying API response - every membership must have product.id = requested product...")
    main_bad = []
    lite_bad = []
    for m in main_memberships:
        pid = (m.get("product") or {}).get("id") if isinstance(m.get("product"), dict) else None
        pid = str(pid or "").strip()
        if pid != MAIN_PRODUCT_ID:
            main_bad.append((m.get("id"), pid))
    for m in lite_memberships:
        pid = (m.get("product") or {}).get("id") if isinstance(m.get("product"), dict) else None
        pid = str(pid or "").strip()
        if pid != LITE_PRODUCT_ID:
            lite_bad.append((m.get("id"), pid))
    
    if main_bad:
        print(f"  WARNING: {len(main_bad)} MAIN memberships have product.id != Main product!")
        for mid, pid in main_bad[:5]:
            print(f"    membership_id={mid} product.id={pid}")
    else:
        print("  OK: All MAIN memberships have product.id = Main product")
    
    if lite_bad:
        print(f"  WARNING: {len(lite_bad)} LITE memberships have product.id != Lite product!")
        for mid, pid in lite_bad[:5]:
            print(f"    membership_id={mid} product.id={pid}")
    else:
        print("  OK: All LITE memberships have product.id = Lite product")
    
    # 4) Build email sets per product (one email can have multiple memberships in same product? No - one per product. But same email can have Main AND Lite)
    def emails_from_memberships(memberships):
        out = set()
        for m in memberships:
            u = m.get("user") or {}
            if isinstance(u, dict):
                e = str(u.get("email") or "").strip().lower()
                if e:
                    out.add(e)
        return out
    
    main_emails = emails_from_memberships(main_memberships)
    lite_emails = emails_from_memberships(lite_memberships)
    overlap = main_emails & lite_emails
    main_only = main_emails - lite_emails
    lite_only = lite_emails - main_emails
    
    print("\nStep 4: Overlap analysis (by email)...")
    print(f"  Emails with MAIN product: {len(main_emails)}")
    print(f"  Emails with LITE product: {len(lite_emails)}")
    print(f"  Emails with BOTH products: {len(overlap)}")
    print(f"  Emails with MAIN only: {len(main_only)}")
    print(f"  Emails with LITE only: {len(lite_only)}")
    
    # 5) How our code behaves
    print("\nStep 5: How our code behaves...")
    print("  When we sync 'Whop API - Reselling Secrets': we call API with product_ids=[prod_RrcvGelB8tVgu] only.")
    print("  When we sync 'Whop API - Reselling Secrets Lite': we call API with product_ids=[prod_U52ytqRZdCFak] only.")
    print("  So each tab gets ONLY memberships for that product. The API filters by product_id.")
    print("  Therefore:")
    print(f"    - Lite tab should contain exactly {len(lite_memberships)} memberships (Lite product only).")
    print(f"    - Of those, {len(overlap)} emails also have the Main product (they have BOTH).")
    print(f"    - {len(lite_only)} emails have LITE only (no Main).")
    if overlap:
        print(f"\n  Sample emails that have BOTH products (will appear in BOTH tabs):")
        for email in list(overlap)[:10]:
            print(f"    - {email}")
    
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print("  - API filtering: product_ids=[product_id] returns only memberships for that product.")
    print("  - Our code uses the same: one product_id per sync. No mixing.")
    print("  - If you see the same person in both tabs, they have two memberships (Main + Lite).")
    print("  - To show only 'Lite-only' members in the Lite tab we would need to exclude")
    print("    anyone who also has a Main membership (extra filter in code).")


if __name__ == "__main__":
    asyncio.run(main())
