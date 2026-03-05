#!/usr/bin/env python3
"""
Try Walmart in-store stock by UPC + zip using mobile-style endpoints.

There is no public API from "Reselling Secrets Monitors" – that bot is Discord-only.
This script attempts known Walmart mobile/search endpoints; they may require auth
or be blocked. If they work, you get store stock without Discord.

Usage:
  python walmart_mobile_api_lookup.py --upc 050946872926 --zip 35058
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("Install: pip install requests", file=sys.stderr)
    sys.exit(1)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def stores_near_zip(zip_code: str, session: requests.Session) -> list[dict]:
    """Try to get store IDs near a zip. Returns list of {store_id, name, ...}."""
    # Walmart store locator (may return HTML or require auth; mobile host often doesn't resolve)
    url = "https://www.walmart.com/store/directory/search"
    params = {"q": zip_code}
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = session.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            return []
        ct = r.headers.get("content-type") or ""
        if "application/json" not in ct:
            return []
        data = r.json()
        stores = data.get("payload", {}).get("stores") or data.get("stores") or []
        if isinstance(stores, list):
            return stores[:20]
        return []
    except requests.exceptions.RequestException:
        return []
    except Exception:
        return []


def product_at_store(upc: str, store_id: str, session: requests.Session) -> dict | None:
    """Try mobile product-by-code endpoint for UPC at a store (host may not resolve publicly)."""
    url = f"https://search.mobile.walmart.com/v1/products-by-code/UPC/{upc}"
    params = {"storeId": store_id}
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = session.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        if "application/json" not in (r.headers.get("content-type") or ""):
            return None
        return r.json()
    except requests.exceptions.RequestException:
        return None
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Try Walmart in-store stock by UPC + zip (mobile API).")
    ap.add_argument("--upc", required=True, help="UPC (e.g. 050946872926)")
    ap.add_argument("--zip", required=True, help="ZIP code (e.g. 35058)")
    ap.add_argument("--verbose", "-v", action="store_true", help="Print raw responses")
    args = ap.parse_args()

    upc = str(args.upc).strip()
    zip_code = str(args.zip).strip()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    print("Reselling Secrets Monitors has no public API; this script tries Walmart endpoints.", file=sys.stderr)
    print("Fetching stores near zip...", file=sys.stderr)

    stores = stores_near_zip(zip_code, session)
    if not stores:
        print("Could not get stores for that zip (endpoint may require auth, be blocked, or return HTML).", file=sys.stderr)
        print("Walmart mobile host search.mobile.walmart.com often does not resolve from outside their network.", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(json.dumps(stores[:2], indent=2), file=sys.stderr)

    # Get store IDs (field name may vary)
    store_ids = []
    for s in stores:
        sid = s.get("id") or s.get("storeId") or s.get("storeNumber")
        if sid:
            store_ids.append(str(sid))

    if not store_ids:
        print("No store IDs in response.", file=sys.stderr)
        sys.exit(1)

    print(f"Checking UPC {upc} at {len(store_ids)} store(s)...", file=sys.stderr)
    for store_id in store_ids[:10]:
        out = product_at_store(upc, store_id, session)
        if out and args.verbose:
            print(json.dumps(out, indent=2)[:1500], file=sys.stderr)
        if out:
            print(f"Store {store_id}: {json.dumps(out)[:200]}")

    if not store_ids:
        print("No results. Mobile API may be blocked or require authentication.", file=sys.stderr)


if __name__ == "__main__":
    main()
