"""
Test eBay sold-comps: build eBay link from product title (same as sheet/Instorebotforwarder)
and optionally scrape sold price ranges for New, Pre-owned, Refurbished.

Usage:
  py -3 scripts/test_instore_ebay_scraper.py
  py -3 scripts/test_instore_ebay_scraper.py "Red Dead Redemption 2 - Xbox One [Digital Code]"
  py -3 scripts/test_instore_ebay_scraper.py "Red Dead Redemption 2 - Xbox One [Digital Code]" --scrape
"""

from __future__ import annotations

import asyncio
import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def _load_cfg():
    from mirror_world_config import load_config_with_secrets
    cfg, _, _ = load_config_with_secrets(REPO_ROOT / "Instorebotforwarder")
    return cfg or {}


def _make_forwarder(cfg):
    """Minimal InstorebotForwarder-like object with eBay helpers (no Discord, no lock)."""
    from Instorebotforwarder.instore_auto_mirror_bot import InstorebotForwarder
    obj = InstorebotForwarder.__new__(InstorebotForwarder)
    obj.config = cfg
    obj._amazon_scrape_cache = {}
    obj._amazon_scrape_cache_ts = {}
    obj._ebay_sold_cache = {}
    obj._ebay_sold_cache_ts = {}
    return obj


async def main():
    p = argparse.ArgumentParser(description="Test eBay link build + optional scrape from product title")
    p.add_argument("title", nargs="?", default="Red Dead Redemption 2 - Xbox One [Digital Code]",
                   help="Product title (e.g. from Amazon listing)")
    p.add_argument("--scrape", action="store_true", help="Fetch eBay sold pages and extract price ranges")
    p.add_argument("--save-html", action="store_true", help="Save first fetched HTML to ebay_sold_sample.html for debugging")
    args = p.parse_args()

    cfg = _load_cfg()
    # Ensure eBay scrape can run (timeout etc. from config)
    fwd = _make_forwarder(cfg)

    title = (args.title or "").strip()
    if not title:
        print("No title provided.")
        return 1

    keyword = fwd._ebay_keyword_from_title(title)
    url_all = fwd._ebay_sold_search_url(keyword)
    url_new = fwd._ebay_sold_search_url(keyword, condition_id=1000)
    url_preowned = fwd._ebay_sold_search_url(keyword, condition_id=3000)
    url_refurb = fwd._ebay_sold_search_url(keyword, condition_id=2000)

    print("=" * 72)
    print("Title:", title)
    print("eBay keyword (sanitized):", keyword)
    print()
    print("eBay sold search URLs (same logic as sheet):")
    print("  All conditions: ", url_all)
    print("  New (1000):     ", url_new)
    print("  Pre-owned (3000):", url_preowned)
    print("  Refurbished (2000):", url_refurb)
    print("=" * 72)

    if not args.scrape:
        print("Run with --scrape to fetch pages and extract sold price ranges.")
        return 0

    if args.save_html:
        # Fetch one page and save HTML for debugging price extraction
        url = fwd._ebay_sold_search_url(keyword, condition_id=1000)
        html, err = await fwd._fetch_ebay_sold_page(url)
        out_path = REPO_ROOT / "ebay_sold_sample.html"
        if err:
            print("Save HTML failed:", err)
        elif html:
            out_path.write_text(html, encoding="utf-8", errors="replace")
            print(f"Saved {len(html)} chars to {out_path}")
        else:
            print("No HTML to save.")

    print("Scraping sold listings (New, Pre-owned, Refurbished)...")
    ranges, err = await fwd._scrape_ebay_sold_ranges(title)
    if err:
        print("Scrape error:", err)
        return 1
    if not ranges:
        print("No ranges returned.")
        return 1
    print()
    print("Sold price ranges:")
    print(json.dumps(ranges, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
