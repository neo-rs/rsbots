"""
Test StockX product scrape: fetch a product page, parse __NEXT_DATA__, print title/SKU/market/size table.
Run until the extracted data looks correct; same logic is used when forwarding to neo-test-server with enrich_stockx.

Usage:
  py -3 scripts/test_stockx_scraper.py
  py -3 scripts/test_stockx_scraper.py "https://stockx.com/nike-mercurial-superfly-8-fg-blueprint-pack-chlorine-blue"
  py -3 scripts/test_stockx_scraper.py "https://stockx.com/nike-air-max-90-se-running-club"
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
    """Minimal InstorebotForwarder with StockX helpers (no Discord, no lock)."""
    from Instorebotforwarder.instore_auto_mirror_bot import InstorebotForwarder
    obj = InstorebotForwarder.__new__(InstorebotForwarder)
    obj.config = cfg
    return obj


async def main():
    p = argparse.ArgumentParser(description="Test StockX product fetch and embed data")
    p.add_argument(
        "url",
        nargs="?",
        default="https://stockx.com/nike-air-max-90-se-running-club",
        help="StockX product URL",
    )
    p.add_argument("--save-html", action="store_true", help="Save fetched HTML to stockx_sample.html")
    p.add_argument("--playwright-only", action="store_true", help="Skip aiohttp; use Playwright only and save HTML")
    args = p.parse_args()

    cfg = _load_cfg()
    forwarder = _make_forwarder(cfg)

    url = (args.url or "").strip()
    if not url.startswith("http"):
        url = "https://stockx.com/" + url.lstrip("/")

    print("Fetching:", url)
    save_html = getattr(args, "save_html", False)
    playwright_only = getattr(args, "playwright_only", False)
    if playwright_only:
        import asyncio
        html = await asyncio.to_thread(forwarder._fetch_stockx_page_playwright_sync, url)
        out_path = REPO_ROOT / "stockx_sample.html"
        if html:
            out_path.write_text(html, encoding="utf-8", errors="replace")
            print("Saved Playwright HTML to", out_path, "(%s chars)" % len(html))
            print("Has __NEXT_DATA__:", "__NEXT_DATA__" in html)
        else:
            print("Playwright returned no HTML")
        if not html:
            return
        # Parse from saved HTML
        from bs4 import BeautifulSoup
        import json
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        raw = script.string if script and script.string else None
        if raw:
            data = json.loads(raw)
            product = forwarder._find_product_in_next_data(data, url)
            print("Product found:", product is not None)
            if product:
                print("Title:", product.get("title"))
                print("Variants:", len(product.get("variants") or []))
        return
    if save_html:
        import aiohttp
        headers = {"User-Agent": (cfg.get("amazon_scrape_user_agent") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async with session.get(url, headers=headers) as resp:
                html = await resp.text(errors="ignore")
        out_path = REPO_ROOT / "stockx_sample.html"
        out_path.write_text(html, encoding="utf-8", errors="replace")
        print("Saved HTML to", out_path, "(%s chars)" % len(html))
        print("Has __NEXT_DATA__:", "__NEXT_DATA__" in html)
    try:
        data = await forwarder._fetch_stockx_product(url)
    except Exception as e:
        print("Exception during fetch:", e)
        data = None
    if not data:
        print("No product data extracted (check __NEXT_DATA__ or URL).")
        if not save_html:
            print("Run with --save-html to save the page and inspect.")
        return

    print("\n--- Extracted product ---")
    print("Title:", data.get("title"))
    print("SKU:", data.get("sku"))
    print("Lowest Ask:", data.get("lowest_ask"))
    print("Highest Bid:", data.get("highest_bid"))
    print("Image URL:", (data.get("image_url") or "")[:80])
    print("Variants count:", len(data.get("variants") or []))
    if data.get("variants"):
        print("\nFirst 5 rows (Size | Sale | L Ask | H Bid):")
        for v in (data["variants"])[:5]:
            print(" ", v.get("size"), "|", v.get("sale"), "|", v.get("lowest_ask"), "|", v.get("highest_bid"))

    embed = forwarder._stockx_product_to_embed(data)
    if embed:
        print("\nEmbed built: title=%r fields=%s" % (embed.title, len(embed.fields)))
    else:
        print("\nEmbed not built (data missing?).")


if __name__ == "__main__":
    asyncio.run(main())
