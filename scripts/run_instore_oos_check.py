#!/usr/bin/env python3
"""
Run the live Instorebotforwarder OOS logic on ASINs that were previously
logging SKIP_OOS, so you can see the expected output: skip vs show card.
Uses the same _extract_amazon_availability_from_html as instore_auto_mirror_bot.py.
"""
import asyncio
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ASINs from your logs that were SKIP_OOS
ASINS = ("B0DRHVDQ8X", "B0DZDYQ1B1", "B0B95M4WYX", "B00JL3PW7I")

TIMEOUT_S = 15
MAX_BYTES = 600_000
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def extract_availability_from_html(html_txt: str) -> str:
    """Same logic as Instorebotforwarder._extract_amazon_availability_from_html (live)."""
    t = html_txt or ""
    if not t:
        return ""
    try:
        m = re.search(r'id=["\']availability["\'][\s\S]{0,1200}?</', t, re.IGNORECASE)
        snippet = (m.group(0) or "").lower() if m else ""
    except Exception:
        snippet = ""
    if not snippet:
        return ""
    strong = (
        "currently unavailable",
        "temporarily out of stock",
        "we don't know when or if this item will be back in stock",
        "out of stock",
    )
    if any(s in snippet for s in strong):
        return "out_of_stock"
    return ""


async def fetch_and_check(asin: str) -> tuple[str, str, str]:
    """Fetch Amazon dp page, run availability extraction. Returns (fetch_status, availability, action)."""
    url = f"https://www.amazon.com/dp/{asin}"
    try:
        import aiohttp
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)
        ) as session:
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                status = resp.status
                if status >= 400:
                    return f"HTTP {status}", "", "—"
                buf = bytearray()
                async for chunk in resp.content.iter_chunked(16_384):
                    buf.extend(chunk)
                    if len(buf) >= MAX_BYTES:
                        break
                html = buf.decode("utf-8", errors="replace")
    except Exception as e:
        return f"fetch failed: {e!s}"[:60], "", "—"

    avail = extract_availability_from_html(html)
    if avail in ("oos", "out_of_stock", "unavailable"):
        action = "SKIP_OOS (would not forward)"
    else:
        action = "SHOW CARD (would forward)"
    return "OK", avail or "(empty = in stock / unknown)", action


async def main() -> int:
    print("Using live OOS logic from Instorebotforwarder (availability block only, non-greedy regex).\n")
    print(f"{'ASIN':<14} {'Fetch':<22} {'Availability':<28} Output")
    print("-" * 85)
    for asin in ASINS:
        fetch_status, availability, action = await fetch_and_check(asin)
        print(f"{asin:<14} {fetch_status:<22} {availability:<28} {action}")
    print("-" * 85)
    print("\nIf Fetch is OK and Availability is empty -> SHOW CARD. If out_of_stock -> SKIP_OOS.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
