"""
Standalone scraper test for Instorebotforwarder.

Goal:
- Run the exact same Amazon scraping code the bot uses (no Discord login)
- Print what we can extract for given URLs (title, image, current/before, discount notes, department)
- Do NOT rewrite / affiliate-tag / shorten the input URLs (we print them raw)

Usage:
  py -3 scripts/test_instore_amazon_scraper.py
  py -3 scripts/test_instore_amazon_scraper.py https://www.amazon.com/dp/B0F5HZQZLQ https://www.amazon.com/dp/B0G87FZZQQ
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def _load_cfg() -> Dict[str, Any]:
    """
    Load the same merged config the bot uses (config.json + config.secrets.json).
    This keeps scrape behavior consistent with runtime settings (UA, playwright flags, zip, etc).
    """
    from mirror_world_config import load_config_with_secrets

    cfg, _cfg_path, _sec_path = load_config_with_secrets(REPO_ROOT / "Instorebotforwarder")
    return cfg or {}


def _make_scraper(cfg: Dict[str, Any]):
    """
    Create an InstorebotForwarder instance WITHOUT running __init__ (no Discord, no lock).
    We only need the scraping methods.
    """
    from Instorebotforwarder.instore_auto_mirror_bot import InstorebotForwarder

    obj = InstorebotForwarder.__new__(InstorebotForwarder)
    obj.config = cfg
    obj._amazon_scrape_cache = {}
    obj._amazon_scrape_cache_ts = {}
    return obj


async def _scrape_one(scraper, url: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    return await scraper._scrape_amazon_page(url)  # noqa: SLF001


def _print_result(url: str, data: Optional[Dict[str, str]], err: Optional[str]) -> None:
    print("=" * 88)
    print(f"URL: {url}")
    if err:
        print(f"ERR: {err}")
    if not data:
        print("DATA: <none>")
        return
    # Pretty stable key order
    out = {
        "title": data.get("title", ""),
        "department": data.get("department", ""),
        "price": data.get("price", ""),
        "before_price": data.get("before_price", ""),
        "discount_notes": data.get("discount_notes", ""),
        "image_url": data.get("image_url", ""),
    }
    print(json.dumps(out, indent=2, ensure_ascii=True))


async def main(argv: list[str]) -> int:
    urls = [a.strip() for a in argv[1:] if a.strip()]
    if not urls:
        urls = [
            "https://www.amazon.com/dp/B0F5HZQZLQ",
            "https://www.amazon.com/dp/B0G87FZZQQ",
        ]

    cfg = _load_cfg()
    scraper = _make_scraper(cfg)

    started = time.time()
    for u in urls:
        try:
            data, err = await _scrape_one(scraper, u)
        except Exception as e:
            data, err = None, f"exception: {e}"
        _print_result(u, data, err)

    print("=" * 88)
    print(f"Done in {time.time() - started:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv)))

