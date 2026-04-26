#!/usr/bin/env python3
"""
Generic Product Checker - Real Chrome CDP (optional) + page extraction

Goal: work across many sites (Target/Walmart/HomeDepot/BestBuy/Costco/etc.)
by prioritizing what is commonly available on the page:
- title
- price (best-effort)
- main image (best-effort)
- brand (best-effort)
- structured data (JSON-LD Product / Offer when present)

This intentionally does NOT try to bypass geo/anti-bot systems.
"""

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


DEFAULT_CDP_URL = "http://127.0.0.1:9222"
OUTPUT = Path("generic_results")
OUTPUT.mkdir(exist_ok=True)

MAX_CAPTURED_BODIES = 30
MAX_CAPTURED_BODY_BYTES = 1_000_000


def _is_chrome_for_testing_bin(path: Path) -> bool:
    p = str(path).lower()
    if "chrome-for-testing" in p or "chrome_for_testing" in p:
        return True
    try:
        out = subprocess.check_output([str(path), "--version"], text=True, stderr=subprocess.STDOUT, timeout=5)
    except Exception:
        return False
    o = (out or "").lower()
    return ("chrome for testing" in o) or ("google chrome for testing" in o)


def _pick_linux_chrome_executable() -> Optional[str]:
    """
    On some hosts `/usr/bin/google-chrome` is actually **Google Chrome for Testing**.
    Prefer a stable install path when present.
    """
    candidates = [
        Path("/opt/google/chrome/google-chrome"),
        Path("/usr/bin/google-chrome-stable"),
        Path("/usr/bin/google-chrome"),
        Path("/usr/bin/chromium"),
        Path("/usr/bin/chromium-browser"),
    ]
    for c in candidates:
        try:
            if c.exists() and os.access(c, os.X_OK) and not _is_chrome_for_testing_bin(c):
                return str(c)
        except Exception:
            continue
    # Last resort: still try common names even if they look like testing builds.
    for c in candidates:
        try:
            if c.exists() and os.access(c, os.X_OK):
                return str(c)
        except Exception:
            continue
    return None


def clean(value: Any) -> str:
    if value is None:
        return "N/A"
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or "N/A"


def _strip_json_prefix(raw: str) -> str:
    s = raw.lstrip()
    for prefix in (")]}',", ")]}',", "while(1);", "for(;;);"):
        if s.startswith(prefix):
            return s[len(prefix) :].lstrip()
    return raw


def _safe_host(url: str) -> str:
    host = (urlparse(url).hostname or "unknown").lower()
    host = re.sub(r"[^a-z0-9._-]+", "_", host)
    return host or "unknown"


def _now_ts() -> int:
    return int(time.time())


def _first(*vals: Any) -> Optional[Any]:
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _looks_like_price(s: str) -> bool:
    if not s:
        return False
    # Common: "$249.99", "249.99", "₱1,234.56"
    return bool(re.search(r"(\$|₱|€|£)\s*\d|(\d[\d,]*\.\d{2})", s))


def _has_currency_symbol(s: str) -> bool:
    return bool(re.search(r"(\$|₱|€|£)", str(s or "")))


def _extract_price_candidates(text: str) -> List[str]:
    # Capture a few obvious price tokens; keep short to avoid noise.
    tokens = re.findall(r"(?:(?:\$|₱|€|£)\s*)?\d[\d,]*\.\d{2}", text)
    out: List[str] = []
    seen = set()
    for t in tokens:
        t = clean(t)
        if not t or t == "N/A":
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= 12:
            break
    return out


def _jsonld_product_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Returns a best-effort summary from JSON-LD Product graphs.
    """
    title = brand = image = sku = gtin = price = currency = availability = None

    def pick_offer(offers: Any) -> Optional[Dict[str, Any]]:
        if isinstance(offers, dict):
            return offers
        if isinstance(offers, list) and offers:
            # choose the first offer with a price-like field
            for o in offers:
                if isinstance(o, dict) and _first(o.get("price"), o.get("lowPrice"), o.get("highPrice")) is not None:
                    return o
            for o in offers:
                if isinstance(o, dict):
                    return o
        return None

    for obj in items:
        if not isinstance(obj, dict):
            continue
        typ = obj.get("@type")
        if isinstance(typ, list):
            typ = next((t for t in typ if isinstance(t, str)), None)
        if not isinstance(typ, str) or typ.lower() != "product":
            continue

        title = _first(title, obj.get("name"), obj.get("headline"))

        b = obj.get("brand")
        if isinstance(b, dict):
            brand = _first(brand, b.get("name"))
        elif isinstance(b, str):
            brand = _first(brand, b)

        img = obj.get("image")
        if isinstance(img, list) and img:
            image = _first(image, img[0])
        elif isinstance(img, str):
            image = _first(image, img)

        sku = _first(sku, obj.get("sku"), obj.get("mpn"))
        gtin = _first(gtin, obj.get("gtin13"), obj.get("gtin12"), obj.get("gtin14"), obj.get("gtin"))

        offer = pick_offer(obj.get("offers"))
        if offer:
            price = _first(price, offer.get("price"), offer.get("lowPrice"))
            currency = _first(currency, offer.get("priceCurrency"))
            availability = _first(availability, offer.get("availability"))

    return {
        "jsonld_title": clean(title),
        "jsonld_brand": clean(brand),
        "jsonld_image": clean(image),
        "jsonld_sku": clean(sku),
        "jsonld_gtin": clean(gtin),
        "jsonld_price": clean(price),
        "jsonld_currency": clean(currency),
        "jsonld_availability": clean(availability),
    }


async def _get_meta(page) -> Dict[str, str]:
    # Extract a small, stable set of meta tags.
    meta = {}
    candidates = [
        ("og:title", "property", "og:title"),
        ("og:image", "property", "og:image"),
        ("product:brand", "property", "product:brand"),
        ("twitter:title", "name", "twitter:title"),
        ("twitter:image", "name", "twitter:image"),
    ]
    for key, attr, val in candidates:
        try:
            loc = page.locator(f"meta[{attr}='{val}']").first
            if await loc.count() > 0:
                content = await loc.get_attribute("content", timeout=1500)
                meta[key] = clean(content)
        except Exception:
            pass
    return meta


async def _get_jsonld(page) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Returns (parsed_jsonld_objects, raw_jsonld_snippets).
    """
    parsed: List[Dict[str, Any]] = []
    raw_snips: List[str] = []
    try:
        scripts = await page.locator("script[type='application/ld+json']").all()
        for s in scripts[:40]:
            try:
                raw = await s.inner_text(timeout=1500)
                if not raw:
                    continue
                raw = raw.strip()
                if len(raw) > 250_000:
                    raw = raw[:250_000]
                raw_snips.append(raw)
                try:
                    data = json.loads(raw)
                    if isinstance(data, dict):
                        parsed.append(data)
                    elif isinstance(data, list):
                        parsed.extend([x for x in data if isinstance(x, dict)])
                except Exception:
                    continue
            except Exception:
                continue
    except Exception:
        pass
    return parsed, raw_snips


async def _first_text(page, selectors: List[str]) -> str:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                text = await loc.inner_text(timeout=2500)
                if clean(text) != "N/A":
                    return clean(text)
        except Exception:
            pass
    return "N/A"


async def _first_attr(page, selectors: List[str], attr: str) -> str:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                val = await loc.get_attribute(attr, timeout=2500)
                if clean(val) != "N/A":
                    return clean(val)
        except Exception:
            pass
    return "N/A"


async def check_url(
    url: str,
    *,
    connect_over_cdp: bool,
    cdp_url: str,
    headless: bool,
    manual_checkpoint: bool,
    chrome_exe: Optional[str],
    auto_wait_s: float,
) -> Dict[str, Any]:
    async with async_playwright() as p:
        browser = None
        if connect_over_cdp:
            browser = await p.chromium.connect_over_cdp(cdp_url)
        else:
            launch_kwargs: Dict[str, Any] = {"headless": headless}
            args: List[str] = []
            if chrome_exe:
                launch_kwargs["executable_path"] = chrome_exe
            if sys.platform.startswith("linux"):
                # Common Oracle/Ubuntu constraints.
                args.extend(["--no-sandbox", "--disable-dev-shm-usage"])
            if args:
                launch_kwargs["args"] = args
            browser = await p.chromium.launch(**launch_kwargs)

        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await context.new_page()

        captured: List[Any] = []
        captured_urls: List[str] = []
        captured_response_meta: List[Dict[str, Any]] = []

        async def on_response(response):
            try:
                u = response.url
                ctype = (response.headers.get("content-type", "") or "").lower()
                rtype = None
                try:
                    rtype = response.request.resource_type
                except Exception:
                    rtype = "unknown"

                # Always record metadata; bodies are best-effort and limited.
                captured_urls.append(f"{u} | status={response.status} | type={rtype} | ctype={ctype or 'N/A'}")
                captured_response_meta.append(
                    {"url": u, "status": response.status, "resource_type": rtype, "content_type": ctype or "N/A"}
                )

                if len(captured) >= MAX_CAPTURED_BODIES:
                    return

                if not ("json" in ctype or "graphql" in ctype or "application/" in ctype):
                    return

                data = None
                if "json" in ctype:
                    try:
                        data = await response.json()
                    except Exception:
                        data = None

                if data is None:
                    try:
                        body = await response.body()
                        if body and len(body) <= MAX_CAPTURED_BODY_BYTES:
                            text = body.decode("utf-8", errors="ignore")
                            text = _strip_json_prefix(text)
                            data = json.loads(text)
                    except Exception:
                        data = None

                if data is not None:
                    captured.append({"_meta": {"url": u, "status": response.status, "resource_type": rtype, "content_type": ctype or "N/A"}, "data": data})
            except Exception:
                pass

        page.on("response", on_response)

        print(f"\nOpening: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeoutError:
            pass

        if manual_checkpoint:
            print("\nManual checkpoint:")
            print("1. Fix any modal/captcha manually if present.")
            print("2. Scroll a bit so price/images load.")
            input("Press ENTER here when ready to extract...")
            await page.wait_for_timeout(2000)
        else:
            # Headless/server-friendly: do a small scroll + wait to trigger lazy loads.
            try:
                await page.wait_for_timeout(int(max(0.0, auto_wait_s) * 1000))
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(750)
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(750)
            except Exception:
                pass

        # Extract visible text snapshot
        body_text = "N/A"
        try:
            body_text = await page.locator("body").inner_text(timeout=15000)
            body_text = body_text or "N/A"
        except Exception:
            body_text = "N/A"

        # Meta + JSON-LD
        meta = await _get_meta(page)
        jsonld_objs, jsonld_raw = await _get_jsonld(page)
        jsonld_summary = _jsonld_product_summary(jsonld_objs)

        # Page DOM fields (generic)
        title_dom = await _first_text(page, ["h1", "[itemprop='name']", "[data-test*='title']"])
        image_dom = await _first_attr(page, ["img[itemprop='image']", "img[alt][src]"], "src")

        # Price: prefer JSON-LD, then meta candidates, then visible text tokens.
        price_candidates = _extract_price_candidates(body_text if isinstance(body_text, str) else "")
        # Prefer currency-tagged candidates to avoid false positives (ratings, counts, etc.)
        price_candidates_sorted = sorted(
            price_candidates,
            key=lambda x: (0 if _has_currency_symbol(x) else 1),
        )
        price_best = _first(
            None if jsonld_summary["jsonld_price"] == "N/A" else jsonld_summary["jsonld_price"],
            next((p for p in price_candidates_sorted if _looks_like_price(p)), None),
        )

        result = {
            "url": url,
            "host": _safe_host(url),
            "page_title": clean(await page.title()),
            "title": clean(_first(None if title_dom == "N/A" else title_dom, meta.get("og:title"), meta.get("twitter:title"), jsonld_summary.get("jsonld_title"))),
            "price": clean(price_best),
            "price_candidates": price_candidates[:12] or ["N/A"],
            "image": clean(_first(meta.get("og:image"), meta.get("twitter:image"), None if image_dom == "N/A" else image_dom, jsonld_summary.get("jsonld_image"))),
            "brand": clean(_first(meta.get("product:brand"), jsonld_summary.get("jsonld_brand"))),
            "jsonld": jsonld_summary,
            "captured_json_payload_count": len(captured),
            "captured_url_count": len(captured_urls),
            "captured_responses_meta": captured_response_meta[:500],
        }

        ts = _now_ts()
        out_dir = OUTPUT / result["host"]
        out_dir.mkdir(parents=True, exist_ok=True)
        prefix = out_dir / f"product_{ts}"

        (Path(str(prefix) + ".json")).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        (Path(str(prefix) + "_captured_urls.txt")).write_text("\n".join(captured_urls), encoding="utf-8")
        (Path(str(prefix) + "_visible_text.txt")).write_text(str(body_text), encoding="utf-8", errors="ignore")
        (Path(str(prefix) + "_raw_payloads.json")).write_text(json.dumps(captured, indent=2, ensure_ascii=False), encoding="utf-8")
        (Path(str(prefix) + "_jsonld_raw.json")).write_text(json.dumps(jsonld_raw[:40], indent=2, ensure_ascii=False), encoding="utf-8")

        try:
            await page.screenshot(path=str(prefix) + ".png", full_page=True)
            result["screenshot"] = str(prefix) + ".png"
        except Exception:
            result["screenshot"] = "N/A"

        await page.close()
        if not connect_over_cdp:
            await browser.close()
        return result


def print_result(r: Dict[str, Any]) -> None:
    print("\n" + "=" * 78)
    print(f"URL: {r.get('url')}")
    print(f"Title: {r.get('title')}")
    print(f"Price: {r.get('price')}")
    print(f"Brand: {r.get('brand')}")
    print(f"Image: {r.get('image')}")
    print(f"Captured JSON Payloads: {r.get('captured_json_payload_count')}")
    print("=" * 78)


async def main_async() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", help="Product URL to open.")
    ap.add_argument("--url-file", help="Text file containing product URLs (one per line).")
    ap.add_argument("--connect-cdp", action="store_true", help="Attach to an existing real Chrome (CDP).")
    ap.add_argument("--cdp-url", default=DEFAULT_CDP_URL, help="CDP URL (default: http://127.0.0.1:9222).")
    ap.add_argument("--headless", action="store_true", help="Launch headless Chrome (only when not using --connect-cdp).")
    ap.add_argument("--manual", action="store_true", help="Pause for manual fixes/scroll before extraction.")
    ap.add_argument("--chrome-exe", help="Path to system Chrome (e.g. /usr/bin/google-chrome). Used when NOT using --connect-cdp.")
    ap.add_argument("--auto-wait-s", type=float, default=3.0, help="Extra wait (seconds) before extraction when not using --manual.")
    args = ap.parse_args()

    urls: List[str] = []
    if args.url:
        urls.append(args.url.strip())
    if args.url_file:
        p = Path(args.url_file)
        raw = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in raw:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)

    urls = [u for u in urls if u]
    if not urls:
        raise SystemExit("Provide --url or --url-file")

    # Default chrome exe on Linux if present and user didn't specify.
    chrome_exe = args.chrome_exe
    if not args.connect_cdp and not chrome_exe and sys.platform.startswith("linux"):
        chrome_exe = _pick_linux_chrome_executable()

    batch_results: List[Dict[str, Any]] = []
    for u in urls:
        try:
            r = await check_url(
                u,
                connect_over_cdp=args.connect_cdp,
                cdp_url=args.cdp_url,
                headless=args.headless,
                manual_checkpoint=args.manual,
                chrome_exe=chrome_exe,
                auto_wait_s=args.auto_wait_s,
            )
            print_result(r)
            batch_results.append(r)
        except Exception as e:
            print(f"Error: {u} -> {e}")

    if len(batch_results) > 1:
        ts = _now_ts()
        out_dir = OUTPUT / "batches"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"batch_{ts}.json").write_text(json.dumps(batch_results, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

