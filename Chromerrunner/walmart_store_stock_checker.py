#!/usr/bin/env python3
"""
Walmart in-store stock via real Chrome CDP (same pattern as generic_product_checker / Instore).

Uses the browser session (cookies) to POST terra-firma/fetch from page context — avoids
datacenter IP blocks that plain requests hit on Oracle.

Usage (on Oracle with CDP Chrome running on :9222):
  python3 Chromerrunner/walmart_store_stock_checker.py --item-id 834343104 --store-id 1158 --connect-cdp
  python3 Chromerrunner/walmart_store_stock_checker.py --item-id 834343104 --stores 1158,2265,2070 --connect-cdp
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

DEFAULT_CDP_URL = "http://127.0.0.1:9222"

TERRAFIRM_JS = """
async ({ itemId, storeId }) => {
  const payload = {
    itemId: String(itemId),
    paginationContext: { selected: false },
    storeFrontIds: [{ usStoreId: parseInt(storeId, 10), preferred: false, semStore: false }],
  };
  const url = "https://www.walmart.com/terra-firma/fetch?rgs=OFFER_PRODUCT,OFFER_INVENTORY,OFFER_PRICE,VARIANT_SUMMARY";
  const referer = `https://www.walmart.com/product/${itemId}/sellers`;
  const r = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "accept": "*/*",
      "referer": referer,
      "pragma": "no-cache",
      "cache-control": "no-cache",
    },
    body: JSON.stringify(payload),
    credentials: "include",
  });
  const text = await r.text();
  return { status: r.status, contentType: r.headers.get("content-type") || "", text: text.slice(0, 8000) };
}
"""


def parse_terrafirm(text: str) -> dict[str, Any] | None:
    if not text or text.strip().startswith("{"):
        try:
            outer = json.loads(text)
        except json.JSONDecodeError:
            return None
        if outer.get("appId") or outer.get("blockScript"):
            return {"_blocked": True}
        data = outer
    else:
        return None
    payload = data.get("payload") or {}
    offers = payload.get("offers") or {}
    if not offers:
        return None
    offer = next(iter(offers.values()))
    if not isinstance(offer, dict):
        return None
    pickup = ((offer.get("fulfillment") or {}).get("pickupOptions") or [{}])[0]
    price = ((offer.get("pricesInfo") or {}).get("priceMap") or {}).get("CURRENT", {}).get("price")
    products = payload.get("products") or {}
    title = ""
    if products:
        p = next(iter(products.values()))
        title = ((p.get("productAttributes") or {}).get("productName") or "")[:120]
    return {
        "title": title,
        "availability": pickup.get("availability"),
        "quantity": pickup.get("inStoreStockStatus"),
        "in_store_price": price,
        "store_id": pickup.get("storeId"),
        "store_name": pickup.get("storeName"),
        "store_city": pickup.get("storeCity"),
    }


def parse_online_price(html: str) -> str:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return ""
    try:
        data = json.loads(m.group(1))
        prod = (
            ((data.get("props") or {}).get("pageProps") or {})
            .get("initialData", {})
            .get("data", {})
            .get("product", {})
        )
        price = ((prod.get("priceInfo") or {}).get("currentPrice") or {}).get("price")
        return f"${float(price):.2f}" if price is not None else ""
    except Exception:
        return ""


async def _pick_walmart_page(context, item_id: str) -> tuple[Any, str]:
    """Prefer the noVNC-warmed tab (store cookies) over opening a fresh tab."""
    product_path = f"/ip/-/{item_id}"
    for page in context.pages:
        url = page.url or ""
        if "walmart.com" in url and product_path in url:
            return page, "existing_product_tab"
    for page in context.pages:
        if "walmart.com" in (page.url or ""):
            return page, "existing_walmart_tab"
    page = await context.new_page()
    return page, "new_tab"


async def check_stores(
    item_id: str,
    store_ids: list[str],
    *,
    connect_cdp: bool,
    cdp_url: str,
    warmup: bool = True,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    product_url = f"https://www.walmart.com/ip/-/{item_id}"

    async with async_playwright() as p:
        if connect_cdp:
            browser = await p.chromium.connect_over_cdp(cdp_url)
        else:
            browser = await p.chromium.launch(headless=False, args=["--no-sandbox", "--disable-dev-shm-usage"])

        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page, page_source = await _pick_walmart_page(context, item_id)
        try:
            await page.bring_to_front()
        except Exception:
            pass
        print(f"Using CDP page: {page_source} url={(page.url or '')[:90]}", file=sys.stderr)

        online_price = ""
        if page_source == "existing_product_tab":
            html = await page.content()
            online_price = parse_online_price(html)
            print(f"  reusing warmed product tab online_price={online_price or '?'}", file=sys.stderr)
        elif warmup:
            print(f"Warmup: {product_url}", file=sys.stderr)
            resp = await page.goto(product_url, wait_until="domcontentloaded", timeout=90000)
            html = await page.content()
            online_price = parse_online_price(html)
            blocked = len(html) < 50000 and "__NEXT_DATA__" not in html
            print(f"  page status={resp.status if resp else '?'} online_price={online_price or '?'} blocked={blocked}", file=sys.stderr)
            await page.wait_for_timeout(2000)

        for sid in store_ids:
            raw = await page.evaluate(TERRAFIRM_JS, {"itemId": item_id, "storeId": sid})
            status = int(raw.get("status") or 0)
            text = str(raw.get("text") or "")
            row: dict[str, Any] = {
                "store_id": sid,
                "http_status": status,
                "online_price": online_price,
                "item_id": item_id,
            }
            if status == 200 and "appId" not in text[:300]:
                parsed = parse_terrafirm(text)
                if parsed:
                    row.update(parsed)
                    row["ok"] = True
                else:
                    row["ok"] = False
                    row["error"] = "no offers in terra-firma payload"
            else:
                row["ok"] = False
                row["error"] = (
                    "blocked"
                    if ("appId" in text or "ttp-marker" in text or status in (412, 403, 404))
                    else text[:200]
                )
            results.append(row)
            avail = row.get("availability", "")
            qty = row.get("quantity", "")
            price = row.get("in_store_price", "")
            print(
                f"store {sid}: ok={row.get('ok')} avail={avail} qty={qty} "
                f"in_store={price} online={online_price or '?'}",
                file=sys.stderr,
            )

        if not connect_cdp:
            await browser.close()

    return results


def main() -> int:
    ap = argparse.ArgumentParser(description="Walmart store stock via CDP browser terra-firma.")
    ap.add_argument("--item-id", required=True)
    ap.add_argument("--store-id", action="append", default=[])
    ap.add_argument("--stores", default="", help="Comma-separated store IDs")
    ap.add_argument("--connect-cdp", action="store_true")
    ap.add_argument("--cdp-url", default=DEFAULT_CDP_URL)
    ap.add_argument("--json", action="store_true", help="Print JSON rows to stdout")
    args = ap.parse_args()

    store_ids = [str(x).strip() for x in args.store_id if str(x).strip()]
    if args.stores:
        store_ids.extend([s.strip() for s in args.stores.split(",") if s.strip()])
    if not store_ids:
        ap.error("Provide --store-id and/or --stores")

    rows = asyncio.run(
        check_stores(
            args.item_id.strip(),
            store_ids,
            connect_cdp=args.connect_cdp,
            cdp_url=args.cdp_url,
        )
    )
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for r in rows:
            print(r)
    ok = any(r.get("ok") for r in rows)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
