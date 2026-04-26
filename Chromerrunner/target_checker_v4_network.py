#!/usr/bin/env python3
"""
Target Checker V4 - Real Chrome CDP + Network JSON Capture

This version attaches to your locally opened Chrome through CDP, then captures
Target network JSON responses while the product page loads.

Goal:
- Product title/link/price/image
- Visible promos/deals
- Visible stock text
- Target network JSON payloads
- Store availability/status
- Quantity if Target exposes quantity in JSON
- Total quantity when quantity is exposed

Important:
Target often does NOT expose exact stock count for all items/stores.
If only status is available, the script prints N/A for quantity.
"""

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

CDP_URL = "http://127.0.0.1:9222"
OUTPUT = Path("target_results_v4")
OUTPUT.mkdir(exist_ok=True)

TARGET_URL = "https://www.target.com/p/-/A-{tcin}"
MAX_CAPTURED_BODIES = 50
MAX_CAPTURED_BODY_BYTES = 1_500_000  # keep files manageable


def _strip_json_prefix(raw: str) -> str:
    """
    Some endpoints prepend anti-XSSI guards (e.g. )]}',) before JSON.
    """
    s = raw.lstrip()
    for prefix in (")]}',", ")]}',", "while(1);", "for(;;);"):
        if s.startswith(prefix):
            return s[len(prefix) :].lstrip()
    return raw


def clean(value: Any) -> str:
    if value is None:
        return "N/A"
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or "N/A"


def walk(obj: Any, path: str = ""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            yield p, v
            yield from walk(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{path}[{i}]"
            yield p, v
            yield from walk(v, p)


def first_nonempty(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def possible_qty(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and 0 <= value <= 10000:
        return value
    if isinstance(value, float) and value.is_integer() and 0 <= value <= 10000:
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        n = int(value.strip())
        if 0 <= n <= 10000:
            return n
    return None


def find_price_lines(text: str) -> List[str]:
    lines = [clean(x) for x in text.splitlines() if clean(x) != "N/A"]
    return [x for x in lines if "$" in x and len(x) <= 100][:15]


def find_promo_lines(text: str) -> List[str]:
    terms = ["save", "off", "deal", "target circle", "circle", "gift card", "weekly ad", "clearance", "sale", "coupon", "buy", "spend"]
    out, seen = [], set()
    for raw in text.splitlines():
        line = clean(raw)
        low = line.lower()
        if line != "N/A" and any(t in low for t in terms) and len(line) <= 200 and line not in seen:
            out.append(line)
            seen.add(line)
    return out[:20]


def find_stock_lines(text: str) -> List[str]:
    terms = ["in stock", "limited stock", "out of stock", "not available", "available", "pickup", "delivery", "shipping", "ship it", "only", "left", "aisle", "ready within", "not sold"]
    out, seen = [], set()
    for raw in text.splitlines():
        line = clean(raw)
        low = line.lower()
        if line != "N/A" and any(t in low for t in terms) and len(line) <= 220 and line not in seen:
            out.append(line)
            seen.add(line)
    return out[:40]


def extract_product_from_payloads(payloads: List[Any], tcin: str) -> Dict[str, str]:
    title = price = image = upc = brand = None

    for payload in payloads:
        if not isinstance(payload, (dict, list)):
            continue
        for path, node in walk(payload):
            if not isinstance(node, dict):
                continue

            values = {str(v) for v in node.values() if isinstance(v, (str, int, float))}
            if tcin not in values and str(node.get("tcin", "")) != tcin:
                continue

            title = first_nonempty(title, node.get("title"), node.get("product_title"), node.get("item_title"))
            price = first_nonempty(price, node.get("current_retail"), node.get("current_price"), node.get("formatted_current_price"), node.get("price"))
            image = first_nonempty(image, node.get("primary_image_url"), node.get("image_url"), node.get("base_url"))
            upc = first_nonempty(upc, node.get("upc"), node.get("barcode"))
            brand = first_nonempty(brand, node.get("brand"), node.get("brand_name"))

    return {
        "network_title": clean(title),
        "network_price": clean(price),
        "network_image": clean(image),
        "network_upc": clean(upc),
        "network_brand": clean(brand),
    }


def extract_inventory_from_payloads(payloads: List[Any]) -> Tuple[List[Dict[str, Any]], str]:
    """
    Generic parser for Target inventory-ish JSON.

    We look for dict nodes with:
    - location/store identifier
    - availability/status
    - quantity-like keys
    """
    qty_key_terms = [
        "quantity", "qty", "available_to_promise_quantity", "available_quantity",
        "on_hand", "onhand", "inventory_count", "stock_level", "stock"
    ]
    status_key_terms = [
        "availability_status", "availability", "inventory_status", "purchase_status",
        "status", "fulfillment_status"
    ]
    store_key_terms = [
        "store_id", "location_id", "storeid", "locationid", "fulfillment_store_id",
        "store_name", "location_name", "storename"
    ]

    rows = {}
    total = 0
    found_qty = False

    for payload in payloads:
        if not isinstance(payload, (dict, list)):
            continue
        for path, node in walk(payload):
            if not isinstance(node, dict):
                continue

            keys = {str(k).lower(): k for k in node.keys()}

            store_id = first_nonempty(
                node.get("store_id"), node.get("location_id"), node.get("storeId"),
                node.get("locationId"), node.get("fulfillment_store_id"),
                node.get("fulfillmentStoreId")
            )
            store_name = first_nonempty(
                node.get("store_name"), node.get("location_name"), node.get("storeName"),
                node.get("name")
            )

            status = None
            for lk, orig in keys.items():
                if any(term == lk or term in lk for term in status_key_terms):
                    status = first_nonempty(status, node.get(orig))

            qty_int = None
            qty_source = None
            for lk, orig in keys.items():
                if any(term == lk or term in lk for term in qty_key_terms):
                    n = possible_qty(node.get(orig))
                    if n is not None:
                        qty_int = n if qty_int is None else max(qty_int, n)
                        qty_source = orig

            # Also catch nested common inventory formats
            if isinstance(node.get("inventory"), dict):
                inv = node["inventory"]
                for k, v in inv.items():
                    n = possible_qty(v)
                    if n is not None and any(term in str(k).lower() for term in qty_key_terms):
                        qty_int = n if qty_int is None else max(qty_int, n)
                        qty_source = f"inventory.{k}"

            has_storeish_key = any(any(term in lk for term in store_key_terms) for lk in keys)
            has_status = status not in (None, "", [], {})
            has_qty = qty_int is not None

            if not (has_storeish_key or has_status or has_qty):
                continue

            # Avoid super generic product nodes unless inventory-ish
            if not (has_status or has_qty or store_id or store_name):
                continue

            key = clean(first_nonempty(store_id, store_name, path))
            current = rows.get(key, {
                "store_id": clean(store_id),
                "store_name": clean(store_name),
                "status": clean(status),
                "quantity": "N/A",
                "quantity_source": "N/A",
                "json_path": path,
            })

            if current["store_id"] == "N/A" and store_id:
                current["store_id"] = clean(store_id)
            if current["store_name"] == "N/A" and store_name:
                current["store_name"] = clean(store_name)
            if current["status"] == "N/A" and status:
                current["status"] = clean(status)

            if qty_int is not None:
                old = current["quantity"]
                if old == "N/A" or qty_int > int(old):
                    current["quantity"] = str(qty_int)
                    current["quantity_source"] = clean(qty_source)
                found_qty = True

            rows[key] = current

    out = list(rows.values())

    # de-dupe useless rows
    filtered = []
    seen = set()
    for r in out:
        sig = (r["store_id"], r["store_name"], r["status"], r["quantity"])
        if sig not in seen:
            filtered.append(r)
            seen.add(sig)

    for r in filtered:
        if str(r["quantity"]).isdigit():
            total += int(r["quantity"])

    filtered.sort(key=lambda r: (r["quantity"] == "N/A", r["store_name"] == "N/A", r["store_id"] == "N/A"))
    return filtered[:100], str(total) if found_qty else "N/A"


async def first_text(page, selectors: List[str]) -> str:
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


async def first_attr(page, selectors: List[str], attr: str) -> str:
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


async def check_tcin(browser, tcin: str) -> Dict[str, Any]:
    url = TARGET_URL.format(tcin=tcin)
    context = browser.contexts[0] if browser.contexts else await browser.new_context()
    page = await context.new_page()

    captured = []
    captured_urls: List[str] = []
    captured_response_meta: List[Dict[str, Any]] = []

    async def on_response(response):
        try:
            u = response.url
            lu = u.lower()
            ctype = response.headers.get("content-type", "").lower()
            rtype = None
            try:
                rtype = response.request.resource_type
            except Exception:
                rtype = "unknown"

            interesting = (
                "target.com" in lu and (
                    "redsky" in lu or
                    "fulfillment" in lu or
                    "inventory" in lu or
                    "pdp" in lu or
                    "product" in lu or
                    "tcin" in lu or
                    "pricing" in lu or
                    "json" in ctype
                )
            )

            if not interesting:
                return

            captured_urls.append(f"{u} | status={response.status} | type={rtype} | ctype={ctype or 'N/A'}")
            captured_response_meta.append(
                {
                    "url": u,
                    "status": response.status,
                    "resource_type": rtype,
                    "content_type": ctype or "N/A",
                }
            )

            if len(captured) >= MAX_CAPTURED_BODIES:
                return

            # Prefer JSON, but fall back to tolerant parsing for "JSON-ish" bodies.
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
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

    try:
        await page.wait_for_load_state("networkidle", timeout=12000)
    except PlaywrightTimeoutError:
        pass

    print("\nManual checkpoint:")
    print("1. Fix any Target modal/error manually.")
    print("2. Make sure ZIP/store is set.")
    print("3. Scroll down until Pickup / Delivery / Shipping cards fully load.")
    print("4. Wait 5-10 seconds after scrolling.")
    input("Press ENTER here when ready to extract...")

    # Try to trigger fulfillment cards (these often lazy-load network calls).
    # This is intentionally "best effort" and should not break if selectors change.
    for label in ("Pickup", "Delivery", "Shipping"):
        try:
            await page.get_by_role("button", name=label, exact=True).click(timeout=1200)
            await page.wait_for_timeout(800)
        except Exception:
            pass

    # Wait a little after manual scroll to collect late network calls
    await page.wait_for_timeout(3000)

    body_text = ""
    try:
        body_text = await page.locator("body").inner_text(timeout=15000)
    except Exception:
        pass

    # Capture embedded JSON scripts too
    for sel in ["script#__NEXT_DATA__", "script[type='application/ld+json']"]:
        try:
            handles = await page.locator(sel).all()
            for h in handles:
                raw = await h.inner_text(timeout=3000)
                if raw and (tcin in raw or "Product" in raw or "inventory" in raw.lower()):
                    try:
                        captured.append({"_meta": {"url": f"embedded:{sel}", "status": "N/A", "resource_type": "embedded", "content_type": "application/json"}, "data": json.loads(raw)})
                    except Exception:
                        captured.append({"_meta": {"url": f"embedded:{sel}", "status": "N/A", "resource_type": "embedded", "content_type": "text/plain"}, "_raw_script_text": raw[:250000]})
        except Exception:
            pass

    title = await first_text(page, ["h1", '[data-test="product-title"]', '[data-test="@web/ProductDetailPage/ProductTitle"]'])
    visible_price = await first_text(page, ['[data-test="product-price"]', '[data-test="current-price"]', '[data-test="@web/Price/Price"]'])
    image = await first_attr(page, ['img[src*="target.scene7"]', 'img[alt][src]'], "src")

    payload_datas = []
    for p in captured:
        if isinstance(p, dict) and "data" in p and "_meta" in p:
            payload_datas.append(p["data"])
        else:
            payload_datas.append(p)

    network_product = extract_product_from_payloads(payload_datas, tcin)
    inventory_rows, total_network_stock = extract_inventory_from_payloads(payload_datas)

    result = {
        "tcin": tcin,
        "title": title if title != "N/A" else network_product["network_title"],
        "link": url,
        "visible_price": visible_price,
        "network_product": network_product,
        "image": image if image != "N/A" else network_product["network_image"],
        "price_lines_found": find_price_lines(body_text) or ["N/A"],
        "promos_deals": find_promo_lines(body_text) or ["N/A"],
        "stock_availability_lines": find_stock_lines(body_text) or ["N/A"],
        "total_network_stock": total_network_stock,
        "inventory_rows": inventory_rows if inventory_rows else [],
        "captured_json_payload_count": len(captured),
        "captured_url_count": len(captured_urls),
        "captured_responses_meta": captured_response_meta[:500],
    }

    ts = int(time.time())
    prefix = OUTPUT / f"target_{tcin}_{ts}"

    (Path(str(prefix) + ".json")).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    (Path(str(prefix) + "_captured_urls.txt")).write_text("\n".join(captured_urls), encoding="utf-8")
    (Path(str(prefix) + "_visible_text.txt")).write_text(body_text, encoding="utf-8", errors="ignore")
    (Path(str(prefix) + "_raw_payloads.json")).write_text(json.dumps(captured, indent=2, ensure_ascii=False), encoding="utf-8")

    try:
        await page.screenshot(path=str(prefix) + ".png", full_page=True)
        result["screenshot"] = str(prefix) + ".png"
    except Exception:
        result["screenshot"] = "N/A"

    await page.close()
    return result


def print_result(r: Dict[str, Any]):
    print("\n" + "=" * 78)
    print(f"TCIN: {r['tcin']}")
    print(f"Title: {r['title']}")
    print(f"Link: {r['link']}")
    print(f"Visible Price: {r['visible_price']}")
    print(f"Image: {r['image']}")
    print(f"Captured JSON Payloads: {r['captured_json_payload_count']}")
    print(f"Total Network Stock: {r['total_network_stock']}")

    print("\nPrice Lines:")
    for x in r["price_lines_found"]:
        print(f"- {x}")

    print("\nPromos / Deals:")
    for x in r["promos_deals"]:
        print(f"- {x}")

    print("\nStock / Availability Lines:")
    for x in r["stock_availability_lines"]:
        print(f"- {x}")

    print("\nInventory Rows:")
    if r["inventory_rows"]:
        for row in r["inventory_rows"][:30]:
            print(
                f"- Store: {row['store_name']} | ID: {row['store_id']} | "
                f"Status: {row['status']} | Qty: {row['quantity']} | Source: {row['quantity_source']}"
            )
    else:
        print("- N/A")

    print("=" * 78)


async def main():
    print("TARGET CHECKER V4 - REAL CHROME CDP + NETWORK JSON")
    print("---------------------------------------------------")
    print("Start Chrome first with start_chrome_target.bat.\n")

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print("Could not connect to Chrome on port 9222.")
            print("Run start_chrome_target.bat first.")
            print(f"Error: {e}")
            return

        while True:
            raw = input("\nEnter TCIN(s), txt file, or q: ").strip()
            if raw.lower() in {"q", "quit", "exit"}:
                break

            path = Path(raw)
            if path.exists() and path.is_file():
                tcins = re.findall(r"\b\d{6,12}\b", path.read_text(encoding="utf-8", errors="ignore"))
            else:
                tcins = re.findall(r"\b\d{6,12}\b", raw)

            if not tcins:
                print("No TCIN found.")
                continue

            batch = []
            for tcin in tcins:
                try:
                    res = await check_tcin(browser, tcin)
                    print_result(res)
                    batch.append(res)
                except Exception as e:
                    print(f"Error checking {tcin}: {e}")

            if batch:
                (OUTPUT / "last_batch_results.json").write_text(json.dumps(batch, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"\nSaved batch results to: {OUTPUT / 'last_batch_results.json'}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
