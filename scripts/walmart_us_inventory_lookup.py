#!/usr/bin/env python3
"""
US Walmart inventory lookup by UPC and zip code.

Primary: Brickseek.com Walmart Inventory Checker (in-store; US).
  - With --no-browser we use requests; install cloudscraper to try bypassing Cloudflare 403:
    pip install cloudscraper
  - Default uses a real browser (undetected-chromedriver) so Brickseek does not block.

Optional: Walmart Grocery API (pickup/delivery availability by zip + item_id).
  - Use --walmart-grocery and --item-id for grocery product availability near a zip.
  - Item ID is from Walmart.com product page (e.g. walmart.com/ip/Product-Name/123456789).

Usage:
  python walmart_us_inventory_lookup.py --upc 050946872926 --zip 90210
  python walmart_us_inventory_lookup.py --upc 050946872926 --zip 35058 72501 --no-browser
  python walmart_us_inventory_lookup.py --zip 35058 --walmart-grocery --item-id 123456789
  python walmart_us_inventory_lookup.py --upc 050946872926 --zip 90210 --no-browser --cookies cookies.txt
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Install: pip install requests beautifulsoup4", file=sys.stderr)
    sys.exit(1)

# Optional: for --browser (default)
def _have_browser():
    try:
        import undetected_chromedriver as uc  # noqa: F401
        return True
    except ImportError:
        return False


def _have_cloudscraper():
    try:
        import cloudscraper  # noqa: F401
        return True
    except ImportError:
        return False


BRICKSEEK_URL = "https://brickseek.com/walmart-inventory-checker/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _session(cookie_path: Path | None = None, use_cloudscraper: bool = True) -> requests.Session:
    """Session for Brickseek. Uses cloudscraper if installed to try bypassing Cloudflare 403."""
    if use_cloudscraper and _have_cloudscraper():
        import cloudscraper
        s = cloudscraper.create_scraper()
    else:
        s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BRICKSEEK_URL,
        "Origin": "https://brickseek.com",
    })
    if cookie_path and cookie_path.exists():
        s.cookies.update(_load_cookies(cookie_path))
    return s


def _load_cookies(path: Path) -> dict[str, str]:
    """Load name=value cookies from a Netscape-style or plain name=value file."""
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7:
            out[parts[5]] = parts[6]
        elif "=" in line:
            name, _, val = line.partition("=")
            out[name.strip()] = val.strip()
    return out


def lookup_via_browser(upc: str, zip_codes: list[str], headless: bool = False, debug_path: Path | None = None) -> list[dict]:
    """Use a real browser (undetected-chromedriver) to query Brickseek and return store results."""
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

    upc = str(upc).strip()
    all_rows: list[dict] = []
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    # Match installed Chrome major version to avoid SessionNotCreatedException
    try:
        driver = uc.Chrome(options=options, version_main=145)
    except Exception as e:
        msg = str(e)
        if "Current browser version is" in msg:
            import re
            m = re.search(r"Current browser version is (\d+)", msg)
            if m:
                driver = uc.Chrome(options=options, version_main=int(m.group(1)))
            else:
                raise
        else:
            raise
    try:
        driver.get(BRICKSEEK_URL)
        wait = WebDriverWait(driver, 45)
        # Brickseek uses Cloudflare; wait until challenge passes (headless often fails)
        try:
            wait.until(lambda d: "Just a moment" not in (d.title or ""))
        except TimeoutException:
            if "Just a moment" in (driver.title or ""):
                print("Cloudflare challenge did not pass (common in headless). Run without --headless so a visible browser opens.", file=sys.stderr)
        time.sleep(3)
        # If still on challenge page, skip form fill
        if "Just a moment" in (driver.title or ""):
            return all_rows
        for zip_code in zip_codes:
            zip_code = str(zip_code).strip()
            if not zip_code:
                continue
            try:
                # Wait for form (Brickseek has input name=upc)
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='upc']")))
                except TimeoutException:
                    pass
                time.sleep(2)
                # UPC radio if present (value=upc)
                for el in driver.find_elements(By.CSS_SELECTOR, "input[type='radio'][name='type']"):
                    if el.get_attribute("value") == "upc":
                        try:
                            el.click()
                        except ElementClickInterceptedException:
                            driver.execute_script("arguments[0].click();", el)
                        break
                time.sleep(0.5)
                # Fill UPC
                product_input = driver.find_elements(By.CSS_SELECTOR, "input[name='upc']")
                if product_input and product_input[0].is_displayed():
                    product_input[0].clear()
                    product_input[0].send_keys(upc)
                # Zip input (may appear after UPC on some flows)
                zip_input = None
                for sel in ["input[name='zip']", "input[name='zipCode']", "input[placeholder*='ip']"]:
                    for e in driver.find_elements(By.CSS_SELECTOR, sel):
                        if e.is_displayed():
                            zip_input = e
                            break
                    if zip_input:
                        break
                if not zip_input:
                    for el in driver.find_elements(By.TAG_NAME, "input"):
                        if el.is_displayed() and (el.get_attribute("name") or "") != "upc" and (el.get_attribute("type") or "text") == "text":
                            zip_input = el
                            break
                if zip_input:
                    zip_input.clear()
                    zip_input.send_keys(zip_code)
                # Submit
                for el in driver.find_elements(By.XPATH, "//button[contains(., 'Check')] | //button[contains(., 'Search')] | //input[@type='submit']"):
                    if el.is_displayed():
                        try:
                            el.click()
                            break
                        except ElementClickInterceptedException:
                            driver.execute_script("arguments[0].click();", el)
                            break
                # Wait for results (or zip step)
                for _ in range(15):
                    time.sleep(1)
                    ps = driver.page_source
                    if "In Stock" in ps or "Out of Stock" in ps or "Limited Stock" in ps or "No results" in ps or "Enter your zip" in ps:
                        break
                    # If zip field appeared, fill and submit again
                    z2 = driver.find_elements(By.CSS_SELECTOR, "input[name='zip'], input[name='zipCode'], input[placeholder*='ip']")
                    if z2 and z2[0].is_displayed() and not z2[0].get_attribute("value"):
                        z2[0].clear()
                        z2[0].send_keys(zip_code)
                        for b in driver.find_elements(By.XPATH, "//button[contains(., 'Check')] | //button[contains(., 'Search')] | //input[@type='submit']"):
                            if b.is_displayed():
                                try:
                                    b.click()
                                    break
                                except Exception:
                                    pass
                        time.sleep(5)
                        break
                time.sleep(2)
                html = driver.page_source
                if debug_path:
                    debug_path.write_text(html, encoding="utf-8", errors="replace")
                    print(f"Debug: saved page HTML to {debug_path}", file=sys.stderr)
                rows = _parse_brickseek_results(html)
                for r in rows:
                    r["zip_searched"] = zip_code
                    all_rows.append(r)
            except Exception as e:
                print(f"Warning: zip {zip_code}: {e}", file=sys.stderr)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return all_rows


def lookup_upc_zip(upc: str, zip_code: str, session: requests.Session | None = None) -> list[dict]:
    """
    Query Brickseek Walmart checker by UPC and zip. Returns list of store results.
    Each item: store_name, city_or_address, availability, quantity, price (optional).
    """
    upc = str(upc).strip()
    zip_code = str(zip_code).strip()
    if not upc or not zip_code:
        return []

    sess = session or _session()
    # First request may trigger Cloudflare challenge (cloudscraper handles it)
    sess.get(BRICKSEEK_URL, timeout=25)
    payload = {
        "search_method": "upc",
        "upc": upc,
        "zip": zip_code,
        "sort": "distance",
    }
    resp = sess.post(BRICKSEEK_URL, data=payload, timeout=25)
    if resp.status_code == 403:
        raise RuntimeError(
            "Brickseek returned 403 (blocked). Try: pip install cloudscraper, or use --browser, "
            "or copy cookies from a browser session (--cookies file)."
        )
    resp.raise_for_status()
    return _parse_brickseek_results(resp.text)


def _parse_brickseek_results(html: str) -> list[dict]:
    """Parse Brickseek results table into list of dicts (store, city/address, availability, quantity, price)."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    # Try __NEXT_DATA__ JSON first (Next.js app)
    try:
        script = soup.find("script", id=re.compile(r"__NEXT_DATA__", re.I))
        if script and script.string:
            import json
            data = json.loads(script.string)
            props = (data.get("props") or {}).get("pageProps") or {}
            # Common patterns: product/stores list or searchResults
            for key in ("stores", "searchResults", "inventory", "results"):
                arr = props.get(key)
                if isinstance(arr, list) and arr:
                    for item in arr:
                        if isinstance(item, dict):
                            rows.append({
                                "store": str(item.get("storeNumber") or item.get("store_name") or item.get("name") or ""),
                                "city_or_address": str(item.get("address") or item.get("city") or item.get("location") or ""),
                                "availability": "In Stock" if item.get("in_stock", item.get("inventory", 0)) else "Out of Stock",
                                "quantity": str(item.get("quantity") or item.get("inventory") or ""),
                                "price": str(item.get("price") or item.get("store_price") or ""),
                            })
                    if rows:
                        return rows
    except Exception:
        pass

    # Look for store rows by content (In Stock, Out of Stock, address)
    for tag in soup.find_all(string=re.compile(r"In Stock|Out of Stock|Limited Stock")):
        parent = tag.parent
        if not parent:
            continue
        for _ in range(5):
            if parent.name in ("tr", "div", "li") and (parent.get("class") or []):
                row_text = parent.get_text(separator=" ", strip=True)
                if len(row_text) > 20 and ("Walmart" in row_text or "stock" in row_text.lower() or re.search(r"\d{5}", row_text)):
                    rows.append({
                        "store": "",
                        "city_or_address": row_text[:200],
                        "availability": "In Stock" if "In Stock" in row_text else ("Limited Stock" if "Limited" in row_text else "Out of Stock"),
                        "quantity": "",
                        "price": "",
                    })
                break
            parent = parent.parent if parent else None
            if not parent:
                break

    # Brickseek / Vincent-Cui style: div.table__body contains rows
    bodies = soup.find_all("div", class_=re.compile(r"table__body"))
    if not bodies and not rows:
        # Fallback: look for any table or grid of stores
        for tbl in soup.find_all(["table", "div"], class_=re.compile(r"table|result|store", re.I)):
            for row in tbl.find_all("tr") or tbl.find_all("div", recursive=False)[:50]:
                cells = row.find_all(["td", "div"]) or [row]
                if len(cells) < 2:
                    continue
                store_name = _text(cells[0])
                addr = _text(cells[1]) if len(cells) > 1 else ""
                avail = _text(cells[2]) if len(cells) > 2 else ""
                qty = _text(cells[3]) if len(cells) > 3 else ""
                if store_name or addr:
                    rows.append({
                        "store": store_name,
                        "city_or_address": addr,
                        "availability": avail,
                        "quantity": qty,
                        "price": "",
                    })
        return rows

    for tag in bodies:
        stores = tag.find_all("strong", class_=re.compile(r"address-location-name|store-name|location"))
        addrs = tag.find_all("address", class_=re.compile(r"address"))
        avails = tag.find_all("span", class_=re.compile(r"availability-status|status"))
        quantities = tag.find_all("span", class_=re.compile(r"quantity|table__cell-quantity"))
        prices = tag.find_all(string=re.compile(r"\$\d+")) or tag.find_all(class_=re.compile(r"price"))

        n = max(len(stores), len(addrs), 1)
        for i in range(n):
            store = stores[i].get_text(strip=True).replace("\n", " ") if i < len(stores) else ""
            addr = addrs[i].get_text(strip=True).replace("\n", " ") if i < len(addrs) else ""
            avail = avails[i].get_text(strip=True) if i < len(avails) else ""
            qty = ""
            if i < len(quantities):
                qty = quantities[i].get_text(strip=True)
                if ":" in qty:
                    qty = qty.split(":", 1)[-1].strip()
            price = ""
            if prices:
                for p in prices[: (i + 1) * 2]:
                    txt = p.get_text(strip=True) if hasattr(p, "get_text") else str(p).strip()
                    if re.match(r"^\$[\d,]+(?:\.\d{2})?$", txt):
                        price = txt
                        break
            rows.append({
                "store": store or f"Store {i+1}",
                "city_or_address": addr,
                "availability": avail,
                "quantity": qty,
                "price": price,
            })

    return rows


def _text(el) -> str:
    if hasattr(el, "get_text"):
        return el.get_text(strip=True)
    return str(el).strip()


# --- Walmart Grocery API (pickup/delivery; uses item_id from walmart.com/ip/.../ITEM_ID) ---
WALMART_GROCERY_BASE = "https://www.walmart.com/grocery"


def walmart_grocery_stores_by_zip(zip_code: str) -> list[dict]:
    """Get stores that support grocery near zip. Returns list of {store_id, address}."""
    url = f"{WALMART_GROCERY_BASE}/v4/api/serviceAvailability"
    params = {"postalCode": zip_code}
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
        access = data.get("accessPointList") or []
        out = []
        for entry in access[:15]:
            sid = entry.get("dispenseStoreId")
            addr = (entry.get("address") or {}).get("line1", "")
            if sid:
                out.append({"store_id": str(sid), "address": addr})
        return out
    except Exception:
        return []


def walmart_grocery_product_at_store(item_id: str, store_id: str) -> dict | None:
    """Check grocery product availability at a store. item_id from Walmart.com product URL."""
    url = f"{WALMART_GROCERY_BASE}/v3/api/products/{item_id}"
    params = {"itemFields": "all", "storeId": store_id}
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def lookup_walmart_grocery(item_id: str, zip_code: str) -> list[dict]:
    """Grocery availability by item_id and zip. Returns list of store results (in stock / out of stock)."""
    stores = walmart_grocery_stores_by_zip(zip_code)
    if not stores:
        return []
    rows = []
    for s in stores:
        sid = s["store_id"]
        addr = s.get("address", "")
        data = walmart_grocery_product_at_store(item_id, sid)
        if data is None:
            rows.append({"store": f"Store {sid}", "city_or_address": addr, "availability": "unknown", "quantity": "", "price": ""})
            continue
        basic = (data.get("basic") or {}) if isinstance(data, dict) else {}
        out_of_stock = basic.get("isOutOfStock", True)
        availability = "In Stock" if not out_of_stock else "Out of Stock"
        rows.append({
            "store": f"Store {sid}",
            "city_or_address": addr,
            "availability": availability,
            "quantity": "" if out_of_stock else "Y",
            "price": "",
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(
        description="US Walmart inventory by UPC and zip (Brickseek) or grocery by item_id.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--upc", default=None, help="UPC of the product (e.g. 050946872926); required for Brickseek")
    ap.add_argument("--zip", required=True, nargs="+", help="One or more zip codes (e.g. 90210 35058)")
    ap.add_argument("--walmart-grocery", action="store_true", help="Use Walmart Grocery API (item_id + zip) instead of Brickseek")
    ap.add_argument("--item-id", default=None, help="Walmart item ID from product URL (e.g. 123456789); required for --walmart-grocery")
    ap.add_argument("--csv", action="store_true", help="Print CSV header and rows")
    ap.add_argument("--cookies", type=Path, default=None, help="Path to Netscape or name=value cookie file from browser")
    ap.add_argument("--browser", action="store_true", default=True, help="Use real browser (default; avoids 403)")
    ap.add_argument("--no-browser", action="store_false", dest="browser", help="Use requests only (often 403)")
    ap.add_argument("--headless", action="store_true", help="Run browser headless (only with --browser)")
    ap.add_argument("--debug", type=Path, metavar="FILE", help="Save last result page HTML to FILE (browser mode)")
    args = ap.parse_args()

    if args.walmart_grocery:
        if not args.item_id:
            ap.error("--item-id is required when using --walmart-grocery (get it from walmart.com/ip/.../ITEM_ID)")
        all_rows = []
        for z in args.zip:
            for r in lookup_walmart_grocery(args.item_id.strip(), z.strip()):
                r["zip_searched"] = z
                all_rows.append(r)
    elif not args.upc:
        ap.error("--upc is required for Brickseek lookup (or use --walmart-grocery with --item-id)")
    else:
        all_rows = []
        if args.browser:
            if not _have_browser():
                print("Browser mode requires: pip install undetected-chromedriver", file=sys.stderr)
                sys.exit(1)
            print("Opening browser to Brickseek (may take a moment)...", file=sys.stderr)
            all_rows = lookup_via_browser(args.upc, args.zip, headless=args.headless, debug_path=args.debug)
        else:
            if _have_cloudscraper():
                print("Using cloudscraper for Brickseek (Cloudflare bypass).", file=sys.stderr)
            session = _session(args.cookies)
            for z in args.zip:
                try:
                    rows = lookup_upc_zip(args.upc, z, session)
                    for r in rows:
                        r["zip_searched"] = z
                        all_rows.append(r)
                except RuntimeError as e:
                    print(e, file=sys.stderr)
                    sys.exit(1)

    if args.csv:
        print("store,city_or_address,availability,quantity,price,zip_searched")
        for r in all_rows:
            print(",".join(_csv_cell(r.get(k, "")) for k in ("store", "city_or_address", "availability", "quantity", "price", "zip_searched")))
        return

    if not all_rows:
        if args.walmart_grocery:
            print("No grocery stores or product data for that zip/item.")
        else:
            print("No results from Brickseek.")
            print("  - Install cloudscraper: pip install cloudscraper")
            print("  - Or run without --no-browser so a visible browser opens (Cloudflare often blocks headless).")
            print("  - Manual check: open", BRICKSEEK_URL, "then enter UPC", args.upc, "and your zip(s).")
        return

    for r in all_rows:
        loc = r.get("city_or_address", "")
        qty = r.get("quantity", "")
        price = f" @ {r['price']}" if r.get("price") else ""
        print(f"  {r.get('store', '')} | {loc} | {r.get('availability', '')} | qty: {qty}{price} (zip searched: {r.get('zip_searched', '')})")
    print(f"\nTotal: {len(all_rows)} store(s).")


def _csv_cell(s: str) -> str:
    s = str(s).replace('"', '""')
    return f'"{s}"' if "," in s or '"' in s or "\n" in s else s


if __name__ == "__main__":
    main()
