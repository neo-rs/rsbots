#!/usr/bin/env python3
"""
Amazon ASIN Promo Checker - PA-API (limited fields) + Playwright (everything else).

PA-API (when enabled and working): title, primary image (Large), availability, current/before price, deal window (badge + times).
Playwright: merchant type, ships/sold, coupons, S&S, codes, condition, order limits, etc. (image only from page if PA-API did not return one).
If PA-API errors or is unavailable, all fields come from Playwright only.

Important:
- Coupons/codes are page-only. PA-API does not supply merchant / fulfillment / ships-from.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception:  # keep import optional so --paapi-only still works
    sync_playwright = None
    PlaywrightTimeoutError = Exception

ROOT = Path(__file__).resolve().parent
PROFILE_DIR = ROOT / "amazon_playwright_profile"
DEFAULT_SETTINGS_PATH = ROOT / "settings.json"
DEFAULT_DOTENV_PATH = ROOT / ".env"


def _parse_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _load_dotenv(path: Path) -> None:
    """Lightweight .env loader (no dependency). Does not override existing env vars."""
    try:
        if not path.exists() or not path.is_file():
            return
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if not k:
                continue
            if k not in os.environ:
                os.environ[k] = v
    except Exception:
        # Never fail the tool just because .env parsing had an issue.
        return


def _load_settings(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


_load_dotenv(DEFAULT_DOTENV_PATH)
SETTINGS = _load_settings(DEFAULT_SETTINGS_PATH)


def _settings_get(path: List[str], default: Any) -> Any:
    cur: Any = SETTINGS
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return default if cur is None else cur


def _ask_yes_no(prompt: str, *, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    raw = input(f"{prompt} ({suffix}): ").strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes", "true", "1", "on")


PAAPI_PARTNER_TAG = os.getenv("PAAPI_PARTNER_TAG") or _settings_get(["paapi", "partner_tag"], "")
PAAPI_MARKETPLACE = os.getenv("PAAPI_MARKETPLACE") or _settings_get(["paapi", "marketplace"], "www.amazon.com")
PAAPI_REGION = os.getenv("PAAPI_REGION") or _settings_get(["paapi", "region"], "us-east-1")
PAAPI_HOST = os.getenv("PAAPI_HOST") or _settings_get(["paapi", "host"], "webservices.amazon.com")
PAAPI_URI = os.getenv("PAAPI_URI") or _settings_get(["paapi", "uri"], "/paapi5/getitems")
PAAPI_TIMEOUT_S = float(_settings_get(["paapi", "timeout_s"], 20))
PAAPI_BATCH_SLEEP_S = float(_settings_get(["paapi", "batch_sleep_s"], 1.05))

PW_ENABLED_DEFAULT = _parse_bool(_settings_get(["playwright", "enabled_default"], True), True)
PW_HEADLESS_DEFAULT = _parse_bool(_settings_get(["playwright", "headless_default"], False), False)
PW_MANUAL_PAUSE_DEFAULT = _parse_bool(_settings_get(["playwright", "manual_pause_default"], True), True)
PW_SLOW_MO_MS = int(_settings_get(["playwright", "slow_mo_ms"], 50))
PW_PER_ASIN_SLEEP_S = float(_settings_get(["playwright", "per_asin_sleep_s"], 0.8))
PW_WAIT_MS_AFTER_GOTO = int(_settings_get(["playwright", "wait_ms_after_goto"], 1800))

OUT_DIR = ROOT / str(_settings_get(["output", "dir"], "output"))
OUT_DIR.mkdir(exist_ok=True)

# GetItems resources: fields merged from PA-API (title, primary image, availability, buy-box price, deal window).
# Merchant type, ships/sold, condition, etc. are Playwright-only. If PA-API fails, everything is Playwright.
RESOURCES = [
    "Images.Primary.Large",
    "ItemInfo.Title",
    "OffersV2.Listings.Availability",
    "OffersV2.Listings.Price",
    "OffersV2.Listings.DealDetails",
    "OffersV2.Listings.IsBuyBoxWinner",
    "Offers.Listings.Availability.Message",
    "Offers.Listings.Availability.Type",
    "Offers.Listings.Price",
    "Offers.Listings.SavingBasis",
    "Offers.Listings.IsBuyBoxWinner",
]

ASIN_RE = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{10})(?![A-Z0-9])", re.I)
PRICE_RE = re.compile(r"\$\s?\d{1,5}(?:,\d{3})*(?:\.\d{2})?")

@dataclass
class Result:
    asin: str
    url: str
    title: str = "N/A"
    current_price: str = "N/A"
    before_price: str = "N/A"
    discount: str = "N/A"
    code: str = "N/A"
    coupon_available: str = "N/A"
    coupon_detail: str = "N/A"
    subscribe_save: str = "N/A"
    deal_badge: str = "N/A"
    deal_start_human: str = "N/A"
    deal_end_human: str = "N/A"
    buybox_winner: str = "N/A"
    seller: str = "N/A"
    sold_by: str = "N/A"
    ships_from: str = "N/A"
    sold_by_page: str = "N/A"  # buy-box "Sold by" from page (used for fulfillment when PA-API seller differs)
    fulfilled_by: str = "N/A"
    fulfillment: str = "N/A"  # AMZ, FBA, FBM, Unknown (shown as Merchant Type in preview)
    detail_page_url: str = "N/A"
    availability: str = "N/A"
    max_order_qty: str = "N/A"
    min_order_qty: str = "N/A"
    condition: str = "N/A"
    prime_eligible: str = "N/A"
    free_shipping: str = "N/A"
    image_url: str = "N/A"
    source_notes: str = ""
    error: str = ""
    field_sources: Dict[str, str] = field(default_factory=dict)


def clean_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def _clip_buybox_value(s: str, max_len: int = 72) -> str:
    """Trim Amazon buy-box garbage (single-line layouts glue the whole page)."""
    s = clean_text(s)
    if not s:
        return ""
    for stop in (
        "Add to List",
        "Other sellers",
        "Other Sellers",
        "New & Used",
        "FREE Shipping",
        "Visit the",
        "Click to see",
        "Ships from",
        "Sold by",
        "Returns",
        "Eligible for Return",
        "Refund or Replacement",
        "Payment",
        "Secure transaction",
        "Add a Protection Plan",
        "Support",
        "List Price:",
        "out of 5 stars",
    ):
        idx = s.find(stop)
        if idx > 0 and idx < min(len(s), 200):
            s = clean_text(s[:idx])
            break
    if len(s) > max_len:
        s = clean_text(s[:max_len]).rstrip(" ,.-|") + "..."
    return s


def _sold_by_is_amazon_retail(name: str) -> bool:
    """True only for first-party retail (not Amazon Resale / Warehouse etc.)."""
    t = (name or "").strip().lower()
    return t in ("amazon.com", "amazon")


def _sold_by_is_amazon_family(name: str) -> bool:
    """Amazon-operated seller (Amazon.com, Amazon Resale, Warehouse, etc.). Used for Merchant Type AMZ vs FBA."""
    if _sold_by_is_amazon_retail(name):
        return True
    t = (name or "").strip().lower()
    return "amazon" in t


def _combined_shipper_seller_to_ships_sold(raw: str) -> Tuple[str, str]:
    """
    Amazon sometimes shows one line: Shipper / Seller -> value.
    - Amazon.com / Amazon -> AMZ (both sides Amazon retail).
    - Amazon Resale / Warehouse (amazon but not retail) + implied Amazon ship -> split (Amazon, name); Merchant Type still AMZ when shipped by Amazon.
    - Pure third-party name (no 'amazon') -> FBM (same entity ships and sells).
    """
    v = _clip_buybox_value(raw, max_len=96)
    if not v:
        return "", ""
    vl = v.lower()
    if _sold_by_is_amazon_retail(v) or vl == "amazon":
        return "Amazon.com", "Amazon.com"
    if "amazon" in vl:
        return "Amazon", v
    return v, v


def _line_is_shipper_seller_label(ln: str) -> bool:
    s = clean_text(ln).lower()
    if "shipper" not in s or "seller" not in s:
        return False
    return "/" in ln or " / " in s


def extract_shipper_seller_from_text(page_text: str) -> str:
    """Return the value line after 'Shipper / Seller' from a short excerpt (not full PDP)."""
    if not page_text:
        return ""
    # Keep excerpt short so we match the buy box, not unrelated footer copy.
    excerpt = page_text[:2800]
    m = re.search(
        r"Shipper\s*/\s*Seller\s*[:\s]*\s*(.+?)(?=\s*(?:Ships\s+from|Sold\s+by|Add\s+to\s+List|Other\s+sellers)\b|\Z)",
        excerpt,
        re.I | re.DOTALL,
    )
    if not m:
        return ""
    return _clip_buybox_value(m.group(1), max_len=96)


def buybox_ships_sold_playwright(page) -> Tuple[str, str]:
    """Read Shipper/Seller (combined) or Ships from / Sold by from tabular buy box when present."""
    ships, sold = "", ""
    for sel in ("#tabular-buybox", "#tabular-buybox-container"):
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            box = loc.first
            if not box.is_visible(timeout=1500):
                continue
            txt = clean_text(box.inner_text(timeout=4000))
            if not txt or len(txt) > 600:
                continue
            lines = [clean_text(x) for x in txt.splitlines() if clean_text(x)]
            for i, ln in enumerate(lines):
                if _line_is_shipper_seller_label(ln) and i + 1 < len(lines):
                    cs, sl = _combined_shipper_seller_to_ships_sold(lines[i + 1])
                    if cs and sl:
                        return cs, sl
                low = ln.lower()
                if low in ("ships from", "ships from:") and i + 1 < len(lines):
                    ships = _clip_buybox_value(lines[i + 1])
                if low in ("sold by", "sold by:") and i + 1 < len(lines):
                    sold = _clip_buybox_value(lines[i + 1])
            if ships or sold:
                return ships, sold
        except Exception:
            continue
    return "", ""


def _set_field(r: Result, field_name: str, value: Any, source: str) -> None:
    if value is None:
        return
    s = str(value) if not isinstance(value, str) else value
    if not s or s == "N/A":
        return
    setattr(r, field_name, s)
    r.field_sources[field_name] = source


def _set_calc_field(r: Result, field_name: str, value: Any, source: str) -> None:
    if value is None:
        return
    s = str(value) if not isinstance(value, str) else value
    if not s or s == "N/A":
        return
    setattr(r, field_name, s)
    # Only set if not already provided by a "real" source.
    r.field_sources.setdefault(field_name, source)


def _format_field_sources(r: Result) -> str:
    keys = [
        "title",
        "current_price",
        "before_price",
        "discount",
        "code",
        "coupon_available",
        "coupon_detail",
        "subscribe_save",
        "deal_badge",
        "deal_start_human",
        "deal_end_human",
        "availability",
        "seller",
        "sold_by",
        "ships_from",
        "sold_by_page",
        "fulfilled_by",
        "fulfillment",
        "detail_page_url",
        "max_order_qty",
        "prime_eligible",
        "free_shipping",
        "image_url",
    ]
    parts: List[str] = []
    for k in keys:
        src = r.field_sources.get(k)
        if src:
            parts.append(f"{k}={src}")
    return ", ".join(parts) if parts else "N/A"


def extract_asins(text: str) -> List[str]:
    out: List[str] = []
    for m in ASIN_RE.finditer(text or ""):
        asin = m.group(1).upper()
        if asin not in out:
            out.append(asin)
    return out


def asin_to_url(asin: str, tag: str = "") -> str:
    tag_q = quote((tag or "").strip())
    return f"https://www.amazon.com/dp/{asin}?tag={tag_q}&th=1&psc=1"


def money_to_float(s: str) -> Optional[float]:
    if not s or s == "N/A":
        return None
    m = PRICE_RE.search(s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0).replace("$", "").replace(" ", ""))
    except Exception:
        return None


def calc_discount(current: str, before: str) -> str:
    c = money_to_float(current)
    b = money_to_float(before)
    if not c or not b or b <= 0 or c >= b:
        return "N/A"
    pct = round((1 - c / b) * 100)
    return f"{pct}% OFF"


def hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def sha256_hex(msg: str) -> str:
    return hashlib.sha256(msg.encode("utf-8")).hexdigest()


def sign_paapi_headers(payload: str, access_key: str, secret_key: str, *, host: str = PAAPI_HOST, region: str = PAAPI_REGION, uri: str = PAAPI_URI) -> Dict[str, str]:
    service = "ProductAdvertisingAPI"
    amz_date = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    date_stamp = dt.datetime.utcnow().strftime("%Y%m%d")
    headers_to_sign = {
        "content-encoding": "amz-1.0",
        "content-type": "application/json; charset=UTF-8",
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-target": "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems",
    }
    signed_headers = ";".join(sorted(headers_to_sign.keys()))
    canonical_headers = "".join(f"{k}:{headers_to_sign[k]}\n" for k in sorted(headers_to_sign.keys()))
    canonical_request = "\n".join(["POST", uri, "", canonical_headers, signed_headers, sha256_hex(payload)])
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, credential_scope, sha256_hex(canonical_request)])
    k_date = hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    out = dict(headers_to_sign)
    out["Authorization"] = authorization
    return out


def paapi_getitems(asins: List[str], *, partner_tag: str, access_key: str, secret_key: str) -> Dict[str, Any]:
    payload_obj = {
        "ItemIds": asins[:10],
        "Resources": RESOURCES,
        "PartnerTag": partner_tag,
        "PartnerType": "Associates",
        "Marketplace": PAAPI_MARKETPLACE,
    }
    payload = json.dumps(payload_obj, separators=(",", ":"))
    headers = sign_paapi_headers(payload, access_key, secret_key)
    req = Request(f"https://{PAAPI_HOST}{PAAPI_URI}", data=payload.encode("utf-8"), headers=headers, method="POST")
    try:
        with urlopen(req, timeout=PAAPI_TIMEOUT_S) as resp:
            raw = resp.read().decode("utf-8", "replace")
            return json.loads(raw)
    except HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        return {"__error__": f"HTTP {e.code}: {body[:500]}"}
    except URLError as e:
        return {"__error__": f"URL error: {e}"}
    except Exception as e:
        return {"__error__": f"PA API failed: {e}"}


def get_path(d: Dict[str, Any], path: List[Any], default: Any = "N/A") -> Any:
    cur: Any = d
    for p in path:
        try:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur[p]
        except Exception:
            return default
    return cur if cur not in (None, "") else default


def _availability_from_listing(listing: Dict[str, Any]) -> str:
    av = listing.get("Availability") or {}
    msg = av.get("Message")
    if msg is not None and str(msg).strip():
        return str(msg).strip()
    typ = av.get("Type")
    if typ is not None and str(typ).strip():
        t = str(typ).strip().upper()
        labels = {
            "IN_STOCK": "In Stock",
            "OUT_OF_STOCK": "Out of Stock",
            "PREORDER": "Pre-order",
            "AVAILABLE_DATE": "Available by date",
        }
        return labels.get(t, t.replace("_", " ").title())
    return "N/A"


def _format_deal_time_iso(iso_s: Optional[str]) -> str:
    if not iso_s or not str(iso_s).strip():
        return "N/A"
    s = str(iso_s).strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        u = dt.datetime.fromisoformat(s)
        if u.tzinfo is None:
            u = u.replace(tzinfo=dt.timezone.utc)
        try:
            local = u.astimezone(ZoneInfo("America/New_York"))
        except Exception:
            local = u.astimezone(dt.timezone.utc)
        return local.strftime("%b %d, %Y %I:%M %p %Z")
    except Exception:
        return s


def _merge_deal_details_v2(r: Result, listing: Dict[str, Any]) -> None:
    dd = listing.get("DealDetails")
    if not isinstance(dd, dict):
        return
    badge = dd.get("Badge")
    if badge:
        b = str(badge).strip()
        # API often returns generic "Deal"; prefer a clearer label when we have a window.
        if b.lower() == "deal" and (dd.get("EndTime") or dd.get("StartTime")):
            b = "Limited-time deal"
        _set_field(r, "deal_badge", b, "PAAPI")
    st = dd.get("StartTime")
    et = dd.get("EndTime")
    if st:
        _set_field(r, "deal_start_human", _format_deal_time_iso(str(st)), "PAAPI")
    if et:
        _set_field(r, "deal_end_human", _format_deal_time_iso(str(et)), "PAAPI")


def extract_ships_sold_from_text(page_text: str) -> Tuple[str, str]:
    """Parse Ships from / Sold by from a short excerpt (single-line buy box on same row)."""
    if not page_text:
        return "", ""
    excerpt = page_text[:4500]
    ships_from, sold_by = "", ""
    m1 = re.search(r"Ships\s+from\s*[:\s]*\s*(.+?)(?=\s+Sold\s+by\b)", excerpt, re.I | re.DOTALL)
    if m1:
        ships_from = _clip_buybox_value(m1.group(1))
    m2 = re.search(
        r"Sold\s+by\s*[:\s]*\s*(.+?)(?=\s+Add\s+to\s+List\b|\s+Other\s+sellers\b|\s+Ships\s+from\b|\s+FREE\b|\s+New\s*&\s*Used\b|\s+from\s+\$|$)",
        excerpt,
        re.I | re.DOTALL,
    )
    if m2:
        sold_by = _clip_buybox_value(m2.group(1))
    return ships_from, sold_by


# PA-API only supplies these; Playwright must not overwrite them when merge succeeded.
PAAPI_PLAYWRIGHT_LOCK: Set[str] = {
    "title",
    "current_price",
    "before_price",
    "availability",
    "deal_badge",
    "deal_start_human",
    "deal_end_human",
    "image_url",
}


def _field_locked_by_paapi(r: Result, field: str) -> bool:
    return r.field_sources.get(field) == "PAAPI"


def merge_paapi(result_map: Dict[str, Result], data: Dict[str, Any]) -> None:
    """Apply PA-API: title, primary image URL, availability, current/before price, deal window. No merchant/fulfillment from API."""
    if data.get("__error__"):
        for r in result_map.values():
            r.source_notes += f"PAAPI error: {data['__error__']} | "
        return
    for item in get_path(data, ["ItemsResult", "Items"], []):
        asin = item.get("ASIN", "").upper()
        if not asin or asin not in result_map:
            continue
        r = result_map[asin]
        _set_field(r, "title", get_path(item, ["ItemInfo", "Title", "DisplayValue"], "N/A"), "PAAPI")
        _set_field(r, "image_url", get_path(item, ["Images", "Primary", "Large", "URL"], "N/A"), "PAAPI")

        def _pick_best_listing(listings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            if not listings:
                return None
            best = listings[0]
            for li in listings:
                if li.get("IsBuyBoxWinner") is True:
                    best = li
                    break
            return best

        offers_v2_listing = _pick_best_listing(get_path(item, ["OffersV2", "Listings"], []))
        offers_listing = _pick_best_listing(get_path(item, ["Offers", "Listings"], []))

        listing = offers_v2_listing or offers_listing
        if listing:
            _set_field(r, "availability", _availability_from_listing(listing), "PAAPI")
            _set_field(
                r,
                "current_price",
                get_path(listing, ["Price", "Money", "DisplayAmount"], get_path(listing, ["Price", "DisplayAmount"], "N/A")),
                "PAAPI",
            )
            _set_field(
                r,
                "before_price",
                get_path(
                    listing,
                    ["Price", "SavingBasis", "Money", "DisplayAmount"],
                    get_path(listing, ["SavingBasis", "DisplayAmount"], "N/A"),
                ),
                "PAAPI",
            )
            _merge_deal_details_v2(r, listing)

        _set_calc_field(r, "discount", calc_discount(r.current_price, r.before_price), "calc")
        r.source_notes += "PAAPI ok | "


def first_text(page, selectors: Iterable[str]) -> str:
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                tx = clean_text(el.text_content() or "")
                if tx:
                    return tx
        except Exception:
            continue
    return ""


def extract_visible_text(page, selector: str = "#ppd") -> str:
    """
    Extract visible text from the main product container when possible.
    Scanning the full <body> is prone to false positives (hidden UI, footers,
    unrelated modules, other variants).
    """
    selectors = [selector, "#centerCol", "#rightCol", "body"]
    for sel in selectors:
        try:
            txt = clean_text(page.locator(sel).inner_text(timeout=4000))
            if txt:
                return txt
        except Exception:
            continue
    try:
        return clean_text(page.content())
    except Exception:
        return ""


def near_match(text: str, keyword_re: str, window: int = 140) -> str:
    m = re.search(keyword_re, text, re.I)
    if not m:
        return ""
    return text[max(0, m.start() - window): min(len(text), m.end() + window)]


def extract_code(text: str) -> str:
    patterns = [
        r"\bCODE\s*[:\-]?\s*([A-Z0-9]{4,30})\b",
        r"\bpromo\s+code\s*[:\-]?\s*([A-Z0-9]{4,30})\b",
        r"\buse\s+code\s*[:\-]?\s*([A-Z0-9]{4,30})\b",
        r"\bapply\s+code\s*[:\-]?\s*([A-Z0-9]{4,30})\b",
        r"\benter\s+code\s*[:\-]?\s*([A-Z0-9]{4,30})\b",
    ]
    skip = {"AMAZON", "COUPON", "DISCOUNT", "PROMO", "PRICE"}
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            code = m.group(1).upper().strip()
            if code not in skip:
                return code
    return "N/A"


def extract_coupon(text: str, current_price: str = "") -> Tuple[str, str]:
    low = text.lower()
    if "coupon" not in low and "clip" not in low:
        return "N/A", "N/A"
    focus = near_match(text, r"clip\s+coupon|coupon") or text
    # Amazon often says "Save $5 with coupon" or "Save 20% with coupon".
    money = PRICE_RE.search(focus)
    pct = re.search(r"(\d{1,2})\s*%", focus)
    if re.search(r"clip\s+coupon|with\s+coupon|coupon", focus, re.I):
        if money:
            detail = f"save {money.group(0).replace(' ', '')}"
            # sanity: don't report coupon savings larger than the current item price
            cur = money_to_float(current_price)
            sav = money_to_float(money.group(0))
            if cur and sav and sav > cur:
                return "N/A", "N/A"
            return "Yes", detail
        if pct:
            return "Yes", f"save {pct.group(1)}%"
        return "Yes", "clip before checkout"
    return "N/A", "N/A"


def extract_subscribe_save(text: str) -> str:
    if not re.search(r"subscribe\s*(?:&|and)\s*save", text, re.I):
        return "N/A"
    focus = near_match(text, r"subscribe\s*(?:&|and)\s*save", 160) or text
    pct = re.search(r"(\d{1,2})\s*%", focus)
    if pct:
        return f"Yes ({pct.group(1)}%)"
    return "Yes"


def extract_deal_badge(text: str) -> str:
    for label in ["Limited time deal", "Prime exclusive deal", "Lightning Deal", "Deal"]:
        if re.search(re.escape(label), text, re.I):
            return label
    return "N/A"


def extract_before_price(text: str, current: str) -> str:
    # Prefer phrases around list/was price.
    patterns = [
        r"List Price:\s*(\$\s?\d[\d,]*(?:\.\d{2})?)",
        r"Was:\s*(\$\s?\d[\d,]*(?:\.\d{2})?)",
        r"Typical price:\s*(\$\s?\d[\d,]*(?:\.\d{2})?)",
        r"Before:\s*(\$\s?\d[\d,]*(?:\.\d{2})?)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            val = m.group(1).replace(" ", "")
            if val != current:
                return val
    return "N/A"


def extract_availability(text: str) -> str:
    """
    Page-text availability fallback.
    Keeps output short + human-readable.
    """
    t = clean_text(text or "")
    if not t:
        return "N/A"

    # Order matters: avoid matching "in stock" inside phrases like "back in stock soon".
    candidates = [
        "Temporarily out of stock",
        "Out of stock",
        "Currently unavailable",
        "Usually ships within",
        "Available to ship in",
        "Pre-order",
        "Only",
    ]
    tl = t.lower()
    for c in candidates:
        idx = tl.find(c.lower())
        if idx >= 0:
            snippet = t[idx: idx + 140]
            snippet = clean_text(snippet)
            snippet = _clip_buybox_value(snippet, max_len=140)
            if snippet:
                return snippet

    # "In Stock" should match as a standalone status, not "back in stock soon".
    m = re.search(r"\bin stock\b", t, re.I)
    if m:
        before = t[max(0, m.start() - 8): m.start()].lower()
        if "back " not in before:
            snippet = t[m.start(): m.start() + 80]
            snippet = clean_text(snippet)
            snippet = _clip_buybox_value(snippet, max_len=80)
            if snippet:
                return snippet
    return "N/A"


def scrape_one_with_playwright(page, asin: str, url: str, *, wait_ms: int = PW_WAIT_MS_AFTER_GOTO) -> Dict[str, str]:
    out: Dict[str, str] = {}
    page.goto(url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(wait_ms)

    title = first_text(page, ["#productTitle", "span#productTitle"])
    price = first_text(page, [
        "span.priceToPay span.a-offscreen",
        "#corePriceDisplay_desktop_feature_div span.priceToPay span.a-offscreen",
        "#corePriceDisplay_desktop_feature_div span.a-price:not(.a-text-price) span.a-offscreen",
        "#apex_desktop span.priceToPay span.a-offscreen",
        "#apex_desktop span.a-price:not(.a-text-price) span.a-offscreen",
        "span.a-price span.a-offscreen",
    ])
    before = first_text(page, [
        "#corePriceDisplay_desktop_feature_div span.a-text-price span.a-offscreen",
        "#corePriceDisplay_desktop_feature_div .basisPrice span.a-offscreen",
        "#apex_desktop span.a-text-price span.a-offscreen",
        ".basisPrice span.a-offscreen",
    ])
    image = ""
    try:
        image = page.get_attribute("#landingImage", "src") or ""
    except Exception:
        pass
    text = extract_visible_text(page, "#ppd")
    if not before:
        before = extract_before_price(text, price)
    code = extract_code(text)
    coupon_available, coupon_detail = extract_coupon(text, price)
    sns = extract_subscribe_save(text)
    deal = extract_deal_badge(text)
    availability_raw = first_text(page, ["#availability span", "#availability"]) or ""
    availability = _clip_buybox_value(availability_raw, max_len=160) if availability_raw else ""
    # Some pages inject JS blobs into #availability; guard against that and fall back to visible text search.
    if availability and any(bad in availability.lower() for bad in ("p.when", "function(", "uelogerror", "aod_assets")):
        availability = ""
    if not availability:
        availability = extract_availability(text)
    seller_raw = first_text(page, ["#sellerProfileTriggerId", "#merchant-info a", "#merchant-info"])
    seller = _clip_buybox_value(seller_raw, 100) if seller_raw else ""

    ships_from, sold_by_page = buybox_ships_sold_playwright(page)
    # Line-based parse on tabular container only (avoid gluing in #merchant-info mega-block).
    buybox_text = first_text(page, ["#tabular-buybox", "#tabular-buybox-container"]) or ""
    if buybox_text and len(buybox_text) < 800:
        lines = [clean_text(x) for x in buybox_text.splitlines() if clean_text(x)]
        for i, ln in enumerate(lines):
            low = ln.lower()
            if low in ("ships from", "ships from:") and i + 1 < len(lines) and not ships_from:
                ships_from = _clip_buybox_value(lines[i + 1])
            if low in ("sold by", "sold by:") and i + 1 < len(lines) and not sold_by_page:
                sold_by_page = _clip_buybox_value(lines[i + 1])

    rs, ss = extract_ships_sold_from_text(text[:4500] if text else "")
    if not ships_from and rs:
        ships_from = rs
    if not sold_by_page and ss:
        sold_by_page = ss

    # Many listings use "Shipper / Seller" on one row (e.g. Amazon.com, or AOKun Store) instead of Ships/Sold split.
    combo = extract_shipper_seller_from_text(text or "")
    if combo and len(combo) <= 120:
        cs, sl = _combined_shipper_seller_to_ships_sold(combo)
        if cs and sl:
            split_ok = bool(ships_from and sold_by_page and ships_from != "N/A" and sold_by_page != "N/A")
            if _sold_by_is_amazon_retail(cs) and _sold_by_is_amazon_retail(sl):
                ships_from, sold_by_page = cs, sl
            elif cs == sl and "amazon" not in cs.lower():
                ships_from, sold_by_page = cs, sl
            elif not split_ok:
                ships_from, sold_by_page = cs, sl

    sold_by_display = sold_by_page or seller or "N/A"

    out.update({
        "title": title or "N/A",
        "current_price": price.replace(" ", "") if price else "N/A",
        "before_price": before.replace(" ", "") if before else "N/A",
        "code": code,
        "coupon_available": coupon_available,
        "coupon_detail": coupon_detail,
        "subscribe_save": sns,
        "deal_badge": deal,
        "availability": availability or "N/A",
        "seller": seller or "N/A",
        "ships_from": ships_from or "N/A",
        "sold_by": sold_by_display,
        "sold_by_page": sold_by_page or "N/A",
        "image_url": image or "N/A",
        "page_text_sample": text[:500],
    })
    out["discount"] = calc_discount(out["current_price"], out["before_price"])
    return out


def apply_scrape_result(r: Result, s: Dict[str, str]) -> None:
    for field in [
        "title",
        "current_price",
        "before_price",
        "discount",
        "code",
        "coupon_available",
        "coupon_detail",
        "subscribe_save",
        "deal_badge",
        "availability",
        "seller",
        "sold_by",
        "sold_by_page",
        "ships_from",
        "image_url",
    ]:
        if field in PAAPI_PLAYWRIGHT_LOCK and _field_locked_by_paapi(r, field):
            continue
        if (
            field == "discount"
            and _field_locked_by_paapi(r, "current_price")
            and _field_locked_by_paapi(r, "before_price")
        ):
            continue
        val = s.get(field, "N/A")
        _set_field(r, field, val, "Playwright")

    # Recompute discount from PA-API prices when both are authoritative (overrides any skipped scrape discount).
    if _field_locked_by_paapi(r, "current_price") and _field_locked_by_paapi(r, "before_price"):
        d = calc_discount(r.current_price, r.before_price)
        if d and d != "N/A":
            r.discount = d
            r.field_sources["discount"] = "calc"

    # Merchant type: Ships from Amazon + any Amazon-family seller (incl. Amazon Resale) -> AMZ; Ships Amazon + non-Amazon seller -> FBA.
    ships_from = (r.ships_from or "").strip()
    sold_for = (r.sold_by_page or "").strip() if r.sold_by_page != "N/A" else (r.sold_by or "").strip() if r.sold_by != "N/A" else (r.seller or "").strip()
    ships_from_norm = ships_from.lower()

    if ships_from and ships_from != "N/A" and sold_for:
        ships_from_is_amz = "amazon" in ships_from_norm
        sold_norm = sold_for.lower()
        if ships_from_is_amz and _sold_by_is_amazon_family(sold_for):
            r.fulfillment, r.fulfilled_by = "AMZ", "Amazon"
        elif ships_from_is_amz:
            r.fulfillment, r.fulfilled_by = "FBA", "Amazon"
        elif (not ships_from_is_amz) and "amazon" not in sold_norm:
            r.fulfillment, r.fulfilled_by = "FBM", sold_for
        else:
            r.fulfillment, r.fulfilled_by = "Unknown", "N/A"
        r.field_sources["fulfillment"] = "Playwright"
        r.field_sources["fulfilled_by"] = "Playwright"
    elif not (r.fulfillment or "").strip() or (r.fulfillment or "").strip() == "N/A":
        r.fulfillment = "Unknown"
        r.field_sources.setdefault("fulfillment", "Playwright")

    r.source_notes += "Playwright ok | "


def run_playwright(results: Dict[str, Result], *, headless: bool, slow_mo: int, manual_pause: bool) -> None:
    if sync_playwright is None:
        for r in results.values():
            r.error += "Playwright not installed. Run: pip install -r requirements.txt && python -m playwright install chromium | "
        return
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            slow_mo=slow_mo,
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1365, "height": 900},
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"],
        )
        page = ctx.new_page()
        try:
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        except Exception:
            pass
        if manual_pause:
            print("\nManual browser step: log in / set delivery ZIP / solve any check if needed.")
            page.goto("https://www.amazon.com/", wait_until="domcontentloaded", timeout=45000)
            input("When the Amazon page looks good, press ENTER here to continue...")
        for idx, (asin, r) in enumerate(results.items(), start=1):
            print(f"[{idx}/{len(results)}] Playwright checking {asin}...")
            try:
                s = scrape_one_with_playwright(page, asin, r.url)
                apply_scrape_result(r, s)
            except Exception as e:
                r.error += f"Playwright failed: {str(e)[:220]} | "
            time.sleep(PW_PER_ASIN_SLEEP_S)
        ctx.close()


def save_outputs(results: List[Result]) -> Tuple[Path, Path]:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = OUT_DIR / f"amazon_asin_results_{ts}.csv"
    jsonl_path = OUT_DIR / f"amazon_asin_results_{ts}.jsonl"
    fields = list(asdict(results[0]).keys()) if results else list(Result("", "").__dataclass_fields__.keys())
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow(asdict(r))
    with jsonl_path.open("w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")
    return csv_path, jsonl_path


def discord_preview(r: Result) -> str:
    lines: List[str] = []
    title = r.title if r.title != "N/A" else r.asin
    lines.append(f"**{title}**")

    def add(label: str, value: str) -> None:
        v = (value or "").strip()
        if not v or v == "N/A":
            return
        lines.append(f"{label}: **{v}**")

    add("Current Price", r.current_price)
    add("Before", r.before_price)
    add("Discount", r.discount)
    add("CODE", r.code)
    if r.deal_badge != "N/A" or r.deal_start_human != "N/A" or r.deal_end_human != "N/A":
        if r.deal_badge != "N/A" and r.deal_badge.strip().lower() != "deal":
            add("Deal", r.deal_badge)
        if r.deal_start_human != "N/A" or r.deal_end_human != "N/A":
            if r.deal_start_human != "N/A" and r.deal_end_human != "N/A":
                lines.append(f"Deal window: **{r.deal_start_human}** -> **{r.deal_end_human}**")

    add("Availability", r.availability)
    sold = r.sold_by if r.sold_by != "N/A" else r.seller
    add("Sold by", sold)
    add("Merchant Type", r.fulfillment)
    lines.append("")
    detail_url = r.detail_page_url if r.detail_page_url != "N/A" else r.url
    add("Detail page", detail_url)
    add("Primary image URL", r.image_url)
    lines.append("")
    if r.subscribe_save != "N/A":
        lines.append("Subscribe & Save - discount may apply (cancel after item arrives)")
    if r.coupon_available != "N/A":
        if r.coupon_available == "Yes":
            cpn = f" ({r.coupon_detail})" if r.coupon_detail != "N/A" else ""
            lines.append(f"Coupon Available - clip it before checkout!{cpn}")
        else:
            lines.append(f"Coupon Available - **{r.coupon_available}**")
    lines += ["", r.url]
    return "\n".join(lines)


def terminal_debug_log(r: Result) -> str:
    lines = [
        "---- DEBUG / SOURCES ----",
        f"field_sources: { _format_field_sources(r) }",
        f"source_notes: {r.source_notes.strip() or 'N/A'}",
    ]
    if r.error:
        lines.append(f"error: {r.error}")
    return "\n".join(lines)


def read_asins_interactive() -> List[str]:
    print("Amazon ASIN Promo Checker")
    print("Paste ASINs or Amazon URLs separated by comma/space/new lines.")
    print("Type a file path to load ASINs from a .txt file. Blank line exits.\n")
    raw = input("ASINs / URLs / file path: ").strip()
    if not raw:
        return []
    p = Path(raw.strip('"'))
    if p.exists() and p.is_file():
        raw = p.read_text(encoding="utf-8", errors="ignore")
    asins = extract_asins(raw)
    if not asins:
        # split fallback
        for part in re.split(r"[\s,;]+", raw):
            if ASIN_RE.fullmatch(part.strip()):
                asins.append(part.strip().upper())
    return asins


def main() -> int:
    asins = read_asins_interactive()
    if not asins:
        print("No ASINs found.")
        return 0
    # de-dupe preserve order
    asins = list(dict.fromkeys([a.upper() for a in asins]))
    partner_tag = (PAAPI_PARTNER_TAG or "").strip()
    if not partner_tag:
        partner_tag = input("PAAPI_PARTNER_TAG (associate tag): ").strip()
    results: Dict[str, Result] = {a: Result(asin=a, url=asin_to_url(a, partner_tag)) for a in asins}

    use_paapi = _ask_yes_no("Use PA API too?", default=False)
    if use_paapi:
        access_key = os.getenv("PAAPI_ACCESS_KEY", "").strip()
        secret_key = os.getenv("PAAPI_SECRET_KEY", "").strip()
        if not access_key:
            access_key = input("PAAPI_ACCESS_KEY: ").strip()
        if not secret_key:
            secret_key = input("PAAPI_SECRET_KEY: ").strip()
        if access_key and secret_key:
            print("Running PA API in batches of 10 ASINs...")
            for i in range(0, len(asins), 10):
                chunk = asins[i:i+10]
                data = paapi_getitems(chunk, partner_tag=partner_tag, access_key=access_key, secret_key=secret_key)
                merge_paapi({a: results[a] for a in chunk}, data)
                time.sleep(PAAPI_BATCH_SLEEP_S)  # safe baseline throttle
        else:
            print("Skipping PA API, missing keys.")

    use_pw = _ask_yes_no("Use Playwright page check for coupons/codes/S&S?", default=PW_ENABLED_DEFAULT)
    if use_pw:
        headless = _ask_yes_no("Run browser headless? Recommended NO for Amazon.", default=PW_HEADLESS_DEFAULT)
        manual_pause = _ask_yes_no("Open browser first so you can login/set ZIP/check page?", default=PW_MANUAL_PAUSE_DEFAULT)
        run_playwright(results, headless=headless, slow_mo=PW_SLOW_MO_MS, manual_pause=manual_pause)

    out = list(results.values())
    csv_path, jsonl_path = save_outputs(out)

    print("\n================ RESULTS ================")
    for r in out:
        print("-" * 48)
        print(discord_preview(r))
        print(terminal_debug_log(r))
    print("-" * 48)
    print(f"Saved CSV:   {csv_path}")
    print(f"Saved JSONL: {jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
