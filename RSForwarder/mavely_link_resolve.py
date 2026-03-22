"""
Canonical Mavely short-link helpers (query-param embeds + headless Chromium when no browser profile).

Single source of truth for behavior shared with `Mavelytest/mavely_link_tester.py`.
`affiliate_rewriter.py` imports this module only — do not duplicate Branch/query/Playwright paths there.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Iterator, List, Optional, Tuple
from urllib.parse import parse_qsl, unquote, urlparse

EMBEDDED_MERCHANT_QUERY_KEYS: Tuple[str, ...] = (
    "url",
    "u",
    "target",
    "dest",
    "destination",
    "redirect",
    "redirect_url",
    "returnurl",
    "return_url",
    "merchant_url",
    "out",
    "link",
    "deeplink",
    "deep_link",
    "original_url",
)

DEFAULT_PLAYWRIGHT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)


def decode_query_value(raw: str) -> str:
    s = (raw or "").strip()
    for _ in range(5):
        nxt = unquote(s)
        if nxt == s:
            break
        s = nxt
    return s.strip()


def url_is_mavely_bridge_surface(url: str) -> bool:
    try:
        h = (urlparse((url or "").strip()).netloc or "").lower()
    except Exception:
        return True
    if h.startswith("www."):
        h = h[4:]
    if h == "mavelyinfluencer.com" or h.endswith(".mavelyinfluencer.com"):
        return True
    if h == "mavely.app.link" or h.endswith(".mavely.app.link"):
        return True
    return False


def iter_embedded_https_urls_from_query(url: str) -> Iterator[str]:
    """Yield https? URLs found in common redirect query keys (any order)."""
    u = (url or "").strip()
    if not u:
        return
    try:
        pairs = parse_qsl(urlparse(u).query or "", keep_blank_values=True)
    except Exception:
        return
    by_lower: dict = {}
    for k, v in pairs:
        kl = (k or "").lower()
        by_lower.setdefault(kl, []).append(v)
    for key in EMBEDDED_MERCHANT_QUERY_KEYS:
        for raw in by_lower.get(key, []) or []:
            cand = decode_query_value(raw)
            if cand.startswith("http://") or cand.startswith("https://"):
                yield cand


def maybe_extract_store_from_query(url: str) -> Optional[str]:
    """First embedded https URL from query (Mavelytest parity; no merchant scoring)."""
    for c in iter_embedded_https_urls_from_query(url):
        return c
    return None


def dedupe_redirect_chain(start: str, history_urls: List[str], final: str) -> List[str]:
    out: List[str] = []
    for part in [start] + list(history_urls) + [final]:
        p = (part or "").strip()
        if p and (not out or out[-1] != p):
            out.append(p)
    return out


def playwright_resolve_mavely_to_merchant_url(url: str, timeout_ms: int) -> Optional[str]:
    """
    Minimal headless Chromium (no persistent profile). Matches the working path in mavely_link_tester:
    domcontentloaded + wait + poll until the address bar leaves Mavely bridge hosts.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None
    u = (url or "").strip()
    if not u.startswith("http"):
        return None
    t_ms = max(3_000, min(int(timeout_ms), 120_000))
    launch_args = ["--disable-dev-shm-usage"]
    if sys.platform.startswith("linux") or (
        (os.getenv("MAVELY_PLAYWRIGHT_NO_SANDBOX", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    ):
        launch_args.append("--no-sandbox")
    ua = (os.getenv("MAVELY_USER_AGENT", "") or "").strip() or DEFAULT_PLAYWRIGHT_UA
    extra: dict = {}
    ck = (os.getenv("MAVELY_COOKIES", "") or "").strip()
    if ck:
        extra["Cookie"] = ck
    budget_s = max(8.0, min(t_ms / 1000.0, 55.0))
    deadline = time.perf_counter() + budget_s
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=launch_args)
            try:
                ctx = browser.new_context(viewport={"width": 1280, "height": 720}, user_agent=ua)
                page = ctx.new_page()
                if extra:
                    page.set_extra_http_headers(extra)
                page.goto(u, wait_until="domcontentloaded", timeout=t_ms)
                try:
                    page.wait_for_timeout(5_000)
                except Exception:
                    pass
                while time.perf_counter() < deadline:
                    try:
                        cur = (page.url or "").strip()
                    except Exception:
                        cur = ""
                    if cur.startswith("http") and (not url_is_mavely_bridge_surface(cur)):
                        return cur
                    try:
                        loc = page.evaluate("() => String(location && location.href ? location.href : '')")
                        if (
                            isinstance(loc, str)
                            and loc.startswith("http")
                            and (not url_is_mavely_bridge_surface(loc.strip()))
                        ):
                            return loc.strip()
                    except Exception:
                        pass
                    try:
                        page.wait_for_timeout(500)
                    except Exception:
                        break
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
    except Exception:
        return None
    return None
