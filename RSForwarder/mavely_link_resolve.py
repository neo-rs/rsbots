"""
Canonical Mavely short-link helpers (query-param embeds + shared CDP Chrome navigation).

Uses Chromerrunner CDP Chrome (oracle_real_chrome_profile on :9222) — no separate Playwright
browser launches on the server.

`affiliate_rewriter.py` imports this module only — do not duplicate Branch/query/Playwright paths there.
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Callable, Iterator, List, Optional, Tuple
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


def _strip_www_and_port(netloc: str) -> str:
    h = (netloc or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    if ":" in h and not h.startswith("["):
        left, _, right = h.rpartition(":")
        if right.isdigit():
            h = left
    return h


def host_is_mavely_bridge_surface(netloc: str) -> bool:
    """
    True if host (urlparse netloc) is still Mavely / Branch tracking, not the merchant.
    Covers app.link shorts, mavelyinfluencer.com, and mavelylife.com hub pages (/u/...).
    """
    h = _strip_www_and_port(netloc)
    if not h:
        return False
    if h == "mavely.app.link" or h.endswith(".mavely.app.link"):
        return True
    if h == "mavelyinfluencer.com" or h.endswith(".mavelyinfluencer.com"):
        return True
    if h == "mavelylife.com" or h.endswith(".mavelylife.com"):
        return True
    return False


def url_is_mavely_bridge_surface(url: str) -> bool:
    try:
        netloc = (urlparse((url or "").strip()).netloc or "").lower()
    except Exception:
        return True
    return host_is_mavely_bridge_surface(netloc)


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
    Navigate via shared CDP Chrome; poll until the address bar leaves Mavely bridge hosts.
    """
    from RSForwarder.mavely_cdp_session import (
        cdp_is_up,
        ensure_cdp_chrome_running,
        resolve_chrome_cdp_url,
        resolve_url_via_cdp,
    )

    u = (url or "").strip()
    if not u.startswith("http"):
        return None
    cdp = resolve_chrome_cdp_url()
    if not cdp_is_up(cdp):
        ensure_cdp_chrome_running(cdp_url=cdp)
    if not cdp_is_up(cdp):
        return None
    t_ms = max(3_000, min(int(timeout_ms), 120_000))
    poll_s = max(8.0, min(t_ms / 1000.0, 55.0))
    return resolve_url_via_cdp(
        u,
        cdp_url=cdp,
        timeout_ms=t_ms,
        accept_url=lambda cur: cur.startswith("http") and (not url_is_mavely_bridge_surface(cur)),
        settle_ms=5_000,
        poll_s=poll_s,
    )


def _strip_trailing_junk(url: str) -> str:
    return (url or "").strip().rstrip(").,]\"'")


def collect_https_urls_from_html(html: str, *, max_chars: int = 2_000_000) -> List[str]:
    """Best-effort https? URLs from HTML/JS (generic; caller filters with merchant predicate)."""
    t = (html or "")[: max(10_000, int(max_chars))]
    if not t:
        return []
    try:
        found = re.findall(r"https?://[^\s\"'<>\\]+", t, flags=re.IGNORECASE)
    except Exception:
        return []
    out: List[str] = []
    seen: set = set()
    for raw in found:
        c = _strip_trailing_junk(raw)
        if not (c.startswith("http://") or c.startswith("https://")):
            continue
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def playwright_resolve_outbound_persistent_sync(
    url: str,
    *,
    timeout_ms: int,
    profile_dir: str,
    headed: bool,
    settle_ms: int,
    poll_s: float,
    accept_merchant: Callable[[str], bool],
) -> Optional[str]:
    """
    Navigate via shared CDP Chrome (oracle_real_chrome_profile). profile_dir/headed are ignored —
    kept for call-site compatibility with affiliate_rewriter.
    """
    from RSForwarder.mavely_cdp_session import (
        cdp_is_up,
        ensure_cdp_chrome_running,
        resolve_chrome_cdp_url,
        resolve_url_via_cdp,
    )

    u = (url or "").strip()
    if not u.startswith("http"):
        return None
    cdp = resolve_chrome_cdp_url()
    if not cdp_is_up(cdp):
        ensure_cdp_chrome_running(cdp_url=cdp)
    if not cdp_is_up(cdp):
        return None
    t_ms = max(5_000, min(int(timeout_ms), 180_000))
    settle_ms = max(500, min(int(settle_ms), 60_000))
    poll_s = max(1.0, min(float(poll_s), 60.0))
    out = resolve_url_via_cdp(
        u,
        cdp_url=cdp,
        timeout_ms=t_ms,
        accept_url=accept_merchant,
        settle_ms=settle_ms,
        poll_s=poll_s,
    )
    if out:
        return out

    # Scan page HTML when address bar did not land on an acceptable merchant URL.
    from RSForwarder.mavely_cdp_session import _close_cdp_page, playwright_page_via_cdp

    p = None
    page = None
    created = False
    try:
        p, _browser, page, created = playwright_page_via_cdp(cdp, ephemeral=True)
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=t_ms)
        except Exception:
            pass
        try:
            content = page.content() or ""
        except Exception:
            content = ""
        best: Optional[str] = None
        best_len = -1
        for cand in collect_https_urls_from_html(content):
            if not accept_merchant(cand):
                continue
            ln = len(cand)
            if ln > best_len:
                best_len = ln
                best = cand
        return best
    except Exception:
        return None
    finally:
        if p is not None:
            _close_cdp_page(p, page, created)
