"""
Canonical outbound URL resolution for RSForwarder affiliate unwrap.

Ports MW universal_link_resolver V2 behavior with two production fixes:
  1) FINAL = highest-scored URL from the full HTTP redirect chain (not only resp.url).
  2) Intermediate detection uses host/path heuristics (go.*, /ml/, affiliate networks), not a
     giant per-shortener allowlist.

Mavely/Amazon tagging stays in affiliate_rewriter.py; this module only resolves merchant URLs.
"""
from __future__ import annotations

import base64
import html
import re
from typing import List, Optional, Set, Tuple
from urllib.parse import parse_qs, parse_qsl, unquote, urlparse

import requests

_DEFAULT_TIMEOUT = 20
_DEFAULT_MAX_DEPTH = 10
_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.0.0 Safari/537.36"
)

_COMMON_QUERY_KEYS = (
    "url", "u", "uri", "target", "dest", "destination", "redirect", "redirect_url",
    "returnUrl", "return_url", "merchant_url", "out", "to", "r", "q", "link", "href",
    "t", "l", "product", "murl", "deep_link_value", "fallback_url", "canonical_url",
    "redirectUrl", "clickurl", "adurl", "camp",
)

_BLOCK_HOSTS = frozenset(
    {
        "cloudflare.com",
        "www.cloudflare.com",
        "challenges.cloudflare.com",
        "captcha-delivery.com",
        "errors.edgesuite.net",
    }
)

_NOISY_HOSTS = frozenset(
    {
        "facebook.com",
        "www.facebook.com",
        "instagram.com",
        "www.instagram.com",
        "twitter.com",
        "x.com",
        "www.x.com",
        "youtube.com",
        "www.youtube.com",
        "google.com",
        "www.google.com",
        "doubleclick.net",
        "googletagmanager.com",
        "w3.org",
        "schema.org",
    }
)

# Minimal exact shorteners (not maintained as the primary gate).
_SHORTENER_EXACT = frozenset(
    {
        "bit.ly",
        "t.co",
        "tinyurl.com",
        "goo.gl",
        "amzn.to",
        "a.co",
        "rebrand.ly",
        "cutt.ly",
        "rb.gy",
        "is.gd",
        "s.id",
        "linktr.ee",
        "mavely.app.link",
        "www.mavely.app.link",
    }
)

_INTERMEDIATE_HOST_PREFIXES = ("go.", "click.", "r.", "lnk.", "redirect.")
_INTERMEDIATE_HOST_MARKERS = (
    "skim",
    "linksynergy",
    "linksynergy",
    "impact",
    "rakuten",
    "anrdoezrs",
    "tkqlhce",
    "dpbolvw",
    "bizrate",
    "redirectingat",
    "awin1",
    "viglink",
    "sovrn",
    "dotomi",
    "emjcd",
    "magik",
    "shopmy",
    "pepperjam",
    "cj.com",
    "branch",
    "app.link",
    "mavely",
    "track",
    "affiliate",
    "clk",
)

_INTERMEDIATE_PATH_RE = re.compile(
    r"^/(ml/|p-\d|l/|link/|redirect/|dlg/|links/)",
    re.IGNORECASE,
)

_MAVELY_BRIDGE_MARKERS = ("mavelyinfluencer.com", "mavelylife.com", "joinmavely.com")


def _host_of(url: Optional[str]) -> str:
    try:
        return (urlparse(url or "").netloc or "").lower()
    except Exception:
        return ""


def _path_of(url: Optional[str]) -> str:
    try:
        return (urlparse(url or "").path or "").lower()
    except Exception:
        return ""


def _looks_like_http_url(url: str) -> bool:
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")):
        return False
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return bool(host) and "." in host and len(host) <= 200
    except Exception:
        return False


def clean_candidate_url(url: str) -> str:
    u = html.unescape(unquote(url or "")).strip().strip("'\"<>),;]")
    u = u.replace("\\/", "/")
    if not u:
        return u
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


def _expand_woot_gatekeeper_url(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u or "woot.com" not in _host_of(u):
        return None
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
        q = {k.lower(): v for k, v in parse_qsl(p.query or "", keep_blank_values=True)}
    except Exception:
        return None
    is_gate = host.startswith("account.") or host.startswith("auth.")
    if not is_gate and "/welcome" not in path and "/signin" not in path:
        return None
    for key in ("returnurl", "redirect", "next", "destination", "continue"):
        v = (q.get(key) or "").strip()
        if not v:
            continue
        v2 = unquote(v)
        if "%" in v2:
            v2 = unquote(v2)
        if _looks_like_http_url(v2) and "woot.com" in _host_of(v2):
            return v2
    return None


def normalize_merchant_url(url: str) -> str:
    """Decode known interstitials (Walmart /blocked?url=, Woot welcome returnUrl, etc.)."""
    u = (url or "").strip()
    if not u:
        return u
    woot = _expand_woot_gatekeeper_url(u)
    if woot:
        u = woot
    try:
        parsed = urlparse(u)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "")
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    except Exception:
        return u
    if "walmart.com" in host and path.startswith("/blocked") and q.get("url"):
        raw = (q.get("url") or "").strip()
        pad = "=" * ((4 - (len(raw) % 4)) % 4)
        try:
            decoded = base64.urlsafe_b64decode(raw + pad).decode("utf-8", errors="ignore").strip()
        except Exception:
            decoded = ""
        if decoded:
            if decoded.startswith("http://") or decoded.startswith("https://"):
                return decoded
            if decoded.startswith("/"):
                return f"{parsed.scheme or 'https'}://{parsed.netloc}{decoded}"
    return u


def is_block_or_infra(url: Optional[str]) -> bool:
    host = _host_of(url)
    path = _path_of(url)
    if not host:
        return True
    if host in _BLOCK_HOSTS or any(host.endswith("." + h) for h in _BLOCK_HOSTS):
        return True
    if any(k in path for k in ("5xx-error-landing", "access-denied", "captcha", "challenge", "blocked?url=")):
        if "walmart.com" in host and path.startswith("/blocked"):
            return False
        if any(k in path for k in ("captcha", "challenge", "access-denied", "5xx-error")):
            return True
    return False


def is_mavely_bridge(url: Optional[str]) -> bool:
    host = _host_of(url)
    return any(m in host for m in _MAVELY_BRIDGE_MARKERS) or host.endswith("mavely.app.link")


def is_intermediate_url(url: Optional[str]) -> bool:
    """Heuristic: shortener / affiliate hop (not a product page)."""
    u = (url or "").strip()
    if not _looks_like_http_url(u):
        return True
    host = _host_of(u)
    path = _path_of(u)
    if host in _SHORTENER_EXACT:
        return True
    if any(host.startswith(p) for p in _INTERMEDIATE_HOST_PREFIXES):
        return True
    if _INTERMEDIATE_PATH_RE.search(path or ""):
        return True
    if any(m in host for m in _INTERMEDIATE_HOST_MARKERS):
        return True
    if is_mavely_bridge(u):
        return True
    return False


def score_outbound_candidate(url: str) -> int:
    u = normalize_merchant_url((url or "").strip())
    if not _looks_like_http_url(u):
        return -100
    if is_block_or_infra(u):
        return -90
    host = _host_of(u)
    if host in _NOISY_HOSTS or any(host.endswith("." + d) for d in _NOISY_HOSTS):
        return -40
    path = _path_of(u)
    if re.search(r"\.(png|jpg|jpeg|gif|webp|svg|css|js|ico|woff2?)(\?|$)", path):
        return -60
    if is_intermediate_url(u):
        return -25
    score = min(len(path) + len(urlparse(u).query or ""), 500) + 40
    if any(x in path for x in ("/ip/", "/p/", "/product", "/products/", "/dp/", "/itm/", "/sku", "/shop/", "/offers/")):
        score += 30
    if "amazon." in host or host.endswith("amazon.com"):
        score += 25
    if any(
        store in host
        for store in (
            "walmart.com",
            "target.com",
            "macys.com",
            "lowes.com",
            "bestbuy.com",
            "costco.com",
            "kohls.com",
            "homedepot.com",
            "woot.com",
            "samsclub.com",
        )
    ):
        score += 20
    return score


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def find_urls_in_text(text: str) -> List[str]:
    if not text:
        return []
    candidates: List[str] = []
    patterns = (
        r"https?://[^\s\"'<>\\]+",
        r"https%3A%2F%2F[^\"' <>{}\\]+",
        r"http%3A%2F%2F[^\"' <>{}\\]+",
    )
    for pattern in patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            c = clean_candidate_url(match)
            if _looks_like_http_url(c):
                candidates.append(c)
    return candidates


def extract_any_url_from_query(url: str) -> Optional[str]:
    try:
        qs = parse_qs(urlparse(url).query, keep_blank_values=True)
    except Exception:
        return None
    candidates: List[str] = []
    for key in _COMMON_QUERY_KEYS:
        for actual_key, vals in qs.items():
            if actual_key == key or actual_key.lower() == key.lower():
                for raw in vals:
                    c = clean_candidate_url(raw)
                    if _looks_like_http_url(c):
                        candidates.append(normalize_merchant_url(c))
    for vals in qs.values():
        for raw in vals:
            candidates.extend(find_urls_in_text(str(raw or "")))
    candidates = [normalize_merchant_url(c) for c in _dedupe_keep_order(candidates)]
    if not candidates:
        return None
    return sorted(candidates, key=score_outbound_candidate, reverse=True)[0]


def extract_best_url_from_html(text: str, base_url: str = "") -> Optional[str]:
    expanded: List[str] = []
    for c in find_urls_in_text(text or ""):
        q = extract_any_url_from_query(c)
        if q:
            expanded.append(q)
        expanded.append(normalize_merchant_url(c))
    base_host = _host_of(base_url)
    filtered = [
        u
        for u in _dedupe_keep_order(expanded)
        if _looks_like_http_url(u) and _host_of(u) and _host_of(u) != base_host
    ]
    if not filtered:
        return None
    return sorted(filtered, key=score_outbound_candidate, reverse=True)[0]


def try_extract_linksynergy_murl(url: str) -> Optional[str]:
    u = (url or "").strip()
    if "linksynergy.com" not in _host_of(u) and "anrdoezrs.net" not in _host_of(u):
        return None
    try:
        qs = parse_qs(urlparse(u).query, keep_blank_values=True)
    except Exception:
        return None
    for key in ("murl", "url"):
        vals = qs.get(key) or []
        if not vals:
            continue
        cand = clean_candidate_url(vals[0])
        if _looks_like_http_url(cand):
            return normalize_merchant_url(cand)
    # anrdoezrs path-embedded destination
    if "anrdoezrs.net" in _host_of(u) and "/https://" in u.lower():
        idx = u.lower().find("/https://")
        if idx >= 0:
            cand = u[idx + 1 :]
            if _looks_like_http_url(cand):
                return normalize_merchant_url(cand)
    return None


def try_extract_murl_from_chain(chain: List[str]) -> Optional[str]:
    for u in reversed(chain or []):
        m = try_extract_linksynergy_murl(u)
        if m:
            return m
    return None


def pick_best_from_candidates(urls: List[str]) -> Optional[str]:
    best_u = ""
    best_s = -10_000
    for raw in urls or []:
        u = normalize_merchant_url((raw or "").strip())
        if not u:
            continue
        s = score_outbound_candidate(u)
        if s > best_s:
            best_s = s
            best_u = u
    if best_s < 0:
        return None
    return best_u or None


def collect_candidates_from_chain(chain: List[str], *, html: str = "") -> List[str]:
    out: List[str] = []
    for u in chain or []:
        u = (u or "").strip()
        if not u:
            continue
        out.append(u)
        q = extract_any_url_from_query(u)
        if q:
            out.append(q)
        m = try_extract_linksynergy_murl(u)
        if m:
            out.append(m)
    if html:
        h = extract_best_url_from_html(html, chain[-1] if chain else "")
        if h:
            out.append(h)
    return _dedupe_keep_order([normalize_merchant_url(x) for x in out])


def request_once(url: str, timeout: int) -> Tuple[Optional[requests.Response], List[str], Optional[str]]:
    headers = {
        "User-Agent": _DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        resp = requests.get(url, timeout=timeout, allow_redirects=True, headers=headers)
    except Exception as e:
        return None, [], str(e)
    chain = [str(h.url) for h in (resp.history or [])] + [str(resp.url or url)]
    return resp, chain, None


def resolve_outbound_url(
    input_url: str,
    *,
    timeout_s: int = _DEFAULT_TIMEOUT,
    max_depth: int = _DEFAULT_MAX_DEPTH,
) -> str:
    """
    Resolve any http(s) link to the best merchant/product URL we can find.
    Does not depend on the host being pre-registered in a shortener list.
    """
    original = clean_candidate_url(input_url)
    current = normalize_merchant_url(original)
    seen: Set[str] = set()

    for _depth in range(max(1, int(max_depth))):
        if not current or current in seen:
            break
        seen.add(current)

        embedded = extract_any_url_from_query(current)
        if embedded and embedded != current:
            current = embedded
            continue

        resp, chain, err = request_once(current, int(timeout_s))
        if err is not None or resp is None:
            break

        html_text = ""
        try:
            html_text = resp.text or ""
        except Exception:
            html_text = ""

        murl = try_extract_murl_from_chain(chain)
        if murl:
            chain = chain + [murl]

        post_q = extract_any_url_from_query(str(resp.url or ""))
        if post_q:
            chain = chain + [post_q]

        # HTML from 403/bot pages often points back to shorteners; prefer redirect chain only.
        use_html = html_text if (resp.status_code and int(resp.status_code) < 400) else ""
        candidates = collect_candidates_from_chain(chain, html=use_html)
        best = pick_best_from_candidates(candidates)
        final_resp = normalize_merchant_url(str(resp.url or current))

        if best and score_outbound_candidate(best) >= score_outbound_candidate(final_resp):
            if best != current and score_outbound_candidate(best) > 0:
                current = best
                if not is_intermediate_url(current):
                    break
                continue

        if not is_intermediate_url(final_resp) and score_outbound_candidate(final_resp) > 0:
            current = final_resp
            break

        if html_text:
            html_best = extract_best_url_from_html(html_text, final_resp)
            if html_best and score_outbound_candidate(html_best) > score_outbound_candidate(final_resp):
                current = html_best
                if not is_intermediate_url(current):
                    break
                continue

        current = final_resp
        if not is_intermediate_url(current):
            break

    out = normalize_merchant_url(current or original)
    if is_intermediate_url(out):
        _, chain, _err = request_once(original, int(timeout_s))
        if chain:
            retry_best = pick_best_from_candidates(collect_candidates_from_chain(chain, html=""))
            if retry_best and not is_intermediate_url(retry_best) and score_outbound_candidate(retry_best) > 0:
                return retry_best
        return original
    return out
