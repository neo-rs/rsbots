"""
RSForwarder Affiliate Rewriter (standalone)

Implements the same rewrite behavior as Instorebotforwarder:
- Detect URLs in text
- Expand/unwrap short & deal-hub links to their final destination
- Amazon: add your affiliate tag and optionally mask as [amzn.to/xxxx](<real_url>)
- Other stores: generate a Mavely affiliate link (when possible)
- Markdown-safe: do not inject markdown links inside existing markdown link targets.

Debug logging (stdout, prefix [AffiliateDebug]): config `affiliate_rewrite_debug` or env
AFFILIATE_REWRITE_DEBUG=1 (compact: one compute summary line per batch). Hop-by-hop noise:
`affiliate_rewrite_debug_verbose` or AFFILIATE_REWRITE_DEBUG_VERBOSE=1. RSForwarder attaches
`_affiliate_compute_memo` so identical URLs across content + multiple embeds run the network/Mavely work once per message.

Mavely short links (`mavely.app.link`) are not bit.ly-style “pure redirects”: HTTP expand often stops at
`mavelyinfluencer.com` (bridge). Final merchant URLs are recovered here via hub HTML + optional Playwright
(profile + cookies); `mavely_client.py` creates new links via GraphQL and does not unwrap HTML.
"""

from __future__ import annotations

import asyncio
import base64
import html as _html
import json
import os
import re
import secrets
import shutil
import string
import subprocess
import tempfile
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl, urljoin, unquote

import aiohttp


def _env_first_token(name: str, default: str = "") -> str:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return (raw.split()[0] if raw.split() else raw).strip() or default


def _bool_or_default(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if not s:
        return default
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def affiliate_rewrite_debug_on(cfg: Optional[dict]) -> bool:
    """
    Affiliate debug logging (compact summaries by default).
    Enable with config `affiliate_rewrite_debug`: true or env AFFILIATE_REWRITE_DEBUG=1.
    RSForwarder also turns this on per-route when channel `repost_debug` is true.
    """
    try:
        if _bool_or_default((cfg or {}).get("affiliate_rewrite_debug"), False):
            return True
    except Exception:
        pass
    raw = (os.getenv("AFFILIATE_REWRITE_DEBUG", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def affiliate_rewrite_debug_verbose_on(cfg: Optional[dict]) -> bool:
    """
    Per-hop expand / HTML / query-unwrap lines (noisy). Default off.
    Config `affiliate_rewrite_debug_verbose` or env AFFILIATE_REWRITE_DEBUG_VERBOSE=1.
    """
    try:
        if _bool_or_default((cfg or {}).get("affiliate_rewrite_debug_verbose"), False):
            return True
    except Exception:
        pass
    raw = (os.getenv("AFFILIATE_REWRITE_DEBUG_VERBOSE", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _aff_dbg_clip(s: str, n: int = 100) -> str:
    t = (s or "").replace("\r", " ").replace("\n", " ")
    if len(t) <= n:
        return t
    return t[: max(0, n - 3)] + "..."


def _aff_dbg(cfg: Optional[dict], msg: str) -> None:
    """Compact / summary affiliate debug line (always printed when debug is on)."""
    if not affiliate_rewrite_debug_on(cfg):
        return
    print(f"[AffiliateDebug] {msg}", flush=True)


def _aff_dbg_verbose(cfg: Optional[dict], msg: str) -> None:
    """Detailed hop-by-hop logs; only when affiliate_rewrite_debug_verbose is set."""
    if not affiliate_rewrite_debug_on(cfg) or not affiliate_rewrite_debug_verbose_on(cfg):
        return
    print(f"[AffiliateDebug] {msg}", flush=True)


def _aff_dbg_notes_summary(notes: Optional[Dict[str, str]], limit: int = 8) -> str:
    if not notes:
        return "(no notes)"
    parts: List[str] = []
    for i, (k, v) in enumerate(notes.items()):
        if i >= limit:
            parts.append(f"... +{len(notes) - limit} more")
            break
        parts.append(f"{_aff_dbg_clip(k, 72)!r} => {_aff_dbg_clip(str(v), 96)!r}")
    return "; ".join(parts)


def _cfg_or_env_str(cfg: dict, cfg_key: str, env_key: str) -> str:
    try:
        v = str((cfg or {}).get(cfg_key) or "").strip()
    except Exception:
        v = ""
    return v if v else (os.getenv(env_key, "") or "").strip()


def _cfg_or_env_int(cfg: dict, cfg_key: str, env_key: str) -> Optional[int]:
    v = (cfg or {}).get(cfg_key)
    if isinstance(v, int):
        return v
    raw = (os.getenv(env_key, "") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _log_once(key: str, seconds: int = 60) -> bool:
    try:
        now = time.time()
        if not hasattr(_log_once, "_recent"):
            setattr(_log_once, "_recent", {})  # type: ignore[attr-defined]
        recent: dict = getattr(_log_once, "_recent")  # type: ignore[attr-defined]
        last = float(recent.get(key, 0.0) or 0.0)
        if last and (now - last) < float(seconds):
            return False
        recent[key] = now
        if len(recent) > 200:
            cutoff = now - float(max(5, seconds))
            for k in list(recent.keys())[:80]:
                try:
                    if float(recent.get(k, 0.0) or 0.0) < cutoff:
                        recent.pop(k, None)
                except Exception:
                    pass
        return True
    except Exception:
        return True


def _add_query_param(url: str, key: str, value: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    try:
        parsed = urlparse(u)
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
        q[key] = value
        new_q = urlencode(q, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_q, parsed.fragment))
    except Exception:
        return u


def _strip_tracking_params(url: str) -> str:
    """
    Remove common tracking / affiliate query params so we don't accidentally
    credit someone else's tracking when we can't generate our own affiliate link.
    """
    u = (url or "").strip()
    if not u:
        return u
    try:
        parsed = urlparse(u)
        q_pairs = list(parse_qsl(parsed.query, keep_blank_values=True))
    except Exception:
        return u

    deny_exact = {
        "irgwc",
        "clickid",
        "click_id",
        "irclickid",
        "irclick",
        "ecid",  # common affiliate/campaign id param (e.g. Ecid=af_Mavely)
        "afsrc",
        "affid",
        "affiliate",
        "aff",
        "cid",  # many merchants use cid=affiliate-_-...
        "source",
        # Target creator branded portal uses TCID=AFL-...
        # This should not be forwarded into our own affiliate link generation.
        "tcid",
        # Target tracking params (non-essential for destination resolution)
        "clkid",
        "cpng",
        "lnm",
        "ref",
        "refid",
        "ref_id",
        "fbclid",
        "gclid",
        "yclid",
        "mc_eid",
        "mc_cid",
        "spm",
        "sc_channel",
        "sc_campaign",
        "sc_medium",
        "sc_content",
        "sc_outcome",
        # Commission Junction / affiliate pixels (e.g. Woot tools.woot.com offers)
        "cjdata",
        "cjevent",
        "cjpid",
        "cjaid",
        # Walmart affiliate / impact-style tracking on /ip/... landers
        "clickid",
        "wmlspartner",
        "affiliates_ad_id",
        "sharedid",
    }

    kept = []
    for k, v in q_pairs:
        kl = (k or "").strip().lower()
        if not kl:
            continue
        if kl.startswith("utm_"):
            continue
        # Branch.io / deep-link tracking params (often huge and can break downstream brand resolution)
        if kl.startswith("_branch"):
            continue
        if kl.startswith("branch_"):
            continue
        # Mavely web-only deep link flag (harmless for users; API/unwrap sometimes leaves it on bridge URLs)
        if kl == "$web_only" or kl == "web_only" or kl.lstrip("$") == "web_only":
            continue
        if kl.startswith("cj") and len(kl) <= 12:
            continue
        if kl in deny_exact:
            continue
        kept.append((k, v))

    new_q = urlencode(kept, doseq=True)
    # Drop fragment too; it's often tracking (e.g. "#code=...").
    try:
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_q, ""))
    except Exception:
        return u


def coerce_plain_url(value: str) -> str:
    """
    Coerce an affiliate rewrite output into a plain URL string.

    - If given a Discord markdown masked link like: [amzn.to/xxxx](<https://...>)
      returns the target URL.
    - If wrapped like <https://...>, unwraps it.
    - Otherwise returns the stripped string.
    """
    s = (value or "").strip()
    if not s:
        return ""
    try:
        target = _extract_markdown_link_target(s)
        if target:
            s = target.strip()
    except Exception:
        pass
    if s.startswith("<") and s.endswith(">") and len(s) > 2:
        inner = s[1:-1].strip()
        if inner.startswith("http://") or inner.startswith("https://"):
            return inner
    return s


async def compute_affiliate_rewrites_plain(cfg: dict, urls: List[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Like compute_affiliate_rewrites, but ensures mapped values are plain URLs
    (no Discord markdown masked links).
    """
    mapped, notes = await compute_affiliate_rewrites(cfg, urls)
    if not mapped:
        return {}, notes
    out: Dict[str, str] = {}
    for k, v in (mapped or {}).items():
        out[k] = coerce_plain_url(v)
    return out, notes


_URL_RE = re.compile(
    r"((?:https?://)?(?:www\.)?[a-z0-9][a-z0-9.-]*\.[a-z]{2,}(?:/[^\s<>()]*)?)",
    re.IGNORECASE,
)


def normalize_input_url(raw: str) -> str:
    """
    Normalize a user-provided URL-ish string into a URL.

    IMPORTANT: This must NOT manufacture fake URLs from Discord mentions (e.g. "https://@everyone").
    If the input is clearly not a URL, return "".
    """
    s = (raw or "").strip()
    if not s:
        return ""

    low = s.lower()

    # Guard: mentions are NOT urls (prevents "https://@everyone" pollution).
    if low.startswith("@") or low in {"@everyone", "@here"}:
        return ""
    if low.startswith("<@") or low.startswith("<#") or low.startswith("<@&"):
        return ""

    # Trim common Discord URL wrappers: <https://...>
    if s.startswith("<") and s.endswith(">") and len(s) > 2:
        inner = s[1:-1].strip()
        if inner:
            s = inner

    # Normalize whitespace in URLs (some store links contain unencoded spaces)
    # so downstream affiliate creation doesn't fail and silently fall back.
    s = s.replace("\r", "").replace("\n", "").replace("\t", " ").strip()
    s = s.replace(" ", "%20")

    if s.startswith("http://") or s.startswith("https://"):
        return s
    return f"https://{s}"


def extract_urls_with_spans(text: str) -> List[Tuple[str, int, int]]:
    s = text or ""
    out: List[Tuple[str, int, int]] = []
    for m in _URL_RE.finditer(s):
        raw = m.group(1)
        start = int(m.start(1))
        end = int(m.end(1))
        trimmed = raw
        while trimmed and trimmed[-1] in ".,);]}>":
            trimmed = trimmed[:-1]
            end -= 1
        trimmed = trimmed.strip()
        if not (trimmed and end > start):
            continue
        if start > 0 and end < len(s) and s[start - 1] == "<" and s[end] == ">":
            out.append((trimmed, start - 1, end + 1))
        else:
            out.append((trimmed, start, end))
    return out


def _extract_markdown_link_target(markdown: str) -> Optional[str]:
    s = (markdown or "").strip()
    if not s.startswith("["):
        return None
    m = re.search(r"\]\(\s*<([^>]+)>\s*\)", s)
    if m:
        return (m.group(1) or "").strip()
    m2 = re.search(r"\]\(\s*([^)]+)\s*\)", s)
    if m2:
        return (m2.group(1) or "").strip()
    return None


def _is_markdown_link_target_context(text: str, start: int, end: int) -> bool:
    try:
        if start < 2 or end > len(text):
            return False
        # Allow whitespace and optional "<" wrapper:
        #   [label](https://...)
        #   [label](<https://...>)
        #   [label]( <https://...> )
        left = text[max(0, start - 12):start]
        j = left.rfind("](")
        if j < 0:
            return False
        between = left[j + 2:]
        if between.strip() not in {"", "<"}:
            return False
        right = text[end:min(len(text), end + 8)]
        r = right.lstrip()
        if r.startswith(")"):
            return True
        if r.startswith(">"):
            return r[1:].lstrip().startswith(")")
        return False
    except Exception:
        return False


def _mavely_bridge_host(host: str) -> bool:
    """
    True if this host is still on Mavely's tracking layer (not the final merchant).
    Expanding mavely.app.link often lands on mavelyinfluencer.com/u/<creator> — that URL
    credits whoever created the original link, not us. We must unwrap further or rewrap via API.
    """
    h = (host or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    if not h:
        return False
    if h == "mavelyinfluencer.com" or h.endswith(".mavelyinfluencer.com"):
        return True
    if h == "mavely.app.link" or h.endswith(".mavely.app.link"):
        return True
    return False


def _url_is_mavely_bridge_surface(url: str) -> bool:
    """True if URL is still mavely.app.link / mavelyinfluencer.com (real browsers often SPA-redirect to the merchant)."""
    try:
        h = (urlparse((url or "").strip()).netloc or "").lower()
    except Exception:
        return True
    return _mavely_bridge_host(h)


def is_mavely_app_short_link(url: str) -> bool:
    """True only for mavely.app.link short URLs (typical Discord affiliate short links)."""
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    return (host == "mavely.app.link") or host.endswith(".mavely.app.link")


def is_mavely_link(url: str) -> bool:
    """
    True for any Mavely tracking surface (app.link short links or mavelyinfluencer.com bridge pages).
    Use for *target* checks: if still a bridge, we must not treat it as the final merchant URL.
    """
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return _mavely_bridge_host(host)


def _is_mavely_or_join_host(host: str) -> bool:
    h = (host or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    if not h:
        return False
    if _mavely_bridge_host(h):
        return True
    if h == "joinmavely.com" or h.endswith(".joinmavely.com"):
        return True
    return False


def _html_fetch_headers_for_hub(url: str) -> Dict[str, str]:
    """
    Mavely bridge pages often sit behind Cloudflare; */* + script UA can yield 403 or a shell
    without __NEXT_DATA__. Use browser-like Accept (and light Sec-Fetch hints) for HTML unwrap GETs.
    """
    ua = (os.getenv("MAVELY_USER_AGENT", "") or "").strip() or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    h: Dict[str, str] = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        host = (urlparse((url or "").strip()).netloc or "").lower()
    except Exception:
        host = ""
    if _is_mavely_or_join_host(host) or "mavely" in host:
        h["Sec-Fetch-Dest"] = "document"
        h["Sec-Fetch-Mode"] = "navigate"
        h["Sec-Fetch-Site"] = "cross-site"
        h["Upgrade-Insecure-Requests"] = "1"
        # Cloudflare often blocks datacenter IPs without a real session; reuse Mavely login cookies.
        ck = (os.getenv("MAVELY_COOKIES", "") or "").strip()
        if ck:
            h["Cookie"] = ck
        base = (os.getenv("MAVELY_BASE_URL", "") or "").strip().rstrip("/") or "https://www.joinmavely.com"
        h.setdefault("Referer", f"{base}/tools")
    return h


def _expand_redirect_headers(url: str) -> Dict[str, str]:
    """
    Headers for short-link redirect expansion (expand_url).

    dealshacks / hiddendealsociety: plain HEAD/GET + */* is usually enough (302 chain).

    mavely.app.link / mavelyinfluencer.com: use the same browser + Cookie profile as hub HTML GETs
    so Branch / Cloudflare see a document navigation, not a script probe — often yields longer 302 chains
    or the same HTML shell we then unwrap (parity with special_html_hosts handling).
    """
    u = (url or "").strip()
    if _url_is_mavely_bridge_surface(u):
        return dict(_html_fetch_headers_for_hub(u))
    ua = (os.getenv("MAVELY_USER_AGENT", "") or "").strip() or "Mozilla/5.0"
    return {"User-Agent": ua, "Accept": "*/*"}


def _fetch_html_via_curl(url: str, headers: Dict[str, str], timeout_s: int) -> Tuple[int, str]:
    """
    Cloudflare often serves 403 to aiohttp/Python TLS; system curl sometimes gets real HTML.
    Used only as a fallback for Mavely bridge unwrap on Linux servers (Oracle).
    """
    if not shutil.which("curl"):
        return 0, ""
    t = max(5, min(int(timeout_s or 8), 60))
    path = ""
    try:
        fd, path = tempfile.mkstemp(suffix=".html")
        os.close(fd)
        cmd: List[str] = [
            "curl",
            "-sS",
            "-L",
            "--compressed",
            "-o",
            path,
            "-w",
            "%{http_code}",
            "--max-time",
            str(t),
            "-A",
            (headers.get("User-Agent") or "Mozilla/5.0").strip(),
            "-H",
            (headers.get("Accept") or "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ]
        if headers.get("Cookie"):
            cmd.extend(["-H", f"Cookie: {headers['Cookie']}"])
        if headers.get("Referer"):
            cmd.extend(["-H", f"Referer: {headers['Referer']}"])
        if headers.get("Accept-Language"):
            cmd.extend(["-H", f"Accept-Language: {headers['Accept-Language']}"])
        for k in ("Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site", "Upgrade-Insecure-Requests"):
            v = headers.get(k)
            if v:
                cmd.extend(["-H", f"{k}: {v}"])
        cmd.append((url or "").strip())
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=t + 5, errors="ignore")
        code_s = (cp.stdout or "").strip()
        try:
            code = int(code_s) if code_s.isdigit() else 0
        except Exception:
            code = 0
        body = Path(path).read_text(encoding="utf-8", errors="ignore") if path else ""
        return code, body
    except Exception:
        return 0, ""
    finally:
        if path:
            try:
                os.unlink(path)
            except OSError:
                pass


def resolve_mavely_profile_dir() -> Optional[Path]:
    """
    Persistent Chromium user-data dir used by mavely_cookie_refresher (MAVELY_PROFILE_DIR).
    Playwright bridge unwrap reuses this so Cloudflare sees the same logged-in browser.
    """
    repo_root = Path(__file__).resolve().parents[1]
    profile_raw = (os.getenv("MAVELY_PROFILE_DIR", "") or "").strip()
    if profile_raw:
        p = Path(profile_raw)
        if not p.is_absolute():
            p = repo_root / p
    else:
        p = Path(__file__).resolve().parent / ".mavely_profile"
    return p if p.is_dir() else None


def _mavely_bridge_playwright_enabled() -> bool:
    raw = (os.getenv("MAVELY_BRIDGE_PLAYWRIGHT", "") or "").strip().lower()
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    return True


def mavely_bridge_playwright_startup_hint() -> str:
    """Short status string for optional startup logging (see MAVELY_STARTUP_BRIDGE_HINT)."""
    try:
        import playwright.sync_api  # noqa: F401
    except Exception:
        return "Mavely bridge: Playwright not installed (pip install playwright && playwright install chromium)"
    prof = resolve_mavely_profile_dir()
    if prof is None:
        return "Mavely bridge: no profile dir (set MAVELY_PROFILE_DIR or run mavely_cookie_refresher to create .mavely_profile)"
    return f"Mavely bridge: Playwright fallback ready (profile={prof})"


_mavely_playwright_last_error: str = ""


def _mavely_playwright_nav_extra_headers(url: str) -> Dict[str, str]:
    """
    Extra HTTP headers for Playwright navigations to Mavely hubs.

    Default is **empty**: persistent Chromium already has cookies + realistic client hints
    in the profile; forcing MAVELY_COOKIES / MAVELY_USER_AGENT / Sec-Fetch-* from aiohttp
    can invalidate cf_clearance or look unlike a real top-level navigation.

    Set MAVELY_PLAYWRIGHT_MERGE_HUB_HEADERS=1 to restore the old aiohttp-matching set
    (User-Agent, Accept, Accept-Language, Cookie, Referer) for debugging.
    """
    raw = (os.getenv("MAVELY_PLAYWRIGHT_MERGE_HUB_HEADERS", "") or "").strip().lower()
    if raw not in {"1", "true", "yes", "y", "on"}:
        return {}
    h = _html_fetch_headers_for_hub(url)
    return {k: h[k] for k in ("User-Agent", "Accept", "Accept-Language", "Cookie", "Referer") if h.get(k)}


def _fetch_mavely_html_via_playwright_sync(url: str, timeout_s: int) -> str:
    """
    Load a Mavely hub URL in Chromium with the cookie-refresher persistent profile.
    When aiohttp/curl only see a Cloudflare challenge, this often returns real HTML/__NEXT_DATA__.
    """
    global _mavely_playwright_last_error
    _mavely_playwright_last_error = ""
    try:
        from playwright.sync_api import sync_playwright
    except Exception as ex:
        _mavely_playwright_last_error = "import: %s" % (ex,)
        return ""
    u = (url or "").strip()
    if not u.startswith("http"):
        return ""
    prof = resolve_mavely_profile_dir()
    if prof is None:
        return ""
    t_ms = max(10_000, min(int(float(timeout_s) * 1000), 120_000))
    launch_args = ["--disable-session-crashed-bubble", "--disable-dev-shm-usage"]
    # Headless Chromium on Ubuntu (Oracle) typically needs --no-sandbox; don't rely only on systemd env.
    _pw_nosb = (os.getenv("MAVELY_PLAYWRIGHT_NO_SANDBOX", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    if _pw_nosb or sys.platform.startswith("linux"):
        launch_args.append("--no-sandbox")
    launch_args.extend(
        (
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
        )
    )
    headless = (os.getenv("MAVELY_PLAYWRIGHT_HEADLESS", "1") or "").strip().lower() not in {"0", "false", "no", "n", "off"}
    try:
        with sync_playwright() as p:
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(prof),
                headless=headless,
                args=launch_args,
                viewport={"width": 1280, "height": 720},
            )
            try:
                page = ctx.new_page()
                try:
                    _extra = _mavely_playwright_nav_extra_headers(u)
                    if _extra:
                        page.set_extra_http_headers(_extra)
                except Exception:
                    pass
                try:
                    page.add_init_script(
                        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                    )
                except Exception:
                    pass
                page.goto(u, wait_until="load", timeout=t_ms)
                try:
                    page.wait_for_load_state("load", timeout=min(25_000, max(5_000, t_ms // 2)))
                except Exception:
                    pass
                try:
                    page.wait_for_timeout(3500)
                except Exception:
                    pass
                # Hubs often client-navigate to Walmart/Amazon after hydration (same as clicking the short link in a browser).
                # aiohttp/curl only see the bridge HTML; wait until the address bar leaves Mavely or timeout.
                poll_budget_s = min(55.0, max(12.0, (t_ms / 1000.0) - 5.0))
                deadline = time.time() + poll_budget_s
                while time.time() < deadline:
                    try:
                        cur = (page.url or "").strip()
                    except Exception:
                        cur = ""
                    if cur.startswith("http") and (not _url_is_mavely_bridge_surface(cur)):
                        esc = _html.escape(cur, quote=True)
                        return (
                            '<!DOCTYPE html><html><body>'
                            f'<a href="{esc}">outbound</a></body></html>'
                        )
                    try:
                        loc = page.evaluate("() => String(location && location.href ? location.href : '')")
                        if (
                            isinstance(loc, str)
                            and loc.startswith("http")
                            and (not _url_is_mavely_bridge_surface(loc))
                        ):
                            esc = _html.escape(loc.strip(), quote=True)
                            return (
                                '<!DOCTYPE html><html><body>'
                                f'<a href="{esc}">outbound</a></body></html>'
                            )
                    except Exception:
                        pass
                    try:
                        page.wait_for_timeout(500)
                    except Exception:
                        break
                # Some hubs only navigate after a real click (headless may not auto-redirect).
                try:
                    if _url_is_mavely_bridge_surface((page.url or "").strip()):
                        for js in (
                            "() => { const a = document.querySelector('a[href*=\"walmart.com\"]'); if (a) { a.click(); return 1; } return 0; }",
                            "() => { const a = document.querySelector('a[href*=\"amazon.\"]'); if (a) { a.click(); return 1; } return 0; }",
                            "() => { const a = document.querySelector('a[href*=\"tools.woot.com\"]'); if (a) { a.click(); return 1; } return 0; }",
                        ):
                            try:
                                if int(page.evaluate(js) or 0):
                                    page.wait_for_timeout(3500)
                            except Exception:
                                pass
                        for _ in range(24):
                            try:
                                cur = (page.url or "").strip()
                            except Exception:
                                cur = ""
                            if cur.startswith("http") and (not _url_is_mavely_bridge_surface(cur)):
                                esc = _html.escape(cur, quote=True)
                                return (
                                    '<!DOCTYPE html><html><body>'
                                    f'<a href="{esc}">outbound</a></body></html>'
                                )
                            try:
                                page.wait_for_timeout(500)
                            except Exception:
                                break
                except Exception:
                    pass
                return page.content() or ""
            finally:
                ctx.close()
    except Exception as ex:
        _mavely_playwright_last_error = "launch/run: %s" % (ex,)
        return ""


_playwright_mavely_lock: Optional[asyncio.Lock] = None


def _playwright_mavely_async_lock() -> asyncio.Lock:
    global _playwright_mavely_lock
    if _playwright_mavely_lock is None:
        _playwright_mavely_lock = asyncio.Lock()
    return _playwright_mavely_lock


def _extract_meta_canonical_urls(html: str) -> List[str]:
    t = html or ""
    out: List[str] = []
    for pat in (
        r'<link[^>]+rel=["\']canonical["\'][^>]*href=["\']([^"\']+)',
        r'<link[^>]+href=["\']([^"\']+)[^>]*rel=["\']canonical["\']',
        r'<meta[^>]+property=["\']og:url["\'][^>]*content=["\']([^"\']+)',
        r'<meta[^>]+content=["\']([^"\']+)[^>]*property=["\']og:url["\']',
    ):
        try:
            for m in re.finditer(pat, t[:500_000], re.IGNORECASE):
                u = _html.unescape((m.group(1) or "").strip())
                if u.startswith("http://") or u.startswith("https://"):
                    out.append(u)
        except Exception:
            pass
    return out


def _walk_json_collect_http_urls(obj: Any, acc: List[str], *, max_strings: int = 4000) -> None:
    if len(acc) >= max_strings:
        return
    if isinstance(obj, str):
        s = obj.strip()
        if len(s) > 11 and (s.startswith("https://") or s.startswith("http://")):
            acc.append(s)
    elif isinstance(obj, dict):
        for v in obj.values():
            _walk_json_collect_http_urls(v, acc, max_strings=max_strings)
    elif isinstance(obj, list):
        for v in obj:
            _walk_json_collect_http_urls(v, acc, max_strings=max_strings)


def _extract_next_data_http_urls(html: str) -> List[str]:
    t = (html or "")[:900_000]
    try:
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>([\s\S]*?)</script>',
            t,
            re.IGNORECASE,
        )
    except Exception:
        m = None
    if not m:
        return []
    raw = (m.group(1) or "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    acc: List[str] = []
    _walk_json_collect_http_urls(data, acc)
    return acc


_HTML_OUTBOUND_DENY_HOSTS = {
    "howl.link",
    "howl.me",
    "www.cloudflare.com",
    "cloudflare.com",
    "www.googletagmanager.com",
    "googletagmanager.com",
    "google-analytics.com",
    "www.google-analytics.com",
    "doubleclick.net",
    "facebook.com",
    "www.facebook.com",
    "tiktok.com",
    "www.tiktok.com",
    "mavelyinfluencer.com",
    "www.mavelyinfluencer.com",
    "mavely.app.link",
    "joinmavely.com",
    "www.joinmavely.com",
}


def _host_matches_deny_outbound(host: str) -> bool:
    h = (host or "").strip().lower()
    if h.startswith("www."):
        h = h[4:]
    if not h:
        return True
    if h in _HTML_OUTBOUND_DENY_HOSTS:
        return True
    if _is_mavely_or_join_host(h):
        return True
    return False


def _score_merchant_outbound_url(url: str) -> int:
    """Higher = more likely the real product/deal destination."""
    u = (url or "").strip()
    if not u:
        return -1
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return -1
    if _host_matches_deny_outbound(host):
        return -1
    if path.endswith((".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".woff", ".woff2")):
        return -1
    score = min(len(path) + len(p.query or ""), 500)
    hl = host.lower()
    if "amazon." in hl or hl.endswith("amazon.com") or "amzn.to" in hl:
        score += 130
    elif any(
        x in hl
        for x in (
            "walmart.com",
            "woot.com",
            "target.com",
            "samsclub.com",
            "costco.com",
            "kohls.com",
            "bestbuy.com",
            "lowes.com",
            "homedepot.com",
            "macys.com",
            "nordstrom.com",
            "ebay.com",
            "timberland.com",
        )
    ):
        score += 120
    if "/offers/" in path or "/product" in path or "/dp/" in path or "/join/" in path:
        score += 40
    if "woot.com" in hl:
        if hl.startswith("account.") or hl.startswith("auth."):
            score -= 220
        if "/welcome" in path or "/signin" in path or "/signup" in path or "/authorize" in path:
            score -= 140
    if ("amazon." in hl or hl.endswith("amazon.com")) and ("/ap/signin" in path or "/ap/register" in path):
        score -= 220
    return score


def _expand_amazon_signin_return_url(url: str) -> Optional[str]:
    """
    Mavely/Amazon HTML sometimes surfaces www.amazon.com/ap/signin?...&openid.return_to=https%3A%2F%2Fwww.amazon.com%2Fdp%2F...
    Prefer the decoded return_to (product/browse destination) over the sign-in interstitial.
    """
    u = (url or "").strip()
    if not u:
        return None
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return None
    if not (("amazon." in host) or host.endswith("amazon.com")):
        return None
    if "/ap/signin" not in path:
        return None
    v = ""
    for k, val in parse_qsl(p.query or "", keep_blank_values=True):
        if (k or "").strip().lower() == "openid.return_to":
            v = (val or "").strip()
            break
    if not v:
        return None
    v2 = unquote(v)
    if "%" in v2:
        v2 = unquote(v2)
    if not (v2.startswith("http://") or v2.startswith("https://")):
        return None
    try:
        h2 = (urlparse(v2).netloc or "").lower()
    except Exception:
        return None
    if ("amazon." in h2) or h2.endswith("amazon.com"):
        return v2
    return None


def _expand_woot_gatekeeper_url(url: str) -> Optional[str]:
    """
    Woot often links via account.woot.com/welcome?...&returnUrl=https%3A%2F%2Fwww.woot.com%2Foffers%2F...
    Prefer the decoded returnUrl (real offer) over the signup gate page.
    """
    u = (url or "").strip()
    if not u:
        return None
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return None
    if "woot.com" not in host:
        return None
    is_gate = host.startswith("account.") or host.startswith("auth.")
    if not is_gate and "/welcome" not in path and "/signin" not in path and "/signup" not in path:
        return None
    q = {k.lower(): v for k, v in parse_qsl(p.query or "", keep_blank_values=True)}
    for key in ("returnurl", "redirect", "redirect_uri", "redirecturi", "next", "destination", "continue"):
        v = (q.get(key) or "").strip()
        if not v:
            continue
        v2 = unquote(v)
        if "%" in v2:
            v2 = unquote(v2)
        if not (v2.startswith("http://") or v2.startswith("https://")):
            continue
        try:
            h2 = (urlparse(v2).netloc or "").lower()
        except Exception:
            continue
        if "woot.com" in h2:
            return v2
    return None


def _expand_gatekeeper_url(url: str) -> Optional[str]:
    """Peel merchant destination out of Woot welcome or Amazon sign-in interstitials."""
    a = _expand_amazon_signin_return_url(url)
    if a:
        return a
    return _expand_woot_gatekeeper_url(url)


def _pick_best_merchant_url_from_candidates(urls: List[str]) -> Optional[str]:
    best_u = ""
    best_s = -1
    seen: set = set()
    for u in urls or []:
        u2 = (u or "").strip()
        if not u2 or u2 in seen:
            continue
        seen.add(u2)
        s = _score_merchant_outbound_url(u2)
        if s > best_s:
            best_s = s
            best_u = u2
    return best_u if best_s > 0 else None


def _is_cloudflare_or_cdn_error_landing(url: str) -> bool:
    """
    Redirect expansion (e.g. mavely.app.link) can end on Cloudflare's generic 5xx page when the
    origin is down — not a real merchant URL. Never use this as affiliate target or pass-through.
    """
    try:
        p = urlparse((url or "").strip())
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return False
    if not host:
        return False
    if "cloudflare.com" in host and (
        "5xx" in path
        or "error-landing" in path
        or "/cdn-cgi/" in path
        or path.rstrip("/").endswith("/cdn-cgi")
    ):
        return True
    return False


def is_amazon_like_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return (
        ("amazon." in host)
        or host.endswith("amazon.com")
        or host.endswith("amazon.co.uk")
        or ("amzn.to" in host)
        or host == "a.co"
        or host.endswith(".a.co")
    )


def extract_asin(text_or_url: str) -> Optional[str]:
    s = (text_or_url or "").strip()
    if not s:
        return None

    # Prefer extracting ASINs ONLY from well-known Amazon product URL path forms.
    # This avoids false positives like matching a 10-char keyword in a search query.
    for pat in (
        r"/dp/([A-Z0-9]{10})(?:[/?]|$)",
        r"/gp/product/([A-Z0-9]{10})(?:[/?]|$)",
        r"/gp/aw/d/([A-Z0-9]{10})(?:[/?]|$)",
        r"/product/([A-Z0-9]{10})(?:[/?]|$)",
    ):
        m = re.search(pat, s, re.IGNORECASE)
        if m:
            return (m.group(1) or "").upper()

    # Only fall back to "bare token" extraction when the input does NOT look like a URL.
    # (Prevents matching e.g. SMARTPHONE in ".../s?k=...Smartphone...")
    low = s.lower()
    looks_like_url = ("://" in low) or ("amazon." in low) or ("amzn.to" in low) or ("www." in low) or ("/" in low) or ("?" in low)
    if looks_like_url:
        return None

    m2 = re.search(r"\b([A-Z0-9]{10})\b", s.upper())
    return (m2.group(1) or "").upper() if m2 else None


def build_amazon_affiliate_url(cfg: dict, raw_url: str) -> Optional[str]:
    u = (raw_url or "").strip()
    if not u:
        return None
    associate_tag = _cfg_or_env_str(cfg, "amazon_associate_tag", "AMAZON_ASSOCIATE_TAG")

    asin = extract_asin(u)
    if not asin:
        # If we don't have an ASIN, we can still tag real Amazon URLs (search, promo pages, etc).
        # Don't try to tag amzn.to short links directly; expand those first.
        if not associate_tag:
            return None
        try:
            host = (urlparse(u).netloc or "").lower()
        except Exception:
            host = ""
        if ("amazon." in host) or host.endswith("amazon.com") or host.endswith("amazon.co.uk"):
            return _add_query_param(u, "tag", associate_tag)
        # a.co is Amazon's shortener; without expansion we cannot tag reliably.
        if host == "a.co" or host.endswith(".a.co"):
            return None
        return None

    marketplace = _cfg_or_env_str(cfg, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
    if marketplace:
        canon_url = f"{marketplace}/dp/{asin}"
    else:
        try:
            parsed = urlparse(u)
            scheme = parsed.scheme or "https"
            host = parsed.netloc or "www.amazon.com"
            canon_url = f"{scheme}://{host}/dp/{asin}"
        except Exception:
            canon_url = f"https://www.amazon.com/dp/{asin}"

    if associate_tag:
        return _add_query_param(canon_url, "tag", associate_tag)
    return canon_url


_ALIAS_ALPHABET = string.ascii_lowercase + string.digits


def _make_alias_slug(length: int = 7) -> str:
    n = max(4, min(int(length or 7), 20))
    return "".join(secrets.choice(_ALIAS_ALPHABET) for _ in range(n))


def discord_masked_link(display_prefix: str, target_url: str, *, slug_len: int = 7) -> str:
    prefix = (display_prefix or "amzn.to").strip().rstrip("/")
    target = (target_url or "").strip()
    slug = _make_alias_slug(slug_len)
    return f"[{prefix}/{slug}](<{target}>)"


def _b64url_decode_text(data: str) -> Optional[str]:
    s = (data or "").strip()
    if not s:
        return None
    try:
        pad = "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s + pad).decode("utf-8", errors="ignore")
    except Exception:
        return None


def _b64_decode_text(data: str) -> Optional[str]:
    """
    Standard base64 decode (not urlsafe). Used for some bot-challenge pages that embed the
    destination URL as base64 under a `b=` parameter.
    """
    s = (data or "").strip()
    if not s:
        return None
    try:
        pad = "=" * ((4 - (len(s) % 4)) % 4)
        return base64.b64decode(s + pad).decode("utf-8", errors="ignore")
    except Exception:
        return None


def _normalize_expanded_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    try:
        parsed = urlparse(u)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "")
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    except Exception:
        return u
    if "walmart.com" in host and path.startswith("/blocked") and q.get("url"):
        decoded = _b64url_decode_text(q.get("url") or "")
        if decoded:
            decoded = decoded.strip()
            if decoded.startswith("http://") or decoded.startswith("https://"):
                return decoded
            if decoded.startswith("/"):
                return f"{parsed.scheme or 'https'}://{parsed.netloc}{decoded}"
    return u


def _expand_hosts_from_env() -> set:
    raw = (os.getenv("AUTO_AFFILIATE_EXPAND_HOSTS", "") or "").strip()
    if not raw:
        return set()
    hosts = set()
    for part in raw.replace("\n", ",").split(","):
        h = (part or "").strip().lower()
        if h:
            hosts.add(h)
    return hosts


def should_expand_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if not host:
        return False
    env_hosts = _expand_hosts_from_env()
    if host in env_hosts:
        return True
    common = {
        "dealshacks.com",
        "www.dealshacks.com",
        "bit.ly",
        "t.co",
        "tinyurl.com",
        "goo.gl",
        "rebrand.ly",
        "cutt.ly",
        "rb.gy",
        "is.gd",
        "s.id",
        "linktr.ee",
        "trackcm.com",
        "walmrt.us",
        "amzn.to",
        "a.co",
        "www.a.co",
        "mavely.app.link",
        "mavelyinfluencer.com",
        "www.mavelyinfluencer.com",
        # Redirect chains seen from go.sylikes.com -> rd.bizrate.com -> go.skimresources.com -> merchant
        "go.sylikes.com",
        "rd.bizrate.com",
        "go.skimresources.com",
        "howl.link",
        "howl.me",
        "deals.pennyexplorer.com",
        "dealsabove.com",
        "www.dealsabove.com",
        "pricedoffers.com",
        "saveyourdeals.com",
        "joylink.io",
        "fkd.deals",
        "ringinthedeals.com",
        "dmflip.com",
    }
    return host in common


def unwrap_known_query_redirects(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u:
        return None
    try:
        parsed = urlparse(u)
        host = (parsed.netloc or "").lower()
        q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    except Exception:
        return None
    if host == "fkd.deals":
        cand = (q.get("product") or "").strip()
        if cand.startswith("http://") or cand.startswith("https://"):
            return cand
    if host == "rd.bizrate.com":
        # Example:
        #   https://rd.bizrate.com/rd2?t=https%3A%2F%2Fgo.skimresources.com%3F...%26url%3Dhttps%253A%252F%252Fwww.lowes.com%252F...
        cand = (q.get("t") or q.get("url") or q.get("u") or "").strip()
        if cand:
            # Typically nested URL-encoded once (or more). Decode a few times.
            for _ in range(3):
                nxt = unquote(cand)
                if nxt == cand:
                    break
                cand = nxt
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    if host == "go.skimresources.com":
        # Skimlinks commonly uses url= as the real destination (often double-encoded).
        cand = (q.get("url") or q.get("u") or q.get("dest") or "").strip()
        if cand:
            for _ in range(3):
                nxt = unquote(cand)
                if nxt == cand:
                    break
                cand = nxt
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    if host in {"dealsabove.com", "www.dealsabove.com"}:
        # Example:
        #   https://www.dealsabove.com/product-redirect?l=https%3A%2F%2Fwww.amazon.com%2Fdp%2FB0BGW6DSLW#code=...
        cand = (q.get("l") or q.get("url") or q.get("u") or "").strip()
        if cand:
            cand = unquote(cand)
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    if host == "joylink.io":
        for k in ("url", "u", "target", "dest"):
            cand = (q.get(k) or "").strip()
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    if host.endswith("linksynergy.com"):
        # Rakuten/LinkSynergy deep links commonly carry the real destination under murl=
        # Example:
        #   https://click.linksynergy.com/deeplink?...&murl=https://www.urbanoutfitters.com/shop/...
        cand = (q.get("murl") or "").strip()
        if cand:
            cand = unquote(cand)
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    return None


def _extract_first_outbound_url_from_html(html: str) -> Optional[str]:
    """
    Pull the real merchant/deal URL from hub HTML (Mavely bridge, deal sites, etc.).
    Mavely often serves Next.js: destination lives in __NEXT_DATA__ JSON, not visible <a> tags.
    """
    t = (html or "")[:900_000]
    if not t:
        return None

    candidates: List[str] = []

    # PerimeterX/other bot challenges often embed the real destination URL as base64 (b=...).
    try:
        m_b = re.search(r"[?&]b=([A-Za-z0-9+/=_-]{40,})", t)
    except Exception:
        m_b = None
    if m_b:
        decoded = _b64_decode_text((m_b.group(1) or "").strip()) or _b64url_decode_text((m_b.group(1) or "").strip())
        if decoded:
            decoded = decoded.strip()
            if decoded.startswith("http://") or decoded.startswith("https://"):
                candidates.append(decoded)

    candidates.extend(_extract_meta_canonical_urls(t))
    candidates.extend(_extract_next_data_http_urls(t))

    for label in (
        "Go to Deal",
        "Continue to Amazon",
        "Claim Amazon Deal",
        "Claim Deal",
        "Shop Now",
        "Shop now",
        "View Deal",
        "Get deal",
        "Continue",
    ):
        try:
            m_btn = re.search(rf'href=["\']([^"\']+)["\'][^>]*>\s*{re.escape(label)}', t, re.IGNORECASE)
        except Exception:
            m_btn = None
        if m_btn:
            u = _html.unescape((m_btn.group(1) or "").strip())
            if u.startswith("http://") or u.startswith("https://"):
                candidates.append(u)

    patterns = [
        r"https?://(?:www\.)?amazon\.[^\s\"'<>]+",
        r"https?://amzn\.to/[A-Za-z0-9]+",
        r"https?://saveyourdeals\.com/[A-Za-z0-9]+",
        r"https?://(?:www\.)?dealsabove\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?walmart\.com/[^\s\"'<>]+",
        r"https?://(?:www\.|tools\.)woot\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?samsclub\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?costco\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?kohls\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?bestbuy\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?lowes\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?homedepot\.com/[^\s\"'<>]+",
        r"https?://walmrt\.us/[A-Za-z0-9]+",
        r"https?://(?:www\.)?target\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?urbanoutfitters\.[^\s\"'<>]+",
        r"https?://(?:www\.)?timberland\.com/[^\s\"'<>]+",
        r"https?://bit\.ly/[A-Za-z0-9]+",
    ]
    for pat in patterns:
        try:
            m = re.search(pat, t, re.IGNORECASE)
        except Exception:
            m = None
        if m:
            u = _html.unescape((m.group(0) or "").strip())
            if u.startswith("http://") or u.startswith("https://"):
                candidates.append(u)

    try:
        hrefs = re.findall(r'href=["\'](https?://[^"\']+)["\']', t, re.IGNORECASE)
    except Exception:
        hrefs = []
    deny_exts = (".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".woff", ".woff2", ".ttf")
    for h in hrefs[:100]:
        cand = _html.unescape((h or "").strip())
        if not (cand.startswith("http://") or cand.startswith("https://")):
            continue
        try:
            host = (urlparse(cand).netloc or "").lower()
        except Exception:
            host = ""
        if _host_matches_deny_outbound(host):
            continue
        if cand.lower().split("?", 1)[0].endswith(deny_exts):
            continue
        candidates.append(cand)

    # Peel gatekeeper interstitials (Woot welcome returnUrl, Amazon ap/signin openid.return_to).
    peeled: List[str] = []
    for u in list(candidates):
        g = _expand_gatekeeper_url(u)
        if g:
            peeled.append(g)
    candidates.extend(peeled)
    try:
        for m in re.finditer(r"(?:returnUrl|return_url)=([^&\s\"'<>]+)", t[:400_000], re.IGNORECASE):
            raw = (m.group(1) or "").strip()
            cand = unquote(raw)
            if "%" in cand:
                cand = unquote(cand)
            if not (cand.startswith("http://") or cand.startswith("https://")):
                continue
            try:
                ph = (urlparse(cand).path or "").lower()
                hh = (urlparse(cand).netloc or "").lower()
            except Exception:
                continue
            if "woot.com" in hh and "/offers/" in ph:
                candidates.append(cand)
        for m in re.finditer(r"openid\.return_to=([^&\s\"'<>]+)", t[:400_000], re.IGNORECASE):
            raw = (m.group(1) or "").strip()
            cand = unquote(raw)
            if "%" in cand:
                cand = unquote(cand)
            if not (cand.startswith("http://") or cand.startswith("https://")):
                continue
            try:
                ph = (urlparse(cand).path or "").lower()
                hh = (urlparse(cand).netloc or "").lower()
            except Exception:
                continue
            if ("amazon." in hh or hh.endswith("amazon.com")) and (
                "/dp/" in ph or "/gp/" in ph or "/d/" in ph or "/deal/" in ph
            ):
                candidates.append(cand)
    except Exception:
        pass

    best = _pick_best_merchant_url_from_candidates(candidates)
    if best:
        for _ in range(4):
            nxt = _expand_gatekeeper_url(best)
            if nxt and nxt != best:
                best = nxt
            else:
                break
    return best


def _first_production_outbound_from_hub_html(html: str) -> Optional[str]:
    """
    First merchant-looking URL from hub HTML, or None if the extractor only finds another Mavely surface.
    Without this, Oracle often gets a 200 + __NEXT_DATA__ page whose *first* match is still mavelyinfluencer.com,
    which incorrectly skipped Playwright (truthy extract → need_pw False).
    """
    o = _extract_first_outbound_url_from_html(html or "")
    if not o:
        return None
    if _url_is_mavely_bridge_surface(o):
        return None
    return o


async def expand_url(session: aiohttp.ClientSession, url: str, *, timeout_s: float = 8.0, max_redirects: int = 8) -> str:
    u = (url or "").strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return u
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    headers = _expand_redirect_headers(u)
    # Branch / Mavely often omit or mishandle HEAD; follow with GET + document headers (same idea as hub GET).
    mavely_like = _url_is_mavely_bridge_surface(u)
    if not mavely_like:
        try:
            async with session.request(
                "HEAD",
                u,
                allow_redirects=True,
                max_redirects=max_redirects,
                timeout=timeout,
                headers=headers,
            ) as resp:
                return _normalize_expanded_url(str(resp.url) or u)
        except Exception:
            pass
    try:
        async with session.get(
            u,
            allow_redirects=True,
            max_redirects=max_redirects,
            timeout=timeout,
            headers=headers,
        ) as resp:
            try:
                await resp.content.read(0)
            except Exception:
                pass
            return _normalize_expanded_url(str(resp.url) or u)
    except Exception:
        pass
    try:
        import requests

        def _do() -> str:
            r = requests.get(u, allow_redirects=True, timeout=max(5, int(timeout_s)), headers=dict(headers))
            return r.url or u

        final = await asyncio.to_thread(_do)
        return _normalize_expanded_url(final or u)
    except Exception:
        return u


def _mavely_cookie_file_path() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    explicit = (os.getenv("MAVELY_COOKIES_FILE", "") or "").strip()
    if explicit:
        p = Path(explicit)
        return p if p.is_absolute() else (repo_root / p)
    return Path(__file__).parent / "mavely_cookies.txt"


def _reload_mavely_cookies_from_file(force: bool = False) -> bool:
    try:
        if (not force) and (os.getenv("MAVELY_COOKIES", "") or "").strip():
            return False
        path = _mavely_cookie_file_path()
        if not path.exists():
            return False
        raw = (path.read_text(encoding="utf-8") or "").strip()
        if not raw:
            return False
        os.environ["MAVELY_COOKIES"] = raw
        return True
    except Exception:
        return False


def _mavely_auto_refresh_enabled() -> bool:
    raw = (os.getenv("MAVELY_AUTO_REFRESH_ON_FAIL", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _mavely_auto_refresh_cooldown_s() -> int:
    try:
        v = int((os.getenv("MAVELY_AUTO_REFRESH_COOLDOWN_S", "") or "").strip() or "600")
    except Exception:
        v = 600
    return max(60, min(v, 24 * 3600))


async def _maybe_refresh_mavely_cookies(reason: str) -> bool:
    if not _mavely_auto_refresh_enabled():
        return False
    cooldown = _mavely_auto_refresh_cooldown_s()
    if not _log_once(f"mavely_cookie_refresh:{reason}", seconds=cooldown):
        return False
    if _reload_mavely_cookies_from_file(force=True):
        return True
    script = Path(__file__).parent / "mavely_cookie_refresher.py"
    if not script.exists():
        return False

    def _run() -> int:
        try:
            return subprocess.call([sys.executable, str(script)], cwd=str(Path(__file__).parent))
        except Exception:
            return 1

    code = await asyncio.to_thread(_run)
    if code != 0:
        return False
    return _reload_mavely_cookies_from_file(force=True)


def _import_mavely_client():
    try:
        from .mavely_client import MavelyClient  # type: ignore
        return MavelyClient
    except Exception:
        return None


def _apply_env_from_cfg(cfg: dict) -> None:
    """
    RSForwarder loads secrets into a JSON config dict, but some underlying helpers
    (and the canonical Mavely client) read from environment variables.
    Bridge selected values from cfg -> os.environ (only if env not already set).
    """
    try:
        mapping = {
            # OAuth refresh (optional)
            "mavely_refresh_token": "MAVELY_REFRESH_TOKEN",
            "mavely_refresh_token_file": "MAVELY_REFRESH_TOKEN_FILE",
            "mavely_enable_oauth_refresh": "MAVELY_ENABLE_OAUTH_REFRESH",
            "mavely_token_endpoint": "MAVELY_TOKEN_ENDPOINT",
            "mavely_client_id": "MAVELY_CLIENT_ID",
            "mavely_auth_audience": "MAVELY_AUTH_AUDIENCE",
            "mavely_auth_scope": "MAVELY_AUTH_SCOPE",
            # Cookie refresh helper (optional)
            "mavely_cookies_file": "MAVELY_COOKIES_FILE",
            "mavely_auto_refresh_on_fail": "MAVELY_AUTO_REFRESH_ON_FAIL",
            "mavely_auto_refresh_cooldown_s": "MAVELY_AUTO_REFRESH_COOLDOWN_S",
            "mavely_profile_dir": "MAVELY_PROFILE_DIR",

            "mavely_id_token": "MAVELY_ID_TOKEN",
            "mavely_base_url": "MAVELY_BASE_URL",
            "mavely_user_agent": "MAVELY_USER_AGENT",
            "mavely_sec_ch_ua": "MAVELY_SEC_CH_UA",
            "mavely_sec_ch_ua_mobile": "MAVELY_SEC_CH_UA_MOBILE",
            "mavely_sec_ch_ua_platform": "MAVELY_SEC_CH_UA_PLATFORM",
            "mavely_sec_fetch_site": "MAVELY_SEC_FETCH_SITE",
            "mavely_sec_fetch_mode": "MAVELY_SEC_FETCH_MODE",
            "mavely_sec_fetch_dest": "MAVELY_SEC_FETCH_DEST",
            "mavely_priority": "MAVELY_PRIORITY",
        }
        for cfg_key, env_key in mapping.items():
            v = str((cfg or {}).get(cfg_key) or "").strip()
            if not v:
                continue
            if (os.getenv(env_key, "") or "").strip():
                continue
            os.environ[env_key] = v
    except Exception:
        pass


async def mavely_create_link(cfg: dict, url: str) -> Tuple[Optional[str], Optional[str]]:
    MavelyClient = _import_mavely_client()
    if MavelyClient is None:
        return None, "Mavely client not available."

    _apply_env_from_cfg(cfg)

    session_token = (os.getenv("MAVELY_COOKIES", "") or "").strip()
    if not session_token:
        # try cookie file
        _reload_mavely_cookies_from_file(force=True)
        session_token = (os.getenv("MAVELY_COOKIES", "") or "").strip()
    auth_token = _cfg_or_env_str(cfg, "mavely_auth_token", "MAVELY_AUTH_TOKEN")
    graphql_endpoint = _cfg_or_env_str(cfg, "mavely_graphql_endpoint", "MAVELY_GRAPHQL_ENDPOINT")
    if not session_token and not auth_token:
        return None, "Missing MAVELY cookies/session (or MAVELY_AUTH_TOKEN)."

    timeout_s = int(_cfg_or_env_int(cfg, "mavely_request_timeout", "REQUEST_TIMEOUT") or 20)
    max_retries = int(_cfg_or_env_int(cfg, "mavely_max_retries", "MAX_RETRIES") or 3)
    try:
        min_seconds = float((cfg or {}).get("mavely_min_seconds_between_requests") or (os.getenv("MIN_SECONDS_BETWEEN_REQUESTS", "") or "").strip() or "2.0")
    except Exception:
        min_seconds = 2.0

    def _do() -> Tuple[Optional[str], str, int]:
        client = MavelyClient(
            session_token=session_token,
            auth_token=auth_token or None,
            graphql_endpoint=graphql_endpoint or None,
            timeout_s=timeout_s,
            max_retries=max_retries,
            min_seconds_between_requests=min_seconds,
        )
        # If persistence is enabled via MAVELY_REFRESH_TOKEN_FILE and we don't have a refresh token yet,
        # sync from /api/auth/session once so token rotation is handled automatically.
        try:
            rt_file = (os.getenv("MAVELY_REFRESH_TOKEN_FILE", "") or "").strip()
            if rt_file and (not getattr(client, "refresh_token", "")) and getattr(client, "cookie_header", ""):
                import requests

                sess = requests.Session()
                client._ensure_auth_token_from_session(sess, force=True)  # updates refreshToken/idToken too
        except Exception:
            pass
        res = client.create_link((url or "").strip())
        link = res.mavely_link if getattr(res, "ok", False) else None
        err = "" if link else (getattr(res, "error", None) or "Failed to generate Mavely link.")
        status = int(getattr(res, "status_code", 0) or 0)
        return link, str(err), status

    link, err, status = await asyncio.to_thread(_do)
    if link:
        return link, None

    err_l = (err or "").lower()
    auth_fail = ("token expired" in err_l) or ("not logged in" in err_l) or ("unauthorized" in err_l) or (status == 401)
    if auth_fail:
        if await _maybe_refresh_mavely_cookies(reason=err or "auth"):
            link2, err2, _status2 = await asyncio.to_thread(_do)
            if link2:
                return link2, None
            err = err2 or err

    # If we still got a 401, call out the one-time server login requirement explicitly.
    if status == 401:
        hint = " (need server login: run RSForwarder/mavely_cookie_refresher.py --interactive on Oracle once)"
    else:
        hint = ""
    return None, f"{err} (status={status}){hint}"


async def mavely_preflight(cfg: dict) -> Tuple[bool, int, Optional[str]]:
    """
    Non-mutating health check for Mavely auth.

    IMPORTANT:
    - Does NOT create affiliate links (avoids "startup spam" in your Mavely dashboard).
    - Uses MavelyClient.preflight(), which validates cookies via /api/auth/session and may
      refresh bearer token if OAuth refresh is enabled.

    Returns: (ok, status_code, error_message)
    """
    MavelyClient = _import_mavely_client()
    if MavelyClient is None:
        return False, 0, "Mavely client not available."

    _apply_env_from_cfg(cfg)

    session_token = (os.getenv("MAVELY_COOKIES", "") or "").strip()
    if not session_token:
        _reload_mavely_cookies_from_file(force=True)
        session_token = (os.getenv("MAVELY_COOKIES", "") or "").strip()

    auth_token = _cfg_or_env_str(cfg, "mavely_auth_token", "MAVELY_AUTH_TOKEN")
    graphql_endpoint = _cfg_or_env_str(cfg, "mavely_graphql_endpoint", "MAVELY_GRAPHQL_ENDPOINT")
    timeout_s = int(_cfg_or_env_int(cfg, "mavely_request_timeout", "REQUEST_TIMEOUT") or 20)
    max_retries = int(_cfg_or_env_int(cfg, "mavely_max_retries", "MAX_RETRIES") or 1)
    try:
        min_seconds = float((cfg or {}).get("mavely_min_seconds_between_requests") or (os.getenv("MIN_SECONDS_BETWEEN_REQUESTS", "") or "").strip() or "0.0")
    except Exception:
        min_seconds = 0.0

    def _do() -> Tuple[bool, int, Optional[str]]:
        client = MavelyClient(
            session_token=session_token,
            auth_token=auth_token or None,
            graphql_endpoint=graphql_endpoint or None,
            timeout_s=timeout_s,
            max_retries=max_retries,
            min_seconds_between_requests=min_seconds,
        )
        res = client.preflight()
        ok = bool(getattr(res, "ok", False))
        status = int(getattr(res, "status_code", 0) or 0)
        err = getattr(res, "error", None)
        return ok, status, (str(err) if err else None)

    ok, status, err = await asyncio.to_thread(_do)
    return ok, status, err

async def compute_affiliate_rewrites(cfg: dict, urls: List[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (mapped, notes):
    - mapped: original url -> replacement text
    - notes: original url -> short reason
    """
    unique = list(dict.fromkeys([(u or "").strip() for u in (urls or []) if (u or "").strip()]))
    if not unique:
        return {}, {}

    memo_lookup = (cfg or {}).get("_affiliate_compute_memo")
    if isinstance(memo_lookup, dict):
        memo_key = tuple(sorted(unique))
        hit = memo_lookup.get(memo_key)
        if hit is not None:
            if affiliate_rewrite_debug_on(cfg) and affiliate_rewrite_debug_verbose_on(cfg):
                _aff_dbg_verbose(cfg, "compute: memo hit %d url(s) (skipped duplicate work)" % len(unique))
            return dict(hit[0]), dict(hit[1])

    mapped: Dict[str, str] = {}
    notes: Dict[str, str] = {}

    # Mention-safe normalization (skip anything that isn't really a URL).
    normalized: Dict[str, str] = {}
    candidates: List[str] = []
    for u in unique:
        nu = normalize_input_url(u)
        if nu:
            normalized[u] = nu
            candidates.append(u)
        else:
            notes[u] = "not a url"

    if not candidates:
        if affiliate_rewrite_debug_on(cfg):
            bad = [x for x in unique if x not in candidates]
            _aff_dbg(
                cfg,
                "compute_affiliate_rewrites: 0 candidates (all not_a_url or empty); samples=%s"
                % ", ".join(_aff_dbg_clip(x, 72) for x in bad[:6]),
            )
        return {}, notes

    # Stable amazon masks per destination within a message
    amazon_mask_cache: Dict[str, str] = {}

    # Domains that should NOT be affiliate-wrapped (marketplaces, etc).
    # If a URL resolves/expands to one of these, we will avoid Mavely/Amazon affiliate rewriting.
    # Config: `affiliate_skip_domains`: ["ebay.com", "stockx.com", ...]
    skip_domains: List[str] = []
    try:
        raw_sd = (cfg or {}).get("affiliate_skip_domains")
        if isinstance(raw_sd, list):
            skip_domains = [str(x or "").strip().lower() for x in raw_sd if str(x or "").strip()]
    except Exception:
        skip_domains = []

    def _host_matches_skip(host: str) -> bool:
        h = (host or "").strip().lower()
        if h.startswith("www."):
            h = h[4:]
        if not h or not skip_domains:
            return False
        for d in skip_domains:
            dd = (d or "").strip().lower()
            if dd.startswith("www."):
                dd = dd[4:]
            if not dd:
                continue
            if h == dd or h.endswith("." + dd):
                return True
        return False

    expand_enabled = _bool_or_default((cfg or {}).get("affiliate_expand_redirects"), True)
    max_redirects = int(_cfg_or_env_int(cfg, "affiliate_max_redirects", "AUTO_AFFILIATE_MAX_REDIRECTS") or 8)
    timeout_s = float(_cfg_or_env_int(cfg, "affiliate_expand_timeout_s", "AUTO_AFFILIATE_EXPAND_TIMEOUT_S") or 8)
    # Hub HTML (Mavely bridge, deal sites) often needs more time than redirect HEAD/GET; Oracle was stuck at 8s.
    _hub_ht = _cfg_or_env_int(cfg, "affiliate_hub_html_timeout_s", "AUTO_AFFILIATE_HUB_HTML_TIMEOUT_S")
    if _hub_ht is not None and int(_hub_ht) > 0:
        hub_html_timeout_s = float(min(int(_hub_ht), 90))
    else:
        hub_html_timeout_s = min(max(timeout_s, 22.0), 90.0)

    if affiliate_rewrite_debug_on(cfg) and affiliate_rewrite_debug_verbose_on(cfg):
        tag = _cfg_or_env_str(cfg, "amazon_associate_tag", "AMAZON_ASSOCIATE_TAG")
        _aff_dbg_verbose(
            cfg,
            "compute_affiliate_rewrites: candidates=%d expand_redirects=%s timeout_s=%.1f hub_html_timeout_s=%.1f max_redirects=%d skip_domain_rules=%d amazon_tag_configured=%s"
            % (
                len(candidates),
                expand_enabled,
                timeout_s,
                hub_html_timeout_s,
                max_redirects,
                len(skip_domains),
                bool((tag or "").strip()),
            ),
        )

    resolved: Dict[str, str] = {u: normalized.get(u) or u for u in candidates}

    for u in candidates:
        cand = unwrap_known_query_redirects(resolved.get(u) or u)
        if cand:
            _aff_dbg_verbose(
                cfg,
                "  query_unwrap %r -> %r"
                % (_aff_dbg_clip(resolved.get(u) or u, 88), _aff_dbg_clip(cand, 88)),
            )
            resolved[u] = cand

    if expand_enabled:
        _apply_env_from_cfg(cfg)
        _reload_mavely_cookies_from_file(force=False)
        async with aiohttp.ClientSession() as session:
            for u in candidates:
                start_u = (resolved.get(u) or u).strip()
                dbg_chain: Optional[List[str]] = (
                    [start_u] if affiliate_rewrite_debug_on(cfg) and affiliate_rewrite_debug_verbose_on(cfg) else None
                )
                if should_expand_url(start_u):
                    final_u = await expand_url(session, start_u, timeout_s=timeout_s, max_redirects=max_redirects)
                    # Mavely / App Links sometimes 302 to Cloudflare's generic 5xx page when origin fails.
                    if _is_cloudflare_or_cdn_error_landing(final_u):
                        if dbg_chain is not None:
                            _aff_dbg_verbose(
                                cfg,
                                "  expand %r: cloudflare/cdn error landing; reverting to pre-expand" % _aff_dbg_clip(u, 72),
                            )
                        final_u = start_u
                    if dbg_chain is not None and final_u != dbg_chain[-1]:
                        dbg_chain.append(final_u)
                    # Chain-expand: redirect stacks often land on another shortener first (e.g. bit.ly -> amzn.to).
                    for _chain in range(4):
                        if not should_expand_url(final_u):
                            break
                        nxt = await expand_url(session, final_u, timeout_s=timeout_s, max_redirects=max_redirects)
                        if _is_cloudflare_or_cdn_error_landing(nxt):
                            break
                        if not nxt or nxt == final_u:
                            break
                        final_u = nxt
                        if dbg_chain is not None:
                            dbg_chain.append(final_u)
                    resolved[u] = final_u
                    if dbg_chain is not None and len(dbg_chain) > 1:
                        _aff_dbg_verbose(
                            cfg,
                            "  expand %r: %s"
                            % (_aff_dbg_clip(u, 72), " -> ".join(_aff_dbg_clip(x, 88) for x in dbg_chain)),
                        )
                    elif dbg_chain is not None:
                        _aff_dbg_verbose(
                            cfg,
                            "  expand %r: single hop (no further shortener chain) -> %r"
                            % (_aff_dbg_clip(u, 72), _aff_dbg_clip(final_u, 88)),
                        )

                    cand2 = unwrap_known_query_redirects(final_u)
                    if cand2:
                        if not _is_cloudflare_or_cdn_error_landing(cand2):
                            resolved[u] = cand2
                            final_u = cand2

                    special_html_hosts = {
                        "deals.pennyexplorer.com",
                        "ringinthedeals.com",
                        "dmflip.com",
                        "trackcm.com",
                        "joylink.io",
                        "fkd.deals",
                        "pricedoffers.com",
                        "saveyourdeals.com",
                        "mavely.app.link",
                        "mavelyinfluencer.com",
                        "www.mavelyinfluencer.com",
                        "go.sylikes.com",
                        "rd.bizrate.com",
                        "go.skimresources.com",
                        "howl.link",
                        "howl.me",
                        # dealshacks.com -> 302 -> hiddendealsociety.com/deal/... (Next.js RSC; outbound URL in payload)
                        "hiddendealsociety.com",
                        "www.hiddendealsociety.com",
                    }

                    # Some hubs require 2 steps:
                    # pricedoffers.com -> saveyourdeals.com -> amazon.com (Go to Deal)
                    candidate = final_u
                    # mavely.app.link: HTTP expand usually lands on mavelyinfluencer.com (tracking shell), not the store.
                    # Branch often completes the hop to the merchant when Playwright opens the *short* URL first; the
                    # influencer hub HTML alone is frequently useless on datacenter IPs (small shell, no __NEXT_DATA__).
                    mavely_short_src = (normalized.get(u) or u).strip()
                    mavely_short_playwright_tried = False
                    if is_mavely_app_short_link(mavely_short_src):
                        if (not _mavely_bridge_playwright_enabled()) or resolve_mavely_profile_dir() is None:
                            if affiliate_rewrite_debug_verbose_on(cfg):
                                _aff_dbg_verbose(
                                    cfg,
                                    "  html_unwrap mavely short-first: skipped (%s)"
                                    % (
                                        "MAVELY_BRIDGE_PLAYWRIGHT off"
                                        if not _mavely_bridge_playwright_enabled()
                                        else "no persistent profile (MAVELY_PROFILE_DIR or RSForwarder/.mavely_profile)"
                                    ),
                                )
                        else:
                            mavely_short_playwright_tried = True
                            if affiliate_rewrite_debug_verbose_on(cfg):
                                _aff_dbg_verbose(
                                    cfg,
                                    "  html_unwrap mavely short-first: playwright starting timeout_s=%s %r (this can take tens of seconds)"
                                    % (
                                        int(hub_html_timeout_s),
                                        _aff_dbg_clip(mavely_short_src, 72),
                                    ),
                                )
                            async with _playwright_mavely_async_lock():
                                pw_short_first = await asyncio.to_thread(
                                    _fetch_mavely_html_via_playwright_sync,
                                    mavely_short_src,
                                    int(hub_html_timeout_s),
                                )
                            out_sf = (
                                _first_production_outbound_from_hub_html(pw_short_first)
                                if pw_short_first
                                else None
                            )
                            if affiliate_rewrite_debug_verbose_on(cfg):
                                _aff_dbg_verbose(
                                    cfg,
                                    "  html_unwrap mavely short-first: playwright done len=%s merchant_out=%s"
                                    % (len(pw_short_first or ""), bool(out_sf)),
                                )
                            if out_sf:
                                out_abs_sf = out_sf
                                if out_abs_sf.startswith("/"):
                                    out_abs_sf = urljoin(mavely_short_src, out_abs_sf)
                                out_abs_sf = unwrap_known_query_redirects(out_abs_sf) or out_abs_sf
                                candidate = out_abs_sf
                                if affiliate_rewrite_debug_verbose_on(cfg):
                                    _aff_dbg_verbose(
                                        cfg,
                                        "  html_unwrap mavely short-first %r -> %r"
                                        % (
                                            _aff_dbg_clip(mavely_short_src, 72),
                                            _aff_dbg_clip(out_abs_sf, 88),
                                        ),
                                    )
                    for _ in range(3):
                        try:
                            parsed = urlparse(candidate)
                            host = (parsed.netloc or "").lower()
                        except Exception:
                            host = ""
                        if host not in special_html_hosts:
                            break
                        try:
                            _hub_headers = _html_fetch_headers_for_hub(candidate)
                            async with session.get(
                                candidate,
                                headers=_hub_headers,
                                timeout=aiohttp.ClientTimeout(total=float(hub_html_timeout_s)),
                            ) as resp:
                                status = int(resp.status or 0)
                                txt = await resp.text(errors="ignore")
                            mv_hub = "mavelyinfluencer.com" in host or "mavely.app.link" in host
                            # Cloudflare often returns 403 to Python/aiohttp even with Mavely cookies; try curl TLS stack.
                            if mv_hub and (status >= 400 or "__NEXT_DATA__" not in (txt or "")):
                                ccode, cbody = await asyncio.to_thread(
                                    _fetch_html_via_curl, candidate, _hub_headers, int(hub_html_timeout_s)
                                )
                                if cbody and ("__NEXT_DATA__" in cbody or _extract_first_outbound_url_from_html(cbody)):
                                    txt = cbody
                            # 200 + __NEXT_DATA__ can still fail extraction (truncated HTML, different shape); curl sometimes differs.
                            elif mv_hub and (not _first_production_outbound_from_hub_html(txt or "")):
                                ccode, cbody = await asyncio.to_thread(
                                    _fetch_html_via_curl, candidate, _hub_headers, int(hub_html_timeout_s)
                                )
                                if cbody and _first_production_outbound_from_hub_html(cbody):
                                    txt = cbody
                            # Real browser + same profile as mavely_cookie_refresher (cf_clearance / session).
                            if _mavely_bridge_playwright_enabled() and (
                                "mavelyinfluencer.com" in host or "mavely.app.link" in host
                            ):
                                have_merchant = bool(_first_production_outbound_from_hub_html(txt or ""))
                                need_pw = (
                                    (status >= 400)
                                    or ("__NEXT_DATA__" not in (txt or ""))
                                    or (not have_merchant)
                                )
                                if need_pw and resolve_mavely_profile_dir() is not None:
                                    if affiliate_rewrite_debug_verbose_on(cfg):
                                        _aff_dbg_verbose(
                                            cfg,
                                            "  html_unwrap playwright(bridge): starting timeout_s=%s %r"
                                            % (
                                                int(hub_html_timeout_s),
                                                _aff_dbg_clip(candidate, 72),
                                            ),
                                        )
                                    async with _playwright_mavely_async_lock():
                                        pw_html = await asyncio.to_thread(
                                            _fetch_mavely_html_via_playwright_sync,
                                            candidate,
                                            int(hub_html_timeout_s),
                                        )
                                    if pw_html:
                                        txt = pw_html
                                        if "__NEXT_DATA__" in pw_html or _extract_first_outbound_url_from_html(
                                            pw_html
                                        ):
                                            _aff_dbg_verbose(
                                                cfg,
                                                "  html_unwrap playwright %r -> len=%s"
                                                % (_aff_dbg_clip(candidate, 72), len(pw_html)),
                                            )
                                        elif affiliate_rewrite_debug_verbose_on(cfg):
                                            hint = (
                                                (" (%s)" % _aff_dbg_clip(_mavely_playwright_last_error, 140))
                                                if (_mavely_playwright_last_error or "").strip()
                                                else ""
                                            )
                                            _aff_dbg_verbose(
                                                cfg,
                                                "  html_unwrap playwright %r -> len=%s (no __NEXT_DATA__/extract yet)%s"
                                                % (_aff_dbg_clip(candidate, 72), len(pw_html), hint),
                                            )
                            out = _first_production_outbound_from_hub_html(txt)
                            if (
                                not out
                                and mv_hub
                                and is_mavely_app_short_link((normalized.get(u) or u).strip())
                                and (not mavely_short_playwright_tried)
                                and _mavely_bridge_playwright_enabled()
                                and resolve_mavely_profile_dir() is not None
                            ):
                                short_u = (normalized.get(u) or u).strip()
                                async with _playwright_mavely_async_lock():
                                    pw_short = await asyncio.to_thread(
                                        _fetch_mavely_html_via_playwright_sync,
                                        short_u,
                                        int(hub_html_timeout_s),
                                    )
                                if pw_short:
                                    txt = pw_short
                                    out = _first_production_outbound_from_hub_html(txt)
                                    if affiliate_rewrite_debug_verbose_on(cfg):
                                        _aff_dbg_verbose(
                                            cfg,
                                            "  html_unwrap playwright(short) %r -> len=%s merchant=%r"
                                            % (
                                                _aff_dbg_clip(short_u, 72),
                                                len(pw_short),
                                                _aff_dbg_clip(out or "", 72),
                                            ),
                                        )
                            if not out:
                                break
                            # Resolve relative links found in HTML against the current page.
                            out_abs = out
                            if out_abs.startswith("/"):
                                out_abs = urljoin(candidate, out_abs)
                            out_abs = unwrap_known_query_redirects(out_abs) or out_abs
                            candidate = out_abs
                            _aff_dbg_verbose(
                                cfg,
                                "  html_unwrap %r: host=%r -> %r"
                                % (_aff_dbg_clip(u, 72), _aff_dbg_clip(host, 48), _aff_dbg_clip(candidate, 88)),
                            )
                        except Exception:
                            break

                    if _is_cloudflare_or_cdn_error_landing(candidate):
                        candidate = start_u
                    resolved[u] = candidate
                elif affiliate_rewrite_debug_verbose_on(cfg):
                    _aff_dbg_verbose(
                        cfg,
                        "  expand skip %r: host not in shortener/env list (still may affiliate if already merchant URL)"
                        % _aff_dbg_clip(start_u, 100),
                    )
    elif affiliate_rewrite_debug_on(cfg) and affiliate_rewrite_debug_verbose_on(cfg):
        _aff_dbg_verbose(
            cfg,
            "compute_affiliate_rewrites: affiliate_expand_redirects disabled (no HTTP expand; query/HTML unwrap may still apply)",
        )

    for u in candidates:
        raw = (normalized.get(u) or u).strip()
        target = (resolved.get(u) or raw).strip()
        if _is_cloudflare_or_cdn_error_landing(target):
            target = raw
            resolved[u] = target

        def _short_err(s: Optional[str], n: int = 160) -> str:
            t = (s or "").replace("\r", " ").replace("\n", " ").strip()
            return t if len(t) <= n else (t[:n] + "...")

        def _is_mavely_unsupported(err_msg: Optional[str]) -> bool:
            m = (err_msg or "").strip().lower()
            return ("merchant not supported" in m) or ("brand not found" in m)

        # Skip affiliate rewriting for configured marketplace domains.
        # Still allow expansion/unwrapping to surface the final destination when available.
        try:
            parsed = urlparse(target)
            host = (parsed.netloc or "").lower()
        except Exception:
            host = ""
        if host and _host_matches_skip(host):
            if target and (target != raw):
                mapped[u] = target
                notes[u] = "expanded only (marketplace skipped)"
            else:
                notes[u] = "marketplace skipped"
            continue

        # Re-wrap mavely.app.link short links into YOUR Mavely link (so forwarded posts always credit you).
        # Other URLs (bit.ly, etc.) that expand to mavelyinfluencer.com are handled after expansion.
        if is_mavely_app_short_link(raw):
            # Prefer merchant when unwrap succeeded. Never pass influencer bridge URLs into
            # createAffiliateLink — that is Mavely→Mavely (wrong input / wrong attribution). Only the
            # final store URL or the app.link short URL are valid API inputs here.
            if target and (not is_mavely_link(target)) and (target != raw):
                target_for_mavely = _strip_tracking_params(target) or target
                # Amazon: always use your associate tag (and optional Discord mask) — skip Mavely GraphQL for storefront URLs.
                if is_amazon_like_url(target):
                    affiliate_url = build_amazon_affiliate_url(cfg, target)
                    if affiliate_url:
                        raw_mask = _env_first_token("AMAZON_DISCORD_MASK_LINK", "1").lower()
                        mask_enabled = raw_mask in {"1", "true", "yes", "y", "on"}
                        mask_prefix = _env_first_token("AMAZON_DISCORD_MASK_PREFIX", "amzn.to") or "amzn.to"
                        try:
                            mask_len = int(_env_first_token("AMAZON_DISCORD_MASK_LEN", "7") or "7")
                        except Exception:
                            mask_len = 7
                        if mask_enabled:
                            rep = amazon_mask_cache.get(affiliate_url)
                            if not rep:
                                rep = discord_masked_link(mask_prefix, affiliate_url, slug_len=mask_len)
                                amazon_mask_cache[affiliate_url] = rep
                            mapped[u] = rep
                        else:
                            mapped[u] = affiliate_url
                        notes[u] = "amazon affiliate (mavely.app.link unwrap)"
                        continue
                link, err = await mavely_create_link(cfg, target_for_mavely)
                if link and not err and link != raw:
                    mapped[u] = link
                    notes[u] = "rewrapped mavely link"
                else:
                    # Non-Amazon: fall back to stripped merchant URL when Mavely cannot rewrap.
                    mapped[u] = _strip_tracking_params(target)
                    reason = _short_err(err)
                    if _is_mavely_unsupported(err):
                        notes[u] = "merchant not supported by Mavely; used expanded destination"
                    else:
                        notes[u] = (
                            f"mavely rewrap failed ({reason}); fell back to expanded destination (stripped tracking)"
                            if reason
                            else "mavely rewrap failed; fell back to expanded destination (stripped tracking)"
                        )
            else:
                # No usable expansion: try short link only.
                link, err = await mavely_create_link(cfg, raw)
                if link and not err and link != raw:
                    mapped[u] = link
                    notes[u] = "rewrapped mavely link (direct)"
                else:
                    reason = _short_err(err)
                    notes[u] = f"rewrap failed ({reason})" if reason else "rewrap failed (no expanded destination)"
            continue

        # Do not pass through another creator's Mavely bridge URL. Always try YOUR link from the *original*
        # URL in the message first (bit.ly / t.co / etc.); Mavely API often resolves those correctly.
        if is_mavely_link(target) and (target != raw) and (not is_mavely_link(raw)):
            link_bridge, err_bridge = await mavely_create_link(cfg, raw)
            if link_bridge and not err_bridge and link_bridge != raw:
                mapped[u] = link_bridge
                notes[u] = "mavely affiliate (API from original URL; avoided bridge pass-through)"
                continue
            # Do not call create_link(influencer bridge) — wrong pipeline (Mavely→Mavely).
            notes[u] = (
                "expand landed on Mavely bridge; API from original URL failed — left unchanged "
                f"({_short_err(err_bridge, 120)})"
                if err_bridge
                else "expand landed on Mavely bridge; API from original URL failed — left unchanged"
            )
            continue

        if is_amazon_like_url(target):
            affiliate_url = build_amazon_affiliate_url(cfg, target)
            if not affiliate_url:
                notes[u] = "amazon link but no asin"
                continue
            final_url = affiliate_url

            raw_mask = _env_first_token("AMAZON_DISCORD_MASK_LINK", "1").lower()
            mask_enabled = raw_mask in {"1", "true", "yes", "y", "on"}
            mask_prefix = _env_first_token("AMAZON_DISCORD_MASK_PREFIX", "amzn.to") or "amzn.to"
            try:
                mask_len = int(_env_first_token("AMAZON_DISCORD_MASK_LEN", "7") or "7")
            except Exception:
                mask_len = 7

            if mask_enabled:
                rep = amazon_mask_cache.get(final_url)
                if not rep:
                    rep = discord_masked_link(mask_prefix, final_url, slug_len=mask_len)
                    amazon_mask_cache[final_url] = rep
                mapped[u] = rep
            else:
                mapped[u] = final_url
            notes[u] = "amazon affiliate"
            continue

        # Unresolved Mavely tracking surface (e.g. raw hub URL in message): never mint from bridge hosts.
        if is_mavely_link(target):
            notes[u] = "mavely link not resolved to merchant; left unchanged (no create_link on bridge)"
            continue

        # Non-Amazon: try Mavely
        target_for_mavely = _strip_tracking_params(target) or target
        link, err = await mavely_create_link(cfg, target_for_mavely)
        if link and not err:
            mapped[u] = link
            notes[u] = "mavely affiliate"
        elif target and (target != raw):
            if is_mavely_link(target):
                link_fb, err_fb = await mavely_create_link(cfg, raw)
                if link_fb and not err_fb and link_fb != raw:
                    mapped[u] = link_fb
                    notes[u] = "mavely affiliate (API from original URL; merchant Mavely create failed)"
                else:
                    notes[u] = "mavely failed; not forwarding intermediate Mavely URL as fallback"
            else:
                mapped[u] = _strip_tracking_params(target) or target
                if _is_mavely_unsupported(err):
                    notes[u] = "expanded only (merchant not supported by Mavely)"
                else:
                    reason = _short_err(err)
                    notes[u] = f"expanded only (mavely failed: {reason})" if reason else "expanded only"
        else:
            if _is_mavely_unsupported(err):
                notes[u] = "merchant not supported by Mavely"
            else:
                notes[u] = _short_err(err, 220) or "no change"

    if affiliate_rewrite_debug_on(cfg):
        if affiliate_rewrite_debug_verbose_on(cfg):
            for u in candidates:
                tgt = (resolved.get(u) or normalized.get(u) or u).strip()
                try:
                    th = (urlparse(tgt).netloc or "").lower()
                except Exception:
                    th = ""
                mv = mapped.get(u)
                _aff_dbg_verbose(
                    cfg,
                    "  outcome %r host=%r replaced=%s note=%r"
                    % (
                        _aff_dbg_clip(u, 72),
                        _aff_dbg_clip(th, 56) if th else "?",
                        _aff_dbg_clip(mv, 96) if mv else "(none)",
                        _aff_dbg_clip(notes.get(u, ""), 140),
                    ),
                )
        else:
            tag = _cfg_or_env_str(cfg, "amazon_associate_tag", "AMAZON_ASSOCIATE_TAG")
            bits = []
            for u in candidates:
                tgt = (resolved.get(u) or normalized.get(u) or u).strip()
                try:
                    th = (urlparse(tgt).netloc or "").lower()
                except Exception:
                    th = ""
                mv = mapped.get(u)
                bits.append(
                    "%s→host=%s %s: %s"
                    % (
                        _aff_dbg_clip(u, 48),
                        _aff_dbg_clip(th, 28) if th else "?",
                        "replaced" if mv else "unchanged",
                        _aff_dbg_clip(notes.get(u, ""), 130),
                    )
                )
            _aff_dbg(
                cfg,
                "compute: %d url(s) expand=%s amazon_assoc_configured=%s | %s"
                % (
                    len(candidates),
                    expand_enabled,
                    bool((tag or "").strip()),
                    " || ".join(bits),
                ),
            )

    if isinstance(memo_lookup, dict) and unique:
        memo_lookup[tuple(sorted(unique))] = (dict(mapped), dict(notes))

    return mapped, notes


async def rewrite_text(cfg: dict, text: str) -> Tuple[str, bool, Dict[str, str]]:
    original = text or ""
    spans = extract_urls_with_spans(original)
    if not spans:
        _aff_dbg(
            cfg,
            "rewrite_text: no URLs matched by extract_urls_with_spans (text_len=%d sample=%r)"
            % (len(original), _aff_dbg_clip(original, 120)),
        )
        return original, False, {}
    urls = [u for (u, _, _) in spans]
    if affiliate_rewrite_debug_verbose_on(cfg):
        _aff_dbg_verbose(
            cfg,
            "rewrite_text: detected %d URL(s): %s"
            % (len(urls), ", ".join(_aff_dbg_clip(x, 88) for x in urls[:14]) + (" ..." if len(urls) > 14 else "")),
        )
    mapped, notes = await compute_affiliate_rewrites(cfg, urls)
    if not mapped:
        if affiliate_rewrite_debug_verbose_on(cfg):
            _aff_dbg_verbose(
                cfg,
                "rewrite_text: compute_affiliate_rewrites returned no replacements; notes=%s" % _aff_dbg_notes_summary(notes),
            )
        return original, False, notes or {}

    changed = False
    out = original
    skipped_same = 0
    for (u, start, end) in sorted(spans, key=lambda t: t[1], reverse=True):
        rep = mapped.get(u)
        if not rep or rep == u:
            if rep == u and affiliate_rewrite_debug_on(cfg):
                skipped_same += 1
            continue
        rep_out = rep

        # If we're inside an existing markdown link target: [label](URL)
        in_md_target = _is_markdown_link_target_context(original, start, end)
        if in_md_target and rep_out.lstrip().startswith("["):
            target = _extract_markdown_link_target(rep_out)
            if target:
                rep_out = target

        # Preserve <...> wrapper if original had it and rep_out is a URL
        try:
            wrapped = (original[start] == "<") and (original[end - 1] == ">")
        except Exception:
            wrapped = False
        if wrapped and rep_out and (not rep_out.startswith("<")) and (not rep_out.lstrip().startswith("[")):
            rep_out = f"<{rep_out.strip()}>"

        out = out[:start] + rep_out + out[end:]
        changed = True

        # Add a human-friendly mapping hint in notes so callers can log: original -> rewritten
        try:
            note = (notes or {}).get(u) or "changed"
            rep_short = (rep_out or "").replace("\r", " ").replace("\n", " ").strip()
            if len(rep_short) > 220:
                rep_short = rep_short[:220] + "..."
            notes[u] = f"{note} -> {rep_short}"
        except Exception:
            pass

    if affiliate_rewrite_debug_on(cfg):
        if affiliate_rewrite_debug_verbose_on(cfg):
            if skipped_same:
                _aff_dbg_verbose(
                    cfg,
                    "rewrite_text: %d span(s) had mapped value identical to original (no substitution)" % skipped_same,
                )
            missing_rep = sum(1 for x in urls if x not in mapped)
            if missing_rep:
                _aff_dbg_verbose(
                    cfg,
                    "rewrite_text: %d detected URL(s) had no entry in mapped (unexpected)" % missing_rep,
                )
            if not changed and mapped:
                _aff_dbg_verbose(
                    cfg,
                    "rewrite_text: compute returned %d replacement(s) but no span edits (check span key match vs mapped keys)"
                    % len(mapped),
                )
            _aff_dbg_verbose(
                cfg,
                "rewrite_text: done changed=%s applied=%d/%d spans"
                % (changed, sum(1 for x in urls if mapped.get(x) and mapped.get(x) != x), len(urls)),
            )
        else:
            _aff_dbg(
                cfg,
                "rewrite_text: content_changed=%s applied_spans=%d/%d"
                % (
                    changed,
                    sum(1 for x in urls if mapped.get(x) and mapped.get(x) != x),
                    len(urls),
                ),
            )

    return out, changed, notes or {}


async def rewrite_embed_dict(cfg: dict, embed: dict) -> Tuple[dict, bool, Dict[str, str]]:
    """
    Rewrite text-bearing fields of an embed dict. Does NOT rewrite images/thumbnails.
    For embed.url (must be a URL), we rewrite by replacing Amazon urls with affiliate_url (no markdown).
    """
    changed = False
    notes_out: Dict[str, str] = {}
    e = dict(embed or {})

    if affiliate_rewrite_debug_on(cfg) and affiliate_rewrite_debug_verbose_on(cfg):
        eu = (e.get("url") or "").strip()
        _aff_dbg_verbose(
            cfg,
            "rewrite_embed_dict: title_len=%d desc_len=%d url=%r fields=%d"
            % (
                len((e.get("title") or "")),
                len((e.get("description") or "")),
                _aff_dbg_clip(eu, 96) if eu else "",
                len(e.get("fields") or []) if isinstance(e.get("fields"), list) else 0,
            ),
        )

    for key in ("title", "description"):
        if isinstance(e.get(key), str) and e.get(key).strip():
            new_v, ch, notes = await rewrite_text(cfg, e.get(key))
            if ch:
                e[key] = new_v
                changed = True
            notes_out.update(notes or {})

    # url field must remain a plain URL (no Discord markdown). Run full expand + affiliate pipeline
    # so amzn.to / a.co / mavely.app.link in embed.url behave like links in message content.
    if isinstance(e.get("url"), str) and e.get("url").strip():
        raw_u = (e.get("url") or "").strip()
        mapped_u, notes_u = await compute_affiliate_rewrites_plain(cfg, [raw_u])
        nu = normalize_input_url(raw_u) or raw_u
        rep = (mapped_u.get(raw_u) or mapped_u.get(nu) or "").strip()
        if rep and rep != raw_u:
            e["url"] = rep
            changed = True
            k_note = (notes_u or {}).get(raw_u) or (notes_u or {}).get(nu) or "embed url rewritten"
            notes_out[raw_u] = str(k_note)
            _aff_dbg(
                cfg,
                "rewrite_embed_dict: embed.url rewritten %r -> %r (%s)"
                % (_aff_dbg_clip(raw_u, 88), _aff_dbg_clip(rep, 88), _aff_dbg_clip(str(k_note), 80)),
            )
        elif affiliate_rewrite_debug_on(cfg) and affiliate_rewrite_debug_verbose_on(cfg):
            n0 = (notes_u or {}).get(raw_u) or (notes_u or {}).get(nu) or ""
            _aff_dbg_verbose(
                cfg,
                "rewrite_embed_dict: embed.url unchanged %r note=%r"
                % (_aff_dbg_clip(raw_u, 96), _aff_dbg_clip(str(n0), 120)),
            )

    if isinstance(e.get("fields"), list):
        new_fields = []
        for f in e.get("fields") or []:
            ff = dict(f or {})
            if isinstance(ff.get("name"), str) and ff.get("name").strip():
                nv, ch, notes = await rewrite_text(cfg, ff.get("name"))
                if ch:
                    ff["name"] = nv
                    changed = True
                notes_out.update(notes or {})
            if isinstance(ff.get("value"), str) and ff.get("value").strip():
                nv, ch, notes = await rewrite_text(cfg, ff.get("value"))
                if ch:
                    ff["value"] = nv
                    changed = True
                notes_out.update(notes or {})
            new_fields.append(ff)
        e["fields"] = new_fields

    return e, changed, notes_out

