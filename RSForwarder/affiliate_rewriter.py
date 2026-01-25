"""
RSForwarder Affiliate Rewriter (standalone)

Implements the same rewrite behavior as Instorebotforwarder:
- Detect URLs in text
- Expand/unwrap short & deal-hub links to their final destination
- Amazon: add your affiliate tag and optionally mask as [amzn.to/xxxx](<real_url>)
- Other stores: generate a Mavely affiliate link (when possible)
- Markdown-safe: do not inject markdown links inside existing markdown link targets.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import secrets
import string
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

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


_URL_RE = re.compile(
    r"((?:https?://)?(?:www\.)?[a-z0-9][a-z0-9.-]*\.[a-z]{2,}(?:/[^\s<>()]*)?)",
    re.IGNORECASE,
)


def normalize_input_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return f"https://{u}"


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
        if start < 2 or end >= len(text):
            return False
        if text[start - 2:start] != "](":
            return False
        return text[end:end + 1] == ")"
    except Exception:
        return False


def is_mavely_link(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return "mavely.app.link" in host


def is_amazon_like_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return ("amazon." in host) or host.endswith("amazon.com") or host.endswith("amazon.co.uk") or ("amzn.to" in host)


def _extract_asin_fallback(text_or_url: str) -> Optional[str]:
    if not text_or_url:
        return None
    m = re.search(r"/dp/([A-Z0-9]{10})", text_or_url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"/gp/product/([A-Z0-9]{10})", text_or_url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"\b([A-Z0-9]{10})\b", text_or_url.upper())
    return m.group(1).upper() if m else None


def build_amazon_affiliate_url(cfg: dict, raw_url: str) -> Optional[str]:
    u = (raw_url or "").strip()
    if not u:
        return None
    asin = _extract_asin_fallback(u)
    if not asin:
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

    associate_tag = _cfg_or_env_str(cfg, "amazon_associate_tag", "AMAZON_ASSOCIATE_TAG")
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
        "deals.pennyexplorer.com",
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
    if host == "joylink.io":
        for k in ("url", "u", "target", "dest"):
            cand = (q.get(k) or "").strip()
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    return None


def _extract_first_outbound_url_from_html(html: str) -> Optional[str]:
    t = (html or "")[:200_000]
    if not t:
        return None
    patterns = [
        r"https?://(?:www\.)?amazon\.[^\s\"'<>]+",
        r"https?://amzn\.to/[A-Za-z0-9]+",
        r"https?://(?:www\.)?walmart\.com/[^\s\"'<>]+",
        r"https?://walmrt\.us/[A-Za-z0-9]+",
        r"https?://(?:www\.)?target\.com/[^\s\"'<>]+",
        r"https?://bit\.ly/[A-Za-z0-9]+",
    ]
    for pat in patterns:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            return (m.group(0) or "").strip()
    return None


async def expand_url(session: aiohttp.ClientSession, url: str, *, timeout_s: float = 8.0, max_redirects: int = 8) -> str:
    u = (url or "").strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return u
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    ua = (os.getenv("MAVELY_USER_AGENT", "") or "").strip() or "Mozilla/5.0"
    headers = {"User-Agent": ua, "Accept": "*/*"}
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
            r = requests.get(u, allow_redirects=True, timeout=max(5, int(timeout_s)), headers={"User-Agent": ua})
            return r.url or u

        final = await asyncio.to_thread(_do)
        return _normalize_expanded_url(final or u)
    except Exception:
        return u


def _mavely_cookie_file_path() -> Path:
    explicit = (os.getenv("MAVELY_COOKIES_FILE", "") or "").strip()
    if explicit:
        return Path(explicit)
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
        res = client.create_link((url or "").strip())
        link = res.mavely_link if getattr(res, "ok", False) else None
        err = "" if link else (getattr(res, "error", None) or "Failed to generate Mavely link.")
        status = int(getattr(res, "status_code", 0) or 0)
        return link, str(err), status

    link, err, status = await asyncio.to_thread(_do)
    if link:
        return link, None

    err_l = (err or "").lower()
    if ("token expired" in err_l) or ("not logged in" in err_l) or ("unauthorized" in err_l):
        if await _maybe_refresh_mavely_cookies(reason=err or "auth"):
            link2, err2, _status2 = await asyncio.to_thread(_do)
            if link2:
                return link2, None
            err = err2 or err

    return None, f"{err} (status={status})"


async def compute_affiliate_rewrites(cfg: dict, urls: List[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (mapped, notes):
    - mapped: original url -> replacement text
    - notes: original url -> short reason
    """
    unique = list(dict.fromkeys([(u or "").strip() for u in (urls or []) if (u or "").strip()]))
    if not unique:
        return {}, {}

    normalized = {u: normalize_input_url(u) for u in unique}
    mapped: Dict[str, str] = {}
    notes: Dict[str, str] = {}

    # Stable amazon masks per destination within a message
    amazon_mask_cache: Dict[str, str] = {}

    expand_enabled = _bool_or_default((cfg or {}).get("affiliate_expand_redirects"), True)
    max_redirects = int(_cfg_or_env_int(cfg, "affiliate_max_redirects", "AUTO_AFFILIATE_MAX_REDIRECTS") or 8)
    timeout_s = float(_cfg_or_env_int(cfg, "affiliate_expand_timeout_s", "AUTO_AFFILIATE_EXPAND_TIMEOUT_S") or 8)

    resolved: Dict[str, str] = {u: normalized.get(u) or u for u in unique}

    for u in unique:
        cand = unwrap_known_query_redirects(resolved.get(u) or u)
        if cand:
            resolved[u] = cand

    if expand_enabled:
        async with aiohttp.ClientSession() as session:
            for u in unique:
                start_u = (resolved.get(u) or u).strip()
                if should_expand_url(start_u):
                    final_u = await expand_url(session, start_u, timeout_s=timeout_s, max_redirects=max_redirects)
                    resolved[u] = final_u

                    cand2 = unwrap_known_query_redirects(final_u)
                    if cand2:
                        resolved[u] = cand2
                        final_u = cand2

                    try:
                        host = (urlparse(final_u).netloc or "").lower()
                    except Exception:
                        host = ""
                    if host in {"deals.pennyexplorer.com", "ringinthedeals.com", "dmflip.com", "trackcm.com", "joylink.io", "fkd.deals"}:
                        try:
                            async with session.get(final_u, timeout=aiohttp.ClientTimeout(total=float(timeout_s))) as resp:
                                txt = await resp.text(errors="ignore")
                            out = _extract_first_outbound_url_from_html(txt)
                            if out:
                                resolved[u] = out
                        except Exception:
                            pass

    for u in unique:
        raw = (normalized.get(u) or u).strip()
        target = (resolved.get(u) or raw).strip()

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

        # Non-Amazon: try Mavely
        link, err = await mavely_create_link(cfg, target)
        if link and not err:
            mapped[u] = link
            notes[u] = "mavely affiliate"
        elif target and (target != raw):
            mapped[u] = target
            notes[u] = "expanded only"
        else:
            notes[u] = err or "no change"

    return mapped, notes


async def rewrite_text(cfg: dict, text: str) -> Tuple[str, bool, Dict[str, str]]:
    original = text or ""
    spans = extract_urls_with_spans(original)
    if not spans:
        return original, False, {}
    urls = [u for (u, _, _) in spans]
    mapped, notes = await compute_affiliate_rewrites(cfg, urls)
    if not mapped:
        return original, False, notes or {}

    changed = False
    out = original
    for (u, start, end) in sorted(spans, key=lambda t: t[1], reverse=True):
        rep = mapped.get(u)
        if not rep or rep == u:
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

    return out, changed, notes or {}


async def rewrite_embed_dict(cfg: dict, embed: dict) -> Tuple[dict, bool, Dict[str, str]]:
    """
    Rewrite text-bearing fields of an embed dict. Does NOT rewrite images/thumbnails.
    For embed.url (must be a URL), we rewrite by replacing Amazon urls with affiliate_url (no markdown).
    """
    changed = False
    notes_out: Dict[str, str] = {}
    e = dict(embed or {})

    for key in ("title", "description"):
        if isinstance(e.get(key), str) and e.get(key).strip():
            new_v, ch, notes = await rewrite_text(cfg, e.get(key))
            if ch:
                e[key] = new_v
                changed = True
            notes_out.update(notes or {})

    # url field must remain a URL, not markdown
    if isinstance(e.get("url"), str) and e.get("url").strip():
        raw = normalize_input_url(e.get("url"))
        if is_amazon_like_url(raw):
            aff = build_amazon_affiliate_url(cfg, raw)
            if aff and aff != raw:
                e["url"] = aff
                changed = True

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

