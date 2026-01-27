#!/usr/bin/env python3
"""
Instorebotforwarder
-------------------
Scans configured source channels for Amazon links (including embed URLs and link buttons),
expands/normalizes them, optionally enriches via Amazon PA-API, routes to output buckets,
and renders RS-style embeds using JSON templates (config-driven).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

# Ensure repo root is importable when executed as a script.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_config_with_secrets, is_placeholder_secret, mask_secret
from RSForwarder import affiliate_rewriter

log = logging.getLogger("instorebotforwarder")


def _setup_logging() -> None:
    level = (os.getenv("LOG_LEVEL", "") or "").strip().upper() or "INFO"
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _safe_int(s: Any) -> Optional[int]:
    try:
        v = int(str(s).strip())
        return v
    except Exception:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _cfg_str(cfg: dict, key: str, env_key: str = "") -> str:
    try:
        v = str((cfg or {}).get(key) or "").strip()
    except Exception:
        v = ""
    if v:
        return v
    if env_key:
        return (os.getenv(env_key, "") or "").strip()
    return ""


def _cfg_int(cfg: dict, key: str, env_key: str = "") -> Optional[int]:
    v = (cfg or {}).get(key)
    if isinstance(v, int):
        return v
    if env_key:
        raw = (os.getenv(env_key, "") or "").strip()
        if raw:
            try:
                return int(raw)
            except Exception:
                return None
    return None


def _cfg_float(cfg: dict, key: str, env_key: str = "") -> Optional[float]:
    v = (cfg or {}).get(key)
    if isinstance(v, (int, float)):
        return float(v)
    if env_key:
        raw = (os.getenv(env_key, "") or "").strip()
        if raw:
            try:
                return float(raw)
            except Exception:
                return None
    return None


def _log_flow(stage: str, **kv: Any) -> None:
    parts: List[str] = []
    for k, v in kv.items():
        if v is None:
            continue
        s = str(v)
        if not s:
            continue
        parts.append(f"{k}={s}")
    log.info("[FLOW:%s] %s", stage, " ".join(parts))


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:  # type: ignore[override]
        return ""


def _tpl(s: Any, ctx: Dict[str, str]) -> str:
    raw = "" if s is None else str(s)
    if not raw:
        return ""
    try:
        return raw.format_map(_SafeFormatDict(ctx))
    except Exception:
        # If the template is malformed, do not crash forwarding; return raw.
        return raw


def _embed_from_template(tpl: Dict[str, Any], ctx: Dict[str, str]) -> Optional[discord.Embed]:
    if not isinstance(tpl, dict) or not tpl:
        return None

    title = _tpl(tpl.get("title", ""), ctx)
    description = _tpl(tpl.get("description", ""), ctx)
    url = _tpl(tpl.get("url", ""), ctx)
    color_val = tpl.get("color", None)
    try:
        color = discord.Color(int(color_val)) if color_val is not None else discord.Color.blurple()
    except Exception:
        color = discord.Color.blurple()

    embed = discord.Embed(
        title=title or discord.Embed.Empty,
        description=description or discord.Embed.Empty,
        url=(url or None),
        color=color,
        timestamp=datetime.now(timezone.utc),
    )

    thumb = tpl.get("thumbnail", None)
    if isinstance(thumb, dict):
        tu = _tpl(thumb.get("url", ""), ctx).strip()
        if tu:
            embed.set_thumbnail(url=tu)

    img = tpl.get("image", None)
    if isinstance(img, dict):
        iu = _tpl(img.get("url", ""), ctx).strip()
        if iu:
            embed.set_image(url=iu)

    author = tpl.get("author", None)
    if isinstance(author, dict):
        an = _tpl(author.get("name", ""), ctx).strip()
        au = _tpl(author.get("url", ""), ctx).strip()
        ai = _tpl(author.get("icon_url", ""), ctx).strip()
        if an or au or ai:
            embed.set_author(name=(an or discord.Embed.Empty), url=(au or None), icon_url=(ai or None))

    footer = tpl.get("footer", None)
    if isinstance(footer, dict):
        ft = _tpl(footer.get("text", ""), ctx).strip()
        fi = _tpl(footer.get("icon_url", ""), ctx).strip()
        if ft or fi:
            embed.set_footer(text=(ft or discord.Embed.Empty), icon_url=(fi or None))

    fields = tpl.get("fields", None)
    if isinstance(fields, list):
        for f in fields[:25]:
            if not isinstance(f, dict):
                continue
            name = _tpl(f.get("name", ""), ctx).strip() or "\u200b"
            value = _tpl(f.get("value", ""), ctx).strip() or "\u200b"
            inline = bool(f.get("inline", False))
            embed.add_field(name=name[:256], value=value[:1024], inline=inline)

    return embed


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = hmac.new(k_date, region.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing


async def _paapi_get_items(cfg: dict, asin: str) -> Tuple[Optional[dict], Optional[str], int]:
    """
    Call Amazon PA-API GetItems for a single ASIN.

    Returns: (response_json, error_string, http_status)
    """
    access_key = _cfg_str(cfg, "amazon_paapi_access_key", "AMAZON_PAAPI_ACCESS_KEY")
    secret_key = _cfg_str(cfg, "amazon_paapi_secret_key", "AMAZON_PAAPI_SECRET_KEY")
    partner_tag = _cfg_str(cfg, "amazon_paapi_partner_tag", "AMAZON_PAAPI_PARTNER_TAG")
    host = _cfg_str(cfg, "amazon_paapi_host", "AMAZON_PAAPI_HOST") or "webservices.amazon.com"
    region = _cfg_str(cfg, "amazon_paapi_region", "AMAZON_PAAPI_REGION") or "us-east-1"
    marketplace = _cfg_str(cfg, "amazon_paapi_marketplace", "AMAZON_PAAPI_MARKETPLACE") or "www.amazon.com"
    resources = (cfg or {}).get("amazon_paapi_resources") or []
    if not isinstance(resources, list) or not resources:
        resources = ["ItemInfo.Title", "Images.Primary.Large", "Offers.Listings.Price", "BrowseNodeInfo.BrowseNodes"]

    if not (access_key and secret_key and partner_tag):
        return None, "PA-API not configured (missing access_key/secret_key/partner_tag)", 0

    service = "ProductAdvertisingAPI"
    target = "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems"
    endpoint = f"https://{host}/paapi5/getitems"

    payload = {
        "ItemIds": [asin],
        "Resources": resources,
        "PartnerTag": partner_tag,
        "PartnerType": "Associates",
        "Marketplace": marketplace,
    }
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    body_hash = hashlib.sha256(body).hexdigest()

    now = datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_uri = "/paapi5/getitems"
    canonical_querystring = ""
    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{host}\n"
        f"x-amz-content-sha256:{body_hash}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:{target}\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target"
    canonical_request = "\n".join(
        [
            "POST",
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            body_hash,
        ]
    )

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _sigv4_key(secret_key, date_stamp, region, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = {
        "Content-Encoding": "amz-1.0",
        "Content-Type": "application/json; charset=utf-8",
        "Host": host,
        "X-Amz-Date": amz_date,
        "X-Amz-Target": target,
        "X-Amz-Content-Sha256": body_hash,
        "Authorization": authorization,
    }

    timeout_s = float(_cfg_float(cfg, "amazon_paapi_timeout_s", "AMAZON_PAAPI_TIMEOUT_S") or 12.0)
    try:
        import aiohttp

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
            async with session.post(endpoint, data=body, headers=headers) as resp:
                status = int(resp.status)
                txt = await resp.text(errors="replace")
                try:
                    data = json.loads(txt) if txt else {}
                except Exception:
                    data = {}
                if status != 200:
                    err = None
                    try:
                        errs = (data or {}).get("Errors") or []
                        if errs and isinstance(errs, list):
                            e0 = errs[0] or {}
                            err = f"{e0.get('Code') or ''} {e0.get('Message') or ''}".strip() or None
                    except Exception:
                        err = None
                    return None, (err or f"HTTP {status}"), status
                return data, None, status
    except Exception as e:
        return None, f"PA-API request failed: {e}", 0


def _amazon_category_path_from_paapi(item: dict) -> str:
    try:
        bni = (item or {}).get("BrowseNodeInfo") or {}
        nodes = bni.get("BrowseNodes") or []
        if not isinstance(nodes, list) or not nodes:
            return ""
        node0 = nodes[0] if isinstance(nodes[0], dict) else {}
        names: List[str] = []
        n = node0
        for _ in range(12):
            if not isinstance(n, dict) or not n:
                break
            dn = str(n.get("DisplayName") or "").strip()
            if dn:
                names.append(dn)
            n = n.get("Ancestor")
        names = list(reversed([x for x in names if x]))
        return " > ".join(names)
    except Exception:
        return ""


async def _amazon_enrich_by_asin(cfg: dict, asin: str) -> Tuple[Optional[dict], Optional[str]]:
    data, err, status = await _paapi_get_items(cfg, asin)
    if err or not data:
        return None, (err or f"PA-API error (status={status})")
    try:
        items = ((data or {}).get("ItemsResult") or {}).get("Items") or []
        if not items or not isinstance(items, list) or not isinstance(items[0], dict):
            return None, "PA-API returned no items (suppressed/restricted)"
        item = items[0]

        title = (
            (((item.get("ItemInfo") or {}).get("Title") or {}).get("DisplayValue") or "")
            if isinstance(item.get("ItemInfo"), dict)
            else ""
        )
        title = str(title or "").strip()

        img = ""
        try:
            img = (
                (((item.get("Images") or {}).get("Primary") or {}).get("Large") or {}).get("URL")
                or (((item.get("Images") or {}).get("Primary") or {}).get("Medium") or {}).get("URL")
                or ""
            )
        except Exception:
            img = ""
        img = str(img or "").strip()

        price = ""
        try:
            offers = (item.get("Offers") or {}).get("Listings") or []
            if isinstance(offers, list) and offers and isinstance(offers[0], dict):
                price = (((offers[0].get("Price") or {}).get("DisplayAmount") or "") if isinstance(offers[0].get("Price"), dict) else "")
        except Exception:
            price = ""
        price = str(price or "").strip()

        category = _amazon_category_path_from_paapi(item)
        return {"asin": asin, "title": title, "image_url": img, "price": price, "category": category}, None
    except Exception as e:
        return None, f"PA-API parse error: {e}"


@dataclass
class AmazonDetection:
    asin: str
    url_used: str
    final_url: str


class InstorebotForwarder:
    def __init__(self) -> None:
        _setup_logging()
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        self.config = cfg
        self.config_path = config_path
        self.secrets_path = secrets_path

        token = str((self.config.get("bot_token") or "")).strip()
        if is_placeholder_secret(token):
            raise RuntimeError("bot_token missing/placeholder in Instorebotforwarder/config.secrets.json")

        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True

        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self._setup_events()
        self._setup_slash_commands()

    def _output_channel_ids(self) -> List[int]:
        oc = (self.config or {}).get("output_channels") or {}
        out: List[int] = []
        if isinstance(oc, dict):
            for k in ("personal", "grocery", "deals", "enrich_failed"):
                v = _safe_int(oc.get(k))
                if v:
                    out.append(v)
        return list(dict.fromkeys(out))

    def _source_channel_ids(self) -> List[int]:
        raw = (self.config or {}).get("source_channel_ids") or []
        if not isinstance(raw, list):
            return []
        out: List[int] = []
        for v in raw:
            vi = _safe_int(v)
            if vi:
                out.append(vi)
        return list(dict.fromkeys(out))

    def _pick_dest_channel_id(self, *, category: str, enrich_failed: bool) -> Tuple[Optional[int], str]:
        oc = (self.config or {}).get("output_channels") or {}
        if not isinstance(oc, dict):
            return None, "output_channels missing"

        personal = _safe_int(oc.get("personal"))
        grocery = _safe_int(oc.get("grocery"))
        deals = _safe_int(oc.get("deals"))
        ef = _safe_int(oc.get("enrich_failed"))

        if enrich_failed:
            return (ef or deals), "enrich_failed"

        cat = (category or "").lower()
        kws = (self.config or {}).get("amazon_grocery_keywords") or []
        if not isinstance(kws, list):
            kws = []
        is_grocery = any((str(k).strip().lower() in cat) for k in kws if str(k).strip())
        if is_grocery and grocery:
            return grocery, "category:grocery"

        if personal:
            return personal, "default:personal"
        return deals, "fallback:deals"

    def _pick_template(self, dest_channel_id: Optional[int], *, enrich_failed: bool) -> Tuple[Optional[Dict[str, Any]], str]:
        templates = (self.config or {}).get("amazon_embed_templates") or {}
        if not isinstance(templates, dict):
            return None, "none"

        if enrich_failed:
            tpl = templates.get("enrich_failed")
            if isinstance(tpl, dict) and tpl:
                return tpl, "enrich_failed"

        by = templates.get("by_channel_id") or {}
        if dest_channel_id and isinstance(by, dict):
            tpl = by.get(str(dest_channel_id))
            if isinstance(tpl, dict) and tpl:
                return tpl, f"by_channel_id.{dest_channel_id}"

        tpl = templates.get("default")
        if isinstance(tpl, dict) and tpl:
            return tpl, "default"
        return None, "none"

    def _message_shape(self, message: discord.Message) -> Tuple[int, int, int]:
        content_len = len(message.content or "")
        embeds_n = len(message.embeds or [])
        comps = getattr(message, "components", None) or []
        comp_rows = len(comps) if isinstance(comps, list) else 0
        return content_len, embeds_n, comp_rows

    def _collect_message_urls(self, message: discord.Message) -> List[str]:
        urls: List[str] = []

        def _push(u: Optional[str]) -> None:
            u = (u or "").strip()
            if not u:
                return
            u_norm = affiliate_rewriter.normalize_input_url(u)
            if u_norm:
                urls.append(u_norm)

        # 1) content
        for (u, _, _) in affiliate_rewriter.extract_urls_with_spans((message.content or "")):
            _push(u)

        # 2) embeds
        for e in (message.embeds or []):
            _push(getattr(e, "url", None))
            _push(getattr(getattr(e, "author", None), "url", None))

            title = (getattr(e, "title", None) or "")
            desc = (getattr(e, "description", None) or "")
            for (u, _, _) in affiliate_rewriter.extract_urls_with_spans(title):
                _push(u)
            for (u, _, _) in affiliate_rewriter.extract_urls_with_spans(desc):
                _push(u)

            for f in (getattr(e, "fields", None) or []):
                for (u, _, _) in affiliate_rewriter.extract_urls_with_spans((getattr(f, "name", "") or "")):
                    _push(u)
                for (u, _, _) in affiliate_rewriter.extract_urls_with_spans((getattr(f, "value", "") or "")):
                    _push(u)

        # 3) components (link buttons)
        for row in (getattr(message, "components", None) or []):
            for child in (getattr(row, "children", None) or []):
                _push(getattr(child, "url", None))

        # stable dedupe
        seen: set[str] = set()
        out: List[str] = []
        for u in urls:
            k = u.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(u)
        return out

    async def _detect_amazon(self, urls: List[str]) -> Optional[AmazonDetection]:
        if not urls:
            return None

        timeout_s = float(_cfg_float(self.config, "amazon_expand_timeout_s", "AMAZON_EXPAND_TIMEOUT_S") or 8.0)
        max_redirects = int(_cfg_int(self.config, "amazon_expand_max_redirects", "AMAZON_EXPAND_MAX_REDIRECTS") or 8)

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                for u in urls:
                    url_used = (u or "").strip()
                    if not url_used:
                        continue

                    cand = affiliate_rewriter.unwrap_known_query_redirects(url_used) or url_used
                    final_url = cand

                    # Expand short/deal-hub URLs when useful.
                    try:
                        if affiliate_rewriter.should_expand_url(cand):
                            final_url = await affiliate_rewriter.expand_url(
                                session,
                                cand,
                                timeout_s=timeout_s,
                                max_redirects=max_redirects,
                            )
                            final_url = affiliate_rewriter.unwrap_known_query_redirects(final_url) or final_url
                    except Exception:
                        final_url = cand

                    if not affiliate_rewriter.is_amazon_like_url(final_url):
                        continue

                    asin = affiliate_rewriter.extract_asin(final_url) or affiliate_rewriter.extract_asin(url_used) or ""
                    return AmazonDetection(asin=asin, url_used=url_used, final_url=final_url)
        except Exception:
            return None

        return None

    async def _analyze_message(self, message: discord.Message) -> Tuple[Optional[discord.Embed], Dict[str, Any]]:
        content_len, embeds_n, comp_rows = self._message_shape(message)
        _log_flow("SCAN", content_len=content_len, embeds=embeds_n, components=comp_rows)

        urls = self._collect_message_urls(message)
        _log_flow("URLS", count=len(urls), sample=" | ".join(urls[:3]))

        det = await self._detect_amazon(urls)
        if not det:
            _log_flow("AMZ_DETECT", found="0")
            return None, {"urls": urls, "amazon": None}

        _log_flow("AMZ_DETECT", found="1", asin=(det.asin or ""), url_used=det.url_used)

        # If we couldn't find an ASIN, we can still forward using the final URL, but enrichment will be skipped.
        asin = (det.asin or "").strip().upper()
        final_url = (det.final_url or "").strip()

        product: Optional[dict] = None
        enrich_err: Optional[str] = None
        enrich_failed = False

        if asin:
            product, enrich_err = await _amazon_enrich_by_asin(self.config, asin)
            if enrich_err or not product:
                enrich_failed = True
                _log_flow("ENRICH_FAIL", asin=asin, err=(enrich_err or "unknown"))
            else:
                _log_flow("ENRICH_OK", asin=asin)
        else:
            enrich_failed = True
            enrich_err = "no asin"
            _log_flow("ENRICH_FAIL", asin="", err="no asin")

        title = str((product or {}).get("title") or "").strip() or "Amazon item"
        price = str((product or {}).get("price") or "").strip()
        category = str((product or {}).get("category") or "").strip()
        image_url = str((product or {}).get("image_url") or "").strip()

        # Canonical final_url: if we have ASIN, prefer dp link on configured marketplace.
        if asin:
            mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
            if mp:
                final_url = f"{mp}/dp/{asin}"
            elif not final_url:
                final_url = f"https://www.amazon.com/dp/{asin}"

        dest_id, route_reason = self._pick_dest_channel_id(category=category, enrich_failed=enrich_failed)
        _log_flow("ROUTE", dest_id=(dest_id or ""), reason=route_reason)

        tpl, tpl_key = self._pick_template(dest_id, enrich_failed=enrich_failed)
        _log_flow("TEMPLATE", key=tpl_key)

        source_line = ""
        try:
            if message.guild and message.channel:
                source_line = f"{message.guild.name} #{getattr(message.channel, 'name', '')}".strip()
        except Exception:
            source_line = ""
        if not source_line:
            source_line = "SOURCE"

        ctx = {
            "asin": asin,
            "final_url": final_url,
            "link": f"<{final_url}>" if final_url else "",
            "title": title,
            "price": price,
            "category": category,
            "image_url": image_url,
            "source_line": source_line,
            "source_jump": str(getattr(message, "jump_url", "") or ""),
            "source_message_id": str(getattr(message, "id", "") or ""),
        }

        embed = _embed_from_template(tpl or {}, ctx) if tpl else None
        if embed:
            _log_flow("RENDER", ok="1", title=(embed.title or "")[:80], fields=len(embed.fields))
        else:
            _log_flow("RENDER", ok="0")

        meta = {
            "urls": urls,
            "amazon": {"asin": asin, "final_url": final_url, "url_used": det.url_used},
            "enrich_failed": enrich_failed,
            "enrich_err": enrich_err,
            "dest_channel_id": dest_id,
            "route_reason": route_reason,
            "template_key": tpl_key,
            "ctx": ctx,
        }
        return embed, meta

    async def _maybe_forward_message(self, message: discord.Message) -> None:
        # Skip our own messages to avoid loops.
        if self.bot.user and message.author and message.author.id == self.bot.user.id:
            return

        # Only process configured source channels.
        sources = set(self._source_channel_ids())
        if not sources:
            return
        if int(message.channel.id) not in sources:
            return

        # Never forward messages from output channels back into themselves.
        if int(message.channel.id) in set(self._output_channel_ids()):
            return

        embed, meta = await self._analyze_message(message)
        if not embed:
            return

        dest_id = meta.get("dest_channel_id")
        if not dest_id:
            _log_flow("SEND_FAIL", reason="dest_channel_missing")
            return

        ch = self.bot.get_channel(int(dest_id))
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(int(dest_id))
            except Exception:
                ch = None
        if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            _log_flow("SEND_FAIL", reason="dest_channel_not_text")
            return

        try:
            await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            _log_flow("SEND_OK", dest_id=dest_id, message_id=str(message.id))
        except Exception as e:
            _log_flow("SEND_FAIL", dest_id=dest_id, err=str(e)[:200])

    async def _startup_amazon_api_selftest(self) -> None:
        asin = _cfg_str(self.config, "startup_test_asin", "STARTUP_TEST_ASIN") or "B0FLMLDTPB"
        asin = asin.strip().upper()
        if not asin:
            return

        log.info("-------- Amazon API Startup Self-Test --------")
        log.info("[BOOT] Testing PA-API with ASIN=%s", asin)

        product, err = await _amazon_enrich_by_asin(self.config, asin)
        if err:
            log.error("[BOOT][PAAPI_FAIL] asin=%s err=%s", asin, err)
            return
        if not product:
            log.warning("[BOOT][PAAPI_EMPTY] asin=%s returned no product (suppressed/restricted)", asin)
            return

        title = str((product or {}).get("title") or "").strip()
        category = str((product or {}).get("category") or "").strip()
        image = str((product or {}).get("image_url") or "").strip()

        log.info("[BOOT][PAAPI_OK] asin=%s", asin)
        log.info("[BOOT][PAAPI_OK] title=%s", title[:120])
        log.info("[BOOT][PAAPI_OK] category=%s", category)
        log.info("[BOOT][PAAPI_OK] image=%s", image)

        # Silent embed build test (no send)
        mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
        final_url = f"{mp}/dp/{asin}" if mp else f"https://www.amazon.com/dp/{asin}"
        ctx = {
            "asin": asin,
            "final_url": final_url,
            "link": f"<{final_url}>",
            "title": title,
            "price": str((product or {}).get("price") or "").strip(),
            "category": category,
            "image_url": image,
            "source_line": "BOOT_SELFTEST",
            "source_jump": "",
            "source_message_id": "",
        }
        personal_id = _safe_int(((self.config.get("output_channels") or {}) if isinstance(self.config.get("output_channels"), dict) else {}).get("personal"))
        tpl, tpl_key = self._pick_template(personal_id, enrich_failed=False)
        embed = _embed_from_template(tpl, ctx) if tpl else None
        if embed:
            log.info("[BOOT][EMBED_OK] template=%s title=%s fields=%d", tpl_key, (embed.title or "")[:80], len(embed.fields))
        else:
            log.info("[BOOT][EMBED_SKIP] No template present; embed build skipped")

    def _setup_events(self) -> None:
        @self.bot.event
        async def on_ready() -> None:
            guild_id = _cfg_int(self.config, "guild_id", "GUILD_ID")
            log.info("Bot ready user=%s guild_id=%s config=%s secrets=%s", self.bot.user, guild_id, self.config_path, self.secrets_path)
            log.info("bot_token=%s", mask_secret(self.config.get("bot_token")))

            # Sync slash commands to guild when configured (fast).
            if guild_id:
                try:
                    await asyncio.sleep(1)
                    synced = await self.bot.tree.sync(guild=discord.Object(id=guild_id))
                    log.info("Synced slash commands to guild=%s count=%s", guild_id, len(synced))
                except Exception as e:
                    log.warning("Slash command sync failed guild=%s err=%s", guild_id, str(e)[:200])

            # Startup self-test (silent; logs only).
            try:
                await self._startup_amazon_api_selftest()
            except Exception:
                log.exception("Startup self-test crashed (continuing)")

        @self.bot.event
        async def on_message(message: discord.Message) -> None:
            # Still allow built-in command processing if you add prefix commands later.
            try:
                await self.bot.process_commands(message)
            except Exception:
                pass
            await self._maybe_forward_message(message)

    def _setup_slash_commands(self) -> None:
        @self.bot.tree.command(name="testallmessage", description="Diagnose URL scan/routing/template behavior for a message")
        @app_commands.describe(channel="Channel to inspect (defaults to current)", message_id="Message ID to inspect (defaults to most recent)")
        async def testallmessage(
            interaction: discord.Interaction,
            channel: Optional[discord.TextChannel] = None,
            message_id: Optional[str] = None,
        ) -> None:
            await interaction.response.defer(ephemeral=True, thinking=True)

            target = channel or getattr(interaction, "channel", None)
            if not isinstance(target, discord.TextChannel):
                await interaction.followup.send("❌ Could not determine a text channel to inspect.", ephemeral=True)
                return

            msg: Optional[discord.Message] = None
            if message_id:
                mid = _safe_int(message_id)
                if not mid:
                    await interaction.followup.send("❌ message_id must be a numeric message ID.", ephemeral=True)
                    return
                try:
                    msg = await target.fetch_message(int(mid))
                except Exception as e:
                    await interaction.followup.send(f"❌ Failed to fetch message: {str(e)[:200]}", ephemeral=True)
                    return
            else:
                try:
                    async for m in target.history(limit=1):
                        msg = m
                        break
                except Exception as e:
                    await interaction.followup.send(f"❌ Failed to read channel history: {str(e)[:200]}", ephemeral=True)
                    return

            if not msg:
                await interaction.followup.send("❌ No message found.", ephemeral=True)
                return

            embed, meta = await self._analyze_message(msg)

            content_len, embeds_n, comp_rows = self._message_shape(msg)
            urls = meta.get("urls") or []
            amz = meta.get("amazon") or None
            enrich_failed = bool(meta.get("enrich_failed"))
            dest_id = meta.get("dest_channel_id")
            tpl_key = str(meta.get("template_key") or "")

            lines: List[str] = []
            lines.append(f"**Scan**")
            lines.append(f"- channel: <#{msg.channel.id}>")
            lines.append(f"- message_id: `{msg.id}`")
            lines.append(f"- content_len: `{content_len}` | embeds: `{embeds_n}` | components: `{comp_rows}`")
            lines.append(f"- urls_found: `{len(urls)}`")
            if urls:
                lines.append(f"- url_sample: `{(' | '.join(urls[:3]))[:180]}`")
            if amz:
                lines.append("**Amazon**")
                lines.append(f"- asin: `{(amz.get('asin') or '')}`")
                lines.append(f"- final_url: `{(amz.get('final_url') or '')[:200]}`")
            else:
                lines.append("**Amazon**")
                lines.append("- detected: `no`")
            lines.append("**Routing**")
            lines.append(f"- enrich_failed: `{enrich_failed}`")
            lines.append(f"- dest_channel_id: `{dest_id or ''}`")
            lines.append(f"- template_key: `{tpl_key}`")

            out = "\n".join(lines)
            if len(out) > 1800:
                out = out[:1790] + "…"

            if embed:
                await interaction.followup.send(content=out, embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            else:
                await interaction.followup.send(content=out, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())

    async def start(self) -> None:
        token = str((self.config.get("bot_token") or "")).strip()
        await self.bot.start(token)


def main() -> int:
    try:
        bot = InstorebotForwarder()
    except Exception as e:
        _setup_logging()
        log.error("Startup failed: %s", e)
        return 2

    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        _setup_logging()
        log.error("Fatal error: %s", e)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""
Instore Auto Mirror Bot

Key design:
- Bot token owns all UI (config editor, buttons/modals) and all preview posts.
- Discum (user token) is OPTIONAL and used ONLY for explicit on-demand forwarding.
- JSON files store:
  - guild config (single file; contains all guilds)
  - runtime state (previews, forwarded history, usage, and Amazon cache)
"""

import os
import re
import json
import time
import sys
import asyncio
import base64
import html as _html
import hashlib
import hmac
import logging
import secrets
import string
import importlib
import subprocess
from logging.handlers import RotatingFileHandler
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl, urljoin, unquote
from typing import Optional, List, Tuple, Dict

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Optional discum (only for on-demand forwarding)
discum_client = None

# Amazon enrichment cache (ASIN -> product dict), used to avoid hammering the API
_amazon_product_cache: Dict[str, Tuple[float, dict]] = {}

# -----------------------
# Env
# -----------------------
# Load env file from script directory, not CWD
_script_dir = Path(__file__).parent
load_dotenv(_script_dir / "api-token.env")
load_dotenv()

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
DISCORD_USER_TOKEN = os.getenv("DISCORD_USER_TOKEN", "").strip()  # optional
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

def _load_mavely_cookies_from_file() -> None:
    """
    Option B support: read a cookie header string from a file and inject it into MAVELY_COOKIES.
    This lets a scheduled Playwright refresher keep the bot logged in without manual copy/paste.
    """
    try:
        # If user explicitly provides MAVELY_COOKIES, trust it.
        if (os.getenv("MAVELY_COOKIES", "") or "").strip():
            return
        explicit = (os.getenv("MAVELY_COOKIES_FILE", "") or "").strip()
        default_path = _script_dir / "mavely_cookies.txt"
        path = Path(explicit) if explicit else default_path
        if not path.exists():
            return
        raw = (path.read_text(encoding="utf-8") or "").strip()
        if not raw:
            return
        os.environ["MAVELY_COOKIES"] = raw
        # Keep this quiet (only a single line). Never print the cookie contents.
        logging.getLogger("instore").info("Mavely cookies loaded from file: %s", str(path))
    except Exception:
        return

def _reload_mavely_cookies_from_file(force: bool = False) -> bool:
    """
    Reload MAVELY_COOKIES from MAVELY_COOKIES_FILE at runtime.
    Returns True if we loaded a non-empty cookie header string.
    """
    try:
        if (not force) and (os.getenv("MAVELY_COOKIES", "") or "").strip():
            return False
        explicit = (os.getenv("MAVELY_COOKIES_FILE", "") or "").strip()
        default_path = _script_dir / "mavely_cookies.txt"
        path = Path(explicit) if explicit else default_path
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
    except ValueError:
        v = 600
    return max(60, min(v, 24 * 3600))

async def _maybe_refresh_mavely_cookies(reason: str) -> bool:
    """
    Run the Playwright cookie refresher (headless) if enabled and not on cooldown.
    Returns True if cookies were refreshed (or reloaded) and should be retried.
    """
    if not _mavely_auto_refresh_enabled():
        return False

    cooldown = _mavely_auto_refresh_cooldown_s()
    key = f"mavely_cookie_refresh:{reason}"
    if not _log_once(key, seconds=cooldown):
        return False

    # If a newer cookie file already exists, reload it first (cheap).
    if _reload_mavely_cookies_from_file(force=True):
        logging.getLogger("instore").warning("Mavely auth: reloaded cookies from file (trigger=%s)", reason)
        return True

    # Run refresher script (requires that you previously logged in once with --interactive).
    script = _script_dir / "mavely_cookie_refresher.py"
    if not script.exists():
        return False

    def _run() -> int:
        try:
            # Use current interpreter; avoids PATH issues on Windows.
            return subprocess.call([sys.executable, str(script)], cwd=str(_script_dir))
        except Exception:
            return 1

    logging.getLogger("instore").warning("Mavely auth: attempting cookie refresh (trigger=%s)", reason)
    code = await asyncio.to_thread(_run)
    if code != 0:
        logging.getLogger("instore").warning("Mavely auth: cookie refresher failed (exit=%s)", code)
        return False

    if _reload_mavely_cookies_from_file(force=True):
        logging.getLogger("instore").warning("Mavely auth: cookie refresh OK (reloaded from file)")
        return True
    logging.getLogger("instore").warning("Mavely auth: cookie refresh ran but cookies file was empty")
    return False

# -----------------------
# Logging (console + file)
# -----------------------
try:
    _colorama = importlib.import_module("colorama")
    _cinit = getattr(_colorama, "init", lambda **kwargs: None)
    _F = getattr(_colorama, "Fore", None)
    _S = getattr(_colorama, "Style", None)
    if callable(_cinit):
        _cinit(autoreset=True)
    if _F is None or _S is None:
        raise ImportError("colorama missing symbols")
except Exception:
    class _Dummy:
        def __getattr__(self, k):
            return ""
    _F = _S = _Dummy()

def _console_sanitize(text: str) -> str:
    # Keep Windows terminals stable + match MWDataManagerBot style.
    return (
        (text or "")
        .replace("→", "->")
        .replace("←", "<-")
        .replace("↔", "<->")
        .replace("•", "*")
        .replace("✓", "[OK]")
        .replace("✗", "[X]")
    )

class _DiscumStyleConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(getattr(record, "msg", "") or "")
        msg = _console_sanitize(msg)

        lvl = (record.levelname or "").upper()
        if lvl == "WARNING":
            tag = "WARN"
            color = _F.YELLOW
        elif lvl == "ERROR" or lvl == "CRITICAL":
            tag = "ERROR"
            color = _F.RED
        elif lvl == "DEBUG":
            tag = "DEBUG"
            color = _F.WHITE
        else:
            tag = "INFO"
            color = _F.GREEN

        line = f"{color}[{tag}]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}"
        # Avoid UnicodeEncodeError on some Windows shells
        try:
            line.encode(getattr(getattr(sys, "stdout", None), "encoding", "") or "utf-8")
            return line
        except Exception:
            safe = line.encode("ascii", errors="replace").decode("ascii")
            return safe

def _setup_logging() -> logging.Logger:
    # File logs can be verbose; console logs should be human-friendly by default.
    file_level_name = (os.getenv("INSTORE_LOG_LEVEL", "INFO") or "INFO").strip().upper()
    file_level = getattr(logging, file_level_name, logging.INFO)
    console_level_name = (os.getenv("INSTORE_CONSOLE_LOG_LEVEL", "") or "").strip().upper()
    console_level = getattr(logging, console_level_name, logging.INFO) if console_level_name else logging.INFO

    # Use root handlers so all module loggers (e.g. "mavely") show up.
    root = logging.getLogger()
    root.setLevel(min(file_level, console_level))

    logger = logging.getLogger("instore")
    logger.setLevel(min(file_level, console_level))

    # Avoid duplicate handlers if reloaded.
    if root.handlers:
        return logger

    # MWDataManagerBot-style console formatter (tagged + colorized)
    console_fmt = _DiscumStyleConsoleFormatter()
    file_fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")

    sh = logging.StreamHandler()
    sh.setLevel(console_level)
    sh.setFormatter(console_fmt)
    root.addHandler(sh)

    logs_dir = _script_dir / "logs"
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    try:
        fh = RotatingFileHandler(
            logs_dir / "instore_auto_mirror_bot.log",
            maxBytes=2_000_000,
            backupCount=5,
            encoding="utf-8",
        )
        fh.setLevel(file_level)
        fh.setFormatter(file_fmt)
        root.addHandler(fh)
    except Exception:
        # If file logging can't be set up, keep console logging.
        pass

    # Keep third-party libraries quieter unless user opts into DEBUG.
    # Default: quiet libs even if file log is DEBUG (console stays readable).
    raw_verbose = (os.getenv("INSTORE_VERBOSE_LIB_LOGS", "") or "").strip().lower()
    verbose_lib_logs = raw_verbose in {"1", "true", "yes", "y", "on"}
    lib_level = file_level if verbose_lib_logs else logging.WARNING

    # Keep discord.py internal logs quiet so our console output stays consistent (MWDataManagerBot style)
    for logger_name in (
        "discord",
        "discord.client",
        "discord.gateway",
        "discord.http",
        "discord.app_commands",
        "discord.app_commands.tree",
        "discord.webhook",
        "discord.webhook.async_",
    ):
        try:
            lg = logging.getLogger(logger_name)
            lg.handlers.clear()
            lg.setLevel(logging.ERROR if not verbose_lib_logs else file_level)
            lg.propagate = False
        except Exception:
            pass
    logging.getLogger("aiohttp").setLevel(lib_level)
    if not verbose_lib_logs:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
    # Keep Mavely client logs quiet by default (only show warnings/errors).
    # Turn on verbose lib logs to see INFO/DEBUG from the Mavely client.
    mavely_logger = logging.getLogger("mavely")
    mavely_logger.setLevel(file_level if verbose_lib_logs else logging.WARNING)
    mavely_logger.propagate = True
    return logger

log = _setup_logging()

# Option B: load cookies written by the Playwright refresher (if present)
_load_mavely_cookies_from_file()

# Simple build stamp so you can confirm you're running the latest file in terminal logs.
INSTORE_BUILD = "2026-01-25 jsoncfg-singlefile+runtime-json+amazon-enrich+category-route+stablemask+testallmessage"
try:
    log.info("Instore build: %s", INSTORE_BUILD)
except Exception:
    pass

# Optional local modules (import-safe)
_amazon_mod_dir = _script_dir / "amazon_leads_forwarder_setup" / "amazon_leads_forwarder_setup"
_mavely_mod_dir = _script_dir / "mavely_link_service" / "mavely_link_service"
for _p in (_amazon_mod_dir, _mavely_mod_dir):
    if _p.exists() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

try:
    from amazon_utils import canonicalize_amazon_url, extract_asin  # type: ignore
except Exception:
    canonicalize_amazon_url = None  # type: ignore
    extract_asin = None  # type: ignore

try:
    from mavely_client import MavelyClient  # type: ignore
except Exception:
    MavelyClient = None  # type: ignore

if MavelyClient is not None:
    try:
        log.info("Loaded MavelyClient from %s", inspect.getsourcefile(MavelyClient) or "(unknown)")
    except Exception:
        pass

LEGACY_DB_PATH = os.getenv("INSTORE_BOT_DB", str(_script_dir / "instore_auto_mirror.sqlite3"))
CONFIG_DIR = Path(os.getenv("INSTORE_CONFIG_DIR", str(_script_dir / "guild_configs")))
CONFIG_FILE = Path(os.getenv("INSTORE_GUILD_CONFIG_FILE", str(CONFIG_DIR / "guild_config.json")))
RUNTIME_FILE = Path(os.getenv("INSTORE_RUNTIME_FILE", str(_script_dir / "instore_auto_mirror_runtime.json")))
NEO_ADMIN_USER_ID = int(os.getenv("NEO_ADMIN_USER_ID", "0") or "0")  # who receives DMs/usage reports

if not DISCORD_BOT_TOKEN:
    raise RuntimeError("Missing DISCORD_BOT_TOKEN in api-token.env or .env")

# Init OpenAI client (optional if key missing)
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Init discum if available (no hard dependency)
if DISCORD_USER_TOKEN:
    try:
        # Attempt local discum import (optional)
        import sys
        project_root = Path(__file__).parent
        discum_path = project_root / "discum"
        if discum_path.exists() and str(discum_path.parent) not in sys.path:
            sys.path.insert(0, str(discum_path.parent))

        import discum  # type: ignore
        discum_client = discum.Client(token=DISCORD_USER_TOKEN, log={"console": False, "file": False})
        print("[OK] Discum client ready (ON-DEMAND forwarding only)")
    except Exception as e:
        discum_client = None
        print(f"[WARN] Discum not available ({e}). On-demand forwarding will use bot token only.")

# -----------------------
# Defaults (per guild)
# -----------------------
DEFAULT_CONFIG = {
    # Mode:
    # - "preview": bot posts reconstructed output to destination channels (test/staging)
    # - "manual": bot DMs admin a copy-ready message instead of posting
    # - "disabled": do nothing
    "post_mode": None,

    # Mentions / formatting
    "role_id": None,                            # role mention id -> <@&...>
    "profit_emoji": "",                          # set via editor
    "success_channel_id": None,                  # used in footer template
    "footer_text": "",

    # Formatting rules
    "wrap_links": None,                          # wrap links in < > to avoid embeds

    # OpenAI behavior
    "openai_model": "",
    "openai_temperature": None,

    # Usage tracking / thresholds
    "daily_token_threshold": None,               # int tokens/day, optional
    "daily_call_threshold": None,                # int calls/day, optional
    "report_channel_id": None,                   # optional channel for threshold alerts

    # Parsing / handling
    "ignore_messages_with_pdsql": None,          # if source already formatted, skip

    # Affiliate helpers (used by /instore amazon + /instore link)
    "amazon_leads_dest_channel_id": None,        # default destination for /instore amazon (channel id)
    "amazon_api_enabled": None,                  # bool; if None, env AMAZON_API_ENABLED controls
    "amazon_custom_endpoint": "",                # e.g. https://your-api/amazon?asin={asin}
    "amazon_api_marketplace": "",                # e.g. https://www.amazon.com
    "amazon_associate_tag": "",                  # e.g. mytag-20 (used to build affiliate links)
    # Optional: Amazon Product Advertising API (PA-API) (used when amazon_api_enabled=1 and no custom endpoint)
    "amazon_paapi_access_key": "",
    "amazon_paapi_secret_key": "",
    "amazon_paapi_partner_tag": "",              # usually same as associate tag
    "amazon_paapi_host": "",                     # e.g. webservices.amazon.com
    "amazon_paapi_region": "",                   # e.g. us-east-1

    # Amazon auto-forward (message listener)
    # Watches for Amazon links/ASINs and posts a rich embed into mapped destination channels.
    # For the "mixed" leads channel you can enable category routing: Grocery -> grocery channel, else -> default channel.
    "amazon_auto_forward_enabled": None,                         # bool
    "amazon_auto_forward_allow_bot_messages": None,              # bool (allow Collector/webhook/bot feeds)
    "amazon_auto_forward_category_source_channel_id": None,      # int (source channel id for category split)
    "amazon_auto_forward_category_grocery_dest_channel_id": None,# int (destination channel id, can be cross-guild)
    "amazon_auto_forward_category_default_dest_channel_id": None,# int (destination channel id, can be cross-guild)
    "amazon_auto_forward_cache_seconds": None,                   # int (cache product info per ASIN)
    "amazon_auto_forward_grocery_keywords": "",                  # CSV keywords; default uses a built-in list
    # Optional: per-destination embed templates (EmbedBuilder JSON + placeholders).
    # Shape:
    #   {
    #     "default": { ...embed json... },
    #     "<channel_id>": { ...embed json... },
    #     "by_channel_id": { "<channel_id>": { ...embed json... } }
    #   }
    # Supported placeholders include: {asin} {title} {final_url} {link} {price} {category} {image_url}
    # and source fields: {source_guild} {source_channel} {source_line} {source_jump} {source_message_id}
    "amazon_embed_templates": {},
    # Channel routing (replaces SQLite `channel_map` table)
    # Map: source_channel_id -> {"destination_channel_id": int, "enabled": bool}
    "channel_map": {},

    # Debug/testing
    "test_output_channel_id": None,              # int channel id to post /testallmessage output

    "mavely_session_token": "",                  # __Secure-next-auth.session-token value
    "mavely_auth_token": "",                     # optional Bearer token (preferred when GraphQL requires it)
    "mavely_graphql_endpoint": "",               # optional full URL override
    "mavely_min_seconds_between_requests": None, # float
    "mavely_request_timeout": None,              # int seconds
    "mavely_max_retries": None,                  # int

    # Auto affiliate rewrite (message listener)
    # When enabled, the bot will detect URLs in messages and reply with a rewritten copy:
    # - Amazon links -> tagged affiliate URL (optionally displayed as amzn.to/<random>)
    # - Other store links -> Mavely affiliate link
    "auto_affiliate_enabled": None,              # bool
    "auto_affiliate_channel_ids": "",            # optional CSV of channel ids to watch; blank = all channels in guild
    "auto_affiliate_output_channel_id": None,    # optional: post rewrites to this channel id (can be cross-guild)
    "auto_affiliate_delete_original": None,      # bool (requires Manage Messages)
    "auto_affiliate_allow_bot_messages": None,   # bool (allow Collector/webhook/bot posts to be rewritten)
    "auto_affiliate_rewrap_mavely_links": None,  # bool (re-expand mavely.app.link and re-create under YOUR Mavely)
    "auto_affiliate_dedupe_seconds": None,       # int (skip duplicate posts for same content in a short window)
    "auto_affiliate_expand_redirects": None,     # bool (expand bit.ly, amzn.to, walmrt.us, etc.)
    "auto_affiliate_max_redirects": None,        # int
    "auto_affiliate_expand_timeout_s": None,     # int
}

# -----------------------
# Runtime state (JSON)
# -----------------------

# -----------------------
# Utilities
# -----------------------
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

def utc_day_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def now_ts() -> int:
    return int(time.time())

def role_mention(role_id: Optional[int]) -> str:
    return f"<@&{role_id}>" if role_id else ""

def wrap_no_embed(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("<") and url.endswith(">"):
        return url
    return f"<{url}>"

def sha256_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()

def sanitize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _log_once(key: str, seconds: int = 60) -> bool:
    """
    Return True if we should log now, False if suppressed.
    Simple in-memory TTL to avoid console spam from repeated identical errors.
    """
    try:
        now = time.time()
        if not hasattr(_log_once, "_recent"):
            setattr(_log_once, "_recent", {})  # type: ignore[attr-defined]
        recent: dict = getattr(_log_once, "_recent")  # type: ignore[attr-defined]
        last = float(recent.get(key, 0.0) or 0.0)
        if last and (now - last) < float(seconds):
            return False
        recent[key] = now
        # opportunistic cleanup
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

def _strip_raw_links_section(text: str) -> str:
    """
    Some feeder bots append a verbose "Raw links:" section containing expanded tracking URLs.
    For affiliate rewrites, we never want to repost that section.
    """
    s = (text or "").strip()
    if not s:
        return s
    lines = s.splitlines()
    out: List[str] = []
    raw_only_mode = False
    for line in lines:
        if line.strip().lower().startswith("raw links"):
            # If we already had real content, drop the raw-links section entirely.
            if out:
                break
            # If the message is ONLY the raw-links dump, we will drop it entirely below.
            raw_only_mode = True
            continue
        out.append(line)
    # trim trailing empties
    while out and (not out[-1].strip()):
        out.pop()
    # If this was just a raw-links dump (label + urls), skip it entirely.
    if raw_only_mode:
        nonempty = [ln.strip() for ln in out if ln.strip()]
        only_urls = bool(nonempty) and all(_URL_RE.fullmatch(ln) for ln in nonempty)
        if only_urls:
            return ""
    return "\n".join(out).strip()

def _strip_reference_artifacts(text: str) -> str:
    """
    Safety filter: removes "reference" artifacts that should never be forwarded as part of a deal message.
    These can appear when users copy/paste Discord embed meta lines.
    """
    s = (text or "").strip()
    if not s:
        return s
    lines = s.splitlines()
    has_ref = False
    for ln in lines:
        t = (ln or "").strip()
        if "jump to original message" in t.lower():
            has_ref = True
            break
        if t.lower().startswith("from ") and ("/ #" in t):
            has_ref = True
            break
    if not has_ref:
        return s

    out: List[str] = []
    for ln in lines:
        t = (ln or "").strip()
        tl = t.lower()
        if not t:
            out.append(ln)
            continue
        if "jump to original message" in tl:
            continue
        if tl.startswith("from ") and ("/ #" in t):
            continue
        # Typical Discord author tag line when copied: Name#1234
        if re.match(r"^[^\s]{2,}#\d{4}$", t):
            continue
        # UI artifacts that sometimes get copied along with embed meta
        if t in {"Eyes", "APP"}:
            continue
        out.append(ln)
    return "\n".join(out).strip()

def _extract_markdown_link_target(markdown: str) -> Optional[str]:
    """
    If markdown is like: [amzn.to/xxxx](<https://...>) or [text](https://...)
    return the inner URL. Otherwise None.
    """
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
    """
    True if [label](URL) where URL span is [start:end] within `text`.
    """
    try:
        if start < 2 or end > len(text):
            return False
        # Look behind for the closest "](" immediately preceding the URL span,
        # allowing for optional whitespace and optional "<" wrapper:
        #   [label](https://...)
        #   [label](<https://...>)
        #   [label]( <https://...> )
        left = text[max(0, start - 12):start]
        j = left.rfind("](")
        if j < 0:
            return False
        between = left[j + 2:]  # chars between ]( and url span start
        if between.strip() not in {"", "<"}:
            return False
        right = text[end:min(len(text), end + 8)]
        r = right.lstrip()
        if r.startswith(")"):
            return True
        # If the URL span didn't include the closing ">" (rare), tolerate it.
        if r.startswith(">"):
            return r[1:].lstrip().startswith(")")
        return False
    except Exception:
        return False

def _message_to_text_for_rewrite(message: discord.Message) -> str:
    """
    Best-effort "full message" text for rewrite:
    - Feeder bots often put the actual deal text in embeds; we extract title/description/fields.
    - We append message.content, but strip/ignore the "Raw links:" label/section.
    """
    parts: List[str] = []

    # Embeds first (this is usually the real deal content)
    try:
        for e in (message.embeds or []):
            title = (getattr(e, "title", None) or "").strip()
            desc = (getattr(e, "description", None) or "").strip()
            url = (getattr(e, "url", None) or "").strip()
            if title:
                parts.append(str(title))
            if desc:
                parts.append(str(desc))
            # Only add embed.url if we don't already have any url in the extracted embed text.
            if url:
                already_has_url = bool(_URL_RE.search("\n".join(parts)))
                if (not already_has_url) and (url not in "\n".join(parts)):
                    parts.append(str(url))
            try:
                for f in (e.fields or []):
                    name = (getattr(f, "name", "") or "").strip()
                    value = (getattr(f, "value", "") or "").strip()
                    if name and name.strip().lower().startswith("raw links"):
                        continue
                    if value and value.strip().lower().startswith("raw links"):
                        continue
                    if name and value:
                        parts.append(f"{name}: {value}")
                    elif value:
                        parts.append(value)
            except Exception:
                pass
    except Exception:
        pass

    # Then message content (often contains tracking/raw link dumps)
    content = _strip_raw_links_section((message.content or "").strip())
    if content:
        # Many feeder bots duplicate the same deal text in BOTH embed and content.
        # If we already have meaningful embed text, only append content if it adds non-link text.
        embed_text = "\n\n".join([p for p in parts if p]).strip()
        content_lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
        content_is_only_urls = bool(content_lines) and all(_URL_RE.fullmatch(ln) for ln in content_lines)
        if embed_text and (content_is_only_urls or (content.strip() in embed_text)):
            pass
        else:
            parts.append(content)

    # Finally, include URLs that might only exist in embed metadata or link buttons.
    # This improves detection for feeder bots that only provide a link button / embed.url.
    try:
        existing = "\n\n".join([p for p in parts if p])
        for u in _collect_message_urls(message)[:12]:
            if u and (u not in existing):
                parts.append(u)
    except Exception:
        pass

    # De-dupe exact duplicate lines while keeping readability
    seen = set()
    final_lines: List[str] = []
    for line in "\n\n".join([p for p in parts if p]).splitlines():
        key = line.strip()
        if not key:
            final_lines.append("")
            continue
        if key in seen:
            continue
        seen.add(key)
        final_lines.append(line)
    text = _strip_reference_artifacts("\n".join(final_lines).strip())

    # De-dupe repeated blocks (feeds sometimes repeat the same 2-6 line block twice)
    blocks = [b.strip() for b in re.split(r"\n{2,}", text) if b.strip()]
    if len(blocks) <= 1:
        return text.strip()
    seen_blocks = set()
    kept: List[str] = []
    for b in blocks:
        key = sanitize_spaces(b).lower()
        if key in seen_blocks:
            continue
        seen_blocks.add(key)
        kept.append(b)
    return "\n\n".join(kept).strip()

def _parse_channel_id(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    cleaned = raw.strip()
    if cleaned.startswith("<#") and cleaned.endswith(">"):
        cleaned = cleaned[2:-1].strip()
    if not cleaned.isdigit():
        return None
    try:
        return int(cleaned)
    except (TypeError, ValueError):
        return None

def strip_prices(text: str) -> str:
    t = (text or "").replace("—", "-")
    t = re.sub(r"\$\s*\d[\d,]*(?:\.\d{1,2})?", "", t)          # $123.45
    t = re.sub(r"\b\d{2,}\s*[-–]\s*\d{2,}\+?\b", "", t)        # 200-260 or 200–260+
    t = re.sub(r"\b\d{2,}\+\b", "", t)                         # 200+
    t = re.sub(r"\b\d{2,}\b", "", t)                           # large standalone numbers
    t = re.sub(r"\s+", " ", t).strip()
    return t

def limit_to_two_sentences(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    parts = re.split(r"(?<=[.!])\s+", text)
    return " ".join(parts[:2]).strip()

def is_discord_internal_url(url: str) -> bool:
    u = (url or "").lower()
    return ("discord.com/channels" in u) or ("discord.gg/" in u) or ("discordapp.com/channels" in u)

MARKET_DOMAINS = (
    "ebay.", "stockx.", "goat.", "grailed.", "mercari.", "poshmark.",
    "depop.", "offerup.", "facebook.com/marketplace", "whatnot.", "etsy."
)

def is_market_url(url: str) -> bool:
    u = (url or "").lower()
    return any(d in u for d in MARKET_DOMAINS)

def pick_links(urls: List[str]) -> Tuple[str, str]:
    """
    Returns: (store_link, market_link)
    - Ignore Discord internal URLs.
    - market_link: first market URL
    - store_link: first non-market URL
    - If only a market URL exists, store_link stays empty.
    """
    clean = [u for u in (urls or []) if u and not is_discord_internal_url(u)]
    store = ""
    market = ""
    for u in clean:
        if not market and is_market_url(u):
            market = u
            continue
        if not store and not is_market_url(u):
            store = u
    return store, market

def detect_market_name_from_link(link: str) -> Optional[str]:
    l = (link or "").lower()
    if "stockx" in l:
        return "StockX"
    if "goat" in l:
        return "GOAT"
    if "ebay" in l:
        return "Ebay"
    if "grailed" in l:
        return "Grailed"
    if "mercari" in l:
        return "Mercari"
    if "poshmark" in l:
        return "Poshmark"
    if "etsy" in l:
        return "Etsy"
    return None

def _get_openai_model(cfg: dict) -> str:
    return (cfg.get("openai_model") or os.getenv("OPENAI_MODEL", "") or "").strip()

def _get_openai_temperature(cfg: dict) -> Optional[float]:
    raw = cfg.get("openai_temperature")
    if raw in (None, ""):
        raw = os.getenv("OPENAI_TEMPERATURE", "")
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None

def _bool_or_default(value: Optional[bool], default: bool) -> bool:
    return default if value is None else bool(value)

def _env_first_token(name: str, default: str = "") -> str:
    # tolerate values like: "1 (default on)" by taking the first token
    raw = (os.getenv(name, default) or "").strip()
    return raw.split()[0].strip() if raw else ""

def _get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default

# /instore test settings
TEST_MIN_UNIQUE_MESSAGES = _get_env_int("INSTORE_TEST_MIN_UNIQUE", 3)
TEST_HISTORY_LIMIT = _get_env_int("INSTORE_TEST_HISTORY_LIMIT", 50)

# -----------------------
# Affiliate helpers (Amazon + Mavely)
# -----------------------
def _env_bool(name: str) -> Optional[bool]:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return None
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return None

def _env_float(name: str) -> Optional[float]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None

def _cfg_or_env_str(cfg: dict, cfg_key: str, env_key: str) -> str:
    v = (cfg.get(cfg_key) or "").strip()
    return v if v else (os.getenv(env_key, "") or "").strip()

def _cfg_or_env_bool(cfg: dict, cfg_key: str, env_key: str) -> Optional[bool]:
    v = cfg.get(cfg_key)
    if isinstance(v, bool):
        return v
    return _env_bool(env_key)

def _cfg_or_env_int(cfg: dict, cfg_key: str, env_key: str) -> Optional[int]:
    v = cfg.get(cfg_key)
    if isinstance(v, int):
        return v
    raw = (os.getenv(env_key, "") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None

async def _fetch_amazon_custom(asin: str, endpoint_template: str) -> dict:
    """
    Calls your custom endpoint:
      https://your-api/amazon?asin={asin}
    Expected JSON keys (best effort): title, image_url (or image), price, category/categories
    """
    endpoint_template = (endpoint_template or "").strip()
    if not endpoint_template:
        return {}
    url = endpoint_template.format(asin=asin)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    try:
                        body = (await resp.text(errors="ignore"))[:300]
                    except Exception:
                        body = ""
                    return {"_error": f"custom_endpoint status={resp.status}", "_error_body": body}
                return await resp.json()
    except Exception:
        return {"_error": "custom_endpoint request failed"}

def _aws_sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def _aws_sigv4_authorization(
    *,
    access_key: str,
    secret_key: str,
    region: str,
    service: str,
    host: str,
    amz_date: str,
    date_stamp: str,
    method: str,
    canonical_uri: str,
    payload: str,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Create SigV4 headers for Amazon PA-API.
    """
    extra_headers = extra_headers or {}
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # Canonical headers must be lowercase, trimmed, and sorted.
    # PA-API commonly requires X-Amz-Target; if provided, it MUST be included in signing.
    def _norm_header_value(v: object) -> str:
        s = str(v or "")
        s = s.replace("\r", " ").replace("\n", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    headers_for_sign: Dict[str, str] = {
        "content-encoding": "amz-1.0",
        "content-type": "application/json; charset=utf-8",
        "host": str(host or "").strip(),
        "x-amz-date": str(amz_date or "").strip(),
    }
    for k, v in (extra_headers or {}).items():
        key = str(k or "").strip().lower()
        if not key:
            continue
        headers_for_sign[key] = _norm_header_value(v)

    header_names = sorted(headers_for_sign.keys())
    canonical_headers = "".join([f"{k}:{headers_for_sign[k]}\n" for k in header_names])
    signed_headers = ";".join(header_names)
    canonical_request = "\n".join([method, canonical_uri, "", canonical_headers, signed_headers, payload_hash])

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        algorithm,
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    k_date = _aws_sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _aws_sign(k_date, region)
    k_service = _aws_sign(k_region, service)
    k_signing = _aws_sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"{algorithm} "
        f"Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    headers = {
        "Content-Encoding": "amz-1.0",
        "Content-Type": "application/json; charset=utf-8",
        "X-Amz-Date": amz_date,
        "Authorization": authorization,
        "Host": host,
        "Accept": "application/json",
    }
    headers.update(extra_headers)
    return headers

async def _fetch_amazon_paapi(
    *,
    asin: str,
    access_key: str,
    secret_key: str,
    partner_tag: str,
    marketplace_url: str,
    host: str,
    region: str,
) -> dict:
    """
    Calls Amazon Product Advertising API (PA-API) GetItems.
    Returns JSON with keys similar to custom endpoint: title, image_url, price.
    """
    access_key = (access_key or "").strip()
    secret_key = (secret_key or "").strip()
    partner_tag = (partner_tag or "").strip()
    host = (host or "").strip()
    region = (region or "").strip()
    if not (asin and access_key and secret_key and partner_tag and host and region):
        return {}

    endpoint = f"https://{host}/paapi5/getitems"
    method = "POST"
    canonical_uri = "/paapi5/getitems"
    now = datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    marketplace = (marketplace_url or "").strip().rstrip("/")
    marketplace_domain = (urlparse(marketplace).netloc or "").strip()
    if not marketplace_domain:
        marketplace_domain = "www.amazon.com"

    payload_obj = {
        "ItemIds": [asin],
        "Resources": [
            "ItemInfo.Title",
            "Images.Primary.Medium",
            "Offers.Listings.Price",
            "BrowseNodeInfo.BrowseNodes",
            "ItemInfo.Classifications",
        ],
        "PartnerTag": partner_tag,
        "PartnerType": "Associates",
        "Marketplace": marketplace_domain,
    }
    payload = json.dumps(payload_obj)
    headers = _aws_sigv4_authorization(
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        service="ProductAdvertisingAPI",
        host=host,
        amz_date=amz_date,
        date_stamp=date_stamp,
        method=method,
        canonical_uri=canonical_uri,
        payload=payload,
        extra_headers={"X-Amz-Target": "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems"},
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint, data=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    try:
                        body = (await resp.text(errors="ignore"))[:500]
                    except Exception:
                        body = ""
                    return {"_error": f"paapi status={resp.status}", "_error_body": body}
                data = await resp.json()
    except Exception:
        return {"_error": "paapi request failed"}

    try:
        items = (((data or {}).get("ItemsResult") or {}).get("Items") or [])
        item0 = items[0] if items else {}
        title = (((item0.get("ItemInfo") or {}).get("Title") or {}).get("DisplayValue") or "").strip()
        image_url = (((((item0.get("Images") or {}).get("Primary") or {}).get("Medium") or {}).get("URL")) or "").strip()
        price = ""
        listings = (((item0.get("Offers") or {}).get("Listings") or []) or [])
        if listings:
            price = (((listings[0].get("Price") or {}).get("DisplayAmount")) or "").strip()
        # Best-effort category
        best_chain: List[str] = []
        try:
            browse_nodes = (((item0.get("BrowseNodeInfo") or {}).get("BrowseNodes") or []) or [])
            for node in browse_nodes:
                chain: List[str] = []
                cur = node
                depth = 0
                while isinstance(cur, dict) and depth < 7:
                    dn = (cur.get("DisplayName") or "").strip()
                    if dn:
                        chain.append(dn)
                    cur = cur.get("Ancestor")
                    depth += 1
                if chain:
                    chain = list(reversed(chain))
                    if len(chain) > len(best_chain):
                        best_chain = chain
        except Exception:
            best_chain = []

        product_group = ""
        try:
            product_group = (
                (((item0.get("ItemInfo") or {}).get("Classifications") or {}).get("ProductGroup") or {}).get("DisplayValue") or ""
            ).strip()
        except Exception:
            product_group = ""
        out = {}
        if title:
            out["title"] = title
        if image_url:
            out["image_url"] = image_url
        if price:
            out["price"] = price
        if best_chain:
            out["categories"] = best_chain
            out["category"] = best_chain[-1]
        elif product_group:
            out["category"] = product_group
        if product_group:
            out["product_group"] = product_group
        return out
    except Exception:
        return {}

def _amazon_cache_ttl_seconds(cfg: dict) -> int:
    v = _cfg_or_env_int(cfg, "amazon_auto_forward_cache_seconds", "AMAZON_AUTO_CACHE_SECONDS")
    if v is None:
        v = 3600
    try:
        v = int(v)
    except (TypeError, ValueError):
        v = 3600
    return max(0, min(v, 24 * 3600))

def _amazon_cache_key(cfg: dict, asin: str) -> str:
    marketplace = _cfg_or_env_str(cfg, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
    return f"{(marketplace or 'default').lower()}|{(asin or '').upper()}"

async def _fetch_amazon_product(cfg: dict, asin: str, *, strict: bool) -> Tuple[dict, Optional[str]]:
    api_enabled = _cfg_or_env_bool(cfg, "amazon_api_enabled", "AMAZON_API_ENABLED")
    api_enabled = bool(api_enabled) if api_enabled is not None else False
    if not api_enabled:
        return {}, None

    endpoint = _cfg_or_env_str(cfg, "amazon_custom_endpoint", "AMAZON_CUSTOM_ENDPOINT")
    if endpoint:
        data = await _fetch_amazon_custom(asin, endpoint)
        if isinstance(data, dict) and data.get("_error"):
            body = str(data.get("_error_body") or "").strip()
            extra = (f" ({body[:140]})" if body else "")
            return {}, f"{str(data.get('_error') or 'custom endpoint error')}{extra}"
        if strict and (not data):
            return {}, "Custom endpoint returned empty data."
        return data or {}, None

    access_key = _cfg_or_env_str(cfg, "amazon_paapi_access_key", "AMAZON_PAAPI_ACCESS_KEY")
    secret_key = _cfg_or_env_str(cfg, "amazon_paapi_secret_key", "AMAZON_PAAPI_SECRET_KEY")
    partner_tag = _cfg_or_env_str(cfg, "amazon_paapi_partner_tag", "AMAZON_PAAPI_PARTNER_TAG")
    pa_host = _cfg_or_env_str(cfg, "amazon_paapi_host", "AMAZON_PAAPI_HOST")
    pa_region = _cfg_or_env_str(cfg, "amazon_paapi_region", "AMAZON_PAAPI_REGION")

    marketplace = _cfg_or_env_str(cfg, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
    if not pa_host:
        try:
            domain = (urlparse(marketplace).netloc or "").strip().lower()
            if domain.startswith("www."):
                domain = domain[4:]
            if domain:
                pa_host = f"webservices.{domain}"
        except Exception:
            pa_host = ""

    if not (access_key and secret_key and partner_tag and pa_host and pa_region):
        if strict:
            return {}, (
                "Amazon API enrichment is enabled, but neither `amazon_custom_endpoint` nor PA-API keys are configured "
                "(need AMAZON_PAAPI_ACCESS_KEY / AMAZON_PAAPI_SECRET_KEY / AMAZON_PAAPI_PARTNER_TAG / AMAZON_PAAPI_REGION)."
            )
        return {}, None

    product = await _fetch_amazon_paapi(
        asin=asin,
        access_key=access_key,
        secret_key=secret_key,
        partner_tag=partner_tag,
        marketplace_url=marketplace or f"https://www.amazon.com/dp/{asin}",
        host=pa_host,
        region=pa_region,
    )
    if isinstance(product, dict) and product.get("_error"):
        body = str(product.get("_error_body") or "").strip()
        extra = (f" ({body[:200]})" if body else "")
        return {}, f"{str(product.get('_error') or 'PA-API error')}{extra}"
    if strict and (not product):
        return {}, "Amazon PA-API returned empty data (check credentials/permissions)."
    return product or {}, None

async def _get_amazon_product_cached(cfg: dict, asin: str, *, strict: bool = False) -> Tuple[dict, Optional[str]]:
    asin = (asin or "").strip().upper()
    if not asin:
        return {}, None

    ttl = _amazon_cache_ttl_seconds(cfg)
    key = _amazon_cache_key(cfg, asin)
    now = time.time()
    if ttl > 0:
        ent = _amazon_product_cache.get(key)
        if ent:
            ts, data = ent
            try:
                if (now - float(ts)) < float(ttl):
                    return (data or {}), None
            except Exception:
                pass
        # Persistent cache (survives restarts) stored in runtime JSON
        try:
            st = _runtime_state if isinstance(_runtime_state, dict) else None  # type: ignore[name-defined]
        except Exception:
            st = None
        if st:
            try:
                cache = st.get("amazon_product_cache") if isinstance(st.get("amazon_product_cache"), dict) else {}
                entry = cache.get(str(key)) if isinstance(cache, dict) else None
                if isinstance(entry, dict):
                    ts2 = float(entry.get("ts", 0.0) or 0.0)
                    data2 = entry.get("data") if isinstance(entry.get("data"), dict) else {}
                    if data2 and ts2 and ((now - ts2) < float(ttl)):
                        _amazon_product_cache[key] = (ts2, data2)
                        return data2, None
            except Exception:
                pass

    product, err = await _fetch_amazon_product(cfg, asin, strict=strict)
    if (not err) and ttl > 0 and product:
        _amazon_product_cache[key] = (now, product)
        # Persist cache
        try:
            async with _runtime_lock:  # type: ignore[name-defined]
                st2 = await _runtime_load()  # type: ignore[name-defined]
                cache2 = st2.get("amazon_product_cache") if isinstance(st2.get("amazon_product_cache"), dict) else {}
                cache2[str(key)] = {"ts": float(now), "data": product}
                st2["amazon_product_cache"] = cache2
                await _runtime_save(st2)  # type: ignore[name-defined]
        except Exception:
            pass
    return product or {}, err

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

_ALIAS_ALPHABET = string.ascii_lowercase + string.digits

def _make_alias_slug(length: int = 7) -> str:
    n = max(4, min(int(length or 7), 20))
    return "".join(secrets.choice(_ALIAS_ALPHABET) for _ in range(n))

def _discord_masked_link(display_prefix: str, target_url: str, *, slug_len: int = 7, slug: Optional[str] = None) -> str:
    """
    Create a Discord markdown link where the visible text looks like amzn.to/xxxxxxx
    but the real target is the Amazon URL.
    """
    prefix = (display_prefix or "amzn.to").strip().rstrip("/")
    target = (target_url or "").strip()
    n = max(4, min(int(slug_len or 7), 20))
    if slug:
        s = "".join(ch for ch in str(slug).strip().lower() if ch.isalnum())
        s = (s[:n] if s else "")
        slug_out = s if s else _make_alias_slug(n)
    else:
        slug_out = _make_alias_slug(n)
    return f"[{prefix}/{slug_out}](<{target}>)"

_URL_RE = re.compile(
    r"((?:https?://)?(?:www\.)?[a-z0-9][a-z0-9.-]*\.[a-z]{2,}(?:/[^\s<>()]*)?)",
    re.IGNORECASE,
)

def _extract_urls_with_spans(text: str) -> List[Tuple[str, int, int]]:
    """
    Return [(url, start, end)] spans for URLs in text.
    Strips common trailing punctuation without breaking the span math.
    """
    s = text or ""
    out: List[Tuple[str, int, int]] = []
    for m in _URL_RE.finditer(s):
        raw = m.group(1)
        start = int(m.start(1))
        end = int(m.end(1))
        # Trim common trailing punctuation that is not part of the URL
        trimmed = raw
        while trimmed and trimmed[-1] in ".,);]}>":
            trimmed = trimmed[:-1]
            end -= 1
        trimmed = trimmed.strip()
        if trimmed and end > start:
            # If the URL is wrapped as <https://...>, expand the span so we don't
            # leave a trailing ">" around our replacement.
            if start > 0 and end < len(s) and s[start - 1] == "<" and s[end] == ">":
                out.append((trimmed, start - 1, end + 1))
            else:
                out.append((trimmed, start, end))
    return out

def _collect_message_urls(message: discord.Message) -> List[str]:
    """
    Collect URLs from every place Discord can store them:
    - message.content
    - embed.url
    - embed.author.url
    - embed field names/values
    - embed title/description
    - link buttons (message components)
    Dedupes while keeping stable order.
    """
    urls: List[str] = []

    def _push(u: Optional[str]) -> None:
        s = (u or "").strip()
        if not s:
            return
        # Collect explicit URLs in the string (handles raw + markdown links)
        for (cand, _, _) in _extract_urls_with_spans(s):
            u2 = _normalize_input_url(cand)
            if u2:
                urls.append(u2)
        # Also accept a single bare URL-like string
        u3 = _normalize_input_url(s)
        if u3 and (u3.startswith("http://") or u3.startswith("https://")):
            urls.append(u3)

    # content
    try:
        _push(message.content or "")
    except Exception:
        pass

    # embeds
    try:
        for e in (message.embeds or []):
            _push(getattr(e, "url", None))
            try:
                author = getattr(e, "author", None)
                _push(getattr(author, "url", None))
            except Exception:
                pass
            _push(getattr(e, "title", None))
            _push(getattr(e, "description", None))
            try:
                for f in (getattr(e, "fields", None) or []):
                    _push(getattr(f, "name", None))
                    _push(getattr(f, "value", None))
            except Exception:
                pass
    except Exception:
        pass

    # components (link buttons)
    try:
        for row in (getattr(message, "components", None) or []):
            for child in (getattr(row, "children", None) or []):
                _push(getattr(child, "url", None))
    except Exception:
        pass

    seen = set()
    out: List[str] = []
    for u in urls:
        k = (u or "").strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(u)
    return out

def _normalize_input_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return u
    # Guard: Discord mentions are not URLs (prevents "https://@everyone" noise)
    low = u.lower()
    if low in {"@everyone", "@here"} or low.startswith("@"):
        return ""
    if low.startswith("<@") or low.startswith("<#") or low.startswith("<@&"):
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        # Additional guard for already-schemed mention accidents like https://@everyone
        try:
            parsed = urlparse(u)
            netloc = (parsed.netloc or "").strip().lower()
            if netloc.startswith("@") or netloc in {"@everyone", "@here"}:
                return ""
        except Exception:
            pass
        return u
    # Accept bare domains like ringinthedeals.com/deal/...
    return f"https://{u}"

def _parse_csv_ints(raw: str) -> List[int]:
    vals: List[int] = []
    for part in (raw or "").replace("\n", ",").split(","):
        p = (part or "").strip()
        if not p:
            continue
        try:
            n = int(p)
        except ValueError:
            continue
        if n > 0:
            vals.append(n)
    return vals

def _normalize_channel_map(value: object) -> Dict[str, dict]:
    """
    Normalize config `channel_map` into a canonical dict:
      { "<source_channel_id>": {"destination_channel_id": <int>, "enabled": <bool>} }
    Accepts:
    - {"123": 456}
    - {"123": {"destination_channel_id": 456, "enabled": true}}
    - {123: 456}
    """
    out: Dict[str, dict] = {}
    if not isinstance(value, dict):
        return out

    for k, v in value.items():
        try:
            src = int(str(k).strip())
        except Exception:
            continue
        if src <= 0:
            continue

        dest: Optional[int] = None
        enabled = True

        if isinstance(v, dict):
            cand = v.get("destination_channel_id")
            if cand in (None, "", 0):
                cand = v.get("dest") or v.get("destination") or v.get("to")
            try:
                dest = int(str(cand).strip()) if cand not in (None, "", 0) else None
            except Exception:
                dest = None
            enabled = bool(v.get("enabled", True))
        else:
            try:
                dest = int(str(v).strip()) if v not in (None, "", 0) else None
            except Exception:
                dest = None
            enabled = True

        if dest and dest > 0:
            out[str(src)] = {"destination_channel_id": int(dest), "enabled": bool(enabled)}

    return out

def _is_amazon_like_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return ("amazon." in host) or host.endswith("amazon.com") or host.endswith("amazon.co.uk") or ("amzn.to" in host)

def _is_mavely_link(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return "mavely.app.link" in host

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
    """
    Fix up "expanded" URLs that land on interstitial/blocked pages.
    Example: Walmart shortlinks sometimes end at /blocked?url=<base64(path+query)>
    """
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
            # decoded is usually a path like "/ip/565037027?irgwc=1&..."
            if decoded.startswith("http://") or decoded.startswith("https://"):
                return decoded
            if decoded.startswith("/"):
                return f"{parsed.scheme or 'https'}://{parsed.netloc}{decoded}"
    return u

def _extract_first_outbound_url_from_html(html: str) -> Optional[str]:
    """
    Best-effort extraction for "deal hub" pages that aren't pure redirects.
    Looks for obvious outbound URLs like amazon.com, amzn.to, walmart.com, target.com.
    """
    t = (html or "")[:200_000]
    if not t:
        return None
    # Prefer explicit deal buttons when present.
    for label in ("Go to Deal", "Continue to Amazon", "Claim Amazon Deal", "Claim Deal"):
        m_btn = re.search(rf'href="([^"]+)"[^>]*>\s*{re.escape(label)}', t, re.IGNORECASE)
        if m_btn:
            return _html.unescape((m_btn.group(1) or "").strip()) or None

    # Prefer full URLs, then short URLs that we can expand further.
    patterns = [
        r"https?://(?:www\.)?amazon\.[^\s\"'<>]+",
        r"https?://amzn\.to/[A-Za-z0-9]+",
        r"https?://saveyourdeals\.com/[A-Za-z0-9]+",
        r"https?://(?:www\.)?dealsabove\.com/[^\s\"'<>]+",
        r"https?://(?:www\.)?walmart\.com/[^\s\"'<>]+",
        r"https?://walmrt\.us/[A-Za-z0-9]+",
        r"https?://(?:www\.)?target\.com/[^\s\"'<>]+",
        r"https?://bit\.ly/[A-Za-z0-9]+",
    ]
    for pat in patterns:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            return _html.unescape((m.group(0) or "").strip()) or None
    return None

async def _expand_url(
    session: aiohttp.ClientSession,
    url: str,
    *,
    timeout_s: float = 8.0,
    max_redirects: int = 8,
) -> str:
    """
    Resolve common short/redirect links to their final URL by following redirects.
    Uses HEAD first (fast), then GET fallback when HEAD isn't supported.
    """
    u = (url or "").strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return u

    timeout = aiohttp.ClientTimeout(total=timeout_s)
    # Some redirectors are picky; use a real browser UA.
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
            final = str(resp.url)
            return _normalize_expanded_url(final or u)
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
            # We only care about the final URL; don't download the body.
            try:
                await resp.content.read(0)
            except Exception:
                pass
            final = str(resp.url)
            return _normalize_expanded_url(final or u)
    except Exception:
        pass

    # Final fallback: use requests (often succeeds where aiohttp doesn't for certain redirectors)
    try:
        import requests  # already a dependency in this project

        def _do() -> str:
            r = requests.get(u, allow_redirects=True, timeout=max(5, int(timeout_s)), headers={"User-Agent": ua})
            return r.url or u

        final = await asyncio.to_thread(_do)
        return _normalize_expanded_url(final or u)
    except Exception:
        return u

def _should_expand_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if not host:
        return False

    # Expand common shorteners/redirectors. Allow user overrides via env.
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
        "mavely.app.link",
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

def _unwrap_known_query_redirects(url: str) -> Optional[str]:
    """
    Some "deal hub" links embed the real destination as a query parameter.
    Example: fkd.deals/?product=https://amzn.to/xxxx
    """
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
        # Sometimes these use ?url= or similar
        for k in ("url", "u", "target", "dest"):
            cand = (q.get(k) or "").strip()
            if cand.startswith("http://") or cand.startswith("https://"):
                return cand
    if host in {"www.dealsabove.com", "dealsabove.com"}:
        # Example: https://www.dealsabove.com/product-redirect?l=https://www.amazon.com/dp/...
        cand = (q.get("l") or q.get("url") or q.get("u") or "").strip()
        # Some sources double-encode; be tolerant.
        cand = unquote(cand) if cand else cand
        if cand.startswith("http://") or cand.startswith("https://"):
            return cand
    return None

def _build_amazon_affiliate_url(cfg: dict, raw_url: str) -> Optional[str]:
    """
    Build a tagged Amazon affiliate URL (no network calls).
    Returns None if we can't confidently treat this as an Amazon product URL.
    """
    u = (raw_url or "").strip()
    if not u:
        return None

    asin = None
    if extract_asin is not None:
        try:
            asin = extract_asin(u)  # type: ignore[misc]
        except Exception:
            asin = None
    if not asin:
        asin = _extract_asin_fallback(u)
    if not asin:
        return None

    marketplace = _cfg_or_env_str(cfg, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
    if marketplace:
        canon_url = f"{marketplace}/dp/{asin}"
    else:
        # Fall back to host from the URL
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

async def _compute_affiliate_rewrites(cfg: dict, urls: List[str]) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """
    Core auto-affiliate logic shared by:
    - message rewriting
    - startup self-tests

    Returns:
    - mapped: original_url_text -> replacement_text
    - resolved: original_url_text -> expanded_destination_url
    - notes: original_url_text -> short reason string for logs
    """
    unique = list(dict.fromkeys([(u or "").strip() for u in (urls or []) if (u or "").strip()]))
    if not unique:
        return {}, {}, {}

    normalized = {u: _normalize_input_url(u) for u in unique}
    mapped: Dict[str, str] = {}
    notes: Dict[str, str] = {}
    rewrap_mavely = _bool_or_default(cfg.get("auto_affiliate_rewrap_mavely_links"), False)
    # Ensure Amazon masked links are stable within the same rewrite (avoid duplicate-looking blocks)
    amazon_mask_cache: Dict[str, str] = {}

    # Expand redirect/short links first so Amazon/Mavely detection uses the real destination.
    expand_enabled = _bool_or_default(cfg.get("auto_affiliate_expand_redirects"), True)
    max_redirects = _cfg_or_env_int(cfg, "auto_affiliate_max_redirects", "AUTO_AFFILIATE_MAX_REDIRECTS") or 8
    timeout_s = _cfg_or_env_int(cfg, "auto_affiliate_expand_timeout_s", "AUTO_AFFILIATE_EXPAND_TIMEOUT_S") or 8
    resolved: Dict[str, str] = {u: normalized.get(u) or u for u in unique}

    # Cheap unwrapping for known query-based redirectors.
    for u in unique:
        cand = _unwrap_known_query_redirects(resolved.get(u) or u)
        if cand:
            resolved[u] = cand
            notes[u] = "unwrapped query redirect"

    if expand_enabled:
        async with aiohttp.ClientSession() as session:
            for u in unique:
                start_u = (resolved.get(u) or u).strip()
                if _is_mavely_link(start_u) and (not rewrap_mavely):
                    continue
                if _should_expand_url(start_u) or (rewrap_mavely and _is_mavely_link(start_u)):
                    final_u = await _expand_url(session, start_u, timeout_s=float(timeout_s), max_redirects=int(max_redirects))
                    resolved[u] = final_u
                    if final_u != start_u and u not in notes:
                        notes[u] = "expanded redirects"

                    # Some redirectors embed the real destination as a query param (unwrap AFTER expansion too).
                    cand2 = _unwrap_known_query_redirects(final_u)
                    if cand2:
                        resolved[u] = cand2
                        notes[u] = "unwrapped after expand"
                        final_u = cand2

                    # If we ended on a deal hub that isn't a redirect, extract outbound links from HTML.
                    # Supports multi-step hubs:
                    #   pricedoffers.com -> saveyourdeals.com -> amazon.com (Go to Deal)
                    special_html_hosts = {
                        "deals.pennyexplorer.com",
                        "ringinthedeals.com",
                        "dmflip.com",
                        "trackcm.com",
                        "joylink.io",
                        "fkd.deals",
                        "pricedoffers.com",
                        "saveyourdeals.com",
                        "dealsabove.com",
                        "www.dealsabove.com",
                        "mavely.app.link",
                    }
                    candidate = final_u
                    for _ in range(3):
                        try:
                            host = (urlparse(candidate).netloc or "").lower()
                        except Exception:
                            host = ""
                        if host not in special_html_hosts:
                            break
                        try:
                            async with session.get(candidate, timeout=aiohttp.ClientTimeout(total=float(timeout_s))) as resp:
                                txt = await resp.text(errors="ignore")
                            out = _extract_first_outbound_url_from_html(txt)
                            if not out:
                                break
                            out_abs = out
                            if out_abs.startswith("/"):
                                out_abs = urljoin(candidate, out_abs)
                            out_abs = _unwrap_known_query_redirects(out_abs) or out_abs
                            candidate = out_abs
                        except Exception:
                            break
                    if candidate and candidate != final_u:
                        resolved[u] = candidate
                        notes[u] = "extracted outbound url"

    # Now build replacements using the resolved destination.
    for u in unique:
        raw = (normalized.get(u) or u).strip()
        target = (resolved.get(u) or raw).strip()

        if _is_mavely_link(raw):
            if not rewrap_mavely:
                notes.setdefault(u, "already mavely link")
                continue
            # Rewrap: expand the mavely link to its destination, then generate OUR mavely link.
            if target and (not _is_mavely_link(target)) and (target != raw):
                link, err = await _mavely_create_link(cfg, target)
                if link and not err:
                    mapped[u] = link
                    notes.setdefault(u, "rewrapped mavely link")
                else:
                    notes.setdefault(u, err or "rewrap failed")
            else:
                notes.setdefault(u, "rewrap skipped (no expanded destination)")
            continue

        if _is_mavely_link(target) and (target != raw):
            mapped[u] = target
            notes.setdefault(u, "resolves to mavely link")
            continue

        if _is_amazon_like_url(target):
            affiliate_url = _build_amazon_affiliate_url(cfg, target)
            if not affiliate_url:
                notes.setdefault(u, "amazon link but no asin")
                continue
            final_url = affiliate_url

            raw_mask = _env_first_token("AMAZON_DISCORD_MASK_LINK", "1").lower()
            mask_enabled = raw_mask in {"1", "true", "yes", "y", "on"}
            mask_prefix = _env_first_token("AMAZON_DISCORD_MASK_PREFIX", "amzn.to") or "amzn.to"
            try:
                mask_len = int(_env_first_token("AMAZON_DISCORD_MASK_LEN", "7") or "7")
            except ValueError:
                mask_len = 7

            if mask_enabled:
                # Reuse same mask for the same destination URL within this message
                rep = amazon_mask_cache.get(final_url)
                if not rep:
                    rep = _discord_masked_link(mask_prefix, final_url, slug_len=mask_len)
                    amazon_mask_cache[final_url] = rep
                mapped[u] = rep
            else:
                mapped[u] = final_url
            notes.setdefault(u, "amazon affiliate")
            continue

        # Non-Amazon: try Mavely on the resolved destination
        link, err = await _mavely_create_link(cfg, target)
        if link and not err:
            mapped[u] = link
            notes.setdefault(u, "mavely affiliate")
        elif target and (target != raw):
            mapped[u] = target
            notes.setdefault(u, "expanded only (mavely unsupported)")
        else:
            notes.setdefault(u, err or "no change")

    return mapped, resolved, notes

async def _rewrite_message_with_affiliates(cfg: dict, text: str) -> Tuple[str, bool, Dict[str, str], Dict[str, str]]:
    """
    Return (rewritten_text, changed, mapped, notes).
    - Amazon links become affiliate links (optionally masked display)
    - Other store links become Mavely links (when available)
    - Short/redirect links are expanded first (best effort)
    """
    original = text or ""
    spans = _extract_urls_with_spans(original)
    if not spans:
        return original, False, {}, {}

    urls = [u for (u, _, _) in spans]
    mapped, _resolved, notes = await _compute_affiliate_rewrites(cfg, urls)
    if not mapped:
        return original, False, {}, notes or {}

    changed = False
    out = original
    for (u, start, end) in sorted(spans, key=lambda t: t[1], reverse=True):
        rep = mapped.get(u)
        if rep and rep != u:
            rep_out = rep
            # If we're inside an existing markdown link target: [label](URL)
            # do NOT inject a markdown link (it breaks formatting). Instead, swap in the real URL.
            in_md_target = _is_markdown_link_target_context(original, start, end)
            if in_md_target and rep_out.lstrip().startswith("["):
                target = _extract_markdown_link_target(rep_out)
                if target:
                    rep_out = target

            # Preserve no-embed wrapper if the original URL span was <...>
            try:
                wrapped = (original[start] == "<") and (original[end - 1] == ">")
            except Exception:
                wrapped = False
            if wrapped and rep_out and (not rep_out.startswith("<")) and (not rep_out.lstrip().startswith("[")):
                rep_out = f"<{rep_out.strip()}>"

            out = out[:start] + rep_out + out[end:]
            changed = True

    return out, changed, mapped, (notes or {})

async def _maybe_auto_affiliate_rewrite(message: discord.Message, cfg: dict) -> None:
    """
    If enabled in config, detect store/Amazon URLs and reply with a rewritten copy.
    This does NOT edit user messages (Discord limitation); it replies and can optionally delete the original.
    """
    enabled = _bool_or_default(cfg.get("auto_affiliate_enabled"), False)
    if not enabled:
        return

    allow_raw = (cfg.get("auto_affiliate_channel_ids") or "").strip()
    if allow_raw:
        allowed = set(_parse_csv_ints(allow_raw))
        if allowed and (message.channel.id not in allowed):
            return

    content = _message_to_text_for_rewrite(message)
    if not content:
        return

    # Dedupe: avoid spamming if the same feed posts identical content repeatedly.
    dedupe_s = _cfg_or_env_int(cfg, "auto_affiliate_dedupe_seconds", "AUTO_AFFILIATE_DEDUPE_SECONDS")
    if dedupe_s is None:
        dedupe_s = 45
    try:
        dedupe_s = int(dedupe_s)
    except (TypeError, ValueError):
        dedupe_s = 45
    dedupe_s = max(0, min(dedupe_s, 3600))

    if dedupe_s > 0:
        now = time.time()
        key = f"{message.guild.id}:{message.channel.id}:{sha256_text(content)}"
        # simple TTL cache
        if not hasattr(_maybe_auto_affiliate_rewrite, "_recent"):
            setattr(_maybe_auto_affiliate_rewrite, "_recent", {})  # type: ignore[attr-defined]
        recent: dict = getattr(_maybe_auto_affiliate_rewrite, "_recent")  # type: ignore[attr-defined]
        last = float(recent.get(key, 0.0) or 0.0)
        if last and (now - last) < dedupe_s:
            return
        recent[key] = now
        # opportunistic cleanup
        if len(recent) > 500:
            cutoff = now - float(dedupe_s)
            for k in list(recent.keys())[:200]:
                try:
                    if float(recent.get(k, 0.0) or 0.0) < cutoff:
                        recent.pop(k, None)
                except Exception:
                    pass

    rewritten, changed, mapped, notes = await _rewrite_message_with_affiliates(cfg, content)
    if not changed or not rewritten or (rewritten == content):
        return

    # Route to a destination channel:
    # - Prefer an explicit channel map entry for this source channel (supports per-channel routing).
    # - Fall back to the configured output channel id (legacy single-output behavior).
    mapped_dest = None
    try:
        mapped_dest = await get_destination_for_source(int(message.guild.id), message.channel.id)
    except Exception:
        mapped_dest = None
    out_id = mapped_dest if mapped_dest else cfg.get("auto_affiliate_output_channel_id")
    try:
        out_id_int = int(str(out_id).strip()) if out_id not in (None, "", 0) else 0
    except Exception:
        out_id_int = 0
    if not out_id_int:
        # If output isn't configured, do nothing (do NOT reply in the source channel).
        if _log_once("auto_affiliate_missing_output", seconds=300):
            log.warning("Auto-affiliate is enabled but no output channel is configured. Skipping rewrite.")
        return
    if message.channel.id == out_id_int:
        # Never process messages in the output channel (prevents rewrite loops).
        return

    out_ch = bot.get_channel(out_id_int)
    if not isinstance(out_ch, discord.TextChannel):
        try:
            fetched = await bot.fetch_channel(out_id_int)
            if isinstance(fetched, discord.TextChannel):
                out_ch = fetched
        except Exception:
            out_ch = None
    if not isinstance(out_ch, discord.TextChannel):
        if _log_once(f"auto_affiliate_output_unresolved:{out_id_int}", seconds=300):
            log.warning("Auto-affiliate output channel could not be resolved (id=%s). Skipping rewrite.", out_id_int)
        return

    # Safety: never post outside the control guild.
    try:
        ctrl = await _get_control_guild_id()
    except Exception:
        ctrl = 0
    if ctrl and int(out_ch.guild.id) != int(ctrl):
        if _log_once(f"auto_affiliate_blocked_dest_guild:{out_ch.guild.id}", seconds=300):
            log.warning("Auto-affiliate blocked: destination is outside control guild (dest_guild=%s)", out_ch.guild.id)
        return

    try:
        # Output must be ONLY the rewritten message text (no notes/headers/embeds).
        await out_ch.send(rewritten, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        return

    # Human-friendly terminal logs for live verification
    try:
        dst = f"{out_ch.guild.name}/#{out_ch.name}"
        src = f"{message.guild.name}/#{message.channel.name}"
        log.info("Auto-affiliate forwarded: %s -> %s (msg=%s)", src, dst, message.id)
        for u, rep in (mapped or {}).items():
            if rep and rep != u:
                log.info("Auto-affiliate: %s -> %s", u, rep)
        # If we changed something but a URL had an error note, show it once.
        for u, note in (notes or {}).items():
            if note and ("failed" in str(note).lower() or "error" in str(note).lower() or "expired" in str(note).lower()):
                log.warning("Auto-affiliate issue: %s (%s)", u, note)
    except Exception:
        pass

    delete_original = _bool_or_default(cfg.get("auto_affiliate_delete_original"), False)
    if delete_original:
        try:
            perms = message.channel.permissions_for(message.guild.me)  # type: ignore[arg-type]
            if getattr(perms, "manage_messages", False):
                await message.delete()
        except Exception:
            pass

async def _maybe_auto_amazon_forward(message: discord.Message, cfg: dict) -> bool:
    """
    If enabled in config, detect Amazon links/ASINs and post a rich embed to the appropriate destination.
    This is independent from auto-affiliate rewrite.
    Returns True if we posted something (handled), else False.
    """
    enabled = _bool_or_default(cfg.get("amazon_auto_forward_enabled"), False)
    if not enabled:
        return False
    plan = await _plan_amazon_forward_for_message(message, cfg)
    if not plan:
        return False

    dest_id = int(plan.get("dest_id") or 0)
    if not dest_id:
        return False

    out_ch = bot.get_channel(int(dest_id))
    if not isinstance(out_ch, discord.TextChannel):
        try:
            fetched = await bot.fetch_channel(int(dest_id))
            out_ch = fetched if isinstance(fetched, discord.TextChannel) else None
        except Exception:
            out_ch = None
    if not isinstance(out_ch, discord.TextChannel):
        return False

    # Safety: never post outside the control guild.
    try:
        ctrl = await _get_control_guild_id()
    except Exception:
        ctrl = 0
    if ctrl and int(out_ch.guild.id) != int(ctrl):
        if _log_once(f"amazon_blocked_dest_guild:{out_ch.guild.id}", seconds=300):
            log.warning("Amazon auto-forward blocked: destination outside control guild (dest_guild=%s)", out_ch.guild.id)
        return False

    try:
        await out_ch.send(
            content=plan.get("content_link") or None,
            embed=plan.get("embed"),
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except Exception:
        return False

    try:
        dst = f"{out_ch.guild.name}/#{out_ch.name}"
        src = f"{message.guild.name}/#{message.channel.name}"
        log.info("Amazon auto-forward: %s -> %s (asin=%s msg=%s)", src, dst, plan.get("asin") or "", message.id)
    except Exception:
        pass
    return True

async def _plan_amazon_forward_for_message(message: discord.Message, cfg: dict) -> Optional[dict]:
    """
    Build the Amazon auto-forward payload for a given Discord message WITHOUT sending it.
    Returns None if the message isn't an Amazon lead we can process.

    Output dict keys:
      asin, amazon_url, final_url, product, category_path, is_grocery,
      dest_id, dest_reason, embed, content_link
    """
    text = _message_to_text_for_rewrite(message)
    if not text:
        return None

    asin = _extract_asin_fallback(text)
    amazon_url: Optional[str] = None

    if not asin:
        urls = _collect_message_urls(message)
        if not urls:
            return None

        timeout_s = _cfg_or_env_int(cfg, "auto_affiliate_expand_timeout_s", "AUTO_AFFILIATE_EXPAND_TIMEOUT_S") or 8
        max_redirects = _cfg_or_env_int(cfg, "auto_affiliate_max_redirects", "AUTO_AFFILIATE_MAX_REDIRECTS") or 8
        timeout_s = max(3, min(int(timeout_s), 20))
        max_redirects = max(1, min(int(max_redirects), 12))

        ua = (os.getenv("MAVELY_USER_AGENT", "") or "").strip() or "Mozilla/5.0"
        headers = {"User-Agent": ua, "Accept": "text/html,*/*"}

        async with aiohttp.ClientSession() as session:
            for raw in urls[:12]:
                u0 = _normalize_input_url(raw)
                if not u0:
                    continue
                cand = _unwrap_known_query_redirects(u0) or u0

                try:
                    if extract_asin is not None:
                        asin = extract_asin(cand)  # type: ignore[misc]
                except Exception:
                    asin = None
                if not asin:
                    asin = _extract_asin_fallback(cand)
                if asin:
                    amazon_url = cand
                    break

                target = cand
                if _should_expand_url(target):
                    try:
                        target = await _expand_url(session, target, timeout_s=float(timeout_s), max_redirects=int(max_redirects))
                    except Exception:
                        target = cand

                if target and (not _is_amazon_like_url(target)):
                    try:
                        host = (urlparse(target).netloc or "").lower()
                    except Exception:
                        host = ""
                    if any(h in host for h in ("dealsabove.com", "saveyourdeals.com", "pricedoffers.com", "ringinthedeals.com", "dmflip.com")):
                        try:
                            async with session.get(target, timeout=aiohttp.ClientTimeout(total=float(timeout_s)), headers=headers) as resp:
                                html = await resp.text(errors="ignore")
                            outbound = _extract_first_outbound_url_from_html(html or "")
                            if outbound:
                                outbound = _normalize_input_url(outbound)
                                if _should_expand_url(outbound):
                                    outbound = await _expand_url(session, outbound, timeout_s=float(timeout_s), max_redirects=int(max_redirects))
                                target = outbound
                        except Exception:
                            pass

                try:
                    if extract_asin is not None:
                        asin = extract_asin(target)  # type: ignore[misc]
                except Exception:
                    asin = None
                if not asin:
                    asin = _extract_asin_fallback(target)
                if asin:
                    amazon_url = target
                    break

    if not asin:
        return None

    asin = asin.strip().upper()

    try:
        cat_src = int(str(cfg.get("amazon_auto_forward_category_source_channel_id") or "0").strip() or "0")
    except Exception:
        cat_src = 0
    is_category_src = bool(cat_src and (message.channel.id == cat_src))

    dest_id: Optional[int] = None
    dest_reason = ""
    if not is_category_src:
        cm = _normalize_channel_map(cfg.get("channel_map"))
        entry = cm.get(str(message.channel.id))
        try:
            if isinstance(entry, dict) and entry.get("enabled", True):
                dest_id = int(entry.get("destination_channel_id"))
        except Exception:
            dest_id = None
        if not dest_id:
            return None
        dest_reason = "channel_map"

    product, err = await _get_amazon_product_cached(cfg, asin, strict=bool(is_category_src))
    if err:
        # Still return a plan (so we can see failures), but category routing may default.
        dest_reason = (dest_reason + " + enrich_failed").strip(" +")

    category_path = _amazon_category_path(product)
    is_grocery = _amazon_is_grocery(cfg, product) if category_path else False

    if is_category_src:
        try:
            grocery_dest = int(str(cfg.get("amazon_auto_forward_category_grocery_dest_channel_id") or "0").strip() or "0")
        except Exception:
            grocery_dest = 0
        try:
            default_dest = int(str(cfg.get("amazon_auto_forward_category_default_dest_channel_id") or "0").strip() or "0")
        except Exception:
            default_dest = 0
        if not (grocery_dest and default_dest):
            return None
        dest_id = grocery_dest if is_grocery else default_dest
        dest_reason = "category_split:grocery" if is_grocery else "category_split:default"

    marketplace = _cfg_or_env_str(cfg, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
    base_url = amazon_url or (f"{marketplace}/dp/{asin}" if marketplace else f"https://www.amazon.com/dp/{asin}")
    final_url = _build_amazon_affiliate_url(cfg, base_url) or base_url

    embed, content_link = _build_amazon_embed(
        cfg,
        asin=asin,
        final_url=final_url,
        product=product,
        source_message=message,
        dest_channel_id=int(dest_id) if dest_id else None,
        dest_reason=dest_reason,
    )
    title = (str((product or {}).get("title") or "")).strip()
    has_image = bool((product or {}).get("image_url") or (product or {}).get("image")
                     )
    has_price = bool((product or {}).get("price"))
    return {
        "asin": asin,
        "amazon_url": amazon_url or "",
        "final_url": final_url,
        "product": product or {},
        "enrich_error": err or "",
        "product_keys": sorted(list((product or {}).keys())) if isinstance(product, dict) else [],
        "product_title": title[:120],
        "has_image": bool(has_image),
        "has_price": bool(has_price),
        "category_path": category_path,
        "is_grocery": bool(is_grocery),
        "dest_id": int(dest_id) if dest_id else 0,
        "dest_reason": dest_reason,
        "embed": embed,
        "content_link": content_link,
    }

def _mavely_cookie_source(cfg: dict) -> str:
    """
    Prefer a full cookie header from env (MAVELY_COOKIES) when present, because it can include cf_clearance.
    Otherwise fall back to per-guild stored session token.
    """
    # Option B runtime: if a cookies file is configured, reload it each time so the bot can
    # pick up refreshed cookies without a restart.
    try:
        explicit = (os.getenv("MAVELY_COOKIES_FILE", "") or "").strip()
        path = Path(explicit) if explicit else (_script_dir / "mavely_cookies.txt")
        if path.exists():
            raw = (path.read_text(encoding="utf-8") or "").strip()
            if raw:
                os.environ["MAVELY_COOKIES"] = raw
    except Exception:
        pass

    env_cookie = (os.getenv("MAVELY_COOKIES", "") or "").strip()
    cfg_cookie = (cfg.get("mavely_session_token") or "").strip()
    if env_cookie and (";" in env_cookie) and ("=" in env_cookie):
        return env_cookie
    if cfg_cookie and (";" in cfg_cookie) and ("=" in cfg_cookie):
        return cfg_cookie
    return cfg_cookie or env_cookie

async def _mavely_create_link(cfg: dict, url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (mavely_link, error_message)
    """
    if MavelyClient is None:
        return None, "Mavely module not available in this bot environment."

    session_token = _mavely_cookie_source(cfg)
    auth_token = _cfg_or_env_str(cfg, "mavely_auth_token", "MAVELY_AUTH_TOKEN")
    graphql_endpoint = _cfg_or_env_str(cfg, "mavely_graphql_endpoint", "MAVELY_GRAPHQL_ENDPOINT")
    if not session_token and not auth_token:
        return None, "Missing MAVELY cookie/session token (or MAVELY_AUTH_TOKEN)."

    timeout_s = cfg.get("mavely_request_timeout")
    if timeout_s in (None, ""):
        timeout_s = _cfg_or_env_int(cfg, "mavely_request_timeout", "REQUEST_TIMEOUT") or 20
    try:
        timeout_s = int(timeout_s)
    except (TypeError, ValueError):
        timeout_s = 20

    max_retries = cfg.get("mavely_max_retries")
    if max_retries in (None, ""):
        max_retries = _cfg_or_env_int(cfg, "mavely_max_retries", "MAX_RETRIES") or 3
    try:
        max_retries = int(max_retries)
    except (TypeError, ValueError):
        max_retries = 3

    min_seconds = cfg.get("mavely_min_seconds_between_requests")
    if min_seconds in (None, ""):
        min_seconds = _env_float("MIN_SECONDS_BETWEEN_REQUESTS") or 2.0
    try:
        min_seconds = float(min_seconds)
    except (TypeError, ValueError):
        min_seconds = 2.0

    def _do() -> Tuple[Optional[str], Optional[str]]:
        client = MavelyClient(
            session_token=session_token,
            auth_token=auth_token or None,
            graphql_endpoint=graphql_endpoint or None,
            timeout_s=timeout_s,
            max_retries=max_retries,
            min_seconds_between_requests=min_seconds,
        )
        res = client.create_link((url or "").strip())
        return (
            res.mavely_link if res.ok else None,
            None if (res.ok and res.mavely_link) else (res.error or "Failed to generate Mavely link."),
            int(getattr(res, "status_code", 0) or 0),
            (getattr(res, "raw_snippet", None) or None),
        )

    mavely_link, err, status_code, raw_snippet = await asyncio.to_thread(_do)
    if mavely_link:
        return mavely_link, None

    # If auth is expired/not-logged-in, optionally auto-refresh cookies (Option B) and retry once.
    err_l = (err or "").lower()
    authish = ("token expired" in err_l) or ("not logged in" in err_l) or ("unauthorized" in err_l)
    if authish and await _maybe_refresh_mavely_cookies(reason=(err or "auth")):
        # Force reload and retry once
        _reload_mavely_cookies_from_file(force=True)
        mavely_link2, err2, status_code2, _raw2 = await asyncio.to_thread(_do)
        if mavely_link2:
            return mavely_link2, None
        err = err2 or err
        status_code = status_code2 or status_code

    # Detailed terminal/file logs (no tokens printed)
    warn_key = f"mavely_fail:{status_code}:{(err or '')[:80]}"
    if _log_once(warn_key, seconds=90):
        log.warning(
            "Mavely link generation failed: status=%s err=%s",
            status_code,
            err,
        )
    # The raw response can be extremely noisy (often includes the full GraphQL query).
    # Only include it when verbose lib logs are enabled.
    raw_verbose = (os.getenv("INSTORE_VERBOSE_LIB_LOGS", "") or "").strip().lower()
    if raw_snippet and (raw_verbose in {"1", "true", "yes", "y", "on"}):
        log.debug("Mavely raw response snippet: %r", raw_snippet[:300])

    return None, f"{err or 'Failed to generate Mavely link.'} (status={status_code}). Check terminal/log file for details."

# -----------------------
# Parsing (minimal)
# -----------------------
@dataclass
class ParsedInstore:
    title: str
    where: str
    retail: str
    resell: str
    store_link: str
    market_link: str
    raw_note: str

def _extract_field(patterns: List[str], text: str) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.MULTILINE)
        if m:
            val = m.group(1).strip()
            val = re.sub(r"^\*\*|\*\*$", "", val).strip()
            return val
    return None

def parse_instore_message(content: str) -> Optional[ParsedInstore]:
    if not content or len(content.strip()) < 3:
        return None

    if "```pdsql" in content.lower():
        # already formatted; upstream may decide to ignore
        pass

    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if not lines:
        return None

    # Title
    title = None
    m = re.search(r"^#{1,6}\s*\*{2}(.+?)\*{2}\s*$", lines[0])
    if m:
        title = m.group(1).strip()
    else:
        title = lines[0].strip("*# ").strip()

    where = _extract_field([
        r"Where\s*:\s*\*{0,2}(.+?)\*{0,2}$",
        r"`Where:`\s*\*{0,2}(.+?)\*{0,2}",
        r">\s*`Where:`\s*\*{0,2}(.+?)\*{0,2}",
    ], content)

    retail = _extract_field([
        r"Retail\s*:\s*\*{0,2}(\$?[0-9][0-9,]*(?:\.[0-9]{1,2})?)\*{0,2}",
        r"`Retail:`\s*\*{0,2}(\$?[0-9][0-9,]*(?:\.[0-9]{1,2})?)\*{0,2}",
        r">\s*`Retail:`\s*\*{0,2}(\$?[0-9][0-9,]*(?:\.[0-9]{1,2})?)\*{0,2}",
    ], content)

    resell = _extract_field([
        r"Resell\s*:\s*\*{0,2}(.+?)\*{0,2}$",
        r"`Resell:`\s*\*{0,2}(.+?)\*{0,2}",
        r">\s*`Resell:`\s*\*{0,2}(.+?)\*{0,2}",
    ], content)

    urls = URL_RE.findall(content)
    store_link, market_link = pick_links(urls)

    # Note: everything except fields/urls
    raw_note = ""
    nm = re.search(r"`Note:`.*?\n(.+)$", content, re.IGNORECASE | re.DOTALL)
    if nm:
        raw_note = nm.group(1).strip()
        raw_note = re.sub(r"```[a-zA-Z0-9_]*\n?", "", raw_note)
        raw_note = raw_note.replace("```", "").strip()
    else:
        filtered = []
        for ln in lines[1:]:
            if re.search(r"\b(where|retail|resell|current market|store link|check stock)\b", ln, re.IGNORECASE):
                continue
            if URL_RE.search(ln):
                continue
            if "```pdsql" in ln.lower():
                continue
            if ln.startswith("-#"):
                continue
            filtered.append(ln)
        raw_note = " ".join(filtered).strip()

    if not title or not where or (not retail and not resell):
        return None

    if retail and not retail.startswith("$"):
        retail = f"${retail}"

    return ParsedInstore(
        title=sanitize_spaces(title),
        where=sanitize_spaces(where),
        retail=sanitize_spaces(retail or "TBA"),
        resell=sanitize_spaces(resell or "See comps"),
        store_link=(store_link or "").strip(),
        market_link=(market_link or "").strip(),
        raw_note=raw_note or "",
    )

# -----------------------
# Locked formatter (pdsql)
# -----------------------
def build_rs_pdsql_post(cfg: dict, parsed: ParsedInstore, note_final: str, *, wrap_in_codeblock: bool = True) -> str:
    """
    Locked format:
    - pdsql code block
    - title line + role mention
    - Where/Retail/Resell block quotes
    - Current Market section AFTER Resell
    - Market links wrapped in < >
    - Short flip-focused note 1–2 sentences, no price repetition
    - Footer minimal, no questions
    """
    r_mention = role_mention(cfg.get("role_id"))
    profit_emoji = (cfg.get("profit_emoji") or "").strip()
    footer_template = (cfg.get("footer_text") or "").strip()
    footer = footer_template.replace("{success_channel_id}", str(cfg.get("success_channel_id") or ""))

    store_url = (parsed.store_link or "").strip()
    market_url = (parsed.market_link or "").strip()
    market_name = detect_market_name_from_link(market_url) or "Market"

    # Wrap links always to avoid embeds (default True if not set)
    if _bool_or_default(cfg.get("wrap_links"), True):
        if store_url:
            store_url = wrap_no_embed(store_url)
        if market_url:
            market_url = wrap_no_embed(market_url)

    lines: List[str] = []
    if wrap_in_codeblock:
        lines.append("```pdsql")
    lines.append(f"### **{parsed.title}**")
    if r_mention:
        lines.append(r_mention)

    # Where: bold store name; include store link if present
    if store_url:
        lines.append(f"> `Where:` **{parsed.where}** ({store_url})")
    else:
        lines.append(f"> `Where:` **{parsed.where}**")

    lines.append(f"> `Retail:` **{parsed.retail}**")
    lines.append(f"> `Resell:` **{parsed.resell}**")
    lines.append("")

    # Current Market only if we have a market link
    if market_url:
        lines.append("**Current Market:**")
        lines.append(f"> -# {market_name}: {market_url}")
        lines.append("")

    # Note
    note_final = (note_final or "").strip()
    if profit_emoji:
        lines.append(f"`Note:` {profit_emoji}".rstrip())
    else:
        lines.append("`Note:`")
    if note_final:
        lines.append(note_final)
    lines.append("")
    if footer:
        lines.append(footer)
    if wrap_in_codeblock:
        lines.append("```")
    return "\n".join(lines).strip()

# -----------------------
# OpenAI: rewrite note ONLY (called once per lead)
# -----------------------
async def rewrite_note(cfg: dict, parsed: ParsedInstore) -> Tuple[str, dict]:
    raw_note = (parsed.raw_note or "").strip()
    if not raw_note:
        return ("", {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})

    model = _get_openai_model(cfg)
    temperature = _get_openai_temperature(cfg)
    if not model or not openai_client:
        text = strip_prices(raw_note)
        text = limit_to_two_sentences(text)
        text = text.replace("—", "-")
        text = re.sub(r"\?\s*$", ".", text).strip()
        return text, {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    if temperature is None:
        temperature = 0.4

    prompt = f"""
Rewrite a NOTE for a Discord in-store reseller success post.

Hard rules:
- Output MUST be 1 paragraph, 1–2 sentences total.
- NO prices, NO price ranges, NO dollar signs, NO numbers that look like prices.
- NO questions.
- Do NOT use the em dash character (—).
- Flip vibe only: demand, fast move, clean pairs, common sizes, list fast.
- Do NOT mention "points".
- Do NOT mention "comps" or any platform names (StockX/eBay/GOAT/etc).

Context:
Product: {parsed.title}
Store: {parsed.where}

Raw note:
{raw_note}

Return ONLY the rewritten note.
""".strip()

    resp = await openai_client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
    )

    text = (resp.choices[0].message.content or "").strip()
    text = strip_prices(text)
    text = limit_to_two_sentences(text)
    text = text.replace("—", "-")
    text = re.sub(r"\?\s*$", ".", text).strip()

    usage = getattr(resp, "usage", None)
    usage_dict = {
        "calls": 1,
        "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
        "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
        "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
    }
    return text, usage_dict

# -----------------------
# DB ops
# -----------------------
# SQLite is no longer used at runtime. A legacy `.sqlite3` file may still exist from older versions;
# we migrate it into JSON once at startup and then stop touching it.

def _merge_guild_config(cfg: Optional[dict], *, defaults: Optional[dict] = None) -> dict:
    """
    Merge config in canonical priority order:
      DEFAULT_CONFIG < defaults (from config file) < per-guild overrides
    """
    base = DEFAULT_CONFIG.copy()

    if isinstance(defaults, dict):
        for k in base.keys():
            if k in defaults:
                base[k] = defaults[k]

    if isinstance(cfg, dict):
        for k in base.keys():
            if k in cfg:
                base[k] = cfg[k]
        # Back-compat: older configs used a separate amazon_auto_forward_channel_map.
        # Merge it into canonical `channel_map` and discard the legacy key.
        legacy_amz = cfg.get("amazon_auto_forward_channel_map")
        if isinstance(legacy_amz, dict):
            cur = _normalize_channel_map(base.get("channel_map"))
            legacy_norm = _normalize_channel_map(legacy_amz)
            for src, entry in legacy_norm.items():
                if src not in cur:
                    cur[src] = entry
            base["channel_map"] = cur
    return base

def _minimize_guild_overrides(*, base: dict, merged: dict) -> dict:
    """
    Store only keys that differ from base (keeps guild_config.json clean).
    """
    out: dict = {}
    for k in DEFAULT_CONFIG.keys():
        try:
            if merged.get(k) != base.get(k):
                out[k] = merged.get(k)
        except Exception:
            out[k] = merged.get(k)
    return out

def _normalize_guild_config_file(data: object) -> Tuple[dict, Dict[str, dict]]:
    """
    Returns (defaults, guilds) where:
      - defaults is a dict of shared values
      - guilds is a { "<guild_id>": { ...overrides... } } map
    Canonical on-disk format is: { "defaults": {...}, "guilds": { "<gid>": { ... } } }
    """
    if not isinstance(data, dict):
        return {}, {}

    defaults = data.get("defaults") if isinstance(data.get("defaults"), dict) else {}

    if isinstance(data.get("guilds"), dict):
        out: Dict[str, dict] = {}
        for k, v in (data.get("guilds") or {}).items():
            if not isinstance(v, dict):
                continue
            kid = str(k).strip()
            if kid.isdigit():
                out[kid] = v
        return defaults, out
    # Backward-compat for early drafts: treat top-level dict as guild map
    out2: Dict[str, dict] = {}
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        kid = str(k).strip()
        if kid.isdigit():
            out2[kid] = v
    return {}, out2

async def _read_all_guild_configs() -> Tuple[dict, Dict[str, dict]]:
    path = CONFIG_FILE
    if not path.exists():
        return {}, {}

    def _read() -> Tuple[dict, Dict[str, dict]]:
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw else {}
            return _normalize_guild_config_file(data)
        except Exception:
            return {}, {}

    return await asyncio.to_thread(_read)

async def _write_all_guild_configs(*, defaults: dict, guilds: Dict[str, dict]) -> None:
    path = CONFIG_FILE
    payload = {"defaults": defaults or {}, "guilds": guilds or {}}

    def _write() -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(str(tmp), str(path))

    await asyncio.to_thread(_write)

async def _migrate_per_guild_json_files_to_single_json() -> None:
    """
    Legacy migration: `guild_<gid>.json` files in CONFIG_DIR -> single JSON file.
    After migration, the legacy files are deleted.
    """
    cfg_dir = CONFIG_DIR
    try:
        paths = list(cfg_dir.glob("guild_*.json"))
    except Exception:
        paths = []
    if not paths:
        return

    defaults, guilds = await _read_all_guild_configs()
    wrote = 0
    to_delete: List[Path] = []
    for p in paths:
        name = p.stem or ""
        m = re.search(r"guild_(\d+)$", name)
        if not m:
            continue
        gid_s = m.group(1)
        if gid_s in guilds:
            to_delete.append(p)
            continue
        try:
            raw = p.read_text(encoding="utf-8")
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        merged = _merge_guild_config(data, defaults=defaults)
        base = _merge_guild_config(None, defaults=defaults)
        overrides = _minimize_guild_overrides(base=base, merged=merged)
        if overrides:
            guilds[gid_s] = overrides
        wrote += 1
        to_delete.append(p)

    if wrote:
        await _write_all_guild_configs(defaults=defaults, guilds=guilds)
        log.info("Config: migrated %d legacy guild_*.json file(s) into %s", wrote, str(CONFIG_FILE))

    def _delete_legacy() -> None:
        for p in to_delete:
            try:
                p.unlink(missing_ok=True)  # type: ignore[call-arg]
            except TypeError:
                try:
                    if p.exists():
                        p.unlink()
                except Exception:
                    pass
            except Exception:
                pass

    await asyncio.to_thread(_delete_legacy)

_control_guild_id: int = 0
_control_guild_lock = asyncio.Lock()

def _parse_int_id(value: object) -> int:
    try:
        s = str(value).strip()
        if not s:
            return 0
        return int(s)
    except Exception:
        return 0

async def _get_control_guild_id() -> int:
    """
    Control guild = the ONLY guild we ever post into.
    All other guilds are treated as read-only sources.

    Priority:
      1) env INSTORE_CONTROL_GUILD_ID
      2) config defaults.control_guild_id
      3) if exactly one guild exists in config file, use that id
    """
    global _control_guild_id
    if _control_guild_id:
        return _control_guild_id
    async with _control_guild_lock:
        if _control_guild_id:
            return _control_guild_id

        env_gid = _parse_int_id(os.getenv("INSTORE_CONTROL_GUILD_ID", ""))
        if env_gid > 0:
            _control_guild_id = env_gid
            return _control_guild_id

        defaults, guilds = await _read_all_guild_configs()
        cfg_gid = _parse_int_id((defaults or {}).get("control_guild_id"))
        if cfg_gid > 0:
            _control_guild_id = cfg_gid
            return _control_guild_id

        if isinstance(guilds, dict) and len(guilds) == 1:
            only = next(iter(guilds.keys()), "")
            only_id = _parse_int_id(only)
            if only_id > 0:
                _control_guild_id = only_id
                return _control_guild_id

        return 0

async def _effective_config_guild_id(fallback_guild_id: int) -> int:
    ctrl = await _get_control_guild_id()
    return int(ctrl) if ctrl else int(fallback_guild_id)

async def get_config(guild_id: int) -> dict:
    gid_s = str(int(guild_id))
    defaults, guilds = await _read_all_guild_configs()
    stored = guilds.get(gid_s) if isinstance(guilds, dict) else None
    return _merge_guild_config(stored, defaults=defaults)

async def set_config(guild_id: int, cfg: dict):
    gid_s = str(int(guild_id))
    defaults, guilds = await _read_all_guild_configs()
    merged = _merge_guild_config(cfg, defaults=defaults)
    base = _merge_guild_config(None, defaults=defaults)
    overrides = _minimize_guild_overrides(base=base, merged=merged)
    if overrides:
        guilds[gid_s] = overrides
    else:
        guilds.pop(gid_s, None)
    await _write_all_guild_configs(defaults=defaults, guilds=guilds)

async def set_channel_map(source_guild_id: int, source_channel_id: int, dest_channel_id: int, enabled: bool = True):
    cfg_gid = await _effective_config_guild_id(int(source_guild_id))
    cfg = await get_config(int(cfg_gid))
    cm = _normalize_channel_map(cfg.get("channel_map"))
    cm[str(int(source_channel_id))] = {
        "destination_channel_id": int(dest_channel_id),
        "enabled": bool(enabled),
    }
    cfg["channel_map"] = cm
    await set_config(int(cfg_gid), cfg)

async def remove_channel_map(source_guild_id: int, source_channel_id: int):
    cfg_gid = await _effective_config_guild_id(int(source_guild_id))
    cfg = await get_config(int(cfg_gid))
    cm = _normalize_channel_map(cfg.get("channel_map"))
    cm.pop(str(int(source_channel_id)), None)
    cfg["channel_map"] = cm
    await set_config(int(cfg_gid), cfg)

async def list_channel_maps(source_guild_id: int) -> List[Tuple[int, int, int]]:
    cfg_gid = await _effective_config_guild_id(int(source_guild_id))
    cfg = await get_config(int(cfg_gid))
    cm = _normalize_channel_map(cfg.get("channel_map"))
    out: List[Tuple[int, int, int]] = []
    for src_s, entry in cm.items():
        try:
            src = int(src_s)
            dst = int(entry.get("destination_channel_id"))
            en = 1 if bool(entry.get("enabled", True)) else 0
        except Exception:
            continue
        out.append((src, dst, en))
    out.sort(key=lambda t: t[0])
    return out

async def get_destination_for_source(source_guild_id: int, source_channel_id: int) -> Optional[int]:
    cfg_gid = await _effective_config_guild_id(int(source_guild_id))
    cfg = await get_config(int(cfg_gid))
    cm = _normalize_channel_map(cfg.get("channel_map"))
    entry = cm.get(str(int(source_channel_id)))
    if not isinstance(entry, dict):
        return None
    if not bool(entry.get("enabled", True)):
        return None
    try:
        return int(entry.get("destination_channel_id"))
    except Exception:
        return None

# -----------------------
# Runtime state (JSON file)
# -----------------------
_runtime_lock = asyncio.Lock()
_runtime_state: Optional[dict] = None

def _runtime_default_state() -> dict:
    return {
        "version": 1,
        "previews": {"next_id": 1, "items": {}},
        "forwarded_posts": [],  # list of dicts
        "usage_daily": {},      # { "<guild_id>": { "<day_utc>": {...} } }
        "amazon_product_cache": {},  # { "<marketplace|asin>": {"ts": <float>, "data": {...}} }
    }

def _runtime_normalize_state(data: object) -> dict:
    if not isinstance(data, dict):
        return _runtime_default_state()
    out = _runtime_default_state()
    try:
        out["version"] = int(data.get("version", 1) or 1)
    except Exception:
        out["version"] = 1
    if isinstance(data.get("previews"), dict):
        p = data.get("previews") or {}
        items = p.get("items") if isinstance(p.get("items"), dict) else {}
        try:
            next_id = int(p.get("next_id", 1) or 1)
        except Exception:
            next_id = 1
        out["previews"] = {"next_id": max(1, next_id), "items": items}
    if isinstance(data.get("forwarded_posts"), list):
        out["forwarded_posts"] = data.get("forwarded_posts") or []
    if isinstance(data.get("usage_daily"), dict):
        out["usage_daily"] = data.get("usage_daily") or {}
    if isinstance(data.get("amazon_product_cache"), dict):
        out["amazon_product_cache"] = data.get("amazon_product_cache") or {}
    return out

async def _runtime_load() -> dict:
    global _runtime_state
    if _runtime_state is not None:
        return _runtime_state
    path = RUNTIME_FILE
    def _read() -> dict:
        if not path.exists():
            return _runtime_default_state()
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw else {}
            return _runtime_normalize_state(data)
        except Exception:
            return _runtime_default_state()
    _runtime_state = await asyncio.to_thread(_read)
    return _runtime_state

async def _runtime_save(state: dict) -> None:
    path = RUNTIME_FILE
    payload = _runtime_normalize_state(state)
    def _write() -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(str(tmp), str(path))
    await asyncio.to_thread(_write)

async def _migrate_legacy_sqlite_runtime_to_json() -> bool:
    """
    One-time migration: export previews/forwarded/usage from the legacy sqlite DB (if present),
    write them into RUNTIME_FILE, then rename the sqlite DB to .migrated.bak.
    Returns True if we migrated anything.
    """
    legacy = Path(str(LEGACY_DB_PATH or "")).expanduser()
    if not legacy.exists():
        return False

    # If we already have runtime data, do not overwrite it.
    if RUNTIME_FILE.exists():
        try:
            raw = RUNTIME_FILE.read_text(encoding="utf-8")
            existing = json.loads(raw) if raw else {}
            norm = _runtime_normalize_state(existing)
            if (norm.get("previews") or {}).get("items"):
                return False
        except Exception:
            pass

    def _extract() -> dict:
        import sqlite3
        state = _runtime_default_state()
        try:
            conn = sqlite3.connect(str(legacy))
        except Exception:
            return state
        try:
            cur = conn.cursor()
            # previews
            try:
                cur.execute(
                    "SELECT preview_id, source_guild_id, source_channel_id, source_message_id, created_ts, title, content_text, content_hash, status "
                    "FROM previews"
                )
                rows = cur.fetchall()
                items: Dict[str, dict] = {}
                max_id = 0
                for r in rows:
                    try:
                        pid = int(r[0])
                        max_id = max(max_id, pid)
                        items[str(pid)] = {
                            "preview_id": pid,
                            "source_guild_id": int(r[1]),
                            "source_channel_id": int(r[2]),
                            "source_message_id": int(r[3]),
                            "created_ts": int(r[4]),
                            "title": str(r[5] or ""),
                            "content_text": str(r[6] or ""),
                            "content_hash": str(r[7] or ""),
                            "status": str(r[8] or "previewed"),
                        }
                    except Exception:
                        continue
                state["previews"] = {"next_id": max_id + 1 if max_id > 0 else 1, "items": items}
            except Exception:
                pass

            # forwarded
            try:
                cur.execute(
                    "SELECT preview_id, destination_guild_id, destination_channel_id, destination_message_id, content_hash, forwarded_ts, via "
                    "FROM forwarded_posts"
                )
                rows = cur.fetchall()
                out = []
                for r in rows:
                    try:
                        out.append({
                            "preview_id": int(r[0]),
                            "destination_guild_id": int(r[1]),
                            "destination_channel_id": int(r[2]),
                            "destination_message_id": int(r[3]) if r[3] is not None else None,
                            "content_hash": str(r[4] or ""),
                            "forwarded_ts": int(r[5]) if r[5] is not None else 0,
                            "via": str(r[6] or ""),
                        })
                    except Exception:
                        continue
                state["forwarded_posts"] = out
            except Exception:
                pass

            # usage
            try:
                cur.execute(
                    "SELECT guild_id, day_utc, calls, prompt_tokens, completion_tokens, total_tokens, last_updated_ts "
                    "FROM usage_daily"
                )
                rows = cur.fetchall()
                ud: Dict[str, dict] = {}
                for r in rows:
                    try:
                        gid = str(int(r[0]))
                        day = str(r[1] or "")
                        rec = {
                            "day_utc": day,
                            "calls": int(r[2] or 0),
                            "prompt_tokens": int(r[3] or 0),
                            "completion_tokens": int(r[4] or 0),
                            "total_tokens": int(r[5] or 0),
                            "last_updated_ts": int(r[6] or 0),
                        }
                        ud.setdefault(gid, {})[day] = rec
                    except Exception:
                        continue
                state["usage_daily"] = ud
            except Exception:
                pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return state

    extracted = await asyncio.to_thread(_extract)
    await _runtime_save(extracted)
    # rename legacy DB so we never dual-write again
    try:
        bak = legacy.with_suffix(legacy.suffix + ".migrated.bak")
        if not bak.exists():
            legacy.rename(bak)
    except Exception:
        pass
    return True

async def init_runtime() -> None:
    # Ensure config is canonical (single JSON file). Safe no-op if already migrated.
    try:
        await _migrate_per_guild_json_files_to_single_json()
    except Exception:
        pass
    # Migrate legacy sqlite runtime (once), then load runtime state into memory.
    try:
        await _migrate_legacy_sqlite_runtime_to_json()
    except Exception:
        pass
    await _runtime_load()

async def insert_preview(source_guild_id: int, source_channel_id: int, source_message_id: int, title: str, content_text: str) -> int:
    h = sha256_text(content_text)
    async with _runtime_lock:
        state = await _runtime_load()
        previews = state.get("previews") if isinstance(state.get("previews"), dict) else {}
        items = previews.get("items") if isinstance(previews.get("items"), dict) else {}
        try:
            next_id = int(previews.get("next_id", 1) or 1)
        except Exception:
            next_id = 1
        pid = max(1, next_id)
        rec = {
            "preview_id": pid,
            "source_guild_id": int(source_guild_id),
            "source_channel_id": int(source_channel_id),
            "source_message_id": int(source_message_id),
            "created_ts": now_ts(),
            "title": str(title or ""),
            "content_text": str(content_text or ""),
            "content_hash": str(h or ""),
            "status": "previewed",
        }
        items[str(pid)] = rec
        state["previews"] = {"next_id": pid + 1, "items": items}
        await _runtime_save(state)
        return pid

async def get_preview(preview_id: int) -> Optional[dict]:
    state = await _runtime_load()
    previews = state.get("previews") if isinstance(state.get("previews"), dict) else {}
    items = previews.get("items") if isinstance(previews.get("items"), dict) else {}
    rec = items.get(str(int(preview_id)))
    return rec if isinstance(rec, dict) else None

async def list_recent_previews(limit: int = 20) -> List[dict]:
    state = await _runtime_load()
    previews = state.get("previews") if isinstance(state.get("previews"), dict) else {}
    items = previews.get("items") if isinstance(previews.get("items"), dict) else {}
    out: List[dict] = []
    for k, v in items.items():
        if not isinstance(v, dict):
            continue
        try:
            out.append({
                "preview_id": int(v.get("preview_id") or int(k)),
                "title": str(v.get("title") or ""),
                "created_ts": int(v.get("created_ts") or 0),
                "status": str(v.get("status") or ""),
            })
        except Exception:
            continue
    out.sort(key=lambda r: int(r.get("preview_id") or 0), reverse=True)
    return out[: max(1, int(limit or 20))]

async def is_already_forwarded(content_hash: str, destination_guild_id: int, destination_channel_id: int) -> bool:
    h = (content_hash or "").strip()
    if not h:
        return False
    state = await _runtime_load()
    rows = state.get("forwarded_posts") if isinstance(state.get("forwarded_posts"), list) else []
    dg = int(destination_guild_id)
    dc = int(destination_channel_id)
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            if (str(r.get("content_hash") or "") == h) and (int(r.get("destination_guild_id") or 0) == dg) and (int(r.get("destination_channel_id") or 0) == dc):
                return True
        except Exception:
            continue
    return False

async def record_forward(preview_id: int, destination_guild_id: int, destination_channel_id: int, destination_message_id: Optional[int], content_hash: str, via: str):
    async with _runtime_lock:
        state = await _runtime_load()
        rows = state.get("forwarded_posts") if isinstance(state.get("forwarded_posts"), list) else []
        rec = {
            "preview_id": int(preview_id),
            "destination_guild_id": int(destination_guild_id),
            "destination_channel_id": int(destination_channel_id),
            "destination_message_id": int(destination_message_id) if destination_message_id is not None else None,
            "content_hash": str(content_hash or ""),
            "forwarded_ts": now_ts(),
            "via": str(via or ""),
        }
        # Preserve legacy uniqueness (preview_id + destination guild + destination channel)
        exists = False
        for r in rows:
            if not isinstance(r, dict):
                continue
            try:
                if int(r.get("preview_id") or 0) == rec["preview_id"] and int(r.get("destination_guild_id") or 0) == rec["destination_guild_id"] and int(r.get("destination_channel_id") or 0) == rec["destination_channel_id"]:
                    exists = True
                    break
            except Exception:
                continue
        if not exists:
            rows.append(rec)
        state["forwarded_posts"] = rows

        previews = state.get("previews") if isinstance(state.get("previews"), dict) else {}
        items = previews.get("items") if isinstance(previews.get("items"), dict) else {}
        p = items.get(str(int(preview_id)))
        if isinstance(p, dict):
            p["status"] = "forwarded"
            items[str(int(preview_id))] = p
        state["previews"] = {"next_id": int(previews.get("next_id", 1) or 1), "items": items}
        await _runtime_save(state)

async def update_usage_daily(guild_id: int, usage: dict) -> dict:
    day = utc_day_str()
    add_calls = int(usage.get("calls", 0) or 0)
    add_pt = int(usage.get("prompt_tokens", 0) or 0)
    add_ct = int(usage.get("completion_tokens", 0) or 0)
    add_tt = int(usage.get("total_tokens", 0) or 0)
    gid_s = str(int(guild_id))
    async with _runtime_lock:
        state = await _runtime_load()
        ud = state.get("usage_daily") if isinstance(state.get("usage_daily"), dict) else {}
        gmap = ud.get(gid_s) if isinstance(ud.get(gid_s), dict) else {}
        cur = gmap.get(day) if isinstance(gmap.get(day), dict) else {}
        calls = int(cur.get("calls", 0) or 0) + add_calls
        pt = int(cur.get("prompt_tokens", 0) or 0) + add_pt
        ct = int(cur.get("completion_tokens", 0) or 0) + add_ct
        tt = int(cur.get("total_tokens", 0) or 0) + add_tt
        out = {
            "day_utc": day,
            "calls": calls,
            "prompt_tokens": pt,
            "completion_tokens": ct,
            "total_tokens": tt,
            "last_updated_ts": now_ts(),
        }
        gmap[day] = out
        ud[gid_s] = gmap
        state["usage_daily"] = ud
        await _runtime_save(state)
        return out

async def get_usage_daily(guild_id: int, day_utc: Optional[str] = None) -> Optional[dict]:
    day = day_utc or utc_day_str()
    state = await _runtime_load()
    ud = state.get("usage_daily") if isinstance(state.get("usage_daily"), dict) else {}
    gmap = ud.get(str(int(guild_id))) if isinstance(ud.get(str(int(guild_id))), dict) else {}
    rec = gmap.get(day)
    return rec if isinstance(rec, dict) else None

async def maybe_report_thresholds(guild: discord.Guild, cfg: dict, totals: dict):
    tok_thr = cfg.get("daily_token_threshold")
    call_thr = cfg.get("daily_call_threshold")
    report_channel_id = cfg.get("report_channel_id")

    print(
        f"[USAGE] guild={guild.id} day={totals['day_utc']} calls={totals['calls']} "
        f"tokens={totals['total_tokens']} (p={totals['prompt_tokens']}, c={totals['completion_tokens']}) "
        f"thresholds: calls={call_thr} tokens={tok_thr}"
    )

    exceeded = []
    if isinstance(tok_thr, int) and tok_thr > 0 and totals["total_tokens"] >= tok_thr:
        exceeded.append(f"tokens {totals['total_tokens']} >= {tok_thr}")
    if isinstance(call_thr, int) and call_thr > 0 and totals["calls"] >= call_thr:
        exceeded.append(f"calls {totals['calls']} >= {call_thr}")

    if not exceeded or not report_channel_id:
        return

    ch = guild.get_channel(int(report_channel_id))
    if not isinstance(ch, discord.TextChannel):
        return

    msg = (
        f"**Instore Mirror Usage Threshold Hit**\n"
        f"- Day (UTC): `{totals['day_utc']}`\n"
        f"- Calls: `{totals['calls']}`\n"
        f"- Tokens: `{totals['total_tokens']}` (prompt `{totals['prompt_tokens']}`, completion `{totals['completion_tokens']}`)\n"
        f"- Exceeded: `{', '.join(exceeded)}`"
    )
    try:
        await ch.send(msg)
    except Exception:
        pass

# -----------------------
# Discord bot
# -----------------------
INTENTS = discord.Intents.default()
INTENTS.message_content = True
bot = commands.Bot(command_prefix="!", intents=INTENTS)

def is_admin(interaction: discord.Interaction) -> bool:
    perms = interaction.user.guild_permissions if interaction.guild else None
    return bool(perms and (perms.manage_guild or perms.administrator))

def _is_unknown_interaction_error(exc: Exception) -> bool:
    # Discord can return "Unknown interaction" (10062) if we respond/defer too late.
    try:
        if isinstance(exc, discord.NotFound):
            return True
    except Exception:
        pass
    msg = str(exc) or ""
    return ("Unknown interaction" in msg) or ("10062" in msg)

async def _safe_defer(interaction: discord.Interaction, *, ephemeral: bool = True) -> bool:
    """
    Defer safely without crashing the command if the interaction expired or was already acknowledged.
    Returns True if it's OK to continue sending followups, False if we should stop.
    """
    try:
        if interaction.response.is_done():
            return True
        await interaction.response.defer(ephemeral=ephemeral, thinking=True)
        return True
    except Exception as e:
        if _is_unknown_interaction_error(e):
            return False
        # If it was already responded to, we can still use followups.
        try:
            already = getattr(discord, "InteractionResponded", None)
            if already and isinstance(e, already):
                return True
        except Exception:
            pass
        return False

# -----------------------
# Commands
# -----------------------
def _parse_bool_text(raw: str) -> Optional[bool]:
    v = (raw or "").strip().lower()
    if not v:
        return None
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return None

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

def _coerce_str_list(value: object) -> List[str]:
    if isinstance(value, list):
        out = []
        for x in value:
            s = (str(x) if x is not None else "").strip()
            if s:
                out.append(s)
        return out
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        # Allow "A > B > C" style category paths
        if ">" in s:
            parts = [p.strip() for p in s.split(">")]
            return [p for p in parts if p]
        return [s]
    return []

def _amazon_category_path(product: dict) -> str:
    if not isinstance(product, dict):
        return ""
    raw = product.get("categories")
    cats = _coerce_str_list(raw)
    if cats:
        return " > ".join(cats)[:900]
    # Accept common variants from custom endpoints
    for k in ("category_path", "categoryPath", "categories_path", "breadcrumb", "browse_node_path", "browseNodePath"):
        v = product.get(k)
        cands = _coerce_str_list(v)
        if cands:
            return " > ".join(cands)[:900]
        if isinstance(v, str) and v.strip():
            return v.strip()[:900]

    c = (str(product.get("category") or "")).strip()
    if c:
        return c[:900]
    pg = (str(product.get("product_group") or product.get("productGroup") or "")).strip()
    return pg[:900]

def _amazon_is_grocery(cfg: dict, product: dict) -> bool:
    text = (_amazon_category_path(product) or "").lower()
    if not text:
        return False
    raw = (cfg.get("amazon_auto_forward_grocery_keywords") or "").strip()
    if raw:
        kws = [k.strip().lower() for k in raw.replace("\n", ",").split(",") if k.strip()]
    else:
        kws = ["grocery", "gourmet", "food", "pantry", "snack", "beverage", "kitchen"]
    return any(k in text for k in kws)

def _amazon_masked_link(final_url: str, *, stable_seed: str = "") -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (content_link, masked_markdown_link).
    - content_link: put in message content so it's always clickable text
    - masked_markdown_link: for embed field usage
    """
    target = (final_url or "").strip()
    if not target:
        return None, None
    raw_mask = _env_first_token("AMAZON_DISCORD_MASK_LINK", "1").lower()
    mask_enabled = raw_mask in {"1", "true", "yes", "y", "on"}
    if not mask_enabled:
        return None, None
    mask_prefix = _env_first_token("AMAZON_DISCORD_MASK_PREFIX", "amzn.to") or "amzn.to"
    try:
        mask_len = int(_env_first_token("AMAZON_DISCORD_MASK_LEN", "7") or "7")
    except ValueError:
        mask_len = 7
    slug = ""
    if stable_seed:
        try:
            slug = hashlib.sha256(stable_seed.encode("utf-8")).hexdigest()
        except Exception:
            slug = ""
    masked = _discord_masked_link(mask_prefix, target, slug_len=mask_len, slug=slug)
    return masked, masked

def _tpl_text(value: object, ctx: dict) -> str:
    """
    Safe template formatter using Python .format(**ctx).
    If formatting fails (missing keys / bad braces), return the raw string unchanged.
    """
    if value is None:
        return ""
    s = str(value)
    if not s:
        return ""
    try:
        return s.format(**ctx)
    except Exception:
        return s

def _parse_embed_color(value: object) -> Optional[int]:
    if value in (None, "", 0):
        return None
    if isinstance(value, int):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.startswith("#"):
            return int(s[1:], 16)
        if s.lower().startswith("0x"):
            return int(s, 16)
        if s.isdigit():
            return int(s)
    except Exception:
        return None
    return None

def _coerce_embed_template(value: object) -> Optional[dict]:
    """
    Accept either a dict (preferred) or a JSON string (EmbedBuilder copy/paste).
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None

def _pick_amazon_embed_template(
    cfg: dict,
    *,
    dest_channel_id: Optional[int],
    dest_reason: str,
    is_grocery: bool,
) -> Tuple[Optional[dict], str]:
    raw = cfg.get("amazon_embed_templates")
    if not isinstance(raw, dict) or not raw:
        return None, ""
    by_id = raw.get("by_channel_id")
    mapping = by_id if isinstance(by_id, dict) else raw
    if not isinstance(mapping, dict) or not mapping:
        return None, ""

    candidates: List[str] = []
    if dest_channel_id:
        candidates.append(str(int(dest_channel_id)))
    if dest_reason:
        candidates.append(str(dest_reason))
        if dest_reason.startswith("category_split:"):
            candidates.append(dest_reason.split(":", 1)[1])
        if "channel_map" in dest_reason:
            candidates.extend(["channel_map", "amazon_deals"])
    candidates.append("grocery" if is_grocery else "personal")
    candidates.append("default")

    seen = set()
    ordered: List[str] = []
    for k in candidates:
        kk = (k or "").strip()
        if not kk or kk in seen:
            continue
        seen.add(kk)
        ordered.append(kk)

    for key in ordered:
        tpl = _coerce_embed_template(mapping.get(key))
        if tpl:
            return tpl, key
    return None, ""

def _embed_from_template(tpl: dict, ctx: dict, *, fallback_color: discord.Color) -> discord.Embed:
    title = _tpl_text(tpl.get("title"), ctx).strip()[:256] or None
    desc = _tpl_text(tpl.get("description"), ctx).strip()[:4096] or None
    url = _tpl_text(tpl.get("url"), ctx).strip() or None

    color_val = _parse_embed_color(tpl.get("color"))
    color = discord.Color(color_val) if isinstance(color_val, int) else fallback_color

    embed = discord.Embed(title=title, description=desc, url=url, color=color)

    author = tpl.get("author") if isinstance(tpl.get("author"), dict) else {}
    author_name = _tpl_text((author or {}).get("name"), ctx).strip()[:256]
    author_url = _tpl_text((author or {}).get("url"), ctx).strip() or None
    author_icon = _tpl_text((author or {}).get("icon_url"), ctx).strip() or None
    if author_name or author_url or author_icon:
        embed.set_author(name=author_name or "\u200b", url=author_url, icon_url=author_icon)

    thumb = tpl.get("thumbnail") if isinstance(tpl.get("thumbnail"), dict) else {}
    thumb_url = _tpl_text((thumb or {}).get("url"), ctx).strip() or ""
    if thumb_url:
        embed.set_thumbnail(url=thumb_url)
    img = tpl.get("image") if isinstance(tpl.get("image"), dict) else {}
    img_url = _tpl_text((img or {}).get("url"), ctx).strip() or ""
    if img_url:
        embed.set_image(url=img_url)

    fields = tpl.get("fields") if isinstance(tpl.get("fields"), list) else []
    for f in fields:
        if not isinstance(f, dict):
            continue
        name = _tpl_text(f.get("name"), ctx).strip()[:256]
        value = _tpl_text(f.get("value"), ctx).strip()[:1024]
        if not value:
            continue
        embed.add_field(name=name or "\u200b", value=value, inline=bool(f.get("inline", False)))

    footer = tpl.get("footer") if isinstance(tpl.get("footer"), dict) else {}
    footer_text = _tpl_text((footer or {}).get("text"), ctx).strip()[:2048]
    footer_icon = _tpl_text((footer or {}).get("icon_url"), ctx).strip() or None
    if footer_text or footer_icon:
        embed.set_footer(text=footer_text or "\u200b", icon_url=footer_icon)

    return embed

def _build_amazon_embed(
    cfg: dict,
    *,
    asin: str,
    final_url: str,
    product: dict,
    source_message: Optional[discord.Message] = None,
    dest_channel_id: Optional[int] = None,
    dest_reason: str = "",
) -> Tuple[discord.Embed, Optional[str]]:
    asin = (asin or "").strip().upper()
    title = (str((product or {}).get("title") or "")).strip() or f"Amazon Deal ({asin})"
    image_url = (str((product or {}).get("image_url") or (product or {}).get("image") or "")).strip()
    price = (str((product or {}).get("price") or "")).strip()
    category = _amazon_category_path(product)
    is_grocery = _amazon_is_grocery(cfg, product) if category else False

    # Prefer a masked display link (clickable) if enabled
    content_link, masked_link = _amazon_masked_link(final_url, stable_seed=asin)
    rendered_link = masked_link or (final_url or "").strip()

    fallback_color = discord.Color.from_rgb(255, 153, 0)

    # Optional: template-based embed rendering (per destination)
    tpl, tpl_key = _pick_amazon_embed_template(
        cfg,
        dest_channel_id=dest_channel_id,
        dest_reason=(dest_reason or "").strip(),
        is_grocery=bool(is_grocery),
    )
    if tpl:
        src_guild = ""
        src_channel = ""
        src_jump = ""
        src_msg_id = ""
        if source_message:
            try:
                src_guild = str(source_message.guild.name or "")
                src_channel = f"#{source_message.channel.name}"
                src_jump = str(source_message.jump_url or "")
                src_msg_id = str(source_message.id)
            except Exception:
                pass

        ctx = {
            "asin": asin,
            "title": title,
            "final_url": (final_url or "").strip(),
            "link": rendered_link,
            "price": price,
            "category": category,
            "image_url": image_url,
            "source_guild": src_guild,
            "source_channel": src_channel,
            "source_line": f"{src_guild} {src_channel}".strip(),
            "source_jump": src_jump,
            "source_message_id": src_msg_id,
            "dest_channel_id": str(int(dest_channel_id)) if dest_channel_id else "",
            "dest_reason": (dest_reason or "").strip(),
            "is_grocery": "true" if is_grocery else "false",
            "template_key": tpl_key,
        }

        embed = _embed_from_template(tpl, ctx, fallback_color=fallback_color)

        # Template can override message content (set to "" to suppress the extra content line)
        if "content" in tpl:
            content_override = _tpl_text(tpl.get("content"), ctx).strip()
            content_link = content_override or None

        # Preserve existing global footer_text unless template explicitly set a footer
        if not (isinstance(tpl.get("footer"), dict) and (tpl.get("footer") or {}).get("text")):
            footer = (cfg.get("footer_text") or "").strip()
            if footer:
                try:
                    embed.set_footer(text=footer[:200])
                except Exception:
                    pass
        return embed, content_link

    embed = discord.Embed(
        title=title[:256],
        url=(final_url or None),
        color=fallback_color,
    )
    desc_lines: List[str] = []
    if price:
        desc_lines.append(f"**Price:** {price[:80]}")
    if category:
        desc_lines.append(f"**Category:** {category[:300]}")
    if source_message:
        try:
            src_name = f"{source_message.guild.name} #{source_message.channel.name}"
            desc_lines.append(f"[Source]({source_message.jump_url}) • `{src_name}`")
        except Exception:
            pass
    if desc_lines:
        embed.description = "\n".join(desc_lines)[:4096]

    if asin:
        embed.add_field(name="ASIN", value=f"`{asin}`", inline=True)
    pg = (str((product or {}).get("product_group") or "")).strip()
    if pg and (not category):
        embed.add_field(name="Product Group", value=pg[:80], inline=True)
    if masked_link:
        embed.add_field(name="Link", value=masked_link, inline=False)
    if image_url:
        embed.set_image(url=image_url)

    footer = (cfg.get("footer_text") or "").strip()
    if footer:
        embed.set_footer(text=footer[:200])
    return embed, content_link

async def _post_amazon_link_to_channel(
    interaction: discord.Interaction,
    cfg: dict,
    *,
    url: str,
    destination_channel: discord.TextChannel
) -> None:
    raw_url = (url or "").strip()
    if not raw_url:
        return await interaction.followup.send("❌ Missing URL.", ephemeral=True)

    # Safety: never post outside the control guild.
    try:
        ctrl = await _get_control_guild_id()
    except Exception:
        ctrl = 0
    if ctrl and int(destination_channel.guild.id) != int(ctrl):
        return await interaction.followup.send("❌ Destination must be inside the control guild.", ephemeral=True)

    # Extract ASIN + canonicalize link (prefer local shared util if present)
    asin = None
    if extract_asin is not None:
        try:
            asin = extract_asin(raw_url)  # type: ignore[misc]
        except Exception:
            asin = None
    if not asin:
        asin = _extract_asin_fallback(raw_url)
    if not asin:
        return await interaction.followup.send("❌ Could not find an ASIN in that link.", ephemeral=True)

    marketplace = _cfg_or_env_str(cfg, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
    if marketplace:
        canon_url = f"{marketplace}/dp/{asin}"
    elif canonicalize_amazon_url is not None:
        try:
            canon_url = canonicalize_amazon_url(raw_url)  # type: ignore[misc]
        except Exception:
            canon_url = raw_url
    else:
        canon_url = raw_url

    product, err = await _get_amazon_product_cached(cfg, asin, strict=True)
    if err:
        return await interaction.followup.send(f"❌ {err}", ephemeral=True)

    final_url = _build_amazon_affiliate_url(cfg, canon_url) or canon_url
    embed, content_link = _build_amazon_embed(
        cfg,
        asin=asin,
        final_url=final_url,
        product=product,
        source_message=None,
        dest_channel_id=int(destination_channel.id) if destination_channel else None,
        dest_reason="manual",
    )

    try:
        await destination_channel.send(content=content_link, embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except Exception as e:
        return await interaction.followup.send(f"❌ Failed to post in {destination_channel.mention}: {e}", ephemeral=True)

    await interaction.followup.send(f"✅ Posted Amazon lead to {destination_channel.mention}", ephemeral=True)


class AmazonQuickSetupModal(discord.ui.Modal):
    def __init__(self, *, guild_id: int, url: str):
        super().__init__(title="Amazon Lead Settings")
        self.guild_id = guild_id
        self.url = url

        self.dest_channel = discord.ui.TextInput(
            label="Destination channel (mention or id)",
            placeholder="#amz-leads or 1234567890",
            required=True,
            max_length=40,
        )
        self.api_enabled = discord.ui.TextInput(
            label="Amazon API enabled? (1/0)",
            placeholder="Leave blank for 0",
            required=False,
            max_length=5,
        )
        self.custom_endpoint = discord.ui.TextInput(
            label="AMAZON_CUSTOM_ENDPOINT (optional)",
            placeholder="https://your-api/amazon?asin={asin}",
            required=False,
            max_length=300,
        )
        self.marketplace = discord.ui.TextInput(
            label="Marketplace base (optional)",
            placeholder="https://www.amazon.com",
            required=False,
            max_length=80,
        )
        self.associate_tag = discord.ui.TextInput(
            label="Associate tag (affiliate) (required)",
            placeholder="mytag-20",
            required=True,
            max_length=60,
        )

        self.add_item(self.dest_channel)
        self.add_item(self.api_enabled)
        self.add_item(self.custom_endpoint)
        self.add_item(self.marketplace)
        self.add_item(self.associate_tag)

    async def on_submit(self, interaction: discord.Interaction):
        if not await _safe_defer(interaction, ephemeral=True):
            return
        cfg = await get_config(self.guild_id)

        dest_id = _parse_channel_id(self.dest_channel.value)
        if not dest_id:
            return await interaction.followup.send("❌ Invalid destination channel. Mention it like `#amz-leads` or paste the channel id.", ephemeral=True)

        enabled = _parse_bool_text(self.api_enabled.value)
        cfg["amazon_api_enabled"] = bool(enabled) if enabled is not None else False
        cfg["amazon_custom_endpoint"] = (self.custom_endpoint.value or "").strip()
        cfg["amazon_api_marketplace"] = (self.marketplace.value or "").strip()
        cfg["amazon_leads_dest_channel_id"] = int(dest_id)
        cfg["amazon_associate_tag"] = (self.associate_tag.value or "").strip()
        await set_config(self.guild_id, cfg)

        ch = bot.get_channel(int(dest_id))
        if not isinstance(ch, discord.TextChannel):
            return await interaction.followup.send("❌ Destination channel id is not a text channel (or the bot can’t see it).", ephemeral=True)

        await _post_amazon_link_to_channel(interaction, cfg, url=self.url, destination_channel=ch)


class MavelyQuickSetupModal(discord.ui.Modal):
    def __init__(self, *, guild_id: int, url: str):
        super().__init__(title="Mavely Settings")
        self.guild_id = guild_id
        self.url = url

        self.session_token = discord.ui.TextInput(
            label="Mavely cookie token (optional)",
            placeholder="__Secure-next-auth.session-token value (or full Cookie header)",
            required=False,
            style=discord.TextStyle.paragraph,
            max_length=2000,
        )
        self.auth_token = discord.ui.TextInput(
            label="Mavely auth token (optional)",
            placeholder="Bearer token (recommended if cookie-only fails)",
            required=False,
            style=discord.TextStyle.paragraph,
            max_length=2000,
        )
        self.min_seconds = discord.ui.TextInput(
            label="Min seconds between requests (optional)",
            placeholder="2.0",
            required=False,
            max_length=20,
        )

        self.add_item(self.session_token)
        self.add_item(self.auth_token)
        self.add_item(self.min_seconds)

    async def on_submit(self, interaction: discord.Interaction):
        if not await _safe_defer(interaction, ephemeral=True):
            return
        cfg = await get_config(self.guild_id)
        st = (self.session_token.value or "").strip()
        at = (self.auth_token.value or "").strip()
        if not st and not at:
            return await interaction.followup.send("❌ Provide either a cookie token or an auth token.", ephemeral=True)

        cfg["mavely_session_token"] = st
        cfg["mavely_auth_token"] = at
        if (self.min_seconds.value or "").strip():
            try:
                cfg["mavely_min_seconds_between_requests"] = float(self.min_seconds.value.strip())
            except ValueError:
                return await interaction.followup.send("❌ Min seconds must be a number (e.g. 2.0).", ephemeral=True)
        await set_config(self.guild_id, cfg)

        link, err = await _mavely_create_link(cfg, self.url)
        if err:
            return await interaction.followup.send(f"❌ {err}", ephemeral=True)
        await interaction.followup.send(f"✅ Mavely link:\n{link}", ephemeral=True)


class InstoreAdminGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="instore", description="Instore mirror admin commands")

    @app_commands.command(name="editor", description="Open the interactive config editor (ephemeral)")
    async def editor(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        # Lazy import to avoid circular dependency
        from config_editor import open_config_editor
        await open_config_editor(interaction, bot)

    @app_commands.command(name="mode", description="Set posting mode for this server: preview, manual, disabled")
    @app_commands.describe(mode="preview = post to mapped destination, manual = DM admin copy-ready, disabled = off")
    async def mode(self, interaction: discord.Interaction, mode: str):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)

        mode = (mode or "").lower().strip()
        if mode not in {"preview", "manual", "disabled"}:
            return await interaction.response.send_message("Mode must be: `preview`, `manual`, or `disabled`.", ephemeral=True)

        cfg = await get_config(interaction.guild.id)
        cfg["post_mode"] = mode
        await set_config(interaction.guild.id, cfg)
        await interaction.response.send_message(f"✅ post_mode set to `{mode}`", ephemeral=True)

    @app_commands.command(name="auto_affiliate", description="Auto-rewrite store/Amazon links in a channel to affiliate links")
    @app_commands.describe(
        channel_id="Channel to watch (mention like #channel or paste id)",
        enabled="1/0 (default 1)",
        delete_original="1/0 (optional; requires Manage Messages)",
        rewrap_mavely="1/0 (optional; if 1, re-create Mavely links under YOUR account)",
        dedupe_seconds="Optional: skip duplicate rewrites for same content (default 45)",
        output_channel_id="Optional: post rewrites to this channel id (can be in another server)"
    )
    async def auto_affiliate(
        self,
        interaction: discord.Interaction,
        channel_id: str,
        enabled: str = "1",
        delete_original: str = "0",
        rewrap_mavely: str = "0",
        dedupe_seconds: str = "45",
        output_channel_id: str = "",
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)

        # Acknowledge immediately so Discord doesn't expire the interaction (10062).
        if not await _safe_defer(interaction, ephemeral=True):
            return

        cid = _parse_channel_id(channel_id)
        if not cid:
            try:
                return await interaction.followup.send("❌ Invalid channel. Mention it like `#channel` or paste the channel id.", ephemeral=True)
            except Exception:
                return

        # Source channels can be in ANY server the bot can see (read-only source guilds supported).
        ch = bot.get_channel(int(cid))
        if not isinstance(ch, discord.TextChannel):
            try:
                fetched = await bot.fetch_channel(int(cid))
                ch = fetched if isinstance(fetched, discord.TextChannel) else None
            except Exception:
                ch = None
        if not isinstance(ch, discord.TextChannel):
            try:
                return await interaction.followup.send("❌ That channel id is not accessible as a text channel.", ephemeral=True)
            except Exception:
                return

        en = _parse_bool_text(enabled)
        if en is None:
            en = True
        do_del = _parse_bool_text(delete_original)
        if do_del is None:
            do_del = False
        rewrap = _parse_bool_text(rewrap_mavely)
        if rewrap is None:
            rewrap = False
        try:
            dd = int((dedupe_seconds or "").strip() or "45")
        except ValueError:
            dd = 45
        dd = max(0, min(dd, 3600))
        out_id_val: Optional[int] = None
        if (output_channel_id or "").strip():
            try:
                out_id_val = int((output_channel_id or "").strip())
            except ValueError:
                out_id_val = None

        cfg = await get_config(interaction.guild.id)
        cfg["auto_affiliate_enabled"] = bool(en)
        cfg["auto_affiliate_channel_ids"] = str(cid)
        cfg["auto_affiliate_delete_original"] = bool(do_del)
        # Most users want this for Collector-style bot/webhook feeds.
        cfg["auto_affiliate_allow_bot_messages"] = True
        cfg["auto_affiliate_rewrap_mavely_links"] = bool(rewrap)
        cfg["auto_affiliate_dedupe_seconds"] = int(dd)
        if out_id_val is not None:
            cfg["auto_affiliate_output_channel_id"] = int(out_id_val)
        await set_config(interaction.guild.id, cfg)

        msg = (
            "✅ Auto affiliate is now **{state}** for {channel}.\n"
            "- **Amazon**: adds your `tag=` and (optionally) masks as `amzn.to/xxxxxxx`\n"
            "- **Other stores**: replies with **Mavely** affiliate links\n"
            "- **Rewrap existing Mavely links**: `{rew}`\n"
            "- **Dedupe window**: `{dd}s`\n"
            "- **Output channel**: `{out}`\n"
            "- **Delete original**: `{delv}`"
        ).format(
            state="ON" if en else "OFF",
            channel=ch.mention,
            delv="ON" if do_del else "OFF",
            rew="ON" if rewrap else "OFF",
            dd=dd,
            out=(f"`{out_id_val}`" if out_id_val else "(reply in-channel)"),
        )
        try:
            await interaction.followup.send(msg, ephemeral=True)
        except Exception:
            return

    @app_commands.command(name="map_add", description="Map a source channel id (any server) to a destination channel (control guild)")
    @app_commands.describe(source_channel_id="Source channel id (can be in another server)", destination_channel="Destination channel (mention it)")
    async def map_add(self, interaction: discord.Interaction, source_channel_id: str, destination_channel: discord.TextChannel):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        src_id = _parse_channel_id(source_channel_id)
        if not src_id:
            return await interaction.response.send_message("❌ Invalid source channel id. Paste the numeric channel id.", ephemeral=True)
        await set_channel_map(interaction.guild.id, int(src_id), destination_channel.id, enabled=True)
        await interaction.response.send_message(
            f"✅ Map added: `{src_id}` → {destination_channel.guild.name} #{destination_channel.name}",
            ephemeral=True
        )

    @app_commands.command(name="map_list", description="List channel mappings for this server")
    async def map_list(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        rows = await list_channel_maps(interaction.guild.id)
        if not rows:
            return await interaction.response.send_message("No mappings for this server yet.", ephemeral=True)
        lines = []
        for src_id, dst_id, enabled in rows:
            src = bot.get_channel(src_id)
            dst = bot.get_channel(dst_id)
            src_name = f"{src.guild.name} #{src.name}" if isinstance(src, discord.TextChannel) else str(src_id)
            dst_name = f"{dst.guild.name} #{dst.name}" if isinstance(dst, discord.TextChannel) else str(dst_id)
            lines.append(f"- `{src_id}` {src_name} → `{dst_id}` {dst_name} ({'on' if enabled else 'off'})")
        await interaction.response.send_message("**Mappings:**\n" + "\n".join(lines), ephemeral=True)

    @app_commands.command(name="previews", description="Show recent reconstructed previews (with posted status)")
    async def previews(self, interaction: discord.Interaction):
        if not is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        items = await list_recent_previews(limit=15)
        if not items:
            return await interaction.response.send_message("No previews stored yet.", ephemeral=True)

        def fmt_ts(ts: int) -> str:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M UTC")

        lines = []
        for it in items:
            status = "✅ posted" if it["status"] == "forwarded" else "❌ not posted"
            lines.append(f"- `#{it['preview_id']}` {it['title']} — {status} — {fmt_ts(it['created_ts'])}")
        await interaction.response.send_message("**Recent previews:**\n" + "\n".join(lines), ephemeral=True)

    @app_commands.command(name="test", description="Build previews from recent messages in a source channel")
    @app_commands.describe(
        source_channel="Channel with raw instore lead messages",
        destination_channel="Optional destination channel (same server)",
        destination_channel_id="Optional destination channel id (cross-server)"
    )
    async def test(
        self,
        interaction: discord.Interaction,
        source_channel: discord.TextChannel,
        destination_channel: Optional[discord.TextChannel] = None,
        destination_channel_id: Optional[str] = None
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)

        if not await _safe_defer(interaction, ephemeral=True):
            return
        cfg = await get_config(interaction.guild.id)

        target_channel: Optional[discord.TextChannel] = destination_channel
        if not target_channel:
            raw_id = _parse_channel_id(destination_channel_id)
            if raw_id:
                resolved = bot.get_channel(raw_id)
                if isinstance(resolved, discord.TextChannel):
                    target_channel = resolved
                else:
                    return await interaction.followup.send(
                        "❌ Destination channel id is invalid or not a text channel.",
                        ephemeral=True
                    )

        if not target_channel:
            mapped_id = await get_destination_for_source(interaction.guild.id, source_channel.id)
            mapped = bot.get_channel(mapped_id) if mapped_id else None
            if isinstance(mapped, discord.TextChannel):
                target_channel = mapped

        if not isinstance(target_channel, discord.TextChannel):
            return await interaction.followup.send(
                "❌ No destination channel. Pass one, a destination id, or set a channel map first.",
                ephemeral=True
            )

        min_needed = max(1, int(TEST_MIN_UNIQUE_MESSAGES))
        history_limit = max(min_needed, int(TEST_HISTORY_LIMIT))
        previews: List[str] = []
        seen_hashes = set()
        ignore_pdsql = _bool_or_default(cfg.get("ignore_messages_with_pdsql"), True)

        async for msg in source_channel.history(limit=history_limit, oldest_first=False):
            if msg.author.bot:
                continue
            content = (msg.content or "").strip()
            if not content:
                continue
            if ignore_pdsql and "```pdsql" in content.lower():
                continue

            parsed = parse_instore_message(content)
            if not parsed:
                continue

            content_hash = sha256_text(content)
            if content_hash in seen_hashes:
                continue
            seen_hashes.add(content_hash)

            note_final, _usage = await rewrite_note(cfg, parsed)
            content_text = build_rs_pdsql_post(cfg, parsed, note_final, wrap_in_codeblock=False)
            previews.append(content_text)
            if len(previews) >= min_needed:
                break

        if len(previews) < min_needed:
            return await interaction.followup.send(
                f"❌ Found {len(previews)} parseable message(s) in #{source_channel.name}. Need at least {min_needed}.",
                ephemeral=True
            )

        posted = 0
        for content_text in previews:
            try:
                await target_channel.send(content_text)
                posted += 1
            except Exception as e:
                return await interaction.followup.send(f"❌ Failed to post preview: {e}", ephemeral=True)

        await interaction.followup.send(
            f"✅ Posted {posted} preview(s) to #{target_channel.name}.",
            ephemeral=True
        )

    @app_commands.command(name="amazon", description="Paste an Amazon link and post an Amazon-leads embed")
    @app_commands.describe(url="Amazon product URL (or anything containing an ASIN)", destination_channel="Optional destination override")
    async def amazon(
        self,
        interaction: discord.Interaction,
        url: str,
        destination_channel: Optional[discord.TextChannel] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)

        cfg = await get_config(interaction.guild.id)

        # Determine destination
        dest = destination_channel
        if not dest:
            dest_id = cfg.get("amazon_leads_dest_channel_id")
            if isinstance(dest_id, int) and dest_id > 0:
                ch = bot.get_channel(int(dest_id))
                if isinstance(ch, discord.TextChannel):
                    dest = ch

        try:
            # If we still don't have a destination, or affiliate tag is missing, prompt for setup.
            endpoint = _cfg_or_env_str(cfg, "amazon_custom_endpoint", "AMAZON_CUSTOM_ENDPOINT")
            associate_tag = _cfg_or_env_str(cfg, "amazon_associate_tag", "AMAZON_ASSOCIATE_TAG")
            api_enabled = _cfg_or_env_bool(cfg, "amazon_api_enabled", "AMAZON_API_ENABLED")
            api_enabled = bool(api_enabled) if api_enabled is not None else False

            # If API is enabled, accept either a custom endpoint OR env-based PA-API keys.
            has_paapi = bool(os.getenv("AMAZON_PAAPI_ACCESS_KEY", "").strip() and os.getenv("AMAZON_PAAPI_SECRET_KEY", "").strip() and os.getenv("AMAZON_PAAPI_PARTNER_TAG", "").strip())
            needs_api_bits = api_enabled and (not endpoint) and (not has_paapi)
            needs_affiliate_tag = not bool(associate_tag)

            if not dest or needs_api_bits or needs_affiliate_tag:
                return await interaction.response.send_modal(AmazonQuickSetupModal(guild_id=interaction.guild.id, url=url))
        except Exception as e:
            return await interaction.response.send_message(f"❌ Amazon setup UI error: {e}", ephemeral=True)

        if not await _safe_defer(interaction, ephemeral=True):
            return
        try:
            await _post_amazon_link_to_channel(interaction, cfg, url=url, destination_channel=dest)
        except Exception as e:
            await interaction.followup.send(f"❌ Amazon command failed: {e}", ephemeral=True)

    @app_commands.command(name="link", description="Paste a store/product URL and get a Mavely affiliate link")
    @app_commands.describe(url="Store/product URL to convert")
    async def link(self, interaction: discord.Interaction, url: str):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)

        cfg = await get_config(interaction.guild.id)
        token = _mavely_cookie_source(cfg)
        if not token:
            return await interaction.response.send_modal(MavelyQuickSetupModal(guild_id=interaction.guild.id, url=url))

        if not await _safe_defer(interaction, ephemeral=True):
            return
        link, err = await _mavely_create_link(cfg, url)
        if err:
            try:
                return await interaction.followup.send(f"❌ {err}", ephemeral=True)
            except Exception:
                return
        try:
            await interaction.followup.send(f"✅ Mavely link:\n{link}", ephemeral=True)
        except Exception:
            return

# Autocomplete for preview_id
async def preview_id_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice]:
    current = (current or "").strip()
    items = await list_recent_previews(limit=25)
    choices = []
    for it in items:
        pid = it["preview_id"]
        label = f"#{pid} • {it['title'][:60]} • {'posted' if it['status']=='forwarded' else 'not posted'}"
        if current and current not in str(pid) and current.lower() not in it["title"].lower():
            continue
        choices.append(app_commands.Choice(name=label[:100], value=str(pid)))
    return choices[:25]

class ForwardConfirmView(discord.ui.View):
    def __init__(self, *, preview: dict, destination_channel: discord.TextChannel, allow_force: bool, via_discum: bool):
        super().__init__(timeout=180)
        self.preview = preview
        self.destination_channel = destination_channel
        self.allow_force = allow_force
        self.via_discum = via_discum
        self.result = None  # "forward" | "force" | "cancel"

        if not allow_force:
            # hide force button if not needed
            self.force_button.disabled = True

        if via_discum and discum_client is None:
            self.forward_button.disabled = True
            self.force_button.disabled = True

    @discord.ui.button(label="Forward", style=discord.ButtonStyle.success)
    async def forward_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "forward"
        self.stop()
        await _safe_defer(interaction, ephemeral=True)

    @discord.ui.button(label="Force Forward", style=discord.ButtonStyle.secondary)
    async def force_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "force"
        self.stop()
        await _safe_defer(interaction, ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.result = "cancel"
        self.stop()
        await _safe_defer(interaction, ephemeral=True)

class InstoreForwardGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="instore_forward", description="Forward a reconstructed preview on-demand")

    @app_commands.command(name="to", description="Forward a stored preview to a destination channel (bot or discum)")
    @app_commands.autocomplete(preview_id=preview_id_autocomplete)
    @app_commands.describe(preview_id="Pick a preview", destination_channel="Where to post", via="bot (default) or discum")
    async def to(self, interaction: discord.Interaction, preview_id: str, destination_channel: discord.TextChannel, via: str = "bot"):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if ctrl and int(destination_channel.guild.id) != int(ctrl):
            return await interaction.response.send_message("Destination must be inside the control guild.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)

        try:
            pid = int(preview_id)
        except ValueError:
            return await interaction.response.send_message("Invalid preview_id.", ephemeral=True)

        preview = await get_preview(pid)
        if not preview:
            return await interaction.response.send_message("Preview not found.", ephemeral=True)

        via = (via or "bot").lower().strip()
        if via not in {"bot", "discum"}:
            return await interaction.response.send_message("via must be `bot` or `discum`.", ephemeral=True)

        already = await is_already_forwarded(preview["content_hash"], destination_channel.guild.id, destination_channel.id)

        # Confirm screen
        embed = discord.Embed(
            title=f"Forward Preview #{pid}",
            description=f"**{preview['title']}**\nDestination: {destination_channel.guild.name} #{destination_channel.name}\nVia: `{via}`",
            color=discord.Color.orange() if already else discord.Color.green()
        )
        if already:
            embed.add_field(name="Already posted?", value="⚠️ Yes — same content hash already posted to this channel.", inline=False)
        embed.add_field(name="Content (copy-ready)", value=(preview["content_text"][:1020] + "…") if len(preview["content_text"]) > 1024 else preview["content_text"], inline=False)

        view = ForwardConfirmView(preview=preview, destination_channel=destination_channel, allow_force=already, via_discum=(via=="discum"))
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        await view.wait()

        if view.result in (None, "cancel"):
            return await interaction.followup.send("Cancelled.", ephemeral=True)

        do_force = (view.result == "force")

        if already and not do_force:
            return await interaction.followup.send("Not forwarded (already posted). Use **Force Forward** if you really want it.", ephemeral=True)

        # Send
        content = preview["content_text"]
        if via == "discum" and discum_client:
            try:
                discum_client.sendMessage(str(destination_channel.id), content)
                await record_forward(pid, destination_channel.guild.id, destination_channel.id, None, preview["content_hash"], "discum")
                return await interaction.followup.send("✅ Forwarded via discum (user token).", ephemeral=True)
            except Exception as e:
                return await interaction.followup.send(f"❌ Discum forward failed: {e}", ephemeral=True)

        # Bot send
        try:
            sent = await destination_channel.send(content)
            await record_forward(pid, destination_channel.guild.id, destination_channel.id, sent.id, preview["content_hash"], "bot")
            await interaction.followup.send("✅ Forwarded via bot token.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Bot forward failed: {e}", ephemeral=True)

# Usage command
class InstoreUsageGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="instore_usage", description="Usage reporting")

    @app_commands.command(name="today", description="Show today's OpenAI usage for this server (UTC)")
    async def today(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        u = await get_usage_daily(int(ctrl) if ctrl else interaction.guild.id)
        if not u:
            return await interaction.response.send_message("No usage recorded yet today.", ephemeral=True)
        await interaction.response.send_message(
            f"📊 **Usage (UTC {u['day_utc']})**\n"
            f"- Calls: `{u['calls']}`\n"
            f"- Tokens: `{u['total_tokens']}` (prompt `{u['prompt_tokens']}`, completion `{u['completion_tokens']}`)",
            ephemeral=True
        )

    @app_commands.command(name="dm_today", description="DM Neo admin today's usage for this server (UTC)")
    async def dm_today(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Run this in a server.", ephemeral=True)
        ctrl = await _get_control_guild_id()
        if ctrl and int(interaction.guild.id) != int(ctrl):
            return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
        if not is_admin(interaction):
            return await interaction.response.send_message("Admins only.", ephemeral=True)
        if not NEO_ADMIN_USER_ID:
            return await interaction.response.send_message("NEO_ADMIN_USER_ID not set in env.", ephemeral=True)
        u = await get_usage_daily(int(ctrl) if ctrl else interaction.guild.id)
        if not u:
            return await interaction.response.send_message("No usage recorded yet today.", ephemeral=True)
        user = bot.get_user(NEO_ADMIN_USER_ID) or await bot.fetch_user(NEO_ADMIN_USER_ID)
        if not user:
            return await interaction.response.send_message("Could not find Neo admin user.", ephemeral=True)
        await user.send(
            f"📊 Instore Mirror Usage — `{interaction.guild.name}` — UTC `{u['day_utc']}`\n"
            f"- Calls: `{u['calls']}`\n"
            f"- Tokens: `{u['total_tokens']}` (prompt `{u['prompt_tokens']}`, completion `{u['completion_tokens']}`)"
        )
        await interaction.response.send_message("✅ DM sent to Neo admin.", ephemeral=True)


@bot.tree.command(
    name="testallmessage",
    description="Test how outputs are built for all mappings; posts results to test_output_channel_id",
)
async def testallmessage(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Run this in a server.", ephemeral=True)
    ctrl = await _get_control_guild_id()
    if ctrl and int(interaction.guild.id) != int(ctrl):
        return await interaction.response.send_message(f"Run this in the control guild: `{ctrl}`.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Admins only.", ephemeral=True)

    if not await _safe_defer(interaction, ephemeral=True):
        return

    cfg = await get_config(interaction.guild.id)
    out_id = cfg.get("test_output_channel_id") or cfg.get("auto_affiliate_output_channel_id") or 0
    try:
        out_id_int = int(str(out_id).strip()) if out_id not in (None, "", 0) else 0
    except Exception:
        out_id_int = 0
    if not out_id_int:
        return await interaction.followup.send("❌ Missing `test_output_channel_id` in config.", ephemeral=True)

    out_ch = bot.get_channel(out_id_int)
    if not isinstance(out_ch, discord.TextChannel):
        try:
            fetched = await bot.fetch_channel(out_id_int)
            out_ch = fetched if isinstance(fetched, discord.TextChannel) else None
        except Exception:
            out_ch = None
    if not isinstance(out_ch, discord.TextChannel):
        return await interaction.followup.send(f"❌ Test output channel not accessible: `{out_id_int}`", ephemeral=True)

    # Collect all source channels we should test across ALL guild configs.
    src_ids: List[int] = []
    defaults_all, all_cfgs = await _read_all_guild_configs()
    for _gid_s, raw_cfg in (all_cfgs or {}).items():
        merged = _merge_guild_config(raw_cfg if isinstance(raw_cfg, dict) else {}, defaults=defaults_all)
        cm = _normalize_channel_map(merged.get("channel_map"))
        for k in cm.keys():
            try:
                src_ids.append(int(k))
            except Exception:
                continue
        try:
            cat_src = int(str(merged.get("amazon_auto_forward_category_source_channel_id") or "0").strip() or "0")
        except Exception:
            cat_src = 0
        if cat_src:
            src_ids.append(cat_src)
    # de-dupe and keep stable order
    seen_src: set = set()
    ordered: List[int] = []
    for x in src_ids:
        if x and x not in seen_src:
            seen_src.add(x)
            ordered.append(x)
    src_ids = ordered

    header = (
        f"🧪 **/testallmessage**\n"
        f"- Guild: `{interaction.guild.name}` (`{interaction.guild.id}`)\n"
        f"- Build: `{INSTORE_BUILD}`\n"
        f"- Sources: `{len(src_ids)}`\n"
        f"- Time: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC`"
    )
    try:
        await out_ch.send(header, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        return await interaction.followup.send("❌ Failed to post header to test output channel.", ephemeral=True)

    posted = 0
    missing = 0
    no_match = 0

    for src_id in src_ids:
        src_ch = bot.get_channel(int(src_id))
        if not isinstance(src_ch, discord.TextChannel):
            try:
                fetched = await bot.fetch_channel(int(src_id))
                src_ch = fetched if isinstance(fetched, discord.TextChannel) else None
            except Exception:
                src_ch = None
        if not isinstance(src_ch, discord.TextChannel):
            missing += 1
            try:
                await out_ch.send(f"❌ Source channel not accessible: `{src_id}`", allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                pass
            continue

        picked = None
        try:
            async for msg in src_ch.history(limit=1):
                picked = msg
                break
        except Exception:
            picked = None

        if not picked:
            no_match += 1
            try:
                await out_ch.send(f"⚠️ No recent message found for `{src_id}`.", allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                pass
            continue

        # IMPORTANT: plan using the CONTROL guild config (Neo Test Server).
        plan = await _plan_amazon_forward_for_message(picked, cfg)

        if not plan:
            no_match += 1
            raw_text = _message_to_text_for_rewrite(picked)
            urls = _collect_message_urls(picked)
            asin_guess = _extract_asin_fallback(raw_text or "") or ""
            show_urls = ", ".join(urls[:3]) if urls else ""
            summary = (
                f"**Source**: `{picked.guild.id}` / `{src_id}`\n"
                f"- Msg: {picked.jump_url}\n"
                f"- content_len: `{len((picked.content or '').strip())}` embeds: `{len(picked.embeds or [])}` components: `{len(getattr(picked, 'components', None) or [])}`\n"
                f"- ASIN guess: `{asin_guess}`\n"
                f"- URLs: `{(show_urls[:180] + '…') if (show_urls and len(show_urls) > 180) else show_urls}`\n"
                f"- Result: `no_amazon_detected`\n"
            )
            try:
                await out_ch.send(summary, allowed_mentions=discord.AllowedMentions.none())
            except Exception:
                pass
            continue

        try:
            would_dest = int(plan.get("dest_id") or 0)
        except Exception:
            would_dest = 0
        reason = str(plan.get("dest_reason") or "")
        asin = str(plan.get("asin") or "")
        cat = str(plan.get("category_path") or "")
        is_g = bool(plan.get("is_grocery"))
        enrich_err = str(plan.get("enrich_error") or "")
        keys = plan.get("product_keys") or []
        key_str = ", ".join([str(k) for k in keys[:10]]) if isinstance(keys, list) else str(keys)[:120]

        cat_show = (cat[:180] + "…") if (cat and len(cat) > 180) else cat
        summary = (
            f"**Source**: `{picked.guild.id}` / `{src_id}`\n"
            f"- Msg: {picked.jump_url}\n"
            f"- ASIN: `{asin}`\n"
            f"- Category: `{cat_show}`\n"
            f"- Grocery: `{is_g}`\n"
            f"- Would route to: `{would_dest}` ({reason or 'n/a'})\n"
            f"- Enrich error: `{enrich_err}`\n"
            f"- Product keys: `{key_str}`\n"
        )
        try:
            await out_ch.send(summary, allowed_mentions=discord.AllowedMentions.none())
            await out_ch.send(
                content=plan.get("content_link") or None,
                embed=plan.get("embed"),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            posted += 1
        except Exception:
            pass

    await interaction.followup.send(
        f"✅ Posted `{posted}` test output(s) to <#{out_id_int}>. Missing `{missing}`, no-match `{no_match}`.",
        ephemeral=True,
    )

# Register groups
bot.tree.add_command(InstoreAdminGroup())
bot.tree.add_command(InstoreForwardGroup())
bot.tree.add_command(InstoreUsageGroup())

def _friendly_cmd_error(err: Exception) -> str:
    # Unwrap CommandInvokeError to its original exception when possible.
    original = getattr(err, "original", None)
    e = original if isinstance(original, Exception) else err
    if _is_unknown_interaction_error(e):
        return "That command took too long to respond. Please run it again."
    # Keep it short; details still go to file logs.
    msg = (str(e) or e.__class__.__name__).strip()
    return msg[:300]

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    # Keep terminal human-friendly and avoid stack traces.
    try:
        text = _friendly_cmd_error(error)
        log.info("Command error: %s", text)
    except Exception:
        text = "Command failed."

    # If interaction is expired, nothing we can do.
    try:
        if _is_unknown_interaction_error(getattr(error, "original", error)):  # type: ignore[arg-type]
            return
    except Exception:
        pass

    # Try to notify the user, but never crash on failure.
    try:
        if interaction.response.is_done():
            await interaction.followup.send(f"❌ {text}", ephemeral=True)
        else:
            try:
                await interaction.response.send_message(f"❌ {text}", ephemeral=True)
            except Exception as e:
                # Discord sometimes reports "already acknowledged" even when is_done() is unreliable.
                # Fall back to followups in that case.
                already = getattr(discord, "InteractionResponded", None)
                if (already and isinstance(e, already)) or ("40060" in str(e)):
                    try:
                        await interaction.followup.send(f"❌ {text}", ephemeral=True)
                    except Exception:
                        pass
                else:
                    raise
    except Exception:
        return

# -----------------------
# Daily DM scheduler (UTC) - sends usage summary to Neo admin for each guild where usage exists
# -----------------------
@tasks.loop(minutes=10)
async def daily_usage_dm_tick():
    if not NEO_ADMIN_USER_ID:
        return
    now = datetime.now(timezone.utc)
    if not (now.hour == 0 and 0 <= now.minute <= 10):
        return
    prev_day = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        user = bot.get_user(NEO_ADMIN_USER_ID) or await bot.fetch_user(NEO_ADMIN_USER_ID)
        if not user:
            return
        ctrl = await _get_control_guild_id()
        if not ctrl:
            return
        u = await get_usage_daily(int(ctrl), prev_day)
        if not u:
            return
        g = bot.get_guild(int(ctrl))
        gname = (g.name if g else str(ctrl))
        lines = [
            f"📊 Instore Mirror Daily Usage — `{gname}` — UTC `{prev_day}`",
            f"- Calls: `{u['calls']}`",
            f"- Tokens: `{u['total_tokens']}` (prompt `{u['prompt_tokens']}`, completion `{u['completion_tokens']}`)",
        ]
        await user.send("\n".join(lines))
    except Exception as e:
        print(f"[WARN] Daily usage DM failed: {e}")

@daily_usage_dm_tick.before_loop
async def before_daily_usage_dm_tick():
    await bot.wait_until_ready()

# -----------------------
# Core mirroring listener
# -----------------------
@bot.event
async def on_ready():
    await init_runtime()
    # Determine control guild once at startup (used to enforce "destination-only" behavior).
    ctrl_gid = await _get_control_guild_id()
    try:
        await bot.tree.sync()
    except Exception:
        pass
    if not daily_usage_dm_tick.is_running():
        daily_usage_dm_tick.start()
    print(f"[OK] Logged in as {bot.user}")
    if ctrl_gid:
        try:
            g = bot.get_guild(int(ctrl_gid))
            gname = (g.name if g else "").strip()
        except Exception:
            gname = ""
        log.info("Control guild locked: %s%s", ctrl_gid, (f" ({gname})" if gname else ""))
        log.info("Read-only sources: all other guilds (no posting).")

    async def _mavely_startup_preflight():
        if MavelyClient is None:
            return
        try:
            test_url = (os.getenv("MAVELY_STARTUP_TEST_URL", "") or "").strip()
            # Keep startup logs short by default; override with MAVELY_STARTUP_TEST_MAX_ATTEMPTS if needed.
            try:
                max_attempts = int((os.getenv("MAVELY_STARTUP_TEST_MAX_ATTEMPTS", "") or "").strip() or "1")
            except ValueError:
                max_attempts = 1
            max_attempts = max(1, min(max_attempts, 10))
            try:
                delay_s = float((os.getenv("MAVELY_STARTUP_TEST_DELAY_S", "") or "").strip() or "2.0")
            except ValueError:
                delay_s = 2.0
            delay_s = max(0.5, min(delay_s, 30.0))

            guild_ids: List[int] = []
            if ctrl_gid:
                guild_ids = [int(ctrl_gid)]
            else:
                guild_ids = [int(g.id) for g in bot.guilds]

            for gid in guild_ids:
                g = bot.get_guild(int(gid))
                cfg = await get_config(int(gid))
                cookie = _mavely_cookie_source(cfg)
                auth = _cfg_or_env_str(cfg, "mavely_auth_token", "MAVELY_AUTH_TOKEN")
                graphql_endpoint = _cfg_or_env_str(cfg, "mavely_graphql_endpoint", "MAVELY_GRAPHQL_ENDPOINT")
                if not cookie and not auth:
                    continue

                timeout_s = cfg.get("mavely_request_timeout")
                if timeout_s in (None, ""):
                    timeout_s = _cfg_or_env_int(cfg, "mavely_request_timeout", "REQUEST_TIMEOUT") or 20
                try:
                    timeout_s = int(timeout_s)
                except (TypeError, ValueError):
                    timeout_s = 20

                max_retries = cfg.get("mavely_max_retries")
                if max_retries in (None, ""):
                    max_retries = _cfg_or_env_int(cfg, "mavely_max_retries", "MAX_RETRIES") or 3
                try:
                    max_retries = int(max_retries)
                except (TypeError, ValueError):
                    max_retries = 3

                min_seconds = cfg.get("mavely_min_seconds_between_requests")
                if min_seconds in (None, ""):
                    min_seconds = _env_float("MIN_SECONDS_BETWEEN_REQUESTS") or 2.0
                try:
                    min_seconds = float(min_seconds)
                except (TypeError, ValueError):
                    min_seconds = 2.0

                def _do() -> Tuple[bool, int, str]:
                    client = MavelyClient(
                        session_token=cookie,
                        auth_token=auth or None,
                        graphql_endpoint=graphql_endpoint or None,
                        timeout_s=timeout_s,
                        max_retries=max_retries,
                        min_seconds_between_requests=min_seconds,
                    )
                    res = client.preflight()
                    return bool(res.ok), int(res.status_code), str(getattr(res, "error", "") or "")

                ok, status, perr = await asyncio.to_thread(_do)
                if ok:
                    log.debug("Mavely preflight: guild=%s ok=%s status=%s", (g.id if g else gid), ok, status)
                else:
                    log.warning("Mavely preflight: guild=%s FAILED (%s)", (g.id if g else gid), perr or f"status={status}")
                    # If preflight indicates we're no longer logged in, optionally try the cookie refresher once.
                    # This only works if you have logged in at least once via:
                    #   python Instorebotforwarder/mavely_cookie_refresher.py --interactive
                    perr_l = (perr or "").lower()
                    if ("session is empty" in perr_l) or ("not logged in" in perr_l) or ("token expired" in perr_l):
                        refreshed = await _maybe_refresh_mavely_cookies(reason=perr or "startup preflight")
                        if refreshed:
                            ok2, status2, perr2 = await asyncio.to_thread(_do)
                            if ok2:
                                log.info("Mavely preflight: guild=%s RECOVERED -> ok (status=%s)", (g.id if g else gid), status2)
                                ok, status, perr = ok2, status2, perr2
                            else:
                                log.warning("Mavely preflight: guild=%s still failing (%s)", (g.id if g else gid), perr2 or f"status={status2}")
                        else:
                            # Make the next action obvious (without dumping secrets)
                            if _log_once(f"mavely_preflight_help:{(g.id if g else gid)}", seconds=600):
                                log.warning(
                                    "Mavely auth needs refresh. Run: python Instorebotforwarder/mavely_cookie_refresher.py --interactive (once), "
                                    "then restart. (Optional: set MAVELY_AUTO_REFRESH_ON_FAIL=1 for auto refresh.)"
                                )

                # Optional: actually try creating a link on startup (real end-to-end test).
                if test_url:
                    for attempt in range(1, max_attempts + 1):
                        link, err = await _mavely_create_link(cfg, test_url)
                        if link and not err:
                            log.info("Mavely startup test: guild=%s PASS -> %s", (g.id if g else gid), link)
                            break
                        # Keep this short and human-readable in console
                        log.warning("Mavely startup test: guild=%s FAIL: %s", (g.id if g else gid), err or "unknown error")
                        # If cookies are not logged in / token expired, retries won't help.
                        if err and ("not logged in" in err.lower() or "token expired" in err.lower()):
                            # Optionally try refresh once at startup; if it works, retry immediately.
                            if await _maybe_refresh_mavely_cookies(reason=err or "startup test"):
                                link2, err2 = await _mavely_create_link(cfg, test_url)
                                if link2 and not err2:
                                    log.info("Mavely startup test: guild=%s PASS -> %s", (g.id if g else gid), link2)
                                else:
                                    log.warning("Mavely startup test: guild=%s FAIL: %s", (g.id if g else gid), err2 or err or "unknown error")
                            break
                        if attempt < max_attempts:
                            await asyncio.sleep(delay_s)

                # Optional: auto-affiliate bypass / expansion tests (so we can verify redirect extraction).
                raw_list = (os.getenv("AUTO_AFFILIATE_STARTUP_TEST_URLS", "") or "").strip()
                if raw_list:
                    tests = []
                    for part in raw_list.replace("\n", ",").split(","):
                        p = (part or "").strip()
                        if p:
                            tests.append(p)
                    if tests:
                        log.info("Auto-affiliate startup test: guild=%s starting (%s url(s))", (g.id if g else gid), len(tests))
                        for u in tests:
                            try:
                                mapped, resolved, notes = await _compute_affiliate_rewrites(cfg, [u])
                                target = (resolved.get(u) or _normalize_input_url(u) or u).strip()
                                out = (mapped.get(u) or "").strip()
                                note = (notes.get(u) or "").strip()
                                if out and out != u:
                                    log.info("Auto-affiliate: %s -> %s", u, out)
                                elif target and target != u:
                                    log.info("Auto-affiliate: %s -> %s", u, target)
                                else:
                                    # Only log failures or meaningful notes on startup; keep quiet otherwise.
                                    if note and ("fail" in note.lower() or "error" in note.lower() or "expired" in note.lower()):
                                        log.warning("Auto-affiliate issue: %s (%s)", u, note)
                            except Exception as e:
                                log.warning("Auto-affiliate: %s -> FAIL (%s)", u, e)
        except Exception as e:
            log.debug("Mavely preflight failed: %s", e)

    async def _amazon_startup_preflight():
        """
        Optional startup Amazon PA-API / custom endpoint test.
        Set env AMAZON_STARTUP_TEST_ASIN to run a real request.
        """
        try:
            gid = int(ctrl_gid) if ctrl_gid else 0
            if not gid:
                return
            cfg = await get_config(gid)

            api_enabled = _cfg_or_env_bool(cfg, "amazon_api_enabled", "AMAZON_API_ENABLED")
            api_enabled = bool(api_enabled) if api_enabled is not None else False
            if not api_enabled:
                log.info("Amazon preflight: enrichment disabled (amazon_api_enabled=0).")
                return

            endpoint = _cfg_or_env_str(cfg, "amazon_custom_endpoint", "AMAZON_CUSTOM_ENDPOINT")
            access_key = _cfg_or_env_str(cfg, "amazon_paapi_access_key", "AMAZON_PAAPI_ACCESS_KEY")
            secret_key = _cfg_or_env_str(cfg, "amazon_paapi_secret_key", "AMAZON_PAAPI_SECRET_KEY")
            partner_tag = _cfg_or_env_str(cfg, "amazon_paapi_partner_tag", "AMAZON_PAAPI_PARTNER_TAG")
            pa_host = _cfg_or_env_str(cfg, "amazon_paapi_host", "AMAZON_PAAPI_HOST")
            pa_region = _cfg_or_env_str(cfg, "amazon_paapi_region", "AMAZON_PAAPI_REGION")

            test_asin = (os.getenv("AMAZON_STARTUP_TEST_ASIN", "") or "").strip().upper()
            if endpoint:
                log.info("Amazon preflight: using custom endpoint.")
            else:
                missing = []
                if not access_key:
                    missing.append("AMAZON_PAAPI_ACCESS_KEY")
                if not secret_key:
                    missing.append("AMAZON_PAAPI_SECRET_KEY")
                if not partner_tag:
                    missing.append("AMAZON_PAAPI_PARTNER_TAG")
                if not pa_region:
                    missing.append("AMAZON_PAAPI_REGION")
                if missing:
                    log.warning(
                        "Amazon preflight: PA-API enabled but missing %s. Docs: https://webservices.amazon.com/paapi5/documentation/",
                        ", ".join(missing),
                    )
                    return
                log.info("Amazon preflight: PA-API configured (host=%s region=%s).", (pa_host or "(auto)"), pa_region)

            if not test_asin:
                log.info("Amazon preflight: set AMAZON_STARTUP_TEST_ASIN to run a live request.")
                return

            # Run a strict fetch to validate the credentials / endpoint.
            product, err = await _fetch_amazon_product(cfg, test_asin, strict=True)
            if err:
                log.warning(
                    "Amazon preflight: FAIL asin=%s (%s). Docs: https://webservices.amazon.com/paapi5/documentation/",
                    test_asin,
                    err,
                )
                return
            title = (product.get("title") if isinstance(product, dict) else "") or ""
            keys = sorted(list(product.keys())) if isinstance(product, dict) else []
            log.info("Amazon preflight: PASS asin=%s title=%s keys=%s", test_asin, str(title)[:80], ", ".join(keys[:10]))
        except Exception as e:
            log.warning("Amazon preflight: FAILED (%s)", e)

    # Preflight Mavely auth in the background (non-fatal).
    asyncio.create_task(_mavely_startup_preflight())
    asyncio.create_task(_amazon_startup_preflight())

@bot.event
async def on_message(message: discord.Message):
    # Ignore DMs / non-text
    if not message.guild or not isinstance(message.channel, discord.TextChannel):
        return
    # Never respond to our own bot messages (prevents loops)
    try:
        if bot.user and message.author and message.author.id == bot.user.id:
            return
    except Exception:
        pass

    cfg_gid = await _effective_config_guild_id(int(message.guild.id))
    # Destination guild is read-only (prevents loops + accidental in-server posting).
    if cfg_gid and int(message.guild.id) == int(cfg_gid):
        return
    cfg = await get_config(int(cfg_gid if cfg_gid else message.guild.id))

    # If the message is from another bot/webhook feed, only proceed if at least one listener explicitly allows it.
    allow_bots_aff = _bool_or_default(cfg.get("auto_affiliate_allow_bot_messages"), False)
    allow_bots_amz = _bool_or_default(cfg.get("amazon_auto_forward_allow_bot_messages"), True)
    if message.author.bot and (not (allow_bots_aff or allow_bots_amz)):
        return

    handled_amz = False
    try:
        handled_amz = await _maybe_auto_amazon_forward(message, cfg)
    except Exception:
        handled_amz = False

    # Optional: auto affiliate rewrite (independent of mapping / preview mode)
    if not handled_amz:
        try:
            await _maybe_auto_affiliate_rewrite(message, cfg)
        except Exception:
            pass

    # For bot/webhook feeds (Collector), stop here to avoid the rest of the preview pipeline.
    if message.author.bot:
        return

    # Channel must be mapped
    dest_channel_id = await get_destination_for_source(int(cfg_gid if cfg_gid else message.guild.id), message.channel.id)
    if not dest_channel_id:
        return

    mode = (cfg.get("post_mode") or "disabled").lower().strip()
    if mode == "disabled":
        return

    # Optionally skip already formatted source
    ignore_pdsql = _bool_or_default(cfg.get("ignore_messages_with_pdsql"), True)
    if ignore_pdsql and "```pdsql" in (message.content or "").lower():
        return

    parsed = parse_instore_message(message.content or "")
    if not parsed:
        return

    # Rewrite note (OpenAI) once
    note_final, usage = await rewrite_note(cfg, parsed)

    # Build locked post
    content_text = build_rs_pdsql_post(cfg, parsed, note_final)
    preview_id = await insert_preview(message.guild.id, message.channel.id, message.id, parsed.title, content_text)

    # Update usage for the CONTROL guild (destination), not the source guild.
    if usage.get("calls", 0) > 0:
        totals = await update_usage_daily(int(cfg_gid if cfg_gid else message.guild.id), usage)
        try:
            ctrl_guild = bot.get_guild(int(cfg_gid)) if cfg_gid else None
        except Exception:
            ctrl_guild = None
        await maybe_report_thresholds(ctrl_guild or message.guild, cfg, totals)

    # Posting behavior
    if mode == "manual":
        if NEO_ADMIN_USER_ID:
            try:
                user = bot.get_user(NEO_ADMIN_USER_ID) or await bot.fetch_user(NEO_ADMIN_USER_ID)
                if user:
                    await user.send(
                        f"🧾 **Copy-ready Instore Lead (Preview #{preview_id})**\n"
                        f"From: `{message.guild.name}` #{message.channel.name}\n\n"
                        f"{content_text}"
                    )
            except Exception as e:
                print(f"[WARN] Failed to DM admin: {e}")
        return

    # preview mode: post to mapped destination channel with bot token
    dest = bot.get_channel(dest_channel_id)
    if isinstance(dest, discord.TextChannel):
        # Safety: never post outside the control guild.
        try:
            ctrl = await _get_control_guild_id()
        except Exception:
            ctrl = 0
        if ctrl and int(dest.guild.id) != int(ctrl):
            log.warning("Blocked preview post outside control guild: dest_guild=%s dest_channel=%s", dest.guild.id, dest.id)
            return
        try:
            await dest.send(f"🧪 Preview `#{preview_id}`\n{content_text}")
        except Exception as e:
            print(f"[WARN] Failed to post preview: {e}")

# -----------------------
# Entrypoint
# -----------------------
def main():
    bot.run(DISCORD_BOT_TOKEN)

if __name__ == "__main__":
    main()
