#!/usr/bin/env python3
"""
Instorebotforwarder
-------------------
Scans configured source channels for Amazon links (including embed URLs and link buttons),
expands/normalizes them, enriches via message reconstruction + optional page scrape, routes to output buckets,
and renders RS-style embeds using JSON templates (config-driven).

Includes:
- /testallmessage diagnostics (ephemeral)
- /embedbuild template manager (list/edit/preview) via Discord modals
"""

from __future__ import annotations

import asyncio
import html as _html
import hashlib
import hmac
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

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

    # Only show a timestamp if explicitly enabled in the template.
    ts_enabled = tpl.get("timestamp", False)
    timestamp = datetime.now(timezone.utc) if bool(ts_enabled) else None

    # NOTE: discord.py does not expose Embed.Empty in all versions; use None/conditional setters.
    embed = discord.Embed(
        title=(title or None),
        description=(description or None),
        url=(url or None),
        color=color,
        timestamp=timestamp,
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
        if an:
            embed.set_author(name=an, url=(au or None), icon_url=(ai or None))

    footer = tpl.get("footer", None)
    if isinstance(footer, dict):
        ft = _tpl(footer.get("text", ""), ctx).strip()
        fi = _tpl(footer.get("icon_url", ""), ctx).strip()
        if ft:
            embed.set_footer(text=ft, icon_url=(fi or None))

    fields = tpl.get("fields", None)
    if isinstance(fields, list):
        for f in fields[:25]:
            if not isinstance(f, dict):
                continue
            name = _tpl(f.get("name", ""), ctx).strip()
            value = _tpl(f.get("value", ""), ctx).strip()
            # Skip empty fields so templates can be flexible.
            if not value:
                continue
            if not name:
                name = "\u200b"
            inline = bool(f.get("inline", False))
            embed.add_field(name=name[:256], value=value[:1024], inline=inline)

    return embed


@dataclass
class AmazonDetection:
    asin: str
    url_used: str
    final_url: str


@dataclass
class _SimpleForwardBuffer:
    src_channel_id: int
    dest_channel_id: int
    guild_id: int
    author_id: int
    parts: List[str]
    shop_url_original: str
    shop_url_expanded: str
    last_ts: float
    seq: int


class InstorebotForwarder:
    _TEMPLATE_ROUTES = ("personal", "grocery", "deals", "default", "enrich_failed")

    def _single_instance_lock_enabled(self) -> bool:
        v = (self.config or {}).get("single_instance_lock_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"0", "false", "no", "off"}:
            return False
        return True

    def _acquire_single_instance_lock(self, base_dir: Path) -> None:
        """
        Prevent multiple running instances (which causes duplicate forwards).

        Implementation: non-blocking OS file lock held for process lifetime.
        """
        if not self._single_instance_lock_enabled():
            log.info("[LOCK] single_instance_lock_enabled=false (skipping lock)")
            self._instance_lock_fh = None
            self._instance_lock_path = ""
            return

        try:
            lock_path_cfg = str((self.config or {}).get("single_instance_lock_path") or "").strip()
        except Exception:
            lock_path_cfg = ""
        lock_path = Path(lock_path_cfg) if lock_path_cfg else (base_dir / ".instorebotforwarder.lock")
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        fh = open(lock_path, "a+b")
        try:
            try:
                fh.seek(0)
                fh.write(b"1")
                fh.flush()
            except Exception:
                pass

            if os.name == "nt":
                import msvcrt  # type: ignore

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl  # type: ignore

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            try:
                fh.close()
            except Exception:
                pass
            raise RuntimeError("Another Instorebotforwarder instance is already running. Stop the other process and retry.")

        self._instance_lock_fh = fh
        self._instance_lock_path = str(lock_path)
        log.info("[LOCK] acquired %s", str(lock_path))

    def __init__(self) -> None:
        _setup_logging()
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        self.config = cfg
        self.config_path = config_path
        self.secrets_path = secrets_path
        self._cfg_lock = asyncio.Lock()
        self._openai_cache: Dict[str, str] = {}
        self._openai_stats: Dict[str, int] = {}
        self._openai_last_mode: Dict[str, str] = {}

        # ASIN de-dupe (avoid forwarding duplicate leads)
        self._dedupe_inflight: set[str] = set()
        self._dedupe_sent_ts: Dict[str, float] = {}
        self._dedupe_skipped: int = 0

        self._amazon_scrape_cache: Dict[str, Dict[str, str]] = {}
        self._amazon_scrape_cache_ts: Dict[str, float] = {}

        # Simple forward buffers (config-gated; does not affect existing Amazon mappings)
        self._simple_forward_lock = asyncio.Lock()
        self._simple_forward_buffers: Dict[int, _SimpleForwardBuffer] = {}
        self._simple_forward_flush_tasks: Dict[int, asyncio.Task[None]] = {}

        # Guard against multiple processes posting duplicates.
        self._instance_lock_fh = None
        self._instance_lock_path = ""
        self._acquire_single_instance_lock(base)

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

    # -----------------------
    # Config IO (IMPORTANT: write config.json ONLY, never secrets)
    # -----------------------
    def _load_base_config(self) -> Dict[str, Any]:
        raw = self.config_path.read_text(encoding="utf-8", errors="replace")
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}

    def _write_base_config(self, base_cfg: Dict[str, Any]) -> None:
        self.config_path.write_text(json.dumps(base_cfg, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    def _require_admin(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            return False
        user = interaction.user
        if isinstance(user, discord.Member):
            return bool(user.guild_permissions.administrator)
        return False

    def _route_to_dest_id(self, route: str) -> Optional[int]:
        oc = (self.config or {}).get("output_channels") or {}
        if not isinstance(oc, dict):
            return None
        return _safe_int(oc.get(route))

    def _template_get_current(self, route: str) -> Tuple[Optional[Dict[str, Any]], str]:
        templates = (self.config or {}).get("amazon_embed_templates") or {}
        if not isinstance(templates, dict):
            return None, "amazon_embed_templates missing"

        r = (route or "").strip().lower()
        if r == "default":
            tpl = templates.get("default")
            return (tpl if isinstance(tpl, dict) else None), "default"
        if r == "enrich_failed":
            tpl = templates.get("enrich_failed")
            return (tpl if isinstance(tpl, dict) else None), "enrich_failed"

        dest_id = self._route_to_dest_id(r)
        if not dest_id:
            return None, f"output_channels.{r} missing"
        by = templates.get("by_channel_id") or {}
        if not isinstance(by, dict):
            return None, "by_channel_id missing"
        tpl = by.get(str(dest_id))
        return (tpl if isinstance(tpl, dict) else None), f"by_channel_id.{dest_id}"

    async def _template_set(self, route: str, tpl_obj: Dict[str, Any]) -> Tuple[bool, str]:
        r = (route or "").strip().lower()
        if r not in self._TEMPLATE_ROUTES:
            return False, f"Unknown route: {route}"
        if not isinstance(tpl_obj, dict):
            return False, "Template must be a JSON object"

        async with self._cfg_lock:
            base_cfg = self._load_base_config()
            templates = base_cfg.get("amazon_embed_templates")
            if not isinstance(templates, dict):
                templates = {}
                base_cfg["amazon_embed_templates"] = templates

            if r == "default":
                templates["default"] = tpl_obj
            elif r == "enrich_failed":
                templates["enrich_failed"] = tpl_obj
            else:
                dest_id = self._route_to_dest_id(r)
                if not dest_id:
                    return False, f"output_channels.{r} missing"
                by = templates.get("by_channel_id")
                if not isinstance(by, dict):
                    by = {}
                    templates["by_channel_id"] = by
                by[str(dest_id)] = tpl_obj

            self._write_base_config(base_cfg)

            # Refresh in-memory config (merged config) for immediate effect.
            self.config["amazon_embed_templates"] = templates

        return True, "saved"

    # -----------------------
    # Routing/templates
    # -----------------------
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

    def _pick_dest_channel_id(self, *, source_channel_id: Optional[int], category: str, enrich_failed: bool) -> Tuple[Optional[int], str]:
        oc = (self.config or {}).get("output_channels") or {}
        if not isinstance(oc, dict):
            return None, "output_channels missing"

        personal = _safe_int(oc.get("personal"))
        grocery = _safe_int(oc.get("grocery"))
        deals = _safe_int(oc.get("deals"))
        ef = _safe_int(oc.get("enrich_failed"))

        if enrich_failed:
            return (ef or deals), "enrich_failed"

        # Per-source routing override (when you want a specific source channel to always land in a route).
        try:
            scr = (self.config or {}).get("source_channel_routes") or {}
        except Exception:
            scr = {}
        if isinstance(scr, dict) and source_channel_id:
            forced = (scr.get(str(int(source_channel_id))) or scr.get(int(source_channel_id)) or "").strip().lower()
        else:
            forced = ""
        if forced in {"deals", "personal", "grocery", "enrich_failed"}:
            forced_id = self._route_to_dest_id(forced)
            if forced_id:
                return forced_id, f"source_route:{forced}"

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

    # -----------------------
    # URL collection / detection
    # -----------------------
    def _message_shape(self, message: discord.Message) -> Tuple[int, int, int]:
        content_len = len(message.content or "")
        embeds_n = len(message.embeds or [])
        comps = getattr(message, "components", None) or []
        comp_rows = len(comps) if isinstance(comps, list) else 0
        return content_len, embeds_n, comp_rows

    def _neutralize_mentions(self, text: str) -> str:
        """
        Make sure user content doesn't ping @everyone/@here or role/user mentions when rendered in an embed.
        AllowedMentions.none() prevents pings for content; embed text is best-effort sanitized here too.
        """
        s = (text or "").replace("@everyone", "@ everyone").replace("@here", "@ here")
        # Hide raw mention tokens like <@123>, <@&123>, <#123>
        s = re.sub(r"<@!?&?\d+>", "@mention", s)
        s = re.sub(r"<#\d+>", "#channel", s)
        return s

    def _amazon_affiliate_enabled(self) -> bool:
        v = (self.config or {}).get("amazon_affiliate_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"0", "false", "no", "n", "off"}:
            return False
        # default enabled (unless explicitly disabled)
        return True

    def _amazon_key_link_mode(self) -> str:
        """
        Controls what we append as the "final link" line in the card:
        - "raw": show the raw canonical Amazon URL (no markdown, no amzn.to)
        - "short": show stable amzn.to-style masked link (legacy)
        """
        s = str((self.config or {}).get("amazon_key_link_mode") or "").strip().lower()
        return s or "raw"

    def _amazon_rewrite_step_links_enabled(self) -> bool:
        v = (self.config or {}).get("amazon_rewrite_step_links_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        # default: keep off (safer/no surprises) unless explicitly enabled
        return False

    def _normalize_price_str(self, price: str) -> str:
        """
        Normalize various scraped/message price formats to the display format you want.
        Examples:
        - "439.99 USD" -> "$439.99"
        - "$ 439.99" -> "$439.99"
        """
        s = " ".join((price or "").split()).strip()
        if not s:
            return ""
        # Support code-prefixed formats like "USD1,472.34" / "PHP1,472.34"
        m0 = re.search(r"^([A-Z]{3})\s?(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)$", s, re.IGNORECASE)
        if m0:
            code = (m0.group(1) or "").upper().strip()
            num = (m0.group(2) or "").replace(",", "").strip()
            sym0 = {"USD": "$", "CAD": "$", "AUD": "$", "GBP": "£", "EUR": "€"}.get(code)
            return f"{sym0}{num}" if sym0 else f"{code}{num}"
        m = re.search(r"([$£€])\s?(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)", s)
        if m:
            return f"{m.group(1)}{m.group(2)}"
        m2 = re.search(r"(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)\s*(USD|CAD|AUD|GBP|EUR)\b", s, re.IGNORECASE)
        if m2:
            num = (m2.group(1) or "").replace(",", "").strip()
            code = (m2.group(2) or "").upper().strip()
            sym = {"USD": "$", "CAD": "$", "AUD": "$", "GBP": "£", "EUR": "€"}.get(code)
            return f"{sym}{num}" if sym else f"{num} {code}".strip()
        return s

    def _sanitize_amazon_price_for_marketplace(self, price: str, *, url: str) -> str:
        """
        Guardrail: for amazon.com pages, reject non-USD currency strings (e.g. PHP/₱).
        We prefer returning "" (missing) over posting the wrong currency.
        """
        p = " ".join((price or "").split()).strip()
        if not p:
            return ""
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception:
            host = ""

        # Only enforce strict USD for amazon.com.
        if not host.endswith("amazon.com"):
            return p

        # Accept $-prefixed or explicit USD formats.
        if "$" in p:
            return p
        if re.search(r"\bUSD\b", p, re.IGNORECASE):
            return p

        # Reject common foreign currency markers (e.g. PHP1,472.34 or ₱1,472.34).
        if "₱" in p:
            return ""

        # If it looks like a currency code price, reject (better N/A than wrong currency).
        # Handles "PHP1,472.34" (prefix) and "1472.34 PHP" (suffix).
        m_pref = re.match(r"^([A-Z]{3})", p, re.IGNORECASE)
        if m_pref:
            code = (m_pref.group(1) or "").upper().strip()
            if code and code != "USD":
                return ""
        m_suf = re.search(r"\b([A-Z]{3})\b", p, re.IGNORECASE)
        if m_suf:
            code = (m_suf.group(1) or "").upper().strip()
            if code and code != "USD":
                return ""

        return p

    def _price_to_float(self, price: str) -> Tuple[Optional[float], str]:
        """
        Parse a normalized price string like "$19.99" into (19.99, "$").
        Returns (None, "") when not parseable.
        """
        s = (price or "").strip()
        if not s or s.upper() == "N/A":
            return None, ""
        m = re.search(r"([$£€])\s?(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)", s)
        if not m:
            return None, ""
        sym = (m.group(1) or "").strip()
        num_raw = (m.group(2) or "").replace(",", "").strip()
        try:
            val = float(num_raw)
        except Exception:
            return None, ""
        return val, sym

    def _extract_amazon_before_price_from_html(self, html_txt: str, *, current_price: str = "") -> str:
        """
        Best-effort "Before" (list/was/MSRP) price extraction from Amazon HTML.
        This is not guaranteed; Amazon often doesn't expose list price for every product.
        """
        t = html_txt or ""
        if not t:
            return ""

        # Only do the "highest currency" fallback when we *also* know a current price.
        # Otherwise we risk picking per-unit prices like "$0.21/oz" as a fake "Before".
        cur_norm = self._normalize_price_str(current_price)
        cur_val: Optional[float] = None
        if cur_norm:
            mcur = re.search(r"([$£€])(\d+(?:\.\d+)?)", cur_norm)
            if mcur:
                try:
                    cur_val = float(mcur.group(2))
                except Exception:
                    cur_val = None
        if cur_val is None:
            return ""

        # Prefer common strike-through/list-price DOM fragments, but only if they are > current.
        # This avoids false "before" values like AppleCare add-ons that can be lower than the item price.
        # Also focus within the main price area when possible.
        focus = t
        try:
            low_all = t.lower()
            mpos = re.search(r"(corepricedisplay_desktop_feature_div|corepricedisplay_mobile_feature_div|apexpricetopay|pricetopay|buybox|apex_desktop|apex_mobile)", low_all)
            if mpos:
                start = max(0, mpos.start() - 2000)
                end = min(len(t), mpos.start() + 9000)
                focus = t[start:end]
        except Exception:
            focus = t

        for pat in (
            r'\bList Price\b[^$£€]{0,80}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)',
            r'\bWas\b[^$£€]{0,80}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)',
            r'\bMSRP\b[^$£€]{0,80}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)',
            r'\bTypical price\b[^$£€]{0,80}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)',
        ):
            m = re.search(pat, focus, re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            cand = self._normalize_price_str(m.group(1) or "")
            if not cand:
                continue
            try:
                cand_val, _cand_sym = self._price_to_float(cand)
            except Exception:
                cand_val = None
            if cand_val is None:
                continue
            if cand_val > cur_val:
                return cand

        # Reduce false positives by focusing on the main price area when possible.
        vals: List[Tuple[float, str]] = []
        for m in re.finditer(r"(?<!\w)([$£€])\s?(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)(?!\w)", focus):
            sym = (m.group(1) or "").strip()
            num_raw = (m.group(2) or "").replace(",", "").strip()
            try:
                val = float(num_raw)
            except Exception:
                continue
            raw = f"{sym}{val:.2f}".rstrip("0").rstrip(".")
            vals.append((val, raw))
        if not vals:
            return ""
        vals.sort(key=lambda x: x[0], reverse=True)
        for val, raw in vals:
            if val <= cur_val:
                continue
            cand = self._normalize_price_str(raw)
            if cand and cand != cur_norm:
                return cand
        return ""

    def _extract_amazon_current_price_from_html(self, html_txt: str) -> str:
        """
        Best-effort current price extraction from Amazon HTML.

        Notes:
        - Amazon's markup varies heavily (and sometimes defers price rendering to JS).
        - We prefer "core price display" regions to avoid per-unit prices.
        - Returns "" if we can't confidently extract a current price.
        """
        t = html_txt or ""
        if not t:
            return ""

        # 1) Prefer core price/buybox areas that usually contain the "price to pay".
        for pat in (
            r"(?:apexPriceToPay|priceToPay)[^>]*>[\s\S]{0,8000}a-offscreen[\"']>\s*([^<]{1,32})<",
            r"(?:corePriceDisplay_desktop_feature_div|corePriceDisplay_mobile_feature_div)[\s\S]{0,12000}a-offscreen[\"']>\s*([^<]{1,32})<",
            r"(?:corePrice_feature_div|corePrice_desktop_feature_div|corePrice_mobile_feature_div)[\s\S]{0,12000}a-offscreen[\"']>\s*([^<]{1,32})<",
            r"(?:buybox|apex_desktop|apex_mobile)[\s\S]{0,16000}a-offscreen[\"']>\s*([^<]{1,32})<",
            r"(?:priceblock_ourprice|priceblock_dealprice|priceblock_saleprice)[\s\S]{0,1500}a-offscreen[\"']>\s*([^<]{1,32})<",
        ):
            m0 = re.search(pat, t, re.IGNORECASE | re.DOTALL)
            if not m0:
                continue
            cand0 = self._normalize_price_str(m0.group(1) or "")
            if cand0:
                return cand0

        # 2) JSON-ish blobs (often present even when HTML is sparse).
        for pat in (
            r'["\']displayPrice["\']\s*:\s*["\'](\$?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)["\']',
            r'["\']formattedPrice["\']\s*:\s*["\'](\$?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)["\']',
            r'["\']priceToPay["\'][\s\S]{0,1200}["\']displayPrice["\']\s*:\s*["\'](\$?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)["\']',
        ):
            m = re.search(pat, t, re.IGNORECASE | re.DOTALL)
            if not m:
                continue
            cand = self._normalize_price_str(m.group(1) or "")
            if cand:
                return cand

        # 3) Last resort: scan for a-offscreen prices ONLY in the main price area.
        # We avoid scanning the whole document because it frequently contains unrelated prices.
        try:
            low_all = t.lower()
            mpos = re.search(r"(corepricedisplay_desktop_feature_div|corepricedisplay_mobile_feature_div|apexpricetopay|pricetopay|buybox|apex_desktop|apex_mobile)", low_all)
        except Exception:
            mpos = None
        if not mpos:
            return ""

        focus = t[max(0, mpos.start() - 1500) : min(len(t), mpos.start() + 12000)]

        candidates: List[str] = []
        for m in re.finditer(
            r'a-offscreen["\']?\s*>\s*([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)\s*<',
            focus,
            re.IGNORECASE,
        ):
            raw = m.group(1) or ""
            cand = self._normalize_price_str(raw)
            if not cand:
                continue
            ctx = (focus[max(0, m.start() - 140) : min(len(focus), m.end() + 140)] or "").lower()
            # Skip obvious per-unit prices.
            if ("/oz" in ctx) or ("/ounce" in ctx) or ("/count" in ctx) or ("/lb" in ctx) or ("/pound" in ctx) or ("per " in ctx and ("ounce" in ctx or "oz" in ctx or "count" in ctx or "lb" in ctx or "pound" in ctx)):
                continue
            # Skip strike/list labels (these are "before" candidates).
            if ("list price" in ctx) or ("typical price" in ctx) or ("msrp" in ctx) or (" was " in ctx):
                continue
            candidates.append(cand)

        if not candidates:
            return ""

        # Prefer the FIRST candidate in the focused region (usually "priceToPay") over "min price",
        # because variant selectors can contain multiple unrelated prices.
        return candidates[0]

    def _extract_amazon_discount_notes_from_html(self, html_txt: str) -> List[str]:
        """
        Best-effort extraction of "why is it cheaper" signals.
        Examples we try to detect:
        - Coupon (clip coupon / save $ / save %)
        - Subscribe & Save % (sometimes exposed in HTML text)
        - Limited time deal badge

        NOTE: This does not guarantee the *final* effective checkout price, because many discounts
        depend on account eligibility, selected variant, delivery location, and interaction (clip coupon).
        """
        t = html_txt or ""
        if not t:
            return []

        # Reduce false positives by focusing on the main price area when possible.
        low_all = t.lower()
        focus = t
        try:
            mpos = re.search(r"(corepricedisplay_desktop_feature_div|apexpricetopay|pricetopay)", low_all)
            if mpos:
                start = max(0, mpos.start() - 2000)
                end = min(len(t), mpos.start() + 9000)
                focus = t[start:end]
        except Exception:
            focus = t

        low = focus.lower()
        notes: List[str] = []

        # Limited time deal / deal badges
        if "limited time deal" in low:
            notes.append("Limited time deal")
        if "prime exclusive deal" in low:
            notes.append("Prime exclusive deal")

        # Subscribe & Save signals
        if "subscribe & save" in low or "subscribe and save" in low:
            # Try to grab a nearby percent if present.
            mss = re.search(r"(subscribe\s*(?:&|and)\s*save)[^%]{0,40}(\d{1,2})\s*%", focus, re.IGNORECASE)
            if mss:
                notes.append(f"Subscribe & Save {mss.group(2)}%")
            else:
                notes.append("Subscribe & Save")

        # Coupon signals (clip coupon, save $ / save %)
        if ("clip coupon" in low) or ("coupon" in low and "clip" in low):
            # Try to capture "Save $X" or "Save X%" near coupon wording.
            mc1 = re.search(r"(clip\s+coupon)[^$£€%]{0,60}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)", focus, re.IGNORECASE)
            mc2 = re.search(r"(clip\s+coupon)[^%]{0,60}(\d{1,2})\s*%", focus, re.IGNORECASE)
            if mc1:
                notes.append(f"Coupon: save {self._normalize_price_str(mc1.group(2) or '')}")
            elif mc2:
                notes.append(f"Coupon: save {mc2.group(2)}%")
            else:
                notes.append("Coupon available")
        else:
            # Sometimes coupon is present as JSON-ish fields.
            mcj = re.search(r"coupon[^\\n]{0,120}(save|off)[^\\n]{0,60}(\d{1,2})\s*%", focus, re.IGNORECASE)
            if mcj:
                notes.append(f"Coupon: save {mcj.group(2)}%")

        # Deduplicate while preserving order
        out: List[str] = []
        seen = set()
        for n in notes:
            nn = " ".join((n or "").split()).strip()
            if nn and nn.lower() not in seen:
                out.append(nn)
                seen.add(nn.lower())
        return out[:4]

    # -----------------------
    # De-dupe (ASIN)
    # -----------------------
    def _dedupe_asin_ttl_s(self) -> float:
        """
        Skip forwarding the same ASIN within this window.
        Config: dedupe_asin_ttl_s
        """
        try:
            v = float((self.config or {}).get("dedupe_asin_ttl_s") or 0.0)
        except Exception:
            v = 0.0
        # Default 6 hours if not configured.
        if v <= 0:
            v = 6 * 3600.0
        return max(60.0, min(v, 14 * 24 * 3600.0))

    def _dedupe_prune(self) -> None:
        ttl = self._dedupe_asin_ttl_s()
        now = time.time()
        # Prune sent cache
        for a, ts in list(self._dedupe_sent_ts.items()):
            try:
                if (now - float(ts or 0.0)) > ttl:
                    self._dedupe_sent_ts.pop(a, None)
            except Exception:
                self._dedupe_sent_ts.pop(a, None)

    def _dedupe_reserve(self, asin: str) -> bool:
        """
        Reserve an ASIN for processing (prevents duplicates wasting scrape/OpenAI).
        """
        a = (asin or "").strip().upper()
        if not a:
            return True
        self._dedupe_prune()
        if a in self._dedupe_inflight:
            self._dedupe_skipped += 1
            return False
        if a in self._dedupe_sent_ts:
            self._dedupe_skipped += 1
            return False
        self._dedupe_inflight.add(a)
        return True

    def _dedupe_commit(self, asin: str) -> None:
        a = (asin or "").strip().upper()
        if not a:
            return
        self._dedupe_inflight.discard(a)
        self._dedupe_sent_ts[a] = time.time()

    def _dedupe_release(self, asin: str) -> None:
        a = (asin or "").strip().upper()
        if not a:
            return
        self._dedupe_inflight.discard(a)

    def _openai_enabled(self) -> bool:
        v = (self.config or {}).get("openai_rephrase_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        # Default: only enable if a key exists.
        return bool(self._openai_api_key())

    def _openai_api_key(self) -> str:
        # Secrets should live in config.secrets.json (merged into self.config), or env.
        k = str((self.config or {}).get("openai_api_key") or "").strip()
        if k:
            return k
        return (os.getenv("OPENAI_API_KEY", "") or "").strip()

    def _openai_model(self) -> str:
        return str((self.config or {}).get("openai_model") or "gpt-4o-mini").strip() or "gpt-4o-mini"

    def _openai_timeout_s(self) -> float:
        try:
            v = float((self.config or {}).get("openai_timeout_s") or 12.0)
        except Exception:
            v = 12.0
        return max(5.0, min(v, 45.0))

    def _openai_max_chars(self) -> int:
        try:
            v = int((self.config or {}).get("openai_max_chars") or 1800)
        except Exception:
            v = 1800
        return max(200, min(v, 6000))

    async def _openai_rephrase(self, kind: str, text: str) -> str:
        """
        Best-effort OpenAI rewrite. Never required; always safe-fallback to original text.
        """
        raw = (text or "").strip()
        if not raw:
            self._openai_last_mode[kind] = "empty"
            self._openai_stats["skip_empty"] = int(self._openai_stats.get("skip_empty", 0) or 0) + 1
            return ""
        if not self._openai_enabled():
            self._openai_last_mode[kind] = "disabled"
            self._openai_stats["skip_disabled"] = int(self._openai_stats.get("skip_disabled", 0) or 0) + 1
            return raw
        key = self._openai_api_key()
        if not key:
            self._openai_last_mode[kind] = "no_key"
            self._openai_stats["skip_no_key"] = int(self._openai_stats.get("skip_no_key", 0) or 0) + 1
            return raw

        # Cache by kind+hash to avoid repeat costs within a run.
        cache_key = f"{kind}:{hashlib.sha256(raw.encode('utf-8', errors='ignore')).hexdigest()[:16]}"
        if cache_key in self._openai_cache:
            self._openai_last_mode[kind] = "cache"
            self._openai_stats["cache_hit"] = int(self._openai_stats.get("cache_hit", 0) or 0) + 1
            return self._openai_cache[cache_key]

        clipped = raw
        max_chars = self._openai_max_chars()
        if len(clipped) > max_chars:
            clipped = clipped[: max_chars - 3] + "..."

        if kind == "steps":
            sys_prompt = (
                "Rewrite deal steps for clarity and actionability.\n"
                "- Keep all URLs unchanged.\n"
                "- Keep promo codes unchanged.\n"
                "- Output a short numbered list.\n"
                "- Do not invent steps.\n"
            )
        else:
            sys_prompt = (
                "Rewrite deal info for clarity.\n"
                "- Keep all URLs unchanged.\n"
                "- Keep promo codes/numbers unchanged.\n"
                "- Do not add claims.\n"
                "- Keep it brief.\n"
            )

        model = self._openai_model()
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": clipped},
            ],
            "temperature": 0.2,
        }

        try:
            import aiohttp

            self._openai_last_mode[kind] = "api_call"
            self._openai_stats["api_call"] = int(self._openai_stats.get("api_call", 0) or 0) + 1
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            timeout_s = self._openai_timeout_s()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
                async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers) as resp:
                    txt = await resp.text(errors="replace")
                    if int(resp.status) >= 400:
                        self._openai_last_mode[kind] = f"api_http_{int(resp.status)}"
                        self._openai_stats["api_fail"] = int(self._openai_stats.get("api_fail", 0) or 0) + 1
                        return raw
                    data = json.loads(txt) if txt else {}
                    out = ""
                    try:
                        out = (((data or {}).get("choices") or [])[0] or {}).get("message", {}).get("content", "") or ""
                    except Exception:
                        out = ""
                    out = str(out).strip()
                    if not out:
                        self._openai_last_mode[kind] = "api_empty"
                        self._openai_stats["api_fail"] = int(self._openai_stats.get("api_fail", 0) or 0) + 1
                        return raw
                    # Safety: prevent pings
                    out = self._neutralize_mentions(out)
                    self._openai_cache[cache_key] = out
                    self._openai_last_mode[kind] = "api_ok"
                    self._openai_stats["api_ok"] = int(self._openai_stats.get("api_ok", 0) or 0) + 1
                    return out
        except Exception:
            self._openai_last_mode[kind] = "api_exc"
            self._openai_stats["api_fail"] = int(self._openai_stats.get("api_fail", 0) or 0) + 1
            return raw

    def _stable_key_slug(self, seed: str, *, length: int = 7) -> str:
        s = (seed or "").encode("utf-8", errors="ignore")
        h = hashlib.sha256(s).hexdigest()
        alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
        # base16 -> pseudo-base36
        out = []
        for ch in h:
            out.append(alphabet[int(ch, 16) % len(alphabet)])
            if len(out) >= length:
                break
        return "".join(out) if out else "amznkey"

    def _key_link(self, final_url: str, *, message_id: str, asin: str) -> str:
        """
        Render the final link line according to config:
        - raw: https://www.amazon.com/dp/ASIN
        - short: [amzn.to/xxxxxxx](<https://www.amazon.com/dp/ASIN>)
        """
        u = (final_url or "").strip()
        if not u:
            return ""
        if self._amazon_key_link_mode() == "raw":
            return u
        # Stable across reposts: do NOT include message_id in the slug seed.
        seed = (asin or "").strip() or u
        slug = self._stable_key_slug(seed, length=7)
        return f"[amzn.to/{slug}](<{u}>)"

    def _extract_promo_codes(self, text: str) -> List[str]:
        s = text or ""
        if not s:
            return []
        codes: List[str] = []
        patterns = [
            r"(?:use|apply)\s+code[:;\s]*([A-Z0-9]{4,20})",
            r"\bcode[:;\s]*([A-Z0-9]{4,20})\b",
            r"promo\s+code[:;\s]*([A-Z0-9]{4,20})",
        ]
        for pat in patterns:
            for m in re.finditer(pat, s, re.IGNORECASE):
                c = (m.group(1) or "").strip().upper()
                if c and c not in codes:
                    codes.append(c)
        return codes[:5]

    def _extract_step_lines(self, text: str) -> List[str]:
        """
        Extract likely "steps" lines from a messy deal message.
        """
        noise_tokens = {"atc", "keepa", "sas", "ebay"}
        action_tokens = {
            "clip",
            "coupon",
            "select",
            "apply",
            "checkout",
            "subscribe",
            "sub",
            "save",
            "buy",
            "click",
            "use code",
            "apply code",
            "buy via",
            "go here",
        }

        def _has_urlish(s: str) -> bool:
            low = (s or "").lower()
            return ("http://" in low) or ("https://" in low) or ("amzn.to/" in low) or ("amazon." in low) or ("/dp/" in low)

        def _looks_actionable(line: str) -> bool:
            low = (line or "").lower()
            if _has_urlish(low):
                return True
            return any(tok in low for tok in action_tokens)

        def _is_noise_step(line: str) -> bool:
            s = " ".join((line or "").split()).strip()
            if not s:
                return True
            low = s.lower()
            # Strip a leading step number like "1)" or "2 -"
            low2 = re.sub(r"^\s*\d+\s*[-:)]\s*", "", low).strip()
            if not low2:
                return True
            # Normalize markdown links: "[ATC](https://...)" -> "ATC"
            try:
                low2_plain = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", low2)
            except Exception:
                low2_plain = low2
            low2_plain = low2_plain.replace("`", "").strip()
            # Common "tools list" line: "ATC | KEEPA | SAS | EBAY"
            if "|" in low2_plain:
                parts = [p.strip() for p in low2_plain.split("|") if p.strip()]
                if parts and all(p in noise_tokens for p in parts):
                    return True
            # Also drop if the entire line is just these tokens separated by spaces/commas
            tmp = re.sub(r"[|,/]+", " ", low2_plain)
            toks = [t for t in tmp.split() if t]
            if toks and all(t in noise_tokens for t in toks):
                return True
            return False

        lines = [ln.strip() for ln in (text or "").splitlines()]
        out: List[str] = []
        for ln in lines:
            if not ln:
                continue
            low = ln.lower()
            if low.startswith("from:") or low.startswith("by:") or low.startswith("powered by"):
                continue
            if "http://" in low or "https://" in low:
                # Keep explicit steps or action lines with links
                if low.startswith("step") or re.match(r"^\d+\s*[-:)]", low):
                    if not _is_noise_step(ln):
                        out.append(ln)
                    continue
                if any(k in low for k in ("clip", "select", "apply", "checkout", "subscribe", "buy via", "click here", "use code")):
                    if not _is_noise_step(ln):
                        out.append(ln)
                    continue
            else:
                # Non-link steps sometimes: "Step 3: Use code X"
                if low.startswith("step"):
                    if (not _is_noise_step(ln)) and _looks_actionable(ln):
                        out.append(ln)
                elif re.match(r"^\d+\s*[-:)]", low):
                    # Numbered lines are only steps if they look actionable (prevents tool-tag rows).
                    if (not _is_noise_step(ln)) and _looks_actionable(ln):
                        out.append(ln)
                elif any(k in low for k in ("use code", "apply code", "clip coupon", "sub & save", "subscribe & save", "buy via", "click here", "checkout")):
                    if (not _is_noise_step(ln)) and _looks_actionable(ln):
                        out.append(ln)
        # Deduplicate
        seen = set()
        dedup = []
        for ln in out:
            k = " ".join(ln.split()).lower()
            if k in seen:
                continue
            seen.add(k)
            dedup.append(ln)
        return dedup[:8]

    def _extract_source_credit(self, text: str) -> str:
        """
        Pull "From: ... | By: ..." style credit line if present.
        """
        for ln in (text or "").splitlines():
            s = ln.strip()
            if not s:
                continue
            low = s.lower()
            if low.startswith("from:"):
                return self._neutralize_mentions(s)[:200]
        return ""

    def _deal_type_from_text(self, text: str) -> str:
        t = (text or "").lower()
        if "warehouse" in t and "alert" in t:
            return "Warehouse Alert"
        if "price error" in t:
            return "Possible Price Error!"
        if "stacking" in t and "glitch" in t:
            return "Stacking Glitch!!"
        if "glitch" in t:
            return "Glitch!!"
        if "promo" in t and "glitch" in t:
            return "Promo Glitch!"
        if "deal" in t:
            return "Amazon Deal"
        return "Amazon Lead"

    def _startup_smoketest_enabled(self) -> bool:
        v = (self.config or {}).get("startup_smoketest_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return False

    def _startup_smoketest_count(self) -> int:
        try:
            v = int((self.config or {}).get("startup_smoketest_count") or 3)
        except Exception:
            v = 3
        return max(1, min(v, 10))

    def _startup_smoketest_scan_limit(self) -> int:
        try:
            v = int((self.config or {}).get("startup_smoketest_scan_limit_per_channel") or 50)
        except Exception:
            v = 50
        return max(5, min(v, 200))

    def _startup_smoketest_send_delay_s(self) -> float:
        try:
            v = float((self.config or {}).get("startup_smoketest_send_delay_s") or 1.0)
        except Exception:
            v = 1.0
        return max(0.0, min(v, 5.0))

    def _startup_smoketest_output_channel_id(self) -> Optional[int]:
        raw = (self.config or {}).get("startup_smoketest_output_channel_id")
        cid = _safe_int(raw)
        if cid:
            return cid
        # Default: send to your "deals" output channel
        return self._route_to_dest_id("deals")

    def _startup_smoketest_sources(self) -> List[int]:
        """
        Prefer config `source_channel_ids`.
        If empty, optionally fall back to `logs/Datalogs/Amazon.json` as a local "channel map"
        so startup smoke tests can run without manual config.
        """
        sources = self._source_channel_ids()
        if sources:
            return sources

        v = (self.config or {}).get("startup_smoketest_use_amazon_log_fallback", True)
        if isinstance(v, bool):
            enabled = v
        else:
            enabled = str(v or "").strip().lower() not in {"0", "false", "no", "off"}
        if not enabled:
            return []

        try:
            p = _REPO_ROOT / "logs" / "Datalogs" / "Amazon.json"
            if not p.exists():
                return []
            raw = p.read_text(encoding="utf-8", errors="replace")
            data = json.loads(raw) if raw else []
            if not isinstance(data, list):
                return []
            ids: List[int] = []
            for item in data[:500]:
                if not isinstance(item, dict):
                    continue
                cid = _safe_int(item.get("source_channel_id"))
                if cid and cid not in ids:
                    ids.append(cid)
                if len(ids) >= 25:
                    break
            return ids
        except Exception:
            return []

    async def _startup_smoketest(self) -> None:
        """
        On startup, scan recent messages in configured source channels and post the bot's rebuilt
        output for a small sample (default: 3) so you can visually verify behavior.
        """
        if not self._startup_smoketest_enabled():
            log.info("[BOOT] Startup smoke test disabled (startup_smoketest_enabled=false).")
            return

        count = self._startup_smoketest_count()
        scan_limit = self._startup_smoketest_scan_limit()
        delay_s = self._startup_smoketest_send_delay_s()
        out_id = self._startup_smoketest_output_channel_id()

        # Reset per-run OpenAI stats (so startup logs tell you exactly what was used this boot)
        self._openai_stats = {}
        self._openai_last_mode = {}

        log.info("-------- Startup Smoke Test --------")
        log.info("[BOOT][SMOKE] target_count=%s scan_limit_per_channel=%s output_channel_id=%s", count, scan_limit, out_id or "")

        sources = self._startup_smoketest_sources()
        if not sources:
            log.warning("[BOOT][SMOKE] No source channels configured. Set Instorebotforwarder/config.json -> source_channel_ids (or enable Amazon.json fallback).")
            return
        if not out_id:
            log.warning("[BOOT][SMOKE] No output channel configured. Set output_channels.deals or startup_smoketest_output_channel_id.")
            return

        out_ch = self.bot.get_channel(int(out_id))
        if out_ch is None:
            try:
                out_ch = await self.bot.fetch_channel(int(out_id))
            except Exception:
                out_ch = None
        if not isinstance(out_ch, (discord.TextChannel, discord.Thread)):
            log.warning("[BOOT][SMOKE] Output channel not found / not text: %s", out_id)
            return

        sent = 0
        for src_id in sources:
            if sent >= count:
                break
            ch = self.bot.get_channel(int(src_id))
            if ch is None:
                try:
                    ch = await self.bot.fetch_channel(int(src_id))
                except Exception:
                    ch = None
            if not isinstance(ch, discord.TextChannel):
                log.info("[BOOT][SMOKE] Skip non-text source channel id=%s", src_id)
                continue

            log.info("[BOOT][SMOKE] Scanning source #%s (%s) limit=%s", getattr(ch, "name", "unknown"), src_id, scan_limit)
            try:
                async for msg in ch.history(limit=scan_limit):
                    if sent >= count:
                        break
                    # Avoid looping on our own posts.
                    try:
                        if self.bot.user and msg.author and msg.author.id == self.bot.user.id:
                            continue
                    except Exception:
                        pass

                    # Fast pre-filter: skip messages that clearly can't contain Amazon links.
                    # This keeps startup smoke tests quick even in noisy channels.
                    try:
                        surfaces = self._gather_message_text_surfaces(msg)
                        joined_l = " ".join(surfaces).lower()
                    except Exception:
                        joined_l = ""
                    if joined_l and not any(
                        k in joined_l
                        for k in (
                            "amazon.",
                            "amazon.com",
                            "amzn.to",
                            "a.co/",
                            "/dp/",
                            "pricedoffers.com",
                            "saveyourdeals.com",
                            "fkd.deals",
                        )
                    ):
                        continue

                    try:
                        embed, meta = await self._analyze_message(msg)
                    except Exception as e:
                        log.info("[BOOT][SMOKE] Analyze failed msg=%s err=%s", getattr(msg, "id", ""), str(e)[:200])
                        continue
                    if not embed:
                        continue

                    sent += 1
                    header = f"Startup smoke-test {sent}/{count} • source <#{src_id}> • [jump]({msg.jump_url})"
                    try:
                        await out_ch.send(content=header, embed=embed, allowed_mentions=discord.AllowedMentions.none())
                        try:
                            scrape_ok = ((meta or {}).get("ctx") or {}).get("scrape_ok", "")
                            scrape_err = ((meta or {}).get("ctx") or {}).get("scrape_err", "")
                            openai_steps = ((meta or {}).get("ctx") or {}).get("openai_steps_mode", "")
                        except Exception:
                            scrape_ok = ""
                            scrape_err = ""
                            openai_steps = ""
                        # Commit de-dupe on successful send
                        try:
                            asin = str(((meta.get("amazon") or {}) if isinstance(meta.get("amazon"), dict) else {}).get("asin") or "").strip().upper()
                        except Exception:
                            asin = ""
                        if bool(meta.get("dedupe_reserved")) and asin:
                            self._dedupe_commit(asin)
                        log.info("[BOOT][SMOKE] SEND_OK %s/%s msg=%s scrape_ok=%s openai_steps=%s scrape_err=%s", sent, count, getattr(msg, "id", ""), scrape_ok, openai_steps, (scrape_err or "")[:120])
                    except Exception as e:
                        log.info("[BOOT][SMOKE] SEND_FAIL msg=%s err=%s", getattr(msg, "id", ""), str(e)[:200])
                        # Release de-dupe reservation if we failed to send.
                        try:
                            asin = str(((meta.get("amazon") or {}) if isinstance(meta.get("amazon"), dict) else {}).get("asin") or "").strip().upper()
                        except Exception:
                            asin = ""
                        if bool(meta.get("dedupe_reserved")) and asin:
                            self._dedupe_release(asin)
                        sent -= 1
                        continue

                    if delay_s > 0:
                        await asyncio.sleep(delay_s)
            except Exception as e:
                log.info("[BOOT][SMOKE] Failed scanning channel id=%s err=%s", src_id, str(e)[:200])

        if sent < count:
            log.warning("[BOOT][SMOKE] Completed with only %s/%s messages found that contain usable Amazon links.", sent, count)
        log.info("[BOOT][SMOKE] OPENAI stats=%s", json.dumps(self._openai_stats or {}, ensure_ascii=True))
        log.info("[BOOT][SMOKE] DEDUPE skipped=%s ttl_s=%s", int(self._dedupe_skipped or 0), int(self._dedupe_asin_ttl_s()))

    def _strip_urls_from_text(self, text: str) -> str:
        s = text or ""
        spans = affiliate_rewriter.extract_urls_with_spans(s)
        if not spans:
            return s
        out = s
        for (_u, start, end) in sorted(spans, key=lambda t: t[1], reverse=True):
            try:
                out = out[:start] + out[end:]
            except Exception:
                continue
        return out

    def _scrape_enabled(self) -> bool:
        v = (self.config or {}).get("amazon_scrape_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return True

    def _scrape_timeout_s(self) -> float:
        try:
            v = float((self.config or {}).get("amazon_scrape_timeout_s") or 10.0)
        except Exception:
            v = 10.0
        return max(3.0, min(v, 25.0))

    def _scrape_max_bytes(self) -> int:
        try:
            v = int((self.config or {}).get("amazon_scrape_max_bytes") or 600_000)
        except Exception:
            v = 600_000
        return max(50_000, min(v, 2_000_000))

    def _scrape_user_agent(self) -> str:
        ua = str((self.config or {}).get("amazon_scrape_user_agent") or "").strip()
        if ua:
            return ua
        return "Mozilla/5.0"

    def _amazon_delivery_zip(self) -> str:
        """
        Optional US ZIP code to set "Deliver to" location in Playwright scrapes.
        This improves consistency for price/availability and discount badges, but is best-effort.
        """
        z = str((self.config or {}).get("amazon_delivery_zip") or "").strip()
        # Simple guard: keep only 5 digits.
        m = re.search(r"\b(\d{5})\b", z)
        return m.group(1) if m else ""

    def _playwright_enabled(self) -> bool:
        """
        Optional stronger scraping using a real browser engine.
        Default: auto (enabled only if Playwright is importable).
        """
        v = (self.config or {}).get("amazon_playwright_scrape_enabled", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"1", "true", "yes", "y", "on", "auto"}:
            # auto: only if Playwright is available
            try:
                import playwright  # type: ignore  # noqa: F401

                return True
            except Exception:
                return False
        if s in {"0", "false", "no", "n", "off"}:
            return False
        # Default: auto
        try:
            import playwright  # type: ignore  # noqa: F401

            return True
        except Exception:
            return False

    def _playwright_timeout_s(self) -> float:
        try:
            v = float((self.config or {}).get("amazon_playwright_timeout_s") or 18.0)
        except Exception:
            v = 18.0
        return max(8.0, min(v, 60.0))

    def _scrape_cache_ttl_s(self) -> float:
        try:
            v = float((self.config or {}).get("amazon_scrape_cache_ttl_s") or 3600.0)
        except Exception:
            v = 3600.0
        return max(60.0, min(v, 7 * 24 * 3600.0))

    def _scrape_cache_get(self, asin: str) -> Optional[Dict[str, str]]:
        a = (asin or "").strip().upper()
        if not a:
            return None
        try:
            ts = float(self._amazon_scrape_cache_ts.get(a, 0.0) or 0.0)
        except Exception:
            ts = 0.0
        if not ts:
            return None
        if (time.time() - ts) > self._scrape_cache_ttl_s():
            self._amazon_scrape_cache_ts.pop(a, None)
            self._amazon_scrape_cache.pop(a, None)
            return None
        return self._amazon_scrape_cache.get(a)

    def _scrape_cache_put(self, asin: str, data: Dict[str, str]) -> None:
        a = (asin or "").strip().upper()
        if not a:
            return
        if not isinstance(data, dict):
            return
        # Only cache if we got a usable image or title
        if not ((data.get("image_url") or "").strip() or (data.get("title") or "").strip()):
            return
        self._amazon_scrape_cache[a] = {
            "title": str(data.get("title") or ""),
            "image_url": str(data.get("image_url") or ""),
            "price": str(data.get("price") or ""),
            "before_price": str(data.get("before_price") or ""),
            "discount_notes": str(data.get("discount_notes") or ""),
            "department": str(data.get("department") or ""),
        }
        self._amazon_scrape_cache_ts[a] = time.time()

    def _playwright_headless(self) -> bool:
        v = (self.config or {}).get("amazon_playwright_headless", None)
        if isinstance(v, bool):
            return v
        s = str(v or "").strip().lower()
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return True

    def _playwright_profile_dir(self) -> str:
        """
        Optional persistent browser profile dir.
        If set, passing a captcha once can keep scraping working.
        """
        raw = str((self.config or {}).get("amazon_playwright_profile_dir") or "").strip()
        if raw:
            return raw
        return ""

    def _extract_amazon_image_from_html(self, html_txt: str) -> str:
        """
        Best-effort product image extraction from Amazon DOM.
        Prefers the "2nd image" (as requested) when multiple candidates exist.
        """
        t = html_txt or ""
        if not t:
            return ""

        # Prefer the image set when present (lets us pick index=1 = 2nd image).
        m_dyn = re.search(r'data-a-dynamic-image=["\']([^"\']+)["\']', t, re.IGNORECASE)
        if m_dyn:
            raw_dyn = _html.unescape((m_dyn.group(1) or "").strip())
            try:
                data = json.loads(raw_dyn) if raw_dyn.startswith("{") else json.loads(raw_dyn.replace("&quot;", '"'))
            except Exception:
                data = {}
            if isinstance(data, dict) and data:
                scored: List[Tuple[int, str]] = []
                for k, v in data.items():
                    if not isinstance(k, str):
                        continue
                    area = 0
                    try:
                        if isinstance(v, list) and len(v) >= 2:
                            area = int(v[0]) * int(v[1])
                    except Exception:
                        area = 0
                    scored.append((area, k))
                scored.sort(key=lambda t: t[0], reverse=True)
                urls = [u for (_a, u) in scored if u and isinstance(u, str)]
                if urls:
                    try:
                        idx = int((self.config or {}).get("amazon_image_index") or 1)
                    except Exception:
                        idx = 1
                    idx = max(0, min(idx, len(urls) - 1))
                    return urls[idx]

        # Fallback: landingImage old-hires (often the primary image)
        m_hi = re.search(r'data-old-hires=["\']([^"\']+)["\']', t, re.IGNORECASE)
        if m_hi:
            return _html.unescape((m_hi.group(1) or "").strip())
        return ""

    def _extract_amazon_title_from_html(self, html_txt: str) -> str:
        t = html_txt or ""
        if not t:
            return ""
        m_pt = re.search(r'<span[^>]+id=["\']productTitle["\'][^>]*>(.*?)</span>', t, re.IGNORECASE | re.DOTALL)
        if m_pt:
            cand = _html.unescape(re.sub(r"<[^>]+>", " ", (m_pt.group(1) or "")))
            return " ".join(cand.split()).strip()
        m_t = re.search(r"<title>(.*?)</title>", t, re.IGNORECASE | re.DOTALL)
        if m_t:
            cand = _html.unescape(re.sub(r"<[^>]+>", " ", (m_t.group(1) or "")))
            return " ".join(cand.split()).strip()
        return ""

    async def _scrape_amazon_page_playwright(self, url: str, *, headless_override: Optional[bool] = None) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        Best-effort Playwright scrape for title/image when requests-based scrape is blocked.
        """
        u = (url or "").strip()
        if not u:
            return None, "missing url"
        if not self._playwright_enabled():
            return None, "playwright disabled"

        timeout_s = self._playwright_timeout_s()
        headless = self._playwright_headless() if headless_override is None else bool(headless_override)
        ua = self._scrape_user_agent()
        profile_dir = self._playwright_profile_dir()
        delivery_zip = self._amazon_delivery_zip()

        def _run() -> Tuple[Optional[Dict[str, str]], Optional[str]]:
            try:
                from playwright.sync_api import sync_playwright  # type: ignore
            except Exception:
                return None, "playwright not installed (pip install playwright)"

            try:
                with sync_playwright() as p:
                    launch_args = [
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                    ]
                    if profile_dir:
                        ctx = p.chromium.launch_persistent_context(
                            user_data_dir=profile_dir,
                            headless=headless,
                            args=launch_args,
                            user_agent=ua,
                            locale="en-US",
                            timezone_id="America/New_York",
                            viewport={"width": 1280, "height": 720},
                        )
                        page = ctx.new_page()
                    else:
                        browser = p.chromium.launch(headless=headless, args=launch_args)
                        ctx = browser.new_context(
                            user_agent=ua,
                            locale="en-US",
                            timezone_id="America/New_York",
                            viewport={"width": 1280, "height": 720},
                        )
                        page = ctx.new_page()
                    # Reduce basic automation fingerprints.
                    try:
                        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
                    except Exception:
                        pass
                    page.goto(u, wait_until="domcontentloaded", timeout=int(timeout_s * 1000))

                    # Best-effort: set delivery ZIP for US pricing consistency (if configured).
                    if delivery_zip:
                        try:
                            # Open location popover (different layouts use different selectors).
                            # nav-global-location-popover-link exists on most Amazon pages.
                            try:
                                page.click("#nav-global-location-popover-link", timeout=1500)
                            except Exception:
                                try:
                                    page.click("#glow-ingress-line2", timeout=1500)
                                except Exception:
                                    pass

                            # Zip input + apply.
                            try:
                                page.wait_for_selector("#GLUXZipUpdateInput", timeout=1800)
                                page.fill("#GLUXZipUpdateInput", delivery_zip)
                                page.click("#GLUXZipUpdate", timeout=1500)
                                # Some flows require a confirmation button.
                                try:
                                    page.click("#GLUXConfirmClose", timeout=1800)
                                except Exception:
                                    try:
                                        page.click("input[name='glowDoneButton']", timeout=1800)
                                    except Exception:
                                        pass
                                page.wait_for_timeout(700)
                                # Reload after location set so price/badges update.
                                page.reload(wait_until="domcontentloaded", timeout=int(timeout_s * 1000))
                            except Exception:
                                pass
                        except Exception:
                            pass

                    # Give the page a moment to populate image attrs.
                    page.wait_for_timeout(700)
                    # Try to read the buybox/current price directly from the DOM (more reliable than regex on HTML).
                    price_txt = ""
                    try:
                        for sel in (
                            "span.priceToPay span.a-offscreen",
                            "#corePriceDisplay_desktop_feature_div span.priceToPay span.a-offscreen",
                            "#corePriceDisplay_desktop_feature_div span.a-price:not(.a-text-price) span.a-offscreen",
                            "#corePriceDisplay_mobile_feature_div span.priceToPay span.a-offscreen",
                            "#corePriceDisplay_mobile_feature_div span.a-price:not(.a-text-price) span.a-offscreen",
                            "#apex_desktop span.priceToPay span.a-offscreen",
                            "#apex_mobile span.priceToPay span.a-offscreen",
                        ):
                            try:
                                el = page.query_selector(sel)
                                tx = (el.text_content() if el else "") or ""
                                tx = " ".join(tx.split()).strip()
                                if tx:
                                    price_txt = tx
                                    break
                            except Exception:
                                continue
                    except Exception:
                        price_txt = ""
                    html_txt = page.content() or ""
                    ctx.close()
                    if not profile_dir:
                        browser.close()
            except Exception as e:
                msg = str(e)[:220]
                # Common "browser not installed" guidance
                if "playwright install" in msg.lower() or "executable doesn't exist" in msg.lower():
                    return None, "playwright browser missing (run: python -m playwright install chromium)"
                return None, f"playwright failed: {msg}"

            low = html_txt.lower()
            if ("robot check" in low) or ("enter the characters you see below" in low) or ("captcha" in low and "amazon" in low):
                return None, "blocked (robot check)"

            title = self._extract_amazon_title_from_html(html_txt)
            image_url = self._extract_amazon_image_from_html(html_txt)
            price = self._normalize_price_str(price_txt) or self._extract_amazon_current_price_from_html(html_txt)
            try:
                price = self._sanitize_amazon_price_for_marketplace(price, url=u)
            except Exception:
                pass
            before_price = self._extract_amazon_before_price_from_html(html_txt, current_price=price)
            try:
                before_price = self._sanitize_amazon_price_for_marketplace(before_price, url=u)
            except Exception:
                pass
            discount_notes = self._extract_amazon_discount_notes_from_html(html_txt)
            dept = self._extract_amazon_department_from_html(html_txt)
            availability = self._extract_amazon_availability_from_html(html_txt)

            out = {
                "title": title.strip(),
                "image_url": image_url.strip(),
                "price": (price or "").strip(),
                "before_price": before_price.strip(),
                "discount_notes": "; ".join([n for n in (discount_notes or []) if n]).strip(),
                "department": (dept or "").strip(),
                "availability": (availability or "").strip(),
            }
            if not (out.get("title") or out.get("image_url") or out.get("price")):
                return None, "no useful fields found"
            return out, None

        return await asyncio.to_thread(_run)

    async def _scrape_deal_hub_for_amazon_assets(self, url: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        When Amazon blocks direct scraping, try to extract Amazon-hosted image/title from
        common deal hub pages (pricedoffers/fkd/saveyourdeals/etc).
        """
        u = (url or "").strip()
        if not u:
            return None, "missing url"
        if not (u.startswith("http://") or u.startswith("https://")):
            return None, "invalid url"

        timeout_s = self._scrape_timeout_s()
        max_bytes = self._scrape_max_bytes()
        headers = {
            "User-Agent": self._scrape_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            import aiohttp

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
                async with session.get(u, headers=headers, allow_redirects=True) as resp:
                    status = int(resp.status)
                    if status >= 400:
                        return None, f"HTTP {status}"
                    buf = bytearray()
                    async for chunk in resp.content.iter_chunked(16_384):
                        if not chunk:
                            break
                        buf.extend(chunk)
                        if len(buf) >= max_bytes:
                            break
                    html_txt = buf.decode("utf-8", errors="replace")
        except Exception as e:
            return None, f"fetch failed: {e}"

        title = self._extract_html_meta(html_txt, prop="og:title") or ""
        if not title:
            m_t = re.search(r"<title>(.*?)</title>", html_txt, re.IGNORECASE | re.DOTALL)
            if m_t:
                cand = _html.unescape(re.sub(r"<[^>]+>", " ", (m_t.group(1) or "")))
                title = " ".join(cand.split()).strip()

        # Find first Amazon-hosted product image.
        # Prefer m.media-amazon.com/images/I/
        image_url = ""
        for pat in (
            r"https?://m\.media-amazon\.com/images/I/[^\s\"'<>]+",
            r"https?://images-na\.ssl-images-amazon\.com/images/I/[^\s\"'<>]+",
        ):
            m = re.search(pat, html_txt, re.IGNORECASE)
            if m:
                image_url = _html.unescape((m.group(0) or "").strip())
                break

        # Fallback: og:image if it points to Amazon image CDN
        if not image_url:
            ogi = self._extract_html_meta(html_txt, prop="og:image") or ""
            if ogi and self._is_amazon_image_url(ogi):
                image_url = ogi.strip()

        # Find a canonical Amazon /dp/ASIN link on the hub page (dmflip, pricedoffers, etc).
        amazon_url = ""
        asin = ""
        try:
            # First, look for explicit /dp/ASIN links.
            m_dp = re.search(r"https?://(?:www\.)?amazon\.[a-z.]+/dp/([A-Z0-9]{10})", html_txt, re.IGNORECASE)
            if m_dp:
                asin = (m_dp.group(1) or "").strip().upper()
            if not asin:
                # Also allow gp/product
                m_gp = re.search(r"https?://(?:www\.)?amazon\.[a-z.]+/gp/product/([A-Z0-9]{10})", html_txt, re.IGNORECASE)
                if m_gp:
                    asin = (m_gp.group(1) or "").strip().upper()
            if asin:
                mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
                amazon_url = f"{mp}/dp/{asin}" if mp else f"https://www.amazon.com/dp/{asin}"
        except Exception:
            amazon_url = ""
            asin = ""

        out = {
            "title": title.strip(),
            "image_url": image_url.strip(),
            "price": "",
            "before_price": "",
            "discount_notes": "",
            "department": "",
            "amazon_url": amazon_url.strip(),
            "asin": asin.strip(),
        }
        if not (out.get("title") or out.get("image_url")):
            return None, "no useful fields found"
        return out, None

    async def _adsystem_image_by_asin(self, asin: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Last-resort: try Amazon adsystem AsinImage endpoint.
        This is not guaranteed to work, but it's cheap to probe.
        """
        a = (asin or "").strip().upper()
        if not a:
            return None, "missing asin"
        # US marketplace
        widget = f"https://ws-na.amazon-adsystem.com/widgets/q?_encoding=UTF8&MarketPlace=US&ASIN={a}&ServiceVersion=20070822&ID=AsinImage&WS=1&Format=_AC_SL500_"
        try:
            import aiohttp

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8.0)) as session:
                async with session.get(widget, allow_redirects=True) as resp:
                    ct = (resp.headers.get("content-type") or "").lower()
                    final = str(resp.url) if getattr(resp, "url", None) else widget
                    if "image/" in ct or final.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                        return final, None
                    # Some endpoints return HTML with an <img src="...">
                    txt = await resp.text(errors="ignore")
                    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', txt, re.IGNORECASE)
                    if m:
                        cand = _html.unescape((m.group(1) or "").strip())
                        if cand:
                            return cand, None
        except Exception as e:
            return None, f"adsystem fetch failed: {e}"
        return None, "adsystem no image"

    def _extract_html_meta(self, html: str, *, prop: str) -> str:
        """
        Extract <meta property="..."> or <meta name="..."> content.
        """
        t = html or ""
        if not t:
            return ""
        # property="og:title" content="..."
        m = re.search(rf'<meta[^>]+property=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)["\']', t, re.IGNORECASE)
        if m:
            return _html.unescape((m.group(1) or "").strip())
        m2 = re.search(rf'<meta[^>]+name=["\']{re.escape(prop)}["\'][^>]+content=["\']([^"\']+)["\']', t, re.IGNORECASE)
        if m2:
            return _html.unescape((m2.group(1) or "").strip())
        return ""

    def _extract_amazon_department_from_html(self, html_txt: str) -> str:
        """
        Best-effort "department" (category) hint from an Amazon product page.
        Examples:
        - "Grocery & Gourmet Food"
        - "Health & Household"

        Used for routing (e.g. grocery) when message text doesn't include keywords.
        """
        t = html_txt or ""
        if not t:
            return ""

        # 1) Title/og:title often ends with ": <Department>"
        candidates: List[str] = []
        try:
            og = self._extract_html_meta(t, prop="og:title")
            if og:
                candidates.append(og)
        except Exception:
            pass
        try:
            m_t = re.search(r"<title>(.*?)</title>", t, re.IGNORECASE | re.DOTALL)
            if m_t:
                cand = _html.unescape(re.sub(r"<[^>]+>", " ", (m_t.group(1) or "")))
                cand = " ".join(cand.split()).strip()
                if cand:
                    candidates.append(cand)
        except Exception:
            pass

        for c in candidates:
            if not c:
                continue
            parts = [p.strip() for p in c.split(":") if p.strip()]
            if not parts:
                continue
            last = parts[-1]
            low_last = last.lower()
            if "amazon" in low_last or "sign in" in low_last:
                continue
            if 3 <= len(last) <= 60:
                return last

        # 2) Breadcrumbs: wayfinding-breadcrumbs_container
        try:
            m = re.search(r'wayfinding-breadcrumbs_container(.{0,5000})</div>', t, re.IGNORECASE | re.DOTALL)
            snippet = m.group(0) if m else ""
            if snippet:
                crumbs: List[str] = []
                for mm in re.finditer(r"<a[^>]*>([^<]{1,80})</a>", snippet, re.IGNORECASE):
                    txt = _html.unescape((mm.group(1) or "").strip())
                    txt = " ".join(txt.split()).strip()
                    if not txt:
                        continue
                    low = txt.lower()
                    if low in {"back", "see all", "details"}:
                        continue
                    if "amazon" in low:
                        continue
                    crumbs.append(txt)
                if crumbs:
                    # Prefer the last crumb that looks like a department label.
                    for cand in reversed(crumbs):
                        if 3 <= len(cand) <= 60:
                            return cand
        except Exception:
            pass

        return ""

    def _extract_amazon_availability_from_html(self, html_txt: str) -> str:
        """
        Best-effort availability signal from an Amazon product page.

        Returns:
        - "out_of_stock" when we see strong OOS/unavailable wording
        - "" when unknown/in-stock (we don't try to guarantee "in stock")
        """
        t = html_txt or ""
        if not t:
            return ""
        low = t.lower()

        strong = (
            "currently unavailable",
            "temporarily out of stock",
            "we don't know when or if this item will be back in stock",
            "out of stock",
        )
        if any(s in low for s in strong):
            return "out_of_stock"

        # Common availability container (sometimes contains "In Stock." / "Out of Stock.")
        try:
            m = re.search(r'id=["\']availability["\'][\s\S]{0,1200}</', t, re.IGNORECASE)
            snippet = (m.group(0) or "").lower() if m else ""
        except Exception:
            snippet = ""
        if snippet:
            if ("currently unavailable" in snippet) or ("temporarily out of stock" in snippet) or ("out of stock" in snippet):
                return "out_of_stock"

        return ""

    def _extract_jsonld_product(self, html: str) -> Dict[str, Any]:
        """
        Parse a Product JSON-LD block if present. Best-effort only.
        """
        t = (html or "")[: self._scrape_max_bytes()]
        if not t:
            return {}
        blocks = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', t, re.IGNORECASE | re.DOTALL)
        for raw in blocks[:6]:
            cand = (raw or "").strip()
            if not cand:
                continue
            # JSON-LD is often surrounded by whitespace/newlines; sometimes has multiple objects/arrays.
            try:
                data = json.loads(cand)
            except Exception:
                continue

            objs: List[Dict[str, Any]] = []
            if isinstance(data, dict):
                objs = [data]
            elif isinstance(data, list):
                objs = [x for x in data if isinstance(x, dict)]

            for obj in objs:
                typ = obj.get("@type")
                if isinstance(typ, list):
                    is_product = any(str(x).lower() == "product" for x in typ)
                else:
                    is_product = str(typ or "").lower() == "product"
                if not is_product:
                    continue
                return obj
        return {}

    async def _scrape_amazon_page(self, url: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        Best-effort enrichment by fetching the Amazon product page and extracting:
        - title
        - image_url
        - price (when present in HTML/JSON-LD)

        NOTE: This does NOT bypass anti-bot pages; it will gracefully fail and fall back to message-derived fields.
        """
        u = (url or "").strip()
        if not u:
            return None, "missing url"
        if not (u.startswith("http://") or u.startswith("https://")):
            return None, "invalid url"

        asin_guess = affiliate_rewriter.extract_asin(u) or ""
        cached = self._scrape_cache_get(asin_guess) if asin_guess else None
        if cached:
            return cached, None

        timeout_s = self._scrape_timeout_s()
        max_bytes = self._scrape_max_bytes()
        headers = {
            "User-Agent": self._scrape_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            import aiohttp

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
                async with session.get(u, headers=headers, allow_redirects=True) as resp:
                    status = int(resp.status)
                    if status >= 400:
                        return None, f"HTTP {status}"
                    # Read up to max_bytes
                    buf = bytearray()
                    async for chunk in resp.content.iter_chunked(16_384):
                        if not chunk:
                            break
                        buf.extend(chunk)
                        if len(buf) >= max_bytes:
                            break
                    html_txt = buf.decode("utf-8", errors="replace")
        except Exception as e:
            return None, f"fetch failed: {e}"

        low = html_txt.lower()
        # Common anti-bot page signals
        if (
            ("robot check" in low)
            or ("enter the characters you see below" in low)
            or ("sorry, we just need to make sure" in low)
            or ("to discuss automated access" in low)
            or ("captcha" in low and "amazon" in low)
        ):
            # Try a real-browser scrape (Playwright) before giving up.
            pw, pw_err = await self._scrape_amazon_page_playwright(u)
            if (not pw) and (pw_err or "").startswith("blocked"):
                # Optional: retry in headful mode (much higher success rate).
                # Requires a display (Windows desktop, or Linux + Xvfb/noVNC).
                v = (self.config or {}).get("amazon_playwright_try_headful_on_block", True)
                allow = bool(v) if isinstance(v, bool) else (str(v or "").strip().lower() not in {"0", "false", "no", "off"})
                if allow:
                    if os.name == "nt" or (os.getenv("DISPLAY", "") or "").strip():
                        pw2, pw2_err = await self._scrape_amazon_page_playwright(u, headless_override=False)
                        if pw2 and not pw2_err:
                            pw, pw_err = pw2, None
            if pw and not pw_err:
                if asin_guess:
                    self._scrape_cache_put(asin_guess, pw)
                return pw, None
            # Last-resort: try adsystem image endpoint by ASIN.
            if asin_guess:
                img, img_err = await self._adsystem_image_by_asin(asin_guess)
                if img and not img_err:
                    data = {"title": "", "image_url": img, "price": "", "before_price": "", "discount_notes": "", "department": ""}
                    self._scrape_cache_put(asin_guess, data)
                    return data, None
            # Even when Playwright fails, preserve the clearer reason if we have one.
            return None, pw_err or "blocked (robot check)"

        # Prefer Product JSON-LD when present (more stable than og:* on Amazon).
        title = ""
        image_url = ""

        price = ""
        before_price = ""
        discount_notes: List[str] = []
        department = ""
        # JSON-LD offers.price is the cleanest when present
        prod = self._extract_jsonld_product(html_txt)
        if prod:
            try:
                title = str(prod.get("name") or "").strip() or title

                # Prefer the "2nd image" when available; otherwise use the 1st.
                img = prod.get("image")
                if isinstance(img, list):
                    if len(img) >= 2:
                        image_url = str(img[1] or "").strip() or image_url
                    elif img:
                        image_url = str(img[0] or "").strip() or image_url
                elif isinstance(img, str):
                    image_url = img.strip() or image_url

                offers = prod.get("offers")
                offer0 = offers[0] if isinstance(offers, list) and offers and isinstance(offers[0], dict) else (offers if isinstance(offers, dict) else None)
                if isinstance(offer0, dict):
                    p = offer0.get("price")
                    cur = offer0.get("priceCurrency")
                    if p and not price:
                        price = str(p).strip()
                        if cur:
                            price = f"{price} {str(cur).strip()}".strip()
            except Exception:
                pass

        # Fallback: og meta
        if not title:
            title = self._extract_html_meta(html_txt, prop="og:title") or ""
        if not image_url:
            image_url = self._extract_html_meta(html_txt, prop="og:image") or ""

        # Fallback: common Amazon DOM patterns (works even when og/meta are missing)
        if not title:
            m_pt = re.search(r'<span[^>]+id=["\']productTitle["\'][^>]*>(.*?)</span>', html_txt, re.IGNORECASE | re.DOTALL)
            if m_pt:
                cand = _html.unescape(re.sub(r"<[^>]+>", " ", (m_pt.group(1) or "")))
                title = " ".join(cand.split()).strip()
        if not title:
            m_t = re.search(r"<title>(.*?)</title>", html_txt, re.IGNORECASE | re.DOTALL)
            if m_t:
                cand = _html.unescape(re.sub(r"<[^>]+>", " ", (m_t.group(1) or "")))
                title = " ".join(cand.split()).strip()

        if not image_url:
            m_hi = re.search(r'data-old-hires=["\']([^"\']+)["\']', html_txt, re.IGNORECASE)
            if m_hi:
                image_url = _html.unescape((m_hi.group(1) or "").strip())

        if not image_url:
            # Try to parse data-a-dynamic-image JSON and prefer the 2nd best image.
            m_dyn = re.search(r'data-a-dynamic-image=["\']([^"\']+)["\']', html_txt, re.IGNORECASE)
            if m_dyn:
                raw_dyn = _html.unescape((m_dyn.group(1) or "").strip())
                try:
                    data = json.loads(raw_dyn) if raw_dyn.startswith("{") else json.loads(raw_dyn.replace("&quot;", '"'))
                except Exception:
                    data = {}
                if isinstance(data, dict) and data:
                    scored: List[Tuple[int, str]] = []
                    for k, v in data.items():
                        if not isinstance(k, str):
                            continue
                        area = 0
                        try:
                            if isinstance(v, list) and len(v) >= 2:
                                area = int(v[0]) * int(v[1])
                        except Exception:
                            area = 0
                        scored.append((area, k))
                    scored.sort(key=lambda t: t[0], reverse=True)
                    urls = [u for (_a, u) in scored if u]
                    if len(urls) >= 2:
                        image_url = urls[1]
                    elif urls:
                        image_url = urls[0]

        # Fallback current price patterns (best-effort).
        if not price:
            try:
                price = self._extract_amazon_current_price_from_html(html_txt) or ""
            except Exception:
                price = ""

        # Best-effort before/list price (strike-through, list price labels, etc).
        before_price = self._extract_amazon_before_price_from_html(html_txt, current_price=price)

        # Currency guardrails (amazon.com should be USD; reject foreign currencies).
        try:
            price = self._sanitize_amazon_price_for_marketplace(price, url=u)
            before_price = self._sanitize_amazon_price_for_marketplace(before_price, url=u)
        except Exception:
            pass

        # Best-effort discount signals (coupon/sub&save/deal badge).
        try:
            discount_notes = self._extract_amazon_discount_notes_from_html(html_txt)
        except Exception:
            discount_notes = []

        # Best-effort department/category hint (e.g. Grocery & Gourmet Food).
        try:
            department = self._extract_amazon_department_from_html(html_txt)
        except Exception:
            department = ""

        # Best-effort availability (out of stock / currently unavailable).
        try:
            availability = self._extract_amazon_availability_from_html(html_txt)
        except Exception:
            availability = ""

        # If we still have no image, try adsystem image by ASIN (works even when the HTML is sparse).
        if (not image_url) and asin_guess:
            try:
                img2, img2_err = await self._adsystem_image_by_asin(asin_guess)
            except Exception as e:
                img2, img2_err = None, str(e)[:200]
            if img2 and not img2_err:
                image_url = str(img2).strip()

        out = {
            "title": " ".join((title or "").split()).strip(),
            "image_url": (image_url or "").strip(),
            "price": " ".join((price or "").split()).strip(),
            "before_price": " ".join((before_price or "").split()).strip(),
            "discount_notes": "; ".join([n for n in (discount_notes or []) if n]).strip(),
            "department": (department or "").strip(),
            "availability": (availability or "").strip(),
        }
        # Ensure at least one field is useful
        if not (out.get("title") or out.get("image_url") or out.get("price")):
            return None, "no useful fields found"

        # Sanity: if our extracted "before_price" is lower than the current price,
        # it's almost certainly a coupon/savings amount or unrelated number.
        # Clear it so Playwright merge (below) can replace it, or it becomes N/A later.
        try:
            cur_v, _cur_s = self._price_to_float(out.get("price", ""))
            bef_v, _bef_s = self._price_to_float(out.get("before_price", ""))
            if (cur_v is not None) and (bef_v is not None) and (bef_v > 0) and (cur_v > 0) and (bef_v < cur_v):
                out["before_price"] = ""
        except Exception:
            pass

        # If the requests-based fetch didn't expose the current price (common on some product layouts),
        # try a Playwright scrape and merge any missing fields.
        pw: Optional[Dict[str, str]] = None
        pw_err: Optional[str] = None
        if self._playwright_enabled() and (not out.get("price") or not out.get("image_url") or not out.get("before_price")):
            try:
                pw, pw_err = await self._scrape_amazon_page_playwright(u)
            except Exception as e:
                pw, pw_err = None, str(e)[:200]
            if pw and not pw_err:
                for k in ("title", "image_url", "price", "before_price", "discount_notes", "department"):
                    if (not str(out.get(k) or "").strip()) and str(pw.get(k) or "").strip():
                        out[k] = str(pw.get(k) or "").strip()

        # Re-run the sanity check AFTER Playwright merge too (Playwright can fill current price, which
        # can reveal a previously-extracted bogus "before" number like an add-on price/coupon).
        try:
            cur_v2, _cur_s2 = self._price_to_float(out.get("price", ""))
            bef_v2, _bef_s2 = self._price_to_float(out.get("before_price", ""))
            if (cur_v2 is not None) and (bef_v2 is not None) and (bef_v2 > 0) and (cur_v2 > 0) and (bef_v2 < cur_v2):
                out["before_price"] = ""
        except Exception:
            pass

        # If we cleared before_price and Playwright had a candidate, try to use it.
        if (not str(out.get("before_price") or "").strip()) and pw and (not pw_err):
            try:
                cand_b = str(pw.get("before_price") or "").strip()
            except Exception:
                cand_b = ""
            if cand_b:
                out["before_price"] = cand_b
                # ... and validate it one more time.
                try:
                    cur_v3, _cur_s3 = self._price_to_float(out.get("price", ""))
                    bef_v3, _bef_s3 = self._price_to_float(out.get("before_price", ""))
                    if (cur_v3 is not None) and (bef_v3 is not None) and (bef_v3 > 0) and (cur_v3 > 0) and (bef_v3 < cur_v3):
                        out["before_price"] = ""
                except Exception:
                    pass
        if asin_guess:
            self._scrape_cache_put(asin_guess, out)
        return out, None

    def _extract_price_guess(self, text: str) -> str:
        """
        "Current price" guess from a messy deal post.

        We intentionally prefer the LOWEST currency amount mentioned, because posts commonly include:
        - retail/list/orig price (higher)
        - deal price (lower)
        """
        s = (text or "")
        if not s:
            return ""

        # Prefer explicitly labeled "Current Price: $X" when present.
        m_lab = re.search(r"\bcurrent\s*price\b[^$£€]{0,20}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)", s, re.IGNORECASE)
        if m_lab:
            cand = self._normalize_price_str(m_lab.group(1) or "")
            if cand:
                return cand

        # Capture all symbol-prefixed prices and pick the minimum numeric value.
        matches = list(re.finditer(r"(?<!\w)([$£€])\s?(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)(?!\w)", s))
        best_sym = ""
        best_val: Optional[float] = None
        best_raw = ""
        for m in matches:
            sym = (m.group(1) or "").strip()
            num_raw = (m.group(2) or "").replace(",", "").strip()
            try:
                val = float(num_raw)
            except Exception:
                continue
            if best_val is None or val < best_val:
                best_val = val
                best_sym = sym
                best_raw = f"{sym}{val:.2f}".rstrip("0").rstrip(".")

        if best_raw:
            return best_raw

        # Fallback: currency codes (pick the minimum numeric amount there too)
        matches2 = list(re.finditer(r"(?<!\w)(\d{1,4}(?:\.\d{2})?)\s?(USD|CAD|AUD|GBP|EUR)\b", s, re.IGNORECASE))
        best_val2: Optional[float] = None
        best_raw2 = ""
        for m in matches2:
            num_raw = (m.group(1) or "").strip()
            code = (m.group(2) or "").upper().strip()
            try:
                val = float(num_raw)
            except Exception:
                continue
            if best_val2 is None or val < best_val2:
                best_val2 = val
                best_raw2 = f"{val:g} {code}".strip()
        return best_raw2

    def _extract_before_price_guess(self, text: str, *, current_price: str) -> str:
        """
        Best-effort "Before" price from common patterns like:
        - retail $39.99
        - (Orig. $30)
        - list price $99
        """
        s = (text or "")
        if not s:
            return ""
        cur = (current_price or "").strip()

        # Prefer explicitly labeled "Before: $X" when present.
        m_b = re.search(r"\bbefore\b[^$£€]{0,20}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)", s, re.IGNORECASE)
        if m_b:
            candb = self._normalize_price_str(m_b.group(1) or "")
            if candb and candb != cur:
                return candb

        # Prefer explicit orig/retail/list patterns.
        patterns = [
            r"\b(?:orig(?:inal)?|retail(?:s)?|list\s*price|was)\b[^$£€]{0,40}([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)",
            r"\(\s*(?:orig(?:inal)?|retail)\.?\s*([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)\s*\)",
        ]
        for pat in patterns:
            m = re.search(pat, s, re.IGNORECASE)
            if m:
                cand = " ".join((m.group(1) or "").split()).strip()
                if cand and cand != cur:
                    return cand

        # Otherwise, take the maximum currency amount if it differs from current.
        vals: List[Tuple[float, str]] = []
        for m in re.finditer(r"(?<!\w)([$£€])\s?(\d{1,4}(?:,\d{3})*(?:\.\d{2})?)(?!\w)", s):
            sym = (m.group(1) or "").strip()
            num_raw = (m.group(2) or "").replace(",", "").strip()
            try:
                val = float(num_raw)
            except Exception:
                continue
            raw = f"{sym}{val:.2f}".rstrip("0").rstrip(".")
            vals.append((val, raw))
        if not vals:
            return ""
        vals.sort(key=lambda t: t[0], reverse=True)
        top = vals[0][1].strip()
        return top if top and top != cur else ""

    def _clean_product_title(self, title: str) -> str:
        t = " ".join((title or "").split()).strip()
        if not t:
            return ""
        # Common og:title prefix
        if t.lower().startswith("amazon.com:"):
            t = t.split(":", 1)[-1].strip()
        # Remove trailing " - Amazon.com" style suffixes
        t = re.sub(r"\s*-\s*amazon\.[a-z.]+\s*$", "", t, flags=re.IGNORECASE).strip()

        # Remove common deal-post suffixes so we keep just the product name.
        # Example: "X for $6.99, retail $19.99!" -> "X"
        t = re.sub(r"\s+for\s+([$£€]\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)\b.*$", "", t, flags=re.IGNORECASE).strip()
        t = re.sub(r"\s*(?:,|\(|-)?\s*(?:retail|retails|orig|original|list\s*price|was)\b.*$", "", t, flags=re.IGNORECASE).strip()

        # User-requested: prefer the part BEFORE the first comma (keeps product name shorter).
        if "," in t:
            left = t.split(",", 1)[0].strip()
            if left:
                t = left
        return t[:240]

    def _is_amazon_image_url(self, url: str) -> bool:
        """
        Only treat Amazon-hosted images as valid "Amazon website" images.
        This avoids using Discord attachment screenshots when you want a clean card image.
        """
        u = (url or "").strip()
        if not u:
            return False
        try:
            host = (urlparse(u).netloc or "").lower()
        except Exception:
            host = ""
        return host.endswith("m.media-amazon.com") or host.endswith("images-na.ssl-images-amazon.com") or host.endswith("images.amazon.com")

    def _gather_message_text_surfaces(self, message: discord.Message) -> List[str]:
        parts: List[str] = []
        if message.content:
            parts.append(str(message.content))
        for e in (message.embeds or []):
            t = getattr(e, "title", None)
            d = getattr(e, "description", None)
            if t:
                parts.append(str(t))
            if d:
                parts.append(str(d))
            for f in (getattr(e, "fields", None) or []):
                fn = getattr(f, "name", None)
                fv = getattr(f, "value", None)
                if fn:
                    parts.append(str(fn))
                if fv:
                    parts.append(str(fv))
        return [p for p in (x.strip() for x in parts) if p]

    def _guess_listing_from_message(self, message: discord.Message, *, asin: str, final_url: str) -> Dict[str, str]:
        """
        Reconstruct a "best effort" product card from the Discord message itself (no PA-API).
        """
        texts = self._gather_message_text_surfaces(message)
        joined = "\n".join(texts)

        # Title: prefer embed title, else first non-empty line of content/description without URLs.
        title = ""
        if message.embeds:
            try:
                t0 = getattr(message.embeds[0], "title", None)
                if t0:
                    title = str(t0).strip()
            except Exception:
                title = ""
        if not title:
            base = self._strip_urls_from_text(message.content or "")
            base = self._neutralize_mentions(base)
            base = " ".join(base.split()).strip()
            if base:
                title = (base[:120] + "…") if len(base) > 120 else base
        if (not title) and message.embeds:
            # Many bots post the "real" text in embed.description with empty message.content.
            try:
                d0 = getattr(message.embeds[0], "description", None)
                if d0:
                    base2 = self._strip_urls_from_text(str(d0))
                    base2 = self._neutralize_mentions(base2)
                    base2 = base2.strip()
                    if base2:
                        first_line = base2.splitlines()[0].strip()
                        if first_line:
                            title = (first_line[:120] + "…") if len(first_line) > 120 else first_line
            except Exception:
                pass

        if not title:
            title = f"Amazon lead {asin}".strip() if asin else "Amazon lead"

        price = self._extract_price_guess(joined)
        before_price = self._extract_before_price_guess(joined, current_price=price)
        promo_codes = self._extract_promo_codes(joined)
        steps_lines = self._extract_step_lines(joined)
        source_credit = self._extract_source_credit(joined)
        deal_type = self._deal_type_from_text(joined)

        # Category guess: look for grocery keywords in the message text (not from PA-API).
        kws = (self.config or {}).get("amazon_grocery_keywords") or []
        kws_l = [str(k).strip().lower() for k in kws] if isinstance(kws, list) else []
        joined_l = joined.lower()
        is_grocery = any(k and (k in joined_l) for k in kws_l)
        category = "grocery" if is_grocery else ""

        # Image: prefer embed image/thumbnail, then first attachment image.
        image_url = ""
        try:
            if message.embeds:
                e0 = message.embeds[0]
                # discord.py embeds expose .thumbnail/.image as objects with .url
                th = getattr(getattr(e0, "thumbnail", None), "url", None)
                im = getattr(getattr(e0, "image", None), "url", None)
                if im:
                    image_url = str(im).strip()
                elif th:
                    image_url = str(th).strip()
        except Exception:
            image_url = ""
        if not image_url:
            try:
                for att in (message.attachments or []):
                    ct = (getattr(att, "content_type", None) or "").lower()
                    if ct.startswith("image/") and getattr(att, "url", None):
                        image_url = str(att.url).strip()
                        break
            except Exception:
                image_url = ""
        # Only accept Amazon-hosted images when using message-derived image fallback.
        if image_url and not self._is_amazon_image_url(image_url):
            image_url = ""

        excerpt = self._strip_urls_from_text(message.content or "")
        excerpt = self._neutralize_mentions(excerpt)
        excerpt = " ".join(excerpt.split()).strip()
        if len(excerpt) > 220:
            excerpt = excerpt[:217] + "..."

        author = ""
        try:
            if message.author:
                author = str(message.author)
        except Exception:
            author = ""

        return {
            "title": title,
            "price": price,
            "before_price": before_price,
            "category": category,
            "image_url": image_url,
            "source_excerpt": excerpt,
            "source_author": author,
            "final_url": final_url,
            "asin": asin,
            "deal_type": deal_type,
            "promo_codes": ", ".join(promo_codes),
            "steps_raw": "\n".join(steps_lines),
            "source_credit": source_credit,
        }

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

    async def _expand_url_best_effort(self, session: Any, url_used: str, *, timeout_s: float, max_redirects: int) -> str:
        """
        Best-effort URL expansion + unwrapping.
        This is shared so both Amazon detection and simple forwarding behave consistently.
        """
        cand = affiliate_rewriter.unwrap_known_query_redirects((url_used or "").strip()) or (url_used or "").strip()
        final_url = cand
        if not final_url:
            return ""

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

        # Special-case: mavely.app.link often uses HTML/JS interstitials and doesn't always 3xx redirect.
        # If we still have a Mavely link after expansion, fetch HTML and extract the first outbound URL.
        try:
            if affiliate_rewriter.is_mavely_link(final_url):
                try:
                    import aiohttp

                    async with session.get(final_url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                        html_txt = await resp.text(errors="ignore")
                except Exception:
                    html_txt = ""
                out = ""
                try:
                    out = affiliate_rewriter._extract_first_outbound_url_from_html(html_txt) or ""  # type: ignore[attr-defined]
                except Exception:
                    out = ""
                if out:
                    out_abs = out
                    if out_abs.startswith("/"):
                        out_abs = urljoin(final_url, out_abs)
                    out_abs = affiliate_rewriter.unwrap_known_query_redirects(out_abs) or out_abs
                    final_url = out_abs
                    # One more redirect-follow if needed
                    if affiliate_rewriter.should_expand_url(final_url):
                        try:
                            final_url = await affiliate_rewriter.expand_url(
                                session,
                                final_url,
                                timeout_s=timeout_s,
                                max_redirects=max_redirects,
                            )
                            final_url = affiliate_rewriter.unwrap_known_query_redirects(final_url) or final_url
                        except Exception:
                            pass
        except Exception:
            pass

        return (final_url or "").strip()

    def _simple_forward_mappings(self) -> Dict[str, Any]:
        try:
            v = (self.config or {}).get("simple_forward_mappings") or {}
        except Exception:
            v = {}
        return v if isinstance(v, dict) else {}

    def _simple_forward_mapping_for_channel(self, src_channel_id: int) -> Optional[Dict[str, Any]]:
        m = self._simple_forward_mappings()
        raw = m.get(str(int(src_channel_id))) or m.get(int(src_channel_id)) or None
        return raw if isinstance(raw, dict) else None

    def _simple_extract_shop_link(self, text: str) -> str:
        s = (text or "").strip()
        if not s:
            return ""
        # Prefer explicit "Shop->URL" style.
        m = re.search(r"(?i)\bshop\s*[-:>]+\s*(https?://\S+)", s)
        if m:
            return str(m.group(1) or "").strip().rstrip(").,")
        # Fallback: line that contains "shop" and a URL.
        for line in s.splitlines():
            if "shop" not in line.lower():
                continue
            spans = affiliate_rewriter.extract_urls_with_spans(line)
            if spans:
                return str(spans[0][0] or "").strip()
        return ""

    def _simple_message_block(self, message: discord.Message) -> str:
        lines: List[str] = []
        content = (message.content or "").strip()
        if content:
            lines.append(content)

        # Add attachments as raw CDN URLs so Discord renders visual embeds.
        try:
            for att in (message.attachments or []):
                u = str(getattr(att, "url", "") or "").strip()
                if u:
                    lines.append(u)
        except Exception:
            pass

        block = "\n".join([x for x in lines if str(x).strip()]).strip()
        return block

    async def _simple_flush_buffer(self, buf: _SimpleForwardBuffer) -> None:
        if not buf or not buf.dest_channel_id:
            return

        ch = self.bot.get_channel(int(buf.dest_channel_id))
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(int(buf.dest_channel_id))
            except Exception:
                ch = None
        if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            return

        parts = [p.strip() for p in (buf.parts or []) if str(p).strip()]
        if not parts:
            return

        out = "\n\n".join(parts).strip()
        if buf.shop_url_original and buf.shop_url_expanded:
            try:
                if buf.shop_url_original in out:
                    out = out.replace(buf.shop_url_original, buf.shop_url_expanded)
            except Exception:
                pass

        # Discord message limit: keep under 2000 chars.
        if len(out) > 1950:
            out = out[:1940] + "…"

        try:
            await ch.send(content=out, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            pass

    def _simple_cancel_flush_task(self, src_channel_id: int) -> None:
        t = self._simple_forward_flush_tasks.pop(int(src_channel_id), None)
        if t and not t.done():
            try:
                t.cancel()
            except Exception:
                pass

    def _simple_schedule_flush(self, *, src_channel_id: int, merge_window_s: float, expected_seq: int) -> None:
        self._simple_cancel_flush_task(src_channel_id)

        async def _runner() -> None:
            try:
                await asyncio.sleep(max(0.1, float(merge_window_s)))
            except Exception:
                return

            to_flush: Optional[_SimpleForwardBuffer] = None
            async with self._simple_forward_lock:
                cur = self._simple_forward_buffers.get(int(src_channel_id))
                if not cur:
                    return
                # Only flush if still the same buffer sequence (no newer message appended).
                if int(cur.seq) != int(expected_seq):
                    return
                to_flush = self._simple_forward_buffers.pop(int(src_channel_id), None)
            if to_flush:
                await self._simple_flush_buffer(to_flush)

        self._simple_forward_flush_tasks[int(src_channel_id)] = asyncio.create_task(_runner())

    async def _maybe_simple_forward(self, message: discord.Message) -> bool:
        """
        Config-gated simple forwarding:
        - merges consecutive messages from the same author (within merge_window_s)
        - expands "Shop->" link to original store URL when it is an interstitial (e.g., Mavely)
        - forwards as a single plain message (raw URLs) so Discord renders link/image embeds
        """
        try:
            src_channel_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        except Exception:
            src_channel_id = 0
        if not src_channel_id:
            return False

        mapping = self._simple_forward_mapping_for_channel(src_channel_id)
        if not mapping:
            return False

        dest_channel_id = _safe_int(mapping.get("dest_channel_id"))
        if not dest_channel_id:
            return True

        try:
            merge_window_s = float(mapping.get("merge_window_s") or 0)
        except Exception:
            merge_window_s = 0.0
        if merge_window_s <= 0:
            merge_window_s = 3.0

        if not message.guild:
            return True

        now = time.time()
        msg_author_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        guild_id = int(getattr(getattr(message, "guild", None), "id", 0) or 0)

        # Build this message block
        block = self._simple_message_block(message)
        if not block:
            # Still keep buffering for attachments-only messages; _simple_message_block already handles.
            return True

        # Extract shop URL candidate (first seen wins)
        shop_url = self._simple_extract_shop_link(message.content or "")

        # Expand Shop link if needed (best-effort). Only do this once per buffer.
        expanded_shop_url = ""
        try:
            import aiohttp

            timeout_s = float((mapping.get("expand_timeout_s") or _cfg_float(self.config, "amazon_expand_timeout_s", "AMAZON_EXPAND_TIMEOUT_S") or 8.0))
            max_redirects = int((mapping.get("expand_max_redirects") or _cfg_int(self.config, "amazon_expand_max_redirects", "AMAZON_EXPAND_MAX_REDIRECTS") or 8))
            async with aiohttp.ClientSession() as session:
                if shop_url:
                    expanded_shop_url = await self._expand_url_best_effort(session, shop_url, timeout_s=timeout_s, max_redirects=max_redirects)
        except Exception:
            expanded_shop_url = ""

        to_flush: Optional[_SimpleForwardBuffer] = None
        async with self._simple_forward_lock:
            cur = self._simple_forward_buffers.get(src_channel_id)
            if cur and (cur.guild_id == guild_id) and (cur.author_id == msg_author_id) and ((now - float(cur.last_ts)) <= float(merge_window_s)):
                cur.parts.append(block)
                cur.last_ts = now
                cur.seq += 1
                if (not cur.shop_url_original) and shop_url:
                    cur.shop_url_original = shop_url
                if (not cur.shop_url_expanded) and expanded_shop_url:
                    cur.shop_url_expanded = expanded_shop_url
                self._simple_schedule_flush(src_channel_id=src_channel_id, merge_window_s=merge_window_s, expected_seq=cur.seq)
                return True

            # Different author (or stale) => flush old buffer and start a new one
            if cur:
                self._simple_cancel_flush_task(src_channel_id)
                to_flush = self._simple_forward_buffers.pop(src_channel_id, None)

            buf = _SimpleForwardBuffer(
                src_channel_id=src_channel_id,
                dest_channel_id=int(dest_channel_id),
                guild_id=guild_id,
                author_id=msg_author_id,
                parts=[block],
                shop_url_original=shop_url or "",
                shop_url_expanded=expanded_shop_url or "",
                last_ts=now,
                seq=1,
            )
            self._simple_forward_buffers[src_channel_id] = buf
            self._simple_schedule_flush(src_channel_id=src_channel_id, merge_window_s=merge_window_s, expected_seq=buf.seq)

        if to_flush:
            await self._simple_flush_buffer(to_flush)
        return True

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

                    try:
                        final_url = await self._expand_url_best_effort(session, url_used, timeout_s=timeout_s, max_redirects=max_redirects)
                    except Exception:
                        final_url = affiliate_rewriter.unwrap_known_query_redirects(url_used) or url_used

                    if not affiliate_rewriter.is_amazon_like_url(final_url):
                        # Try deal-hub pages (dmflip/pricedoffers/etc) to locate the embedded Amazon /dp/ASIN link.
                        try:
                            hub, hub_err = await self._scrape_deal_hub_for_amazon_assets(final_url)
                        except Exception:
                            hub, hub_err = None, None
                        if hub and not hub_err:
                            hub_amz = str(hub.get("amazon_url") or "").strip()
                            if hub_amz and affiliate_rewriter.is_amazon_like_url(hub_amz):
                                final_url = hub_amz
                            else:
                                # If we at least got an ASIN, build canonical URL.
                                hub_asin = str(hub.get("asin") or "").strip().upper()
                                if hub_asin:
                                    mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
                                    final_url = f"{mp}/dp/{hub_asin}" if mp else f"https://www.amazon.com/dp/{hub_asin}"
                        if not affiliate_rewriter.is_amazon_like_url(final_url):
                            continue

                    # Only treat it as a valid Amazon lead if we have a real ASIN from a product URL.
                    # (Avoids false positives like a 10-char keyword from /s?k=... search URLs.)
                    asin = affiliate_rewriter.extract_asin(final_url) or affiliate_rewriter.extract_asin(url_used) or ""
                    if not asin:
                        continue
                    return AmazonDetection(asin=asin, url_used=url_used, final_url=final_url)
        except Exception:
            return None

        return None

    # -----------------------
    # Analyze + send
    # -----------------------
    async def _analyze_message(self, message: discord.Message) -> Tuple[Optional[discord.Embed], Dict[str, Any]]:
        content_len, embeds_n, comp_rows = self._message_shape(message)
        _log_flow("SCAN", content_len=content_len, embeds=embeds_n, components=comp_rows)

        urls = self._collect_message_urls(message)
        _log_flow("URLS", count=len(urls), sample=" | ".join(urls[:3]))

        det = await self._detect_amazon(urls)
        if not det:
            # If we saw Amazon URLs but none contained a real ASIN, call that out.
            saw_amazon = False
            try:
                saw_amazon = any(affiliate_rewriter.is_amazon_like_url(u) for u in (urls or []))
            except Exception:
                saw_amazon = False
            if saw_amazon:
                _log_flow("AMZ_DETECT", found="0", reason="no_valid_asin")
            else:
                _log_flow("AMZ_DETECT", found="0")
            return None, {"urls": urls, "amazon": None}

        _log_flow("AMZ_DETECT", found="1", asin=(det.asin or ""), url_used=det.url_used)

        asin = (det.asin or "").strip().upper()
        final_url = (det.final_url or "").strip()

        if asin:
            mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
            if mp:
                final_url = f"{mp}/dp/{asin}"
            elif not final_url:
                final_url = f"https://www.amazon.com/dp/{asin}"

        # Optional affiliate tagging (disabled when you want raw canonical URLs only).
        if self._amazon_affiliate_enabled():
            try:
                affiliate_final = affiliate_rewriter.build_amazon_affiliate_url(self.config or {}, final_url)
            except Exception:
                affiliate_final = None
            if affiliate_final:
                final_url = affiliate_final

        # De-dupe by ASIN to avoid forwarding the same lead repeatedly.
        dedupe_reserved = False
        if asin:
            if not self._dedupe_reserve(asin):
                _log_flow("DUP_ASIN_SKIP", asin=asin)
                return None, {"urls": urls, "amazon": {"asin": asin, "final_url": final_url, "url_used": det.url_used}, "dup_asin": asin, "dedupe_reserved": False}
            dedupe_reserved = True

        # Reconstruct listing details from the source message (no PA-API required).
        guessed = self._guess_listing_from_message(message, asin=asin, final_url=final_url)

        scrape_attempted = False
        scrape_ok = False
        scrape_err: Optional[str] = None
        scraped: Optional[Dict[str, str]] = None

        # Web-page scrape enrichment (best-effort).
        # This is best-effort and will gracefully fail (Amazon may present robot checks).
        if self._scrape_enabled() and final_url:
            scrape_attempted = True
            scraped, scrape_err = await self._scrape_amazon_page(final_url)
            if scraped and not scrape_err:
                scrape_ok = True
                _log_flow(
                    "SCRAPE_OK",
                    asin=(asin or ""),
                    has_title=bool(scraped.get("title")),
                    has_price=bool(scraped.get("price")),
                    has_before=bool(scraped.get("before_price")),
                    has_image=bool(scraped.get("image_url")),
                    has_discount=bool((scraped.get("discount_notes") or "").strip()),
                )
            else:
                _log_flow("SCRAPE_FAIL", asin=(asin or ""), err=(scrape_err or "unknown"))
                # If Amazon blocks scraping, try to extract an Amazon-hosted image from the deal hub URL.
                try:
                    if det and det.url_used and (not affiliate_rewriter.is_amazon_like_url(det.url_used)):
                        hub, hub_err = await self._scrape_deal_hub_for_amazon_assets(det.url_used)
                    else:
                        hub, hub_err = None, None
                except Exception as e:
                    hub, hub_err = None, str(e)[:200]
                if hub and not hub_err:
                    # Merge hub-derived title/image only when Amazon scrape failed.
                    scraped = {**(scraped or {}), **{k: v for k, v in (hub or {}).items() if v}}
                    _log_flow("HUB_OK", host=str(urlparse(det.url_used).netloc or ""), has_title=bool(hub.get("title")), has_image=bool(hub.get("image_url")))
                elif hub_err:
                    _log_flow("HUB_FAIL", err=str(hub_err)[:180])
        else:
            _log_flow("SCRAPE_SKIP", asin=(asin or ""), reason=("disabled" if not self._scrape_enabled() else "no_url"))

        # Skip out-of-stock / unavailable items.
        try:
            avail = str((scraped or {}).get("availability") or "").strip().lower()
        except Exception:
            avail = ""
        if avail in {"oos", "out_of_stock", "unavailable"}:
            _log_flow("SKIP_OOS", asin=(asin or ""), url=final_url)
            if dedupe_reserved and asin:
                self._dedupe_release(asin)
                dedupe_reserved = False
            return None, {"urls": urls, "amazon": {"asin": asin, "final_url": final_url, "url_used": det.url_used}, "skip_reason": "oos"}

        # Final fields:
        # - Title: prefer Amazon product name when available, else message reconstruction.
        # - Price/Before: prefer message-derived values; if missing, fill from scrape when available.
        # - Image: prefer Amazon page image; fall back to Amazon-hosted image already in the source embed.
        raw_title = str((scraped or {}).get("title") or "").strip() or guessed.get("title", "").strip() or "Amazon lead"
        title = self._clean_product_title(raw_title) or raw_title or "Amazon lead"
        price = guessed.get("price", "").strip()
        before_price = guessed.get("before_price", "").strip()

        price_src = "message" if price else "none"
        before_src = "message" if before_price else "none"

        # Fill missing prices from scrape.
        if not price:
            sp = str((scraped or {}).get("price") or "").strip()
            spn = self._normalize_price_str(sp)
            if spn:
                price = spn
                price_src = "scrape"
        if not before_price:
            sb = str((scraped or {}).get("before_price") or "").strip()
            sbn = self._normalize_price_str(sb)
            if sbn:
                before_price = sbn
                before_src = "scrape"

        # Ensure the card always has both lines.
        if not price:
            price_src = "missing"
        if not before_price:
            before_src = "missing"
        # Category: detect from message text, and also from scraped title when available
        # (so grocery routing can work even if the source post is short).
        category = guessed.get("category", "").strip()
        if not category:
            try:
                combined = f"{guessed.get('title','')}\n{raw_title}\n{(scraped or {}).get('department','')}\n{final_url}"
            except Exception:
                combined = ""
            kws = (self.config or {}).get("amazon_grocery_keywords") or []
            kws_l = [str(k).strip().lower() for k in kws] if isinstance(kws, list) else []
            combined_l = combined.lower()
            if any(k and (k in combined_l) for k in kws_l):
                category = "grocery"
        image_url = str((scraped or {}).get("image_url") or "").strip()
        if not image_url:
            # If Amazon blocks scraping, fall back to an Amazon-hosted image already present in the source embed.
            # (Still "Amazon website" image, but avoids blank cards.)
            image_url = guessed.get("image_url", "").strip()
        try:
            img_src = "scrape" if str((scraped or {}).get("image_url") or "").strip() else ("embed" if guessed.get("image_url", "").strip() else "none")
        except Exception:
            img_src = "none"
        _log_flow("IMAGE", source=img_src, has=("1" if bool(image_url) else "0"))

        # Percent off (only when both prices are known and Before > Current).
        discount_pct_str = ""
        try:
            cur_val, cur_sym = self._price_to_float(price)
            bef_val, bef_sym = self._price_to_float(before_price)
        except Exception:
            cur_val, cur_sym = None, ""
            bef_val, bef_sym = None, ""

        # Sanity: "Before" should never be lower than "Current".
        # If it is, try to swap in a better scraped before_price; otherwise SKIP (not a deal).
        if (cur_val is not None) and (bef_val is not None) and (bef_val > 0) and (cur_val > 0) and (bef_val < cur_val):
            sb_raw = str((scraped or {}).get("before_price") or "").strip()
            sb = self._normalize_price_str(sb_raw) if sb_raw else ""
            try:
                sb_val, sb_sym = self._price_to_float(sb)
            except Exception:
                sb_val, sb_sym = None, ""
            if sb and (sb_val is not None) and (sb_val > cur_val) and ((not cur_sym) or (not sb_sym) or (cur_sym == sb_sym)):
                before_price = sb
                before_src = "scrape"
                bef_val, bef_sym = sb_val, sb_sym
                _log_flow("BEFORE_FIXUP", action="swap_to_scrape", before=str(before_price), current=str(price))
            else:
                _log_flow("SKIP_NOT_DEAL", reason="current_gt_before", before=str(before_price), current=str(price))
                if dedupe_reserved and asin:
                    self._dedupe_release(asin)
                    dedupe_reserved = False
                return None, {"urls": urls, "amazon": {"asin": asin, "final_url": final_url, "url_used": det.url_used}, "skip_reason": "current_gt_before"}

        _log_flow("PRICES", current_src=price_src, before_src=before_src, has_current=("1" if bool(price) else "0"), has_before=("1" if bool(before_price) else "0"))
        if (cur_val is not None) and (bef_val is not None) and bef_val > 0 and cur_val < bef_val:
            # Only show when symbols match (or one is missing).
            if (not cur_sym) or (not bef_sym) or (cur_sym == bef_sym):
                pct = int(round(((bef_val - cur_val) / bef_val) * 100.0))
                if pct > 0:
                    discount_pct_str = f"{pct}% OFF"
                    _log_flow("PCT_OFF", pct=str(pct), current=str(price), before=str(before_price))

        # Deal notes (coupon/sub&save/limited time deal) from scrape, rendered in your preferred wording.
        raw_discount = str((scraped or {}).get("discount_notes") or "").strip()
        discount_lines: List[str] = []
        if raw_discount:
            parts = [p.strip() for p in raw_discount.split(";") if p.strip()]
            for p in parts[:4]:
                low = p.lower()
                if ("limited time deal" in low) or ("prime exclusive deal" in low):
                    discount_lines.append("Limited time deal ONLY!")
                    continue
                if low.startswith("coupon:"):
                    amt = p.split(":", 1)[-1].strip()
                    if amt:
                        discount_lines.append(f"Coupon Available — clip it before checkout! (save {amt})")
                    else:
                        discount_lines.append("Coupon Available — clip it before checkout!")
                    continue
                if "coupon" in low:
                    discount_lines.append("Coupon Available — clip it before checkout!")
                    continue
                if ("subscribe" in low) and ("save" in low):
                    m = re.search(r"(\\d{1,2})\\s*%", p)
                    pct = m.group(1) if m else ""
                    if pct:
                        discount_lines.append(f"Subscribe & Save — get up to {pct}% off (cancel after item arrives)")
                    else:
                        discount_lines.append("Subscribe & Save — discount may apply (cancel after item arrives)")
                    continue
                discount_lines.append(p)

            # Deduplicate, preserve order, cap lines.
            seen_dl: set[str] = set()
            deduped: List[str] = []
            for ln in discount_lines:
                k = ln.strip().lower()
                if k and k not in seen_dl:
                    deduped.append(ln.strip())
                    seen_dl.add(k)
            discount_lines = deduped[:3]

        if discount_lines:
            _log_flow("DISCOUNT", notes=" | ".join(discount_lines)[:180])

        # Build RS-style blocks from message-derived info.
        promo_codes = (guessed.get("promo_codes") or "").strip()
        steps_raw = (guessed.get("steps_raw") or "").strip()
        info_raw = (guessed.get("source_excerpt") or "").strip()

        # Normalize into numbered steps, but aggressively drop noisy / redundant lines:
        # - "Buy via Amazon --> <link>" (we show ONE key link at the bottom)
        # - bare URL-only lines
        steps_block = ""
        if steps_raw:
            lines = [ln.strip() for ln in steps_raw.splitlines() if ln.strip()]
            kept: List[str] = []
            for ln in lines:
                low = ln.lower().strip()
                # Drop the common redundant "buy via amazon" line.
                if "buy via amazon" in low:
                    continue
                # Drop url-only lines.
                if re.fullmatch(r"(https?://\S+|amzn\.to/\S+|www\.\S+)", low):
                    continue
                kept.append(ln)

            numbered: List[str] = []
            for i, ln in enumerate(kept, start=1):
                if re.match(r"^\d+\s*[-:)]", ln):
                    numbered.append(ln)
                elif ln.lower().startswith("step"):
                    numbered.append(ln)
                else:
                    numbered.append(f"{i}) {ln}")

            # Only show "How it works" when we have at least 2 meaningful steps.
            if len(numbered) >= 2:
                steps_block = "\n".join(numbered)

        # Optional URL rewrite inside steps (OFF by default; when off, we keep raw links).
        steps_block_rewritten = steps_block
        if steps_block and self._amazon_rewrite_step_links_enabled():
            try:
                steps_block_rewritten, changed, notes = await affiliate_rewriter.rewrite_text(self.config or {}, steps_block)
                if changed:
                    _log_flow("STEPS_URLS", changed="1", notes=str(len(notes or {})))
                else:
                    _log_flow("STEPS_URLS", changed="0")
            except Exception as e:
                _log_flow("STEPS_URLS_FAIL", err=str(e)[:180])
        elif steps_block:
            _log_flow("STEPS_URLS_SKIP", reason="disabled")

        # Optional OpenAI rephrase (only if key present/enabled and source text exists)
        if steps_block_rewritten:
            steps_rephrased = await self._openai_rephrase("steps", steps_block_rewritten)
            openai_steps_mode = self._openai_last_mode.get("steps", "")
        else:
            steps_rephrased = ""
            openai_steps_mode = "skip_no_steps"

        # IMPORTANT: we currently do not display info text in the RS card output,
        # so do NOT spend OpenAI usage rewriting it.
        info_rephrased = info_raw
        openai_info_mode = "skip_unused"

        # Optional combined block (kept for compatibility with older templates).
        promo_steps_parts: List[str] = []
        if promo_codes:
            promo_steps_parts.append(f"CODE: {promo_codes}")
        if steps_rephrased:
            promo_steps_parts.append(steps_rephrased)
        promo_steps = "\n".join([p for p in promo_steps_parts if p]).strip()

        key_link = self._key_link(final_url, message_id=str(getattr(message, "id", "") or ""), asin=asin)
        deal_type = (guessed.get("deal_type") or "").strip() or "Amazon Lead"
        source_credit = (guessed.get("source_credit") or "").strip()

        # Build the "card body" to match your desired format (no footer/timestamp, one key link).
        card_lines: List[str] = []
        card_lines.append(f"Current Price: **{price}**" if price else "Current Price:")
        card_lines.append(f"Before: **{before_price}**" if before_price else "Before:")
        if discount_pct_str:
            card_lines.append(f"Discount: **{discount_pct_str}**")
        if discount_lines:
            card_lines.append("")
            card_lines.extend([str(x) for x in discount_lines if str(x).strip()])
        if promo_codes:
            card_lines.append(f"CODE: {promo_codes}")
        if steps_rephrased:
            card_lines.append("")
            card_lines.append("How it works:")
            card_lines.append(steps_rephrased)
        if key_link:
            card_lines.append("")
            card_lines.append(key_link)
        card_body = "\n".join(card_lines).strip()

        # Routing: allow per-source overrides, otherwise personal vs grocery based on detected category.
        try:
            src_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        except Exception:
            src_id = 0
        dest_id, route_reason = self._pick_dest_channel_id(source_channel_id=(src_id or None), category=category, enrich_failed=False)
        _log_flow("ROUTE", dest_id=(dest_id or ""), reason=route_reason)

        tpl, tpl_key = self._pick_template(dest_id, enrich_failed=False)
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
            "key_link": key_link,
            "deal_type": deal_type,
            "title": title,
            "price": price,
            "before_price": before_price,
            "discount_pct": discount_pct_str,
            "category": category,
            "image_url": image_url,
            "discount_notes": "\n".join([str(x) for x in (discount_lines or []) if str(x).strip()]).strip(),
            "source_line": source_line,
            "source_jump": str(getattr(message, "jump_url", "") or ""),
            "source_message_id": str(getattr(message, "id", "") or ""),
            "source_author": guessed.get("source_author", ""),
            "source_excerpt": guessed.get("source_excerpt", ""),
            "source_credit": source_credit,
            "scrape_attempted": "yes" if scrape_attempted else "no",
            "scrape_ok": "yes" if scrape_ok else "no",
            "scrape_err": (scrape_err or ""),
            "promo_codes": promo_codes,
            "steps_raw": steps_raw,
            "steps_rephrased": steps_rephrased,
            "info_raw": info_raw,
            "info_rephrased": info_rephrased,
            "promo_steps": promo_steps,
            "card_body": card_body,
            "openai_steps_mode": openai_steps_mode,
            "openai_info_mode": openai_info_mode,
        }

        embed = _embed_from_template(tpl or {}, ctx) if tpl else None
        if embed:
            _log_flow("RENDER", ok="1", title=(embed.title or "")[:80], fields=len(embed.fields))
        else:
            _log_flow("RENDER", ok="0")
            if dedupe_reserved and asin:
                self._dedupe_release(asin)

        meta = {
            "urls": urls,
            "amazon": {"asin": asin, "final_url": final_url, "url_used": det.url_used},
            "enrich_failed": False,
            "enrich_err": None,
            "dest_channel_id": dest_id,
            "route_reason": route_reason,
            "template_key": tpl_key,
            "ctx": ctx,
            "dedupe_reserved": bool(dedupe_reserved),
        }
        return embed, meta

    async def _maybe_forward_message(self, message: discord.Message) -> None:
        if self.bot.user and message.author and message.author.id == self.bot.user.id:
            return

        sources = set(self._source_channel_ids())
        if not sources:
            return
        if int(message.channel.id) not in sources:
            return

        if int(message.channel.id) in set(self._output_channel_ids()):
            return

        # NEW: config-gated simple forwarding (does not affect existing Amazon mappings unless enabled per-channel)
        try:
            if await self._maybe_simple_forward(message):
                return
        except Exception:
            # Never let the optional simple-forward path break existing behavior.
            pass

        embed, meta = await self._analyze_message(message)
        if not embed:
            return

        try:
            asin = str(((meta.get("amazon") or {}) if isinstance(meta.get("amazon"), dict) else {}).get("asin") or "").strip().upper()
        except Exception:
            asin = ""
        reserved = bool(meta.get("dedupe_reserved"))

        dest_id = meta.get("dest_channel_id")
        if not dest_id:
            _log_flow("SEND_FAIL", reason="dest_channel_missing")
            if reserved and asin:
                self._dedupe_release(asin)
            return

        ch = self.bot.get_channel(int(dest_id))
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(int(dest_id))
            except Exception:
                ch = None
        if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
            _log_flow("SEND_FAIL", reason="dest_channel_not_text")
            if reserved and asin:
                self._dedupe_release(asin)
            return

        try:
            await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            _log_flow("SEND_OK", dest_id=dest_id, message_id=str(message.id))
            if reserved and asin:
                self._dedupe_commit(asin)
        except Exception as e:
            _log_flow("SEND_FAIL", dest_id=dest_id, err=str(e)[:200])
            if reserved and asin:
                self._dedupe_release(asin)

    # -----------------------
    # Discord events/commands
    # -----------------------
    def _setup_events(self) -> None:
        @self.bot.event
        async def on_ready() -> None:
            guild_id = _cfg_int(self.config, "guild_id", "GUILD_ID")
            log.info("Bot ready user=%s guild_id=%s config=%s secrets=%s", self.bot.user, guild_id, self.config_path, self.secrets_path)
            log.info("bot_token=%s", mask_secret(self.config.get("bot_token")))

            if guild_id:
                try:
                    await asyncio.sleep(1)
                    synced = await self.bot.tree.sync(guild=discord.Object(id=guild_id))
                    log.info("Synced slash commands to guild=%s count=%s", guild_id, len(synced))
                except Exception as e:
                    log.warning("Slash command sync failed guild=%s err=%s", guild_id, str(e)[:200])

            try:
                await self._startup_smoketest()
            except Exception:
                log.exception("Startup smoke test crashed (continuing)")

            # Optional run-once mode (used for scripted checks / CI / assistant-run tests).
            exit_after = (os.getenv("INSTORE_EXIT_AFTER_STARTUP", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
            if exit_after:
                log.info("[BOOT] exit-after-startup enabled; closing bot.")
                try:
                    await self.bot.close()
                except Exception:
                    pass

        @self.bot.event
        async def on_message(message: discord.Message) -> None:
            try:
                await self.bot.process_commands(message)
            except Exception:
                pass
            await self._maybe_forward_message(message)

    def _setup_slash_commands(self) -> None:
        # ---- /testallmessage ----
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
            lines.append("**Scan**")
            lines.append(f"- channel: <#{msg.channel.id}>")
            lines.append(f"- message_id: `{msg.id}`")
            lines.append(f"- content_len: `{content_len}` | embeds: `{embeds_n}` | components: `{comp_rows}`")
            lines.append(f"- urls_found: `{len(urls)}`")
            if urls:
                lines.append(f"- url_sample: `{(' | '.join(urls[:3]))[:180]}`")
            lines.append("**Amazon**")
            if amz:
                lines.append(f"- asin: `{(amz.get('asin') or '')}`")
                lines.append(f"- final_url: `{(amz.get('final_url') or '')[:200]}`")
            else:
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

        # ---- /embedbuild ----
        embedbuild = app_commands.Group(name="embedbuild", description="Manage amazon embed templates (admin only)")

        def _route_choices() -> List[app_commands.Choice[str]]:
            return [app_commands.Choice(name=r, value=r) for r in self._TEMPLATE_ROUTES]

        class TemplateEditModal(discord.ui.Modal):
            def __init__(self, *, route: str, current_json: str):
                super().__init__(title=f"Edit template: {route}")
                self._route = route
                # Discord modal limits are strict; keep within 4000.
                if len(current_json) > 4000:
                    current_json = current_json[:3997] + "..."
                self.template_json = discord.ui.TextInput(
                    label="Template JSON (object)",
                    style=discord.TextStyle.paragraph,
                    required=True,
                    max_length=4000,
                    default=current_json,
                )
                self.add_item(self.template_json)

            async def on_submit(self, interaction: discord.Interaction) -> None:
                if not self._route:
                    await interaction.response.send_message("❌ Missing route.", ephemeral=True)
                    return
                raw = str(self.template_json.value or "").strip()
                try:
                    obj = json.loads(raw) if raw else {}
                except Exception as e:
                    await interaction.response.send_message(f"❌ Invalid JSON: {str(e)[:200]}", ephemeral=True)
                    return
                if not isinstance(obj, dict):
                    await interaction.response.send_message("❌ Template must be a JSON object (top-level {}).", ephemeral=True)
                    return
                ok, msg = await self_outer._template_set(self._route, obj)  # type: ignore[name-defined]
                if ok:
                    await interaction.response.send_message(f"✅ Saved `{self._route}` template to `config.json`.", ephemeral=True)
                else:
                    await interaction.response.send_message(f"❌ Failed to save: {msg}", ephemeral=True)

        # Hack to reference outer self inside nested Modal class cleanly
        self_outer = self

        @embedbuild.command(name="list", description="List current template routes + keys (admin only)")
        async def embedbuild_list(interaction: discord.Interaction) -> None:
            if not self._require_admin(interaction):
                await interaction.response.send_message("❌ Admin only (run in a server).", ephemeral=True)
                return

            lines: List[str] = []
            lines.append("**Embed templates**")
            for r in self._TEMPLATE_ROUTES:
                tpl, key = self._template_get_current(r)
                present = bool(tpl)
                lines.append(f"- `{r}`: {'✅' if present else '❌'} (`{key}`)")

            out = "\n".join(lines)
            if len(out) > 1900:
                out = out[:1890] + "…"
            await interaction.response.send_message(out, ephemeral=True)

        @embedbuild.command(name="edit", description="Edit a template JSON via modal (admin only)")
        @app_commands.describe(route="Which route template to edit")
        @app_commands.choices(route=_route_choices())
        async def embedbuild_edit(interaction: discord.Interaction, route: app_commands.Choice[str]) -> None:
            if not self._require_admin(interaction):
                await interaction.response.send_message("❌ Admin only (run in a server).", ephemeral=True)
                return

            r = str(route.value or "").strip().lower()
            tpl, _key = self._template_get_current(r)
            current_json = json.dumps(tpl or {}, indent=2, ensure_ascii=True)
            await interaction.response.send_modal(TemplateEditModal(route=r, current_json=current_json))

        @embedbuild.command(name="preview", description="Preview a template render (ephemeral)")
        @app_commands.describe(route="Which route template to preview", asin="Optional ASIN for enrichment/placeholder context")
        @app_commands.choices(route=_route_choices())
        async def embedbuild_preview(
            interaction: discord.Interaction,
            route: app_commands.Choice[str],
            asin: Optional[str] = None,
        ) -> None:
            if not self._require_admin(interaction):
                await interaction.response.send_message("❌ Admin only (run in a server).", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True, thinking=True)
            r = str(route.value or "").strip().lower()
            asin2 = (asin or "").strip().upper()
            if not asin2:
                asin2 = (_cfg_str(self.config, "startup_test_asin", "STARTUP_TEST_ASIN") or "B0FLMLDTPB").strip().upper()

            mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
            final_url = f"{mp}/dp/{asin2}" if mp else f"https://www.amazon.com/dp/{asin2}"

            # Reconstructed defaults (no PA-API required)
            title_guess = f"Amazon lead {asin2}".strip() if asin2 else "Amazon lead"
            product: Dict[str, Any] = {}
            ctx = {
                "asin": asin2,
                "final_url": final_url,
                "link": f"<{final_url}>",
                "title": title_guess,
                "price": "",
                "category": "",
                "image_url": "",
                "source_line": "EMBED_PREVIEW",
                "source_jump": "",
                "source_message_id": "",
                "promo_steps": "",
                "info_rephrased": "",
                "key_link": f"[amzn.to/{self._stable_key_slug(final_url)}](<{final_url}>)" if final_url else "",
                "deal_type": "Preview",
                "source_credit": "",
            }

            if r == "enrich_failed":
                tpl, tpl_key = self._pick_template(None, enrich_failed=True)
            elif r == "default":
                tpl, tpl_key = self._pick_template(None, enrich_failed=False)
            else:
                dest_id = self._route_to_dest_id(r)
                tpl, tpl_key = self._pick_template(dest_id, enrich_failed=False)

            embed = _embed_from_template(tpl or {}, ctx) if tpl else None
            header = f"- route: `{r}`\n- template_key: `{tpl_key}`\n- asin: `{asin2}`"

            if embed:
                await interaction.followup.send(content=header, embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            else:
                await interaction.followup.send(content=header + "\n\n❌ No template found to preview.", ephemeral=True)

        self.bot.tree.add_command(embedbuild)

    async def start(self) -> None:
        token = str((self.config.get("bot_token") or "")).strip()
        await self.bot.start(token)


def main() -> int:
    # CLI flags (minimal; avoids adding deps)
    argv = [a.strip() for a in (sys.argv[1:] or []) if (a or "").strip()]
    if ("--exit-after-startup" in argv) or ("--run-once" in argv):
        os.environ["INSTORE_EXIT_AFTER_STARTUP"] = "1"
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

