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


class InstorebotForwarder:
    _TEMPLATE_ROUTES = ("personal", "grocery", "deals", "default", "enrich_failed")

    def __init__(self) -> None:
        _setup_logging()
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        self.config = cfg
        self.config_path = config_path
        self.secrets_path = secrets_path
        self._cfg_lock = asyncio.Lock()
        self._openai_cache: Dict[str, str] = {}

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
            return ""
        if not self._openai_enabled():
            return raw
        key = self._openai_api_key()
        if not key:
            return raw

        # Cache by kind+hash to avoid repeat costs within a run.
        cache_key = f"{kind}:{hashlib.sha256(raw.encode('utf-8', errors='ignore')).hexdigest()[:16]}"
        if cache_key in self._openai_cache:
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

            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            timeout_s = self._openai_timeout_s()
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
                async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers) as resp:
                    txt = await resp.text(errors="replace")
                    if int(resp.status) >= 400:
                        return raw
                    data = json.loads(txt) if txt else {}
                    out = ""
                    try:
                        out = (((data or {}).get("choices") or [])[0] or {}).get("message", {}).get("content", "") or ""
                    except Exception:
                        out = ""
                    out = str(out).strip()
                    if not out:
                        return raw
                    # Safety: prevent pings
                    out = self._neutralize_mentions(out)
                    self._openai_cache[cache_key] = out
                    return out
        except Exception:
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
        Render a stable masked display link like:
        [amzn.to/8fdaf3b](<https://www.amazon.com/dp/ASIN?...>)
        """
        u = (final_url or "").strip()
        if not u:
            return ""
        seed = f"{asin}|{message_id}|{u}"
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
                    out.append(ln)
                    continue
                if any(k in low for k in ("clip", "select", "apply", "checkout", "subscribe", "buy via", "click here", "use code")):
                    out.append(ln)
                    continue
            else:
                # Non-link steps sometimes: "Step 3: Use code X"
                if low.startswith("step") or any(k in low for k in ("use code", "apply code", "clip coupon", "sub & save", "subscribe & save")):
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
            return "Promo Glitch"
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

        log.info("-------- Startup Smoke Test --------")
        log.info("[BOOT][SMOKE] target_count=%s scan_limit_per_channel=%s output_channel_id=%s", count, scan_limit, out_id or "")

        sources = self._source_channel_ids()
        if not sources:
            log.warning("[BOOT][SMOKE] No source channels configured. Set Instorebotforwarder/config.json -> source_channel_ids.")
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
                        log.info("[BOOT][SMOKE] SEND_OK %s/%s msg=%s", sent, count, getattr(msg, "id", ""))
                    except Exception as e:
                        log.info("[BOOT][SMOKE] SEND_FAIL msg=%s err=%s", getattr(msg, "id", ""), str(e)[:200])
                        sent -= 1
                        continue

                    if delay_s > 0:
                        await asyncio.sleep(delay_s)
            except Exception as e:
                log.info("[BOOT][SMOKE] Failed scanning channel id=%s err=%s", src_id, str(e)[:200])

        if sent < count:
            log.warning("[BOOT][SMOKE] Completed with only %s/%s messages found that contain usable Amazon links.", sent, count)

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
        if ("robot check" in low) or ("enter the characters you see below" in low) or ("sorry, we just need to make sure" in low):
            return None, "blocked (robot check)"

        title = self._extract_html_meta(html_txt, prop="og:title") or ""
        image_url = self._extract_html_meta(html_txt, prop="og:image") or ""

        price = ""
        # JSON-LD offers.price is the cleanest when present
        prod = self._extract_jsonld_product(html_txt)
        if prod:
            try:
                if not title:
                    title = str(prod.get("name") or "").strip()
                if not image_url:
                    img = prod.get("image")
                    if isinstance(img, list) and img:
                        image_url = str(img[0] or "").strip()
                    elif isinstance(img, str):
                        image_url = img.strip()
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

        # Fallback price patterns (very best-effort)
        if not price:
            m = re.search(r'["\']price["\']\s*:\s*["\'](\$?\d{1,4}(?:,\d{3})*(?:\.\d{2})?)["\']', html_txt, re.IGNORECASE)
            if m:
                price = (m.group(1) or "").strip()
        if not price:
            m2 = re.search(r'(?<!\w)(\$)\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?', html_txt)
            if m2:
                price = (m2.group(0) or "").strip()

        out = {
            "title": " ".join((title or "").split()).strip(),
            "image_url": (image_url or "").strip(),
            "price": " ".join((price or "").split()).strip(),
        }
        # Ensure at least one field is useful
        if not (out.get("title") or out.get("image_url") or out.get("price")):
            return None, "no useful fields found"
        return out, None

    def _extract_price_guess(self, text: str) -> str:
        s = (text or "")
        if not s:
            return ""
        # Basic price patterns seen in deal posts.
        m = re.search(r"(?<!\w)(\$|£|€)\s?\d{1,4}(?:,\d{3})*(?:\.\d{2})?", s)
        if m:
            return (m.group(0) or "").strip()
        m2 = re.search(r"(?<!\w)\d{1,4}(?:\.\d{2})?\s?(USD|CAD|AUD|GBP|EUR)\b", s, re.IGNORECASE)
        if m2:
            return (m2.group(0) or "").strip()
        return ""

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

        # Title: prefer embed title, else first non-empty line of content without URLs.
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

        if not title:
            title = f"Amazon lead {asin}".strip() if asin else "Amazon lead"

        price = self._extract_price_guess(joined)
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
                _log_flow("SCRAPE_OK", asin=(asin or ""), has_title=bool(scraped.get("title")), has_price=bool(scraped.get("price")), has_image=bool(scraped.get("image_url")))
            else:
                _log_flow("SCRAPE_FAIL", asin=(asin or ""), err=(scrape_err or "unknown"))
        else:
            _log_flow("SCRAPE_SKIP", asin=(asin or ""), reason=("disabled" if not self._scrape_enabled() else "no_url"))

        # Final fields: prefer scrape values when present, else use reconstructed values.
        title = str((scraped or {}).get("title") or "").strip() or guessed.get("title", "").strip() or "Amazon lead"
        price = str((scraped or {}).get("price") or "").strip() or guessed.get("price", "").strip()
        category = guessed.get("category", "").strip()
        image_url = str((scraped or {}).get("image_url") or "").strip() or guessed.get("image_url", "").strip()

        # Build RS-style blocks from message-derived info.
        promo_codes = (guessed.get("promo_codes") or "").strip()
        steps_raw = (guessed.get("steps_raw") or "").strip()
        info_raw = (guessed.get("source_excerpt") or "").strip()

        promo_block = f"Code: `{promo_codes}`" if promo_codes else ""
        steps_block = ""
        if steps_raw:
            # Normalize into numbered steps if not already.
            lines = [ln.strip() for ln in steps_raw.splitlines() if ln.strip()]
            numbered = []
            for i, ln in enumerate(lines, start=1):
                if re.match(r"^\d+\s*[-:)]", ln):
                    numbered.append(ln)
                elif ln.lower().startswith("step"):
                    numbered.append(ln)
                else:
                    numbered.append(f"{i}) {ln}")
            steps_block = "\n".join(numbered)

        # Optional OpenAI rephrase (only if key present/enabled and source text exists)
        steps_rephrased = await self._openai_rephrase("steps", steps_block) if steps_block else ""
        info_rephrased = await self._openai_rephrase("info", info_raw) if info_raw else ""

        promo_steps_parts = []
        if promo_block:
            promo_steps_parts.append(promo_block)
        if steps_rephrased:
            promo_steps_parts.append(steps_rephrased)
        promo_steps = "\n".join([p for p in promo_steps_parts if p]).strip()

        key_link = self._key_link(final_url, message_id=str(getattr(message, "id", "") or ""), asin=asin)
        deal_type = (guessed.get("deal_type") or "").strip() or "Amazon Lead"
        source_credit = (guessed.get("source_credit") or "").strip()

        # Routing is based on message-derived category only.
        dest_id, route_reason = self._pick_dest_channel_id(category=category, enrich_failed=False)
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
            "category": category,
            "image_url": image_url,
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
        }

        embed = _embed_from_template(tpl or {}, ctx) if tpl else None
        if embed:
            _log_flow("RENDER", ok="1", title=(embed.title or "")[:80], fields=len(embed.fields))
        else:
            _log_flow("RENDER", ok="0")

        meta = {
            "urls": urls,
            "amazon": {"asin": asin, "final_url": final_url, "url_used": det.url_used},
            "enrich_failed": False,
            "enrich_err": None,
            "dest_channel_id": dest_id,
            "route_reason": route_reason,
            "template_key": tpl_key,
            "ctx": ctx,
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

            product = None
            err = None
            enrich_attempted = False
            enrich_failed = False
            enrich_ok = False
            if asin2 and (self._paapi_ok is True):
                enrich_attempted = True
                product, err = await _amazon_enrich_by_asin(self.config, asin2)
                enrich_failed = bool(err or (not product))
                enrich_ok = bool(product) and (not err)

            mp = _cfg_str(self.config, "amazon_api_marketplace", "AMAZON_API_MARKETPLACE").rstrip("/")
            final_url = f"{mp}/dp/{asin2}" if mp else f"https://www.amazon.com/dp/{asin2}"

            # Reconstructed defaults (no PA-API required)
            title_guess = f"Amazon lead {asin2}".strip() if asin2 else "Amazon lead"
            ctx = {
                "asin": asin2,
                "final_url": final_url,
                "link": f"<{final_url}>",
                "title": str((product or {}).get("title") or title_guess).strip(),
                "price": str((product or {}).get("price") or "").strip(),
                "category": str((product or {}).get("category") or "").strip(),
                "image_url": str((product or {}).get("image_url") or "").strip(),
                "source_line": "EMBED_PREVIEW",
                "source_jump": "",
                "source_message_id": "",
                "paapi_ok": "yes" if (self._paapi_ok is True) else ("no" if (self._paapi_ok is False) else ""),
                "enrich_attempted": "yes" if enrich_attempted else "no",
                "enrich_ok": "yes" if enrich_ok else "no",
                "enrich_failed": "yes" if enrich_failed else "no",
                "enrich_err": (str(err) if err else ""),
            }

            if r == "enrich_failed":
                tpl, tpl_key = self._pick_template(None, enrich_failed=True)
            elif r == "default":
                tpl, tpl_key = self._pick_template(None, enrich_failed=False)
            else:
                dest_id = self._route_to_dest_id(r)
                tpl, tpl_key = self._pick_template(dest_id, enrich_failed=False)

            embed = _embed_from_template(tpl or {}, ctx) if tpl else None
            header = (
                f"- route: `{r}`\n"
                f"- template_key: `{tpl_key}`\n"
                f"- asin: `{asin2}`\n"
                f"- paapi_ok: `{ctx.get('paapi_ok')}`\n"
                f"- enrich_attempted: `{ctx.get('enrich_attempted')}`\n"
                f"- enrich_failed: `{ctx.get('enrich_failed')}`"
            )
            if ctx.get("enrich_err"):
                header += f"\n- enrich_err: `{str(ctx.get('enrich_err'))[:160]}`"

            if embed:
                await interaction.followup.send(content=header, embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            else:
                await interaction.followup.send(content=header + "\n\n❌ No template found to preview.", ephemeral=True)

        self.bot.tree.add_command(embedbuild)

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

