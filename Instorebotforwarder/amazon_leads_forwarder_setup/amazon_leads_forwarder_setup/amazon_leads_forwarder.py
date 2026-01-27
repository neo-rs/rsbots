#!/usr/bin/env python3
"""amazon_leads_forwarder.py

Standalone Amazon-leads forwarder + enrichment + (optional) OpenAI normalization.

What it does
- Watches configured source channels.
- Extracts Amazon URL/ASIN from message content AND embeds.
- If only dmflip link is present, fetches dmflip page and extracts the real Amazon link.
- Enriches with Amazon API (via a simple CUSTOM endpoint you control).
- Uses OpenAI only when needed (optional) and caches results to keep usage low.
- Dedupe by ASIN cooldown to prevent spam.

Notes on “smart but low-usage”
- First pass uses deterministic parsing (regex + rules) for deal type, prices, promo codes, etc.
- OpenAI is gated and cached. If you don’t set OPENAI_API_KEY, it never runs.

Requirements
- python 3.10+
- discord.py 2.x
- aiohttp
- openai (optional)

Env
- DISCORD_BOT_TOKEN (required)
- OPENAI_API_KEY (optional)
- AMAZON_CUSTOM_ENDPOINT (optional but recommended): https://your-api/amazon?asin={asin}

Config
- config_amazon_leads.json (same folder)
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import aiohttp
import discord
from discord.ext import commands

# -----------------------------
# Patterns
# -----------------------------

from amazon_utils import (
    AMAZON_URL_RE,
    canonicalize_amazon_url,
    extract_asin,
    find_amazon_url,
    find_dmflip_urls,
)

CODE_RE = re.compile(r"\b(?:code|promo)\s*[:\-]?\s*([A-Z0-9]{4,20})\b", re.IGNORECASE)

NON_AMAZON_HINTS = (
    "chipotle", "bogo", "in-store", "instore", "meijer", "walmart", "target", "costco",
    "event info", "no link",
)

DEAL_TYPE_RULES = [
    ("WAREHOUSE", re.compile(r"\bwarehouse\b|\blike-new\b|\bused\b", re.IGNORECASE)),
    ("COUPON_CODE", re.compile(r"\bcode\b|\bpromo\b|\bcoupon\b|\bclip\b", re.IGNORECASE)),
    ("STACKED_PROMO", re.compile(r"\bstack\b|\bstacked\b|\bextra\s*\d+%|\bsubscribe\b", re.IGNORECASE)),
    ("GLITCH", re.compile(r"\bglitch\b|\bprice\s*error\b|\bmisprice\b|\bymmv\b", re.IGNORECASE)),
    ("PRE_ORDER", re.compile(r"\bpre[-\s]?order\b", re.IGNORECASE)),
]

DEFAULT_DEAL_TYPE = "STANDARD"

# -----------------------------
# Config
# -----------------------------

@dataclass
class ChannelRoute:
    source_channel_id: int
    dest_channel_id: int

@dataclass
class BotConfig:
    guild_allowlist: Optional[List[int]]
    routes: List[ChannelRoute]
    role_ping_id: Optional[int]
    allow_everyone_ping: bool

    dedupe_hours: int
    same_asin_cooldown_minutes: int

    openai_enabled: bool
    openai_only_when_missing_fields: bool

    include_source_link: bool
    include_dmflip_link: bool
    include_amazon_link_in_content: bool

    block_non_amazon_hints: bool


def load_config(path: str) -> BotConfig:
    if not os.path.exists(path):
        return BotConfig(
            guild_allowlist=None,
            routes=[],
            role_ping_id=None,
            allow_everyone_ping=False,
            dedupe_hours=24,
            same_asin_cooldown_minutes=180,
            openai_enabled=bool(os.getenv("OPENAI_API_KEY")),
            openai_only_when_missing_fields=True,
            include_source_link=False,
            include_dmflip_link=False,
            include_amazon_link_in_content=True,
            block_non_amazon_hints=True,
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    routes = [ChannelRoute(int(r["source"]), int(r["dest"])) for r in raw.get("routes", [])]
    return BotConfig(
        guild_allowlist=[int(x) for x in raw.get("guild_allowlist", [])] or None,
        routes=routes,
        role_ping_id=int(raw["role_ping_id"]) if raw.get("role_ping_id") else None,
        allow_everyone_ping=bool(raw.get("allow_everyone_ping", False)),
        dedupe_hours=int(raw.get("dedupe_hours", 24)),
        same_asin_cooldown_minutes=int(raw.get("same_asin_cooldown_minutes", 180)),
        openai_enabled=bool(raw.get("openai_enabled", bool(os.getenv("OPENAI_API_KEY")))),
        openai_only_when_missing_fields=bool(raw.get("openai_only_when_missing_fields", True)),
        include_source_link=bool(raw.get("include_source_link", False)),
        include_dmflip_link=bool(raw.get("include_dmflip_link", False)),
        include_amazon_link_in_content=bool(raw.get("include_amazon_link_in_content", True)),
        block_non_amazon_hints=bool(raw.get("block_non_amazon_hints", True)),
    )

# -----------------------------
# Logging
# -----------------------------

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("amazon_leads_forwarder")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(os.path.join("logs", "amazon_leads_forwarder.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = setup_logger()

# -----------------------------
# SQLite State
# -----------------------------

class StateDB:
    def __init__(self, path: str = "amazon_leads_state.sqlite"):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init()

    def _init(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seen (
          key TEXT PRIMARY KEY,
          first_seen_ts INTEGER,
          last_seen_ts INTEGER,
          meta TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS openai_cache (
          cache_key TEXT PRIMARY KEY,
          created_ts INTEGER,
          value_json TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS usage (
          day TEXT PRIMARY KEY,
          openai_calls INTEGER,
          prompt_tokens INTEGER,
          completion_tokens INTEGER,
          total_tokens INTEGER
        );
        """)
        self.conn.commit()

    def seen_recently(self, key: str, within_seconds: int) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT last_seen_ts FROM seen WHERE key=?", (key,))
        row = cur.fetchone()
        if not row:
            return False
        last_seen = int(row[0])
        return (int(time.time()) - last_seen) <= within_seconds

    def mark_seen(self, key: str, meta: Optional[dict] = None):
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("SELECT first_seen_ts FROM seen WHERE key=?", (key,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE seen SET last_seen_ts=?, meta=? WHERE key=?",
                        (now, json.dumps(meta or {}, ensure_ascii=False), key))
        else:
            cur.execute("INSERT INTO seen(key, first_seen_ts, last_seen_ts, meta) VALUES(?,?,?,?)",
                        (key, now, now, json.dumps(meta or {}, ensure_ascii=False)))
        self.conn.commit()

    def openai_cache_get(self, cache_key: str) -> Optional[dict]:
        cur = self.conn.cursor()
        cur.execute("SELECT value_json FROM openai_cache WHERE cache_key=?", (cache_key,))
        row = cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def openai_cache_put(self, cache_key: str, value: dict):
        cur = self.conn.cursor()
        cur.execute("""
        INSERT INTO openai_cache(cache_key, created_ts, value_json)
        VALUES(?,?,?)
        ON CONFLICT(cache_key) DO UPDATE SET created_ts=excluded.created_ts, value_json=excluded.value_json;
        """, (cache_key, int(time.time()), json.dumps(value, ensure_ascii=False)))
        self.conn.commit()

    def add_usage(self, prompt_tokens: int, completion_tokens: int):
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        total = prompt_tokens + completion_tokens
        cur = self.conn.cursor()
        cur.execute("SELECT openai_calls, prompt_tokens, completion_tokens, total_tokens FROM usage WHERE day=?", (day,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO usage(day, openai_calls, prompt_tokens, completion_tokens, total_tokens) VALUES(?,?,?,?,?)",
                (day, 1, prompt_tokens, completion_tokens, total),
            )
        else:
            calls, pt, ct, tt = row
            cur.execute(
                "UPDATE usage SET openai_calls=?, prompt_tokens=?, completion_tokens=?, total_tokens=? WHERE day=?",
                (calls + 1, pt + prompt_tokens, ct + completion_tokens, tt + total, day),
            )
        self.conn.commit()


db = StateDB()

# -----------------------------
# Text helpers
# -----------------------------

def parse_prices(text: str) -> Dict[str, Optional[float]]:
    out = {"sale_price": None, "retail_price": None, "market_price": None}
    if not text:
        return out

    t = text.replace(",", "")

    reg = re.search(r"\b(?:reg|was)\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)", t, re.IGNORECASE)
    if reg:
        out["retail_price"] = float(reg.group(1))

    now = re.search(r"\b(?:price|now)\s*[:\-]?\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)", t, re.IGNORECASE)
    if now:
        out["sale_price"] = float(now.group(1))
    else:
        m = re.search(r"\$\s*([0-9]+(?:\.[0-9]{1,2})?)", t)
        if m:
            out["sale_price"] = float(m.group(1))

    avg = re.search(r"\baverage\s*(?:30|60|90)?\s*[:\-]?\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)", t, re.IGNORECASE)
    if avg:
        out["market_price"] = float(avg.group(1))

    return out


def infer_deal_type(text: str) -> str:
    if not text:
        return DEFAULT_DEAL_TYPE
    for name, rx in DEAL_TYPE_RULES:
        if rx.search(text):
            return name
    return DEFAULT_DEAL_TYPE


def extract_conditions(text: str) -> str:
    if not text:
        return ""
    parts = []
    m = CODE_RE.search(text)
    if m:
        parts.append(f"Code: {m.group(1).upper()}")
    if re.search(r"\bclip\b.*\bcoupon\b|\bcoupon\b.*\bclip\b", text, re.IGNORECASE):
        parts.append("Clip coupon")
    if re.search(r"\bsubscribe\b", text, re.IGNORECASE):
        parts.append("Subscribe & Save (if required)")
    if re.search(r"\bymmv\b", text, re.IGNORECASE):
        parts.append("YMMV")
    # de-dupe
    parts = list(dict.fromkeys(parts))
    return " • ".join(parts)[:180]


def looks_non_amazon(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(h in t for h in NON_AMAZON_HINTS)

# -----------------------------
# dmflip -> amazon resolver
# -----------------------------

async def extract_amazon_from_dmflip(session: aiohttp.ClientSession, dmflip_url: str) -> Optional[str]:
    try:
        async with session.get(dmflip_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()
    except Exception:
        return None

    m = AMAZON_URL_RE.search(html)
    if not m:
        return None
    return canonicalize_amazon_url(m.group(1))

# -----------------------------
# Amazon enrichment (CUSTOM endpoint)
# -----------------------------

@dataclass
class AmazonProduct:
    asin: str
    title: Optional[str] = None
    image_url: Optional[str] = None
    price: Optional[str] = None
    category: Optional[str] = None
    categories: Optional[List[str]] = None


def _coerce_str_list(value: object) -> List[str]:
    if isinstance(value, list):
        out: List[str] = []
        for x in value:
            s = (str(x) if x is not None else "").strip()
            if s:
                out.append(s)
        return out
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        if ">" in s:
            parts = [p.strip() for p in s.split(">")]
            return [p for p in parts if p]
        return [s]
    return []


def _amazon_category_path(prod: AmazonProduct) -> str:
    cats = _coerce_str_list(getattr(prod, "categories", None))
    if cats:
        return " > ".join(cats)[:300]
    c = (getattr(prod, "category", None) or "").strip()
    return c[:300]


def _add_query_param(url: str, key: str, value: str) -> str:
    from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

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


def build_affiliate_url(amazon_url: str) -> str:
    """
    Build an already-affiliated Amazon URL (adds `tag=`) when AMAZON_ASSOCIATE_TAG is set.
    """
    u = (amazon_url or "").strip()
    tag = os.getenv("AMAZON_ASSOCIATE_TAG", "").strip()
    return _add_query_param(u, "tag", tag) if (u and tag) else u


async def fetch_amazon_product_custom(session: aiohttp.ClientSession, asin: str) -> AmazonProduct:
    endpoint = os.getenv("AMAZON_CUSTOM_ENDPOINT", "").strip()
    if not endpoint:
        return AmazonProduct(asin=asin)

    url = endpoint.format(asin=asin)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return AmazonProduct(asin=asin)
            data = await resp.json()
    except Exception:
        return AmazonProduct(asin=asin)

    categories = _coerce_str_list(data.get("categories"))
    cat = (str(data.get("category") or "").strip() or None)
    if (not cat) and categories:
        cat = categories[-1]

    return AmazonProduct(
        asin=asin,
        title=data.get("title"),
        image_url=data.get("image_url") or data.get("image"),
        price=data.get("price"),
        category=cat,
        categories=categories or None,
    )


async def fetch_amazon_product(session: aiohttp.ClientSession, asin: str) -> AmazonProduct:
    enabled = os.getenv("AMAZON_API_ENABLED", "0").strip() == "1"
    if not enabled:
        return AmazonProduct(asin=asin)

    mode = os.getenv("AMAZON_API_MODE", "CUSTOM").strip().upper()
    if mode == "CUSTOM":
        return await fetch_amazon_product_custom(session, asin)

    # Placeholder: PAAPI signing not included here.
    return AmazonProduct(asin=asin)

# -----------------------------
# OpenAI (optional + cached)
# -----------------------------

async def openai_normalize_if_needed(raw_text: str, inferred: dict) -> dict:
    """Return dict merged with inferred.

    Output keys:
      deal_type, title_clean, conditions, one_liner_note

    This call is cached by stable hash to minimize spend.
    """
    if not os.getenv("OPENAI_API_KEY"):
        return inferred

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
    max_tokens = int(os.getenv("OPENAI_MAX_TOKENS", "220"))
    temperature = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))

    base = json.dumps({"raw": raw_text[:1800], "known": inferred}, ensure_ascii=False, sort_keys=True)
    cache_key = hashlib.sha256(base.encode("utf-8")).hexdigest()
    cached = db.openai_cache_get(cache_key)
    if cached:
        return {**inferred, **cached}

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    except Exception:
        return inferred

    system = (
        "You format Amazon deal posts for Discord. "
        "Return ONLY valid JSON. No markdown. No extra keys. "
        "Never invent prices or resell values. "
        "one_liner_note must be 1-2 sentences, no questions, no emojis."
    )

    user_payload = {
        "known": {
            "deal_type": inferred.get("deal_type", ""),
            "title_guess": inferred.get("title_clean", ""),
            "conditions_guess": inferred.get("conditions", ""),
        },
        "deal_type_enum": ["WAREHOUSE", "GLITCH", "STACKED_PROMO", "COUPON_CODE", "PRE_ORDER", "STANDARD"],
        "constraints": {
            "title_clean_max_chars": 100,
            "conditions_max_chars": 160,
        },
        "input": raw_text[:1800],
    }

    schema_hint = '{"deal_type":"STANDARD","title_clean":"","conditions":"","one_liner_note":""}'

    try:
        resp = await client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": f"{schema_hint}\n\n{json.dumps(user_payload, ensure_ascii=False)}"},
            ],
        )
        content = (resp.choices[0].message.content or "").strip()
        data = json.loads(content)

        deal_type = data.get("deal_type", inferred.get("deal_type", DEFAULT_DEAL_TYPE))
        if deal_type not in {"WAREHOUSE", "GLITCH", "STACKED_PROMO", "COUPON_CODE", "PRE_ORDER", "STANDARD"}:
            deal_type = inferred.get("deal_type", DEFAULT_DEAL_TYPE)

        out = {
            "deal_type": deal_type,
            "title_clean": (data.get("title_clean") or inferred.get("title_clean") or "")[:100],
            "conditions": (data.get("conditions") or inferred.get("conditions") or "")[:160],
            "one_liner_note": (data.get("one_liner_note") or "")[:240],
        }

        usage = getattr(resp, "usage", None)
        if usage:
            db.add_usage(int(getattr(usage, "prompt_tokens", 0)), int(getattr(usage, "completion_tokens", 0)))

        db.openai_cache_put(cache_key, out)
        return {**inferred, **out}
    except Exception:
        return inferred

# -----------------------------
# Discord embed builder
# -----------------------------

def build_embed(
    *,
    title: str,
    url: str,
    asin: str,
    image_url: Optional[str],
    category_path: str,
    deal_type: str,
    prices: Dict[str, Optional[float]],
    api_price: Optional[str],
    conditions: str,
    one_liner_note: str,
    include_source: bool,
    source_url: Optional[str],
) -> discord.Embed:
    e = discord.Embed(
        title=title[:256],
        url=url,
        color=discord.Color.from_rgb(255, 153, 0),
    )

    desc_parts = []
    if one_liner_note:
        desc_parts.append(one_liner_note)
    if conditions:
        desc_parts.append(f"Conditions: {conditions}")

    e.description = "\n".join(desc_parts)[:4096] if desc_parts else None

    sale = prices.get("sale_price")
    retail = prices.get("retail_price")
    market = prices.get("market_price")

    if sale is not None:
        e.add_field(name="Amazon Price", value=f"${sale:,.2f}", inline=True)
    elif api_price:
        e.add_field(name="Amazon Price", value=str(api_price)[:80], inline=True)
    if retail is not None:
        e.add_field(name="Reg / Was", value=f"${retail:,.2f}", inline=True)
    if market is not None:
        e.add_field(name="Market Avg", value=f"${market:,.2f}", inline=True)

    e.add_field(name="Deal Type", value=deal_type, inline=True)
    if category_path:
        e.add_field(name="Category", value=category_path[:100], inline=True)
    if asin:
        e.add_field(name="ASIN", value=f"`{asin}`", inline=True)

    if image_url:
        e.set_image(url=image_url)

    if include_source and source_url:
        e.add_field(name="Source Link", value=source_url, inline=False)

    return e

# -----------------------------
# Bot
# -----------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)

CONFIG_PATH = os.getenv("AMAZON_LEADS_CONFIG", "config_amazon_leads.json")
cfg = load_config(CONFIG_PATH)

ROUTE_MAP: Dict[int, int] = {r.source_channel_id: r.dest_channel_id for r in cfg.routes}

@bot.event
async def on_ready():
    log.info(f"Logged in as {bot.user} (id={bot.user.id})")
    log.info(f"Routes: {len(ROUTE_MAP)}")
    log.info(f"OpenAI enabled: {cfg.openai_enabled and bool(os.getenv('OPENAI_API_KEY'))}")
    log.info("Ready")


def guild_allowed(guild_id: int) -> bool:
    return (cfg.guild_allowlist is None) or (guild_id in cfg.guild_allowlist)


def should_process_channel(channel_id: int) -> bool:
    return channel_id in ROUTE_MAP


def normalize_title_guess(raw_text: str, asin: str) -> str:
    # Prefer first non-empty line
    for line in (raw_text or "").splitlines():
        line = line.strip()
        if line:
            return line[:100]
    return f"Amazon Deal ({asin})"


def should_call_openai(inferred: dict, raw_text: str) -> bool:
    if not (cfg.openai_enabled and os.getenv("OPENAI_API_KEY")):
        return False

    if not cfg.openai_only_when_missing_fields:
        return True

    # Only spend when it adds value:
    # - coupon/code present but conditions empty
    # - glitch/stack cues but deal type looks too generic
    # - title guess is weak
    text_l = (raw_text or "").lower()
    if not inferred.get("title_clean"):
        return True
    if ("code" in text_l or "promo" in text_l or "coupon" in text_l) and not inferred.get("conditions"):
        return True
    if ("glitch" in text_l or "stack" in text_l) and inferred.get("deal_type") == DEFAULT_DEAL_TYPE:
        return True

    return False


def gather_text(message: discord.Message) -> str:
    parts = []
    if message.content:
        parts.append(message.content)
    for emb in message.embeds or []:
        if emb.title:
            parts.append(emb.title)
        if emb.description:
            parts.append(emb.description)
        if emb.url:
            parts.append(emb.url)
    return "\n".join(p for p in parts if p).strip()


async def process_message(message: discord.Message):
    if not message.guild:
        return
    if not guild_allowed(message.guild.id):
        return
    if not should_process_channel(message.channel.id):
        return

    raw_text = gather_text(message)
    if not raw_text:
        return

    if cfg.block_non_amazon_hints and looks_non_amazon(raw_text):
        return

    dest_channel_id = ROUTE_MAP.get(message.channel.id)
    dest_channel = message.guild.get_channel(dest_channel_id) if dest_channel_id else None
    if not isinstance(dest_channel, (discord.TextChannel, discord.Thread)):
        log.warning(f"Dest channel missing/invalid for source={message.channel.id}")
        return

    async with aiohttp.ClientSession() as session:
        amazon_url = find_amazon_url(raw_text)
        dmflip_urls = find_dmflip_urls(raw_text)

        dmflip_resolved = None
        if not amazon_url and dmflip_urls:
            dmflip_resolved = await extract_amazon_from_dmflip(session, dmflip_urls[0])
            amazon_url = dmflip_resolved

        if not amazon_url:
            return

        amazon_url = canonicalize_amazon_url(amazon_url)
        asin = extract_asin(amazon_url) or extract_asin(raw_text)
        if not asin:
            # fallback but still stable
            asin = hashlib.sha1(amazon_url.encode("utf-8")).hexdigest()[:10].upper()

        # Dedupe by ASIN cooldown
        cooldown_sec = cfg.same_asin_cooldown_minutes * 60
        if db.seen_recently(f"asin:{asin}", cooldown_sec):
            return

        prices = parse_prices(raw_text)
        deal_type = infer_deal_type(raw_text)
        conditions = extract_conditions(raw_text)

        # Enrich
        product = await fetch_amazon_product(session, asin)
        affiliate_url = build_affiliate_url(amazon_url)

        title_guess = product.title or normalize_title_guess(raw_text, asin)

        inferred = {
            "deal_type": deal_type,
            "title_clean": title_guess[:100],
            "conditions": conditions,
            "one_liner_note": "",
        }

        if should_call_openai(inferred, raw_text):
            inferred = await openai_normalize_if_needed(raw_text, inferred)

        final_title = (product.title or inferred.get("title_clean") or title_guess).strip()[:256]
        final_deal_type = inferred.get("deal_type") or deal_type
        final_conditions = inferred.get("conditions") or conditions
        one_liner_note = inferred.get("one_liner_note") or ""

        embed = build_embed(
            title=final_title,
            url=affiliate_url,
            asin=asin,
            image_url=product.image_url,
            category_path=_amazon_category_path(product),
            deal_type=final_deal_type,
            prices=prices,
            api_price=product.price,
            conditions=final_conditions,
            one_liner_note=one_liner_note,
            include_source=cfg.include_source_link,
            source_url=(message.jump_url if cfg.include_source_link else None),
        )

        # Content
        content_parts = []
        if cfg.role_ping_id:
            content_parts.append(f"<@&{cfg.role_ping_id}>")
        if cfg.include_amazon_link_in_content:
            content_parts.append(affiliate_url)
        if cfg.include_dmflip_link and dmflip_urls:
            content_parts.append(dmflip_urls[0])

        content = "\n".join(content_parts).strip() if content_parts else None

        allowed_mentions = discord.AllowedMentions(
            everyone=cfg.allow_everyone_ping,
            roles=bool(cfg.role_ping_id),
            users=False,
        )

        await dest_channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions)
        db.mark_seen(
            f"asin:{asin}",
            meta={"amazon_url": amazon_url, "src": message.channel.id, "dst": dest_channel.id},
        )
        log.info(f"Forwarded ASIN={asin} {message.channel.name} -> {dest_channel.name}")


@bot.event
async def on_message(message: discord.Message):
    if message.author and bot.user and message.author.id == bot.user.id:
        return
    try:
        await process_message(message)
    except Exception as e:
        log.exception(f"Unhandled error: {e}")

    await bot.process_commands(message)


def main():
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("DISCORD_BOT_TOKEN is required")

    log.info(f"Loaded config: {CONFIG_PATH} | routes={len(ROUTE_MAP)}")
    bot.run(token)


if __name__ == "__main__":
    main()
