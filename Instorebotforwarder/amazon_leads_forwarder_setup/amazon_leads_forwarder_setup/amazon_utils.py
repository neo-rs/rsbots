from __future__ import annotations

import os
import re
from typing import Optional, List

# -----------------------------
# Patterns (shared, import-safe)
# -----------------------------

AMAZON_DOMAINS = (
    "amazon.com",
    "amzn.to",
    "a.co",
    "smile.amazon.com",
    "amazon.ca",
    "amazon.co.uk",
    "amazon.de",
    "amazon.fr",
    "amazon.it",
    "amazon.es",
    "amazon.co.jp",
)

ASIN_RE = re.compile(r"\b([A-Z0-9]{10})\b")
AMAZON_URL_RE = re.compile(
    r"(https?://(?:www\.)?(?:%s)[^\s<>()]+)" % "|".join(re.escape(d) for d in AMAZON_DOMAINS),
    re.IGNORECASE,
)
DMFLIP_URL_RE = re.compile(r"(https?://(?:www\.)?dmflip\.com/[^\s<>()]+)", re.IGNORECASE)


def strip_discord_markdown_link(text: str) -> str:
    # [label](https://url) -> https://url
    return re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", r"\2", text or "")


def find_amazon_url(text: str) -> Optional[str]:
    if not text:
        return None
    t = strip_discord_markdown_link(text)
    m = AMAZON_URL_RE.search(t)
    return m.group(1) if m else None


def find_dmflip_urls(text: str) -> List[str]:
    if not text:
        return []
    t = strip_discord_markdown_link(text)
    return DMFLIP_URL_RE.findall(t)


def extract_asin(text_or_url: str) -> Optional[str]:
    if not text_or_url:
        return None
    m = re.search(r"/dp/([A-Z0-9]{10})", text_or_url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"/gp/product/([A-Z0-9]{10})", text_or_url, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = ASIN_RE.search(text_or_url.upper())
    return m.group(1).upper() if m else None


def canonicalize_amazon_url(url: str) -> str:
    if not url:
        return url
    url = url.strip().rstrip(").,]")
    asin = extract_asin(url)
    if asin:
        marketplace = os.getenv("AMAZON_API_MARKETPLACE", "https://www.amazon.com").rstrip("/")
        return f"{marketplace}/dp/{asin}"
    return url

