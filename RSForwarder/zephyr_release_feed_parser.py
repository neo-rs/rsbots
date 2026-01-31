from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class ZephyrReleaseFeedItem:
    sku: str
    store: str
    source_tag: str


_HEADER_RE = re.compile(r"release\s+feed\(s\)", re.IGNORECASE)
_SKU_LINE_RE = re.compile(r"^\s*(\d+)\.\s*([+-]{1,2})\s*(.+?)(?:\s*\||$)")
_MONITOR_RE = re.compile(r"\b([a-z0-9]+(?:[-_][a-z0-9]+)*)-monitor\b", re.IGNORECASE)
_BRACKET_RE = re.compile(r"\[([^\]]+)\]")


def looks_like_release_feed_embed_text(text: str) -> bool:
    return bool(_HEADER_RE.search(text or ""))


def _norm_token(s: str) -> str:
    return (s or "").strip().lower().replace("_", "-")


def tag_to_store(tag: str) -> Optional[str]:
    """
    Map a Zephyr tag/monitor token (e.g. "gamestop-monitor" or "us-mint") to the STORE value expected by the sheet formulas.
    """
    m = _norm_token(tag)
    base = m[:-len("-monitor")] if m.endswith("-monitor") else m

    direct = {
        "amazon": "Amazon",
        "walmart": "Walmart",
        "target": "Target",
        "homedepot": "Homedepot",
        "gamestop": "Gamestop",
        "costco": "Costco",
        "bestbuy": "Bestbuy",
        "topps": "Topps",
        "hotopic": "Hotopic",
        "mattel": "Mattel",
        "shopify": "Shopify",
        "us-mint": "US Mint",
        # Some deployments use these variants:
        "barnes": "Barnes and Nobles",
        "barnesandnobles": "Barnes and Nobles",
        "barnes-nobles": "Barnes and Nobles",
        "samsclub": "Sam's Club",
        "sam-s-club": "Sam's Club",
    }

    store = direct.get(base)
    if store:
        return store

    # Only best-effort for known monitor-ish tags. Otherwise return None (so we can skip unsupported tags).
    if base.endswith("-monitor"):
        return base.replace("-", " ").title()
    return None


def _extract_bracket_tag(line: str) -> Optional[str]:
    """
    Parse tag names from lines like:
      [🤖┃gamestop-monitor]
      [us-mint]
      [🤖┃pokemon-center]
    """
    m = _BRACKET_RE.search(line or "")
    if not m:
        return None
    inner = (m.group(1) or "").strip()
    if not inner:
        return None
    # Often has emoji + box drawing separator "┃" (U+2503)
    if "┃" in inner:
        inner = inner.split("┃")[-1].strip()
    # Sometimes has pipe
    if "|" in inner:
        inner = inner.split("|")[-1].strip()
    return inner or None


def _clean_sku_token(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # Drop surrounding backticks or quotes
    s = s.strip("`").strip().strip('"').strip("'").strip()
    # Drop leading + if present (some feeds show ++123)
    while s.startswith("+"):
        s = s[1:].strip()
    return s


def _iter_lines(text: str) -> Iterable[str]:
    for raw in (text or "").splitlines():
        s = (raw or "").strip()
        if s:
            yield s


def parse_release_feed_items(text: str) -> List[ZephyrReleaseFeedItem]:
    """
    Parse Zephyr "Release Feed(s) in this server:" embed text into (sku, store, monitor).

    Expected shape (example):
      Release Feed(s) in this server:
      1. +20023800 | 💶┃full-send-🤖
      [🤖┃gamestop-monitor]
    """
    items: List[ZephyrReleaseFeedItem] = []
    pending_sku: Optional[str] = None
    pending_sign: Optional[str] = None

    for line in _iter_lines(text):
        m_sku = _SKU_LINE_RE.match(line)
        if m_sku:
            sign = (m_sku.group(2) or "").strip()
            token = _clean_sku_token(m_sku.group(3) or "")
            # Ignore removals (e.g. "-15558409905")
            if sign.startswith("-"):
                pending_sku = None
                pending_sign = None
                continue

            pending_sku = token
            pending_sign = sign
            if not pending_sku:
                pending_sku = None
                pending_sign = None
                continue

            # Sometimes the tag/monitor is present on the same line.
            tag_inline = _extract_bracket_tag(line)
            if not tag_inline:
                m_mon_inline = _MONITOR_RE.search(line)
                if m_mon_inline:
                    tag_inline = f"{m_mon_inline.group(1)}-monitor"
            if tag_inline:
                store = tag_to_store(tag_inline)
                if store:
                    items.append(ZephyrReleaseFeedItem(sku=pending_sku, store=store, source_tag=_norm_token(tag_inline)))
                pending_sku = None
                pending_sign = None
            continue

        # Monitor usually comes on the next line.
        if pending_sku:
            tag = _extract_bracket_tag(line)
            if not tag:
                m_mon = _MONITOR_RE.search(line)
                if m_mon:
                    tag = f"{m_mon.group(1)}-monitor"
            if tag:
                store = tag_to_store(tag)
                if store:
                    items.append(ZephyrReleaseFeedItem(sku=pending_sku, store=store, source_tag=_norm_token(tag)))
                pending_sku = None
                pending_sign = None

    return items


def parse_release_feed_pairs(text: str) -> List[Tuple[str, str]]:
    """Convenience: returns [(store, sku), ...]."""
    out: List[Tuple[str, str]] = []
    for it in parse_release_feed_items(text):
        out.append((it.store, it.sku))
    return out

