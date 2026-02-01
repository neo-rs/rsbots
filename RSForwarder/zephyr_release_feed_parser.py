from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class ZephyrReleaseFeedItem:
    release_id: int
    sku: str
    store: str
    source_tag: str
    # Extra metadata for robust downstream behavior (Current List tab, reporting, etc.)
    monitor_tag: str = ""
    category: str = ""
    channel_id: str = ""
    raw_text: str = ""
    is_sku_candidate: bool = True


_HEADER_RE = re.compile(r"release\s+feed\(s\)", re.IGNORECASE)
_SKU_LINE_RE = re.compile(r"^\s*(\d+)\.\s*([+-]{1,2})\s*(.+?)(?:\s*\||$)")
_MONITOR_RE = re.compile(r"\b([a-z0-9]+(?:[-_][a-z0-9]+)*)-monitor\b", re.IGNORECASE)
_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
_ITEM_START_INLINE_RE = re.compile(r"(?:^|\s)(\d{1,4})\s*\.\s*([+-]{1,2})\s*([^|\[]+?)(?=\s*(?:\||\[))")
_MD_BOLD_RE = re.compile(r"\*\*")
_CHANNEL_ID_RE = re.compile(r"\bchannel\s*id\s*:\s*(\d{10,})\b", re.IGNORECASE)


def looks_like_release_feed_embed_text(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if _HEADER_RE.search(t):
        return True
    low = t.lower()
    # Zephyr often splits the list across multiple messages/embeds.
    # Continuation parts may:
    # - omit the "Release Feed(s)" header
    # - place the `[*-monitor]` tag on a separate message
    # - include numbered release lines without an inline `-monitor` token (tag appears on next line/message)
    if "zephyr companion bot" in low:
        # Tag-only continuation message (no numbers).
        if "-monitor" in low:
            return True
        # Numbered list lines (e.g. "61. +2025 topps chrome ... | Channel ID: ...")
        if re.search(r"\b\d{1,4}\s*\.\s*[+-]", low):
            return True
    # Also accept any text that clearly contains monitor tags (even if author string differs).
    if "-monitor" in low and (_BRACKET_RE.search(t) or _MONITOR_RE.search(t)):
        return True
    return False


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
        "lowes": "Lowes",
        "homedepot": "Homedepot",
        "gamestop": "Gamestop",
        "costco": "Costco",
        "bestbuy": "Bestbuy",
        "topps": "Topps",
        "hotopic": "Hotopic",
        "mattel": "Mattel",
        "shopify": "Shopify",
        "us-mint": "US Mint",
        "funkopop": "Funkopop",
        "funko": "Funkopop",
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


def _strip_markdown(s: str) -> str:
    t = (s or "").replace("\n", " ").replace("\r", " ").strip()
    if not t:
        return ""
    # Minimal markdown normalization (Discord Companion bolds numbers and pipes).
    t = _MD_BOLD_RE.sub("", t)
    # Normalize special separators into plain spaces.
    t = t.replace("\u2503", "|")  # box drawing vertical bar
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _is_sku_candidate(token: str) -> bool:
    """
    Heuristic: detect whether the "+token" looks like a real SKU/ID (vs a product title label).
    """
    t = (token or "").strip()
    if not t:
        return False
    if any(c.isspace() for c in t):
        return False
    # Pure digits (most stores)
    if t.isdigit():
        return len(t) >= 5
    # Alpha-numeric ids like ASIN / handles (no spaces)
    cleaned = "".join([c for c in t if c.isalnum()])
    if len(cleaned) < 4:
        return False
    # Reject heavy punctuation (likely title-ish)
    allowed = set("-_")
    if any((not c.isalnum()) and (c not in allowed) for c in t):
        return False
    return True


def parse_release_feed_records(text: str) -> List[ZephyrReleaseFeedItem]:
    """
    Parse the merged Zephyr /listreleases output into one record per release_id.
    Includes non-SKU items (e.g. Topps titles with Channel ID lines).
    """
    records: List[ZephyrReleaseFeedItem] = []

    cur_rid: Optional[int] = None
    cur_sign: str = ""
    cur_token: str = ""
    cur_lines: List[str] = []

    def _finalize() -> None:
        nonlocal cur_rid, cur_sign, cur_token, cur_lines
        if not cur_rid or not cur_token:
            cur_rid, cur_sign, cur_token, cur_lines = None, "", "", []
            return

        # Ignore removals (sign starts with "-")
        if cur_sign.strip().startswith("-"):
            cur_rid, cur_sign, cur_token, cur_lines = None, "", "", []
            return

        monitor_tag = ""
        category = ""
        channel_id = ""
        raw_seg = "\n".join(cur_lines).strip()

        for ln in cur_lines:
            m_c = _CHANNEL_ID_RE.search(ln or "")
            if m_c and not channel_id:
                channel_id = (m_c.group(1) or "").strip()

            bt = _extract_bracket_tag(ln)
            if bt:
                if "-monitor" in bt.lower():
                    monitor_tag = bt
                else:
                    category = bt

        if not monitor_tag:
            m_mon = _MONITOR_RE.search(raw_seg)
            if m_mon:
                monitor_tag = f"{m_mon.group(1)}-monitor"

        store = tag_to_store(monitor_tag) if monitor_tag else None
        store_s = store or ""
        is_sku = _is_sku_candidate(cur_token)

        records.append(
            ZephyrReleaseFeedItem(
                release_id=int(cur_rid or 0),
                sku=cur_token,
                store=store_s,
                source_tag=_norm_token(monitor_tag) if monitor_tag else "",
                monitor_tag=_norm_token(monitor_tag) if monitor_tag else "",
                category=(category or "").strip(),
                channel_id=(channel_id or "").strip(),
                raw_text=raw_seg,
                is_sku_candidate=bool(is_sku),
            )
        )
        cur_rid, cur_sign, cur_token, cur_lines = None, "", "", []

    # Line-based segmentation first (preferred; preserves next-line monitor tags).
    saw_any = False
    for line in _iter_lines(text):
        m_sku = _SKU_LINE_RE.match(line)
        if m_sku:
            saw_any = True
            _finalize()
            try:
                cur_rid = int(str(m_sku.group(1) or "0").strip() or "0")
            except Exception:
                cur_rid = None
            cur_sign = (m_sku.group(2) or "").strip()
            cur_token = _clean_sku_token(m_sku.group(3) or "")
            cur_lines = [line]
            continue
        if cur_rid is not None:
            cur_lines.append(line)

    if saw_any:
        _finalize()
        return records

    # Inline fallback for Discord Companion formatting (everything in one line).
    t = _strip_markdown(text)
    if not t:
        return []
    starts = list(_ITEM_START_INLINE_RE.finditer(t))
    if not starts:
        return []

    for i, m in enumerate(starts):
        try:
            rid = int(str(m.group(1) or "0").strip() or "0")
        except Exception:
            rid = 0
        sign = (m.group(2) or "").strip()
        token = _clean_sku_token(m.group(3) or "")
        if not token or rid <= 0:
            continue
        if sign.startswith("-"):
            continue
        seg_start = int(m.start())
        seg_end = int(starts[i + 1].start()) if i + 1 < len(starts) else len(t)
        seg = t[seg_start:seg_end]
        tag = _extract_bracket_tag(seg)
        if not tag:
            m_mon = _MONITOR_RE.search(seg)
            if m_mon:
                tag = f"{m_mon.group(1)}-monitor"
        store = tag_to_store(tag or "") if tag else None
        m_c = _CHANNEL_ID_RE.search(seg or "")
        ch_id = (m_c.group(1) or "").strip() if m_c else ""
        records.append(
            ZephyrReleaseFeedItem(
                release_id=int(rid),
                sku=token,
                store=str(store or ""),
                source_tag=_norm_token(tag) if tag else "",
                monitor_tag=_norm_token(tag) if tag else "",
                category="",
                channel_id=ch_id,
                raw_text=seg.strip(),
                is_sku_candidate=_is_sku_candidate(token),
            )
        )

    return records


def parse_release_feed_items(text: str) -> List[ZephyrReleaseFeedItem]:
    """
    Parse Zephyr "Release Feed(s) in this server:" embed text into (sku, store, monitor).

    Expected shape (example):
      Release Feed(s) in this server:
      1. +20023800 | 💶┃full-send-🤖
      [🤖┃gamestop-monitor]
    """
    # Backward-compatible behavior: return only rows suitable for sheet writes
    # (must have a store mapping and a token that looks like an ID/SKU).
    out: List[ZephyrReleaseFeedItem] = []
    for r in parse_release_feed_records(text):
        if not (r.store and r.is_sku_candidate):
            continue
        out.append(r)
    return out


def parse_release_feed_pairs(text: str) -> List[Tuple[str, str]]:
    """Convenience: returns [(store, sku), ...]."""
    out: List[Tuple[str, str]] = []
    for it in parse_release_feed_items(text):
        out.append((it.store, it.sku))
    return out

