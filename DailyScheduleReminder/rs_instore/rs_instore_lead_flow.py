#!/usr/bin/env python3
"""
Interactive RS In-Store flow:

1) Read a breakdown Discord message by jump link (ptb/discord.com both OK).
2) For each product row:
   - If no store URL in the breakdown row: send product id to Product-ID resolver channel, wait for URL.
   - Send store URL to Link-Value resolver channel, wait for title/price/image embed.
   - Build and post !m lead line to the command-post channel.
   - Wait for the destination lead post, capture the jump link.
3) Edit the original breakdown message, filling:
     Go here to check stock -> <jump link>

Config: DailyScheduleReminder/rs_instore/config.json
  channels.product_ids            (resolver channel)
  channels.product_links          (resolver channel)
  channels.command_post_channel_id
  channels.destination_channel_id
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_SCRIPT_DIR = Path(__file__).resolve().parent
_DSR_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _DSR_ROOT.parent

for _p in (_DSR_ROOT, _REPO_ROOT):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

import reminder_bot  # noqa: E402
from mirror_message_to_m_lead import (  # noqa: E402
    discord_get,
    discord_patch,
    discord_post,
    fetch_message_with_token_fallback,
    list_messages_after,
    parse_jump_url,
)

DISCORD_API = "https://discord.com/api/v10"
_URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)

# Supported store keys for !m lead flow (canonical list per user request).
_SUPPORTED_STORES: set[str] = {
    "bestbuy",
    "lowes",
    "target",
    "walmart",
    "bjs",
    "snipes",
    "acehardware",
    "samsclub",
    "gamestop",
    "homedepot",
    "sephora",
    "dollargeneral",
    "costco",
    "kohls",
}


class ResolverNoHit(RuntimeError):
    """Raised when a resolver explicitly reports no result."""



def _load_cfg() -> dict:
    p = _SCRIPT_DIR / "config.json"
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("rs_instore/config.json must be a JSON object.")
    return data


def _load_watch_cfg(cfg: dict) -> dict:
    w = cfg.get("watch")
    return w if isinstance(w, dict) else {}


def _cfg_int(cfg: dict, key: str, *, default: int, min_v: int, max_v: int) -> int:
    raw = cfg.get(key, default)
    try:
        v = int(raw)
    except Exception:
        v = int(default)
    return max(min_v, min(max_v, v))


_STATE_PATH = _SCRIPT_DIR / "_watch_state.json"


def _load_state() -> dict:
    if not _STATE_PATH.is_file():
        return {}
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8", errors="replace") or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        _STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass


def _cfg_channel_id(cfg: dict, key: str) -> str:
    ch = cfg.get("channels")
    if not isinstance(ch, dict):
        raise ValueError('config.json must have object "channels".')
    v = str(ch.get(key) or "").strip()
    if not v.isdigit():
        raise ValueError(f'config.json channels.{key} must be a numeric string channel id.')
    return v


def _get_me_id(token: str) -> str:
    r = discord_get(f"{DISCORD_API}/users/@me", token)
    if r.status_code != 200:
        raise RuntimeError(f"GET /users/@me failed HTTP {r.status_code}: {(r.text or '')[:200]}")
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("GET /users/@me returned non-object JSON.")
    return str(data.get("id") or "").strip()


def _first_url(s: str) -> str:
    text = str(s or "")
    # Common Discord markdown wrappers: <https://...> or (https://...) or [x](https://...)
    m = _URL_RE.search(text)
    if not m:
        return ""
    u = (m.group(0) or "").strip()
    return u.strip("<>()")


def _best_media_url(block: object) -> str:
    if not isinstance(block, dict):
        return ""
    return str(block.get("proxy_url") or block.get("url") or "").strip()


def _embed_image_url(embed: dict) -> str:
    u = _best_media_url(embed.get("image"))
    if u:
        return u
    return _best_media_url(embed.get("thumbnail"))


def _field_map(embed: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for f in embed.get("fields") or []:
        if not isinstance(f, dict):
            continue
        name = re.sub(r"\s+", " ", str(f.get("name") or "").strip().lower())
        val = str(f.get("value") or "").strip()
        if name:
            out[name] = val
    return out


def _get_field(fm: dict[str, str], *names: str) -> str:
    for n in names:
        if n in fm:
            return fm[n]
    for k, v in fm.items():
        for n in names:
            if n in k:
                return v
    return ""


def _clean_price(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    # Strip markdown/backticks and keep first $... token
    s = re.sub(r"[`*_]", "", s)
    m = re.search(r"\$\s*[\d,.]+", s)
    if m:
        return m.group(0).replace(" ", "")

    # No '$' present: extract a number and format as $X.XX
    m2 = re.search(r"(?<!\w)(\d+(?:[.,]\d{1,2})?)(?!\w)", s)
    if not m2:
        tok = s.split()[0]
        return tok if tok.startswith("$") else f"${tok}"
    num = m2.group(1).replace(",", "")
    try:
        v = float(num)
        return f"${v:.2f}"
    except ValueError:
        tok = m2.group(1)
        return tok if tok.startswith("$") else f"${tok}"


def _recent_messages(channel_id: str, token: str, *, limit: int = 25) -> list[dict]:
    lim = max(1, min(100, int(limit)))
    url = f"{DISCORD_API}/channels/{channel_id}/messages?limit={lim}"
    r = discord_get(url, token)
    if r.status_code != 200:
        raise RuntimeError(f"GET recent messages failed HTTP {r.status_code}: {(r.text or '')[:200]}")
    data = r.json()
    if not isinstance(data, list):
        return []
    return [m for m in data if isinstance(m, dict) and m.get("id")]


def _jump_link(guild_id: str, channel_id: str, message_id: str) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def _send_plain(channel_id: str, token: str, content: str) -> dict:
    body = {"content": str(content or ""), "tts": False}
    r = discord_post(f"{DISCORD_API}/channels/{channel_id}/messages", token, body)
    if r.status_code != 200:
        raise RuntimeError(f"POST message failed HTTP {r.status_code}: {(r.text or '')[:240]}")
    data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("POST message returned non-object JSON.")
    return data


def _send_reply(channel_id: str, token: str, *, reply_to_message_id: str, content: str) -> dict:
    """
    Post a message reply. Used when we cannot edit the original breakdown message.
    """
    body = {
        "content": str(content or ""),
        "tts": False,
        "message_reference": {"message_id": str(reply_to_message_id or "").strip()},
        "allowed_mentions": {"parse": []},
    }
    r = discord_post(f"{DISCORD_API}/channels/{channel_id}/messages", token, body)
    if r.status_code != 200:
        raise RuntimeError(f"POST reply failed HTTP {r.status_code}: {(r.text or '')[:240]}")
    data = r.json()
    return data if isinstance(data, dict) else {}


def _wait_for_reply(
    *,
    channel_id: str,
    after_message_id: str,
    token: str,
    my_user_id: str,
    must_contain: str,
    timeout_s: float,
    must_reply_to_message_id: str = "",
) -> dict:
    """
    Wait for the first message after after_message_id not authored by us,
    whose content or embed text contains must_contain (case-insensitive).
    """
    deadline = time.time() + float(timeout_s)
    needle = (must_contain or "").strip().lower()
    reply_to = (must_reply_to_message_id or "").strip()
    last_seen = after_message_id
    while time.time() < deadline:
        batch = list_messages_after(channel_id, last_seen, token, limit=50)
        if batch:
            # advance cursor
            last_seen = str(batch[-1].get("id") or last_seen)
        for m in batch:
            author = m.get("author") if isinstance(m.get("author"), dict) else {}
            aid = str(author.get("id") or "").strip()
            if not aid or aid == my_user_id:
                continue

            # Prefer a direct reply-to our query message when available.
            if reply_to:
                ref = m.get("message_reference") if isinstance(m.get("message_reference"), dict) else {}
                ref_id = str(ref.get("message_id") or "").strip()
                if ref_id and ref_id != reply_to:
                    continue
                # Some payloads include referenced_message instead of message_reference.
                if not ref_id:
                    refm = m.get("referenced_message") if isinstance(m.get("referenced_message"), dict) else {}
                    rid = str(refm.get("id") or "").strip()
                    if rid and rid != reply_to:
                        continue
                # If there is no reference info at all, do NOT treat as a reply.
                if not ref_id and not rid:
                    continue

            blob = str(m.get("content") or "")
            embeds = m.get("embeds") or []
            if isinstance(embeds, list):
                for e in embeds:
                    if isinstance(e, dict):
                        blob += "\n" + str(e.get("title") or "") + "\n" + str(e.get("description") or "")
                        for f in e.get("fields") or []:
                            if isinstance(f, dict):
                                blob += "\n" + str(f.get("name") or "") + ": " + str(f.get("value") or "")
            if needle and needle not in blob.lower():
                continue
            return m
        time.sleep(0.6)
    raise TimeoutError(f"Timed out waiting for resolver reply (needle={must_contain!r}).")


def _url_host(url: str) -> str:
    try:
        u = urlparse(str(url or "").strip())
        return (u.netloc or "").lower()
    except Exception:
        return ""


def _store_key_from_url(url: str) -> str:
    """
    Best-effort map from store URL host to a canonical store key.
    """
    h = _url_host(url)
    if not h:
        return ""
    # Strip common prefixes.
    hh = h.lower()
    for prefix in ("www.", "m."):
        if hh.startswith(prefix):
            hh = hh[len(prefix) :]

    # Direct host matches.
    if "bestbuy." in hh or "bbystatic." in hh:
        return "bestbuy"
    if "lowes." in hh:
        return "lowes"
    if "target." in hh:
        return "target"
    if "walmart." in hh or "walmartimages." in hh:
        return "walmart"
    if "bjs." in hh or "bjswholesale" in hh:
        return "bjs"
    if "snipes." in hh:
        return "snipes"
    if "acehardware." in hh or "acehardware" in hh:
        return "acehardware"
    if "samsclub." in hh or "samsclub" in hh:
        return "samsclub"
    if "gamestop." in hh:
        return "gamestop"
    if "homedepot." in hh:
        return "homedepot"
    if "sephora." in hh:
        return "sephora"
    if "dollargeneral." in hh or "dollargeneral" in hh:
        return "dollargeneral"
    if "costco." in hh:
        return "costco"
    if "kohls." in hh:
        return "kohls"

    return ""


def _apply_removals_to_breakdown(original: str, ids_to_remove: set[str]) -> str:
    """
    If we cannot generate a working stock link for a row, remove the placeholder
    ' | Go here to check stock →' (and any trailing URL) from the ID line so it
    doesn't point to nothingness.
    """
    if not ids_to_remove:
        return original
    lines = (original or "").splitlines()
    out: list[str] = []
    for line in lines:
        new_line = line
        for pid in ids_to_remove:
            if f"`{pid}`" not in new_line:
                continue
            if "go here to check stock" not in new_line.lower():
                continue
            # Remove from the pipe onwards (covers: " | Go here..." and any appended URL).
            new_line = re.sub(
                r"\s*\|\s*Go here to check stock\s*→.*$",
                "",
                new_line,
                flags=re.IGNORECASE,
            ).rstrip()
            break
        out.append(new_line)
    return "\n".join(out)


def _truncate_for_discord(content: str, *, max_len: int = 2000) -> str:
    s = str(content or "")
    if len(s) <= max_len:
        return s
    # Keep a hard cap with a clear tail marker.
    return s[: max(0, max_len - 20)].rstrip() + "\n... (truncated)"


def _build_minimal_breakdown_edit(updated: str) -> str:
    """
    To fit within Discord's 2000-char limit, drop long narrative sections.
    Keep everything until the first '**Info:**' or '**Notes**' header (inclusive stop).
    """
    lines = (updated or "").splitlines()
    out: list[str] = []
    for ln in lines:
        low = ln.strip().lower()
        if low.startswith("**info:**") or low.startswith("**notes"):
            break
        out.append(ln)
    return "\n".join(out).rstrip()


def _render_link_mapping_lines(rows: list["BreakdownRow"], jump_by_id: dict[str, str]) -> str:
    """
    Reply-friendly mapping list when we cannot edit the original message.
    """
    lines: list[str] = []
    for r in rows:
        j = jump_by_id.get(r.id_value) or ""
        if not j:
            continue
        lines.append(f"- {r.id_type}:{r.id_value} -> {j}")
    return "\n".join(lines)


def _is_only_bots_single_message_403(resp) -> bool:
    """
    Discord returns 403 + code 20002 for user tokens on GET .../messages/{message_id}.
    Mirror the logic in mirror_message_to_m_lead.py (but keep this script self-contained).
    """
    try:
        if getattr(resp, "status_code", None) != 403:
            return False
        j = resp.json()
        if isinstance(j, dict) and int(j.get("code") or 0) == 20002:
            return True
        return "only bots" in str((j or {}).get("message") or "").lower()
    except Exception:
        return False


def _fetch_message_by_id(channel_id: str, message_id: str, token: str) -> dict | None:
    """
    Fetch a message payload by id. For user tokens, Discord may block direct GET and require around= fallback.
    """
    mid = str(message_id or "").strip()
    cid = str(channel_id or "").strip()
    if not (mid.isdigit() and cid.isdigit()):
        return None
    direct = f"{DISCORD_API}/channels/{cid}/messages/{mid}"
    r = discord_get(direct, token)
    if r.status_code == 200:
        data = r.json()
        return data if isinstance(data, dict) and data.get("id") else None
    if _is_only_bots_single_message_403(r):
        around = f"{DISCORD_API}/channels/{cid}/messages?around={mid}&limit=9"
        r2 = discord_get(around, token)
        if r2.status_code == 200:
            arr = r2.json()
            if isinstance(arr, list):
                for m in arr:
                    if isinstance(m, dict) and str(m.get("id") or "") == mid:
                        return m
    return None


def _extract_details_from_single_embed(e0: dict) -> tuple[str, str, str]:
    title = str(e0.get("title") or "").strip()
    if not title:
        desc = str(e0.get("description") or "").strip()
        if desc:
            title = desc.splitlines()[0].strip()
    fm = _field_map(e0)
    msrp = _clean_price(_get_field(fm, "msrp", "retail", "price"))
    low = _clean_price(_get_field(fm, "as low as", "as low", "low"))
    price = msrp or low
    img = _embed_image_url(e0)
    if not (title and price and img):
        raise RuntimeError(f"missing(title={bool(title)} price={bool(price)} img={bool(img)})")
    return title, price, img


def _extract_details_from_plaintext_block(content: str) -> tuple[str, str, str]:
    """
    Chromerrunner-style plaintext block (when embed fields differ): **Title:** / **Price:** / **Image:** lines.
    """
    text = str(content or "")
    if not text.strip():
        return "", "", ""
    title = ""
    m = re.search(r"(?im)^\s*(?:\*\*)?(?:title)(?:\*\*)?\s*:\s*(.+)$", text)
    if m:
        title = re.sub(r"[`_*]", "", (m.group(1) or "").strip()).strip()
    price = ""
    m = re.search(
        r"(?im)^\s*(?:\*\*)?(?:msrp|price|retail|as low as|as low)(?:\*\*)?\s*:\s*([^\n]+)",
        text,
    )
    if m:
        price = _clean_price(m.group(1))
    img = ""
    m = re.search(r"(?im)^\s*(?:\*\*)?(?:image)(?:\*\*)?\s*:\s*(https?://\S+)", text)
    if m:
        img = (m.group(1) or "").strip().rstrip(").,]")
    if not img:
        m2 = re.search(r"(https?://[^\s<>\[\]()\"']+\.(?:png|jpe?g|webp|gif)(?:\?[^\s\"']*)?)", text, re.I)
        if m2:
            img = m2.group(1).strip()
    return title, price, img


def _try_extract_resolver_details(reply: dict) -> tuple[str, str, str]:
    """
    Full extraction: try every embed, then plaintext (Chromerrunner often edits one message in-place).
    """
    embeds = reply.get("embeds") or []
    if isinstance(embeds, list):
        for e in embeds:
            if not isinstance(e, dict):
                continue
            try:
                return _extract_details_from_single_embed(e)
            except Exception:
                continue
    t2, p2, i2 = _extract_details_from_plaintext_block(str(reply.get("content") or ""))
    if t2 and p2 and i2:
        return t2, p2, i2
    raise RuntimeError("resolver message had no complete embed + plaintext did not parse")


def _extract_details_from_first_embed(reply: dict) -> tuple[str, str, str]:
    """Backward-compatible name; delegates to multi-embed + plaintext resolver."""
    return _try_extract_resolver_details(reply)


def _message_debug_preview(m: dict) -> str:
    parts: list[str] = []
    c = str(m.get("content") or "").strip()
    if c:
        parts.append(c)
    embeds = m.get("embeds") or []
    if isinstance(embeds, list):
        for e in embeds[:2]:
            if not isinstance(e, dict):
                continue
            t = str(e.get("title") or "").strip()
            if t:
                parts.append(f"[embed.title] {t}")
            u = str(e.get("url") or "").strip()
            if u:
                parts.append(f"[embed.url] {u}")
            d = str(e.get("description") or "").strip()
            if d:
                parts.append(f"[embed.desc] {d[:160]}")
            for f in (e.get("fields") or [])[:6]:
                if isinstance(f, dict):
                    fn = str(f.get("name") or "").strip()
                    fv = str(f.get("value") or "").strip()
                    if fn or fv:
                        parts.append(f"[field] {fn}: {fv[:140]}")
            img = e.get("image")
            thumb = e.get("thumbnail")
            iu = _best_media_url(img)
            tu = _best_media_url(thumb)
            if iu:
                parts.append(f"[embed.image] {iu}")
            if tu:
                parts.append(f"[embed.thumb] {tu}")
    out = " | ".join([p.replace("\n", " ") for p in parts]).strip()
    if len(out) > 320:
        out = out[:320] + "..."
    return out


def _console_safe(s: str) -> str:
    """
    Windows console often uses cp1252; replace unsupported characters so printing never crashes.
    """
    try:
        return str(s).encode("cp1252", errors="replace").decode("cp1252", errors="replace")
    except Exception:
        return re.sub(r"[^\x20-\x7E]+", "?", str(s))


@dataclass(frozen=True)
class BreakdownRow:
    store_name: str
    store_url: str
    id_type: str
    id_value: str


def _extract_rows(markdown: str) -> list[BreakdownRow]:
    """
    Parse rows like:
      * [Five Below](<https://...>)
        * **SKU:** `9164424` | Go here to check stock →
    """
    text = str(markdown or "")
    rows: list[BreakdownRow] = []

    # Store line then id line. Allow varying indentation and trailing text after backticks.
    pat = re.compile(
        r"^\*\s+\[(?P<store>[^\]]+)\]\(<(?P<url>https?://[^>]+)>\)\s*$"
        r"(?:\r?\n)+"
        r"^\s*\*\s+\*\*(?P<id_type>[A-Za-z0-9 _-]+)\:\*\*\s+`(?P<id>\d{5,20})`[^\n]*$",
        re.MULTILINE,
    )
    for m in pat.finditer(text):
        store = (m.group("store") or "").strip()
        url = (m.group("url") or "").strip()
        id_type = (m.group("id_type") or "").strip()
        pid = (m.group("id") or "").strip()
        if store and pid:
            rows.append(BreakdownRow(store_name=store, store_url=url, id_type=id_type, id_value=pid))

    if rows:
        return rows

    # Fallback format: standalone **UPC:** 123... plus a "Where:" store list.
    # Example:
    #   > Where: **Walmart, Target, GameStop, Best Buy**
    #   **UPC:** 787926178791
    m_upc = re.search(r"^\s*\*\*UPC:\*\*\s*`?(\d{8,20})`?\s*$", text, re.MULTILINE | re.IGNORECASE)
    if not m_upc:
        m_upc = re.search(r"^\s*\*\*UPC:\*\*\s*(\d{8,20})\s*$", text, re.MULTILINE | re.IGNORECASE)
    upc = (m_upc.group(1) if m_upc else "").strip()
    if not upc:
        return []

    m_where = re.search(r"^\s*>\s*Where:\s*\*\*([^*]+)\*\*\s*$", text, re.MULTILINE | re.IGNORECASE)
    stores_raw = (m_where.group(1) if m_where else "").strip()
    stores: list[str] = []
    if stores_raw:
        for part in stores_raw.split(","):
            s = part.strip()
            if s:
                stores.append(s)
    if not stores:
        # If no Where list, create a single generic row; store_slug will fall back to URL host later.
        stores = ["unknown"]

    for s in stores:
        rows.append(BreakdownRow(store_name=s, store_url="", id_type="UPC", id_value=upc))
    return rows


def _resolve_id_to_url(
    *,
    product_id: str,
    resolver_channel_id: str,
    token: str,
    my_user_id: str,
    timeout_s: float = 25.0,
) -> str:
    sent = _send_plain(resolver_channel_id, token, product_id)
    sent_id = str(sent.get("id") or "")
    reply = _wait_for_reply(
        channel_id=resolver_channel_id,
        after_message_id=sent_id,
        token=token,
        my_user_id=my_user_id,
        # Resolver bots often do not echo the queried id. Take the first reply after our query.
        must_contain="",
        timeout_s=timeout_s,
        must_reply_to_message_id=sent_id,
    )
    # Try content first, then embeds.
    u = _first_url(str(reply.get("content") or ""))
    if u:
        return u
    embeds = reply.get("embeds") or []
    if isinstance(embeds, list):
        for e in embeds:
            if isinstance(e, dict):
                # Common "no hits" shape: title "Monitor data search", field "<query>: No hits."
                title_l = str(e.get("title") or "").strip().lower()
                desc_l = str(e.get("description") or "").strip().lower()
                if "no hit" in desc_l or "no hits" in desc_l:
                    raise ResolverNoHit(f"No hits for id={product_id}")
                u = _first_url(str(e.get("url") or "")) or _first_url(str(e.get("description") or ""))
                if u:
                    return u
                for f in e.get("fields") or []:
                    if isinstance(f, dict):
                        fv_l = str(f.get("value") or "").strip().lower()
                        fn_l = str(f.get("name") or "").strip().lower()
                        if ("no hit" in fv_l or "no hits" in fv_l) and (product_id in fn_l or product_id in fv_l):
                            raise ResolverNoHit(f"No hits for id={product_id}")
                        u = _first_url(str(f.get("value") or ""))
                        if u:
                            return u
    raise RuntimeError(
        "Resolver reply did not include a URL. "
        f"reply_preview={_message_debug_preview(reply)!r}"
    )


def _resolve_url_to_details(
    *,
    store_url: str,
    resolver_channel_id: str,
    token: str,
    my_user_id: str,
    timeout_s: float = 30.0,
) -> tuple[str, str, str]:
    sent = _send_plain(resolver_channel_id, token, store_url)
    sent_id = str(sent.get("id") or "")
    host = _url_host(store_url)
    print(f"  link->resolver: posted url (host={host or '-'}) msg_id={sent_id}")
    # The link resolver may post "Chromerrunner starting…" and then edit the *same* message id.
    # A single one-shot wait is not enough: if that wait ends before the edit lands, the next
    # list_messages_after() often returns an empty batch (no new ids), and we would never
    # re-fetch the same message. Track pending message ids and re-poll them
    # every loop until the global deadline.
    deadline = time.time() + float(timeout_s)
    cursor = sent_id
    last_preview = ""
    pending_ids: list[str] = []
    seen_pending: set[str] = set()
    reply: dict | None = None

    def _add_pending(mid: str) -> None:
        mid = str(mid or "").strip()
        if not mid.isdigit() or mid in seen_pending:
            return
        seen_pending.add(mid)
        pending_ids.append(mid)

    while time.time() < deadline:
        # 1) Re-fetch known resolver messages (same id, edited in place).
        for mid in list(pending_ids):
            m = _fetch_message_by_id(resolver_channel_id, mid, token)
            if not isinstance(m, dict):
                continue
            last_preview = _message_debug_preview(m) or last_preview
            try:
                _try_extract_resolver_details(m)
                reply = m
                break
            except Exception:
                pass
        if reply is not None:
            break

        batch = list_messages_after(resolver_channel_id, cursor, token, limit=50)
        if batch:
            cursor = str(batch[-1].get("id") or cursor)

        for m in batch:
            author = m.get("author") if isinstance(m.get("author"), dict) else {}
            aid = str(author.get("id") or "").strip()
            if not aid or aid == my_user_id:
                continue

            preview = _message_debug_preview(m)
            last_preview = preview or last_preview

            blob = preview.lower()
            if host and host not in blob:
                continue

            mid = str(m.get("id") or "").strip()
            if (
                "chromerrunner starting" in blob
                or ("starting" in blob and "chromerrunner" in blob)
                or ("processing" in blob and "url:" in blob)
            ):
                print(f"  link->resolver: status: {_console_safe(preview)}")
                if mid:
                    _add_pending(mid)
                continue

            embeds = m.get("embeds") or []
            if isinstance(embeds, list) and embeds:
                if mid:
                    _add_pending(mid)
                try:
                    _try_extract_resolver_details(m)
                    reply = m
                    break
                except Exception:
                    last_preview = preview or last_preview
                    continue
            if preview:
                print(f"  link->resolver: non-embed update: {_console_safe(preview)}")
        if reply is not None:
            break

        time.sleep(1.0)

    if reply is None:
        raise TimeoutError(f"Timed out waiting for link resolver embed. last_preview={last_preview!r}")

    title, price, img = _try_extract_resolver_details(reply)
    return title, price, img


def _send_m_lead(
    *,
    command_channel_id: str,
    token: str,
    title: str,
    price: str,
    product_id: str,
    image_url: str,
    store_slug: str,
    destination_channel_id: str,
) -> dict:
    # Avoid breaking the command parser with quotes.
    safe_title = (title or "").replace('"', "'").strip()
    dest_mention = f"<#{destination_channel_id}>"
    # Keep store slug simple for downstream parsing.
    slug = re.sub(r"[^a-z0-9-]+", "-", (store_slug or "").strip().lower()).strip("-") or "unknown"
    content = f"!m lead {safe_title} {price} {price} {product_id} {image_url} {slug} {dest_mention}"
    return _send_plain(command_channel_id, token, content)


def _build_m_lead_line(
    *,
    title: str,
    price: str,
    product_id: str,
    image_url: str,
    store_slug: str,
    destination_channel_id: str,
) -> str:
    safe_title = (title or "").replace('"', "'").strip()
    slug = re.sub(r"[^a-z0-9-]+", "-", (store_slug or "").strip().lower()).strip("-") or "unknown"
    dest_mention = f"<#{destination_channel_id}>"
    return f"!m lead {safe_title} {price} {price} {product_id} {image_url} {slug} {dest_mention}"


def _wait_for_destination_post(
    *,
    guild_id: str,
    destination_channel_id: str,
    token: str,
    after_message_id: str,
    product_id: str = "",
    title_hint: str = "",
    timeout_s: float = 35.0,
) -> tuple[str, dict]:
    """
    Prefer a deterministic wait: get messages strictly after `after_message_id` and
    return the first new one. Optional product_id/title_hint matching is kept as a fallback.
    """
    after = str(after_message_id or "").strip()
    if not after.isdigit():
        after = "0"
    deadline = time.time() + float(timeout_s)
    needle_id = (product_id or "").strip()
    hint = re.sub(r"\s+", " ", (title_hint or "").strip().lower())[:40]

    last_after = after
    while time.time() < deadline:
        batch = list_messages_after(destination_channel_id, last_after, token, limit=25)
        if batch:
            # Prefer the first truly new message.
            first_new = batch[0]
            mid = str(first_new.get("id") or "").strip()
            if mid:
                return f"https://discord.com/channels/{guild_id}/{destination_channel_id}/{mid}", first_new
            last_after = str(batch[-1].get("id") or last_after)

        # Fallback: scan recent messages for matching text (older behavior)
        if needle_id or hint:
            msgs = _recent_messages(destination_channel_id, token, limit=30)
            for m in msgs:
                blob = _message_debug_preview(m)
                low = blob.lower()
                if needle_id and needle_id in low:
                    mid = str(m.get("id") or "").strip()
                    return f"https://discord.com/channels/{guild_id}/{destination_channel_id}/{mid}", m
                if hint and hint in low:
                    mid = str(m.get("id") or "").strip()
                    return f"https://discord.com/channels/{guild_id}/{destination_channel_id}/{mid}", m

        time.sleep(0.9)
    raise TimeoutError("Timed out waiting for the destination lead post (no new messages after baseline).")


def _apply_jump_links_to_breakdown(original: str, rows: list[BreakdownRow], jump_by_id: dict[str, str]) -> str:
    lines = (original or "").splitlines()
    out: list[str] = []
    for line in lines:
        new_line = line
        for r in rows:
            jump = jump_by_id.get(r.id_value) or ""
            if not jump:
                continue
            if f"`{r.id_value}`" not in new_line:
                continue
            if "Go here to check stock" not in new_line:
                continue
            # Only append if no URL already after the arrow.
            if "http" in new_line.lower():
                continue
            new_line = new_line.rstrip() + " " + jump
            break
        out.append(new_line)
    return "\n".join(out)


def _edit_message_content(*, channel_id: str, message_id: str, token: str, new_content: str) -> None:
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}"
    r = discord_patch(url, token, {"content": new_content})
    if r.status_code != 200:
        raise RuntimeError(f"PATCH message failed HTTP {r.status_code}: {(r.text or '')[:240]}")


def _is_cannot_edit_other_user_403(err: BaseException) -> bool:
    s = str(err)
    return ("Cannot edit a message authored by another user" in s) or ('"code": 50005' in s)


def _process_breakdown_message(
    *,
    cfg: dict,
    guild_id: str,
    source_channel_id: str,
    source_message_id: str,
    token: str,
    my_id: str,
    max_rows: int,
    no_send: bool,
    preview_edit_only: bool = False,
) -> int:
    product_ids_ch = _cfg_channel_id(cfg, "product_ids")
    link_values_ch = _cfg_channel_id(cfg, "product_links")
    command_post_ch = _cfg_channel_id(cfg, "command_post_channel_id")
    dest_ch = _cfg_channel_id(cfg, "destination_channel_id")
    try:
        link_timeout = float(cfg.get("link_resolver_timeout_seconds") or 180.0)
    except Exception:
        link_timeout = 180.0
    link_timeout = max(30.0, min(600.0, link_timeout))

    msg, _ch, _label, _tok_used = fetch_message_with_token_fallback(guild_id, source_channel_id, source_message_id)
    original_text = str(msg.get("content") or "")
    author = msg.get("author") if isinstance(msg.get("author"), dict) else {}
    author_id = str(author.get("id") or "").strip()
    if not original_text.strip():
        print("ERROR: source message has no content text (embed-only breakdown not supported yet).", file=sys.stderr)
        return 2

    rows = _extract_rows(original_text)
    if not rows:
        print("ERROR: could not find any store rows in the breakdown message.", file=sys.stderr)
        return 2
    if max_rows and max_rows > 0:
        rows = rows[: int(max_rows)]

    jump_by_id: dict[str, str] = {}
    ids_to_remove: set[str] = set()

    print(f"\nFound {len(rows)} product row(s). Resolving and posting...\n")
    for i, r in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] store={r.store_name} id={r.id_type}:{r.id_value}")

        store_url = (r.store_url or "").strip()
        if not store_url:
            try:
                store_url = _resolve_id_to_url(
                    product_id=r.id_value,
                    resolver_channel_id=product_ids_ch,
                    token=token,
                    my_user_id=my_id,
                )
                print(f"  id->url: {store_url}")
            except ResolverNoHit as e:
                print(f"  id->url: NO HIT ({e})")
                ids_to_remove.add(r.id_value)
                continue

        store_key = _store_key_from_url(store_url)
        if not store_key or store_key not in _SUPPORTED_STORES:
            print(
                f"  store gate: UNSUPPORTED store_key={store_key!r} host={_url_host(store_url)!r} -> remove placeholder"
            )
            ids_to_remove.add(r.id_value)
            continue

        title, price, img = _resolve_url_to_details(
            store_url=store_url,
            resolver_channel_id=link_values_ch,
            token=token,
            my_user_id=my_id,
            timeout_s=link_timeout,
        )
        print(f"  url->details: title={title[:70]!r} price={price} img={(img[:60] + '...') if len(img) > 60 else img}")

        cmd_line = _build_m_lead_line(
            title=title,
            price=price,
            product_id=r.id_value,
            image_url=img,
            # Use URL-derived store key (canonical). The bracket label in breakdowns is often product name.
            store_slug=store_key,
            destination_channel_id=dest_ch,
        )
        print(f"  !m lead: {cmd_line}")

        if no_send:
            print("  (no-send) skipping command post + destination wait")
            continue

        baseline_msgs = _recent_messages(dest_ch, token, limit=1)
        baseline_after = str(baseline_msgs[0].get("id") or "") if baseline_msgs else "0"
        print(f"  dest baseline after={baseline_after or '0'}")

        sent_cmd = _send_m_lead(
            command_channel_id=command_post_ch,
            token=token,
            title=title,
            price=price,
            product_id=r.id_value,
            image_url=img,
            store_slug=store_key,
            destination_channel_id=dest_ch,
        )
        cmd_id = str(sent_cmd.get("id") or "")
        print(f"  posted command msg_id={cmd_id}")

        jump, _dest_msg = _wait_for_destination_post(
            guild_id=guild_id,
            destination_channel_id=dest_ch,
            token=token,
            after_message_id=baseline_after,
            product_id=r.id_value,
            title_hint=title,
        )
        jump_by_id[r.id_value] = jump
        print(f"  destination jump: {jump}")

    if no_send:
        print("\n(no-send) not editing breakdown message.")
        return 0

    updated = _apply_jump_links_to_breakdown(original_text, rows, jump_by_id)
    updated = _apply_removals_to_breakdown(updated, ids_to_remove)
    if updated.strip() == original_text.strip():
        print("\nNo breakdown edits applied (lines may already have URLs).")
        return 0

    minimal = _build_minimal_breakdown_edit(updated)
    chosen_edit = updated
    if len(chosen_edit) > 2000 and minimal and len(minimal) <= 2000:
        chosen_edit = minimal

    print("\n--- breakdown preview (first 40 lines) ---")
    for ln in chosen_edit.splitlines()[:40]:
        print(ln)
    if len(chosen_edit.splitlines()) > 40:
        print("... (truncated)")
    print(f"[edit length] {len(chosen_edit)} chars")

    if preview_edit_only:
        print("(preview-only) skipping Discord edit/post.")
        return 0

    # Edit the original breakdown only if we authored it; otherwise reply with the cleaned version.
    if author_id and author_id == my_id:
        try:
            if len(chosen_edit) > 2000:
                raise RuntimeError("EDIT_TOO_LONG: exceeds 2000 chars")
            _edit_message_content(
                channel_id=source_channel_id,
                message_id=source_message_id,
                token=token,
                new_content=chosen_edit,
            )
            print("\nUpdated the original breakdown message with jump links.")
        except RuntimeError as e:
            # If too long or cannot edit, fall back to a reply.
            if "BASE_TYPE_MAX_LENGTH" in str(e) or "EDIT_TOO_LONG" in str(e) or _is_cannot_edit_other_user_403(e):
                author_id = ""  # fall through to reply
            else:
                raise
    else:
        snippet_lines = updated.splitlines()
        snippet = "\n".join(snippet_lines[:40]) + ("\n... (truncated)" if len(snippet_lines) > 40 else "")
        _send_reply(
            source_channel_id,
            token,
            reply_to_message_id=source_message_id,
            content="I can't edit the original breakdown message (not my message). Cleaned version:\n\n"
            + snippet,
        )
        print("\nPosted a reply with the cleaned breakdown (cannot edit original).")

    # If we couldn't edit (too long or permissions), also reply with a compact mapping list.
    if not (author_id and author_id == my_id):
        mapping = _render_link_mapping_lines(rows, jump_by_id)
        if mapping.strip():
            _send_reply(
                source_channel_id,
                token,
                reply_to_message_id=source_message_id,
                content=_truncate_for_discord(
                    "Jump links (for stock check):\n\n" + mapping, max_len=2000
                ),
            )
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    cfg = _load_cfg()

    ap = argparse.ArgumentParser(description="RS In-Store lead flow (interactive, --url, or --watch).")
    ap.add_argument("--url", default="", help="Breakdown Discord message jump link.")
    ap.add_argument("--max-rows", type=int, default=0, help="Limit rows for testing (0 = all).")
    ap.add_argument(
        "--no-send",
        action="store_true",
        help="Do not post !m lead or edit the source message (still queries resolvers).",
    )
    ap.add_argument(
        "--watch",
        action="store_true",
        help="Watch the configured source channel and auto-process new breakdown messages.",
    )
    ap.add_argument(
        "--preview-edit",
        action="store_true",
        help="Build and print the would-edit content + length, but do not post/edit anything.",
    )
    args = ap.parse_args(argv)

    token = reminder_bot.load_token()
    my_id = _get_me_id(token)

    if args.watch:
        w = _load_watch_cfg(cfg)
        source_channel_id = str(w.get("source_channel_id") or "").strip()
        if not source_channel_id.isdigit():
            raise ValueError('config.json watch.source_channel_id must be set to the breakdown channel id.')
        poll_s = float(_cfg_int(w, "poll_interval_seconds", default=12, min_v=3, max_v=120))
        max_poll = _cfg_int(w, "max_messages_per_poll", default=25, min_v=5, max_v=100)

        # Determine guild id from the first observed message, store in state.
        state = _load_state()
        last_id = str(state.get("last_processed_message_id") or "").strip()
        print(f"Watching channel_id={source_channel_id} poll={poll_s:.1f}s (state_last={last_id or '-'})")
        while True:
            try:
                msgs = _recent_messages(source_channel_id, token, limit=max_poll)
            except Exception as e:
                print(f"WATCH: fetch error: {e}")
                time.sleep(poll_s)
                continue
            if not msgs:
                time.sleep(poll_s)
                continue
            # Oldest-first for processing
            msgs_sorted = sorted(msgs, key=lambda m: int(str(m.get("id") or 0)))
            for m in msgs_sorted:
                mid = str(m.get("id") or "").strip()
                if not mid.isdigit():
                    continue
                if last_id.isdigit() and int(mid) <= int(last_id):
                    continue

                gid = str(m.get("guild_id") or state.get("guild_id") or "").strip()
                if not gid.isdigit():
                    # Some message payloads don't include guild_id. Require it in state via --url once.
                    # Best effort: skip until state has it.
                    print(f"WATCH: missing guild_id for msg_id={mid}; run once with --url to seed state.guild_id")
                    last_id = mid
                    state["last_processed_message_id"] = last_id
                    _save_state(state)
                    continue

                print("\n" + "=" * 72)
                print(f"WATCH: processing msg_id={mid}")
                url = _jump_link(gid, source_channel_id, mid)
                rc: int = 1
                try:
                    rc = _process_breakdown_message(
                        cfg=cfg,
                        guild_id=gid,
                        source_channel_id=source_channel_id,
                        source_message_id=mid,
                        token=token,
                        my_id=my_id,
                        max_rows=int(args.max_rows or 0),
                        no_send=bool(args.no_send),
                        preview_edit_only=bool(args.preview_edit),
                    )
                except Exception as e:
                    print(f"WATCH: ERROR msg_id={mid}: {e}", file=sys.stderr)
                    rc = 1
                finally:
                    # Always advance state so one bad message doesn't crash-loop forever.
                    last_id = mid
                    state["guild_id"] = gid
                    state["last_processed_message_id"] = last_id
                    _save_state(state)
                print(f"WATCH: done rc={rc} url={url}")

            time.sleep(poll_s)

    url = (args.url or "").strip()
    if not url:
        print("Paste the breakdown Discord message link (discord.com or ptb.discord.com).")
        url = input("Message URL: ").strip()
    if not url:
        print("Exit.")
        return 0

    guild_id, source_channel_id, source_message_id = parse_jump_url(url)
    # Seed watch state so --watch can run even when message payload lacks guild_id.
    st = _load_state()
    st["guild_id"] = guild_id
    _save_state(st)

    return _process_breakdown_message(
        cfg=cfg,
        guild_id=guild_id,
        source_channel_id=source_channel_id,
        source_message_id=source_message_id,
        token=token,
        my_id=my_id,
        max_rows=int(args.max_rows or 0),
        no_send=bool(args.no_send),
        preview_edit_only=bool(args.preview_edit),
    )


if __name__ == "__main__":
    raise SystemExit(main())

