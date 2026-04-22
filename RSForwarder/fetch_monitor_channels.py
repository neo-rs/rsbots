"""
Fetch ALL channels under the given Discord category IDs via the Discord API
and update RSForwarder/config.json with rs_fs_monitor_channel_ids.

Includes every text channel: store monitors (e.g. five-below, boxlunch-hottopic),
banner/separator channels (e.g. ------, ----commands----), and any other channels.
No filter by "monitor" in the name.

Usage (from repo root):
  python -m RSForwarder.fetch_monitor_channels
  python -m RSForwarder.fetch_monitor_channels --interactive

Requires bot_token and guild_id in config (from config.json + config.secrets.json).
Category IDs come from RSForwarder/config.json -> rs_fs_monitor_category_ids.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Repo root for mirror_world_config
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import requests

from mirror_world_config import load_config_with_secrets

DISCORD_API = "https://discord.com/api/v10"
_URL_RE = re.compile(r"https?://\S+")
_ASIN_RE = re.compile(r"\b([A-Z0-9]{10})\b")
_FANATICS_P_RE = re.compile(r"(?:^|[?&])p-(\d{6,})\b", re.IGNORECASE)
_COSTCO_PID_RE = re.compile(r"\.product\.(\d{6,})\.html", re.IGNORECASE)
_NUM_ID_RE = re.compile(r"\b(\d{6,})\b")
_ALNUM_ID_RE = re.compile(r"\b([A-Z0-9][A-Z0-9\-]{5,24})\b", re.IGNORECASE)


def normalize_channel_key(name: str) -> str:
    """Normalize for config key: strip emoji/prefix, take last part. Preserve banners (hyphens)."""
    s = (name or "").strip().lower()
    if not s:
        return ""
    s = s.replace("\u2503", "|").replace("\u2502", "|").replace("\u4e28", "|")  # ┃ │ 丨
    parts = [p.strip() for p in re.split(r"[|]+", s) if p.strip()]
    if parts:
        s = parts[-1]
    # Strip leading non-alphanumeric but keep rest (so "----commands----" -> "commands----" or keep full)
    # For config key we want a unique string; keep full if it's all punctuation (banner)
    alnum_stripped = re.sub(r"^[^a-z0-9]+", "", s)
    return alnum_stripped if alnum_stripped else s


def _discord_get_json(url: str, headers: dict, *, timeout_s: float = 20.0, max_tries: int = 5) -> object:
    """
    Discord REST helper with basic rate-limit handling (429).
    Avoids hardcoded sleeps by honoring Discord's retry_after.
    """
    last_err: Optional[str] = None
    for attempt in range(1, max_tries + 1):
        r = requests.get(url, headers=headers, timeout=timeout_s)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            try:
                body = r.json()
            except Exception:
                body = {}
            retry_after = body.get("retry_after")
            try:
                retry_after_s = float(retry_after)
            except Exception:
                retry_after_s = 2.0
            retry_after_s = max(0.5, min(30.0, retry_after_s))
            print(f"Rate limited (429). Waiting retry_after={retry_after_s:.2f}s (attempt {attempt}/{max_tries})")
            time.sleep(retry_after_s)
            continue
        last_err = f"{r.status_code}: {r.text[:500]}"
        break
    raise RuntimeError(f"Discord API request failed for {url}: {last_err or 'unknown error'}")


def _extract_primary_title_and_url(msg: dict) -> Tuple[str, str]:
    """
    Best-effort extraction of the "important bits":
    - title: embed title/name-ish, else first non-empty line of content
    - url: embed url, else first url in content, else first embed field url-ish
    """
    content = str(msg.get("content") or "").strip()
    embeds = msg.get("embeds") or []
    title = ""
    url = ""

    if isinstance(embeds, list):
        for e in embeds:
            if not isinstance(e, dict):
                continue
            if not title:
                title = str(e.get("title") or "").strip()
            if not url:
                url = str(e.get("url") or "").strip()
            if title and url:
                break

    if not title and content:
        for line in content.splitlines():
            t = line.strip()
            if t:
                title = t
                break

    if not url:
        m = _URL_RE.search(content or "")
        if m:
            url = m.group(0)

    return title, url


def _safe_one_line(s: object, *, max_len: int = 200) -> str:
    t = str(s or "").replace("\r", " ").replace("\n", " ").strip()
    if len(t) > max_len:
        return t[: max_len - 3] + "..."
    return t


def _shorten_url_for_console(url: str, *, max_len: int = 110) -> str:
    """
    Make long URLs readable: keep domain + a hint of path/query.
    """
    u = str(url or "").strip()
    if not u:
        return ""
    # Strip markdown wrappers if any
    u = u.strip("<>").strip()
    # Domain
    m = re.match(r"^https?://([^/]+)(/.*)?$", u, re.IGNORECASE)
    if not m:
        return _safe_one_line(u, max_len=max_len)
    host = m.group(1)
    rest = m.group(2) or ""
    # Keep last path segment + key query bits
    base = rest.split("?", 1)[0]
    seg = base.rstrip("/").split("/")[-1] if base else ""
    q = rest.split("?", 1)[1] if "?" in rest else ""
    # Keep a= / skuId= / pid= / tcIn= / upc= if present
    key_q = ""
    for key in ("a", "asin", "skuid", "skuId", "pid", "tcin", "upc", "ean", "isbn", "p"):
        mm = re.search(rf"(?:^|[&]){re.escape(key)}=([^&]+)", q, re.IGNORECASE)
        if mm:
            key_q = f"{key}={mm.group(1)[:24]}"
            break
    out = f"{host}/.../{seg}" if seg else host
    if key_q:
        out += f"?{key_q}"
    return _safe_one_line(out, max_len=max_len)


def _shorten_markdown_links_for_console(s: str, *, max_len: int = 160) -> str:
    """
    Convert markdown links into compact 'Label(host)' chunks and clip.
    """
    t = str(s or "")
    # Replace [Label](url) -> Label(host)
    def repl(m: re.Match) -> str:
        label = (m.group(1) or "").strip()
        url = (m.group(2) or "").strip()
        host = ""
        mm = re.match(r"^https?://([^/]+)", url, re.IGNORECASE)
        if mm:
            host = mm.group(1)
        if label and host:
            return f"{label}({host})"
        return label or host or "link"

    t2 = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", repl, t)
    t2 = re.sub(r"\s+", " ", t2).strip()
    return _safe_one_line(t2, max_len=max_len)


def _is_noisy_field(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return True
    # These are huge/verbose and not useful for quick scanning.
    if "one click checkout" in n:
        return True
    if n in {"links"}:
        return True
    return False


def _extract_embed_human_summary(msg: dict) -> dict:
    """
    Best-effort extraction from the first embed, aiming for operator-readable output.
    """
    embeds = msg.get("embeds") or []
    if not isinstance(embeds, list) or not embeds:
        title, primary_url = _extract_primary_title_and_url(msg)
        return {
            "title": title,
            "url": primary_url,
            "fields": [],
        }

    first = embeds[0] if isinstance(embeds[0], dict) else {}
    title = _safe_one_line(first.get("title") or "")
    url = _safe_one_line(first.get("url") or "")
    desc = _safe_one_line(first.get("description") or "", max_len=350)
    fields_out: List[dict] = []
    fields = first.get("fields") or []
    if isinstance(fields, list):
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = _safe_one_line(f.get("name") or "", max_len=60)
            value = _safe_one_line(f.get("value") or "", max_len=260)
            if not name and not value:
                continue
            fields_out.append({"name": name, "value": value})

    # If title/url missing, fall back
    if not title or not url:
        t2, u2 = _extract_primary_title_and_url(msg)
        if not title:
            title = _safe_one_line(t2)
        if not url:
            url = _safe_one_line(u2)

    out = {"title": title, "url": url, "fields": fields_out}
    if desc:
        out["description"] = desc
    return out


def _find_field_value(fields: List[dict], *, wanted_names: List[str]) -> Tuple[str, str]:
    """
    Return (field_name, field_value) if found, else ("","").
    Case-insensitive match on trimmed name.
    """
    want = {w.strip().lower(): w for w in wanted_names if w.strip()}
    for f in fields:
        if not isinstance(f, dict):
            continue
        nm = str(f.get("name") or "").strip()
        if not nm:
            continue
        key = nm.lower()
        if key in want:
            val = str(f.get("value") or "").strip()
            return nm, val
    return "", ""


def _derive_product_id_from_url(url: str) -> Tuple[str, str]:
    """
    Best-effort "derived id" from a product URL.
    Returns (id_type, id_value) or ("","") if not found.
    """
    u = str(url or "").strip()
    if not u:
        return "", ""

    # Costco: .product.<PID>.html
    m = _COSTCO_PID_RE.search(u)
    if m:
        return "PID", m.group(1)

    # Amazon / Zephyr Amazon: ASIN appears as a=ASIN, /dp/ASIN, /gp/product/ASIN, keepa etc.
    # Prefer query a=ASIN
    m = re.search(r"(?:^|[?&])a=([A-Z0-9]{10})(?:&|$)", u, re.IGNORECASE)
    if m:
        return "ASIN", m.group(1).upper()
    m = re.search(r"/dp/([A-Z0-9]{10})(?:[/?]|$)", u, re.IGNORECASE)
    if m:
        return "ASIN", m.group(1).upper()
    m = re.search(r"/gp/product/([A-Z0-9]{10})(?:[/?]|$)", u, re.IGNORECASE)
    if m:
        return "ASIN", m.group(1).upper()
    m = _ASIN_RE.search(u)
    if m and "amazon" in u.lower():
        return "ASIN", m.group(1).upper()

    # Fanatics: p-<digits> appears in URL
    m = _FANATICS_P_RE.search(u)
    if m:
        return "PID", m.group(1)

    # Shopify-ish / generic: last path segment often includes a numeric id
    # Try long numeric first
    m = _NUM_ID_RE.search(u)
    if m:
        return "ID", m.group(1)

    # Hasbro: trailing product code like G20985L00
    # Take last path segment and attempt alnum code
    try:
        last_seg = u.split("?", 1)[0].rstrip("/").split("/")[-1]
    except Exception:
        last_seg = ""
    if last_seg:
        m = _ALNUM_ID_RE.fullmatch(last_seg)
        if m:
            return "CODE", m.group(1).upper()

    return "", ""


def _extract_product_id(msg: dict) -> dict:
    """
    Prefer explicit embed fields (SKU/PID/ASIN/etc). Fall back to derived-from-URL.
    Returns:
      {"kind": "field"|"derived"|"none", "name": <field/id type>, "value": <id>}
    """
    human = _extract_embed_human_summary(msg)
    fields = human.get("fields") if isinstance(human.get("fields"), list) else []

    # Try known field names first (case variants included)
    for names in (
        ["ASIN"],
        ["TCIN"],
        ["SKU", "Sku"],
        ["PID"],
        ["UPC"],
        ["EAN"],
        ["ISBN"],
        ["Item #", "Item#", "Item No", "Item No.", "Item Number"],
        ["Product ID", "ProductID"],
        ["Style"],
        ["Model"],
    ):
        nm, val = _find_field_value(fields, wanted_names=names)
        if nm and val:
            # Strip backticks around ids like Offer Id does
            v = val.strip().strip("`").strip()
            return {"kind": "field", "name": nm, "value": v}

    # Derive from URL (embed url preferred)
    url = str(human.get("url") or "").strip()
    id_type, id_value = _derive_product_id_from_url(url)
    if id_type and id_value:
        return {"kind": "derived", "name": id_type, "value": id_value}

    return {"kind": "none", "name": "", "value": ""}


def _load_json_file(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            v = json.load(f)
        return v if isinstance(v, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _load_checkpoint(path: Path) -> dict:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return {}
    if not isinstance(data.get("channels"), dict):
        data["channels"] = {}
    return data


def _save_checkpoint(path: Path, data: dict) -> None:
    _save_json_file(path, data)


def _upsert_channel_messages(store_path: Path, *, channel_key: str, channel_id: int, new_messages: List[dict], max_store: int) -> dict:
    """
    Store format (canonical; upsert by product identity, not by Discord message-id):
      {
        "channel_key": "...",
        "channel_id": 123,
        "updated_at_unix": 123.4,
        "items_by_key": {
          "<item_key>": {
            "item_key": "...",
            "product_id": {kind/name/value},
            "title_key": "...",              # normalized title fallback key
            "first_seen_timestamp": "...",
            "last_seen_timestamp": "...",
            "last_message_id": "...",
            "latest": { ...message snapshot... }
          }
        },
        "item_keys_sorted": ["<item_key>", ...]   # newest-first by last_seen_timestamp
      }
    """
    data = _load_json_file(store_path)

    # Back-compat: if older format exists, start fresh to avoid dual storage.
    if "messages_by_id" in data or "message_ids_sorted" in data:
        data = {}

    items_by_key = data.get("items_by_key")
    if not isinstance(items_by_key, dict):
        items_by_key = {}

    def _norm_title_key(title: str) -> str:
        t = str(title or "").strip().lower()
        t = re.sub(r"\s+", " ", t)
        t = re.sub(r"[^a-z0-9\s]+", "", t)
        t = t.strip()
        if len(t) > 140:
            t = t[:140].strip()
        return t

    def _item_key_for_message(m: dict) -> Tuple[str, dict, str]:
        ex = m.get("extracted") or {}
        pid = ex.get("product_id") if isinstance(ex, dict) else {}
        title = ""
        if isinstance(ex, dict):
            human = ex.get("human") if isinstance(ex.get("human"), dict) else {}
            title = str(human.get("title") or ex.get("title") or "")
        title_key = _norm_title_key(title) or f"msg-{str(m.get('id') or '')}"

        if isinstance(pid, dict) and pid.get("kind") in {"field", "derived"} and pid.get("value"):
            nm = str(pid.get("name") or "ID").strip()
            val = str(pid.get("value") or "").strip()
            key = f"{nm}:{val}"
            return key, pid, title_key
        return f"TITLE:{title_key}", {"kind": "title", "name": "TITLE", "value": title_key}, title_key

    # Upsert items
    for m in new_messages:
        mid = str(m.get("id") or "").strip()
        ts = str(m.get("timestamp") or "").strip()
        item_key, product_id, title_key = _item_key_for_message(m)

        prev = items_by_key.get(item_key)
        if isinstance(prev, dict):
            first_seen = str(prev.get("first_seen_timestamp") or ts or "")
        else:
            first_seen = ts

        items_by_key[item_key] = {
            "item_key": item_key,
            "product_id": product_id,
            "title_key": title_key,
            "first_seen_timestamp": first_seen,
            "last_seen_timestamp": ts,
            "last_message_id": mid,
            "latest": m,
        }

    # Sort newest-first by last_seen_timestamp
    def _sort_key_item(k: str) -> Tuple[str, str]:
        it = items_by_key.get(k) or {}
        ts = str(it.get("last_seen_timestamp") or "").strip()
        return (ts, k)

    keys = sorted(items_by_key.keys(), key=_sort_key_item, reverse=True)
    if max_store > 0 and len(keys) > max_store:
        keep = set(keys[:max_store])
        items_by_key = {k: v for (k, v) in items_by_key.items() if k in keep}
        keys = keys[:max_store]

    out = {
        "channel_key": channel_key,
        "channel_id": int(channel_id),
        "updated_at_unix": time.time(),
        "items_by_key": items_by_key,
        "item_keys_sorted": keys,
    }
    _save_json_file(store_path, out)
    return out


def _parse_int_list(values: object) -> List[int]:
    out: List[int] = []
    if isinstance(values, list):
        for v in values:
            s = str(v or "").strip()
            if not s:
                continue
            try:
                out.append(int(s))
            except Exception:
                continue
    elif isinstance(values, str):
        # allow "1,2,3"
        for part in values.split(","):
            s = str(part or "").strip()
            if not s:
                continue
            try:
                out.append(int(s))
            except Exception:
                continue
    return [i for i in out if int(i) > 0]


def _read_existing_excludes(cfg: dict) -> List[str]:
    raw = cfg.get("rs_fs_monitor_exclude_keys")
    if isinstance(raw, list):
        out = [str(x or "").strip() for x in raw]
        return sorted([x for x in out if x])
    return []


def _format_selection_help() -> str:
    return (
        "Enter exclusions by number/range (based on the list shown).\n"
        "Examples: 3 7 10-15, 2,5,9, 1-4\n"
        "Special: 'none' (exclude nothing), 'all' (exclude all), 'q' (quit without writing)\n"
    )


def _parse_selection(s: str, n: int) -> Tuple[bool, List[int], str]:
    """
    Return (ok, indices_0_based, mode) where mode in {'none','all','pick'}.
    """
    t = str(s or "").strip().lower()
    if not t:
        return True, [], "none"
    if t in {"q", "quit", "exit"}:
        return True, [], "quit"
    if t in {"none", "no", "n"}:
        return True, [], "none"
    if t in {"all", "*"}:
        return True, list(range(0, max(0, int(n)))), "all"

    # normalize separators
    t2 = t.replace(";", ",").replace(" ", ",")
    parts = [p.strip() for p in t2.split(",") if p.strip()]
    picked: List[int] = []
    for p in parts:
        if "-" in p:
            a, b = p.split("-", 1)
            try:
                ia = int(a.strip())
                ib = int(b.strip())
            except Exception:
                return False, [], "pick"
            if ia <= 0 or ib <= 0:
                return False, [], "pick"
            lo = min(ia, ib)
            hi = max(ia, ib)
            for k in range(lo, hi + 1):
                if 1 <= k <= n:
                    picked.append(k - 1)
        else:
            try:
                k = int(p.strip())
            except Exception:
                return False, [], "pick"
            if 1 <= k <= n:
                picked.append(k - 1)
            else:
                return False, [], "pick"
    # unique preserve order
    seen = set()
    out2: List[int] = []
    for i in picked:
        if i in seen:
            continue
        seen.add(i)
        out2.append(i)
    return True, out2, "pick"


def _print_channel_list_grouped(items: List[Tuple[int, str, str, int]]) -> None:
    """
    items: (category_id, category_name, normalized_key, channel_id)
    """
    print("\nChannels found (grouped by category):")
    last_cat_id: Optional[int] = None
    for i, (cat_id, cat_name, k, v) in enumerate(items, start=1):
        if last_cat_id != cat_id:
            safe_cat = (cat_name or f"category-{cat_id}").encode("ascii", "replace").decode("ascii")
            print(f"\n  Category: {safe_cat} ({cat_id})")
            last_cat_id = cat_id
        safe_k = k.encode("ascii", "replace").decode("ascii")
        print(f"    {i:>3}. {safe_k} -> {v}")


def main() -> None:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--report-product-id-fields",
        action="store_true",
        help="Scan RSForwarder/monitor_data/*.json and print which embed field name holds the product identifier (ASIN/SKU/PID/etc) per channel.",
    )
    parser.add_argument(
        "--report-id-coverage",
        action="store_true",
        help="Scan RSForwarder/monitor_data/*.json and list which channels have reliable FIELD/DERIVED ids vs TITLE-only fallback.",
    )
    parser.add_argument(
        "--fetch-recent-messages",
        action="store_true",
        help="Fetch recent messages for each rs_fs_monitor_channel_ids entry and store into RSForwarder/monitor_data/",
    )
    parser.add_argument(
        "--clear-existing",
        action="store_true",
        help="When fetching recent messages, clear existing per-channel JSON files first.",
    )
    parser.add_argument(
        "--messages-limit",
        type=int,
        default=10,
        help="How many recent messages to fetch per channel (default: 10).",
    )
    parser.add_argument(
        "--messages-target-per-channel",
        type=int,
        default=0,
        help="If set (>0), paginates backwards to fetch up to this many messages per channel (uses before=<id>).",
    )
    parser.add_argument(
        "--messages-store-max",
        type=int,
        default=500,
        help="Max unique messages to keep per channel JSON file (default: 500).",
    )
    parser.add_argument(
        "--messages-min-delay-s",
        type=float,
        default=0.0,
        help="Optional pacing delay (seconds) between Discord requests (default: 0).",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=str,
        default="",
        help="Optional checkpoint JSON path (default: RSForwarder/monitor_data/_backfill_checkpoint.json).",
    )
    parser.add_argument(
        "--resume-checkpoint",
        action="store_true",
        help="When doing bulk backfill, resume per-channel pagination from checkpoint if available.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Interactive mode: list channels and allow exclusions before writing config.",
    )
    parser.add_argument(
        "--noninteractive",
        action="store_true",
        help="Non-interactive mode (default): write all channels found under configured categories.",
    )
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    config, config_path, _ = load_config_with_secrets(base)
    guild_id = config.get("guild_id") or config.get("rs_server_guild_id")
    token = (config.get("bot_token") or "").strip()
    if not token:
        print("ERROR: bot_token required in config.secrets.json")
        sys.exit(1)
    if not guild_id:
        print("ERROR: guild_id or rs_server_guild_id required in config.json")
        sys.exit(1)
    guild_id = int(guild_id)

    if bool(args.report_product_id_fields):
        root = base / "monitor_data"
        if not root.exists():
            print(f"ERROR: monitor_data folder not found: {root}")
            sys.exit(1)

        # We only use the newest stored message per channel (message_ids_sorted[0]).
        id_name_re = re.compile(
            r"^(asin|tcin|sku|pid|product\s*id|item\s*#|item\s*no\.?|item\s*number|style|model|upc|ean|isbn|offer\s*id)$",
            re.IGNORECASE,
        )

        def _norm_name(n: str) -> str:
            return re.sub(r"\s+", " ", (n or "").strip())

        files = sorted(root.glob("*.json"), key=lambda p: p.name.lower())
        print(f"Product-id field report (files={len(files)})")
        for p in files:
            data = _load_json_file(p)
            # New format
            keys_sorted = data.get("item_keys_sorted") or []
            if not keys_sorted:
                print(f"- {p.stem}: (no messages)")
                continue
            items = data.get("items_by_key") or {}
            newest_item = items.get(keys_sorted[0]) if isinstance(items, dict) else None
            latest = (newest_item or {}).get("latest") if isinstance(newest_item, dict) else None
            msg = latest if isinstance(latest, dict) else {}
            extracted = msg.get("extracted") or {}
            product_id = extracted.get("product_id") if isinstance(extracted, dict) else None
            if isinstance(product_id, dict) and str(product_id.get("kind") or ""):
                kind = str(product_id.get("kind") or "")
                nm = str(product_id.get("name") or "")
                val = str(product_id.get("value") or "")
                if kind == "field":
                    print(f"- {p.stem}: FIELD:{nm}={val}")
                elif kind == "derived":
                    print(f"- {p.stem}: DERIVED:{nm}={val}")
                else:
                    print(f"- {p.stem}: (no id)")
                continue

            # Back-compat: older stored files might not have product_id yet
            human = ((extracted or {}).get("human") or {}) if isinstance(extracted, dict) else {}
            fields = human.get("fields") or []
            names: List[str] = []
            if isinstance(fields, list):
                for f in fields:
                    if not isinstance(f, dict):
                        continue
                    nm = _norm_name(str(f.get("name") or ""))
                    if nm and id_name_re.match(nm):
                        names.append(nm)
            seen = set()
            names2: List[str] = []
            for n in names:
                k = n.lower()
                if k in seen:
                    continue
                seen.add(k)
                names2.append(n)

            if names2:
                print(f"- {p.stem}: FIELD:{', '.join(names2)}")
            else:
                # Try derive from URL from the stored human/url
                url = str(human.get('url') or '')
                t, v = _derive_product_id_from_url(url)
                if t and v:
                    print(f"- {p.stem}: DERIVED:{t}={v}")
                else:
                    print(f"- {p.stem}: (no id)")
        return

    if bool(args.report_id_coverage):
        root = base / "monitor_data"
        if not root.exists():
            print(f"ERROR: monitor_data folder not found: {root}")
            sys.exit(1)
        files = sorted(root.glob("*.json"), key=lambda p: p.name.lower())
        has_id: List[str] = []
        no_id: List[str] = []

        for p in files:
            data = _load_json_file(p)
            keys_sorted = data.get("item_keys_sorted") or []
            items = data.get("items_by_key") or {}
            if not keys_sorted or not isinstance(items, dict):
                no_id.append(p.stem)
                continue
            newest_item = items.get(keys_sorted[0]) or {}
            latest = newest_item.get("latest") if isinstance(newest_item, dict) else {}
            ex = (latest or {}).get("extracted") or {}
            pid = ex.get("product_id") if isinstance(ex, dict) else {}
            kind = str(pid.get("kind") or "") if isinstance(pid, dict) else ""
            if kind in {"field", "derived"} and str(pid.get("value") or "").strip():
                has_id.append(p.stem)
            else:
                no_id.append(p.stem)

        print("Channels with reliable ids (FIELD/DERIVED):")
        for k in has_id:
            print(f"- {k}")
        print("")
        print("Channels without ids (TITLE fallback):")
        for k in no_id:
            print(f"- {k}")
        return

    # If message fetch mode: use existing configured monitor channels (do not recompute list)
    if bool(args.fetch_recent_messages):
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        mapping = cfg.get("rs_fs_monitor_channel_ids")
        if not isinstance(mapping, dict) or not mapping:
            print("ERROR: rs_fs_monitor_channel_ids missing/empty in RSForwarder/config.json")
            print("Run the channel fetch mode first, then re-run with --fetch-recent-messages.")
            sys.exit(1)

        out_dir = base / "monitor_data"
        limit = int(args.messages_limit or 10)
        limit = max(1, min(100, limit))  # per-request Discord API limit max is 100
        target_total = int(args.messages_target_per_channel or 0)
        target_total = max(0, target_total)
        store_max = int(args.messages_store_max or 500)
        store_max = max(50, min(5000, store_max))
        clear_existing = bool(args.clear_existing)
        min_delay_s = float(args.messages_min_delay_s or 0.0)
        if min_delay_s < 0:
            min_delay_s = 0.0
        if min_delay_s > 10:
            min_delay_s = 10.0

        headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
        mode = "bulk" if target_total > 0 else "recent"
        print(
            f"LIVE FETCH: {mode} messages | guild_id={guild_id} | channels={len(mapping)} | "
            f"per_request_limit={limit} | target_per_channel={target_total or limit} | pace_s={min_delay_s}"
        )

        if clear_existing:
            out_dir.mkdir(parents=True, exist_ok=True)
            cleared = 0
            for p in out_dir.glob("*.json"):
                try:
                    p.unlink()
                    cleared += 1
                except Exception:
                    continue
            print(f"Cleared monitor_data: deleted {cleared} existing *.json file(s).")
            # Also clear the default checkpoint file (fresh run).
            try:
                (out_dir / "_backfill_checkpoint.json").unlink()
            except Exception:
                pass

        checkpoint_path = Path(args.checkpoint_file).expanduser() if str(args.checkpoint_file or "").strip() else (out_dir / "_backfill_checkpoint.json")
        resume_checkpoint = bool(args.resume_checkpoint) and target_total > 0
        ckpt = _load_checkpoint(checkpoint_path) if target_total > 0 else {}
        if target_total > 0:
            ckpt.setdefault("schema", "backfill_checkpoint_v1")
            ckpt.setdefault("updated_at_unix", time.time())
            ckpt.setdefault("channels", {})
            if resume_checkpoint:
                print(f"Checkpoint: resume enabled -> {checkpoint_path}")
            else:
                print(f"Checkpoint: writing progress -> {checkpoint_path}")

        written_files = 0
        for channel_key, channel_id in sorted(mapping.items(), key=lambda kv: str(kv[0]).lower()):
            try:
                ch_id_int = int(channel_id)
            except Exception:
                continue
            # Fetch messages (recent or bulk backfill)
            fetched_total = 0
            before_id: Optional[str] = None
            msgs_all: List[dict] = []

            # Resume cursor (bulk only)
            if resume_checkpoint and isinstance(ckpt.get("channels"), dict):
                prev = ckpt["channels"].get(str(channel_key)) or {}
                if isinstance(prev, dict):
                    before_prev = str(prev.get("before_id") or "").strip()
                    fetched_prev = int(prev.get("fetched_total") or 0)
                    if before_prev:
                        before_id = before_prev
                        fetched_total = max(0, fetched_prev)
                        print(f"\n(resume) {channel_key}: before={before_id} fetched={fetched_total}/{target_total}")

            while True:
                if target_total > 0 and fetched_total >= target_total:
                    break
                req_limit = limit
                if target_total > 0:
                    req_limit = min(100, max(1, target_total - fetched_total))

                url = f"{DISCORD_API}/channels/{ch_id_int}/messages?limit={req_limit}"
                if before_id:
                    url += f"&before={before_id}"

                try:
                    msgs = _discord_get_json(url, headers)
                except Exception as e:
                    print(f"  ERROR: {channel_key} ({ch_id_int}) -> {type(e).__name__}: {e}")
                    break
                if not isinstance(msgs, list):
                    print(f"  ERROR: {channel_key} ({ch_id_int}) -> unexpected response (not a list)")
                    break
                if not msgs:
                    break

                # accumulate
                for m in msgs:
                    if isinstance(m, dict):
                        msgs_all.append(m)
                fetched_total += len(msgs)

                # next page: before oldest id in this page
                oldest = msgs[-1]
                oid = str(oldest.get("id") or "").strip() if isinstance(oldest, dict) else ""
                if not oid or oid == before_id:
                    break
                before_id = oid

                # checkpoint update (bulk only)
                if target_total > 0 and isinstance(ckpt.get("channels"), dict):
                    ckpt["channels"][str(channel_key)] = {
                        "channel_id": ch_id_int,
                        "before_id": before_id,
                        "fetched_total": int(fetched_total),
                        "updated_at_unix": time.time(),
                    }
                    ckpt["updated_at_unix"] = time.time()
                    _save_checkpoint(checkpoint_path, ckpt)

                # pacing (optional)
                if min_delay_s > 0:
                    time.sleep(min_delay_s)

                # progress (bulk only)
                if target_total > 0 and fetched_total % 500 == 0:
                    print(f"  ... {channel_key}: fetched {fetched_total}/{target_total}")

            if not msgs_all:
                print(f"  OK: {channel_key} -> no messages fetched")
                # still write an empty file for consistency
                store_path = out_dir / f"{channel_key}.json"
                out = _upsert_channel_messages(
                    store_path,
                    channel_key=str(channel_key),
                    channel_id=ch_id_int,
                    new_messages=[],
                    max_store=store_max,
                )
                written_files += 1
                continue

            stored: List[dict] = []
            # Only process up to target_total in case Discord returned more than requested
            msgs_to_process = msgs_all[:target_total] if target_total > 0 else msgs_all
            for m in msgs_to_process:
                if not isinstance(m, dict):
                    continue
                title, primary_url = _extract_primary_title_and_url(m)
                human = _extract_embed_human_summary(m)
                product_id = _extract_product_id(m)
                stored.append(
                    {
                        "id": str(m.get("id") or ""),
                        "timestamp": str(m.get("timestamp") or ""),
                        "author": {
                            "id": str((m.get("author") or {}).get("id") or ""),
                            "username": str((m.get("author") or {}).get("username") or ""),
                        },
                        "content": str(m.get("content") or ""),
                        "embeds": m.get("embeds") if isinstance(m.get("embeds"), list) else [],
                        "attachments": m.get("attachments") if isinstance(m.get("attachments"), list) else [],
                        "extracted": {
                            "title": title,
                            "primary_url": primary_url,
                            "human": human,
                            "product_id": product_id,
                        },
                    }
                )

            store_path = out_dir / f"{channel_key}.json"
            out = _upsert_channel_messages(
                store_path,
                channel_key=str(channel_key),
                channel_id=ch_id_int,
                new_messages=stored,
                max_store=store_max,
            )
            written_files += 1

            # Print quick preview of newest item
            first_key = (out.get("item_keys_sorted") or [None])[0]
            if first_key and isinstance(out.get("items_by_key"), dict):
                first_item = out["items_by_key"].get(first_key) or {}
                first = first_item.get("latest") if isinstance(first_item, dict) else {}
                ex = (first or {}).get("extracted") or {}
                human = ex.get("human") if isinstance(ex.get("human"), dict) else {}
                pid = ex.get("product_id") if isinstance(ex, dict) else {}
                safe_title = str(human.get("title") or ex.get("title") or "").encode("ascii", "replace").decode("ascii")
                raw_url = str(human.get("url") or ex.get("primary_url") or "")
                safe_url = _shorten_url_for_console(raw_url, max_len=110).encode("ascii", "replace").decode("ascii")

                print("")
                print("=" * 78)
                print(f"CHANNEL: {channel_key}  (id={ch_id_int})  ->  file={store_path.name}")
                print("=" * 78)
                if target_total > 0:
                    print(f"fetched: {min(fetched_total, target_total)}/{target_total} (stored_items={len(out.get('item_keys_sorted') or [])})")
                else:
                    print(f"fetched: {len(msgs_to_process)} (stored_items={len(out.get('item_keys_sorted') or [])})")
                print(f"product: {safe_title[:160]}")
                if safe_url:
                    print(f"url:     {safe_url}")
                # Always show an ID line for quick scanning.
                if isinstance(pid, dict):
                    kind = str(pid.get("kind") or "")
                    nm = str(pid.get("name") or "")
                    val = str(pid.get("value") or "")
                    if kind in {"field", "derived"} and val:
                        tag = "FIELD" if kind == "field" else "DERIVED"
                        label = nm or ("ID" if kind == "derived" else "FIELD")
                        print(f"id:      {tag}:{label}={val}")
                    elif kind == "title" and val:
                        print(f"id:      TITLE:{val}")
                    else:
                        print("id:      NONE")
                else:
                    print("id:      NONE")
                if isinstance(human, dict):
                    desc = str(human.get('description') or '').encode('ascii', 'replace').decode('ascii')
                    if desc:
                        print(f"desc:    {desc[:220]}")
                    fields = human.get("fields") if isinstance(human.get("fields"), list) else []
                    if fields:
                        # Prioritize key fields; drop noisy long ones.
                        print("fields:")
                        shown = 0
                        for f in fields:
                            if not isinstance(f, dict):
                                continue
                            nm = str(f.get("name") or "").encode("ascii", "replace").decode("ascii")
                            val = str(f.get("value") or "").encode("ascii", "replace").decode("ascii")
                            if _is_noisy_field(nm):
                                continue
                            if nm or val:
                                v2 = _shorten_markdown_links_for_console(val, max_len=150)
                                print(f"  - {nm}: {v2}")
                                shown += 1
                                if shown >= 12:
                                    break
            else:
                print(f"  OK: {channel_key} -> wrote {store_path.name}")

        print(f"Done. Wrote {written_files} file(s) into {out_dir}")
        return

    # Category IDs under which to list channels (from config)
    category_ids = _parse_int_list((config or {}).get("rs_fs_monitor_category_ids"))
    if not category_ids:
        print("ERROR: rs_fs_monitor_category_ids missing/empty in RSForwarder/config.json")
        print("Add it as a JSON array of category IDs, e.g. [1350..., 1411..., ...]")
        sys.exit(1)

    url = f"{DISCORD_API}/guilds/{guild_id}/channels"
    headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
    channels = _discord_get_json(url, headers, timeout_s=15.0, max_tries=5)
    if not isinstance(channels, list):
        print("ERROR: Unexpected API response (not a list)")
        sys.exit(1)

    # Build category id->name from the live guild channel list
    category_name_by_id: Dict[int, str] = {}
    for ch in channels:
        if not isinstance(ch, dict):
            continue
        if ch.get("type") != 4:  # 4 = GUILD_CATEGORY
            continue
        cid = ch.get("id")
        try:
            cid_i = int(cid)
        except (TypeError, ValueError):
            continue
        name = str(ch.get("name") or "").strip()
        if name:
            category_name_by_id[cid_i] = name

    # All text channels (type 0) under the given categories — no filter by "monitor"
    found: dict[str, int] = {}
    found_category_by_key: dict[str, int] = {}
    for ch in channels:
        if not isinstance(ch, dict):
            continue
        if ch.get("type") != 0:  # 0 = GUILD_TEXT
            continue
        parent_id = ch.get("parent_id")
        if parent_id is None:
            continue
        try:
            parent_id = int(parent_id)
        except (TypeError, ValueError):
            continue
        if parent_id not in category_ids:
            continue
        name = (ch.get("name") or "").strip()
        ch_id = ch.get("id")
        try:
            ch_id = int(ch_id)
        except (TypeError, ValueError):
            continue
        key = normalize_channel_key(name)
        # Keep every channel; banners (e.g. ------) normalize to empty — use unique key so we don't overwrite
        if not key:
            key = f"channel-{ch_id}"
        found[key] = ch_id
        found_category_by_key[key] = parent_id

    # Load config.json for write (do not touch secrets)
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    existing_excludes = _read_existing_excludes(cfg)

    # Present list in interactive mode for exclusions
    # Sort by category name then normalized key for stable grouping
    def _cat_sort_key(k: str) -> Tuple[str, str]:
        cat_id = int(found_category_by_key.get(k) or 0)
        cat_name = category_name_by_id.get(cat_id) or f"category-{cat_id}"
        return (cat_name.lower(), k.lower())

    items_grouped: List[Tuple[int, str, str, int]] = []
    for k, v in found.items():
        cat_id = int(found_category_by_key.get(k) or 0)
        cat_name = category_name_by_id.get(cat_id) or f"category-{cat_id}"
        items_grouped.append((cat_id, cat_name, k, v))
    items_grouped.sort(key=lambda t: ((t[1] or "").lower(), (t[2] or "").lower()))

    exclude_keys: List[str] = []
    if bool(args.interactive) and (not bool(args.noninteractive)):
        if existing_excludes:
            print(f"Existing excludes in config ({len(existing_excludes)}): {', '.join(existing_excludes[:12])}{' ...' if len(existing_excludes) > 12 else ''}")
        _print_channel_list_grouped(items_grouped)
        print("")
        print(_format_selection_help())
        while True:
            raw = input("Exclude selection> ").strip()
            ok_sel, idxs, mode = _parse_selection(raw, len(items_grouped))
            if not ok_sel:
                print("Invalid selection. Try again.\n")
                continue
            if mode == "quit":
                print("Quit: no changes written.")
                return
            if mode == "none":
                exclude_keys = []
            else:
                exclude_keys = [items_grouped[i][2] for i in idxs if 0 <= i < len(items_grouped)]
            break
        # Also allow carrying forward existing excludes that are still present (optional)
        # If user picked none/all/pick, we treat that as the authoritative list.
        print(f"Excluding {len(exclude_keys)} channel(s).")
    else:
        # Non-interactive mode respects existing configured excludes (if any).
        exclude_keys = list(existing_excludes)

    exclude_set = {k for k in exclude_keys if k}
    filtered = {k: v for (_, _, k, v) in items_grouped if k not in exclude_set}

    cfg["rs_fs_monitor_category_ids"] = [str(i) for i in category_ids]
    cfg["rs_fs_monitor_channel_ids"] = filtered
    # Only interactive mode modifies the configured exclusions.
    if bool(args.interactive) and (not bool(args.noninteractive)):
        cfg["rs_fs_monitor_exclude_keys"] = sorted(list(exclude_set))
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    print(f"Updated {config_path} with rs_fs_monitor_category_ids and rs_fs_monitor_channel_ids.")

    print(f"Guild ID: {guild_id}")
    print(f"Categories: {category_ids}")
    print(f"Channels found: {len(found)} | written: {len(filtered)} | excluded: {len(exclude_set)}")
    if bool(args.interactive) and exclude_set:
        shown = sorted(list(exclude_set))
        print(f"Excluded keys ({len(shown)}): {', '.join(shown[:20])}{' ...' if len(shown) > 20 else ''}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise
    except EOFError:
        # Common when a .bat is double-clicked and stdin closes unexpectedly.
        print("\nERROR: Input stream closed (EOF) while waiting for selection.")
        print("Tip: run from a terminal (PowerShell/cmd) or re-run the .bat and try again.")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: Unexpected failure: {type(e).__name__}: {e}")
        raise

