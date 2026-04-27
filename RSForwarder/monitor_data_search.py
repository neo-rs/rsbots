"""
Search RSForwarder/monitor_data/*.json for a product id (ASIN, SKU, TCIN, UPC, etc.) or substring.

CLI:
  py -3 -m RSForwarder.monitor_data_search B0DN4LQL4Y
  py -3 -m RSForwarder.monitor_data_search --dir RSForwarder/monitor_data "6665448" --limit 20

Interactive: use RSForwarder/run_monitor_data_search.bat (no args).
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[misc, assignment]


def _clean_ws(s: str) -> str:
    t = str(s or "")
    t = t.replace("\u200b", "").replace("\u200c", "").replace("\ufeff", "")
    return t.strip()


def _strip_md_links(s: str, *, max_len: int = 400) -> str:
    t = str(s or "")
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > max_len:
        return t[: max_len - 3] + "..."
    return t


def _norm_alnum(s: str) -> str:
    return "".join(c for c in (s or "").lower() if c.isalnum())


def _parse_ts(raw: str) -> Optional[datetime]:
    s = (raw or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _discord_relative_style(now: datetime, then: datetime) -> str:
    """Approximate Discord <t:...:R> style (not locale-identical, but readable)."""
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    delta = (then - now).total_seconds()
    future = delta > 0
    sec = abs(int(delta))

    def one(u: int, name: str) -> str:
        n = max(1, sec // u)
        label = name + ("s" if n != 1 else "")
        if future:
            return f"in {n} {label}"
        return f"{n} {label} ago"

    if sec < 45:
        return "in a few seconds" if future else "a few seconds ago"
    if sec < 90:
        return "in a minute" if future else "a minute ago"
    if sec < 3600:
        return one(60, "minute")
    if sec < 86400:
        return one(3600, "hour")
    if sec < 604800:
        return one(86400, "day")
    if sec < 2592000:
        return one(604800, "week")
    return one(2592000, "month")


def _format_et(dt: datetime) -> str:
    """e.g. 04/26/2026 2:00am ET (America/New_York)."""
    if ZoneInfo is None:
        return dt.astimezone(timezone.utc).strftime("%m/%d/%Y %I:%M%p UTC")
    et = dt.astimezone(ZoneInfo("America/New_York"))
    h12 = et.hour % 12
    if h12 == 0:
        h12 = 12
    ampm = "am" if et.hour < 12 else "pm"
    return f"{et.month:02d}/{et.day:02d}/{et.year} {h12}:{et.minute:02d}{ampm} ET"


def _human_field_pairs(human: Dict[str, Any]) -> List[Tuple[str, str]]:
    """(field_name_lower, field_value) for embed human summary fields."""
    out: List[Tuple[str, str]] = []
    fields = human.get("fields") if isinstance(human.get("fields"), list) else []
    for f in fields:
        if not isinstance(f, dict):
            continue
        name = _clean_ws(str(f.get("name") or "")).lower()
        raw_val = str(f.get("value") or "")
        val = _clean_ws(raw_val)
        # Allow numeric "0" / short emoji-only values
        if not val and raw_val.strip() != "0":
            continue
        if not val and raw_val.strip() == "0":
            val = "0"
        out.append((name, val))
    return out


def _stock_value_for_console(val: str) -> str:
    """
    Normalize common stock indicators for Windows consoles (ASCII-friendly),
    while keeping numeric / text values readable.
    """
    v = _clean_ws(val)
    if not v:
        return ""
    # Green / red circle indicators (some monitors use emoji instead of digits)
    if "\U0001f7e2" in v and len(v) <= 8:
        return "in stock (green circle)"
    if "\U0001f7e9" in v and len(v) <= 8:
        return "in stock (green square)"
    if "\U0001f534" in v and len(v) <= 8:
        return "out/low stock (red circle)"
    return _strip_md_links(v, max_len=500)


def _topps_variant_stock(val: str) -> str:
    """
    Topps-style: 'Default Title [1+] / 47692727287965' -> prefer bracket after title as stock hint.
    """
    v = str(val or "")
    m = re.search(r"\[([^\]]+)\]\s*/", v)
    if m:
        inner = m.group(1).strip()
        if inner:
            return inner
    m2 = re.search(r"\[([^\]]+)\]", v)
    if m2:
        return m2.group(1).strip()
    return _stock_value_for_console(v)


def _extract_type_field(human: Dict[str, Any]) -> str:
    """Embed field named 'Type' (Restock / New Product / Restock (2), etc.)."""
    for name_l, val in _human_field_pairs(human):
        if name_l == "type":
            return _strip_md_links(val, max_len=240)
    return ""


def _extract_stock_display(human: Dict[str, Any]) -> str:
    """
    Best-effort stock / inventory line. Field **names** drive matching (values vary by monitor).
    Covers: Stock (numeric or emoji), Stock Locked, Total Stock, Inventory, Confirmed Stock,
    Product Status, Variants [Stock] / Id, Cart Limit, Sizes/Stock, etc.
    """
    pairs = _human_field_pairs(human)
    if not pairs:
        return ""

    skip_names = {
        "price",
        "seller",
        "asin",
        "tcin",
        "sku",
        "pid",
        "upc",
        "ean",
        "isbn",
        "offer id",
        "links",
        "one click checkout",
        "type",
    }

    def first_match(pred: Callable[[str, str], bool]) -> Tuple[str, str]:
        for name_l, val in pairs:
            if name_l in skip_names:
                continue
            if pred(name_l, val):
                return name_l, val
        return "", ""

    rules: List[Tuple[str, Callable[[str, str], bool]]] = [
        ("confirmed_stock", lambda n, v: "confirmed" in n and "stock" in n),
        ("stock_locked", lambda n, v: "stock" in n and "locked" in n),
        ("total_stock", lambda n, v: "total" in n and "stock" in n),
        ("stock_exact", lambda n, v: n == "stock"),
        ("inventory", lambda n, v: n == "inventory"),
        ("variants_stock", lambda n, v: "variant" in n and "stock" in n),
        ("cart_limit", lambda n, v: "cart" in n and "limit" in n),
        ("product_status", lambda n, v: n == "product status"),
        ("sizes_stock", lambda n, v: "size" in n and "stock" in n),
        ("availability", lambda n, v: "availability" in n),
        ("in_out_stock", lambda n, v: "in stock" in n or "out of stock" in n),
        ("stock_general", lambda n, v: "stock" in n),
    ]

    for label, pred in rules:
        _n, raw = first_match(pred)
        if not raw:
            continue
        if label == "variants_stock":
            return _topps_variant_stock(raw) or _stock_value_for_console(raw)
        return _stock_value_for_console(raw)
    return ""


def _title_url_from_item(item: Dict[str, Any]) -> Tuple[str, str]:
    latest = item.get("latest") if isinstance(item.get("latest"), dict) else {}
    ex = latest.get("extracted") if isinstance(latest.get("extracted"), dict) else {}
    human = ex.get("human") if isinstance(ex.get("human"), dict) else {}
    title = str(human.get("title") or ex.get("title") or "").strip()
    url = str(human.get("url") or ex.get("primary_url") or "").strip()
    return title, url


def _item_matches(query: str, item_key: str, item: Dict[str, Any]) -> bool:
    q = (query or "").strip()
    if not q:
        return False
    q_lower = q.lower()
    q_digits = "".join(c for c in q if c.isdigit())
    q_alnum = _norm_alnum(q)

    pid = item.get("product_id") if isinstance(item.get("product_id"), dict) else {}
    pid_val = str(pid.get("value") or "").strip()
    pid_digits = "".join(c for c in pid_val if c.isdigit())

    title, _ = _title_url_from_item(item)
    title_key = str(item.get("title_key") or "").strip()

    if q_lower == (item_key or "").lower():
        return True
    if q_alnum and q_alnum in _norm_alnum(item_key):
        return True
    if pid_val and (q_lower == pid_val.lower() or q_alnum == _norm_alnum(pid_val)):
        return True
    if q_digits and len(q_digits) >= 4 and pid_digits and q_digits == pid_digits:
        return True
    if title and q_lower in title.lower():
        return True
    if title_key and q_alnum and q_alnum in _norm_alnum(title_key):
        return True
    return False


def _load_channel_json(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8", errors="replace") or "{}")
    except Exception:
        return path.stem, {}
    if not isinstance(obj, dict):
        return path.stem, {}
    ck = str(obj.get("channel_key") or path.stem).strip() or path.stem
    return ck, obj


def search_monitor_data(
    *,
    monitor_dir: Path,
    query: str,
    limit: int,
    exclude_channels: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    hits: List[Dict[str, Any]] = []
    if not monitor_dir.is_dir():
        return hits
    excl = exclude_channels or set()

    for path in sorted(monitor_dir.glob("*.json")):
        if path.name.startswith("_"):
            continue
        channel_key, obj = _load_channel_json(path)
        if channel_key in excl:
            continue
        # Saved file metadata
        try:
            channel_id = int(obj.get("channel_id") or 0) if isinstance(obj, dict) else 0
        except Exception:
            channel_id = 0
        items = obj.get("items_by_key")
        if not isinstance(items, dict):
            continue
        for ik, item in items.items():
            if not isinstance(item, dict):
                continue
            if not _item_matches(query, str(ik), item):
                continue
            title, url = _title_url_from_item(item)
            latest = item.get("latest") if isinstance(item.get("latest"), dict) else {}
            ex = latest.get("extracted") if isinstance(latest.get("extracted"), dict) else {}
            human = ex.get("human") if isinstance(ex.get("human"), dict) else {}
            last_raw = str(item.get("last_seen_timestamp") or latest.get("timestamp") or "").strip()
            dt = _parse_ts(last_raw)
            stock = _clean_ws(_extract_stock_display(human))
            embed_type = _clean_ws(_extract_type_field(human))
            source_message_id = str(item.get("last_message_id") or latest.get("id") or "").strip()
            pid = item.get("product_id") if isinstance(item.get("product_id"), dict) else {}
            pid_label = ""
            if str(pid.get("kind") or "") in {"field", "derived"} and str(pid.get("value") or "").strip():
                pid_label = f"{pid.get('name') or 'ID'}={pid.get('value')}"
            elif str(pid.get("kind") or "") == "title":
                pid_label = f"TITLE={pid.get('value') or ''}"

            unix = int(dt.timestamp()) if dt else 0
            hits.append(
                {
                    "channel": channel_key,
                    "file": path.name,
                    "item_key": str(ik),
                    "source_channel_id": channel_id,
                    "source_message_id": source_message_id,
                    "product_id": pid_label,
                    "title": title,
                    "url": url,
                    "stock": stock,
                    "embed_type": embed_type,
                    "last_seen_iso": last_raw,
                    "last_seen_unix": unix,
                }
            )
            if len(hits) >= limit:
                return hits
    return hits


def _print_hits(hits: List[Dict[str, Any]], *, as_json: bool) -> None:
    now = datetime.now(timezone.utc)
    if as_json:
        print(json.dumps({"hits": hits, "count": len(hits)}, indent=2, ensure_ascii=False))
        return
    if not hits:
        print("No hits.")
        return
    print(f"Hits: {len(hits)}\n")
    for i, h in enumerate(hits, 1):
        dt = _parse_ts(str(h.get("last_seen_iso") or ""))
        _na = "-"
        rel = _discord_relative_style(now, dt) if dt else _na
        readable = _format_et(dt) if dt else _na
        unix = int(h.get("last_seen_unix") or 0)
        disc = f"<t:{unix}:R>" if unix else _na
        print("=" * 72)
        print(f"{i}) channel={h.get('channel')}  file={h.get('file')}")
        print(f"    item_key={h.get('item_key')}")
        print(f"    id={h.get('product_id') or 'NONE'}")
        print(f"    title={h.get('title') or _na}")
        print(f"    url={h.get('url') or _na}")
        st = _clean_ws(str(h.get("stock") or ""))
        print(f"    stock={st if st else _na}")
        et = _clean_ws(str(h.get("embed_type") or ""))
        print(f"    type={et if et else _na}")
        print(f"    last_seen={readable}  relative={rel}  discord={disc}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Search monitor_data JSON across all monitor channels.")
    ap.add_argument("query", nargs="?", default="", help="Product id / ASIN / SKU / substring")
    ap.add_argument(
        "--dir",
        default="",
        help="monitor_data directory (default: RSForwarder/monitor_data next to this file)",
    )
    ap.add_argument("--limit", type=int, default=50, help="Max hits to return (default 50)")
    ap.add_argument("--json", action="store_true", help="Machine-readable JSON output")
    ap.add_argument(
        "--include-aio",
        action="store_true",
        help="Include aggregate AIO channels like needoh-aio (excluded by default to reduce duplicates).",
    )
    args = ap.parse_args(argv)

    q = (args.query or "").strip()
    if not q:
        print("Usage: py -3 -m RSForwarder.monitor_data_search <query> [--dir PATH] [--limit N] [--json]")
        return 2

    base = Path(__file__).resolve().parent
    root = Path(args.dir).resolve() if args.dir else (base / "monitor_data")
    default_excludes = {"needoh-aio"}
    excludes = set() if bool(args.include_aio) else set(default_excludes)
    hits = search_monitor_data(
        monitor_dir=root,
        query=q,
        limit=max(1, min(int(args.limit or 50), 500)),
        exclude_channels=excludes,
    )
    _print_hits(hits, as_json=bool(args.json))
    return 0 if hits else 1


if __name__ == "__main__":
    raise SystemExit(main())
