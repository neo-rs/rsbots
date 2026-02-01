"""
Local debug helper: monitor lookup via Discord REST (Bot token).

Why: Discord bots cannot "search" messages. This script scans history with pagination
until it finds a matching SKU in embed fields, then prints:
  - the Discord jump link to the matching message
  - extracted title and product URL

It uses channel IDs produced by `!rsfsmonitorscan` (stored in RSForwarder/config.json):
  rs_fs_monitor_channel_ids: { "walmart-monitor": 123..., ... }

Usage examples (run from repo root):
  # Use config.json mapping created by !rsfsmonitorscan:
  py -3 scripts/rsfs_monitor_lookup_local.py --store walmart --sku 15558409905 --max-messages 2000

  # Or use explicit channel id (does NOT require rs_fs_monitor_channel_ids):
  py -3 scripts/rsfs_monitor_lookup_local.py --channel-id 1411756672891748422 --guild-id 876528050081251379 --sku 15558409905 --max-messages 5000
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import requests


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_cfg() -> dict:
    root = _repo_root()
    cfg_path = root / "RSForwarder" / "config.json"
    return json.loads(cfg_path.read_text(encoding="utf-8"))


def _load_secrets() -> dict:
    root = _repo_root()
    sec_path = root / "RSForwarder" / "config.secrets.json"
    return json.loads(sec_path.read_text(encoding="utf-8"))


def _normalize_monitor_channel_name(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return ""
    s = s.replace("┃", "|").replace("│", "|").replace("丨", "|")
    parts = [p.strip() for p in re.split(r"[|]+", s) if p.strip()]
    if parts:
        s = parts[-1]
    s = re.sub(r"^[^a-z0-9]+", "", s)
    return s


def _monitor_channel_base_for_store(store: str) -> str:
    s = (store or "").strip().lower()
    mapping = {
        "amazon": "amazon-monitor",
        "walmart": "walmart-monitor",
        "target": "target-monitor",
        "lowes": "lowes-monitor",
        "gamestop": "gamestop-monitor",
        "costco": "costco-monitor",
        "bestbuy": "bestbuy-monitor",
        "homedepot": "homedepot-monitor",
        "topps": "topps-monitor",
        "funko": "funkopop-monitor",
        "funkopop": "funkopop-monitor",
    }
    for k, v in mapping.items():
        if k in s:
            return v
    return ""


def _clean_sku(value: str) -> str:
    s = (value or "").strip().strip("`").strip()
    return "".join([c for c in s if c.isalnum() or c in {"-", "_"}]).strip().lower()


def _first_url(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    m = re.search(r"(https?://[^\s<>()]+)", t)
    return (m.group(1) or "").strip() if m else ""


def _id_like_field_name(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return False
    hints = ("sku", "pid", "tcin", "asin", "upc", "item", "product", "model", "mpn", "id")
    return any(h in n for h in hints)


def _extract_title_url_from_embed(embed: dict) -> Tuple[str, str]:
    title = str((embed or {}).get("title") or "").strip()
    url = str((embed or {}).get("url") or "").strip()
    fields = (embed or {}).get("fields") or []
    if not url and isinstance(fields, list):
        for f in fields:
            url = _first_url(str((f or {}).get("value") or ""))
            if url:
                break
    if not url:
        url = _first_url(str((embed or {}).get("description") or ""))
    if not title:
        title = url or ""
    return title, url


def _embed_matches_sku(embed: dict, sku: str) -> bool:
    target = _clean_sku(sku)
    if not target:
        return False
    target_digits = "".join([c for c in target if c.isdigit()])
    fields = (embed or {}).get("fields") or []
    if not isinstance(fields, list):
        fields = []

    def _value_matches(v: str) -> bool:
        vc = _clean_sku(v)
        if not vc:
            return False
        if vc == target:
            return True
        if target_digits:
            vd = "".join([c for c in vc if c.isdigit()])
            if vd and vd == target_digits and len(target_digits) >= 6:
                return True
        return False

    # Pass 1: ID-like fields
    for f in fields:
        n = str((f or {}).get("name") or "").strip()
        v = str((f or {}).get("value") or "").strip()
        if not (n and v):
            continue
        if not _id_like_field_name(n):
            continue
        if _value_matches(v):
            return True

    # Pass 2: any value that looks like an ID
    for f in fields:
        v = str((f or {}).get("value") or "").strip()
        if not v:
            continue
        vc = _clean_sku(v)
        if len(vc) < 6:
            continue
        if _value_matches(v):
            return True

    # Pass 3: blob search
    blob = " ".join(
        [
            str((embed or {}).get("title") or ""),
            str((embed or {}).get("description") or ""),
            " ".join([str((f or {}).get("name") or "") + " " + str((f or {}).get("value") or "") for f in fields]),
        ]
    )
    return target in _clean_sku(blob)


def _iter_messages(
    token: str,
    channel_id: int,
    *,
    max_messages: int,
    sleep_s: float = 0.35,
) -> Iterable[dict]:
    headers = {"Authorization": f"Bot {token}"}
    base = f"https://discord.com/api/v10/channels/{int(channel_id)}/messages"
    fetched = 0
    before = None
    while fetched < max_messages:
        limit = min(100, max_messages - fetched)
        params = {"limit": str(limit)}
        if before:
            params["before"] = str(before)
        r = requests.get(base, headers=headers, params=params, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            return
        for msg in batch:
            yield msg
        fetched += len(batch)
        before = batch[-1].get("id")
        time.sleep(float(sleep_s))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", required=False, default="")
    ap.add_argument("--sku", required=True)
    ap.add_argument("--max-messages", type=int, default=2000)
    ap.add_argument("--channel-id", type=int, default=0, help="Monitor channel id to scan (optional; overrides --store mapping)")
    ap.add_argument("--guild-id", type=int, default=0, help="Guild id used to build jump links (optional)")
    ap.add_argument("--sleep-s", type=float, default=0.25, help="Sleep between pages (seconds)")
    args = ap.parse_args()

    cfg = _load_cfg()
    sec = _load_secrets()
    token = str(sec.get("bot_token") or "").strip()
    if not token:
        print("missing bot_token in RSForwarder/config.secrets.json", file=sys.stderr)
        return 2

    guild_id = int(args.guild_id or 0) or int(cfg.get("guild_id") or 0)

    channel_id = int(args.channel_id or 0)
    base = ""
    if not channel_id:
        base = _monitor_channel_base_for_store(args.store)
        if not base:
            print("Provide either --channel-id OR a supported --store.", file=sys.stderr)
            return 2

        mapping = cfg.get("rs_fs_monitor_channel_ids")
        if not isinstance(mapping, dict) or not mapping:
            print("missing rs_fs_monitor_channel_ids in RSForwarder/config.json (run !rsfsmonitorscan first)", file=sys.stderr)
            return 2

        channel_id = int(mapping.get(base) or 0)
        if not channel_id:
            # try normalized keys
            norm_map = {_normalize_monitor_channel_name(k): int(v) for k, v in mapping.items() if str(k).strip()}
            channel_id = int(norm_map.get(base) or 0)
        if not channel_id:
            print(f"no channel_id found for {base!r} in rs_fs_monitor_channel_ids", file=sys.stderr)
            return 2

    if not guild_id:
        print("missing guild_id (pass --guild-id or set RSForwarder/config.json guild_id)", file=sys.stderr)
        return 2

    store_label = (args.store or "").strip() or "(channel-id-mode)"
    print(f"store={store_label} base_channel={base or '(n/a)'} channel_id={channel_id} sku={args.sku} max_messages={args.max_messages}")

    checked = 0
    for msg in _iter_messages(token, channel_id, max_messages=args.max_messages, sleep_s=float(args.sleep_s)):
        checked += 1
        embeds = msg.get("embeds") or []
        if not isinstance(embeds, list) or not embeds:
            continue
        for e in embeds:
            if not isinstance(e, dict):
                continue
            if _embed_matches_sku(e, args.sku):
                title, url = _extract_title_url_from_embed(e)
                mid = int(msg.get("id") or 0)
                jump = f"https://discord.com/channels/{guild_id}/{channel_id}/{mid}" if mid else ""
                print("HIT")
                print("jump:", jump)
                print("title:", title)
                print("url:", url)
                return 0

    print(f"MISS (scanned_messages={checked})")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

