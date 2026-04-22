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


def _upsert_channel_messages(store_path: Path, *, channel_key: str, channel_id: int, new_messages: List[dict], max_store: int) -> dict:
    """
    Store format:
      {
        "channel_key": "...",
        "channel_id": 123,
        "updated_at_unix": 123.4,
        "messages_by_id": { "<id>": { ...minimal... } },
        "message_ids_sorted": ["<id>", ...]   # newest-first
      }
    """
    data = _load_json_file(store_path)
    messages_by_id = data.get("messages_by_id")
    if not isinstance(messages_by_id, dict):
        messages_by_id = {}

    # Upsert
    for m in new_messages:
        mid = str(m.get("id") or "").strip()
        if not mid:
            continue
        messages_by_id[mid] = m

    # Sort newest-first by timestamp when available, else by id as fallback
    def _sort_key(mid: str) -> Tuple[str, str]:
        mm = messages_by_id.get(mid) or {}
        ts = str(mm.get("timestamp") or "").strip()
        return (ts, mid)

    ids = sorted(messages_by_id.keys(), key=_sort_key, reverse=True)
    if max_store > 0 and len(ids) > max_store:
        keep = set(ids[:max_store])
        messages_by_id = {k: v for (k, v) in messages_by_id.items() if k in keep}
        ids = ids[:max_store]

    out = {
        "channel_key": channel_key,
        "channel_id": int(channel_id),
        "updated_at_unix": time.time(),
        "messages_by_id": messages_by_id,
        "message_ids_sorted": ids,
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
        "--fetch-recent-messages",
        action="store_true",
        help="Fetch recent messages for each rs_fs_monitor_channel_ids entry and store into RSForwarder/monitor_data/",
    )
    parser.add_argument(
        "--messages-limit",
        type=int,
        default=10,
        help="How many recent messages to fetch per channel (default: 10).",
    )
    parser.add_argument(
        "--messages-store-max",
        type=int,
        default=500,
        help="Max unique messages to keep per channel JSON file (default: 500).",
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
        limit = max(1, min(100, limit))  # Discord API limit max is 100
        store_max = int(args.messages_store_max or 500)
        store_max = max(50, min(5000, store_max))

        headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
        print(f"LIVE FETCH: recent messages | guild_id={guild_id} | channels={len(mapping)} | limit={limit}")
        written_files = 0
        for channel_key, channel_id in sorted(mapping.items(), key=lambda kv: str(kv[0]).lower()):
            try:
                ch_id_int = int(channel_id)
            except Exception:
                continue
            url = f"{DISCORD_API}/channels/{ch_id_int}/messages?limit={limit}"
            try:
                msgs = _discord_get_json(url, headers)
            except Exception as e:
                print(f"  ERROR: {channel_key} ({ch_id_int}) -> {type(e).__name__}: {e}")
                continue
            if not isinstance(msgs, list):
                print(f"  ERROR: {channel_key} ({ch_id_int}) -> unexpected response (not a list)")
                continue

            stored: List[dict] = []
            for m in msgs:
                if not isinstance(m, dict):
                    continue
                title, primary_url = _extract_primary_title_and_url(m)
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

            # Print quick preview of extracted title/url for the newest message
            first_id = (out.get("message_ids_sorted") or [None])[0]
            if first_id and isinstance(out.get("messages_by_id"), dict):
                first = out["messages_by_id"].get(first_id) or {}
                ex = first.get("extracted") or {}
                safe_title = str(ex.get("title") or "").encode("ascii", "replace").decode("ascii")
                safe_url = str(ex.get("primary_url") or "").encode("ascii", "replace").decode("ascii")
                print(f"  OK: {channel_key} -> wrote {store_path.name} (stored={len(out.get('message_ids_sorted') or [])})")
                print(f"      newest: {safe_title[:110]} | {safe_url[:160]}")
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
