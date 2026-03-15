"""
Fetch ALL channels under the given Discord category IDs via the Discord API
and update RSForwarder/config.json with rs_fs_monitor_channel_ids.

Includes every text channel: store monitors (e.g. five-below, boxlunch-hottopic),
banner/separator channels (e.g. ------, ----commands----), and any other channels.
No filter by "monitor" in the name.

Usage (from repo root):
  python -m RSForwarder.fetch_monitor_channels

Requires bot_token and guild_id in config (from config.json + config.secrets.json).
Category IDs are hardcoded below; edit CATEGORY_IDS to change.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Repo root for mirror_world_config
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import requests

from mirror_world_config import load_config_with_secrets

# Category IDs under which to list channels (edit as needed)
CATEGORY_IDS = [
    1350953333069713528,
    1411757054908960819,
    1351327970463060088,
]

DISCORD_API = "https://discord.com/api/v10"


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


def main() -> None:
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

    url = f"{DISCORD_API}/guilds/{guild_id}/channels"
    headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
    r = requests.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        print(f"ERROR: Discord API returned {r.status_code}: {r.text[:500]}")
        sys.exit(1)

    channels = r.json()
    if not isinstance(channels, list):
        print("ERROR: Unexpected API response (not a list)")
        sys.exit(1)

    # All text channels (type 0) under the given categories — no filter by "monitor"
    found: dict[str, int] = {}
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
        if parent_id not in CATEGORY_IDS:
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

    # Update config.json first (do not touch secrets)
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["rs_fs_monitor_category_ids"] = [str(i) for i in CATEGORY_IDS]
    cfg["rs_fs_monitor_channel_ids"] = {k: v for k, v in sorted(found.items())}
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    print(f"Updated {config_path} with rs_fs_monitor_category_ids and rs_fs_monitor_channel_ids.")

    print(f"Guild ID: {guild_id}")
    print(f"Categories: {CATEGORY_IDS}")
    print(f"Channels found: {len(found)}")
    for k, v in sorted(found.items()):
        safe_k = k.encode("ascii", "replace").decode("ascii")
        print(f"  {safe_k} -> {v}")


if __name__ == "__main__":
    main()
