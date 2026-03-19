from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_KEYS = {
    "guild_id",
    "allowed_launcher_role_ids",
    "log_channel_id",
    "embed_color",
    "default_batch_size",
    "default_batch_interval_minutes",
    "max_batch_size",
    "max_batch_interval_minutes",
    "max_campaign_recipients",
    "preview_recipient_user_id",
    "cta_button_style",
    "status_update_interval_seconds",
    "session_timeout_minutes",
    "send_timeout_seconds",
    "dm_delay_min_seconds",
    "dm_delay_max_seconds",
    "notify_role_id",
    "exclude_bots",
    "default_test_mode",
    "data_dir",
    "logs_dir",
}


def load_json_file(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def load_config(base_dir: str | Path) -> dict[str, Any]:
    base_path = Path(base_dir)
    config = load_json_file(base_path / "config.json")
    missing = sorted(REQUIRED_KEYS - set(config.keys()))
    if missing:
        raise ValueError(f"Missing config keys: {', '.join(missing)}")
    return config
