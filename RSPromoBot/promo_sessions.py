from __future__ import annotations

from typing import Any

from storage import JSONStorage
from utils import iso_now


class PromoSessionStore:
    def __init__(self, storage: JSONStorage) -> None:
        self.storage = storage
        self.file_name = "sessions.json"

    def _load(self) -> dict[str, Any]:
        return self.storage.read(self.file_name, {"sessions": {}})

    def _save(self, payload: dict[str, Any]) -> None:
        self.storage.write(self.file_name, payload)

    def get(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        payload = self._load()
        return payload["sessions"].get(f"{guild_id}:{user_id}")

    def upsert(self, guild_id: int, user_id: int, session: dict[str, Any]) -> dict[str, Any]:
        payload = self._load()
        key = f"{guild_id}:{user_id}"
        session["updated_at"] = iso_now()
        payload["sessions"][key] = session
        self._save(payload)
        return session

    def delete(self, guild_id: int, user_id: int) -> None:
        payload = self._load()
        payload["sessions"].pop(f"{guild_id}:{user_id}", None)
        self._save(payload)

    def build_default(self, guild_id: int, user_id: int, config: dict[str, Any]) -> dict[str, Any]:
        return {
            "guild_id": str(guild_id),
            "user_id": str(user_id),
            "campaign_name": "",
            "target_role_id": "",
            "message_body": "",
            "embed_title": "",
            "banner_url": (config.get("default_banner_url") or "").strip(),
            "cta_label": "",
            "cta_url": "",
            "batch_size": int(config["default_batch_size"]),
            "batch_interval_minutes": int(config["default_batch_interval_minutes"]),
            "status": "draft",
            "created_at": iso_now(),
            "updated_at": iso_now()
        }
