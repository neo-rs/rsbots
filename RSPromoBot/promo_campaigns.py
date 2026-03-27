from __future__ import annotations

import uuid
from typing import Any

from storage import JSONStorage
from utils import iso_now


class PromoCampaignStore:
    def __init__(self, storage: JSONStorage) -> None:
        self.storage = storage
        self.file_name = "campaigns.json"

    def _load(self) -> dict[str, Any]:
        return self.storage.read(self.file_name, {"campaigns": {}})

    def _save(self, payload: dict[str, Any]) -> None:
        self.storage.write(self.file_name, payload)

    def get(self, campaign_id: str) -> dict[str, Any] | None:
        payload = self._load()
        return payload["campaigns"].get(campaign_id)

    def upsert(self, campaign: dict[str, Any]) -> dict[str, Any]:
        payload = self._load()
        campaign["updated_at"] = iso_now()
        payload["campaigns"][campaign["campaign_id"]] = campaign
        self._save(payload)
        return campaign

    def create_from_session(self, guild_id: int, creator_id: int, session: dict[str, Any], recipients: list[int]) -> dict[str, Any]:
        campaign_id = f"campaign_{uuid.uuid4().hex[:12]}"
        campaign = {
            "campaign_id": campaign_id,
            "guild_id": str(guild_id),
            "created_by": str(creator_id),
            "campaign_name": session["campaign_name"],
            "target_role_id": session["target_role_id"],
            "message_body": session["message_body"],
            "embed_title": session.get("embed_title", ""),
            "banner_url": session.get("banner_url", ""),
            "attachment_urls": session.get("attachment_urls", ""),
            "cta_label": session.get("cta_label", ""),
            "cta_url": session.get("cta_url", ""),
            "batch_size": int(session["batch_size"]),
            "batch_interval_minutes": int(session["batch_interval_minutes"]),
            "recipient_ids": [str(item) for item in recipients],
            "recipient_count": len(recipients),
            "sent_count": 0,
            "failed_count": 0,
            "status": "draft",
            "created_at": iso_now(),
            "updated_at": iso_now(),
            "started_at": "",
            "completed_at": "",
            "paused_at": "",
            "cancelled_at": ""
        }
        return self.upsert(campaign)

    def delete(self, campaign_id: str) -> bool:
        """Remove a campaign from history. Returns True if it existed and was removed."""
        payload = self._load()
        if campaign_id not in payload["campaigns"]:
            return False
        del payload["campaigns"][campaign_id]
        self._save(payload)
        return True

    def list_recent(self, limit: int = 10) -> list[dict[str, Any]]:
        payload = self._load()
        campaigns = list(payload["campaigns"].values())
        campaigns.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        return campaigns[:limit]
