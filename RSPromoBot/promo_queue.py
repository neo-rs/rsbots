from __future__ import annotations

from typing import Any

from storage import JSONStorage


class PromoQueueStore:
    def __init__(self, storage: JSONStorage) -> None:
        self.storage = storage
        self.file_name = "queue.json"

    def get(self) -> dict[str, Any]:
        return self.storage.read(
            self.file_name,
            {
                "campaign_id": "",
                "guild_id": "",
                "status": "idle",
                "recipients": [],
                "pending_count": 0,
                "sent_count": 0,
                "failed_count": 0,
                "last_run_at": "",
                "next_run_at": ""
            }
        )

    def save(self, payload: dict[str, Any]) -> None:
        self.storage.write(self.file_name, payload)
