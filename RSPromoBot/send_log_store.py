from __future__ import annotations

from typing import Any

from storage import JSONStorage


class SendLogStore:
    def __init__(self, storage: JSONStorage) -> None:
        self.storage = storage
        self.file_name = "send_logs.json"

    def _load(self) -> dict[str, Any]:
        return self.storage.read(self.file_name, {"entries": []})

    def append(self, entry: dict[str, Any]) -> None:
        payload = self._load()
        payload["entries"].append(entry)
        self.storage.write(self.file_name, payload)
