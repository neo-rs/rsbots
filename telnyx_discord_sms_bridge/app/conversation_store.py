from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.phone import normalize_e164


def thread_key(*, our_line: str, remote_party: str) -> str:
    return f"{normalize_e164(our_line)}|{normalize_e164(remote_party)}"


def digits_key(key: str) -> str:
    """Compact key for Discord custom_id (digits only, pipe-separated)."""
    parts = key.split("|")
    return "|".join("".join(ch for ch in p if ch.isdigit()) for p in parts)


class ConversationStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({"threads": {}, "updated_at": _iso_now()})

    def list_threads(self) -> dict[str, dict[str, Any]]:
        data = self._read()
        threads = data.get("threads", {})
        return {k: dict(v) for k, v in threads.items() if isinstance(v, dict)}

    def get_thread(self, key: str) -> dict[str, Any] | None:
        data = self._read()
        thread = data.get("threads", {}).get(key)
        return dict(thread) if isinstance(thread, dict) else None

    def upsert_thread(self, key: str, patch: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            data = self._read()
            threads = data.setdefault("threads", {})
            current = dict(threads.get(key) or {})
            current.update(patch)
            threads[key] = current
            data["updated_at"] = _iso_now()
            self._write(data)
            return dict(current)

    def append_line(
        self,
        key: str,
        *,
        direction: str,
        text: str,
        max_lines: int,
    ) -> dict[str, Any]:
        with self._lock:
            data = self._read()
            threads = data.setdefault("threads", {})
            current = dict(threads.get(key) or {})
            lines = list(current.get("lines") or [])
            normalized_text = str(text or "")
            if lines:
                last = lines[-1]
                if (
                    str(last.get("direction") or "") == direction
                    and str(last.get("text") or "") == normalized_text
                ):
                    return dict(current)
            lines.append(
                {
                    "direction": direction,
                    "text": normalized_text,
                    "at": _iso_now(),
                }
            )
            if max_lines > 0 and len(lines) > max_lines:
                lines = lines[-max_lines:]
            current["lines"] = lines
            threads[key] = current
            data["updated_at"] = _iso_now()
            self._write(data)
            return dict(current)

    def _read(self) -> dict[str, Any]:
        with self.path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {"threads": {}}

    def _write(self, data: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(self.path)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()
