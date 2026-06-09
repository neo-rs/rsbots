from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from app.phone import normalize_e164


def _bool_from_env(value: str | None, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class AppConfig:
    telnyx_api_key: str
    telnyx_from_number: str
    telnyx_api_base: str
    telnyx_messaging_profile_id: str | None
    telnyx_public_key: str | None
    telnyx_require_signature: bool
    discord_webhook_url: str
    bridge_api_key: str
    app_host: str
    app_port: int
    log_level: str
    settings: dict[str, Any]

    @classmethod
    def load(cls) -> "AppConfig":
        load_dotenv()

        config_path = Path(os.getenv("CONFIG_PATH", "config/settings.json"))
        settings = _load_json_config(config_path)

        required = {
            "TELNYX_API_KEY": os.getenv("TELNYX_API_KEY", "").strip(),
            "TELNYX_FROM_NUMBER": os.getenv("TELNYX_FROM_NUMBER", "").strip(),
            "DISCORD_WEBHOOK_URL": os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
            "BRIDGE_API_KEY": os.getenv("BRIDGE_API_KEY", "").strip(),
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            joined = ", ".join(missing)
            raise RuntimeError(f"Missing required environment value(s): {joined}")

        telnyx_from_number = normalize_e164(required["TELNYX_FROM_NUMBER"])

        return cls(
            telnyx_api_key=required["TELNYX_API_KEY"],
            telnyx_from_number=telnyx_from_number,
            telnyx_api_base=os.getenv("TELNYX_API_BASE", "https://api.telnyx.com/v2").rstrip("/"),
            telnyx_messaging_profile_id=os.getenv("TELNYX_MESSAGING_PROFILE_ID", "").strip() or None,
            telnyx_public_key=os.getenv("TELNYX_PUBLIC_KEY", "").strip() or None,
            telnyx_require_signature=_bool_from_env(os.getenv("TELNYX_REQUIRE_SIGNATURE"), False),
            discord_webhook_url=required["DISCORD_WEBHOOK_URL"],
            bridge_api_key=required["BRIDGE_API_KEY"],
            app_host=os.getenv("APP_HOST", "0.0.0.0").strip(),
            app_port=int(os.getenv("APP_PORT", "8787")),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            settings=settings,
        )

    def allowed_from_numbers(self) -> list[str]:
        numbers: list[str] = []
        sms_cfg = self.settings.get("sms", {})
        for entry in sms_cfg.get("from_numbers", []):
            raw = entry.get("number") if isinstance(entry, dict) else str(entry)
            if not str(raw or "").strip():
                continue
            normalized = normalize_e164(str(raw))
            if normalized not in numbers:
                numbers.append(normalized)

        extra = os.getenv("TELNYX_FROM_NUMBERS", "").strip()
        if extra:
            for part in extra.split(","):
                part = part.strip()
                if not part:
                    continue
                normalized = normalize_e164(part)
                if normalized not in numbers:
                    numbers.append(normalized)

        if self.telnyx_from_number not in numbers:
            numbers.insert(0, self.telnyx_from_number)

        return numbers

    def resolve_from_number(self, requested: str | None) -> str:
        if not requested or not str(requested).strip():
            return self.telnyx_from_number

        normalized = normalize_e164(str(requested))
        allowed = self.allowed_from_numbers()
        if normalized not in allowed:
            joined = ", ".join(allowed)
            raise ValueError(f"from number not allowed: {normalized}. Allowed: {joined}")
        return normalized


def _load_json_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
