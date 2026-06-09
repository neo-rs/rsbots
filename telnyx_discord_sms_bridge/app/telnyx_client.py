from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import AppConfig
from app.phone import normalize_e164
from app.redact import redact_phone, safe_preview

log = logging.getLogger("telnyx")


class TelnyxClient:
    def __init__(self, config: AppConfig):
        self.config = config

    async def send_sms(self, *, to_number: str, text: str, from_number: str | None = None) -> dict[str, Any]:
        url = f"{self.config.telnyx_api_base}/messages"
        sender = self.config.resolve_from_number(from_number)
        destination = normalize_e164(to_number)
        payload: dict[str, Any] = {
            "from": sender,
            "to": destination,
            "text": text,
        }

        if self.config.telnyx_messaging_profile_id:
            payload["messaging_profile_id"] = self.config.telnyx_messaging_profile_id

        redaction_cfg = self.config.settings.get("logging", {})
        redact_enabled = bool(redaction_cfg.get("redact_phone_numbers", True))
        visible_digits = int(redaction_cfg.get("phone_visible_last_digits", 4))

        log.info(
            "event=outbound_send_attempt reason=operator_requested_sms from=%s to=%s chars=%s preview=%r",
            redact_phone(sender, enabled=redact_enabled, visible_last_digits=visible_digits),
            redact_phone(destination, enabled=redact_enabled, visible_last_digits=visible_digits),
            len(text),
            safe_preview(text),
        )

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {self.config.telnyx_api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=payload,
            )

        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text}

        if response.status_code >= 400:
            log.error(
                "event=outbound_send_failed reason=telnyx_api_rejected status=%s response=%s",
                response.status_code,
                body,
            )
            raise RuntimeError(f"Telnyx send failed with status {response.status_code}: {body}")

        telnyx_id = body.get("data", {}).get("id") if isinstance(body, dict) else None
        log.info(
            "event=outbound_send_success reason=telnyx_api_accepted from=%s to=%s telnyx_id=%s",
            redact_phone(sender, enabled=redact_enabled, visible_last_digits=visible_digits),
            redact_phone(destination, enabled=redact_enabled, visible_last_digits=visible_digits),
            telnyx_id or "unknown",
        )
        return body
