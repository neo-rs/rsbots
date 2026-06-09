from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

import httpx

from app.config import AppConfig
from app.discord_format import format_message_block, format_party_line, format_route_summary
from app.phone import normalize_e164

if TYPE_CHECKING:
    from app.conversation_service import ConversationService

log = logging.getLogger("discord")


class DiscordClient:
    def __init__(self, config: AppConfig, *, conversations: "ConversationService | None" = None):
        self.config = config
        self.conversations = conversations

    async def post_inbound(self, *, telnyx_data: dict[str, Any], from_number: str, to_number: str, text: str) -> None:
        if self._use_conversations():
            await self.conversations.record_inbound(  # type: ignore[union-attr]
                our_line=normalize_e164(to_number),
                remote_party=normalize_e164(from_number),
                text=text,
            )
            return
        payload = self._build_inbound_payload(telnyx_data)
        await self._post_webhook(payload=payload, webhook_url=self._webhook_for_line(to_number), reason="inbound_message_forwarded")

    async def post_outbound_notice(
        self,
        *,
        to_number: str,
        text: str,
        telnyx_response: dict[str, Any],
        from_number: str | None = None,
    ) -> None:
        sender = from_number or self.config.telnyx_from_number
        if self._use_conversations():
            await self.conversations.record_outbound(  # type: ignore[union-attr]
                our_line=normalize_e164(sender),
                remote_party=normalize_e164(to_number),
                text=text,
            )
            return
        payload = self._build_outbound_payload(
            to_number=to_number,
            text=text,
            telnyx_response=telnyx_response,
            from_number=sender,
        )
        await self._post_webhook(payload=payload, webhook_url=self._webhook_for_line(sender), reason="outbound_send_logged")

    async def post_test(self) -> None:
        discord_cfg = self.config.settings.get("discord", {})
        payload = {
            "username": discord_cfg.get("username", "Telnyx SMS Bridge"),
            "avatar_url": discord_cfg.get("avatar_url") or None,
            "embeds": [
                {
                    "title": "✅ Bridge Connected",
                    "description": (
                        "Discord is configured.\n"
                        "Conversation mode uses the bot token when set.\n"
                        "Inbound Telnyx webhook: `https://YOUR_PUBLIC_DOMAIN/webhooks/telnyx`"
                    ),
                    "color": int(discord_cfg.get("test_color", 5763719)),
                    "fields": [
                        {"name": "Status", "value": "OK", "inline": True},
                        {"name": "Bot mode", "value": "on" if self.config.discord_bot_token else "off", "inline": True},
                    ],
                    "footer": {"text": "Telnyx SMS Bridge • setup test"},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }
        await self._post_webhook(payload=payload, webhook_url=self.config.discord_webhook_url, reason="manual_discord_test")

    def _use_conversations(self) -> bool:
        return bool(self.conversations and self.conversations.conversations_enabled())

    def _webhook_for_line(self, our_line: str) -> str:
        number = normalize_e164(our_line)
        discord_cfg = self.config.settings.get("discord", {})
        lines = discord_cfg.get("lines", {})
        if isinstance(lines, dict):
            entry = lines.get(number)
            if isinstance(entry, dict):
                url = str(entry.get("webhook_url") or "").strip()
                if url:
                    return url
        digits = "".join(ch for ch in number if ch.isdigit())
        for suffix in (digits, digits[-4:] if len(digits) >= 4 else ""):
            if not suffix:
                continue
            env_url = os.getenv(f"DISCORD_WEBHOOK_URL_{suffix}", "").strip()
            if env_url:
                return env_url
        return self.config.discord_webhook_url

    async def _post_webhook(self, *, payload: dict[str, Any], webhook_url: str, reason: str) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(webhook_url, json=payload)

        if response.status_code >= 400:
            log.error(
                "event=discord_post_failed reason=%s status=%s response=%r",
                reason,
                response.status_code,
                response.text[:500],
            )
            raise RuntimeError(f"Discord webhook failed with status {response.status_code}")

        log.info("event=discord_post_success reason=%s status=%s", reason, response.status_code)

    def _discord_display_settings(self) -> dict[str, Any]:
        discord_cfg = self.config.settings.get("discord", {})
        logging_cfg = self.config.settings.get("logging", {})
        sms_cfg = self.config.settings.get("sms", {})

        redact_enabled = bool(discord_cfg.get("redact_phone_numbers", logging_cfg.get("redact_phone_numbers", False)))
        return {
            "discord_cfg": discord_cfg,
            "sms_cfg": sms_cfg,
            "redact_enabled": redact_enabled,
            "visible_digits": int(logging_cfg.get("phone_visible_last_digits", 4)),
            "show_formatted_numbers": bool(discord_cfg.get("show_formatted_numbers", True)),
            "show_labels": bool(discord_cfg.get("show_number_labels", True)),
            "max_chars": int(sms_cfg.get("max_discord_text_chars", 1800)),
            "from_numbers": sms_cfg.get("from_numbers", []),
        }

    def _build_inbound_payload(self, telnyx_data: dict[str, Any]) -> dict[str, Any]:
        display = self._discord_display_settings()
        discord_cfg = display["discord_cfg"]
        sms_cfg = display["sms_cfg"]

        payload_data = telnyx_data.get("payload", telnyx_data)
        from_number = str(payload_data.get("from", {}).get("phone_number") or payload_data.get("from") or "unknown")
        to_number = str(
            payload_data.get("to", [{}])[0].get("phone_number")
            if isinstance(payload_data.get("to"), list) and payload_data.get("to")
            else payload_data.get("to") or "unknown"
        )
        text = str(payload_data.get("text") or payload_data.get("body") or "")
        message_id = str(payload_data.get("id") or telnyx_data.get("id") or "unknown")
        media = payload_data.get("media") or []
        event_type = str(telnyx_data.get("event_type") or "message.received")

        route = format_route_summary(
            from_number=from_number,
            to_number=to_number,
            from_numbers=display["from_numbers"],
            show_labels=display["show_labels"],
        )

        fields = [
            {"name": "Direction", "value": route, "inline": False},
            {
                "name": "Sender",
                "value": format_party_line(
                    number=from_number,
                    from_numbers=display["from_numbers"],
                    redact_enabled=display["redact_enabled"],
                    visible_last_digits=display["visible_digits"],
                    show_formatted_numbers=display["show_formatted_numbers"],
                    show_labels=display["show_labels"],
                ),
                "inline": True,
            },
            {
                "name": "Received On",
                "value": format_party_line(
                    number=to_number,
                    from_numbers=display["from_numbers"],
                    redact_enabled=display["redact_enabled"],
                    visible_last_digits=display["visible_digits"],
                    show_formatted_numbers=display["show_formatted_numbers"],
                    show_labels=display["show_labels"],
                ),
                "inline": True,
            },
        ]

        if media and sms_cfg.get("allow_mms_notice", True):
            fields.append(
                {"name": "Attachments", "value": f"{len(media)} media item(s) in Telnyx payload", "inline": False}
            )

        embed: dict[str, Any] = {
            "title": discord_cfg.get("inbound_title", "📨 Inbound SMS"),
            "description": format_message_block(text, max_chars=display["max_chars"]),
            "color": int(discord_cfg.get("inbound_color", 3066993)),
            "fields": fields,
            "footer": self._build_footer(message_id=message_id, event_type=event_type, discord_cfg=discord_cfg),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return {
            "username": discord_cfg.get("username", "Telnyx SMS Bridge"),
            "avatar_url": discord_cfg.get("avatar_url") or None,
            "embeds": [embed],
        }

    def _build_outbound_payload(
        self,
        *,
        to_number: str,
        text: str,
        telnyx_response: dict[str, Any],
        from_number: str | None = None,
    ) -> dict[str, Any]:
        display = self._discord_display_settings()
        discord_cfg = display["discord_cfg"]
        sender = from_number or self.config.telnyx_from_number
        telnyx_id = str(telnyx_response.get("data", {}).get("id", "unknown"))

        route = format_route_summary(
            from_number=sender,
            to_number=to_number,
            from_numbers=display["from_numbers"],
            show_labels=display["show_labels"],
        )

        fields = [
            {"name": "Direction", "value": route, "inline": False},
            {
                "name": "From",
                "value": format_party_line(
                    number=sender,
                    from_numbers=display["from_numbers"],
                    redact_enabled=display["redact_enabled"],
                    visible_last_digits=display["visible_digits"],
                    show_formatted_numbers=display["show_formatted_numbers"],
                    show_labels=display["show_labels"],
                ),
                "inline": True,
            },
            {
                "name": "To",
                "value": format_party_line(
                    number=to_number,
                    from_numbers=display["from_numbers"],
                    redact_enabled=display["redact_enabled"],
                    visible_last_digits=display["visible_digits"],
                    show_formatted_numbers=display["show_formatted_numbers"],
                    show_labels=display["show_labels"],
                ),
                "inline": True,
            },
        ]

        embed: dict[str, Any] = {
            "title": discord_cfg.get("outbound_title", "📤 Outbound SMS Sent"),
            "description": format_message_block(text, max_chars=display["max_chars"]),
            "color": int(discord_cfg.get("outbound_color", 3447003)),
            "fields": fields,
            "footer": self._build_footer(message_id=telnyx_id, event_type="message.sent", discord_cfg=discord_cfg),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return {
            "username": discord_cfg.get("username", "Telnyx SMS Bridge"),
            "avatar_url": discord_cfg.get("avatar_url") or None,
            "embeds": [embed],
        }

    def _build_footer(self, *, message_id: str, event_type: str, discord_cfg: dict[str, Any]) -> dict[str, str]:
        parts = ["Telnyx SMS Bridge"]
        if discord_cfg.get("include_telnyx_ids", True):
            parts.append(f"ID {message_id}")
        parts.append(event_type)
        return {"text": " • ".join(parts)}
