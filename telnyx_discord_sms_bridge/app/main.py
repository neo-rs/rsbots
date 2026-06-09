from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from app.logging_setup import setup_logging
from app.redact import redact_phone, safe_preview
from app.runtime import BridgeRuntime
from app.telnyx_signature import verify_telnyx_signature

runtime = BridgeRuntime.build()
setup_logging(runtime.config.log_level)

log = logging.getLogger("main")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    log.info(
        "event=bridge_startup conversation_mode=%s discord_bot=%s",
        runtime.conversations.conversations_enabled(),
        bool(runtime.discord_bot),
    )
    await runtime.start_discord_bot()
    yield
    await runtime.stop_discord_bot()


app = FastAPI(title="Telnyx Discord SMS Bridge", lifespan=lifespan)


class SendSmsRequest(BaseModel):
    to: str = Field(..., description="Destination phone number in E.164 format")
    text: str = Field(..., min_length=1, max_length=5000)
    from_number: str | None = Field(
        default=None,
        description="Optional Telnyx sender number. Defaults to TELNYX_FROM_NUMBER.",
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhooks/telnyx")
async def telnyx_webhook(
    request: Request,
    telnyx_signature_ed25519: str | None = Header(default=None),
    telnyx_timestamp: str | None = Header(default=None),
) -> dict[str, str]:
    raw_body = await request.body()

    signature_ok = verify_telnyx_signature(
        public_key_hex=runtime.config.telnyx_public_key,
        timestamp=telnyx_timestamp,
        signature_hex=telnyx_signature_ed25519,
        raw_body=raw_body,
        require_signature=runtime.config.telnyx_require_signature,
    )
    if not signature_ok:
        raise HTTPException(status_code=401, detail="Invalid Telnyx webhook signature")

    try:
        event = await request.json()
    except Exception as exc:
        log.warning("event=inbound_rejected reason=invalid_json error=%s", type(exc).__name__)
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    data = _extract_telnyx_data(event)
    payload_data = data.get("payload", data)
    text = str(payload_data.get("text") or payload_data.get("body") or "")
    from_number = _extract_from_number(payload_data)
    to_number = _extract_to_number(payload_data)

    redaction_cfg = runtime.config.settings.get("logging", {})
    redact_enabled = bool(redaction_cfg.get("redact_phone_numbers", True))
    visible_digits = int(redaction_cfg.get("phone_visible_last_digits", 4))

    log.info(
        "event=inbound_received reason=telnyx_webhook_parsed from=%s to=%s chars=%s preview=%r",
        redact_phone(from_number, enabled=redact_enabled, visible_last_digits=visible_digits),
        redact_phone(to_number, enabled=redact_enabled, visible_last_digits=visible_digits),
        len(text),
        safe_preview(text),
    )

    await runtime.discord.post_inbound(
        telnyx_data=data,
        from_number=from_number,
        to_number=to_number,
        text=text,
    )
    return {"status": "ok"}


@app.post("/send")
async def send_sms(
    body: SendSmsRequest,
    x_bridge_key: str | None = Header(default=None),
) -> dict[str, Any]:
    if not x_bridge_key or x_bridge_key != runtime.config.bridge_api_key:
        log.warning("event=outbound_rejected reason=invalid_bridge_key")
        raise HTTPException(status_code=401, detail="Invalid bridge key")

    try:
        from_number = runtime.config.resolve_from_number(body.from_number)
    except ValueError as exc:
        log.warning("event=outbound_rejected reason=invalid_from_number error=%s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    telnyx_response = await runtime.telnyx.send_sms(
        to_number=body.to,
        text=body.text,
        from_number=from_number,
    )
    await runtime.discord.post_outbound_notice(
        to_number=body.to,
        text=body.text,
        telnyx_response=telnyx_response,
        from_number=from_number,
    )
    return {"status": "sent", "telnyx_response": telnyx_response}


def _extract_telnyx_data(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event.get("data"), dict):
        return event["data"]
    return event


def _extract_from_number(payload_data: dict[str, Any]) -> str:
    from_obj = payload_data.get("from")
    if isinstance(from_obj, dict):
        return str(from_obj.get("phone_number") or from_obj.get("number") or "unknown")
    return str(from_obj or "unknown")


def _extract_to_number(payload_data: dict[str, Any]) -> str:
    to_obj = payload_data.get("to")
    if isinstance(to_obj, list) and to_obj:
        first = to_obj[0]
        if isinstance(first, dict):
            return str(first.get("phone_number") or first.get("number") or "unknown")
        return str(first)
    if isinstance(to_obj, dict):
        return str(to_obj.get("phone_number") or to_obj.get("number") or "unknown")
    return str(to_obj or "unknown")


if __name__ == "__main__":
    log.info(
        "event=server_start reason=operator_started_bridge host=%s port=%s signature_required=%s bot_mode=%s",
        runtime.config.app_host,
        runtime.config.app_port,
        runtime.config.telnyx_require_signature,
        runtime.conversations.conversations_enabled(),
    )
    uvicorn.run("app.main:app", host=runtime.config.app_host, port=runtime.config.app_port, reload=False)
