from __future__ import annotations

import base64
import logging

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

log = logging.getLogger("signature")


def _decode_public_key(public_key: str) -> bytes:
    """Decode Telnyx Ed25519 public key from base64 (portal default) or hex."""
    key = public_key.strip()
    if not key:
        raise ValueError("empty public key")

    try:
        decoded = base64.b64decode(key, validate=True)
        if len(decoded) == 32:
            return decoded
    except Exception:
        pass

    try:
        decoded = bytes.fromhex(key)
        if len(decoded) == 32:
            return decoded
    except ValueError as exc:
        raise ValueError("public key must be base64 or hex Ed25519 key") from exc

    raise ValueError("public key must decode to 32 bytes")


def verify_telnyx_signature(
    *,
    public_key_hex: str | None,
    timestamp: str | None,
    signature_hex: str | None,
    raw_body: bytes,
    require_signature: bool,
) -> bool:
    """Verify Telnyx webhook signature when configured.

    Telnyx signs webhooks with an Ed25519 signature. The signed payload is:
    timestamp + "|" + raw request body.
    """

    if not require_signature:
        log.info("event=signature_skipped reason=require_signature_disabled")
        return True

    if not public_key_hex:
        log.warning("event=signature_failed reason=missing_public_key")
        return False

    if not timestamp or not signature_hex:
        log.warning("event=signature_failed reason=missing_signature_headers")
        return False

    try:
        verify_key = VerifyKey(_decode_public_key(public_key_hex))
        signed_payload = timestamp.encode("utf-8") + b"|" + raw_body
        verify_key.verify(signed_payload, bytes.fromhex(signature_hex))
    except (ValueError, BadSignatureError) as exc:
        log.warning("event=signature_failed reason=bad_signature error=%s", type(exc).__name__)
        return False

    log.info("event=signature_ok reason=telnyx_signature_verified")
    return True
