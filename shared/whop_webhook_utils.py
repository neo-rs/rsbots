import base64
import hmac
import hashlib
import os
import time
from typing import Tuple, List


def _decode_webhook_secret(secret: str) -> bytes:
    s = str(secret or "").strip()
    if not s:
        return b""
    if s.startswith("whsec_"):
        s = s[len("whsec_") :]
    try:
        return base64.b64decode(s)
    except Exception:
        return s.encode("utf-8")


def _parse_webhook_signatures(sig_header: str) -> List[str]:
    out: list[str] = []
    for part in str(sig_header or "").split():
        token = part.strip()
        if not token:
            continue
        if "," in token:
            _ver, sig = token.split(",", 1)
            out.append(sig.strip())
            continue
        if "=" in token:
            _ver, sig = token.split("=", 1)
            out.append(sig.strip())
            continue
        out.append(token)
    return [s for s in out if s]


def verify_standard_webhook(
    headers: dict,
    body_bytes: bytes,
    *,
    secret: str,
    tolerance_seconds: int = 300,
    verify: bool = True,
) -> Tuple[bool, str]:
    if not verify:
        return (True, "disabled")
    secret = str(secret or os.getenv("WHOP_WEBHOOK_SECRET", "")).strip()
    if not secret:
        return (False, "missing_webhook_secret")
    wh_id = str(headers.get("webhook-id") or "").strip()
    wh_ts = str(headers.get("webhook-timestamp") or "").strip()
    wh_sig = str(headers.get("webhook-signature") or "").strip()
    if not wh_id or not wh_ts or not wh_sig:
        return (False, "missing_headers")
    try:
        ts_i = int(float(wh_ts))
    except Exception:
        return (False, "bad_timestamp")
    if tolerance_seconds > 0:
        now = int(time.time())
        if abs(now - ts_i) > int(tolerance_seconds):
            return (False, "timestamp_out_of_range")
    signed_content = f"{wh_id}.{wh_ts}.{body_bytes.decode('utf-8', errors='ignore')}"
    secret_bytes = _decode_webhook_secret(secret)
    if not secret_bytes:
        return (False, "bad_secret")
    digest = hmac.new(secret_bytes, signed_content.encode("utf-8"), hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    for sig in _parse_webhook_signatures(wh_sig):
        if hmac.compare_digest(expected, sig):
            return (True, "ok")
    return (False, "signature_mismatch")
