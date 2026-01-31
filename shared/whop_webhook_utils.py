import base64
import hmac
import hashlib
import os
import time
from typing import Tuple, List


def _try_b64decode(s: str) -> bytes:
    try:
        return base64.b64decode(s)
    except Exception:
        return b""


def _try_urlsafe_b64decode(s: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(s)
    except Exception:
        return b""


def _candidate_webhook_secret_bytes(secret: str) -> List[bytes]:
    """Return candidate secret-bytes for signature verification.

    Whop follows the Standard Webhooks signature spec. Depending on where you copy the
    secret from (dashboard vs SDK examples) you may have:
    - a raw secret string (common in dashboards / API responses)
    - a `whsec_...` secret (prefix + base64-ish payload)
    - a base64 (or urlsafe base64) encoded secret

    To avoid silently rejecting valid webhooks due to secret formatting differences,
    we try a small set of reasonable interpretations and accept if any matches.
    """
    s = str(secret or "").strip()
    if not s:
        return []

    raw = s.encode("utf-8")
    out: list[bytes] = [raw]

    # `whsec_`-prefixed secrets commonly store base64 after the prefix.
    if s.startswith("whsec_"):
        s2 = s[len("whsec_") :]
        for b in (_try_b64decode(s2), _try_urlsafe_b64decode(s2)):
            if b and b not in out:
                out.append(b)
        return out

    # Non-prefixed secrets: try base64 / urlsafe base64 as optional interpretations.
    for b in (_try_b64decode(s), _try_urlsafe_b64decode(s)):
        if b and b not in out:
            out.append(b)
    return out


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
    secret_candidates = _candidate_webhook_secret_bytes(secret)
    if not secret_candidates:
        return (False, "bad_secret")
    sigs = _parse_webhook_signatures(wh_sig)
    if not sigs:
        return (False, "missing_headers")
    for secret_bytes in secret_candidates:
        digest = hmac.new(secret_bytes, signed_content.encode("utf-8"), hashlib.sha256).digest()
        expected = base64.b64encode(digest).decode("utf-8")
        for sig in sigs:
            if hmac.compare_digest(expected, sig):
                return (True, "ok")
    return (False, "signature_mismatch")
