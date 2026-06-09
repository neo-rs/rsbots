from __future__ import annotations


def normalize_e164(number: str) -> str:
    """Normalize a phone number to E.164 (+digits only)."""
    raw = str(number or "").strip()
    if not raw:
        raise ValueError("phone number is empty")

    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        raise ValueError(f"phone number has no digits: {number!r}")

    if raw.startswith("+"):
        return f"+{digits}"

    if len(digits) == 10:
        return f"+1{digits}"

    return f"+{digits}"
