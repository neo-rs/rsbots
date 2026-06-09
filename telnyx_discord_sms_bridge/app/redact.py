from __future__ import annotations

import re


PHONE_RE = re.compile(r"(?<!\d)\+?\d[\d\-\s().]{6,}\d(?!\d)")


def redact_phone(value: str, *, enabled: bool = True, visible_last_digits: int = 4) -> str:
    if not enabled:
        return value

    def _replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = re.sub(r"\D", "", raw)
        if len(digits) <= visible_last_digits:
            return "***"
        return "***" + digits[-visible_last_digits:]

    return PHONE_RE.sub(_replace, value)


def safe_preview(value: str, limit: int = 120) -> str:
    cleaned = " ".join(value.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."
