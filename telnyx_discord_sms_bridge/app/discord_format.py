from __future__ import annotations

import re
from typing import Any

from app.redact import redact_phone


def format_phone_display(number: str) -> str:
    digits = re.sub(r"\D", "", str(number or ""))
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    if len(digits) == 10:
        return f"+1 ({digits[0:3]}) {digits[3:6]}-{digits[6:]}"
    if digits:
        return f"+{digits}"
    return str(number or "unknown")


def resolve_number_label(number: str, from_numbers: list[dict[str, Any]] | list[Any]) -> str | None:
    normalized = re.sub(r"\D", "", str(number or ""))
    if not normalized:
        return None

    for entry in from_numbers:
        if isinstance(entry, dict):
            raw = str(entry.get("number") or "")
            label = str(entry.get("label") or "").strip()
        else:
            raw = str(entry)
            label = ""
        entry_digits = re.sub(r"\D", "", raw)
        if entry_digits == normalized or entry_digits.endswith(normalized) or normalized.endswith(entry_digits):
            return label or None
    return None


def format_party_line(
    *,
    number: str,
    from_numbers: list[dict[str, Any]] | list[Any],
    redact_enabled: bool,
    visible_last_digits: int,
    show_formatted_numbers: bool,
    show_labels: bool,
) -> str:
    display_number = format_phone_display(number) if show_formatted_numbers else str(number)
    if redact_enabled:
        display_number = redact_phone(display_number, enabled=True, visible_last_digits=visible_last_digits)

    label = resolve_number_label(number, from_numbers) if show_labels else None
    if label:
        return f"**{label}**\n`{display_number}`"
    return f"`{display_number}`"


def format_route_summary(
    *,
    from_number: str,
    to_number: str,
    from_numbers: list[dict[str, Any]] | list[Any],
    show_labels: bool,
) -> str:
    from_label = resolve_number_label(from_number, from_numbers) if show_labels else None
    to_label = resolve_number_label(to_number, from_numbers) if show_labels else None

    left = from_label or format_phone_display(from_number)
    right = to_label or format_phone_display(to_number)
    return f"**{left}** → **{right}**"


def format_message_block(text: str, *, max_chars: int) -> str:
    body = str(text or "").strip() or "[empty message]"
    if len(body) > max_chars:
        body = body[: max_chars - 20] + "\n...[truncated]"
    return f"```\n{body}\n```"
