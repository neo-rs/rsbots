from __future__ import annotations

from typing import Any

from app.discord_format import format_phone_display, resolve_number_label
from app.phone import normalize_e164


def render_thread_content(
    *,
    our_line: str,
    remote_party: str,
    lines: list[dict[str, Any]],
    display_name: str | None,
    from_numbers: list[dict[str, Any]] | list[Any],
    max_chars: int,
) -> str:
    remote_display = (display_name or "").strip() or format_phone_display(remote_party)
    our_label = resolve_number_label(our_line, from_numbers) or format_phone_display(our_line)

    header = [
        f"**{remote_display}**",
        f"`{format_phone_display(remote_party)}`",
        f"Your line: **{our_label}** (`{format_phone_display(our_line)}`)",
        "—" * 28,
    ]

    body_lines: list[str] = []
    remote_short = _short_label(remote_party)
    for entry in lines:
        direction = str(entry.get("direction") or "in")
        text = str(entry.get("text") or "").strip() or "[empty]"
        if direction == "out":
            body_lines.append(f"**Me:** {text}")
        else:
            body_lines.append(f"**{remote_short}:** {text}")

    if not body_lines:
        body_lines.append("_(no messages yet — use **Send message**)_")

    content = "\n".join(header + [""] + body_lines)
    if len(content) <= max_chars:
        return content

    # Trim oldest chat lines until we fit.
    trimmed = list(body_lines)
    while len(trimmed) > 1:
        trimmed.pop(0)
        candidate = "\n".join(header + ["", "_(older messages truncated)_", ""] + trimmed)
        if len(candidate) <= max_chars:
            return candidate
    return content[: max_chars - 20] + "\n...[truncated]"


def _short_label(number: str) -> str:
    digits = "".join(ch for ch in normalize_e164(number) if ch.isdigit())
    if len(digits) >= 4:
        return digits[-4:]
    return digits or "them"
