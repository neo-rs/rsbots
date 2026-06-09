from __future__ import annotations


def format_send_error(exc: Exception) -> str:
    msg = str(exc).lower()
    if "40021" in msg or "not a mobile phone" in msg or "mobile-only" in msg:
        return (
            "This line can only text real cell phones (Telnyx mobile-only is on). "
            "It cannot reply to your other Telnyx numbers. "
            "Use the local line channel to text the toll-free line, or disable mobile-only in Telnyx."
        )
    if "10002" in msg or "invalid destination" in msg:
        return "Invalid destination number — use a real mobile number."
    return f"Send failed: {exc}"
