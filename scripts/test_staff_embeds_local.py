#!/usr/bin/env python3
"""
Local (no-Discord) formatter test for RSCheckerbot staff embeds.

This script is meant to be run locally to validate:
- human-friendly labels (no snake_case in visible output)
- section headers (Discord Access)
- context-aware date labels (Next Billing Date vs Access Ends On)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


class _URL:
    def __init__(self, url: str):
        self.url = url


class FakeMember:
    def __init__(self, user_id: int, name: str):
        self.id = int(user_id)
        self._name = str(name)
        self.mention = f"<@{self.id}>"
        # Enough for staff_embeds.apply_member_header()
        self.display_avatar = _URL("https://example.invalid/avatar.png")

    def __str__(self) -> str:
        return self._name


def _field(embed_dict: dict, name: str) -> dict | None:
    for f in embed_dict.get("fields") or []:
        if f.get("name") == name:
            return f
    return None


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    # Allow importing namespace packages like RSCheckerbot/ without requiring __init__.py
    sys.path.insert(0, str(repo_root))

    from RSCheckerbot.staff_embeds import build_case_minimal_embed, build_member_status_detailed_embed

    member = FakeMember(123, "TestUser")  # type: ignore[arg-type]
    brief = {
        "status": "past_due",
        "product": "Reselling Secrets",
        "member_since": "May 12, 2025",
        "renewal_start": "January 5, 2026",
        "renewal_end": "April 7, 2026",
        "cancel_at_period_end": "no",
        "last_payment_failure": "Insufficient funds.",
        "is_first_membership": "—",
    }

    detailed_pf = build_member_status_detailed_embed(
        title="❌ Payment Failed — Action Needed",
        member=member,  # type: ignore[arg-type]
        access_roles="Members",
        color=0xED4245,
        discord_kv=[("event", "payment.failed")],
        whop_brief=brief,
        event_kind="payment_failed",
    ).to_dict()

    minimal_cancel = build_case_minimal_embed(
        title="⚠️ Cancellation Scheduled",
        member=member,  # type: ignore[arg-type]
        access_roles="Members",
        whop_brief={**brief, "cancel_at_period_end": "yes"},
        color=0xFEE75C,
        event_kind="cancellation_scheduled",
    ).to_dict()

    # Assertions: no snake_case labels, correct headers, correct date labels
    for emb in (detailed_pf, minimal_cancel):
        for f in emb.get("fields") or []:
            v = str(f.get("value") or "")
            if "member_since" in v or "renewal_start" in v or "renewal_end" in v or "access_roles" in v:
                raise AssertionError("Found snake_case in visible embed output")

    discord_access = _field(detailed_pf, "Discord Access")
    if not discord_access:
        raise AssertionError("Missing field: Discord Access")

    payment_info = _field(detailed_pf, "Payment Info")
    if not payment_info or "Next Billing Date:" not in str(payment_info.get("value") or ""):
        raise AssertionError("Payment Info missing Next Billing Date label for payment_failed")

    member_info_cancel = _field(minimal_cancel, "Member Info")
    if not member_info_cancel or "Access Ends On:" not in str(member_info_cancel.get("value") or ""):
        raise AssertionError("Cancellation Scheduled missing Access Ends On label")

    out = {
        "ok": True,
        "examples": {"detailed_payment_failed": detailed_pf, "minimal_cancellation_scheduled": minimal_cancel},
    }
    sys.stdout.write(json.dumps(out, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

