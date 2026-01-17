from __future__ import annotations

from contextlib import suppress
from datetime import datetime, timezone

import discord


def _member_avatar_url(user: discord.abc.User) -> str | None:
    """Best-effort avatar URL that works across discord.py versions and user types."""
    try:
        return str(user.display_avatar.url)
    except Exception:
        pass
    try:
        avatar = getattr(user, "avatar", None)
        if avatar:
            return str(avatar.url)
    except Exception:
        pass
    try:
        return str(user.default_avatar.url)
    except Exception:
        return None


def apply_member_header(embed: discord.Embed, user: discord.abc.User) -> None:
    """Apply author icon + thumbnail if an avatar URL is available."""
    url = _member_avatar_url(user)
    if not url:
        return
    with suppress(Exception):
        embed.set_author(name=str(user), icon_url=url)
        embed.set_thumbnail(url=url)


_LABEL_OVERRIDES: dict[str, str] = {
    # Whop / membership
    "product": "Membership",
    "member_since": "Member Since",
    "renewal_start": "Billing Period Started",
    "renewal_end": "Next Billing Date",
    "trial_end": "Trial Ends",
    "remaining_days": "Remaining Days",
    "dashboard_url": "Whop Dashboard",
    "manage_url": "Whop Billing Manage",
    "total_spent": "Total Spent",
    "last_success_paid_at": "Last Successful Payment",
    "last_payment_failure": "Payment Issue",
    "cancel_at_period_end": "Cancel At Period End",
    "is_first_membership": "First Membership",
    "last_payment_method": "Last Payment Method",
    "last_payment_type": "Last Payment Type",
    # Discord / staff
    "access_roles": "Current Roles",
    "account_created": "Discord Account Created",
    "roles_removed": "Roles Removed",
}


def _human_label(key: str, *, label_overrides: dict[str, str] | None = None) -> str:
    k = str(key or "").strip()
    if not k:
        return ""
    if label_overrides and k in label_overrides:
        return str(label_overrides[k]).strip() or k
    if k in _LABEL_OVERRIDES:
        return _LABEL_OVERRIDES[k]
    # If already human-looking, keep as-is.
    if "_" not in k and "-" not in k:
        return k[:1].upper() + k[1:] if k else k
    # snake_case / kebab-case -> Title Case
    parts = [p for p in k.replace("-", "_").split("_") if p]
    return " ".join(p[:1].upper() + p[1:] for p in parts) if parts else k


def _kv_line(
    key: str,
    value: object,
    *,
    keep_blank: bool = False,
    label_overrides: dict[str, str] | None = None,
) -> str | None:
    """Format `Label: value` while hiding blanks by default."""
    raw = str(key or "").strip()
    label = _human_label(raw, label_overrides=label_overrides)
    if not label:
        return None
    if value is None:
        return f"{label}: —" if keep_blank else None
    s = str(value).strip()
    if not s or s == "—":
        return f"{label}: —" if keep_blank else None
    return f"{label}: {s}"


def kv_block(
    pairs: list[tuple[str, object]],
    *,
    keep_blank_keys: set[str] | None = None,
    label_overrides: dict[str, str] | None = None,
) -> str:
    keep = keep_blank_keys or set()
    lines: list[str] = []
    for k, v in pairs:
        line = _kv_line(k, v, keep_blank=(k in keep), label_overrides=label_overrides)
        if line:
            lines.append(line)
    return ("\n".join(lines)[:1024]) if lines else "—"


def brief_payment_kv(brief: dict | None) -> list[tuple[str, object]]:
    b = brief if isinstance(brief, dict) else {}
    return [
        ("status", b.get("status")),
        ("product", b.get("product")),
        ("member_since", b.get("member_since")),
        ("trial_end", b.get("trial_end")),
        ("renewal_start", b.get("renewal_start")),
        ("renewal_end", b.get("renewal_end")),
        ("remaining_days", b.get("remaining_days")),
        ("dashboard_url", b.get("dashboard_url")),
        ("manage_url", b.get("manage_url")),
        ("total_spent", b.get("total_spent")),
        ("last_success_paid_at", b.get("last_success_paid_at")),
        ("cancel_at_period_end", b.get("cancel_at_period_end")),
        ("is_first_membership", b.get("is_first_membership")),
        ("last_payment_method", b.get("last_payment_method")),
        ("last_payment_type", b.get("last_payment_type")),
        ("last_payment_failure", b.get("last_payment_failure")),
    ]


def _truthy(v: object) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _infer_event_kind(title: str) -> str:
    t = (title or "").lower()
    if "cancellation scheduled" in t:
        return "cancellation_scheduled"
    if "payment failed" in t:
        return "payment_failed"
    if "deactivated" in t:
        return "deactivated"
    return "active"


def build_case_minimal_embed(
    *,
    title: str,
    member: discord.Member,
    access_roles: str,
    whop_brief: dict | None,
    color: int,
    event_kind: str | None = None,
) -> discord.Embed:
    """Minimal staff case embed (for payment-failure / member-cancelation)."""
    b = whop_brief if isinstance(whop_brief, dict) else {}
    kind = str(event_kind or "").strip().lower() or _infer_event_kind(title)
    cancel_at_period_end = _truthy(b.get("cancel_at_period_end"))
    label_overrides: dict[str, str] = {}
    if kind in {"cancellation_scheduled", "deactivated"}:
        label_overrides["renewal_start"] = "Current Period Started"
        label_overrides["renewal_end"] = "Access Ends On"
    else:
        label_overrides["renewal_start"] = "Billing Period Started"
        label_overrides["renewal_end"] = "Access Ends On" if cancel_at_period_end else "Next Billing Date"
    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    apply_member_header(embed, member)
    embed.add_field(
        name="Member Info",
        value=kv_block(
            [
                ("member", member.mention),
                ("product", b.get("product")),
                ("member_since", b.get("member_since")),
                ("renewal_start", b.get("renewal_start")),
                ("renewal_end", b.get("renewal_end")),
                ("remaining_days", b.get("remaining_days")),
                ("dashboard_url", b.get("dashboard_url")),
                ("manage_url", b.get("manage_url")),
                ("total_spent", b.get("total_spent")),
                ("last_payment_failure", b.get("last_payment_failure")),
            ],
            label_overrides=label_overrides,
        ),
        inline=False,
    )
    embed.add_field(
        name="Discord Access",
        value=kv_block([("access_roles", access_roles)]),
        inline=False,
    )
    embed.set_footer(text="RSCheckerbot")
    return embed


def build_member_status_detailed_embed(
    *,
    title: str,
    member: discord.Member,
    access_roles: str,
    color: int,
    discord_kv: list[tuple[str, object]] | None = None,
    member_kv: list[tuple[str, object]] | None = None,
    whop_brief: dict | None = None,
    event_kind: str | None = None,
) -> discord.Embed:
    """Detailed staff embed for member-status-logs (key:value cards)."""
    b = whop_brief if isinstance(whop_brief, dict) else {}
    kind = str(event_kind or "").strip().lower() or _infer_event_kind(title)
    cancel_at_period_end = _truthy(b.get("cancel_at_period_end"))
    label_overrides: dict[str, str] = {}
    if kind in {"cancellation_scheduled", "deactivated"}:
        label_overrides["renewal_start"] = "Current Period Started"
        label_overrides["renewal_end"] = "Access Ends On"
    else:
        label_overrides["renewal_start"] = "Billing Period Started"
        label_overrides["renewal_end"] = "Access Ends On" if cancel_at_period_end else "Next Billing Date"
    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    apply_member_header(embed, member)
    embed.add_field(
        name="Member Info",
        value=kv_block(
            [
                ("member", member.mention),
                *([p for p in (member_kv or []) if isinstance(p, tuple) and len(p) == 2]),
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Discord Access",
        value=kv_block(
            [
                ("access_roles", access_roles),
                *([p for p in (discord_kv or []) if isinstance(p, tuple) and len(p) == 2]),
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Payment Info",
        value=kv_block(
            brief_payment_kv(whop_brief),
            keep_blank_keys={"is_first_membership"},
            label_overrides=label_overrides,
        ),
        inline=False,
    )
    embed.set_footer(text="RSCheckerbot • Member Status Tracking")
    return embed

