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


def _kv_line(key: str, value: object, *, keep_blank: bool = False) -> str | None:
    """Format `key: value` while hiding blanks by default."""
    k = str(key or "").strip()
    if not k:
        return None
    if value is None:
        return f"{k}: —" if keep_blank else None
    s = str(value).strip()
    if not s or s == "—":
        return f"{k}: —" if keep_blank else None
    return f"{k}: {s}"


def kv_block(pairs: list[tuple[str, object]], *, keep_blank_keys: set[str] | None = None) -> str:
    keep = keep_blank_keys or set()
    lines: list[str] = []
    for k, v in pairs:
        line = _kv_line(k, v, keep_blank=(k in keep))
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
        ("cancel_at_period_end", b.get("cancel_at_period_end")),
        ("is_first_membership", b.get("is_first_membership")),
        ("last_payment_method", b.get("last_payment_method")),
        ("last_payment_type", b.get("last_payment_type")),
        ("last_payment_failure", b.get("last_payment_failure")),
    ]


def build_case_minimal_embed(
    *,
    title: str,
    member: discord.Member,
    access_roles: str,
    whop_brief: dict | None,
    color: int,
) -> discord.Embed:
    """Minimal staff case embed (for payment-failure / member-cancelation)."""
    b = whop_brief if isinstance(whop_brief, dict) else {}
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
                ("last_payment_failure", b.get("last_payment_failure")),
            ]
        ),
        inline=False,
    )
    embed.add_field(
        name="Discord Info",
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
) -> discord.Embed:
    """Detailed staff embed for member-status-logs (key:value cards)."""
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
        name="Discord Info",
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
        value=kv_block(brief_payment_kv(whop_brief), keep_blank_keys={"is_first_membership"}),
        inline=False,
    )
    embed.set_footer(text="RSCheckerbot • Member Status Tracking")
    return embed

