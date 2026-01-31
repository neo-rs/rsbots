from __future__ import annotations

from contextlib import suppress

import discord


def slug_channel_name(s: str, *, max_len: int = 90) -> str:
    """Discord channel name slug (lowercase, alnum + hyphen)."""
    raw = str(s or "").strip().lower()
    out: list[str] = []
    last_dash = False
    for ch in raw:
        ok = ("a" <= ch <= "z") or ("0" <= ch <= "9")
        if ok:
            out.append(ch)
            last_dash = False
        else:
            if not last_dash:
                out.append("-")
                last_dash = True
    slug = "".join(out).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    if not slug:
        slug = "case"
    return slug[: int(max_len or 90)]


async def ensure_ticket_like_channel(
    *,
    guild: discord.Guild,
    category_id: int,
    case_key: str,
    channel_name: str,
    topic: str,
    reason: str = "RSCheckerbot: ticket-like channel",
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] | None = None,
    apply_overwrites_if_found: bool = False,
) -> discord.TextChannel | None:
    """Find or create a ticket-like text channel under a category.

    Canonical behavior (used by existing Whop dispute/resolution channels):
    - First scan all text channels for `case_key` in the topic (channel may have been moved).
    - Then scan within the desired category.
    - Else create a new text channel under the category with the provided topic.

    Support-ticket behavior:
    - Can optionally apply/refresh overwrites for an existing channel.
    """
    if not isinstance(guild, discord.Guild):
        return None
    if int(category_id or 0) <= 0:
        return None

    try:
        cat = guild.get_channel(int(category_id))
    except Exception:
        cat = None
    if not isinstance(cat, discord.CategoryChannel):
        return None

    key = str(case_key or "").strip()

    async def _maybe_update_existing(ch: discord.TextChannel) -> None:
        # Best-effort move to requested category
        with suppress(Exception):
            if int(getattr(ch, "category_id", 0) or 0) != int(cat.id):
                await ch.edit(category=cat, reason=reason)
        # Best-effort rename to match requested channel_name
        with suppress(Exception):
            nm = slug_channel_name(channel_name, max_len=90)
            if nm and str(getattr(ch, "name", "") or "") != nm:
                await ch.edit(name=nm, reason=reason)
        # Best-effort topic refresh (keep case_key)
        with suppress(Exception):
            top = str(topic or "").strip()
            if key and key not in top:
                top = (top + "\n" + key).strip()
            if len(top) > 950:
                top = top[:950]
            if top and str(getattr(ch, "topic", "") or "") != top:
                await ch.edit(topic=top, reason=reason)
        # Optional overwrite refresh (support tickets)
        if apply_overwrites_if_found and overwrites is not None:
            with suppress(Exception):
                await ch.edit(overwrites=overwrites, reason=reason)

    # First: global lookup by topic key (channel could have been moved categories).
    if key:
        with suppress(Exception):
            for ch in list(getattr(guild, "text_channels", []) or []):
                if not isinstance(ch, discord.TextChannel):
                    continue
                if key in str(ch.topic or ""):
                    await _maybe_update_existing(ch)
                    return ch

    # Second: category-local scan.
    with suppress(Exception):
        for ch in list(cat.channels):
            if not isinstance(ch, discord.TextChannel):
                continue
            if key and key in str(ch.topic or ""):
                await _maybe_update_existing(ch)
                return ch

    nm = slug_channel_name(channel_name, max_len=90)
    top = str(topic or "").strip()
    if key and key not in top:
        top = (top + "\n" + key).strip()
    if len(top) > 950:
        top = top[:950]
    try:
        created = await guild.create_text_channel(
            name=nm,
            category=cat,
            topic=top,
            overwrites=overwrites,
            reason=reason,
        )
        return created if isinstance(created, discord.TextChannel) else None
    except Exception:
        return None

