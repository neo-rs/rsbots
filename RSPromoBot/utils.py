from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

import discord


ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> datetime:
    return datetime.now(UTC)



def iso_now() -> str:
    return utc_now().strftime(ISO_FORMAT)



def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None



def next_run_iso(minutes: int) -> str:
    return (utc_now() + timedelta(minutes=minutes)).strftime(ISO_FORMAT)



def human_rate(batch_size: int, interval_minutes: int) -> str:
    user_word = "user" if batch_size == 1 else "users"
    minute_word = "minute" if interval_minutes == 1 else "minutes"
    return f"{batch_size} {user_word} every {interval_minutes} {minute_word}"


def estimated_duration_str(recipient_count: int, batch_size: int, interval_minutes: int) -> str:
    """Return human-readable estimated duration e.g. '~3h 5m' or '~45m'."""
    if recipient_count <= 0 or batch_size <= 0:
        return "—"
    batches = (recipient_count + batch_size - 1) // batch_size
    total_minutes = batches * interval_minutes
    if total_minutes < 60:
        return f"~{total_minutes}m"
    hours, mins = divmod(total_minutes, 60)
    if mins == 0:
        return f"~{hours}h"
    return f"~{hours}h {mins}m"



def has_any_allowed_role(member: discord.Member, allowed_role_ids: Iterable[str]) -> bool:
    role_ids = {str(role.id) for role in member.roles}
    return any(str(role_id) in role_ids for role_id in allowed_role_ids)



def chunk_list(items: list, size: int) -> list[list]:
    return [items[index:index + size] for index in range(0, len(items), size)]



def build_cta_view(label: str | None, url: str | None) -> discord.ui.View | None:
    if not label or not url:
        return None
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(label=label, url=url))
    return view


def build_dm_embeds(data: dict[str, Any], embed_color: int) -> list[discord.Embed]:
    """Build a single embed (banner + body) with one color so banner and message align. Campaign name is not shown in output."""
    banner_url = (data.get("banner_url") or "").strip()
    description = (data.get("message_body") or "").strip()

    # Single embed: one color, image on top then description, no title (campaign name stays internal only)
    embed = discord.Embed(description=description or "\u200b", color=embed_color)
    if banner_url:
        embed.set_image(url=banner_url)
    return [embed]
