from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import urlparse, urlunparse

import discord


ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def format_log_user_id(user_id: int | str | None) -> str:
    """Log segment: numeric User-ID (grep) plus Discord mention <@id> so journal/log channels resolve the user."""
    if user_id is None:
        return "User-ID:?"
    try:
        uid = int(user_id)
        return f"User-ID:{uid} <@{uid}>"
    except (TypeError, ValueError):
        return f"User-ID:{user_id!s}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)



def iso_now() -> str:
    return utc_now().strftime(ISO_FORMAT)



def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=timezone.utc)
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


def is_well_formed_http_url(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return False
    parsed = urlparse(raw)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def normalize_discord_image_url(value: str | None) -> str:
    """Normalize Discord CDN/media image URLs to stable embed-safe form."""
    raw = (value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return raw
    host = parsed.netloc.lower()
    if host == "media.discordapp.net":
        # media.discordapp.net links often include expiring query params.
        return urlunparse((parsed.scheme, "cdn.discordapp.com", parsed.path, "", "", ""))
    return raw


def parse_banner_urls(value: str | None, max_urls: int = 2) -> list[str]:
    """Parse up to max_urls banner links from newline/comma-separated input."""
    raw = (value or "").strip()
    if not raw:
        return []
    parts = raw.replace(",", "\n").splitlines()
    urls: list[str] = []
    for part in parts:
        normalized = normalize_discord_image_url(part.strip())
        if not normalized:
            continue
        urls.append(normalized)
        if len(urls) >= max_urls:
            break
    return urls


def parse_attachment_urls(value: str | None, max_urls: int = 2) -> list[str]:
    """Parse up to max_urls attachment-style image links."""
    return parse_banner_urls(value, max_urls=max_urls)


def build_attachment_content(value: str | None, max_urls: int = 2) -> str:
    urls = parse_attachment_urls(value, max_urls=max_urls)
    return "\n".join(urls)


def build_dm_embeds(data: dict[str, Any], embed_color: int) -> list[discord.Embed]:
    """Build embeds for body + up to two banner images."""
    banner_urls = parse_banner_urls(data.get("banner_url"), max_urls=2)
    description = (data.get("message_body") or "").strip()

    embeds: list[discord.Embed] = []
    first = discord.Embed(description=description or "\u200b", color=embed_color)
    if banner_urls:
        first.set_image(url=banner_urls[0])
    embeds.append(first)

    # Discord allows one image per embed; use a second embed for the second image.
    for extra_url in banner_urls[1:]:
        extra = discord.Embed(color=embed_color)
        extra.set_image(url=extra_url)
        embeds.append(extra)
    return embeds
