"""
Free-member RS preview: webhook posts when a Daily Reminder is posted/edited in RS.

Canonical state: RSAdminBot/data/review_rs_free_preview_state.json
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

import discord

from review_rs_daily_blurbs import (
    blurb_for_channel_id,
    format_free_preview_intro,
    format_reminder_date_label,
    blurbs_from_reminder_message,
)


@dataclass(frozen=True)
class FreePreviewCategorySpec:
    category_id: int
    header_lines: Tuple[str, ...]


DEFAULT_FREE_CATEGORIES: Tuple[FreePreviewCategorySpec, ...] = (
    FreePreviewCategorySpec(1313260017989713981, ("Today's Release Schedule",)),
    FreePreviewCategorySpec(1400619782692409404, ("This Week's Upcoming Heat",)),
    FreePreviewCategorySpec(
        1400165387001135134,
        ("Ongoing Instore Cooks", "**Instore Important**"),
    ),
)

def _state_file_path(base_path: Path) -> Path:
    data = base_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    return data / "review_rs_free_preview_state.json"


def load_preview_state(base_path: Path) -> Dict[str, Any]:
    path = _state_file_path(base_path)
    try:
        raw = json.loads(path.read_text(encoding="utf-8") or "{}")
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def save_preview_state(base_path: Path, state: Dict[str, Any]) -> None:
    path = _state_file_path(base_path)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _date_key(month: int, day: int) -> str:
    return format_reminder_date_label(month, day)


async def format_category_block(
    guild: discord.Guild,
    category_id: int,
    header_lines: Sequence[str],
    reminder_blurbs: Dict[int, str],
    *,
    blurb_max: int,
    list_text_channels: Callable[[discord.Guild, int], Awaitable[List[discord.TextChannel]]],
) -> List[str]:
    lines: List[str] = []
    for hl in header_lines:
        s = str(hl or "").strip()
        if not s:
            continue
        if s.startswith("**") and s.endswith("**"):
            lines.append(s)
        else:
            lines.append(f"**{s}**")

    text_channels = await list_text_channels(guild, int(category_id))
    if not text_channels:
        lines.append("- (no text channels found)")
        return lines

    for ch in text_channels:
        extra = blurb_for_channel_id(int(ch.id), str(ch.name), reminder_blurbs, max_blurb_chars=blurb_max)
        lines.append(f"- <#{int(ch.id)}> - {extra}")
    return lines


async def build_free_preview_bodies(
    guild: discord.Guild,
    *,
    month: int,
    day: int,
    reminder_blurbs: Dict[int, str],
    blurb_max: int,
    siren_emoji_id: str,
    footer_template: str,
    paid_channel_id: int,
    locked_emoji_id: str,
    list_text_channels: Callable[[discord.Guild, int], Awaitable[List[discord.TextChannel]]],
    category_specs: Sequence[FreePreviewCategorySpec] = DEFAULT_FREE_CATEGORIES,
) -> List[str]:
    """One string per webhook message (already chunked under 1900 by caller)."""
    intro = format_free_preview_intro(month=month, day=day, siren_emoji_id=siren_emoji_id)
    footer = str(footer_template or "").strip()
    if paid_channel_id:
        footer = footer.replace("<#1155729485048594483>", f"<#{int(paid_channel_id)}>")
    if locked_emoji_id and footer and "<a:lockedup:" not in footer:
        footer = footer + f" <a:lockedup:{locked_emoji_id}>"

    bodies: List[str] = [intro]
    for spec in category_specs:
        block = await format_category_block(
            guild,
            spec.category_id,
            spec.header_lines,
            reminder_blurbs,
            blurb_max=blurb_max,
            list_text_channels=list_text_channels,
        )
        bodies.append("\n".join(block))
    if footer:
        bodies.append(footer)
    return bodies


async def delete_webhook_messages(webhook: discord.Webhook, message_ids: Sequence[int]) -> None:
    for mid in message_ids:
        try:
            await webhook.delete_message(int(mid))
        except Exception:
            pass


async def resolve_avatar_url(bot: discord.Client, user_id: int) -> Optional[str]:
    if not user_id:
        return None
    try:
        u = await bot.fetch_user(int(user_id))
        if u and u.display_avatar:
            return str(u.display_avatar.url)
    except Exception:
        pass
    return None


async def purge_other_dates(
    webhook: discord.Webhook,
    state: Dict[str, Any],
    keep_date_key: str,
) -> None:
    by_date = state.get("by_date")
    if not isinstance(by_date, dict):
        state["by_date"] = {}
        return
    for key in list(by_date.keys()):
        if str(key) == str(keep_date_key):
            continue
        entry = by_date.get(key)
        if not isinstance(entry, dict):
            del by_date[key]
            continue
        ids = entry.get("webhook_message_ids") or []
        if isinstance(ids, list):
            await delete_webhook_messages(webhook, [int(x) for x in ids if str(x).isdigit()])
        del by_date[key]
    state["active_date"] = keep_date_key


async def send_free_preview_suite(
    bot: discord.Client,
    *,
    base_path: Path,
    webhook_url: str,
    username: str,
    avatar_url: Optional[str],
    bodies: List[str],
    date_key: str,
    reminder_source_message_id: int,
) -> Tuple[List[int], Optional[str]]:
    """Delete prior suite for this date (and other dates), post new chunks, update state."""
    wh_partial = discord.Webhook.from_url(webhook_url.strip(), client=bot)
    webhook = await wh_partial.fetch()

    state = load_preview_state(base_path)
    by_date = state.get("by_date")
    if not isinstance(by_date, dict):
        by_date = {}
        state["by_date"] = by_date

    await purge_other_dates(webhook, state, date_key)

    entry = by_date.get(date_key)
    if not isinstance(entry, dict):
        entry = {}
        by_date[date_key] = entry
    old_ids = entry.get("webhook_message_ids") or []
    if isinstance(old_ids, list) and old_ids:
        await delete_webhook_messages(webhook, [int(x) for x in old_ids if str(x).isdigit()])

    allowed = discord.AllowedMentions(everyone=True, roles=False, users=False)
    posted: List[int] = []
    send_kwargs: Dict[str, Any] = {
        "username": username or None,
        "allowed_mentions": allowed,
        "wait": True,
    }
    if avatar_url:
        send_kwargs["avatar_url"] = avatar_url
    try:
        for body in bodies:
            if not str(body or "").strip():
                continue
            msg = await webhook.send(str(body), **send_kwargs)
            if msg and getattr(msg, "id", None):
                posted.append(int(msg.id))
    except Exception as e:
        entry["webhook_message_ids"] = posted
        entry["reminder_source_message_id"] = str(reminder_source_message_id)
        entry["updated_at_iso"] = datetime.now(timezone.utc).isoformat()
        save_preview_state(base_path, state)
        return posted, f"{type(e).__name__}: {e}"

    entry["webhook_message_ids"] = posted
    entry["reminder_source_message_id"] = str(reminder_source_message_id)
    entry["updated_at_iso"] = datetime.now(timezone.utc).isoformat()
    state["active_date"] = date_key
    save_preview_state(base_path, state)
    return posted, None


def chunk_single_body_lines(lines: List[str], *, max_chars: int = 1900) -> List[str]:
    """Chunk a list of lines (category block) like review_rs _chunk_lines."""
    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0
    for line in lines:
        add_len = len(line) + (1 if cur else 0)
        if cur and (cur_len + add_len) > max_chars:
            chunks.append("\n".join(cur))
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += add_len
    if cur:
        chunks.append("\n".join(cur))
    return chunks


async def run_free_preview_for_reminder(
    bot: discord.Client,
    reminder_message: discord.Message,
    *,
    base_path: Path,
    rs_guild: discord.Guild,
    webhook_url: str,
    username: str,
    avatar_user_id: int,
    siren_emoji_id: str,
    locked_emoji_id: str,
    paid_channel_id: int,
    footer_template: str,
    blurb_max: int,
    list_text_channels: Callable[[discord.Guild, int], Awaitable[List[discord.TextChannel]]],
    category_specs: Sequence[FreePreviewCategorySpec] = DEFAULT_FREE_CATEGORIES,
) -> Tuple[bool, str]:
    parsed, reminder_blurbs = blurbs_from_reminder_message(reminder_message)
    if not parsed:
        return False, "Could not parse DAILY REMINDER - MM/DD from message."
    month, day = parsed
    date_key = _date_key(month, day)

    avatar_url = await resolve_avatar_url(bot, avatar_user_id)
    section_bodies = await build_free_preview_bodies(
        rs_guild,
        month=month,
        day=day,
        reminder_blurbs=reminder_blurbs,
        blurb_max=blurb_max,
        siren_emoji_id=siren_emoji_id,
        footer_template=footer_template,
        paid_channel_id=paid_channel_id,
        locked_emoji_id=locked_emoji_id,
        list_text_channels=list_text_channels,
        category_specs=category_specs,
    )

    # Flatten: intro + each section (chunked if needed) + footer
    all_chunks: List[str] = []
    if section_bodies:
        all_chunks.append(section_bodies[0])
        for part in section_bodies[1:-1]:
            for c in chunk_single_body_lines(part.splitlines(), max_chars=1900):
                all_chunks.append(c)
        if len(section_bodies) > 1:
            all_chunks.append(section_bodies[-1])

    posted, err = await send_free_preview_suite(
        bot,
        base_path=base_path,
        webhook_url=webhook_url,
        username=username,
        avatar_url=avatar_url,
        bodies=all_chunks,
        date_key=date_key,
        reminder_source_message_id=int(reminder_message.id),
    )
    if err:
        return False, f"Posted {len(posted)} chunk(s) then failed: {err}"
    return True, f"Free preview posted ({len(posted)} message(s)) for **{date_key}**."
