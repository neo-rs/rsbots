import asyncio
import io
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import discord
from discord.ext import commands

from explainable_log import ExplainableLog


TITLE_PATTERN = re.compile(r"^\s*New\s+Catalog\s*-\s*(?P<store>.+?)\s+(?P<category>.+?)\s*$", re.IGNORECASE)
WHITESPACE_RE = re.compile(r"\s+")
DISCORD_MESSAGE_URL_RE = re.compile(
    r"^https://discord\.com/channels/(?P<guild_id>\d+)/(?P<channel_id>\d+)/(?P<message_id>\d+)$",
    re.IGNORECASE,
)


def _embed_description_text(embed: discord.Embed) -> str:
    return embed.description if embed.description is not None else ""


def _embed_colour_value(embed: discord.Embed) -> Optional[int]:
    c = embed.colour
    if c is None:
        return None
    return int(c.value)


def _first_link_button_label_url(message: discord.Message) -> Optional[Tuple[str, str]]:
    for row in message.components or []:
        for child in getattr(row, "children", []) or []:
            url = getattr(child, "url", None)
            if url:
                label = getattr(child, "label", None) or ""
                return (str(label), str(url))
    return None


def _navigation_reply_matches_target(
    reply_message: discord.Message,
    embed: discord.Embed,
    *,
    button_label: str,
    button_url: str,
) -> bool:
    if not reply_message.embeds:
        return False
    current = reply_message.embeds[0]
    if _embed_description_text(current) != _embed_description_text(embed):
        return False
    if _embed_colour_value(current) != _embed_colour_value(embed):
        return False
    link = _first_link_button_label_url(reply_message)
    if link is None:
        return False
    cur_label, cur_url = link
    return cur_label == button_label and cur_url == button_url


@dataclass(frozen=True)
class Config:
    token: str
    source_bot_id: int
    nav_emoji: str
    embed_color: int
    placeholder_text: str
    separator: str
    allowed_channel_ids: List[int]
    allowed_guild_ids: List[int]
    state_path: Path
    log_level: str
    title_regex: str
    ignore_bots_except_source: bool
    navigation_button_label: str
    main_catalog_label: str
    main_catalog_banner_url: str
    main_catalog_intro: str
    menu_guild_id: int
    menu_channel_id: int
    menu_message_id: int
    explain_trace: bool
    log_skip_traffic: bool
    navigation_edit_min_interval_seconds: float
    delete_superseded_source_catalog_messages: bool

    @staticmethod
    def from_dict(data: dict, root: Path) -> "Config":
        token = str(data.get("token", "")).strip()
        if not token:
            raise ValueError("config.token is required")

        source_bot_id = int(data["source_bot_id"])
        nav_emoji = str(data.get("nav_emoji", "")).strip()
        if not nav_emoji:
            raise ValueError("config.nav_emoji is required")

        menu_url = str(data.get("menu_message_url", "")).strip()
        match = DISCORD_MESSAGE_URL_RE.match(menu_url)
        if not match:
            raise ValueError("config.menu_message_url must be a full Discord message URL")

        allowed_channel_ids = [int(v) for v in data.get("allowed_channel_ids", [])]
        allowed_guild_ids = [int(v) for v in data.get("allowed_guild_ids", [])]
        menu_guild_id = int(match.group("guild_id"))
        if allowed_guild_ids and menu_guild_id not in allowed_guild_ids:
            raise ValueError("config.menu_message_url guild_id must be listed in allowed_guild_ids")

        state_rel = str(data.get("state_path", "data/navigation_state.json"))
        state_path = (root / state_rel).resolve()
        state_path.parent.mkdir(parents=True, exist_ok=True)

        nav_edit_interval = float(data.get("navigation_edit_min_interval_seconds", 0.35))
        if nav_edit_interval < 0:
            nav_edit_interval = 0.0
        if nav_edit_interval > 10.0:
            nav_edit_interval = 10.0

        return Config(
            token=token,
            source_bot_id=source_bot_id,
            nav_emoji=nav_emoji,
            embed_color=int(str(data.get("embed_color", "0x2B2D31")), 16),
            placeholder_text=str(data.get("placeholder_text", "Waiting for more store links...")).strip(),
            separator=str(data.get("separator", "")).strip(),
            allowed_channel_ids=allowed_channel_ids,
            allowed_guild_ids=allowed_guild_ids,
            state_path=state_path,
            log_level=str(data.get("log_level", "INFO")).upper(),
            title_regex=str(data.get("title_regex", TITLE_PATTERN.pattern)),
            ignore_bots_except_source=bool(data.get("ignore_bots_except_source", True)),
            navigation_button_label=str(data.get("navigation_button_label", "Main Catalog")).strip() or "Main Catalog",
            main_catalog_label=str(data.get("main_catalog_label", "")).strip(),
            main_catalog_banner_url=str(data.get("main_catalog_banner_url", "")).strip(),
            main_catalog_intro=str(data.get("main_catalog_intro", "")).strip(),
            menu_guild_id=menu_guild_id,
            menu_channel_id=int(match.group("channel_id")),
            menu_message_id=int(match.group("message_id")),
            explain_trace=bool(data.get("explain_trace", False)),
            log_skip_traffic=bool(data.get("log_skip_traffic", False)),
            navigation_edit_min_interval_seconds=nav_edit_interval,
            delete_superseded_source_catalog_messages=bool(
                data.get("delete_superseded_source_catalog_messages", True)
            ),
        )


class NavigationState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data = {
            "categories": {},
            "reply_index": {},
            "main_catalog_by_channel": {},
        }
        self._lock = asyncio.Lock()
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            self.save_sync()
            return
        try:
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            backup = self.path.with_suffix(".corrupt.json")
            self.path.replace(backup)
            self.data = {"categories": {}, "reply_index": {}, "main_catalog_by_channel": {}}
            self.save_sync()

    def save_sync(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    async def upsert_entry(
        self,
        *,
        category_slug: str,
        category_label: str,
        store_slug: str,
        store_label: str,
        source_channel_id: int,
        source_message_id: int,
        source_jump_url: str,
        reply_channel_id: int,
        reply_message_id: int,
    ) -> dict:
        async with self._lock:
            categories = self.data.setdefault("categories", {})
            category = categories.setdefault(
                category_slug,
                {
                    "label": category_label,
                    "stores": {},
                    "reply_targets": {},
                },
            )
            category["label"] = category_label
            category.setdefault("stores", {})[store_slug] = {
                "label": store_label,
                "source_channel_id": str(source_channel_id),
                "source_message_id": str(source_message_id),
                "jump_url": source_jump_url,
            }
            category.setdefault("reply_targets", {})[str(reply_message_id)] = {
                "reply_channel_id": str(reply_channel_id),
                "reply_message_id": str(reply_message_id),
                "store_slug": store_slug,
                "source_message_id": str(source_message_id),
            }
            self.data.setdefault("reply_index", {})[str(reply_message_id)] = category_slug
            self.save_sync()
            return category

    async def remove_reply_targets_for_store(
        self,
        *,
        category_slug: str,
        store_slug: str,
        channel_id: int,
    ) -> List[Tuple[int, int]]:
        """
        Drop nav reply rows for the same store in this category/channel so a reposted catalog
        does not leave duplicate nav messages in state (stores[store_slug] already overwrites).
        Returns (reply_channel_id, reply_message_id) for each removed row (for Discord delete).
        """
        async with self._lock:
            cat = self.data.get("categories", {}).get(category_slug)
            if not cat:
                return []
            targets = cat.setdefault("reply_targets", {})
            reply_index = self.data.setdefault("reply_index", {})
            removed: List[Tuple[int, int]] = []
            for rid, target in list(targets.items()):
                if target.get("store_slug") != store_slug:
                    continue
                if int(target.get("reply_channel_id", 0)) != channel_id:
                    continue
                targets.pop(rid, None)
                reply_index.pop(rid, None)
                removed.append((int(target["reply_channel_id"]), int(target["reply_message_id"])))
            if removed:
                self.save_sync()
            return removed

    async def set_main_catalog_message(self, channel_id: int, message_id: int) -> None:
        async with self._lock:
            self.data.setdefault("main_catalog_by_channel", {})[str(channel_id)] = str(message_id)
            self.save_sync()

    async def get_main_catalog_message_id(self, channel_id: int) -> Optional[int]:
        async with self._lock:
            value = self.data.setdefault("main_catalog_by_channel", {}).get(str(channel_id))
            return int(value) if value else None

    async def get_category(self, category_slug: str) -> Optional[dict]:
        async with self._lock:
            category = self.data.get("categories", {}).get(category_slug)
            if not category:
                return None
            return json.loads(json.dumps(category))

    async def get_categories_for_channel(self, channel_id: int) -> Dict[str, dict]:
        async with self._lock:
            result: Dict[str, dict] = {}
            for slug, category in self.data.get("categories", {}).items():
                stores = {
                    store_slug: store
                    for store_slug, store in category.get("stores", {}).items()
                    if int(store.get("source_channel_id", 0)) == channel_id
                }
                if stores:
                    result[slug] = {
                        "label": category.get("label", slug),
                        "stores": json.loads(json.dumps(stores)),
                        "reply_targets": json.loads(json.dumps(category.get("reply_targets", {}))),
                    }
            return result

    async def cleanup_missing_reply_targets(self, category_slug: str, stale_reply_ids: List[str]) -> None:
        if not stale_reply_ids:
            return
        async with self._lock:
            latest = self.data.get("categories", {}).get(category_slug, {})
            for reply_id in stale_reply_ids:
                latest.get("reply_targets", {}).pop(reply_id, None)
                self.data.get("reply_index", {}).pop(reply_id, None)
            self.save_sync()


class LinkButtonView(discord.ui.View):
    def __init__(self, *, label: str, url: str) -> None:
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label=label, url=url, style=discord.ButtonStyle.link))


class MultiLinkButtonView(discord.ui.View):
    def __init__(self, buttons: List[Tuple[str, str]]) -> None:
        super().__init__(timeout=None)
        for label, url in buttons[:25]:
            self.add_item(discord.ui.Button(label=label[:80], url=url, style=discord.ButtonStyle.link))


class CatalogNavigationBot(commands.Bot):
    def __init__(self, config: Config) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True
        super().__init__(command_prefix="!", intents=intents)
        self.config_data = config
        self.state = NavigationState(config.state_path)
        self.title_pattern = re.compile(config.title_regex, re.IGNORECASE)
        self._log = logging.getLogger("catalog_nav_bot")
        self.explain = ExplainableLog(
            self._log,
            trace_enabled=config.explain_trace,
            log_skip_traffic=config.log_skip_traffic,
        )
        self._catalog_handoff_locks: Dict[int, asyncio.Lock] = {}

    def _catalog_handoff_lock(self, channel_id: int) -> asyncio.Lock:
        """One mutex per channel so two catalog posts in the same channel cannot interleave dedupe/upsert/main-catalog."""
        lk = self._catalog_handoff_locks.get(channel_id)
        if lk is None:
            lk = asyncio.Lock()
            self._catalog_handoff_locks[channel_id] = lk
        return lk

    async def _throttle_after_navigation_edit(self) -> None:
        delay = self.config_data.navigation_edit_min_interval_seconds
        if delay > 0:
            await asyncio.sleep(delay)

    async def _delete_navigation_reply_message(self, channel_id: int, message_id: int) -> None:
        """Best-effort delete of a nav reply we no longer track (e.g. same catalog reposted)."""
        channel = await self._resolve_text_channel(channel_id)
        if channel is None:
            self._log.warning("dedupe: could not resolve channel_id=%s for delete", channel_id)
            return
        try:
            msg = await channel.fetch_message(message_id)
            if self.user and msg.author.id == self.user.id:
                await msg.delete()
        except discord.NotFound:
            pass
        except discord.Forbidden:
            self._log.warning("dedupe: forbidden deleting nav reply message_id=%s", message_id)
        except discord.HTTPException:
            self._log.exception("dedupe: failed deleting nav reply message_id=%s", message_id)

    async def _delete_superseded_source_catalog_message(
        self,
        *,
        channel_id: int,
        superseded_source_message_id: int,
        new_source_message_id: int,
    ) -> bool:
        """
        Remove the previous source-bot catalog post when the same store+category is posted again.
        Only deletes if the message author matches source_bot_id (never deletes arbitrary users).
        """
        if superseded_source_message_id == new_source_message_id:
            return False
        channel = self.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            channel = await self._resolve_text_channel(channel_id)
        if channel is None:
            self._log.warning(
                "source catalog delete: channel_id=%s not resolvable",
                channel_id,
            )
            return False
        try:
            msg = await channel.fetch_message(superseded_source_message_id)
        except discord.NotFound:
            return False
        except discord.Forbidden:
            self._log.warning(
                "source catalog delete: forbidden fetching message_id=%s channel_id=%s",
                superseded_source_message_id,
                channel_id,
            )
            return False
        except discord.HTTPException:
            self._log.exception(
                "source catalog delete: fetch failed message_id=%s channel_id=%s",
                superseded_source_message_id,
                channel_id,
            )
            return False
        if msg.author.id != self.config_data.source_bot_id:
            self._log.warning(
                "source catalog delete: skipped wrong author_id=%s (expected source_bot_id=%s) message_id=%s",
                msg.author.id,
                self.config_data.source_bot_id,
                superseded_source_message_id,
            )
            return False
        try:
            await msg.delete()
            return True
        except discord.NotFound:
            return False
        except discord.Forbidden:
            self._log.warning(
                "source catalog delete: forbidden deleting message_id=%s channel_id=%s",
                superseded_source_message_id,
                channel_id,
            )
            return False
        except discord.HTTPException:
            self._log.exception(
                "source catalog delete: delete failed message_id=%s channel_id=%s",
                superseded_source_message_id,
                channel_id,
            )
            return False

    async def on_ready(self) -> None:
        self.explain.section("READY")
        self.explain.eli5(
            "Bot is online and will react to catalog titles from the configured source bot.",
            [
                f"logged in as {self.user} ({getattr(self.user, 'id', 'unknown')})",
                f"source_bot_id={self.config_data.source_bot_id}",
                f"allowed_guild_ids={self.config_data.allowed_guild_ids}",
                f"allowed_channel_ids={self.config_data.allowed_channel_ids}",
                f"explain_trace={self.config_data.explain_trace}",
                f"log_skip_traffic={self.config_data.log_skip_traffic}",
                f"navigation_edit_min_interval_seconds={self.config_data.navigation_edit_min_interval_seconds}",
            ],
        )
        self.explain.trace(
            {
                "event": "ready",
                "title_regex": self.config_data.title_regex,
                "state_path": str(self.config_data.state_path),
            }
        )
        await self._sync_navigation_with_state_on_startup()

    async def _resolve_text_channel(self, channel_id: int) -> Optional[discord.TextChannel]:
        ch = self.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            return ch
        if ch is not None:
            return None
        try:
            fetched = await self.fetch_channel(channel_id)
        except discord.HTTPException:
            return None
        return fetched if isinstance(fetched, discord.TextChannel) else None

    async def _sync_navigation_with_state_on_startup(self) -> None:
        """Re-edit existing nav replies so Main Catalog matches state (fixes stale links after restart or code fixes)."""
        ids = self.config_data.allowed_channel_ids
        if not ids:
            self.explain.trace({"event": "startup_nav_sync_skip", "reason": "allowed_channel_ids_empty"})
            return

        self.explain.section("STARTUP / NAV SYNC")
        synced = 0
        for cid in ids:
            channel = await self._resolve_text_channel(cid)
            if channel is None:
                self._log.warning("startup nav sync: channel_id=%s not found or not text", cid)
                continue
            await self._repair_missing_main_catalog_message(channel)
            await self._refresh_all_category_navigation_messages(cid)
            synced += 1
        self.explain.eli5(
            "Startup navigation sync finished: refreshed nav replies from JSON state (no new main catalog post).",
            [
                f"channels_touched={synced}/{len(ids)}",
                f"main_catalog ids from state: {dict(self.state.data.get('main_catalog_by_channel', {}))}",
            ],
        )
        self.explain.trace({"event": "startup_nav_sync_done", "channels_synced": synced})

    async def on_message(self, message: discord.Message) -> None:
        if message.author.id != self.config_data.source_bot_id:
            if self.config_data.ignore_bots_except_source and message.author.bot:
                self.explain.debug_skip(
                    "ignored_non_source_bot",
                    author_id=message.author.id,
                    channel_id=getattr(message.channel, "id", None),
                )
                return
            self.explain.debug_skip(
                "ignored_non_source_author",
                author_id=message.author.id,
                channel_id=getattr(message.channel, "id", None),
            )
            return

        if self.config_data.allowed_guild_ids and (not message.guild or message.guild.id not in self.config_data.allowed_guild_ids):
            self.explain.debug_skip(
                "ignored_guild_filter",
                guild_id=getattr(message.guild, "id", None),
                allowed_guild_ids=self.config_data.allowed_guild_ids,
            )
            return

        if self.config_data.allowed_channel_ids and message.channel.id not in self.config_data.allowed_channel_ids:
            self.explain.debug_skip(
                "ignored_channel_filter",
                channel_id=message.channel.id,
                allowed_channel_ids=self.config_data.allowed_channel_ids,
            )
            return

        parsed = self._parse_message(message)
        if not parsed:
            candidates = self._title_candidates(message)
            self.explain.section("SOURCE MESSAGE / NO REGEX MATCH")
            self.explain.eli5(
                "This message was from the source bot in an allowed channel, but nothing matched title_regex.",
                [
                    f"source_message_id={message.id}",
                    f"title_candidates={candidates!r}",
                ],
            )
            self.explain.failure(
                [
                    "Check embed title, first line of embed description, embed field name/value, or first line of message content against title_regex.",
                    "Ensure named groups (?P<store>...) and (?P<category>...) are present.",
                ]
            )
            self.explain.trace(
                {
                    "event": "parse_miss",
                    "message_id": message.id,
                    "channel_id": message.channel.id,
                    "candidates": candidates,
                    "pattern": self.config_data.title_regex,
                    "message_shape": _message_shape_for_trace(message),
                }
            )
            return

        store_label, category_label = parsed
        store_slug = slugify(store_label)
        category_slug = slugify(category_label)

        async with self._catalog_handoff_lock(message.channel.id):
            self.explain.section("SOURCE MESSAGE / CATALOG HANDOFF")
            self.explain.trace(
                {
                    "event": "catalog_start",
                    "message_id": message.id,
                    "channel_id": message.channel.id,
                    "store_label": store_label,
                    "category_label": category_label,
                    "store_slug": store_slug,
                    "category_slug": category_slug,
                }
            )

            superseded_source_message_id: Optional[int] = None
            prior_category = await self.state.get_category(category_slug)
            if prior_category:
                store_row = prior_category.get("stores", {}).get(store_slug)
                if store_row and int(store_row.get("source_channel_id", 0)) == message.channel.id:
                    try:
                        sid = int(store_row["source_message_id"])
                        if sid != message.id:
                            superseded_source_message_id = sid
                    except (TypeError, ValueError, KeyError):
                        superseded_source_message_id = None

            # One main-catalog post per event: if we posted it twice, the first id would be deleted and
            # every "Main Catalog" link button would still point at the deleted message.
            superseded = await self.state.remove_reply_targets_for_store(
                category_slug=category_slug,
                store_slug=store_slug,
                channel_id=message.channel.id,
            )
            for rch_id, rmsg_id in superseded:
                await self._delete_navigation_reply_message(rch_id, rmsg_id)
            if superseded:
                self.explain.trace(
                    {
                        "event": "catalog_dedupe_store",
                        "category_slug": category_slug,
                        "store_slug": store_slug,
                        "removed_reply_ids": [p[1] for p in superseded],
                    }
                )

            source_catalog_deleted = False
            if (
                self.config_data.delete_superseded_source_catalog_messages
                and superseded_source_message_id is not None
            ):
                source_catalog_deleted = await self._delete_superseded_source_catalog_message(
                    channel_id=message.channel.id,
                    superseded_source_message_id=superseded_source_message_id,
                    new_source_message_id=message.id,
                )
                self.explain.trace(
                    {
                        "event": "source_catalog_superseded_delete",
                        "category_slug": category_slug,
                        "store_slug": store_slug,
                        "superseded_source_message_id": superseded_source_message_id,
                        "deleted": source_catalog_deleted,
                    }
                )

            main_catalog_url = await self._get_main_catalog_url(message.channel.id)
            reply_message = await self._create_placeholder_reply(message, category_label, main_catalog_url)
            await self.state.upsert_entry(
                category_slug=category_slug,
                category_label=category_label,
                store_slug=store_slug,
                store_label=store_label,
                source_channel_id=message.channel.id,
                source_message_id=message.id,
                source_jump_url=message.jump_url,
                reply_channel_id=reply_message.channel.id,
                reply_message_id=reply_message.id,
            )
            await self._refresh_main_catalog_message(message.channel)
            await self._refresh_all_category_navigation_messages(message.channel.id)
            main_catalog_url = await self._get_main_catalog_url(message.channel.id)

            self.explain.eli5(
                "Catalog navigation updated: reply posted, single main catalog at bottom, every category's nav replies retargeted.",
                [
                    f"reply_message_id={reply_message.id}",
                    f"main_catalog_button_url={main_catalog_url}",
                    f"category_slug={category_slug}",
                ],
            )
            self.explain.human(
                "Rules that fired: source author, guild/channel allowlist, title_regex match, state upsert, main catalog refresh (once), refresh ALL category nav replies in channel (retarget Main Catalog).",
                yes=[
                    f"Parsed store={store_label!r} category={category_label!r}",
                    "Posted or refreshed navigation reply under source message",
                    "Posted exactly one new main catalog message and removed the previous one",
                    "Edited navigation replies for every category in this channel so Main Catalog points at the new bottom message",
                ],
                notes=[f"State file: {self.config_data.state_path}"],
            )
            self.explain.trace(
                {
                    "event": "catalog_done",
                    "source_message_id": message.id,
                    "reply_message_id": reply_message.id,
                    "category_slug": category_slug,
                    "superseded_source_deleted": source_catalog_deleted,
                }
            )

    def _title_candidates(self, message: discord.Message) -> List[str]:
        """Strings compared to title_regex (order: embed title/desc/fields/author, then message content)."""
        out: List[str] = []
        seen: set[str] = set()

        def add(text: Optional[str]) -> None:
            if not text:
                return
            s = text.strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)

        for embed in message.embeds:
            add(embed.title)
            for line in (embed.description or "").splitlines():
                add(line)
            if embed.author and getattr(embed.author, "name", None):
                add(str(embed.author.name))
            for field in embed.fields:
                add(field.name)
                for line in (field.value or "").splitlines():
                    add(line)
        for line in (message.content or "").splitlines():
            add(line)
        return out

    def _parse_message(self, message: discord.Message) -> Optional[Tuple[str, str]]:
        for title in self._title_candidates(message):
            match = self.title_pattern.match(title)
            if match:
                store_label = normalize_label(match.group("store"))
                category_label = normalize_label(match.group("category"))
                if store_label and category_label:
                    return store_label, category_label
        return None

    async def _get_main_catalog_url(self, channel_id: int) -> str:
        message_id = await self.state.get_main_catalog_message_id(channel_id)
        if not message_id:
            return self._build_message_url(
                self.config_data.menu_guild_id,
                self.config_data.menu_channel_id,
                self.config_data.menu_message_id,
            )
        return self._build_message_url(self.config_data.menu_guild_id, channel_id, message_id)

    async def _create_placeholder_reply(self, message: discord.Message, category_label: str, main_catalog_url: str) -> discord.Message:
        embed = discord.Embed(
            description=self._render_navigation_text(category_label, []),
            color=self.config_data.embed_color,
        )
        view = LinkButtonView(label=self.config_data.navigation_button_label, url=main_catalog_url)
        return await message.reply(embed=embed, view=view, mention_author=False)

    async def _refresh_all_category_navigation_messages(self, channel_id: int) -> None:
        """After main catalog moves, every category's nav replies must get the new Main Catalog URL (not only the triggering category)."""
        categories = await self.state.get_categories_for_channel(channel_id)
        for slug in sorted(categories.keys()):
            await self._refresh_category_messages(slug, channel_id, log_route=False)
        self.explain.route(
            "ALL CATEGORY NAV REPLIES",
            destination=f"channel_id={channel_id}",
            detail=f"categories_refreshed={len(categories)}",
        )
        self.explain.trace(
            {
                "event": "all_categories_refresh",
                "channel_id": channel_id,
                "category_slugs": sorted(categories.keys()),
            }
        )

    async def _refresh_category_messages(self, category_slug: str, channel_id: int, *, log_route: bool = True) -> None:
        category = await self.state.get_category(category_slug)
        if not category:
            self.explain.trace({"event": "category_refresh_skip", "reason": "no_category", "category_slug": category_slug})
            return

        stores = sorted(
            [s for s in category.get("stores", {}).values() if int(s.get("source_channel_id", 0)) == channel_id],
            key=lambda item: item["label"].lower(),
        )
        if not stores:
            self.explain.trace({"event": "category_refresh_skip", "reason": "no_stores_in_channel", "category_slug": category_slug, "channel_id": channel_id})
            return

        main_catalog_url = await self._get_main_catalog_url(channel_id)
        content = self._render_navigation_text(category.get("label", category_slug), stores)
        embed = discord.Embed(description=content, color=self.config_data.embed_color)
        view = LinkButtonView(label=self.config_data.navigation_button_label, url=main_catalog_url)

        stale_reply_ids: List[str] = []
        edited = 0
        skipped_unchanged = 0
        btn_label = self.config_data.navigation_button_label
        for reply_id, target in category.get("reply_targets", {}).items():
            if int(target.get("reply_channel_id", 0)) != channel_id:
                continue
            channel = self.get_channel(int(target["reply_channel_id"]))
            if channel is None:
                stale_reply_ids.append(reply_id)
                continue
            try:
                reply_message = await channel.fetch_message(int(target["reply_message_id"]))
                if _navigation_reply_matches_target(
                    reply_message,
                    embed,
                    button_label=btn_label,
                    button_url=main_catalog_url,
                ):
                    skipped_unchanged += 1
                    continue
                await reply_message.edit(embed=embed, view=view)
                edited += 1
                await self._throttle_after_navigation_edit()
            except discord.NotFound:
                stale_reply_ids.append(reply_id)
            except discord.Forbidden:
                self._log.warning("forbidden editing reply_message_id=%s channel_id=%s", reply_id, target.get("reply_channel_id"))
            except discord.HTTPException:
                self._log.exception("http error editing reply_message_id=%s", reply_id)

        await self.state.cleanup_missing_reply_targets(category_slug, stale_reply_ids)
        if log_route:
            self.explain.route(
                "CATEGORY NAV REPLIES",
                destination=f"channel_id={channel_id}",
                detail=(
                    f"edited={edited} skipped_unchanged={skipped_unchanged} "
                    f"stale_removed={len(stale_reply_ids)} main_button_url_set"
                ),
            )
        self.explain.trace(
            {
                "event": "category_refresh",
                "category_slug": category_slug,
                "channel_id": channel_id,
                "edited": edited,
                "skipped_unchanged": skipped_unchanged,
                "stale_removed": len(stale_reply_ids),
            }
        )

    async def _repair_missing_main_catalog_message(self, channel: discord.TextChannel) -> None:
        """
        If navigation_state still references a main catalog message that was deleted (manual delete,
        mod cleanup, etc.), repost the main catalog and update state so nav buttons can be fixed
        on the same startup pass.
        """
        old_id = await self.state.get_main_catalog_message_id(channel.id)
        if not old_id:
            return
        try:
            await channel.fetch_message(old_id)
            return
        except discord.NotFound:
            self._log.info(
                "main catalog message_id=%s missing in channel_id=%s; reposting from state",
                old_id,
                channel.id,
            )
            self.explain.trace(
                {
                    "event": "main_catalog_missing_repair",
                    "channel_id": channel.id,
                    "stale_message_id": old_id,
                }
            )
        except discord.Forbidden:
            self._log.warning(
                "main catalog repair: forbidden fetching message_id=%s channel_id=%s",
                old_id,
                channel.id,
            )
            return
        except discord.HTTPException:
            self._log.exception(
                "main catalog repair: fetch failed message_id=%s channel_id=%s",
                old_id,
                channel.id,
            )
            return
        await self._refresh_main_catalog_message(channel)

    async def _refresh_main_catalog_message(self, channel: discord.abc.Messageable) -> None:
        if not isinstance(channel, discord.TextChannel):
            self.explain.trace({"event": "main_catalog_skip", "reason": "not_text_channel", "channel": type(channel).__name__})
            return

        categories = await self.state.get_categories_for_channel(channel.id)
        text = self._render_main_catalog_text(categories)
        if len(text) > 2000:
            self._log.warning("main catalog text truncated from %s to 2000 chars", len(text))
            text = text[:1997] + "…"

        buttons = []
        for category_slug, category in sorted(categories.items(), key=lambda item: item[1].get("label", item[0]).lower()):
            first_store = sorted(category.get("stores", {}).values(), key=lambda s: s.get("label", "").lower())[0]
            buttons.append((category.get("label", category_slug), first_store.get("jump_url", "")))
        view = MultiLinkButtonView(buttons) if buttons else None

        banner_file: Optional[discord.File] = None
        if self.config_data.main_catalog_banner_url:
            data = await fetch_url_bytes(self.config_data.main_catalog_banner_url, self._log)
            if data:
                ext = _guess_image_extension(self.config_data.main_catalog_banner_url, data)
                banner_file = discord.File(io.BytesIO(data), filename=f"catalog-banner{ext}")

        old_message_id = await self.state.get_main_catalog_message_id(channel.id)
        old_message: Optional[discord.Message] = None
        if old_message_id:
            try:
                old_message = await channel.fetch_message(old_message_id)
            except discord.NotFound:
                old_message = None
            except discord.HTTPException:
                self._log.exception("failed loading previous main catalog message_id=%s", old_message_id)

        send_kwargs: dict = {"content": text, "view": view}
        if banner_file:
            send_kwargs["file"] = banner_file
        new_message = await channel.send(**send_kwargs)
        await self.state.set_main_catalog_message(channel.id, new_message.id)

        deleted_old = False
        if old_message and old_message.id != new_message.id:
            try:
                await old_message.delete()
                deleted_old = True
            except discord.NotFound:
                pass
            except discord.Forbidden:
                self._log.warning("forbidden deleting old main catalog message_id=%s", old_message.id)
            except discord.HTTPException:
                self._log.exception("failed deleting old main catalog message_id=%s", old_message.id)

        self.explain.route(
            "MAIN CATALOG",
            destination=f"channel_id={channel.id}",
            detail=f"new_message_id={new_message.id} plain_text=1 banner_attached={bool(banner_file)} category_buttons={len(buttons)} deleted_previous={deleted_old}",
        )
        self.explain.trace(
            {
                "event": "main_catalog_refresh",
                "channel_id": channel.id,
                "new_message_id": new_message.id,
                "old_message_id": old_message_id,
                "category_count": len(categories),
                "button_count": len(buttons),
                "deleted_previous": deleted_old,
                "plain_message": True,
                "banner_attached": bool(banner_file),
            }
        )

    def _render_navigation_text(self, category_label: str, stores: List[dict]) -> str:
        lines = [f"{self.config_data.nav_emoji} **{category_label} Pokemon**"]
        if stores:
            for store in stores:
                lines.append(store_bullet_line(store, guild_id=self.config_data.menu_guild_id))
        else:
            lines.append(self.config_data.placeholder_text)
        if self.config_data.separator:
            lines.append(self.config_data.separator)
        return "\n".join(lines)

    def _render_main_catalog_text(self, categories: Dict[str, dict]) -> str:
        """Optional title/intro; category navigation is via link buttons. Omit label+intro keys (or use \"\") for banner+buttons-only."""
        lines: List[str] = []
        label = (self.config_data.main_catalog_label or "").strip()
        intro = (self.config_data.main_catalog_intro or "").strip()
        emoji = (self.config_data.nav_emoji or "").strip()
        if label:
            lines.append(f"{emoji} **{label}**" if emoji else f"**{label}**")
        if intro:
            lines.append(intro)
        if not categories:
            lines.append(self.config_data.placeholder_text)
        return "\n".join(lines)

    def _build_message_url(self, guild_id: int, channel_id: int, message_id: int) -> str:
        return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def store_bullet_line(store: dict, *, guild_id: Optional[int] = None) -> str:
    """Link each store to its catalog *message* (jump URL), not only the channel — avoids every line opening the same #channel."""
    label = store.get("label", "Store")
    url = str(store.get("jump_url") or "").strip()
    if not url and guild_id:
        cid = int(store["source_channel_id"])
        mid = int(store["source_message_id"])
        url = f"https://discord.com/channels/{guild_id}/{cid}/{mid}"
    if url:
        return f"• **{label}** → {url}"
    cid = int(store["source_channel_id"])
    return f"• **{label}** → <#{cid}>"


async def fetch_url_bytes(url: str, log: logging.Logger) -> Optional[bytes]:
    timeout = aiohttp.ClientTimeout(total=25)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                url,
                headers={"User-Agent": "DiscordBot (https://github.com/Rapptz/discord.py)"},
            ) as resp:
                if resp.status != 200:
                    log.warning("banner fetch HTTP %s for %s", resp.status, url)
                    return None
                return await resp.read()
    except Exception:
        log.exception("banner fetch failed for %s", url)
        return None


def _guess_image_extension(url: str, data: bytes) -> str:
    path = urlparse(url).path.lower()
    for ext in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
        if path.endswith(ext):
            return ext if ext != ".jpeg" else ".jpg"
    if len(data) >= 3 and data[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if len(data) >= 8 and data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if len(data) >= 6 and (data[:6] in (b"GIF87a", b"GIF89a")):
        return ".gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ".png"


def _first_nonempty_line(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    for line in str(text).splitlines():
        s = line.strip()
        if s:
            return s
    return None


def _message_shape_for_trace(message: discord.Message) -> dict:
    embeds = message.embeds or []
    return {
        "embed_count": len(embeds),
        "content_len": len(message.content or ""),
        "embed_titles": [e.title for e in embeds if e.title],
        "embed_desc_lines0": [_first_nonempty_line(e.description) for e in embeds],
        "field_count": sum(len(e.fields) for e in embeds),
    }


def normalize_label(value: str) -> str:
    value = value.replace("_", " ").replace("-", " ")
    value = WHITESPACE_RE.sub(" ", value).strip()
    return value.title()


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def load_config(config_path: Path) -> Config:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    return Config.from_dict(data, config_path.parent)


def configure_logging(level: str, *, explain_trace: bool) -> None:
    root_level = logging.DEBUG if explain_trace else getattr(logging, level, logging.INFO)
    logging.basicConfig(
        level=root_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    if explain_trace:
        logging.getLogger("discord").setLevel(logging.INFO)
        logging.getLogger("discord.http").setLevel(logging.WARNING)


def main() -> None:
    config_path = Path(os.environ.get("CATALOG_NAV_CONFIG", "config.json")).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    config = load_config(config_path)
    configure_logging(config.log_level, explain_trace=config.explain_trace)
    bot = CatalogNavigationBot(config)
    bot.run(config.token)


if __name__ == "__main__":
    main()
