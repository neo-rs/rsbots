from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional, Sequence, Iterable, Tuple

import discord
from discord.ext import commands


@dataclass(frozen=True)
class ReviewRSConfig:
    # Trigger channel (Neo Test Server)
    trigger_channel_id: int = 1496065906923540561
    catalog_trigger_channel_id: int = 1496075487452069920

    # Source guild (Reselling Secrets)
    rs_server_guild_id: int = 876528050081251379

    # Categories (Reselling Secrets)
    category_weekly_guides_upcoming_id: int = 1400619782692409404
    category_daily_schedule_id: int = 1313260017989713981
    category_instore_important_id: int = 1400165387001135134

    # Channels (Reselling Secrets) to show recent message links
    important_channel_ids: Sequence[int] = (
        1255590577144201358,  # online-important
        1400615121415438446,  # deals-important
        1344378714368118794,  # instore-important
        1344779023577776148,  # pokemon-important
        1461434068804829372,  # mtg-important
        878305076073087026,   # sneakers-important
        1400616066060783646,  # brick-links
        1312134455506239508,  # aco-forms
    )

    # Max links to process per trigger message (spam safety)
    catalog_max_links: int = 8


def _chunk_lines(lines: list[str], *, max_chars: int = 1900) -> list[str]:
    chunks: list[str] = []
    cur: list[str] = []
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


_DISCORD_MSG_LINK_RE = re.compile(
    r"https?://(?:(?:ptb|canary)\.)?discord\.com/channels/(?P<guild>\d+)/(?P<channel>\d+)/(?P<message>\d+)",
    re.IGNORECASE,
)


def _extract_discord_message_links(text: str) -> list[Tuple[int, int, int]]:
    out: list[Tuple[int, int, int]] = []
    for m in _DISCORD_MSG_LINK_RE.finditer(str(text or "")):
        try:
            out.append((int(m.group("guild")), int(m.group("channel")), int(m.group("message"))))
        except Exception:
            continue
    # de-dupe in order
    seen = set()
    uniq: list[Tuple[int, int, int]] = []
    for t in out:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)
    return uniq


def _first_nonempty(*vals: Optional[str]) -> str:
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


class ReviewRSServerListener(commands.Cog):
    """
    Neo Test Server: watch a single channel; on "review rs", reply with:
    - RS category channel lists (clickable mentions)
    - Recent message links from RS "important" channels
    """

    def __init__(self, bot: commands.Bot, *, cfg: Optional[ReviewRSConfig] = None):
        self.bot = bot
        self.cfg = cfg or ReviewRSConfig()

    def _is_allowed_author(self, message: discord.Message) -> bool:
        # Reuse RSAdminBot's canonical admin/owner checks when available.
        try:
            rsadmin = getattr(self.bot, "rsadmin_instance", None)
            if rsadmin is None:
                return True  # fail-open if the main instance isn't attached (dev / unit tests)
            if not message.guild or not message.author:
                return False

            try:
                owner_id = int(getattr(message.guild, "owner_id", 0) or 0)
                user_id = int(getattr(message.author, "id", 0) or 0)
                if owner_id and user_id == owner_id:
                    return True
            except Exception:
                pass

            # Allow configured admin user ids regardless of guild/roles (role IDs differ across servers).
            try:
                cfg = getattr(rsadmin, "config", None)
                admin_user_ids = []
                if isinstance(cfg, dict):
                    admin_user_ids = cfg.get("admin_user_ids", []) or []
                if str(getattr(message.author, "id", "")) in [str(uid) for uid in admin_user_ids]:
                    return True
            except Exception:
                pass

            if isinstance(message.author, discord.Member) and rsadmin.is_admin(  # type: ignore[attr-defined]
                message.author,
                allow_administrator_permission=False,
            ):
                return True
            return False
        except Exception:
            return False

    async def _get_rs_guild(self) -> Optional[discord.Guild]:
        gid = int(self.cfg.rs_server_guild_id or 0)
        if not gid:
            return None
        g = self.bot.get_guild(gid)
        if g:
            return g
        try:
            return await self.bot.fetch_guild(gid)
        except Exception:
            return None

    async def _get_text_channel(self, guild: discord.Guild, channel_id: int) -> Optional[discord.TextChannel]:
        ch = guild.get_channel(int(channel_id))
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(int(channel_id))
            if isinstance(fetched, discord.TextChannel):
                return fetched
        except Exception:
            return None
        return None

    async def _format_category_channels(self, guild: discord.Guild, category_id: int, *, title: str) -> list[str]:
        lines: list[str] = [f"**{title}**"]
        cat = guild.get_channel(int(category_id))
        if not isinstance(cat, discord.CategoryChannel):
            try:
                fetched = await self.bot.fetch_channel(int(category_id))
                if isinstance(fetched, discord.CategoryChannel):
                    cat = fetched
            except Exception:
                cat = None

        if not isinstance(cat, discord.CategoryChannel):
            lines.append("- (category not found / not accessible)")
            return lines

        text_channels = [c for c in (cat.channels or []) if isinstance(c, discord.TextChannel)]
        if not text_channels:
            # If the guild isn't chunked/cached, fetched category may not have populated `channels`.
            # Fall back to an explicit channel fetch.
            try:
                all_channels = await guild.fetch_channels()
                text_channels = [
                    c for c in all_channels
                    if isinstance(c, discord.TextChannel) and getattr(c, "category_id", None) == int(cat.id)
                ]
            except Exception:
                text_channels = []
        if not text_channels:
            lines.append("- (no text channels found)")
            return lines

        for i, ch in enumerate(text_channels, start=1):
            lines.append(f"{i}. <#{int(ch.id)}>")
        return lines

    async def _format_recent_links(self, guild: discord.Guild, channel_id: int) -> list[str]:
        ch = await self._get_text_channel(guild, int(channel_id))
        if not ch:
            return [f"**<#{int(channel_id)}>**: (channel not found / not accessible)"]

        lines: list[str] = [f"**<#{int(ch.id)}>**"]
        try:
            msgs = []
            async for m in ch.history(limit=3, oldest_first=False):
                msgs.append(m)
        except Exception:
            lines.append("- (missing Read Message History permission?)")
            return lines

        if not msgs:
            lines.append("- (no messages found)")
            return lines

        for m in msgs:
            link = f"https://discord.com/channels/{int(guild.id)}/{int(ch.id)}/{int(m.id)}"
            lines.append(f"- {link}")
        return lines

    def _extract_catalog_fields(self, msg: discord.Message) -> tuple[str, list[str], str]:
        """
        Returns (product_title, id_lines, image_url)
        - product_title: embed.title preferred
        - id_lines: e.g. ["TCIN: 95082118", "UPC: 012345..."] (only those found)
        - image_url: attachment image url OR embed.image/thumbnail url
        """
        title = ""
        id_lines: list[str] = []
        image_url = ""

        # Attachments first (direct URL)
        try:
            for a in (getattr(msg, "attachments", None) or [])[:5]:
                try:
                    ct = str(getattr(a, "content_type", "") or "")
                    if ct.startswith("image/") and getattr(a, "url", None):
                        image_url = str(a.url)
                        break
                except Exception:
                    continue
        except Exception:
            pass

        embed = None
        try:
            embed = (msg.embeds or [None])[0]
        except Exception:
            embed = None

        if isinstance(embed, discord.Embed):
            try:
                title = _first_nonempty(getattr(embed, "title", None), getattr(embed, "description", None))
            except Exception:
                title = ""

            # IDs from fields
            try:
                for f in (getattr(embed, "fields", None) or []):
                    name = str(getattr(f, "name", "") or "").strip().lower()
                    value = str(getattr(f, "value", "") or "").strip()
                    if not name or not value:
                        continue
                    if name in {"tcin", "sku", "upc"}:
                        dig = _digits_only(value)
                        if dig:
                            id_lines.append(f"{name.upper()}: {dig}")
            except Exception:
                pass

            # Fallback: look for TCIN/SKU/UPC patterns anywhere in embed text
            if not id_lines:
                blob = ""
                try:
                    blob = "\n".join(
                        [
                            str(getattr(embed, "title", "") or ""),
                            str(getattr(embed, "description", "") or ""),
                            "\n".join(
                                f"{getattr(f,'name','')}: {getattr(f,'value','')}"
                                for f in (getattr(embed, "fields", None) or [])
                            ),
                        ]
                    )
                except Exception:
                    blob = ""
                for key in ("tcin", "sku", "upc"):
                    m = re.search(rf"(?i)\b{key}\b[^\d]*(\d{{6,}})", blob)
                    if m:
                        id_lines.append(f"{key.upper()}: {m.group(1)}")

            # Image URL from embed if no attachment url found
            if not image_url:
                try:
                    image_url = _first_nonempty(
                        getattr(getattr(embed, "image", None), "url", None),
                        getattr(getattr(embed, "thumbnail", None), "url", None),
                    )
                except Exception:
                    image_url = ""

        title = (title or "").strip()
        if title and "\n" in title:
            title = title.splitlines()[0].strip()

        # Clean id_lines duplicates while preserving order
        seen = set()
        uniq_ids: list[str] = []
        for line in id_lines:
            if line in seen:
                continue
            seen.add(line)
            uniq_ids.append(line)

        return title, uniq_ids, image_url

    def _format_catalog_reply(self, *, title: str, ids: Iterable[str], image_url: str) -> list[str]:
        ids_list = [str(x).strip() for x in (ids or []) if str(x).strip()]
        ids_block = "\n".join(ids_list) if ids_list else "(not found)"
        img = image_url.strip() if image_url else "(not found)"
        ttl = title.strip() if title else "(not found)"
        return [
            f"Product Title:```{ttl}```",
            f"SKU/TCIN/UPC```{ids_block}```",
            f"Image URL ```{img}```",
        ]

    async def _handle_catalog_links(self, message: discord.Message) -> None:
        links = _extract_discord_message_links(str(message.content or ""))
        if not links:
            await message.reply("❌ No Discord message links found in your message.", mention_author=False)
            return

        if len(links) > int(self.cfg.catalog_max_links):
            links = links[: int(self.cfg.catalog_max_links)]

        out_lines: list[str] = []
        for (gid, cid, mid) in links:
            try:
                ch = await self.bot.fetch_channel(int(cid))
                if not isinstance(ch, discord.TextChannel):
                    out_lines.append(f"❌ Could not fetch text channel `{cid}`.")
                    out_lines.append("")
                    continue
                src_msg = await ch.fetch_message(int(mid))
                title, ids, image_url = self._extract_catalog_fields(src_msg)
                out_lines.extend(self._format_catalog_reply(title=title, ids=ids, image_url=image_url))
                out_lines.append("")
            except Exception as e:
                out_lines.append(f"❌ Failed to fetch/parse `{gid}/{cid}/{mid}` ({type(e).__name__}).")
                out_lines.append("")

        chunks = _chunk_lines(out_lines, max_chars=1900)
        first = True
        for chunk in chunks:
            if not chunk.strip():
                continue
            if first:
                await message.reply(chunk, mention_author=False)
                first = False
            else:
                await message.channel.send(chunk)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        try:
            if not message:
                return
            if message.author and getattr(message.author, "bot", False):
                return

            ch_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
            if not ch_id:
                return

            # Catalog formatting trigger (Neo Test Server)
            if ch_id == int(self.cfg.catalog_trigger_channel_id):
                if not self._is_allowed_author(message):
                    try:
                        await message.reply("❌ Owner/Admin-only.", mention_author=False)
                    except Exception:
                        pass
                    return
                if not getattr(message, "content", None):
                    return
                await self._handle_catalog_links(message)
                return

            # RS review trigger (Neo Test Server)
            if ch_id != int(self.cfg.trigger_channel_id):
                return

            if not getattr(message, "content", None):
                return
            content = str(message.content or "").strip().lower()
            if content != "review rs":
                return

            if not self._is_allowed_author(message):
                try:
                    await message.reply("❌ Owner/Admin-only.", mention_author=False)
                except Exception:
                    pass
                return

            rs_guild = await self._get_rs_guild()
            if not rs_guild:
                await message.reply("❌ Could not resolve RS server guild in cache.", mention_author=False)
                return

            out_lines: list[str] = []
            out_lines.append("**RS Server Review**")
            out_lines.append(f"- **Guild**: `{rs_guild.name}` (`{rs_guild.id}`)")
            out_lines.append("")
            out_lines.append("**Categories → Channels** (clickable)")
            out_lines.append("")

            out_lines.extend(
                await self._format_category_channels(
                    rs_guild,
                    int(self.cfg.category_weekly_guides_upcoming_id),
                    title="Weekly Guides / Upcoming",
                )
            )
            out_lines.append("")
            out_lines.extend(
                await self._format_category_channels(
                    rs_guild,
                    int(self.cfg.category_daily_schedule_id),
                    title="Daily Schedule",
                )
            )
            out_lines.append("")
            out_lines.extend(
                await self._format_category_channels(
                    rs_guild,
                    int(self.cfg.category_instore_important_id),
                    title="Instore Important",
                )
            )

            out_lines.append("")
            out_lines.append("**Recent messages (last 3) — links**")
            out_lines.append("")

            for cid in self.cfg.important_channel_ids:
                out_lines.extend(await self._format_recent_links(rs_guild, int(cid)))
                out_lines.append("")

            chunks = _chunk_lines(out_lines, max_chars=1900)
            first = True
            for chunk in chunks:
                if first:
                    await message.reply(chunk, mention_author=False)
                    first = False
                else:
                    await message.channel.send(chunk)
        except Exception:
            return


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ReviewRSServerListener(bot))

