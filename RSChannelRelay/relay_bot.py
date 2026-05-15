"""
Standalone relay: mirror messages from configured source channels into destination
channels (same or other guild). Preserves message content, embeds, and attachments;
redacts external URLs in text (and embed title links). Discord message links and
embed images/thumbnails are kept so the post still looks like the original.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import discord
from discord.errors import PrivilegedIntentsRequired
from discord.ext import commands

_BASE = Path(__file__).resolve().parent

_DISCORD_MSG_LINK_RE = re.compile(
    r"^https?://(?:(?:ptb|canary)\.)?discord(?:app)?\.com/channels/\d+/\d+/\d+",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://[^\s<>`]+", re.IGNORECASE)
_EBAY_URL_START_RE = re.compile(
    r"^https?://(?:[\w-]+\.)*ebay\.(?:com|co\.uk|com\.au|de|fr|it|es|ca|at|ch|ie|nl|be|pl|ph|us)(?:/|$|\?|:|\#)",
    re.IGNORECASE,
)
_DISCORD_CDN_URL_RE = re.compile(
    r"^https?://(?:cdn\.discordapp\.com|media\.discordapp\.net|images-ext-\d+\.discordapp\.net)/",
    re.IGNORECASE,
)
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", re.IGNORECASE)
_WHERE_LINE_RE = re.compile(
    r"^(?P<indent>\s*)"
    r"(?P<prefix>\*{0,2}\s*where\s*\*{0,2}\s*:\s*\*{0,2}\s*)"
    r"(?P<val>.*?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _snipe_where_lines(text: str, emoji: str) -> str:
    if not (emoji or "").strip():
        return text

    def repl(m: re.Match[str]) -> str:
        indent = m.group("indent")
        prefix = m.group("prefix")
        em = emoji.strip()
        if "**" in prefix or prefix.count("*") >= 2:
            return f"{indent}**Where:** {em}"
        return f"{indent}Where: {em}"

    return _WHERE_LINE_RE.sub(repl, text)


def _load_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    return data if isinstance(data, dict) else {}


def _resolve_token() -> str:
    t = (os.environ.get("RS_CHANNEL_RELAY_TOKEN") or "").strip()
    if t:
        return t
    sec = _BASE / "config.secrets.json"
    if sec.is_file():
        d = _load_json(sec)
        t = str(d.get("discord_bot_token") or "").strip()
        if t:
            return t
    print("Missing token: set RS_CHANNEL_RELAY_TOKEN or config.secrets.json -> discord_bot_token", file=sys.stderr)
    return ""


def _mirror_dest_channel_name(display: str, fallback: str) -> str:
    """Match source channel naming for mirror destinations (emojis, casing); trim length + control chars only."""
    raw = (display or "").strip() or fallback
    out = "".join(ch for ch in raw if ord(ch) >= 32)
    out = out.replace("\t", " ").strip()
    if not out:
        out = fallback
    return out[:100]


def _url_preserved(u: str, *, preserve_ebay: bool) -> bool:
    if _DISCORD_MSG_LINK_RE.match(u):
        return True
    if _DISCORD_CDN_URL_RE.match(u):
        return True
    if preserve_ebay and _EBAY_URL_START_RE.match(u):
        return True
    return False


def _redact_urls(text: str, placeholder: str, *, preserve_ebay: bool) -> tuple[str, int]:
    redactions = 0

    def repl(m: re.Match[str]) -> str:
        nonlocal redactions
        raw = m.group(0)
        tail = ""
        u = raw
        while u and u[-1] in ".,);]`>":
            tail = u[-1] + tail
            u = u[:-1]
        if _url_preserved(u, preserve_ebay=preserve_ebay):
            return raw
        redactions += 1
        return placeholder + tail

    return _URL_RE.sub(repl, text), redactions


def _redact_markdown_links(text: str, placeholder: str, *, preserve_ebay: bool) -> tuple[str, int]:
    """[label](url) → label only when url is redacted (avoids broken markdown)."""
    redactions = 0

    def repl(m: re.Match[str]) -> str:
        nonlocal redactions
        label = m.group(1)
        u = m.group(2).rstrip(".,);]`>")
        if _url_preserved(u, preserve_ebay=preserve_ebay):
            return m.group(0)
        redactions += 1
        return label

    return _MD_LINK_RE.sub(repl, text), redactions


def _sanitize_text_block(
    text: str,
    placeholder: str,
    *,
    preserve_ebay: bool,
    where_snipe_emoji: str = "",
) -> tuple[str, int]:
    raw = (text or "").strip()
    if not raw:
        return "", 0
    if where_snipe_emoji:
        raw = _snipe_where_lines(raw, where_snipe_emoji)
    raw, n1 = _redact_markdown_links(raw, placeholder, preserve_ebay=preserve_ebay)
    raw, n2 = _redact_urls(raw, placeholder, preserve_ebay=preserve_ebay)
    return raw, n1 + n2


def _truncate(s: str, limit: int) -> str:
    s = (s or "").strip()
    if limit <= 0 or len(s) <= limit:
        return s
    if limit == 1:
        return "…"
    return s[: limit - 1] + "…"


def _clone_embed_redacted(
    emb: discord.Embed,
    *,
    placeholder: str,
    preserve_ebay: bool,
    where_snipe_emoji: str,
) -> tuple[discord.Embed, int]:
    total_redactions = 0
    title = None
    if emb.title:
        title, n = _sanitize_text_block(
            str(emb.title), placeholder, preserve_ebay=preserve_ebay, where_snipe_emoji=where_snipe_emoji
        )
        total_redactions += n
        title = _truncate(title, 256) or None

    description = None
    if emb.description:
        description, n = _sanitize_text_block(
            str(emb.description), placeholder, preserve_ebay=preserve_ebay, where_snipe_emoji=where_snipe_emoji
        )
        total_redactions += n
        description = _truncate(description, 4096) or None

    out = discord.Embed(title=title, description=description, colour=emb.colour)

    embed_url = str(getattr(emb, "url", None) or "").strip()
    if embed_url:
        if _url_preserved(embed_url, preserve_ebay=preserve_ebay):
            out.url = embed_url
        else:
            total_redactions += 1

    if emb.timestamp:
        out.timestamp = emb.timestamp

    if emb.image and emb.image.url:
        out.set_image(url=emb.image.url)
    if emb.thumbnail and emb.thumbnail.url:
        out.set_thumbnail(url=emb.thumbnail.url)

    if emb.author and (emb.author.name or emb.author.icon_url):
        author_name = ""
        if emb.author.name:
            author_name, n = _sanitize_text_block(
                str(emb.author.name), placeholder, preserve_ebay=preserve_ebay
            )
            total_redactions += n
            author_name = _truncate(author_name, 256)
        icon = str(emb.author.icon_url or "").strip() or None
        if author_name or icon:
            out.set_author(name=author_name or "\u200b", icon_url=icon)

    if emb.footer and (emb.footer.text or emb.footer.icon_url):
        footer_text = ""
        if emb.footer.text:
            footer_text, n = _sanitize_text_block(
                str(emb.footer.text), placeholder, preserve_ebay=preserve_ebay
            )
            total_redactions += n
            footer_text = _truncate(footer_text, 2048)
        icon = str(emb.footer.icon_url or "").strip() or None
        if footer_text or icon:
            out.set_footer(text=footer_text or "\u200b", icon_url=icon)

    for field in emb.fields[:25]:
        fname = ""
        fval = ""
        if field.name:
            fname, n = _sanitize_text_block(str(field.name), placeholder, preserve_ebay=preserve_ebay)
            total_redactions += n
            fname = _truncate(fname, 256)
        if field.value:
            fval, n = _sanitize_text_block(
                str(field.value), placeholder, preserve_ebay=preserve_ebay, where_snipe_emoji=where_snipe_emoji
            )
            total_redactions += n
            fval = _truncate(fval, 1024)
        if fname or fval:
            out.add_field(name=fname or "\u200b", value=fval or "\u200b", inline=bool(field.inline))

    return out, total_redactions


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


class RelayBot(commands.Bot):
    def __init__(self, cfg: dict[str, Any], *, channel_map_path: Path) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        super().__init__(command_prefix="!relayunused_", intents=intents, help_command=None)
        self._cfg = cfg
        self._channel_map_path = channel_map_path
        wh_file = str(cfg.get("webhook_urls_filename") or "relay_webhook_urls.json").strip()
        self._webhook_urls_path = _BASE / wh_file
        self._webhook_urls: Dict[int, str] = {}
        self._webhook_obj_cache: Dict[int, discord.Webhook] = {}
        self._webhook_lock = asyncio.Lock()
        self._use_webhook = bool(cfg.get("use_webhook_impersonation", True))
        self._webhook_name = (str(cfg.get("relay_webhook_name") or "RSChannelRelay").strip() or "RSChannelRelay")[:80]
        self._source_ids: Set[int] = set()
        for x in cfg.get("source_channel_ids") or []:
            try:
                self._source_ids.add(int(x))
            except (TypeError, ValueError):
                continue
        self._src_gid = int(cfg.get("source_guild_id") or 0)
        self._dst_gid = int(cfg.get("destination_guild_id") or 0)
        self._cat_id = int(cfg.get("destination_category_id") or 0)
        self._placeholder = str(cfg.get("link_redaction_placeholder") or "[link hidden]")
        self._preserve_ebay = bool(cfg.get("preserve_ebay_urls", True))
        self._where_snipe_emoji = str(cfg.get("where_line_snipe_emoji") or "").strip()
        self._cta = str(cfg.get("cta_footer") or "")
        self._end_footer = str(cfg.get("mirror_end_footer") or "")
        self._ignore_bots = bool(cfg.get("ignore_bot_messages", True))
        self._mirror_attach = bool(cfg.get("mirror_attachments", True))
        self._mirror_embeds = bool(cfg.get("mirror_embeds", True))
        self._jump = bool(cfg.get("include_jump_to_original", True))
        self._max_body = int(cfg.get("max_body_chars") or 1600)
        self._map: dict[str, str] = {}
        self._map_lock = asyncio.Lock()
        self._ensure_lock = asyncio.Lock()
        self._human_log = bool(cfg.get("human_readable_logging", True))
        self._log_bot_skips = bool(cfg.get("log_skip_bot_messages", False))
        self._technical = bool(cfg.get("show_technical_trace", False))

    async def setup_hook(self) -> None:
        if self._channel_map_path.is_file():
            try:
                raw = _load_json(self._channel_map_path)
                for k, v in raw.items():
                    if str(k).isdigit() and str(v).isdigit():
                        self._map[str(k)] = str(v)
                if self._human_log and self._map:
                    print(
                        f"[relay] Loaded {len(self._map)} source→destination mapping(s) from "
                        f"{self._channel_map_path.name}",
                        flush=True,
                    )
            except Exception as e:
                print(
                    f"[relay] WARNING: could not load {self._channel_map_path.name}: {type(e).__name__}: {e}",
                    flush=True,
                )
        if self._webhook_urls_path.is_file():
            try:
                wh_raw = _load_json(self._webhook_urls_path)
                for k, v in wh_raw.items():
                    if str(k).isdigit() and isinstance(v, str) and v.strip().lower().startswith("http"):
                        self._webhook_urls[int(k)] = v.strip()
                if self._human_log and self._webhook_urls:
                    print(
                        f"[relay] Loaded {len(self._webhook_urls)} webhook URL(s) from {self._webhook_urls_path.name}",
                        flush=True,
                    )
            except Exception as e:
                print(
                    f"[relay] WARNING: could not load {self._webhook_urls_path.name}: {type(e).__name__}: {e}",
                    flush=True,
                )

    def _webhook_display_name(self, author: discord.abc.User) -> str:
        n = getattr(author, "display_name", None) or getattr(author, "name", None) or "User"
        n = str(n)
        n = re.sub(r"[\r\n\x00-\x1f\t]", " ", n).strip() or "User"
        return n[:80]

    def _log_banner(self, title: str) -> None:
        if not self._human_log:
            return
        print("==============================================================================", flush=True)
        print(title, flush=True)
        print("==============================================================================", flush=True)

    def _save_map_sync(self) -> None:
        _atomic_write_json(self._channel_map_path, dict(self._map))

    async def _persist_map(self) -> None:
        async with self._map_lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._save_map_sync)

    def _save_webhooks_sync(self) -> None:
        data = {str(k): v for k, v in sorted(self._webhook_urls.items())}
        _atomic_write_json(self._webhook_urls_path, data)

    async def _persist_webhooks(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._save_webhooks_sync)

    async def _ensure_relay_webhook(self, dest: discord.TextChannel) -> Optional[discord.Webhook]:
        async with self._webhook_lock:
            did = dest.id
            if did in self._webhook_obj_cache:
                return self._webhook_obj_cache[did]
            url = self._webhook_urls.get(did)
            if url:
                wh = discord.Webhook.from_url(url, client=self)
                self._webhook_obj_cache[did] = wh
                return wh
            try:
                existing = await dest.webhooks()
            except Exception as e:
                print(f"[relay] ERROR: cannot list webhooks on #{dest.name}: {e}", flush=True)
                return None
            wh = discord.utils.get(existing, name=self._webhook_name)
            if wh is None:
                try:
                    wh = await dest.create_webhook(name=self._webhook_name, reason="RSChannelRelay mirror")
                except Exception as e:
                    print(f"[relay] ERROR: create_webhook failed on #{dest.name}: {e}", flush=True)
                    return None
            wurl = str(getattr(wh, "url", "") or "").strip()
            if not wurl:
                print(
                    "[relay] ERROR: webhook has no URL (cannot execute). Remove stale webhooks or grant Manage Webhooks.",
                    flush=True,
                )
                return None
            self._webhook_urls[did] = wurl
            self._webhook_obj_cache[did] = wh
            await self._persist_webhooks()
            if self._human_log:
                print(f"[relay] SETUP: created webhook {self._webhook_name!r} on #{dest.name}", flush=True)
            return wh

    async def _ensure_destination(self, source_id: int) -> Optional[discord.TextChannel]:
        key = str(source_id)
        async with self._ensure_lock:
            if key in self._map:
                ch = self.get_channel(int(self._map[key]))
                if isinstance(ch, discord.TextChannel):
                    return ch
            dst_g = self.get_guild(self._dst_gid)
            if not dst_g:
                print(f"[relay] destination guild {self._dst_gid} not visible to bot", flush=True)
                return None
            cat = dst_g.get_channel(self._cat_id)
            if not isinstance(cat, discord.CategoryChannel):
                print(f"[relay] category {self._cat_id} not found", flush=True)
                return None
            src = self.get_channel(source_id)
            name_hint = src.name if isinstance(src, discord.TextChannel) else key
            base = _mirror_dest_channel_name(str(name_hint), f"channel-{key}")
            name = base
            n = 2
            existing = {c.name for c in cat.channels if isinstance(c, discord.TextChannel)}
            while name in existing:
                suffix = f"-{n}"
                name = (base[: 100 - len(suffix)] + suffix)[:100]
                n += 1
            try:
                created = await dst_g.create_text_channel(
                    name=name,
                    category=cat,
                    reason="RSChannelRelay mirror destination",
                )
            except discord.Forbidden:
                print("[relay] Missing permission: Manage Channels on destination guild", flush=True)
                return None
            except Exception as e:
                print(f"[relay] create_text_channel failed: {e}", flush=True)
                return None
            self._map[key] = str(created.id)
            await self._persist_map()
            if self._human_log:
                print(
                    f"[relay] SETUP: created mirror destination #{created.name} (id={created.id}) "
                    f"for source channel id={source_id}",
                    flush=True,
                )
            return created

    def _build_mirror_payload(self, message: discord.Message) -> Tuple[Optional[str], List[discord.Embed], int, bool]:
        """Content (no flattened embed text), mirrored embeds, redaction count, content truncated flag."""
        redactions = 0
        content = ""
        if (message.content or "").strip():
            content, n = _sanitize_text_block(
                message.content,
                self._placeholder,
                preserve_ebay=self._preserve_ebay,
                where_snipe_emoji=self._where_snipe_emoji,
            )
            redactions += n

        embeds: List[discord.Embed] = []
        if self._mirror_embeds:
            for emb in (message.embeds or [])[:10]:
                cloned, n = _clone_embed_redacted(
                    emb,
                    placeholder=self._placeholder,
                    preserve_ebay=self._preserve_ebay,
                    where_snipe_emoji=self._where_snipe_emoji,
                )
                redactions += n
                embeds.append(cloned)

        truncated = False
        if content and len(content) > self._max_body:
            content = content[: self._max_body - 1] + "…"
            truncated = True

        return (content or None), embeds, redactions, truncated

    def _finalize_content(
        self,
        content: Optional[str],
        *,
        jump_url: str,
    ) -> Tuple[Optional[str], bool]:
        parts: List[str] = []
        if content and content.strip():
            parts.append(content.strip())
        if self._jump and jump_url:
            parts.append(f"[Open original]({jump_url})")
        if self._cta.strip():
            parts.append(self._cta.strip())
        if not parts:
            return None, False
        pre = "\n\n".join(parts)
        end_f = self._end_footer.strip()
        out_trunc = False
        if end_f:
            suffix_block = "\n\n" + end_f
            max_pre = max(1, 2000 - len(suffix_block))
            if len(pre) > max_pre:
                pre = pre[: max(0, max_pre - 1)] + "…"
                out_trunc = True
            return pre + suffix_block, out_trunc
        if len(pre) > 2000:
            pre = pre[:1997] + "…"
            out_trunc = True
        return pre, out_trunc

    async def _post_mirror(
        self,
        dest: discord.TextChannel,
        message: discord.Message,
        *,
        content: Optional[str],
        embeds: List[discord.Embed],
        files: list[discord.File],
    ) -> tuple[bool, bool, Optional[str]]:
        """Returns (success, used_webhook_impersonation, error_detail)."""
        if not content and not embeds and not files:
            content = "_[no content]_"

        if self._use_webhook:
            try:
                wh = await self._ensure_relay_webhook(dest)
                if wh is not None:
                    kwargs: dict[str, Any] = {
                        "content": content,
                        "username": self._webhook_display_name(message.author),
                        "avatar_url": message.author.display_avatar.url,
                        "allowed_mentions": discord.AllowedMentions.none(),
                    }
                    if embeds:
                        kwargs["embeds"] = embeds
                    if files:
                        kwargs["files"] = files
                    await wh.send(**kwargs)
                    return True, True, None
            except discord.HTTPException as e:
                print(
                    f"[relay] WARNING: webhook execute failed (HTTP {e.status}): "
                    f"{getattr(e, 'text', None) or str(e)!r} — falling back to bot account",
                    flush=True,
                )
            except Exception as e:
                print(f"[relay] WARNING: webhook execute {type(e).__name__}: {e} — falling back to bot", flush=True)
        try:
            kwargs2: dict[str, Any] = {
                "content": content,
                "allowed_mentions": discord.AllowedMentions.none(),
            }
            if embeds:
                kwargs2["embeds"] = embeds
            if files:
                kwargs2["files"] = files
            await dest.send(**kwargs2)
            return True, False, None
        except discord.HTTPException as e:
            return False, False, getattr(e, "text", None) or str(e)
        except Exception as e:
            return False, False, f"{type(e).__name__}: {e}"

    def _log_mirror_blocked(self, message: discord.Message, *, reason: str, hints: List[str]) -> None:
        if not self._human_log:
            return
        self._log_banner("RSChannelRelay / BLOCKED")
        ch = message.channel
        src_label = f"#{ch.name}" if isinstance(ch, discord.TextChannel) else str(ch.id)
        print("", flush=True)
        print("1) WHAT HAPPENED", flush=True)
        print(f"- Discord delivered a message you might expect to mirror, but the relay did not post a copy.", flush=True)
        print(f"- Reason: {reason}", flush=True)
        print("", flush=True)
        print("2) MESSAGE INFO", flush=True)
        print(f"- author: {message.author}", flush=True)
        print(f"- source: {src_label} (channel_id={message.channel.id})", flush=True)
        print(f"- message_id: {message.id}", flush=True)
        print("", flush=True)
        print("3) FAILURE HINTS (fix these, then retry)", flush=True)
        for h in hints:
            print(f"- {h}", flush=True)
        print("", flush=True)

    def _log_mirror_success(
        self,
        message: discord.Message,
        *,
        dest: discord.TextChannel,
        body: str,
        redactions: int,
        truncated: bool,
        out_chars: int,
        out_truncated: bool,
        attachment_lines: List[str],
        mirrored_files: int,
        total_attachments: int,
        mirrored_embeds: int,
        posted_via_webhook: bool,
    ) -> None:
        if not self._human_log:
            return
        ch = message.channel
        src_label = f"#{ch.name}" if isinstance(ch, discord.TextChannel) else str(ch.id)
        author = message.author.display_name if isinstance(message.author, discord.Member) else str(message.author)
        self._log_banner("RSChannelRelay / MIRROR")
        print("", flush=True)
        print("1) MESSAGE INFO", flush=True)
        print(f"- author: {author}", flush=True)
        print(f"- source: {src_label} (channel_id={message.channel.id}, guild_id={message.guild.id})", flush=True)
        print(f"- message_id: {message.id}", flush=True)
        print("", flush=True)
        print("2) ELI5 SUMMARY", flush=True)
        print("Bottom line: A teaser copy was posted to your mirror channel.", flush=True)
        print(f"- External URLs redacted (non-Discord links): {redactions}", flush=True)
        print(f"- Text body truncated to max_body_chars: {'yes' if truncated else 'no'}", flush=True)
        print(f"- Final Discord payload truncated to 2000 chars: {'yes' if out_truncated else 'no'}", flush=True)
        print(f"- Embeds mirrored: {mirrored_embeds}", flush=True)
        print(f"- Attachments on original message: {total_attachments}", flush=True)
        print(f"- Attachments mirrored: {mirrored_files}", flush=True)
        print("", flush=True)
        print("3) DESTINATION DECISION", flush=True)
        print("- Decision tag: RELAY", flush=True)
        print(f"- Destination: #{dest.name} (channel_id={dest.id}, guild_id={dest.guild.id})", flush=True)
        print(f"- Posted as: {'incoming webhook (poster name + avatar)' if posted_via_webhook else 'bot account'}", flush=True)
        print("", flush=True)
        if attachment_lines:
            print("4) ATTACHMENT DETAILS", flush=True)
            for line in attachment_lines:
                print(f"- {line}", flush=True)
            print("", flush=True)
        if self._technical:
            preview = (body or "")[:240].replace("\n", "\\n")
            if len(body or "") > 240:
                preview += "…"
            print("5) TECHNICAL TRACE", flush=True)
            print(f"- redacted_body_preview: {preview!r}", flush=True)
            print(f"- outbound_char_count: {out_chars}", flush=True)
            print("", flush=True)
        print("6) RESULT", flush=True)
        print("- OK: mirror message sent", flush=True)
        print("", flush=True)

    async def on_message(self, message: discord.Message) -> None:
        await self.process_commands(message)
        if message.guild is None:
            return
        if message.guild.id != self._src_gid:
            return
        if message.channel.id not in self._source_ids:
            return
        if self._ignore_bots and message.author.bot:
            if self._human_log and self._log_bot_skips:
                print(
                    f"[relay] SKIP: bot message ignored (message_id={message.id}, "
                    f"channel_id={message.channel.id}) — enable log_skip_bot_messages in config to log these",
                    flush=True,
                )
            return
        dest = await self._ensure_destination(message.channel.id)
        if dest is None:
            if self._human_log:
                self._log_mirror_blocked(
                    message,
                    reason="No destination channel available (see earlier [relay] lines for the root cause).",
                    hints=[
                        "Confirm the bot is in the destination guild and online.",
                        "Confirm destination_category_id is a real category in that guild.",
                        "Grant Manage Channels on the destination guild (needed to auto-create mirror channels).",
                        "Grant Manage Webhooks on mirror destination channels (needed for poster name + avatar).",
                        "If map file points at deleted channels, delete relay_channel_map.json and restart.",
                    ],
                )
            else:
                print(
                    "[relay] BLOCKED: could not resolve mirror destination channel "
                    "(turn on human_readable_logging in config for a full checklist)",
                    flush=True,
                )
            return
        content, embeds, n_red, body_trunc = self._build_mirror_payload(message)
        out, out_trunc = self._finalize_content(content, jump_url=message.jump_url or "")
        files: list[discord.File] = []
        attach_notes: list[str] = []
        atts = list(message.attachments or [])[:8]
        for att in atts:
            if not self._mirror_attach:
                attach_notes.append(f"skipped {att.filename!r}: mirror_attachments is false in config")
                continue
            if att.size > 8 * 1024 * 1024:
                attach_notes.append(f"skipped {att.filename!r}: size {att.size} bytes exceeds 8 MiB cap")
                continue
            try:
                data = await att.read()
                files.append(discord.File(io.BytesIO(data), filename=att.filename or "file.bin"))
                attach_notes.append(f"ok {att.filename!r} ({len(data)} bytes)")
            except Exception as e:
                attach_notes.append(f"FAILED {att.filename!r}: {type(e).__name__}: {e}")
        ok, used_wh, err = await self._post_mirror(
            dest, message, content=out, embeds=embeds, files=files
        )
        if ok:
            self._log_mirror_success(
                message,
                dest=dest,
                body=content or "",
                redactions=n_red,
                truncated=body_trunc,
                out_chars=len(out or ""),
                out_truncated=out_trunc,
                attachment_lines=attach_notes,
                mirrored_files=len(files),
                total_attachments=len(atts),
                mirrored_embeds=len(embeds),
                posted_via_webhook=used_wh,
            )
            return
        print(f"[relay] ERROR: mirror post failed ({err!r}) — retrying text-only as bot", flush=True)
        try:
            await dest.send(
                content=(out or "")[: min(len(out or ""), 2000)] or None,
                embeds=embeds or None,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            print("[relay] PARTIAL: retry as bot (attachments may be dropped)", flush=True)
            self._log_mirror_success(
                message,
                dest=dest,
                body=content or "",
                redactions=n_red,
                truncated=body_trunc,
                out_chars=min(len(out or ""), 2000),
                out_truncated=out_trunc,
                attachment_lines=attach_notes + ["note: primary post failed; attachments may be missing"],
                mirrored_files=0,
                total_attachments=len(atts),
                mirrored_embeds=len(embeds),
                posted_via_webhook=False,
            )
        except Exception as e2:
            print(f"[relay] ERROR: fallback send also failed: {type(e2).__name__}: {e2}", flush=True)
            self._log_mirror_blocked(
                message,
                reason="Discord rejected the mirror payload even after a text-only retry.",
                hints=[
                    "Check bot role has Send Messages / Attach Files / Embed Links in the destination channel.",
                    "Grant Manage Webhooks if you use poster impersonation (webhook execute).",
                    "If the CTA contains a channel mention (<#id>), the id must exist in the destination guild.",
                    f"Primary failure detail: {err!s}",
                    f"Fallback error was: {e2!s}",
                ],
            )
            if not self._human_log:
                print(
                    "[relay] BLOCKED: send failed twice (turn on human_readable_logging for hints)",
                    flush=True,
                )


def main() -> None:
    cfg_path = _BASE / "config.json"
    if not cfg_path.is_file():
        print(f"Missing {cfg_path}", file=sys.stderr)
        sys.exit(1)
    cfg = _load_json(cfg_path)
    map_name = str(cfg.get("channel_map_filename") or "relay_channel_map.json")
    channel_map_path = _BASE / map_name
    token = _resolve_token()
    if not token:
        sys.exit(1)
    bot = RelayBot(cfg, channel_map_path=channel_map_path)

    @bot.event
    async def on_ready() -> None:
        print(
            f"[relay] logged in as {bot.user} | sources={len(bot._source_ids)} "
            f"src_guild={bot._src_gid} dst_guild={bot._dst_gid}",
            flush=True,
        )

    try:
        bot.run(token)
    except PrivilegedIntentsRequired:
        print(
            "\nRSChannelRelay requires Message Content Intent (privileged).\n"
            "1) https://discord.com/developers/applications/ → your app → Bot\n"
            '2) Under "Privileged Gateway Intents", turn ON "Message Content Intent"\n'
            "3) Save, then run relay_bot.py again.\n",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
