from __future__ import annotations

import asyncio
import logging
from typing import Any, TYPE_CHECKING

from app.conversation_render import render_thread_content
from app.conversation_store import ConversationStore, digits_key, thread_key
from app.phone import normalize_e164

if TYPE_CHECKING:
    from app.config import AppConfig
    from app.discord_bot_runner import DiscordBotRunner

log = logging.getLogger("conversations")


class ConversationService:
    def __init__(self, *, config: "AppConfig", store: ConversationStore):
        self.config = config
        self.store = store
        self._bot: DiscordBotRunner | None = None
        self._locks: dict[str, asyncio.Lock] = {}

    def attach_bot(self, bot: "DiscordBotRunner") -> None:
        self._bot = bot

    def conversations_enabled(self) -> bool:
        conv = self.config.settings.get("conversations", {})
        return bool(conv.get("enabled", True)) and bool(self.config.discord_bot_token)

    async def record_inbound(self, *, our_line: str, remote_party: str, text: str) -> None:
        our = normalize_e164(our_line)
        remote = normalize_e164(remote_party)
        if self._is_own_line_echo_inbound(our_line=our, remote_party=remote, text=text):
            log.info(
                "event=inbound_skipped reason=own_line_delivery_echo from=%s to=%s",
                remote,
                our,
            )
            return
        await self._record(our_line=our, remote_party=remote, text=text, direction="in")

    async def record_outbound(self, *, our_line: str, remote_party: str, text: str) -> None:
        await self._record(our_line=our_line, remote_party=remote_party, text=text, direction="out")

    async def send_from_discord(self, *, our_line: str, remote_party: str, text: str) -> None:
        if not self._bot:
            raise RuntimeError("Discord bot is not ready")
        from app.telnyx_client import TelnyxClient

        telnyx = TelnyxClient(self.config)
        await telnyx.send_sms(to_number=remote_party, text=text, from_number=our_line)
        await self.record_outbound(our_line=our_line, remote_party=remote_party, text=text)

    async def rename_contact(self, *, our_line: str, remote_party: str, display_name: str) -> None:
        key = thread_key(our_line=our_line, remote_party=remote_party)
        self.store.upsert_thread(key, {"display_name": display_name.strip()})
        await self._sync_discord_message(our_line=our_line, remote_party=remote_party)

    async def _record(self, *, our_line: str, remote_party: str, text: str, direction: str) -> None:
        if not self.conversations_enabled():
            return
        our = normalize_e164(our_line)
        remote = normalize_e164(remote_party)
        key = thread_key(our_line=our, remote_party=remote)
        conv_cfg = self.config.settings.get("conversations", {})
        max_lines = int(conv_cfg.get("max_lines", 40))

        existing = self.store.get_thread(key)
        if not existing:
            self.store.upsert_thread(
                key,
                {
                    "our_line": our,
                    "remote_party": remote,
                    "display_name": None,
                    "channel_id": self.config.channel_id_for_line(our),
                    "message_id": None,
                    "lines": [],
                },
            )

        self.store.append_line(key, direction=direction, text=text, max_lines=max_lines)
        await self._sync_discord_message(our_line=our, remote_party=remote)

    async def _sync_discord_message(self, *, our_line: str, remote_party: str) -> None:
        if not self._bot:
            log.warning("event=conversation_sync_skipped reason=discord_bot_not_ready")
            return

        our = normalize_e164(our_line)
        remote = normalize_e164(remote_party)
        key = thread_key(our_line=our, remote_party=remote)
        lock = self._locks.setdefault(key, asyncio.Lock())

        async with lock:
            try:
                await self._bot.wait_ready(timeout=30)
            except TimeoutError:
                log.error(
                    "event=conversation_sync_failed reason=discord_bot_not_ready "
                    "hint=use_a_dedicated_bot_token_not_rsadminbot"
                )
                return
            thread = self.store.get_thread(key) or {}
            channel_id = int(thread.get("channel_id") or self.config.channel_id_for_line(our) or 0)
            if channel_id <= 0:
                log.error("event=conversation_sync_failed reason=missing_channel_id our_line=%s", our)
                return

            content = self._render(our_line=our, remote_party=remote, thread=thread)
            message_id = thread.get("message_id")
            custom = digits_key(key)

            if message_id:
                try:
                    await self._bot.edit_thread_message(
                        channel_id=channel_id,
                        message_id=int(message_id),
                        content=content,
                        custom_id_key=custom,
                    )
                    log.info("event=conversation_updated key=%s channel_id=%s message_id=%s", key, channel_id, message_id)
                except Exception as exc:
                    err = str(exc).lower()
                    if "forbidden" in err or "50005" in err or "cannot edit" in err:
                        log.warning(
                            "event=conversation_recreate reason=cannot_edit_existing key=%s message_id=%s",
                            key,
                            message_id,
                        )
                        self.store.upsert_thread(key, {"message_id": None})
                        message_id = None
                    else:
                        raise
            if not message_id:
                new_id = await self._bot.post_thread_message(
                    channel_id=channel_id,
                    content=content,
                    custom_id_key=custom,
                )
                self.store.upsert_thread(key, {"message_id": str(new_id), "channel_id": channel_id})
                log.info("event=conversation_created key=%s channel_id=%s message_id=%s", key, channel_id, new_id)

    def _render(self, *, our_line: str, remote_party: str, thread: dict[str, Any]) -> str:
        conv_cfg = self.config.settings.get("conversations", {})
        sms_cfg = self.config.settings.get("sms", {})
        max_chars = int(conv_cfg.get("max_content_chars", 1900))
        return render_thread_content(
            our_line=our_line,
            remote_party=remote_party,
            lines=list(thread.get("lines") or []),
            display_name=thread.get("display_name"),
            from_numbers=sms_cfg.get("from_numbers", []),
            max_chars=max_chars,
        )

    async def refresh_all_threads(self) -> int:
        """Re-render every stored thread (e.g. after format changes)."""
        count = 0
        for key, thread in self.store.list_threads().items():
            our_line = str(thread.get("our_line") or key.split("|", 1)[0])
            remote_party = str(thread.get("remote_party") or key.split("|", 1)[-1])
            if not thread.get("message_id"):
                continue
            await self._sync_discord_message(our_line=our_line, remote_party=remote_party)
            count += 1
        return count

    def _is_own_line_echo_inbound(self, *, our_line: str, remote_party: str, text: str) -> bool:
        """Skip inbound when another Telnyx line we own just sent the same text."""
        allowed = set(self.config.allowed_from_numbers())
        if remote_party not in allowed:
            return False
        sender_key = thread_key(our_line=remote_party, remote_party=our_line)
        thread = self.store.get_thread(sender_key)
        if not thread:
            return False
        lines = list(thread.get("lines") or [])
        if not lines:
            return False
        last = lines[-1]
        return (
            str(last.get("direction") or "") == "out"
            and str(last.get("text") or "").strip() == str(text or "").strip()
        )

    def parse_custom_id(self, custom_id: str) -> tuple[str, str] | None:
        # telnyx:send:5419202540|5551234567  or telnyx:rename:...
        parts = str(custom_id or "").split(":")
        if len(parts) < 3 or parts[0] != "telnyx":
            return None
        digits = parts[2]
        if "|" not in digits:
            return None
        our_digits, remote_digits = digits.split("|", 1)
        our = _digits_to_e164(our_digits)
        remote = _digits_to_e164(remote_digits)
        if not our or not remote:
            return None
        return our, remote


def _digits_to_e164(digits: str) -> str | None:
    raw = "".join(ch for ch in str(digits) if ch.isdigit())
    if not raw:
        return None
    if len(raw) == 10:
        return f"+1{raw}"
    return f"+{raw}"
