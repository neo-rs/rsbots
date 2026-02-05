#!/usr/bin/env python3
"""
RS Forwarder Bot
----------------
Standalone bot for forwarding messages from RS Server channels to webhooks.
All messages are branded with "Reselling Secrets" name and avatar from RS Server.
"""

import os
import sys
import json
import asyncio
import discord
from discord.ext import commands
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime
import platform
import subprocess
import shlex
import time
import hashlib
import unicodedata
from urllib.parse import urlparse

from RSForwarder import affiliate_rewriter
from RSForwarder import novnc_stack
from RSForwarder import rs_fs_sheet_sync
from RSForwarder import zephyr_release_feed_parser

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def _rsfs_embed(
    title: str,
    *,
    status: str = "",
    description: str = "",
    color: Optional[discord.Color] = None,
    fields: Optional[List[Tuple[str, str, bool]]] = None,
    footer: str = "RS-FS",
) -> discord.Embed:
    """
    Canonical RS-FS embed format for all `!rsfs*` commands.
    """
    c = color or discord.Color.dark_teal()
    emb = discord.Embed(title=title, color=c)
    if description:
        emb.description = description
    if status:
        emb.add_field(name="Status", value=status, inline=False)
    for (n, v, inline) in (fields or []):
        if not n:
            continue
        vv = str(v or "").strip() or "—"
        if len(vv) > 1024:
            vv = vv[:1021] + "..."
        emb.add_field(name=str(n), value=vv, inline=bool(inline))
    if footer:
        emb.set_footer(text=footer)
    return emb


class _RsFsManualResolveModal(discord.ui.Modal):
    def __init__(self, view: "_RsFsManualResolveView", *, store: str = "", sku: str = ""):
        st = (store or "").strip()
        sk = (sku or "").strip()
        t = "RS-FS: Provide store link"
        if st and sk:
            t = f"RS-FS: {st} {sk}"
        # Discord modal title is limited; keep it safe.
        t = (t[:42] + "...") if len(t) > 45 else t
        super().__init__(title=t)
        self._view = view
        self.url = discord.ui.TextInput(label="Store URL", placeholder="https://www.walmart.com/ip/...", required=True)
        self.title_in = discord.ui.TextInput(
            label="Product title (optional)",
            placeholder="Leave blank to keep current title",
            required=False,
            max_length=200,
        )
        self.add_item(self.url)
        self.add_item(self.title_in)

    async def on_submit(self, interaction: discord.Interaction):  # type: ignore[override]
        await self._view._handle_modal_submit(interaction, str(self.url.value or ""), str(self.title_in.value or ""))


class _RsFsManualResolveView(discord.ui.View):
    def __init__(self, bot_obj: "RSForwarderBot", ctx, entries: List[rs_fs_sheet_sync.RsFsPreviewEntry]):
        super().__init__(timeout=900)
        self._bot = bot_obj
        self._owner_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
        self._channel = getattr(ctx, "channel", None)
        self._items: List[Dict[str, str]] = []
        for e in entries or []:
            self._items.append(
                {
                    "store": str(getattr(e, "store", "") or ""),
                    "sku": str(getattr(e, "sku", "") or ""),
                    "title": str(getattr(e, "title", "") or ""),
                    "url": str(getattr(e, "monitor_url", "") or getattr(e, "url", "") or ""),
                    "error": str(getattr(e, "error", "") or ""),
                    "resolved_url": "",
                    "resolved_title": "",
                }
            )
        self._idx = 0

    def _current(self) -> Optional[Dict[str, str]]:
        if 0 <= self._idx < len(self._items):
            return self._items[self._idx]
        return None

    def _render_embed(self) -> discord.Embed:
        it = self._current() or {}
        store = str(it.get("store") or "").strip()
        sku = str(it.get("sku") or "").strip()
        title = str(it.get("title") or "").strip()
        url = str(it.get("url") or "").strip()
        err = str(it.get("error") or "").strip()
        rurl = str(it.get("resolved_url") or "").strip()
        rtitle = str(it.get("resolved_title") or "").strip()
        fields: List[Tuple[str, str, bool]] = [
            ("Item", f"`{self._idx + 1}` / `{len(self._items)}`", True),
            ("Store / SKU", f"`{store}` / `{sku}`", False),
        ]
        if title:
            fields.append(("Current title", title[:900], False))
        if url:
            fields.append(("Current URL", url[:900], False))
        if err:
            fields.append(("Reason", err[:500], False))
        if rurl:
            fields.append(("Resolved URL", rurl[:900], False))
        if rtitle:
            fields.append(("Resolved title", rtitle[:900], False))
        return _rsfs_embed(
            "RS-FS Manual Resolve",
            status="Action required",
            color=discord.Color.orange(),
            fields=fields,
            footer="RS-FS • Provide link saves + updates sheet • Next = view next item",
        )

    async def _guard(self, interaction: discord.Interaction) -> bool:
        try:
            uid = int(getattr(getattr(interaction, "user", None), "id", 0) or 0)
        except Exception:
            uid = 0
        if self._owner_id and uid and uid != self._owner_id:
            try:
                await interaction.response.send_message("❌ This resolver session belongs to the command invoker.", ephemeral=True)
            except Exception:
                pass
            return False
        return True

    async def _handle_modal_submit(self, interaction: discord.Interaction, url: str, title: str) -> None:
        if not await self._guard(interaction):
            return
        it = self._current()
        if not it:
            try:
                await interaction.response.send_message("Done.", ephemeral=True)
            except Exception:
                pass
            return

        u = (url or "").strip()
        if not (u.startswith("http://") or u.startswith("https://")):
            try:
                await interaction.response.send_message("❌ URL must start with http:// or https://", ephemeral=True)
            except Exception:
                pass
            return

        t = (title or "").strip() or (it.get("title") or "").strip() or u

        # Compute affiliate URL (plain) and upsert into sheet immediately.
        aff = u
        try:
            mapped, _notes = await affiliate_rewriter.compute_affiliate_rewrites_plain(self._bot.config, [u])
            aff = str((mapped or {}).get(u) or u).strip()
        except Exception:
            aff = u

        store = str(it.get("store") or "")
        sku = str(it.get("sku") or "")
        ok, msg, added, updated = await self._bot._rs_fs_sheet.upsert_rows([[store, sku, t, aff, u]])

        # Persist override
        try:
            overrides = self._bot._load_rs_fs_manual_overrides()
            overrides[self._bot._rs_fs_override_key(store, sku)] = {"url": u, "title": t}
            self._bot._save_rs_fs_manual_overrides(overrides)
        except Exception:
            pass

        it["resolved_url"] = u
        it["resolved_title"] = t

        try:
            await interaction.response.send_message(
                f"✅ Saved. sheet_added={added} sheet_updated={updated}",
                ephemeral=True,
            )
        except Exception:
            pass

        # Advance to next item
        self._idx = min(self._idx + 1, len(self._items))
        # If we're done, keep the view visible but disable navigation buttons (Finish stays available).
        if self._idx >= len(self._items):
            for child in self.children:
                try:
                    if getattr(child, "label", "") in {"Provide link", "Next"}:
                        child.disabled = True  # type: ignore[attr-defined]
                except Exception:
                    pass
        try:
            if interaction.message:
                await interaction.message.edit(embed=self._render_embed(), view=self)
        except Exception:
            pass

    @discord.ui.button(label="Provide link", style=discord.ButtonStyle.primary)
    async def provide_link(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if not await self._guard(interaction):
            return
        if self._idx >= len(self._items):
            try:
                await interaction.response.send_message("✅ Nothing left to resolve.", ephemeral=True)
            except Exception:
                pass
            return
        try:
            it = self._current() or {}
            await interaction.response.send_modal(
                _RsFsManualResolveModal(
                    self,
                    store=str(it.get("store") or ""),
                    sku=str(it.get("sku") or ""),
                )
            )
        except Exception:
            try:
                await interaction.response.send_message("❌ Could not open modal.", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if not await self._guard(interaction):
            return
        self._idx = min(self._idx + 1, len(self._items))
        if self._idx >= len(self._items):
            for child in self.children:
                try:
                    if getattr(child, "label", "") in {"Provide link", "Next"}:
                        child.disabled = True  # type: ignore[attr-defined]
                except Exception:
                    pass
        try:
            await interaction.response.edit_message(embed=self._render_embed(), view=self)
        except Exception:
            pass

    @discord.ui.button(label="Finish", style=discord.ButtonStyle.danger)
    async def finish(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if not await self._guard(interaction):
            return
        for child in self.children:
            try:
                child.disabled = True  # type: ignore[attr-defined]
            except Exception:
                pass
        try:
            await interaction.response.edit_message(embed=self._render_embed(), view=self)
        except Exception:
            pass


class _RsFsInteractionCtx:
    """
    Minimal ctx-like wrapper so we can re-use existing command callbacks from button clicks.
    Uses interaction.followup for all sends (caller should defer first).
    """

    def __init__(self, interaction: discord.Interaction):
        self._interaction = interaction
        self.author = getattr(interaction, "user", None)
        self.guild = getattr(interaction, "guild", None)
        self.channel = getattr(interaction, "channel", None)

    async def send(self, content: Optional[str] = None, **kwargs):
        return await self._interaction.followup.send(content=content, **kwargs)


class _RsFsCheckView(discord.ui.View):
    def __init__(self, bot_obj: "RSForwarderBot", *, owner_id: int = 0, run_limit: int = 250):
        super().__init__(timeout=900)
        self._bot = bot_obj
        self._owner_id = int(owner_id or 0)
        self._run_limit = int(run_limit or 0)

    async def _guard(self, interaction: discord.Interaction) -> bool:
        try:
            uid = int(getattr(getattr(interaction, "user", None), "id", 0) or 0)
        except Exception:
            uid = 0
        # Allow server admins to use the buttons even if not the owner.
        try:
            perms = getattr(getattr(interaction, "user", None), "guild_permissions", None)
            is_admin = bool(getattr(perms, "administrator", False))
        except Exception:
            is_admin = False
        if self._owner_id and uid and uid != self._owner_id and not is_admin:
            try:
                await interaction.response.send_message("❌ This RS-FS session belongs to the command invoker.", ephemeral=True)
            except Exception:
                pass
            return False
        return True

    @discord.ui.button(label="Run mirror sync", style=discord.ButtonStyle.primary)
    async def run_sync(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if not await self._guard(interaction):
            return
        # Require admin to start writes from a button (prevents accidental runs).
        try:
            perms = getattr(getattr(interaction, "user", None), "guild_permissions", None)
            if not bool(getattr(perms, "administrator", False)):
                await interaction.response.send_message("❌ Admins only: run `!rsfsrun` if you need a live write.", ephemeral=True)
                return
        except Exception:
            pass
        try:
            await interaction.response.defer(thinking=False)
        except Exception:
            pass
        cmd = None
        try:
            cmd = self._bot.bot.get_command("rsfsrun")
        except Exception:
            cmd = None
        if not cmd:
            await interaction.followup.send("❌ `rsfsrun` command not found.", ephemeral=True)
            return
        ctx2 = _RsFsInteractionCtx(interaction)
        try:
            await cmd.callback(ctx2, str(self._run_limit))  # type: ignore[misc]
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to run sync: {str(e)[:200]}", ephemeral=True)

    @discord.ui.button(label="Monitor scan", style=discord.ButtonStyle.secondary)
    async def scan_monitors(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if not await self._guard(interaction):
            return
        try:
            perms = getattr(getattr(interaction, "user", None), "guild_permissions", None)
            if not bool(getattr(perms, "administrator", False)):
                await interaction.response.send_message("❌ Admins only.", ephemeral=True)
                return
        except Exception:
            pass
        try:
            await interaction.response.defer(thinking=False)
        except Exception:
            pass
        cmd = None
        try:
            cmd = self._bot.bot.get_command("rsfsmonitorscan")
        except Exception:
            cmd = None
        if not cmd:
            await interaction.followup.send("❌ `rsfsmonitorscan` command not found.", ephemeral=True)
            return
        ctx2 = _RsFsInteractionCtx(interaction)
        try:
            await cmd.callback(ctx2)  # type: ignore[misc]
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to scan monitors: {str(e)[:200]}", ephemeral=True)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.success)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):  # type: ignore[override]
        if not await self._guard(interaction):
            return
        try:
            await interaction.response.defer(thinking=False)
        except Exception:
            pass
        try:
            emb = await self._bot._build_rsfs_check_embed()
            if interaction.message:
                await interaction.message.edit(embed=emb, view=self)
        except Exception as e:
            await interaction.followup.send(f"❌ Refresh failed: {str(e)[:200]}", ephemeral=True)


class RSForwarderBot:
    """Main bot class for forwarding messages with Reselling Secrets branding"""
    
    def __init__(self):
        self.config_path = Path(__file__).parent / "config.json"
        self.config: Dict[str, Any] = {}
        self.rs_guild: Optional[discord.Guild] = None
        self.rs_icon_url: Optional[str] = None
        self.stats = {
            'messages_forwarded': 0,
            'errors': 0,
            'started_at': None
        }
        # Mavely monitoring / alerting state (in-memory)
        self._mavely_monitor_task: Optional[asyncio.Task] = None
        self._mavely_last_preflight_ok: Optional[bool] = None
        self._mavely_last_preflight_status: Optional[int] = None
        self._mavely_last_preflight_err: Optional[str] = None
        self._mavely_last_alert_ts: float = 0.0
        self._mavely_last_autologin_ts: float = 0.0
        self._mavely_last_autologin_ok: Optional[bool] = None
        self._mavely_last_autologin_msg: Optional[str] = None
        self._mavely_last_refresher_pid: Optional[int] = None
        self._mavely_last_refresher_log_path: Optional[str] = None
        self.load_config()

        # RS - Full Send List sheet sync (Zephyr release feed -> Google Sheet)
        self._rs_fs_sheet = rs_fs_sheet_sync.RsFsSheetSync(self.config)
        self._rs_fs_seen_message_ids: Set[int] = set()
        # Guard to prevent concurrent sheet syncs during manual runs (avoids partial-chunk thrash)
        self._rs_fs_manual_run_in_progress: bool = False
        # Debounce for auto check/status messages (avoid spamming on multi-chunk listreleases).
        self._rs_fs_last_auto_check_ts: float = 0.0
        # Debounce for auto Current List writes (avoid re-writing on every chunk message).
        self._rs_fs_last_current_list_hash: str = ""
        self._rs_fs_last_current_list_ts: float = 0.0
        
        # Validate required config
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        # Setup bot
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        
        # Use "!" as the command prefix. Commands are already namespaced (rsadd, rsfsrun, etc.)
        # so there is no need to double-prefix as "!rs" (which would require typing "!rsrsfsrun").
        self.bot = commands.Bot(command_prefix='!', intents=intents)
        self._setup_events()
        self._setup_commands()

    def _mavely_monitor_interval_s(self) -> int:
        try:
            v = int((self.config or {}).get("mavely_monitor_interval_s") or 300)
        except Exception:
            v = 300
        return max(60, min(v, 3600))

    def _rs_server_guild_id(self) -> int:
        """
        RS Server guild ID (used for branding/icon fetch). Canonical config keys:
        - rs_server_guild_id (preferred)
        - guild_id (legacy)
        """
        try:
            return int((self.config or {}).get("rs_server_guild_id") or (self.config or {}).get("guild_id") or 0)
        except Exception:
            return 0

    def _test_server_guild_id(self) -> int:
        """
        Neo Test Server guild ID (diagnostic / optional features).
        """
        try:
            return int((self.config or {}).get("test_server_guild_id") or 0)
        except Exception:
            return 0

    async def _startup_validate_visibility(self) -> None:
        """
        Best-effort startup diagnostics:
        - verify bot is in RS server / test server (if configured)
        - verify configured source channel IDs are accessible
        """
        try:
            rs_gid = self._rs_server_guild_id()
            test_gid = self._test_server_guild_id()
            if rs_gid and (self.bot.get_guild(int(rs_gid)) is None):
                print(f"{Colors.YELLOW}[Startup] ⚠️ Bot is NOT in RS Server guild_id={rs_gid}{Colors.RESET}")
            if test_gid and (self.bot.get_guild(int(test_gid)) is None):
                print(f"{Colors.YELLOW}[Startup] ⚠️ Bot is NOT in Neo Test Server guild_id={test_gid}{Colors.RESET}")

            channels = (self.config or {}).get("channels") or []
            bad: List[str] = []
            okc = 0
            for ch in channels:
                try:
                    cid = str((ch or {}).get("source_channel_id") or "").strip()
                    if not cid or not cid.isdigit():
                        continue
                    obj = await self._resolve_channel_by_id(int(cid))
                    if obj is None:
                        bad.append(cid)
                    else:
                        okc += 1
                except Exception:
                    continue
            if okc or bad:
                print(f"{Colors.CYAN}[Startup] Channel access: ok={okc} missing={len(bad)}{Colors.RESET}")
                if bad:
                    print(f"{Colors.YELLOW}[Startup] Missing/unreadable channels (first 6): {', '.join(bad[:6])}{Colors.RESET}")
        except Exception:
            pass

    def _mavely_alert_cooldown_s(self) -> int:
        try:
            v = int((self.config or {}).get("mavely_alert_cooldown_s") or 1800)
        except Exception:
            v = 1800
        return max(300, min(v, 24 * 3600))

    def _mavely_autologin_enabled(self) -> bool:
        try:
            v = (self.config or {}).get("mavely_autologin_on_fail")
            if v is None:
                v = os.getenv("MAVELY_AUTOLOGIN_ON_FAIL", "")
            if isinstance(v, str):
                return v.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(v) if v is not None else True
        except Exception:
            return True

    def _mavely_login_creds(self) -> Tuple[str, str]:
        """
        Resolve Mavely login credentials for auto-login.

        Priority:
        1) merged config (config.json + config.secrets.json)
        2) env vars
        3) secrets file directly (server-only)
        """
        email = ""
        password = ""
        try:
            email = str((self.config or {}).get("mavely_login_email") or "").strip()
            password = str((self.config or {}).get("mavely_login_password") or "").strip()
        except Exception:
            email, password = "", ""

        if not (email and password):
            try:
                e2 = (os.getenv("MAVELY_LOGIN_EMAIL", "") or "").strip()
                p2 = (os.getenv("MAVELY_LOGIN_PASSWORD", "") or "").strip()
                if e2 and p2:
                    email, password = e2, p2
            except Exception:
                pass

        if not (email and password):
            try:
                s = self._load_secrets_dict()
                email = email or str((s or {}).get("mavely_login_email") or "").strip()
                password = password or str((s or {}).get("mavely_login_password") or "").strip()
            except Exception:
                pass

        return email, password

    def _rsfs_auto_check_on_zephyr(self) -> bool:
        try:
            return bool((self.config or {}).get("rs_fs_auto_check_on_zephyr", False))
        except Exception:
            return False

    def _rsfs_auto_check_debounce_s(self) -> float:
        try:
            v = float((self.config or {}).get("rs_fs_auto_check_debounce_s", 45.0) or 45.0)
        except Exception:
            v = 45.0
        return max(10.0, min(v, 300.0))

    def _rsfs_auto_write_current_list(self) -> bool:
        try:
            return bool((self.config or {}).get("rs_fs_auto_write_current_list", True))
        except Exception:
            return True

    def _rsfs_current_list_debounce_s(self) -> float:
        try:
            v = float((self.config or {}).get("rs_fs_current_list_debounce_s", 20.0) or 20.0)
        except Exception:
            v = 20.0
        return max(5.0, min(v, 180.0))

    @staticmethod
    def _rsfs_key_store_sku(store: str, sku: str) -> str:
        return f"{str(store or '').strip().lower()}|{str(sku or '').strip().lower()}"

    @staticmethod
    def _rsfs_title_is_bad(title: str, *, url: str = "") -> bool:
        """
        Detect cached "titles" that are actually URLs or generic placeholders.
        If true, we should re-resolve (monitor/website/manual) instead of trusting cache.
        """
        t = str(title or "").strip()
        if not t:
            return True
        tl = t.lower()
        if tl.startswith("http://") or tl.startswith("https://"):
            return True
        u = str(url or "").strip()
        if u and t.strip() == u.strip():
            return True
        if tl in {"amazon.com", "amazon", "target", "walmart", "best buy", "bestbuy", "costco", "gamestop"}:
            return True
        return False

    async def _rsfs_write_current_list(
        self,
        merged_text: str,
        *,
        resolved_by_key: Optional[Dict[str, Dict[str, str]]] = None,
        reason: str = "auto",
    ) -> Tuple[bool, str, int]:
        """
        Mirror the latest /listreleases run into the Current List tab.
        `resolved_by_key` keys: store_lower|sku_lower -> {title,url,affiliate_url,source,last_release_id}
        """
        if not getattr(self, "_rs_fs_sheet", None):
            return False, "sheet sync not initialized", 0
        if not self._rs_fs_sheet.enabled():
            return False, "sheet disabled", 0
        if not self._rsfs_auto_write_current_list():
            return True, "auto current list disabled", 0

        txt = (merged_text or "").strip()
        if not txt:
            return False, "empty merged text", 0

        # Debounce by content hash (avoid rewriting on every chunk)
        h = hashlib.sha1(txt.encode("utf-8", errors="ignore")).hexdigest()[:12]
        now = time.time()
        force_write = bool(resolved_by_key) or (str(reason or "").strip().lower() != "auto")
        if (
            (not force_write)
            and h == (self._rs_fs_last_current_list_hash or "")
            and (now - float(self._rs_fs_last_current_list_ts or 0.0)) < self._rsfs_current_list_debounce_s()
        ):
            return True, "debounced", 0
        self._rs_fs_last_current_list_hash = h
        self._rs_fs_last_current_list_ts = now

        # Pull cached history once (cheap) so we can pre-fill title/url without scanning.
        history = {}
        try:
            history = await self._rs_fs_sheet.fetch_history_cache(force=False)
        except Exception:
            history = {}
        overrides = {}
        try:
            overrides = self._load_rs_fs_manual_overrides()
        except Exception:
            overrides = {}

        recs = zephyr_release_feed_parser.parse_release_feed_records(txt) or []
        rows: List[List[str]] = []
        last_seen = rs_fs_sheet_sync.RsFsSheetSync._utc_now_iso()  # type: ignore[attr-defined]

        for r in recs:
            rid = int(getattr(r, "release_id", 0) or 0)
            store = str(getattr(r, "store", "") or "").strip()
            sku_label = str(getattr(r, "sku", "") or "").strip()
            monitor_tag = str(getattr(r, "monitor_tag", "") or "").strip()
            category = str(getattr(r, "category", "") or "").strip()
            ch_id = str(getattr(r, "channel_id", "") or "").strip()
            is_sku = bool(getattr(r, "is_sku_candidate", True))

            key = self._rsfs_key_store_sku(store, sku_label) if (store and sku_label) else ""

            title = ""
            url = ""
            aff = ""
            status_bits: List[str] = []
            status_bits.append("sku" if is_sku else "non_sku")
            if store:
                status_bits.append("store")
            else:
                status_bits.append("no_store")
            if monitor_tag:
                status_bits.append("monitor")
            else:
                status_bits.append("no_monitor")

            # Prefer explicit resolved map (used by rsfsrun end-of-run)
            src = ""
            if key and resolved_by_key and key in resolved_by_key:
                d = resolved_by_key.get(key) or {}
                title = str(d.get("title") or "").strip()
                url = str(d.get("url") or "").strip()
                aff = str(d.get("affiliate_url") or "").strip()
                src = str(d.get("source") or "").strip()
                status_bits.append("resolved")
            # Manual overrides
            if key and not url:
                ov = overrides.get(self._rs_fs_override_key(store, sku_label)) if store and sku_label else None
                if isinstance(ov, dict) and str(ov.get("url") or "").strip():
                    url = str(ov.get("url") or "").strip()
                    # Never use the URL as a title.
                    ov_t = str(ov.get("title") or "").strip()
                    if ov_t and not self._rsfs_title_is_bad(ov_t, url=url):
                        title = ov_t
                    status_bits.append("manual")
                    src = src or "manual"
            # History cache
            if key and not url and key in (history or {}):
                hrec = history.get(key) or {}
                url = str(hrec.get("url") or "").strip()
                ht = str(hrec.get("title") or "").strip()
                if ht and not self._rsfs_title_is_bad(ht, url=url):
                    title = ht
                aff = str(hrec.get("affiliate_url") or "").strip()
                if url or title:
                    status_bits.append("history")
                    src = src or "history"

            if title and len(title) > 140:
                title = title[:137] + "..."

            remove_cmd = f"/removereleaseid release_id: {rid}" if rid else "/removereleaseid release_id: ?"
            status = ",".join([s for s in status_bits if s])
            if src:
                status = (status + f",src={src}").strip(",")

            rows.append(
                [
                    str(rid or ""),
                    store,
                    sku_label,
                    monitor_tag,
                    category,
                    ch_id,
                    title,
                    url,
                    aff,
                    status,
                    remove_cmd,
                    last_seen,
                ]
            )

        ok, msg, n = await self._rs_fs_sheet.write_current_list_mirror(rows)
        try:
            print(f"{Colors.CYAN}[RS-FS Current]{Colors.RESET} mirror ok={ok} rows={n} reason={reason} hash={h} msg={msg}")
        except Exception:
            pass
        return ok, msg, n

    async def _collect_latest_zephyr_release_feed_text(
        self,
        ch: discord.TextChannel,
        *,
        history_limit: int = 350,
        max_chunks: int = 30,
    ) -> Tuple[str, int, bool]:
        """
        Collect the most recent Zephyr /listreleases embed chunks and return merged text.
        Returns (merged_text, chunk_count, found_header).
        """
        parts: List[str] = []
        found_header = False
        try:
            async for m in ch.history(limit=int(history_limit or 350)):
                if not (getattr(m, "embeds", None) or []) and not (getattr(m, "content", None) or ""):
                    continue
                t = self._collect_embed_text(m)
                if not t:
                    continue
                if not zephyr_release_feed_parser.looks_like_release_feed_embed_text(t):
                    continue
                parts.append(t)
                if "release feed" in t.lower():
                    found_header = True
                    break
                if len(parts) >= int(max_chunks or 30):
                    break
        except Exception:
            parts = []
            found_header = False

        if not parts:
            return "", 0, False
        merged = "\n".join(reversed(parts))
        return merged, len(parts), bool(found_header)

    async def _build_rsfs_check_embed(self) -> discord.Embed:
        """
        Build a rich RS-FS status/check embed:
        - Google Sheets readiness
        - Sheet SKU count
        - Latest /listreleases counts (and why they may differ)
        """
        sid = str((self.config or {}).get("rs_fs_sheet_spreadsheet_id") or "").strip()
        gid = str((self.config or {}).get("rs_fs_sheet_tab_gid") or "").strip()

        ok = False
        msg = "not initialized"
        tab = None
        n = 0
        try:
            if getattr(self, "_rs_fs_sheet", None):
                ok, msg, tab, n = await self._rs_fs_sheet.preflight()
        except Exception as e:
            ok, msg, tab, n = False, f"failed: {str(e)[:200]}", None, 0

        emb = _rsfs_embed(
            "RS-FS Check",
            status=("✅ OK" if ok else f"❌ NOT ready: {msg}"),
            color=(discord.Color.green() if ok else discord.Color.red()),
            fields=[
                ("Spreadsheet", f"`{sid}`" if sid else "—", False),
                ("Tab", f"`{tab}` (gid={gid})" if (tab and gid) else (f"gid={gid}" if gid else "—"), False),
                ("Existing SKUs (sheet)", str(int(n or 0)), True),
            ],
            footer="RS-FS • Buttons below run actions",
        )

        # Best-effort: analyze the latest /listreleases output (why counts differ)
        try:
            ch_id = self._zephyr_release_feed_channel_id()
        except Exception:
            ch_id = None
        if not ch_id:
            return emb

        ch = None
        try:
            ch = await self._resolve_channel_by_id(int(ch_id))
        except Exception:
            ch = None
        if not ch or not hasattr(ch, "history"):
            emb.add_field(name="Latest /listreleases", value="❌ cannot access Zephyr channel", inline=False)
            return emb

        merged_text, chunk_n, _found_header = await self._collect_latest_zephyr_release_feed_text(ch)
        if not merged_text:
            emb.add_field(name="Latest /listreleases", value="❌ no recent release-feed embeds found", inline=False)
            return emb

        # Analyze the merged run using the robust record parser (handles chunk splits + non-SKU entries).
        recs = zephyr_release_feed_parser.parse_release_feed_records(merged_text) or []
        all_ids = sorted({int(getattr(r, "release_id", 0) or 0) for r in recs if int(getattr(r, "release_id", 0) or 0) > 0})
        total_items = len(all_ids)

        sku_candidate_recs = [r for r in recs if int(getattr(r, "release_id", 0) or 0) > 0 and bool(getattr(r, "is_sku_candidate", True))]
        non_sku_recs = [r for r in recs if int(getattr(r, "release_id", 0) or 0) > 0 and (not bool(getattr(r, "is_sku_candidate", True)))]
        parseable_recs = [r for r in sku_candidate_recs if str(getattr(r, "store", "") or "").strip()]
        sku_unknown_store_recs = [r for r in sku_candidate_recs if not str(getattr(r, "store", "") or "").strip()]

        # SKU maps (public sheet uses SKU uniqueness)
        sku_to_store: Dict[str, str] = {}
        sku_to_rid: Dict[str, int] = {}
        skus: List[str] = []
        for r in parseable_recs:
            st = str(getattr(r, "store", "") or "").strip()
            sk = str(getattr(r, "sku", "") or "").strip()
            rid = int(getattr(r, "release_id", 0) or 0)
            k = sk.lower()
            if not k:
                continue
            if k not in sku_to_store:
                sku_to_store[k] = st
            if k not in sku_to_rid and rid > 0:
                sku_to_rid[k] = rid
            skus.append(k)

        unique_skus = set(skus)
        dupes = max(0, len(skus) - len(unique_skus))

        # Compare to what's currently in the sheet (if enabled)
        existing_set: Set[str] = set()
        try:
            if ok and getattr(self, "_rs_fs_sheet", None):
                await self._rs_fs_sheet._fetch_existing_skus_if_needed()  # type: ignore[attr-defined]
                existing_set = set(getattr(self._rs_fs_sheet, "_dedupe_skus", set()) or set())
        except Exception:
            existing_set = set()

        missing = sorted([k for k in unique_skus if k not in existing_set]) if existing_set else []
        extra = sorted([k for k in existing_set if k not in unique_skus]) if existing_set else []

        # Detect "bad" titles already in the sheet (URL-as-title / placeholders).
        bad_titles: List[Tuple[int, str, str, str]] = []  # (rid, store, sku_lower, title)
        try:
            abc_map: Dict[str, Dict[str, str]] = {}
            if ok and getattr(self, "_rs_fs_sheet", None):
                abc_map = await self._rs_fs_sheet.fetch_sheet_abc_map()  # type: ignore[attr-defined]
            for sku_l in sorted(list(unique_skus)):
                rec = (abc_map or {}).get(sku_l) or {}
                title0 = str(rec.get("title") or "").strip()
                store0 = str(rec.get("store") or "").strip() or str(sku_to_store.get(sku_l) or "").strip()
                sku0 = str(rec.get("sku") or "").strip() or sku_l
                url0 = ""
                try:
                    if store0 and sku0:
                        url0 = rs_fs_sheet_sync.build_store_link(store0, sku0)
                except Exception:
                    url0 = ""
                if self._rsfs_title_is_bad(title0, url=url0):
                    rid0 = int(sku_to_rid.get(sku_l) or 0)
                    bad_titles.append((rid0, store0 or "?", sku_l, title0))
        except Exception:
            bad_titles = []

        emb.add_field(
            name="Latest /listreleases",
            value="\n".join(
                [
                    f"items `{total_items}` • chunks `{chunk_n}`",
                    f"SKU candidates `{len(sku_candidate_recs)}`",
                    f"parseable store+sku `{len(parseable_recs)}` (unique `{len(unique_skus)}`, dupes `{dupes}`)",
                    f"non-SKU `{len(non_sku_recs)}`",
                    f"SKU but unknown store `{len(sku_unknown_store_recs)}`",
                ]
            ),
            inline=False,
        )

        # Explain why items are not expected to appear in the public sheet.
        why_lines: List[str] = []
        cmd_lines: List[str] = []
        for r in (sku_unknown_store_recs[:6] + non_sku_recs[:6]):
            rid = int(getattr(r, "release_id", 0) or 0)
            sk = str(getattr(r, "sku", "") or "").strip()
            st = str(getattr(r, "store", "") or "").strip()
            is_sku = bool(getattr(r, "is_sku_candidate", True))
            kind = "non-SKU" if not is_sku else "unknown-store"
            why_lines.append(f"- `{rid}` `{kind}` {('`'+st+'`' if st else '')} `{sk}`")
            if rid:
                cmd_lines.append(f"/removereleaseid release_id: {rid}")
        if why_lines:
            why_val = "\n".join(why_lines).strip()
            if cmd_lines:
                # Each command gets its own code block wrapper for individual copying
                cmd_blocks = "\n".join([f"```\n{cmd}\n```" for cmd in cmd_lines])
                why_val = (why_val + "\n\nCommands (copy):\n" + cmd_blocks).strip()
            emb.add_field(
                name="Why some items aren’t in the public sheet",
                value=why_val,
                inline=False,
            )
        if missing:
            sample = []
            for k in missing[:12]:
                st = sku_to_store.get(k) or "?"
                rid = int(sku_to_rid.get(k) or 0)
                sample.append(f"- `{rid}` `{st}` `{k}`")
            cmd_lines2 = [f"/removereleaseid release_id: {int(sku_to_rid.get(k) or 0)}" for k in missing[:12] if int(sku_to_rid.get(k) or 0) > 0]
            val2 = f"`{len(missing)}`\n" + "\n".join(sample)
            if cmd_lines2:
                # Each command gets its own code block wrapper for individual copying
                cmd_blocks2 = "\n".join([f"```\n{cmd}\n```" for cmd in cmd_lines2])
                val2 = (val2 + "\n\nCommands (copy):\n" + cmd_blocks2).strip()
            emb.add_field(
                name="Missing from sheet (parseable SKUs)",
                value=val2,
                inline=False,
            )
        if extra:
            sample = [f"- `{k}`" for k in extra[:12]]
            emb.add_field(
                name="In sheet but not in latest list",
                value=f"`{len(extra)}`\n" + "\n".join(sample),
                inline=False,
            )

        if bad_titles:
            # Keep this field short; this is a "diagnostic" list.
            lines: List[str] = []
            for rid0, store0, sku_l, title0 in bad_titles[:10]:
                t = (title0 or "").replace("\n", " ").strip()
                if len(t) > 60:
                    t = t[:57] + "..."
                lines.append(f"- `{rid0}` `{store0}` `{sku_l}` — {t if t else '(blank)'}")
            emb.add_field(
                name="Bad titles in sheet (will be re-resolved)",
                value=f"`{len(bad_titles)}`\n" + "\n".join(lines),
                inline=False,
            )

        return emb

    def _mavely_autologin_cooldown_s(self) -> int:
        try:
            v = int((self.config or {}).get("mavely_autologin_cooldown_s") or 300)
        except Exception:
            v = 300
        return max(30, min(v, 6 * 3600))

    def _mavely_state_dir(self) -> Path:
        """
        Local durable state/log directory for Mavely automation.
        - On Oracle/Linux: defaults to the same dir used by noVNC stack.
        - Else: keep it local under RSForwarder/.tmp (git-ignored).
        """
        try:
            if self._is_local_exec():
                raw = (
                    str((self.config or {}).get("mavely_novnc_state_dir") or "").strip()
                    or (os.getenv("MAVELY_NOVNC_STATE_DIR", "") or "").strip()
                    or "/tmp/rsforwarder_mavely_novnc"
                )
                p = Path(raw)
            else:
                p = Path(__file__).parent / ".tmp" / "mavely_monitor"
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            return Path(__file__).parent

    def _mavely_status_path(self) -> Path:
        return self._mavely_state_dir() / "mavely_status.json"

    def _mavely_monitor_log_path(self) -> Path:
        return self._mavely_state_dir() / "mavely_monitor.log"

    def _mavely_append_log(self, msg: str) -> None:
        try:
            ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            line = f"[{ts}] {msg}".rstrip()
            print(f"{Colors.CYAN}[Mavely]{Colors.RESET} {line}")
            try:
                p = self._mavely_monitor_log_path()
                with open(p, "a", encoding="utf-8", errors="replace") as f:
                    f.write(line + "\n")
            except Exception:
                pass
        except Exception:
            pass

    def _mavely_write_status(self, extra: Optional[Dict[str, Any]] = None) -> None:
        try:
            def _short(s: Optional[str], n: int) -> str:
                t = (s or "").replace("\n", " ").strip()
                return t if len(t) <= n else (t[:n] + "...")

            data: Dict[str, Any] = {
                "ts_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "preflight_ok": bool(self._mavely_last_preflight_ok),
                "preflight_status": self._mavely_last_preflight_status,
                "preflight_err": _short(self._mavely_last_preflight_err, 500),
                "last_alert_ts": float(self._mavely_last_alert_ts or 0.0),
                "last_autologin_ts": float(self._mavely_last_autologin_ts or 0.0),
                "last_autologin_ok": self._mavely_last_autologin_ok,
                "last_autologin_msg": _short(self._mavely_last_autologin_msg, 1200),
                "last_refresher_pid": self._mavely_last_refresher_pid,
                "last_refresher_log_path": self._mavely_last_refresher_log_path,
            }
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in data:
                        data[k] = v
            tmp = self._mavely_status_path().with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8", errors="replace")
            os.replace(str(tmp), str(self._mavely_status_path()))
        except Exception:
            pass

    def _mavely_alert_user_ids(self) -> List[int]:
        """
        Read alert targets from config.secrets.json (server-only).
        """
        try:
            d = self._load_secrets_dict()
            raw = d.get("mavely_alert_user_ids") or []
            if isinstance(raw, int):
                raw = [raw]
            if not isinstance(raw, list):
                return []
            out: List[int] = []
            for x in raw:
                try:
                    out.append(int(str(x).strip()))
                except Exception:
                    continue
            # unique, stable order
            seen = set()
            uniq: List[int] = []
            for i in out:
                if i in seen:
                    continue
                seen.add(i)
                uniq.append(i)
            return uniq
        except Exception:
            return []

    def _mavely_admin_user_ids(self) -> List[int]:
        """
        Allowed to run Mavely login commands in DMs (server-only).
        """
        try:
            d = self._load_secrets_dict()
            raw = d.get("mavely_admin_user_ids") or []
            if isinstance(raw, int):
                raw = [raw]
            if not isinstance(raw, list):
                return []
            out: List[int] = []
            for x in raw:
                try:
                    out.append(int(str(x).strip()))
                except Exception:
                    continue
            seen = set()
            uniq: List[int] = []
            for i in out:
                if i in seen:
                    continue
                seen.add(i)
                uniq.append(i)
            return uniq
        except Exception:
            return []

    def _save_mavely_user_lists(self, *, alert_ids: List[int], admin_ids: List[int]) -> bool:
        try:
            d = self._load_secrets_dict()
            d["mavely_alert_user_ids"] = [int(x) for x in (alert_ids or [])]
            d["mavely_admin_user_ids"] = [int(x) for x in (admin_ids or [])]
            return self._save_secrets_dict(d)
        except Exception:
            return False

    def _ensure_mavely_user(self, user_id: int) -> bool:
        """
        Ensure user is both an alert target and DM-admin for Mavely login commands.
        """
        try:
            uid = int(user_id)
        except Exception:
            return False
        alert_ids = self._mavely_alert_user_ids()
        admin_ids = self._mavely_admin_user_ids()
        if uid not in alert_ids:
            alert_ids.append(uid)
        if uid not in admin_ids:
            admin_ids.append(uid)
        return self._save_mavely_user_lists(alert_ids=alert_ids, admin_ids=admin_ids)

    def _remove_mavely_user(self, user_id: int) -> bool:
        try:
            uid = int(user_id)
        except Exception:
            return False
        alert_ids = [i for i in self._mavely_alert_user_ids() if i != uid]
        admin_ids = [i for i in self._mavely_admin_user_ids() if i != uid]
        return self._save_mavely_user_lists(alert_ids=alert_ids, admin_ids=admin_ids)

    def _is_mavely_admin_ctx(self, ctx) -> bool:
        """
        Guild: require administrator permission.
        DM: require user id to be in mavely_admin_user_ids or mavely_alert_user_ids.
        """
        try:
            if getattr(ctx, "guild", None) is not None:
                return bool(getattr(ctx.author, "guild_permissions", None) and ctx.author.guild_permissions.administrator)
            uid = int(getattr(ctx.author, "id", 0) or 0)
            if not uid:
                return False
            return (uid in self._mavely_admin_user_ids()) or (uid in self._mavely_alert_user_ids())
        except Exception:
            return False

    async def _dm_user(self, user_id: int, content: str) -> bool:
        try:
            uid = int(user_id)
            if uid <= 0:
                return False
            user = self.bot.get_user(uid)
            if user is None:
                user = await self.bot.fetch_user(uid)
            if user is None:
                return False
            await user.send(content)
            return True
        except Exception:
            return False

    def _build_tunnel_instructions(self, web_port: int, url_path: str) -> str:
        host_hint = "<oracle-host>"
        user_hint = "rsadmin"
        try:
            oraclekeys_path = _REPO_ROOT / "oraclekeys"
            servers_json = oraclekeys_path / "servers.json"
            if servers_json.exists():
                servers = json.loads(servers_json.read_text(encoding="utf-8", errors="replace") or "[]")
                if isinstance(servers, list) and servers:
                    host_hint = str((servers[0] or {}).get("host") or host_hint)
                    user_hint = str((servers[0] or {}).get("user") or user_hint)
        except Exception:
            pass
        tunnel_cmd = f"ssh -i <YOUR_KEY> -L {int(web_port)}:127.0.0.1:{int(web_port)} {user_hint}@{host_hint}"
        return (
            "✅ noVNC is running (localhost-only on the server).\n\n"
            f"- If you already keep a tunnel running, just open:\n`http://localhost:{int(web_port)}{url_path}`\n\n"
            "Otherwise:\n"
            f"1) On your PC, open an SSH tunnel:\n```{tunnel_cmd}```\n"
            f"2) In your PC browser, open:\n`http://localhost:{int(web_port)}{url_path}`\n"
            "3) Log into Mavely in the Chromium window on that desktop.\n\n"
            "When you're done, run `!rsmavelycheck` to confirm the session is valid."
        )

    async def _mavely_monitor_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            interval = self._mavely_monitor_interval_s()
            try:
                targets = self._mavely_alert_user_ids()
                prev_ok = self._mavely_last_preflight_ok
                ok, status, err = await affiliate_rewriter.mavely_preflight(self.config)
                self._mavely_last_preflight_ok = bool(ok)
                self._mavely_last_preflight_status = int(status or 0)
                self._mavely_last_preflight_err = (err or "").strip() if not ok else ""
                self._mavely_write_status()
                if ok:
                    if prev_ok is not True:
                        self._mavely_append_log(f"preflight OK (status={status})")
                    await asyncio.sleep(interval)
                    continue

                # Log the failure (never log tokens/cookies; err is already "safe"/short).
                try:
                    msg0 = (err or "unknown error").replace("\n", " ").strip()
                    if len(msg0) > 240:
                        msg0 = msg0[:240] + "..."
                    self._mavely_append_log(f"preflight FAIL (status={status}) {msg0}")
                except Exception:
                    pass

                # If credentials are configured, try a headless auto-login first.
                # If it succeeds, we recover without requiring user action.
                email, password = self._mavely_login_creds()

                now = time.time()
                autologin_attempted = False
                # Log (once per fail-streak) why auto-login didn't run; otherwise it looks "stuck".
                try:
                    if prev_ok is not False:
                        reasons = []
                        if not self._is_local_exec():
                            reasons.append("not running on the Linux host")
                        if not self._mavely_autologin_enabled():
                            reasons.append(
                                "auto-login disabled (set mavely_autologin_on_fail=true in RSForwarder/config.json OR MAVELY_AUTOLOGIN_ON_FAIL=1)"
                            )
                        if not (email and password):
                            reasons.append("missing mavely_login_email/password in config.secrets.json")
                        if reasons:
                            self._mavely_append_log("auto-login skipped: " + "; ".join(reasons))
                except Exception:
                    pass

                if self._is_local_exec() and self._mavely_autologin_enabled() and email and password:
                    cooldown2 = self._mavely_autologin_cooldown_s()
                    if (now - float(self._mavely_last_autologin_ts or 0.0)) >= float(cooldown2):
                        autologin_attempted = True
                        self._mavely_last_autologin_ts = now
                        self._mavely_append_log("preflight FAIL -> attempting headless auto-login (cookie refresher)")
                        cfg_run = dict(self.config or {})
                        cfg_run["mavely_login_email"] = email
                        cfg_run["mavely_login_password"] = password
                        ok_run, out = await asyncio.to_thread(novnc_stack.run_cookie_refresher_headless, cfg_run, wait_login_s=180)
                        out_s = (out or "").strip()
                        if len(out_s) > 4000:
                            out_s = out_s[:4000] + "\n... (truncated)"
                        self._mavely_last_autologin_ok = bool(ok_run)
                        self._mavely_last_autologin_msg = out_s or ("ok" if ok_run else "failed")
                        self._mavely_write_status()
                        if ok_run:
                            ok2, status2, err2 = await affiliate_rewriter.mavely_preflight(self.config)
                            self._mavely_last_preflight_status = int(status2 or 0)
                            self._mavely_last_preflight_err = (err2 or "").strip() if not ok2 else ""
                            self._mavely_write_status()
                            if ok2:
                                self._mavely_append_log(f"auto-login recovered session (preflight OK status={status2})")
                                self._mavely_last_preflight_ok = True
                                await asyncio.sleep(interval)
                                continue

                # Failure path: alert (rate-limited) and optionally start noVNC + refresher automatically.
                cooldown = self._mavely_alert_cooldown_s()
                should_alert = (self._mavely_last_preflight_ok is True) or ((now - float(self._mavely_last_alert_ts)) >= float(cooldown))
                self._mavely_last_preflight_ok = False
                self._mavely_write_status()
                if not should_alert:
                    await asyncio.sleep(interval)
                    continue

                info = None
                pid = None
                log_path = None
                start_err = None
                # Only start noVNC if someone can be alerted (otherwise there's no human to complete login).
                if self._is_local_exec() and targets:
                    info, start_err = await asyncio.to_thread(novnc_stack.ensure_novnc, self.config)
                    if info and not start_err:
                        pid, log_path, _err2 = await asyncio.to_thread(
                            novnc_stack.start_cookie_refresher,
                            self.config,
                            display=str((info or {}).get("display") or ":99"),
                            wait_login_s=900,
                        )
                        self._mavely_last_refresher_pid = int(pid) if pid else None
                        self._mavely_last_refresher_log_path = str(log_path) if log_path else None
                        self._mavely_write_status(
                            {
                                "novnc_display": str((info or {}).get("display") or ""),
                                "novnc_web_port": int((info or {}).get("web_port") or 0),
                                "novnc_url_path": str((info or {}).get("url_path") or ""),
                            }
                        )

                msg = (err or "unknown error").replace("\n", " ").strip()
                if len(msg) > 240:
                    msg = msg[:240] + "..."
                header = f"⚠️ Mavely session check FAILED (status={status}).\n{msg}\n\n"
                if autologin_attempted:
                    if self._mavely_last_autologin_ok:
                        header = "⚠️ Mavely session check FAILED, but auto-login ran.\n(Preflight still failing; manual login may be required.)\n\n" + header
                    else:
                        header = "⚠️ Mavely session check FAILED and headless auto-login did NOT recover.\nManual login required.\n\n" + header

                if start_err:
                    body = (
                        f"noVNC auto-start failed: {str(start_err)[:300]}\n\n"
                        "To try headless Playwright auto-login now, run `!rsmavelyautologin`.\n"
                        "To start manual login via noVNC, run `!rsmavelylogin`."
                    )
                else:
                    web_port = int((info or {}).get("web_port") or 6080)
                    url_path = str((info or {}).get("url_path") or "/vnc.html")
                    body = (
                        "Optional (fast): try headless Playwright auto-login now with `!rsmavelyautologin`.\n"
                        "Auto-login can be enabled for future failures by setting `mavely_autologin_on_fail=true` in RSForwarder/config.json.\n\n"
                        + self._build_tunnel_instructions(web_port, url_path)
                    )
                    if pid:
                        body += f"\n\nCookie refresher PID: `{pid}`"
                    if log_path:
                        body += f"\nRefresher log (server path): `{log_path}`"

                # DM only if targets exist; automation still runs regardless.
                if targets:
                    for uid in targets:
                        await self._dm_user(uid, header + body)
                    self._mavely_last_alert_ts = now
                    self._mavely_write_status()
                else:
                    # No DM recipients configured; keep a clear log trail.
                    self._mavely_append_log("preflight still failing and no alert recipients are configured (run !rsmavelyalertme)")
            except Exception:
                pass

            await asyncio.sleep(interval)
    
    def load_config(self):
        """Load configuration from config.json + config.secrets.json (server-only)."""
        if self.config_path.exists():
            try:
                self.config, _, secrets_path = load_config_with_secrets(Path(__file__).parent)
                if not secrets_path.exists():
                    print(f"{Colors.YELLOW}[Config] Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
                print(f"{Colors.GREEN}[Config] Loaded configuration{Colors.RESET}")
                
                # Load saved icon URL from config
                saved_icon_url = self.config.get("rs_server_icon_url", "")
                if saved_icon_url:
                    self.rs_icon_url = saved_icon_url
                    print(f"{Colors.GREEN}[Config] Loaded RS Server icon from config: {saved_icon_url[:50]}...{Colors.RESET}")

                # Apply destination webhook URLs from secrets mapping (server-only)
                webhooks = self.config.get("destination_webhooks", {}) or {}
                if not isinstance(webhooks, dict):
                    webhooks = {}
                missing_hooks = []
                for ch in self.config.get("channels", []) or []:
                    src_id = str(ch.get("source_channel_id", "")).strip()
                    if not src_id:
                        continue
                    # In-place repost channels do not forward to an external webhook, so they
                    # do not require destination_webhooks entries in config.secrets.json.
                    if bool((ch or {}).get("repost_in_place")):
                        continue
                    hook = webhooks.get(src_id)
                    if not hook:
                        missing_hooks.append(src_id)
                        continue
                    ch["destination_webhook_url"] = hook
                if missing_hooks:
                    print(f"{Colors.RED}[Config] ERROR: Missing destination webhook(s) for source_channel_id(s): {', '.join(missing_hooks[:5])}{Colors.RESET}")
                    print(f"{Colors.RED}[Config] Add them to config.secrets.json under destination_webhooks{{...}}{Colors.RESET}")
                    sys.exit(1)

                # Refresh optional subsystems that depend on config
                try:
                    if hasattr(self, "_rs_fs_sheet") and self._rs_fs_sheet:
                        self._rs_fs_sheet.refresh_config(self.config)
                except Exception:
                    pass
            except Exception as e:
                print(f"{Colors.RED}[Config] Failed to load config: {e}{Colors.RESET}")
                self.config = {}
        else:
            # Create empty config structure
            self.config = {
                "guild_id": 0,
                "brand_name": "Reselling Secrets",
                "forwarding_logs_channel_id": "",
                "rs_server_icon_url": "",
                "channels": []
            }
            self.save_config()
            print(f"{Colors.YELLOW}[Config] Created empty config.json - please configure it{Colors.RESET}")
            print(f"{Colors.YELLOW}[Config] Required fields: guild_id, brand_name (secrets live in config.secrets.json){Colors.RESET}")

    def _is_local_exec(self) -> bool:
        """Return True when RSForwarder runs on the Ubuntu host and can manage services locally."""
        try:
            if os.name == "nt":
                return False
            return _REPO_ROOT.is_dir()
        except Exception:
            return False

    def _run_local_botctl(self, action: str) -> Tuple[bool, str]:
        """Run botctl.sh locally on Ubuntu to manage RSAdminBot."""
        botctl = _REPO_ROOT / "RSAdminBot" / "botctl.sh"
        if not botctl.exists():
            return False, f"botctl.sh not found at: {botctl}"
        cmd = f"bash {shlex.quote(str(botctl))} {shlex.quote(action)} rsadminbot"
        try:
            r = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace")
            out = (r.stdout or r.stderr or "").strip()
            return r.returncode == 0, out
        except Exception as e:
            return False, str(e)
    
    def save_config(self):
        """Save configuration to JSON file"""
        try:
            # Never write secrets back into config.json
            config_to_save = dict(self.config or {})
            config_to_save.pop("bot_token", None)
            config_to_save.pop("destination_webhooks", None)
            # Strip any other secrets that were merged in from config.secrets.json.
            # This prevents accidental leakage of tokens/cookies into the synced config.json.
            try:
                secrets_path = self._secrets_path()
                if secrets_path.exists():
                    secrets_obj = json.loads(secrets_path.read_text(encoding="utf-8", errors="replace") or "{}")
                    if isinstance(secrets_obj, dict):
                        for k in list(secrets_obj.keys()):
                            config_to_save.pop(k, None)
            except Exception:
                pass
            # Also strip webhook URLs from channel configs (these live in config.secrets.json)
            try:
                channels = config_to_save.get("channels")
                if isinstance(channels, list):
                    for ch in channels:
                        if isinstance(ch, dict):
                            ch.pop("destination_webhook_url", None)
            except Exception:
                pass
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_to_save, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to save config: {e}{Colors.RESET}")

    def _secrets_path(self) -> Path:
        """Return the server-only secrets file path for this bot."""
        return Path(__file__).parent / "config.secrets.json"

    def _load_secrets_dict(self) -> Dict[str, Any]:
        p = self._secrets_path()
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
        except Exception:
            return {}

    def _save_secrets_dict(self, d: Dict[str, Any]) -> bool:
        p = self._secrets_path()
        try:
            p.write_text(json.dumps(d, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            return True
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to save secrets: {e}{Colors.RESET}")
            return False

    def _set_destination_webhook_secret(self, source_channel_id: str, webhook_url: str) -> bool:
        """Persist destination webhook mapping into config.secrets.json (server-only)."""
        src = str(source_channel_id or "").strip()
        url = str(webhook_url or "").strip()
        if not src or not url:
            return False
        secrets = self._load_secrets_dict()
        wh = secrets.get("destination_webhooks")
        if not isinstance(wh, dict):
            wh = {}
        wh[src] = url
        secrets["destination_webhooks"] = wh
        return self._save_secrets_dict(secrets)

    def _delete_destination_webhook_secret(self, source_channel_id: str) -> bool:
        src = str(source_channel_id or "").strip()
        if not src:
            return False
        secrets = self._load_secrets_dict()
        wh = secrets.get("destination_webhooks")
        if not isinstance(wh, dict):
            return True
        if src in wh:
            wh.pop(src, None)
            secrets["destination_webhooks"] = wh
            return self._save_secrets_dict(secrets)
        return True
    
    def get_channel_config(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a source channel"""
        for channel in self.config.get("channels", []):
            if str(channel.get("source_channel_id")) == str(channel_id):
                return channel
        return None

    def _zephyr_release_feed_channel_id(self) -> Optional[int]:
        try:
            raw = str((self.config or {}).get("zephyr_release_feed_channel_id") or "").strip()
            if not raw:
                raw = str(os.getenv("ZEPHYR_RELEASE_FEED_CHANNEL_ID", "") or "").strip()
            if not raw:
                return None
            return int(raw)
        except Exception:
            return None

    async def _resolve_channel_by_id(self, channel_id: int):
        """
        Resolve a channel object even if not cached.
        """
        try:
            ch = self.bot.get_channel(int(channel_id))
            if ch is not None:
                return ch
        except Exception:
            ch = None
        try:
            return await self.bot.fetch_channel(int(channel_id))
        except Exception:
            return None

    def _extract_channel_id_from_ref(self, channel_ref: str) -> Optional[int]:
        """
        Parse a channel reference into an ID.

        Accepts:
        - "<#123...>" channel mention
        - "123..." raw channel ID
        """
        s = str(channel_ref or "").strip()
        if not s:
            return None
        try:
            if s.startswith("<#") and s.endswith(">"):
                s = s[2:-1].strip()
        except Exception:
            pass
        try:
            return int(s) if s.isdigit() else None
        except Exception:
            return None

    def _get_destination_channel_webhook_url(self, destination_channel_id: int) -> str:
        """
        Cache: destination channel id -> webhook url (stored server-side in config.secrets.json).
        Used by the rsadd mapper so users never paste webhook URLs.
        """
        try:
            secrets = self._load_secrets_dict()
            d = secrets.get("destination_channel_webhooks")
            if not isinstance(d, dict):
                return ""
            return str(d.get(str(int(destination_channel_id))) or "").strip()
        except Exception:
            return ""

    def _set_destination_channel_webhook_url(self, destination_channel_id: int, webhook_url: str) -> bool:
        try:
            dest_id = str(int(destination_channel_id))
            url = str(webhook_url or "").strip()
            if not (dest_id and url):
                return False
            secrets = self._load_secrets_dict()
            d = secrets.get("destination_channel_webhooks")
            if not isinstance(d, dict):
                d = {}
            d[dest_id] = url
            secrets["destination_channel_webhooks"] = d
            return self._save_secrets_dict(secrets)
        except Exception:
            return False

    async def _get_or_create_destination_webhook_url(self, destination_channel: discord.TextChannel) -> Tuple[bool, str, str]:
        """
        Return (ok, message, webhook_url) for a destination channel.
        Requires Manage Webhooks permission in the destination channel.
        """
        try:
            dest_id = int(getattr(destination_channel, "id", 0) or 0)
        except Exception:
            dest_id = 0
        if not dest_id:
            return False, "Invalid destination channel.", ""

        cached = self._get_destination_channel_webhook_url(dest_id)
        if cached:
            return True, "ok", cached

        # Try to reuse an existing webhook created by this bot.
        try:
            hooks = await destination_channel.webhooks()
        except Exception:
            hooks = []

        try:
            bot_user = getattr(self.bot, "user", None)
            bot_id = int(getattr(bot_user, "id", 0) or 0)
        except Exception:
            bot_id = 0

        for h in hooks or []:
            try:
                creator_id = int(getattr(getattr(h, "user", None), "id", 0) or 0)
                if bot_id and creator_id and creator_id != bot_id:
                    continue
                url = str(getattr(h, "url", "") or "").strip()
                if url:
                    self._set_destination_channel_webhook_url(dest_id, url)
                    return True, "ok", url
            except Exception:
                continue

        # Create a new webhook.
        try:
            wh = await destination_channel.create_webhook(
                name="RSForwarder",
                reason="RSForwarder mapping (auto-created destination webhook)",
            )
            url = str(getattr(wh, "url", "") or "").strip()
            if not url:
                return False, "Webhook created but URL was not available.", ""
            self._set_destination_channel_webhook_url(dest_id, url)
            return True, "ok", url
        except Exception as e:
            return False, f"Failed to create webhook in destination channel (need Manage Webhooks): {str(e)[:180]}", ""

    async def _rsadd_apply(
        self,
        *,
        source_channel_id: int,
        destination_webhook_url: str,
        role_id: str = "",
        text: str = "",
    ) -> Tuple[bool, str, Optional[discord.Embed]]:
        """
        Canonical implementation for adding a forwarding job.
        Used by both:
        - manual `!rsadd <channel> <webhook_url> ...`
        - interactive mapper `!rsadd` (auto webhook)
        """
        try:
            src_id = int(source_channel_id or 0)
        except Exception:
            src_id = 0
        if src_id <= 0:
            return False, "Invalid source channel id.", None

        wh_url = str(destination_webhook_url or "").strip()
        if not wh_url.startswith("https://discord.com/api/webhooks/"):
            return False, "Invalid webhook URL format.", None

        src_key = str(src_id)
        if self.get_channel_config(src_key):
            return False, f"Source channel `{src_key}` is already configured (use `!rsupdate` / `!rsremove`).", None

        ch_obj = await self._resolve_channel_by_id(int(src_id))
        ch_name = str(getattr(ch_obj, "name", "") or f"channel-{src_id}") if ch_obj else f"channel-{src_id}"

        rid = str(role_id or "").strip()
        rtext = str(text or "").strip()
        if rid:
            try:
                int(rid)
            except Exception:
                return False, "Invalid role_id (must be numeric).", None

        if not self._set_destination_webhook_secret(src_key, wh_url):
            return False, "Failed to write webhook into config.secrets.json on the server.", None

        new_channel = {
            "source_channel_id": src_key,
            "source_channel_name": ch_name,
            "role_mention": {"role_id": rid, "text": rtext},
        }
        if "channels" not in self.config:
            self.config["channels"] = []
        self.config["channels"].append(new_channel)
        self.save_config()
        self.load_config()

        emb = discord.Embed(
            title="✅ New Forwarding Job Added",
            color=discord.Color.green(),
            description="Forwarding is now enabled for this source channel.",
        )
        emb.add_field(name="📥 Source", value=f"`{ch_name}`\nID: `{src_key}`", inline=True)
        emb.add_field(name="📤 Destination", value="Webhook configured (server-only secrets)", inline=True)
        if rid:
            emb.add_field(name="📢 Role Mention", value=f"<@&{rid}> {rtext}", inline=False)
        emb.set_footer(text="Use !rslist / !rsview / !rsupdate / !rsremove")
        return True, "ok", emb

    def _rsremove_apply(self, *, source_channel_id: int) -> Tuple[bool, str]:
        try:
            src_id = int(source_channel_id or 0)
        except Exception:
            src_id = 0
        if src_id <= 0:
            return False, "Invalid source channel id."

        channels = (self.config or {}).get("channels")
        if not isinstance(channels, list):
            return False, "No channels configured."

        src_key = str(src_id)
        before = len(channels)
        channels2 = [c for c in channels if str((c or {}).get("source_channel_id") or "").strip() != src_key]
        if len(channels2) == before:
            return False, "That source channel is not configured."

        self.config["channels"] = channels2
        self.save_config()
        self.load_config()
        self._delete_destination_webhook_secret(src_key)
        return True, "ok"

    def _format_progress_bar(self, done: int, total: int, *, width: int = 18) -> str:
        try:
            t = max(1, int(total))
            d = max(0, min(int(done), t))
            w = max(8, min(int(width), 30))
            filled = int(round((d / t) * w))
            filled = max(0, min(filled, w))
            bar = ("█" * filled) + ("░" * (w - filled))
            pct = int(round((d / t) * 100))
            return f"[{bar}] {d}/{t} ({pct}%)"
        except Exception:
            return f"{done}/{total}"

    def _rs_fs_monitor_lookup_enabled(self) -> bool:
        try:
            v = (self.config or {}).get("rs_fs_monitor_lookup_enabled")
            if v is None:
                return True
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            return True

    def _rs_fs_monitor_history_limit(self) -> int:
        try:
            v = int((self.config or {}).get("rs_fs_monitor_lookup_history_limit") or 120)
        except Exception:
            v = 120
        return max(20, min(v, 500))

    def _monitor_channel_name_for_store(self, store: str) -> Optional[str]:
        s = (store or "").strip().lower()
        mapping = {
            "amazon": "amazon-monitor",
            "walmart": "walmart-monitor",
            "target": "target-monitor",
            "lowes": "lowes-monitor",
            "gamestop": "gamestop-monitor",
            "costco": "costco-monitor",
            "bestbuy": "bestbuy-monitor",
            "homedepot": "homedepot-monitor",
            "topps": "topps-monitor",
            "funko": "funkopop-monitor",
            "funkopop": "funkopop-monitor",
        }
        for key, name in mapping.items():
            if key in s:
                return name
        return None

    @staticmethod
    def _normalize_monitor_channel_name(name: str) -> str:
        """
        Normalize monitor channel names so we can match:
          "🤖┃walmart-monitor" -> "walmart-monitor"
          "walmart-monitor"    -> "walmart-monitor"
        """
        import re

        s = (name or "").strip().lower()
        if not s:
            return ""
        s = s.replace("┃", "|").replace("│", "|").replace("丨", "|")
        parts = [p.strip() for p in re.split(r"[|]+", s) if p.strip()]
        if parts:
            s = parts[-1]
        s = re.sub(r"^[^a-z0-9]+", "", s)
        return s

    def _rs_fs_monitor_channel_ids(self) -> Dict[str, int]:
        """
        Config mapping of monitor channel base-name -> channel_id.
        Example:
          {
            "walmart-monitor": 1411756672891748422,
            "costco-monitor": 1411757054908960819
          }
        """
        raw = (self.config or {}).get("rs_fs_monitor_channel_ids")
        if not isinstance(raw, dict):
            return {}
        out: Dict[str, int] = {}
        for k, v in raw.items():
            key = self._normalize_monitor_channel_name(str(k or ""))
            if not key:
                continue
            try:
                out[key] = int(v)
            except Exception:
                continue
        return out

    def _rs_fs_manual_overrides_path(self) -> Path:
        # Runtime JSON (server-side). Not intended to be committed.
        return (Path(__file__).resolve().parent / "rs_fs_manual_overrides.json").resolve()

    @staticmethod
    def _rs_fs_override_key(store: str, sku: str) -> str:
        return f"{(store or '').strip().lower()}|{(sku or '').strip().lower()}"

    def _load_rs_fs_manual_overrides(self) -> Dict[str, Dict[str, str]]:
        """
        Load manual overrides: key "store|sku" -> {"url": "...", "title": "..."}.
        """
        try:
            p = self._rs_fs_manual_overrides_path()
            if not p.exists():
                return {}
            obj = json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
            if not isinstance(obj, dict):
                return {}
            out: Dict[str, Dict[str, str]] = {}
            for k, v in obj.items():
                if not isinstance(k, str):
                    continue
                if not isinstance(v, dict):
                    continue
                url = str(v.get("url") or "").strip()
                title = str(v.get("title") or "").strip()
                if not url:
                    continue
                out[str(k).strip().lower()] = {"url": url, "title": title}
            return out
        except Exception:
            return {}

    def _save_rs_fs_manual_overrides(self, overrides: Dict[str, Dict[str, str]]) -> bool:
        try:
            p = self._rs_fs_manual_overrides_path()
            tmp = dict(overrides or {})
            p.write_text(json.dumps(tmp, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        except Exception:
            return False

    @staticmethod
    def _preferred_store_domains(store: str) -> List[str]:
        s = (store or "").strip().lower()
        if "walmart" in s:
            return ["walmart.com"]
        if "costco" in s:
            return ["costco.com"]
        if "target" in s:
            return ["target.com"]
        if "gamestop" in s:
            return ["gamestop.com"]
        if "bestbuy" in s or "best buy" in s:
            return ["bestbuy.com"]
        if "homedepot" in s or "home depot" in s:
            return ["homedepot.com"]
        if "amazon" in s:
            return ["amazon."]
        if "topps" in s:
            return ["topps.com"]
        return []

    async def _resolve_monitor_channel_for_store(
        self,
        store: str,
        *,
        guild: Optional[discord.Guild] = None,
    ) -> Optional[discord.TextChannel]:
        """
        Resolve a monitor channel for this store.
        Preference:
          1) explicit channel_id mapping in config (rs_fs_monitor_channel_ids)
          2) name-based resolution fallback
        """
        ch_name = self._monitor_channel_name_for_store(store)
        if not ch_name:
            return None
        base = self._normalize_monitor_channel_name(ch_name)
        cid = (self._rs_fs_monitor_channel_ids() or {}).get(base)
        if cid:
            ch = await self._resolve_channel_by_id(int(cid))
            if isinstance(ch, discord.TextChannel):
                return ch
        # Fallback: name match (handles emoji prefixes too)
        return await self._resolve_text_channel_by_name(ch_name, guild=guild)

    async def _resolve_text_channel_by_name(self, name: str, *, guild: Optional[discord.Guild] = None) -> Optional[discord.TextChannel]:
        """
        Resolve a text channel by name.

        Priority:
        1) provided guild (usually the Zephyr/monitor guild)
        2) configured guild_id (legacy)
        3) any guild the bot is in
        """
        target = (name or "").strip().lower()
        if not target:
            return None

        def _find_in_g(g: Optional[discord.Guild]) -> Optional[discord.TextChannel]:
            if not g:
                return None
            try:
                for ch in getattr(g, "text_channels", []) or []:
                    raw = str(getattr(ch, "name", "") or "").strip().lower()
                    if not raw:
                        continue
                    if raw == target:
                        return ch
                    nrm = self._normalize_monitor_channel_name(raw)
                    if nrm == target or nrm.endswith(target):
                        return ch
            except Exception:
                return None
            return None

        # 1) Explicit guild (best)
        hit = _find_in_g(guild)
        if hit:
            return hit

        # 2) Configured guild_id
        try:
            guild_id = int((self.config or {}).get("guild_id") or 0)
        except Exception:
            guild_id = 0
        if guild_id:
            hit = _find_in_g(self.bot.get_guild(guild_id))
            if hit:
                return hit

        # 3) Any guild
        try:
            for g in list(getattr(self.bot, "guilds", []) or [])[:50]:
                hit = _find_in_g(g)
                if hit:
                    return hit
        except Exception:
            pass
        return None

    @staticmethod
    def _clean_sku_text(value: str) -> str:
        s = (value or "").strip().strip("`").strip()
        # keep alnum only, lower
        out = "".join([c for c in s if c.isalnum() or c in {"-", "_"}]).strip().lower()
        return out

    @staticmethod
    def _first_url_in_text(text: str) -> str:
        t = (text or "").strip()
        if not t:
            return ""
        import re

        m = re.search(r"(https?://[^\s<>()]+)", t)
        return (m.group(1) or "").strip() if m else ""

    async def _monitor_lookup_for_store(
        self,
        store: str,
        sku: str,
        *,
        guild: Optional[discord.Guild] = None,
    ) -> Optional[rs_fs_sheet_sync.RsFsPreviewEntry]:
        """
        Try to find a matching monitor embed for (store, sku) and extract title + url from that embed.
        Returns None if not found / not accessible.
        """
        if not self._rs_fs_monitor_lookup_enabled():
            return None
        ch_name = self._monitor_channel_name_for_store(store)
        if not ch_name:
            return None
        ch = await self._resolve_monitor_channel_for_store(store, guild=guild)
        if not ch:
            try:
                if bool((self.config or {}).get("rs_fs_monitor_debug")):
                    print(f"{Colors.YELLOW}[RS-FS Monitor]{Colors.RESET} channel not found for store={store!r} wanted_name={ch_name!r}")
            except Exception:
                pass
            return None

        target = self._clean_sku_text(sku)
        if not target:
            return None

        target_digits = "".join([c for c in target if c.isdigit()])

        def _looks_like_id_value(cleaned: str) -> bool:
            # Heuristic: IDs are usually fairly long, or contain letters (ASIN / BestBuy sku).
            if not cleaned:
                return False
            if any(c.isalpha() for c in cleaned):
                return len(cleaned) >= 6
            # numeric
            return len(cleaned) >= 6

        def _id_like_field_name(name: str) -> bool:
            n = (name or "").strip().lower()
            if not n:
                return False
            hints = (
                "sku",
                "pid",
                "tcin",
                "asin",
                "upc",
                "item",
                "product",
                "product id",
                "productid",
                "model",
                "mpn",
                "id",
            )
            return any(h in n for h in hints)

        def _value_matches_target(raw_val: str) -> bool:
            val_clean = self._clean_sku_text(raw_val or "")
            if not val_clean:
                return False
            if val_clean == target:
                return True
            # Numeric-only match (handles formatting/commas/spaces)
            if target_digits:
                val_digits = "".join([c for c in val_clean if c.isdigit()])
                if val_digits and val_digits == target_digits and len(target_digits) >= 6:
                    return True
            return False

        limit = self._rs_fs_monitor_history_limit()
        debug_enabled = False
        try:
            debug_enabled = bool((self.config or {}).get("rs_fs_monitor_debug"))
        except Exception:
            debug_enabled = False
        scanned_msgs = 0
        scanned_embeds = 0
        try:
            async for m in ch.history(limit=limit):
                scanned_msgs += 1
                embeds = getattr(m, "embeds", None) or []
                for e in embeds:
                    scanned_embeds += 1
                    fields = getattr(e, "fields", None) or []

                    def _extract_title_url() -> Tuple[str, str]:
                        title = str(getattr(e, "title", "") or "").strip()
                        urls: List[str] = []
                        u0 = str(getattr(e, "url", "") or "").strip()
                        if u0:
                            urls.append(u0)
                        # Pull URLs from any field values
                        for f2 in fields:
                            vv = str(getattr(f2, "value", "") or "")
                            uu = self._first_url_in_text(vv)
                            if uu:
                                urls.append(uu)
                        dd = str(getattr(e, "description", "") or "")
                        uu2 = self._first_url_in_text(dd)
                        if uu2:
                            urls.append(uu2)

                        # Prefer store-domain URLs when possible
                        preferred = self._preferred_store_domains(store)
                        url = ""
                        for u in urls:
                            try:
                                host = (urlparse(u).netloc or "").lower()
                            except Exception:
                                host = ""
                            if any(d in host for d in preferred):
                                url = u
                                break
                        if not url:
                            url = (urls[0] if urls else "").strip()
                        if not title:
                            title = url or ""
                        return title, url

                    # Pass 1: ID-like fields (SKU/PID/TCIN/ASIN/UPC/etc)
                    for f in fields:
                        name = str(getattr(f, "name", "") or "").strip()
                        val = str(getattr(f, "value", "") or "").strip()
                        if not (name and val):
                            continue
                        if not _id_like_field_name(name):
                            continue
                        if _value_matches_target(val):
                            title, url = _extract_title_url()
                            if debug_enabled:
                                try:
                                    g_id = int(getattr(getattr(ch, "guild", None), "id", 0) or 0)
                                    jump = (
                                        f"https://discord.com/channels/{g_id}/{int(getattr(ch,'id',0) or 0)}/{int(getattr(m,'id',0) or 0)}"
                                        if g_id and int(getattr(ch,'id',0) or 0) and int(getattr(m,'id',0) or 0)
                                        else ""
                                    )
                                    print(
                                        f"{Colors.CYAN}[RS-FS Monitor]{Colors.RESET} HIT store={store} sku={sku} "
                                        f"channel={getattr(ch,'name',ch_name)} method=field name={name!r} scanned_msgs={scanned_msgs} scanned_embeds={scanned_embeds}"
                                        + (f" jump={jump}" if jump else "")
                                    )
                                except Exception:
                                    pass
                            return rs_fs_sheet_sync.RsFsPreviewEntry(
                                store=store,
                                sku=sku,
                                url=url,
                                title=title,
                                error="" if url else "no url in monitor embed",
                                source=f"monitor:{ch_name}",
                                monitor_url=url,
                                affiliate_url="",
                            )

                    # Pass 2: exact match anywhere in values (avoid false positives on short numbers)
                    for f in fields:
                        val = str(getattr(f, "value", "") or "").strip()
                        if not val:
                            continue
                        val_clean = self._clean_sku_text(val)
                        if not _looks_like_id_value(val_clean):
                            continue
                        if _value_matches_target(val):
                            title, url = _extract_title_url()
                            if debug_enabled:
                                try:
                                    g_id = int(getattr(getattr(ch, "guild", None), "id", 0) or 0)
                                    jump = (
                                        f"https://discord.com/channels/{g_id}/{int(getattr(ch,'id',0) or 0)}/{int(getattr(m,'id',0) or 0)}"
                                        if g_id and int(getattr(ch,'id',0) or 0) and int(getattr(m,'id',0) or 0)
                                        else ""
                                    )
                                    print(
                                        f"{Colors.CYAN}[RS-FS Monitor]{Colors.RESET} HIT store={store} sku={sku} "
                                        f"channel={getattr(ch,'name',ch_name)} method=any_value scanned_msgs={scanned_msgs} scanned_embeds={scanned_embeds}"
                                        + (f" jump={jump}" if jump else "")
                                    )
                                except Exception:
                                    pass
                            return rs_fs_sheet_sync.RsFsPreviewEntry(
                                store=store,
                                sku=sku,
                                url=url,
                                title=title,
                                error="" if url else "no url in monitor embed",
                                source=f"monitor:{ch_name}",
                                monitor_url=url,
                                affiliate_url="",
                            )

                    # Fallback: raw text match anywhere in embed
                    blob = " ".join(
                        [
                            str(getattr(e, "title", "") or ""),
                            str(getattr(e, "description", "") or ""),
                            " ".join([str(getattr(f, "name", "") or "") + " " + str(getattr(f, "value", "") or "") for f in fields]),
                        ]
                    )
                    if target and target in self._clean_sku_text(blob):
                        title = str(getattr(e, "title", "") or "").strip()
                        url = str(getattr(e, "url", "") or "").strip()
                        if not url:
                            url = self._first_url_in_text(blob)
                        if not title:
                            title = url or ""
                        if debug_enabled:
                            try:
                                g_id = int(getattr(getattr(ch, "guild", None), "id", 0) or 0)
                                jump = (
                                    f"https://discord.com/channels/{g_id}/{int(getattr(ch,'id',0) or 0)}/{int(getattr(m,'id',0) or 0)}"
                                    if g_id and int(getattr(ch,'id',0) or 0) and int(getattr(m,'id',0) or 0)
                                    else ""
                                )
                                print(
                                    f"{Colors.CYAN}[RS-FS Monitor]{Colors.RESET} HIT store={store} sku={sku} "
                                    f"channel={getattr(ch,'name',ch_name)} method=blob scanned_msgs={scanned_msgs} scanned_embeds={scanned_embeds}"
                                    + (f" jump={jump}" if jump else "")
                                )
                            except Exception:
                                pass
                        return rs_fs_sheet_sync.RsFsPreviewEntry(
                            store=store,
                            sku=sku,
                            url=url,
                            title=title,
                            error="" if url else "no url in monitor embed",
                            source=f"monitor:{ch_name}",
                            monitor_url=url,
                            affiliate_url="",
                        )
        except Exception:
            return None
        if debug_enabled:
            try:
                print(
                    f"{Colors.YELLOW}[RS-FS Monitor]{Colors.RESET} MISS store={store} sku={sku} "
                    f"channel={getattr(ch,'name',ch_name)} scanned_msgs={scanned_msgs} scanned_embeds={scanned_embeds} limit={limit}"
                )
            except Exception:
                pass
        return None

    def _collect_embed_text(self, message: discord.Message) -> str:
        parts: List[str] = []
        try:
            # Some bots put all content in message.content (not in embeds).
            c = getattr(message, "content", None)
            if isinstance(c, str) and c.strip():
                parts.append(c.strip())
        except Exception:
            pass
        try:
            for e in (message.embeds or []):
                try:
                    t = getattr(e, "title", None)
                    if isinstance(t, str) and t.strip():
                        parts.append(t.strip())
                except Exception:
                    pass
                try:
                    a = getattr(e, "author", None)
                    an = getattr(a, "name", None) if a is not None else None
                    if isinstance(an, str) and an.strip():
                        parts.append(an.strip())
                except Exception:
                    pass
                try:
                    d = getattr(e, "description", None)
                    if isinstance(d, str) and d.strip():
                        parts.append(d.strip())
                except Exception:
                    pass
                try:
                    fields = getattr(e, "fields", None) or []
                    for f in fields:
                        try:
                            n = getattr(f, "name", None)
                            v = getattr(f, "value", None)
                        except Exception:
                            n, v = None, None
                        if isinstance(n, str) and n.strip():
                            parts.append(n.strip())
                        if isinstance(v, str) and v.strip():
                            parts.append(v.strip())
                except Exception:
                    pass
                try:
                    ft = getattr(getattr(e, "footer", None), "text", None)
                    if isinstance(ft, str) and ft.strip():
                        parts.append(ft.strip())
                except Exception:
                    pass
        except Exception:
            return ""
        return "\n".join(parts).strip()

    async def _maybe_sync_rs_fs_sheet_from_message(self, message: discord.Message) -> None:
        """
        Auto handler for Zephyr /listreleases output.\n
        Behavior:\n
        - Always rebuild the latest merged run (chunks + tag-only continuation messages)\n
        - Write ONLY the Current List tab (mirror)\n
        - Post the RS-FS Check card\n
        - Never touch the public list tab unless a user runs `!rsfsrun`\n
        \n
        In dry-run/test mode (`!rsfstest`), it keeps the old preview behavior.\n
        """
        try:
            if not getattr(self, "_rs_fs_sheet", None):
                return
            # Allow "dry-run" / preview mode without Google Sheets credentials.
            try:
                test_enabled = bool((self.config or {}).get("rs_fs_sheet_test_output_enabled"))
            except Exception:
                test_enabled = False
            sheet_enabled = bool(self._rs_fs_sheet.enabled())
            if not (sheet_enabled or test_enabled):
                return

            target_ch = self._zephyr_release_feed_channel_id()
            if not target_ch:
                return
            if int(getattr(message.channel, "id", 0) or 0) != int(target_ch):
                return

            # If a manual rsfsrun is in progress, do not process live Zephyr messages here.
            # Manual runs build a single merged list and sync once (prevents add/remove thrash).
            if bool(getattr(self, "_rs_fs_manual_run_in_progress", False)):
                return

            # Debug: confirm we are seeing messages in the target channel.
            try:
                embeds_n = len(message.embeds or [])
                a = getattr(message, "author", None)
                aid = getattr(a, "id", None)
                aname = getattr(a, "name", None)
                print(f"{Colors.CYAN}[RS-FS Sheet]{Colors.RESET} Seen msg in zephyr channel id={target_ch} msg_id={getattr(message,'id',None)} author={aname}({aid}) embeds={embeds_n}")
            except Exception:
                pass

            mid = int(getattr(message, "id", 0) or 0)
            if mid and mid in (self._rs_fs_seen_message_ids or set()):
                return

            text = self._collect_embed_text(message)
            try:
                short = (text or "").replace("\n", " ").strip()
                if len(short) > 220:
                    short = short[:220] + "..."
                print(f"{Colors.CYAN}[RS-FS Sheet]{Colors.RESET} Collected_text_len={len(text or '')} sample={short!r}")
            except Exception:
                pass

            if not text:
                try:
                    print(f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Skip: empty collected text (no content/embeds)")
                except Exception:
                    pass
                return

            # Don't try to parse user command messages (prevents noisy "parsed 0 items" when someone runs !rsfstest).
            try:
                if isinstance(getattr(message, "content", None), str):
                    c0 = (message.content or "").strip()
                    if c0.startswith("!"):
                        return
            except Exception:
                pass

            async def _maybe_post_auto_check() -> None:
                """
                Post an RS-FS Check card automatically after /listreleases chunks arrive.
                Debounced to avoid multi-chunk spam.
                """
                try:
                    if not self._rsfs_auto_check_on_zephyr():
                        return
                    if not zephyr_release_feed_parser.looks_like_release_feed_embed_text(text or ""):
                        return
                    now = time.time()
                    if (now - float(getattr(self, "_rs_fs_last_auto_check_ts", 0.0) or 0.0)) < self._rsfs_auto_check_debounce_s():
                        return
                    self._rs_fs_last_auto_check_ts = now

                    out_id_raw = str((self.config or {}).get("rs_fs_sheet_status_channel_id") or "").strip()
                    out_id = int(out_id_raw) if out_id_raw else int(target_ch)
                    out_ch2 = await self._resolve_channel_by_id(int(out_id))
                    if not (out_ch2 and hasattr(out_ch2, "send")):
                        try:
                            print(
                                f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Auto check: cannot resolve sendable channel id={out_id}"
                            )
                        except Exception:
                            pass
                        return
                    emb = await self._build_rsfs_check_embed()
                    try:
                        run_lim = int((self.config or {}).get("rs_fs_sheet_max_per_run") or 250)
                    except Exception:
                        run_lim = 250
                    run_lim = max(10, min(run_lim, 500))
                    try:
                        print(
                            f"{Colors.CYAN}[RS-FS Sheet]{Colors.RESET} Auto check: attempting send to channel_id={out_id} (debounce={self._rsfs_auto_check_debounce_s():.0f}s)"
                        )
                    except Exception:
                        pass
                    try:
                        await out_ch2.send(
                            embed=emb,
                            view=_RsFsCheckView(self, owner_id=0, run_limit=run_lim),
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                        try:
                            print(f"{Colors.CYAN}[RS-FS Sheet]{Colors.RESET} Auto check: sent embed+buttons")
                        except Exception:
                            pass
                    except Exception as e:
                        # Fallback: still send buttons even if embed permissions are missing.
                        msg0 = (str(e) or "send failed").replace("\n", " ").strip()
                        if len(msg0) > 220:
                            msg0 = msg0[:220] + "..."
                        try:
                            print(f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Auto check: send failed: {msg0}")
                        except Exception:
                            pass
                        try:
                            await out_ch2.send(
                                content="RS-FS Check: (embed failed) use the buttons below or run `!rsfscheck`.",
                                view=_RsFsCheckView(self, owner_id=0, run_limit=run_lim),
                                allowed_mentions=discord.AllowedMentions.none(),
                            )
                            try:
                                print(f"{Colors.CYAN}[RS-FS Sheet]{Colors.RESET} Auto check: sent fallback text+buttons")
                            except Exception:
                                pass
                        except Exception:
                            return
                except Exception:
                    return

            # Always parse from the MOST RECENT merged listreleases text, not a single chunk.
            # Zephyr splits output into multiple messages/embeds and often places the `*-monitor`
            # tag on the next line. Parsing per-chunk can misclassify entries as "missing monitor tag".
            text_for_parse = text
            try:
                if zephyr_release_feed_parser.looks_like_release_feed_embed_text(text or ""):
                    merged_text, chunk_n, _found_header = await self._collect_latest_zephyr_release_feed_text(message.channel)  # type: ignore[arg-type]
                    if merged_text:
                        text_for_parse = merged_text
                        try:
                            print(
                                f"{Colors.CYAN}[RS-FS Sheet]{Colors.RESET} Using merged listreleases text (chunks={chunk_n}, len={len(merged_text)})"
                            )
                        except Exception:
                            pass
            except Exception:
                text_for_parse = text

            # Decide mode early.
            try:
                dry_run = bool((self.config or {}).get("rs_fs_sheet_dry_run"))
            except Exception:
                dry_run = False
            dry_run = bool(dry_run or test_enabled)

            # Normal mode: update Current List only + post check; no public-sheet writes.
            if not dry_run:
                try:
                    await self._rsfs_write_current_list(text_for_parse, reason="auto")
                except Exception:
                    pass
                await _maybe_post_auto_check()

                # Mark message processed and return (no more work).
                if mid:
                    try:
                        self._rs_fs_seen_message_ids.add(mid)
                        if len(self._rs_fs_seen_message_ids) > 2000:
                            self._rs_fs_seen_message_ids = set(list(self._rs_fs_seen_message_ids)[-1200:])
                    except Exception:
                        pass
                return

            # Dry-run path below uses SKU-only pairs for preview output.
            items = zephyr_release_feed_parser.parse_release_feed_items(text_for_parse)
            pairs = [(it.store, it.sku) for it in (items or [])]
            rid_by_key: Dict[str, int] = {}
            try:
                for it in (items or []):
                    try:
                        rid_by_key[self._rs_fs_override_key(getattr(it, "store", ""), getattr(it, "sku", ""))] = int(getattr(it, "release_id", 0) or 0)
                    except Exception:
                        continue
            except Exception:
                rid_by_key = {}

            # Post a visible check card early (even in dry-run, debounced).
            await _maybe_post_auto_check()

            if not pairs:
                try:
                    print(f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Skip: parsed 0 items from embed text")
                except Exception:
                    pass
                # If test mode is enabled, post a visible hint so the user isn't left waiting.
                try:
                    test_enabled = bool((self.config or {}).get("rs_fs_sheet_test_output_enabled"))
                except Exception:
                    test_enabled = False
                if test_enabled:
                    try:
                        try:
                            out_ch_raw = str((self.config or {}).get("rs_fs_sheet_test_output_channel_id") or "").strip()
                            out_ch_id2 = int(out_ch_raw) if out_ch_raw else int(target_ch)
                        except Exception:
                            out_ch_id2 = int(target_ch)
                        out_ch = await self._resolve_channel_by_id(int(out_ch_id2 or 0))
                        if out_ch and hasattr(out_ch, "send"):
                            await out_ch.send(
                                "RS-FS: ❌ Parsed 0 items from the Zephyr embed text (this chunk had no parseable store+sku pairs).",
                                allowed_mentions=discord.AllowedMentions.none(),
                            )
                    except Exception:
                        pass
                return

            try:
                limit = int((self.config or {}).get("rs_fs_sheet_test_limit") or 25)
            except Exception:
                limit = 25
            limit = max(1, min(limit, 200))

            # In real sheet mode, don't process everything at once.
            try:
                max_per_run = int((self.config or {}).get("rs_fs_sheet_max_per_run") or 250)
            except Exception:
                max_per_run = 250
            max_per_run = max(10, min(max_per_run, 1000))

            try:
                out_ch_raw = str((self.config or {}).get("rs_fs_sheet_test_output_channel_id") or "").strip()
                out_ch_id = int(out_ch_raw) if out_ch_raw else int(target_ch)
            except Exception:
                out_ch_id = int(target_ch)

            out_ch = None
            progress_msg = None
            progress_enabled = False
            progress_interval_s = 2.0
            try:
                progress_enabled = bool((self.config or {}).get("rs_fs_sheet_progress_enabled", True))
                progress_interval_s = float((self.config or {}).get("rs_fs_sheet_progress_update_s", 2.0) or 2.0)
            except Exception:
                progress_enabled = True
                progress_interval_s = 2.0
            progress_interval_s = max(1.0, min(progress_interval_s, 10.0))

            if dry_run:
                try:
                    out_ch = await self._resolve_channel_by_id(int(out_ch_id))
                except Exception:
                    out_ch = None
                if out_ch and hasattr(out_ch, "send"):
                    title = "RS-FS Preview (dry-run)" if sheet_enabled else "RS-FS Preview (no-sheet)"
                    await out_ch.send(
                        embed=_rsfs_embed(
                            title,
                            status="Running",
                            color=discord.Color.blurple(),
                            description="(No Google Sheet writes.)",
                            fields=[
                                ("Parsed", str(len(pairs)), True),
                                ("Will process", str(min(len(pairs), limit)), True),
                            ],
                            footer="RS-FS",
                        ),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    if progress_enabled:
                        try:
                            progress_msg = await out_ch.send(
                                embed=_rsfs_embed(
                                    "RS-FS Progress",
                                    status="Running",
                                    fields=[
                                        ("Stage", "resolve", True),
                                        ("Progress", self._format_progress_bar(0, min(len(pairs), limit)), False),
                                        ("Errors", "0", True),
                                    ],
                                    footer="RS-FS",
                                ),
                                allowed_mentions=discord.AllowedMentions.none(),
                            )
                        except Exception:
                            progress_msg = None
                else:
                    print(f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Cannot resolve output channel id={out_ch_id} (no send permission or not cached)")

            started = time.monotonic()
            last_update = 0.0

            async def _on_progress(done: int, total: int, errors: int, entry) -> None:
                nonlocal last_update
                if not progress_msg:
                    return
                now = time.monotonic()
                if done != total and (now - last_update) < progress_interval_s:
                    return
                last_update = now
                try:
                    last = ""
                    try:
                        last = f" | last: {getattr(entry, 'store', '')} {getattr(entry, 'sku', '')}".strip()
                    except Exception:
                        last = ""
                    await progress_msg.edit(
                        embed=_rsfs_embed(
                            "RS-FS Progress",
                            status="Running",
                            fields=[
                                ("Stage", "resolve", True),
                                ("Progress", self._format_progress_bar(done, total), False),
                                ("Errors", str(errors), True),
                                ("Last", last or "—", False),
                            ],
                            footer="RS-FS",
                        )
                    )
                except Exception:
                    return

            # Prefer items where we can build a URL (so small limits like 1 still show something useful).
            try:
                # If we're actually writing to the sheet, pre-dedupe first (saves work).
                if sheet_enabled and (not dry_run):
                    try:
                        pairs = await self._rs_fs_sheet.filter_new_pairs(list(pairs or []))
                    except Exception:
                        pass
                    if not pairs:
                        return

                # Pick items that have either:
                # - a buildable store URL, OR
                # - a known monitor channel mapping (so we can still fetch title+url from the monitor embed)
                candidates = []
                monitor_capable = 0
                for (st, sk) in (pairs or []):
                    try:
                        if rs_fs_sheet_sync.build_store_link(st, sk):
                            candidates.append((st, sk))
                            continue
                    except Exception:
                        pass
                    try:
                        if self._monitor_channel_name_for_store(st):
                            candidates.append((st, sk))
                            monitor_capable += 1
                    except Exception:
                        pass
                base = (candidates if candidates else (pairs or []))
                if dry_run:
                    chosen = base[:limit]
                else:
                    chosen = base[:max_per_run]

                if dry_run and out_ch and hasattr(out_ch, "send"):
                    try:
                        await out_ch.send(
                            f"RS-FS: parsed {len(pairs)} item(s); eligible_candidates={len(candidates)} (monitor_only={monitor_capable}); processing {len(chosen)}.",
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                    except Exception:
                        pass

                # Stage 1: try monitor-channel lookup (fast + accurate when monitor embeds exist).
                monitor_hits: List[rs_fs_sheet_sync.RsFsPreviewEntry] = []
                remaining: List[Tuple[str, str]] = []
                if self._rs_fs_monitor_lookup_enabled():
                    if dry_run and out_ch and hasattr(out_ch, "send"):
                        try:
                            await out_ch.send("RS-FS: Stage 1/2 monitor lookup…", allowed_mentions=discord.AllowedMentions.none())
                        except Exception:
                            pass
                    zephyr_guild = getattr(getattr(message, "channel", None), "guild", None)
                    for st, sk in chosen:
                        found = await self._monitor_lookup_for_store(st, sk, guild=zephyr_guild)
                        if found:
                            monitor_hits.append(found)
                        else:
                            remaining.append((st, sk))
                else:
                    remaining = list(chosen)

                # Stage 2: website fallback for anything not found in monitor channel.
                offset_done = len(monitor_hits)

                async def _on_progress2(done: int, total: int, errors: int, entry) -> None:
                    # Rebase progress onto overall total.
                    await _on_progress(offset_done + done, offset_done + total, errors, entry)

                if remaining:
                    web_entries = await rs_fs_sheet_sync.build_preview_entries(
                        remaining,
                        self.config,
                        on_progress=_on_progress2 if progress_msg else None,
                    )
                else:
                    web_entries = []

                raw_entries = list(monitor_hits) + list(web_entries)
            except Exception as e:
                if progress_msg:
                    try:
                        await progress_msg.edit(content=f"RS-FS progress: ❌ failed: {str(e)[:200]}")
                    except Exception:
                        pass
                raise

            entries = list(raw_entries or [])
            skipped_no_url = sum(1 for e in entries if not (getattr(e, "url", "") or "").strip())

            # Compute affiliate links for the resolved URL (column G), and persist the resolved/monitor URL (column H).
            # Use the canonical affiliate rewriter so behavior matches forwarding.
            try:
                rewrite_enabled = bool(self.config.get("affiliate_rewrite_enabled", True))
            except Exception:
                rewrite_enabled = True
            if entries:
                try:
                    enriched: List[rs_fs_sheet_sync.RsFsPreviewEntry] = []
                    url_list: List[str] = []
                    for e in entries:
                        u0 = (getattr(e, "monitor_url", "") or getattr(e, "url", "") or "").strip()
                        if u0:
                            url_list.append(u0)
                    aff_map: Dict[str, str] = {}
                    if rewrite_enabled and url_list:
                        # De-dupe while preserving order
                        seen_u: Set[str] = set()
                        unique_urls: List[str] = []
                        for u in url_list:
                            if u in seen_u:
                                continue
                            seen_u.add(u)
                            unique_urls.append(u)
                        mapped, _notes = await affiliate_rewriter.compute_affiliate_rewrites_plain(self.config, unique_urls)
                        aff_map = {str(k or "").strip(): str(v or "").strip() for k, v in (mapped or {}).items()}

                    for e in entries:
                        u0 = (getattr(e, "monitor_url", "") or getattr(e, "url", "") or "").strip()
                        prev_aff = str(getattr(e, "affiliate_url", "") or "").strip()
                        aff = (aff_map.get(u0) or "").strip() if u0 else ""
                        if not aff:
                            aff = prev_aff
                        enriched.append(
                            rs_fs_sheet_sync.RsFsPreviewEntry(
                                store=getattr(e, "store", "") or "",
                                sku=getattr(e, "sku", "") or "",
                                url=getattr(e, "url", "") or "",
                                title=getattr(e, "title", "") or "",
                                error=getattr(e, "error", "") or "",
                                source=getattr(e, "source", "") or "",
                                monitor_url=u0,
                                affiliate_url=aff,
                            )
                        )
                    entries = enriched
                except Exception:
                    # Never let affiliate computation break RS-FS flow
                    pass

            if dry_run:
                try:
                    out_ch = await self._resolve_channel_by_id(int(out_ch_id))
                except Exception:
                    out_ch = None
                if out_ch and hasattr(out_ch, "send"):
                    if progress_msg:
                        try:
                            total_show = min(len(pairs), limit)
                            elapsed = time.monotonic() - started
                            err_count = sum(1 for e in raw_entries if (e.error or "").strip())
                            await progress_msg.edit(
                                embed=_rsfs_embed(
                                    "RS-FS Progress",
                                    status="✅ done",
                                    color=discord.Color.green(),
                                    fields=[
                                        ("Progress", f"`{total_show}/{total_show}` in `{elapsed:.1f}s`", False),
                                        ("Errors", str(err_count), True),
                                    ],
                                    footer="RS-FS",
                                )
                            )
                        except Exception:
                            pass
                    await out_ch.send(
                        embed=_rsfs_embed(
                            "RS-FS Preview (results)",
                            status="Complete",
                            fields=[
                                ("Showing", str(len(entries)), True),
                                ("Skipped (no URL)", str(skipped_no_url), True),
                            ],
                            color=discord.Color.dark_teal(),
                            footer="RS-FS",
                        ),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )

                    chunk: List[rs_fs_sheet_sync.RsFsPreviewEntry] = []
                    for e in entries:
                        chunk.append(e)
                        if len(chunk) >= 20:
                            await self._send_rs_fs_preview_embed(out_ch, chunk)
                            chunk = []
                    if chunk:
                        await self._send_rs_fs_preview_embed(out_ch, chunk)
                else:
                    print(f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Cannot resolve output channel id={out_ch_id} for results send")

            ok, msg, added = True, "dry-run", 0
            if (not dry_run) and sheet_enabled:
                rows = [[e.store, e.sku, e.title, e.affiliate_url, e.monitor_url] for e in entries]
                ok, msg, added = await self._rs_fs_sheet.append_rows(rows)

                # Optional status output
                try:
                    status_ch_raw = str((self.config or {}).get("rs_fs_sheet_status_channel_id") or "").strip()
                    if status_ch_raw:
                        sch = await self._resolve_channel_by_id(int(status_ch_raw))
                        if sch and hasattr(sch, "send"):
                            if ok and added > 0:
                                sample_lines: List[str] = []
                                try:
                                    for e in (entries or [])[: min(10, len(entries or []))]:
                                        st2 = str(getattr(e, "store", "") or "").strip()
                                        sk2 = str(getattr(e, "sku", "") or "").strip()
                                        rid2 = int(rid_by_key.get(self._rs_fs_override_key(st2, sk2)) or 0)
                                        cmd = f"/removereleaseid release_id: {rid2}" if rid2 else "/removereleaseid release_id: ?"
                                        sample_lines.append(f"`{rid2}` `{st2}` `{sk2}`  {cmd}")
                                except Exception:
                                    sample_lines = []
                                try:
                                    await sch.send(
                                        embed=_rsfs_embed(
                                            "RS-FS Sheet (auto)",
                                            status=f"✅ added {added} row(s)",
                                            color=discord.Color.green(),
                                            fields=[
                                                ("Sample", "\n".join(sample_lines) if sample_lines else "—", False),
                                            ],
                                            footer="RS-FS • Auto append from /listreleases",
                                        ),
                                        allowed_mentions=discord.AllowedMentions.none(),
                                    )
                                except Exception:
                                    # Fallback if embed links are blocked in this channel.
                                    txt = "RS-FS Sheet (auto): ✅ added {n} row(s).".format(n=int(added or 0))
                                    if sample_lines:
                                        txt += "\n" + "\n".join(sample_lines[:6])
                                    await sch.send(txt, allowed_mentions=discord.AllowedMentions.none())
                            elif not ok:
                                await sch.send(
                                    embed=_rsfs_embed(
                                        "RS-FS Sheet (auto)",
                                        status=f"❌ {msg}",
                                        color=discord.Color.red(),
                                        footer="RS-FS",
                                    ),
                                    allowed_mentions=discord.AllowedMentions.none(),
                                )
                except Exception:
                    pass

            # Mark message processed to avoid reprocessing the same embed.
            if mid:
                try:
                    self._rs_fs_seen_message_ids.add(mid)
                    # Keep bounded
                    if len(self._rs_fs_seen_message_ids) > 2000:
                        self._rs_fs_seen_message_ids = set(list(self._rs_fs_seen_message_ids)[-1200:])
                except Exception:
                    pass

            if ok and added > 0:
                print(f"{Colors.GREEN}[RS-FS Sheet]{Colors.RESET} Added {added} row(s) (msg_id={mid})")
            elif not ok:
                short = (msg or "").replace("\n", " ").strip()
                if len(short) > 240:
                    short = short[:240] + "..."
                print(f"{Colors.YELLOW}[RS-FS Sheet]{Colors.RESET} Sync skipped/failed: {short}")
        except Exception:
            # Never let sheet sync break forwarding.
            return

    async def _send_rs_fs_preview_embed(self, channel: discord.abc.Messageable, entries: List[rs_fs_sheet_sync.RsFsPreviewEntry]) -> None:
        try:
            emb = discord.Embed(title="RS-FS Results", color=discord.Color.dark_teal())
            for e in entries:
                name = f"{e.store} | {e.sku}".strip()
                if len(name) > 256:
                    name = name[:253] + "..."
                title = (e.title or "").strip()
                url = (e.url or "").strip()
                monitor_url = (getattr(e, "monitor_url", "") or url).strip()
                affiliate_url = (getattr(e, "affiliate_url", "") or "").strip()
                err = (e.error or "").strip()
                if len(title) > 800:
                    title = title[:800] + "..."
                if len(url) > 300:
                    url = url[:300] + "..."
                value_parts: List[str] = []
                if title:
                    value_parts.append(title)
                if monitor_url:
                    if len(monitor_url) > 420:
                        monitor_url = monitor_url[:420] + "..."
                    value_parts.append(f"Monitor: {monitor_url}")
                else:
                    value_parts.append("(no url)")
                if affiliate_url and affiliate_url != monitor_url:
                    if len(affiliate_url) > 420:
                        affiliate_url = affiliate_url[:420] + "..."
                    value_parts.append(f"Affiliate: {affiliate_url}")
                src = str(getattr(e, "source", "") or "").strip()
                if src:
                    value_parts.append(f"(source: {src})")
                if err and err.lower() not in {"", "title not found"}:
                    value_parts.append(f"(err: {err})")
                value = "\n".join(value_parts).strip() or "(no data)"
                if len(value) > 1024:
                    value = value[:1021] + "..."
                emb.add_field(name=name, value=value, inline=False)
            await channel.send(embed=emb, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            return
    
    def _get_guild_icon_url(self, guild: discord.Guild) -> Optional[str]:
        """Get guild icon URL"""
        if not guild or not guild.icon:
            return None
        
        try:
            # Use the Asset's url property directly - it returns the full URL
            icon_url = str(guild.icon.url)
            # Ensure size=256 parameter
            if "?" in icon_url:
                # URL has query params - replace or add size parameter
                import re
                # Remove existing size parameter if present
                icon_url = re.sub(r'[?&]size=\d+', '', icon_url)
                # Add size=256
                separator = "&" if "?" in icon_url else "?"
                return f"{icon_url}{separator}size=256"
            else:
                # No query params, just add size=256
                return f"{icon_url}?size=256"
        except (AttributeError, Exception) as e:
            # Fallback: construct URL manually from icon hash
            try:
                # Get the icon hash/key from the Asset
                icon_hash = str(guild.icon.key) if hasattr(guild.icon, 'key') else None
                if not icon_hash:
                    # Last resort: try to extract from string representation
                    icon_str = str(guild.icon)
                    # If it's already a URL, extract the hash
                    if "/icons/" in icon_str:
                        # Extract hash from URL pattern: .../icons/GUILD_ID/HASH.ext
                        parts = icon_str.split("/")
                        if len(parts) >= 2:
                            hash_part = parts[-1].split(".")[0].split("?")[0]
                            icon_hash = hash_part
                    else:
                        icon_hash = icon_str
                
                if icon_hash and not icon_hash.startswith("http"):
                    # Construct URL from hash
                    ext = "gif" if icon_hash.startswith("a_") else "png"
                    return f"https://cdn.discordapp.com/icons/{guild.id}/{icon_hash}.{ext}?size=256"
                else:
                    print(f"{Colors.YELLOW}[Icon] Could not extract icon hash: {e}{Colors.RESET}")
                    return None
            except Exception as e2:
                print(f"{Colors.RED}[Icon] Error getting icon URL: {e2}{Colors.RESET}")
                return None
    
    async def _fetch_guild_icon_via_api(self, guild_id: int, save_to_config: bool = True):
        """Fetch guild icon via Discord API as fallback"""
        try:
            import requests
            bot_token = self.config.get("bot_token", "").strip()
            if not bot_token:
                print(f"{Colors.RED}[Icon] No bot token available for API fetch{Colors.RESET}")
                return False
            
            headers = {
                'Authorization': f'Bot {bot_token}',
                'User-Agent': 'DiscordBot (RSForwarder)'
            }
            
            print(f"{Colors.CYAN}[Icon] Fetching RS Server icon via API for guild {guild_id}...{Colors.RESET}")
            response = requests.get(
                f'https://discord.com/api/v10/guilds/{guild_id}?with_counts=false',
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                guild_data = response.json()
                icon_hash = guild_data.get("icon")
                if icon_hash:
                    ext = "gif" if str(icon_hash).startswith("a_") else "png"
                    self.rs_icon_url = f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}?size=256"
                    print(f"{Colors.GREEN}[Icon] ✅ RS Server icon fetched: {self.rs_icon_url}{Colors.RESET}")
                    
                    # Save to config if requested
                    if save_to_config:
                        self.config["rs_server_icon_url"] = self.rs_icon_url
                        self.save_config()
                        print(f"{Colors.GREEN}[Icon] ✅ Icon saved to config.json{Colors.RESET}")
                    
                    return True
                else:
                    print(f"{Colors.YELLOW}[Icon] ⚠️ RS Server has no icon (fetched via API){Colors.RESET}")
                    return False
            elif response.status_code == 404:
                print(f"{Colors.RED}[Icon] ❌ RS Server not found (404) - bot may not be in server{Colors.RESET}")
                return False
            elif response.status_code == 403:
                print(f"{Colors.RED}[Icon] ❌ No permission to fetch RS Server info (403){Colors.RESET}")
                return False
            else:
                print(f"{Colors.YELLOW}[Icon] ⚠️ Failed to fetch RS Server info: {response.status_code} - {response.text[:100]}{Colors.RESET}")
                return False
        except Exception as e:
            print(f"{Colors.RED}[Icon] ❌ Error fetching guild icon via API: {e}{Colors.RESET}")
            import traceback
            if "--verbose" in sys.argv:
                traceback.print_exc()
            return False
    
    def _setup_events(self):
        """Setup Discord event handlers"""
        
        @self.bot.event
        async def on_ready():
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  📤 RS Forwarder Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            
            # Get RS Server guild - try multiple times as guilds may not be cached immediately
            guild_id = self._rs_server_guild_id()
            if guild_id:
                # Try to get guild, wait a bit if not immediately available
                for attempt in range(3):
                    self.rs_guild = self.bot.get_guild(guild_id)
                    if self.rs_guild:
                        break
                    await asyncio.sleep(1)
                
                if self.rs_guild:
                    print(f"{Colors.GREEN}[Bot] Connected to: {self.rs_guild.name}{Colors.RESET}")
                
                # If icon not loaded from config, try to fetch it
                if not self.rs_icon_url:
                    if self.rs_guild:
                        self.rs_icon_url = self._get_guild_icon_url(self.rs_guild)
                        if self.rs_icon_url:
                            print(f"{Colors.GREEN}[Icon] RS Server icon from guild: {self.rs_icon_url[:50]}...{Colors.RESET}")
                            # Save to config
                            self.config["rs_server_icon_url"] = self.rs_icon_url
                            self.save_config()
                        else:
                            print(f"{Colors.YELLOW}[Icon] RS Server has no icon from guild object, trying API...{Colors.RESET}")
                            # Try API as fallback even if guild is found (icon might not be in cache)
                            await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
                    else:
                        print(f"{Colors.YELLOW}[Icon] RS Server (ID: {guild_id}) not found in cache - trying API...{Colors.RESET}")
                        # Try to fetch icon via API as fallback
                        icon_fetched = await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
                        if not icon_fetched:
                            print(f"{Colors.RED}[Icon] ⚠️ Could not fetch RS Server icon. Check that:{Colors.RESET}")
                            print(f"{Colors.RED}[Icon]   1. Bot is in RS Server (ID: {guild_id}){Colors.RESET}")
                            print(f"{Colors.RED}[Icon]   2. Bot has permission to view server info{Colors.RESET}")
                            print(f"{Colors.RED}[Icon]   3. RS Server has an icon set{Colors.RESET}")
                else:
                    print(f"{Colors.GREEN}[Icon] Using saved RS Server icon from config{Colors.RESET}")

            # Startup diagnostics (guild/channel visibility). Helpful when “nothing happens”.
            try:
                await self._startup_validate_visibility()
            except Exception:
                pass
            
            # Display config information
            print(f"\n{Colors.CYAN}[Config] Configuration Information:{Colors.RESET}")
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            
            if self.rs_guild:
                print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET} {Colors.BOLD}{self.rs_guild.name}{Colors.RESET} (ID: {guild_id})")
            elif guild_id:
                print(f"{Colors.YELLOW}⚠️  Guild:{Colors.RESET} Not found (ID: {guild_id})")
            else:
                print(f"{Colors.YELLOW}⚠️  Guild:{Colors.RESET} Not configured")
            
            brand_name = self.config.get("brand_name", "Reselling Secrets")
            print(f"{Colors.GREEN}🏷️  Brand Name:{Colors.RESET} {Colors.BOLD}{brand_name}{Colors.RESET}")
            
            if self.rs_icon_url:
                print(f"{Colors.GREEN}🖼️  RS Server Icon:{Colors.RESET} {Colors.BOLD}Loaded{Colors.RESET}")
            else:
                print(f"{Colors.YELLOW}⚠️  RS Server Icon:{Colors.RESET} Not available")
            
            forwarding_logs_channel_id = self.config.get("forwarding_logs_channel_id")
            if forwarding_logs_channel_id and self.rs_guild:
                log_channel = self.rs_guild.get_channel(forwarding_logs_channel_id)
                if log_channel:
                    print(f"{Colors.GREEN}📝 Forwarding Logs Channel:{Colors.RESET} {Colors.BOLD}{log_channel.name}{Colors.RESET} (ID: {forwarding_logs_channel_id})")
                else:
                    print(f"{Colors.YELLOW}⚠️  Forwarding Logs Channel:{Colors.RESET} Not found (ID: {forwarding_logs_channel_id})")
            
            # Channel configurations
            channels = self.config.get("channels", [])
            print(f"{Colors.GREEN}📡 Forwarding Jobs:{Colors.RESET} {len(channels)} channel(s)")
            for i, channel_config in enumerate(channels[:5], 1):  # Show first 5
                source_id = channel_config.get("source_channel_id", "N/A")
                source_name = channel_config.get("source_channel_name", "N/A")
                if self.rs_guild and source_id != "N/A":
                    source_channel = self.rs_guild.get_channel(int(source_id))
                    if source_channel:
                        print(f"   {i}. {Colors.BOLD}{source_channel.name}{Colors.RESET} → Webhook")
                    else:
                        print(f"   {i}. {Colors.YELLOW}Channel {source_id}{Colors.RESET} → Webhook")
                else:
                    print(f"   {i}. {Colors.BOLD}{source_name}{Colors.RESET} → Webhook")
            if len(channels) > 5:
                print(f"   ... and {len(channels) - 5} more")
            
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
            
            # Initialize stats
            from datetime import datetime, timezone
            self.stats['started_at'] = datetime.now(timezone.utc).isoformat()
            
            # ALWAYS try to fetch icon via API as final attempt (even if guild was found)
            if not self.rs_icon_url and guild_id:
                print(f"{Colors.CYAN}[Icon] Final attempt: Fetching icon via API...{Colors.RESET}")
                await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
            
            # Final check - if still no icon, log warning
            if not self.rs_icon_url:
                print(f"{Colors.RED}[Bot] ❌ CRITICAL: RS Server icon NOT available!{Colors.RESET}")
                print(f"{Colors.RED}[Bot] Branding will not work properly without the icon.{Colors.RESET}")
                print(f"{Colors.RED}[Bot] Use !rsfetchicon to manually fetch the icon.{Colors.RESET}")

            # Affiliate rewrite startup self-test
            try:
                if bool(self.config.get("affiliate_rewrite_enabled")):
                    print(f"\n{Colors.CYAN}[Affiliate] Startup self-test:{Colors.RESET}")

                    amazon_test_url = (os.getenv("RS_STARTUP_TEST_AMAZON_URL", "") or "").strip() or "https://www.amazon.com/dp/B000000000"
                    try:
                        mapped, notes = await affiliate_rewriter.compute_affiliate_rewrites(self.config, [amazon_test_url])
                        out = (mapped.get(amazon_test_url) or "").strip()
                        if out and out != amazon_test_url:
                            print(f"{Colors.GREEN}[Affiliate] ✅ Amazon PASS{Colors.RESET} -> {out}")
                        else:
                            why = (notes.get(amazon_test_url) or "no change").strip()
                            print(f"{Colors.YELLOW}[Affiliate] ⚠️  Amazon NO-CHANGE{Colors.RESET} ({why})")
                    except Exception as e:
                        print(f"{Colors.RED}[Affiliate] ❌ Amazon FAIL{Colors.RESET} ({e})")

                    # Mavely startup check (NON-mutating):
                    # Use a preflight/session check instead of creating an affiliate link (prevents dashboard spam).
                    try:
                        ok, status, err = await affiliate_rewriter.mavely_preflight(self.config)
                        if ok:
                            print(f"{Colors.GREEN}[Affiliate] ✅ Mavely preflight OK{Colors.RESET} (status={status})")
                        else:
                            # Keep error short (never print cookies/tokens)
                            msg = (err or "unknown error").replace("\n", " ").strip()
                            if len(msg) > 180:
                                msg = msg[:180] + "..."
                            print(f"{Colors.RED}[Affiliate] ❌ Mavely preflight FAIL{Colors.RESET} (status={status}) {msg}")
                    except Exception as e:
                        print(f"{Colors.RED}[Affiliate] ❌ Mavely preflight FAIL{Colors.RESET} ({e})")
            except Exception:
                pass

            # Start background Mavely monitor (DM alerts + auto-start login desktop)
            try:
                if self._mavely_monitor_task is None or self._mavely_monitor_task.done():
                    self._mavely_monitor_task = asyncio.create_task(self._mavely_monitor_loop())
            except Exception:
                pass
            
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  🔄 RS Forwarder Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            
            channels = self.config.get("channels", [])
            print(f"{Colors.GREEN}[Bot] Monitoring {len(channels)} channel(s){Colors.RESET}")
            
            if channels:
                # List monitored channels
                for channel in channels:
                    source_id = channel.get("source_channel_id", "unknown")
                    source_name = channel.get("source_channel_name", "unknown")
                    webhook_set = "✓" if channel.get("destination_webhook_url") else "✗"
                    role_mention = ""
                    role_config = channel.get("role_mention", {})
                    if role_config.get("role_id"):
                        role_id = role_config.get("role_id")
                        text = role_config.get("text", "")
                        role_mention = f" | Role: <@&{role_id}> {text}"
                    print(f"  {webhook_set} {source_name} ({source_id}){role_mention}")
            else:
                print(f"  {Colors.YELLOW}No channels configured. Use !add command to add channels.{Colors.RESET}")
            
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
            self.stats['started_at'] = datetime.now().isoformat()
        
        @self.bot.event
        async def on_message(message: discord.Message):
            # Process commands first
            await self.bot.process_commands(message)
            
            # Don't skip bot messages - we want to forward ALL messages including bot messages
            # Only skip our own bot's messages to avoid loops
            if message.author.id == self.bot.user.id:
                return

            # Optional: Zephyr release feed -> RS-FS Google Sheet sync
            try:
                await self._maybe_sync_rs_fs_sheet_from_message(message)
            except Exception:
                pass
            
            # Check if this is a monitored channel
            channel_id = str(message.channel.id)
            channel_config = self.get_channel_config(channel_id)
            
            if channel_config:
                # Forward the message
                await self.forward_message(message, channel_id, channel_config)

        @self.bot.event
        async def on_command(ctx):  # type: ignore[override]
            try:
                cmd = getattr(getattr(ctx, "command", None), "qualified_name", None) or getattr(getattr(ctx, "command", None), "name", None) or "?"
                ch_id = int(getattr(getattr(ctx, "channel", None), "id", 0) or 0)
                g_id = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
                u_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
                print(f"{Colors.CYAN}[Cmd] {cmd} user={u_id} guild={g_id} channel={ch_id}{Colors.RESET}")
            except Exception:
                pass

        @self.bot.event
        async def on_command_error(ctx, error):  # type: ignore[override]
            # Make command failures visible in journal + (best-effort) to the invoking user.
            try:
                cmd = getattr(getattr(ctx, "command", None), "qualified_name", None) or getattr(getattr(ctx, "command", None), "name", None) or "?"
                ch_id = int(getattr(getattr(ctx, "channel", None), "id", 0) or 0)
                g_id = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
                u_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
                orig = getattr(error, "original", None)
                et = type(orig).__name__ if orig is not None else type(error).__name__
                msg = str(orig or error or "").replace("\r", " ").replace("\n", " ").strip()
                if len(msg) > 260:
                    msg = msg[:260] + "..."
                print(f"{Colors.RED}[CmdErr] {cmd} user={u_id} guild={g_id} channel={ch_id} {et}: {msg}{Colors.RESET}")
            except Exception:
                pass

            # Try to send a short message. If channel send fails, DM the user.
            short = f"❌ `{getattr(getattr(ctx, 'command', None), 'name', 'cmd')}` failed: {type(error).__name__}"
            try:
                await ctx.send(short)
                return
            except Exception:
                pass
            try:
                if getattr(ctx, "author", None):
                    await ctx.author.send(short)
            except Exception:
                pass
    
    def _setup_commands(self):
        """Setup bot commands"""
        
        @self.bot.command(name="rsadd", aliases=["add"])
        async def add_channel(ctx, source_channel: str = None, destination_webhook_url: str = None, role_id: str = None, *, text: str = None):
            """
            Add a forwarding job.

            Preferred workflow (DiscumBot-style):
              - run `!rsadd` in the destination channel
              - select source guild/category/channels
              - click "Map → destination" (auto-creates/uses a webhook)

            Manual mode:
              `!rsadd <#channel|channel_id> <webhook_url> [role_id] [text]`
            """
            # Interactive mapper
            if not source_channel and not destination_webhook_url:
                if not isinstance(getattr(ctx, "channel", None), discord.TextChannel):
                    await ctx.send("❌ Run `!rsadd` inside a server text channel.")
                    return

                owner_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
                dest_channel: discord.TextChannel = ctx.channel  # type: ignore[assignment]

                class _RsBrowseMapperView(discord.ui.View):
                    """
                    Discum-style browse flow (destination first):
                      dest guild -> dest category -> dest channel -> Next
                      src guild -> src category -> src channel(s) -> Map
                    """

                    def __init__(self, bot_obj: "RSForwarderBot"):
                        super().__init__(timeout=900)
                        self._bot = bot_obj
                        self._owner_id = int(owner_id or 0)
                        self._message: Optional[discord.Message] = None

                        # Steps: dest_guild, dest_category, dest_channel, src_guild, src_category, src_channel
                        self.step: str = "dest_guild"

                        # Paging indices
                        self.dest_guild_page = 0
                        self.src_guild_page = 0
                        self.dest_channel_page = 0
                        self.src_channel_page = 0

                        # Current selections
                        self.dest_guild_id = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
                        self.dest_category_index = 0  # index into categories list (+0 == all)
                        self.dest_channel_id = 0

                        self.src_guild_id = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
                        self.src_category_index = 0
                        self.selected_src_channel_ids: Set[int] = set()

                        self._rebuild()

                    async def _guard(self, interaction: discord.Interaction) -> bool:
                        try:
                            if self._owner_id and int(interaction.user.id) != self._owner_id:
                                await interaction.response.send_message("❌ This mapper is not for you. Run `!rsadd` yourself.", ephemeral=True)
                                return False
                        except Exception:
                            return False
                        return True

                    def _all_guilds(self) -> List[discord.Guild]:
                        try:
                            return list(self._bot.bot.guilds or [])
                        except Exception:
                            return []

                    def _guild_page(self, page: int) -> Tuple[List[discord.Guild], int]:
                        gs = self._all_guilds()
                        page_size = 25
                        if not gs:
                            return [], 0
                        max_page = max(0, (len(gs) - 1) // page_size)
                        p = max(0, min(int(page), int(max_page)))
                        start = p * page_size
                        return gs[start:start + page_size], max_page

                    def _guild_by_id(self, guild_id: int) -> Optional[discord.Guild]:
                        try:
                            return self._bot.bot.get_guild(int(guild_id))
                        except Exception:
                            return None

                    def _categories_for(self, guild: discord.Guild) -> List[Optional[discord.CategoryChannel]]:
                        cats = list(getattr(guild, "categories", []) or [])
                        cats_sorted = sorted(cats, key=lambda c: int(getattr(c, "position", 0) or 0))
                        return [None] + cats_sorted  # None => all

                    def _channels_for_category(self, guild: discord.Guild, category: Optional[discord.CategoryChannel]) -> List[discord.TextChannel]:
                        if category is None:
                            chans = [c for c in (getattr(guild, "text_channels", []) or []) if isinstance(c, discord.TextChannel)]
                        else:
                            chans = [c for c in (getattr(category, "channels", []) or []) if isinstance(c, discord.TextChannel)]
                        return sorted(chans, key=lambda c: int(getattr(c, "position", 0) or 0))

                    def _channels_page(self, channels: List[discord.TextChannel], page: int) -> Tuple[List[discord.TextChannel], int]:
                        page_size = 25
                        if not channels:
                            return [], 0
                        max_page = max(0, (len(channels) - 1) // page_size)
                        p = max(0, min(int(page), int(max_page)))
                        start = p * page_size
                        return channels[start:start + page_size], max_page

                    async def _edit_message(self, interaction: discord.Interaction) -> None:
                        emb = await self._build_embed()
                        try:
                            await interaction.response.edit_message(embed=emb, view=self)
                            self._message = interaction.message or self._message
                            return
                        except Exception:
                            pass
                        try:
                            msg = interaction.message or self._message
                            if msg:
                                await msg.edit(embed=emb, view=self)
                        except Exception:
                            pass

                    async def _build_embed(self) -> discord.Embed:
                        emb = discord.Embed(title="RSForwarder Mapper", color=discord.Color.blurple())
                        if self.step.startswith("dest_"):
                            emb.description = "Pick a **destination** guild/category/channel."
                            g = self._guild_by_id(self.dest_guild_id)
                            emb.add_field(name="Destination guild", value=f"`{g.name}` ({g.id})" if g else f"`unknown` ({self.dest_guild_id})", inline=False)
                            emb.add_field(name="Destination channel", value=f"<#{self.dest_channel_id}>" if self.dest_channel_id else "Not set", inline=False)
                            emb.set_footer(text="Destination browse → Next")
                        else:
                            emb.description = "Pick a **source** guild/category/channel(s)."
                            g = self._guild_by_id(self.src_guild_id)
                            emb.add_field(name="Source guild", value=f"`{g.name}` ({g.id})" if g else f"`unknown` ({self.src_guild_id})", inline=False)
                            if self.selected_src_channel_ids:
                                shown = ", ".join([f"<#{cid}>" for cid in list(self.selected_src_channel_ids)[:12]])
                                emb.add_field(name="Selected source", value=shown, inline=False)
                            else:
                                emb.add_field(name="Selected source", value="None", inline=False)
                            emb.add_field(name="Destination", value=f"<#{self.dest_channel_id}>" if self.dest_channel_id else "Not set", inline=False)
                            emb.set_footer(text="Source browse → Map → destination")
                        return emb

                    def _rebuild(self) -> None:
                        self.clear_items()

                        # Buttons: always include Cancel
                        cancel_btn = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.danger, row=4)
                        cancel_btn.callback = self._cancel  # type: ignore[assignment]

                        if self.step == "dest_guild":
                            items, max_page = self._guild_page(self.dest_guild_page)
                            opts: List[discord.SelectOption] = []
                            for g in items:
                                opts.append(discord.SelectOption(label=str(g.name)[:100], value=str(int(g.id))))
                            if not opts:
                                opts = [discord.SelectOption(label="(no guilds)", value="0")]
                            sel = discord.ui.Select(placeholder="Select destination guild…", min_values=1, max_values=1, options=opts, row=0)
                            sel.callback = self._dest_pick_guild  # type: ignore[assignment]
                            self.add_item(sel)

                            prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=(self.dest_guild_page <= 0))
                            next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=(self.dest_guild_page >= max_page))
                            prev_btn.callback = self._dest_prev_guilds  # type: ignore[assignment]
                            next_btn.callback = self._dest_next_guilds  # type: ignore[assignment]
                            self.add_item(prev_btn)
                            self.add_item(next_btn)

                            go_btn = discord.ui.Button(label="Continue", style=discord.ButtonStyle.primary, row=4, disabled=(int(self.dest_guild_id or 0) <= 0))
                            go_btn.callback = self._dest_guild_continue  # type: ignore[assignment]
                            self.add_item(go_btn)
                            self.add_item(cancel_btn)
                            return

                        if self.step == "dest_category":
                            g = self._guild_by_id(self.dest_guild_id)
                            cats = self._categories_for(g) if g else [None]
                            idx = max(0, min(int(self.dest_category_index), len(cats) - 1))
                            self.dest_category_index = idx
                            cat = cats[idx]
                            label = "All channels" if cat is None else str(getattr(cat, "name", "") or "Category")
                            info = discord.ui.Button(label=f"Category: {label}", style=discord.ButtonStyle.secondary, row=0, disabled=True)
                            self.add_item(info)

                            prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=(idx <= 0))
                            next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=(idx >= (len(cats) - 1)))
                            prev_btn.callback = self._dest_prev_category  # type: ignore[assignment]
                            next_btn.callback = self._dest_next_category  # type: ignore[assignment]
                            self.add_item(prev_btn)
                            self.add_item(next_btn)

                            back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
                            back_btn.callback = self._back  # type: ignore[assignment]
                            go_btn = discord.ui.Button(label="Continue", style=discord.ButtonStyle.primary, row=4)
                            go_btn.callback = self._dest_category_continue  # type: ignore[assignment]
                            self.add_item(back_btn)
                            self.add_item(go_btn)
                            self.add_item(cancel_btn)
                            return

                        if self.step == "dest_channel":
                            g = self._guild_by_id(self.dest_guild_id)
                            cats = self._categories_for(g) if g else [None]
                            cat = cats[max(0, min(int(self.dest_category_index), len(cats) - 1))] if cats else None
                            chans = self._channels_for_category(g, cat) if g else []
                            page_items, max_page = self._channels_page(chans, self.dest_channel_page)
                            opts: List[discord.SelectOption] = []
                            for ch in page_items:
                                opts.append(discord.SelectOption(label=f"#{str(getattr(ch, 'name', '') or ch.id)[:95]}", value=str(int(ch.id))))
                            if not opts:
                                opts = [discord.SelectOption(label="(no channels)", value="0")]
                            sel = discord.ui.Select(placeholder="Select destination channel…", min_values=1, max_values=1, options=opts, row=0)
                            sel.callback = self._dest_pick_channel  # type: ignore[assignment]
                            self.add_item(sel)

                            prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=(self.dest_channel_page <= 0))
                            next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=(self.dest_channel_page >= max_page))
                            prev_btn.callback = self._dest_prev_channels  # type: ignore[assignment]
                            next_btn.callback = self._dest_next_channels  # type: ignore[assignment]
                            self.add_item(prev_btn)
                            self.add_item(next_btn)

                            back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
                            back_btn.callback = self._back  # type: ignore[assignment]
                            go_btn = discord.ui.Button(label="Next (source)", style=discord.ButtonStyle.primary, row=4, disabled=(int(self.dest_channel_id or 0) <= 0))
                            go_btn.callback = self._to_src_guild  # type: ignore[assignment]
                            self.add_item(back_btn)
                            self.add_item(go_btn)
                            self.add_item(cancel_btn)
                            return

                        if self.step == "src_guild":
                            items, max_page = self._guild_page(self.src_guild_page)
                            opts: List[discord.SelectOption] = []
                            for g in items:
                                opts.append(discord.SelectOption(label=str(g.name)[:100], value=str(int(g.id))))
                            if not opts:
                                opts = [discord.SelectOption(label="(no guilds)", value="0")]
                            sel = discord.ui.Select(placeholder="Select source guild…", min_values=1, max_values=1, options=opts, row=0)
                            sel.callback = self._src_pick_guild  # type: ignore[assignment]
                            self.add_item(sel)

                            prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=(self.src_guild_page <= 0))
                            next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=(self.src_guild_page >= max_page))
                            prev_btn.callback = self._src_prev_guilds  # type: ignore[assignment]
                            next_btn.callback = self._src_next_guilds  # type: ignore[assignment]
                            self.add_item(prev_btn)
                            self.add_item(next_btn)

                            back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
                            back_btn.callback = self._back  # type: ignore[assignment]
                            go_btn = discord.ui.Button(label="Continue", style=discord.ButtonStyle.primary, row=4, disabled=(int(self.src_guild_id or 0) <= 0))
                            go_btn.callback = self._src_guild_continue  # type: ignore[assignment]
                            self.add_item(back_btn)
                            self.add_item(go_btn)
                            self.add_item(cancel_btn)
                            return

                        if self.step == "src_category":
                            g = self._guild_by_id(self.src_guild_id)
                            cats = self._categories_for(g) if g else [None]
                            idx = max(0, min(int(self.src_category_index), len(cats) - 1))
                            self.src_category_index = idx
                            cat = cats[idx]
                            label = "All channels" if cat is None else str(getattr(cat, "name", "") or "Category")
                            info = discord.ui.Button(label=f"Category: {label}", style=discord.ButtonStyle.secondary, row=0, disabled=True)
                            self.add_item(info)

                            prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=(idx <= 0))
                            next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=(idx >= (len(cats) - 1)))
                            prev_btn.callback = self._src_prev_category  # type: ignore[assignment]
                            next_btn.callback = self._src_next_category  # type: ignore[assignment]
                            self.add_item(prev_btn)
                            self.add_item(next_btn)

                            back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
                            back_btn.callback = self._back  # type: ignore[assignment]
                            go_btn = discord.ui.Button(label="Continue", style=discord.ButtonStyle.primary, row=4)
                            go_btn.callback = self._src_category_continue  # type: ignore[assignment]
                            self.add_item(back_btn)
                            self.add_item(go_btn)
                            self.add_item(cancel_btn)
                            return

                        # src_channel
                        g = self._guild_by_id(self.src_guild_id)
                        cats = self._categories_for(g) if g else [None]
                        cat = cats[max(0, min(int(self.src_category_index), len(cats) - 1))] if cats else None
                        chans = self._channels_for_category(g, cat) if g else []
                        page_items, max_page = self._channels_page(chans, self.src_channel_page)
                        opts: List[discord.SelectOption] = []
                        for ch in page_items:
                            opts.append(discord.SelectOption(label=f"#{str(getattr(ch, 'name', '') or ch.id)[:95]}", value=str(int(ch.id))))
                        if not opts:
                            opts = [discord.SelectOption(label="(no channels)", value="0")]
                        # Discord requires max_values <= number of options.
                        max_v = min(25, max(1, len(opts)))
                        sel = discord.ui.Select(
                            placeholder="Select source channel(s)…",
                            min_values=0,
                            max_values=max_v,
                            options=opts,
                            row=0,
                        )
                        sel.callback = self._src_pick_channels  # type: ignore[assignment]
                        self.add_item(sel)

                        prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=(self.src_channel_page <= 0))
                        next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=(self.src_channel_page >= max_page))
                        prev_btn.callback = self._src_prev_channels  # type: ignore[assignment]
                        next_btn.callback = self._src_next_channels  # type: ignore[assignment]
                        self.add_item(prev_btn)
                        self.add_item(next_btn)

                        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, row=4)
                        back_btn.callback = self._back  # type: ignore[assignment]
                        map_btn = discord.ui.Button(label="Map → destination", style=discord.ButtonStyle.success, row=4, disabled=(int(self.dest_channel_id or 0) <= 0))
                        map_btn.callback = self._map  # type: ignore[assignment]
                        self.add_item(back_btn)
                        self.add_item(map_btn)
                        self.add_item(cancel_btn)

                    # --- callbacks ---
                    async def _dest_pick_guild(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        try:
                            v = int(interaction.data.get("values", ["0"])[0])  # type: ignore[union-attr]
                            self.dest_guild_id = v if v > 0 else 0
                            self.dest_category_index = 0
                            self.dest_channel_id = 0
                            self.dest_channel_page = 0
                        except Exception:
                            pass
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_prev_guilds(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.dest_guild_page = max(0, int(self.dest_guild_page) - 1)
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_next_guilds(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.dest_guild_page = int(self.dest_guild_page) + 1
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_guild_continue(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.step = "dest_category"
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_prev_category(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.dest_category_index = max(0, int(self.dest_category_index) - 1)
                        self.dest_channel_id = 0
                        self.dest_channel_page = 0
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_next_category(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.dest_category_index = int(self.dest_category_index) + 1
                        self.dest_channel_id = 0
                        self.dest_channel_page = 0
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_category_continue(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.step = "dest_channel"
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_pick_channel(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        try:
                            v = int(interaction.data.get("values", ["0"])[0])  # type: ignore[union-attr]
                            self.dest_channel_id = v if v > 0 else 0
                        except Exception:
                            pass
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_prev_channels(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.dest_channel_page = max(0, int(self.dest_channel_page) - 1)
                        self.dest_channel_id = 0
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _dest_next_channels(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.dest_channel_page = int(self.dest_channel_page) + 1
                        self.dest_channel_id = 0
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _to_src_guild(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.step = "src_guild"
                        self.selected_src_channel_ids = set()
                        self.src_category_index = 0
                        self.src_channel_page = 0
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_pick_guild(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        try:
                            v = int(interaction.data.get("values", ["0"])[0])  # type: ignore[union-attr]
                            self.src_guild_id = v if v > 0 else 0
                            self.src_category_index = 0
                            self.src_channel_page = 0
                            self.selected_src_channel_ids = set()
                        except Exception:
                            pass
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_prev_guilds(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.src_guild_page = max(0, int(self.src_guild_page) - 1)
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_next_guilds(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.src_guild_page = int(self.src_guild_page) + 1
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_guild_continue(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.step = "src_category"
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_prev_category(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.src_category_index = max(0, int(self.src_category_index) - 1)
                        self.src_channel_page = 0
                        self.selected_src_channel_ids = set()
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_next_category(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.src_category_index = int(self.src_category_index) + 1
                        self.src_channel_page = 0
                        self.selected_src_channel_ids = set()
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_category_continue(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.step = "src_channel"
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_pick_channels(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        try:
                            vals = interaction.data.get("values", [])  # type: ignore[union-attr]
                            self.selected_src_channel_ids = set(int(v) for v in (vals or []) if str(v).isdigit() and int(v) > 0)
                        except Exception:
                            self.selected_src_channel_ids = set()
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_prev_channels(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.src_channel_page = max(0, int(self.src_channel_page) - 1)
                        self.selected_src_channel_ids = set()
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _src_next_channels(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.src_channel_page = int(self.src_channel_page) + 1
                        self.selected_src_channel_ids = set()
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _map(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        if int(self.dest_channel_id or 0) <= 0:
                            await interaction.response.send_message("❌ Pick a destination channel first.", ephemeral=True)
                            return
                        if not self.selected_src_channel_ids:
                            await interaction.response.send_message("❌ Pick at least one source channel first.", ephemeral=True)
                            return
                        try:
                            await interaction.response.defer(ephemeral=True)
                        except Exception:
                            pass
                        dest_obj = await self._bot._resolve_channel_by_id(int(self.dest_channel_id))
                        if not isinstance(dest_obj, discord.TextChannel):
                            await interaction.followup.send("❌ Destination channel not accessible.", ephemeral=True)
                            return
                        ok_wh, msg_wh, wh_url = await self._bot._get_or_create_destination_webhook_url(dest_obj)
                        if not ok_wh:
                            await interaction.followup.send(f"❌ {msg_wh}", ephemeral=True)
                            return
                        added = 0
                        skipped = 0
                        for cid in list(self.selected_src_channel_ids):
                            ok2, _msg2, _emb2 = await self._bot._rsadd_apply(source_channel_id=int(cid), destination_webhook_url=wh_url)
                            if ok2:
                                added += 1
                            else:
                                skipped += 1
                        await interaction.followup.send(f"✅ Mapped: {added} ok, {skipped} skipped.", ephemeral=True)
                        # Reset to destination browse
                        self.step = "dest_guild"
                        self.selected_src_channel_ids = set()
                        self.dest_channel_id = 0
                        self._rebuild()
                        try:
                            emb = await self._build_embed()
                            msg = interaction.message or self._message
                            if msg:
                                await msg.edit(embed=emb, view=self)
                        except Exception:
                            pass

                    async def _back(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        back_map = {
                            "dest_category": "dest_guild",
                            "dest_channel": "dest_category",
                            "src_guild": "dest_channel",
                            "src_category": "src_guild",
                            "src_channel": "src_category",
                        }
                        self.step = back_map.get(self.step, "dest_guild")
                        self._rebuild()
                        await self._edit_message(interaction)

                    async def _cancel(self, interaction: discord.Interaction):
                        if not await self._guard(interaction):
                            return
                        self.stop()
                        try:
                            await interaction.response.edit_message(view=None)
                        except Exception:
                            try:
                                msg = interaction.message or self._message
                                if msg:
                                    await msg.edit(view=None)
                            except Exception:
                                pass

                view = _RsBrowseMapperView(self)
                emb = await view._build_embed()
                try:
                    sent = await ctx.send(embed=emb, view=view)
                    view._message = sent
                except Exception:
                    await ctx.send(embed=emb, view=view)
                return

            # Manual mode
            cid = self._extract_channel_id_from_ref(str(source_channel or ""))
            if not cid:
                await ctx.send("❌ Invalid source channel. Use `#channel` or a numeric channel ID.")
                return
            if not destination_webhook_url:
                await ctx.send("❌ Missing webhook URL. Tip: run `!rsadd` with no args for the mapper UI.")
                return

            ok, msg, emb = await self._rsadd_apply(
                source_channel_id=int(cid),
                destination_webhook_url=str(destination_webhook_url or ""),
                role_id=str(role_id or ""),
                text=str(text or ""),
            )
            if not ok or not emb:
                await ctx.send(f"❌ {msg}")
                return
            await ctx.send(embed=emb)
        
        @self.bot.command(name='rslist', aliases=['list'])
        async def list_channels(ctx):
            """List all configured channels"""
            channels = self.config.get("channels", [])
            if not channels:
                await ctx.send("❌ No channels configured.")
                return
            
            embed = discord.Embed(
                title="📋 Configured Channels",
                color=discord.Color.blue()
            )
            
            for channel in channels:
                source_id = channel.get("source_channel_id", "unknown")
                source_name = channel.get("source_channel_name", "unknown")
                webhook = channel.get("destination_webhook_url", "")
                role_config = channel.get("role_mention", {})
                
                status = "✅" if webhook else "❌"
                value = f"Webhook: {'Configured' if webhook else 'Not set'}"
                
                if role_config.get("role_id"):
                    role_id = role_config.get("role_id")
                    text = role_config.get("text", "")
                    value += f"\nRole: <@&{role_id}> {text}"
                
                embed.add_field(
                    name=f"{status} {source_name}",
                    value=f"ID: `{source_id}`\n{value}",
                    inline=False
                )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsupdate', aliases=['update'])
        async def update_channel(ctx, source_channel: discord.TextChannel = None, destination_webhook_url: str = None,
                                role_id: str = None, *, text: str = None):
            """Update an existing forwarding job
            
            Usage: !rsupdate <#channel|channel_id> [webhook_url] [role_id] [text]
            
            Examples:
            !rsupdate #personal-deals <WEBHOOK_URL> 886824827745337374 "new text"
            !rsupdate 1446174806981480578 <WEBHOOK_URL>
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsupdate <#channel|channel_id> [webhook_url] [role_id] [text]`\n\n"
                    "**Examples:**\n"
                    "`!rsupdate #personal-deals <WEBHOOK_URL> 886824827745337374 \"new text\"`\n"
                    "`!rsupdate 1446174806981480578 <WEBHOOK_URL>`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            # Find existing channel config
            existing = self.get_channel_config(source_channel_id)
            if not existing:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) is not configured!\n"
                    f"Use `!rsadd` to add it first."
                )
                return
            
            # Update fields if provided
            updated = False
            if destination_webhook_url:
                if not destination_webhook_url.startswith('https://discord.com/api/webhooks/'):
                    await ctx.send("❌ Invalid webhook URL format. Must be a Discord webhook URL.")
                    return
                ok = self._set_destination_webhook_secret(source_channel_id, destination_webhook_url.strip())
                if not ok:
                    await ctx.send("❌ Failed to write webhook into `config.secrets.json`. Check file permissions on the server.")
                    return
                # Keep in-memory value for display; it won't be written to config.json
                existing["destination_webhook_url"] = destination_webhook_url.strip()
                updated = True
            
            if role_id is not None:
                try:
                    int(role_id)  # Validate format
                except ValueError:
                    await ctx.send("❌ Invalid role ID format. Role ID must be a number.")
                    return
                if "role_mention" not in existing:
                    existing["role_mention"] = {}
                existing["role_mention"]["role_id"] = role_id.strip()
                updated = True
            
            if text is not None:
                if "role_mention" not in existing:
                    existing["role_mention"] = {}
                existing["role_mention"]["text"] = text.strip()
                updated = True
            
            if not updated:
                await ctx.send("❌ No fields to update. Provide at least one: webhook_url, role_id, or text.")
                return
            
            # Save changes
            self.save_config()
            self.load_config()
            
            # Build confirmation message
            embed = discord.Embed(
                title="✅ Forwarding Job Updated",
                color=discord.Color.blue(),
                description=f"Updated configuration for `{channel_name}`"
            )
            
            webhook = existing.get("destination_webhook_url", "")
            role_config = existing.get("role_mention", {})
            
            embed.add_field(
                name="📥 Source Channel",
                value=f"`{channel_name}`\nID: `{source_channel_id}`",
                inline=True
            )
            embed.add_field(
                name="📤 Destination",
                value="Webhook configured (saved to secrets)" if webhook else "Not set",
                inline=True
            )
            
            if role_config.get("role_id"):
                embed.add_field(
                    name="📢 Role Mention",
                    value=f"<@&{role_config.get('role_id')}> {role_config.get('text', '')}",
                    inline=False
                )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsview', aliases=['view'])
        async def view_channel(ctx, source_channel: discord.TextChannel = None):
            """View details of a specific forwarding job
            
            Usage: !rsview <#channel|channel_id>
            
            Example: !rsview #personal-deals
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsview <#channel|channel_id>`\n"
                    "**Example:** `!rsview #personal-deals`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            # Find channel config
            channel_config = self.get_channel_config(source_channel_id)
            if not channel_config:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) is not configured!\n"
                    f"Use `!rsadd` to add it."
                )
                return
            
            # Build detailed view
            webhook = channel_config.get("destination_webhook_url", "")
            role_config = channel_config.get("role_mention", {})
            
            embed = discord.Embed(
                title=f"📋 Forwarding Job: {channel_name}",
                color=discord.Color.blue(),
                description=f"Detailed configuration for this forwarding job"
            )
            
            embed.add_field(
                name="📥 Source Channel",
                value=f"**Name:** `{channel_name}`\n**ID:** `{source_channel_id}`",
                inline=False
            )
            
            embed.add_field(
                name="📤 Destination Webhook",
                value=f"`{mask_secret(webhook)}`" if webhook else "❌ Not configured",
                inline=False
            )
            
            role_id = role_config.get("role_id", "")
            role_text = role_config.get("text", "")
            if role_id:
                embed.add_field(
                    name="📢 Role Mention",
                    value=f"**Role:** <@&{role_id}>\n**Text:** {role_text if role_text else '(empty)'}",
                    inline=False
                )
            else:
                embed.add_field(
                    name="📢 Role Mention",
                    value="❌ Not configured",
                    inline=False
                )
            
            embed.set_footer(text="Use !rsupdate to modify or !rsremove to delete")
            
            await ctx.send(embed=embed)

        @self.bot.command(name="rsfstest", aliases=["fstest", "rsfs"])
        async def rsfs_test(ctx, limit: str = "25"):
            """
            Manual dry-run: fetch the most recent Zephyr "Release Feed(s)" embed from the configured channel
            and post the preview output into that same channel.
            """
            try:
                try:
                    lim = int(str(limit or "").strip() or "25")
                except Exception:
                    lim = 25
                lim = max(1, min(lim, 120))
                self.config["rs_fs_sheet_test_limit"] = lim
                self.config["rs_fs_sheet_test_output_enabled"] = True
                self.config["rs_fs_sheet_dry_run"] = True

                ch_id = self._zephyr_release_feed_channel_id()
                if not ch_id:
                    await ctx.send("❌ `zephyr_release_feed_channel_id` is not configured.")
                    return

                ch = await self._resolve_channel_by_id(int(ch_id))
                if not ch or not hasattr(ch, "history"):
                    await ctx.send(f"❌ Could not access Zephyr channel `{ch_id}` (no permission or not found).")
                    return

                await ctx.send(f"✅ Running RS-FS dry-run preview from <#{ch_id}> (limit={lim})…")

                # Zephyr often posts the list across multiple chunks. Collect a few recent chunks and merge.
                merged_text_parts: List[str] = []
                try:
                    async for m in ch.history(limit=80):
                        if not (getattr(m, "embeds", None) or []) and not (getattr(m, "content", None) or ""):
                            continue
                        t = self._collect_embed_text(m)
                        if not t:
                            continue
                        # Keep only chunks that look like Zephyr list content (monitor tags + numbered items)
                        if zephyr_release_feed_parser.looks_like_release_feed_embed_text(t):
                            merged_text_parts.append(t)
                        if len(merged_text_parts) >= 4:
                            break
                except Exception:
                    merged_text_parts = []

                if not merged_text_parts:
                    await ctx.send("❌ Could not find recent Zephyr `Release Feed(s)` embed chunks in that channel.")
                    return

                # Create a lightweight shim message-like object and reuse the same handler.
                class _Shim:
                    def __init__(self, channel, text: str):
                        self.channel = channel
                        self.id = int(time.time() * 1000)
                        self.author = ctx.author
                        self.content = text
                        self.embeds = []

                shim = _Shim(ch, "\n".join(reversed(merged_text_parts)))
                await self._maybe_sync_rs_fs_sheet_from_message(shim)  # type: ignore[arg-type]
            except Exception as e:
                await ctx.send(f"❌ RS-FS test failed: {str(e)[:200]}")

        @self.bot.command(name="rsfstestsku", aliases=["fstestsku", "rsfssku"])
        async def rsfs_test_sku(ctx, store: str = "", sku: str = ""):
            """
            Manual dry-run for ONE item (bypasses Zephyr list parsing):
              !rsfstestsku gamestop 20023800
            Posts output into the channel where you run the command.
            """
            st_in = (store or "").strip()
            sk_in = (sku or "").strip()
            if not (st_in and sk_in):
                await ctx.send(
                    embed=_rsfs_embed(
                        "RS-FS Test SKU",
                        status="❌ missing args",
                        description="Usage: `!rsfstestsku <store> <sku>` (example: `!rsfstestsku gamestop 20023800`)",
                        color=discord.Color.red(),
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                return

            # Ensure we output to the invoking channel for this test.
            try:
                self.config["rs_fs_sheet_test_output_enabled"] = True
                self.config["rs_fs_sheet_dry_run"] = True
                self.config["rs_fs_sheet_test_output_channel_id"] = str(getattr(ctx.channel, "id", "") or "")
                self.config["rs_fs_sheet_test_limit"] = 1
            except Exception:
                pass

            out_ch = ctx.channel
            await ctx.send(
                embed=_rsfs_embed(
                    "RS-FS Test SKU",
                    status="Running",
                    fields=[
                        ("Store", f"`{st_in}`", True),
                        ("SKU", f"`{sk_in}`", True),
                        ("Mode", "monitor → website", True),
                    ],
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )

            # Make monitor lookup verbose for this command (journald + clearer behavior).
            prev_dbg = (self.config or {}).get("rs_fs_monitor_debug")
            prev_hist = (self.config or {}).get("rs_fs_monitor_lookup_history_limit")
            try:
                self.config["rs_fs_monitor_debug"] = True
                try:
                    self.config["rs_fs_monitor_lookup_history_limit"] = max(int(prev_hist or 0), 2000)
                except Exception:
                    self.config["rs_fs_monitor_lookup_history_limit"] = 2000
            except Exception:
                pass

            # Show which monitor channel we will use (name resolution is a common failure mode).
            try:
                ch_name = self._monitor_channel_name_for_store(st_in) or ""
                ch_hit = await self._resolve_monitor_channel_for_store(st_in, guild=getattr(ctx, "guild", None)) if ch_name else None
                if ch_hit:
                    await ctx.send(
                        embed=_rsfs_embed(
                            "RS-FS Test SKU",
                            status="Monitor channel resolved",
                            fields=[("Channel", f"#{getattr(ch_hit,'name','')} (`{getattr(ch_hit,'id','')}`)", False)],
                        ),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                else:
                    await ctx.send(
                        embed=_rsfs_embed(
                            "RS-FS Test SKU",
                            status="⚠️ monitor channel not found",
                            color=discord.Color.orange(),
                            fields=[("Expected", f"`{ch_name}`", True)],
                        ),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
            except Exception:
                pass

            try:

                # Progress message
                progress_msg = None
                try:
                    progress_msg = await out_ch.send(
                        f"RS-FS progress: {self._format_progress_bar(0, 1)} | errors: 0",
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                except Exception:
                    progress_msg = None

                # Stage 1: monitor lookup
                entry = None
                try:
                    entry = await self._monitor_lookup_for_store(st_in, sk_in, guild=getattr(ctx, "guild", None))
                except Exception:
                    entry = None

                if entry is None:
                    try:
                        await ctx.send(
                            embed=_rsfs_embed(
                                "RS-FS Test SKU",
                                status="Monitor MISS → website fallback",
                                color=discord.Color.orange(),
                            ),
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                    except Exception:
                        pass
                    # Stage 2: website fallback
                    try:
                        if progress_msg:
                            await progress_msg.edit(content=f"RS-FS progress: {self._format_progress_bar(0, 1)} | errors: 0 | stage: website")
                    except Exception:
                        pass
                    try:
                        web_entries = await rs_fs_sheet_sync.build_preview_entries([(st_in, sk_in)], self.config)
                        entry = web_entries[0] if web_entries else None
                    except Exception as e:
                        entry = rs_fs_sheet_sync.RsFsPreviewEntry(
                            store=st_in,
                            sku=sk_in,
                            url=rs_fs_sheet_sync.build_store_link(st_in, sk_in),
                            title="",
                            error=str(e)[:200],
                            source="website",
                        )

                if progress_msg:
                    try:
                        err_count = 1 if (entry and (entry.error or "").strip()) else 0
                        await progress_msg.edit(content=f"RS-FS progress: ✅ done 1/1 | errors: {err_count}")
                    except Exception:
                        pass

                if not entry:
                    await ctx.send(
                        embed=_rsfs_embed("RS-FS Test SKU", status="❌ no result", color=discord.Color.red()),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return

                await self._send_rs_fs_preview_embed(out_ch, [entry])
            finally:
                try:
                    # Restore debug knobs
                    if prev_dbg is None:
                        (self.config or {}).pop("rs_fs_monitor_debug", None)
                    else:
                        self.config["rs_fs_monitor_debug"] = prev_dbg
                    if prev_hist is None:
                        (self.config or {}).pop("rs_fs_monitor_lookup_history_limit", None)
                    else:
                        self.config["rs_fs_monitor_lookup_history_limit"] = prev_hist
                except Exception:
                    pass

        @self.bot.command(name="rsfscheck", aliases=["fscheck", "rsfsstatus"])
        async def rsfs_check(ctx):
            """
            Validate Google Sheets configuration/credentials (non-mutating).
            """
            try:
                emb = await self._build_rsfs_check_embed()
                try:
                    run_lim = int((self.config or {}).get("rs_fs_sheet_max_per_run") or 250)
                except Exception:
                    run_lim = 250
                run_lim = max(10, min(run_lim, 500))
                try:
                    owner_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
                except Exception:
                    owner_id = 0
                await ctx.send(
                    embed=emb,
                    view=_RsFsCheckView(self, owner_id=owner_id, run_limit=run_lim),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except Exception as e:
                await ctx.send(
                    embed=_rsfs_embed(
                        "RS-FS Check",
                        status=f"❌ failed: {str(e)[:200]}",
                        color=discord.Color.red(),
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )

        @self.bot.command(name="rsfsrun", aliases=["rsfslive", "rsfswrite"])
        async def rsfs_run(ctx, limit: str = "120"):
            """
            Live sync: fetch recent Zephyr Release Feed chunks and WRITE rows into the Google Sheet.
            This is the "real data" test (not dry-run).
            """
            try:
                try:
                    lim = int(str(limit or "").strip() or "120")
                except Exception:
                    lim = 120
                lim = max(10, min(lim, 500))

                if not getattr(self, "_rs_fs_sheet", None) or not self._rs_fs_sheet.enabled():
                    await ctx.send("❌ RS-FS sheet sync is not enabled.")
                    return

                ch_id = self._zephyr_release_feed_channel_id()
                if not ch_id:
                    await ctx.send("❌ `zephyr_release_feed_channel_id` is not configured.")
                    return

                ch = await self._resolve_channel_by_id(int(ch_id))
                if not ch or not hasattr(ch, "history"):
                    await ctx.send(f"❌ Could not access Zephyr channel `{ch_id}` (no permission or not found).")
                    return

                # Single merged-run, single mirror sync.
                # Prevent live Zephyr message chunks from also being processed while we run.
                prev_manual = bool(getattr(self, "_rs_fs_manual_run_in_progress", False))
                self._rs_fs_manual_run_in_progress = True
                prev_hist = None
                try:
                    await ctx.send(f"✅ Running RS-FS LIVE mirror sync from <#{ch_id}> (max={lim})…")

                    def _progress_embed(stage: str, done: int, total: int, *, monitor_hits: int = 0, remaining: int = 0, web_errors: int = 0) -> discord.Embed:
                        emb = discord.Embed(title="RS-FS Live Sync", color=discord.Color.dark_teal())
                        emb.add_field(name="Stage", value=stage or "…", inline=False)
                        emb.add_field(name="Progress", value=self._format_progress_bar(done, total), inline=False)
                        emb.add_field(name="Monitor hits", value=str(int(monitor_hits)), inline=True)
                        emb.add_field(name="Remaining", value=str(int(remaining)), inline=True)
                        if web_errors:
                            emb.add_field(name="Website errors", value=str(int(web_errors)), inline=True)
                        emb.set_footer(text="This message updates live.")
                        return emb

                    progress_msg = None
                    try:
                        progress_msg = await ctx.send(
                            embed=_progress_embed("collect", 0, lim),
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                    except Exception:
                        progress_msg = None

                    # Collect the most recent listreleases run (merged from Zephyr chunks).
                    merged_text, _chunk_n, _found_header = await self._collect_latest_zephyr_release_feed_text(ch)
                    if not merged_text:
                        await ctx.send("❌ Could not find recent Zephyr `Release Feed(s)` embed chunks in that channel.")
                        return

                    # Keep Current List in sync with the merged run (no scraping here; just mirror + cache fill).
                    try:
                        await self._rsfs_write_current_list(merged_text, reason="rsfsrun-start")
                    except Exception:
                        pass
                    # Parse ALL records (like Current List), then filter to store+sku for Live List sync
                    all_recs = zephyr_release_feed_parser.parse_release_feed_records(merged_text) or []
                    # Filter to items with store+sku for Live List sync (but keep all for Current List)
                    items = [r for r in all_recs if r.store and r.is_sku_candidate]
                    if not items:
                        await ctx.send("❌ Parsed 0 items with store+sku from the merged Zephyr text.")
                        return

                    # Limit to the requested max (preserve order).
                    items = list(items)[:lim]
                    pairs = [(it.store, it.sku) for it in items]
                    # Release id lookup for reporting (/removereleaseid helper).
                    rid_by_key: Dict[str, int] = {}
                    for it in items:
                        try:
                            rid_by_key[self._rs_fs_override_key(it.store, it.sku)] = int(getattr(it, "release_id", 0) or 0)
                        except Exception:
                            continue
                    total = len(pairs)

                    if progress_msg:
                        try:
                            await progress_msg.edit(embed=_progress_embed("resolve (monitor first)", 0, total))
                        except Exception:
                            pass

                    # Stage 0: History cache (fast, avoids monitor/website scanning when already known)
                    history_hits: List[rs_fs_sheet_sync.RsFsPreviewEntry] = []
                    remaining_pairs_0: List[Tuple[str, str]] = []
                    try:
                        hist = await self._rs_fs_sheet.fetch_history_cache(force=False)
                    except Exception:
                        hist = {}
                    for st, sk in (pairs or []):
                        key = self._rsfs_key_store_sku(st, sk)
                        hrec = (hist or {}).get(key) if key else None
                        if isinstance(hrec, dict) and str(hrec.get("url") or "").strip():
                            u0 = str(hrec.get("url") or "").strip()
                            t0 = str(hrec.get("title") or "").strip()
                            a0 = str(hrec.get("affiliate_url") or "").strip()
                            # Only treat as a "history hit" if the cached title looks real.
                            # If the title is actually a URL (old bad data), re-resolve to fix it.
                            if not self._rsfs_title_is_bad(t0, url=u0):
                                history_hits.append(
                                    rs_fs_sheet_sync.RsFsPreviewEntry(
                                        store=st,
                                        sku=sk,
                                        url=u0,
                                        title=t0,
                                        error="",
                                        source="history",
                                        monitor_url=u0,
                                        affiliate_url=a0,
                                    )
                                )
                            else:
                                remaining_pairs_0.append((st, sk))
                        else:
                            remaining_pairs_0.append((st, sk))

                    # Stage 0.5: Manual overrides (persisted runtime JSON). These bypass monitor/website scanning.
                    overrides = self._load_rs_fs_manual_overrides()
                    manual_hits: List[rs_fs_sheet_sync.RsFsPreviewEntry] = []
                    remaining_pairs: List[Tuple[str, str]] = []
                    for st, sk in (remaining_pairs_0 or []):
                        k = self._rs_fs_override_key(st, sk)
                        ov = (overrides or {}).get(k)
                        if isinstance(ov, dict) and str(ov.get("url") or "").strip():
                            u0 = str(ov.get("url") or "").strip()
                            t0 = str(ov.get("title") or "").strip() or u0
                            manual_hits.append(
                                rs_fs_sheet_sync.RsFsPreviewEntry(
                                    store=st,
                                    sku=sk,
                                    url=u0,
                                    title=t0,
                                    error="",
                                    source="manual",
                                    monitor_url=u0,
                                    affiliate_url="",
                                )
                            )
                        else:
                            remaining_pairs.append((st, sk))

                    pairs = remaining_pairs
                    total = len(remaining_pairs) + len(manual_hits) + len(history_hits)
                    if progress_msg:
                        try:
                            await progress_msg.edit(
                                embed=_progress_embed(
                                    "resolve (monitor first)",
                                    len(manual_hits) + len(history_hits),
                                    total,
                                    monitor_hits=len(manual_hits) + len(history_hits),
                                    remaining=len(remaining_pairs),
                                )
                            )
                        except Exception:
                            pass

                    # Temporarily increase monitor lookup history (improves hit rate; reduces website scraping).
                    prev_hist = (self.config or {}).get("rs_fs_monitor_lookup_history_limit")
                    try:
                        self.config["rs_fs_monitor_lookup_history_limit"] = max(int(prev_hist or 0), 600)
                    except Exception:
                        self.config["rs_fs_monitor_lookup_history_limit"] = 600

                    # Stage 1: monitor lookup (history+manual overrides count as hits)
                    monitor_hits: List[rs_fs_sheet_sync.RsFsPreviewEntry] = list(history_hits) + list(manual_hits)
                    remaining: List[Tuple[str, str]] = []
                    g_obj = getattr(ch, "guild", None)
                    done = 0
                    errors = 0
                    # Build per-channel index once (avoid scanning history per SKU).
                    monitor_cache: Dict[str, Tuple[Optional[discord.TextChannel], Dict[str, Tuple[str, str]]]] = {}

                    import re as _re

                    def _id_like_field_name(name: str) -> bool:
                        n = (name or "").strip().lower()
                        if not n:
                            return False
                        hints = ("sku", "pid", "tcin", "asin", "upc", "item", "product", "model", "mpn", "id")
                        return any(h in n for h in hints)

                    async def _build_index(ch2: discord.TextChannel, *, limit_n: int) -> Dict[str, Tuple[str, str]]:
                        idx: Dict[str, Tuple[str, str]] = {}
                        base_name = self._normalize_monitor_channel_name(str(getattr(ch2, "name", "") or ""))
                        preferred_domains = self._preferred_store_domains(base_name)

                        def _all_urls(text: str) -> List[str]:
                            try:
                                return [u.strip() for u in _re.findall(r"(https?://[^\\s<>()]+)", text or "") if u.strip()]
                            except Exception:
                                return []

                        def _pick_url(cands: List[str]) -> str:
                            if not cands:
                                return ""
                            for u in cands:
                                try:
                                    host = (urlparse(u).netloc or "").lower()
                                except Exception:
                                    host = ""
                                if any(d in host for d in preferred_domains):
                                    return u
                            return cands[0]

                        async for mm in ch2.history(limit=limit_n):
                            embeds2 = getattr(mm, "embeds", None) or []
                            for ee in embeds2:
                                fields2 = getattr(ee, "fields", None) or []
                                title2 = str(getattr(ee, "title", "") or "").strip()
                                url_candidates: List[str] = []
                                u0 = str(getattr(ee, "url", "") or "").strip()
                                if u0:
                                    url_candidates.append(u0)
                                for ff in fields2:
                                    url_candidates.extend(_all_urls(str(getattr(ff, "value", "") or "")))
                                url_candidates.extend(_all_urls(str(getattr(ee, "description", "") or "")))
                                url2 = _pick_url([u for u in url_candidates if u])
                                # IMPORTANT: never use URL as "title". Leave it blank if we can't extract a title.
                                if not title2:
                                    title2 = ""

                                for ff in fields2:
                                    name = str(getattr(ff, "name", "") or "").strip()
                                    if not _id_like_field_name(name):
                                        continue
                                    val = str(getattr(ff, "value", "") or "").strip()
                                    if not val:
                                        continue
                                    cleaned = self._clean_sku_text(val)
                                    if cleaned and len(cleaned) >= 6 and cleaned not in idx:
                                        idx[cleaned] = (title2, url2)
                                    digits = "".join([c for c in cleaned if c.isdigit()])
                                    if digits and len(digits) >= 6 and digits not in idx:
                                        idx[digits] = (title2, url2)
                        return idx

                    # Use a larger history limit for manual runs (better hit rate).
                    try:
                        limit_mon = max(int((self.config or {}).get("rs_fs_monitor_lookup_history_limit") or 0), 2000)
                    except Exception:
                        limit_mon = 2000

                    for st, sk in pairs:
                        found = None
                        if self._rs_fs_monitor_lookup_enabled():
                            ch_name = self._monitor_channel_name_for_store(st)
                            if ch_name:
                                base = self._normalize_monitor_channel_name(ch_name)
                                if base not in monitor_cache:
                                    ch2 = await self._resolve_monitor_channel_for_store(st, guild=g_obj)
                                    idx = {}
                                    if ch2:
                                        try:
                                            idx = await _build_index(ch2, limit_n=limit_mon)
                                        except Exception:
                                            idx = {}
                                    monitor_cache[base] = (ch2, idx)
                                ch2, idx = monitor_cache.get(base) or (None, {})
                                target_clean = self._clean_sku_text(sk)
                                target_digits = "".join([c for c in target_clean if c.isdigit()])
                                hit = idx.get(target_clean) or (idx.get(target_digits) if target_digits else None)
                                if hit:
                                    t2, u2 = hit
                                    found = rs_fs_sheet_sync.RsFsPreviewEntry(
                                        store=st,
                                        sku=sk,
                                        url=u2 or "",
                                        title=(t2 or "").strip(),
                                        error=("" if (t2 or "").strip() else "title not found (monitor embed)") if (u2 or "").strip() else "no url in monitor embed",
                                        source=f"monitor:{getattr(ch2,'name',ch_name)}",
                                        monitor_url=(u2 or "").strip(),
                                        affiliate_url="",
                                    )

                        if found:
                            monitor_hits.append(found)
                        else:
                            remaining.append((st, sk))

                        done += 1
                        if progress_msg and (done % 5 == 0 or done == total):
                            try:
                                await progress_msg.edit(
                                    embed=_progress_embed(
                                        "monitor",
                                        len(monitor_hits),
                                        total,
                                        monitor_hits=len(monitor_hits),
                                        remaining=len(remaining),
                                    )
                                )
                            except Exception:
                                pass

                    # Stage 2: website fallback for anything not found in monitor channel.
                    offset_done = len(monitor_hits)

                    async def _on_web_progress(web_done: int, web_total: int, web_errors: int, entry) -> None:
                        if not progress_msg:
                            return
                        try:
                            await progress_msg.edit(
                                embed=_progress_embed(
                                    "website",
                                    offset_done + web_done,
                                    offset_done + web_total,
                                    monitor_hits=offset_done,
                                    remaining=max(0, offset_done + web_total - (offset_done + web_done)),
                                    web_errors=web_errors,
                                )
                            )
                        except Exception:
                            return

                    if remaining:
                        web_entries = await rs_fs_sheet_sync.build_preview_entries(
                            remaining,
                            self.config,
                            on_progress=_on_web_progress if progress_msg else None,
                        )
                    else:
                        web_entries = []

                    raw_entries = list(monitor_hits) + list(web_entries)

                    # Stage 3: affiliate links (plain URL for sheet)
                    if progress_msg:
                        try:
                            await progress_msg.edit(embed=_progress_embed("affiliate", total, total, monitor_hits=len(monitor_hits), remaining=0))
                        except Exception:
                            pass

                    entries = list(raw_entries or [])
                    # Ensure all items from the original parse are included in sync, even if unresolved
                    # This prevents sync_rows_mirror from deleting rows that should exist
                    entries_by_key: Dict[str, rs_fs_sheet_sync.RsFsPreviewEntry] = {}
                    for e in entries:
                        st = str(getattr(e, "store", "") or "").strip()
                        sk = str(getattr(e, "sku", "") or "").strip()
                        if st and sk:
                            entries_by_key[self._rsfs_key_store_sku(st, sk)] = e
                    # Add placeholders for any items that weren't resolved
                    for it in items:
                        st = str(getattr(it, "store", "") or "").strip()
                        sk = str(getattr(it, "sku", "") or "").strip()
                        if not (st and sk):
                            continue
                        key = self._rsfs_key_store_sku(st, sk)
                        if key not in entries_by_key:
                            # Add placeholder entry for unresolved item
                            entries_by_key[key] = rs_fs_sheet_sync.RsFsPreviewEntry(
                                store=st,
                                sku=sk,
                                url="",
                                title="",
                                error="",
                                source="unresolved",
                                monitor_url="",
                                affiliate_url="",
                            )
                    entries = list(entries_by_key.values())
                    try:
                        rewrite_enabled = bool(self.config.get("affiliate_rewrite_enabled", True))
                    except Exception:
                        rewrite_enabled = True
                    if entries:
                        try:
                            url_list: List[str] = []
                            for e in entries:
                                u0 = (getattr(e, "monitor_url", "") or getattr(e, "url", "") or "").strip()
                                if u0:
                                    url_list.append(u0)
                            aff_map: Dict[str, str] = {}
                            if rewrite_enabled and url_list:
                                seen_u: Set[str] = set()
                                unique_urls: List[str] = []
                                for u in url_list:
                                    if u in seen_u:
                                        continue
                                    seen_u.add(u)
                                    unique_urls.append(u)
                                mapped, _notes = await affiliate_rewriter.compute_affiliate_rewrites_plain(self.config, unique_urls)
                                aff_map = {str(k or "").strip(): str(v or "").strip() for k, v in (mapped or {}).items()}

                            enriched: List[rs_fs_sheet_sync.RsFsPreviewEntry] = []
                            for e in entries:
                                u0 = (getattr(e, "monitor_url", "") or getattr(e, "url", "") or "").strip()
                                prev_aff = str(getattr(e, "affiliate_url", "") or "").strip()
                                aff = (aff_map.get(u0) or "").strip() if u0 else ""
                                if not aff:
                                    aff = prev_aff
                                enriched.append(
                                    rs_fs_sheet_sync.RsFsPreviewEntry(
                                        store=getattr(e, "store", "") or "",
                                        sku=getattr(e, "sku", "") or "",
                                        url=getattr(e, "url", "") or "",
                                        title=getattr(e, "title", "") or "",
                                        error=getattr(e, "error", "") or "",
                                        source=getattr(e, "source", "") or "",
                                        monitor_url=u0,
                                        affiliate_url=aff,
                                    )
                                )
                            entries = enriched
                        except Exception:
                            pass

                    # Stage 4: mirror sync (single call)
                    if progress_msg:
                        try:
                            await progress_msg.edit(embed=_progress_embed("sheet sync", total, total, monitor_hits=len(monitor_hits), remaining=0))
                        except Exception:
                            pass

                    rows = [[e.store, e.sku, e.title, e.affiliate_url, e.monitor_url] for e in entries]
                    ok, msg, added, updated, deleted = await self._rs_fs_sheet.sync_rows_mirror(rows)

                    # Write back to History cache + update Current List with resolved columns.
                    try:
                        now_iso = rs_fs_sheet_sync.RsFsSheetSync._utc_now_iso()  # type: ignore[attr-defined]
                    except Exception:
                        now_iso = ""
                    resolved_by_key: Dict[str, Dict[str, str]] = {}
                    history_rows: List[List[str]] = []
                    for e in (entries or []):
                        st0 = str(getattr(e, "store", "") or "").strip()
                        sk0 = str(getattr(e, "sku", "") or "").strip()
                        if not (st0 and sk0):
                            continue
                        k0 = self._rsfs_key_store_sku(st0, sk0)
                        u0 = str(getattr(e, "monitor_url", "") or getattr(e, "url", "") or "").strip()
                        t0 = str(getattr(e, "title", "") or "").strip()
                        a0 = str(getattr(e, "affiliate_url", "") or "").strip()
                        src0 = str(getattr(e, "source", "") or "").strip()
                        try:
                            rid0 = int(rid_by_key.get(self._rs_fs_override_key(st0, sk0)) or 0)
                        except Exception:
                            rid0 = 0
                        resolved_by_key[k0] = {
                            "title": t0,
                            "url": u0,
                            "affiliate_url": a0,
                            "source": src0,
                            "last_release_id": str(rid0 or ""),
                        }
                        history_rows.append([st0, sk0, t0, u0, a0, "", now_iso, str(rid0 or ""), src0])

                    if history_rows:
                        try:
                            ok_h, msg_h, added_h, updated_h = await self._rs_fs_sheet.upsert_history_rows(history_rows)
                            try:
                                print(
                                    f"{Colors.CYAN}[RS-FS History]{Colors.RESET} upsert ok={ok_h} added={added_h} updated={updated_h} msg={msg_h}"
                                )
                            except Exception:
                                pass
                        except Exception:
                            pass
                    try:
                        await self._rsfs_write_current_list(merged_text, resolved_by_key=resolved_by_key, reason="rsfsrun-end")
                    except Exception:
                        pass

                    if ok:
                        # Summary embed
                        try:
                            n_manual = sum(1 for e in entries if str(getattr(e, "source", "") or "").startswith("manual"))
                            n_monitor = sum(1 for e in entries if str(getattr(e, "source", "") or "").startswith("monitor"))
                            n_web = sum(1 for e in entries if str(getattr(e, "source", "") or "").strip() == "website")
                            n_blocked = sum(1 for e in entries if "blocked" in str(getattr(e, "error", "") or "").lower())
                            summ = discord.Embed(title="RS-FS Sheet Sync (mirror)", color=discord.Color.green())
                            summ.add_field(name="Sheet changes", value=f"added `{added}`\nupdated `{updated}`\nremoved `{deleted}`", inline=True)
                            summ.add_field(name="Resolution", value=f"manual `{n_manual}`\nmonitor `{n_monitor}`\nwebsite `{n_web}`", inline=True)
                            if n_blocked:
                                summ.add_field(name="Blocked pages", value=str(n_blocked), inline=True)
                            await ctx.send(embed=summ, allowed_mentions=discord.AllowedMentions.none())
                        except Exception:
                            await ctx.send(
                                f"RS-FS Sheet (mirror): ✅ added {added}, updated {updated}, removed {deleted}.",
                                allowed_mentions=discord.AllowedMentions.none(),
                            )

                        # Management output: one embed per store with release_id + sku (+ short title) and
                        # a copy-ready /removereleaseid command.
                        try:
                            by_store: Dict[str, List[rs_fs_sheet_sync.RsFsPreviewEntry]] = {}
                            for e in entries or []:
                                st = str(getattr(e, "store", "") or "").strip() or "Unknown"
                                by_store.setdefault(st, []).append(e)

                            # Collect "unparseable" records as: release IDs present in the merged list that
                            # did NOT parse into a (store, sku) item for the public sheet.
                            parsed_ids: Set[int] = set()
                            try:
                                parsed_ids = {int(v) for v in (rid_by_key or {}).values() if int(v or 0) > 0}
                            except Exception:
                                parsed_ids = set()
                            recs0 = zephyr_release_feed_parser.parse_release_feed_records(merged_text) or []
                            unparseable_recs = [
                                r
                                for r in recs0
                                if int(getattr(r, "release_id", 0) or 0) > 0 and int(getattr(r, "release_id", 0) or 0) not in parsed_ids
                            ]
                            # Deduplicate by release_id (keep first).
                            seen_rid: Set[int] = set()
                            unparseable_recs2 = []
                            for r in unparseable_recs:
                                ridv = int(getattr(r, "release_id", 0) or 0)
                                if ridv in seen_rid:
                                    continue
                                seen_rid.add(ridv)
                                unparseable_recs2.append(r)
                            unparseable_recs = unparseable_recs2

                            # Sort stores for stability.
                            for st in sorted(by_store.keys(), key=lambda s: s.lower()):
                                es = by_store.get(st) or []

                                def _rid_for(e2) -> int:
                                    try:
                                        return int(rid_by_key.get(self._rs_fs_override_key(getattr(e2, "store", ""), getattr(e2, "sku", ""))) or 0)
                                    except Exception:
                                        return 0

                                es = sorted(es, key=lambda e2: (_rid_for(e2) or 10**9, str(getattr(e2, "sku", "") or "")))

                                # Build formatted lines with command directly under each product
                                formatted_lines: List[str] = []
                                for e2 in es:
                                    sku2 = str(getattr(e2, "sku", "") or "").strip()
                                    title2 = str(getattr(e2, "title", "") or "").strip()
                                    rid2 = _rid_for(e2)
                                    if len(title2) > 60:
                                        title2 = title2[:57] + "..."
                                    info = f"`{rid2}` `{sku2}`" + (f" — {title2}" if title2 else "")
                                    if rid2:
                                        # Format: product info line followed immediately by remove command
                                        formatted_lines.append(f"{info}\n```\n/removereleaseid release_id: {rid2}\n```")
                                    else:
                                        formatted_lines.append(info)

                                # Split across multiple embeds if needed.
                                part = 1
                                cur_lines: List[str] = []

                                def _render_desc(lines: List[str]) -> str:
                                    return "\n\n".join(lines).strip()

                                for line in formatted_lines:
                                    next_lines = cur_lines + [line]
                                    desc_try = _render_desc(next_lines)
                                    if len(desc_try) > 3800 and cur_lines:
                                        emb2 = discord.Embed(
                                            title=f"RS-FS Remove Helper — {st}" + (f" (part {part})" if part > 1 else ""),
                                            color=discord.Color.dark_teal(),
                                        )
                                        emb2.description = _render_desc(cur_lines)
                                        emb2.set_footer(text="Use the code block copy button")
                                        await ctx.send(embed=emb2, allowed_mentions=discord.AllowedMentions.none())
                                        part += 1
                                        cur_lines = [line]
                                    else:
                                        cur_lines.append(line)

                                if cur_lines:
                                    emb2 = discord.Embed(
                                        title=f"RS-FS Remove Helper — {st}" + (f" (part {part})" if part > 1 else ""),
                                        color=discord.Color.dark_teal(),
                                    )
                                    emb2.description = _render_desc(cur_lines)
                                    emb2.set_footer(text="Use the code block copy button")
                                    await ctx.send(embed=emb2, allowed_mentions=discord.AllowedMentions.none())

                            if unparseable_recs:
                                up_lines: List[str] = []
                                for r in unparseable_recs[:25]:
                                    rid2 = int(getattr(r, "release_id", 0) or 0)
                                    sk2 = str(getattr(r, "sku", "") or "").strip()
                                    st2 = str(getattr(r, "store", "") or "").strip()
                                    is_sku2 = bool(getattr(r, "is_sku_candidate", True))
                                    kind = "non-SKU" if not is_sku2 else ("unknown-store" if not st2 else "unknown")
                                    # Format: product info line followed immediately by remove command
                                    info_line = f"`{rid2}` `{kind}` {sk2}"
                                    if rid2:
                                        up_lines.append(f"{info_line}\n```\n/removereleaseid release_id: {rid2}\n```")
                                    else:
                                        up_lines.append(info_line)
                                emb_u = discord.Embed(
                                    title="RS-FS Remove Helper — Unparseable (could not parse store/SKU)",
                                    color=discord.Color.orange(),
                                )
                                desc = "\n\n".join(up_lines).strip()
                                emb_u.description = desc
                                emb_u.set_footer(text="These release IDs could not be mapped to a store/SKU automatically • Use code block copy")
                                await ctx.send(embed=emb_u, allowed_mentions=discord.AllowedMentions.none())
                        except Exception:
                            pass

                        # Offer manual resolution for any items missing titles or URLs (regardless of source)
                        needs_manual = [
                            e
                            for e in entries
                            if (
                                not str(getattr(e, "title", "") or "").strip()
                                or not str(getattr(e, "monitor_url", "") or getattr(e, "url", "") or "").strip()
                                or "blocked" in str(getattr(e, "error", "") or "").lower()
                                or "title not found" in str(getattr(e, "error", "") or "").lower()
                            )
                        ]
                        if needs_manual:
                            view = _RsFsManualResolveView(self, ctx, needs_manual)  # type: ignore[name-defined]
                            await ctx.send(
                                embed=view._render_embed(),
                                view=view,
                                allowed_mentions=discord.AllowedMentions.none(),
                            )
                        else:
                            await ctx.send("✅ RS-FS LIVE sync finished. Check the sheet.", allowed_mentions=discord.AllowedMentions.none())
                    else:
                        await ctx.send(f"❌ RS-FS live run failed: {msg}", allowed_mentions=discord.AllowedMentions.none())
                finally:
                    # Restore monitor lookup limit and manual-run guard
                    try:
                        if prev_hist is None:
                            (self.config or {}).pop("rs_fs_monitor_lookup_history_limit", None)
                        else:
                            self.config["rs_fs_monitor_lookup_history_limit"] = prev_hist
                    except Exception:
                        pass
                    self._rs_fs_manual_run_in_progress = prev_manual
            except Exception as e:
                await ctx.send(f"❌ RS-FS live run failed: {str(e)[:200]}")

        @self.bot.command(name="rsfsmonitorscan", aliases=["rsfsmonitor", "rsfsmonitors"])
        async def rsfs_monitor_scan(ctx, *category_ids: str):
            """
            Scan monitor categories in this guild and store monitor channel IDs in config.json.
            This makes monitor lookup deterministic (uses channel_id instead of name matching).

            Usage:
              !rsfsmonitorscan 1350953333069713528 1411757054908960819
            """
            try:
                if not getattr(ctx.author, "guild_permissions", None) or not ctx.author.guild_permissions.administrator:
                    await ctx.send(
                        embed=_rsfs_embed("RS-FS Monitor Scan", status="❌ admins only", color=discord.Color.red()),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return

                g = getattr(ctx, "guild", None)
                if not g:
                    await ctx.send(
                        embed=_rsfs_embed("RS-FS Monitor Scan", status="❌ must be run in a guild", color=discord.Color.red()),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return

                ids: List[int] = []
                if category_ids:
                    for s in category_ids:
                        try:
                            ids.append(int(str(s or "").strip()))
                        except Exception:
                            continue
                else:
                    raw = (self.config or {}).get("rs_fs_monitor_category_ids")
                    if isinstance(raw, list):
                        for x in raw:
                            try:
                                ids.append(int(str(x or "").strip()))
                            except Exception:
                                continue
                ids = [i for i in ids if i > 0]
                if not ids:
                    await ctx.send(
                        embed=_rsfs_embed(
                            "RS-FS Monitor Scan",
                            status="❌ missing category IDs",
                            description="Provide category IDs, or set `rs_fs_monitor_category_ids` in `RSForwarder/config.json`.",
                            color=discord.Color.red(),
                        ),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return

                found: Dict[str, int] = {}
                # Iterate channels under those categories
                for ch in getattr(g, "channels", []) or []:
                    try:
                        if not isinstance(ch, discord.TextChannel):
                            continue
                        cat_id = int(getattr(ch, "category_id", 0) or 0)
                        if cat_id not in ids:
                            continue
                        base = self._normalize_monitor_channel_name(str(getattr(ch, "name", "") or ""))
                        if not base.endswith("-monitor"):
                            continue
                        found[base] = int(getattr(ch, "id", 0) or 0)
                    except Exception:
                        continue

                if not found:
                    await ctx.send(
                        embed=_rsfs_embed(
                            "RS-FS Monitor Scan",
                            status="❌ no monitor channels found",
                            description="No `*-monitor` channels were found under the provided categories.",
                            color=discord.Color.red(),
                        ),
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    return

                # Persist mapping
                self.config["rs_fs_monitor_category_ids"] = [str(i) for i in ids]
                self.config["rs_fs_monitor_channel_ids"] = {k: int(v) for k, v in sorted(found.items()) if int(v) > 0}
                try:
                    self.save_config()
                    self.load_config()
                except Exception:
                    pass

                # Report summary (truncate)
                sample_lines = [f"`{k}` → `{v}`" for k, v in sorted(found.items())][:20]
                more = max(0, len(found) - len(sample_lines))
                await ctx.send(
                    embed=_rsfs_embed(
                        "RS-FS Monitor Scan",
                        status="✅ saved channel ids",
                        color=discord.Color.green(),
                        fields=[
                            ("Categories", ", ".join([f"`{i}`" for i in ids]), False),
                            ("Channels found", str(len(found)), True),
                            ("Sample", "\n".join(sample_lines) + (f"\n… and {more} more" if more else ""), False),
                        ],
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except Exception as e:
                await ctx.send(
                    embed=_rsfs_embed(
                        "RS-FS Monitor Scan",
                        status=f"❌ failed: {str(e)[:200]}",
                        color=discord.Color.red(),
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
        
        @self.bot.command(name='rsremove', aliases=['remove'])
        async def remove_channel(ctx, source_channel: discord.TextChannel = None):
            """Remove a channel configuration
            
            Usage: !rsremove <#channel|channel_id>
            
            Example: !rsremove #personal-deals
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsremove <#channel|channel_id>`\n"
                    "**Example:** `!rsremove #personal-deals`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            channels = self.config.get("channels", [])
            original_count = len(channels)
            self.config["channels"] = [
                ch for ch in channels 
                if str(ch.get("source_channel_id")) != str(source_channel_id)
            ]
            
            if len(self.config["channels"]) < original_count:
                # Remove secret webhook mapping too (server-only)
                self._delete_destination_webhook_secret(source_channel_id)
                self.save_config()
                self.load_config()
                await ctx.send(
                    f"✅ **Channel removed!**\n"
                    f"`{channel_name}` ({source_channel_id}) has been removed from configuration."
                )
            else:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) not found in configuration."
                )
        
        @self.bot.command(name='rstest', aliases=['test'])
        async def test_forward(ctx, source_channel_id: str = None, limit: int = 1):
            """Test forwarding by forwarding recent messages from a source channel
            
            Usage: !rstest [source_channel_id] [limit]
            Example: !rstest 1446174861931188387 5
            """
            if not source_channel_id:
                # If no channel specified, test with current channel
                source_channel_id = str(ctx.channel.id)
            
            channel_config = self.get_channel_config(source_channel_id)
            if not channel_config:
                await ctx.send(f"❌ Channel `{source_channel_id}` is not configured. Use `!add` to add it first.")
                return
            
            webhook_url = channel_config.get("destination_webhook_url", "").strip()
            if not webhook_url:
                await ctx.send(f"❌ No webhook configured for channel `{source_channel_id}`")
                return
            
            try:
                source_channel = self.bot.get_channel(int(source_channel_id))
                if not source_channel:
                    await ctx.send(f"❌ Cannot access channel `{source_channel_id}`. Bot may not have permission.")
                    return
                
                await ctx.send(f"🔄 Fetching last {limit} message(s) from <#{source_channel_id}>...")
                
                forwarded_count = 0
                async for message in source_channel.history(limit=limit):
                    # Don't skip bot messages - forward ALL messages
                    # Only skip our own bot's messages to avoid loops
                    if message.author.id == self.bot.user.id:
                        continue
                    
                    await self.forward_message(message, source_channel_id, channel_config)
                    forwarded_count += 1
                
                if forwarded_count > 0:
                    await ctx.send(f"✅ Successfully forwarded {forwarded_count} message(s) to webhook!")
                else:
                    await ctx.send(f"ℹ️ No messages found to forward (only skipped our own bot's messages)")
                    
            except discord.Forbidden:
                await ctx.send(f"❌ Bot doesn't have permission to read messages in channel `{source_channel_id}`")
            except Exception as e:
                await ctx.send(f"❌ Error: {str(e)}")

        async def _get_or_create_test_webhook(channel: discord.TextChannel) -> Optional[str]:
            """
            Create/reuse a webhook in the given channel (requires Manage Webhooks permission).
            Returns webhook URL or None.
            """
            try:
                hooks = await channel.webhooks()
            except Exception:
                hooks = []
            me_id = int(self.bot.user.id) if self.bot.user else 0
            for h in hooks:
                try:
                    if h.user and me_id and int(h.user.id) == me_id:
                        return str(h.url)
                except Exception:
                    continue
            try:
                wh = await channel.create_webhook(name="RSForwarder Test")
                return str(wh.url)
            except Exception:
                return None

        @self.bot.command(name='rstestall', aliases=['testall'])
        async def test_forward_all(ctx, test_channel_id: str = "1446372213757313034", limit: int = 1):
            """
            Test forwarding for ALL configured channels by sending the most recent message(s)
            from each source channel into the given test channel via an auto-created webhook.

            Usage: !rstestall [test_channel_id] [limit]
            Example: !rstestall 1446372213757313034 1
            """
            # Admin-only (this can spam the test channel)
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ Admins only.")
                return

            try:
                test_ch = self.bot.get_channel(int(str(test_channel_id).strip()))
            except Exception:
                test_ch = None
            if not isinstance(test_ch, discord.TextChannel):
                await ctx.send(f"❌ Test channel not found or not accessible: `{test_channel_id}`")
                return

            webhook_url = await _get_or_create_test_webhook(test_ch)
            if not webhook_url:
                await ctx.send("❌ Could not create/reuse a webhook in the test channel. Check **Manage Webhooks** permission.")
                return

            channels = self.config.get("channels", []) or []
            if not channels:
                await ctx.send("❌ No channels configured in RSForwarder/config.json")
                return

            try:
                limit_i = int(limit)
            except Exception:
                limit_i = 1
            limit_i = max(1, min(limit_i, 5))

            await ctx.send(f"🧪 Testing `{len(channels)}` channel(s) → <#{test_channel_id}> (limit={limit_i}) ...")

            ok = 0
            fail = 0
            for chcfg in channels:
                src_id = str((chcfg or {}).get("source_channel_id", "")).strip()
                if not src_id:
                    continue
                src = self.bot.get_channel(int(src_id)) if src_id.isdigit() else None
                if not isinstance(src, discord.TextChannel):
                    fail += 1
                    continue

                # Clone config but override destination webhook to the test channel webhook
                tmp_cfg = dict(chcfg)
                tmp_cfg["destination_webhook_url"] = webhook_url
                tmp_cfg["source_channel_name"] = str((chcfg or {}).get("source_channel_name") or src.name)

                sent_any = False
                try:
                    async for message in src.history(limit=limit_i):
                        if self.bot.user and message.author.id == self.bot.user.id:
                            continue
                        await self.forward_message(message, src_id, tmp_cfg)
                        sent_any = True
                        break
                except Exception:
                    sent_any = False

                if sent_any:
                    ok += 1
                else:
                    fail += 1

            await ctx.send(f"✅ rstestall complete: ok={ok} fail={fail}")
        
        @self.bot.command(name='rsstatus', aliases=['status'])
        async def bot_status(ctx):
            """Show bot status and configuration"""
            embed = discord.Embed(
                title="🤖 RS Forwarder Bot Status",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            # Bot info
            embed.add_field(
                name="Bot Status",
                value=f"✅ Online\nUser: {self.bot.user}\nUptime: {self._get_uptime()}",
                inline=False
            )
            
            # RS Server info
            guild_id = self._rs_server_guild_id()
            if self.rs_guild:
                embed.add_field(
                    name="RS Server",
                    value=f"✅ Connected\nName: {self.rs_guild.name}\nID: {guild_id}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="RS Server",
                    value=f"❌ Not found\nID: {guild_id}",
                    inline=True
                )

            # Neo Test Server info (diagnostic)
            test_gid = self._test_server_guild_id()
            if test_gid:
                test_g = self.bot.get_guild(int(test_gid))
                if test_g:
                    embed.add_field(
                        name="Neo Test Server",
                        value=f"✅ Connected\nName: {test_g.name}\nID: {test_gid}",
                        inline=True,
                    )
                else:
                    embed.add_field(
                        name="Neo Test Server",
                        value=f"❌ Not found\nID: {test_gid}",
                        inline=True,
                    )
            
            # Icon status
            if self.rs_icon_url:
                embed.add_field(
                    name="RS Server Icon",
                    value=f"✅ Loaded\n[View Icon]({self.rs_icon_url})",
                    inline=True
                )
            else:
                embed.add_field(
                    name="RS Server Icon",
                    value="❌ Not available",
                    inline=True
                )
            
            # Stats
            embed.add_field(
                name="Statistics",
                value=f"Messages Forwarded: {self.stats['messages_forwarded']}\nErrors: {self.stats['errors']}",
                inline=False
            )
            
            # Channels
            channels = self.config.get("channels", [])
            channels_info = []
            for ch in channels:
                try:
                    if bool((ch or {}).get("repost_in_place")):
                        status = "♻️"
                    else:
                        status = "✅" if (ch or {}).get("destination_webhook_url") else "❌"
                except Exception:
                    status = "❌"
                channels_info.append(f"{status} {ch.get('source_channel_name', 'unknown')}")
            
            embed.add_field(
                name=f"Configured Channels ({len(channels)})",
                value="\n".join(channels_info) if channels_info else "No channels configured",
                inline=False
            )
            
            embed.set_footer(text="Use !rscommands for command list")
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsfetchicon', aliases=['fetchicon'])
        async def fetch_icon(ctx):
            """Manually fetch RS Server icon"""
            guild_id = self._rs_server_guild_id()
            if not guild_id:
                await ctx.send("❌ No `guild_id` configured in config.json")
                return
            
            await ctx.send(f"🔄 Fetching RS Server icon for guild {guild_id}...")
            
            # Try guild object first
            self.rs_guild = self.bot.get_guild(guild_id)
            if self.rs_guild:
                self.rs_icon_url = self._get_guild_icon_url(self.rs_guild)
                if self.rs_icon_url:
                    await ctx.send(f"✅ Icon fetched from guild object: {self.rs_icon_url[:50]}...")
                    return
            
            # Try API
            icon_fetched = await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
            if icon_fetched and self.rs_icon_url:
                await ctx.send(f"✅ Icon fetched via API and saved to config: {self.rs_icon_url[:50]}...")
            else:
                await ctx.send(f"❌ Failed to fetch icon. Check console for details.")
        
        @self.bot.command(name='rsstartadminbot', aliases=['startadminbot', 'startadmin'])
        async def start_admin_bot(ctx):
            """Start RSAdminBot remotely on the server (admin only)"""
            # Check if user has admin permissions
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            
            # Prefer local-exec when RSForwarder runs on the Ubuntu host.
            if self._is_local_exec():
                await ctx.send("🔄 Starting RSAdminBot locally on this server...")
                ok, out = self._run_local_botctl("start")
                if ok:
                    await ctx.send("✅ **RSAdminBot start requested (local)**")
                    if out:
                        await ctx.send(f"```{out[:1500]}```")
                else:
                    await ctx.send(f"❌ Failed to start RSAdminBot (local):\n```{out[:1500]}```")
                return

            await ctx.send("🔄 Starting RSAdminBot on remote server...")
            
            try:
                # subprocess/shlex imported at module level
                
                # Get SSH config from oraclekeys
                oraclekeys_path = Path(__file__).parent.parent / "oraclekeys"
                servers_json = oraclekeys_path / "servers.json"
                
                if not servers_json.exists():
                    await ctx.send("❌ Could not find servers.json configuration")
                    return
                
                import json
                with open(servers_json, 'r') as f:
                    servers = json.load(f)
                
                if not servers:
                    await ctx.send("❌ No servers configured")
                    return
                
                server = servers[0]
                remote_user = server.get("user", "rsadmin")
                remote_host = server.get("host", "")
                ssh_key = server.get("key")
                
                if not remote_host:
                    await ctx.send("❌ Server host not configured")
                    return
                
                # Build SSH command - check multiple locations for key
                ssh_key_path = None
                if ssh_key:
                    if Path(ssh_key).is_absolute():
                        ssh_key_path = Path(ssh_key)
                    else:
                        # Try oraclekeys folder first
                        ssh_key_path = oraclekeys_path / ssh_key
                        # If not found, try RSAdminBot folder (where key might also be)
                        if not ssh_key_path.exists():
                            rsadminbot_path = Path(__file__).parent.parent / "RSAdminBot" / ssh_key
                            if rsadminbot_path.exists():
                                ssh_key_path = rsadminbot_path
                
                # Fix SSH key permissions on Windows (required for SSH to work)
                if ssh_key_path and ssh_key_path.exists() and platform.system() == "Windows":
                    self._fix_ssh_key_permissions(ssh_key_path)
                
                if ssh_key and not (ssh_key_path and ssh_key_path.exists()):
                    await ctx.send(f"❌ SSH key not found: {ssh_key}\nChecked: oraclekeys/{ssh_key} and RSAdminBot/{ssh_key}")
                    return
                
                ssh_base = ["ssh"]
                if ssh_key_path and ssh_key_path.exists():
                    ssh_base.extend(["-i", str(ssh_key_path)])
                # Add options for better connection handling (match RSAdminBot)
                ssh_base.extend([
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10"
                ])
                
                # Use canonical botctl.sh script to start RSAdminBot
                botctl_path = "/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh"
                start_cmd = f"bash {botctl_path} start rsadminbot"
                
                # Escape command for bash -lc
                escaped_cmd = shlex.quote(start_cmd)
                
                # Build command (no -t flag needed, script handles everything)
                cmd = ssh_base + ["-o", "ConnectTimeout=10", f"{remote_user}@{remote_host}", "bash", "-lc", escaped_cmd]
                
                print(f"{Colors.CYAN}[RSForwarder] Starting RSAdminBot remotely using botctl.sh...{Colors.RESET}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, shell=False, encoding='utf-8', errors='replace')
                
                if result.returncode == 0:
                    # Script handles verification internally
                    output = result.stdout or result.stderr or ""
                    if "SUCCESS" in output or "active" in output.lower():
                        await ctx.send("✅ **RSAdminBot started successfully on remote server!**")
                        print(f"{Colors.GREEN}[RSForwarder] RSAdminBot started successfully{Colors.RESET}")
                    else:
                        await ctx.send(f"⚠️ RSAdminBot start completed but verification unclear:\n```{output[:300]}```")
                        print(f"{Colors.YELLOW}[RSForwarder] RSAdminBot start completed: {output[:200]}{Colors.RESET}")
                else:
                    error_msg = result.stderr or result.stdout or "Unknown error"
                    await ctx.send(f"❌ Failed to start RSAdminBot:\n```{error_msg[:500]}```")
                    print(f"{Colors.RED}[RSForwarder] Failed to start RSAdminBot: {error_msg[:200]}{Colors.RESET}")
                    
            except subprocess.TimeoutExpired:
                await ctx.send("❌ Command timed out - RSAdminBot may still be starting")
            except FileNotFoundError:
                await ctx.send("❌ SSH not found - make sure SSH is installed")
            except Exception as e:
                await ctx.send(f"❌ Error: {str(e)[:500]}")
                print(f"{Colors.RED}[RSForwarder] Error starting RSAdminBot: {e}{Colors.RESET}")
        
        @self.bot.command(name='rsrestartadminbot', aliases=['restartadminbot', 'restartadmin', 'restart'])
        async def restart_admin_bot(ctx):
            """Restart RSAdminBot remotely on the server (admin only)"""
            # Check if user has admin permissions
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            
            # Prefer local-exec when RSForwarder runs on the Ubuntu host.
            if self._is_local_exec():
                await ctx.send("🔄 Restarting RSAdminBot locally on this server...")
                ok, out = self._run_local_botctl("restart")
                if ok:
                    await ctx.send("✅ **RSAdminBot restart requested (local)**")
                    if out:
                        await ctx.send(f"```{out[:1500]}```")
                else:
                    await ctx.send(f"❌ Failed to restart RSAdminBot (local):\n```{out[:1500]}```")
                return

            await ctx.send("🔄 Restarting RSAdminBot on remote server...")
            
            try:
                # subprocess/shlex imported at module level
                
                # Get SSH config from oraclekeys
                oraclekeys_path = Path(__file__).parent.parent / "oraclekeys"
                servers_json = oraclekeys_path / "servers.json"
                
                if not servers_json.exists():
                    await ctx.send("❌ Could not find servers.json configuration")
                    return
                
                import json
                with open(servers_json, 'r') as f:
                    servers = json.load(f)
                
                if not servers:
                    await ctx.send("❌ No servers configured")
                    return
                
                server = servers[0]
                remote_user = server.get("user", "rsadmin")
                remote_host = server.get("host", "")
                ssh_key = server.get("key")
                
                if not remote_host:
                    await ctx.send("❌ Server host not configured")
                    return
                
                # Build SSH command - check multiple locations for key
                ssh_key_path = None
                if ssh_key:
                    if Path(ssh_key).is_absolute():
                        ssh_key_path = Path(ssh_key)
                    else:
                        # Try oraclekeys folder first
                        ssh_key_path = oraclekeys_path / ssh_key
                        # If not found, try RSAdminBot folder (where key actually is)
                        if not ssh_key_path.exists():
                            rsadminbot_path = Path(__file__).parent.parent / "RSAdminBot" / ssh_key
                            if rsadminbot_path.exists():
                                ssh_key_path = rsadminbot_path
                
                # Fix SSH key permissions on Windows (required for SSH to work)
                if ssh_key_path and ssh_key_path.exists() and platform.system() == "Windows":
                    self._fix_ssh_key_permissions(ssh_key_path)
                
                if ssh_key_path and not ssh_key_path.exists():
                    await ctx.send(f"❌ SSH key not found: {ssh_key_path}\nExpected at: oraclekeys/{ssh_key} or RSAdminBot/{ssh_key}")
                    return
                
                ssh_base = ["ssh"]
                if ssh_key_path and ssh_key_path.exists():
                    ssh_base.extend(["-i", str(ssh_key_path)])
                # Add options for better connection handling (match RSAdminBot)
                ssh_base.extend([
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10"
                ])
                
                # Use canonical botctl.sh script to restart RSAdminBot
                botctl_path = "/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh"
                restart_cmd = f"bash {botctl_path} restart rsadminbot"
                
                # Escape command for bash -lc
                escaped_cmd = shlex.quote(restart_cmd)
                
                # Build command (no -t flag needed, script handles everything)
                cmd = ssh_base + ["-o", "ConnectTimeout=10", f"{remote_user}@{remote_host}", "bash", "-lc", escaped_cmd]
                
                print(f"{Colors.CYAN}[RSForwarder] Restarting RSAdminBot remotely using botctl.sh...{Colors.RESET}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, shell=False, encoding='utf-8', errors='replace')
                
                if result.returncode == 0:
                    # Script handles verification internally
                    output = result.stdout or result.stderr or ""
                    if "SUCCESS" in output or "active" in output.lower():
                        await ctx.send("✅ **RSAdminBot restarted successfully on remote server!**")
                        print(f"{Colors.GREEN}[RSForwarder] RSAdminBot restarted successfully{Colors.RESET}")
                    else:
                        await ctx.send(f"⚠️ RSAdminBot restart completed but verification unclear:\n```{output[:300]}```")
                        print(f"{Colors.YELLOW}[RSForwarder] RSAdminBot restart completed: {output[:200]}{Colors.RESET}")
                else:
                    error_msg = result.stderr or result.stdout or "Unknown error"
                    await ctx.send(f"❌ Failed to restart RSAdminBot:\n```{error_msg[:500]}```")
                    print(f"{Colors.RED}[RSForwarder] Failed to restart RSAdminBot: {error_msg[:200]}{Colors.RESET}")
                    
            except subprocess.TimeoutExpired:
                await ctx.send("❌ Command timed out - RSAdminBot may still be restarting")
            except FileNotFoundError:
                await ctx.send("❌ SSH not found - make sure SSH is installed")
            except Exception as e:
                await ctx.send(f"❌ Error: {str(e)[:500]}")
                print(f"{Colors.RED}[RSForwarder] Error restarting RSAdminBot: {e}{Colors.RESET}")

        @self.bot.command(name="rsmavelylogin", aliases=["mavelylogin", "refreshtoken"])
        async def mavely_login(ctx, wait_seconds: str = None):
            """Start/ensure noVNC desktop and launch interactive Mavely cookie refresher (admin only).

            Usage:
              !rsmavelylogin
              !rsmavelylogin 900

            Notes:
            - This does NOT log in automatically; it opens a browser on the server desktop.
            - You connect via SSH tunnel to noVNC and log in manually.
            """
            if not self._is_mavely_admin_ctx(ctx):
                await ctx.send("❌ You don't have permission to use this command.")
                return

            if not self._is_local_exec():
                await ctx.send("❌ This command only works when RSForwarder is running on the Linux host (Oracle).")
                return

            try:
                wait_s = int((wait_seconds or "").strip() or "900")
            except Exception:
                wait_s = 900
            wait_s = max(60, min(wait_s, 3600))

            # Auto-enroll invoker for alerts + DM admin (guild-only).
            try:
                if getattr(ctx, "guild", None) is not None:
                    self._ensure_mavely_user(int(ctx.author.id))
            except Exception:
                pass

            # Best effort: delete the command message (keeps channels clean)
            try:
                if getattr(ctx, "guild", None) is not None:
                    await ctx.message.delete()
            except Exception:
                pass

            channel_ack = None
            try:
                if getattr(ctx, "guild", None) is not None:
                    channel_ack = await ctx.send("🔄 Starting noVNC desktop + launching Mavely login browser... I’ll DM you the tunnel + URL.")
            except Exception:
                channel_ack = None

            info, err = await asyncio.to_thread(novnc_stack.ensure_novnc, self.config)
            if err or not info:
                await ctx.send(f"❌ noVNC start failed: {str(err)[:500]}")
                return

            pid, log_path, err2 = await asyncio.to_thread(
                novnc_stack.start_cookie_refresher,
                self.config,
                display=str(info.get("display") or ":99"),
                wait_login_s=int(wait_s),
            )
            if err2:
                await ctx.send(f"❌ Failed to launch cookie refresher: {str(err2)[:500]}")
                return

            web_port = int(info.get("web_port") or 6080)
            url_path = str(info.get("url_path") or "/vnc.html")

            msg = self._build_tunnel_instructions(web_port, url_path)
            if pid:
                msg += f"\n\nCookie refresher PID: `{pid}`"
            if log_path:
                msg += f"\nRefresher log (server path): `{log_path}`"

            # Prefer DM. If DM fails, fall back to channel.
            sent_dm = False
            try:
                await ctx.author.send(msg)
                sent_dm = True
            except Exception:
                sent_dm = False

            if getattr(ctx, "guild", None) is not None:
                try:
                    if sent_dm:
                        done = await ctx.send("✅ Sent you a DM with the noVNC tunnel + URL. Run `!rsmavelycheck` after you log in.")
                    else:
                        done = await ctx.send(msg)
                    try:
                        await done.delete(delay=20)  # type: ignore[arg-type]
                    except Exception:
                        pass
                except Exception:
                    pass
                try:
                    if channel_ack:
                        await channel_ack.delete(delay=5)  # type: ignore[arg-type]
                except Exception:
                    pass
            else:
                # DM channel: we already sent details (or failed). Keep a short confirmation.
                if sent_dm:
                    await ctx.send("✅ Sent. After logging in, run `!rsmavelycheck`.")

        @self.bot.command(name="rsmavelyautologin", aliases=["mavelyautologin", "mavelyheadless", "headlesslogin"])
        async def mavely_autologin(ctx, wait_seconds: str = None):
            """Run headless Playwright auto-login now (admin only).

            Notes:
            - Requires mavely_login_email/password to be configured (ideally in config.secrets.json).
            - This is a one-shot attempt; it does NOT start noVNC.
            """
            if not self._is_mavely_admin_ctx(ctx):
                await ctx.send("❌ You don't have permission to use this command.")
                return

            if not self._is_local_exec():
                await ctx.send("❌ This command only works when RSForwarder is running on the Linux host (Oracle).")
                return

            try:
                wait_s = int((wait_seconds or "").strip() or "180")
            except Exception:
                wait_s = 180
            wait_s = max(30, min(wait_s, 1800))

            # Best effort: delete the command message (keeps channels clean)
            try:
                if getattr(ctx, "guild", None) is not None:
                    await ctx.message.delete()
            except Exception:
                pass

            email, password = self._mavely_login_creds()
            if not (email and password):
                await ctx.send("❌ Missing Mavely creds. Add `mavely_login_email/password` to RSForwarder/config.secrets.json on the server.")
                return

            await ctx.send("🔄 Running headless Playwright auto-login now (this can take ~1–3 minutes)...")
            now = time.time()
            self._mavely_last_autologin_ts = now
            self._mavely_last_autologin_ok = None
            self._mavely_last_autologin_msg = ""
            self._mavely_write_status()

            cfg_run = dict(self.config or {})
            cfg_run["mavely_login_email"] = email
            cfg_run["mavely_login_password"] = password
            ok_run, out = await asyncio.to_thread(novnc_stack.run_cookie_refresher_headless, cfg_run, wait_login_s=int(wait_s))
            out_s = (out or "").strip()
            if len(out_s) > 2500:
                out_s = out_s[:2500] + "\n... (truncated)"
            self._mavely_last_autologin_ok = bool(ok_run)
            self._mavely_last_autologin_msg = out_s or ("ok" if ok_run else "failed")
            self._mavely_write_status()

            # Verify result with preflight
            try:
                ok2, status2, err2 = await affiliate_rewriter.mavely_preflight(self.config)
                self._mavely_last_preflight_ok = bool(ok2)
                self._mavely_last_preflight_status = int(status2 or 0)
                self._mavely_last_preflight_err = (err2 or "").strip() if not ok2 else ""
                self._mavely_write_status()
            except Exception:
                ok2, status2, err2 = False, None, "preflight threw an exception"

            if ok_run and ok2:
                await ctx.send(f"✅ Headless auto-login finished, and preflight is OK (status={status2}).")
                return

            msg = (str(err2 or "")).replace("\n", " ").strip()
            if len(msg) > 220:
                msg = msg[:220] + "..."
            tail = f"\n\nAuto-login output (truncated):\n```{out_s[-1200:]}```" if out_s else ""
            await ctx.send(
                "⚠️ Headless auto-login ran but session still looks invalid.\n"
                f"Preflight: {'✅ OK' if ok2 else '❌ FAIL'} (status={status2}) {msg}\n\n"
                "Next: run `!rsmavelylogin` to do a manual login via noVNC, then run `!rsmavelycheck`."
                + tail
            )

        @self.bot.command(name="rsmavelyalertme", aliases=["mavelyalertme"])
        async def mavely_alert_me(ctx):
            """Enable DM alerts for Mavely session failures (admin only; must be run in a guild)."""
            if getattr(ctx, "guild", None) is None:
                await ctx.send("❌ Run this in a server channel (not DMs) so we can verify admin permission.")
                return
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            ok = self._ensure_mavely_user(int(ctx.author.id))
            if ok:
                await ctx.send("✅ Mavely alerts enabled for you. If the session expires, I’ll DM you with the noVNC login steps.")
            else:
                await ctx.send("❌ Failed to save alert settings (could not write config.secrets.json).")

        @self.bot.command(name="rsmavelyalertoff", aliases=["mavelyalertoff"])
        async def mavely_alert_off(ctx):
            """Disable DM alerts for Mavely session failures (admin only; must be run in a guild)."""
            if getattr(ctx, "guild", None) is None:
                await ctx.send("❌ Run this in a server channel (not DMs) so we can verify admin permission.")
                return
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            ok = self._remove_mavely_user(int(ctx.author.id))
            if ok:
                await ctx.send("✅ Mavely alerts disabled for you.")
            else:
                await ctx.send("❌ Failed to save alert settings (could not write config.secrets.json).")

        @self.bot.command(name="rsmavelycheck", aliases=["mavelycheck"])
        async def mavely_check(ctx):
            """Run a non-mutating Mavely auth preflight check (safe)."""
            try:
                ok, status, err = await affiliate_rewriter.mavely_preflight(self.config)
                if ok:
                    await ctx.send(f"✅ Mavely preflight OK (status={status})")
                else:
                    msg = (err or "unknown error").replace("\n", " ").strip()
                    if len(msg) > 180:
                        msg = msg[:180] + "..."
                    await ctx.send(f"❌ Mavely preflight FAIL (status={status}) {msg}")
            except Exception as e:
                await ctx.send(f"❌ Mavely preflight FAIL ({str(e)[:300]})")

        @self.bot.command(name="rsmavelystatus", aliases=["mavelystatus"])
        async def mavely_status(ctx):
            """Show last Mavely automation status (admin only; safe, no tokens)."""
            if not self._is_mavely_admin_ctx(ctx):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            try:
                p = self._mavely_status_path()
                data = {}
                if p.exists():
                    try:
                        data = json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
                    except Exception:
                        data = {}
                if not isinstance(data, dict):
                    data = {}

                pre_ok = bool(data.get("preflight_ok"))
                pre_status = data.get("preflight_status")
                pre_err = str(data.get("preflight_err") or "").strip()
                al_ts = float(data.get("last_autologin_ts") or 0.0)
                al_ok = data.get("last_autologin_ok")
                al_msg = str(data.get("last_autologin_msg") or "").strip()
                pid = data.get("last_refresher_pid")
                lp = str(data.get("last_refresher_log_path") or "").strip()

                lines = []
                lines.append(f"**Mavely status** (from `{p}`)")
                lines.append(f"- preflight: {'✅ OK' if pre_ok else '❌ FAIL'} (status={pre_status})")
                if (not pre_ok) and pre_err:
                    lines.append(f"- error: `{pre_err[:240]}`")
                if al_ts > 0:
                    ts_str = datetime.utcfromtimestamp(al_ts).isoformat(timespec="seconds") + "Z"
                    lines.append(f"- last auto-login: {ts_str} (ok={al_ok})")
                if al_msg:
                    lines.append(f"- auto-login msg: `{al_msg[:240]}`")
                if pid:
                    lines.append(f"- last noVNC refresher PID: `{pid}`")
                if lp:
                    lines.append(f"- refresher log: `{lp}`")
                lines.append(f"- monitor log: `{self._mavely_monitor_log_path()}`")
                await ctx.send("\n".join(lines)[:1900])
            except Exception as e:
                await ctx.send(f"❌ Failed to read Mavely status: {str(e)[:300]}")
        
        @self.bot.command(name='rscommands', aliases=['commands'])
        async def bot_help(ctx):
            """Show available commands"""
            embed = discord.Embed(
                title="📋 RS Forwarder Bot Commands",
                color=discord.Color.green()
            )
            
            commands_list = [
                ("`!rsstatus`", "Show bot status and configuration"),
                ("`!rslist`", "List all configured forwarding jobs"),
                ("`!rsadd <#channel|id> <webhook_url> [role_id] [text]`", "Add a new forwarding job"),
                ("`!rsupdate <#channel|id> [webhook_url] [role_id] [text]`", "Update an existing forwarding job"),
                ("`!rsview <#channel|id>`", "View details of a specific forwarding job"),
                ("`!rsremove <#channel|id>`", "Remove a forwarding job"),
                ("`!rstest [channel_id] [limit]`", "Test forwarding by forwarding recent messages (default: 1 message)"),
                ("`!rsfetchicon`", "Manually fetch RS Server icon"),
                ("`!rsstartadminbot`", "Start RSAdminBot remotely on server (admin only)"),
                ("`!rsrestartadminbot` or `!restart`", "Restart RSAdminBot remotely on server (admin only)"),
                ("`!rsmavelylogin` or `!refreshtoken`", "Open noVNC + Mavely login browser (admin only; manual login)"),
                ("`!rsmavelyautologin`", "Run headless Playwright auto-login now (admin only; best-effort)"),
                ("`!rsmavelycheck`", "Check if Mavely session is valid (safe)"),
                ("`!rsmavelystatus`", "Show last Mavely auto-login/noVNC status (admin only)"),
                ("`!rsmavelyalertme`", "DM me if Mavely session expires (admin only)"),
                ("`!rsmavelyalertoff`", "Disable Mavely expiry DMs (admin only)"),
                ("`!rscommands`", "Show this help message"),
            ]
            
            for cmd, desc in commands_list:
                embed.add_field(name=cmd, value=desc, inline=False)
            
            embed.set_footer(text="Example: !rstest 1446174861931188387 5")
            await ctx.send(embed=embed)
    
    def _get_uptime(self) -> str:
        """Get bot uptime as formatted string"""
        if not self.stats.get('started_at'):
            return "Unknown"
        
        try:
            started = datetime.fromisoformat(self.stats['started_at'])
            uptime = datetime.now() - started
            hours, remainder = divmod(int(uptime.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours}h {minutes}m {seconds}s"
        except:
            return "Unknown"
    
    def _fix_ssh_key_permissions(self, key_path: Path):
        """Fix SSH key file permissions on Windows (required for SSH to work).
        
        SSH requires private keys to have restricted permissions (only readable by owner).
        On Windows, we need to remove permissions for BUILTIN\\Users group.
        
        Args:
            key_path: Path to SSH private key file
        """
        if platform.system() != "Windows":
            return  # Only needed on Windows
        
        try:
            import win32security
            import ntsecuritycon as con
            
            # Get current file security descriptor
            sd = win32security.GetFileSecurity(str(key_path), win32security.DACL_SECURITY_INFORMATION)
            dacl = sd.GetSecurityDescriptorDacl()
            
            # Remove BUILTIN\\Users group permissions
            users_sid = win32security.LookupAccountName("", "BUILTIN\\Users")[0]
            
            # Check if Users group has permissions
            has_users_perms = False
            for i in range(dacl.GetAceCount()):
                ace = dacl.GetAce(i)
                if ace[2] == users_sid:
                    has_users_perms = True
                    break
            
            if has_users_perms:
                # Create new DACL without Users group
                new_dacl = win32security.ACL()
                
                # Add owner full control
                owner_sid = win32security.LookupAccountName("", os.environ.get("USERNAME", ""))[0]
                new_dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, owner_sid)
                
                # Add SYSTEM full control
                system_sid = win32security.LookupAccountName("", "NT AUTHORITY\\SYSTEM")[0]
                new_dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, system_sid)
                
                # Set new DACL
                sd.SetSecurityDescriptorDacl(1, new_dacl, 0)
                win32security.SetFileSecurity(str(key_path), win32security.DACL_SECURITY_INFORMATION, sd)
                print(f"{Colors.GREEN}[RSForwarder] Fixed SSH key permissions (removed BUILTIN\\Users access){Colors.RESET}")
        except ImportError:
            # pywin32 not available - try using icacls command instead
            try:
                import subprocess
                # Remove Users group permissions using icacls
                result = subprocess.run(
                    ["icacls", str(key_path), "/remove", "BUILTIN\\Users"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    print(f"{Colors.GREEN}[RSForwarder] Fixed SSH key permissions using icacls{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[RSForwarder] Warning: Could not fix SSH key permissions: {result.stderr}{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.YELLOW}[RSForwarder] Warning: Could not fix SSH key permissions: {e}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.YELLOW}[RSForwarder] Warning: Could not fix SSH key permissions: {e}{Colors.RESET}")
    
    def _get_channel_alert_footer(self, channel_name: str) -> str:
        """Get custom alert footer text based on channel name"""
        channel_lower = channel_name.lower()
        if "online-important" in channel_lower or "online important" in channel_lower:
            return "RS Leads - Online Important Alert!"
        elif "personal-deals" in channel_lower or "personal deals" in channel_lower:
            return "RS Leads - Personal Deals Alert!"
        elif "minor-deals" in channel_lower or "minor deals" in channel_lower:
            return "RS Leads - Minor Deals Alert!"
        else:
            return f"RS Leads - {channel_name.title()} Alert!"
    
    def _format_timestamp(self, timestamp: datetime = None) -> str:
        """Format timestamp as 'at 1:27 AM' format"""
        if timestamp is None:
            timestamp = datetime.now()
        # Format as "at 1:27 AM" - Windows compatible
        try:
            # Try Unix-style format first (removes leading zero)
            time_str = timestamp.strftime("%-I:%M %p")
            return f"at {time_str}"
        except ValueError:
            # Windows fallback - remove leading zero manually
            time_str = timestamp.strftime("%I:%M %p")
            if time_str.startswith("0"):
                time_str = time_str[1:]
            return f"at {time_str}"
    
    def _apply_rs_branding(self, embeds: List[Dict[str, Any]], channel_name: str = None) -> List[Dict[str, Any]]:
        """Apply Reselling Secrets branding to embeds"""
        branded_embeds = []
        
        # Get brand name once at the start - used throughout the function
        brand_name = self.config.get("brand_name", "Reselling Secrets")
        
        for embed in embeds:
            # Deep copy to avoid modifying original
            branded_embed = json.loads(json.dumps(embed))
            
            # Preserve image from original embed (if it exists)
            # Image will be added later if from message attachments
            
            # Update footer with RS branding and timestamp
            if "footer" in branded_embed:
                footer = branded_embed.get("footer", {}) or {}
            else:
                footer = {}
            
            # Set footer text to just "Reselling Secrets" (simple footer per boss request)
            footer["text"] = brand_name
            
            # Remove footer icon (no logo at bottom per boss request)
            if "icon_url" in footer:
                del footer["icon_url"]
            
            branded_embed["footer"] = footer
            
            # Remove author field completely (no "Reselling Secrets" at top per boss request)
            if "author" in branded_embed:
                del branded_embed["author"]
            
            # Preserve title (this is often the product name on deal cards), but only
            # if it's a valid non-empty string. Discord rejects `title: null`.
            if "title" in branded_embed:
                title_val = branded_embed.get("title")
                if not isinstance(title_val, str):
                    del branded_embed["title"]
                else:
                    t = title_val.strip()
                    if not t:
                        del branded_embed["title"]
                    else:
                        if len(t) > 256:
                            t = t[:253] + "..."
                        branded_embed["title"] = t
            
            # Set embed color to blue sidebar (branding blue)
            branded_embed["color"] = 0x0099FF  # Branding blue color
            
            branded_embeds.append(branded_embed)
        
        return branded_embeds
    
    def _create_embed_from_message(self, content: str, channel_name: str, author_name: str = None) -> Dict[str, Any]:
        """Create an embed from a normal text message"""
        brand_name = self.config.get("brand_name", "Reselling Secrets")
        timestamp_str = self._format_timestamp()
        alert_text = self._get_channel_alert_footer(channel_name)
        
        embed = {
            # No title (simple embed per boss request)
            "description": content[:4096] if content else "\u200b",  # Discord embed description limit
            "color": 0x0099FF,  # Branding blue color
            "footer": {
                "text": brand_name,  # Just "Reselling Secrets" in footer
            }
            # No author field (no "Reselling Secrets" at top per boss request)
        }
        
        return embed
    
    def _get_role_mention_text(self, channel_config: Dict[str, Any]) -> Optional[str]:
        """Get role mention text for a channel if configured"""
        role_mention = channel_config.get("role_mention", {})
        if not role_mention:
            return None
        
        role_id = role_mention.get("role_id", "").strip()
        text = role_mention.get("text", "").strip()
        
        if not role_id:
            return None
        
        # If text is blank, return just the role mention without text
        if not text:
            return f"<@&{role_id}>"
        
        return f"<@&{role_id}> {text}"

    def _members_role_ids(self) -> List[int]:
        """
        Return role IDs that correspond to a role named "Members" (case-insensitive).
        Used to prevent forwarding/pinging @Members.
        """
        try:
            if not self.rs_guild:
                guild_id = int(self._rs_server_guild_id() or 0)
                if guild_id:
                    self.rs_guild = self.bot.get_guild(guild_id)
            g = self.rs_guild
            if not g:
                return []
            out: List[int] = []
            for r in getattr(g, "roles", []) or []:
                name = (getattr(r, "name", "") or "").strip().lower()
                if name == "members":
                    out.append(int(getattr(r, "id", 0) or 0))
            return [rid for rid in out if rid > 0]
        except Exception:
            return []

    def _strip_members_mentions(self, text: str) -> str:
        """
        Remove @Members role mentions from forwarded content.
        Keeps other role mentions intact.
        """
        t = text or ""
        if not t:
            return t
        try:
            for rid in self._members_role_ids():
                t = t.replace(f"<@&{rid}>", "")
            # Also handle plain-text "@Members" if present
            t = t.replace("@Members", "")
            # Normalize whitespace / empty lines after removal
            lines = [ln.rstrip() for ln in t.splitlines()]
            # Drop lines that became empty due only to mention removal
            compact: List[str] = []
            for ln in lines:
                if ln.strip() == "":
                    if compact and compact[-1] != "":
                        compact.append("")
                    continue
                compact.append(ln)
            # Trim leading/trailing blank lines
            while compact and compact[0] == "":
                compact.pop(0)
            while compact and compact[-1] == "":
                compact.pop()
            return "\n".join(compact).strip()
        except Exception:
            return (text or "").strip()
    
    async def _send_forwarding_log(self, message: discord.Message, channel_config: Dict[str, Any], success: bool, error: str = None):
        """Send forwarding log to configured logging channel"""
        try:
            log_channel_id = self.config.get("forwarding_logs_channel_id")
            if not log_channel_id:
                return
            
            log_channel = self.bot.get_channel(int(log_channel_id))
            if not log_channel:
                return
            
            source_channel_id = channel_config.get("source_channel_id", "unknown")
            source_channel_name = channel_config.get("source_channel_name", "unknown")
            dest_webhook = channel_config.get("destination_webhook_url", "")
            
            # Get destination info from webhook URL if possible
            dest_info = "Unknown"
            if dest_webhook:
                try:
                    # Extract webhook ID from URL
                    parts = dest_webhook.split("/")
                    if len(parts) >= 2:
                        webhook_id = parts[-2] if parts[-1] else parts[-1]
                        dest_info = f"Webhook {webhook_id[:8]}..."
                except:
                    dest_info = "Webhook configured"
            
            embed = discord.Embed(
                color=discord.Color.green() if success else discord.Color.red(),
                timestamp=datetime.now()
            )
            
            if success:
                embed.title = "✅ Message Forwarded"
                embed.description = f"Successfully forwarded message from `{source_channel_name}`"
            else:
                embed.title = "❌ Forward Failed"
                embed.description = f"Failed to forward message from `{source_channel_name}`"
                if error:
                    embed.add_field(name="Error", value=error[:1024], inline=False)
            
            embed.add_field(name="Source Channel", value=f"`{source_channel_name}`\nID: `{source_channel_id}`", inline=True)
            embed.add_field(name="Destination", value=dest_info, inline=True)
            
            # Message info
            msg_content_preview = message.content[:100] + "..." if message.content and len(message.content) > 100 else (message.content or "[No content]")
            embed.add_field(name="Message", value=msg_content_preview, inline=False)
            
            # Embed count
            embed_count = len(message.embeds) if message.embeds else 0
            if embed_count > 0:
                embed.add_field(name="Embeds", value=str(embed_count), inline=True)
            
            # Attachment count
            attachment_count = len(message.attachments) if message.attachments else 0
            if attachment_count > 0:
                embed.add_field(name="Attachments", value=str(attachment_count), inline=True)
            
            # Author info
            if message.author:
                embed.add_field(name="Author", value=f"{message.author.name}#{message.author.discriminator}\nID: `{message.author.id}`", inline=True)
            
            # Message link
            if message.jump_url:
                embed.add_field(name="Message Link", value=f"[Jump to Message]({message.jump_url})", inline=True)
            
            # Stats
            embed.set_footer(text=f"Total forwarded: {self.stats['messages_forwarded']} | Errors: {self.stats['errors']}")
            
            await log_channel.send(embed=embed)
        except Exception as e:
            # Don't fail forwarding if logging fails
            print(f"{Colors.YELLOW}[Log] Failed to send forwarding log: {e}{Colors.RESET}")

    def _is_resendable_embed_dict(self, embed_dict: Dict[str, Any]) -> bool:
        """
        Discord "link preview" embeds are not generally re-sendable as custom embeds.
        For in-place reposting, we only resend "rich" embeds (typically bot-made).
        Link previews will be regenerated naturally from the reposted content URLs.
        """
        try:
            t = str((embed_dict or {}).get("type") or "").strip().lower()
            return (not t) or (t == "rich")
        except Exception:
            return False

    async def _repost_in_place_message(self, message: discord.Message, channel_id: str, channel_config: Dict[str, Any]) -> None:
        """
        In-place affiliate rewrite:
        - Rewrite affiliate URLs in message content + resendable embeds
        - Repost into the SAME channel (as the bot)
        - Delete the original message (only after successful repost)

        NOTE: This only triggers when a rewrite actually changes something, to avoid
        pointless churn for plain text messages.
        """
        try:
            rewrite_enabled = bool(self.config.get("affiliate_rewrite_enabled", True))
            if not rewrite_enabled:
                return
            debug = bool((channel_config or {}).get("repost_debug"))
            if debug:
                try:
                    mid = int(getattr(message, "id", 0) or 0)
                    aid = int(getattr(getattr(message, "author", None), "id", 0) or 0)
                    print(f"{Colors.CYAN}[Repost] saw message id={mid} author={aid} channel={channel_id}{Colors.RESET}")
                except Exception:
                    pass

            any_changed = False

            # Content rewrite
            content = message.content or ""

            where_only = bool((channel_config or {}).get("repost_affiliate_where_only"))
            where_marker = str((channel_config or {}).get("repost_affiliate_where_marker") or "`Where:`").strip()
            if where_only and content and where_marker:
                def _lstrip_invisible_prefix(s: str) -> str:
                    """
                    Discord sometimes injects Unicode "format" characters (category Cf) around inline code.
                    Those are not removed by .lstrip(), but we want marker matching to ignore them.
                    """
                    t = s or ""
                    i = 0
                    n = len(t)
                    while i < n:
                        ch = t[i]
                        if ch.isspace():
                            i += 1
                            continue
                        try:
                            if unicodedata.category(ch) == "Cf":
                                i += 1
                                continue
                        except Exception:
                            pass
                        break
                    return t[i:]

                def _codepoints_prefix(s: str, limit: int = 12) -> str:
                    try:
                        return " ".join([f"U+{ord(c):04X}" for c in (s or "")[: max(0, int(limit))]])
                    except Exception:
                        return ""

                try:
                    lines = content.splitlines()
                except Exception:
                    lines = [content]
                out_lines: List[str] = []
                where_matched = 0
                where_notes: List[str] = []
                for ln in lines:
                    # Only affiliate-rewrite URLs in the `Where:` line.
                    ln_norm = _lstrip_invisible_prefix(ln)
                    hit = bool(where_marker and ln_norm.startswith(where_marker))
                    # Fallback: accept "Where:" without backticks too.
                    if (not hit) and where_marker == "`Where:`":
                        hit = ln_norm.startswith("Where:") or ln_norm.startswith("**Where:**")

                    if hit:
                        where_matched += 1
                        new_ln, changed, notes = await affiliate_rewriter.rewrite_text(self.config, ln)
                        if changed:
                            any_changed = True
                        if debug and isinstance(notes, dict) and notes:
                            shown = 0
                            for u, note in list(notes.items()):
                                if shown >= 2:
                                    break
                                nu = (str(u or "").strip() or "?")[:140]
                                nn = (str(note or "").replace("\r", " ").replace("\n", " ").strip() or "?")
                                if len(nn) > 200:
                                    nn = nn[:200] + "..."
                                where_notes.append(f"{nu} ({nn})")
                                shown += 1
                        out_lines.append(new_ln)
                    else:
                        out_lines.append(ln)
                content = "\n".join(out_lines)
                if debug:
                    try:
                        if where_matched == 0:
                            first = lines[0] if lines else ""
                            cp = _codepoints_prefix(str(first or ""), limit=16)
                            print(
                                f"{Colors.YELLOW}[Repost] where-only enabled but no lines matched marker={where_marker!r}. "
                                f"first_line_prefix={cp}{Colors.RESET}"
                            )
                        elif where_notes:
                            print(f"{Colors.CYAN}[Repost] where rewrite notes: { ' | '.join(where_notes) }{Colors.RESET}")
                    except Exception:
                        pass
            elif content:
                content2, changed, _notes = await affiliate_rewriter.rewrite_text(self.config, content)
                if changed:
                    any_changed = True
                content = content2

            # Embed rewrite (rich embeds only)
            embeds_raw_all = [e.to_dict() for e in message.embeds] if message.embeds else []
            embeds_raw = [e for e in embeds_raw_all if self._is_resendable_embed_dict(e)]
            if embeds_raw and (not where_only):
                rewritten = []
                for e in embeds_raw:
                    ee, ch, _notes = await affiliate_rewriter.rewrite_embed_dict(self.config, e)
                    if ch:
                        any_changed = True
                    rewritten.append(ee)
                embeds_raw = rewritten

            if not any_changed:
                if debug:
                    try:
                        print(f"{Colors.YELLOW}[Repost] no affiliate rewrite for message in channel={channel_id}{Colors.RESET}")
                    except Exception:
                        pass
                return

            # Discord limits
            if content and len(content) > 2000:
                content = content[:1997] + "..."

            embeds: List[discord.Embed] = []
            for e in embeds_raw[:10]:
                try:
                    embeds.append(discord.Embed.from_dict(e))
                except Exception:
                    pass

            files: List[discord.File] = []
            for att in (message.attachments or [])[:10]:
                try:
                    files.append(await att.to_file())
                except Exception:
                    pass

            # Prevent double-pings: original already pinged; repost should not.
            allowed_mentions = discord.AllowedMentions.none()

            send_kwargs: Dict[str, Any] = {"allowed_mentions": allowed_mentions}
            if content:
                send_kwargs["content"] = content
            if embeds:
                send_kwargs["embeds"] = embeds
            if files:
                send_kwargs["files"] = files
            if not send_kwargs.get("content") and not send_kwargs.get("embeds") and not send_kwargs.get("files"):
                return

            # Preserve reply chain (reply to the same referenced message, not to the original).
            try:
                if message.reference and getattr(message.reference, "message_id", None):
                    send_kwargs["reference"] = message.reference
                    send_kwargs["mention_author"] = False
            except Exception:
                pass

            # Send the repost
            try:
                await message.channel.send(**send_kwargs)
            except Exception as e:
                print(f"{Colors.RED}[Repost] Failed to repost in-place for channel {channel_id}: {e}{Colors.RESET}")
                return
            if debug:
                try:
                    print(f"{Colors.GREEN}[Repost] reposted affiliate-updated message in channel={channel_id}{Colors.RESET}")
                except Exception:
                    pass

            # Delete original only after repost succeeded.
            delete_original = (channel_config or {}).get("repost_delete_original")
            if delete_original is None:
                delete_original = True
            if bool(delete_original):
                try:
                    await message.delete()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Repost] Reposted but could not delete original: {e}{Colors.RESET}")
                else:
                    if debug:
                        try:
                            print(f"{Colors.GREEN}[Repost] deleted original message in channel={channel_id}{Colors.RESET}")
                        except Exception:
                            pass
        except Exception as e:
            print(f"{Colors.RED}[Repost] Exception: {e}{Colors.RESET}")
    
    async def forward_message(self, message: discord.Message, channel_id: str, channel_config: Dict[str, Any]):
        """Forward a message to the configured webhook"""
        try:
            # In-place repost mode (same channel): rewrite affiliate links and delete original.
            if bool((channel_config or {}).get("repost_in_place")):
                await self._repost_in_place_message(message, channel_id, channel_config)
                return

            webhook_url = channel_config.get("destination_webhook_url", "").strip()
            source_channel_name = channel_config.get("source_channel_name", f"Channel {channel_id}")
            
            if not webhook_url:
                if self.stats['messages_forwarded'] == 0:  # Only warn once
                    print(f"{Colors.YELLOW}[Forward] ⚠️ No webhook configured for channel {source_channel_name} (ID: {channel_id}){Colors.RESET}")
                return
            
            print(f"{Colors.CYAN}[Forward] Forwarding message from {source_channel_name}...{Colors.RESET}")
            
            # Prepare message content
            content = message.content or ""

            # Affiliate rewrite (standalone, same behavior as Instorebotforwarder)
            rewrite_enabled = bool(self.config.get("affiliate_rewrite_enabled", True))
            affiliate_changed = False
            affiliate_notes: Dict[str, str] = {}
            if rewrite_enabled and content:
                content, _changed, _notes = await affiliate_rewriter.rewrite_text(self.config, content)
                if _changed:
                    affiliate_changed = True
                if isinstance(_notes, dict):
                    for k, v in _notes.items():
                        if k and v:
                            affiliate_notes[str(k)] = str(v)

            # Never forward/ping @Members
            if content:
                content = self._strip_members_mentions(content)
            
            # Get channel name for custom titles
            channel_name = channel_config.get("source_channel_name", "Unknown Channel")
            
            # Prepare embeds - convert normal messages to embeds if needed
            embeds_raw = [e.to_dict() for e in message.embeds] if message.embeds else []

            if rewrite_enabled and embeds_raw:
                rewritten_embeds = []
                for e in embeds_raw:
                    ee, _ch, _notes = await affiliate_rewriter.rewrite_embed_dict(self.config, e)
                    if _ch:
                        affiliate_changed = True
                    if isinstance(_notes, dict):
                        for k, v in _notes.items():
                            if k and v:
                                affiliate_notes[str(k)] = str(v)
                    rewritten_embeds.append(ee)
                embeds_raw = rewritten_embeds

            # Human-friendly affiliate signal (helps debug why links didn't change)
            if rewrite_enabled and affiliate_notes:
                try:
                    if affiliate_changed:
                        print(f"{Colors.GREEN}[Affiliate] ✅ Rewrote affiliate links ({len(affiliate_notes)} url(s)) {Colors.RESET}")
                    else:
                        print(f"{Colors.YELLOW}[Affiliate] ↩ No affiliate rewrite ({len(affiliate_notes)} url(s)) {Colors.RESET}")

                    shown = 0
                    limit = 4 if affiliate_changed else 2
                    for u, note in list(affiliate_notes.items()):
                        if shown >= limit:
                            break
                        # `note` may include "reason -> replacement" from affiliate_rewriter.rewrite_text
                        print(f"{Colors.CYAN}[Affiliate] - {u} ({note}){Colors.RESET}")
                        shown += 1
                except Exception:
                    pass
            
            # If no embeds and we have content, create an embed from the message
            if not embeds_raw and content:
                embed = self._create_embed_from_message(content, channel_name, str(message.author) if message.author else None)
                embeds_raw = [embed]
            
            # Apply RS branding to all embeds
            embeds = self._apply_rs_branding(embeds_raw, channel_name)
            
            # Check if we need to add role mention (works for both normal messages and embeds)
            role_mention_text = self._get_role_mention_text(channel_config)
            # Never add @Members even if configured
            if role_mention_text:
                for rid in self._members_role_ids():
                    if f"<@&{rid}>" in role_mention_text:
                        role_mention_text = None
                        break
            # Debug: Log role mention status
            if self.stats['messages_forwarded'] == 0:
                if role_mention_text:
                    print(f"{Colors.CYAN}[Debug] Role mention will be added: {role_mention_text[:50]}...{Colors.RESET}")
                else:
                    print(f"{Colors.CYAN}[Debug] No role mention (text is blank or not configured){Colors.RESET}")
            
            # Prepare attachments - get first image for embed
            attachment_urls = [att.url for att in message.attachments] if message.attachments else []
            first_image_url = None
            if message.attachments:
                # Find first image attachment
                for att in message.attachments:
                    if att.content_type and att.content_type.startswith('image/'):
                        first_image_url = att.url
                        break
                # If no image found but we have attachments, use first one
                if not first_image_url and attachment_urls:
                    first_image_url = attachment_urls[0]
            
            # Add image to embeds if we have one and embed doesn't already have an image
            if first_image_url and embeds:
                # Add image to first embed if it doesn't already have one
                if "image" not in embeds[0] or not embeds[0].get("image", {}).get("url"):
                    embeds[0]["image"] = {"url": first_image_url}
            
            # Build webhook payload
            import requests
            
            brand_name = self.config.get("brand_name", "Reselling Secrets")
            
            # ALWAYS set avatar from RS Server icon (for ALL message types)
            # If icon not loaded yet, try to fetch it now
            if not self.rs_icon_url:
                guild_id = self._rs_server_guild_id()
                if guild_id:
                    # Try to get from cached guild first
                    self.rs_guild = self.bot.get_guild(guild_id)
                    if self.rs_guild:
                        self.rs_icon_url = self._get_guild_icon_url(self.rs_guild)
                    # If still not found, try API synchronously
                    if not self.rs_icon_url:
                        # Fetch icon via API (synchronous for this message)
                        import requests
                        try:
                            bot_token = self.config.get("bot_token", "").strip()
                            if bot_token:
                                headers = {
                                    'Authorization': f'Bot {bot_token}',
                                    'User-Agent': 'DiscordBot (RSForwarder)'
                                }
                                response = requests.get(
                                    f'https://discord.com/api/v10/guilds/{guild_id}?with_counts=false',
                                    headers=headers,
                                    timeout=5
                                )
                                if response.status_code == 200:
                                    guild_data = response.json()
                                    icon_hash = guild_data.get("icon")
                                    if icon_hash:
                                        ext = "gif" if str(icon_hash).startswith("a_") else "png"
                                        self.rs_icon_url = f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}?size=256"
                                        print(f"{Colors.GREEN}[Forward] Fetched RS icon via API: {self.rs_icon_url[:50]}...{Colors.RESET}")
                        except Exception as e:
                            if self.stats['messages_forwarded'] == 0:
                                print(f"{Colors.YELLOW}[Warn] Could not fetch RS icon: {e}{Colors.RESET}")
            
            # Build payload - ALWAYS set username and avatar
            payload = {
                "username": brand_name,  # ALWAYS override with RS branding
            }
            
            # ALWAYS set avatar if we have it
            if self.rs_icon_url:
                payload["avatar_url"] = self.rs_icon_url
                if self.stats['messages_forwarded'] <= 1:  # Log first few times
                    print(f"{Colors.GREEN}[Forward] Using RS icon: {self.rs_icon_url[:50]}...{Colors.RESET}")
            else:
                # Log warning if icon still not available
                if self.stats['messages_forwarded'] == 0:
                    print(f"{Colors.RED}[Warn] RS Server icon not available - webhook will use default avatar{Colors.RESET}")
                    print(f"{Colors.YELLOW}[Warn] Check that bot is in RS Server (ID: {self._rs_server_guild_id()}){Colors.RESET}")
            
            # Build content with role mention if needed
            # Since we're converting everything to embeds, role mention goes in content before embed
            final_content_parts = []
            
            # Add role mention if configured (appears before embed)
            if role_mention_text:
                final_content_parts.append(role_mention_text)
            
            # If the original message had content and embeds, preserve the content too (forward full message).
            if content and embeds:
                final_content_parts.append(content)

            # Only add content if we have anything to post in content (role mention and/or original content)
            if final_content_parts:
                
                # Combine content
                final_content = "\n".join(final_content_parts) if final_content_parts else None
                
                # Truncate if too long
                if final_content and len(final_content) > 2000:
                    final_content = final_content[:1997] + "..."
                
                # Add content if present
                if final_content:
                    payload["content"] = final_content
            
            # Add embeds (max 10)
            if embeds:
                payload["embeds"] = embeds[:10]
                # Debug: Log embed titles (first message only)
                if self.stats['messages_forwarded'] == 0:
                    for i, embed in enumerate(embeds[:3]):  # Log first 3 embeds
                        embed_title = embed.get("title", "[No title]")
                        embed_footer = embed.get("footer", {}).get("text", "[No footer]")
                        print(f"{Colors.CYAN}[Debug] Embed {i+1} title: '{embed_title}' | footer: '{embed_footer[:50]}...'{Colors.RESET}")
            
            # Add attachment URLs to content if no embeds and content is short
            if attachment_urls and not embeds and final_content and len(final_content) + sum(len(url) for url in attachment_urls[:5]) < 2000:
                payload["content"] = final_content + "\n" + "\n".join(attachment_urls[:5])
            elif attachment_urls and not embeds and not final_content:
                payload["content"] = "\n".join(attachment_urls[:5])
            
            # Ensure at least content or embeds
            if not payload.get("content") and not payload.get("embeds"):
                payload["content"] = "[Message forwarded from RS Server]"
            
            # Debug: Log payload (first message only)
            if self.stats['messages_forwarded'] == 0:
                print(f"{Colors.CYAN}[Debug] Payload username: {payload.get('username')}{Colors.RESET}")
                print(f"{Colors.CYAN}[Debug] Payload avatar_url: {payload.get('avatar_url', 'NOT SET')[:50] if payload.get('avatar_url') else 'NOT SET'}{Colors.RESET}")
            
            # Send to webhook
            response = requests.post(webhook_url, json=payload, timeout=10)
            
            if response.status_code in [200, 204]:
                self.stats['messages_forwarded'] += 1
                channel_name = channel_config.get("source_channel_name", channel_id)
                embed_count = len(embeds)
                role_mention = " (with role mention)" if role_mention_text else ""
                print(f"{Colors.GREEN}[Forward] ✓ {channel_name} → {len(content)} chars, {embed_count} embed(s){role_mention}{Colors.RESET}")
                
                # Send forwarding log
                await self._send_forwarding_log(message, channel_config, success=True)
            else:
                self.stats['errors'] += 1
                error_msg = f"{response.status_code}: {response.text[:200]}"
                print(f"{Colors.RED}[Forward] ✗ Error {error_msg}{Colors.RESET}")
                
                # Send forwarding log with error
                await self._send_forwarding_log(message, channel_config, success=False, error=error_msg)
                
        except Exception as e:
            self.stats['errors'] += 1
            error_msg = str(e)
            print(f"{Colors.RED}[Forward] Exception: {error_msg}{Colors.RESET}")
            
            # Send forwarding log with exception
            await self._send_forwarding_log(message, channel_config, success=False, error=error_msg)
            
            import traceback
            if "--verbose" in sys.argv:
                traceback.print_exc()
    
    async def start(self):
        """Start the bot"""
        bot_token = self.config.get("bot_token", "").strip()
        if not bot_token:
            print(f"{Colors.RED}[Bot] ERROR: bot_token is required in config.secrets.json (server-only){Colors.RESET}")
            return
        
        try:
            await self.bot.start(bot_token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Bot] Shutting down...{Colors.RESET}")
            await self.bot.close()
            print(f"{Colors.CYAN}Stats:{Colors.RESET}")
            print(f"  Messages forwarded: {self.stats['messages_forwarded']}")
            print(f"  Errors: {self.stats['errors']}")


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--check-config", action="store_true", help="Validate config + secrets and exit (no Discord connection).")
    args = parser.parse_args()

    if args.check_config:
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors: List[str] = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")

        channels = cfg.get("channels") or []
        webhooks = cfg.get("destination_webhooks") or {}
        if channels and not isinstance(webhooks, dict):
            errors.append("destination_webhooks must be an object in config.secrets.json")
        else:
            missing = []
            for ch in channels:
                src = str((ch or {}).get("source_channel_id", "")).strip()
                if bool((ch or {}).get("repost_in_place")):
                    continue
                if src and not (webhooks or {}).get(src):
                    missing.append(src)
            if missing:
                errors.append(f"Missing destination_webhooks entries for source_channel_id(s): {', '.join(missing[:5])}")

        if errors:
            print(f"{Colors.RED}[ConfigCheck] FAILED{Colors.RESET}")
            for e in errors:
                print(f"- {e}")
            return

        print(f"{Colors.GREEN}[ConfigCheck] OK{Colors.RESET}")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        print(f"- channels: {len(channels)}")
        return

    bot = RSForwarderBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[Bot] Stopped{Colors.RESET}")


if __name__ == '__main__':
    main()

