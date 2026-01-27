"""
RSNotes/rsnote.py

Slash command: /rsnote
- Ephemeral per-user notes panel
- Add / Update / Remove via dropdown + buttons + modal
- JSON storage with atomic writes + asyncio lock
- Zero hardcoded channel IDs: reads RSNotes/configs.json
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _human_ts(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y %I:%M %p UTC")
    except Exception:
        return iso


def _truncate(s: str, n: int) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= n else (s[: max(0, n - 3)] + "...")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(path: str, data: Dict[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


DEFAULT_ALLOWED_LABELS = ["SKU", "RS Post Link", "Other"]


class RSNoteConfig:
    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        self.config_path = os.path.join(root_dir, "configs.json")

        cfg = _read_json(self.config_path)
        notes_cfg = (cfg or {}).get("notes", {})

        self.data_dir = str(notes_cfg.get("data_dir", "data"))
        self.db_filename = str(notes_cfg.get("db_filename", "rsnotes.json"))

        self.page_size = int(notes_cfg.get("page_size", 8))
        self.max_notes_per_user = int(notes_cfg.get("max_notes_per_user", 200))
        self.max_render_content_len = int(notes_cfg.get("max_render_content_len", 1800))

        self.sku_search_channel_id = notes_cfg.get("sku_search_channel_id", None)
        if not isinstance(self.sku_search_channel_id, int):
            raise ValueError(
                "RSNotes/configs.json missing required notes.sku_search_channel_id (must be int)"
            )

        if self.page_size <= 0:
            self.page_size = 8
        if self.max_notes_per_user <= 0:
            self.max_notes_per_user = 200
        if self.max_render_content_len <= 0:
            self.max_render_content_len = 1800

    def db_path(self) -> str:
        data_path = os.path.join(self.root_dir, self.data_dir)
        _ensure_dir(data_path)
        return os.path.join(data_path, self.db_filename)


@dataclass
class RSNote:
    id: str
    label: str
    title: str
    content: str
    rs_post_url: str = ""
    created_at: str = ""
    updated_at: str = ""

    @staticmethod
    def new(label: str, title: str, content: str, rs_post_url: str = "") -> "RSNote":
        now = _utcnow_iso()
        return RSNote(
            id=f"n_{int(time.time())}_{uuid.uuid4().hex[:6]}",
            label=label,
            title=title,
            content=content,
            rs_post_url=rs_post_url or "",
            created_at=now,
            updated_at=now,
        )


class RSNoteStore:
    def __init__(self, db_path: str, max_notes_per_user: int):
        self.db_path = db_path
        self.max_notes_per_user = max_notes_per_user
        self._lock = asyncio.Lock()
        self._data: Dict[str, Any] = {"version": 1, "notes_by_user": {}}

    async def load(self) -> None:
        _ensure_dir(os.path.dirname(self.db_path))
        if not os.path.exists(self.db_path):
            async with self._lock:
                _write_json_atomic(self.db_path, self._data)
            return

        async with self._lock:
            try:
                self._data = _read_json(self.db_path)
                if "notes_by_user" not in self._data:
                    raise ValueError("Missing notes_by_user")
            except Exception:
                bak = f"{self.db_path}.bak_{int(time.time())}"
                try:
                    os.replace(self.db_path, bak)
                except Exception:
                    pass
                self._data = {"version": 1, "notes_by_user": {}}
                _write_json_atomic(self.db_path, self._data)

    async def _save_locked(self) -> None:
        _write_json_atomic(self.db_path, self._data)

    async def get_notes(self, user_id: int) -> List[RSNote]:
        async with self._lock:
            raw_list = self._data.get("notes_by_user", {}).get(str(user_id), [])
            notes: List[RSNote] = []
            for item in raw_list:
                try:
                    notes.append(RSNote(**item))
                except Exception:
                    continue
            notes.sort(key=lambda n: n.updated_at or n.created_at, reverse=True)
            return notes

    async def upsert_note(
        self,
        user_id: int,
        note: RSNote,
        mode: str,
        note_id: Optional[str] = None,
    ) -> RSNote:
        async with self._lock:
            by_user = self._data.setdefault("notes_by_user", {})
            arr: List[Dict[str, Any]] = by_user.setdefault(str(user_id), [])

            if mode == "add":
                if len(arr) >= self.max_notes_per_user:
                    raise ValueError(f"Note limit reached ({self.max_notes_per_user}).")
                arr.append(asdict(note))
                await self._save_locked()
                return note

            if mode == "update":
                if not note_id:
                    raise ValueError("Missing note_id for update.")
                for i, existing in enumerate(arr):
                    if existing.get("id") == note_id:
                        created_at = existing.get("created_at") or note.created_at
                        merged = asdict(note)
                        merged["id"] = note_id
                        merged["created_at"] = created_at
                        merged["updated_at"] = _utcnow_iso()
                        arr[i] = merged
                        await self._save_locked()
                        return RSNote(**merged)
                raise ValueError("Note not found.")

            raise ValueError("Invalid mode.")

    async def delete_note(self, user_id: int, note_id: str) -> None:
        async with self._lock:
            by_user = self._data.setdefault("notes_by_user", {})
            arr: List[Dict[str, Any]] = by_user.setdefault(str(user_id), [])
            by_user[str(user_id)] = [x for x in arr if x.get("id") != note_id]
            await self._save_locked()


def _normalize_label(label: str) -> str:
    label = (label or "").strip()
    if label.upper() == "SKU":
        return "SKU"
    compact = label.lower().replace(" ", "")
    if compact in ("rspostlink", "post", "link"):
        return "RS Post Link"
    if compact in ("other", "notes", "note"):
        return "Other"
    if label in DEFAULT_ALLOWED_LABELS:
        return label
    return "Other"


def build_panel_embed(
    user: discord.abc.User,
    notes: List[RSNote],
    page: int,
    page_size: int,
) -> discord.Embed:
    total = len(notes)
    pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, pages - 1))

    e = discord.Embed(
        title="RS Notes",
        description="Private saved notes (only you can see this).",
    )
    e.set_footer(text=f"Page {page + 1}/{pages} | Notes: {total}")

    start = page * page_size
    chunk = notes[start : start + page_size]

    if not chunk:
        e.add_field(
            name="No notes yet",
            value="Use **Add** to save a SKU, RS post link, or any quick note you don't want buried.",
            inline=False,
        )
        return e

    for n in chunk:
        label = n.label if n.label in DEFAULT_ALLOWED_LABELS else "Other"
        title = _truncate(n.title or "(untitled)", 60)
        meta = f"Updated: {_human_ts(n.updated_at or n.created_at)}"
        e.add_field(
            name=f"{label} | {title}",
            value=meta,
            inline=False,
        )

    return e


def build_note_detail_block(
    note: RSNote,
    sku_search_channel_id: int,
    max_render_content_len: int,
) -> str:
    label = note.label if note.label in DEFAULT_ALLOWED_LABELS else "Other"
    title = _truncate(note.title or "(untitled)", 60)

    content = (note.content or "").strip()
    content = _truncate(content, max_render_content_len)

    lines: List[str] = [f"**{label} | {title}**"]

    if label == "SKU":
        lines.append(f"SKU:\n```{content}```")
        lines.append(f"Search: <#{sku_search_channel_id}>")
    elif label == "RS Post Link":
        if content:
            lines.append(f"Link: <{content}>")
    else:
        if content:
            lines.append(content)

    if note.rs_post_url:
        lines.append(f"Post: <{note.rs_post_url}>")

    lines.append(f"Created: {_human_ts(note.created_at)}")
    lines.append(f"Updated: {_human_ts(note.updated_at or note.created_at)}")
    return "\n".join(lines)


def note_select_options(notes: List[RSNote], page: int, page_size: int) -> List[discord.SelectOption]:
    total = len(notes)
    pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, pages - 1))
    start = page * page_size
    chunk = notes[start : start + page_size]

    opts: List[discord.SelectOption] = []
    for n in chunk:
        label = n.label if n.label in DEFAULT_ALLOWED_LABELS else "Other"
        opts.append(
            discord.SelectOption(
                label=_truncate(f"{label}: {n.title}", 100),
                value=n.id,
                description=_truncate((n.content or "").replace("\n", " "), 90),
            )
        )
    if not opts:
        opts.append(discord.SelectOption(label="No notes on this page", value="__none__"))
    return opts


class RSNoteUpsertModal(discord.ui.Modal):
    def __init__(
        self,
        parent: "RSNotePanelView",
        mode: str,
        existing: Optional[RSNote] = None,
    ):
        super().__init__(title="RS Note" if mode == "add" else "Update RS Note")
        self.parent = parent
        self.mode = mode
        self.existing = existing

        default_label = existing.label if existing else "SKU"
        default_title = existing.title if existing else ""
        default_content = existing.content if existing else ""
        default_post = existing.rs_post_url if existing else ""

        self.label_in = discord.ui.TextInput(
            label="Label (SKU / RS Post Link / Other)",
            required=True,
            max_length=30,
            default=default_label,
            placeholder="SKU",
        )
        self.title_in = discord.ui.TextInput(
            label="Title (short)",
            required=True,
            max_length=80,
            default=default_title,
            placeholder="Target Pokemon Tin",
        )
        self.content_in = discord.ui.TextInput(
            label="Content (SKU / link / note text)",
            required=True,
            style=discord.TextStyle.paragraph,
            max_length=1500,
            default=default_content,
            placeholder="B0FLMLDTPB",
        )
        self.post_in = discord.ui.TextInput(
            label="RS Post Link (optional)",
            required=False,
            max_length=300,
            default=default_post,
            placeholder="https://discord.com/channels/.../.../...",
        )

        self.add_item(self.label_in)
        self.add_item(self.title_in)
        self.add_item(self.content_in)
        self.add_item(self.post_in)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.parent.owner_id:
            await interaction.response.send_message("Not your notes panel.", ephemeral=True)
            return

        label = _normalize_label(str(self.label_in.value))
        title = str(self.title_in.value).strip()
        content = str(self.content_in.value).strip()
        rs_post_url = str(self.post_in.value).strip()

        try:
            if self.mode == "add":
                note = RSNote.new(label, title, content, rs_post_url=rs_post_url)
                await self.parent.store.upsert_note(self.parent.owner_id, note, mode="add")
            else:
                if not self.existing:
                    await interaction.response.send_message("No note selected.", ephemeral=True)
                    return
                note = RSNote(
                    id=self.existing.id,
                    label=label,
                    title=title,
                    content=content,
                    rs_post_url=rs_post_url,
                    created_at=self.existing.created_at,
                    updated_at=_utcnow_iso(),
                )
                await self.parent.store.upsert_note(
                    self.parent.owner_id, note, mode="update", note_id=self.existing.id
                )
        except ValueError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return

        await self.parent.refresh(interaction, toast="Saved.")


class RSNotePanelView(discord.ui.View):
    def __init__(self, store: RSNoteStore, owner_id: int, cfg: RSNoteConfig):
        super().__init__(timeout=300)
        self.store = store
        self.owner_id = owner_id
        self.cfg = cfg

        self.page = 0
        self.selected_note_id: Optional[str] = None

        self.select = discord.ui.Select(
            placeholder="Select a note (this page)",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="Loading...", value="__loading__")],
        )
        self.select.callback = self.on_select  # type: ignore
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("This notes panel isn't yours.", ephemeral=True)
            return False
        return True

    async def refresh(self, interaction: discord.Interaction, toast: str = "") -> None:
        notes = await self.store.get_notes(self.owner_id)
        self.select.options = note_select_options(notes, self.page, self.cfg.page_size)

        embed = build_panel_embed(
            interaction.user,
            notes,
            self.page,
            self.cfg.page_size,
        )

        detail = ""
        if self.selected_note_id:
            selected = next((n for n in notes if n.id == self.selected_note_id), None)
            if selected:
                detail = build_note_detail_block(
                    selected,
                    self.cfg.sku_search_channel_id,
                    self.cfg.max_render_content_len,
                )

        content: Optional[str] = toast if toast else None
        if detail:
            if content:
                content = content + "\n\n" + detail
            else:
                content = detail

        if interaction.response.is_done():
            await interaction.edit_original_response(content=content, embed=embed, view=self)
        else:
            await interaction.response.edit_message(content=content, embed=embed, view=self)

    async def on_select(self, interaction: discord.Interaction) -> None:
        val = self.select.values[0]
        self.selected_note_id = None if val in ("__none__", "__loading__") else val
        await self.refresh(interaction)

    @discord.ui.button(label="Add", style=discord.ButtonStyle.success)
    async def add_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interaction.response.send_modal(RSNoteUpsertModal(self, mode="add"))

    @discord.ui.button(label="Update", style=discord.ButtonStyle.primary)
    async def update_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.selected_note_id:
            await interaction.response.send_message("Select a note first.", ephemeral=True)
            return

        notes = await self.store.get_notes(self.owner_id)
        existing = next((n for n in notes if n.id == self.selected_note_id), None)
        if not existing:
            await interaction.response.send_message("That note no longer exists.", ephemeral=True)
            return

        await interaction.response.send_modal(RSNoteUpsertModal(self, mode="update", existing=existing))

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger)
    async def remove_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.selected_note_id:
            await interaction.response.send_message("Select a note first.", ephemeral=True)
            return
        await self.store.delete_note(self.owner_id, self.selected_note_id)
        self.selected_note_id = None
        await self.refresh(interaction, toast="Deleted.")

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.page = max(0, self.page - 1)
        await self.refresh(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.page += 1
        await self.refresh(interaction)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.refresh(interaction)


class RSNoteCog(commands.Cog):
    def __init__(self, bot: commands.Bot, cfg: RSNoteConfig, store: RSNoteStore):
        self.bot = bot
        self.cfg = cfg
        self.store = store
        self._ready = False

    async def cog_load(self) -> None:
        await self.store.load()
        self._ready = True

    @app_commands.command(name="rsnote", description="Private saved notes (SKU / RS links / anything).")
    async def rsnote(self, interaction: discord.Interaction) -> None:
        if not self._ready:
            await interaction.response.send_message("Notes store is still loading. Try again.", ephemeral=True)
            return

        view = RSNotePanelView(self.store, owner_id=interaction.user.id, cfg=self.cfg)
        notes = await self.store.get_notes(interaction.user.id)
        view.select.options = note_select_options(notes, view.page, self.cfg.page_size)

        embed = build_panel_embed(interaction.user, notes, view.page, self.cfg.page_size)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


def _module_root_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


async def setup(bot: commands.Bot) -> None:
    """
    For discord.py extension loader:
      await bot.load_extension("RSNotes.rsnote")
    """
    root = _module_root_dir()
    cfg = RSNoteConfig(root_dir=root)
    store = RSNoteStore(db_path=cfg.db_path(), max_notes_per_user=cfg.max_notes_per_user)
    await bot.add_cog(RSNoteCog(bot, cfg, store))

