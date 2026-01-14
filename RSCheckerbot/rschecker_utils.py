from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import discord

log = logging.getLogger("rs-checker")


def load_json(path: Path) -> dict:
    """Load JSON from disk; return {} on missing/empty/invalid."""
    path = Path(path)
    if not path.exists():
        return {}
    try:
        if path.stat().st_size == 0:
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = f.read().strip()
            return {} if not data else json.loads(data)
    except Exception as e:
        log.error(f"Failed to read {path}: {e}. Treating as empty.")
        return {}


def save_json(path: Path, data: dict) -> None:
    """Atomic JSON write (tmp + replace)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def roles_plain(member: discord.Member) -> str:
    """Comma-separated role names (no role mentions, excludes @everyone).

    NOTE: We intentionally include managed roles because integrations (including Whop) often create managed roles,
    and support needs to see the *actual* role state.
    """
    roles = [r.name for r in member.roles if r != member.guild.default_role]
    return ", ".join(roles) if roles else "—"


def access_roles_plain(member: discord.Member, relevant_role_ids: set[int]) -> str:
    """Comma-separated role names for access-relevant roles only (no mentions, excludes @everyone)."""
    try:
        ids = {int(x) for x in (relevant_role_ids or set())}
    except Exception:
        ids = set()
    if not ids:
        return "—"
    names: list[str] = []
    seen: set[str] = set()
    for r in member.roles:
        if r == member.guild.default_role:
            continue
        if r.id not in ids:
            continue
        nm = str(r.name or "").strip()
        if not nm or nm in seen:
            continue
        seen.add(nm)
        names.append(nm)
    return ", ".join(names) if names else "—"


def coerce_role_ids(*values: object) -> set[int]:
    """Normalize mixed int/str role IDs into a set[int]."""
    out: set[int] = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, int):
            out.add(v)
            continue
        try:
            s = str(v).strip()
        except Exception:
            continue
        if s.isdigit():
            out.add(int(s))
    return out

