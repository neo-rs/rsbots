from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import discord

log = logging.getLogger("rs-checker")

def fmt_date_any(ts_str: str | int | float | None) -> str:
    """Human-friendly date like 'January 8, 2026' (best-effort)."""
    try:
        if ts_str is None:
            return "—"
        if isinstance(ts_str, (int, float)):
            dt = datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
        else:
            s = str(ts_str).strip()
            if not s:
                return "—"
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        out = dt.astimezone(timezone.utc).strftime("%B %d, %Y")
        return out.replace(" 0", " ")
    except Exception:
        return "—"


def parse_dt_any(ts_str: str | int | float | None) -> datetime | None:
    """Parse ISO/unix-ish timestamps into UTC datetime (best-effort)."""
    if ts_str is None or ts_str == "":
        return None
    try:
        if isinstance(ts_str, (int, float)):
            return datetime.fromtimestamp(float(ts_str), tz=timezone.utc)
        s = str(ts_str).strip()
        if not s:
            return None
        # ISO-ish path
        if "T" in s or "-" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        # Unix-ish path (strings like "1700000000" or "1700000000.0")
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    except Exception:
        return None

def usd_amount(v: object) -> float:
    """Parse a USD-ish amount from strings like '$0', '0', '74,860.00', '$1.23'.

    Returns 0.0 on blanks/invalid values.
    """
    try:
        if v is None or v == "":
            return 0.0
        if isinstance(v, bool):
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return 0.0
        s = s.replace("$", "").replace(",", "")
        cleaned = "".join(ch for ch in s if ch.isdigit() or ch in ".-")
        return float(cleaned) if cleaned else 0.0
    except Exception:
        return 0.0


def extract_discord_id_from_whop_member_record(rec: dict) -> str:
    """Best-effort extract of Discord user ID from Whop /members/{mber_...} record.

    Safety: only returns an ID if it appears under a key-path containing 'discord'
    (avoids accidentally grabbing unrelated numeric IDs).
    """
    import re

    if not isinstance(rec, dict):
        return ""

    def _as_discord_id(v: object) -> str:
        m = re.search(r"\b(\d{17,19})\b", str(v or ""))
        return m.group(1) if m else ""

    def _walk(obj: object, *, discord_context: bool, depth: int) -> str:
        if depth > 6:
            return ""
        if isinstance(obj, dict):
            for k, v in obj.items():
                k_low = str(k or "").lower()
                ctx = discord_context or ("discord" in k_low)
                if ctx:
                    cand = _as_discord_id(v)
                    if cand:
                        return cand
                cand2 = _walk(v, discord_context=ctx, depth=depth + 1)
                if cand2:
                    return cand2
        elif isinstance(obj, list):
            for it in obj:
                cand3 = _walk(it, discord_context=discord_context, depth=depth + 1)
                if cand3:
                    return cand3
        return ""

    return _walk(rec, discord_context=False, depth=0)


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


def fmt_money(amount: object, currency: str | None = None) -> str:
    """Format Whop money values (usually floats) into a readable string."""
    if amount is None or amount == "":
        return ""
    try:
        amt = float(str(amount))
    except (ValueError, TypeError):
        return str(amount)
    cur = (currency or "").strip().lower()
    if cur in ("", "usd"):
        return f"${amt:.2f}"
    return f"{amt:.2f} {cur.upper()}"

