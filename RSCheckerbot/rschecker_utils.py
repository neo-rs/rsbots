from __future__ import annotations

import json
import logging
import os
import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import discord

log = logging.getLogger("rs-checker")

def fmt_date_any(ts_str: str | int | float | None) -> str:
    """Human-friendly date like 'January 8, 2026' (best-effort)."""
    try:
        dt = parse_dt_any(ts_str)
        if not dt:
            return "—"
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
            val = float(ts_str)
            # Heuristic: treat large values as milliseconds.
            if abs(val) > 1.0e11:
                val = val / 1000.0
            return datetime.fromtimestamp(val, tz=timezone.utc)
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
        val = float(s)
        if abs(val) > 1.0e11:
            val = val / 1000.0
        return datetime.fromtimestamp(val, tz=timezone.utc)
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
            # Special-case: connection objects often look like {"provider":"discord", ... "user_id":"123..."}
            try:
                prov = str(obj.get("provider") or obj.get("service") or "").strip().lower()
                if prov == "discord":
                    for k in ("user_id", "id", "uid", "account_id", "snowflake"):
                        cand = _as_discord_id(obj.get(k))
                        if cand:
                            return cand
                    # Fallback: scan all values in this dict for a discord-like id.
                    for _k, _v in obj.items():
                        cand = _as_discord_id(_v)
                        if cand:
                            return cand
            except Exception:
                pass
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
            if not data:
                return {}
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                # Best-effort salvage for corrupted files (e.g., concatenated JSON objects).
                # Extract the first complete JSON object/array from the file and parse it.
                s = data.lstrip()
                if not s:
                    return {}
                start = 0
                # Find first opening brace/bracket
                while start < len(s) and s[start] not in "{[":
                    start += 1
                if start >= len(s):
                    return {}
                open_ch = s[start]
                close_ch = "}" if open_ch == "{" else "]"
                depth = 0
                in_str = False
                esc = False
                end_idx = None
                for i in range(start, len(s)):
                    ch = s[i]
                    if in_str:
                        if esc:
                            esc = False
                            continue
                        if ch == "\\":
                            esc = True
                            continue
                        if ch == '"':
                            in_str = False
                        continue
                    if ch == '"':
                        in_str = True
                        continue
                    if ch == open_ch:
                        depth += 1
                    elif ch == close_ch:
                        depth -= 1
                        if depth == 0:
                            end_idx = i + 1
                            break
                if end_idx is None:
                    return {}
                candidate = s[start:end_idx].strip()
                if not candidate:
                    return {}
                obj = json.loads(candidate)
                # If we salvaged a dict, rewrite the file cleanly (best-effort).
                if isinstance(obj, dict):
                    try:
                        save_json(path, obj)
                    except Exception:
                        pass
                return obj if isinstance(obj, dict) else {}
    except Exception as e:
        log.error(f"Failed to read {path}: {e}. Treating as empty.")
        return {}


def save_json(path: Path, data: dict) -> None:
    """Atomic JSON write (tmp + replace)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique tmp name to avoid collisions between multiple processes.
    pid = str(os.getpid())
    tmp = path.with_suffix(path.suffix + f".tmp.{pid}")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    # Windows + OneDrive can transiently lock files; retry a few times.
    last_err: Exception | None = None
    for _attempt in range(6):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(0.05)
        except OSError as e:
            last_err = e
            time.sleep(0.05)
    # Cleanup tmp on failure (best-effort) then re-raise.
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    if last_err:
        raise last_err
    raise OSError("save_json failed")


_JSONL_LOCKS: dict[str, asyncio.Lock] = {}


def _jsonl_lock(path: Path) -> asyncio.Lock:
    key = str(Path(path).resolve())
    lock = _JSONL_LOCKS.get(key)
    if not lock:
        lock = asyncio.Lock()
        _JSONL_LOCKS[key] = lock
    return lock


async def append_jsonl(path: Path, record: dict) -> None:
    """Append a JSON record to a JSONL file with per-file locking."""
    lock = _jsonl_lock(path)
    async with lock:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def iter_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file and return records (best-effort)."""
    out: list[dict] = []
    p = Path(path)
    if not p.exists():
        return out
    try:
        with open(p, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    out.append(obj)
    except Exception:
        return out
    return out


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

