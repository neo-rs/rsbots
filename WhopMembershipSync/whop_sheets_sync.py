#!/usr/bin/env python3
"""
Whop Membership to Google Sheets Sync
Syncs Whop API membership data to Google Sheets for integration with GHL, KIT, and N8N.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("whop-sheets-sync")

# Add parent directory to path to import RSCheckerbot modules
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from RSCheckerbot.whop_api_client import WhopAPIClient, WhopAPIError
    from RSCheckerbot.rschecker_utils import extract_discord_id_from_whop_member_record
except ImportError:
    print("Error: Could not import WhopAPIClient. Make sure RSCheckerbot is available.")
    sys.exit(1)


def _cfg_str(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    v = str((cfg or {}).get(key) or "").strip()
    if v:
        return v
    return str(os.getenv(key.upper(), "") or "").strip() or default


def _cfg_bool(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    v = (cfg or {}).get(key)
    if isinstance(v, bool):
        return v
    if v is None:
        v = os.getenv(key.upper(), "")
    s = str(v or "").strip().lower()
    if not s:
        return default
    return s in {"1", "true", "yes", "y", "on"}


def _format_timestamp(timestamp_str: str) -> str:
    """Format timestamp string to readable date/time format for Google Sheets."""
    if not timestamp_str:
        return ""
    
    try:
        # Try parsing ISO format timestamp
        if "T" in timestamp_str:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        else:
            # Try parsing as Unix timestamp
            dt = datetime.fromtimestamp(float(timestamp_str), tz=timezone.utc)
        
        # Format as: YYYY-MM-DD HH:MM:SS UTC
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        # If parsing fails, return as-is
        return str(timestamp_str)


def _format_date_mmddyy(timestamp_str: str) -> str:
    """Format timestamp string to MM/DD/YY (UTC) for Google Sheets."""
    s = str(timestamp_str or "").strip()
    if not s:
        return ""
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
        else:
            dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
        return dt.strftime("%m/%d/%y")
    except Exception:
        return ""


def _extract_total_spend(mship: Dict[str, Any], member_record: Optional[Dict[str, Any]]) -> str:
    """
    Best-effort Total Spend extraction.
    Returns a string suitable for a sheet cell (e.g. "$30.00" or "30.00").
    """
    def _walk_find(obj: Any, *, keys: Set[str], out: List[Any], max_nodes: int = 2000) -> None:
        """Best-effort recursive search for keys in nested dict/list payloads."""
        seen = 0
        stack: List[Any] = [obj]
        while stack and seen < max_nodes:
            cur = stack.pop()
            seen += 1
            if isinstance(cur, dict):
                for k, v in cur.items():
                    ks = str(k or "").strip().lower()
                    if ks in keys:
                        out.append(v)
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(cur, list):
                for v in cur:
                    if isinstance(v, (dict, list)):
                        stack.append(v)

    candidates: List[Any] = []
    if isinstance(mship, dict):
        candidates.extend(
            [
                mship.get("total_spent"),
                mship.get("total_spend"),
                mship.get("amount_spent"),
                mship.get("lifetime_spend"),
            ]
        )
        # Try to find nested spend fields too (Whop payloads sometimes nest summaries)
        _walk_find(
            mship,
            keys={"total_spent", "total spend", "lifetime_spend", "lifetime spend", "amount_spent", "amount spent"},
            out=candidates,
        )
        # Special /members payload passthrough if present
        for k in ("_left_member_data", "_churned_member_data", "_canceling_member_data"):
            v = mship.get(k)
            if isinstance(v, dict):
                candidates.extend([v.get("total_spent"), v.get("total_spend"), v.get("lifetime_spend")])
    if isinstance(member_record, dict):
        candidates.extend(
            [
                member_record.get("total_spent"),
                member_record.get("total_spend"),
                member_record.get("amount_spent"),
                member_record.get("lifetime_spend"),
            ]
        )
        _walk_find(
            member_record,
            keys={"total_spent", "total spend", "lifetime_spend", "lifetime spend", "amount_spent", "amount spent"},
            out=candidates,
        )

    for v in candidates:
        if v is None or v == "" or v == {} or v == []:
            continue
        # Preserve already-formatted money strings
        if isinstance(v, str):
            s = v.strip()
            if not s:
                continue
            # Normalize common "$30" -> "$30.00" if it looks like dollars
            if s.startswith("$"):
                num = s[1:].replace(",", "").strip()
                try:
                    f = float(num)
                    return f"${f:.2f}"
                except Exception:
                    return s
            return s
        # Numeric
        try:
            f = float(v)  # type: ignore[arg-type]
            # If Whop returns cents as int, callers should convert; we do not guess here.
            return f"{f:.2f}"
        except Exception:
            continue
    return ""


def _member_key_from_row(row: List[str]) -> str:
    """
    Build a stable key for sheet rows.
    Prefer Email (col C), else Discord ID (col F). Returns "" if neither present.
    """
    try:
        email = str(row[2] or "").strip().lower()
    except Exception:
        email = ""
    if email:
        return email
    try:
        did = str(row[5] or "").strip()
    except Exception:
        did = ""
    return f"discord_{did}" if did else ""


def _col_letter(n_1_based: int) -> str:
    """Convert 1-based column index to Sheets column letter (A, B, ..., Z, AA...)."""
    n = int(n_1_based)
    if n <= 0:
        return "A"
    out = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _resolve_discord_id_from_identity_cache(cache: Dict[str, Any], email: str) -> str:
    """Return Discord ID from RSCheckerbot identity cache by email (best-effort)."""
    em = str(email or "").strip().lower()
    if not em or "@" not in em:
        return ""
    if not isinstance(cache, dict):
        return ""
    rec = cache.get(em)
    if not isinstance(rec, dict):
        return ""
    did = str(rec.get("discord_id") or "").strip()
    return did if did.isdigit() else ""


def _try_parse_service_account_json(raw: str) -> Optional[dict]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_service_account_info(cfg: Dict[str, Any]) -> Optional[dict]:
    """Load Google service account credentials from config."""
    # 1) dict already
    v = (cfg or {}).get("google_service_account_json")
    if isinstance(v, dict):
        return v

    # 2) inline JSON string
    if isinstance(v, str):
        parsed = _try_parse_service_account_json(v)
        if parsed:
            return parsed

    # 3) explicit file path
    p = _cfg_str(cfg, "google_service_account_file", "")
    if p:
        try:
            path = Path(p)
            if not path.is_absolute():
                repo_root = Path(__file__).resolve().parents[1]
                path = (repo_root / path).resolve()
            if path.exists():
                parsed = _try_parse_service_account_json(path.read_text(encoding="utf-8", errors="replace"))
                if parsed:
                    return parsed
        except Exception:
            pass

    # 4) env
    env = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "") or "").strip()
    if env:
        parsed = _try_parse_service_account_json(env)
        if parsed:
            return parsed
    return None


def _build_sheets_service(service_account_info: dict):
    """Build Google Sheets API service."""
    try:
        import warnings
        warnings.filterwarnings(
            "ignore",
            category=FutureWarning,
            module=r"google\.api_core\._python_version_support",
        )
    except Exception:
        pass
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _extract_discord_id(membership: Dict[str, Any], member_record: Optional[Dict[str, Any]] = None) -> str:
    """Extract Discord ID from membership or member record data.
    
    Based on Whop dashboard, Discord info is in:
    - Connected accounts section with Discord username and Discord ID (17-19 digits)
    - Example: Discord ID: 1077124475994251305
    """
    import re
    import json
    
    def _as_discord_id(v: object) -> str:
        """Extract Discord ID (17-19 digits) from a value."""
        if not v:
            return ""
        # Discord IDs are 17-19 digit numbers
        m = re.search(r"\b(\d{17,19})\b", str(v))
        return m.group(1) if m else ""
    
    def _check_connected_accounts(accounts: Any) -> str:
        """Check connected_accounts list for Discord ID."""
        if not isinstance(accounts, list):
            return ""
        
        for acc in accounts:
            if not isinstance(acc, dict):
                continue
            
            # Check provider type
            provider = str(acc.get("provider") or acc.get("service") or acc.get("type") or "").strip().lower()
            if provider != "discord":
                continue
            
            # Try all possible fields for Discord ID (17-19 digits)
            # Based on Whop API, it might be in various fields
            possible_keys = [
                "discord_id",
                "discordId", 
                "discord_user_id",
                "discordUserId",
                "user_id",
                "userId",
                "id",
                "provider_id",
                "providerId",
                "external_id",
                "externalId",
                "account_id",
                "accountId",
            ]
            
            for key in possible_keys:
                value = acc.get(key)
                if value:
                    did = _as_discord_id(value)
                    if did:
                        log.debug(f"      Found Discord ID in connected_accounts.{key}: {did}")
                        return did
            
            # Fallback: scan all string/numeric values in the account object
            for key, value in acc.items():
                if isinstance(value, (str, int, float)):
                    did = _as_discord_id(value)
                    if did and len(did) >= 17:  # Ensure it's a valid Discord ID length
                        log.debug(f"      Found Discord ID in connected_accounts.{key}: {did}")
                        return did
        
        return ""
    
    # First check member record (most reliable source - matches /members/{mber_...} endpoint)
    # Use RSCheckerbot's proven extraction function first
    if member_record and isinstance(member_record, dict):
        try:
            # Use the proven extraction function from RSCheckerbot
            discord_id = extract_discord_id_from_whop_member_record(member_record)
            if discord_id:
                log.debug(f"      Found Discord ID via RSCheckerbot extractor: {discord_id}")
                return discord_id
        except Exception as e:
            log.debug(f"      RSCheckerbot extractor failed: {e}")
        
        # Fallback: Check connected_accounts in member record (this is where Whop dashboard shows it)
        accounts = member_record.get("connected_accounts") or member_record.get("connectedAccounts") or []
        discord_id = _check_connected_accounts(accounts)
        if discord_id:
            return discord_id
        
        # Also check user object within member record
        user_in_member = member_record.get("user") or {}
        if isinstance(user_in_member, dict):
            accounts = user_in_member.get("connected_accounts") or user_in_member.get("connectedAccounts") or []
            discord_id = _check_connected_accounts(accounts)
            if discord_id:
                return discord_id
        
        # Check metadata in member record
        metadata = member_record.get("metadata") or {}
        if isinstance(metadata, dict):
            for key in ["discord_id", "discordId", "discord_user_id", "discordUserId"]:
                did = _as_discord_id(metadata.get(key))
                if did:
                    log.debug(f"      Found Discord ID in member_record.metadata.{key}: {did}")
                    return did
    
    # Fallback to membership user object
    user = membership.get("user") or {}
    if isinstance(user, dict):
        # Check connected_accounts in membership user
        accounts = user.get("connected_accounts") or user.get("connectedAccounts") or []
        discord_id = _check_connected_accounts(accounts)
        if discord_id:
            return discord_id
        
        # Check metadata
        metadata = user.get("metadata") or {}
        if isinstance(metadata, dict):
            for key in ["discord_id", "discordId", "discord_user_id"]:
                did = _as_discord_id(metadata.get(key))
                if did:
                    return did
    
    # Check membership-level metadata
    metadata = membership.get("metadata") or {}
    if isinstance(metadata, dict):
        for key in ["discord_id", "discordId"]:
            did = _as_discord_id(metadata.get(key))
            if did:
                return did
    
    return ""


def _extract_member_data(membership: Dict[str, Any], member_record: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Extract member data from membership and member records."""
    user = membership.get("user") or {}
    if isinstance(user, dict):
        user_id = str(user.get("id") or "").strip()
    else:
        user_id = str(membership.get("user_id") or "").strip()
    
    # Get member record for email/phone if available
    email = ""
    phone = ""
    name = ""
    
    if member_record:
        email = str(member_record.get("email") or "").strip()
        phone = str(member_record.get("phone") or "").strip()
        name = str(member_record.get("name") or member_record.get("username") or "").strip()
    
    # Fallback to user object
    if not email and isinstance(user, dict):
        email = str(user.get("email") or "").strip()
        name = str(user.get("name") or user.get("username") or "").strip()
    
    # Get product name
    product = ""
    product_obj = membership.get("product") or {}
    if isinstance(product_obj, dict):
        product = str(product_obj.get("title") or product_obj.get("name") or "").strip()
    
    # Get status
    status = str(membership.get("status") or "").strip()
    
    return {
        "name": name,
        "phone": phone,
        "email": email,
        "product": product,
        "status": status,
        "user_id": user_id,
    }


class WhopSheetsSync:
    """Sync Whop memberships to Google Sheets."""
    
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self._service = None
        self._api_lock = asyncio.Lock()
        self._last_error = ""
        # Discord-side identity cache (email -> discord_id), built by RSCheckerbot from native whop logs.
        self._identity_cache: Dict[str, Any] = {}
        self._identity_cache_mtime: float = 0.0
        self._identity_cache_loaded_at: float = 0.0
        # Member detail cache (mber_id -> {phone, fetched_at_iso})
        self._member_detail_cache: Dict[str, Any] = {}
        self._member_detail_cache_mtime: float = 0.0
        self._member_detail_cache_loaded_at: float = 0.0
        # GHL Website Data Info cache (email -> phone)
        self._ghl_phone_map: Dict[str, str] = {}
        self._ghl_phone_map_at: float = 0.0
        # Member history cache (discord_id -> total_spent string)
        self._mh_total_spent: Dict[str, str] = {}
        self._mh_total_spent_mtime: float = 0.0
        self._mh_total_spent_at: float = 0.0

    def _identity_cache_enabled(self) -> bool:
        return _cfg_bool(self.cfg, "discord_identity_cache_enabled", True)

    def _identity_cache_path(self) -> Path:
        p = _cfg_str(self.cfg, "discord_identity_cache_path", "RSCheckerbot/whop_identity_cache.json")
        path = Path(p)
        if not path.is_absolute():
            repo_root = Path(__file__).resolve().parents[1]
            path = (repo_root / path).resolve()
        return path

    def _load_identity_cache_if_needed(self, *, force: bool = False) -> Dict[str, Any]:
        if not self._identity_cache_enabled():
            self._identity_cache = {}
            self._identity_cache_mtime = 0.0
            self._identity_cache_loaded_at = 0.0
            return {}

        # Small TTL to avoid repeated disk reads during a sync cycle.
        now = 0.0
        try:
            now = float(datetime.now(timezone.utc).timestamp())
        except Exception:
            now = 0.0
        ttl_s = 30.0
        if (not force) and self._identity_cache and self._identity_cache_loaded_at and (now - self._identity_cache_loaded_at) < ttl_s:
            return self._identity_cache

        path = self._identity_cache_path()
        try:
            mtime = float(path.stat().st_mtime)
        except Exception:
            mtime = 0.0

        if (not force) and self._identity_cache and mtime and self._identity_cache_mtime and mtime == self._identity_cache_mtime:
            self._identity_cache_loaded_at = now
            return self._identity_cache

        raw: Dict[str, Any] = {}
        try:
            if path.exists():
                obj = json.loads(path.read_text(encoding="utf-8") or "{}")
                raw = obj if isinstance(obj, dict) else {}
        except Exception:
            raw = {}

        self._identity_cache = raw
        self._identity_cache_mtime = mtime
        self._identity_cache_loaded_at = now
        return self._identity_cache

    def _enrich_discord_id(self, *, email: str, current_discord_id: str) -> str:
        """If Discord ID missing, try to resolve from RSCheckerbot identity cache via email."""
        did = str(current_discord_id or "").strip()
        if did:
            return did
        em = str(email or "").strip()
        if not em:
            return ""
        cache = self._load_identity_cache_if_needed()
        return _resolve_discord_id_from_identity_cache(cache, em)

    def _member_detail_cache_enabled(self) -> bool:
        return _cfg_bool(self.cfg, "member_detail_cache_enabled", True)

    def _member_detail_cache_path(self) -> Path:
        p = _cfg_str(self.cfg, "member_detail_cache_path", "WhopMembershipSync/member_detail_cache.json")
        path = Path(p)
        if not path.is_absolute():
            repo_root = Path(__file__).resolve().parents[1]
            path = (repo_root / path).resolve()
        return path

    def _load_member_detail_cache_if_needed(self, *, force: bool = False) -> Dict[str, Any]:
        if not self._member_detail_cache_enabled():
            self._member_detail_cache = {}
            self._member_detail_cache_mtime = 0.0
            self._member_detail_cache_loaded_at = 0.0
            return {}

        now = 0.0
        try:
            now = float(datetime.now(timezone.utc).timestamp())
        except Exception:
            now = 0.0
        ttl_s = 30.0
        if (not force) and self._member_detail_cache and self._member_detail_cache_loaded_at and (now - self._member_detail_cache_loaded_at) < ttl_s:
            return self._member_detail_cache

        path = self._member_detail_cache_path()
        try:
            mtime = float(path.stat().st_mtime)
        except Exception:
            mtime = 0.0

        if (not force) and self._member_detail_cache and mtime and self._member_detail_cache_mtime and mtime == self._member_detail_cache_mtime:
            self._member_detail_cache_loaded_at = now
            return self._member_detail_cache

        raw: Dict[str, Any] = {}
        try:
            if path.exists():
                obj = json.loads(path.read_text(encoding="utf-8") or "{}")
                raw = obj if isinstance(obj, dict) else {}
        except Exception:
            raw = {}

        self._member_detail_cache = raw
        self._member_detail_cache_mtime = mtime
        self._member_detail_cache_loaded_at = now
        return self._member_detail_cache

    def _save_member_detail_cache(self) -> None:
        if not self._member_detail_cache_enabled():
            return
        path = self._member_detail_cache_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._member_detail_cache, indent=2, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, path)
            try:
                self._member_detail_cache_mtime = float(path.stat().st_mtime)
            except Exception:
                self._member_detail_cache_mtime = 0.0
        except Exception:
            pass

    def _cached_member_phone(self, member_id: str) -> str:
        mid = str(member_id or "").strip()
        if not mid:
            return ""
        cache = self._load_member_detail_cache_if_needed()
        rec = cache.get(mid) if isinstance(cache, dict) else None
        if not isinstance(rec, dict):
            return ""
        return str(rec.get("phone") or "").strip()

    def _set_cached_member_phone(self, member_id: str, phone: str) -> None:
        mid = str(member_id or "").strip()
        ph = str(phone or "").strip()
        if not (mid and ph):
            return
        cache = self._load_member_detail_cache_if_needed()
        if not isinstance(cache, dict):
            return
        try:
            fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            fetched_at = ""
        cache[mid] = {"phone": ph, "fetched_at": fetched_at}
        self._member_detail_cache = cache

    def _ghl_phone_enabled(self) -> bool:
        return _cfg_bool(self.cfg, "ghl_phone_enrichment_enabled", True)

    def _whop_member_detail_fetch_enabled(self) -> bool:
        # For your Whop account, /members/{id} does not return phone/discord, so this should usually be disabled.
        return _cfg_bool(self.cfg, "whop_member_detail_fetch_enabled", False)

    def _member_history_spend_enabled(self) -> bool:
        return _cfg_bool(self.cfg, "member_history_total_spent_enabled", True)

    def _member_history_path(self) -> Path:
        p = _cfg_str(self.cfg, "member_history_path", "RSCheckerbot/member_history.json")
        path = Path(p)
        if not path.is_absolute():
            repo_root = Path(__file__).resolve().parents[1]
            path = (repo_root / path).resolve()
        return path

    def _load_member_history_total_spent_if_needed(self, *, force: bool = False) -> Dict[str, str]:
        """Load discord_id -> total_spent from member_history.json (cached)."""
        if not self._member_history_spend_enabled():
            self._mh_total_spent = {}
            self._mh_total_spent_mtime = 0.0
            self._mh_total_spent_at = 0.0
            return {}

        now = 0.0
        try:
            now = float(datetime.now(timezone.utc).timestamp())
        except Exception:
            now = 0.0
        ttl_s = 60.0
        if (not force) and self._mh_total_spent and self._mh_total_spent_at and (now - self._mh_total_spent_at) < ttl_s:
            return dict(self._mh_total_spent)

        path = self._member_history_path()
        try:
            mtime = float(path.stat().st_mtime)
        except Exception:
            mtime = 0.0
        if (not force) and self._mh_total_spent and mtime and self._mh_total_spent_mtime and mtime == self._mh_total_spent_mtime:
            self._mh_total_spent_at = now
            return dict(self._mh_total_spent)

        raw: Dict[str, Any] = {}
        try:
            if path.exists():
                obj = json.loads(path.read_text(encoding="utf-8") or "{}")
                raw = obj if isinstance(obj, dict) else {}
        except Exception:
            raw = {}

        out: Dict[str, str] = {}
        # member_history is keyed by discord_id string
        for did, rec in (raw or {}).items():
            if not (isinstance(did, str) and did.isdigit() and isinstance(rec, dict)):
                continue
            wh = rec.get("whop") if isinstance(rec.get("whop"), dict) else {}
            ls = wh.get("last_summary") if isinstance(wh.get("last_summary"), dict) else {}
            ts = str((ls or {}).get("total_spent") or "").strip()
            if ts:
                out[did] = ts

        self._mh_total_spent = dict(out)
        self._mh_total_spent_mtime = mtime
        self._mh_total_spent_at = now
        # One-time-ish visibility: confirm member_history spend coverage on this host.
        try:
            if out:
                sample_items = list(out.items())[:3]
                sample_txt = ", ".join(f"{k}:{v}" for (k, v) in sample_items)
            else:
                sample_txt = ""
            log.info(
                f"  Loaded member_history total_spent map: {len(out)} entries"
                + (f" (samples: {sample_txt})" if sample_txt else "")
            )
        except Exception:
            pass
        return dict(out)

    def _enrich_total_spend_from_member_history(self, *, discord_id: str, current_total_spend: str) -> str:
        cur = str(current_total_spend or "").strip()
        if cur:
            return cur
        did = str(discord_id or "").strip()
        if not did.isdigit():
            return ""
        m = self._load_member_history_total_spent_if_needed()
        return str(m.get(did) or "").strip()

    def _resolve_discord_id_for_spend(
        self,
        *,
        email: str,
        discord_id: str,
        existing_discord_by_email: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Resolve a Discord ID for spend lookup without requiring column F to be populated.
        Order:
          1) provided discord_id
          2) existing sheet discord_id by email (if available)
          3) RSCheckerbot identity cache (email -> discord_id)
        """
        did = str(discord_id or "").strip()
        if did.isdigit():
            return did
        em = str(email or "").strip().lower()
        if em and isinstance(existing_discord_by_email, dict):
            did2 = str(existing_discord_by_email.get(em) or "").strip()
            if did2.isdigit():
                return did2
        if em:
            did3 = str(self._enrich_discord_id(email=em, current_discord_id="") or "").strip()
            if did3.isdigit():
                return did3
        return ""

    def _ghl_phone_tab_title(self) -> str:
        # Tab in the same spreadsheet: contains Email + Phone Number columns
        return _cfg_str(self.cfg, "ghl_phone_tab_name", "GHL Website Data Info")

    async def _load_ghl_phone_map_if_needed(self, *, force: bool = False) -> Dict[str, str]:
        """Load email->phone map from the GHL Website Data Info tab (cached)."""
        if not self._ghl_phone_enabled():
            self._ghl_phone_map = {}
            self._ghl_phone_map_at = 0.0
            return {}

        now = 0.0
        try:
            now = float(datetime.now(timezone.utc).timestamp())
        except Exception:
            now = 0.0

        ttl_s = 15 * 60.0
        if (not force) and self._ghl_phone_map and self._ghl_phone_map_at and (now - self._ghl_phone_map_at) < ttl_s:
            return dict(self._ghl_phone_map)

        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return {}
        service = self._get_service()
        if not service:
            return {}

        tab = self._ghl_phone_tab_title()
        rng = f"'{tab}'!A:Z"

        async with self._api_lock:
            def _do_get():
                return service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception:
                return {}

        values = resp.get("values") if isinstance(resp, dict) else None
        if not isinstance(values, list) or not values:
            self._ghl_phone_map = {}
            self._ghl_phone_map_at = now
            return {}

        header = values[0] if isinstance(values[0], list) else []
        header_n = [str(h or "").strip().lower() for h in header]

        def _find_col(names: Set[str]) -> int:
            for i, h in enumerate(header_n):
                if h in names:
                    return i
            # fallback: contains
            for i, h in enumerate(header_n):
                for nm in names:
                    if nm and nm in h:
                        return i
            return -1

        email_idx = _find_col({"email", "e-mail"})
        phone_idx = _find_col({"phone", "phone number", "phone_number", "phone #", "phonenumber"})
        if email_idx < 0 or phone_idx < 0:
            # Can't parse reliably
            self._ghl_phone_map = {}
            self._ghl_phone_map_at = now
            return {}

        out: Dict[str, str] = {}
        for row in values[1:]:
            if not isinstance(row, list):
                continue
            em = str(row[email_idx] if email_idx < len(row) else "" or "").strip().lower()
            ph = str(row[phone_idx] if phone_idx < len(row) else "" or "").strip()
            if not (em and "@" in em and ph):
                continue
            # first wins (avoid churn)
            if em not in out:
                out[em] = ph

        self._ghl_phone_map = dict(out)
        self._ghl_phone_map_at = now
        return dict(out)

    async def _enrich_phone_from_ghl(self, *, email: str, current_phone: str) -> str:
        ph = str(current_phone or "").strip()
        if ph:
            return ph
        em = str(email or "").strip().lower()
        if not em or "@" not in em:
            return ""
        m = await self._load_ghl_phone_map_if_needed()
        return str(m.get(em) or "").strip()
        
    def _get_service(self):
        """Get or create Google Sheets service."""
        if self._service is not None:
            return self._service
        
        log.debug("Loading Google service account credentials...")
        info = _load_service_account_info(self.cfg)
        if not info:
            self._last_error = "missing google service account json/file"
            log.error(f"  ✗ {self._last_error}")
            log.error("    Check config.secrets.json or RSForwarder/config.secrets.json for google_service_account_json")
            return None
        
        log.debug("Building Google Sheets API service...")
        try:
            self._service = _build_sheets_service(info)
            self._last_error = ""
            log.debug("✓ Google Sheets service initialized")
        except ImportError as e:
            self._service = None
            self._last_error = f"missing google libs: {e}"
            log.error(f"  ✗ {self._last_error}")
            log.error("    Install with: pip install google-api-python-client google-auth")
            return None
        except Exception as e:
            self._last_error = f"failed to initialize google sheets client: {type(e).__name__}: {e}"
            log.error(f"  ✗ {self._last_error}", exc_info=True)
            self._service = None
            return None
        
        return self._service
    
    async def _get_existing_tabs(self) -> List[str]:
        """Get list of all existing tab names in the spreadsheet."""
        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return []
        
        service = self._get_service()
        if not service:
            return []
        
        async with self._api_lock:
            def _do_get():
                return service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    fields="sheets(properties(sheetId,title))",
                ).execute()
            
            try:
                resp = await asyncio.to_thread(_do_get)
                sheets = resp.get("sheets") if isinstance(resp, dict) else None
                if isinstance(sheets, list):
                    tab_names = []
                    for sh in sheets:
                        props = sh.get("properties") if isinstance(sh, dict) else None
                        if isinstance(props, dict):
                            title = str(props.get("title") or "").strip()
                            if title:
                                tab_names.append(title)
                    return tab_names
            except Exception as e:
                log.debug(f"Failed to get existing tabs: {e}")
                return []
        
        return []
    
    async def _find_existing_tab(self, desired_tab_name: str) -> Optional[str]:
        """Find existing tab that matches desired name (case-insensitive, handles variations like Cancelled/Canceled)."""
        existing_tabs = await self._get_existing_tabs()
        desired_lower = desired_tab_name.strip().lower()
        
        # Exact match first
        for tab in existing_tabs:
            if tab.strip().lower() == desired_lower:
                return tab
        
        # Handle common variations
        variations = {
            "cancelled": ["canceled", "cancelling", "canceling"],
            "canceled": ["cancelled", "cancelling", "canceling"],
            "canceling": ["cancelling", "canceled", "cancelled"],
            "cancelling": ["canceling", "canceled", "cancelled"],
        }
        
        for base, variants in variations.items():
            if desired_lower == base:
                for variant in variants:
                    for tab in existing_tabs:
                        if tab.strip().lower() == variant:
                            log.info(f"  -> Found existing tab '{tab}' (matches desired '{desired_tab_name}')")
                            return tab
        
        return None
    
    async def _ensure_sheet_tab(self, tab_title: str) -> Tuple[Optional[str], Optional[str]]:
        """Ensure a tab exists in the spreadsheet. Returns (tab_name, error)."""
        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return None, "missing spreadsheet_id"
        
        service = self._get_service()
        if not service:
            return None, self._last_error or "missing google service"
        
        async with self._api_lock:
            def _do_get():
                return service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    fields="sheets(properties(sheetId,title))",
                ).execute()
            
            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception as e:
                return None, f"spreadsheets.get failed: {e}"
            
            sheets = resp.get("sheets") if isinstance(resp, dict) else None
            if isinstance(sheets, list):
                # Check for exact match first
                for sh in sheets:
                    props = sh.get("properties") if isinstance(sh, dict) else None
                    if not isinstance(props, dict):
                        continue
                    title = str(props.get("title") or "").strip()
                    if title == tab_title:
                        return title, None
                
                # Check for existing similar tab (handles Cancelled vs Canceled, etc.)
                # Build list of existing tabs from current response
                existing_tabs = []
                for sh in sheets:
                    props = sh.get("properties") if isinstance(sh, dict) else None
                    if isinstance(props, dict):
                        title = str(props.get("title") or "").strip()
                        if title:
                            existing_tabs.append(title)
                
                # Find matching tab
                desired_lower = tab_title.strip().lower()
                for tab in existing_tabs:
                    if tab.strip().lower() == desired_lower:
                        return tab, None
                
                # Handle common variations
                variations = {
                    "cancelled": ["canceled", "cancelling", "canceling"],
                    "canceled": ["cancelled", "cancelling", "canceling"],
                    "canceling": ["cancelling", "canceled", "cancelled"],
                    "cancelling": ["canceling", "canceled", "cancelled"],
                }
                
                for base, variants in variations.items():
                    if desired_lower == base:
                        for variant in variants:
                            for tab in existing_tabs:
                                if tab.strip().lower() == variant:
                                    log.info(f"  -> Using existing tab '{tab}' (matches desired '{tab_title}')")
                                    return tab, None
            
            # Not found -> create
            log.info(f"  -> Creating new tab '{tab_title}'...")
            def _do_add():
                return service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "requests": [
                            {
                                "addSheet": {
                                    "properties": {
                                        "title": tab_title,
                                        "gridProperties": {"frozenRowCount": 1},
                                    }
                                }
                            }
                        ]
                    },
                ).execute()
            
            try:
                resp2 = await asyncio.to_thread(_do_add)
                replies = resp2.get("replies") if isinstance(resp2, dict) else None
                if isinstance(replies, list) and replies:
                    props = replies[0].get("addSheet", {}).get("properties", {})
                    title2 = str(props.get("title") or tab_title).strip()
                    return title2, None
            except Exception as e:
                return None, f"addSheet failed: {e}"
            
            return tab_title, None
    
    async def _fetch_product_user_ids(self, whop_client: WhopAPIClient, product_id: str) -> Set[str]:
        """Fetch all user IDs that have any membership for the given product (for Lite-only exclusion)."""
        user_ids: Set[str] = set()
        statuses = ["trialing", "active", "past_due", "completed", "expired", "unresolved", "drafted"]
        for status_filter in statuses:
            after = None
            for _ in range(50):
                try:
                    batch, page_info = await whop_client.list_memberships(
                        first=100,
                        after=after,
                        params={"product_ids": [product_id], "statuses[]": [status_filter]},
                    )
                    for mship in batch:
                        if isinstance(mship, dict):
                            u = mship.get("user") or {}
                            if isinstance(u, dict):
                                uid = str(u.get("id") or "").strip()
                                if uid:
                                    user_ids.add(uid)
                    if not page_info.get("has_next_page"):
                        break
                    after = page_info.get("end_cursor")
                    if not after:
                        break
                except Exception:
                    break
        return user_ids
    
    async def sync_product_memberships(
        self,
        whop_client: WhopAPIClient,
        product_id: str,
        tab_name: str,
        exclude_user_ids: Optional[Set[str]] = None,
    ) -> Tuple[bool, str, int]:
        """
        Sync memberships for a specific product to a Google Sheets tab.
        Uses /members endpoint to get ALL statuses including Churned, Left, Cancelling.
        If exclude_user_ids is set (e.g. Main product user IDs when syncing Lite),
        only members who do NOT have that other product are included (Lite-only).
        
        Returns: (success, message, member_count)
        """
        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return False, "missing spreadsheet_id", 0
        
        # Ensure tab exists
        log.info(f"  → Ensuring tab '{tab_name}' exists...")
        print(f"  -> Ensuring tab '{tab_name}' exists...")
        tab_title, err = await self._ensure_sheet_tab(tab_name)
        if err or not tab_title:
            error_msg = f"failed to ensure tab: {err}"
            log.error(f"  ✗ {error_msg}")
            return False, error_msg, 0
        log.info(f"  ✓ Tab '{tab_title}' ready")
        print(f"  OK Tab '{tab_title}' ready")

        # Read existing tab rows once so we can preserve phone/discord and avoid refetching details.
        existing_rows = await self.read_source_tab(tab_title)
        existing_phone_by_email: Dict[str, str] = {}
        existing_discord_by_email: Dict[str, str] = {}
        for r in (existing_rows or []):
            if not isinstance(r, list) or len(r) < 3:
                continue
            em = str(r[2] or "").strip().lower()
            if not em:
                continue
            if len(r) > 1 and str(r[1] or "").strip():
                existing_phone_by_email[em] = str(r[1] or "").strip()
            if len(r) > 5 and str(r[5] or "").strip():
                existing_discord_by_email[em] = str(r[5] or "").strip()
        
        # Use /memberships endpoint directly - it has all the data we need
        # Valid statuses: trialing, active, past_due, completed, canceled, expired, unresolved, drafted
        # Note: "canceling" is a filter parameter - memberships show status="active"/"trialing" with cancel_at_period_end=true
        all_memberships = []
        
        log.info(f"  -> Fetching memberships from /memberships endpoint for product {product_id}...")
        print(f"  -> Fetching memberships from /memberships endpoint (includes ALL statuses)...")
        
        # Fetch ALL statuses EXCEPT "canceling" and "canceled"
        # - "canceling": We'll identify by cancel_at_period_end=true (avoids double-counting)
        # - "canceled": Removed - focus on "canceling" only
        statuses_to_fetch = ["trialing", "active", "past_due", "completed", "expired", "unresolved", "drafted"]
        seen_membership_ids = set()  # Deduplicate
        
        for status_filter in statuses_to_fetch:
            log.debug(f"    Fetching memberships with status filter: {status_filter}...")
            status_after = None
            status_max_pages = 50
            
            for page in range(status_max_pages):
                try:
                    if page == 0 or (page + 1) % 10 == 0:
                        print(f"    Fetching {status_filter} (page {page + 1})...", end="\r")
                    
                    batch, page_info = await whop_client.list_memberships(
                        first=100,
                        after=status_after,
                        params={
                            "product_ids": [product_id],
                            "statuses[]": [status_filter]
                        } if product_id else {"statuses[]": [status_filter]}
                    )
                    
                    batch_size = len(batch) if isinstance(batch, list) else 0
                    # Deduplicate by membership ID
                    for mship in batch:
                        if isinstance(mship, dict):
                            mship_id = str(mship.get("id") or "").strip()
                            if mship_id and mship_id not in seen_membership_ids:
                                seen_membership_ids.add(mship_id)
                                all_memberships.append(mship)
                    
                    if not page_info.get("has_next_page"):
                        break
                    status_after = page_info.get("end_cursor")
                    if not status_after:
                        break
                        
                except WhopAPIError as e:
                    # Some status filters might fail, continue with others
                    log.debug(f"    Status filter '{status_filter}' failed: {e}")
                    break
                except Exception as e:
                    log.debug(f"    Status filter '{status_filter}' error: {e}")
                    break
        
        print()  # New line after progress
        
        # Lite-only: exclude members who also have the other product (e.g. Main)
        if exclude_user_ids:
            before = len(all_memberships)
            all_memberships = [
                m for m in all_memberships
                if isinstance(m, dict) and str((m.get("user") or {}).get("id") or "").strip() not in exclude_user_ids
            ]
            log.info(f"  Lite-only filter: excluded {before - len(all_memberships)} members who have the other product")
            print(f"  OK Lite-only: excluded {before - len(all_memberships)} members who have the other product")
        
        # Build set of active member identifiers to exclude from special status lists
        # CRITICAL FIX: Some members appear in both active memberships AND special status lists
        # (e.g., "renewing"). The /members endpoint's memberships array may not include their
        # active membership, so we need to cross-reference with our active memberships list.
        active_member_emails = set()
        active_member_ids = set()
        
        for mship in all_memberships:
            if not isinstance(mship, dict):
                continue
            
            # Check if this is an active or trialing membership
            base_status = str(mship.get("status") or "").strip().lower()
            if base_status in ["active", "trialing"]:
                # Get email
                user_obj = mship.get("user") or {}
                if isinstance(user_obj, dict):
                    email = str(user_obj.get("email") or "").strip().lower()
                    if email:
                        active_member_emails.add(email)
                
                # Get member ID
                member_obj = mship.get("member") or {}
                if isinstance(member_obj, dict):
                    member_id = str(member_obj.get("id") or "").strip()
                    if member_id:
                        active_member_ids.add(member_id)
        
        log.debug(f"  Built active member set: {len(active_member_emails)} emails, {len(active_member_ids)} member IDs")
        
        # Also fetch special status members from /members endpoint
        # These use most_recent_action field: "left", "churned", "canceling"
        # IMPORTANT: We need to verify these members actually had a membership for THIS product
        # NOTE: "canceling" can be filtered directly via most_recent_actions[] parameter (matches dashboard)
        # Dashboard URL shows: members:most_recent_actions=canceling
        log.info(f"  -> Fetching special status members from /members endpoint (left, churned, canceling)...")
        print(f"  -> Fetching special status members from /members endpoint (left, churned, canceling)...")
        
        special_members = {"left": [], "churned": [], "canceling": []}
        special_after = None
        special_max_pages = 100  # Fetch more to find churned/canceling
        seen_member_ids = set()  # Deduplicate special members
        excluded_from_special = 0  # Track how many we exclude
        
        # Fetch members with most_recent_actions filter for canceling, churned
        # This matches the dashboard filter: members:most_recent_actions=canceling
        for action_type in ["canceling", "churned"]:
            action_after = None
            for page in range(special_max_pages):
                try:
                    if page == 0 or (page + 1) % 20 == 0:
                        print(f"    Fetching {action_type} members (page {page + 1})...", end="\r")
                    
                    params = {"product_ids": [product_id]} if product_id else {}
                    params["most_recent_actions[]"] = [action_type]
                    
                    batch, page_info = await whop_client.list_members(
                        first=100,
                        after=action_after,
                        params=params
                    )
                    
                    for member in batch:
                        if not isinstance(member, dict):
                            continue
                        
                        member_id = str(member.get("id") or "").strip()
                        if not member_id or member_id in seen_member_ids:
                            continue
                        
                        # CRITICAL FIX: Check if this member is already in our active memberships list
                        # Some members appear in both active and special status lists, but the
                        # /members endpoint's memberships array may not show their active membership
                        user_obj = member.get("user") or {}
                        email = ""
                        if isinstance(user_obj, dict):
                            email = str(user_obj.get("email") or "").strip().lower()
                        
                        # Check if member is already active (by email or member ID)
                        is_already_active = False
                        if email and email in active_member_emails:
                            is_already_active = True
                        elif member_id in active_member_ids:
                            is_already_active = True
                        
                        if is_already_active:
                            # Member already has active membership - exclude from special list
                            excluded_from_special += 1
                            continue
                        
                        # Verify product association
                        memberships = member.get("memberships") or []
                        has_product_membership = False
                        
                        if isinstance(memberships, list) and memberships:
                            for mship in memberships:
                                product_obj = mship.get("product") or {}
                                if isinstance(product_obj, dict):
                                    mship_product_id = str(product_obj.get("id") or "").strip()
                                    if mship_product_id == product_id:
                                        # IMPORTANT: For canceling/churned, check if they have active membership
                                        # If they have active membership, they'll be included via /memberships fetch
                                        # Only add to special list if they DON'T have active membership
                                        mship_status = str(mship.get("status") or "").strip().lower()
                                        if mship_status in ["active", "trialing"]:
                                            # Member has active membership - they'll be handled by regular fetch
                                            # Skip adding to special list to avoid duplicates
                                            has_product_membership = False
                                            break
                                        has_product_membership = True
                                        break
                        
                        if not has_product_membership:
                            if product_id and not memberships:
                                # No memberships array - trust API filter
                                has_product_membership = True
                            else:
                                continue
                        
                        if not has_product_membership:
                            continue
                        
                        # Lite-only: do not add if this user has the other product
                        if exclude_user_ids:
                            uid = str((user_obj.get("id") if isinstance(user_obj, dict) else None) or "").strip()
                            if uid in exclude_user_ids:
                                continue
                        
                        seen_member_ids.add(member_id)
                        special_members[action_type].append(member)
                    
                    if not page_info.get("has_next_page"):
                        break
                    action_after = page_info.get("end_cursor")
                    if not action_after:
                        break
                except Exception as e:
                    log.debug(f"    Error fetching {action_type} members: {e}")
                    break
        
        # Also fetch "left" members (status field, not most_recent_action)
        for page in range(special_max_pages):
            try:
                if page == 0 or (page + 1) % 20 == 0:
                    print(f"    Fetching left members (page {page + 1})...", end="\r")
                
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=special_after,
                    params={"product_ids": [product_id]} if product_id else {}
                )
                
                for member in batch:
                    if not isinstance(member, dict):
                        continue
                    
                    member_id = str(member.get("id") or "").strip()
                    if not member_id or member_id in seen_member_ids:
                        continue
                    
                    # CRITICAL FIX: Check if this member is already in our active memberships list
                    user_obj = member.get("user") or {}
                    email = ""
                    if isinstance(user_obj, dict):
                        email = str(user_obj.get("email") or "").strip().lower()
                    
                    # Check if member is already active (by email or member ID)
                    is_already_active = False
                    if email and email in active_member_emails:
                        is_already_active = True
                    elif member_id in active_member_ids:
                        is_already_active = True
                    
                    if is_already_active:
                        # Member already has active membership - exclude from "left" list
                        excluded_from_special += 1
                        continue
                    
                    # For "left" members, they don't have active memberships
                    # The product_ids filter should already filter them, but we need to be more strict
                    # Check if member has any indication they were associated with this product
                    memberships = member.get("memberships") or []
                    has_product_membership = False
                    
                    # Check memberships array if available
                    if isinstance(memberships, list) and memberships:
                        for mship in memberships:
                            product_obj = mship.get("product") or {}
                            if isinstance(product_obj, dict):
                                mship_product_id = str(product_obj.get("id") or "").strip()
                                if mship_product_id == product_id:
                                    # IMPORTANT: If member has an active membership, they're NOT "left"
                                    # Check if any membership is still active
                                    mship_status = str(mship.get("status") or "").strip().lower()
                                    if mship_status in ["active", "trialing"]:
                                        # Member has active membership - skip adding to "left" list
                                        # They'll be included via regular /memberships fetch
                                        has_product_membership = False
                                        break
                                    has_product_membership = True
                                    break
                    
                    # For "left" members without memberships array:
                    # The product_ids filter in /members endpoint should handle this
                    # But to be safe, if we have product_ids filter AND no memberships array,
                    # trust the filter (it means they were associated with this product at some point)
                    if not has_product_membership:
                        # Only trust the filter if we explicitly filtered by product_id
                        # AND the member has no memberships (meaning they're truly "left")
                        if product_id and not memberships:
                            # Trust the API filter - member was associated with this product
                            has_product_membership = True
                        else:
                            # Skip if we can't verify product association
                            continue
                    
                    if not has_product_membership:
                        continue
                    
                    # Lite-only: do not add if this user has the other product
                    if exclude_user_ids:
                        uid = str((user_obj.get("id") if isinstance(user_obj, dict) else None) or "").strip()
                        if uid in exclude_user_ids:
                            continue
                    
                    seen_member_ids.add(member_id)
                    
                    # Check status (for "left")
                    status = str(member.get("status") or "").strip().lower()
                    
                    # "left" is a status field - but only if they don't have active memberships
                    if status == "left":
                        special_members["left"].append(member)
                
                if not page_info.get("has_next_page"):
                    break
                special_after = page_info.get("end_cursor")
                if not special_after:
                    break
            except WhopAPIError as e:
                log.debug(f"    Error fetching special members: {e}")
                break
            except Exception as e:
                log.debug(f"    Error fetching special members: {e}")
                break
        
        print()  # New line
        log.info(f"  OK Found {len(special_members['left'])} 'left', {len(special_members['churned'])} 'churned', {len(special_members['canceling'])} 'canceling' members (for product {product_id})")
        print(f"  OK Found {len(special_members['left'])} 'left', {len(special_members['churned'])} 'churned', {len(special_members['canceling'])} 'canceling' members (for product {product_id})")
        if excluded_from_special > 0:
            log.info(f"  OK Excluded {excluded_from_special} special status members (already have active memberships)")
            print(f"  OK Excluded {excluded_from_special} special status members (already have active memberships)")
        
        # Convert special members to membership-like format for processing
        for status_type, members_list in special_members.items():
            for member in members_list:
                if not isinstance(member, dict):
                    continue
                
                # Convert real /members API "member" into same shape as /memberships "membership"
                # so the rest of the pipeline can use one format. All data is from Whop API.
                member_updated = member.get("updated_at") or member.get("created_at") or ""
                member_as_membership = {
                    "id": f"{status_type}_{member.get('id', '')}",
                    "status": status_type,
                    "member": {"id": str(member.get("id") or "").strip()},
                    "user": member.get("user") or {},
                    "product": {"id": product_id, "title": "Reselling Secrets" if "Reselling Secrets" in tab_name and "Lite" not in tab_name else "Reselling Secrets Lite"},
                    "updated_at": member_updated,
                    "created_at": member.get("created_at") or member_updated,
                    f"_is_{status_type}_member": True,
                    f"_{status_type}_member_data": member,
                }
                all_memberships.append(member_as_membership)
        
        total_special = sum(len(m) for m in special_members.values())
        log.info(f"  ✓ Found {len(all_memberships)} total memberships (including {total_special} special status members)")
        print(f"  OK Found {len(all_memberships)} total memberships (including {total_special} special status members)")
        
        # Note: Status breakdown will be calculated AFTER deduplication
        # This is just a preliminary count from raw memberships
        
        log.info(f"  -> Processing {len(all_memberships)} memberships...")
        print(f"  -> Processing {len(all_memberships)} memberships...")
        
        # Extract member IDs for fetching detailed records (phone primarily; Discord ID is Discord-side cache).
        # We only fetch detail records when we cannot satisfy phone from:
        #  - existing sheet row (by email), or
        #  - local member_detail_cache (by member_id)
        member_ids: Set[str] = set()
        member_id_email_map: Dict[str, str] = {}
        for mship in all_memberships:
            # Get member_id from membership.member field
            member_obj = mship.get("member") or {}
            if isinstance(member_obj, dict):
                member_id = str(member_obj.get("id") or "").strip()
                if member_id and member_id.startswith("mber_"):
                    member_ids.add(member_id)
                    user_obj = mship.get("user") or {}
                    em = ""
                    if isinstance(user_obj, dict):
                        em = str(user_obj.get("email") or "").strip().lower()
                    if em:
                        member_id_email_map[member_id] = em
        
        total_member_ids = len(member_ids)
        log.info(f"  -> Extracted {total_member_ids} unique member IDs (detail fetch minimized)")
        print(f"  -> Extracted {total_member_ids} unique member IDs (detail fetch minimized)")

        # Decide which member IDs actually need a detail fetch.
        # If phone is already known (sheet or cache), skip.
        member_cache: Dict[str, Any] = {}
        need_fetch: List[str] = []
        ghl_map = await self._load_ghl_phone_map_if_needed()
        for mid in sorted(member_ids):
            em = member_id_email_map.get(mid, "")
            phone_from_sheet = existing_phone_by_email.get(em, "") if em else ""
            phone_from_cache = self._cached_member_phone(mid)
            phone_from_ghl = str(ghl_map.get(em) or "").strip() if em else ""
            if phone_from_sheet or phone_from_cache or phone_from_ghl:
                if phone_from_cache:
                    member_cache[mid] = {"phone": phone_from_cache}
                continue
            need_fetch.append(mid)

        member_ids_list = list(need_fetch)
        log.info(f"  -> Detail fetch required for {len(member_ids_list)}/{total_member_ids} members (missing phone)")
        print(f"  -> Detail fetch required for {len(member_ids_list)}/{total_member_ids} members (missing phone)")

        if not self._whop_member_detail_fetch_enabled():
            log.info("  -> Detail fetch disabled (whop_member_detail_fetch_enabled=false). Using sheet+GHL only.")
            print("  -> Detail fetch disabled (whop_member_detail_fetch_enabled=false). Using sheet+GHL only.")
            member_ids_list = []
        
        log.info(f"  -> Fetching {len(member_ids_list)} member records from /members/{{mber_...}} endpoint...")
        print(f"  -> Fetching {len(member_ids_list)} member records from /members/{{mber_...}} endpoint...")
        
        success_count = 0
        error_count = 0
        
        for i, member_id in enumerate(member_ids_list):
            try:
                # Update progress more frequently
                if (i + 1) % 10 == 0 or i == 0 or (i + 1) == len(member_ids_list):
                    progress_pct = int((i + 1) / len(member_ids_list) * 100)
                    log.debug(f"    Fetching member records: {i + 1}/{len(member_ids_list)} ({progress_pct}%)")
                    print(f"    Fetching member records: {i + 1}/{len(member_ids_list)} ({progress_pct}%) - Found: {success_count}, Not found: {error_count}", end="\r")
                
                # Use member_id (mber_...) to fetch member record - this is the correct endpoint
                member = await whop_client.get_member_by_id(member_id)
                if member:
                    member_cache[member_id] = member
                    # Persist phone into local cache for future cycles
                    if isinstance(member, dict):
                        ph = str(member.get("phone") or "").strip()
                        if ph:
                            self._set_cached_member_phone(member_id, ph)
                    success_count += 1
                else:
                    # "Not found" is normal - some member IDs might not exist
                    error_count += 1
                
                # Small delay every 10 requests to avoid rate limiting
                if (i + 1) % 10 == 0:
                    await asyncio.sleep(0.1)
                    
            except WhopAPIError as e:
                error_count += 1
                # Only log if it's not a "not found" error (those are expected)
                if "not found" not in str(e).lower():
                    log.debug(f"    Whop API error fetching member {uid}: {str(e)}")
                # Only print progress every 50th error to avoid spam
                if (i + 1) % 50 == 0:
                    print(f"    WARNING: Some members not found (normal) - Progress: {i + 1}/{len(member_ids_list)} ({int((i + 1) / len(member_ids_list) * 100)}%)", end="\r")
            except Exception as e:
                error_count += 1
                log.debug(f"    Unexpected error fetching member {member_id}: {type(e).__name__}: {str(e)}")
                if (i + 1) % 50 == 0:
                    print(f"    WARNING: Errors encountered (checking member {i + 1}/{len(member_ids_list)})...", end="\r")
        
        print()  # New line after progress
        log.info(f"  ✓ Fetched {success_count} member records successfully ({error_count} errors)")
        print(f"  OK Fetched {success_count} member records successfully ({error_count} errors)")
        # Best-effort persist cache changes
        self._save_member_detail_cache()
        
        # Build rows:
        #   A Name, B Phone Number, C Email, D Product, E Status, F Discord ID, G Status Updated,
        #   H Date Left, I Date Joined, J Total Spend
        # IMPORTANT: Each member should appear in ONLY ONE status tab (most recent status)
        # Group by member identifier (email or Discord ID) and pick the most recent status
        headers = [
            "Name",
            "Phone Number",
            "Email",
            "Product",
            "Status",
            "Discord ID",
            "Status Updated",
            "Date Left",
            "Date Joined",
            "Total Spend",
        ]
        
        # Track members by email and Discord ID to ensure one status per member
        member_status_map: Dict[str, Dict[str, Any]] = {}  # key: email_lower or discord_id -> {status, row_data, updated_at}
        
        rows_with_email = 0
        rows_with_phone = 0
        rows_with_discord = 0
        rows_with_spend = 0
        rows_spend_from_member_history = 0
        rows_spend_from_whop = 0
        spend_samples: List[str] = []
        
        # Status priority: IMPORTANT - "left" only applies if member has NO active membership
        # Active memberships take precedence over "left" status from /members endpoint
        # Priority: canceling > renewing > active > trialing > churned > expired > completed > past_due > unresolved > drafted > left
        status_priority = {
            "canceling": 1,  # Highest priority - active membership being canceled
            "renewing": 2,
            "active": 3,
            "trialing": 4,
            "churned": 5,
            "expired": 6,
            "completed": 7,
            "past_due": 8,
            "unresolved": 9,
            "drafted": 10,
            "left": 11,  # Lowest priority - only if no active membership exists
        }
        
        def get_status_priority(status: str) -> int:
            """Get priority for status (lower = higher priority)."""
            return status_priority.get(status.lower(), 999)
        
        for idx, mship in enumerate(all_memberships):
            try:
                if (idx + 1) % 50 == 0 or idx == 0:
                    progress_pct = int((idx + 1) / len(all_memberships) * 100)
                    log.debug(f"    Processing memberships: {idx + 1}/{len(all_memberships)} ({progress_pct}%)")
                    print(f"    Processing: {idx + 1}/{len(all_memberships)} ({progress_pct}%)", end="\r")
                
                if not isinstance(mship, dict):
                    continue
                
                # Get member_id from membership.member field
                member_obj = mship.get("member") or {}
                member_id = None
                if isinstance(member_obj, dict):
                    member_id = str(member_obj.get("id") or "").strip()
                
                # Get detailed member record (for Discord ID, phone)
                member_record = member_cache.get(member_id) if member_id else None
                
                # Extract data from membership
                # Check if this is a "canceling" membership (active/trialing with cancel_at_period_end=true)
                # IMPORTANT: Only mark as "canceling" if status is active/trialing AND cancel_at_period_end=true
                # Do NOT mark canceled/expired memberships as "canceling"
                base_status = str(mship.get("status") or "").strip().lower()
                
                # Skip canceled status - we're removing it
                if base_status == "canceled":
                    continue
                
                if mship.get("cancel_at_period_end") is True and base_status in ["active", "trialing"]:
                    status = "canceling"
                else:
                    status = base_status
                
                # Get user data from membership.user field
                user_obj = mship.get("user") or {}
                name = ""
                email = ""
                if isinstance(user_obj, dict):
                    name = str(user_obj.get("name") or user_obj.get("username") or "").strip()
                    email = str(user_obj.get("email") or "").strip()
                
                # For special status members (left, churned), get data from member_data if available
                for status_type in ["left", "churned"]:
                    if mship.get(f"_is_{status_type}_member") and mship.get(f"_{status_type}_member_data"):
                        special_data = mship.get(f"_{status_type}_member_data")
                        if isinstance(special_data, dict):
                            if not name:
                                name = str(special_data.get("name") or special_data.get("username") or "").strip()
                            if not email:
                                user_from_special = special_data.get("user") or {}
                                if isinstance(user_from_special, dict):
                                    email = str(user_from_special.get("email") or "").strip()
                                    if not name:
                                        name = str(user_from_special.get("name") or user_from_special.get("username") or "").strip()
                            break
                
                # Phone: preserve from existing sheet row if present; otherwise try GHL Website Data Info tab (email->phone).
                phone = ""
                if email:
                    phone = str(existing_phone_by_email.get(email.strip().lower(), "") or "").strip()
                if not phone and isinstance(member_record, dict):
                    phone = str(member_record.get("phone") or "").strip()
                if not phone:
                    # Try from special member data
                    for status_type in ["left", "churned"]:
                        if mship.get(f"_{status_type}_member_data"):
                            special_data = mship.get(f"_{status_type}_member_data")
                            if isinstance(special_data, dict):
                                phone = str(special_data.get("phone") or "").strip()
                                if phone:
                                    break
                if not phone and email:
                    phone = await self._enrich_phone_from_ghl(email=email, current_phone=phone)
                
                # Get product name from membership
                product_obj = mship.get("product") or {}
                product_name = ""
                if isinstance(product_obj, dict):
                    product_name = str(product_obj.get("title") or product_obj.get("name") or "").strip()
                
                # Fallback product name
                if not product_name:
                    product_name = "Reselling Secrets" if "Reselling Secrets" in tab_name and "Lite" not in tab_name else "Reselling Secrets Lite"
                
                # Discord ID:
                # - Whop-side extraction (usually blank in your account)
                # - Preserve existing sheet Discord ID by email (keeps previously linked rows stable)
                # - Fallback to RSCheckerbot identity cache (email -> discord_id)
                discord_id = _extract_discord_id(mship, member_record)
                if not discord_id and email:
                    discord_id = str(existing_discord_by_email.get(email.strip().lower(), "") or "").strip() or discord_id
                if not discord_id and email:
                    discord_id = self._enrich_discord_id(email=email, current_discord_id=discord_id)
                
                # Track data completeness
                if email:
                    rows_with_email += 1
                if phone:
                    rows_with_phone += 1
                if discord_id:
                    rows_with_discord += 1
                
                # Get updated_at timestamp for determining most recent status
                updated_at = mship.get("updated_at") or mship.get("created_at") or ""
                formatted_timestamp = _format_timestamp(updated_at)

                # Joined/Left dates (MM/DD/YY)
                created_at = mship.get("created_at") or ""
                date_joined = _format_date_mmddyy(str(created_at or ""))
                left_statuses = {"left", "expired", "completed", "churned"}
                date_left = _format_date_mmddyy(str(updated_at or "")) if status in left_statuses else ""

                total_spend = _extract_total_spend(mship, member_record)
                if not total_spend:
                    # Spend fallback: resolve DID via email (no need for column F to be present)
                    did_for_spend = self._resolve_discord_id_for_spend(
                        email=email,
                        discord_id=discord_id,
                        existing_discord_by_email=existing_discord_by_email,
                    )
                    if did_for_spend:
                        total_spend = self._enrich_total_spend_from_member_history(
                            discord_id=did_for_spend,
                            current_total_spend=total_spend,
                        )
                if total_spend:
                    rows_with_spend += 1
                    rows_spend_from_whop += 1

                # Spend fallback: resolve DID via email (no need for column F to be present)
                if not total_spend:
                    did_for_spend = self._resolve_discord_id_for_spend(
                        email=email,
                        discord_id=discord_id,
                        existing_discord_by_email=existing_discord_by_email,
                    )
                    if did_for_spend:
                        total_spend = self._enrich_total_spend_from_member_history(
                            discord_id=did_for_spend,
                            current_total_spend=total_spend,
                        )
                        if total_spend:
                            rows_with_spend += 1
                            rows_spend_from_member_history += 1

                if total_spend and len(spend_samples) < 5:
                    spend_samples.append(f"{email or '-'} did={discord_id or '-'} spend={total_spend}")
                if not total_spend and discord_id:
                    total_spend = self._enrich_total_spend_from_member_history(
                        discord_id=discord_id,
                        current_total_spend=total_spend,
                    )

                # Joined/Left dates (MM/DD/YY)
                created_at = mship.get("created_at") or ""
                date_joined = _format_date_mmddyy(str(created_at or ""))
                # Only populate Date Left for left-ish statuses
                left_statuses = {"left", "expired", "completed", "churned"}
                date_left = _format_date_mmddyy(str(updated_at or "")) if status in left_statuses else ""

                total_spend = _extract_total_spend(mship, member_record)
                
                # Determine member key (prefer email, fallback to Discord ID, then member ID)
                # CRITICAL: Active members should NEVER be skipped - use member ID as last resort
                member_key = None
                if email:
                    member_key = email.strip().lower()
                elif discord_id:
                    member_key = f"discord_{discord_id}"
                elif member_id and member_id.startswith("mber_"):
                    # For active/trialing members, use member ID as fallback to ensure 100% accuracy
                    if status in ["active", "trialing"]:
                        member_key = f"member_{member_id}"
                    else:
                        # For other statuses, skip if no email/Discord ID
                        continue
                else:
                    # Skip if no identifier (except for active/trialing which are handled above)
                    continue
                
                # Check if we already have this member with a status
                existing = member_status_map.get(member_key)
                current_priority = get_status_priority(status)
                
                if existing:
                    existing_priority = get_status_priority(existing.get("status", ""))
                    # IMPORTANT: Active memberships (from /memberships) take precedence over "left" (from /members)
                    # Only use "left" if there's no active membership
                    if existing.get("status", "").lower() == "left" and current_priority < 11:
                        # Existing is "left" but current is an active membership - replace with active
                        member_status_map[member_key] = {
                            "status": status,
                            "row": [
                                name,
                                phone,
                                email,
                                product_name,
                                status,
                                discord_id,
                                formatted_timestamp,
                                date_left,
                                date_joined,
                                total_spend,
                            ],
                            "updated_at": updated_at,
                        }
                    elif current_priority < existing_priority:
                        # Current status has higher priority, replace
                        member_status_map[member_key] = {
                            "status": status,
                            "row": [
                                name,
                                phone,
                                email,
                                product_name,
                                status,
                                discord_id,
                                formatted_timestamp,
                                date_left,
                                date_joined,
                                total_spend,
                            ],
                            "updated_at": updated_at,
                        }
                    elif current_priority == existing_priority and updated_at > existing.get("updated_at", ""):
                        # Same priority but more recent, replace
                        member_status_map[member_key] = {
                            "status": status,
                            "row": [
                                name,
                                phone,
                                email,
                                product_name,
                                status,
                                discord_id,
                                formatted_timestamp,
                                date_left,
                                date_joined,
                                total_spend,
                            ],
                            "updated_at": updated_at,
                        }
                    # Otherwise keep existing
                else:
                    # First time seeing this member
                    member_status_map[member_key] = {
                        "status": status,
                        "row": [
                            name,
                            phone,
                            email,
                            product_name,
                            status,
                            discord_id,
                            formatted_timestamp,
                            date_left,
                            date_joined,
                            total_spend,
                        ],
                        "updated_at": updated_at,
                    }
                
                # Debug: Log sample data (only first 3 to avoid spam)
                if idx < 3:
                    if discord_id:
                        log.info(f"    OK Sample {idx + 1}: Status={status}, Discord ID={discord_id}")
                    else:
                        log.info(f"    WARNING: Sample {idx + 1}: Status={status}, No Discord ID")
            except Exception as e:
                log.error(f"    Error processing membership {idx + 1}: {type(e).__name__}: {str(e)}", exc_info=True)
                # Continue processing other memberships even if one fails
                continue
        
        # Convert member_status_map to rows (one row per member)
        rows = []
        status_breakdown = {}
        skipped_no_id = 0
        active_skipped = 0
        
        # Count how many active memberships were skipped
        for mship in all_memberships:
            if not isinstance(mship, dict):
                continue
            
            base_status = str(mship.get("status") or "").strip().lower()
            if base_status == "canceled":
                continue
            
            # Check if this would be active
            if mship.get("cancel_at_period_end") is True and base_status in ["active", "trialing"]:
                final_status = "canceling"
            else:
                final_status = base_status
            
            if final_status == "active":
                user_obj = mship.get("user") or {}
                email = ""
                if isinstance(user_obj, dict):
                    email = str(user_obj.get("email") or "").strip()
                
                member_obj = mship.get("member") or {}
                member_id = None
                if isinstance(member_obj, dict):
                    member_id = str(member_obj.get("id") or "").strip()
                
                discord_id = ""
                if member_id:
                    member_record = member_cache.get(member_id) if member_id else None
                    discord_id = _extract_discord_id(mship, member_record)
                
                if not email and not discord_id:
                    skipped_no_id += 1
                    active_skipped += 1
        
        for member_key, member_data in member_status_map.items():
            rows.append(member_data["row"])
            final_status = member_data.get("status", "").lower()
            status_breakdown[final_status] = status_breakdown.get(final_status, 0) + 1
        
        log.info(f"  -> Deduplicated: {len(all_memberships)} memberships -> {len(rows)} unique members")
        print(f"  -> Deduplicated: {len(all_memberships)} memberships -> {len(rows)} unique members")
        log.info(f"  -> Skipped (no email/Discord ID): {skipped_no_id} total ({active_skipped} active)")
        print(f"  -> Skipped (no email/Discord ID): {skipped_no_id} total ({active_skipped} active)")
        log.info(f"  -> Status breakdown before final write: {dict(status_breakdown)}")
        print(f"  -> Status breakdown before final write: {dict(status_breakdown)}")
        
        # Warn if active count is suspiciously low
        active_count = status_breakdown.get("active", 0)
        if active_count < 50:
            log.warning(f"  WARNING: Only {active_count} active members found - expected ~226 from API!")
            print(f"  WARNING: Only {active_count} active members found - expected ~226 from API!")
        
        print()  # New line after progress
        
        # Calculate final status breakdown from deduplicated rows
        final_status_counts: Dict[str, int] = {}
        for row in rows:
            if len(row) > 4:
                status = str(row[4] or "").strip().lower()
                if status:
                    final_status_counts[status] = final_status_counts.get(status, 0) + 1
        
        if final_status_counts:
            log.info(f"  Final status breakdown (after deduplication): {dict(final_status_counts)}")
            print(f"  Final status breakdown (after deduplication): {dict(final_status_counts)}")
        
        # Log trialing members with timestamp (for trial-abuse tracking)
        trialing_rows = [r for r in rows if len(r) > 4 and str(r[4] or "").strip().lower() == "trialing"]
        if trialing_rows:
            log.info(f"  Trialing members ({len(trialing_rows)}) - record for trial-abuse detection:")
            print(f"  Trialing members ({len(trialing_rows)}) - record for trial-abuse detection:")
            for r in trialing_rows[:20]:
                email = str(r[2] or "").strip() if len(r) > 2 else ""
                ts = str(r[6] or "").strip() if len(r) > 6 else ""
                log.info(f"    {email} | Status Updated={ts}")
                print(f"    {email} | Status Updated={ts}")
            if len(trialing_rows) > 20:
                log.info(f"    ... and {len(trialing_rows) - 20} more")
                print(f"    ... and {len(trialing_rows) - 20} more")
        
        log.info(f"  OK Built {len(rows)} rows (Email: {rows_with_email}, Phone: {rows_with_phone}, Discord: {rows_with_discord})")
        print(f"  OK Built {len(rows)} rows (Email: {rows_with_email}, Phone: {rows_with_phone}, Discord: {rows_with_discord})")
        log.info(
            f"  Spend coverage: {rows_with_spend}/{len(rows)} (from_whop={rows_spend_from_whop}, from_member_history={rows_spend_from_member_history})"
        )
        print(
            f"  Spend coverage: {rows_with_spend}/{len(rows)} (from_whop={rows_spend_from_whop}, from_member_history={rows_spend_from_member_history})"
        )
        if spend_samples:
            log.info("  Spend samples (up to 5): " + " | ".join(spend_samples))
        
        # Write to sheet with diff-upsert (only touches changed/new/stale members)
        log.info(f"  -> Writing {len(rows)} rows to Google Sheets tab '{tab_title}' (diff-upsert)...")
        print(f"  -> Writing {len(rows)} rows to Google Sheets tab '{tab_title}' (diff-upsert)...")
        log.info(f"  -> Status breakdown for '{tab_title}': {dict(final_status_counts)}")
        print(f"  -> Status breakdown for '{tab_title}': {dict(final_status_counts)}")
        
        success, msg = await self._write_tab_diff_upsert(tab_title, headers, rows, log_context=f"Tab '{tab_title}'")
        if not success:
            log.error(f"  ✗ {msg}")
            return False, msg, len(rows)
        
        log.info(f"    ✓ Wrote {len(rows)} rows to sheet")
        print(f"    OK Wrote {len(rows)} rows to sheet")
        return True, "ok", len(rows)
    
    async def read_source_tab(self, tab_name: str) -> List[List[str]]:
        """Read all rows from a source tab (excluding header)."""
        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return []
        
        service = self._get_service()
        if not service:
            return []
        
        async with self._api_lock:
            def _do_get():
                return service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{tab_name}'!A:J",
                ).execute()
            
            try:
                resp = await asyncio.to_thread(_do_get)
                values = resp.get("values") if isinstance(resp, dict) else None
                if isinstance(values, list) and len(values) > 1:
                    # Skip header row, return data rows
                    return values[1:]
            except Exception as e:
                log.error(f"Failed to read source tab '{tab_name}': {e}")
                return []
        
        return []
    
    async def _write_tab_diff_upsert(
        self,
        tab_title: str,
        headers: List[str],
        rows: List[List[str]],
        log_context: str = "",
    ) -> Tuple[bool, str]:
        """
        Upsert rows to a tab with minimal writes:
        - Update only member rows whose values changed (keyed by Discord ID, else Email)
        - Append missing members
        - Clear rows for members no longer present (does not rewrite whole sheet)
        """
        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return False, "missing spreadsheet_id"
        
        service = self._get_service()
        if not service:
            return False, self._last_error or "missing google service"

        # Normalize row shapes to header length
        col_count = max(1, len(headers))
        end_col = _col_letter(col_count)
        desired_rows: List[List[str]] = []
        desired_by_key: Dict[str, List[str]] = {}
        for r in (rows or []):
            rr = [str(c or "") for c in (r or [])]
            if len(rr) < col_count:
                rr = rr + [""] * (col_count - len(rr))
            else:
                rr = rr[:col_count]
            k = _member_key_from_row(rr)
            if not k:
                continue
            desired_rows.append(rr)
            desired_by_key[k] = rr

        # Read existing sheet A:J including header
        async with self._api_lock:
            def _do_get() -> Dict[str, Any]:
                return service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{tab_title}'!A:{end_col}",
                ).execute()

            try:
                resp = await asyncio.to_thread(_do_get)
            except Exception as e:
                return False, f"read existing failed: {e}"

        values = resp.get("values") if isinstance(resp, dict) else None
        existing_values: List[List[str]] = []
        if isinstance(values, list):
            for row in values:
                if not isinstance(row, list):
                    continue
                row_str = [str(c or "") for c in row]
                if len(row_str) < col_count:
                    row_str = row_str + [""] * (col_count - len(row_str))
                else:
                    row_str = row_str[:col_count]
                existing_values.append(row_str)

        # Ensure header row
        header_needs_update = True
        if existing_values:
            existing_header = existing_values[0]
            if [str(c or "") for c in existing_header[:col_count]] == [str(c or "") for c in headers[:col_count]]:
                header_needs_update = False

        # Build map key -> (row_index_1_based, row_values)
        existing_by_key: Dict[str, Tuple[int, List[str]]] = {}
        for i, row in enumerate(existing_values, start=1):
            if i == 1:
                continue
            k = _member_key_from_row(row)
            if not k:
                continue
            # First occurrence wins (avoid churn if duplicates exist)
            if k not in existing_by_key:
                existing_by_key[k] = (i, row)

        to_update: List[Dict[str, Any]] = []
        updated_members = 0
        for k, desired in desired_by_key.items():
            if k not in existing_by_key:
                continue
            row_i, existing = existing_by_key[k]
            if desired != existing:
                to_update.append(
                    {
                        "range": f"'{tab_title}'!A{row_i}:{end_col}{row_i}",
                        "values": [desired],
                    }
                )

        to_add = [desired_by_key[k] for k in desired_by_key.keys() if k not in existing_by_key]
        stale_keys = [k for k in existing_by_key.keys() if k not in desired_by_key]

        # Clear stale member rows (only rows that existed and now disappeared)
        to_clear: List[Dict[str, Any]] = []
        for k in stale_keys:
            row_i, _existing = existing_by_key[k]
            to_clear.append(
                {
                    "range": f"'{tab_title}'!A{row_i}:{end_col}{row_i}",
                    "values": [[""] * col_count],
                }
            )

        async with self._api_lock:
            # Header update first (if needed)
            if header_needs_update:
                def _do_header() -> None:
                    service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"'{tab_title}'!A1:{end_col}1",
                        valueInputOption="USER_ENTERED",
                        body={"values": [headers[:col_count]]},
                    ).execute()

                try:
                    await asyncio.to_thread(_do_header)
                except Exception as e:
                    return False, f"header update failed: {e}"

            if to_update:
                def _do_updates() -> None:
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={"valueInputOption": "USER_ENTERED", "data": to_update},
                    ).execute()

                try:
                    await asyncio.to_thread(_do_updates)
                    updated_members = len(to_update)
                except Exception as e:
                    return False, f"member updates failed: {e}"

            if to_add:
                def _do_add() -> None:
                    service.spreadsheets().values().append(
                        spreadsheetId=spreadsheet_id,
                        range=f"'{tab_title}'!A:{end_col}",
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={"values": to_add},
                    ).execute()

                try:
                    await asyncio.to_thread(_do_add)
                except Exception as e:
                    return False, f"append failed: {e}"

            if to_clear:
                def _do_clear_rows() -> None:
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={"valueInputOption": "USER_ENTERED", "data": to_clear},
                    ).execute()

                try:
                    await asyncio.to_thread(_do_clear_rows)
                except Exception as e:
                    return False, f"clear stale rows failed: {e}"

        if log_context:
            log.debug(
                f"    {log_context} diff-upsert: updated={updated_members}, added={len(to_add)}, cleared={len(to_clear)}"
            )
        return True, "ok"
    
    async def write_status_tab(self, tab_name: str, rows: List[List[str]]) -> Tuple[bool, str]:
        """Write rows to a status tab (diff-upsert)."""
        tab_title, err = await self._ensure_sheet_tab(tab_name)
        if err or not tab_title:
            return False, f"failed to ensure tab: {err}"
        
        headers = [
            "Name",
            "Phone Number",
            "Email",
            "Product",
            "Status",
            "Discord ID",
            "Status Updated",
            "Date Left",
            "Date Joined",
            "Total Spend",
        ]
        return await self._write_tab_diff_upsert(tab_title, headers, rows, log_context=f"Status tab '{tab_title}'")
    
    async def segregate_by_status(self) -> Dict[str, Tuple[bool, str, int]]:
        """
        Read from source tab and segregate members by status into status tabs.
        
        Returns: Dict mapping status tab name -> (success, message, count)
        """
        status_cfg = self.cfg.get("status_tabs", {})
        if not status_cfg.get("enabled", True):
            return {}
        
        source_tab = status_cfg.get("source_tab", "Whop API - Reselling Secrets")
        status_mapping = status_cfg.get("status_mapping", {})
        
        if not source_tab or not status_mapping:
            log.warning("Status tabs sync disabled or misconfigured")
            return {}
        
        log.info(f"  -> Reading source tab '{source_tab}'...")
        print(f"  -> Reading source tab '{source_tab}'...")
        
        source_rows = await self.read_source_tab(source_tab)
        log.info(f"  ✓ Read {len(source_rows)} members from source tab")
        print(f"  OK Read {len(source_rows)} members from source tab")
        
        if not source_rows:
            log.warning("  WARNING: Source tab is empty, nothing to segregate")
            return {}
        
        # Group rows by status
        status_groups: Dict[str, List[List[str]]] = {}
        status_counts: Dict[str, int] = {}
        
        # Map Whop API statuses to tab names (normalize both sides)
        status_to_tab: Dict[str, str] = {}
        for api_status, tab_name in status_mapping.items():
            status_lower = api_status.lower()
            status_to_tab[status_lower] = tab_name
        
        # Special handling: "canceling" maps to "Canceling" tab (single 'l' to match existing tab)
        status_to_tab["canceling"] = "Canceling"
        
        for row in source_rows:
            if len(row) < 5:  # Need at least Status column (E)
                continue
            
            status = str(row[4] or "").strip().lower()  # Column E = Status
            
            # Map status to tab name
            tab_name = status_to_tab.get(status)
            
            # Try direct match if mapping didn't work
            if not tab_name:
                for tab in status_mapping.values():
                    if status == tab.lower():
                        tab_name = tab
                        break
            
            if tab_name:
                if tab_name not in status_groups:
                    status_groups[tab_name] = []
                # Ensure 10 columns (adds Date Left, Date Joined, Total Spend)
                row_padded = list(row) if len(row) >= 10 else list(row) + [""] * (10 - len(row))
                if len(row_padded) > 10:
                    row_padded = row_padded[:10]
                status_groups[tab_name].append(row_padded)
                status_counts[tab_name] = status_counts.get(tab_name, 0) + 1
            else:
                log.debug(f"    Unknown status '{status}', skipping")
        
        # Write to each status tab
        results = {}
        log.info(f"  -> Segregating into {len(status_groups)} status tabs...")
        print(f"  -> Segregating into {len(status_groups)} status tabs...")
        
        # Log summary before writing
        log.info(f"  Status breakdown from source tab:")
        print(f"  Status breakdown from source tab:")
        for tab_name, rows in sorted(status_groups.items()):
            count = len(rows)
            log.info(f"    - {tab_name}: {count} members")
            print(f"    - {tab_name}: {count} members")
        
        for tab_name, rows in status_groups.items():
            count = len(rows)
            log.info(f"  -> Writing {count} members to '{tab_name}' tab...")
            print(f"  -> Writing {count} members to '{tab_name}' tab...")
            
            # Find existing tab name (handles Cancelled vs Canceled, etc.)
            actual_tab_name = await self._find_existing_tab(tab_name)
            if not actual_tab_name:
                actual_tab_name = tab_name
            
            success, msg = await self.write_status_tab(actual_tab_name, rows)
            results[actual_tab_name] = (success, msg, count)
            
            if success:
                log.info(f"    OK Wrote {count} members to '{actual_tab_name}' tab")
                print(f"    OK Wrote {count} members to '{actual_tab_name}' tab")
                # Log sample data (first 3 rows)
                if rows and len(rows) > 0:
                    sample_count = min(3, len(rows))
                    log.debug(f"      Sample rows (first {sample_count}):")
                    for i, row in enumerate(rows[:sample_count]):
                        if len(row) >= 5:
                            name = row[0] if len(row) > 0 else ""
                            email = row[2] if len(row) > 2 else ""
                            status = row[4] if len(row) > 4 else ""
                            log.debug(f"        {i+1}. {name} ({email}) - Status: {status}")
            else:
                log.error(f"    X Failed to write '{actual_tab_name}': {msg}")
                print(f"    X Failed to write '{actual_tab_name}': {msg}")
        
        # Final summary
        log.info(f"  Summary: Wrote data to {len([r for r in results.values() if r[0]])} status tabs")
        print(f"  Summary: Wrote data to {len([r for r in results.values() if r[0]])} status tabs")
        for tab_name, (success, msg, count) in sorted(results.items()):
            if success:
                log.info(f"    - {tab_name}: {count} members")
                print(f"    - {tab_name}: {count} members")
        
        return results
    
    async def sync_source_incremental(self, whop_client: WhopAPIClient, product_id: str, source_tab: str) -> Tuple[bool, str, int, List[List[str]]]:
        """
        Sync source tab incrementally: add new members or update existing ones.
        
        Returns: (success, message, added_count, updated_rows)
        """
        spreadsheet_id = _cfg_str(self.cfg, "spreadsheet_id", "")
        if not spreadsheet_id:
            return False, "missing spreadsheet_id", 0, []
        
        # Read existing source tab
        existing_rows = await self.read_source_tab(source_tab)
        existing_by_email: Dict[str, int] = {}  # email -> row_index
        existing_by_discord: Dict[str, int] = {}  # discord_id -> row_index
        
        existing_email_to_timestamp: Dict[str, str] = {}  # email_lower -> Status Updated (column G) for "only fetch if changed"
        for idx, row in enumerate(existing_rows):
            if len(row) > 2:
                email = str(row[2] or "").strip().lower()
                if email:
                    existing_by_email[email] = idx
                    if len(row) > 6:
                        existing_email_to_timestamp[email] = str(row[6] or "").strip()
            if len(row) > 5:
                discord_id = str(row[5] or "").strip()
                if discord_id:
                    existing_by_discord[discord_id] = idx
        
        log.info(f"  -> Found {len(existing_rows)} existing members in source tab")
        print(f"  -> Found {len(existing_rows)} existing members in source tab")
        
        # Use /memberships endpoint directly - it has all the data we need
        all_memberships = []
        after = None
        max_pages = 200
        
        log.info(f"  -> Fetching current memberships from /memberships endpoint (includes ALL statuses)...")
        print(f"  -> Fetching current memberships from /memberships endpoint (includes ALL statuses)...")
        
        for page in range(max_pages):
            try:
                if page == 0 or (page + 1) % 10 == 0:
                    print(f"    Fetching page {page + 1}...", end="\r")
                
                batch, page_info = await whop_client.list_memberships(
                    first=100,
                    after=after,
                    params={"product_ids": [product_id]} if product_id else {}
                )
                all_memberships.extend(batch)
                
                if not page_info.get("has_next_page"):
                    break
                after = page_info.get("end_cursor")
                if not after:
                    break
            except Exception as e:
                log.error(f"Failed to fetch memberships: {e}")
                return False, f"failed to fetch: {e}", 0, []
        
        print()  # New line
        log.info(f"  OK Fetched {len(all_memberships)} memberships from API")
        print(f"  OK Fetched {len(all_memberships)} memberships from API")
        
        # Fetch special status members (left, churned, canceling) from /members endpoint
        # These are not available in /memberships endpoint
        # NOTE: "canceling" can be filtered directly via most_recent_actions[] parameter (matches dashboard)
        special_members: Dict[str, List[Dict]] = {"left": [], "churned": [], "canceling": []}
        tab_name = source_tab  # For product name fallback
        
        log.info(f"  -> Fetching special status members (left, churned, canceling) from /members endpoint...")
        print(f"  -> Fetching special status members (left, churned, canceling)...")
        
        seen_member_ids = set()
        
        # Fetch members with most_recent_actions filter for canceling, churned
        # This matches the dashboard filter: members:most_recent_actions=canceling
        for action_type in ["canceling", "churned"]:
            action_after = None
            for page in range(50):
                try:
                    params = {"product_ids": [product_id]} if product_id else {}
                    params["most_recent_actions[]"] = [action_type]
                    
                    batch, page_info = await whop_client.list_members(
                        first=100,
                        after=action_after,
                        params=params
                    )
                    
                    for member in batch:
                        if not isinstance(member, dict):
                            continue
                        
                        member_id = str(member.get("id") or "").strip()
                        if not member_id or member_id in seen_member_ids:
                            continue
                        
                        # Verify product association
                        memberships = member.get("memberships") or []
                        has_product_membership = False
                        if memberships:
                            for mship in memberships:
                                if isinstance(mship, dict):
                                    mship_product = mship.get("product") or {}
                                    if isinstance(mship_product, dict):
                                        mship_product_id = str(mship_product.get("id") or "").strip()
                                        if mship_product_id == product_id:
                                            has_product_membership = True
                                            break
                        else:
                            if product_id:
                                has_product_membership = True
                        
                        if not has_product_membership:
                            continue
                        
                        seen_member_ids.add(member_id)
                        special_members[action_type].append(member)
                    
                    if not page_info.get("has_next_page"):
                        break
                    action_after = page_info.get("end_cursor")
                    if not action_after:
                        break
                except Exception as e:
                    log.debug(f"    Error fetching {action_type} members: {e}")
                    break
        
        # Also fetch "left" members (status field, not most_recent_action)
        special_after = None
        for page in range(50):
            try:
                batch, page_info = await whop_client.list_members(
                    first=100,
                    after=special_after,
                    params={"product_ids": [product_id], "statuses[]": ["left"]} if product_id else {"statuses[]": ["left"]}
                )
                
                for member in batch:
                    if not isinstance(member, dict):
                        continue
                    
                    member_id = str(member.get("id") or "").strip()
                    if not member_id or member_id in seen_member_ids:
                        continue
                    
                    # Verify product association
                    memberships = member.get("memberships") or []
                    has_product_membership = False
                    if memberships:
                        for mship in memberships:
                            if isinstance(mship, dict):
                                mship_product = mship.get("product") or {}
                                if isinstance(mship_product, dict):
                                    mship_product_id = str(mship_product.get("id") or "").strip()
                                    if mship_product_id == product_id:
                                        has_product_membership = True
                                        break
                    else:
                        if product_id:
                            has_product_membership = True
                    
                    if not has_product_membership:
                        continue
                    
                    seen_member_ids.add(member_id)
                    status = str(member.get("status") or "").strip().lower()
                    if status == "left":
                        special_members["left"].append(member)
                
                if not page_info.get("has_next_page"):
                    break
                special_after = page_info.get("end_cursor")
                if not special_after:
                    break
            except Exception as e:
                log.debug(f"    Error fetching left members: {e}")
                break
        
        # Convert special members to membership-like format
        for status_type, members_list in special_members.items():
            for member in members_list:
                if not isinstance(member, dict):
                    continue
                # Convert real /members API "member" into same shape as /memberships "membership" (all data from API)
                member_updated = member.get("updated_at") or member.get("created_at") or ""
                member_as_membership = {
                    "id": f"{status_type}_{member.get('id', '')}",
                    "status": status_type,
                    "member": {"id": str(member.get("id") or "").strip()},
                    "user": member.get("user") or {},
                    "product": {"id": product_id, "title": "Reselling Secrets" if "Reselling Secrets" in tab_name and "Lite" not in tab_name else "Reselling Secrets Lite"},
                    "updated_at": member_updated,
                    "created_at": member.get("created_at") or member_updated,
                    f"_is_{status_type}_member": True,
                    f"_{status_type}_member_data": member,
                }
                all_memberships.append(member_as_membership)
        
        total_special = sum(len(m) for m in special_members.values())
        if total_special > 0:
            log.info(f"  OK Added {total_special} special status members (left: {len(special_members['left'])}, churned: {len(special_members['churned'])}, canceling: {len(special_members['canceling'])})")
            print(f"  OK Added {total_special} special status members")
        
        # Fetch detailed member records (phone primarily).
        # OPTIMIZATION:
        # - Skip "left"
        # - Skip existing members when Status Updated unchanged
        # - Skip per-member detail fetch when phone is already available (sheet or local cache)
        existing_emails = set(existing_by_email.keys())
        member_ids: Set[str] = set()
        member_id_email_map: Dict[str, str] = {}
        skipped_left_count = 0
        skipped_unchanged_count = 0
        skipped_has_phone_count = 0
        for mship in all_memberships:
            # Skip "left" members - don't fetch their detailed records
            if mship.get("_is_left_member") or str(mship.get("status") or "").strip().lower() == "left":
                skipped_left_count += 1
                continue
            
            user_obj = mship.get("user") or {}
            email = str(user_obj.get("email") or "").strip().lower() if isinstance(user_obj, dict) else ""
            # Existing member: only skip fetch if Status Updated is unchanged (same timestamp)
            if email and email in existing_emails:
                api_updated = mship.get("updated_at") or mship.get("created_at") or ""
                formatted_ts = _format_timestamp(api_updated)
                sheet_ts = existing_email_to_timestamp.get(email, "")
                if formatted_ts and sheet_ts and formatted_ts.strip() == sheet_ts.strip():
                    skipped_unchanged_count += 1
                    continue
                # Timestamp changed or missing -> fetch to get fresh Discord/phone
            # New member or timestamp changed -> add to fetch list
            member_obj = mship.get("member") or {}
            if isinstance(member_obj, dict):
                member_id = str(member_obj.get("id") or "").strip()
                if member_id and member_id.startswith("mber_"):
                    member_ids.add(member_id)
                    if email:
                        member_id_email_map[member_id] = email
        
        if skipped_left_count > 0:
            log.info(f"  -> Skipping {skipped_left_count} 'left' members from detailed record fetch")
            print(f"  -> Skipping {skipped_left_count} 'left' members from detailed record fetch")
        if skipped_unchanged_count > 0:
            log.info(f"  -> Skipping {skipped_unchanged_count} members (Status Updated unchanged, no fetch needed)")
            print(f"  -> Skipping {skipped_unchanged_count} members (Status Updated unchanged, no fetch needed)")
        
        # Further minimize: only fetch detail if phone missing from sheet AND cache.
        member_cache: Dict[str, Any] = {}
        need_fetch: List[str] = []
        ghl_map = await self._load_ghl_phone_map_if_needed()
        for mid in sorted(member_ids):
            em = member_id_email_map.get(mid, "")
            phone_from_sheet = ""
            if em and em in existing_by_email:
                idx = existing_by_email[em]
                if idx < len(existing_rows) and len(existing_rows[idx]) > 1:
                    phone_from_sheet = str(existing_rows[idx][1] or "").strip()
            phone_from_cache = self._cached_member_phone(mid)
            phone_from_ghl = str(ghl_map.get(em) or "").strip() if em else ""
            if phone_from_sheet or phone_from_cache or phone_from_ghl:
                skipped_has_phone_count += 1
                if phone_from_cache:
                    member_cache[mid] = {"phone": phone_from_cache}
                continue
            need_fetch.append(mid)

        if skipped_has_phone_count > 0:
            log.info(f"  -> Skipping {skipped_has_phone_count} members (phone already known, no detail fetch needed)")
            print(f"  -> Skipping {skipped_has_phone_count} members (phone already known, no detail fetch needed)")

        member_ids_list = list(need_fetch)
        
        if member_ids_list and (not self._whop_member_detail_fetch_enabled()):
            log.info("  -> Detail fetch disabled (whop_member_detail_fetch_enabled=false). Using sheet+GHL only.")
            print("  -> Detail fetch disabled (whop_member_detail_fetch_enabled=false). Using sheet+GHL only.")
            member_ids_list = []

        if member_ids_list:
            log.info(f"  -> Fetching {len(member_ids_list)} detailed member records for Discord ID/phone...")
            print(f"  -> Fetching {len(member_ids_list)} detailed member records...")
            
            for i, member_id in enumerate(member_ids_list):
                try:
                    if (i + 1) % 50 == 0:
                        print(f"    Fetching: {i + 1}/{len(member_ids_list)}", end="\r")
                    member = await whop_client.get_member_by_id(member_id)
                    if member:
                        member_cache[member_id] = member
                        if isinstance(member, dict):
                            ph = str(member.get("phone") or "").strip()
                            if ph:
                                self._set_cached_member_phone(member_id, ph)
                    if (i + 1) % 10 == 0:
                        await asyncio.sleep(0.1)
                except Exception:
                    pass
            
            print()  # New line
            self._save_member_detail_cache()
        
        # Deduplicate memberships: each member should have only ONE status (most recent/highest priority)
        # Status priority: IMPORTANT - "left" only applies if member has NO active membership
        # Active memberships take precedence over "left" status from /members endpoint
        # Priority: canceling > active > trialing > churned > expired > completed > past_due > unresolved > drafted > left
        status_priority = {
            "canceling": 1,  # Highest priority - active membership being canceled
            "active": 2,
            "trialing": 3,
            "churned": 4,
            "expired": 5,
            "completed": 6,
            "past_due": 7,
            "unresolved": 8,
            "drafted": 9,
            "left": 10,  # Lowest priority - only if no active membership exists
        }
        
        def get_status_priority(status: str) -> int:
            """Get priority for status (lower = higher priority)."""
            return status_priority.get(status.lower(), 999)
        
        # Group memberships by member identifier (email or Discord ID)
        member_status_map: Dict[str, Dict[str, Any]] = {}  # key: email_lower or discord_id -> {status, row_data, updated_at}
        rows_with_spend = 0
        rows_spend_from_member_history = 0
        rows_spend_from_whop = 0
        spend_samples: List[str] = []
        
        # Get product name
        product_name = "Reselling Secrets" if "Reselling Secrets" in source_tab and "Lite" not in source_tab else "Reselling Secrets Lite"
        
        for mship in all_memberships:
            try:
                if not isinstance(mship, dict):
                    continue
                
                # Get member_id from membership.member field
                member_obj = mship.get("member") or {}
                member_id = None
                if isinstance(member_obj, dict):
                    member_id = str(member_obj.get("id") or "").strip()
                
                member_record = member_cache.get(member_id) if member_id else None
                
                # Extract data from membership
                # Check if this is a "canceling" membership (active/trialing with cancel_at_period_end=true)
                # IMPORTANT: Only mark as "canceling" if status is active/trialing AND cancel_at_period_end=true
                # Do NOT mark canceled/expired memberships as "canceling"
                base_status = str(mship.get("status") or "").strip().lower()
                
                # Skip canceled status - we're removing it
                if base_status == "canceled":
                    continue
                
                if mship.get("cancel_at_period_end") is True and base_status in ["active", "trialing"]:
                    status = "canceling"
                else:
                    status = base_status
                
                # Get user data from membership.user field
                user_obj = mship.get("user") or {}
                name = ""
                email = ""
                if isinstance(user_obj, dict):
                    name = str(user_obj.get("name") or user_obj.get("username") or "").strip()
                    email = str(user_obj.get("email") or "").strip()
                
                # For special status members (left, churned), get data from member_data if available
                for status_type in ["left", "churned"]:
                    if mship.get(f"_is_{status_type}_member") and mship.get(f"_{status_type}_member_data"):
                        special_data = mship.get(f"_{status_type}_member_data")
                        if isinstance(special_data, dict):
                            if not name:
                                name = str(special_data.get("name") or special_data.get("username") or "").strip()
                            if not email:
                                user_from_special = special_data.get("user") or {}
                                if isinstance(user_from_special, dict):
                                    email = str(user_from_special.get("email") or "").strip()
                                    if not name:
                                        name = str(user_from_special.get("name") or user_from_special.get("username") or "").strip()
                            break
                
                # Phone:
                # - Preserve from existing row (sheet)
                # - Else, if Whop returned it (rare for your account), use it
                # - Else, try GHL Website Data Info tab (email->phone)
                phone = ""
                if email:
                    email_lower = email.strip().lower()
                    if email_lower in existing_by_email:
                        idx = existing_by_email[email_lower]
                        if idx < len(existing_rows) and len(existing_rows[idx]) > 1:
                            phone = str(existing_rows[idx][1] or "").strip()
                if not phone and isinstance(member_record, dict):
                    phone = str(member_record.get("phone") or "").strip()
                if not phone:
                    # Try from special member data
                    for status_type in ["left", "churned"]:
                        if mship.get(f"_{status_type}_member_data"):
                            special_data = mship.get(f"_{status_type}_member_data")
                            if isinstance(special_data, dict):
                                phone = str(special_data.get("phone") or "").strip()
                                if phone:
                                    break
                if not phone and email:
                    phone = await self._enrich_phone_from_ghl(email=email, current_phone=phone)
                
                # Extract Discord ID (Whop-side). If missing, enrich from Discord-side identity cache (email match).
                discord_id = _extract_discord_id(mship, member_record)
                if not discord_id and email:
                    discord_id = self._enrich_discord_id(email=email, current_discord_id=discord_id)
                # Continuous sync: preserve from existing row if we skipped fetch (existing member)
                if not discord_id and email:
                    email_lower = email.strip().lower()
                    if email_lower in existing_by_email:
                        idx = existing_by_email[email_lower]
                        if idx < len(existing_rows) and len(existing_rows[idx]) > 5:
                            discord_id = str(existing_rows[idx][5] or "").strip()
                
                # Get updated_at timestamp for determining most recent status
                updated_at = mship.get("updated_at") or mship.get("created_at") or ""
                formatted_timestamp = _format_timestamp(updated_at)

                # Joined/Left dates (MM/DD/YY)
                created_at = mship.get("created_at") or ""
                date_joined = _format_date_mmddyy(str(created_at or ""))
                left_statuses = {"left", "expired", "completed", "churned"}
                date_left = _format_date_mmddyy(str(updated_at or "")) if status in left_statuses else ""

                total_spend = _extract_total_spend(mship, member_record)
                if total_spend:
                    rows_with_spend += 1
                    rows_spend_from_whop += 1
                if not total_spend and discord_id:
                    total_spend = self._enrich_total_spend_from_member_history(
                        discord_id=discord_id,
                        current_total_spend=total_spend,
                    )
                    if total_spend:
                        rows_with_spend += 1
                        rows_spend_from_member_history += 1
                if total_spend and len(spend_samples) < 5:
                    spend_samples.append(f"{email or '-'} did={discord_id or '-'} spend={total_spend}")
                
                # Determine member key (prefer email, fallback to Discord ID)
                member_key = None
                if email:
                    member_key = email.strip().lower()
                elif discord_id:
                    member_key = f"discord_{discord_id}"
                
                if not member_key:
                    # Skip if no identifier
                    continue
                
                new_row = [
                    name,
                    phone,
                    email,
                    product_name,
                    status,
                    discord_id,
                    formatted_timestamp,
                    date_left,
                    date_joined,
                    total_spend,
                ]
                
                # Check if we already have this member with a status
                existing = member_status_map.get(member_key)
                current_priority = get_status_priority(status)
                
                if existing:
                    existing_priority = get_status_priority(existing.get("status", ""))
                    # IMPORTANT: Active memberships (from /memberships) take precedence over "left" (from /members)
                    # Only use "left" if there's no active membership
                    if existing.get("status", "").lower() == "left" and current_priority < 11:
                        # Existing is "left" but current is an active membership - replace with active
                        member_status_map[member_key] = {
                            "status": status,
                            "row": new_row,
                            "updated_at": updated_at,
                        }
                    elif current_priority < existing_priority:
                        # Current status has higher priority, replace
                        member_status_map[member_key] = {
                            "status": status,
                            "row": new_row,
                            "updated_at": updated_at,
                        }
                    elif current_priority == existing_priority and updated_at > existing.get("updated_at", ""):
                        # Same priority but more recent, replace
                        member_status_map[member_key] = {
                            "status": status,
                            "row": new_row,
                            "updated_at": updated_at,
                        }
                    # Otherwise keep existing
                else:
                    # First time seeing this member
                    member_status_map[member_key] = {
                        "status": status,
                        "row": new_row,
                        "updated_at": updated_at,
                    }
            except Exception:
                continue
        
        log.info(f"  -> Deduplicated: {len(all_memberships)} memberships -> {len(member_status_map)} unique members")
        print(f"  -> Deduplicated: {len(all_memberships)} memberships -> {len(member_status_map)} unique members")
        log.info(
            f"  Spend coverage: {rows_with_spend}/{len(member_status_map)} (from_whop={rows_spend_from_whop}, from_member_history={rows_spend_from_member_history})"
        )
        print(
            f"  Spend coverage: {rows_with_spend}/{len(member_status_map)} (from_whop={rows_spend_from_whop}, from_member_history={rows_spend_from_member_history})"
        )
        if spend_samples:
            log.info("  Spend samples (up to 5): " + " | ".join(spend_samples))
        
        # Now update/add rows based on deduplicated data
        # Handle backward compatibility: existing rows might have fewer columns (older sheet layout)
        updated_rows = []
        for row in (existing_rows or []):
            # Pad to 10 columns (A:J)
            row2 = list(row)
            if len(row2) < 10:
                row2 = row2 + [""] * (10 - len(row2))
            if len(row2) > 10:
                row2 = row2[:10]
            updated_rows.append(row2)
        
        rows_to_update: Dict[int, List[str]] = {}  # row_index -> new_row
        rows_to_add: List[List[str]] = []
        
        for member_key, member_data in member_status_map.items():
            new_row = member_data["row"]
            if len(new_row) < 10:
                new_row = list(new_row) + [""] * (10 - len(new_row))
            if len(new_row) > 10:
                new_row = list(new_row)[:10]
            email = new_row[2] if len(new_row) > 2 else ""
            discord_id = new_row[5] if len(new_row) > 5 else ""
            
            email_lower = email.strip().lower() if email else ""
            
            # Check if exists by email or discord_id
            existing_idx = None
            if email_lower and email_lower in existing_by_email:
                existing_idx = existing_by_email[email_lower]
            elif discord_id and discord_id in existing_by_discord:
                existing_idx = existing_by_discord[discord_id]
            
            if existing_idx is not None:
                # Update existing row
                rows_to_update[existing_idx] = new_row
            else:
                # Add new row
                rows_to_add.append(new_row)
        
        # Change detection and logging (timestamp-based: what changed vs existing sheet)
        changes_log: List[str] = []
        status_changes: List[str] = []
        for idx, new_row in rows_to_update.items():
            if idx >= len(updated_rows):
                continue
            old_row = updated_rows[idx]
            old_status = str(old_row[4] or "").strip() if len(old_row) > 4 else ""
            new_status = str(new_row[4] or "").strip() if len(new_row) > 4 else ""
            new_ts = str(new_row[6] or "").strip() if len(new_row) > 6 else ""
            email = str(new_row[2] or "").strip() if len(new_row) > 2 else ""
            if old_status != new_status:
                status_changes.append(f"  {email}: {old_status or '(none)'} -> {new_status} ({new_ts})")
            updated_rows[idx] = new_row
        
        for new_row in rows_to_add:
            new_status = str(new_row[4] or "").strip() if len(new_row) > 4 else ""
            new_ts = str(new_row[6] or "").strip() if len(new_row) > 6 else ""
            email = str(new_row[2] or "").strip() if len(new_row) > 2 else ""
            changes_log.append(f"  NEW: {email} | status={new_status} | Status Updated={new_ts}")
            if new_status == "trialing":
                log.info(f"  [Trialing] {email} | Status Updated={new_ts}")
                print(f"  [Trialing] {email} | Status Updated={new_ts}")
        
        if status_changes:
            log.info("  Changes (status):")
            print("  Changes (status):")
            for line in status_changes[:50]:
                log.info(line)
                print(line)
            if len(status_changes) > 50:
                log.info(f"  ... and {len(status_changes) - 50} more")
                print(f"  ... and {len(status_changes) - 50} more")
        if changes_log:
            log.info("  New members:")
            print("  New members:")
            for line in changes_log[:30]:
                log.info(line)
                print(line)
            if len(changes_log) > 30:
                log.info(f"  ... and {len(changes_log) - 30} more")
                print(f"  ... and {len(changes_log) - 30} more")
        
        # Apply updates (already applied in loop above for rows_to_update)
        # Add new rows
        updated_rows.extend(rows_to_add)
        
        # Write back to source tab
        tab_title, err = await self._ensure_sheet_tab(source_tab)
        if err or not tab_title:
            return False, f"failed to ensure tab: {err}", len(rows_to_add), []

        headers = [
            "Name",
            "Phone Number",
            "Email",
            "Product",
            "Status",
            "Discord ID",
            "Status Updated",
            "Date Left",
            "Date Joined",
            "Total Spend",
        ]

        ok, msg = await self._write_tab_diff_upsert(tab_title, headers, updated_rows, log_context=f"Source tab '{tab_title}'")
        if not ok:
            return False, msg, len(rows_to_add), []

        log.info(f"  ✓ Updated source tab: {len(rows_to_update)} updated, {len(rows_to_add)} added")
        print(f"  OK Updated source tab: {len(rows_to_update)} updated, {len(rows_to_add)} added")
        return True, "ok", len(rows_to_add), updated_rows
    
    async def sync_all_products(self, whop_client: WhopAPIClient) -> Dict[str, Tuple[bool, str, int]]:
        """Sync all configured products."""
        products = self.cfg.get("products", [])
        if not isinstance(products, list):
            return {}
        
        results = {}
        status_cfg = self.cfg.get("status_tabs", {})
        source_tab = status_cfg.get("source_tab", "Whop API - Reselling Secrets") if status_cfg.get("enabled", True) else None
        main_product_id = None
        lifetime_product_id = None
        lifetime_tab_name = None
        
        # First, sync all products normally (clear and replace), but skip Lifetime for now
        for product_cfg in products:
            product_id = str(product_cfg.get("product_id") or "").strip()
            tab_name = str(product_cfg.get("tab_name") or "").strip()
            enabled = product_cfg.get("enabled", True)  # Default to enabled if not specified
            
            if not product_id or not tab_name:
                continue
            
            # Skip if disabled
            if not enabled:
                log.info(f"Skipping disabled product: {product_id} -> {tab_name}")
                print(f"Skipping disabled product: {product_id} -> {tab_name}")
                continue
            
            # Track Lifetime product to sync separately at the end
            if product_id == "prod_76xygbFOv0aUM" or "Lifetime" in tab_name:
                lifetime_product_id = product_id
                lifetime_tab_name = tab_name
                log.info(f"Lifetime product detected: {product_id} -> {tab_name} (will sync separately at end)")
                print(f"Lifetime product detected: {product_id} -> {tab_name} (will sync separately at end)")
                continue
            
            # Track which is the main "Reselling Secrets" product (not Lite)
            if source_tab and tab_name == source_tab:
                main_product_id = product_id
            
            # Lite tab: only members who have Lite and do NOT have Main (exclude Main user IDs)
            exclude_user_ids: Optional[Set[str]] = None
            if (product_id == "prod_U52ytqRZdCFak" or "Lite" in tab_name) and main_product_id:
                log.info(f"  Fetching Main product user IDs to exclude from Lite tab (Lite-only)...")
                print(f"  -> Lite-only: excluding members who also have Main product...")
                exclude_user_ids = await self._fetch_product_user_ids(whop_client, main_product_id)
                log.info(f"  Excluding {len(exclude_user_ids)} Main product user IDs from Lite tab")
            
            print(f"\nSyncing product: {product_id} -> tab: {tab_name}")
            success, msg, count = await self.sync_product_memberships(
                whop_client,
                product_id,
                tab_name,
                exclude_user_ids=exclude_user_ids,
            )
            results[product_id] = (success, msg, count)
            
            if success:
                print(f"OK Synced {count} members to '{tab_name}'")
            else:
                print(f"X Failed: {msg}")
        
        # Then, segregate source tab by status into status tabs
        if source_tab and status_cfg.get("enabled", True):
            print(f"\n{'='*60}")
            print(f"Segregating '{source_tab}' by status into status tabs...")
            print(f"{'='*60}")
            
            status_results = await self.segregate_by_status()
            for tab_name, (success, msg, count) in status_results.items():
                if success:
                    print(f"OK {tab_name}: {count} members")
                else:
                    print(f"X {tab_name}: {msg}")
        
        # Finally, sync Lifetime members separately at the end (simpler, fewer data)
        if lifetime_product_id and lifetime_tab_name:
            print(f"\n{'='*60}")
            print(f"Syncing Lifetime members separately: {lifetime_product_id} -> {lifetime_tab_name}")
            print(f"{'='*60}")
            
            success, msg, count = await self.sync_product_memberships(
                whop_client,
                lifetime_product_id,
                lifetime_tab_name,
            )
            results[lifetime_product_id] = (success, msg, count)
            
            if success:
                print(f"OK Synced {count} Lifetime members to '{lifetime_tab_name}'")
            else:
                print(f"X Failed: {msg}")
        
        return results
    
    async def sync_continuous_cycle(self, whop_client: WhopAPIClient) -> Dict[str, Tuple[bool, str, int]]:
        """
        Continuous sync cycle: Update all enabled products incrementally, then segregate source tab by status.
        """
        status_cfg = self.cfg.get("status_tabs", {})
        if not status_cfg.get("enabled", True):
            return {}
        
        source_tab = status_cfg.get("source_tab", "Whop API - Reselling Secrets")
        products = self.cfg.get("products", [])
        
        # Find product_id for source tab (main product)
        main_product_id = None
        for product_cfg in products:
            tab_name = str(product_cfg.get("tab_name") or "").strip()
            if tab_name == source_tab:
                main_product_id = str(product_cfg.get("product_id") or "").strip()
                break
        
        if not main_product_id:
            log.error(f"Could not find product_id for source tab '{source_tab}'")
            return {}
        
        log.info("=" * 60)
        log.info("Continuous sync cycle: Updating all enabled products...")
        log.info("=" * 60)
        print("=" * 60)
        print("Continuous sync cycle: Updating all enabled products...")
        print("=" * 60)
        
        results = {}
        
        # Sync all enabled products (incremental for source tab, full sync for others)
        for product_cfg in products:
            product_id = str(product_cfg.get("product_id") or "").strip()
            tab_name = str(product_cfg.get("tab_name") or "").strip()
            enabled = product_cfg.get("enabled", True)
            
            if not product_id or not tab_name:
                continue
            
            if not enabled:
                continue
            
            # Skip Lifetime in continuous sync (it's simpler, fewer changes)
            if product_id == "prod_76xygbFOv0aUM" or "Lifetime" in tab_name:
                continue
            
            print(f"\nUpdating product: {product_id} -> tab: {tab_name}")
            
            # For source tab (main product): use incremental sync
            if tab_name == source_tab:
                success, msg, added_count, updated_rows = await self.sync_source_incremental(
                    whop_client,
                    product_id,
                    tab_name,
                )
                if success:
                    results[tab_name] = (True, f"added {added_count}", len(updated_rows))
                    print(f"OK Updated {tab_name}: {added_count} new members added")
                else:
                    results[tab_name] = (False, msg, 0)
                    print(f"X Failed to update {tab_name}: {msg}")
            else:
                # For other products (e.g. Lite): use full sync with exclusion if needed
                exclude_user_ids: Optional[Set[str]] = None
                if (product_id == "prod_U52ytqRZdCFak" or "Lite" in tab_name) and main_product_id:
                    log.info(f"  Fetching Main product user IDs to exclude from Lite tab (Lite-only)...")
                    print(f"  -> Lite-only: excluding members who also have Main product...")
                    exclude_user_ids = await self._fetch_product_user_ids(whop_client, main_product_id)
                    log.info(f"  Excluding {len(exclude_user_ids)} Main product user IDs from Lite tab")
                
                success, msg, count = await self.sync_product_memberships(
                    whop_client,
                    product_id,
                    tab_name,
                    exclude_user_ids=exclude_user_ids,
                )
                if success:
                    results[tab_name] = (success, msg, count)
                    print(f"OK Synced {count} members to '{tab_name}'")
                else:
                    results[tab_name] = (False, msg, 0)
                    print(f"X Failed: {msg}")
        
        # Segregate source tab by status
        print(f"\n{'='*60}")
        print(f"Segregating '{source_tab}' by status...")
        print(f"{'='*60}")
        
        status_results = await self.segregate_by_status()
        
        # Merge status results into main results
        for tab_name, (success, msg, count) in status_results.items():
            results[tab_name] = (success, msg, count)
        
        return results
