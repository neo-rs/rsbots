import os
import sys
import json
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
from typing import Dict, Optional
from contextlib import suppress

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Always resolve bot-local files relative to this directory (do not depend on cwd).
BASE_DIR = Path(__file__).resolve().parent

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

import discord
from discord.ext import commands, tasks
from aiohttp import web
import aiohttp

from rschecker_utils import load_json, save_json, roles_plain, access_roles_plain, coerce_role_ids
from staff_embeds import (
    apply_member_header as _apply_member_header,
    build_case_minimal_embed as _build_case_minimal_embed,
    build_member_status_detailed_embed as _build_member_status_detailed_embed,
)
from whop_brief import fetch_whop_brief
from staff_channels import (
    PAYMENT_FAILURE_CHANNEL_NAME,
    MEMBER_CANCELLATION_CHANNEL_NAME,
    STAFF_ALERTS_CATEGORY_ID,
)
from staff_alerts_store import (
    load_staff_alerts,
    save_staff_alerts,
    should_post_alert,
    record_alert_post,
)

# Import Whop webhook handler
from whop_webhook_handler import (
    initialize as init_whop_handler,
    handle_whop_webhook_message,
    get_cached_whop_membership_id,
)

# Import Whop API client
from whop_api_client import WhopAPIClient, WhopAPIError

# -----------------------------
# RSCheckerbot Rules
# -----------------------------
# - No role mentions ever (<@&...> is forbidden)
# - Only mention users (@user) and channels (#channel)
# - Commands must only trigger with .checker or bot mention
# - DM sequence must be toggleable via commands
# -----------------------------

# -----------------------------
# Load Configuration
# -----------------------------
def load_config():
    config, _, secrets_path = load_config_with_secrets(BASE_DIR)
    if not secrets_path.exists():
        raise RuntimeError(f"Missing server-only secrets file: {secrets_path}")
    if not config.get("bot_token"):
        raise RuntimeError(f"bot_token must be set in {secrets_path}")
    return config

config = load_config()

# -----------------------------
# ENV / CONSTANTS
# -----------------------------
TOKEN = config.get("bot_token")
GUILD_ID = config.get("guild_id")

# Whop API Config
WHOP_API_CONFIG = config.get("whop_api", {})
WHOP_API_KEY = WHOP_API_CONFIG.get("api_key", "")
WHOP_RESOLUTION_CATEGORY_ID = WHOP_API_CONFIG.get("resolution_category_id")
WHOP_DISPUTE_CHANNEL_NAME = WHOP_API_CONFIG.get("dispute_channel_name", "dispute-fighter")
WHOP_RESOLUTION_CHANNEL_NAME = WHOP_API_CONFIG.get("resolution_channel_name", "resolution-center")
WHOP_SUPPORT_PING_ROLE_ID = WHOP_API_CONFIG.get("support_ping_role_id", "")
WHOP_SUPPORT_PING_ROLE_NAME = WHOP_API_CONFIG.get("support_ping_role_name", "")

# Invite Tracking Config
INVITE_CONFIG = config.get("invite_tracking", {})
HTTP_SERVER_PORT = INVITE_CONFIG.get("http_server_port", 8080)
INVITE_CHANNEL_ID = INVITE_CONFIG.get("invite_channel_id")
FALLBACK_INVITE = INVITE_CONFIG.get("fallback_invite", "")
WHOP_LOGS_CHANNEL_ID = INVITE_CONFIG.get("whop_logs_channel_id")
WHOP_WEBHOOK_CHANNEL_ID = INVITE_CONFIG.get("whop_webhook_channel_id")
DISCORD_WEBHOOK_URL = INVITE_CONFIG.get("discord_webhook_url", "")
GHL_API_KEY = INVITE_CONFIG.get("ghl_api_key", "")
GHL_LOCATION_ID = INVITE_CONFIG.get("ghl_location_id", "")
GHL_CF_DISCORD_USERNAME = INVITE_CONFIG.get("ghl_cf_discord_username", "")
GHL_CF_DISCORD_ID = INVITE_CONFIG.get("ghl_cf_discord_id", "")

# DM Sequence Config
DM_CONFIG = config.get("dm_sequence", {})
ROLE_TO_ASSIGN = DM_CONFIG.get("role_to_assign")
ROLE_TRIGGER = DM_CONFIG.get("role_trigger", ROLE_TO_ASSIGN)
WELCOME_ROLE_ID = DM_CONFIG.get("welcome_role_id")  # Role that triggers RSOnboarding ticket creation
ROLE_CANCEL_A = DM_CONFIG.get("role_cancel_a")
ROLE_CANCEL_B = DM_CONFIG.get("role_cancel_b")
FORMER_MEMBER_ROLE = DM_CONFIG.get("former_member_role")
FORMER_MEMBER_DELAY_SECONDS = DM_CONFIG.get("former_member_delay_seconds", 60)
ROLES_TO_CHECK = set(DM_CONFIG.get("roles_to_check", []))
SEND_SPACING_SECONDS = DM_CONFIG.get("send_spacing_seconds", 30.0)
DAY_GAP_HOURS = DM_CONFIG.get("day_gap_hours", 24)
DAY7B_DELAY_MIN = DM_CONFIG.get("day7b_delay_min", 30)
TEST_INTERVAL_SECONDS = DM_CONFIG.get("test_interval_seconds", 10.0)
LOG_FIRST_CHANNEL_ID = DM_CONFIG.get("log_first_channel_id")
LOG_OTHER_CHANNEL_ID = DM_CONFIG.get("log_other_channel_id")
MEMBER_STATUS_LOGS_CHANNEL_ID = DM_CONFIG.get("member_status_logs_channel_id")

# Logging controls (spam reduction + correlation)
LOG_CONTROLS = config.get("log_controls", {}) if isinstance(config, dict) else {}
try:
    BOOT_POST_MIN_HOURS = float(LOG_CONTROLS.get("boot_post_min_hours", 6))
except Exception:
    BOOT_POST_MIN_HOURS = 6.0
try:
    ROLE_UPDATE_BATCH_SECONDS = float(LOG_CONTROLS.get("role_update_batch_seconds", 2))
except Exception:
    ROLE_UPDATE_BATCH_SECONDS = 2.0
try:
    CID_TTL_MINUTES = float(LOG_CONTROLS.get("cid_ttl_minutes", 10))
except Exception:
    CID_TTL_MINUTES = 10.0
VERBOSE_ROLE_LISTS = bool(LOG_CONTROLS.get("verbose_role_lists", False))

# Files
QUEUE_FILE = BASE_DIR / "queue.json"
REGISTRY_FILE = BASE_DIR / "registry.json"
INVITES_FILE = BASE_DIR / "invites.json"
MESSAGES_FILE = BASE_DIR / "messages.json"
SETTINGS_FILE = BASE_DIR / "settings.json"
MEMBER_HISTORY_FILE = BASE_DIR / "member_history.json"
WHOP_WEBHOOK_RAW_LOG_FILE = BASE_DIR / "whop_webhook_raw_payloads.json"
BOOT_STATE_FILE = BASE_DIR / "boot_state.json"

# Message order keys
DAY_KEYS = ["day_1", "day_2", "day_3", "day_4", "day_5", "day_6", "day_7a", "day_7b"]

# UTM links
UTM_LINKS = config.get("utm_links", {})

# Load messages from JSON (fallback to Python modules if JSON doesn't exist)
def load_messages():
    """Load messages from JSON file (single source of truth; no Python-module fallback)."""
    if not MESSAGES_FILE.exists():
        raise RuntimeError(f"Missing {MESSAGES_FILE} (required).")
    try:
        with open(MESSAGES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load {MESSAGES_FILE}: {e}") from e

    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid {MESSAGES_FILE}: expected JSON object at top-level.")

    days = data.get("days")
    if not isinstance(days, dict):
        raise RuntimeError(f"Invalid {MESSAGES_FILE}: expected 'days' object.")

    missing = [k for k in DAY_KEYS if k not in days]
    if missing:
        raise RuntimeError(f"Invalid {MESSAGES_FILE}: missing day key(s): {', '.join(missing)}")

    for k in DAY_KEYS:
        day = days.get(k)
        if not isinstance(day, dict):
            raise RuntimeError(f"Invalid {MESSAGES_FILE}: days['{k}'] must be an object.")
        desc = day.get("description", "")
        if not isinstance(desc, str) or not desc.strip():
            raise RuntimeError(f"Invalid {MESSAGES_FILE}: days['{k}'].description must be a non-empty string.")

    return data

messages_data = load_messages()

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rs-checker")

# -----------------------------
# Discord client
# -----------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True
intents.invites = True

bot = commands.Bot(command_prefix=commands.when_mentioned_or(".checker "), intents=intents)

# Make bot instance accessible for message editor
class RSCheckerBot:
    """Wrapper to make bot instance accessible to message editor"""
    def __init__(self):
        self.config = config
        self.messages = messages_data
    
    def save_messages(self):
        """Save messages to JSON file"""
        global messages_data
        try:
            with open(MESSAGES_FILE, "w", encoding="utf-8") as f:
                json.dump(self.messages, f, indent=2, ensure_ascii=False)
            messages_data = self.messages  # Update global
            log.info("Messages saved to messages.json")
        except Exception as e:
            log.error(f"Failed to save messages: {e}")

bot_instance = RSCheckerBot()

# -----------------------------
# Invite Tracking JSON
# -----------------------------
def load_invites():
    """Load invites from JSON file"""
    invites_path = Path(INVITES_FILE)
    if invites_path.exists():
        try:
            with open(invites_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("invites", {})
        except Exception as e:
            log.error(f"Failed to load invites: {e}")
            return {}
    return {}

def save_invites(invites_data: dict):
    """Save invites to JSON file"""
    invites_path = Path(INVITES_FILE)
    try:
        json_data = {
            "invites": invites_data,
            "last_updated": datetime.now().isoformat()
        }
        with open(invites_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"Failed to save invites: {e}")

# Load invites on startup
invites_data = load_invites()

# -----------------------------
# State
# -----------------------------
queue_state: Dict[str, Dict[str, str]] = {}
registry: Dict[str, Dict[str, str]] = {}
last_send_at: Optional[datetime] = None
pending_checks: set[int] = set()
pending_former_checks: set[int] = set()
invite_tracking: Dict[str, str] = {}  # invite_code -> lead_id
invite_usage_cache: Dict[str, int] = {}  # invite_code -> last known uses

# Whop API client (initialized from config)
whop_api_client = None

# Channels for Whop resolution/dispute reporting (created/located at runtime)
WHOP_DISPUTE_CHANNEL_ID: int | None = None
WHOP_RESOLUTION_CHANNEL_ID: int | None = None

# Correlation IDs (short-lived, for searchability across related events)
cid_cache: Dict[int, Dict[str, str]] = {}  # user_id -> {"cid": str, "expires_at": iso}

# Role update batching (reduce spam from rapid add/remove sequences)
pending_role_updates: Dict[int, Dict[str, object]] = {}  # user_id -> {"added": set[int], "removed": set[int], "member": discord.Member, "task": asyncio.Task}

# -----------------------------
# Utils: persistence/time
# -----------------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)

def save_all():
    save_json(QUEUE_FILE, queue_state)
    save_json(REGISTRY_FILE, registry)

def _cid_for(user_id: int) -> str:
    """Return a short-lived correlation ID for a user (helps support search one token)."""
    now = _now()
    rec = cid_cache.get(int(user_id))
    if rec:
        try:
            exp = datetime.fromisoformat(str(rec.get("expires_at", "")).replace("Z", "+00:00"))
            if exp > now and rec.get("cid"):
                return str(rec["cid"])
        except Exception:
            pass
    cid = f"CID-{int(user_id)}-{now.strftime('%m%d%H%M%S')}"
    cid_cache[int(user_id)] = {"cid": cid, "expires_at": (now + timedelta(minutes=CID_TTL_MINUTES)).isoformat()}
    return cid

def _should_post_boot() -> bool:
    """Throttle noisy boot posts to Discord."""
    try:
        if BOOT_POST_MIN_HOURS <= 0:
            return True
        state = load_json(BOOT_STATE_FILE)
        last_iso = str(state.get("last_boot_post_iso") or "").strip()
        if last_iso:
            try:
                last_dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
                if (_now() - last_dt) < timedelta(hours=BOOT_POST_MIN_HOURS):
                    return False
            except Exception:
                pass
        state["last_boot_post_iso"] = _now().isoformat()
        save_json(BOOT_STATE_FILE, state)
        return True
    except Exception:
        return True

async def _flush_role_update(user_id: int) -> None:
    """Flush a batched role update after a short debounce window."""
    await asyncio.sleep(max(0.2, ROLE_UPDATE_BATCH_SECONDS))
    rec = pending_role_updates.pop(int(user_id), None)
    if not rec:
        return
    member = rec.get("member")
    if not isinstance(member, discord.Member):
        return
    added: set[int] = rec.get("added") or set()
    removed: set[int] = rec.get("removed") or set()
    if not added and not removed:
        return

    cid = _cid_for(member.id)
    added_names = _fmt_role_list(set(added), member.guild) if added else None
    removed_names = _fmt_role_list(set(removed), member.guild) if removed else None

    log_msg = f"🔄 **Roles Changed:** {_fmt_user(member)}\n"
    log_msg += f"   🧩 CID: `{cid}`\n"
    if removed_names:
        log_msg += f"   ➖ **Removed:** {removed_names}\n"
    if added_names:
        log_msg += f"   ➕ **Added:** {added_names}\n"
    await log_role_event(log_msg.rstrip())

def _save_raw_webhook_payload(payload: dict, headers: dict = None):
    """Save raw webhook payload to JSON file for inspection"""
    try:
        # Load existing payloads
        if WHOP_WEBHOOK_RAW_LOG_FILE.exists():
            with open(WHOP_WEBHOOK_RAW_LOG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {"payloads": []}
        
        # Add new payload with timestamp
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
            "headers": dict(headers) if headers else {}
        }
        data["payloads"].append(entry)
        
        # Keep last 1000 entries to avoid file bloat
        data["payloads"] = data["payloads"][-1000:]
        
        # Save to file
        with open(WHOP_WEBHOOK_RAW_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved raw webhook payload to {WHOP_WEBHOOK_RAW_LOG_FILE.name}")
    except Exception as e:
        log.error(f"Failed to save raw webhook payload: {e}")

# -----------------------------
# Member History Helpers
# -----------------------------
def _load_member_history() -> dict:
    """Load member history from JSON file"""
    try:
        if not MEMBER_HISTORY_FILE.exists() or MEMBER_HISTORY_FILE.stat().st_size == 0:
            return {}
        return json.loads(MEMBER_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_member_history(db: dict) -> None:
    """Save member history to JSON file"""
    try:
        MEMBER_HISTORY_FILE.write_text(json.dumps(db, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


STAFF_ALERTS_FILE = BASE_DIR / "staff_alerts.json"

def _touch_join(discord_id: int) -> dict:
    """Record member join event, return history record"""
    now = int(datetime.now(timezone.utc).timestamp())
    db = _load_member_history()
    key = str(discord_id)
    rec = db.get(key, {
        "first_join_ts": now,
        "last_join_ts": now,
        "last_leave_ts": None,
        "join_count": 0
    })
    rec["last_join_ts"] = now
    rec["join_count"] = int(rec.get("join_count", 0)) + 1
    # Only set first_join_ts if this is truly the first join
    if "first_join_ts" not in rec or rec["first_join_ts"] is None:
        rec["first_join_ts"] = now
    db[key] = rec
    _save_member_history(db)
    return rec

def _touch_leave(discord_id: int) -> dict:
    """Record member leave event, return history record"""
    now = int(datetime.now(timezone.utc).timestamp())
    db = _load_member_history()
    key = str(discord_id)
    rec = db.get(key)
    if not rec:
        # User left but we never tracked a join (edge case)
        rec = {
            "first_join_ts": None,
            "last_join_ts": None,
            "last_leave_ts": now,
            "join_count": 0
        }
    else:
        rec["last_leave_ts"] = now
    db[key] = rec
    _save_member_history(db)
    return rec

def _fmt_ts(ts: int | None, style: str = "D") -> str:
    """Format timestamp as Discord timestamp (human-readable)
    
    Styles:
    - 'D' = Short date: Aug 11, 2025
    - 'F' = Full date: August 11, 2025 4:40 PM
    - 'R' = Relative: 3 months ago
    """
    if not ts:
        return "—"
    try:
        return f"<t:{int(ts)}:{style}>"
    except Exception:
        return "—"


def _fmt_discord_ts_any(ts_str: str | int | float | None, style: str = "D") -> str:
    """Format ISO or unix timestamp into a Discord timestamp string."""
    if ts_str is None or ts_str == "":
        return "—"
    try:
        s = str(ts_str).strip()
        if not s:
            return "—"
        # ISO-ish path
        if "T" in s or "-" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return f"<t:{int(dt.timestamp())}:{style}>"
        return f"<t:{int(float(s))}:{style}>"
    except Exception:
        return "—"

def _fmt_date_any(ts_str: str | int | float | None) -> str:
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


def _fmt_money(amount: object, currency: str | None = None) -> str:
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

async def _fetch_whop_brief_by_membership_id(membership_id: str) -> dict:
    """Fetch a minimal Whop summary for staff (no internal IDs)."""
    global whop_api_client
    mid = (membership_id or "").strip()
    if not mid:
        return {}
    return await fetch_whop_brief(
        whop_api_client,
        mid,
        enable_enrichment=bool(WHOP_API_CONFIG.get("enable_enrichment", True)),
    )

def get_member_history(discord_id: int) -> dict:
    """Get member history record (exposed for whop_webhook_handler and support cards)"""
    db = _load_member_history()
    return db.get(str(discord_id), {})

def _backfill_whop_timeline_from_whop_history() -> None:
    """Backfill Whop lifecycle timeline from whop_history.json.
    
    Stores Whop events in member_history[discord_id]["whop"] sub-object.
    Non-destructive: only adds/updates "whop" timeline, never overwrites Discord join/leave fields.
    """
    try:
        # Load path from config
        default_path = BASE_DIR.parent / "RSAdminBot" / "whop_data" / "whop_history.json"
        whop_history_path = default_path
        
        try:
            config_path = BASE_DIR / "config.json"
            if config_path.exists():
                with open(config_path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                custom_path = config_data.get("paths", {}).get("whop_history")
                if custom_path:
                    whop_history_path = (BASE_DIR / custom_path).resolve()
        except Exception:
            pass  # Use default path if config loading fails
        
        if not whop_history_path.exists():
            log.info("whop_history.json not found, skipping Whop timeline backfill")
            return
        
        # Load whop_history.json
        try:
            with open(whop_history_path, "r", encoding="utf-8") as f:
                whop_history = json.load(f)
        except Exception as e:
            log.warning(f"Failed to load whop_history.json: {e}")
            return
        
        events = whop_history.get("membership_events", [])
        if not events:
            log.info("No membership events in whop_history.json")
            return
        
        # Load existing member_history
        member_history = _load_member_history()
        backfilled_count = 0
        
        for event in events:
            discord_id_str = event.get("discord_id", "").strip()
            if not discord_id_str:
                continue
            
            try:
                discord_id = int(discord_id_str)
            except (ValueError, TypeError):
                continue
            
            # Parse timestamp
            timestamp_str = event.get("timestamp") or event.get("created_at")
            if not timestamp_str:
                continue
            
            try:
                # Parse ISO timestamp to Unix timestamp
                if "T" in str(timestamp_str):
                    dt = datetime.fromisoformat(str(timestamp_str).replace("Z", "+00:00"))
                    event_ts = int(dt.timestamp())
                else:
                    event_ts = int(float(str(timestamp_str)))
            except (ValueError, TypeError, AttributeError):
                continue
            
            # Get or create member record
            key = str(discord_id)
            if key not in member_history:
                member_history[key] = {}
            
            # Get or create whop timeline sub-object
            if "whop" not in member_history[key]:
                member_history[key]["whop"] = {}
            
            whop_timeline = member_history[key]["whop"]
            
            # Get status (normalize to lowercase)
            status = (event.get("membership_status", "") or "").strip().lower()
            event_type = (event.get("event_type", "") or "").strip().lower()
            
            # Update timeline fields
            # first_seen_ts: min of all timestamps (only set if missing or new event is earlier)
            if "first_seen_ts" not in whop_timeline or whop_timeline["first_seen_ts"] is None or event_ts < whop_timeline["first_seen_ts"]:
                whop_timeline["first_seen_ts"] = event_ts
            
            # last_seen_ts: max of all timestamps
            if "last_seen_ts" not in whop_timeline or whop_timeline["last_seen_ts"] is None or event_ts > whop_timeline["last_seen_ts"]:
                whop_timeline["last_seen_ts"] = event_ts
            
            # last_active_ts: max timestamp where status is "active" or "trialing"
            if status in ("active", "trialing"):
                if "last_active_ts" not in whop_timeline or whop_timeline["last_active_ts"] is None or event_ts > whop_timeline["last_active_ts"]:
                    whop_timeline["last_active_ts"] = event_ts
            
            # last_canceled_ts: max timestamp where status is "canceled" or event_type is "cancellation"
            if status == "canceled" or event_type == "cancellation":
                if "last_canceled_ts" not in whop_timeline or whop_timeline["last_canceled_ts"] is None or event_ts > whop_timeline["last_canceled_ts"]:
                    whop_timeline["last_canceled_ts"] = event_ts
            
            # last_status: most recent status (normalize to lowercase)
            if status:
                whop_timeline["last_status"] = status
            
            # last_membership_id: from event if available
            membership_id = event.get("whop_key") or event.get("membership_id")
            if membership_id:
                whop_timeline["last_membership_id"] = str(membership_id)
            
            # last_user_id: from event if available (though whop_history.json doesn't seem to have this)
            # Keeping for future compatibility
            
            backfilled_count += 1
        
        # Save merged history (non-destructive: only whop sub-object was modified)
        _save_member_history(member_history)
        
        log.info(f"Whop timeline backfill complete: {backfilled_count} events processed, {len([k for k, v in member_history.items() if v.get('whop')])} members with Whop timeline")
    except Exception as e:
        log.error(f"Whop timeline backfill failed: {e}", exc_info=True)

def _access_roles_plain(member: discord.Member) -> str:
    """Return a compact list of access-relevant role names (no mentions).

    This intentionally filters out "noise" roles so support can quickly see access state.
    """
    relevant_ids = coerce_role_ids(ROLE_CANCEL_A, ROLE_CANCEL_B, WELCOME_ROLE_ID, ROLE_TRIGGER, FORMER_MEMBER_ROLE)
    try:
        relevant_ids.update({int(x) for x in ROLES_TO_CHECK if str(x).strip().isdigit()})
    except Exception:
        pass
    return access_roles_plain(member, relevant_ids)

def load_settings() -> dict:
    """Load settings from JSON file, default to enabled if missing/bad"""
    if not SETTINGS_FILE.exists():
        return {"dm_sequence_enabled": True}
    try:
        data = load_json(SETTINGS_FILE)
        return {"dm_sequence_enabled": data.get("dm_sequence_enabled", True)}
    except Exception:
        return {"dm_sequence_enabled": True}

def save_settings(settings: dict) -> None:
    """Save settings to JSON file"""
    save_json(SETTINGS_FILE, settings)

# -----------------------------
# Logging to Discord channels
# -----------------------------
def _fmt_user(member: discord.abc.User) -> str:
    return f"**{member.display_name}** ({member.id})"

def _fmt_role(role_id: int, guild: discord.Guild) -> str:
    """Format role as name (ID) or just ID if not found."""
    role = guild.get_role(role_id) if guild else None
    if role:
        return f"**{role.name}** (`{role_id}`)"
    return f"`{role_id}`"

def _fmt_role_list(role_ids: set, guild: discord.Guild) -> str:
    """Format list of roles as names. Excludes @everyone role to prevent mentions."""
    roles = []
    for rid in role_ids:
        # Filter out @everyone role (ID equals guild ID) to prevent mentions
        if guild and rid == guild.id:
            continue
        role = guild.get_role(rid) if guild else None
        if role:
            roles.append(f"**{role.name}**")
        else:
            roles.append(f"`{rid}`")
    return ", ".join(roles) if roles else "none"

def m_user(member: discord.Member) -> str:
    """Format member as mentionable user (@user)"""
    return member.mention

def m_channel(channel: discord.abc.GuildChannel) -> str:
    """Format channel as mentionable (#channel)"""
    return channel.mention

def t_role(role_id: int, guild: discord.Guild) -> str:
    """Format role as plain text (no mention) - alias for _fmt_role for clarity"""
    return _fmt_role(role_id, guild)

async def log_first(msg: str):
    ch = bot.get_channel(LOG_FIRST_CHANNEL_ID)
    if ch:
        with suppress(Exception):
            await ch.send(msg)

async def log_other(msg: str):
    ch = bot.get_channel(LOG_OTHER_CHANNEL_ID)
    if ch:
        with suppress(Exception):
            await ch.send(msg)

async def log_role_event(message: str):
    await log_other(message)

async def log_whop(msg: str):
    """Log to Whop logs channel (for subscription data from Whop system)"""
    if WHOP_LOGS_CHANNEL_ID:
        ch = bot.get_channel(WHOP_LOGS_CHANNEL_ID)
        if ch:
            with suppress(Exception):
                await ch.send(msg)

def _find_text_channel_by_name(guild: discord.Guild, name: str) -> discord.TextChannel | None:
    nm = (name or "").strip().lower()
    if not guild or not nm:
        return None
    for ch in guild.text_channels:
        if str(ch.name or "").lower() == nm:
            return ch
    return None


async def _ensure_alert_channels(guild: discord.Guild) -> None:
    """Ensure the two staff alert channels exist and live under the staff alerts category."""
    if not guild:
        return
    category = guild.get_channel(STAFF_ALERTS_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        category = None

    for name in (PAYMENT_FAILURE_CHANNEL_NAME, MEMBER_CANCELLATION_CHANNEL_NAME):
        existing = _find_text_channel_by_name(guild, name)
        if existing:
            # Best-effort: ensure it is inside the category for visibility/organization.
            if category and getattr(existing, "category_id", None) != category.id:
                with suppress(Exception):
                    await existing.edit(category=category, reason="RSCheckerbot: move staff alert channel into category")
            continue
        try:
            await guild.create_text_channel(name=name, category=category, reason="RSCheckerbot: staff alert channel")
        except Exception as e:
            # Fallback: category permissions can block creation even if the bot can create channels in general.
            log.warning(f"[Alerts] Failed to create #{name} in category; retrying without category: {e}")
            with suppress(Exception):
                await guild.create_text_channel(name=name, reason="RSCheckerbot: staff alert channel (fallback)")


async def log_member_status(msg: str, embed: discord.Embed = None, *, channel_name: str | None = None):
    """Log staff embeds. Defaults to member status logs channel, but can route by channel name."""
    guild = bot.get_guild(GUILD_ID) if GUILD_ID else None
    if not guild:
        return

    ch: discord.TextChannel | None = None
    if channel_name:
        ch = _find_text_channel_by_name(guild, channel_name)
        if ch is None:
            log.warning(f"[Log] Requested channel '{channel_name}' not found; falling back to member_status_logs_channel_id")
    if ch is None and MEMBER_STATUS_LOGS_CHANNEL_ID:
        base = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
        ch = base if isinstance(base, discord.TextChannel) else None

    if not ch:
        return

    with suppress(Exception):
        # Use provided embed or create default one
        if embed is None:
            embed = discord.Embed(
                description=msg,
                color=0x5865F2,  # Discord blurple color
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text="RSCheckerbot • Member Status Tracking")
        # Mentions should be clickable but MUST NOT ping users.
        await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

# -----------------------------
# Registry helpers
# -----------------------------
def has_sequence_before(user_id: int) -> bool:
    return str(user_id) in registry

def mark_started(user_id: int):
    uid = str(user_id)
    if uid not in registry:
        registry[uid] = {
            "started_at": _now().isoformat(),
            "completed": False,
        }
        save_json(REGISTRY_FILE, registry)

def mark_cancelled(user_id: int, reason: str):
    uid = str(user_id)
    if uid not in registry:
        registry[uid] = {"started_at": _now().isoformat()}
    registry[uid]["completed"] = True
    registry[uid]["cancel_reason"] = reason
    queue_state.pop(uid, None)
    save_all()

def mark_finished(user_id: int):
    uid = str(user_id)
    if uid not in registry:
        registry[uid] = {"started_at": _now().isoformat()}
    registry[uid]["completed"] = True
    registry[uid]["cancel_reason"] = "finished"
    queue_state.pop(uid, None)
    save_all()

# -----------------------------
# Queue helpers
# -----------------------------
def enqueue_first_day(user_id: int):
    settings = load_settings()
    if not settings.get("dm_sequence_enabled", True):
        return  # DM sequence disabled, don't enqueue
    queue_state[str(user_id)] = {
        "current_day": "day_1",
        "next_send": _now().isoformat().replace("+00:00", "Z"),
    }
    save_json(QUEUE_FILE, queue_state)
    mark_started(user_id)

def schedule_next(user_id: int, current_day: str):
    if current_day not in DAY_KEYS:
        mark_cancelled(user_id, "internal_error_bad_day")
        return
    if current_day == "day_7b":
        mark_finished(user_id)
        return

    idx = DAY_KEYS.index(current_day)
    next_day = DAY_KEYS[idx + 1]
    delay = timedelta(minutes=DAY7B_DELAY_MIN) if next_day == "day_7b" else timedelta(hours=DAY_GAP_HOURS)
    next_time = _now() + delay

    queue_state[str(user_id)] = {
        "current_day": next_day,
        "next_send": next_time.isoformat().replace("+00:00", "Z"),
    }
    save_json(QUEUE_FILE, queue_state)

def is_due(next_send_iso: str) -> bool:
    try:
        nxt = datetime.fromisoformat(next_send_iso.replace("Z", "+00:00"))
        return _now() >= nxt
    except Exception:
        return True

# -----------------------------
# Role checks
# -----------------------------
def has_cancel_role(member: discord.Member) -> bool:
    role_ids = {r.id for r in member.roles}
    return (ROLE_CANCEL_A in role_ids) or (ROLE_CANCEL_B in role_ids)

def has_trigger_role(member: discord.Member) -> bool:
    return any(r.id == ROLE_TRIGGER for r in member.roles)

def has_member_role(member: discord.Member) -> bool:
    return any(r.id == ROLE_CANCEL_A for r in member.roles)

def has_former_member_role(member: discord.Member) -> bool:
    return any(r.id == FORMER_MEMBER_ROLE for r in member.roles)

# -----------------------------
# Message loader/sender
# -----------------------------
async def send_day(member: discord.Member, day_key: str):
    settings = load_settings()
    if not settings.get("dm_sequence_enabled", True):
        return  # DM sequence disabled, don't send
    global last_send_at

    if last_send_at:
        delta = (_now() - last_send_at).total_seconds()
        if delta < SEND_SPACING_SECONDS:
            await asyncio.sleep(SEND_SPACING_SECONDS - delta)

    if has_cancel_role(member):
        cancel_roles = []
        if ROLE_CANCEL_A and any(r.id == ROLE_CANCEL_A for r in member.roles):
            cancel_roles.append(_fmt_role(ROLE_CANCEL_A, member.guild))
        if ROLE_CANCEL_B and any(r.id == ROLE_CANCEL_B for r in member.roles):
            cancel_roles.append(_fmt_role(ROLE_CANCEL_B, member.guild))
        cancel_info = ", ".join(cancel_roles) if cancel_roles else "cancel role"
        mark_cancelled(member.id, "cancel_role_present_pre_send")
        await log_other(f"🛑 Cancelled pre-send for {_fmt_user(member)} — {cancel_info} present (DM not sent)")
        return

    join_url = UTM_LINKS.get(day_key)
    if not join_url:
        mark_cancelled(member.id, "missing_utm")
        await log_other(f"❌ Missing UTM for `{day_key}` on {_fmt_user(member)} — sequence cancelled")
        return

    try:
        # Build embed from JSON (single source of truth)
        from view import get_dm_view
        day_data = (messages_data.get("days") or {}).get(day_key) or {}

        banner_url = day_data.get("banner_url") or messages_data.get("banner_url")
        footer_url = day_data.get("footer_url") or messages_data.get("footer_url")
        main_image_url = day_data.get("main_image_url")
        description = str(day_data.get("description", "")).format(join_url=join_url)

        banner_embed = discord.Embed()
        if banner_url:
            banner_embed.set_image(url=banner_url)

        content_embed = discord.Embed(description=description)
        if main_image_url:
            content_embed.set_image(url=main_image_url)
        elif footer_url:
            content_embed.set_image(url=footer_url)

        view = get_dm_view(day_number=day_key, join_url=join_url)
        embeds = [banner_embed, content_embed]
    except Exception as e:
        mark_cancelled(member.id, "embed_build_error")
        await log_other(f"❌ build_embed error `{day_key}` for {_fmt_user(member)}: `{e}` — sequence cancelled")
        return

    try:
        await member.send(embeds=embeds, view=view)
        last_send_at = _now()
        if day_key == "day_1":
            await log_first(f"✅ Sent **{day_key}** to {_fmt_user(member)}")
        else:
            await log_other(f"✅ Sent **{day_key}** to {_fmt_user(member)}")
    except discord.Forbidden:
        mark_cancelled(member.id, "dm_forbidden")
        await log_other(f"🚫 DM forbidden for {_fmt_user(member)} — sequence cancelled (user blocked DMs)")
    except Exception as e:
        await log_other(f"⚠️ Failed to send **{day_key}** to {_fmt_user(member)}: `{e}`")

# -----------------------------
# Invite Tracking Functions
# -----------------------------
async def create_single_use_invite(email: str, lead_id: str, utm_data: dict = None) -> Optional[str]:
    """Create a single-use Discord invite link and store it in JSON."""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            log.error("Guild not found")
            return FALLBACK_INVITE if FALLBACK_INVITE else None

        channel = guild.get_channel(INVITE_CHANNEL_ID) if INVITE_CHANNEL_ID else None
        if not channel:
            log.error("Invite channel not found")
            return FALLBACK_INVITE if FALLBACK_INVITE else None

        invite = await channel.create_invite(
            max_uses=1,
            max_age=604800,  # 7 days
            unique=True
        )

        # Store in JSON
        global invites_data
        invites_data[invite.code] = {
            "lead_id": lead_id,
            "email": email,
            "utm_data": utm_data,
            "created_at": _now().isoformat(),
            "used_at": None,
            "discord_user_id": None,
            "discord_username": None
        }
        save_invites(invites_data)

        invite_tracking[invite.code] = lead_id
        log.info(f"Created invite {invite.code} for lead {lead_id}")
        return invite.url

    except Exception as e:
        log.error(f"Failed to create invite: {e}")
        return FALLBACK_INVITE if FALLBACK_INVITE else None

async def update_ghl_contact(lead_id: str, discord_user_id: str, discord_username: str, tag: str = "Discord Joined"):
    """Update HighLevel contact with Discord information."""
    if not GHL_API_KEY or not GHL_LOCATION_ID:
        log.warning("GHL API credentials not configured, skipping update")
        return

    try:
        url = f"https://services.leadconnectorhq.com/contacts/{lead_id}"
        headers = {
            "Authorization": f"Bearer {GHL_API_KEY}",
            "Version": "2021-07-28",
            "Content-Type": "application/json"
        }

        update_data = {
            "tags": [tag]
        }

        if GHL_CF_DISCORD_USERNAME:
            update_data["customFields"] = [
                {
                    "id": GHL_CF_DISCORD_USERNAME,
                    "value": discord_username
                }
            ]
        if GHL_CF_DISCORD_ID:
            if "customFields" not in update_data:
                update_data["customFields"] = []
            update_data["customFields"].append({
                "id": GHL_CF_DISCORD_ID,
                "value": str(discord_user_id)
            })

        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=headers, json=update_data) as resp:
                if resp.status == 200:
                    log.info(f"Updated GHL contact {lead_id} with Discord info")
                else:
                    log.error(f"GHL update failed: {resp.status} - {await resp.text()}")

    except Exception as e:
        log.error(f"Failed to update GHL: {e}")

async def track_invite_usage(invite_code: str, member: discord.Member):
    """Track when an invite is used and update GHL."""
    global invites_data
    
    if invite_code in invites_data:
        invite_entry = invites_data[invite_code]
        used_at = invite_entry.get("used_at")
        
        # Only process if not already tracked
        if used_at is None:
            invite_entry["used_at"] = _now().isoformat()
            invite_entry["discord_user_id"] = str(member.id)
            invite_entry["discord_username"] = str(member)
            save_invites(invites_data)

            lead_id = invite_entry.get("lead_id")
            await update_ghl_contact(lead_id, str(member.id), str(member), "Discord Joined")
            log.info(f"Tracked invite usage: {invite_code} -> {member} (lead: {lead_id})")
        else:
            log.debug(f"Invite {invite_code} already tracked")
    else:
        log.warning(f"Unknown invite code used: {invite_code}")

# -----------------------------
# HTTP Server for Invite Creation
# -----------------------------
async def handle_create_invite(request):
    """Handle POST request to create an invite."""
    try:
        data = await request.json()
        email = data.get("email")
        lead_id = data.get("lead_id")
        utm_data = data.get("utm_data", {})

        if not email or not lead_id:
            return web.json_response({"error": "email and lead_id required"}, status=400)

        invite_url = await create_single_use_invite(email, lead_id, utm_data)
        if not invite_url:
            return web.json_response({"error": "Failed to create invite"}, status=500)

        if GHL_API_KEY and GHL_LOCATION_ID:
            try:
                url = f"https://services.leadconnectorhq.com/contacts/{lead_id}/tags"
                headers = {
                    "Authorization": f"Bearer {GHL_API_KEY}",
                    "Version": "2021-07-28",
                    "Content-Type": "application/json"
                }
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json={"tags": ["Discord Invited"]}) as resp:
                        if resp.status != 200:
                            log.warning(f"Failed to tag GHL contact: {resp.status}")
            except Exception as e:
                log.error(f"Failed to tag GHL: {e}")

        return web.json_response({"invite_url": invite_url})

    except Exception as e:
        log.error(f"Error handling invite request: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def handle_whop_webhook_receiver(request):
    """Receive Whop webhook payloads, log them, and forward to Discord"""
    try:
        # Get raw payload
        if request.content_type == 'application/json':
            payload = await request.json()
        else:
            # Try form data
            form_data = await request.post()
            payload = dict(form_data)
        
        # Get headers (for debugging)
        headers = dict(request.headers)
        
        # Log the raw payload
        _save_raw_webhook_payload(payload, headers)
        log.info(f"Received Whop webhook payload (saved to {WHOP_WEBHOOK_RAW_LOG_FILE.name})")
        
        # Forward to Discord webhook if URL is configured
        if not DISCORD_WEBHOOK_URL:
            log.warning("Discord webhook URL not configured - payload logged but not forwarded")
            return web.Response(text="OK (logged, not forwarded - no webhook URL)", status=200)
        
        # Forward to Discord webhook
        async with aiohttp.ClientSession() as session:
            async with session.post(
                DISCORD_WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as resp:
                if resp.status == 200 or resp.status == 204:
                    log.info(f"Forwarded webhook to Discord successfully (status: {resp.status})")
                    return web.Response(text="OK", status=200)
                else:
                    error_text = await resp.text()
                    log.error(f"Failed to forward webhook to Discord (status: {resp.status}): {error_text}")
                    return web.Response(text=f"Forward failed: {error_text}", status=500)
    
    except Exception as e:
        log.error(f"Error handling webhook receiver: {e}", exc_info=True)
        return web.Response(text=f"Error: {str(e)}", status=500)

async def init_http_server():
    """Initialize the HTTP server for invite creation and Whop webhook receiver."""
    app = web.Application()
    app.router.add_post("/create-invite", handle_create_invite)
    app.router.add_post("/whop-webhook", handle_whop_webhook_receiver)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_SERVER_PORT)
    await site.start()
    log.info(f"HTTP server started on port {HTTP_SERVER_PORT}")
    log.info(f"Whop webhook receiver: http://0.0.0.0:{HTTP_SERVER_PORT}/whop-webhook")

# -----------------------------
# Cross-bot communication checks
# -----------------------------
async def check_onboarding_ticket(user_id: int, guild: discord.Guild, delay_seconds: int = 5):
    """Check if RSOnboarding created a ticket after Welcome role was assigned."""
    await asyncio.sleep(delay_seconds)  # Give RSOnboarding time to process
    
    try:
        member = guild.get_member(user_id)
        if not member:
            return
        
        # Check if user has Welcome role but no Member role (should have ticket)
        welcome_role = guild.get_role(WELCOME_ROLE_ID)
        member_role = guild.get_role(ROLE_CANCEL_A)  # Member role
        has_welcome = welcome_role and welcome_role in member.roles
        has_member = member_role and member_role in member.roles
        
        if has_welcome and not has_member:
            # User has Welcome but not Member - ticket should exist
            await log_role_event(
                f"🔍 **Onboarding Check** for {_fmt_user(member)}\n"
                f"   ✅ Has {_fmt_role(WELCOME_ROLE_ID, guild)}\n"
                f"   ❌ Missing {_fmt_role(ROLE_CANCEL_A, guild)}\n"
                f"   📋 Expected: RSOnboarding should create ticket"
            )
    except Exception as e:
        log.error(f"Error checking onboarding ticket for {user_id}: {e}")

# -----------------------------
# 60-second fallback checker
# -----------------------------
async def check_and_assign_role(member: discord.Member):
    if member.bot or member.id in pending_checks:
        return
    pending_checks.add(member.id)
    try:
        await asyncio.sleep(60)

        guild = member.guild
        current_roles = {r.id for r in member.roles}
        has_any = any(role.id in ROLES_TO_CHECK for role in member.roles)
        
        # Get which checked roles the user has vs missing
        user_has_checked = [r.id for r in member.roles if r.id in ROLES_TO_CHECK]
        user_missing_checked = [rid for rid in ROLES_TO_CHECK if rid not in current_roles]
        
        if not has_any:
            trigger_role = guild.get_role(ROLE_TO_ASSIGN)
            if trigger_role is None:
                await log_role_event(f"❌ **Error:** Trigger role {_fmt_role(ROLE_TO_ASSIGN, guild)} not found for {_fmt_user(member)}")
                return
            try:
                roles_to_add = [trigger_role]
                roles_to_add_names = [trigger_role.name]
                
                await member.add_roles(*roles_to_add, reason="No valid roles after 60s")
                
                # Format all checked roles and which ones are missing
                all_checked_roles = _fmt_role_list(ROLES_TO_CHECK, guild)
                missing_checked_roles = _fmt_role_list(set(user_missing_checked), guild)
                
                await log_role_event(
                    f"✅ **Roles Assigned** to {_fmt_user(member)}\n"
                    f"   📌 Assigned: {', '.join(f'**{name}**' for name in roles_to_add_names)}\n"
                    f"   🔍 **Basis for assignment:**\n"
                    f"      ❌ User has NONE of the checked roles\n"
                    f"      📋 Checked roles (all {len(ROLES_TO_CHECK)}): {all_checked_roles}\n"
                    f"      ❌ Missing all: {missing_checked_roles}\n"
                    f"   ⏱️ Reason: No valid roles found after 60s → assigning trigger role"
                )
                
                if not has_sequence_before(member.id):
                    enqueue_first_day(member.id)
                    await log_first(f"🧵 Enqueued **day_1** for {_fmt_user(member)} (60s fallback - no checked roles)")
            except Exception as e:
                await log_role_event(f"⚠️ **Failed to assign roles** to {_fmt_user(member)}\n   ❌ Error: `{e}`")
        else:
            # User has checked roles - log what they have vs what's checked
            user_has_names = _fmt_role_list(set(user_has_checked), guild)
            all_checked_roles = _fmt_role_list(ROLES_TO_CHECK, guild)
            
            await log_role_event(
                f"ℹ️ **Role check skipped** for {_fmt_user(member)}\n"
                f"   ✅ User HAS checked roles: {user_has_names}\n"
                f"   🔍 All checked roles ({len(ROLES_TO_CHECK)}): {all_checked_roles}\n"
                f"   📋 **Basis:** User has valid role(s) → no trigger role needed"
            )
    finally:
        pending_checks.discard(member.id)

async def delayed_assign_former_member(member: discord.Member):
    if member.bot or member.id in pending_former_checks:
        return
    pending_former_checks.add(member.id)
    try:
        await asyncio.sleep(FORMER_MEMBER_DELAY_SECONDS)

        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return
        refreshed = guild.get_member(member.id)
        if not refreshed:
            return

        if has_member_role(refreshed):
            await log_role_event(
                f"↩️ **Member Role Regained:** {_fmt_user(refreshed)}\n"
                f"   ✅ Has {_fmt_role(ROLE_CANCEL_A, guild)} again\n"
                f"   📋 Not marking as Former Member"
            )
            return

        if not has_former_member_role(refreshed):
            role = guild.get_role(FORMER_MEMBER_ROLE)
            if role is None:
                await log_role_event(f"❌ **Error:** Former-member role {_fmt_role(FORMER_MEMBER_ROLE, guild)} not found for {_fmt_user(refreshed)}")
            else:
                try:
                    await refreshed.add_roles(role, reason="Lost member role; mark as former member")
                    await log_role_event(
                        f"🏷️ **Former Member Role Assigned:** {_fmt_user(refreshed)}\n"
                        f"   📌 Assigned: {_fmt_role(FORMER_MEMBER_ROLE, guild)}"
                    )
                except Exception as e:
                    await log_role_event(f"⚠️ **Failed to assign Former Member role** to {_fmt_user(refreshed)}\n   ❌ Error: `{e}`")

        extra_role = guild.get_role(1224748748920328384)
        if extra_role and extra_role not in refreshed.roles:
            try:
                await refreshed.add_roles(extra_role, reason="Lost member role; add extra role")
                await log_role_event(
                    f"🏷️ **Extra Role Assigned:** {_fmt_user(refreshed)}\n"
                    f"   📌 Assigned: {_fmt_role(1224748748920328384, guild)}"
                )
            except Exception as e:
                await log_role_event(f"⚠️ **Failed to assign extra role** to {_fmt_user(refreshed)}\n   ❌ Error: `{e}`")
    finally:
        pending_former_checks.discard(member.id)

# -----------------------------
# Scheduler loop
# -----------------------------
@tasks.loop(seconds=10)
async def scheduler_loop():
    try:
        if not bot.is_ready():
            return
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return

        for uid, payload in list(queue_state.items()):
            try:
                day_key = payload.get("current_day")
                next_send = payload.get("next_send", "")

                if not day_key or not is_due(next_send):
                    continue

                member = guild.get_member(int(uid))
                if not member:
                    mark_cancelled(int(uid), "left_guild")
                    await log_other(f"👋 User `{uid}` left guild — sequence cancelled")
                    continue

                if has_cancel_role(member):
                    cancel_roles = []
                    if ROLE_CANCEL_A and any(r.id == ROLE_CANCEL_A for r in member.roles):
                        cancel_roles.append(_fmt_role(ROLE_CANCEL_A, guild))
                    if ROLE_CANCEL_B and any(r.id == ROLE_CANCEL_B for r in member.roles):
                        cancel_roles.append(_fmt_role(ROLE_CANCEL_B, guild))
                    cancel_info = ", ".join(cancel_roles) if cancel_roles else "cancel role"
                    mark_cancelled(member.id, "cancel_role_present")
                    await log_other(f"🛑 Cancelled for {_fmt_user(member)} — {cancel_info} present (during scheduler)")
                    continue

                await send_day(member, day_key)

                # user may have been popped inside send_day (forbidden/cancel/finish)
                if str(member.id) in queue_state:
                    prev = day_key
                    schedule_next(member.id, day_key)

                    # schedule_next may have finished & popped; guard read
                    nxt = queue_state.get(str(member.id))
                    if nxt:
                        target_ch = log_other if prev != "day_1" else log_first
                        await target_ch(
                            f"🗓️ Scheduled **{nxt['current_day']}** for {_fmt_user(member)} at `{nxt['next_send']}`"
                        )
            except Exception as e:
                await log_other(f"⚠️ scheduler_loop user error for uid `{uid}`: `{e}`")
    except Exception as e:
        await log_other(f"❌ scheduler_loop tick error: `{e}`")

@scheduler_loop.error
async def scheduler_loop_error(error):
    await log_other(f"🔁 scheduler_loop crashed: `{error}` — restarting in 5s")
    with suppress(Exception):
        scheduler_loop.cancel()
    await asyncio.sleep(5)
    with suppress(Exception):
        scheduler_loop.start()

# -----------------------------
# Whop membership sync job
# -----------------------------
@tasks.loop(hours=6)  # Default interval, changed in on_ready() from config
async def sync_whop_memberships():
    """Periodically sync Discord roles with Whop membership status"""
    global whop_api_client
    
    if not whop_api_client:
        return  # API client not available
    
    if not bot.is_ready():
        return
    
    # Check if sync is enabled
    if not WHOP_API_CONFIG.get("enable_sync", True):
        return
    
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return
    
    log.info("Starting Whop membership sync...")
    synced_count = 0
    error_count = 0
    
    # Get all members with Member role
    member_role = guild.get_role(ROLE_CANCEL_A)
    if not member_role:
        log.warning("Member role not found, skipping sync")
        return
    
    members_to_check = [m for m in guild.members if member_role in m.roles]
    log.info(f"Checking {len(members_to_check)} members with Member role...")
    
    for member in members_to_check:
        try:
            # Check membership status via API (membership_id-based; avoids mismatched users)
            membership_id = get_cached_whop_membership_id(member.id)
            if not membership_id:
                continue
            verification = await whop_api_client.verify_membership_status(membership_id, "active")

            # Cancellation scheduled signal (active/trialing but cancel_at_period_end=true).
            # This does NOT DM users; it's staff-only visibility (case channel + member status logs).
            membership_data = verification.get("membership_data") if isinstance(verification, dict) else None
            if isinstance(membership_data, dict):
                status_now = str(membership_data.get("status") or "").strip().lower()
                cape_now = membership_data.get("cancel_at_period_end")
                if cape_now is True and status_now in ("active", "trialing"):
                    # Only emit this alert once per membership per renewal_end per day (prevents spam).
                    renewal_end_key = str(membership_data.get("renewal_period_end") or "").strip()
                    issue_key = f"cancel_scheduled:{membership_id}:{renewal_end_key}"

                    db_alerts = load_staff_alerts(STAFF_ALERTS_FILE)
                    if not should_post_alert(db_alerts, member.id, issue_key, cooldown_hours=24.0):
                        continue

                    access = _access_roles_plain(member)
                    whop_brief = await _fetch_whop_brief_by_membership_id(membership_id)

                    # Detailed card -> member-status-logs
                    hist = get_member_history(member.id) or {}
                    detailed = _build_member_status_detailed_embed(
                        title="⚠️ Cancellation Scheduled",
                        member=member,
                        access_roles=access,
                        color=0xFEE75C,
                        member_kv=[
                            ("first_joined", _fmt_ts(hist.get("first_join_ts"), "D") if hist.get("first_join_ts") else "—"),
                            ("join_count", hist.get("join_count") or "—"),
                        ],
                        discord_kv=[
                            ("reason", "cancel_at_period_end=true"),
                        ],
                        whop_brief=whop_brief,
                    )
                    await log_member_status("", embed=detailed)

                    # Minimal card -> member-cancelation
                    minimal = _build_case_minimal_embed(
                        title="⚠️ Cancellation Scheduled",
                        member=member,
                        access_roles=access,
                        whop_brief=whop_brief,
                        color=0xFEE75C,
                    )
                    await log_member_status("", embed=minimal, channel_name=MEMBER_CANCELLATION_CHANNEL_NAME)

                    record_alert_post(db_alerts, member.id, issue_key)
                    save_staff_alerts(STAFF_ALERTS_FILE, db_alerts)
            
            if not verification["matches"]:
                actual_status = verification["actual_status"]
                
                # If API says canceled but user has Member role, remove it
                if actual_status in ("canceled", "completed", "past_due", "unpaid"):
                    await member.remove_roles(
                        member_role, 
                        reason=f"Whop sync: Status is {actual_status}"
                    )
                    await log_other(
                        f"🔄 **Sync Removed Role:** {_fmt_user(member)}\n"
                        f"   API Status: `{actual_status}`\n"
                        f"   Removed: {_fmt_role(ROLE_CANCEL_A, guild)}"
                    )
                    synced_count += 1
        except Exception as e:
            error_count += 1
            log.error(f"Error syncing member {member.id}: {e}")
    
    log.info(f"Sync complete: {synced_count} roles updated, {error_count} errors")
    if synced_count > 0 or error_count > 0:
        await log_other(
            f"🔄 **Whop Sync Complete**\n"
            f"   Members checked: {len(members_to_check)}\n"
            f"   Roles updated: {synced_count}\n"
            f"   Errors: {error_count}"
        )

@sync_whop_memberships.error
async def sync_whop_memberships_error(error):
    await log_other(f"❌ Sync job error: `{error}`")
    log.error(f"Sync job error: {error}", exc_info=True)

# -----------------------------
# Events
# -----------------------------
@bot.event
async def on_ready():
    global queue_state, registry, invite_usage_cache, whop_api_client
    
    # Comprehensive startup logging
    log.info("="*60)
    log.info("  🔍 RS Checker Bot")
    log.info("="*60)
    log.info(f"[Bot] Ready as {bot.user} (ID: {bot.user.id})")
    
    queue_state = load_json(QUEUE_FILE)
    registry = load_json(REGISTRY_FILE)
    
    # Backfill Whop timeline from whop_history.json (before initializing whop handler)
    _backfill_whop_timeline_from_whop_history()
    
    guild = bot.get_guild(GUILD_ID)
    if guild:
        log.info(f"[Bot] Connected to: {guild.name}")

        # Ensure the two staff alert channels exist (payment-failure + member-cancelation).
        await _ensure_alert_channels(guild)
        
        # Display config information
        log.info("")
        log.info("[Config] Configuration Information:")
        log.info("-"*60)
        log.info(f"🏠 Guild: {guild.name} (ID: {GUILD_ID})")
        
        # DM Sequence Config
        if ROLE_TRIGGER:
            trigger_role = guild.get_role(ROLE_TRIGGER)
            if trigger_role:
                log.info(f"🎯 Trigger Role: {trigger_role.name} (ID: {ROLE_TRIGGER})")
            else:
                log.warning(f"⚠️  Trigger Role: Not found (ID: {ROLE_TRIGGER})")
        
        if WELCOME_ROLE_ID:
            welcome_role = guild.get_role(WELCOME_ROLE_ID)
            if welcome_role:
                log.info(f"👋 Welcome Role: {welcome_role.name} (ID: {WELCOME_ROLE_ID})")
            else:
                log.warning(f"⚠️  Welcome Role: Not found (ID: {WELCOME_ROLE_ID})")
        
        if ROLES_TO_CHECK:
            log.info(f"🔍 Roles to Check: {len(ROLES_TO_CHECK)} role(s)")
            for role_id in list(ROLES_TO_CHECK)[:3]:
                role = guild.get_role(role_id)
                if role:
                    log.info(f"   • {role.name} (ID: {role_id})")
                else:
                    log.warning(f"   • ❌ Not found (ID: {role_id})")
            if len(ROLES_TO_CHECK) > 3:
                log.info(f"   ... and {len(ROLES_TO_CHECK) - 3} more")
        
        # Channels
        if LOG_FIRST_CHANNEL_ID:
            log_channel = guild.get_channel(LOG_FIRST_CHANNEL_ID)
            if log_channel:
                log.info(f"📝 Log First Channel: {log_channel.name} (ID: {LOG_FIRST_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Log First Channel: Not found (ID: {LOG_FIRST_CHANNEL_ID})")
        
        if LOG_OTHER_CHANNEL_ID:
            log_channel = guild.get_channel(LOG_OTHER_CHANNEL_ID)
            if log_channel:
                log.info(f"📝 Log Other Channel: {log_channel.name} (ID: {LOG_OTHER_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Log Other Channel: Not found (ID: {LOG_OTHER_CHANNEL_ID})")
        
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            status_channel = guild.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if status_channel:
                log.info(f"📊 Member Status Channel: {status_channel.name} (ID: {MEMBER_STATUS_LOGS_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Member Status Channel: Not found (ID: {MEMBER_STATUS_LOGS_CHANNEL_ID})")
        
        # Invite Tracking
        if INVITE_CHANNEL_ID:
            invite_channel = guild.get_channel(INVITE_CHANNEL_ID)
            if invite_channel:
                log.info(f"🔗 Invite Channel: {invite_channel.name} (ID: {INVITE_CHANNEL_ID})")
            else:
                log.warning(f"⚠️  Invite Channel: Not found (ID: {INVITE_CHANNEL_ID})")
        
        # Ensure Whop dispute/resolution channels exist (optional, config-driven)
        await _ensure_whop_resolution_channels(guild)
        
        log.info("-"*60)
        
        # Initialize invite usage cache
        try:
            invites = await guild.invites()
            for invite in invites:
                invite_usage_cache[invite.code] = invite.uses
            log.info(f"[Invites] Cached {len(invite_usage_cache)} invites")
        except Exception as e:
            log.error(f"[Invites] Error caching invites: {e}")
    else:
        log.warning(f"⚠️  Guild not found (ID: {GUILD_ID})")

    for uid, payload in queue_state.items():
        iso = payload.get("next_send")
        if not iso or is_due(iso):
            payload["next_send"] = (_now() + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    save_json(QUEUE_FILE, queue_state)

    # Queue and Registry Status
    queue_count = len(queue_state)
    registry_count = len(registry)
    log.info(f"[Queue] Active entries: {queue_count}")
    log.info(f"[Registry] Registered users: {registry_count}")
    
    if not scheduler_loop.is_running():
        scheduler_loop.start()
        log.info("[Scheduler] Started and state restored")

    if _should_post_boot():
        await log_other("🟢 [BOOT] Scheduler started and state restored.")
    
    # Cleanup old data on startup
    cleanup_old_data()
    cleanup_old_invites()
    log.info("[Cleanup] Old data cleanup completed")

    if guild:
        scheduled = 0
        for m in guild.members:
            if not m.bot and not any(r.id in ROLES_TO_CHECK for r in m.roles):
                asyncio.create_task(check_and_assign_role(m))
                scheduled += 1
        if scheduled:
            log.info(f"[Boot Check] Scheduled fallback role checks for {scheduled} member(s)")
            await log_role_event(
                f"🔍 **Boot Check Scheduled**\n"
                f"   📋 Scheduled fallback role checks for **{scheduled}** member(s)\n"
                f"   ⏱️ Will check in 60s if they need trigger role assigned"
            )

    # Start HTTP server for invite tracking
    asyncio.create_task(init_http_server())
    log.info(f"[HTTP Server] Started on port {HTTP_SERVER_PORT}")
    
    # Initialize Whop API client if key provided
    if WHOP_API_KEY and WhopAPIClient:
        try:
            if not is_placeholder_secret(WHOP_API_KEY):
                base_url = WHOP_API_CONFIG.get("base_url", "https://api.whop.com/api/v1")
                company_id = WHOP_API_CONFIG.get("company_id", "")
                whop_api_client = WhopAPIClient(WHOP_API_KEY, base_url, company_id)
                log.info("[Whop API] Client initialized")
            else:
                whop_api_client = None
                log.info("[Whop API] Client disabled (placeholder key)")
        except Exception as e:
            whop_api_client = None
            log.warning(f"[Whop API] Failed to initialize: {e}")
    else:
        whop_api_client = None
        if not WhopAPIClient:
            log.info("[Whop API] Client disabled (module not available)")
        else:
            log.info("[Whop API] Client disabled (no API key)")
    
    # Initialize Whop webhook handler
    if WHOP_WEBHOOK_CHANNEL_ID or WHOP_LOGS_CHANNEL_ID:
        init_whop_handler(
            webhook_channel_id=WHOP_WEBHOOK_CHANNEL_ID,
            whop_logs_channel_id=WHOP_LOGS_CHANNEL_ID,
            role_trigger=ROLE_TRIGGER,
            welcome_role_id=WELCOME_ROLE_ID,
            role_cancel_a=ROLE_CANCEL_A,
            role_cancel_b=ROLE_CANCEL_B,
            log_other_func=log_other,
            log_member_status_func=log_member_status,
            fmt_user_func=_fmt_user,
            member_status_logs_channel_id=MEMBER_STATUS_LOGS_CHANNEL_ID,
            get_member_history_func=get_member_history,
            whop_api_key=WHOP_API_KEY,
            whop_api_config=WHOP_API_CONFIG,
            dispute_channel_id=WHOP_DISPUTE_CHANNEL_ID,
            resolution_channel_id=WHOP_RESOLUTION_CHANNEL_ID,
            support_ping_role_id=WHOP_SUPPORT_PING_ROLE_ID,
            support_ping_role_name=WHOP_SUPPORT_PING_ROLE_NAME,
        )
        channels = []
        if WHOP_WEBHOOK_CHANNEL_ID:
            channels.append(f"webhook channel {WHOP_WEBHOOK_CHANNEL_ID}")
        if WHOP_LOGS_CHANNEL_ID:
            channels.append(f"logs channel {WHOP_LOGS_CHANNEL_ID}")
        log.info(f"[Whop] Webhook handler initialized for {', '.join(channels)}")
    else:
        log.warning("[Whop] Webhook/logs channel IDs not configured - webhook handler disabled")
    
    # Schedule periodic cleanup (every 24 hours)
    @tasks.loop(hours=24)
    async def periodic_cleanup():
        cleanup_old_data()
        cleanup_old_invites()
    
    periodic_cleanup.start()
    log.info("[Cleanup] Periodic cleanup scheduled (every 24 hours)")
    
    # Start Whop membership sync job if enabled
    if whop_api_client and WHOP_API_CONFIG.get("enable_sync", True):
        sync_interval = WHOP_API_CONFIG.get("sync_interval_hours", 6)
        if not sync_whop_memberships.is_running():
            sync_whop_memberships.change_interval(hours=sync_interval)
            sync_whop_memberships.start()
            log.info(f"[Whop Sync] Membership sync job started (every {sync_interval} hours)")


async def _ensure_whop_resolution_channels(guild: discord.Guild) -> None:
    """Ensure dispute/resolution channels exist under configured category (best-effort)."""
    global WHOP_DISPUTE_CHANNEL_ID, WHOP_RESOLUTION_CHANNEL_ID

    if not WHOP_API_CONFIG.get("enable_resolution_reporting", True):
        return
    if not guild:
        return
    if not WHOP_RESOLUTION_CATEGORY_ID:
        return
    try:
        category_id_int = int(str(WHOP_RESOLUTION_CATEGORY_ID).strip())
    except Exception:
        return

    category = guild.get_channel(category_id_int)
    if not isinstance(category, discord.CategoryChannel):
        log.warning(f"[Whop Resolution] Category not found (ID: {category_id_int})")
        return

    async def ensure_channel(name: str) -> discord.TextChannel | None:
        if not name:
            return None
        # Look for existing channel with same name under category
        for ch in category.channels:
            if isinstance(ch, discord.TextChannel) and ch.name == name:
                return ch
        # Create it
        try:
            return await guild.create_text_channel(
                name=name,
                category=category,
                reason="RSCheckerbot: Whop resolution/dispute reporting channel",
            )
        except Exception as e:
            log.warning(f"[Whop Resolution] Failed to create channel '{name}': {e}")
            return None

    dispute_ch = await ensure_channel(str(WHOP_DISPUTE_CHANNEL_NAME).strip())
    resolution_ch = await ensure_channel(str(WHOP_RESOLUTION_CHANNEL_NAME).strip())

    WHOP_DISPUTE_CHANNEL_ID = dispute_ch.id if dispute_ch else None
    WHOP_RESOLUTION_CHANNEL_ID = resolution_ch.id if resolution_ch else None

    if dispute_ch:
        log.info(f"[Whop Resolution] Dispute channel ready: {dispute_ch.name} (ID: {dispute_ch.id})")
    if resolution_ch:
        log.info(f"[Whop Resolution] Resolution channel ready: {resolution_ch.name} (ID: {resolution_ch.id})")
    else:
        if not whop_api_client:
            log.info("[Whop Sync] Sync job disabled (no API client)")
        else:
            log.info("[Whop Sync] Sync job disabled (enable_sync=false)")
    
    log.info("="*60)
    log.info("")

@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id == GUILD_ID and not member.bot:
        # Track join event FIRST (before other logic)
        rec = _touch_join(member.id)

        # Determine join method via invite usage deltas (best-effort).
        used_invite_code = None
        used_invite_inviter = None
        used_invite_inviter_name = None
        used_invite_inviter_id = None
        join_method_lines: list[str] = []
        try:
            invites = await member.guild.invites()
            for invite in invites:
                previous_uses = invite_usage_cache.get(invite.code, 0)
                if invite.uses > previous_uses and used_invite_code is None:
                    used_invite_code = invite.code
                    used_invite_inviter = invite.inviter
                    used_invite_inviter_name = invite.inviter.name if invite.inviter else None
                    used_invite_inviter_id = invite.inviter.id if invite.inviter else None
                # Keep cache updated
                invite_usage_cache[invite.code] = invite.uses
        except Exception as e:
            join_method_lines.append(f"• Invite tracking: error ({str(e)[:120]})")

        if used_invite_code:
            join_method_lines.append(f"• Invite code: `{used_invite_code}`")
            if used_invite_inviter_name and used_invite_inviter_id:
                join_method_lines.append(f"• Invited by: {used_invite_inviter_name} (`{used_invite_inviter_id}`)")
            invite_entry = invites_data.get(used_invite_code) or {}
            is_tracked = bool(invite_entry) and invite_entry.get("used_at") is None
            join_method_lines.append(f"• Tracked invite: {'yes' if is_tracked else 'no'}")
            if is_tracked:
                join_method_lines.append("• Source: One-time invite")
                lead_id = invite_entry.get("lead_id") or ""
                if lead_id:
                    join_method_lines.append(f"• Lead ID: `{lead_id}`")
            else:
                join_method_lines.append("• Source: Untracked/permanent or external")
        else:
            # Pure output (no explanations)
            if not join_method_lines:
                join_method_lines.append("• Invite code: —")
                join_method_lines.append("• Invited by: —")
                join_method_lines.append("• Tracked invite: —")
                join_method_lines.append("• Source: —")
        
        # Optional: Log join to member-status-logs
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if ch:
                inviter_s = "—"
                if used_invite_inviter_name and used_invite_inviter_id:
                    inviter_s = f"{used_invite_inviter_name} ({used_invite_inviter_id})"
                tracked_s = "—"
                source_s = "—"
                if used_invite_code:
                    invite_entry = invites_data.get(used_invite_code) or {}
                    is_tracked = bool(invite_entry) and invite_entry.get("used_at") is None
                    tracked_s = "yes" if is_tracked else "no"
                    source_s = "one_time_invite" if is_tracked else "untracked_or_external"
                access = _access_roles_plain(member)

                # Detailed card in member-status-logs
                mid = get_cached_whop_membership_id(member.id)
                whop_brief = await _fetch_whop_brief_by_membership_id(mid) if mid else {}
                detailed = _build_member_status_detailed_embed(
                    title="👋 Member Joined",
                    member=member,
                    access_roles=access,
                    color=0x57F287,
                    member_kv=[
                        ("account_created", member.created_at.strftime("%b %d, %Y")),
                        ("first_joined", _fmt_ts(rec.get("first_join_ts"), "D")),
                        ("join_count", rec.get("join_count", 1)),
                        ("returning_member", "true" if rec.get("join_count", 0) > 1 else "false"),
                    ],
                    discord_kv=[
                        ("invite_code", used_invite_code or "—"),
                        ("invited_by", inviter_s),
                        ("tracked_invite", tracked_s),
                        ("source", source_s),
                    ],
                    whop_brief=whop_brief,
                )
                await log_member_status("", embed=detailed)
        
        guild = member.guild
        current_roles = {r.id for r in member.roles}
        current_role_names = _fmt_role_list(current_roles, guild) if current_roles else "none"
        
        # Check if they already have Welcome role
        has_welcome = WELCOME_ROLE_ID and WELCOME_ROLE_ID in current_roles
        welcome_status = f"✅ Has {_fmt_role(WELCOME_ROLE_ID, guild)}" if has_welcome else f"❌ Missing {_fmt_role(WELCOME_ROLE_ID, guild)}"
        
        # Check which checked roles they have vs missing
        user_has_checked = [r.id for r in member.roles if r.id in ROLES_TO_CHECK]
        user_missing_checked = [rid for rid in ROLES_TO_CHECK if rid not in current_roles]
        has_any_checked = len(user_has_checked) > 0
        
        checked_status = ""
        if has_any_checked:
            checked_status = f"   ✅ Has checked roles: {_fmt_role_list(set(user_has_checked), guild)}\n"
        else:
            checked_status = f"   ❌ Has NO checked roles\n"
        
        # Avoid spam: only print the full checked role list when verbose logging is enabled.
        all_checked_roles = _fmt_role_list(ROLES_TO_CHECK, guild) if VERBOSE_ROLE_LISTS else ""
        sample_checked = _fmt_role_list(set(list(ROLES_TO_CHECK)[:3]), guild) if ROLES_TO_CHECK else ""
        
        await log_role_event(
            f"👤 **New Member Joined:** {_fmt_user(member)}\n"
            f"   {welcome_status}\n"
            f"   📋 Current roles: {current_role_names}\n"
            f"   {checked_status}"
            f"   🔍 **Basis for 60s check:** Will verify user has ANY checked role ({len(ROLES_TO_CHECK)} total)"
            f"{f': {all_checked_roles}' if all_checked_roles else ''}\n"
            f"{f'   🧪 Sample checked roles: {sample_checked}' if (not all_checked_roles and sample_checked) else ''}\n"
            f"   ⏱️ If user has NONE after 60s → will assign trigger role"
        )
        asyncio.create_task(check_and_assign_role(member))

        # If we detected a tracked invite, mark it used (non-destructive; persists metadata for audit).
        try:
            if used_invite_code:
                invite_entry = invites_data.get(used_invite_code)
                if invite_entry and invite_entry.get("used_at") is None:
                    await track_invite_usage(used_invite_code, member)
        except Exception as e:
            log.error(f"❌ Error updating tracked invite usage for {member} ({member.id}): {e}")

@bot.event
async def on_member_remove(member: discord.Member):
    """Track member leave events and log to member-status-logs"""
    if member.guild.id == GUILD_ID and not member.bot:
        rec = _touch_leave(member.id)
        
        # Log to member-status-logs channel
        if MEMBER_STATUS_LOGS_CHANNEL_ID:
            ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
            if ch:
                access = _access_roles_plain(member)

                mid = get_cached_whop_membership_id(member.id)
                whop_brief = await _fetch_whop_brief_by_membership_id(mid) if mid else {}
                detailed = _build_member_status_detailed_embed(
                    title="🚪 Member Left",
                    member=member,
                    access_roles=access,
                    color=0xFAA61A,
                    member_kv=[
                        ("left_at", _fmt_ts(rec.get("last_leave_ts"), "D") if rec.get("last_leave_ts") else "—"),
                        ("first_joined", _fmt_ts(rec.get("first_join_ts"), "D") if rec.get("first_join_ts") else "—"),
                        ("join_count", rec.get("join_count") or "—"),
                    ],
                    discord_kv=[
                        ("access_roles_at_leave", access),
                    ],
                    whop_brief=whop_brief,
                )
                await log_member_status("", embed=detailed)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    before_roles = {r.id for r in before.roles}
    after_roles = {r.id for r in after.roles}
    
    # Detect all role changes (added and removed)
    roles_added = after_roles - before_roles
    roles_removed = before_roles - after_roles
    
    # Log general role changes if any occurred (but skip if we'll log them specifically below)
    if roles_added or roles_removed:
        # Skip logging here if it's a specific case we handle below (we'll log it there with more detail)
        is_specific_case = (
            (ROLE_CANCEL_A in after_roles or ROLE_CANCEL_B in after_roles) and str(after.id) in queue_state or
            (ROLE_TRIGGER not in before_roles and ROLE_TRIGGER in after_roles) or
            (any(r.id in ROLES_TO_CHECK for r in before.roles) and not any(r.id in ROLES_TO_CHECK for r in after.roles)) or
            ((ROLE_CANCEL_A in before_roles) and (ROLE_CANCEL_A not in after_roles)) or
            ((ROLE_CANCEL_A not in before_roles) and (ROLE_CANCEL_A in after_roles))
        )
        
        if not is_specific_case:
            # General role change - batch to reduce spam (rapid sequences often produce multiple events)
            uid = int(after.id)
            rec = pending_role_updates.get(uid)
            if not rec:
                rec = {"added": set(), "removed": set(), "member": after, "task": None}
                pending_role_updates[uid] = rec
            try:
                rec["added"].update(set(roles_added))
                rec["removed"].update(set(roles_removed))
                # Cancel out roles that were both added and removed within the batch window
                both = rec["added"].intersection(rec["removed"])
                if both:
                    rec["added"].difference_update(both)
                    rec["removed"].difference_update(both)
            except Exception:
                pass
            if not rec.get("task"):
                rec["task"] = asyncio.create_task(_flush_role_update(uid))

    if (ROLE_CANCEL_A in after_roles or ROLE_CANCEL_B in after_roles) and str(after.id) in queue_state:
        cancel_roles = []
        if ROLE_CANCEL_A in after_roles:
            cancel_roles.append(_fmt_role(ROLE_CANCEL_A, after.guild))
        if ROLE_CANCEL_B in after_roles:
            cancel_roles.append(_fmt_role(ROLE_CANCEL_B, after.guild))
        cancel_info = ", ".join(cancel_roles) if cancel_roles else "cancel role"
        
        # Check if Member role was just added (user completed onboarding/payment)
        member_role_just_added = ROLE_CANCEL_A not in before_roles and ROLE_CANCEL_A in after_roles
        if member_role_just_added:
            await log_member_status(
                f"✅ **Payment/Onboarding Complete**\n"
                f"**User:** {_fmt_user(after)}\n"
                f"**Roles Added:** {cancel_info}\n"
                f"**Action:** DM sequence cancelled — user now has full access"
            )
        
        mark_cancelled(after.id, "cancel_role_added")
        await log_other(f"🛑 Cancelled for {_fmt_user(after)} — {cancel_info} was added (role update)")
        return

    if ROLE_TRIGGER not in before_roles and ROLE_TRIGGER in after_roles:
        guild = after.guild
        trigger_role_name = _fmt_role(ROLE_TRIGGER, guild)
        
        if has_sequence_before(after.id):
            await log_other(f"⏭️ Skipped DM sequence for {_fmt_user(after)} — sequence previously run")
            return
        
        enqueue_first_day(after.id)
        
        # Simple logging format with clear trigger indication
        await log_first(f"🧵 Enqueued **day_1** for {_fmt_user(after)} (trigger role added)")
        
        return

    had_checked = any(r.id in ROLES_TO_CHECK for r in before.roles)
    has_checked_now = any(r.id in ROLES_TO_CHECK for r in after.roles)
    if had_checked and not has_checked_now:
        # Get which checked roles were lost
        before_checked = [r.id for r in before.roles if r.id in ROLES_TO_CHECK]
        after_checked = [r.id for r in after.roles if r.id in ROLES_TO_CHECK]
        lost_checked = [rid for rid in before_checked if rid not in after_checked]
        
        # Show all role changes in this update
        all_removed_in_update = before_roles - after_roles
        all_added_in_update = after_roles - before_roles
        all_removed_names = _fmt_role_list(all_removed_in_update, after.guild)
        all_added_names = _fmt_role_list(all_added_in_update, after.guild) if all_added_in_update else None
        
        lost_checked_names = _fmt_role_list(set(lost_checked), after.guild)
        all_checked_roles = _fmt_role_list(ROLES_TO_CHECK, after.guild)
        
        log_msg = (
            f"🔄 **Member lost all checked roles:** {_fmt_user(after)}\n"
            f"   ➖ **Roles removed:** {all_removed_names}\n"
        )
        if all_added_names:
            log_msg += f"   ➕ **Roles added:** {all_added_names}\n"
        log_msg += (
            f"   ❌ **Lost checked roles:** {lost_checked_names}\n"
            f"   🔍 **Basis for trigger:** User had checked roles, now has NONE\n"
            f"   📋 All checked roles ({len(ROLES_TO_CHECK)}): {all_checked_roles}\n"
            f"   ⏱️ Will check and assign trigger role in 60s if still needed"
        )
        await log_role_event(log_msg)
        asyncio.create_task(check_and_assign_role(after))

    if (ROLE_CANCEL_A in before_roles) and (ROLE_CANCEL_A not in after_roles):
        # Show all roles removed in this update (not just Member role)
        all_removed_in_update = before_roles - after_roles
        all_added_in_update = after_roles - before_roles
        removed_names = _fmt_role_list(all_removed_in_update, after.guild)
        added_names = _fmt_role_list(all_added_in_update, after.guild) if all_added_in_update else None
        
        # Check if this looks like a subscription/payment cancellation
        # If Member role was the ONLY role removed (or only with Welcome), likely payment-related
        only_member_removed = len(all_removed_in_update) == 1 or (len(all_removed_in_update) == 2 and ROLE_CANCEL_B in all_removed_in_update)
        if only_member_removed:
            # Determine whether this is a payment failure (past_due/unpaid) vs cancellation.
            whop_mid = get_cached_whop_membership_id(after.id)
            whop_brief = await _fetch_whop_brief_by_membership_id(whop_mid) if whop_mid else {}
            whop_status = str((whop_brief.get("status") or "")).strip().lower()
            is_payment_failed = whop_status in ("past_due", "unpaid") or bool(whop_brief.get("last_payment_failure"))
            access = _access_roles_plain(after)
            db_alerts = load_staff_alerts(STAFF_ALERTS_FILE)
            issue_key = f"payment_failed:{whop_status}" if is_payment_failed else "payment_cancellation_detected"
            if should_post_alert(db_alerts, after.id, issue_key, cooldown_hours=6.0):
                dest = PAYMENT_FAILURE_CHANNEL_NAME if is_payment_failed else MEMBER_CANCELLATION_CHANNEL_NAME
                title = "💳 Payment Failed — Action Needed" if is_payment_failed else "💳 Payment Cancellation Detected"
                color = 0xED4245 if is_payment_failed else 0xFF0000

                # Detailed card -> member-status-logs
                hist = get_member_history(after.id) or {}
                detailed = _build_member_status_detailed_embed(
                    title=title,
                    member=after,
                    access_roles=access,
                    color=color,
                    member_kv=[
                        ("account_created", after.created_at.strftime("%b %d, %Y")),
                        ("first_joined", _fmt_ts(hist.get("first_join_ts"), "D") if hist.get("first_join_ts") else "—"),
                        ("join_count", hist.get("join_count") or "—"),
                    ],
                    discord_kv=[
                        ("roles_removed", removed_names),
                        ("reason", "member_role_removed"),
                    ],
                    whop_brief=whop_brief,
                )
                await log_member_status("", embed=detailed)

                # Minimal card -> case channel
                minimal = _build_case_minimal_embed(
                    title=title,
                    member=after,
                    access_roles=access,
                    whop_brief=whop_brief,
                    color=color,
                )
                await log_member_status("", embed=minimal, channel_name=dest)

                record_alert_post(db_alerts, after.id, issue_key)
                save_staff_alerts(STAFF_ALERTS_FILE, db_alerts)
        
        log_msg = (
            f"📉 **Member Role Removed:** {_fmt_user(after)}\n"
            f"   ➖ **Roles removed:** {removed_names}\n"
        )
        if added_names:
            log_msg += f"   ➕ **Roles added:** {added_names}\n"
        log_msg += (
            f"   ⚠️ **Key removal:** {_fmt_role(ROLE_CANCEL_A, after.guild)}\n"
            f"   ⏱️ Will mark as 'Former Member' in {FORMER_MEMBER_DELAY_SECONDS}s if not regained"
        )
        await log_role_event(log_msg)
        
        # If Member role was removed and user has active DM sequence, cancel it
        if str(after.id) in queue_state:
            mark_cancelled(after.id, "member_role_removed_payment")
            await log_other(f"🛑 Cancelled DM sequence for {_fmt_user(after)} — Member role removed (likely payment cancellation)")
        
        asyncio.create_task(delayed_assign_former_member(after))

    if (ROLE_CANCEL_A not in before_roles) and (ROLE_CANCEL_A in after_roles):
        # Show all role changes when Member role is regained
        all_removed_in_update = before_roles - after_roles
        all_added_in_update = after_roles - before_roles
        removed_names = _fmt_role_list(all_removed_in_update, after.guild) if all_removed_in_update else None
        added_names = _fmt_role_list(all_added_in_update, after.guild)
        
        # Check if this looks like payment reactivation (Member role regained)
        # If Member role was the ONLY role added, likely payment reactivation
        only_member_added = len(all_added_in_update) == 1
        if only_member_added:
            access = _access_roles_plain(after)
            db_alerts = load_staff_alerts(STAFF_ALERTS_FILE)
            issue_key = "payment_reactivated"
            if should_post_alert(db_alerts, after.id, issue_key, cooldown_hours=2.0):
                whop_mid = get_cached_whop_membership_id(after.id)
                whop_brief = await _fetch_whop_brief_by_membership_id(whop_mid) if whop_mid else {}
                hist = get_member_history(after.id) or {}

                # Detailed card -> member-status-logs
                detailed = _build_member_status_detailed_embed(
                    title="✅ Payment Reactivated",
                    member=after,
                    access_roles=access,
                    color=0x57F287,
                    member_kv=[
                        ("first_joined", _fmt_ts(hist.get("first_join_ts"), "D") if hist.get("first_join_ts") else "—"),
                        ("join_count", hist.get("join_count") or "—"),
                    ],
                    discord_kv=[
                        ("roles_added", added_names),
                        ("roles_removed", removed_names or "—"),
                        ("reason", "member_role_regained"),
                    ],
                    whop_brief=whop_brief,
                )
                await log_member_status("", embed=detailed)

                # Minimal card -> payment-failure channel
                minimal = _build_case_minimal_embed(
                    title="✅ Payment Reactivated",
                    member=after,
                    access_roles=access,
                    whop_brief=whop_brief,
                    color=0x57F287,
                )
                await log_member_status("", embed=minimal, channel_name=PAYMENT_FAILURE_CHANNEL_NAME)

                record_alert_post(db_alerts, after.id, issue_key)
                save_staff_alerts(STAFF_ALERTS_FILE, db_alerts)
        
        if has_former_member_role(after):
            role = after.guild.get_role(FORMER_MEMBER_ROLE)
            if role:
                with suppress(Exception):
                    await after.remove_roles(role, reason="Regained member role; remove former-member marker")
                    log_msg = (
                        f"🧹 **Former Member Role Removed:** {_fmt_user(after)}\n"
                        f"   ➕ **Roles added:** {added_names}\n"
                    )
                    if removed_names:
                        log_msg += f"   ➖ **Roles removed:** {removed_names}\n"
                    log_msg += f"   ✅ **Reason:** Regained {_fmt_role(ROLE_CANCEL_A, after.guild)} → removed Former Member"
                    await log_role_event(log_msg)

@bot.event
async def on_message(message: discord.Message):
    """
    Handle incoming messages.
    Processes Whop webhook messages from configured channels (workflow webhooks and native integration).
    """
    # Process commands first (canonical pattern)
    await bot.process_commands(message)
    
    # Check if this is a Whop message (from either channel)
    if message.author.bot:
        # Check workflow webhook channel
        if (WHOP_WEBHOOK_CHANNEL_ID and message.channel.id == WHOP_WEBHOOK_CHANNEL_ID):
            await handle_whop_webhook_message(message)
            return
        
        # Check native Whop logs channel
        if (WHOP_LOGS_CHANNEL_ID and message.channel.id == WHOP_LOGS_CHANNEL_ID):
            # Channel ID is the source of truth (do not gate on bot/app name).
            await handle_whop_webhook_message(message)
            return
    
    # Message processing continues here if needed for other handlers
    # (Currently no other message handlers, but this preserves extensibility)

# -----------------------------
# Data Cleanup Functions
# -----------------------------
def cleanup_old_data():
    """Clean up old completed entries from queue.json and registry.json"""
    global queue_state, registry
    
    # Clean queue: remove entries older than 30 days that are completed
    cleaned_queue = {}
    cutoff_date = _now() - timedelta(days=30)
    
    for uid, payload in queue_state.items():
        try:
            next_send = payload.get("next_send", "")
            if next_send:
                next_dt = datetime.fromisoformat(next_send.replace("Z", "+00:00"))
                # Keep if not old or if still active
                if next_dt > cutoff_date:
                    cleaned_queue[uid] = payload
        except Exception:
            # Keep entry if we can't parse date
            cleaned_queue[uid] = payload
    
    removed_queue = len(queue_state) - len(cleaned_queue)
    queue_state = cleaned_queue
    
    # Clean registry: remove completed entries older than 90 days
    cleaned_registry = {}
    registry_cutoff = _now() - timedelta(days=90)
    
    for uid, data in registry.items():
        try:
            started_at = data.get("started_at", "")
            if started_at:
                started_dt = datetime.fromisoformat(started_at.replace("+00:00", ""))
                completed = data.get("completed", False)
                # Keep if recent OR if not completed
                if not completed or started_dt > registry_cutoff:
                    cleaned_registry[uid] = data
        except Exception:
            # Keep entry if we can't parse date
            cleaned_registry[uid] = data
    
    removed_registry = len(registry) - len(cleaned_registry)
    registry = cleaned_registry
    
    if removed_queue > 0 or removed_registry > 0:
        save_all()
        log.info(f"Cleaned up {removed_queue} old queue entries and {removed_registry} old registry entries")

def cleanup_old_invites():
    """Clean up old invite JSON entries (older than 90 days and used)"""
    try:
        global invites_data
        cutoff_date = (_now() - timedelta(days=90)).isoformat()
        
        # Remove old used invites
        removed = 0
        invites_to_remove = []
        
        for invite_code, invite_entry in invites_data.items():
            used_at = invite_entry.get("used_at")
            if used_at and used_at < cutoff_date:
                invites_to_remove.append(invite_code)
        
        for invite_code in invites_to_remove:
            del invites_data[invite_code]
            removed += 1
        
        if removed > 0:
            save_invites(invites_data)
            log.info(f"Cleaned up {removed} old invite entries")
    except Exception as e:
        log.error(f"Error cleaning invites: {e}")

# -----------------------------
# Admin commands
# -----------------------------
@bot.command(name="editmessages", aliases=["checker-edit", "cedit", "checker-messages"])
@commands.has_permissions(administrator=True)
async def edit_messages(ctx):
    """Edit DM messages via embedded interface"""
    try:
        # Update messages_data before loading editor
        bot_instance.messages = messages_data
        from message_editor import MessageEditorView
        view = MessageEditorView(bot_instance)
        embed = view.get_main_embed()
        await ctx.send(embed=embed, view=view)
        try:
            await ctx.message.delete()
        except Exception:
            pass
    except ImportError as e:
        await ctx.send(f"❌ Failed to import message editor: {e}", delete_after=10)
    except Exception as e:
        await ctx.send(f"❌ Error: {e}", delete_after=10)

@bot.command(name="reloadmessages", aliases=["checker-reload", "creload"])
@commands.has_permissions(administrator=True)
async def reload_messages(ctx):
    """Reload messages from JSON file"""
    global messages_data
    try:
        messages_data = load_messages()
        bot_instance.messages = messages_data
        await ctx.send("✅ Messages reloaded from messages.json!", delete_after=5)
    except Exception as e:
        await ctx.send(f"❌ Failed to reload messages.json: {e}", delete_after=15)
    try:
        await ctx.message.delete()
    except Exception:
        pass

@bot.command(name="cleanup")
@commands.has_permissions(administrator=True)
async def cleanup_data(ctx):
    """Manually trigger data cleanup"""
    cleanup_old_data()
    cleanup_old_invites()
    await ctx.send("✅ Data cleanup completed!", delete_after=5)
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name="purgecases", aliases=["purgecasechannels", "deletecases", "deletecasechannels"])
@commands.has_permissions(administrator=True)
async def purge_case_channels(ctx, confirm: str = ""):
    """Delete legacy per-user payment case channels under the configured category.

    Usage:
      .checker purgecases confirm
    """
    LEGACY_CATEGORY_ID = 1458533733681598654
    if str(confirm or "").strip().lower() != "confirm":
        await ctx.send("❌ Confirmation required. Use: `.checker purgecases confirm`", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        await ctx.send("❌ Guild not found / bot not ready.", delete_after=10)
        with suppress(Exception):
            await ctx.message.delete()
        return

    category = guild.get_channel(LEGACY_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        await ctx.send("❌ Legacy case category not found or not a category.", delete_after=12)
        with suppress(Exception):
            await ctx.message.delete()
        return

    deleted = 0
    skipped = 0
    failed = 0

    for ch in list(category.channels):
        if not isinstance(ch, discord.TextChannel):
            skipped += 1
            continue
        nm = str(ch.name or "")
        topic = str(ch.topic or "")
        # Category is dedicated to legacy case channels; be permissive in matching.
        is_legacy = ("rschecker_payment_case" in topic) or nm.startswith("pay-")
        if not is_legacy:
            skipped += 1
            continue
        try:
            await ch.delete(reason="RSCheckerbot: purge legacy payment case channels")
            deleted += 1
        except Exception:
            failed += 1

    await ctx.send(f"✅ purgecases complete — deleted: {deleted}, skipped: {skipped}, failed: {failed}", delete_after=20)
    with suppress(Exception):
        await ctx.message.delete()

@bot.command(name="dmenable")
@commands.has_permissions(administrator=True)
async def dm_enable(ctx):
    """Enable DM sequence"""
    settings = load_settings()
    settings["dm_sequence_enabled"] = True
    save_settings(settings)
    await ctx.send("✅ DM sequence enabled", delete_after=5)
    try:
        await ctx.message.delete()
    except Exception:
        pass

@bot.command(name="dmdisable")
@commands.has_permissions(administrator=True)
async def dm_disable(ctx):
    """Disable DM sequence"""
    settings = load_settings()
    settings["dm_sequence_enabled"] = False
    save_settings(settings)
    await ctx.send("⛔ DM sequence disabled", delete_after=5)
    try:
        await ctx.message.delete()
    except Exception:
        pass

@bot.command(name="dmstatus")
@commands.has_permissions(administrator=True)
async def dm_status(ctx):
    """Show DM sequence status"""
    settings = load_settings()
    status = "ENABLED" if settings.get("dm_sequence_enabled", True) else "DISABLED"
    emoji = "✅" if settings.get("dm_sequence_enabled", True) else "⛔"
    await ctx.send(f"{emoji} DM sequence: **{status}**", delete_after=10)
    try:
        await ctx.message.delete()
    except Exception:
        pass


@bot.command(name="whois", aliases=["whof"])
@commands.has_permissions(administrator=True)
async def whois_member(ctx, member: discord.Member):
    """Whop API-first lookup for a Discord user.

    Usage:
      .checker whois @user
    """
    try:
        embed = discord.Embed(
            title="🔎 Whop Lookup",
            description=f"Lookup for {member.mention}",
            color=0x2B2D31,
            timestamp=datetime.now(timezone.utc),
        )
        _apply_member_header(embed, member)
        access = _access_roles_plain(member)
        embed.add_field(name="Member", value=member.mention, inline=True)
        embed.add_field(name="Access", value=access or "—", inline=True)

        mid = get_cached_whop_membership_id(member.id)
        brief = await _fetch_whop_brief_by_membership_id(mid) if mid else {}
        lines: list[str] = []
        if brief:
            if brief.get("product") and brief["product"] != "—":
                lines.append(str(brief["product"]))
            if brief.get("status") and brief["status"] != "—":
                lines.append(f"Status {brief['status']}")
            if brief.get("member_since") and brief["member_since"] != "—":
                lines.append(f"Member since {brief['member_since']}")
            if brief.get("renewal_start") != "—" and brief.get("renewal_end") != "—":
                lines.append(f"Renewal {brief['renewal_start']} to {brief['renewal_end']}")
            if brief.get("cancel_at_period_end") == "yes":
                lines.append("Cancels at period end")
            if brief.get("last_payment_method") and brief["last_payment_method"] != "—":
                lines.append(f"Method {brief['last_payment_method']}")
            if brief.get("last_payment_type") and brief["last_payment_type"] != "—":
                lines.append(f"Type {brief['last_payment_type']}")
            if brief.get("last_payment_failure"):
                lines.append(f"Last failure: {brief['last_payment_failure']}")
        else:
            lines.append("No Whop membership link found for this Discord user.")
        summary = "\n".join([str(x) for x in lines if str(x).strip()])[:1024] or "—"
        embed.add_field(name="Summary", value=summary, inline=False)

        await ctx.send(embed=embed, delete_after=30)
    except Exception as e:
        await ctx.send(f"❌ whois error: {e}", delete_after=10)
    finally:
        with suppress(Exception):
            await ctx.message.delete()


@bot.command(name="whopmembership", aliases=["whopmember", "whopmem"])
@commands.has_permissions(administrator=True)
async def whop_membership_lookup(ctx, membership_id: str):
    """Direct Whop membership lookup by membership_id.

    Usage:
      .checker whopmembership mem_...
    """
    global whop_api_client
    if not whop_api_client:
        await ctx.send("❌ Whop API client is not initialized.", delete_after=10)
        return

    mid = (membership_id or "").strip()
    if not mid:
        await ctx.send("❌ Provide a membership_id.", delete_after=10)
        return

    brief = await _fetch_whop_brief_by_membership_id(mid)
    if not brief:
        await ctx.send("❌ Whop lookup failed (no data returned).", delete_after=15)
        with suppress(Exception):
            await ctx.message.delete()
        return

    embed = discord.Embed(
        title="🔎 Whop Membership",
        description="Membership lookup complete.",
        color=0x2B2D31,
        timestamp=datetime.now(timezone.utc),
    )
    lines: list[str] = []
    if brief.get("product") and brief["product"] != "—":
        lines.append(str(brief["product"]))
    if brief.get("status") and brief["status"] != "—":
        lines.append(f"Status {brief['status']}")
    if brief.get("member_since") and brief["member_since"] != "—":
        lines.append(f"Member since {brief['member_since']}")
    if brief.get("renewal_start") != "—" and brief.get("renewal_end") != "—":
        lines.append(f"Renewal {brief['renewal_start']} to {brief['renewal_end']}")
    if brief.get("cancel_at_period_end") == "yes":
        lines.append("Cancels at period end")
    if brief.get("last_payment_method") and brief["last_payment_method"] != "—":
        lines.append(f"Method {brief['last_payment_method']}")
    if brief.get("last_payment_type") and brief["last_payment_type"] != "—":
        lines.append(f"Type {brief['last_payment_type']}")
    if brief.get("last_payment_failure"):
        lines.append(f"Last failure: {brief['last_payment_failure']}")

    summary = "\n".join([str(x) for x in lines if str(x).strip()])[:1024] or "—"
    embed.add_field(name="Summary", value=summary, inline=False)
    await ctx.send(embed=embed, delete_after=30)
    with suppress(Exception):
        await ctx.message.delete()

@bot.command(name="start")
@commands.has_permissions(administrator=True)
async def start_sequence(ctx, member: discord.Member):
    if not has_trigger_role(member):
        await ctx.reply("❗ User does not have the trigger role; sequence only starts after that role is added.")
        return
    if has_sequence_before(member.id):
        await ctx.reply("User already had sequence before; not starting again.")
        return
    enqueue_first_day(member.id)
    await ctx.reply(f"Queued day_1 for {m_user(member)} now.")
    await log_first(f"🧵 (Admin) Enqueued **day_1** for {_fmt_user(member)}")

@bot.command(name="cancel")
@commands.has_permissions(administrator=True)
async def cancel_sequence(ctx, member: discord.Member):
    if str(member.id) not in queue_state:
        await ctx.reply("User not in active queue.")
        return
    mark_cancelled(member.id, "admin_cancel")
    await ctx.reply(f"Cancelled sequence for {m_user(member)}.")
    await log_other(f"🛑 (Admin) Cancelled sequence for {_fmt_user(member)}")

@bot.command(name="test")
@commands.has_permissions(administrator=True)
async def test_sequence(ctx, member: discord.Member):
    await ctx.reply(f"Starting test sequence for {m_user(member)}...")
    for day_key in DAY_KEYS:
        try:
            join_url = UTM_LINKS[day_key]

            from view import get_dm_view
            day_data = (messages_data.get("days") or {}).get(day_key) or {}

            banner_url = day_data.get("banner_url") or messages_data.get("banner_url")
            footer_url = day_data.get("footer_url") or messages_data.get("footer_url")
            main_image_url = day_data.get("main_image_url")
            description = str(day_data.get("description", "")).format(join_url=join_url)

            banner_embed = discord.Embed()
            if banner_url:
                banner_embed.set_image(url=banner_url)

            content_embed = discord.Embed(description=description)
            if main_image_url:
                content_embed.set_image(url=main_image_url)
            elif footer_url:
                content_embed.set_image(url=footer_url)

            view = get_dm_view(day_number=day_key, join_url=join_url)
            embeds = [banner_embed, content_embed]
            await member.send(embeds=embeds, view=view)
            log.info(f"[TEST] Sent {day_key} to {member} ({member.id})")
            if day_key == "day_1":
                await log_first(f"🧪 TEST sent **{day_key}** to {_fmt_user(member)}")
            else:
                await log_other(f"🧪 TEST sent **{day_key}** to {_fmt_user(member)}")
        except Exception as e:
            await log_other(f"🧪❌ TEST failed `{day_key}` for {_fmt_user(member)}: `{e}`")
        await asyncio.sleep(TEST_INTERVAL_SECONDS)
    await ctx.send(f"✅ Test sequence complete for {m_user(member)}.")

@bot.command(name="relocate")
@commands.has_permissions(administrator=True)
async def relocate_sequence(ctx, member: discord.Member, day: str):
    d = day.strip().lower()
    if d.isdigit():
        idx = int(d) - 1
        day_key = DAY_KEYS[idx] if 0 <= idx < len(DAY_KEYS) else None
    elif d in {"7a", "7b"}:
        day_key = f"day_{d}"
    elif d.startswith("day_") and d in DAY_KEYS:
        day_key = d
    else:
        day_key = None

    if not day_key:
        await ctx.reply("Invalid day. Use 1–6, 7a, 7b, or day_1..day_7b.")
        return

    queue_state[str(member.id)] = {
        "current_day": day_key,
        "next_send": (_now() + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
    }
    save_json(QUEUE_FILE, queue_state)
    await ctx.reply(f"Relocated {m_user(member)} to **{day_key}**, will send in ~5s.")
    await log_other(f"➡️ Relocated {_fmt_user(member)} to **{day_key}**")

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--check-config", action="store_true", help="Validate config + secrets and exit (no Discord connection).")
    args = parser.parse_args()

    if args.check_config:
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")
        if errors:
            print("[ConfigCheck] FAILED")
            for e in errors:
                print(f"- {e}")
            raise SystemExit(2)
        print("[ConfigCheck] OK")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        raise SystemExit(0)

    if not TOKEN:
        raise RuntimeError("bot_token must be set in config.secrets.json (server-only)")
    bot.run(TOKEN)

