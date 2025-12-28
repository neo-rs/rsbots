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

# Import Whop webhook handler
from whop_webhook_handler import (
    initialize as init_whop_handler,
    handle_whop_webhook_message,
)

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

# Invite Tracking Config
INVITE_CONFIG = config.get("invite_tracking", {})
HTTP_SERVER_PORT = INVITE_CONFIG.get("http_server_port", 8080)
INVITE_CHANNEL_ID = INVITE_CONFIG.get("invite_channel_id")
FALLBACK_INVITE = INVITE_CONFIG.get("fallback_invite", "")
WHOP_LOGS_CHANNEL_ID = INVITE_CONFIG.get("whop_logs_channel_id")
WHOP_WEBHOOK_CHANNEL_ID = INVITE_CONFIG.get("whop_webhook_channel_id")
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
MEMBER_STATUS_LOGS_CHANNEL_ID = DM_CONFIG.get("member_status_logs_channel_id", 1452835008170426368)

# Files
QUEUE_FILE = BASE_DIR / "queue.json"
REGISTRY_FILE = BASE_DIR / "registry.json"
INVITES_FILE = BASE_DIR / "invites.json"
MESSAGES_FILE = BASE_DIR / "messages.json"
SETTINGS_FILE = BASE_DIR / "settings.json"

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

# -----------------------------
# Utils: persistence/time
# -----------------------------
def _now() -> datetime:
    return datetime.now(timezone.utc)

def load_json(path: Path) -> dict:
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
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)

def save_all():
    save_json(QUEUE_FILE, queue_state)
    save_json(REGISTRY_FILE, registry)

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
    """Format list of roles as names."""
    roles = []
    for rid in role_ids:
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

async def log_member_status(msg: str, embed: discord.Embed = None):
    """Log to member status logs channel (for payment/subscription status and bot invites)"""
    if MEMBER_STATUS_LOGS_CHANNEL_ID:
        ch = bot.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
        if ch:
            with suppress(Exception):
                # Use provided embed or create default one
                if embed is None:
                    embed = discord.Embed(
                        description=msg,
                        color=0x5865F2,  # Discord blurple color
                        timestamp=datetime.now(timezone.utc)
                    )
                    embed.set_footer(text="RSCheckerbot • Member Status Tracking")
                await ch.send(embed=embed)

async def search_whop_logs_for_user(discord_id: int, lookback_hours: int = 24) -> dict:
    """Search Whop logs channel for user's cancellation/subscription info"""
    if not WHOP_LOGS_CHANNEL_ID:
        return {}
    
    try:
        channel = bot.get_channel(WHOP_LOGS_CHANNEL_ID)
        if not channel:
            return {}
        
        # Search recent messages (last 24 hours by default)
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        whop_info = {}
        
        async for message in channel.history(limit=100, after=cutoff_time):
            content = message.content
            # Check if message contains the Discord ID (look for "Discord ID" followed by the ID)
            if f"Discord ID" in content and str(discord_id) in content:
                # Parse Whop log format (label on one line, value on next line)
                lines = [line.strip() for line in content.split('\n') if line.strip()]
                whop_data = {}
                
                # Helper function to extract value after a label
                def get_value_after(label):
                    for i, line in enumerate(lines):
                        if label in line and i + 1 < len(lines):
                            return lines[i + 1]
                    return None
                
                # Extract all fields
                key_match = get_value_after("Key")
                access_pass = get_value_after("Access Pass")
                name = get_value_after("Name")
                email = get_value_after("Email")
                membership_status = get_value_after("Membership Status")
                discord_username = get_value_after("Discord Username")
                
                # Verify this is actually for our user by checking Discord ID field
                discord_id_value = get_value_after("Discord ID")
                if discord_id_value and str(discord_id) in discord_id_value:
                    # Store found info
                    whop_info = {
                        "key": key_match,
                        "access_pass": access_pass,
                        "name": name,
                        "email": email,
                        "membership_status": membership_status,
                        "discord_username": discord_username,
                        "found_at": message.created_at.isoformat(),
                        "message_id": message.id
                    }
                    return whop_info  # Return immediately when found
        
        return whop_info
    except Exception as e:
        log.error(f"Error searching Whop logs: {e}")
        return {}

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

async def init_http_server():
    """Initialize the HTTP server for invite creation."""
    app = web.Application()
    app.router.add_post("/create-invite", handle_create_invite)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_SERVER_PORT)
    await site.start()
    log.info(f"HTTP server started on port {HTTP_SERVER_PORT}")

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
# Events
# -----------------------------
@bot.event
async def on_ready():
    global queue_state, registry, invite_usage_cache
    
    # Comprehensive startup logging
    log.info("="*60)
    log.info("  🔍 RS Checker Bot")
    log.info("="*60)
    log.info(f"[Bot] Ready as {bot.user} (ID: {bot.user.id})")
    
    queue_state = load_json(QUEUE_FILE)
    registry = load_json(REGISTRY_FILE)
    
    guild = bot.get_guild(GUILD_ID)
    if guild:
        log.info(f"[Bot] Connected to: {guild.name}")
        
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
            fmt_user_func=_fmt_user
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
    
    log.info("="*60)
    log.info("")

@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id == GUILD_ID and not member.bot:
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
        
        all_checked_roles = _fmt_role_list(ROLES_TO_CHECK, guild)
        
        await log_role_event(
            f"👤 **New Member Joined:** {_fmt_user(member)}\n"
            f"   {welcome_status}\n"
            f"   📋 Current roles: {current_role_names}\n"
            f"   {checked_status}"
            f"   🔍 **Basis for 60s check:** Will verify if user has ANY of these checked roles ({len(ROLES_TO_CHECK)} total): {all_checked_roles}\n"
            f"   ⏱️ If user has NONE after 60s → will assign trigger role"
        )
        asyncio.create_task(check_and_assign_role(member))

        # Track invite usage
        try:
            invites = await member.guild.invites()
            matched_invite = None
            used_invite_code = None
            used_invite_inviter = None
            
            # First pass: Find which invite was used (uses increased)
            for invite in invites:
                previous_uses = invite_usage_cache.get(invite.code, 0)
                if invite.uses > previous_uses:
                    # This invite was used - capture it (only first one found)
                    if used_invite_code is None:
                        used_invite_code = invite.code
                        used_invite_inviter = invite.inviter
                    invite_usage_cache[invite.code] = invite.uses
                else:
                    # Update cache even if not used (to track current state)
                    invite_usage_cache[invite.code] = invite.uses
            
            # Second pass: Check if the used invite is in our tracked invites
            if used_invite_code:
                # Check if invite exists and is unused
                invite_entry = invites_data.get(used_invite_code)
                if invite_entry and invite_entry.get("used_at") is None:
                    matched_invite = used_invite_code
                    await track_invite_usage(used_invite_code, member)
                    # Log to terminal with details
                    inviter_name = used_invite_inviter.name if used_invite_inviter else "Unknown"
                    log.info(f"✅ Member {member} ({member.id}) joined via tracked invite: {used_invite_code} (invited by {inviter_name})")
            
            if not matched_invite:
                # Determine join method
                if used_invite_code:
                    # Invite was used but not in our tracked invites (untracked invite)
                    inviter_name = used_invite_inviter.name if used_invite_inviter else "Unknown"
                    inviter_id = used_invite_inviter.id if used_invite_inviter else "Unknown"
                    log.warning(f"⚠️ Member {member} ({member.id}) joined via untracked invite: {used_invite_code} (invited by {inviter_name} ({inviter_id}))")
                    # Could be a bot invite from Whop - log to member status channel
                    await log_member_status(
                        f"🤖 **Potential Bot Invite Join**\n"
                        f"**User:** {_fmt_user(member)}\n"
                        f"**Invite Code:** `{used_invite_code}`\n"
                        f"**Inviter:** {inviter_name} (`{inviter_id}`)\n"
                        f"**Status:** ⚠️ Not in tracked invites"
                    )
                else:
                    # No invite code matched - could be bot invite, direct link, or unknown
                    join_method = "Bot Invite (likely Whop)" if not member.bot else "Bot Account Join"
                    log.warning(f"❓ Member {member} ({member.id}) joined via {join_method} or direct link")
                    # Log to member status channel since this is likely a subscription-based join
                    await log_member_status(
                        f"🔐 **{join_method}**\n"
                        f"**User:** {_fmt_user(member)}\n"
                        f"**Invite Code:** No matching code found\n"
                        f"**Type:** 💳 May be subscription-based (Whop)\n"
                        f"**Status:** ⚠️ Not tracked in invites"
                    )
                
        except Exception as e:
            log.error(f"❌ Error tracking invite for {member} ({member.id}): {e}")

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
            # General role change - log it
            added_names = _fmt_role_list(roles_added, after.guild) if roles_added else None
            removed_names = _fmt_role_list(roles_removed, after.guild) if roles_removed else None
            
            log_msg = f"🔄 **Role Update:** {_fmt_user(after)}\n"
            if removed_names:
                log_msg += f"   ➖ **Removed:** {removed_names}\n"
            if added_names:
                log_msg += f"   ➕ **Added:** {added_names}\n"
            
            await log_role_event(log_msg.rstrip())

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
            # Search Whop logs for cancellation details
            whop_info = await search_whop_logs_for_user(after.id, lookback_hours=24)
            
            # Build embed with Whop info if found
            embed = discord.Embed(
                title="💳 Payment Cancellation Detected",
                color=0xFF0000,  # Red for cancellation
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(name="User", value=_fmt_user(after), inline=False)
            embed.add_field(name="Roles Removed", value=removed_names, inline=False)
            
            if whop_info:
                # Add Whop details if found
                if whop_info.get("key"):
                    embed.add_field(name="🔑 Whop Key", value=f"`{whop_info['key']}`", inline=True)
                if whop_info.get("access_pass"):
                    embed.add_field(name="📦 Access Pass", value=whop_info["access_pass"], inline=True)
                if whop_info.get("name"):
                    embed.add_field(name="👤 Name", value=whop_info["name"], inline=True)
                if whop_info.get("email"):
                    embed.add_field(name="📧 Email", value=whop_info["email"], inline=True)
                if whop_info.get("membership_status"):
                    embed.add_field(name="📊 Membership Status", value=whop_info["membership_status"], inline=True)
                if whop_info.get("discord_username"):
                    embed.add_field(name="💬 Discord Username", value=whop_info["discord_username"], inline=True)
            else:
                embed.add_field(
                    name="⚠️ Whop Info", 
                    value="No matching Whop log found in last 24 hours", 
                    inline=False
                )
            
            embed.add_field(
                name="Reason", 
                value="Member role removed — may indicate subscription cancellation or payment failure", 
                inline=False
            )
            embed.set_footer(text="RSCheckerbot • Member Status Tracking")
            
            await log_member_status("", embed=embed)
        
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
            # Log to member status channel for payment tracking
            await log_member_status(
                f"✅ **Payment Reactivated**\n"
                f"**User:** {_fmt_user(after)}\n"
                f"**Roles Added:** {added_names}\n"
                f"**Reason:** Member role regained — may indicate subscription reactivated or payment successful"
            )
        
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
            # Check if it's from Whop Events app (native integration)
            if message.author.name == "Whop Events" or "Whop" in (message.author.name or ""):
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

