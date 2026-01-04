#!/usr/bin/env python3
"""
Whop Webhook Handler
--------------------
Handles webhook messages from Whop workflows posted to Discord channel.
Monitors Discord channel for Whop webhook messages and processes them.

Canonical Owner: This module owns Whop webhook processing logic.
"""

import json
import re
import logging
import discord
from pathlib import Path
from datetime import datetime, timezone
from contextlib import suppress

log = logging.getLogger("rs-checker")

# Configuration (initialized from main)
WHOP_WEBHOOK_CHANNEL_ID = None
WHOP_LOGS_CHANNEL_ID = None
ROLE_TRIGGER = None
WELCOME_ROLE_ID = None
ROLE_CANCEL_A = None
ROLE_CANCEL_B = None

# Logging functions (initialized from main - canonical ownership)
_log_other = None
_log_member_status = None
_fmt_user = None
_get_member_history = None

# Identity tracking and trial abuse detection
MEMBER_STATUS_LOGS_CHANNEL_ID = None

# Expected roles config (loaded from config.json)
EXPECTED_ROLES = {}

# File paths for JSON storage (canonical: JSON-only, no SQLite)
BASE_DIR = Path(__file__).resolve().parent
IDENTITY_CACHE_FILE = BASE_DIR / "whop_identity_cache.json"
TRIAL_CACHE_FILE = BASE_DIR / "trial_history.json"
IDENTITY_CONFLICTS_FILE = BASE_DIR / "identity_conflicts.jsonl"


def _norm_email(s: str) -> str:
    """Normalize email address for consistent storage/lookup"""
    return (s or "").strip().lower()

def _roles_plain(member: discord.Member) -> str:
    """Comma-separated role names (no role mentions, excludes @everyone and managed roles)."""
    roles = [r.name for r in member.roles if r != member.guild.default_role and not r.managed]
    return ", ".join(roles) if roles else "—"

def _load_json(path: Path) -> dict:
    """Load JSON file, returning empty dict if file doesn't exist or is invalid"""
    try:
        if not path.exists() or path.stat().st_size == 0:
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_json(path: Path, data: dict) -> None:
    """Save data to JSON file with error handling"""
    try:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _cache_identity(email: str, discord_id: str, discord_username: str = "") -> None:
    """Cache email -> discord_id mapping for future enrichment"""
    email = _norm_email(email)
    if not email or not discord_id:
        return
    db = _load_json(IDENTITY_CACHE_FILE)
    db[email] = {
        "discord_id": str(discord_id),
        "discord_username": (discord_username or "").strip(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }
    _save_json(IDENTITY_CACHE_FILE, db)

def _lookup_identity(email: str) -> dict | None:
    """Look up cached identity mapping by email"""
    email = _norm_email(email)
    if not email:
        return None
    db = _load_json(IDENTITY_CACHE_FILE)
    return db.get(email)

def _load_whop_history() -> dict:
    """Load whop_history.json from RSAdminBot/whop_data/ directory.
    
    Path is resolved from config or uses default relative path.
    Returns empty dict if file doesn't exist or is invalid.
    """
    try:
        # Default path (relative to RSCheckerbot folder)
        default_path = BASE_DIR.parent / "RSAdminBot" / "whop_data" / "whop_history.json"
        whop_history_path = default_path
        
        # Try to load config to get custom path (if available)
        try:
            config_path = BASE_DIR / "config.json"
            if config_path.exists():
                config_data = _load_json(config_path)
                custom_path = config_data.get("paths", {}).get("whop_history")
                if custom_path:
                    # Resolve relative to BASE_DIR
                    whop_history_path = (BASE_DIR / custom_path).resolve()
        except Exception:
            pass  # Use default path if config loading fails
        
        if not whop_history_path.exists():
            return {}
        
        data = _load_json(whop_history_path)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        log.warning(f"Failed to load whop_history.json: {e}")
        return {}

def _build_identity_cache_from_history(whop_history: dict) -> dict:
    """Build identity cache dictionary from whop_history membership events.
    
    Args:
        whop_history: Dictionary with 'membership_events' key containing list of events
    
    Returns:
        Dictionary mapping email (normalized) to {discord_id, discord_username, last_seen, source}
    """
    cache = {}
    events = whop_history.get("membership_events", [])
    
    for event in events:
        email = event.get("email", "").strip()
        discord_id = event.get("discord_id", "").strip()
        discord_username = event.get("discord_username", "").strip()
        timestamp = event.get("timestamp") or event.get("created_at")
        
        if not email or not discord_id:
            continue
        
        email_norm = _norm_email(email)
        if not email_norm:
            continue
        
        # Parse timestamp to ISO format for last_seen
        last_seen_iso = timestamp if timestamp else datetime.now(timezone.utc).isoformat()
        
        cache[email_norm] = {
            "discord_id": str(discord_id),
            "discord_username": discord_username,
            "last_seen": last_seen_iso,
            "source": "whop_history"
        }
    
    return cache

def _backfill_identity_cache() -> None:
    """Backfill identity cache from whop_history.json.
    
    Merge rules:
    - If email not present → add
    - If email present and discord_id matches → update metadata
    - If email present and discord_id differs → log conflict, do NOT overwrite
    """
    try:
        whop_history = _load_whop_history()
        if not whop_history:
            log.info("whop_history.json not found or empty, skipping identity backfill")
            return
        
        history_cache = _build_identity_cache_from_history(whop_history)
        if not history_cache:
            log.info("No identity mappings found in whop_history.json")
            return
        
        # Load existing cache
        existing_cache = _load_json(IDENTITY_CACHE_FILE)
        
        added_count = 0
        updated_count = 0
        conflict_count = 0
        
        # Log conflicts to file
        conflicts_log = []
        
        for email, history_entry in history_cache.items():
            existing_entry = existing_cache.get(email)
            
            if not existing_entry:
                # New entry - add it
                existing_cache[email] = history_entry
                added_count += 1
            else:
                # Entry exists - check discord_id
                existing_id = str(existing_entry.get("discord_id", "")).strip()
                history_id = str(history_entry.get("discord_id", "")).strip()
                
                if existing_id == history_id:
                    # IDs match - update metadata (last_seen, username if newer)
                    existing_cache[email]["last_seen"] = history_entry["last_seen"]
                    if history_entry.get("discord_username"):
                        existing_cache[email]["discord_username"] = history_entry["discord_username"]
                    updated_count += 1
                else:
                    # IDs differ - log conflict, do NOT overwrite
                    conflict_count += 1
                    conflicts_log.append({
                        "email": email,
                        "existing_discord_id": existing_id,
                        "history_discord_id": history_id,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
        
        # Save merged cache
        _save_json(IDENTITY_CACHE_FILE, existing_cache)
        
        # Log conflicts if any
        if conflicts_log:
            try:
                with open(IDENTITY_CONFLICTS_FILE, "a", encoding="utf-8") as f:
                    for conflict in conflicts_log:
                        f.write(json.dumps(conflict, ensure_ascii=False) + "\n")
            except Exception as e:
                log.warning(f"Failed to write identity conflicts log: {e}")
        
        log.info(f"Identity backfill complete: {added_count} added, {updated_count} updated, {conflict_count} conflicts")
    except Exception as e:
        log.error(f"Identity backfill failed: {e}", exc_info=True)

async def _send_lookup_request(message: discord.Message, event_type: str, email: str, whop_user_id: str = "", membership_id: str = ""):
    """
    Posts a 'lookup needed' message into #member-status-logs so staff/bots can resolve identity.
    Only posts if identity is not already cached.
    """
    if not MEMBER_STATUS_LOGS_CHANNEL_ID or not message.guild:
        return
    ch = message.guild.get_channel(MEMBER_STATUS_LOGS_CHANNEL_ID)
    if not ch:
        return

    email_n = _norm_email(email)
    cached = _lookup_identity(email_n)

    if cached and cached.get("discord_id"):
        # already resolved, no lookup needed
        return

    lines = []
    lines.append("🔎 **Lookup Needed (Whop → Discord)**")
    lines.append(f"• Event: `{event_type}`")
    if email_n:
        lines.append(f"• Email: `{email_n}`")
    if whop_user_id:
        lines.append(f"• Whop User: `{whop_user_id}`")
    if membership_id:
        lines.append(f"• Membership: `{membership_id}`")
    lines.append("")
    lines.append("Action:")
    # Use channel mention format (resolves to actual channel name, no hardcoded text)
    # WHOP_LOGS_CHANNEL_ID is available as a global from initialize()
    whop_logs_mention = f"<#{WHOP_LOGS_CHANNEL_ID}>" if WHOP_LOGS_CHANNEL_ID else "Whop logs channel"
    lines.append(f"• Check {whop_logs_mention} native Whop event (Discord ID field) or forwarder logs")
    lines.append("• Once found, link it (email ↔ discord_id)")

    await ch.send("\n".join(lines))

def _record_trial_event(email: str, discord_id: str, membership_id: str, trial_days: str, is_first_membership: str, event_type: str) -> dict:
    """
    Store trial activity and detect suspicious patterns.
    Returns dict with 'suspicious', 'reason', 'key', 'count' fields.
    """
    email_n = _norm_email(email)
    key = f"{email_n}|{discord_id or 'no_discord'}"

    db = _load_json(TRIAL_CACHE_FILE)
    rec = db.get(key, {"email": email_n, "discord_id": discord_id or "", "events": []})

    rec["events"].append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "membership_id": membership_id or "",
        "trial_days": str(trial_days or ""),
        "is_first_membership": str(is_first_membership or ""),
    })

    # Keep last 50 events per identity to avoid bloat
    rec["events"] = rec["events"][-50:]
    db[key] = rec
    _save_json(TRIAL_CACHE_FILE, db)

    # Suspicion logic:
    # 1) If is_first_membership == false AND trial_days > 0 => strong repeat-trial signal
    # 2) If we see multiple trial activations historically => weak repeat-trial signal
    suspicious = False
    reason = ""
    try:
        td = int(str(trial_days or "0"))
    except ValueError:
        td = 0

    if str(is_first_membership).lower() == "false" and td > 0:
        suspicious = True
        reason = "Trial started but is_first_membership=false (repeat trial likely)"
    else:
        # count trial-type events
        trial_events = [e for e in rec["events"] if str(e.get("trial_days","0")).isdigit() and int(e.get("trial_days","0")) > 0]
        if len(trial_events) >= 2:
            suspicious = True
            reason = f"Multiple trial events seen ({len(trial_events)})"

    return {"suspicious": suspicious, "reason": reason, "key": key, "count": len(rec["events"])}


def _fmt_discord_ts(ts_str: str | None, style: str = "D") -> str:
    """Format timestamp string as Discord timestamp (human-readable)
    
    Args:
        ts_str: ISO timestamp string or Unix timestamp string
        style: Discord timestamp style ('D' = short date, 'F' = full date, 'R' = relative)
    
    Returns:
        Discord timestamp string like <t:1234567890:D> or "—" if invalid
    """
    if not ts_str:
        return "—"
    try:
        # Try parsing as ISO timestamp
        if "T" in str(ts_str) or "-" in str(ts_str):
            dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            unix_ts = int(dt.timestamp())
        else:
            # Assume Unix timestamp (string or int)
            unix_ts = int(float(str(ts_str)))
        return f"<t:{unix_ts}:{style}>"
    except (ValueError, TypeError, AttributeError):
        return "—"


async def _resolve_member_safe(guild: discord.Guild, discord_id: int | None, force_fetch: bool = False) -> discord.Member | None:
    """Safely resolve a member with rate-limit protection.
    
    Args:
        guild: Discord guild to resolve member in
        discord_id: Discord user ID to resolve
        force_fetch: If True, always try fetch_member (bypasses cache check)
    
    Returns:
        discord.Member if found, None otherwise
    """
    if not discord_id or not guild:
        return None
    
    # Try fast path first (cached member)
    member = guild.get_member(discord_id)
    if member:
        return member
    
    # Only fetch if explicitly requested or for critical events
    # (This prevents API spam - fetch_member is expensive)
    if force_fetch:
        try:
            member = await guild.fetch_member(discord_id)
            return member
        except (discord.NotFound, discord.HTTPException):
            return None
    
    return None

def _safe_get(event_data: dict, *keys: str, default: str = "—") -> str:
    """Safely get nested dict value using dot notation keys (e.g., 'user.username', 'membership.status')
    
    Args:
        event_data: Event data dictionary
        keys: Variable number of key paths to try (e.g., 'user.username', 'username')
        default: Default value if all keys fail
    
    Returns:
        Value as string, or default
    """
    for key_path in keys:
        parts = key_path.split(".")
        value = event_data
        try:
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                if value is None:
                    break
            if value is not None and value != "":
                return str(value)
        except (AttributeError, TypeError, KeyError):
            continue
    return default


def _build_support_card_embed(
    title: str,
    color: int,
    member: discord.Member | None,
    event_data: dict,
    guild: discord.Guild | None = None,
    roles_added: list[str] | None = None,
    roles_removed: list[str] | None = None,
    flags: list[str] | None = None
) -> discord.Embed:
    """Build a support card embed with structured fields (Success Bot style)
    
    Args:
        title: Embed title
        color: Embed color (hex integer)
        member: Discord member object (if available)
        event_data: Event data dictionary
        guild: Discord guild object (for member lookup fallback)
        roles_added: List of role names that were added in this event
        roles_removed: List of role names that were removed in this event
        flags: List of warning/info flags to display in Access section
    
    Returns:
        discord.Embed with structured fields
    """
    roles_added = roles_added or []
    roles_removed = roles_removed or []
    flags = flags or []
    
    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    
    # Author + thumbnail (Amazon-style card header)
    if member:
        with suppress(Exception):
            embed.set_author(name=str(member), icon_url=member.display_avatar.url)
            embed.set_thumbnail(url=member.display_avatar.url)
    
    # Member field (mention if available, otherwise user ID) + avatar thumbnail when resolvable
    if member:
        embed.add_field(name="Member", value=member.mention, inline=False)
        embed.add_field(name="User ID", value=str(member.id), inline=False)
    else:
        discord_id = event_data.get("discord_user_id", "") or _safe_get(event_data, "user.discord_id", default="")
        if discord_id and discord_id != "—":
            try:
                user_id_int = int(str(discord_id).strip())
                if guild:
                    fallback_member = guild.get_member(user_id_int)
                    if fallback_member:
                        embed.add_field(name="Member", value=fallback_member.mention, inline=False)
                        embed.add_field(name="User ID", value=str(user_id_int), inline=False)
                        with suppress(Exception):
                            embed.set_author(name=str(fallback_member), icon_url=fallback_member.display_avatar.url)
                            embed.set_thumbnail(url=fallback_member.display_avatar.url)
                    else:
                        embed.add_field(name="Member", value=f"<@{user_id_int}>", inline=False)
                        embed.add_field(name="User ID", value=str(user_id_int), inline=False)
                else:
                    embed.add_field(name="Member", value=f"<@{user_id_int}>", inline=False)
                    embed.add_field(name="User ID", value=str(user_id_int), inline=False)
            except (ValueError, TypeError):
                embed.add_field(name="User ID", value=str(discord_id), inline=False)
        else:
            embed.add_field(name="User ID", value="N/A", inline=False)
    
    # Identity section
    username = _safe_get(event_data, "user.username", "username", default="—")
    name = _safe_get(event_data, "user.name", "name", default="—")
    email = _safe_get(event_data, "email", "user.email", default="")
    
    identity_lines: list[str] = []
    if username != "—":
        identity_lines.append(f"• Username: {username}")
    if name != "—" and name != username:
        identity_lines.append(f"• Name: {name}")
    if email:
        identity_lines.append(f"• Email: `{_norm_email(email)}`")
    
    if identity_lines:
        embed.add_field(name="Identity", value="\n".join(identity_lines), inline=False)
    
    # Membership + billing section
    membership_status = _safe_get(event_data, "membership.status", "status", default="")
    membership_id = _safe_get(event_data, "membership_id", "membership.id", default="")
    total_spent = _safe_get(event_data, "user.total_spent_in_usd", "total_spent", default="")
    amount = _safe_get(event_data, "amount", "payment.formatted_amount", "payment.amount", default="")
    
    membership_lines: list[str] = []
    if membership_status and membership_status != "—":
        membership_lines.append(f"• Status: {membership_status}")
    if membership_id and membership_id != "—":
        membership_lines.append(f"• Membership ID: `{membership_id}`")
    if total_spent and total_spent != "—":
        val = f"${total_spent}" if not str(total_spent).startswith("$") else str(total_spent)
        membership_lines.append(f"• Total Spent: {val}")
    if amount and amount not in ("—", "N/A"):
        val = f"${amount}" if not str(amount).startswith("$") else str(amount)
        membership_lines.append(f"• Last Amount: {val}")
    
    if membership_lines:
        embed.add_field(name="Membership", value="\n".join(membership_lines), inline=False)
    
    # Member history (join/leave) - from Discord events only
    try:
        did = None
        if member:
            did = member.id
        else:
            raw_did = _safe_get(event_data, "discord_user_id", "user.discord_id", default="")
            if raw_did and raw_did != "—":
                try:
                    did = int(str(raw_did).strip())
                except (ValueError, TypeError):
                    pass
        if did and _get_member_history:
            hist = _get_member_history(did) or {}
            if hist:
                hist_lines = []
                if hist.get("first_join_ts"):
                    try:
                        hist_lines.append(f"• First joined: <t:{int(hist.get('first_join_ts'))}:D>")
                    except Exception:
                        pass
                if hist.get("last_join_ts"):
                    try:
                        hist_lines.append(f"• Last joined: <t:{int(hist.get('last_join_ts'))}:R>")
                    except Exception:
                        pass
                if hist.get("last_leave_ts"):
                    try:
                        hist_lines.append(f"• Last left: <t:{int(hist.get('last_leave_ts'))}:R>")
                    except Exception:
                        pass
                if hist.get("join_count", 0) > 0:
                    hist_lines.append(f"• Join count: {hist.get('join_count', 0)}")
                if hist_lines:
                    embed.add_field(name="Member History", value="\n".join(hist_lines), inline=False)
    except Exception:
        pass
    
    # Access section (roles changed + current roles + flags)
    access_lines: list[str] = []
    if roles_added:
        access_lines.append("**+ Added:** " + ", ".join(roles_added))
    if roles_removed:
        access_lines.append("**- Removed:** " + ", ".join(roles_removed))
    if member:
        access_lines.append("**Current Roles:** " + _roles_plain(member))
        
        # Missing role detection (compare expected vs current)
        membership_status = _safe_get(event_data, "membership.status", "status", default="")
        if membership_status and membership_status != "—":
            status_normalized = (membership_status or "").strip().lower()
            expected_role_names = EXPECTED_ROLES.get(status_normalized, [])
            if expected_role_names:
                current_role_names = {r.name for r in member.roles if r != member.guild.default_role and not r.managed}
                for expected_role in expected_role_names:
                    if expected_role not in current_role_names:
                        flags.append(f"{status_normalized} on Whop but missing {expected_role} role")
    else:
        access_lines.append("**Current Roles:** — (member not found)")
    if flags:
        access_lines.extend([f"⚠️ {f}" for f in flags])
    if access_lines:
        embed.add_field(name="Access", value="\n".join(access_lines), inline=False)
    
    # Links (dashboard, manage, checkout)
    dashboard_url = _safe_get(event_data, "user.dashboard_url", "dashboard_url", default="")
    manage_url = _safe_get(event_data, "membership.manage_url", "manage_url", default="")
    checkout_url = _safe_get(event_data, "plan.purchase_url", "checkout_url", "purchase_url", default="")
    
    links_text = []
    if dashboard_url and dashboard_url != "—":
        links_text.append(f"[Dashboard]({dashboard_url})")
    if manage_url and manage_url != "—":
        links_text.append(f"[Manage]({manage_url})")
    if checkout_url and checkout_url != "—":
        links_text.append(f"[Checkout]({checkout_url})")
    
    if links_text:
        embed.add_field(name="Links", value=" • ".join(links_text), inline=False)
    
    # Footer removed per user request (clean card style)
    
    return embed


def initialize(webhook_channel_id, whop_logs_channel_id, role_trigger, welcome_role_id, role_cancel_a, role_cancel_b,
               log_other_func, log_member_status_func, fmt_user_func, member_status_logs_channel_id=None, get_member_history_func=None):
    """
    Initialize handler with configuration and logging functions.
    
    Args:
        webhook_channel_id: Channel ID where Whop workflow webhooks are posted
        whop_logs_channel_id: Channel ID where Whop native integration posts
        role_trigger: Cleanup/trigger role ID
        welcome_role_id: Welcome role ID
        role_cancel_a: Member role ID
        role_cancel_b: Welcome role ID (same as welcome_role_id)
        log_other_func: Function to log to other channel (canonical owner)
        log_member_status_func: Function to log to member status channel (canonical owner)
        fmt_user_func: Function to format user display (canonical owner)
        member_status_logs_channel_id: Channel ID for member status logs (lookup requests, trial alerts)
        get_member_history_func: Function to get member history record (canonical owner)
    """
    global WHOP_WEBHOOK_CHANNEL_ID, WHOP_LOGS_CHANNEL_ID, ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B
    global _log_other, _log_member_status, _fmt_user, _get_member_history
    global MEMBER_STATUS_LOGS_CHANNEL_ID, EXPECTED_ROLES
    
    WHOP_WEBHOOK_CHANNEL_ID = webhook_channel_id
    WHOP_LOGS_CHANNEL_ID = whop_logs_channel_id
    ROLE_TRIGGER = role_trigger
    WELCOME_ROLE_ID = welcome_role_id
    ROLE_CANCEL_A = role_cancel_a
    ROLE_CANCEL_B = role_cancel_b
    _log_other = log_other_func
    _log_member_status = log_member_status_func
    _fmt_user = fmt_user_func
    _get_member_history = get_member_history_func
    MEMBER_STATUS_LOGS_CHANNEL_ID = member_status_logs_channel_id
    
    # Load expected roles config
    try:
        config_path = BASE_DIR / "config.json"
        if config_path.exists():
            config_data = _load_json(config_path)
            EXPECTED_ROLES = config_data.get("whop_webhook", {}).get("expected_roles", {})
        else:
            EXPECTED_ROLES = {}
    except Exception as e:
        log.warning(f"Failed to load expected roles config: {e}")
        EXPECTED_ROLES = {}
    
    # Backfill identity cache from whop_history.json
    _backfill_identity_cache()
    
    log.info(f"Whop webhook handler initialized")
    log.info(f"Monitoring webhook channel {webhook_channel_id} and logs channel {whop_logs_channel_id}")


async def handle_whop_webhook_message(message: discord.Message):
    """
    Handle messages from Whop webhook in Discord channel.
    
    Supports two formats:
    1. Workflow webhooks (EVENT_DATA JSON in description)
    2. Native Whop integration messages (embed fields)
    
    Canonical owner for Whop webhook message processing.
    """
    try:
        # Check if message has embeds
        if not message.embeds:
            return
        
        embed = message.embeds[0]
        description = embed.description or ""
        title = embed.title or ""
        
        log.info(f"Whop message detected: {title}")
        
        # Try to extract EVENT_DATA from description (workflow format)
        json_match = re.search(r'EVENT_DATA:(\{.*\})', description)
        
        if json_match:
            # Workflow webhook format
            await _handle_workflow_webhook(message, embed, json_match)
        else:
            # Native Whop integration format
            await _handle_native_whop_message(message, embed)
        
    except Exception as e:
        log.error(f"Error handling webhook message: {e}", exc_info=True)
        if _log_other:
            await _log_other(f"❌ **Whop Webhook Error:** {e}")


async def _handle_workflow_webhook(message: discord.Message, embed: discord.Embed, json_match: re.Match):
    """Handle workflow webhook format (EVENT_DATA JSON)"""
    try:
        # Parse event data
        json_string = json_match.group(1)
        event_data = json.loads(json_string)
        
        event_type = event_data.get('event_type', '').strip()
        discord_user_id = event_data.get('discord_user_id', '').strip()
        email = event_data.get('email', '').strip()
        
        # Check if EVENT_DATA is empty (all fields are empty strings)
        has_data = any(v and v.strip() for k, v in event_data.items() if k != 'event_type' or v.strip())
        
        if not event_type:
            log.warning(f"Whop workflow webhook has no event_type: {json_string}")
            if _log_other:
                await _log_other(f"⚠️ **Whop Webhook:** Received webhook with empty event_type. Check Whop workflow variables.")
            return
        
        if not has_data:
            log.warning(f"Whop workflow webhook has empty EVENT_DATA fields: {json_string}")
            if _log_other:
                await _log_other(
                    f"⚠️ **Whop Webhook Error:** EVENT_DATA fields are empty!\n"
                    f"**Event Type:** `{event_type}`\n"
                    f"**Issue:** Whop workflow variables not populated. Check workflow configuration.\n"
                    f"**Message ID:** {message.id}"
                )
            return
        
        log.info(f"Processing Whop workflow event: {event_type} for user {discord_user_id}")
        
        # Trial abuse tracking (workflow path)
        trial_days = event_data.get("trial_period_days", "") or event_data.get("trial_days", "")
        is_first = event_data.get("is_first_membership", "")
        membership_id_val = event_data.get("membership_id", "")

        # Consider trial tracking for activation/pending events
        if event_type in ("membership.activated.pending", "membership.activated", "payment.succeeded.activation", "payment.succeeded.renewal"):
            info = _record_trial_event(
                email=email,
                discord_id=discord_user_id,
                membership_id=membership_id_val,
                trial_days=trial_days,
                is_first_membership=is_first,
                event_type=event_type,
            )
            # Alert logic: only alert when actionable
            # - If discord_id is empty: only alert on strong signal (is_first=false && trial_days>0)
            # - If discord_id exists: alert on any suspicious pattern
            should_alert = info.get("suspicious", False)
            if not discord_user_id:
                # When discord_id missing, only alert on strong signal (not weak "multiple trials" signal)
                try:
                    td = int(str(trial_days or "0"))
                    is_strong_signal = (str(is_first).lower() == "false" and td > 0)
                    should_alert = should_alert and is_strong_signal
                except ValueError:
                    should_alert = False
            
            if should_alert and _log_member_status:
                # Create embed with structured fields (like Success Bot format)
                embed = discord.Embed(
                    title="🚩 Trial Abuse Signal",
                    color=0xFF6B6B,  # Red/orange for alert
                    timestamp=datetime.now(timezone.utc)
                )
                
                # Format Discord member mention and User ID
                guild = message.guild if message.guild else None
                if discord_user_id:
                    try:
                        user_id_int = int(discord_user_id)
                        if guild:
                            member = guild.get_member(user_id_int)
                            if member:
                                embed.add_field(name="Member", value=member.mention, inline=False)
                                embed.add_field(name="User ID", value=str(user_id_int), inline=False)
                            else:
                                # User not in guild, use mention format
                                embed.add_field(name="Member", value=f"<@{user_id_int}>", inline=False)
                                embed.add_field(name="User ID", value=str(user_id_int), inline=False)
                        else:
                            embed.add_field(name="Member", value=f"<@{user_id_int}>", inline=False)
                            embed.add_field(name="User ID", value=str(user_id_int), inline=False)
                    except (ValueError, TypeError):
                        embed.add_field(name="User ID", value=str(discord_user_id), inline=False)
                else:
                    embed.add_field(name="User ID", value="N/A", inline=False)
                
                # Email field
                if email:
                    embed.add_field(name="Email", value=f"`{_norm_email(email)}`", inline=False)
                
                # Reason field
                embed.add_field(name="Reason", value=info.get('reason', 'Unknown'), inline=False)
                
                embed.set_footer(text="RSCheckerbot • Member Status Tracking")
                
                await _log_member_status("", embed=embed)
        
        if not discord_user_id:
            if _log_other:
                await _log_other(
                    f"⚠️ **Whop Webhook:** No discord_user_id in event.\n"
                    f"**Event Type:** `{event_type}`\n"
                    f"**Email:** {email if email else 'N/A'}\n"
                    f"**Message ID:** {message.id}"
                )

            # Request a lookup in #member-status-logs
            await _send_lookup_request(
                message=message,
                event_type=event_type,
                email=email,
                whop_user_id=event_data.get("user_id", ""),
                membership_id=event_data.get("membership_id", ""),
            )
            return
        
        # Get guild and member
        guild = message.guild
        try:
            did_int = int(str(discord_user_id).strip())
        except ValueError:
            log.error(f"Invalid discord_user_id format: {discord_user_id}")
            if _log_other:
                await _log_other(f"❌ **Whop Webhook Error:** Invalid discord_user_id format: `{discord_user_id}`")
            return
        
        member = await _resolve_member_safe(guild, did_int, force_fetch=True)
        
        if not member:
            if _log_other:
                await _log_other(
                    f"⚠️ **Whop Webhook:** Member not found in guild.\n"
                    f"**Discord ID:** `{discord_user_id}`\n"
                    f"**Event Type:** `{event_type}`\n"
                    f"**Email:** {email if email else 'N/A'}"
                )
            return
        
        # Route to handler based on event type
        if event_type == 'membership.activated':
            await handle_membership_activated(member, event_data)
        elif event_type == 'membership.activated.pending':
            await handle_membership_activated_pending(member, event_data)
        elif event_type == 'membership.deactivated':
            await handle_membership_deactivated(member, event_data)
        elif event_type == 'membership.deactivated.payment_failure':
            await handle_membership_deactivated_payment_failure(member, event_data)
        elif event_type == 'payment.succeeded.renewal':
            await handle_payment_renewal(member, event_data)
        elif event_type == 'payment.succeeded.activation':
            await handle_payment_activation(member, event_data)
        elif event_type == 'payment.failed':
            await handle_payment_failed(member, event_data)
        elif event_type == 'payment.refunded':
            await handle_payment_refunded(member, event_data)
        elif event_type == 'waitlist.entry_approved':
            await handle_waitlist_approved(member, event_data)
        else:
            if _log_other:
                await _log_other(f"ℹ️ **Whop Webhook:** Unhandled event type: {event_type}")
    except json.JSONDecodeError as e:
        log.error(f"JSON decode error in workflow webhook: {e}")
        if _log_other:
            await _log_other(f"❌ **Whop Webhook Error:** Failed to parse JSON: {e}")


async def _handle_native_whop_message(message: discord.Message, embed: discord.Embed):
    """
    Handle native Whop integration messages (embed fields format).
    """
    try:
        title = embed.title or ""
        description = embed.description or ""
        content = message.content or ""
        
        # Extract data from embed fields (primary source)
        fields_data = {}
        for field in embed.fields:
            fields_data[field.name.lower()] = field.value
        
        # Also parse message content as fallback (for messages without embeds)
        content_data = _parse_whop_content(content)
        
        # Merge: embed fields take precedence, content as fallback
        parsed_data = {**content_data, **fields_data}
        
        # Extract Discord ID
        discord_id_str = None
        
        # Try embed fields first
        if "discord id" in fields_data:
            discord_id_str = fields_data["discord id"]
            discord_id_str = re.sub(r'<@!?(\d+)>', r'\1', discord_id_str).strip()
        elif "Discord ID" in [f.name for f in embed.fields]:
            for field in embed.fields:
                if field.name == "Discord ID":
                    discord_id_str = re.sub(r'<@!?(\d+)>', r'\1', field.value).strip()
                    break
        
        # Try content parsing
        if not discord_id_str and content_data.get("discord_id"):
            discord_id_str = content_data["discord_id"]
        
        # Try description
        if not discord_id_str:
            desc_match = re.search(r'Discord ID[:\s]+(\d+)', description, re.IGNORECASE)
            if desc_match:
                discord_id_str = desc_match.group(1)
        
        if not discord_id_str or discord_id_str == "No Discord":
            log.info(f"Native Whop message has no Discord ID: {title}")
            return
        
        # Extract numeric Discord ID
        discord_id_match = re.search(r'(\d{17,19})', discord_id_str)
        if not discord_id_match:
            log.warning(f"Could not extract valid Discord ID from: {discord_id_str}")
            return
        
        discord_id = discord_id_match.group(1)
        discord_user_id = int(discord_id)
        
        # Get guild and member
        guild = message.guild
        member = guild.get_member(discord_user_id)
        
        if not member:
            if _log_other:
                await _log_other(f"⚠️ **Whop Native:** Member {discord_user_id} not found in guild")
            # Still store in DB even if member not found
            member = None
        
        # Extract membership status and event type
        membership_status = parsed_data.get("membership_status", "") or fields_data.get("membership status", "")
        event_type = _determine_event_type_from_message(title, description, content, membership_status)
        
        # Email can come from parsed content/fields; use best-effort and never crash on missing.
        email_value = (
            parsed_data.get("email")
            or fields_data.get("membership status", {}).get("email", "") if isinstance(fields_data.get("membership status"), dict) else ""
            or fields_data.get("email")
            or fields_data.get("Email")
            or ""
        )

        # Cache identity mapping for enrichment (email -> discord_id)
        discord_username_value = (
            parsed_data.get("discord_username")
            or fields_data.get("discord username")
            or ""
        )
        _cache_identity(email_value, str(discord_user_id), str(discord_username_value))

        # Process role changes if member found
        if member:
            # Check for payment failed
            if "payment failed" in title.lower() or "payment failed" in description.lower():
                event_data = {
                    "event_type": "payment.failed",
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                    "amount": "N/A",
                    "failure_reason": "Payment failed",
                }
                await handle_payment_failed(member, event_data)

            # Check for cancel action
            elif "performing cancel" in description.lower() or "removeallroles" in description.lower():
                event_data = {
                    "event_type": "membership.deactivated",
                    "discord_user_id": str(discord_user_id),
                    "email": email_value,
                    "cancellation_reason": "Whop native cancel action",
                }
                await handle_membership_deactivated(member, event_data)

            # Check for membership status changes
            elif "membership update" in title.lower():
                if "past due" in membership_status.lower():
                    if _log_member_status:
                        await _log_member_status(f"⚠️ **Whop Native:** {_fmt_user(member)} - Membership Past Due")
                elif "active" in membership_status.lower():
                    event_data = {
                        "event_type": "membership.activated",
                        "discord_user_id": str(discord_user_id),
                        "email": email_value,
                        "status": "active",
                    }
                    await handle_membership_activated(member, event_data)

            else:
                log.info(f"Processed native Whop message: {title}")
        else:
            log.info(f"Native Whop message (member not in guild): {title}")
            
    except (ValueError, KeyError) as e:
        log.error(f"Error parsing native Whop message: {e}", exc_info=True)
        if _log_other:
            await _log_other(f"❌ **Whop Native Error:** Failed to parse message: {e}")


def _parse_whop_content(content: str) -> dict:
    """
    Parse Whop message content (text format, like whop_tracker.py).
    Handles format: Label on one line, value on next line.
    """
    if not content:
        return {}
    
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    
    def get_value_after(label: str) -> str:
        for i, line in enumerate(lines):
            if label in line and i + 1 < len(lines):
                return lines[i + 1]
        return ""
    
    discord_id_value = get_value_after("Discord ID")
    discord_id_match = re.search(r'(\d{17,19})', discord_id_value) if discord_id_value else None
    
    return {
        "discord_id": discord_id_match.group(1) if discord_id_match else "",
        "discord_username": get_value_after("Discord Username"),
        "whop_key": get_value_after("Key"),
        "access_pass": get_value_after("Access Pass"),
        "name": get_value_after("Name"),
        "email": get_value_after("Email"),
        "membership_status": get_value_after("Membership Status")
    }


def _determine_event_type_from_message(title: str, description: str, content: str, membership_status: str) -> str:
    """
    Determine event type from message (matching whop_tracker.py logic).
    Returns: 'new', 'renewal', 'cancellation', 'completed', or 'payment_failed'
    """
    title_lower = title.lower()
    desc_lower = description.lower()
    content_lower = content.lower()
    status_lower = membership_status.lower()
    
    if "payment failed" in title_lower or "payment failed" in desc_lower:
        return "payment_failed"
    elif "renewal" in content_lower or "renew" in content_lower or "renewal" in desc_lower:
        return "renewal"
    elif "cancel" in status_lower or "cancel" in content_lower or "cancel" in desc_lower or "removeallroles" in desc_lower:
        return "cancellation"
    elif "completed" in status_lower:
        return "completed"
    else:
        return "new"


# Event handlers - canonical owners for their respective event types
async def handle_membership_activated(member: discord.Member, event_data: dict):
    """Handle new active membership - assign Cleanup role and log with support card embed"""
    guild = member.guild
    
    cleanup_role = guild.get_role(ROLE_TRIGGER)
    
    if cleanup_role and cleanup_role not in member.roles:
        await member.add_roles(cleanup_role, reason="Whop: Membership activated")
        log.info(f"Assigned cleanup role to {member} for membership activation")
    
    if _log_member_status:
        embed = _build_support_card_embed(
            title="✅ Membership Activated",
            color=0x00FF00,  # Green
            member=member,
            event_data=event_data,
            guild=guild,
            roles_added=[cleanup_role.name] if cleanup_role else None
        )
        embed.description = "New membership activated from Whop."
        
        # Add trial info if available
        trial_days = _safe_get(event_data, "trial_period_days", "trial_days", "plan.trial_period_days", default="")
        if trial_days and trial_days != "—" and trial_days != "0":
            embed.add_field(name="Trial Days", value=trial_days, inline=True)
        
        is_first = _safe_get(event_data, "is_first_membership", "membership.is_first_membership", default="")
        if is_first and is_first != "—":
            embed.add_field(name="First Membership", value="Yes" if str(is_first).lower() == "true" else "No", inline=True)
        
        if cleanup_role and cleanup_role not in member.roles:
            embed.add_field(name="Next Step", value="Cleanup role assigned. Monitor early lifecycle to ensure onboarding completes.", inline=False)
        else:
            embed.add_field(name="Next Step", value="Member already had cleanup role. Verify they still have correct access.", inline=False)
        
        await _log_member_status("", embed=embed)


async def handle_membership_activated_pending(member: discord.Member, event_data: dict):
    """Handle pending membership activation - log with support card embed"""
    if _log_member_status:
        guild = member.guild if member else None
        embed = _build_support_card_embed(
            title="⏳ Membership Activated (Pending)",
            color=0xFFFF00,  # Yellow
            member=member,
            event_data=event_data,
            guild=guild
        )
        embed.description = "Membership activation is pending. Awaiting first payment or Whop confirmation."
        embed.add_field(name="Next Step", value="Verify payment status in Whop and confirm whether activation should complete.", inline=False)
        await _log_member_status("", embed=embed)


async def handle_membership_deactivated(member: discord.Member, event_data: dict):
    """Handle membership deactivation - remove Member and Welcome roles and log with support card embed"""
    guild = member.guild
    
    member_role = guild.get_role(ROLE_CANCEL_A)
    welcome_role = guild.get_role(ROLE_CANCEL_B)
    
    roles_to_remove = []
    if member_role and member_role in member.roles:
        roles_to_remove.append(member_role)
    if welcome_role and welcome_role in member.roles:
        roles_to_remove.append(welcome_role)
    
    if roles_to_remove:
        await member.remove_roles(*roles_to_remove, reason="Whop: Membership deactivated")
        log.info(f"Removed roles from {member} for membership deactivation")
    
    if _log_member_status:
        embed = _build_support_card_embed(
            title="🟧 Membership Deactivated",
            color=0xFFA500,  # Orange
            member=member,
            event_data=event_data,
            guild=guild,
            roles_removed=[r.name for r in roles_to_remove] if roles_to_remove else None
        )
        embed.description = "Membership was deactivated and access roles have been removed from this member."
        
        # Add cancellation reason if available
        cancellation_reason = _safe_get(event_data, "membership.cancellation_reason", "cancellation_reason", default="")
        if cancellation_reason and cancellation_reason != "—":
            embed.add_field(name="Cancellation Reason", value=cancellation_reason, inline=False)
        
        canceled_at = _safe_get(event_data, "membership.canceled_at", "canceled_at", default="")
        if canceled_at and canceled_at != "—":
            canceled_fmt = _fmt_discord_ts(canceled_at, "D")
            embed.add_field(name="Canceled At", value=canceled_fmt, inline=True)
        
        if roles_to_remove:
            embed.add_field(
                name="Next Step",
                value="Confirm cancellation reason in Whop and ensure this member should remain without access.",
                inline=False,
            )
        
        await _log_member_status("", embed=embed)


async def handle_membership_deactivated_payment_failure(member: discord.Member, event_data: dict):
    """Handle payment failure deactivation"""
    await handle_membership_deactivated(member, event_data)


async def handle_payment_renewal(member: discord.Member, event_data: dict):
    """Handle payment renewal - ensure Member role is assigned and log with support card embed"""
    guild = member.guild
    member_role = guild.get_role(ROLE_CANCEL_A)
    
    if member_role and member_role not in member.roles:
        await member.add_roles(member_role, reason="Whop: Payment renewal")
        log.info(f"Assigned Member role to {member} for payment renewal")
    
    if _log_member_status:
        embed = _build_support_card_embed(
            title="✅ Payment Renewed",
            color=0x00FF00,  # Green
            member=member,
            event_data=event_data,
            guild=guild
        )
        embed.description = "Recurring Whop payment succeeded. Member access remains active."
        embed.add_field(
            name="Next Step",
            value="No immediate action required. Spot-check renewal schedule if this member has a complex plan.",
            inline=False,
        )
        await _log_member_status("", embed=embed)


async def handle_payment_activation(member: discord.Member, event_data: dict):
    """Handle first payment - assign Member role and log with support card embed"""
    guild = member.guild
    member_role = guild.get_role(ROLE_CANCEL_A)
    
    if member_role and member_role not in member.roles:
        await member.add_roles(member_role, reason="Whop: Payment activation")
        log.info(f"Assigned Member role to {member} for payment activation")
    
    if _log_member_status:
        embed = _build_support_card_embed(
            title="✅ Payment Activated",
            color=0x00FF00,  # Green
            member=member,
            event_data=event_data,
            guild=guild,
            roles_added=[member_role.name] if member_role else None
        )
        embed.description = "First Whop payment succeeded. Member role has been assigned."
        embed.add_field(
            name="Next Step",
            value="Confirm onboarding is complete and that the member can see all paid channels.",
            inline=False,
        )
        await _log_member_status("", embed=embed)


async def handle_payment_failed(member: discord.Member, event_data: dict):
    """Handle payment failure - log with support card embed"""
    if _log_member_status:
        guild = member.guild if member else None
        embed = _build_support_card_embed(
            title="❌ Payment Failed — Action Needed",
            color=0xFF6B6B,  # Red
            member=member,
            event_data=event_data,
            guild=guild,
            flags=["Billing issue (access risk)"]
        )
        embed.description = "A Whop payment failed for this member. Review billing and take follow-up action."
        
        # Add payment-specific fields
        failure_reason = _safe_get(event_data, "failure_reason", "payment.failure_reason", default="Unknown")
        embed.add_field(name="Failure Reason", value=failure_reason, inline=False)
        
        # Renewal window if available
        renewal_start = _safe_get(event_data, "membership.renewal_period_start", "renewal_period_start", default="")
        renewal_end = _safe_get(event_data, "membership.renewal_period_end", "renewal_period_end", default="")
        if renewal_start != "—" and renewal_end != "—":
            start_fmt = _fmt_discord_ts(renewal_start, "D")
            end_fmt = _fmt_discord_ts(renewal_end, "D")
            # Avoid placeholders like "— → —" if parsing fails
            if start_fmt != "—" and end_fmt != "—":
                embed.add_field(name="Renewal Window", value=f"{start_fmt} → {end_fmt}", inline=False)
        
        embed.add_field(
            name="Next Step",
            value="Check the member's Whop invoice, confirm card status, and decide whether to retry, grace, or revoke access.",
            inline=False,
        )
        
        await _log_member_status("", embed=embed)


async def handle_payment_refunded(member: discord.Member, event_data: dict):
    """Handle payment refund - remove Member role"""
    await handle_membership_deactivated(member, event_data)


async def handle_waitlist_approved(member: discord.Member, event_data: dict):
    """Handle waitlist approval - same as membership activated"""
    await handle_membership_activated(member, event_data)

