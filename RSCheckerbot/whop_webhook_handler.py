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

# Identity tracking and trial abuse detection
MEMBER_STATUS_LOGS_CHANNEL_ID = None

# File paths for JSON storage (canonical: JSON-only, no SQLite)
IDENTITY_CACHE_FILE = Path(__file__).resolve().parent / "whop_identity_cache.json"
TRIAL_CACHE_FILE = Path(__file__).resolve().parent / "trial_history.json"


def _norm_email(s: str) -> str:
    """Normalize email address for consistent storage/lookup"""
    return (s or "").strip().lower()

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


def initialize(webhook_channel_id, whop_logs_channel_id, role_trigger, welcome_role_id, role_cancel_a, role_cancel_b,
               log_other_func, log_member_status_func, fmt_user_func, member_status_logs_channel_id=None):
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
    """
    global WHOP_WEBHOOK_CHANNEL_ID, WHOP_LOGS_CHANNEL_ID, ROLE_TRIGGER, WELCOME_ROLE_ID, ROLE_CANCEL_A, ROLE_CANCEL_B
    global _log_other, _log_member_status, _fmt_user
    global MEMBER_STATUS_LOGS_CHANNEL_ID
    
    WHOP_WEBHOOK_CHANNEL_ID = webhook_channel_id
    WHOP_LOGS_CHANNEL_ID = whop_logs_channel_id
    ROLE_TRIGGER = role_trigger
    WELCOME_ROLE_ID = welcome_role_id
    ROLE_CANCEL_A = role_cancel_a
    ROLE_CANCEL_B = role_cancel_b
    _log_other = log_other_func
    _log_member_status = log_member_status_func
    _fmt_user = fmt_user_func
    MEMBER_STATUS_LOGS_CHANNEL_ID = member_status_logs_channel_id
    
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
            member = guild.get_member(int(discord_user_id))
        except ValueError:
            log.error(f"Invalid discord_user_id format: {discord_user_id}")
            if _log_other:
                await _log_other(f"❌ **Whop Webhook Error:** Invalid discord_user_id format: `{discord_user_id}`")
            return
        
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
    """Handle new active membership - assign Cleanup role only"""
    guild = member.guild
    
    cleanup_role = guild.get_role(ROLE_TRIGGER)
    
    if cleanup_role and cleanup_role not in member.roles:
        await member.add_roles(cleanup_role, reason="Whop: Membership activated")
        if _log_other:
            await _log_other(f"✅ **Whop Webhook:** Assigned cleanup role to {_fmt_user(member)} - Membership activated")
        log.info(f"Assigned cleanup role to {member} for membership activation")
    else:
        if _log_other:
            await _log_other(f"ℹ️ **Whop Webhook:** {_fmt_user(member)} already has cleanup role")


async def handle_membership_activated_pending(member: discord.Member, event_data: dict):
    """Handle pending membership activation - just log"""
    if _log_other:
        await _log_other(f"⏳ **Whop Webhook:** {_fmt_user(member)} - Membership activated (pending status)")


async def handle_membership_deactivated(member: discord.Member, event_data: dict):
    """Handle membership deactivation - remove Member and Welcome roles"""
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
        if _log_member_status:
            await _log_member_status(f"📉 **Whop Webhook:** Removed roles from {_fmt_user(member)} - Membership deactivated")
        log.info(f"Removed roles from {member} for membership deactivation")


async def handle_membership_deactivated_payment_failure(member: discord.Member, event_data: dict):
    """Handle payment failure deactivation"""
    await handle_membership_deactivated(member, event_data)


async def handle_payment_renewal(member: discord.Member, event_data: dict):
    """Handle payment renewal - ensure Member role is assigned"""
    guild = member.guild
    member_role = guild.get_role(ROLE_CANCEL_A)
    
    if member_role and member_role not in member.roles:
        await member.add_roles(member_role, reason="Whop: Payment renewal")
        if _log_member_status:
            await _log_member_status(f"✅ **Whop Webhook:** Assigned Member role to {_fmt_user(member)} - Payment renewal")
        log.info(f"Assigned Member role to {member} for payment renewal")


async def handle_payment_activation(member: discord.Member, event_data: dict):
    """Handle first payment - assign Member role"""
    guild = member.guild
    member_role = guild.get_role(ROLE_CANCEL_A)
    
    if member_role and member_role not in member.roles:
        await member.add_roles(member_role, reason="Whop: Payment activation")
        if _log_member_status:
            await _log_member_status(f"✅ **Whop Webhook:** Assigned Member role to {_fmt_user(member)} - First payment")
        log.info(f"Assigned Member role to {member} for payment activation")


async def handle_payment_failed(member: discord.Member, event_data: dict):
    """Handle payment failure - log only"""
    amount = event_data.get('amount', 'N/A')
    reason = event_data.get('failure_reason', 'Unknown')
    if _log_member_status:
        await _log_member_status(f"❌ **Whop Webhook:** Payment failed for {_fmt_user(member)} - ${amount} - {reason}")


async def handle_payment_refunded(member: discord.Member, event_data: dict):
    """Handle payment refund - remove Member role"""
    await handle_membership_deactivated(member, event_data)


async def handle_waitlist_approved(member: discord.Member, event_data: dict):
    """Handle waitlist approval - same as membership activated"""
    await handle_membership_activated(member, event_data)

