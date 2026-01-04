#!/usr/bin/env python3
"""
Create sample output showing the enhanced RSCheckerbot features from this chat thread.
This demonstrates what the system does after our updates.
"""

import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

# Sample data based on the enhancements we made
SAMPLE_OUTPUT = {
    "enhancements_summary": {
        "title": "RSCheckerbot Enhancement Summary",
        "date": "2026-01-03",
        "features": [
            {
                "name": "Identity Caching System",
                "description": "Caches email → Discord ID mappings from native Whop embeds",
                "file": "RSCheckerbot/whop_identity_cache.json",
                "example": {
                    "email": "user@example.com",
                    "discord_id": "123456789012345678",
                    "discord_username": "username#1234",
                    "last_seen": "2026-01-03T12:00:00+00:00"
                }
            },
            {
                "name": "Lookup Request System",
                "description": "Posts 'Lookup Needed' messages when workflow events lack Discord ID",
                "channel": "#member-status-logs",
                "format": "Embed with email, Whop user ID, instructions to check #whop-logs"
            },
            {
                "name": "Trial Abuse Detection",
                "description": "Tracks repeat trial usage patterns and alerts on suspicious activity",
                "file": "RSCheckerbot/trial_history.json",
                "alert_conditions": [
                    "is_first_membership=false AND trial_days>0 (strong signal)",
                    "Multiple trial events from same email/discord_id (weak signal, only when Discord ID known)"
                ]
            },
            {
                "name": "Member History Tracking",
                "description": "Tracks join/leave timestamps and join count for membership lifecycle",
                "file": "RSCheckerbot/member_history.json",
                "fields": ["first_join_ts", "last_join_ts", "last_leave_ts", "join_count"]
            },
            {
                "name": "Enhanced Support Card Embeds",
                "description": "All events now use rich Discord embeds with structured fields",
                "events": [
                    "Payment Failed (Action Needed)",
                    "Payment Succeeded",
                    "Membership Activated",
                    "Membership Cancelled",
                    "Lookup Needed",
                    "Trial Abuse Signal",
                    "Member Joined/Left"
                ],
                "fields": [
                    "Member mention + Discord ID",
                    "Identity (Username, Name, Email, Total spent)",
                    "Membership (Status, ID, Trial days, etc.)",
                    "Renewal window (human-readable dates)",
                    "Actions (Dashboard, Manage, Checkout links)"
                ]
            },
            {
                "name": "Webhook Interception",
                "description": "Intercepts raw Whop webhook payloads before forwarding to Discord",
                "endpoint": "/whop-webhook",
                "log_file": "RSCheckerbot/whop_webhook_raw_payloads.json",
                "function": "Logs raw payloads for inspection, then forwards to Discord webhook"
            },
            {
                "name": "SQLite to JSON Migration",
                "description": "Migrated RSAdminBot whop_tracker from SQLite to JSON storage",
                "old_file": "RSAdminBot/whop_data/whop_history.db (DELETED)",
                "new_file": "RSAdminBot/whop_data/whop_history.json",
                "compliant": "CANONICAL_RULES.md - JSON-only storage"
            }
        ]
    },
    "sample_embeds": {
        "payment_failed": {
            "title": "Payment Failed (Action Needed)",
            "color": 15158332,  # Red
            "fields": [
                {"name": "Member", "value": "<@123456789012345678>", "inline": False},
                {"name": "User ID", "value": "123456789012345678", "inline": False},
                {"name": "Identity", "value": "• Username: user123\n• Name: User Name\n• Email: user@example.com\n• Total spent: $120", "inline": False},
                {"name": "Membership", "value": "• Membership ID: mem_abc123\n• Status: past_due\n• First membership: false", "inline": False},
                {"name": "Renewal Window", "value": "<t:1735890000:D> → <t:1738492800:D>", "inline": False},
                {"name": "Actions", "value": "• [Dashboard](https://whop.com/dashboard/...)\n• [Manage](https://whop.com/billing/manage/...)\n• [Checkout](https://dash.resellingsecrets.com/checkout/...)", "inline": False}
            ],
            "footer": "RSCheckerbot • Member Status Tracking"
        },
        "lookup_needed": {
            "title": "Lookup Needed (Whop → Discord)",
            "color": 16776960,  # Yellow
            "fields": [
                {"name": "Event", "value": "payment.failed", "inline": False},
                {"name": "Email", "value": "user@example.com", "inline": False},
                {"name": "Whop User ID", "value": "user_abc123", "inline": False},
                {"name": "Membership ID", "value": "mem_xyz789", "inline": False},
                {"name": "Instructions", "value": "Check <#1076440941814091787> native Whop event for Discord ID", "inline": False}
            ],
            "footer": "RSCheckerbot • Identity Resolution"
        },
        "trial_abuse": {
            "title": "🚩 Trial Abuse Signal",
            "color": 15158332,  # Red
            "fields": [
                {"name": "Member", "value": "<@123456789012345678>", "inline": False},
                {"name": "Email", "value": "user@example.com", "inline": False},
                {"name": "Reason", "value": "Trial started but is_first_membership=false (repeat trial likely)", "inline": False}
            ],
            "footer": "RSCheckerbot • Trial Abuse Detection"
        },
        "member_joined": {
            "title": "👋 Member Joined",
            "color": 3066993,  # Green
            "fields": [
                {"name": "Member", "value": "<@123456789012345678>", "inline": False},
                {"name": "User ID", "value": "123456789012345678", "inline": False},
                {"name": "Member Since", "value": "<t:1735890000:D> (<t:1735890000:R>)", "inline": False},
                {"name": "Join Count", "value": "2 (Returning member)", "inline": False},
                {"name": "Last Left", "value": "<t:1736200000:D>", "inline": False}
            ],
            "footer": "RSCheckerbot • Member Lifecycle Tracking"
        }
    },
    "file_structures": {
        "member_history.json": {
            "585588388761960460": {
                "first_join_ts": 1735890000,
                "last_join_ts": 1735890000,
                "last_leave_ts": None,
                "join_count": 1
            }
        },
        "whop_identity_cache.json": {
            "user@example.com": {
                "discord_id": "123456789012345678",
                "discord_username": "username#1234",
                "last_seen": "2026-01-03T12:00:00+00:00"
            }
        },
        "trial_history.json": {
            "user@example.com|123456789012345678": {
                "email": "user@example.com",
                "discord_id": "123456789012345678",
                "events": [
                    {
                        "ts": "2026-01-03T12:00:00+00:00",
                        "event_type": "membership.activated",
                        "membership_id": "mem_abc123",
                        "trial_days": "7",
                        "is_first_membership": "false"
                    }
                ]
            }
        },
        "whop_history.json": {
            "membership_events": [],
            "membership_timeline": [],
            "migrated_at": "2026-01-04T00:06:19.060573+00:00"
        }
    },
    "discord_timestamp_formats": {
        "full_date": "<t:1735890000:F> → August 11, 2025 4:40 PM",
        "short_date": "<t:1735890000:D> → Aug 11, 2025",
        "relative": "<t:1735890000:R> → 3 months ago"
    }
}

def create_sample_output_json(output_path: Path):
    """Create a JSON file with sample output."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(SAMPLE_OUTPUT, f, indent=2, ensure_ascii=False)
    print(f"[OK] Created sample output: {output_path}")

if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    output_path = script_dir / "enhancement_sample_output.json"
    create_sample_output_json(output_path)
    print("\n[OK] Sample output created successfully!")
    print("This file shows all enhancements made to RSCheckerbot in this chat thread.")

