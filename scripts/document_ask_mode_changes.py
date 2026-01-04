#!/usr/bin/env python3
"""
Document Ask Mode Changes
-------------------------
Generates a markdown report of all changes made in ask mode session.
Lists all modified files, new features, and enhancements.
"""

import json
from pathlib import Path
from datetime import datetime, timezone

# Add repo root to path
_REPO_ROOT = Path(__file__).resolve().parents[1]

# Output file
OUTPUT_FILE = _REPO_ROOT / "docs" / "ASK_MODE_CHANGES_REPORT.md"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# Changes documented
CHANGES = {
    "session_date": datetime.now(timezone.utc).isoformat(),
    "summary": {
        "title": "Ask Mode Changes - RSCheckerbot & RSAdminBot Enhancements",
        "description": "Comprehensive enhancements to RSCheckerbot and RSAdminBot for membership lifecycle tracking, identity caching, trial abuse detection, and improved embed formatting.",
    },
    "rcheckerbot_changes": {
        "title": "RSCheckerbot Enhancements",
        "files_modified": [
            "RSCheckerbot/main.py",
            "RSCheckerbot/whop_webhook_handler.py",
            "RSCheckerbot/config.json",
        ],
        "new_files": [
            "RSCheckerbot/member_history.json (runtime data)",
            "RSCheckerbot/whop_identity_cache.json (runtime data)",
            "RSCheckerbot/trial_history.json (runtime data)",
            "RSCheckerbot/whop_webhook_raw_payloads.json (runtime data)",
        ],
        "features": [
            {
                "name": "Identity Caching System",
                "description": "Caches email → Discord ID mappings from native Whop embeds",
                "file": "RSCheckerbot/whop_webhook_handler.py",
                "functions": ["_cache_identity()", "_lookup_identity()"],
                "data_file": "whop_identity_cache.json",
            },
            {
                "name": "Lookup Request System",
                "description": "Posts 'Lookup Needed' messages to #member-status-logs when workflow events lack Discord ID",
                "file": "RSCheckerbot/whop_webhook_handler.py",
                "functions": ["_send_lookup_request()"],
                "channel": "#member-status-logs (1452835008170426368)",
            },
            {
                "name": "Trial Abuse Detection",
                "description": "Tracks repeat trial usage patterns and alerts on suspicious activity",
                "file": "RSCheckerbot/whop_webhook_handler.py",
                "functions": ["_record_trial_event()"],
                "data_file": "trial_history.json",
                "alert_conditions": [
                    "is_first_membership=false AND trial_days>0 (strong signal)",
                    "Multiple trial events from same email/discord_id (weak signal, only when Discord ID known)",
                ],
            },
            {
                "name": "Member History Tracking",
                "description": "Tracks join/leave timestamps and join counts for membership lifecycle",
                "file": "RSCheckerbot/main.py",
                "functions": ["_touch_join()", "_touch_leave()", "get_member_history()"],
                "data_file": "member_history.json",
                "events": ["on_member_join", "on_member_remove"],
            },
            {
                "name": "Enhanced Support Card Embeds",
                "description": "All events now use rich Discord embeds with structured fields",
                "file": "RSCheckerbot/whop_webhook_handler.py",
                "functions": ["_build_support_card_embed()", "_safe_get()", "_fmt_discord_ts()"],
                "events": [
                    "Payment Failed (Action Needed)",
                    "Payment Succeeded",
                    "Membership Activated",
                    "Membership Cancelled",
                    "Lookup Needed",
                    "Trial Abuse Signal",
                    "Member Joined/Left",
                ],
            },
            {
                "name": "Webhook Interception",
                "description": "Intercepts raw Whop webhook payloads before forwarding to Discord",
                "file": "RSCheckerbot/main.py",
                "functions": ["_save_raw_webhook_payload()", "handle_whop_webhook_receiver()"],
                "endpoint": "/whop-webhook",
                "data_file": "whop_webhook_raw_payloads.json",
            },
        ],
    },
    "rsadminbot_changes": {
        "title": "RSAdminBot Enhancements",
        "files_modified": [
            "RSAdminBot/whop_tracker.py",
            "RSAdminBot/admin_bot.py",
            "RSAdminBot/migrate_whop_db_to_json.py",
        ],
        "files_deleted": [
            "RSAdminBot/whop_data/whop_history.db (migrated to JSON)",
        ],
        "features": [
            {
                "name": "Enhanced !whopstats Command",
                "description": "Updated to use rich embed format matching RSCheckerbot support card style",
                "file": "RSAdminBot/admin_bot.py",
                "command": "!whopstats",
                "changes": [
                    "Structured fields with inline formatting",
                    "Event Breakdown section with bullet points",
                    "Bold formatting for numbers",
                    "Timestamp added to embed",
                    "Footer style: 'RSAdminBot • Whop Statistics | Data source: whop_history.json | Run !whopscan to update'",
                ],
            },
            {
                "name": "SQLite to JSON Migration",
                "description": "Migrated whop_tracker from SQLite to JSON storage (CANONICAL_RULES compliant)",
                "file": "RSAdminBot/whop_tracker.py",
                "migration_script": "RSAdminBot/migrate_whop_db_to_json.py",
                "old_file": "whop_data/whop_history.db (deleted)",
                "new_file": "whop_data/whop_history.json",
                "compliance": "CANONICAL_RULES.md - JSON-only storage",
            },
            {
                "name": "Fixed Parser",
                "description": "Updated _parse_whop_message() to handle embed-based messages (not just plain text)",
                "file": "RSAdminBot/whop_tracker.py",
                "functions": ["_parse_whop_message()", "_parse_from_embed()"],
                "improvements": [
                    "Parses Discord embed fields",
                    "Falls back to plain text parsing (legacy format)",
                    "Skips messages with 'No Discord' in Discord ID field",
                ],
            },
        ],
    },
    "cleanup": {
        "title": "Files Cleaned Up",
        "removed_files": [
            "RSAdminBot/create_sample_output.py",
            "RSAdminBot/post_sample_output_to_discord.py",
            "RSAdminBot/generate_live_sample_output.py",
            "RSAdminBot/enhancement_sample_output.json",
            "RSAdminBot/whopstats_live_sample.json",
        ],
        "reason": "Temporary sample files removed per user request",
    },
    "canonical_rules_compliance": {
        "title": "CANONICAL_RULES Compliance",
        "items": [
            "JSON-only storage (no SQLite databases)",
            "No parallel logic (enhancements replace existing code)",
            "No hardcoded values (configuration-driven)",
            "One source of truth (single implementation per feature)",
            "Enhancements upgrade existing paths (old code removed)",
            "Comments updated (database → JSON file references)",
        ],
    },
}


def generate_markdown_report() -> str:
    """Generate markdown report from changes data"""
    lines = []
    
    # Header
    lines.append(f"# {CHANGES['summary']['title']}")
    lines.append("")
    lines.append(f"**Generated:** {CHANGES['session_date']}")
    lines.append("")
    lines.append(CHANGES['summary']['description'])
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # RSCheckerbot Changes
    rc = CHANGES['rcheckerbot_changes']
    lines.append(f"## {rc['title']}")
    lines.append("")
    lines.append("### Files Modified")
    for file in rc['files_modified']:
        lines.append(f"- `{file}`")
    lines.append("")
    lines.append("### New Runtime Data Files")
    for file in rc['new_files']:
        lines.append(f"- `{file}`")
    lines.append("")
    lines.append("### Features Added")
    for feature in rc['features']:
        lines.append(f"#### {feature['name']}")
        lines.append("")
        lines.append(f"{feature['description']}")
        lines.append("")
        lines.append(f"- **File:** `{feature['file']}`")
        if 'functions' in feature:
            lines.append(f"- **Functions:** {', '.join(f'`{f}()`' for f in feature['functions'])}")
        if 'data_file' in feature:
            lines.append(f"- **Data File:** `{feature['data_file']}`")
        if 'channel' in feature:
            lines.append(f"- **Channel:** {feature['channel']}")
        if 'endpoint' in feature:
            lines.append(f"- **Endpoint:** `{feature['endpoint']}`")
        if 'events' in feature:
            lines.append("- **Events:**")
            for event in feature['events']:
                lines.append(f"  - {event}")
        if 'alert_conditions' in feature:
            lines.append("- **Alert Conditions:**")
            for condition in feature['alert_conditions']:
                lines.append(f"  - {condition}")
        lines.append("")
    
    # RSAdminBot Changes
    ra = CHANGES['rsadminbot_changes']
    lines.append(f"## {ra['title']}")
    lines.append("")
    lines.append("### Files Modified")
    for file in ra['files_modified']:
        lines.append(f"- `{file}`")
    lines.append("")
    if 'files_deleted' in ra:
        lines.append("### Files Deleted/Migrated")
        for file in ra['files_deleted']:
            lines.append(f"- `{file}`")
        lines.append("")
    lines.append("### Features Added/Enhanced")
    for feature in ra['features']:
        lines.append(f"#### {feature['name']}")
        lines.append("")
        lines.append(f"{feature['description']}")
        lines.append("")
        lines.append(f"- **File:** `{feature['file']}`")
        if 'command' in feature:
            lines.append(f"- **Command:** `{feature['command']}`")
        if 'functions' in feature:
            lines.append(f"- **Functions:** {', '.join(f'`{f}()`' for f in feature['functions'])}")
        if 'migration_script' in feature:
            lines.append(f"- **Migration Script:** `{feature['migration_script']}`")
        if 'old_file' in feature:
            lines.append(f"- **Old File:** `{feature['old_file']}`")
        if 'new_file' in feature:
            lines.append(f"- **New File:** `{feature['new_file']}`")
        if 'compliance' in feature:
            lines.append(f"- **Compliance:** {feature['compliance']}")
        if 'improvements' in feature:
            lines.append("- **Improvements:**")
            for improvement in feature['improvements']:
                lines.append(f"  - {improvement}")
        if 'changes' in feature:
            lines.append("- **Changes:**")
            for change in feature['changes']:
                lines.append(f"  - {change}")
        lines.append("")
    
    # Cleanup
    cleanup = CHANGES['cleanup']
    lines.append(f"## {cleanup['title']}")
    lines.append("")
    lines.append(f"**Reason:** {cleanup['reason']}")
    lines.append("")
    for file in cleanup['removed_files']:
        lines.append(f"- `{file}`")
    lines.append("")
    
    # Compliance
    compliance = CHANGES['canonical_rules_compliance']
    lines.append(f"## {compliance['title']}")
    lines.append("")
    for item in compliance['items']:
        lines.append(f"- ✅ {item}")
    lines.append("")
    
    return "\n".join(lines)


def main():
    """Generate documentation report"""
    print(f"Generating documentation report...")
    
    markdown = generate_markdown_report()
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(markdown)
    
    print(f"✅ Documentation report saved to: {OUTPUT_FILE}")
    print(f"   Total lines: {len(markdown.splitlines())}")


if __name__ == "__main__":
    main()

