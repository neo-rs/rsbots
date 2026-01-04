#!/usr/bin/env python3
"""
Generate Sample Embeds from Scanned Data
-----------------------------------------
Reads whop_history.json from downloaded Oracle server data and generates
sample embed outputs for each event type (new, renewal, cancellation).
Shows what RSCheckerbot embeds would look like based on real scanned data.
"""

import json
import sys
import re
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# Add repo root to path
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

DATA_DIR = _REPO_ROOT / "OracleServerData"
WHOP_HISTORY_PATH = DATA_DIR / "RSAdminBot" / "whop_data" / "whop_history.json"
OUTPUT_FILE = DATA_DIR / "sample_embeds_output.json"
REPORT_FILE = DATA_DIR / "sample_embeds_report.md"


def load_whop_history() -> dict | None:
    """Load whop_history.json from downloaded data"""
    if not WHOP_HISTORY_PATH.exists():
        print(f"❌ {WHOP_HISTORY_PATH} not found")
        print("   Run scripts/sync_oracle_runtime_data.py first")
        return None
    
    try:
        with open(WHOP_HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading {WHOP_HISTORY_PATH}: {e}")
        return None


def _norm_text(s: str) -> str:
    return (s or "").strip()


def _norm_status(s: str) -> str:
    return _norm_text(s).lower()


def _norm_username(s: str) -> str:
    """Normalize a stored discord_username so sample output doesn't duplicate mentions."""
    s = _norm_text(s)
    if not s:
        return ""
    # Remove raw mentions like <@123> or <@!123>
    s = re.sub(r"<@!?\d+>", "", s).strip()
    # Remove trailing parentheses like "name (....)"
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s).strip()
    return s


def pick_sample_event(event_type: str, events: list[dict]) -> dict:
    """Pick a representative sample event for a given type.

    Avoids misleading samples (e.g., renewal events where membership_status is 'Canceled').
    Prefers events with a discord_id present and status matching the type.
    """
    et = _norm_text(event_type).lower()
    # Iterate from newest to oldest to get a more current representative event.
    candidates = [e for e in reversed(events) if isinstance(e, dict)]

    def status_ok(ev: dict) -> bool:
        st = _norm_status(ev.get("membership_status", ""))
        if et in ("cancellation",):
            return ("cancel" in st) or (st == "canceled")
        if et in ("completed",):
            return "completed" in st
        if et in ("new", "renewal"):
            # Prefer non-canceled / non-completed states for positive events.
            if "cancel" in st or "completed" in st:
                return False
            return True if st else True
        return True

    # 1) Prefer events with discord_id and a matching status
    for ev in candidates:
        if _norm_text(ev.get("discord_id", "")) and status_ok(ev):
            return ev
    # 2) Next prefer any event with discord_id
    for ev in candidates:
        if _norm_text(ev.get("discord_id", "")):
            return ev
    # 3) Fallback
    return candidates[0] if candidates else (events[0] if events else {})


def generate_embed_preview(event_type: str, sample_event: dict) -> dict:
    """Generate embed preview structure for a sample event"""
    discord_id = sample_event.get("discord_id", "")
    discord_username = _norm_username(sample_event.get("discord_username", ""))
    name = _norm_text(sample_event.get("name", ""))
    email = _norm_text(sample_event.get("email", ""))
    membership_status = _norm_text(sample_event.get("membership_status", ""))
    whop_key = _norm_text(sample_event.get("whop_key", ""))
    access_pass = _norm_text(sample_event.get("access_pass", ""))
    timestamp = _norm_text(sample_event.get("timestamp", ""))
    
    # Determine embed title and color based on event type
    if event_type == "new":
        title = "✅ Membership Activated"
        color = 0x00FF00  # Green
    elif event_type == "renewal":
        title = "✅ Payment Renewed"
        color = 0x00FF00  # Green
    elif event_type == "cancellation":
        title = "🟧 Membership Deactivated"
        color = 0xFFA500  # Orange
    elif event_type == "completed":
        title = "✅ Membership Completed"
        color = 0x00FF00  # Green
    else:
        title = f"ℹ️ Membership Event ({event_type})"
        color = 0x5865F2  # Blurple
    
    # Build embed structure
    embed = {
        "title": title,
        "color": color,
        "color_hex": hex(color),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [],
    }
    
    # Member field
    if discord_id:
        embed["fields"].append({
            "name": "Member",
            "value": f"<@{discord_id}>",
            "inline": False
        })
        embed["fields"].append({
            "name": "User ID",
            "value": discord_id,
            "inline": False
        })
    else:
        embed["fields"].append({
            "name": "User ID",
            "value": "N/A",
            "inline": False
        })
    
    # Identity section
    identity_lines = []
    if discord_username:
        identity_lines.append(f"• Username: {discord_username}")
    if name and name != discord_username:
        identity_lines.append(f"• Name: {name}")
    if email:
        identity_lines.append(f"• Email: `{email}`")
    
    if identity_lines:
        embed["fields"].append({
            "name": "Identity",
            "value": "\n".join(identity_lines),
            "inline": False
        })
    
    # Membership section
    membership_lines = []
    if membership_status:
        membership_lines.append(f"• Status: {membership_status}")
    if whop_key:
        membership_lines.append(f"• Key: {whop_key}")
    if access_pass:
        membership_lines.append(f"• Access Pass: {access_pass}")
    
    if membership_lines:
        embed["fields"].append({
            "name": "Membership",
            "value": "\n".join(membership_lines),
            "inline": False
        })
    
    # Event-specific fields
    if event_type == "cancellation":
        embed["fields"].append({
            "name": "Cancellation Details",
            "value": f"• Event Type: {event_type}\n• Membership Status: {membership_status}",
            "inline": False
        })
    
    embed["footer"] = "RSCheckerbot • Member Status Tracking"
    
    return embed


def generate_text_preview(event_type: str, sample_event: dict, embed_data: dict) -> str:
    """Generate text preview of embed"""
    lines = []
    
    lines.append("=" * 80)
    lines.append(f"EVENT TYPE: {event_type.upper()}")
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Title: {embed_data['title']}")
    lines.append(f"Color: {embed_data['color_hex']}")
    lines.append(f"Timestamp: {embed_data['timestamp']}")
    lines.append("")
    lines.append("Fields:")
    lines.append("-" * 80)
    
    for field in embed_data["fields"]:
        lines.append(f"{field['name']}:")
        lines.append(f"  {field['value']}")
        lines.append("")
    
    lines.append(f"Footer: {embed_data['footer']}")
    lines.append("")
    lines.append("=" * 80)
    lines.append("")
    
    return "\n".join(lines)


def main():
    """Main function"""
    print("Generating sample embed outputs from scanned data...")
    print(f"Source: {WHOP_HISTORY_PATH}")
    print("")
    
    data = load_whop_history()
    if not data:
        sys.exit(1)
    
    events = data.get("membership_events", [])
    if not events:
        print("❌ No events found in whop_history.json")
        sys.exit(1)
    
    # Group events by type
    events_by_type = defaultdict(list)
    for event in events:
        event_type = event.get("event_type", "unknown")
        events_by_type[event_type].append(event)
    
    print(f"Total events: {len(events)}")
    print(f"Event types: {', '.join(sorted(events_by_type.keys()))}")
    print("")
    
    # Generate samples
    samples = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(WHOP_HISTORY_PATH),
        "total_events": len(events),
        "event_type_counts": {k: len(v) for k, v in events_by_type.items()},
        "sample_embeds": {},
    }
    
    text_report_lines = []
    text_report_lines.append("# Sample Embed Outputs from Scanned Whop Data")
    text_report_lines.append("")
    text_report_lines.append(f"**Generated:** {samples['generated_at']}")
    text_report_lines.append(f"**Source:** {samples['source_file']}")
    text_report_lines.append(f"**Total Events:** {samples['total_events']}")
    text_report_lines.append("")
    text_report_lines.append("---")
    text_report_lines.append("")
    
    # Generate sample for each event type
    for event_type in ["new", "renewal", "cancellation", "completed"]:
        if event_type not in events_by_type:
            continue
        
        sample_event = pick_sample_event(event_type, events_by_type[event_type])
        
        # Generate embed structure
        embed_data = generate_embed_preview(event_type, sample_event)
        samples["sample_embeds"][event_type] = {
            "sample_event_data": sample_event,
            "embed": embed_data,
        }
        
        # Generate text preview
        text_preview = generate_text_preview(event_type, sample_event, embed_data)
        text_report_lines.append(text_preview)
    
    # Summary
    text_report_lines.append("## Summary by Event Type")
    text_report_lines.append("")
    text_report_lines.append("-" * 80)
    for event_type, events_list in sorted(events_by_type.items()):
        count = len(events_list)
        percentage = (count / len(events)) * 100
        text_report_lines.append(f"  {event_type}: {count} events ({percentage:.1f}%)")
    text_report_lines.append("")
    
    # Save JSON output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] JSON output saved to: {OUTPUT_FILE}")
    
    # Save markdown report
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(text_report_lines))
    
    print(f"[OK] Markdown report saved to: {REPORT_FILE}")
    print("")
    print("Sample embed generation complete!")


if __name__ == "__main__":
    main()

