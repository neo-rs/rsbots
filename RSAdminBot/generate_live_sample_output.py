#!/usr/bin/env python3
"""
Generate live sample output from actual whopstats data.
This shows what the embed looks like with real data from whop_history.json
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add repo root to path
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

def load_live_stats():
    """Load stats from whop_history.json (if available locally) or use sample data"""
    data_dir = Path(__file__).resolve().parent / "whop_data"
    json_path = data_dir / "whop_history.json"
    
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            events = data.get("membership_events", [])
            timeline = data.get("membership_timeline", [])
            
            # Calculate stats (matching whop_tracker.py logic)
            unique_members = set(e.get("discord_id") for e in events if e.get("discord_id"))
            total_members = len(unique_members)
            new_members = sum(1 for e in events if e.get("event_type") == "new")
            renewals = sum(1 for e in events if e.get("event_type") == "renewal")
            cancellations = sum(1 for e in events if e.get("event_type") == "cancellation")
            active_memberships = sum(1 for t in timeline if t.get("status") == "active")
            
            durations = [t.get("duration_days") for t in timeline if t.get("duration_days") is not None]
            avg_duration_days = round(sum(durations) / len(durations), 2) if durations else None
            
            return {
                "total_members": total_members,
                "new_members": new_members,
                "renewals": renewals,
                "cancellations": cancellations,
                "active_memberships": active_memberships,
                "avg_duration_days": avg_duration_days,
                "total_events": len(events),
                "source": "live_data"
            }
        except Exception as e:
            print(f"[WARN] Failed to load live data: {e}")
    
    # Fallback to known good sample data
    return {
        "total_members": 533,
        "new_members": 6,
        "renewals": 950,
        "cancellations": 39,
        "active_memberships": 6,
        "avg_duration_days": None,
        "total_events": 1031,
        "source": "sample_data"
    }

def generate_embed_preview(stats):
    """Generate a text preview of what the embed looks like"""
    total_members = stats.get("total_members", 0)
    new_members = stats.get("new_members", 0)
    renewals = stats.get("renewals", 0)
    cancellations = stats.get("cancellations", 0)
    active_memberships = stats.get("active_memberships", 0)
    avg_duration_days = stats.get("avg_duration_days")
    
    preview = []
    preview.append("=" * 60)
    preview.append("MEMBERSHIP STATISTICS (Live Sample Output)")
    preview.append("=" * 60)
    preview.append("")
    preview.append(f"Title: Membership Statistics")
    preview.append(f"Color: 0x5865F2 (Discord Blurple)")
    preview.append(f"Timestamp: {datetime.now().isoformat()}")
    preview.append("")
    preview.append("Fields:")
    preview.append("")
    preview.append(f"  Total Members: **{total_members}** (inline)")
    preview.append(f"  Active Memberships: **{active_memberships}** (inline)")
    if avg_duration_days:
        preview.append(f"  Avg Duration: **{avg_duration_days}** days (inline)")
    preview.append("")
    preview.append("  Event Breakdown (not inline):")
    preview.append(f"    • New Members: **{new_members}**")
    preview.append(f"    • Renewals: **{renewals}**")
    preview.append(f"    • Cancellations: **{cancellations}**")
    preview.append("")
    preview.append("Footer:")
    preview.append("  RSAdminBot • Whop Statistics | Data source: whop_history.json | Run !whopscan to update")
    preview.append("")
    preview.append("=" * 60)
    
    return "\n".join(preview)

def generate_json_sample(stats):
    """Generate JSON sample output"""
    total_members = stats.get("total_members", 0)
    new_members = stats.get("new_members", 0)
    renewals = stats.get("renewals", 0)
    cancellations = stats.get("cancellations", 0)
    active_memberships = stats.get("active_memberships", 0)
    avg_duration_days = stats.get("avg_duration_days")
    
    sample = {
        "whopstats_live_sample": {
            "generated_at": datetime.now().isoformat(),
            "data_source": stats.get("source", "unknown"),
            "statistics": {
                "total_members": total_members,
                "new_members": new_members,
                "renewals": renewals,
                "cancellations": cancellations,
                "active_memberships": active_memberships,
                "avg_duration_days": avg_duration_days,
                "total_events": stats.get("total_events", 0)
            },
            "embed_preview": {
                "title": "Membership Statistics",
                "color": "0x5865F2",
                "timestamp": datetime.now().isoformat(),
                "fields": [
                    {
                        "name": "Total Members",
                        "value": f"**{total_members}**",
                        "inline": True
                    },
                    {
                        "name": "Active Memberships",
                        "value": f"**{active_memberships}**",
                        "inline": True
                    }
                ]
            }
        }
    }
    
    # Add avg duration if available
    if avg_duration_days:
        sample["whopstats_live_sample"]["embed_preview"]["fields"].append({
            "name": "Avg Duration",
            "value": f"**{avg_duration_days}** days",
            "inline": True
        })
    
    # Add event breakdown
    sample["whopstats_live_sample"]["embed_preview"]["fields"].append({
        "name": "Event Breakdown",
        "value": f"• New Members: **{new_members}**\n• Renewals: **{renewals}**\n• Cancellations: **{cancellations}**",
        "inline": False
    })
    
    sample["whopstats_live_sample"]["embed_preview"]["footer"] = "RSAdminBot • Whop Statistics | Data source: whop_history.json | Run !whopscan to update"
    
    return sample

if __name__ == "__main__":
    print("Loading live whopstats data...")
    stats = load_live_stats()
    
    print(f"\nData source: {stats.get('source', 'unknown')}")
    print(f"Total Members: {stats.get('total_members', 0)}")
    print(f"Events: {stats.get('total_events', 0)}")
    
    # Generate text preview
    print("\n" + generate_embed_preview(stats))
    
    # Generate JSON sample
    sample_output = generate_json_sample(stats)
    output_path = Path(__file__).resolve().parent / "whopstats_live_sample.json"
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sample_output, f, indent=2, ensure_ascii=False)
    
    print(f"\n[OK] JSON sample saved to: {output_path.name}")
    print("\nTo view on server, run this command:")
    print("  ssh -i oraclekeys/ssh-key-2025-12-15.key rsadmin@137.131.14.157")
    print("  cd /home/rsadmin/bots/mirror-world/RSAdminBot")
    print("  python3 generate_live_sample_output.py")

