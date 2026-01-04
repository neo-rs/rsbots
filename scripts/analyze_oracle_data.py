#!/usr/bin/env python3
"""
Analyze Oracle Server Runtime Data
-----------------------------------
Reads all downloaded JSON files and generates statistics and summary reports.
Identifies missing or empty files.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add repo root to path
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

DATA_DIR = _REPO_ROOT / "OracleServerData"
OUTPUT_FILE = DATA_DIR / "analysis_report.json"
REPORT_FILE = DATA_DIR / "analysis_report.md"


def load_json_file(file_path: Path) -> tuple[dict | None, int, str]:
    """Load JSON file and return data, size, and status"""
    if not file_path.exists():
        return None, 0, "missing"
    
    file_size = file_path.stat().st_size
    if file_size == 0:
        return {}, 0, "empty"
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data, file_size, "ok"
    except json.JSONDecodeError as e:
        return None, file_size, f"invalid_json: {str(e)}"
    except Exception as e:
        return None, file_size, f"error: {str(e)}"


def analyze_rcheckerbot_data(data_dir: Path) -> dict:
    """Analyze RSCheckerbot runtime data"""
    bot_dir = data_dir / "RSCheckerbot"
    analysis = {
        "bot": "RSCheckerbot",
        "files": {},
    }
    
    files_to_check = [
        "member_history.json",
        "whop_identity_cache.json",
        "trial_history.json",
        "whop_webhook_raw_payloads.json",
        "registry.json",
        "queue.json",
        "invites.json",
    ]
    
    for filename in files_to_check:
        file_path = bot_dir / filename
        data, size, status = load_json_file(file_path)
        
        file_analysis = {
            "filename": filename,
            "status": status,
            "size_bytes": size,
            "stats": {},
        }
        
        if data is not None and status == "ok":
            if filename == "member_history.json":
                file_analysis["stats"]["total_members"] = len(data)
                if data:
                    join_counts = [v.get("join_count", 0) for v in data.values()]
                    file_analysis["stats"]["max_join_count"] = max(join_counts) if join_counts else 0
                    file_analysis["stats"]["total_joins"] = sum(join_counts)
            
            elif filename == "whop_identity_cache.json":
                file_analysis["stats"]["total_mappings"] = len(data)
            
            elif filename == "trial_history.json":
                file_analysis["stats"]["total_identities"] = len(data)
                total_events = sum(len(v.get("events", [])) for v in data.values())
                file_analysis["stats"]["total_trial_events"] = total_events
            
            elif filename == "whop_webhook_raw_payloads.json":
                payloads = data.get("payloads", [])
                file_analysis["stats"]["total_payloads"] = len(payloads)
            
            elif filename == "registry.json":
                file_analysis["stats"]["total_entries"] = len(data) if isinstance(data, dict) else 0
            
            elif filename == "queue.json":
                queue_items = data if isinstance(data, list) else data.get("queue", [])
                file_analysis["stats"]["queue_length"] = len(queue_items) if isinstance(queue_items, list) else 0
            
            elif filename == "invites.json":
                invites = data.get("invites", {})
                file_analysis["stats"]["total_invites"] = len(invites) if isinstance(invites, dict) else 0
        
        analysis["files"][filename] = file_analysis
    
    return analysis


def analyze_rsadminbot_data(data_dir: Path) -> dict:
    """Analyze RSAdminBot runtime data"""
    bot_dir = data_dir / "RSAdminBot"
    analysis = {
        "bot": "RSAdminBot",
        "files": {},
    }
    
    # whop_history.json
    whop_history_path = bot_dir / "whop_data" / "whop_history.json"
    data, size, status = load_json_file(whop_history_path)
    
    file_analysis = {
        "filename": "whop_data/whop_history.json",
        "status": status,
        "size_bytes": size,
        "stats": {},
    }
    
    if data is not None and status == "ok":
        events = data.get("membership_events", [])
        timeline = data.get("membership_timeline", [])
        
        file_analysis["stats"]["total_events"] = len(events)
        file_analysis["stats"]["total_timeline_entries"] = len(timeline)
        
        # Event type breakdown
        event_types = defaultdict(int)
        for event in events:
            event_type = event.get("event_type", "unknown")
            event_types[event_type] += 1
        file_analysis["stats"]["event_types"] = dict(event_types)
        
        # Unique members
        unique_members = set(e.get("discord_id") for e in events if e.get("discord_id"))
        file_analysis["stats"]["unique_members"] = len(unique_members)
    
    analysis["files"]["whop_history.json"] = file_analysis
    
    # whop_scan_history.json
    scan_history_path = bot_dir / "whop_data" / "whop_scan_history.json"
    data, size, status = load_json_file(scan_history_path)
    
    file_analysis = {
        "filename": "whop_data/whop_scan_history.json",
        "status": status,
        "size_bytes": size,
        "stats": {},
    }
    
    if data is not None and status == "ok":
        scans = data if isinstance(data, list) else []
        file_analysis["stats"]["total_scans"] = len(scans)
        if scans:
            last_scan = scans[-1]
            file_analysis["stats"]["last_scan"] = {
                "date": last_scan.get("scan_date"),
                "messages_scanned": last_scan.get("messages_scanned", 0),
                "events_found": last_scan.get("events_found", 0),
            }
    
    analysis["files"]["whop_scan_history.json"] = file_analysis
    
    # Bot movements
    movements_dir = bot_dir / "whop_data" / "bot_movements"
    if movements_dir.exists():
        movement_files = list(movements_dir.glob("*.json"))
        analysis["files"]["bot_movements"] = {
            "filename": "whop_data/bot_movements/*.json",
            "status": "ok",
            "file_count": len(movement_files),
            "stats": {},
        }
        
        total_movements = 0
        for movement_file in movement_files:
            data, size, status = load_json_file(movement_file)
            if data is not None and status == "ok":
                movements = data if isinstance(data, list) else []
                total_movements += len(movements)
        
        analysis["files"]["bot_movements"]["stats"]["total_movements"] = total_movements
    
    return analysis


def analyze_rsonboarding_data(data_dir: Path) -> dict:
    """Analyze RSOnboarding runtime data"""
    bot_dir = data_dir / "RSOnboarding"
    analysis = {
        "bot": "RSOnboarding",
        "files": {},
    }
    
    tickets_path = bot_dir / "tickets.json"
    data, size, status = load_json_file(tickets_path)
    
    file_analysis = {
        "filename": "tickets.json",
        "status": status,
        "size_bytes": size,
        "stats": {},
    }
    
    if data is not None and status == "ok":
        file_analysis["stats"]["total_tickets"] = len(data) if isinstance(data, dict) else 0
    
    analysis["files"]["tickets.json"] = file_analysis
    
    return analysis


def analyze_rsuccessbot_data(data_dir: Path) -> dict:
    """Analyze RSuccessBot runtime data"""
    bot_dir = data_dir / "RSuccessBot"
    analysis = {
        "bot": "RSuccessBot",
        "files": {},
    }
    
    points_path = bot_dir / "success_points.json"
    data, size, status = load_json_file(points_path)
    
    file_analysis = {
        "filename": "success_points.json",
        "status": status,
        "size_bytes": size,
        "stats": {},
    }
    
    if data is not None and status == "ok":
        points_data = data.get("points", {})
        file_analysis["stats"]["total_users"] = len(points_data) if isinstance(points_data, dict) else 0
        
        if isinstance(points_data, dict):
            total_points = sum(v.get("points", 0) for v in points_data.values())
            file_analysis["stats"]["total_points"] = total_points
    
    analysis["files"]["success_points.json"] = file_analysis
    
    return analysis


def generate_markdown_report(all_analysis: list[dict]) -> str:
    """Generate markdown report from analysis"""
    lines = []
    
    lines.append("# Oracle Server Runtime Data Analysis")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().isoformat()}")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    for analysis in all_analysis:
        bot_name = analysis["bot"]
        lines.append(f"## {bot_name}")
        lines.append("")
        
        for filename, file_data in analysis["files"].items():
            status = file_data["status"]
            size = file_data["size_bytes"]
            
            status_icon = "✅" if status == "ok" else "⚠️" if status == "missing" else "❌"
            lines.append(f"### {status_icon} {filename}")
            lines.append("")
            lines.append(f"- **Status:** {status}")
            lines.append(f"- **Size:** {size:,} bytes")
            
            if file_data.get("stats"):
                lines.append("- **Statistics:**")
                for key, value in file_data["stats"].items():
                    if isinstance(value, dict):
                        lines.append(f"  - **{key}:**")
                        for k, v in value.items():
                            lines.append(f"    - {k}: {v}")
                    else:
                        lines.append(f"  - **{key}:** {value}")
            
            lines.append("")
    
    return "\n".join(lines)


def main():
    """Main analysis function"""
    if not DATA_DIR.exists():
        print(f"❌ Data directory not found: {DATA_DIR}")
        print("   Run scripts/sync_oracle_runtime_data.py first")
        sys.exit(1)
    
    print("Analyzing Oracle server runtime data...")
    print(f"Data directory: {DATA_DIR}")
    print("")
    
    all_analysis = []
    
    # Analyze each bot
    all_analysis.append(analyze_rcheckerbot_data(DATA_DIR))
    all_analysis.append(analyze_rsadminbot_data(DATA_DIR))
    all_analysis.append(analyze_rsonboarding_data(DATA_DIR))
    all_analysis.append(analyze_rsuccessbot_data(DATA_DIR))
    
    # Save JSON report
    report_data = {
        "analysis_timestamp": datetime.now().isoformat(),
        "bots": all_analysis,
    }
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ JSON report saved to: {OUTPUT_FILE}")
    
    # Generate markdown report
    markdown = generate_markdown_report(all_analysis)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(markdown)
    
    print(f"✅ Markdown report saved to: {REPORT_FILE}")
    print("")
    print("Analysis complete!")


if __name__ == "__main__":
    main()

