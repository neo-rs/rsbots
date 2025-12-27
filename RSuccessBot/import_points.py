#!/usr/bin/env python3
"""
Import Points from History
--------------------------
Standalone script to import points from points_history.txt into JSON storage.
"""

import sys
import re
import json
from pathlib import Path
from datetime import datetime, timezone

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

def main():
    base_path = Path(__file__).parent
    config_path = base_path / "config.json"
    # Check both in RSuccessBot folder and parent folder
    history_file = base_path / "points_history.txt"
    if not history_file.exists():
        history_file = base_path.parent / "points_history.txt"
    
    # Load config
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"{Colors.RED}[ERROR] config.json not found{Colors.RESET}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}[ERROR] Invalid JSON in config.json: {e}{Colors.RESET}")
        sys.exit(1)
    
    # Check history file
    if not history_file.exists():
        print(f"{Colors.RED}[ERROR] points_history.txt not found{Colors.RESET}")
        print(f"{Colors.YELLOW}Run !scanhistory command in Discord first{Colors.RESET}")
        sys.exit(1)
    
    # Load JSON data
    json_path = base_path / "success_points.json"
    
    try:
        if json_path.exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = {
                "points": {},
                "image_hashes": {},
                "point_movements": [],
                "migrated_at": datetime.now(timezone.utc).isoformat()
            }
        
        # Ensure all required keys exist
        if "points" not in data:
            data["points"] = {}
        if "point_movements" not in data:
            data["point_movements"] = []
        
        print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
        print(f"{Colors.BOLD}  [IMPORT] Importing Points from History{Colors.RESET}")
        print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
        print(f"{Colors.CYAN}JSON file: {json_path}{Colors.RESET}")
        print(f"{Colors.CYAN}History file: {history_file}{Colors.RESET}\n")
        
        imported = 0
        updated = 0
        skipped = 0
        errors = 0
        
        def get_user_points(user_id):
            user_id_str = str(user_id)
            if user_id_str in data["points"]:
                return data["points"][user_id_str].get("points", 0)
            return 0
        
        with open(history_file, "r", encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # Skip header lines
                if line.startswith("===") or line.startswith("Total") or not line.strip():
                    continue
                
                match = re.match(r'\d+\. .+ \(ID: (\d+)\) - (\d+) points', line)
                if match:
                    try:
                        user_id = int(match.group(1))
                        points = int(match.group(2))
                        
                        current_points = get_user_points(user_id)
                        
                        if current_points != points:
                            # Calculate change
                            change_amount = points - current_points
                            user_id_str = str(user_id)
                            
                            # Log the movement
                            movement = {
                                "user_id": user_id,
                                "change_amount": change_amount,
                                "old_balance": current_points,
                                "new_balance": points,
                                "reason": f"Imported from history (line {line_num})",
                                "admin_user_id": None,
                                "created_at": datetime.now(timezone.utc).isoformat()
                            }
                            data["point_movements"].append(movement)
                            
                            # Update points
                            if user_id_str not in data["points"]:
                                data["points"][user_id_str] = {}
                            
                            data["points"][user_id_str]["points"] = points
                            data["points"][user_id_str]["last_updated"] = datetime.now(timezone.utc).isoformat()
                            data["points"][user_id_str]["source"] = "history_import"
                            
                            if current_points == 0:
                                imported += 1
                                print(f"{Colors.GREEN}[{line_num:4d}] Imported {points:3d} points for user {user_id}{Colors.RESET}")
                            else:
                                updated += 1
                                print(f"{Colors.YELLOW}[{line_num:4d}] Updated user {user_id}: {current_points:3d} -> {points:3d} points{Colors.RESET}")
                        else:
                            skipped += 1
                            if line_num <= 20:  # Only show first 20 skipped
                                print(f"{Colors.CYAN}[{line_num:4d}] User {user_id} already has {points} points, skipping{Colors.RESET}")
                    except Exception as e:
                        errors += 1
                        print(f"{Colors.RED}[{line_num:4d}] Error: {e}{Colors.RESET}")
        
        print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
        print(f"{Colors.BOLD}  [SUMMARY] Import Summary{Colors.RESET}")
        print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
        print(f"{Colors.GREEN}New users imported: {imported}{Colors.RESET}")
        print(f"{Colors.YELLOW}Users updated: {updated}{Colors.RESET}")
        print(f"{Colors.CYAN}Users skipped (already correct): {skipped}{Colors.RESET}")
        print(f"{Colors.RED}Errors: {errors}{Colors.RESET}")
        
        # Show current totals
        total_users = len(data["points"])
        total_points = sum(user_data.get("points", 0) for user_data in data["points"].values())
        print(f"\n{Colors.BOLD}Current JSON Totals:{Colors.RESET}")
        print(f"  Total users: {total_users}")
        print(f"  Total points: {total_points}")
        
        # Save JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n{Colors.GREEN}[SUCCESS] Import complete! Data saved to {json_path}{Colors.RESET}")
        
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}[ERROR] JSON error: {e}{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Unexpected error: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

