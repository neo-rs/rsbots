"""
Sequence 3: Server Status

Phase 3: Remote server status check
"""

import sys
from pathlib import Path

# Import from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors


async def run(admin_bot):
    """Run Phase 3: Check SSH and remote server status"""
    print(f"\n{Colors.CYAN}[Phase 3] Checking SSH and remote server status...{Colors.RESET}")
    try:
        if admin_bot.current_server:
            print(f"{Colors.CYAN}[Phase 3] [3.1] Checking SSH connection...{Colors.RESET}")
            print(f"{Colors.GREEN}[Phase 3] [3.1] ✓ SSH available - Server: {admin_bot.current_server.get('name', 'Unknown')}{Colors.RESET}")
            print(f"{Colors.CYAN}[Phase 3] [3.1] ✓ Host: {admin_bot.current_server.get('host', 'N/A')}{Colors.RESET}")
            print(f"{Colors.CYAN}[Phase 3] [3.1] ✓ User: {admin_bot.current_server.get('user', 'N/A')}{Colors.RESET}\n")
            
            print(f"{Colors.CYAN}[Phase 3] [3.2] Scanning remote services...{Colors.RESET}")
            
            try:
                # Scan for remote services
                scan_cmd = "systemctl list-units --type=service --no-pager | grep 'mirror-world' | grep -v 'testcenter' | awk '{print $1}' | sed 's/.service$//'"
                success, stdout, stderr = admin_bot._execute_ssh_command(scan_cmd, timeout=15)
                
                remote_services = []
                if success and stdout:
                    all_services = [line.strip().lower() for line in stdout.strip().split('\n') if line.strip()]
                    remote_services = [svc for svc in all_services if 'testcenter' not in svc]
                
                # Also check unit-files
                unit_files_cmd = "systemctl list-unit-files --type=service --no-pager | grep 'mirror-world' | grep -v 'testcenter' | awk '{print $1}' | sed 's/.service$//'"
                unit_success, unit_stdout, _ = admin_bot._execute_ssh_command(unit_files_cmd, timeout=15)
                if unit_success and unit_stdout:
                    unit_services = [line.strip().lower() for line in unit_stdout.strip().split('\n') if line.strip()]
                    unit_services = [svc for svc in unit_services if 'testcenter' not in svc]
                    all_remote_services = list(set(remote_services + unit_services))
                else:
                    all_remote_services = remote_services
                
                if all_remote_services:
                    print(f"{Colors.GREEN}[Phase 3] [3.2] ✓ Found {len(all_remote_services)} service(s) on remote server{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[Phase 3] [3.2] ⚠️  No mirror-world services found on remote server{Colors.RESET}")
                
                print(f"{Colors.CYAN}[Phase 3] [3.3] Checking status of {len(admin_bot.BOTS)} configured bot(s)...{Colors.RESET}")
                # Check status of each configured bot
                running_count = 0
                stopped_count = 0
                not_found_count = 0
                
                for idx, (bot_key, bot_info) in enumerate(admin_bot.BOTS.items(), 1):
                    service_name = bot_info["service"]
                    service_base = service_name.replace('.service', '').lower()
                    service_exists = service_base in all_remote_services
                    
                    if service_exists and admin_bot.service_manager:
                        exists, state, error = admin_bot.service_manager.get_status(service_name, bot_name=bot_key)
                        if exists and state:
                            is_active = state == "active"
                            status_icon = "🟢" if is_active else "🟡"
                            status_text = "Running" if is_active else "Stopped"
                            print(f"{Colors.CYAN}[Phase 3] [3.3] [{idx}/{len(admin_bot.BOTS)}] {status_icon} {bot_info['name']}: {status_text}{Colors.RESET}")
                            if is_active:
                                running_count += 1
                            else:
                                stopped_count += 1
                        else:
                            print(f"{Colors.YELLOW}[Phase 3] [3.3] [{idx}/{len(admin_bot.BOTS)}] ⚠️  {bot_info['name']}: Status check failed{Colors.RESET}")
                            not_found_count += 1
                    else:
                        print(f"{Colors.YELLOW}[Phase 3] [3.3] [{idx}/{len(admin_bot.BOTS)}] ⚠️  {bot_info['name']}: Service not found{Colors.RESET}")
                        not_found_count += 1
                
                print(f"\n{Colors.GREEN}[Phase 3] ✓ Remote server status check complete{Colors.RESET}")
                print(f"{Colors.CYAN}[Phase 3] Summary: {running_count} running, {stopped_count} stopped, {not_found_count} not found{Colors.RESET}\n")
            except Exception as e:
                print(f"{Colors.RED}[Phase 3] ✗ Failed to check remote status: {e}{Colors.RESET}")
                import traceback
                print(f"{Colors.RED}[Phase 3] Traceback: {traceback.format_exc()[:300]}{Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}[Phase 3] ⚠️  No SSH server configured - skipping remote check{Colors.RESET}")
            print(f"{Colors.YELLOW}[Phase 3] Configure 'ssh_server' in config.json to enable remote management{Colors.RESET}\n")
    except Exception as e:
        print(f"{Colors.RED}[Phase 3] ✗ Unexpected error: {e}{Colors.RESET}")
        import traceback
        print(f"{Colors.RED}[Phase 3] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")

