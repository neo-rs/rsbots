"""
Sequence 6: Background Tasks

Phase 6: Background tasks initialization
"""

import sys
import discord
from pathlib import Path

# Import from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors


async def run(admin_bot):
    """Run Phase 6: Start background tasks"""
    print(f"\n{Colors.CYAN}[Phase 6] Starting background tasks...{Colors.RESET}")
    
    print(f"{Colors.CYAN}[Phase 6] [6.1] Setting bot presence...{Colors.RESET}")
    # Set bot to offline/invisible status
    await admin_bot.bot.change_presence(
        status=discord.Status.invisible,
        activity=None
    )
    print(f"{Colors.GREEN}[Phase 6] [6.1] ✓ Bot presence set to invisible{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}[Phase 6] [6.2] Starting periodic whop scanning task...{Colors.RESET}")
    # Start periodic whop scanning task
    try:
        if admin_bot.whop_tracker:
            admin_bot._start_whop_scanning_task()
            print(f"{Colors.GREEN}[Phase 6] [6.2] ✓ Periodic whop scanning task started{Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}[Phase 6] [6.2] ⚠️  Whop tracker not available - skipping periodic scan{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.YELLOW}[Phase 6] [6.2] ⚠️  Failed to start periodic scan (non-critical): {e}{Colors.RESET}")
        import traceback
        print(f"{Colors.RED}[Phase 6] [6.2] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")

    print(f"{Colors.CYAN}[Phase 6] [6.3] Starting RS service monitor task...{Colors.RESET}")
    try:
        admin_bot._start_service_monitor_task()
        print(f"{Colors.GREEN}[Phase 6] [6.3] ✓ RS service monitor task started{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.YELLOW}[Phase 6] [6.3] ⚠️  Failed to start service monitor (non-critical): {e}{Colors.RESET}")

    print(f"{Colors.CYAN}[Phase 6] [6.4] Starting OracleFiles snapshot sync task...{Colors.RESET}")
    try:
        admin_bot._start_oraclefiles_sync_task()
        print(f"{Colors.GREEN}[Phase 6] [6.4] ✓ OracleFiles snapshot sync task started{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.YELLOW}[Phase 6] [6.4] ⚠️  Failed to start OracleFiles sync (non-critical): {e}{Colors.RESET}")
    
    # Phase 6 completion
    print(f"\n{Colors.GREEN}[Phase 6] ✓ Background tasks initialized{Colors.RESET}\n")
    
    # ============================================================
    # STARTUP COMPLETE
    # ============================================================
    print(f"\n{Colors.GREEN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}  ✅ RS Admin Bot Ready{Colors.RESET}")
    print(f"{Colors.GREEN}{'='*60}{Colors.RESET}")
    
    # Show readiness summary
    print(f"\n{Colors.CYAN}📊 Startup Summary:{Colors.RESET}")
    print(f"{Colors.GREEN}  ✓ Bot connected to Discord{Colors.RESET}")
    print(f"{Colors.GREEN}  ✓ Tracking modules initialized{Colors.RESET}")
    if admin_bot.current_server:
        print(f"{Colors.GREEN}  ✓ SSH server configured: {admin_bot.current_server.get('name', 'Unknown')}{Colors.RESET}")
    if admin_bot.service_manager:
        print(f"{Colors.GREEN}  ✓ Service management ready{Colors.RESET}")
    if admin_bot.whop_tracker:
        print(f"{Colors.GREEN}  ✓ Whop tracking active{Colors.RESET}")
    if admin_bot.bot_movement_tracker:
        print(f"{Colors.GREEN}  ✓ Bot movement tracking active{Colors.RESET}")
    
    print(f"\n{Colors.CYAN}🎮 Bot is now ready to manage RS bots!{Colors.RESET}")
    print(f"{Colors.YELLOW}  Use Discord commands: !botlist, !botstatus, !botstart, !botstop, etc.{Colors.RESET}")
    print(f"{Colors.YELLOW}  Use !status to check bot status anytime{Colors.RESET}")
    print(f"{Colors.GREEN}{'='*60}{Colors.RESET}\n")

