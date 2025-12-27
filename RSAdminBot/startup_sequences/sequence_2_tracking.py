"""
Sequence 2: Tracking Modules

Phase 2: Tracking modules initialization
"""

import sys
from pathlib import Path

# Import from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors, TRACKER_AVAILABLE

if TRACKER_AVAILABLE:
    from whop_tracker import WhopTracker
    from bot_movement_tracker import BotMovementTracker
    from test_server_organizer import TestServerOrganizer


async def run(admin_bot):
    """Run Phase 2: Initialize tracking modules"""
    print(f"\n{Colors.CYAN}[Phase 2] Initializing tracking modules...{Colors.RESET}")
    
    if TRACKER_AVAILABLE:
        try:
            print(f"{Colors.CYAN}[Phase 2] [2.1] Initializing WhopTracker...{Colors.RESET}")
            admin_bot.whop_tracker = WhopTracker(admin_bot.bot, admin_bot.config)
            print(f"{Colors.GREEN}[Phase 2] [2.1] ✓ WhopTracker initialized{Colors.RESET}")
            
            # Initialize TestServerOrganizer first (needed by BotMovementTracker)
            print(f"{Colors.CYAN}[Phase 2] [2.2] Initializing TestServerOrganizer...{Colors.RESET}")
            admin_bot.test_server_organizer = TestServerOrganizer(admin_bot.bot, admin_bot.config, admin_bot.BOTS)
            print(f"{Colors.GREEN}[Phase 2] [2.2] ✓ TestServerOrganizer initialized{Colors.RESET}")
            
            print(f"{Colors.CYAN}[Phase 2] [2.3] Initializing BotMovementTracker...{Colors.RESET}")
            admin_bot.bot_movement_tracker = BotMovementTracker(
                admin_bot.bot, 
                admin_bot.BOTS, 
                admin_bot.config, 
                test_server_organizer=admin_bot.test_server_organizer
            )
            print(f"{Colors.GREEN}[Phase 2] [2.3] ✓ BotMovementTracker initialized{Colors.RESET}")
            print(f"{Colors.GREEN}[Phase 2] ✓ All trackers initialized successfully{Colors.RESET}\n")
            
            # Initialize bot movement tracker bot IDs from the RS Server guild (single safe source of truth).
            print(f"{Colors.CYAN}[Phase 2] [2.4] Initializing bot IDs from RS Server guild...{Colors.RESET}")
            try:
                await admin_bot.bot_movement_tracker.initialize_bot_ids()
                initialized_count = len(admin_bot.bot_movement_tracker.bot_user_ids)
                if initialized_count > 0:
                    print(f"{Colors.GREEN}[Phase 2] [2.4] ✓ Initialized {initialized_count} bot ID(s) for movement tracking{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[Phase 2] [2.4] ⚠️  No bot IDs initialized - movement tracking may be limited{Colors.RESET}")
            except Exception as bot_id_error:
                print(f"{Colors.YELLOW}[Phase 2] [2.4] ⚠️  Bot ID initialization failed (non-critical): {bot_id_error}{Colors.RESET}")
                print(f"{Colors.YELLOW}[Phase 2] [2.4] Bot movement tracking may be limited until IDs are initialized{Colors.RESET}")
            
            # Auto-setup test server monitoring channels (non-blocking)
            print(f"{Colors.CYAN}[Phase 2] [2.5] Setting up test server monitoring channels...{Colors.RESET}")
            if admin_bot.test_server_organizer:
                try:
                    setup_result = await admin_bot.test_server_organizer.setup_monitoring_channels()
                    if "error" not in setup_result:
                        channels_created = setup_result.get("channels", {})
                        if channels_created:
                            print(f"{Colors.CYAN}[Phase 2] [2.5] Created {len(channels_created)} monitoring channel(s){Colors.RESET}")
                            for channel_name, channel_id in list(channels_created.items())[:5]:
                                print(f"{Colors.CYAN}    • {channel_name}: {channel_id}{Colors.RESET}")
                        print(f"{Colors.GREEN}[Phase 2] [2.5] ✓ Test server monitoring channels ready{Colors.RESET}")
                        # Publish command index into the test server commands channel (idempotent).
                        try:
                            await admin_bot._publish_command_index_to_test_server()
                        except Exception:
                            pass
                    else:
                        print(f"{Colors.YELLOW}[Phase 2] [2.5] ⚠️  Test server setup: {setup_result.get('error', 'Unknown error')}{Colors.RESET}")
                except Exception as setup_error:
                    print(f"{Colors.YELLOW}[Phase 2] [2.5] ⚠️  Test server setup failed (non-critical): {setup_error}{Colors.RESET}")
            else:
                print(f"{Colors.YELLOW}[Phase 2] [2.5] ⚠️  TestServerOrganizer not available{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}[Phase 2] ✗ Failed to initialize trackers: {e}{Colors.RESET}")
            import traceback
            print(f"{Colors.RED}[Phase 2] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")
            print(f"{Colors.YELLOW}[Phase 2] Continuing startup without trackers...{Colors.RESET}")
    else:
        print(f"{Colors.YELLOW}[Phase 2] Trackers not available (import failed){Colors.RESET}")

