"""
Sequence 2: Tracking Modules

Phase 2: Tracking modules initialization
"""

import sys
from pathlib import Path

# Import from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors, ORGANIZER_AVAILABLE


async def run(admin_bot):
    """Run Phase 2: Initialize optional organizer (no trackers)."""
    print(f"\n{Colors.CYAN}[Phase 2] Initializing optional modules...{Colors.RESET}")

    # Trackers were removed (Whop + movement tracking + testcard tooling).
    # We keep TestServerOrganizer only if other features (journal live / command catalog) rely on it.
    if not ORGANIZER_AVAILABLE:
        print(f"{Colors.YELLOW}[Phase 2] TestServerOrganizer not available (import failed){Colors.RESET}")
        return

    try:
        from test_server_organizer import TestServerOrganizer

        print(f"{Colors.CYAN}[Phase 2] [2.1] Initializing TestServerOrganizer...{Colors.RESET}")
        admin_bot.test_server_organizer = TestServerOrganizer(admin_bot.bot, admin_bot.config, admin_bot.BOTS)
        print(f"{Colors.GREEN}[Phase 2] [2.1] ✓ TestServerOrganizer initialized{Colors.RESET}")
        print(f"{Colors.GREEN}[Phase 2] ✓ Optional modules initialized successfully{Colors.RESET}\n")
    except Exception as e:
        print(f"{Colors.YELLOW}[Phase 2] ⚠️  Failed to initialize TestServerOrganizer (non-critical): {e}{Colors.RESET}")
        import traceback
        print(f"{Colors.DIM}[Phase 2] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")
        admin_bot.test_server_organizer = None

