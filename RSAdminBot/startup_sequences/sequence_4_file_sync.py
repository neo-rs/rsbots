"""
Sequence 4: File Sync

Phase 4: File sync check (using sync_bot.sh)
"""

import sys
from pathlib import Path

# Import from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors


async def run(admin_bot):
    """Run Phase 4: Check and sync files"""
    print(f"\n{Colors.CYAN}[Phase 4] Checking file sync status...{Colors.RESET}")
    try:
        await admin_bot._check_and_sync_files()
    except Exception as e:
        print(f"{Colors.YELLOW}[Phase 4] Sync check error: {e}{Colors.RESET}")
        import traceback
        print(f"{Colors.RED}[Phase 4] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")

