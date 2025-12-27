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
    """Run Phase 4: (removed) legacy file sync / tree-compare / auto-sync."""
    # The canonical update model is now:
    # - GitHub python-only updates via !selfupdate / !botupdate
    # - Full deploy via botctl.sh deploy_apply / !deploy for scripts/systemd/venv
    print(f"\n{Colors.CYAN}[Phase 4] Legacy file sync removed - skipping{Colors.RESET}")
    return

