"""
Sequence 5: Discord Channels

Phase 5: Discord channel setup
"""

import sys
from pathlib import Path

# Import from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors


async def run(admin_bot):
    """Run Phase 5: Setup Discord channels"""
    print(f"\n{Colors.CYAN}[Phase 5] Setting up Discord channels...{Colors.RESET}")
    try:
        print(f"{Colors.CYAN}[Phase 5] [5.1] Sending SSH commands to channel...{Colors.RESET}")
        await admin_bot._send_ssh_commands_to_channel()
        print(f"{Colors.GREEN}[Phase 5] [5.1] ✓ SSH commands sent to channel{Colors.RESET}")
        print(f"{Colors.GREEN}[Phase 5] ✓ Channel setup complete{Colors.RESET}\n")
    except Exception as e:
        print(f"{Colors.YELLOW}[Phase 5] ⚠️  Channel setup failed (non-critical): {e}{Colors.RESET}")
        import traceback
        print(f"{Colors.RED}[Phase 5] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}\n")

