"""
Sequence 1: Initialization

Phase 1: Bot connection, config loading, SSH setup
"""

import sys
from pathlib import Path

# Import Colors from parent module
sys.path.insert(0, str(Path(__file__).parent.parent))
from admin_bot import Colors


async def run(admin_bot):
    """Run Phase 1: Bot connection and initialization"""
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}  🔧 RS Admin Bot - Starting Up{Colors.RESET}")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
    
    print(f"{Colors.CYAN}[Phase 1] [1.1] Checking bot connection status...{Colors.RESET}")
    print(f"{Colors.GREEN}[Phase 1] [1.1] ✓ Bot connected as: {admin_bot.bot.user}{Colors.RESET}")
    print(f"{Colors.GREEN}[Phase 1] [1.1] ✓ Bot ID: {admin_bot.bot.user.id}{Colors.RESET}")
    print(f"{Colors.GREEN}[Phase 1] [1.1] ✓ Bot latency: {round(admin_bot.bot.latency * 1000)}ms{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}[Phase 1] [1.2] Checking connected servers...{Colors.RESET}")
    # Show all connected guilds
    guilds = list(admin_bot.bot.guilds)
    print(f"{Colors.GREEN}[Phase 1] [1.2] ✓ Connected to {len(guilds)} server(s):{Colors.RESET}")
    for guild in guilds:
        member_count = guild.member_count if hasattr(guild, 'member_count') else 'N/A'
        print(f"{Colors.CYAN}    • {guild.name} (ID: {guild.id}, Members: {member_count}){Colors.RESET}")
    print()
    
    print(f"{Colors.CYAN}[Phase 1] [1.3] Verifying configured guild IDs...{Colors.RESET}")
    # Verify configured guild IDs
    config_guild_id = admin_bot.config.get('guild_id')
    if config_guild_id:
        config_guild = admin_bot.bot.get_guild(int(config_guild_id))
        if config_guild:
            print(f"{Colors.GREEN}[Phase 1] [1.3] ✓ Primary guild verified: {config_guild.name}{Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}[Phase 1] [1.3] ⚠️  Configured guild ID {config_guild_id} not found{Colors.RESET}")
    else:
        print(f"{Colors.YELLOW}[Phase 1] [1.3] ⚠️  No primary guild ID configured{Colors.RESET}")
    
    print(f"{Colors.GREEN}[Phase 1] ✓ Bot connection status verified{Colors.RESET}\n")
    
    # Show registered commands
    print(f"{Colors.CYAN}[Phase 1] [1.4] Registered Commands:{Colors.RESET}")
    if hasattr(admin_bot, 'registered_commands') and admin_bot.registered_commands:
        for cmd_name, cmd_desc, is_admin in admin_bot.registered_commands:
            admin_marker = " [ADMIN]" if is_admin else ""
            print(f"{Colors.GREEN}    ✓ !{cmd_name}{admin_marker} - {cmd_desc}{Colors.RESET}")
    else:
        # Fallback: get commands from bot
        all_commands = list(admin_bot.bot.commands)
        for cmd in sorted(all_commands, key=lambda c: c.name):
            admin_marker = " [ADMIN]" if cmd.checks else ""
            desc = cmd.help or cmd.brief or "No description"
            print(f"{Colors.GREEN}    ✓ !{cmd.name}{admin_marker} - {desc[:60]}{Colors.RESET}")
    print()

