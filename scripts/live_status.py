#!/usr/bin/env python3
"""
Mirror World - Live System Status Monitor
==========================================
Shows real-time status of all bots with live updates.
Run: python scripts/live_status.py
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ANSI colors
class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    WHITE = "\033[97m"
    DIM = "\033[2m"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_bot_status():
    """Check which bots are running."""
    bots = {
        'discumbot': False,
        'datamanager': False,
        'pingbot': False,
        'testcenter': False,
        'dashboard': False
    }
    
    try:
        result = subprocess.run(['tasklist', '/v'], capture_output=True, text=True, timeout=5)
        output = result.stdout.lower()
        
        if 'discumbot' in output:
            bots['discumbot'] = True
        if 'datamanager' in output:
            bots['datamanager'] = True
        if 'pingbot' in output:
            bots['pingbot'] = True
        if 'testcenter' in output:
            bots['testcenter'] = True
    except:
        pass
    
    # Check dashboard port
    try:
        result = subprocess.run(['netstat', '-an'], capture_output=True, text=True, timeout=5)
        if ':8080' in result.stdout and 'LISTENING' in result.stdout:
            bots['dashboard'] = True
    except:
        pass
    
    return bots

def get_last_log_entry(bot_name: str) -> dict:
    """Get the last log entry for a bot."""
    log_path = PROJECT_ROOT / "logs" / "Botlogs" / f"{bot_name}logs.json"
    if not log_path.exists():
        return {}
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            logs = json.load(f)
            if logs and isinstance(logs, list):
                return logs[-1]
    except:
        pass
    return {}

def get_channel_stats():
    """Get monitored channel counts."""
    try:
        from neonxt.core.config import SMART_SOURCE_CHANNELS, SMART_SOURCE_CHANNELS_ONLINE, SMART_SOURCE_CHANNELS_INSTORE
        return {
            'total': len(SMART_SOURCE_CHANNELS),
            'online': len(SMART_SOURCE_CHANNELS_ONLINE),
            'instore': len(SMART_SOURCE_CHANNELS_INSTORE)
        }
    except:
        return {'total': 0, 'online': 0, 'instore': 0}

def format_timestamp(ts_str: str) -> str:
    """Format timestamp to relative time."""
    if not ts_str:
        return "never"
    try:
        # Parse ISO format
        if 'T' in ts_str:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
            dt = dt.replace(tzinfo=timezone.utc)
        
        now = datetime.now(timezone.utc)
        diff = (now - dt).total_seconds()
        
        if diff < 60:
            return f"{int(diff)}s ago"
        elif diff < 3600:
            return f"{int(diff/60)}m ago"
        elif diff < 86400:
            return f"{int(diff/3600)}h ago"
        else:
            return f"{int(diff/86400)}d ago"
    except:
        return ts_str[:19] if len(ts_str) > 19 else ts_str

def display_status():
    """Display the system status."""
    clear_screen()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    bots = get_bot_status()
    channels = get_channel_stats()
    
    # Header
    print(f"""
{C.CYAN}╔══════════════════════════════════════════════════════════════════════════╗
║{C.WHITE}{C.BOLD}                    MIRROR WORLD - LIVE STATUS                              {C.RESET}{C.CYAN}║
║{C.DIM}                         {now}                             {C.RESET}{C.CYAN}║
╠══════════════════════════════════════════════════════════════════════════╣{C.RESET}
""")
    
    # Bot Status Section
    print(f"{C.CYAN}║{C.WHITE}  BOT STATUS                                                              {C.CYAN}║{C.RESET}")
    print(f"{C.CYAN}╟──────────────────────────────────────────────────────────────────────────╢{C.RESET}")
    
    bot_info = [
        ('Dashboard', 'dashboard', 'http://localhost:8080'),
        ('DiscumBot', 'discumbot', 'Source channel listener'),
        ('DataManager', 'datamanager', 'Message classifier'),
        ('PingBot', 'pingbot', 'Mention handler'),
        ('TestCenter', 'testcenter', 'Slash commands'),
    ]
    
    for name, key, desc in bot_info:
        status = f"{C.GREEN}● RUNNING{C.RESET}" if bots.get(key) else f"{C.RED}○ STOPPED{C.RESET}"
        
        # Get last activity
        if key != 'dashboard':
            last_log = get_last_log_entry(key)
            last_ts = last_log.get('timestamp', '')
            activity = format_timestamp(last_ts)
            last_msg = last_log.get('message', '')[:35] if last_log.get('message') else ''
        else:
            activity = "-"
            last_msg = ""
        
        print(f"{C.CYAN}║{C.RESET}  {name:12} {status:20}  {C.DIM}{desc:25}{C.RESET} {C.CYAN}║{C.RESET}")
    
    print(f"{C.CYAN}╟──────────────────────────────────────────────────────────────────────────╢{C.RESET}")
    
    # Channel Stats
    print(f"{C.CYAN}║{C.WHITE}  CHANNEL MONITORING                                                      {C.CYAN}║{C.RESET}")
    print(f"{C.CYAN}╟──────────────────────────────────────────────────────────────────────────╢{C.RESET}")
    print(f"{C.CYAN}║{C.RESET}  Total Channels: {C.GREEN}{channels['total']:3}{C.RESET}    Online: {C.BLUE}{channels['online']:3}{C.RESET}    Instore: {C.YELLOW}{channels['instore']:3}{C.RESET}               {C.CYAN}║{C.RESET}")
    
    print(f"{C.CYAN}╟──────────────────────────────────────────────────────────────────────────╢{C.RESET}")
    
    # Recent Activity
    print(f"{C.CYAN}║{C.WHITE}  RECENT ACTIVITY                                                        {C.CYAN}║{C.RESET}")
    print(f"{C.CYAN}╟──────────────────────────────────────────────────────────────────────────╢{C.RESET}")
    
    # Get last few log entries
    for bot_key in ['discumbot', 'datamanager', 'pingbot']:
        last_log = get_last_log_entry(bot_key)
        if last_log:
            ts = format_timestamp(last_log.get('timestamp', ''))
            msg = last_log.get('message', last_log.get('event', ''))
            if msg:
                # Clean ANSI codes
                import re
                msg = re.sub(r'\x1b\[[0-9;]*m', '', str(msg))
                msg = msg[:55]
                print(f"{C.CYAN}║{C.RESET}  {C.MAGENTA}{bot_key:12}{C.RESET} {C.DIM}{ts:8}{C.RESET} {msg:55} {C.CYAN}║{C.RESET}")
    
    # Footer
    print(f"""{C.CYAN}╠══════════════════════════════════════════════════════════════════════════╣
║{C.WHITE}  COMMANDS: {C.YELLOW}BOTSTART.bat{C.WHITE} - Start  {C.YELLOW}BOTSTOP.bat{C.WHITE} - Stop  {C.YELLOW}Ctrl+C{C.WHITE} - Exit       {C.CYAN}║
╚══════════════════════════════════════════════════════════════════════════╝{C.RESET}
""")

def main():
    """Main loop - refresh status every 3 seconds."""
    print(f"{C.CYAN}Starting Live Status Monitor... Press Ctrl+C to exit.{C.RESET}")
    time.sleep(1)
    
    try:
        while True:
            display_status()
            time.sleep(3)
    except KeyboardInterrupt:
        print(f"\n{C.YELLOW}Status monitor stopped.{C.RESET}")

if __name__ == '__main__':
    main()

