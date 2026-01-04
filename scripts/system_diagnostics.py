#!/usr/bin/env python3
"""
Mirror World - Unified System Diagnostics
==========================================
Provides comprehensive diagnostics for all bots and services.
Shows live status, channel configurations, and health checks.

Run: python scripts/system_diagnostics.py
"""

import os
import sys
import json
import socket
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ANSI Colors
class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    WHITE = "\033[97m"


class BotDiagnostics:
    """Diagnostics for a single bot."""
    
    def __init__(self, bot_name: str):
        self.bot_name = bot_name.lower()
        self.status = "unknown"
        self.config: Dict[str, Any] = {}
        self.channels: Dict[str, Any] = {}
        self.logs: Dict[str, Any] = {}
        self.issues: List[str] = []
        self.warnings: List[str] = []
    
    def check_running(self) -> bool:
        """Check if the bot is running (uses bot_service.py detection method)."""
        try:
            # Method 1: Check via WMIC command line (most accurate)
            result = subprocess.run(
                ['wmic', 'process', 'where', "name='python.exe'", 'get', 'commandline', '/format:list'],
                capture_output=True, text=True, timeout=10
            )
            cmd_lines = result.stdout.lower()
            
            # Check for bot script name in any running python process
            bot_script_names = [
                f"{self.bot_name}.py",
                f"{self.bot_name}bot.py",
                f"{self.bot_name}_bot.py"
            ]
            
            for script_name in bot_script_names:
                if script_name in cmd_lines:
                    return True
            
            # Method 2: Check window titles as fallback
            tasklist = subprocess.run(['tasklist', '/v'], capture_output=True, text=True, timeout=5)
            if self.bot_name in tasklist.stdout.lower():
                return True
            
            return False
        except:
            return False
    
    def check_log_file(self) -> Dict[str, Any]:
        """Check the bot's log file."""
        log_path = PROJECT_ROOT / "logs" / "Botlogs" / f"{self.bot_name}logs.json"
        
        if not log_path.exists():
            return {"exists": False, "entries": 0, "last_entry": None}
        
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                logs = json.load(f)
            
            last_entry = logs[-1] if logs else None
            return {
                "exists": True,
                "entries": len(logs),
                "size_bytes": log_path.stat().st_size,
                "last_entry": last_entry
            }
        except:
            return {"exists": True, "entries": 0, "error": "Failed to parse"}
    
    def get_last_activity(self) -> Optional[str]:
        """Get the timestamp of last activity."""
        log_info = self.check_log_file()
        if log_info.get("last_entry"):
            return log_info["last_entry"].get("timestamp", None)
        return None


class SystemDiagnostics:
    """Full system diagnostics."""
    
    def __init__(self):
        self.bots = {
            'discumbot': BotDiagnostics('discumbot'),
            'datamanagerbot': BotDiagnostics('datamanagerbot'),
            'pingbot': BotDiagnostics('pingbot'),
            'testcenter': BotDiagnostics('testcenter'),
        }
        self.dashboard_status = False
        self.ports: Dict[int, str] = {}
        self.config_loaded = False
    
    def check_dashboard(self) -> bool:
        """Check if dashboard is running on port 8080."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 8080))
            sock.close()
            self.dashboard_status = (result == 0)
            return self.dashboard_status
        except:
            return False
    
    def check_all_bots(self) -> Dict[str, bool]:
        """Check status of all bots."""
        status = {}
        for name, bot in self.bots.items():
            status[name] = bot.check_running()
        return status
    
    def load_config(self) -> Dict[str, Any]:
        """Load and validate configuration."""
        config = {}
        
        try:
            from neonxt.core.config import (
                DISCUM_BOT, PING_BOT, DATAMANAGER_BOT,
                SMART_SOURCE_CHANNELS, SMART_SOURCE_CHANNELS_ONLINE, SMART_SOURCE_CHANNELS_INSTORE,
                SMARTFILTER_AMAZON_CHANNEL_ID, SMARTFILTER_MAJOR_STORES_CHANNEL_ID,
                SMARTFILTER_DEFAULT_CHANNEL_ID, MIRRORWORLD_SERVER
            )
            
            config['tokens'] = {
                'DISCUM_BOT': bool(DISCUM_BOT),
                'PING_BOT': bool(PING_BOT),
                'DATAMANAGER_BOT': bool(DATAMANAGER_BOT),
            }
            
            config['channels'] = {
                'source_total': len(SMART_SOURCE_CHANNELS),
                'source_online': len(SMART_SOURCE_CHANNELS_ONLINE),
                'source_instore': len(SMART_SOURCE_CHANNELS_INSTORE),
            }
            
            config['destinations'] = {
                'AMAZON': SMARTFILTER_AMAZON_CHANNEL_ID,
                'MAJOR_STORES': SMARTFILTER_MAJOR_STORES_CHANNEL_ID,
                'DEFAULT': SMARTFILTER_DEFAULT_CHANNEL_ID,
            }
            
            config['server'] = MIRRORWORLD_SERVER
            self.config_loaded = True
            
        except Exception as e:
            config['error'] = str(e)
            
        return config
    
    def get_channel_details(self) -> Dict[str, Any]:
        """Get detailed channel configuration."""
        details = {'source': [], 'destination': {}}
        
        try:
            from neonxt.core.config import (
                SMART_SOURCE_CHANNELS,
                SMARTFILTER_AMAZON_CHANNEL_ID, SMARTFILTER_MAJOR_STORES_CHANNEL_ID,
                SMARTFILTER_DEFAULT_CHANNEL_ID, SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID,
                SMARTFILTER_UPCOMING_CHANNEL_ID, SMARTFILTER_INSTORE_LEADS_CHANNEL_ID,
                SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID,
                SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID, SMARTFILTER_INSTORE_CARDS_CHANNEL_ID,
                SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID, SMARTFILTER_FULL_SEND_CHANNEL_ID,
                SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID,
                SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID
            )
            
            details['source'] = list(SMART_SOURCE_CHANNELS)[:20]  # Limit for display
            
            details['destination'] = {
                'AMAZON': SMARTFILTER_AMAZON_CHANNEL_ID or "NOT SET",
                'MAJOR_STORES': SMARTFILTER_MAJOR_STORES_CHANNEL_ID or "NOT SET",
                'AFFILIATED_LINKS': SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID or "NOT SET",
                'UPCOMING': SMARTFILTER_UPCOMING_CHANNEL_ID or "NOT SET",
                'INSTORE_LEADS': SMARTFILTER_INSTORE_LEADS_CHANNEL_ID or "NOT SET",
                'DISCOUNTED_STORES': SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID or "NOT SET",
                'INSTORE_SEASONAL': SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID or "NOT SET",
                'INSTORE_SNEAKERS': SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID or "NOT SET",
                'INSTORE_CARDS': SMARTFILTER_INSTORE_CARDS_CHANNEL_ID or "NOT SET",
                'MONITORED_KEYWORD': SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID or "NOT SET",
                'FULL_SEND': SMARTFILTER_FULL_SEND_CHANNEL_ID or "NOT SET",
                'PRICE_ERROR': SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID or "NOT SET",
                'FLIPS_PROFITABLE': SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID or "NOT SET",
                'MAJOR_CLEARANCE': SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID or "NOT SET",
                'DEFAULT': SMARTFILTER_DEFAULT_CHANNEL_ID or "NOT SET",
            }
            
        except Exception as e:
            details['error'] = str(e)
            
        return details
    
    def get_log_paths(self) -> Dict[str, Dict]:
        """Get log file status for all bots."""
        log_dir = PROJECT_ROOT / "logs" / "Botlogs"
        data_dir = PROJECT_ROOT / "logs" / "Datalogs"
        
        log_files = {
            'Bot Logs': {
                'discumbot': log_dir / "discumlogs.json",
                'datamanagerbot': log_dir / "datamanagerlogs.json",
                'pingbot': log_dir / "pingbotlogs.json",
                'testcenter': log_dir / "testcenterlogs.json",
            },
            'Data Logs': {
                'mirror_world_sol': data_dir / "Mirror_World_SOL.json",
                'amazon': data_dir / "Amazon.json",
                'mavely': data_dir / "Mavely.json",
                'instoreleads': data_dir / "Instoreleads.json",
            }
        }
        
        result = {}
        for category, files in log_files.items():
            result[category] = {}
            for name, path in files.items():
                if path.exists():
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        result[category][name] = {
                            'exists': True,
                            'entries': len(data) if isinstance(data, list) else 1,
                            'size': path.stat().st_size
                        }
                    except:
                        result[category][name] = {'exists': True, 'error': 'parse failed'}
                else:
                    result[category][name] = {'exists': False}
        
        return result
    
    def generate_full_report(self) -> str:
        """Generate comprehensive diagnostics report."""
        lines = []
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Header
        lines.append(f"\n{C.CYAN}{'=' * 70}{C.RESET}")
        lines.append(f"{C.CYAN}  MIRROR WORLD - FULL SYSTEM DIAGNOSTICS{C.RESET}")
        lines.append(f"{C.DIM}  {now}{C.RESET}")
        lines.append(f"{C.CYAN}{'=' * 70}{C.RESET}")
        
        # Bot Status
        lines.append(f"\n{C.WHITE}[BOT STATUS]{C.RESET}")
        lines.append(f"{C.DIM}{'-' * 50}{C.RESET}")
        
        bot_status = self.check_all_bots()
        dashboard_ok = self.check_dashboard()
        
        services = [
            ('Dashboard', dashboard_ok, 'http://localhost:8080'),
            ('DiscumBot', bot_status.get('discumbot', False), 'Source channel listener'),
            ('DataManagerBot', bot_status.get('datamanagerbot', False), 'Message classifier'),
            ('PingBot', bot_status.get('pingbot', False), 'Mention handler'),
            ('TestCenter', bot_status.get('testcenter', False), 'Slash commands'),
        ]
        
        running_count = 0
        for name, is_running, desc in services:
            if is_running:
                status = f"{C.GREEN}● RUNNING{C.RESET}"
                running_count += 1
            else:
                status = f"{C.RED}○ STOPPED{C.RESET}"
            lines.append(f"  {name:18} {status:20} {C.DIM}{desc}{C.RESET}")
        
        lines.append(f"\n  {C.WHITE}Total: {running_count}/5 services running{C.RESET}")
        
        # Configuration
        lines.append(f"\n{C.WHITE}[CONFIGURATION]{C.RESET}")
        lines.append(f"{C.DIM}{'-' * 50}{C.RESET}")
        
        config = self.load_config()
        
        if 'error' not in config:
            # Tokens
            lines.append(f"  {C.YELLOW}Tokens:{C.RESET}")
            for token, is_set in config.get('tokens', {}).items():
                sym = f"{C.GREEN}✓{C.RESET}" if is_set else f"{C.RED}✗{C.RESET}"
                lines.append(f"    {sym} {token}")
            
            # Channels
            lines.append(f"\n  {C.YELLOW}Source Channels:{C.RESET}")
            ch = config.get('channels', {})
            lines.append(f"    Total: {ch.get('source_total', 0)}")
            lines.append(f"    Online: {ch.get('source_online', 0)}")
            lines.append(f"    Instore: {ch.get('source_instore', 0)}")
        else:
            lines.append(f"  {C.RED}Error loading config: {config['error']}{C.RESET}")
        
        # Destination Channels
        lines.append(f"\n{C.WHITE}[DESTINATION CHANNELS]{C.RESET}")
        lines.append(f"{C.DIM}{'-' * 50}{C.RESET}")
        
        channels = self.get_channel_details()
        for name, channel_id in channels.get('destination', {}).items():
            if channel_id and channel_id != "NOT SET":
                sym = f"{C.GREEN}✓{C.RESET}"
            else:
                sym = f"{C.YELLOW}⚠{C.RESET}"
                channel_id = "NOT SET"
            lines.append(f"  {sym} {name:20} {channel_id}")
        
        # Log Files
        lines.append(f"\n{C.WHITE}[LOG FILES]{C.RESET}")
        lines.append(f"{C.DIM}{'-' * 50}{C.RESET}")
        
        logs = self.get_log_paths()
        for category, files in logs.items():
            lines.append(f"  {C.YELLOW}{category}:{C.RESET}")
            for name, info in files.items():
                if info.get('exists'):
                    entries = info.get('entries', '?')
                    size = info.get('size', 0)
                    size_kb = size / 1024
                    lines.append(f"    {C.GREEN}✓{C.RESET} {name}: {entries} entries ({size_kb:.1f} KB)")
                else:
                    lines.append(f"    {C.DIM}○ {name}: not created yet{C.RESET}")
        
        # Recent Activity
        lines.append(f"\n{C.WHITE}[RECENT ACTIVITY]{C.RESET}")
        lines.append(f"{C.DIM}{'-' * 50}{C.RESET}")
        
        for bot_name, bot in self.bots.items():
            last = bot.get_last_activity()
            if last:
                lines.append(f"  {bot_name:18} Last: {last[:19]}")
            else:
                lines.append(f"  {bot_name:18} {C.DIM}No activity logged{C.RESET}")
        
        # Footer
        lines.append(f"\n{C.CYAN}{'=' * 70}{C.RESET}")
        lines.append(f"  {C.WHITE}QUICK COMMANDS:{C.RESET}")
        lines.append(f"    BOTSTART.bat    - Start/restart all services")
        lines.append(f"    BOTSTOP.bat     - Stop all services")
        lines.append(f"    Dashboard: {C.BLUE}http://localhost:8080{C.RESET}")
        lines.append(f"{C.CYAN}{'=' * 70}{C.RESET}\n")
        
        return "\n".join(lines)
    
    def generate_bot_report(self, bot_name: str) -> str:
        """Generate detailed report for a specific bot."""
        bot = self.bots.get(bot_name.lower().replace('bot', ''))
        if not bot:
            # Try with 'bot' suffix
            for key in self.bots:
                if bot_name.lower() in key:
                    bot = self.bots[key]
                    break
        
        if not bot:
            return f"Unknown bot: {bot_name}"
        
        lines = []
        lines.append(f"\n{'=' * 70}")
        lines.append(f"[DIAGNOSTICS] {bot.bot_name.upper()} - Configuration & Status")
        lines.append(f"{'=' * 70}")
        
        # Status
        is_running = bot.check_running()
        if is_running:
            lines.append(f"\n[✓] Bot Status: LIVE & CONNECTED")
        else:
            lines.append(f"\n[✗] Bot Status: STOPPED")
        
        # Log info
        log_info = bot.check_log_file()
        lines.append(f"\n[📁] Log File:")
        if log_info.get('exists'):
            lines.append(f"  ✓ Entries: {log_info.get('entries', 0)}")
            lines.append(f"  ✓ Size: {log_info.get('size_bytes', 0)} bytes")
            if log_info.get('last_entry'):
                lines.append(f"  ✓ Last: {log_info['last_entry'].get('timestamp', 'unknown')[:19]}")
        else:
            lines.append(f"  ⚠ Log file not found")
        
        lines.append(f"\n{'=' * 70}")
        lines.append(f"[READY] {bot.bot_name} diagnostics complete.")
        lines.append(f"{'=' * 70}\n")
        
        return "\n".join(lines)


def wait_for_services(timeout: int = 30) -> Dict[str, bool]:
    """Wait for all services to start within timeout."""
    import time
    
    diag = SystemDiagnostics()
    start = time.time()
    
    while time.time() - start < timeout:
        dashboard = diag.check_dashboard()
        bots = diag.check_all_bots()
        
        all_running = dashboard and all(bots.values())
        if all_running:
            return {'dashboard': True, **bots, 'all_ready': True}
        
        time.sleep(2)
    
    # Timeout - return current status
    return {
        'dashboard': diag.check_dashboard(),
        **diag.check_all_bots(),
        'all_ready': False,
        'timeout': True
    }


def main():
    import argparse
    
    # Fix Windows console encoding
    if sys.platform == 'win32':
        try:
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        except Exception as e:
            print(f"[WARN] {type(e).__name__}: {e}")  # Was silent pass
    
    parser = argparse.ArgumentParser(description="Mirror World System Diagnostics")
    parser.add_argument("--bot", help="Show diagnostics for specific bot")
    parser.add_argument("--wait", type=int, default=0, help="Wait N seconds for services to start")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()
    
    diag = SystemDiagnostics()
    
    if args.wait > 0:
        print(f"Waiting up to {args.wait}s for services to start...")
        status = wait_for_services(args.wait)
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            if status.get('all_ready'):
                print("✓ All services are running!")
            else:
                print("⚠ Some services not ready:")
                for svc, running in status.items():
                    if svc not in ['all_ready', 'timeout']:
                        sym = "✓" if running else "✗"
                        print(f"  {sym} {svc}")
    elif args.bot:
        print(diag.generate_bot_report(args.bot))
    elif args.json:
        data = {
            'dashboard': diag.check_dashboard(),
            'bots': diag.check_all_bots(),
            'config': diag.load_config(),
            'channels': diag.get_channel_details(),
            'logs': diag.get_log_paths()
        }
        print(json.dumps(data, indent=2, default=str))
    else:
        print(diag.generate_full_report())


if __name__ == "__main__":
    main()

