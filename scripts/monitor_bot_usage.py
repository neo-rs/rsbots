#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comprehensive Bot Usage Monitor
Tracks API calls, messages, commands, resources, and more
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Import central logger to read logs
try:
    from neonxt.core.central_logger import (
        LOG_FILES, BOT_LOGS_DIR, DATA_LOGS_DIR, NETWORK_LOGS_DIR,
        CentralLogger
    )
except ImportError:
    print("[ERROR] Could not import central_logger")
    sys.exit(1)

class BotUsageMonitor:
    """Monitor and measure bot usage"""
    
    def __init__(self):
        self.project_root = project_root
        self.logs_dir = project_root / "logs"
        
    def load_json_log(self, log_path: Path, max_age_hours: int = 24) -> List[Dict]:
        """Load JSON log file, filter by age"""
        if not log_path.exists():
            return []
        
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                return []
            
            # Filter by timestamp if available
            cutoff = datetime.now() - timedelta(hours=max_age_hours)
            filtered = []
            for entry in data:
                if isinstance(entry, dict):
                    ts_str = entry.get('timestamp', '')
                    if ts_str:
                        try:
                            ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                            if ts.replace(tzinfo=None) >= cutoff:
                                filtered.append(entry)
                        except:
                            filtered.append(entry)  # Include if can't parse
                    else:
                        filtered.append(entry)
            
            return filtered
        except Exception as e:
            return []
    
    def analyze_api_calls(self) -> Dict[str, Any]:
        """Analyze API calls from network logs"""
        api_log = NETWORK_LOGS_DIR / "api_calls.json"
        entries = self.load_json_log(api_log, max_age_hours=24)
        
        stats = {
            "total_calls": len(entries),
            "by_bot": defaultdict(int),
            "by_endpoint": defaultdict(int),
            "by_method": defaultdict(int),
            "by_status": defaultdict(int),
            "rate_limited": 0,
            "errors": 0,
        }
        
        for entry in entries:
            bot = entry.get('bot', 'unknown')
            endpoint = entry.get('endpoint', 'unknown')
            method = entry.get('method', 'GET')
            status = entry.get('status', 0)
            rate_limited = entry.get('rate_limited', False)
            
            stats["by_bot"][bot] += 1
            stats["by_endpoint"][endpoint] += 1
            stats["by_method"][method] += 1
            stats["by_status"][status] += 1
            
            if rate_limited:
                stats["rate_limited"] += 1
            if status >= 400:
                stats["errors"] += 1
        
        return stats
    
    def analyze_messages(self) -> Dict[str, Any]:
        """Analyze message activity"""
        stats = {
            "total_messages": 0,
            "by_bot": defaultdict(int),
            "by_channel": defaultdict(int),
            "by_guild": defaultdict(int),
        }
        
        # Check bot logs for message activity
        bot_logs = {
            "testcenter": BOT_LOGS_DIR / "testcenterlogs.json",
            "datamanagerbot": BOT_LOGS_DIR / "datamanagerlogs.json",
            "discumbot": BOT_LOGS_DIR / "discumlogs.json",
            "pingbot": BOT_LOGS_DIR / "pingbotlogs.json",
        }
        
        for bot_key, log_path in bot_logs.items():
            entries = self.load_json_log(log_path, max_age_hours=24)
            for entry in entries:
                if isinstance(entry, dict):
                    # Count message-related entries
                    msg_type = entry.get('type', '')
                    if 'message' in msg_type.lower() or 'on_message' in str(entry):
                        stats["total_messages"] += 1
                        stats["by_bot"][bot_key] += 1
        
        return stats
    
    def analyze_commands(self) -> Dict[str, Any]:
        """Analyze command usage"""
        stats = {
            "total_commands": 0,
            "by_command": defaultdict(int),
            "by_bot": defaultdict(int),
            "successful": 0,
            "failed": 0,
        }
        
        # Check command logs (if they exist)
        # This would need to be implemented based on your command logging
        
        return stats
    
    def analyze_resources(self) -> Dict[str, Any]:
        """Analyze resource usage (CPU, memory)"""
        import psutil
        
        stats = {
            "bots_running": 0,
            "total_cpu_percent": 0.0,
            "total_memory_mb": 0.0,
            "by_bot": {},
        }
        
        bot_scripts = {
            "testcenter": "testcenter_bot.py",
            "datamanagerbot": "datamanagerbot.py",
            "discumbot": "discumbot.py",
            "pingbot": "pingbot.py",
        }
        
        # First pass: collect all matching processes
        matching_procs = {}
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if not cmdline:
                    continue
                
                cmdline_str = ' '.join(str(arg) for arg in cmdline).lower()
                
                # Check which bot this process belongs to
                for bot_key, script_name in bot_scripts.items():
                    if script_name.lower() in cmdline_str and bot_key not in matching_procs:
                        matching_procs[bot_key] = proc
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Second pass: get CPU and memory for matched processes
        for bot_key, proc in matching_procs.items():
            try:
                # Get CPU (call it twice for accurate reading)
                proc.cpu_percent(interval=None)  # First call returns 0.0
                time.sleep(0.1)  # Small delay
                cpu = proc.cpu_percent(interval=None)
                
                # Get memory
                mem_info = proc.memory_info()
                mem_mb = mem_info.rss / 1024 / 1024
                
                stats["bots_running"] += 1
                stats["total_cpu_percent"] += cpu
                stats["total_memory_mb"] += mem_mb
                stats["by_bot"][bot_key] = {
                    "pid": proc.pid,
                    "cpu_percent": cpu,
                    "memory_mb": mem_mb,
                }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        return stats
    
    def analyze_gemini_usage(self) -> Dict[str, Any]:
        """Analyze Gemini API usage"""
        gemini_log = self.logs_dir / "gemini_usage.json"
        
        if not gemini_log.exists():
            return {"available": False, "message": "No Gemini usage data"}
        
        try:
            with open(gemini_log, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return {
                "available": True,
                "date": data.get("date", "Unknown"),
                "rephrase_calls": data.get("rephrase_calls", 0),
                "dm_calls": data.get("dm_calls", 0),
                "tokens_used": data.get("tokens_used", 0),
                "last_call": data.get("last_call", "Never"),
                "api_available": data.get("api_available", False),
            }
        except Exception as e:
            return {"available": False, "error": str(e)}
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive usage report"""
        print("="*70)
        print("BOT USAGE MONITOR - Generating Report")
        print("="*70)
        print("\nAnalyzing logs...")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "api_calls": self.analyze_api_calls(),
            "messages": self.analyze_messages(),
            "commands": self.analyze_commands(),
            "resources": self.analyze_resources(),
            "gemini": self.analyze_gemini_usage(),
        }
        
        return report
    
    def print_report(self, report: Dict[str, Any]):
        """Print formatted usage report"""
        print("\n" + "="*70)
        print("BOT USAGE REPORT")
        print("="*70)
        
        # API Calls
        api = report.get("api_calls", {})
        print("\n[API CALLS]")
        print("-"*70)
        print(f"Total API Calls (24h): {api.get('total_calls', 0):,}")
        print(f"Rate Limited: {api.get('rate_limited', 0)}")
        print(f"Errors (4xx/5xx): {api.get('errors', 0)}")
        
        if api.get('by_bot'):
            print("\nBy Bot:")
            for bot, count in sorted(api['by_bot'].items(), key=lambda x: x[1], reverse=True):
                print(f"  {bot:20} {count:>6,} calls")
        
        if api.get('by_endpoint'):
            print("\nTop Endpoints:")
            top_endpoints = sorted(api['by_endpoint'].items(), key=lambda x: x[1], reverse=True)[:10]
            for endpoint, count in top_endpoints:
                print(f"  {endpoint[:50]:50} {count:>6,}")
        
        # Messages
        messages = report.get("messages", {})
        print("\n[MESSAGES]")
        print("-"*70)
        print(f"Total Messages (24h): {messages.get('total_messages', 0):,}")
        if messages.get('by_bot'):
            print("\nBy Bot:")
            for bot, count in sorted(messages['by_bot'].items(), key=lambda x: x[1], reverse=True):
                print(f"  {bot:20} {count:>6,} messages")
        
        # Resources
        resources = report.get("resources", {})
        print("\n[RESOURCE USAGE]")
        print("-"*70)
        if resources.get('error'):
            print(f"  [WARN] {resources['error']}")
        else:
            print(f"Bots Running: {resources.get('bots_running', 0)}")
            print(f"Total CPU: {resources.get('total_cpu_percent', 0):.1f}%")
            print(f"Total Memory: {resources.get('total_memory_mb', 0):.1f} MB")
            
            if resources.get('by_bot'):
                print("\nBy Bot:")
                for bot, info in resources['by_bot'].items():
                    pid = info.get('pid', '?')
                    cpu = info.get('cpu_percent', 0)
                    mem = info.get('memory_mb', 0)
                    print(f"  {bot:20} PID {pid:>6} | CPU {cpu:>5.1f}% | MEM {mem:>6.1f} MB")
        
        # Gemini Usage
        gemini = report.get("gemini", {})
        if gemini.get("available"):
            print("\n[GEMINI API]")
            print("-"*70)
            print(f"Date: {gemini.get('date', 'Unknown')}")
            print(f"Rephrase Calls: {gemini.get('rephrase_calls', 0)} / 100")
            print(f"DM Calls: {gemini.get('dm_calls', 0)}")
            print(f"Tokens Used: {gemini.get('tokens_used', 0):,}")
            print(f"Last Call: {gemini.get('last_call', 'Never')}")
            print(f"Status: {'Available' if gemini.get('api_available') else 'Unavailable'}")
        
        print("\n" + "="*70)
        print("Report generated at:", report.get('timestamp', 'Unknown'))
        print("="*70)

def main():
    """Main function"""
    monitor = BotUsageMonitor()
    
    try:
        report = monitor.generate_report()
        monitor.print_report(report)
        
        # Save report to file
        report_file = project_root / "logs" / "usage_report.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n[OK] Report saved to: {report_file}")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

