#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Start All Bots and Monitor Them Live
Starts all bots, then shows live monitoring with totals
"""

import os
import sys
import time
import threading
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Import bot starter
from neonxt.core.basic_bot_runner import start_bot, BOT_SCRIPTS

try:
    import psutil
except ImportError:
    print("[ERROR] psutil not installed. Install with: pip install psutil")
    sys.exit(1)


class StartAndMonitor:
    """Start bots and monitor them"""
    
    def __init__(self):
        self.running = True
        self.bot_scripts = {
            "testcenter": "testcenter_bot.py",
            "datamanagerbot": "datamanagerbot.py",
            "discumbot": "discumbot.py",
            "pingbot": "pingbot.py",
        }
        self.stats = {
            "start_time": time.time(),
            "total_memory_mb": 0.0,
            "total_cpu_percent": 0.0,
            "total_network_sent": 0,
            "total_network_recv": 0,
            "disk_usage_percent": 0.0,
            "disk_free_gb": 0.0,
            "bots_found": 0,
            "by_bot": {},
        }
        self.last_net_io = None
        
    def start_all_bots(self):
        """Start all bots"""
        print("="*70)
        print("STARTING ALL BOTS")
        print("="*70)
        print()
        
        # Start bots in priority order
        bots_sorted = sorted(BOT_SCRIPTS.items(), key=lambda x: x[1].priority)
        
        results = {}
        for bot_key, bot_config in bots_sorted:
            print(f"Starting {bot_config.name}...", end=" ", flush=True)
            result = start_bot(bot_key, force=False)
            
            if result.get("success"):
                pid = result.get("pid", "?")
                print(f"[OK] PID: {pid}")
                results[bot_key] = True
            else:
                error = result.get("error", "Unknown error")
                print(f"[FAIL] {error}")
                results[bot_key] = False
            
            time.sleep(2)  # Brief pause between starts
        
        print("\n" + "="*70)
        print("STARTUP SUMMARY")
        print("="*70)
        
        success_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        
        for bot_key, success in results.items():
            status = "[OK]" if success else "[FAIL]"
            bot_name = BOT_SCRIPTS[bot_key].name
            print(f"  {status} {bot_name}")
        
        print(f"\nStarted: {success_count}/{total_count} bots")
        print("\nStarting live monitor in 3 seconds...")
        print("="*70)
        time.sleep(3)
        
        return success_count == total_count
    
    def format_bytes(self, bytes_val: int) -> str:
        """Format bytes to human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def find_bot_processes(self):
        """Find all running bot processes - verifies they're actually alive"""
        matching_procs = {}
        
        # Use the EXACT same method as basic_bot_runner._iter_bot_pids
        # but verify processes are actually alive
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").lower()
                if "python" not in name:
                    continue
                
                cmdline = proc.info.get("cmdline") or []
                if not cmdline:
                    continue
                
                # Check each bot script
                for bot_key, script_name in self.bot_scripts.items():
                    # Skip if we already found this bot
                    if bot_key in matching_procs:
                        continue
                    
                    # Check if script filename appears in any cmdline argument
                    # This is the exact same check as basic_bot_runner
                    if any(script_name in str(arg) for arg in cmdline):
                        # Verify process is actually alive and accessible
                        try:
                            # Try to access the process to verify it's real
                            pid = proc.info.get("pid")
                            if pid and psutil.pid_exists(pid):
                                # Get fresh process object to ensure it's alive
                                live_proc = psutil.Process(pid)
                                # Verify cmdline matches
                                live_cmdline = live_proc.cmdline()
                                if any(script_name in str(arg) for arg in live_cmdline):
                                    matching_procs[bot_key] = live_proc
                                    break
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            # Process died between check and access, skip it
                            continue
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
            except Exception:
                # Skip any other errors silently
                continue
        
        return matching_procs
    
    def update_stats(self):
        """Update all statistics"""
        # Find bot processes
        bot_procs = self.find_bot_processes()
        self.stats["bots_found"] = len(bot_procs)
        
        # Reset totals
        total_memory = 0.0
        total_cpu = 0.0
        by_bot = {}
        
        # Get stats for each bot
        for bot_key, proc in bot_procs.items():
            try:
                # Memory
                mem_info = proc.memory_info()
                mem_mb = mem_info.rss / 1024 / 1024
                total_memory += mem_mb
                
                # CPU (needs two calls for accuracy)
                proc.cpu_percent(interval=None)
                time.sleep(0.1)
                cpu = proc.cpu_percent(interval=None)
                total_cpu += cpu
                
                by_bot[bot_key] = {
                    "pid": proc.pid,
                    "memory_mb": mem_mb,
                    "cpu_percent": cpu,
                }
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        self.stats["total_memory_mb"] = total_memory
        self.stats["total_cpu_percent"] = total_cpu
        self.stats["by_bot"] = by_bot
        
        # Network usage (cumulative since boot, but we track changes)
        net_io = psutil.net_io_counters()
        if self.last_net_io:
            # Calculate difference
            sent_diff = net_io.bytes_sent - self.last_net_io.bytes_sent
            recv_diff = net_io.bytes_recv - self.last_net_io.bytes_recv
            self.stats["total_network_sent"] += sent_diff
            self.stats["total_network_recv"] += recv_diff
        else:
            # First run - just store current values
            self.stats["total_network_sent"] = net_io.bytes_sent
            self.stats["total_network_recv"] = net_io.bytes_recv
        
        self.last_net_io = net_io
        
        # Disk usage
        disk = psutil.disk_usage('/')
        self.stats["disk_usage_percent"] = disk.percent
        self.stats["disk_free_gb"] = disk.free / 1024 / 1024 / 1024
    
    def display_live(self):
        """Display live stats"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("="*70)
        print("LIVE BOT RESOURCE MONITOR")
        print("="*70)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Uptime: {int(time.time() - self.stats['start_time'])} seconds")
        print("="*70)
        
        # Totals
        print("\n[TOTALS - All Bots Combined]")
        print("-"*70)
        print(f"Bots Running:     {self.stats['bots_found']}")
        print(f"Total Memory:     {self.stats['total_memory_mb']:.1f} MB")
        print(f"Total CPU:        {self.stats['total_cpu_percent']:.1f}%")
        print(f"Network Sent:     {self.format_bytes(self.stats['total_network_sent'])}")
        print(f"Network Received: {self.format_bytes(self.stats['total_network_recv'])}")
        print(f"Disk Usage:       {self.stats['disk_usage_percent']:.1f}%")
        print(f"Disk Free:        {self.stats['disk_free_gb']:.2f} GB")
        
        # Per-bot breakdown
        if self.stats['by_bot']:
            print("\n[PER-BOT BREAKDOWN]")
            print("-"*70)
            for bot_key, info in sorted(self.stats['by_bot'].items()):
                pid = info['pid']
                mem = info['memory_mb']
                cpu = info['cpu_percent']
                print(f"{bot_key:20} PID {pid:>6} | CPU {cpu:>5.1f}% | MEM {mem:>6.1f} MB")
        
        print("\n" + "="*70)
        print("Press ENTER for detailed summary, Ctrl+C to exit")
        print("="*70)
    
    def print_summary(self):
        """Print detailed summary"""
        print("\n" + "="*70)
        print("DETAILED SUMMARY")
        print("="*70)
        
        runtime = time.time() - self.stats['start_time']
        hours = int(runtime // 3600)
        minutes = int((runtime % 3600) // 60)
        seconds = int(runtime % 60)
        
        print(f"\nMonitoring Duration: {hours}h {minutes}m {seconds}s")
        print(f"Start Time: {datetime.fromtimestamp(self.stats['start_time']).strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n[RESOURCE TOTALS]")
        print("-"*70)
        print(f"Total Memory Used:     {self.stats['total_memory_mb']:.2f} MB ({self.stats['total_memory_mb']/1024:.2f} GB)")
        print(f"Total CPU Usage:      {self.stats['total_cpu_percent']:.2f}%")
        print(f"Total Network Sent:    {self.format_bytes(self.stats['total_network_sent'])}")
        print(f"Total Network Recv:   {self.format_bytes(self.stats['total_network_recv'])}")
        print(f"Total Network Total:   {self.format_bytes(self.stats['total_network_sent'] + self.stats['total_network_recv'])}")
        print(f"Disk Usage:            {self.stats['disk_usage_percent']:.2f}%")
        print(f"Disk Free Space:       {self.stats['disk_free_gb']:.2f} GB")
        
        # Network rates
        if runtime > 0:
            sent_rate = self.stats['total_network_sent'] / runtime
            recv_rate = self.stats['total_network_recv'] / runtime
            print(f"\nAverage Network Speed:")
            print(f"  Sent:    {self.format_bytes(sent_rate)}/s")
            print(f"  Received: {self.format_bytes(recv_rate)}/s")
        
        # Per-bot details
        if self.stats['by_bot']:
            print("\n[PER-BOT DETAILS]")
            print("-"*70)
            for bot_key, info in sorted(self.stats['by_bot'].items()):
                print(f"\n{bot_key}:")
                print(f"  PID:        {info['pid']}")
                print(f"  Memory:     {info['memory_mb']:.2f} MB")
                print(f"  CPU:        {info['cpu_percent']:.2f}%")
        
        # System-wide stats
        print("\n[SYSTEM-WIDE STATS]")
        print("-"*70)
        try:
            # Overall system memory
            mem = psutil.virtual_memory()
            print(f"System Memory:  {mem.percent:.1f}% used ({mem.used/1024/1024/1024:.2f} GB / {mem.total/1024/1024/1024:.2f} GB)")
            
            # Overall system CPU
            cpu = psutil.cpu_percent(interval=1)
            print(f"System CPU:     {cpu:.1f}%")
            
            # Overall network
            net = psutil.net_io_counters()
            print(f"System Network (since boot):")
            print(f"  Sent:    {self.format_bytes(net.bytes_sent)}")
            print(f"  Received: {self.format_bytes(net.bytes_recv)}")
        except Exception as e:
            print(f"Could not get system stats: {e}")
        
        print("\n" + "="*70)
    
    def run(self):
        """Main function - start bots then monitor"""
        # Step 1: Start all bots
        self.start_all_bots()
        
        # Step 2: Start monitoring
        print("\nStarting live monitor...")
        print("Press ENTER for summary, Ctrl+C to exit\n")
        time.sleep(2)
        
        # Thread for handling Enter key
        def wait_for_enter():
            while self.running:
                try:
                    input()  # Wait for Enter
                    self.print_summary()
                    print("\nPress ENTER again for another summary, Ctrl+C to exit")
                except (EOFError, KeyboardInterrupt):
                    break
        
        enter_thread = threading.Thread(target=wait_for_enter, daemon=True)
        enter_thread.start()
        
        try:
            while self.running:
                self.update_stats()
                self.display_live()
                time.sleep(2)  # Update every 2 seconds
        except KeyboardInterrupt:
            self.running = False
            print("\n\nMonitoring stopped.")
            self.print_summary()


def main():
    """Main function"""
    monitor = StartAndMonitor()
    monitor.run()


if __name__ == "__main__":
    main()

