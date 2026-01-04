#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Live Bot Usage Monitor
Continuously monitors and displays bot usage in real-time
"""

import os
import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from scripts.monitor_bot_usage import BotUsageMonitor

def clear_screen():
    """Clear terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    """Main function - live monitoring loop"""
    monitor = BotUsageMonitor()
    update_interval = 5  # seconds
    
    print("="*70)
    print("LIVE BOT USAGE MONITOR")
    print("="*70)
    print(f"Update interval: {update_interval} seconds")
    print("Press Ctrl+C to exit")
    print("="*70)
    
    try:
        while True:
            clear_screen()
            print("="*70)
            print("LIVE BOT USAGE MONITOR")
            print(f"Last update: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*70)
            
            report = monitor.generate_report()
            monitor.print_report(report)
            
            print(f"\n[Refreshing in {update_interval} seconds...]")
            time.sleep(update_interval)
            
    except KeyboardInterrupt:
        print("\n\n[INFO] Monitoring stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

