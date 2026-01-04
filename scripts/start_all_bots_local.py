#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Start all bots locally with usage monitoring
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

from neonxt.core.basic_bot_runner import start_bot, BOT_SCRIPTS

def start_all_bots_local():
    """Start all bots locally"""
    print("="*70)
    print("STARTING ALL BOTS LOCALLY")
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
    print("SUMMARY")
    print("="*70)
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    for bot_key, success in results.items():
        status = "[OK]" if success else "[FAIL]"
        bot_name = BOT_SCRIPTS[bot_key].name
        print(f"  {status} {bot_name}")
    
    print(f"\nStarted: {success_count}/{total_count} bots")
    print("\nTo monitor usage, run:")
    print("  python scripts/monitor_bot_usage.py")
    print("="*70)
    
    return success_count == total_count

if __name__ == "__main__":
    try:
        success = start_all_bots_local()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

