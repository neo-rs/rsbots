#!/usr/bin/env python3
"""
Stop all bot processes cleanly.
"""
import sys
import subprocess
import platform
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def stop_all_bots():
    """Stop all bot processes."""
    print("Stopping all bot processes...")
    
    # Try using unified_bot_runner first
    try:
        from neonxt.core.unified_bot_runner import stop_all_bots, stop_service
        print("  Stopping bot service...")
        stop_service()
        print("  Stopping all bots...")
        results = stop_all_bots()
        for bot, result in results.items():
            if result.get('success'):
                print(f"  ✓ {bot}: {result.get('message', 'stopped')}")
            else:
                print(f"  ✗ {bot}: {result.get('error', 'failed')}")
    except Exception as e:
        print(f"  Warning: Could not use unified_bot_runner: {e}")
    
    # Also kill by process name (Windows)
    if platform.system() == "Windows":
        print("\nKilling remaining Python bot processes...")
        bot_scripts = [
            "testcenter_bot.py",
            "datamanagerbot.py",
            "discumbot.py",
            "pingbot.py",
            "rs_forwarder_bot.py"
        ]
        
        for script in bot_scripts:
            try:
                # Use wmic to find and kill processes
                result = subprocess.run(
                    ['wmic', 'process', 'where', f'commandline like "%{script}%"', 'get', 'processid'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0 and 'ProcessId' in result.stdout:
                    lines = result.stdout.strip().split('\n')
                    for line in lines[1:]:  # Skip header
                        pid = line.strip()
                        if pid and pid.isdigit():
                            try:
                                subprocess.run(['taskkill', '/F', '/PID', pid], timeout=3, capture_output=True)
                                print(f"  ✓ Killed {script} (PID: {pid})")
                            except:
                                pass
            except Exception as e:
                print(f"  ✗ Error killing {script}: {e}")
    
    print("\nDone!")

if __name__ == "__main__":
    stop_all_bots()


















