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
    """Stop all bot processes (MW bots only; no neonxt)."""
    print("Stopping all bot processes...")
    if platform.system() == "Windows":
        print("Killing Python bot processes by script name...")
        bot_scripts = [
            "datamanagerbot.py",
            "discumbot.py",
            "pingbot.py",
            "rs_forwarder_bot.py",
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
    else:
        print("  (Non-Windows: run pkill or kill manually for bot processes)")
    print("\nDone!")

if __name__ == "__main__":
    stop_all_bots()


















