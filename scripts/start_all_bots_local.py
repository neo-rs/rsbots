#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Start all MW bots locally (no neonxt). Uses subprocess like run_start_all_once.
"""

import os
import sys
import time
import subprocess
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"

# MW bots only, priority order
BOT_SCRIPTS = [
    ("datamanagerbot", "MWBots/MWDataManagerBot/datamanagerbot.py", "DataManager"),
    ("discumbot", "MWBots/MWDiscumBot/discumbot.py", "Discum"),
    ("pingbot", "MWBots/MWPingBot/pingbot.py", "Ping"),
]


def start_all_bots_local():
    """Start all MW bots locally via subprocess."""
    print("=" * 70)
    print("STARTING MW BOTS LOCALLY")
    print("=" * 70)
    print()
    results = {}
    for bot_key, rel_path, display_name in BOT_SCRIPTS:
        script_path = project_root / rel_path
        print(f"Starting {display_name}...", end=" ", flush=True)
        if not script_path.exists():
            print(f"[FAIL] script not found: {script_path}")
            results[bot_key] = False
            continue
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONPATH"] = str(project_root)
        try:
            proc = subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=project_root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            time.sleep(1)
            if proc.poll() is None:
                print(f"[OK] PID: {proc.pid}")
                results[bot_key] = True
            else:
                err = (proc.stderr.read() or b"").decode("utf-8", errors="replace")[:200]
                print(f"[FAIL] exited: {err}")
                results[bot_key] = False
        except Exception as e:
            print(f"[FAIL] {e}")
            results[bot_key] = False
        time.sleep(2)
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for bot_key, _, display_name in BOT_SCRIPTS:
        status = "[OK]" if results.get(bot_key) else "[FAIL]"
        print(f"  {status} {display_name}")
    print(f"\nStarted: {sum(1 for v in results.values() if v)}/{len(results)} bots")
    print("\nTo monitor usage, run:")
    print("  python scripts/monitor_bot_usage.py")
    print("=" * 70)
    return all(results.values())

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

