#!/usr/bin/env python3
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import psutil
from neonxt.core.basic_bot_runner import BOT_SCRIPTS, _iter_bot_pids, _normalize_bot_name

print("="*70)
print("DEBUG: Bot Process Detection")
print("="*70)

bot_scripts = {
    "testcenter": "testcenter_bot.py",
    "datamanagerbot": "datamanagerbot.py",
    "discumbot": "discumbot.py",
    "pingbot": "pingbot.py",
}

print("\n[Method 1: Using basic_bot_runner]")
print("-"*70)
for bot_key, script_name in bot_scripts.items():
    normalized = _normalize_bot_name(bot_key)
    cfg = BOT_SCRIPTS.get(normalized)
    if cfg:
        pids = list(_iter_bot_pids(cfg))
        print(f"{bot_key:20} -> PIDs: {pids}")
        if pids:
            try:
                proc = psutil.Process(pids[0])
                print(f"  {'':20} -> CMDLINE: {' '.join(proc.cmdline()[:3])}...")
            except Exception as e:
                print(f"  {'':20} -> ERROR: {e}")
    else:
        print(f"{bot_key:20} -> Config not found")

print("\n[Method 2: Using psutil search]")
print("-"*70)
for bot_key, script_name in bot_scripts.items():
    found = False
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.cmdline()
            if not cmdline:
                continue
            cmdline_str = ' '.join(str(arg) for arg in cmdline).lower()
            if script_name.lower() in cmdline_str:
                print(f"{bot_key:20} -> PID {proc.pid}: {cmdline[0]} ... {cmdline[-1] if cmdline else ''}")
                found = True
                break
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if not found:
        print(f"{bot_key:20} -> NOT FOUND")

print("\n" + "="*70)









