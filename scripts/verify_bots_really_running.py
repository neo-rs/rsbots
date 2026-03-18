#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verify MW bots are actually running (no neonxt). Uses psutil + script name in cmdline.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import psutil

# MW bots only
BOT_SCRIPTS = [
    ("datamanagerbot", "datamanagerbot.py"),
    ("discumbot", "discumbot.py"),
    ("pingbot", "pingbot.py"),
]

print("=" * 70)
print("VERIFYING MW BOTS ARE RUNNING")
print("=" * 70)
print()

all_running = True
for bot_key, script_name in BOT_SCRIPTS:
    found = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            cmdline_str = " ".join(str(a) for a in cmdline).lower()
            if script_name.lower() in cmdline_str:
                found.append((proc.info["pid"], cmdline))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if not found:
        print(f"{bot_key:20} -> NOT RUNNING")
        all_running = False
    else:
        pid, cmd = found[0]
        preview = " ".join(str(a) for a in cmd[:3])[:50] if cmd else "N/A"
        print(f"{bot_key:20} -> PID {pid:>6} ALIVE | {preview}...")

print()
print("=" * 70)
print("RESULT: ALL MW BOTS RUNNING" if all_running else "RESULT: SOME BOTS NOT RUNNING")
print("=" * 70)
sys.exit(0 if all_running else 1)
