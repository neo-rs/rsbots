#!/usr/bin/env python3
"""Debug MW bot process detection (no neonxt)."""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import psutil

BOT_SCRIPTS = [
    ("datamanagerbot", "datamanagerbot.py"),
    ("discumbot", "discumbot.py"),
    ("pingbot", "pingbot.py"),
]

print("=" * 70)
print("DEBUG: MW Bot Process Detection")
print("=" * 70)
print("\n[psutil search by script name]")
print("-" * 70)
for bot_key, script_name in BOT_SCRIPTS:
    found = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if not cmdline:
                continue
            cmdline_str = " ".join(str(a) for a in cmdline).lower()
            if script_name.lower() in cmdline_str:
                found.append((proc.info["pid"], cmdline))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if found:
        pid, cmd = found[0]
        print(f"{bot_key:20} -> PID {pid}: {' '.join(str(a) for a in cmd[:2])} ... {cmd[-1] if cmd else ''}")
    else:
        print(f"{bot_key:20} -> NOT FOUND")
print("\n" + "=" * 70)
