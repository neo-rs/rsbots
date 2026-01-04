#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verify bots are ACTUALLY running - not just what logs say
"""

import sys
import psutil
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from neonxt.core.basic_bot_runner import BOT_SCRIPTS, _iter_bot_pids, _normalize_bot_name

print("="*70)
print("VERIFYING BOTS ARE ACTUALLY RUNNING")
print("="*70)
print()

all_running = True

for bot_key in ["testcenter", "datamanagerbot", "discumbot", "pingbot"]:
    normalized = _normalize_bot_name(bot_key)
    cfg = BOT_SCRIPTS.get(normalized)
    
    if not cfg:
        print(f"{bot_key:20} -> CONFIG NOT FOUND")
        all_running = False
        continue
    
    # Get PIDs from basic_bot_runner
    pids = list(_iter_bot_pids(cfg))
    
    if not pids:
        print(f"{bot_key:20} -> NOT RUNNING (no PIDs found)")
        all_running = False
        continue
    
    # Check if process actually exists
    alive_count = 0
    for pid in pids:
        if psutil.pid_exists(pid):
            try:
                proc = psutil.Process(pid)
                cmdline = proc.cmdline()
                cmdline_str = ' '.join(cmdline[:3]) if cmdline else 'N/A'
                
                # Verify it's actually the bot script
                script_found = any(cfg.script.split('/')[-1] in str(arg) for arg in cmdline)
                status = "ALIVE" if script_found else "ALIVE (wrong process?)"
                
                print(f"{bot_key:20} -> PID {pid:>6} {status} | {cmdline_str[:50]}...")
                alive_count += 1
            except Exception as e:
                print(f"{bot_key:20} -> PID {pid:>6} EXISTS but error: {e}")
        else:
            print(f"{bot_key:20} -> PID {pid:>6} DEAD (process not found)")
    
    if alive_count == 0:
        all_running = False
        print(f"{bot_key:20} -> NO RUNNING PROCESSES FOUND")

print()
print("="*70)
if all_running:
    print("RESULT: ALL BOTS ARE ACTUALLY RUNNING")
else:
    print("RESULT: SOME BOTS ARE NOT RUNNING")
print("="*70)

