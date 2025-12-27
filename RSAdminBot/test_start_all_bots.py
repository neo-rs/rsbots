#!/usr/bin/env python3
"""Test starting all bots using RSAdminBot code"""

import sys
from pathlib import Path
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root))

from RSAdminBot.admin_bot import RSAdminBot

bot = RSAdminBot()

print("="*70)
print("Testing Bot Start Commands via RSAdminBot")
print("="*70)

# Test all bots from BOTS dict (canonical source)
for bot_name in bot.BOTS.keys():
    if bot_name not in bot.BOTS:
        print(f"❌ {bot_name}: Not in BOTS dictionary")
        continue
    
    bot_info = bot.BOTS[bot_name]
    service_name = bot_info["service"]
    
    print(f"\n{'='*70}")
    print(f"Testing: {bot_info['name']} ({service_name})")
    print('='*70)
    
    # Check current status using ServiceManager
    if not bot.service_manager:
        print(f"❌ {bot_name}: ServiceManager not available")
        continue
    
    exists, state, error = bot.service_manager.get_status(service_name)
    if exists and state:
        is_running = state == "active"
        print(f"Current status: {'🟢 Running' if is_running else '🔴 Stopped'}")
    else:
        print(f"Current status: ⚠️ Service not found or error: {error}")
        continue
    
    # Start command using ServiceManager
    print(f"Executing start command...")
    success, stdout, stderr = bot.service_manager.start(service_name, unmask=True)
    
    if success:
        print(f"✅ SUCCESS: {bot_info['name']} start command executed")
        if stdout:
            print(f"   Output: {stdout[:200]}")
        
        # Verify status after start (ServiceManager handles retry logic)
        is_running, verify_error = bot.service_manager.verify_started(service_name)
        if is_running:
            print(f"Status after start: 🟢 Running (verified)")
        else:
            print(f"Status after start: 🔴 Failed - {verify_error}")
    else:
        print(f"❌ FAILED: {bot_info['name']} start command failed")
        if stderr:
            print(f"   Error: {stderr[:200]}")
        if stdout:
            print(f"   Output: {stdout[:200]}")

print("\n" + "="*70)
print("Test Complete")
print("="*70)

