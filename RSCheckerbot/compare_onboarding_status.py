"""
Compare RSCheckerbot day_1 sends with RSOnboarding ticket creation

This script:
1. Finds all members who received day_1 DM from RSCheckerbot
2. Checks RSOnboarding's tickets.json to see which ones got tickets
3. Reports discrepancies (received day_1 but no ticket)

Usage: python compare_onboarding_status.py
"""
import os
import json
import re
import asyncio
from pathlib import Path
import discord
from discord.ext import commands

# Get script directory
script_dir = Path(__file__).parent
parent_dir = script_dir.parent

# Load RSCheckerbot config
rs_checker_config_path = script_dir / "config.json"
with open(rs_checker_config_path, "r") as f:
    rs_checker_config = json.load(f)

# Load RSOnboarding config
rs_onboarding_config_path = parent_dir / "RSOnboarding" / "config.json"
rs_onboarding_tickets_path = parent_dir / "RSOnboarding" / "tickets.json"

TOKEN = os.environ.get("DISCORD_BOT_TOKEN") or rs_checker_config.get("bot_token")
GUILD_ID = rs_checker_config.get("guild_id")
LOG_FIRST_CHANNEL_ID = rs_checker_config.get("dm_sequence", {}).get("log_first_channel_id")
WELCOME_ROLE_ID = rs_checker_config.get("dm_sequence", {}).get("welcome_role_id")

intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        print(f"❌ Guild {GUILD_ID} not found")
        await bot.close()
        return
    
    log_channel = bot.get_channel(LOG_FIRST_CHANNEL_ID)
    if not log_channel:
        print(f"❌ Log channel {LOG_FIRST_CHANNEL_ID} not found")
        await bot.close()
        return
    
    # Load RSOnboarding tickets
    onboarding_tickets = {}
    if rs_onboarding_tickets_path.exists():
        try:
            with open(rs_onboarding_tickets_path, "r", encoding="utf-8") as f:
                onboarding_tickets = json.load(f)
            print(f"✅ Loaded {len(onboarding_tickets)} tickets from RSOnboarding")
        except Exception as e:
            print(f"⚠️ Could not load RSOnboarding tickets: {e}")
    else:
        print(f"⚠️ RSOnboarding tickets.json not found at {rs_onboarding_tickets_path}")
    
    print(f"\n🔍 Scanning channel {log_channel.name} ({LOG_FIRST_CHANNEL_ID})...")
    print(f"📋 Looking for day_1 messages...\n")
    
    # Find all "Sent day_1" or "DM Sequence Started" messages
    day_1_users = {}  # user_id -> message info
    messages_checked = 0
    messages_with_day1 = 0
    
    async for message in log_channel.history(limit=1000):
        messages_checked += 1
        content = message.content or ""
        
        if ("day_1" in content.lower() and ("sent" in content.lower() or "✅" in content or "🧵" in content)) or \
           ("dm sequence started" in content.lower()):
            messages_with_day1 += 1
            try:
                # Match Discord IDs (17-19 digits)
                user_id_match = re.search(r'\((\d{17,19})\)', content)
                if user_id_match:
                    user_id_str = user_id_match.group(1)
                    user_id = int(user_id_str)
                    
                    if user_id not in day_1_users:
                        day_1_users[user_id] = {
                            "message_id": message.id,
                            "timestamp": message.created_at.isoformat(),
                            "message_content": content[:200]
                        }
            except (ValueError, IndexError) as e:
                pass
    
    print(f"📊 Checked {messages_checked} messages, found {messages_with_day1} messages mentioning day_1")
    print(f"✅ Found {len(day_1_users)} unique members who received day_1 DM\n")
    
    # Check which ones have tickets in RSOnboarding
    has_ticket = []
    missing_ticket = []
    not_in_guild = []
    
    welcome_role = guild.get_role(WELCOME_ROLE_ID) if WELCOME_ROLE_ID else None
    
    for user_id, info in day_1_users.items():
        member = guild.get_member(user_id)
        
        # Check if they have a ticket in RSOnboarding
        has_onboarding_ticket = str(user_id) in onboarding_tickets
        
        if not member:
            not_in_guild.append((user_id, info, has_onboarding_ticket))
        elif has_onboarding_ticket:
            has_ticket.append((user_id, member, info))
        else:
            # Check if they have Welcome role (should trigger ticket creation)
            has_welcome = welcome_role and welcome_role in member.roles
            missing_ticket.append((user_id, member, info, has_welcome))
    
    # Print results
    print("=" * 80)
    print("📊 COMPARISON RESULTS")
    print("=" * 80)
    
    print(f"\n✅ Members with RSOnboarding ticket: {len(has_ticket)}")
    print(f"❌ Members MISSING RSOnboarding ticket: {len(missing_ticket)}")
    print(f"🚫 Members no longer in guild: {len(not_in_guild)}")
    
    if missing_ticket:
        print(f"\n{'=' * 80}")
        print("❌ MEMBERS WHO GOT DAY_1 BUT NO RSOnboarding TICKET:")
        print(f"{'=' * 80}\n")
        
        for user_id, member, info, has_welcome in missing_ticket:
            print(f"👤 {member.display_name} ({member.name})")
            print(f"   ID: {user_id}")
            print(f"   Received day_1 at: {info['timestamp']}")
            print(f"   Has Welcome role: {'✅ YES' if has_welcome else '❌ NO'}")
            if has_welcome:
                print(f"   ⚠️ Has Welcome role but no ticket - RSOnboarding should have created one!")
            else:
                print(f"   ℹ️ No Welcome role - ticket creation requires Welcome role")
            print(f"   Current roles: {', '.join([r.name for r in member.roles if r.name != '@everyone'])}")
            print()
    
    if not_in_guild:
        print(f"\n{'=' * 80}")
        print("🚫 MEMBERS NO LONGER IN GUILD:")
        print(f"{'=' * 80}\n")
        
        for user_id, info, has_ticket_flag in not_in_guild:
            print(f"👤 User ID: {user_id}")
            print(f"   Received day_1 at: {info['timestamp']}")
            print(f"   Had RSOnboarding ticket: {'✅ YES' if has_ticket_flag else '❌ NO'}")
            print()
    
    # Save detailed report
    report = {
        "total_day_1_sent": len(day_1_users),
        "has_onboarding_ticket": len(has_ticket),
        "missing_onboarding_ticket": len(missing_ticket),
        "not_in_guild": len(not_in_guild),
        "missing_ticket_details": [
            {
                "user_id": str(user_id),
                "username": member.name,
                "display_name": member.display_name,
                "received_day1_at": info["timestamp"],
                "has_welcome_role": welcome_role and welcome_role in member.roles,
                "current_roles": [r.name for r in member.roles if r.name != "@everyone"]
            }
            for user_id, member, info, has_welcome in missing_ticket
        ],
        "has_ticket_details": [
            {
                "user_id": str(user_id),
                "username": member.name,
                "display_name": member.display_name,
                "received_day1_at": info["timestamp"],
                "ticket_data": onboarding_tickets.get(str(user_id), {})
            }
            for user_id, member, info in has_ticket
        ]
    }
    
    report_path = script_dir / "onboarding_comparison_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Detailed report saved to: {report_path}")
    print("\n✅ Analysis complete!")
    
    await bot.close()

if __name__ == "__main__":
    bot.run(TOKEN)

