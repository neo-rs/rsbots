"""
Script to check which members received day_1 DM but didn't get Welcome role
(meaning RSOnboarding didn't create a ticket for them)

Usage: python check_missed_onboarding.py
"""
import os
import json
import asyncio
import re
import discord
from discord.ext import commands

# Get the script's directory and load config
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "config.json")

with open(config_path, "r") as f:
    config = json.load(f)

TOKEN = os.environ.get("DISCORD_BOT_TOKEN") or config.get("bot_token")
GUILD_ID = config.get("guild_id")
LOG_FIRST_CHANNEL_ID = config.get("dm_sequence", {}).get("log_first_channel_id")
WELCOME_ROLE_ID = config.get("dm_sequence", {}).get("welcome_role_id")

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
    
    welcome_role = guild.get_role(WELCOME_ROLE_ID) if WELCOME_ROLE_ID else None
    
    print(f"\n🔍 Scanning channel {log_channel.name} ({LOG_FIRST_CHANNEL_ID})...")
    print(f"📋 Looking for 'day_1' messages...")
    print(f"📄 Scanning up to 1000 recent messages...\n")
    
    # Find all "Sent day_1" messages
    day_1_users = {}  # user_id -> message info
    
    messages_checked = 0
    messages_with_day1 = 0
    
    async for message in log_channel.history(limit=1000):
        messages_checked += 1
        content = message.content or ""
        
        # Look for various formats: "Sent day_1", "**Sent day_1**", "DM Sequence Started", etc.
        # Also check for "DM Sequence Started" which happens when day_1 is enqueued
        if ("day_1" in content.lower() and ("sent" in content.lower() or "✅" in content or "🧵" in content)) or \
           ("dm sequence started" in content.lower()):
            messages_with_day1 += 1
            # Extract user ID from message
            # Format examples:
            # - "✅ **Sent day_1** to **username** (ID)"
            # - "🧵 **DM Sequence Started** for **username** (ID)"
            try:
                # Match pattern like (123456789012345678) - Discord IDs are 17-19 digits
                user_id_match = re.search(r'\((\d{17,19})\)', content)
                if user_id_match:
                    user_id_str = user_id_match.group(1)
                    user_id = int(user_id_str)
                    
                    if user_id not in day_1_users:  # Avoid duplicates
                        day_1_users[user_id] = {
                            "message_id": message.id,
                            "timestamp": message.created_at.isoformat(),
                            "message_content": content[:200]  # Truncate for storage
                        }
                else:
                    # Debug: show messages that mention day_1 but we can't extract ID
                    if "day_1" in content.lower():
                        print(f"⚠️ Could not find user ID in message: {content[:150]}...")
            except (ValueError, IndexError) as e:
                print(f"⚠️ Could not parse user ID from message: {content[:100]}... Error: {e}")
    
    print(f"📊 Checked {messages_checked} messages, found {messages_with_day1} messages mentioning day_1")
    
    print(f"✅ Found {len(day_1_users)} members who received day_1 DM\n")
    
    # Check which ones don't have Welcome role
    missing_welcome = []
    has_welcome = []
    not_in_guild = []
    
    for user_id, info in day_1_users.items():
        member = guild.get_member(user_id)
        if not member:
            not_in_guild.append((user_id, info))
        elif welcome_role and welcome_role in member.roles:
            has_welcome.append((user_id, member, info))
        else:
            missing_welcome.append((user_id, member, info))
    
    # Print results
    print("=" * 80)
    print("📊 RESULTS")
    print("=" * 80)
    
    print(f"\n✅ Members with Welcome role: {len(has_welcome)}")
    print(f"❌ Members MISSING Welcome role: {len(missing_welcome)}")
    print(f"🚫 Members no longer in guild: {len(not_in_guild)}")
    
    if missing_welcome:
        print(f"\n{'=' * 80}")
        print("❌ MEMBERS WHO GOT DAY_1 BUT MISSING WELCOME ROLE (Onboarding didn't trigger):")
        print(f"{'=' * 80}\n")
        
        for user_id, member, info in missing_welcome:
            print(f"👤 {member.display_name} ({member.name})")
            print(f"   ID: {user_id}")
            print(f"   Received day_1 at: {info['timestamp']}")
            print(f"   Current roles: {', '.join([r.name for r in member.roles if r.name != '@everyone'])}")
            print()
    
    if not_in_guild:
        print(f"\n{'=' * 80}")
        print("🚫 MEMBERS NO LONGER IN GUILD:")
        print(f"{'=' * 80}\n")
        
        for user_id, info in not_in_guild:
            print(f"👤 User ID: {user_id}")
            print(f"   Received day_1 at: {info['timestamp']}")
            print()
    
    # Save to file
    report = {
        "total_day_1_sent": len(day_1_users),
        "has_welcome_role": len(has_welcome),
        "missing_welcome_role": len(missing_welcome),
        "not_in_guild": len(not_in_guild),
        "missing_welcome_details": [
            {
                "user_id": str(user_id),
                "username": member.name,
                "display_name": member.display_name,
                "received_at": info["timestamp"],
                "current_roles": [r.name for r in member.roles if r.name != "@everyone"]
            }
            for user_id, member, info in missing_welcome
        ],
        "not_in_guild_details": [
            {
                "user_id": str(user_id),
                "received_at": info["timestamp"]
            }
            for user_id, info in not_in_guild
        ]
    }
    
    report_path = os.path.join(script_dir, "missed_onboarding_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"\n💾 Report saved to: {report_path}")
    print("\n✅ Analysis complete!")
    
    await bot.close()

if __name__ == "__main__":
    bot.run(TOKEN)

