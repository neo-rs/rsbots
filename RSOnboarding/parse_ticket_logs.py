"""
Parse RSOnboarding logs to extract all ticket creation/closing history

This script reads the log channel messages to build a complete history of
all tickets, since tickets.json only stores currently active tickets.
"""
import json
import re
import asyncio
from pathlib import Path
from datetime import datetime
import discord
from discord.ext import commands

# Get script directory
script_dir = Path(__file__).parent
config_path = script_dir / "config.json"

from mirror_world_config import load_config_with_secrets

# Load config.json + config.secrets.json (server-only)
config, _, secrets_path = load_config_with_secrets(script_dir)

TOKEN = config.get("bot_token")
GUILD_ID = config.get("guild_id")
LOG_CHANNEL_ID = config.get("log_channel_id")

if not TOKEN:
    print("❌ bot_token not found (expected in config.secrets.json)")
    print(f"   - config: {config_path}")
    print(f"   - secrets: {secrets_path}")
    exit(1)

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
    
    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if not log_channel:
        print(f"❌ Log channel {LOG_CHANNEL_ID} not found")
        await bot.close()
        return
    
    print(f"\n🔍 Scanning log channel: {log_channel.name} ({LOG_CHANNEL_ID})...")
    print(f"📋 Looking for ticket creation/closing messages...\n")
    
    ticket_history = {}  # user_id -> list of ticket events
    messages_scanned = 0
    
    async for message in log_channel.history(limit=2000):
        messages_scanned += 1
        
        # Check embeds for ticket information
        for embed in message.embeds:
            title = embed.title or ""
            description = embed.description or ""
            
            # Look for ticket created events
            if "Ticket Created" in title or "Ticket channel created successfully" in description:
                # Extract user ID from member field
                user_id = None
                channel_id = None
                timestamp = message.created_at.isoformat()
                
                # Check embed fields
                for field in embed.fields:
                    field_name = field.name or ""
                    field_value = field.value or ""
                    
                    if "Member" in field_name:
                        # Extract user ID from field like "Member\nID: `123456789`"
                        id_match = re.search(r'ID: `(\d{17,19})`', field_value)
                        if id_match:
                            user_id = int(id_match.group(1))
                    
                    if "Ticket Channel" in field_name:
                        # Extract channel ID
                        id_match = re.search(r'ID: `(\d{17,19})`', field_value)
                        if id_match:
                            channel_id = int(id_match.group(1))
                
                if user_id:
                    if user_id not in ticket_history:
                        ticket_history[user_id] = []
                    ticket_history[user_id].append({
                        "type": "created",
                        "channel_id": channel_id,
                        "timestamp": timestamp,
                        "message_id": message.id,
                        "raw_description": description
                    })
            
            # Look for ticket closed events
            elif "Ticket Closed" in title or "Auto-closed onboarding ticket" in description or "Closed onboarding ticket" in description:
                user_id = None
                timestamp = message.created_at.isoformat()
                
                for field in embed.fields:
                    field_name = field.name or ""
                    field_value = field.value or ""
                    
                    if "Member" in field_name:
                        id_match = re.search(r'ID: `(\d{17,19})`', field_value)
                        if id_match:
                            user_id = int(id_match.group(1))
                
                if user_id:
                    if user_id not in ticket_history:
                        ticket_history[user_id] = []
                    ticket_history[user_id].append({
                        "type": "closed",
                        "timestamp": timestamp,
                        "message_id": message.id,
                        "raw_description": description
                    })
    
    print(f"📊 Scanned {messages_scanned} messages")
    print(f"✅ Found ticket history for {len(ticket_history)} members\n")
    
    # Build report
    report = {
        "scan_date": datetime.now().isoformat(),
        "messages_scanned": messages_scanned,
        "total_members_with_tickets": len(ticket_history),
        "ticket_history": {}
    }
    
    for user_id, events in ticket_history.items():
        member = guild.get_member(user_id)
        member_info = {
            "user_id": str(user_id),
            "username": member.name if member else "Unknown",
            "display_name": member.display_name if member else "Unknown",
            "events": events
        }
        
        # Count created vs closed
        created_count = sum(1 for e in events if e["type"] == "created")
        closed_count = sum(1 for e in events if e["type"] == "closed")
        
        member_info["stats"] = {
            "total_created": created_count,
            "total_closed": closed_count,
            "currently_open": created_count - closed_count
        }
        
        report["ticket_history"][str(user_id)] = member_info
    
    # Save report
    report_path = script_dir / "ticket_history_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("=" * 80)
    print("📊 TICKET HISTORY SUMMARY")
    print("=" * 80)
    
    total_created = sum(m["stats"]["total_created"] for m in report["ticket_history"].values())
    total_closed = sum(m["stats"]["total_closed"] for m in report["ticket_history"].values())
    currently_open = sum(m["stats"]["currently_open"] for m in report["ticket_history"].values())
    
    print(f"\n📈 Statistics:")
    print(f"   Total tickets created: {total_created}")
    print(f"   Total tickets closed: {total_closed}")
    print(f"   Currently open: {currently_open}")
    print(f"   Members with ticket history: {len(ticket_history)}")
    
    # Show members with currently open tickets
    open_tickets = {
        uid: info for uid, info in report["ticket_history"].items()
        if info["stats"]["currently_open"] > 0
    }
    
    if open_tickets:
        print(f"\n🎫 Members with open tickets ({len(open_tickets)}):")
        for user_id, info in list(open_tickets.items())[:20]:  # Show first 20
            member = guild.get_member(int(user_id))
            print(f"   • {member.display_name if member else info['username']} ({user_id}) - {info['stats']['currently_open']} open")
        if len(open_tickets) > 20:
            print(f"   ... and {len(open_tickets) - 20} more")
    
    print(f"\n💾 Full report saved to: {report_path}")
    print("\n✅ Analysis complete!")
    
    await bot.close()

if __name__ == "__main__":
    bot.run(TOKEN)

