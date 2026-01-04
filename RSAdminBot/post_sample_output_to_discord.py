#!/usr/bin/env python3
"""
Post sample output to Discord test server showing RSCheckerbot enhancements.
This demonstrates what we built in this chat thread.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add repo root to path
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_config_with_secrets
import discord
from discord.ext import commands

async def post_sample_output():
    """Post sample output embed to Discord test server."""
    # Load config
    config = load_config_with_secrets("RSAdminBot")
    test_server_channel_id = config.get("systemd_events", {}).get("test_server_channel_id")
    
    if not test_server_channel_id:
        print("❌ test_server_channel_id not found in config")
        return
    
    # Load sample output
    sample_path = Path(__file__).resolve().parent / "enhancement_sample_output.json"
    if not sample_path.exists():
        print(f"❌ Sample output file not found: {sample_path}")
        return
    
    with open(sample_path, "r", encoding="utf-8") as f:
        sample_data = json.load(f)
    
    # Create bot
    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix="!", intents=intents)
    
    @bot.event
    async def on_ready():
        print(f"✅ Bot connected: {bot.user}")
        
        channel = bot.get_channel(int(test_server_channel_id))
        if not channel:
            print(f"❌ Channel {test_server_channel_id} not found")
            await bot.close()
            return
        
        # Create main summary embed
        summary = sample_data.get("enhancements_summary", {})
        embed = discord.Embed(
            title="📋 RSCheckerbot Enhancement Summary",
            description="All enhancements made in this chat thread",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )
        
        features = summary.get("features", [])
        for feature in features[:6]:  # Show first 6 features
            name = feature.get("name", "Unknown")
            desc = feature.get("description", "")
            file_info = feature.get("file", "")
            if file_info:
                desc += f"\n📁 `{file_info}`"
            embed.add_field(name=f"✅ {name}", value=desc, inline=False)
        
        embed.set_footer(text="Run !enhancementdetails for full details")
        await channel.send(embed=embed)
        
        # Create sample embed examples
        sample_embeds_data = sample_data.get("sample_embeds", {})
        
        # Payment Failed example
        if "payment_failed" in sample_embeds_data:
            pf_data = sample_embeds_data["payment_failed"]
            pf_embed = discord.Embed(
                title=pf_data.get("title", "Payment Failed"),
                color=pf_data.get("color", discord.Color.red())
            )
            for field in pf_data.get("fields", []):
                pf_embed.add_field(
                    name=field.get("name", ""),
                    value=field.get("value", ""),
                    inline=field.get("inline", False)
                )
            pf_embed.set_footer(text=pf_data.get("footer", ""))
            await channel.send("**Sample Embed: Payment Failed**", embed=pf_embed)
        
        # Lookup Needed example
        if "lookup_needed" in sample_embeds_data:
            ln_data = sample_embeds_data["lookup_needed"]
            ln_embed = discord.Embed(
                title=ln_data.get("title", "Lookup Needed"),
                color=ln_data.get("color", discord.Color.gold())
            )
            for field in ln_data.get("fields", []):
                ln_embed.add_field(
                    name=field.get("name", ""),
                    value=field.get("value", ""),
                    inline=field.get("inline", False)
                )
            ln_embed.set_footer(text=ln_data.get("footer", ""))
            await channel.send("**Sample Embed: Lookup Needed**", embed=ln_embed)
        
        # Member Joined example
        if "member_joined" in sample_embeds_data:
            mj_data = sample_embeds_data["member_joined"]
            mj_embed = discord.Embed(
                title=mj_data.get("title", "Member Joined"),
                color=mj_data.get("color", discord.Color.green())
            )
            for field in mj_data.get("fields", []):
                mj_embed.add_field(
                    name=field.get("name", ""),
                    value=field.get("value", ""),
                    inline=field.get("inline", False)
                )
            mj_embed.set_footer(text=mj_data.get("footer", ""))
            await channel.send("**Sample Embed: Member Joined**", embed=mj_embed)
        
        print("✅ Sample output posted to Discord")
        await bot.close()
    
    # Run bot
    token = config.get("bot_token")
    if not token:
        print("❌ bot_token not found in config")
        return
    
    await bot.start(token)

if __name__ == "__main__":
    import asyncio
    asyncio.run(post_sample_output())

