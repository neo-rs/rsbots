#!/usr/bin/env python3
"""
Send Startup DM to Neo
======================
Sends a Discord DM with startup status.
Called from START.bat after startup sequence.
"""

import sys
import asyncio
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def send_dm(status: str, errors: int, warnings: int):
    """Send startup DM to Neo."""
    try:
        import discord
        from neonxt.core.config import MENTION_BOT_TOKEN
        
        if not MENTION_BOT_TOKEN:
            print("    [WARN] No MENTION_BOT_TOKEN (testcenter) token")
            return False
        
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        sent = False
        
        @client.event
        async def on_ready():
            nonlocal sent
            try:
                user = await client.fetch_user(971528709876113478)
                
                color = 0x00ff88 if status == "HEALTHY" else (0xffaa00 if status == "WARNING" else 0xff4444)
                emoji = "✅" if status == "HEALTHY" else ("⚠️" if status == "WARNING" else "❌")
                
                embed = discord.Embed(
                    title=f'{emoji} Mirror World Startup Complete',
                    description='Unified startup sequence finished',
                    color=color
                )
                embed.add_field(name='Status', value=status, inline=True)
                embed.add_field(name='Errors', value=str(errors), inline=True)
                embed.add_field(name='Warnings', value=str(warnings), inline=True)
                embed.add_field(name='Services', value='Dashboard: :8080\nBot Service: :8000', inline=False)
                embed.set_footer(text='Auto-generated startup report')
                
                await user.send(embed=embed)
                sent = True
                print(f"    [OK]   DM sent to Neo ({status})")
            except discord.Forbidden:
                print("    [WARN] Cannot DM Neo - DMs disabled")
            except Exception as e:
                print(f"    [WARN] Could not DM: {e}")
            finally:
                await client.close()
        
        # Run with timeout
        try:
            await asyncio.wait_for(client.start(MENTION_BOT_TOKEN), timeout=15.0)
        except asyncio.TimeoutError:
            if not sent:
                print("    [WARN] DM timeout - bot taking too long")
            await client.close()
        
        return sent
        
    except ImportError as e:
        print(f"    [WARN] Discord not available: {e}")
        return False
    except Exception as e:
        print(f"    [WARN] DM service error: {e}")
        return False


def main():
    """Main entry point."""
    # Parse args: status errors warnings
    status = sys.argv[1] if len(sys.argv) > 1 else "UNKNOWN"
    errors = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    warnings = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    
    # Run async
    success = asyncio.run(send_dm(status, errors, warnings))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()


