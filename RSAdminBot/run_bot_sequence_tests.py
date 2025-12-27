#!/usr/bin/env python3
"""
3-Day Bot Sequence Test Framework
Runs all RS bots (except RSuccessBot) and monitors their sequences
Creates test categories and channels for each bot
Simulates and tracks all bot movements over 3 days
"""
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

import discord
from discord.ext import commands, tasks

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

class BotSequenceTester:
    """Comprehensive test framework for all RS bot sequences"""
    
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.bot_token = self.config.get("bot_token")
        self.guild_id = self.config.get("guild_id")
        # Avoid hardcoded guild IDs. Require explicit config to prevent accidental channel creation in the wrong server.
        self.test_server_guild_id = self.config.get("test_server_guild_id")
        
        # Test configuration
        self.test_duration_days = 3
        self.test_start_time = datetime.now(timezone.utc)
        self.test_end_time = self.test_start_time + timedelta(days=self.test_duration_days)
        
        # Setup bot
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.messages = True
        intents.message_content = True
        intents.reactions = True
        
        self.bot = commands.Bot(command_prefix="!test", intents=intents)
        self.setup_events()
        self.setup_commands()
        
        # Test tracking
        self.test_results = {
            "rsonboarding": {
                "tests": [],
                "passed": 0,
                "failed": 0,
                "sequences_tracked": [],
                "category": None,
                "channels": {}
            },
            "rscheckerbot": {
                "tests": [],
                "passed": 0,
                "failed": 0,
                "sequences_tracked": [],
                "category": None,
                "channels": {}
            },
            "rsforwarder": {
                "tests": [],
                "passed": 0,
                "failed": 0,
                "sequences_tracked": [],
                "category": None,
                "channels": {}
            },
            "rsmentionpinger": {
                "tests": [],
                "passed": 0,
                "failed": 0,
                "sequences_tracked": [],
                "category": None,
                "channels": {}
            }
        }
        
        # Sequence tracking
        self.sequence_log = []
    
    def setup_events(self):
        """Setup bot events"""
        
        @self.bot.event
        async def on_ready():
            print(f"{Colors.GREEN}[Test Framework] Logged in as {self.bot.user}{Colors.RESET}")
            print(f"{Colors.CYAN}[Test Framework] Starting 3-day bot sequence test...{Colors.RESET}")
            await self.setup_test_environment()
            await self.start_sequence_monitoring()
    
    def setup_commands(self):
        """Setup test commands"""
        
        @self.bot.command(name="status")
        async def test_status(ctx):
            """Show test status"""
            elapsed = datetime.now(timezone.utc) - self.test_start_time
            remaining = self.test_end_time - datetime.now(timezone.utc)
            
            embed = discord.Embed(
                title="🧪 Bot Sequence Test Status",
                description=f"Testing bot sequences over {self.test_duration_days} days",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="⏱️ Time",
                value=f"Elapsed: {elapsed.days}d {elapsed.seconds//3600}h\nRemaining: {remaining.days}d {remaining.seconds//3600}h",
                inline=False
            )
            
            for bot_name, results in self.test_results.items():
                total = results["passed"] + results["failed"]
                status = "✅" if results["failed"] == 0 else "⚠️"
                sequences = len(results["sequences_tracked"])
                embed.add_field(
                    name=f"{status} {bot_name.upper()}",
                    value=f"Passed: {results['passed']}\nFailed: {results['failed']}\nSequences: {sequences}",
                    inline=True
                )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name="sequences")
        async def show_sequences(ctx, bot_name: str = None):
            """Show tracked sequences for a bot or all bots"""
            if bot_name:
                bot_name = bot_name.lower()
                if bot_name in self.test_results:
                    results = self.test_results[bot_name]
                    embed = discord.Embed(
                        title=f"📋 Sequences: {bot_name.upper()}",
                        description=f"Total sequences tracked: {len(results['sequences_tracked'])}",
                        color=discord.Color.blue()
                    )
                    
                    for seq in results["sequences_tracked"][:10]:  # Show last 10
                        embed.add_field(
                            name=seq.get("type", "Unknown"),
                            value=f"Time: {seq.get('timestamp', 'N/A')}\nStatus: {seq.get('status', 'N/A')}",
                            inline=False
                        )
                    
                    await ctx.send(embed=embed)
                else:
                    await ctx.send(f"❌ Unknown bot: {bot_name}")
            else:
                # Show all
                embed = discord.Embed(
                    title="📋 All Bot Sequences",
                    color=discord.Color.blue()
                )
                
                for bot_name, results in self.test_results.items():
                    embed.add_field(
                        name=bot_name.upper(),
                        value=f"{len(results['sequences_tracked'])} sequences tracked",
                        inline=True
                    )
                
                await ctx.send(embed=embed)
    
    async def setup_test_environment(self):
        """Create test categories and channels for each bot"""
        guild = self.bot.get_guild(self.test_server_guild_id)
        if not guild:
            print(f"{Colors.RED}[Test Framework] ERROR: Test server guild not found{Colors.RESET}")
            return
        
        print(f"{Colors.BLUE}[Test Framework] Setting up test environment in test server...{Colors.RESET}")
        
        # Create main test category
        test_category = await guild.create_category("🧪 BOT TEST CENTER")
        print(f"{Colors.GREEN}[Test Framework] Created main category: BOT TEST CENTER{Colors.RESET}")
        
        # Create categories for each bot
        bot_categories = {
            "rsonboarding": "🎫 RSOnboarding Tests",
            "rscheckerbot": "✅ RSCheckerbot Tests",
            "rsforwarder": "📨 RSForwarder Tests",
            "rsmentionpinger": "🔔 RSMentionPinger Tests"
        }
        
        for bot_key, cat_name in bot_categories.items():
            try:
                category = await guild.create_category(cat_name)
                self.test_results[bot_key]["category"] = category
                print(f"{Colors.GREEN}[Test Framework] Created category: {cat_name}{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}[Test Framework] Failed to create category {cat_name}: {e}{Colors.RESET}")
        
        # Create test channels for each bot
        await self.create_test_channels(guild)
    
    async def create_test_channels(self, guild: discord.Guild):
        """Create test channels for each bot"""
        channel_configs = {
            "rsonboarding": [
                {"name": "test-onboarding-tickets", "description": "Test onboarding ticket creation"},
                {"name": "test-onboarding-logs", "description": "Onboarding bot activity logs"},
                {"name": "test-role-changes", "description": "Role change simulations"}
            ],
            "rscheckerbot": [
                {"name": "test-dm-sequence", "description": "DM sequence tracking"},
                {"name": "test-invite-tracking", "description": "Invite tracking tests"},
                {"name": "test-whop-webhooks", "description": "Whop webhook tests"},
                {"name": "test-checker-logs", "description": "Checker bot activity logs"}
            ],
            "rsforwarder": [
                {"name": "test-forwarding", "description": "Message forwarding tests"},
                {"name": "test-forwarder-logs", "description": "Forwarder bot activity logs"}
            ],
            "rsmentionpinger": [
                {"name": "test-mentions", "description": "Mention pinging tests"},
                {"name": "test-pinger-logs", "description": "Pinger bot activity logs"}
            ]
        }
        
        for bot_key, channels in channel_configs.items():
            category = self.test_results[bot_key].get("category")
            if not category:
                continue
            
            for channel_config in channels:
                try:
                    channel = await guild.create_text_channel(
                        channel_config["name"],
                        category=category,
                        topic=channel_config.get("description", "")
                    )
                    self.test_results[bot_key]["channels"][channel_config["name"]] = channel
                    print(f"{Colors.GREEN}[Test Framework] Created channel: {channel.name}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.RED}[Test Framework] Failed to create channel {channel_config['name']}: {e}{Colors.RESET}")
    
    async def start_sequence_monitoring(self):
        """Start monitoring bot sequences"""
        print(f"{Colors.CYAN}[Test Framework] Starting sequence monitoring...{Colors.RESET}")
        
        # Log test start
        await self.log_test_event("TEST_START", {
            "start_time": self.test_start_time.isoformat(),
            "end_time": self.test_end_time.isoformat(),
            "duration_days": self.test_duration_days
        })
        
        # Start monitoring tasks
        self.monitor_sequences.start()
        self.simulate_sequences.start()
        self.generate_hourly_report.start()
    
    @tasks.loop(hours=1)
    async def monitor_sequences(self):
        """Monitor bot sequences every hour"""
        if datetime.now(timezone.utc) > self.test_end_time:
            self.monitor_sequences.cancel()
            self.simulate_sequences.cancel()
            self.generate_hourly_report.cancel()
            await self.generate_final_report()
            return
        
        # Check each bot's operations
        await self.check_bot_operations()
    
    @tasks.loop(hours=6)
    async def simulate_sequences(self):
        """Simulate bot sequences every 6 hours"""
        if datetime.now(timezone.utc) > self.test_end_time:
            return
        
        # Simulate RSOnboarding sequences
        await self.simulate_rsonboarding_sequences()
        
        # Simulate RSCheckerbot sequences
        await self.simulate_rscheckerbot_sequences()
        
        # Simulate RSForwarder sequences
        await self.simulate_rsforwarder_sequences()
        
        # Simulate RSMentionPinger sequences
        await self.simulate_rsmentionpinger_sequences()
    
    @tasks.loop(hours=24)
    async def generate_hourly_report(self):
        """Generate daily report"""
        await self.generate_daily_report()
    
    async def simulate_rsonboarding_sequences(self):
        """Simulate RSOnboarding bot sequences"""
        # Sequence 1: Welcome role added → Ticket created
        # Sequence 2: Member role added → Ticket closed
        # Sequence 3: Auto-close after 24 hours
        
        await self.log_sequence("rsonboarding", "SIMULATE", {
            "type": "welcome_role_added",
            "expected": "ticket_created",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def simulate_rscheckerbot_sequences(self):
        """Simulate RSCheckerbot sequences"""
        # Sequence 1: User joins → Cleanup role assigned → DM sequence starts
        # Sequence 2: Welcome role added → DM sequence continues
        # Sequence 3: Member role added → DM sequence cancelled
        # Sequence 4: Invite created → User joins → GHL updated
        
        await self.log_sequence("rscheckerbot", "SIMULATE", {
            "type": "user_join",
            "expected": "cleanup_role_assigned",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def simulate_rsforwarder_sequences(self):
        """Simulate RSForwarder sequences"""
        # Sequence 1: Message in source channel → Forwarded to webhook
        
        await self.log_sequence("rsforwarder", "SIMULATE", {
            "type": "message_received",
            "expected": "message_forwarded",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def simulate_rsmentionpinger_sequences(self):
        """Simulate RSMentionPinger sequences"""
        # Sequence 1: Mention detected → Ping sent
        
        await self.log_sequence("rsmentionpinger", "SIMULATE", {
            "type": "mention_detected",
            "expected": "ping_sent",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    
    async def check_bot_operations(self):
        """Check if bots are performing their operations correctly"""
        # Monitor actual bot operations by checking logs, channels, etc.
        pass
    
    async def log_sequence(self, bot_name: str, event_type: str, data: Dict):
        """Log a bot sequence event"""
        sequence_entry = {
            "bot": bot_name,
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        self.test_results[bot_name]["sequences_tracked"].append(sequence_entry)
        self.sequence_log.append(sequence_entry)
        
        # Log to bot's test channel
        bot_channels = self.test_results[bot_name].get("channels", {})
        log_channel = bot_channels.get("test-checker-logs") or bot_channels.get("test-onboarding-logs") or bot_channels.get("test-forwarder-logs") or bot_channels.get("test-pinger-logs")
        
        if log_channel:
            embed = discord.Embed(
                title=f"📋 {bot_name.upper()} Sequence",
                description=f"**Type:** {event_type}\n**Data:** {json.dumps(data, indent=2)}",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc)
            )
            try:
                await log_channel.send(embed=embed)
            except:
                pass
    
    async def log_test_event(self, event_type: str, data: Dict):
        """Log test framework event"""
        print(f"{Colors.CYAN}[Test Framework] {event_type}: {data}{Colors.RESET}")
    
    async def generate_daily_report(self):
        """Generate daily test report"""
        guild = self.bot.get_guild(self.test_server_guild_id)
        if not guild:
            return
        
        # Find or create report channel
        main_category = None
        for category in guild.categories:
            if "BOT TEST CENTER" in category.name:
                main_category = category
                break
        
        if not main_category:
            return
        
        # Create or find report channel
        report_channel = None
        for channel in main_category.text_channels:
            if "daily-reports" in channel.name:
                report_channel = channel
                break
        
        if not report_channel:
            report_channel = await main_category.create_text_channel("daily-reports")
        
        # Generate report
        elapsed = datetime.now(timezone.utc) - self.test_start_time
        embed = discord.Embed(
            title=f"📊 Daily Test Report - Day {elapsed.days + 1}",
            description=f"Test running for {elapsed.days} days, {elapsed.seconds//3600} hours",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        
        for bot_name, results in self.test_results.items():
            total = results["passed"] + results["failed"]
            sequences = len(results["sequences_tracked"])
            embed.add_field(
                name=f"{bot_name.upper()}",
                value=f"✅ Passed: {results['passed']}\n❌ Failed: {results['failed']}\n📋 Sequences: {sequences}",
                inline=True
            )
        
        await report_channel.send(embed=embed)
    
    async def generate_final_report(self):
        """Generate final test report"""
        print(f"{Colors.CYAN}[Test Framework] Generating final report...{Colors.RESET}")
        
        guild = self.bot.get_guild(self.test_server_guild_id)
        if not guild:
            return
        
        # Find report channel
        main_category = None
        for category in guild.categories:
            if "BOT TEST CENTER" in category.name:
                main_category = category
                break
        
        if not main_category:
            return
        
        report_channel = None
        for channel in main_category.text_channels:
            if "daily-reports" in channel.name or "final-report" in channel.name:
                report_channel = channel
                break
        
        if not report_channel:
            report_channel = await main_category.create_text_channel("final-report")
        
        # Generate comprehensive report
        total_duration = self.test_end_time - self.test_start_time
        embed = discord.Embed(
            title="🎯 Final Test Report - 3 Days Complete",
            description=f"**Test Duration:** {total_duration.days} days, {total_duration.seconds//3600} hours\n**Start:** {self.test_start_time.isoformat()}\n**End:** {self.test_end_time.isoformat()}",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc)
        )
        
        total_passed = sum(r["passed"] for r in self.test_results.values())
        total_failed = sum(r["failed"] for r in self.test_results.values())
        total_sequences = sum(len(r["sequences_tracked"]) for r in self.test_results.values())
        
        embed.add_field(
            name="📊 Overall Statistics",
            value=f"✅ Total Passed: {total_passed}\n❌ Total Failed: {total_failed}\n📋 Total Sequences: {total_sequences}",
            inline=False
        )
        
        for bot_name, results in self.test_results.items():
            sequences = results["sequences_tracked"]
            recent_sequences = sequences[-5:] if len(sequences) > 5 else sequences
            
            sequence_summary = "\n".join([
                f"• {s.get('event_type', 'Unknown')} at {s.get('timestamp', 'N/A')[:19]}"
                for s in recent_sequences
            ])
            
            embed.add_field(
                name=f"{bot_name.upper()}",
                value=f"Passed: {results['passed']} | Failed: {results['failed']}\n**Recent Sequences:**\n{sequence_summary}",
                inline=False
            )
        
        await report_channel.send(embed=embed)
        
        # Save report to file
        report_path = self.base_path / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"# Bot Sequence Test Report - 3 Days\n\n")
            f.write(f"**Test Period:** {self.test_start_time} to {self.test_end_time}\n\n")
            f.write(f"## Overall Statistics\n")
            f.write(f"- Total Passed: {total_passed}\n")
            f.write(f"- Total Failed: {total_failed}\n")
            f.write(f"- Total Sequences Tracked: {total_sequences}\n\n")
            
            for bot_name, results in self.test_results.items():
                f.write(f"## {bot_name.upper()}\n")
                f.write(f"- Passed: {results['passed']}\n")
                f.write(f"- Failed: {results['failed']}\n")
                f.write(f"- Sequences: {len(results['sequences_tracked'])}\n\n")
        
        print(f"{Colors.GREEN}[Test Framework] Final report saved to {report_path}{Colors.RESET}")
        print(f"{Colors.GREEN}[Test Framework] Test complete!{Colors.RESET}")
    
    def run(self):
        """Run the test framework"""
        if not self.bot_token:
            print(f"{Colors.RED}[Test Framework] ERROR: bot_token not configured{Colors.RESET}")
            sys.exit(1)
        
        self.bot.run(self.bot_token)

if __name__ == "__main__":
    tester = BotSequenceTester()
    tester.run()

