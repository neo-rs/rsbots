#!/usr/bin/env python3
"""
RS Admin Bot
------------
Admin bot for server management. Runs invisible/offline.
Configuration is split across:
- config.json (non-secret settings)
- config.secrets.json (server-only secrets, not committed)

Features:
- SSH command execution for bot management
- Start/stop/restart bots via systemd using .sh scripts
- Sync bot files using sync_bot.sh
- Status logging to Discord channel
"""

import os
import sys
import json
import asyncio
import subprocess
import shlex
import importlib.util
import platform
import requests
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

from rsbots_manifest import compare_manifests as rs_compare_manifests
from rsbots_manifest import generate_manifest as rs_generate_manifest
from rsbots_manifest import DEFAULT_EXCLUDE_GLOBS as RS_DEFAULT_EXCLUDE_GLOBS

import discord
from discord.ext import commands
from discord import ui

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    WHITE = '\033[97m'
    RESET = '\033[0m'


# Standardized Discord message helper
class MessageHelper:
    """Helper class for creating consistent Discord messages across all commands."""
    
    @staticmethod
    def create_status_embed(title: str, description: str = "", color: discord.Color = discord.Color.blue(), 
                           fields: List[Dict] = None, footer: str = None) -> discord.Embed:
        """Create a standardized status embed.
        
        Args:
            title: Embed title
            description: Embed description
            color: Embed color (default: blue)
            fields: List of field dicts with 'name', 'value', 'inline' keys
            footer: Footer text
            
        Returns:
            discord.Embed
        """
        embed = discord.Embed(
            title=title,
            description=description,
            color=color,
            timestamp=datetime.now()
        )
        
        if fields:
            for field in fields:
                embed.add_field(
                    name=field.get('name', ''),
                    value=field.get('value', ''),
                    inline=field.get('inline', False)
                )
        
        if footer:
            embed.set_footer(text=footer)
        
        return embed
    
    @staticmethod
    def create_success_embed(title: str, message: str, details: str = None) -> discord.Embed:
        """Create a success embed with consistent formatting."""
        embed = MessageHelper.create_status_embed(
            title=f"✅ {title}",
            description=message,
            color=discord.Color.green()
        )
        if details:
            embed.add_field(name="Details", value=f"```{details[:1000]}```", inline=False)
        return embed
    
    @staticmethod
    def create_error_embed(title: str, message: str, error_details: str = None) -> discord.Embed:
        """Create an error embed with consistent formatting."""
        embed = MessageHelper.create_status_embed(
            title=f"❌ {title}",
            description=message,
            color=discord.Color.red()
        )
        if error_details:
            embed.add_field(name="Error", value=f"```{error_details[:1000]}```", inline=False)
        return embed
    
    @staticmethod
    def create_warning_embed(title: str, message: str, details: str = None) -> discord.Embed:
        """Create a warning embed with consistent formatting."""
        embed = MessageHelper.create_status_embed(
            title=f"⚠️ {title}",
            description=message,
            color=discord.Color.orange()
        )
        if details:
            embed.add_field(name="Details", value=f"```{details[:1000]}```", inline=False)
        return embed
    
    @staticmethod
    def create_info_embed(title: str, message: str = "", fields: List[Dict] = None, *, description: str = None) -> discord.Embed:
        """Create an info embed with consistent formatting.
        
        Args:
            title: Embed title
            message: Main message/description (can be empty)
            fields: Optional fields list
            description: Alias for message parameter (for compatibility)
        """
        # Support both message and description for compatibility
        desc = description if description is not None else message
        return MessageHelper.create_status_embed(
            title=title,
            description=desc,
            color=discord.Color.blue(),
            fields=fields
        )

# RSAdminBot is self-contained - no external dependencies
# All functionality is within RSAdminBot folder

import importlib.util as _importlib_util

# Avoid import-time side effects. We only check module availability here; actual imports are lazy.
INSPECTOR_AVAILABLE = _importlib_util.find_spec("bot_inspector") is not None
TRACKER_AVAILABLE = (
    _importlib_util.find_spec("whop_tracker") is not None
    and _importlib_util.find_spec("bot_movement_tracker") is not None
    and _importlib_util.find_spec("test_server_organizer") is not None
)


class ServiceManager:
    """Centralized service management using .sh scripts.
    
    Canonical owner for all bot management operations.
    Uses .sh scripts as single source of truth.
    """
    
    def __init__(self, script_executor, bot_group_getter):
        """Initialize ServiceManager with script executor functions.
        
        Args:
            script_executor: Function to execute .sh scripts (script_name, action, bot_name, *args) -> (success, stdout, stderr)
            bot_group_getter: Function to get bot group (bot_name) -> group_name
        """
        self._execute_script = script_executor
        self._get_bot_group = bot_group_getter

        self._script_map = {
            "rsadminbot": "manage_rsadminbot.sh",
            "rs_bots": "manage_rs_bots.sh",
            "mirror_bots": "manage_mirror_bots.sh",
        }

    def _script_for_bot(self, bot_name: str) -> Tuple[Optional[str], Optional[str]]:
        """Resolve the canonical management script for a bot name."""
        if not bot_name:
            return None, "bot_name is required"
        bot_group = self._get_bot_group(bot_name)
        if not bot_group:
            return None, f"Unknown bot group for {bot_name}"
        return self._script_map.get(bot_group, "manage_bots.sh"), None
    
    def get_status(self, service_name: str, bot_name: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Get service status using .sh script.
        
        Args:
            service_name: Systemd service name (unused, kept for compatibility)
            bot_name: Bot name (e.g., "rsforwarder") - REQUIRED
        
        Returns:
            (exists, state, error_msg)
            - exists: True if service exists
            - state: 'active', 'inactive', 'failed', 'not_found', or None if error
            - error_msg: Error message if status check failed
        """
        script_name, err = self._script_for_bot(bot_name)
        if not script_name:
            return False, None, err
        success, stdout, stderr = self._execute_script(script_name, "status", bot_name)
        
        if success:
            state = (stdout or "").strip().lower()
            if state == "not_found":
                return False, None, None
            return True, state, None
        else:
            return True, None, stderr or "Status check failed"
    
    
    def get_detailed_status(self, service_name: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Get detailed service status output.
        
        Returns:
            (success, output, error_msg)
        """
        # Use canonical .sh scripts (single source of truth) instead of direct SSH/systemctl here.
        bot_name = None
        if service_name:
            # Attempt to infer bot name from service name for compatibility
            svc = service_name
            if svc.endswith(".service"):
                svc = svc[:-8]
            if svc.startswith("mirror-world-"):
                bot_name = svc[len("mirror-world-"):]
        if not bot_name:
            return False, None, "Could not infer bot_name from service name"
        return self._execute_script("botctl.sh", "details", bot_name)
    
    def get_pid(self, service_name: str) -> Optional[int]:
        """Get service PID if running.
        
        Returns:
            PID as int, or None if not running or error
        """
        bot_name = None
        if service_name:
            svc = service_name
            if svc.endswith(".service"):
                svc = svc[:-8]
            if svc.startswith("mirror-world-"):
                bot_name = svc[len("mirror-world-"):]
        if not bot_name:
            return None
        success, stdout, _ = self._execute_script("botctl.sh", "pid", bot_name)
        if not success:
            return None
        pid_str = (stdout or "").strip()
        if pid_str.isdigit():
            try:
                return int(pid_str)
            except ValueError:
                return None
        return None
    
    def start(self, service_name: str, unmask: bool = True, bot_name: str = None) -> Tuple[bool, Optional[str], Optional[str]]:
        """Start a service using .sh script.
        
        Args:
            service_name: Systemd service name (unused, kept for compatibility)
            unmask: Ignored (script handles unmask/enable)
            bot_name: Bot name (e.g., "rsforwarder") - REQUIRED
        
        Returns:
            (success, stdout, stderr)
        """
        script_name, err = self._script_for_bot(bot_name or "")
        if not script_name:
            return False, None, err
        return self._execute_script(script_name, "start", bot_name)
    
    def stop(self, service_name: str, script_pattern: Optional[str] = None, bot_name: str = None) -> Tuple[bool, Optional[str], Optional[str]]:
        """Stop a service using .sh script.
        
        Args:
            service_name: Systemd service name (unused, kept for compatibility)
            script_pattern: Ignored (script handles this)
            bot_name: Bot name (e.g., "rsforwarder") - REQUIRED
        
        Returns:
            (success, stdout, stderr)
        """
        script_name, err = self._script_for_bot(bot_name or "")
        if not script_name:
            return False, None, err
        return self._execute_script(script_name, "stop", bot_name)
    
    def restart(self, service_name: str, script_pattern: Optional[str] = None, bot_name: str = None) -> Tuple[bool, Optional[str], Optional[str]]:
        """Restart a service using .sh script.
        
        Args:
            service_name: Systemd service name (unused, kept for compatibility)
            script_pattern: Ignored (script handles this)
            bot_name: Bot name (e.g., "rsforwarder") - REQUIRED
        
        Returns:
            (success, stdout, stderr)
        """
        script_name, err = self._script_for_bot(bot_name or "")
        if not script_name:
            return False, None, err
        return self._execute_script(script_name, "restart", bot_name)
    
    def get_failure_logs(self, service_name: str, lines: int = 50) -> Optional[str]:
        """Get recent journalctl logs for service failures.
        
        Args:
            service_name: Systemd service name
            lines: Number of log lines to retrieve
        
        Returns:
            Log output or None if error
        """
        bot_name = None
        if service_name:
            svc = service_name
            if svc.endswith(".service"):
                svc = svc[:-8]
            if svc.startswith("mirror-world-"):
                bot_name = svc[len("mirror-world-"):]
        if not bot_name:
            return None
        success, stdout, _ = self._execute_script("botctl.sh", "logs", bot_name, str(lines))
        if success and stdout:
            return stdout
        return None
    
    def verify_started(self, service_name: str, max_wait: int = 10, bot_name: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Verify service started successfully with retry logic.
        
        Args:
            service_name: Systemd service name
            max_wait: Maximum seconds to wait
            bot_name: Bot name (e.g., "rsforwarder") - if provided, uses .sh script
        
        Returns:
            (is_running, error_msg)
        """
        import time
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            exists, state, error = self.get_status(service_name, bot_name=bot_name)
            
            if not exists:
                return False, "Service does not exist"
            
            if state == "active":
                return True, None
            
            if state == "failed":
                logs = self.get_failure_logs(service_name, lines=20)
                error_msg = "Service failed to start"
                if logs:
                    error_msg += f"\n\nRecent logs:\n{logs[-500:]}"
                return False, error_msg
            
            # Wait before retry
            time.sleep(1)
        
        # Timeout
        exists, state, error = self.get_status(service_name, bot_name=bot_name)
        if exists:
            logs = self.get_failure_logs(service_name, lines=20)
            error_msg = f"Service did not become active (state: {state})"
            if logs:
                error_msg += f"\n\nRecent logs:\n{logs[-500:]}"
            return False, error_msg
        
        return False, "Service does not exist"


class ChannelTransferView(ui.View):
    """View with SelectMenus for channel and category selection"""
    
    def __init__(self, admin_bot_instance, ctx):
        super().__init__(timeout=300)
        self.admin_bot = admin_bot_instance
        self.ctx = ctx
        self.selected_channel = None
        self.selected_category = None
        
        # Channel select
        channels = [ch for ch in ctx.guild.channels if isinstance(ch, discord.TextChannel)]
        channel_options = [
            ui.SelectOption(label=ch.name, value=str(ch.id), description=f"#{ch.name}")
            for ch in sorted(channels, key=lambda x: x.position)[:25]
        ]
        if channel_options:
            self.channel_select = ui.Select(
                placeholder="Select a channel...",
                options=channel_options,
                min_values=1,
                max_values=1
            )
            self.channel_select.callback = self.on_channel_select
            self.add_item(self.channel_select)
        
        # Category select
        categories = [ch for ch in ctx.guild.channels if isinstance(ch, discord.CategoryChannel)]
        category_options = [
            ui.SelectOption(label=cat.name, value=str(cat.id), description=f"Category: {cat.name}")
            for cat in sorted(categories, key=lambda x: x.position)[:25]
        ]
        if category_options:
            self.category_select = ui.Select(
                placeholder="Select a category...",
                options=category_options,
                min_values=1,
                max_values=1
            )
            self.category_select.callback = self.on_category_select
            self.add_item(self.category_select)
    
    async def on_channel_select(self, interaction: discord.Interaction):
        channel_id = int(self.channel_select.values[0])
        self.selected_channel = interaction.guild.get_channel(channel_id)
        await interaction.response.send_message(f"✅ Channel selected: `{self.selected_channel.name}`. Now select a category.", ephemeral=True)
    
    async def on_category_select(self, interaction: discord.Interaction):
        category_id = int(self.category_select.values[0])
        self.selected_category = interaction.guild.get_channel(category_id)
        
        if not self.selected_channel:
            # Try to get from channel select if it was selected
            if hasattr(self, 'channel_select') and self.channel_select.values:
                channel_id = int(self.channel_select.values[0])
                self.selected_channel = interaction.guild.get_channel(channel_id)
        
        if not self.selected_channel:
            await interaction.response.send_message("❌ Please select a channel first", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=False)
        
        try:
            await self.selected_channel.edit(category=self.selected_category)
            await interaction.followup.send(
                f"✅ **Channel Transferred**\n`{self.selected_channel.name}` → `{self.selected_category.name}`"
            )
        except discord.Forbidden:
            await interaction.followup.send("❌ I don't have permission to edit this channel")
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to transfer channel: {str(e)[:200]}")

class BotSelectView(ui.View):
    """View with SelectMenu for bot selection"""
    
    def __init__(self, admin_bot_instance, action: str, action_display: str, action_kwargs: Optional[Dict[str, Any]] = None):
        """
        Args:
            admin_bot_instance: RSAdminBot instance
            action: Action name ('start', 'stop', 'restart', 'status', 'update', 'details', 'logs', 'info', 'config', 'movements', 'diagnose')
            action_display: Display name for action ('Start', 'Stop', etc.)
            action_kwargs: Optional extra params for handlers (e.g. logs lines)
        """
        super().__init__(timeout=300)  # 5 minute timeout
        self.admin_bot = admin_bot_instance
        self.action = action
        self.action_display = action_display
        self.action_kwargs = action_kwargs or {}
        
        # Create SelectMenu with all bots + "All Bots" option for start/stop/restart actions
        options = [
            discord.SelectOption(
                label=bot_info['name'],
                value=bot_key,
                description=f"{action_display} {bot_info['name']}"
            )
            for bot_key, bot_info in admin_bot_instance.BOTS.items()
        ]
        
        # Add "All Bots" option for service control actions
        if self.action in ["start", "stop", "restart"]:
            options.insert(0, discord.SelectOption(
                label="🔄 All Bots",
                value="all_bots",
                description=f"{action_display} all bots"
            ))
        
        select = ui.Select(
            placeholder=f"Select bot to {action_display.lower()}...",
            options=options
        )
        select.callback = self.on_select
        self.add_item(select)
    
    async def on_select(self, interaction: discord.Interaction):
        """Handle bot selection"""
        if not self.admin_bot.is_admin(interaction.user):
            await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
            return
        
        bot_name = interaction.data['values'][0]
        
        # Defer to prevent timeout
        await interaction.response.defer(ephemeral=False)
        
        # Route to appropriate command handler (supports single bot or "all_bots")
        if self.action == "start":
            await self._handle_start(interaction, bot_name)
        elif self.action == "stop":
            await self._handle_stop(interaction, bot_name)
        elif self.action == "restart":
            await self._handle_restart(interaction, bot_name)
        elif self.action == "status":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_status(interaction, bot_name, bot_info)
        elif self.action == "update":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_update(interaction, bot_name, bot_info)
        elif self.action == "details":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_details(interaction, bot_name, bot_info)
        elif self.action == "logs":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_logs(interaction, bot_name, bot_info)
        elif self.action == "info":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_info(interaction, bot_name, bot_info)
        elif self.action == "config":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_config(interaction, bot_name, bot_info)
        elif self.action == "movements":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_movements(interaction, bot_name, bot_info)
        elif self.action == "diagnose":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_diagnose(interaction, bot_name, bot_info)
    
    async def _handle_start(self, interaction, bot_name):
        """Handle bot start (supports single bot or 'all_bots')"""
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available")
            return
        
        # Handle "all_bots" case - use group-specific scripts for efficiency
        if bot_name == "all_bots":
            status_msg = await interaction.followup.send(f"🔄 **Starting all bots using group-specific scripts...**\n```\nCalling manage_rsadminbot.sh, manage_rs_bots.sh, and manage_mirror_bots.sh...\n```")
            
            results = []
            
            # Start RSAdminBot
            try:
                success_rsadmin, stdout_rsadmin, stderr_rsadmin = self.admin_bot._execute_sh_script("manage_rsadminbot.sh", "start", "rsadminbot")
                if success_rsadmin:
                    results.append("✅ **RSAdminBot**: Started successfully")
                else:
                    error_msg = stderr_rsadmin or stdout_rsadmin or "Unknown error"
                    results.append(f"❌ **RSAdminBot**: {error_msg[:100]}")
            except Exception as e:
                results.append(f"❌ **RSAdminBot**: {str(e)[:100]}")
            
            # Start all RS bots
            try:
                success_rs, stdout_rs, stderr_rs = self.admin_bot._execute_sh_script("manage_rs_bots.sh", "start", "all")
                if success_rs:
                    results.append("✅ **RS Bots** (rsforwarder, rsonboarding, rsmentionpinger, rscheckerbot, rssuccessbot): Started successfully")
                else:
                    error_msg = stderr_rs or stdout_rs or "Unknown error"
                    results.append(f"⚠️ **RS Bots**: {error_msg[:150]}")
            except Exception as e:
                results.append(f"❌ **RS Bots**: {str(e)[:100]}")
            
            # Start all mirror-world bots
            try:
                success_mirror, stdout_mirror, stderr_mirror = self.admin_bot._execute_sh_script("manage_mirror_bots.sh", "start", "all")
                if success_mirror:
                    results.append("✅ **Mirror-World Bots** (datamanagerbot, pingbot, discumbot): Started successfully")
                else:
                    error_msg = stderr_mirror or stdout_mirror or "Unknown error"
                    results.append(f"⚠️ **Mirror-World Bots**: {error_msg[:150]}")
            except Exception as e:
                results.append(f"❌ **Mirror-World Bots**: {str(e)[:100]}")
            
            summary = f"🔄 **Start All Complete**\n\n" + "\n".join(results)
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            await status_msg.edit(content=summary)
            await self.admin_bot._log_to_discord(f"🔄 **All Bots Start** completed")
            return
        
        # Handle single bot case
        bot_info = self.admin_bot.BOTS[bot_name]
        service_name = bot_info["service"]
        await interaction.followup.send(f"🔄 **Starting {bot_info['name']}...**\n```\nConnecting to server...\n```")
        before_exists, before_state, _ = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
        before_pid = self.admin_bot.service_manager.get_pid(service_name)

        success, stdout, stderr = self.admin_bot.service_manager.start(service_name, unmask=True, bot_name=bot_name)
        
        if success:
            is_running, verify_error = self.admin_bot.service_manager.verify_started(service_name, bot_name=bot_name)
            if is_running:
                after_exists, after_state, _ = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
                after_pid = self.admin_bot.service_manager.get_pid(service_name)
                pid_note = ""
                if before_pid and after_pid and before_pid != after_pid:
                    pid_note = f" (pid {before_pid} -> {after_pid})"
                elif before_pid is None and after_pid:
                    pid_note = f" (pid -> {after_pid})"
                before_state_txt = before_state or "unknown"
                after_state_txt = after_state or "unknown"
                before_pid_txt = str(before_pid or 0)
                after_pid_txt = str(after_pid or 0)
                await interaction.followup.send(
                    f"✅ **{bot_info['name']}** started successfully!{pid_note}\n"
                    f"```\nBefore: state={before_state_txt} pid={before_pid_txt}\nAfter:  state={after_state_txt} pid={after_pid_txt}\n```"
                )
                await self.admin_bot._log_to_discord(
                    f"✅ **{bot_info['name']}** started\nState: `{after_state or 'unknown'}` | PID: `{after_pid or 0}`\nBefore: `{before_state or 'unknown'}` | PID: `{before_pid or 0}`"
                )
            else:
                error_msg = verify_error or stderr or stdout or "Unknown error"
                await interaction.followup.send(f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```")
        else:
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```")
    
    async def _handle_stop(self, interaction, bot_name):
        """Handle bot stop (supports single bot or 'all_bots')"""
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available")
            return
        
        # Handle "all_bots" case - use group-specific scripts for efficiency
        if bot_name == "all_bots":
            status_msg = await interaction.followup.send(f"🔄 **Stopping all bots using group-specific scripts...**\n```\nCalling manage_rsadminbot.sh, manage_rs_bots.sh, and manage_mirror_bots.sh...\n```")
            
            results = []
            
            # Stop RSAdminBot
            try:
                success_rsadmin, stdout_rsadmin, stderr_rsadmin = self.admin_bot._execute_sh_script("manage_rsadminbot.sh", "stop", "rsadminbot")
                if success_rsadmin:
                    results.append("✅ **RSAdminBot**: Stopped successfully")
                else:
                    error_msg = stderr_rsadmin or stdout_rsadmin or "Unknown error"
                    results.append(f"❌ **RSAdminBot**: {error_msg[:100]}")
            except Exception as e:
                results.append(f"❌ **RSAdminBot**: {str(e)[:100]}")
            
            # Stop all RS bots
            try:
                success_rs, stdout_rs, stderr_rs = self.admin_bot._execute_sh_script("manage_rs_bots.sh", "stop", "all")
                if success_rs:
                    results.append("✅ **RS Bots** (rsforwarder, rsonboarding, rsmentionpinger, rscheckerbot, rssuccessbot): Stopped successfully")
                else:
                    error_msg = stderr_rs or stdout_rs or "Unknown error"
                    results.append(f"⚠️ **RS Bots**: {error_msg[:150]}")
            except Exception as e:
                results.append(f"❌ **RS Bots**: {str(e)[:100]}")
            
            # Stop all mirror-world bots
            try:
                success_mirror, stdout_mirror, stderr_mirror = self.admin_bot._execute_sh_script("manage_mirror_bots.sh", "stop", "all")
                if success_mirror:
                    results.append("✅ **Mirror-World Bots** (datamanagerbot, pingbot, discumbot): Stopped successfully")
                else:
                    error_msg = stderr_mirror or stdout_mirror or "Unknown error"
                    results.append(f"⚠️ **Mirror-World Bots**: {error_msg[:150]}")
            except Exception as e:
                results.append(f"❌ **Mirror-World Bots**: {str(e)[:100]}")
            
            summary = f"🔄 **Stop All Complete**\n\n" + "\n".join(results)
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            await status_msg.edit(content=summary)
            await self.admin_bot._log_to_discord(f"🔄 **All Bots Stop** completed")
            return
        
        # Handle single bot case
        bot_info = self.admin_bot.BOTS[bot_name]
        service_name = bot_info["service"]
        script_pattern = bot_info.get("script", bot_name)
        await interaction.followup.send(f"🔄 **Stopping {bot_info['name']}...**\n```\nConnecting to server...\n```")
        before_exists, before_state, _ = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
        before_pid = self.admin_bot.service_manager.get_pid(service_name)

        success, stdout, stderr = self.admin_bot.service_manager.stop(service_name, script_pattern=script_pattern, bot_name=bot_name)
        
        if success:
            after_exists, after_state, _ = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
            after_pid = self.admin_bot.service_manager.get_pid(service_name)
            pid_note = ""
            if before_pid and not after_pid:
                pid_note = f" (pid {before_pid} -> 0)"
            before_state_txt = before_state or "unknown"
            after_state_txt = after_state or "unknown"
            before_pid_txt = str(before_pid or 0)
            after_pid_txt = str(after_pid or 0)
            await interaction.followup.send(
                f"✅ **{bot_info['name']}** stopped successfully!{pid_note}\n"
                f"```\nBefore: state={before_state_txt} pid={before_pid_txt}\nAfter:  state={after_state_txt} pid={after_pid_txt}\n```"
            )
            await self.admin_bot._log_to_discord(
                f"✅ **{bot_info['name']}** stopped\nState: `{after_state or 'unknown'}` | PID: `{after_pid or 0}`\nBefore: `{before_state or 'unknown'}` | PID: `{before_pid or 0}`"
            )
        else:
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(f"❌ Failed to stop {bot_info['name']}:\n```{error_msg[:500]}```")
    
    async def _handle_restart(self, interaction, bot_name):
        """Handle bot restart (supports single bot or 'all_bots')"""
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available")
            return
        
        # Handle "all_bots" case - use group-specific scripts for efficiency
        if bot_name == "all_bots":
            status_msg = await interaction.followup.send(f"🔄 **Restarting all bots using group-specific scripts...**\n```\nCalling manage_rsadminbot.sh, manage_rs_bots.sh, and manage_mirror_bots.sh...\n```")
            
            results = []
            
            # Restart RSAdminBot
            try:
                success_rsadmin, stdout_rsadmin, stderr_rsadmin = self.admin_bot._execute_sh_script("manage_rsadminbot.sh", "restart", "rsadminbot")
                if success_rsadmin:
                    results.append("✅ **RSAdminBot**: Restarted successfully")
                else:
                    error_msg = stderr_rsadmin or stdout_rsadmin or "Unknown error"
                    results.append(f"❌ **RSAdminBot**: {error_msg[:100]}")
            except Exception as e:
                results.append(f"❌ **RSAdminBot**: {str(e)[:100]}")
            
            # Restart all RS bots
            try:
                success_rs, stdout_rs, stderr_rs = self.admin_bot._execute_sh_script("manage_rs_bots.sh", "restart", "all")
                if success_rs:
                    results.append("✅ **RS Bots** (rsforwarder, rsonboarding, rsmentionpinger, rscheckerbot, rssuccessbot): Restarted successfully")
                else:
                    error_msg = stderr_rs or stdout_rs or "Unknown error"
                    results.append(f"⚠️ **RS Bots**: {error_msg[:150]}")
            except Exception as e:
                results.append(f"❌ **RS Bots**: {str(e)[:100]}")
            
            # Restart all mirror-world bots
            try:
                success_mirror, stdout_mirror, stderr_mirror = self.admin_bot._execute_sh_script("manage_mirror_bots.sh", "restart", "all")
                if success_mirror:
                    results.append("✅ **Mirror-World Bots** (datamanagerbot, pingbot, discumbot): Restarted successfully")
                else:
                    error_msg = stderr_mirror or stdout_mirror or "Unknown error"
                    results.append(f"⚠️ **Mirror-World Bots**: {error_msg[:150]}")
            except Exception as e:
                results.append(f"❌ **Mirror-World Bots**: {str(e)[:100]}")
            
            summary = f"🔄 **Restart All Complete**\n\n" + "\n".join(results)
            if len(summary) > 2000:
                summary = summary[:1997] + "..."
            await status_msg.edit(content=summary)
            await self.admin_bot._log_to_discord(f"🔄 **All Bots Restart** completed")
            return
        
        # Handle single bot case
        bot_info = self.admin_bot.BOTS[bot_name]
        service_name = bot_info["service"]
        script_pattern = bot_info.get("script", bot_name)
        await interaction.followup.send(f"🔄 **Restarting {bot_info['name']}...**\n```\nConnecting to server...\n```")
        before_exists, before_state, _ = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
        before_pid = self.admin_bot.service_manager.get_pid(service_name)

        success, stdout, stderr = self.admin_bot.service_manager.restart(service_name, script_pattern=script_pattern, bot_name=bot_name)
        
        if success:
            is_running, verify_error = self.admin_bot.service_manager.verify_started(service_name, bot_name=bot_name)
            if is_running:
                after_exists, after_state, _ = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
                after_pid = self.admin_bot.service_manager.get_pid(service_name)
                pid_note = ""
                if before_pid and after_pid and before_pid != after_pid:
                    pid_note = f" (pid {before_pid} -> {after_pid})"
                elif before_pid and after_pid and before_pid == after_pid:
                    pid_note = f" (pid unchanged: {after_pid})"
                elif before_pid is None and after_pid:
                    pid_note = f" (pid -> {after_pid})"
                before_state_txt = before_state or "unknown"
                after_state_txt = after_state or "unknown"
                before_pid_txt = str(before_pid or 0)
                after_pid_txt = str(after_pid or 0)
                await interaction.followup.send(
                    f"✅ **{bot_info['name']}** restarted successfully!{pid_note}\n"
                    f"```\nBefore: state={before_state_txt} pid={before_pid_txt}\nAfter:  state={after_state_txt} pid={after_pid_txt}\n```"
                )
                await self.admin_bot._log_to_discord(
                    f"✅ **{bot_info['name']}** restarted{pid_note}\nState: `{after_state or 'unknown'}` | PID: `{after_pid or 0}`\nBefore: `{before_state or 'unknown'}` | PID: `{before_pid or 0}`"
                )
            else:
                error_msg = verify_error or stderr or stdout or "Unknown error"
                await interaction.followup.send(f"❌ Failed to restart {bot_info['name']}:\n```{error_msg[:500]}```")
        else:
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(f"❌ Failed to restart {bot_info['name']}:\n```{error_msg[:500]}```")
    
    async def _handle_status(self, interaction, bot_name, bot_info):
        """Handle bot status check"""
        service_name = bot_info["service"]
        check_exists_cmd = f"systemctl list-unit-files {service_name} 2>/dev/null | grep -q {service_name} && echo 'exists' || echo 'not_found'"
        exists_success, exists_output, _ = self.admin_bot._execute_ssh_command(check_exists_cmd, timeout=10)
        service_exists = exists_success and "exists" in (exists_output or "").lower()
        
        embed = discord.Embed(
            title=f"📊 {bot_info['name']} Status",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )
        
        if not service_exists:
            embed.add_field(name="Status", value="⚠️ Service not found", inline=False)
        else:
            exists, state, error = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
            if exists and state:
                is_active = state == "active"
                status_icon = "✅" if is_active else "❌"
                embed.add_field(name="Status", value=f"{status_icon} {'Running' if is_active else 'Stopped'}", inline=True)
                if is_active:
                    pid = self.admin_bot.service_manager.get_pid(service_name)
                    if pid:
                        embed.add_field(name="PID", value=str(pid), inline=True)
            else:
                embed.add_field(name="Error", value=f"```{error or 'Status check failed'}```", inline=False)
        
        await interaction.followup.send(embed=embed)
    
    async def _handle_update(self, interaction, bot_name, bot_info):
        """Handle bot update (GitHub python-only) from the dropdown."""
        # This matches the canonical update model: pull rsbots-code and overwrite live *.py.
        ssh_ok, error_msg = self.admin_bot._check_ssh_available()
        if not ssh_ok:
            await interaction.followup.send(f"❌ SSH not configured: {error_msg}")
            return

        bot_key = (bot_name or "").strip().lower()
        if bot_key == "rsadminbot":
            await interaction.followup.send("ℹ️ Use `!selfupdate` to update RSAdminBot.")
            return

        bot_folder = str(bot_info.get("folder") or "")
        service_name = str(bot_info.get("service") or "")
        await interaction.followup.send(
            f"📦 **Updating {bot_info['name']} from GitHub (python-only)...**\n"
            "```\nPulling + copying *.py from /home/rsadmin/bots/rsbots-code\n```"
        )

        success, stats = self.admin_bot._github_py_only_update(bot_folder)
        if not success:
            await interaction.followup.send(f"❌ Update failed:\n```{stats.get('error','unknown error')[:900]}```")
            return

        old = (stats.get("old") or "").strip()
        new = (stats.get("new") or "").strip()
        py_count = str(stats.get("py_count") or "0").strip()
        changed_count = str(stats.get("changed_count") or "0").strip()
        changed_sample = stats.get("changed_sample") or []

        restart_ok = False
        restart_err = ""
        if self.admin_bot.service_manager and service_name:
            ok_r, out_r, err_r = self.admin_bot.service_manager.restart(service_name, bot_name=bot_key)
            if not ok_r:
                restart_err = (err_r or out_r or "restart failed")[:800]
            else:
                running, verify_err = self.admin_bot.service_manager.verify_started(service_name, bot_name=bot_key)
                restart_ok = bool(running)
                if not restart_ok:
                    restart_err = (verify_err or "service did not become active")[:800]
        else:
            restart_err = "ServiceManager not available or missing service mapping"

        summary = f"✅ **{bot_info['name']} updated from GitHub (python-only)**\n```"
        summary += f"\nGit: {old[:12]} -> {new[:12]}"
        summary += f"\nPython copied: {py_count} | Changed: {changed_count}"
        summary += f"\nRestart: {'OK' if restart_ok else 'FAILED'}"
        summary += "\n```"
        if changed_sample:
            summary += "\nChanged sample:\n```" + "\n".join(str(x) for x in changed_sample[:20]) + "```"
        if not restart_ok and restart_err:
            summary += "\nRestart error:\n```" + restart_err[:900] + "```"
        await interaction.followup.send(summary[:1900])
    
    async def _handle_info(self, interaction, bot_name, bot_info):
        """Handle bot info"""
        await interaction.followup.send("ℹ️ Use `!botinfo <botname>` for detailed information.")
    
    async def _handle_config(self, interaction, bot_name, bot_info):
        """Handle bot config"""
        await interaction.followup.send("ℹ️ Use `!botconfig <botname>` to view config.")
    
    async def _handle_movements(self, interaction, bot_name, bot_info):
        """Handle bot movements"""
        await interaction.followup.send("ℹ️ Use `!botmovements <botname>` to view activity logs.")
    
    async def _handle_diagnose(self, interaction, bot_name, bot_info):
        """Handle bot diagnose"""
        # Use the same logic as botdiagnose command
        service_name = bot_info["service"]
        embed = discord.Embed(
            title=f"🔍 {bot_info['name']} Diagnostics",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )
        
        if self.admin_bot.service_manager:
            exists, state, error = self.admin_bot.service_manager.get_status(service_name, bot_name=bot_name)
            if exists:
                status_icon = "✅" if state == "active" else "❌"
                embed.add_field(name="Service Status", value=f"{status_icon} {state.capitalize()}", inline=True)
                
                if state != "active":
                    logs = self.admin_bot.service_manager.get_failure_logs(service_name, lines=30)
                    if logs:
                        error_lines = [line for line in logs.split('\n') if any(kw in line.lower() for kw in ['error', 'failed', 'exception'])]
                        if error_lines:
                            error_text = "\n".join(error_lines[-15:])
                            if len(error_text) > 1000:
                                error_text = error_text[:1000] + "..."
                            embed.add_field(name="Recent Errors", value=f"```\n{error_text}\n```", inline=False)
            else:
                embed.add_field(name="Service Status", value="⚠️ Service not found", inline=False)
        
        await interaction.followup.send(embed=embed)

    async def _handle_details(self, interaction, bot_name, bot_info):
        """Show systemd details via botctl.sh (dropdown action)."""
        if not self.admin_bot.is_admin(interaction.user):
            await interaction.followup.send("❌ You don't have permission to use this command.")
            return
        svc = str(bot_info.get("service") or "")
        await interaction.followup.send(f"🧾 **Details: {bot_info.get('name', bot_name)}**\nService: `{svc}`")
        success, out, err = self.admin_bot._execute_sh_script("botctl.sh", "details", bot_name)
        await interaction.followup.send(self.admin_bot._codeblock(out or err or ""))

    async def _handle_logs(self, interaction, bot_name, bot_info):
        """Show journalctl logs via botctl.sh (dropdown action)."""
        if not self.admin_bot.is_admin(interaction.user):
            await interaction.followup.send("❌ You don't have permission to use this command.")
            return
        svc = str(bot_info.get("service") or "")
        lines = int(self.action_kwargs.get("lines") or 80)
        lines = max(10, min(lines, 400))
        await interaction.followup.send(f"📜 **Logs: {bot_info.get('name', bot_name)}**\nService: `{svc}`\nLines: `{lines}`")
        success, out, err = self.admin_bot._execute_sh_script("botctl.sh", "logs", bot_name, str(lines))
        await interaction.followup.send(self.admin_bot._codeblock(out or err or ""))


class StartBotView(ui.View):
    """View with button to start a stopped bot"""
    
    def __init__(self, admin_bot_instance, bot_name: str, bot_display_name: str):
        super().__init__(timeout=300)  # 5 minute timeout
        self.admin_bot = admin_bot_instance
        self.bot_name = bot_name
        self.bot_display_name = bot_display_name
    
    @ui.button(label="🟢 Start Bot", style=discord.ButtonStyle.success)
    async def start_bot(self, interaction: discord.Interaction, button: ui.Button):
        """Start the bot when button is clicked"""
        # Check if user is admin
        if not self.admin_bot.is_admin(interaction.user):
            await interaction.response.send_message("❌ You don't have permission to start bots.", ephemeral=True)
            return
        
        # Disable button to prevent multiple clicks
        button.disabled = True
        button.label = "⏳ Starting..."
        await interaction.response.edit_message(view=self)
        
        # Start the bot
        bot_info = self.admin_bot.BOTS[self.bot_name]
        service_name = bot_info["service"]
        
        # Log to Discord
        await self.admin_bot._log_to_discord(f"🟢 **Starting {bot_info['name']}**\nService: `{service_name}`")
        
        # Start service using ServiceManager
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ SSH not available", ephemeral=False)
            return
        
        success, stdout, stderr = self.admin_bot.service_manager.start(service_name, unmask=True, bot_name=self.bot_name)
        
        if success:
            # Verify service actually started
            is_running, verify_error = self.admin_bot.service_manager.verify_started(service_name, bot_name=self.bot_name)
            if is_running:
                button.label = "✅ Started"
                button.style = discord.ButtonStyle.success
                await interaction.followup.send(f"✅ **{bot_info['name']}** started successfully!", ephemeral=False)
                await self.admin_bot._log_to_discord(f"✅ **{bot_info['name']}** started successfully!")
            else:
                button.label = "❌ Failed"
                button.style = discord.ButtonStyle.danger
                error_msg = verify_error or stderr or stdout or "Unknown error"
                await interaction.followup.send(f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```", ephemeral=False)
                await self.admin_bot._log_to_discord(f"❌ **{bot_info['name']}** failed to start:\n```{error_msg[:500]}```")
        else:
            button.label = "❌ Failed"
            button.style = discord.ButtonStyle.danger
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```", ephemeral=False)
            await self.admin_bot._log_to_discord(f"❌ **{bot_info['name']}** failed to start:\n```{error_msg[:500]}```")
        
        # Update the message
        await interaction.edit_original_response(view=self)


class RSAdminBot:
    """Main admin bot class"""
    
    # Bot definitions - Matched with BOT_SSH_COMMANDS_COMPLETE.md
    BOTS = {
        "datamanagerbot": {
            "name": "DataManager Bot",
            "service": "mirror-world-datamanagerbot.service",
            "folder": "neonxt/bots",
            "script": "datamanagerbot.py"  # For pkill command
        },
        "discumbot": {
            "name": "Discum Bot",
            "service": "mirror-world-discumbot.service",
            "folder": "neonxt/bots",
            "script": "discumbot.py"  # For pkill command
        },
        "pingbot": {
            "name": "Ping Bot",
            "service": "mirror-world-pingbot.service",
            "folder": "neonxt/bots",
            "script": "pingbot.py"  # For pkill command
        },
        "rsforwarder": {
            "name": "RS Forwarder",
            "service": "mirror-world-rsforwarder.service",
            "folder": "RSForwarder",
            "script": "rs_forwarder_bot.py"  # For pkill command
        },
        "rsonboarding": {
            "name": "RS Onboarding",
            "service": "mirror-world-rsonboarding.service",
            "folder": "RSOnboarding",
            "script": "rs_onboarding_bot.py"  # For pkill command
        },
        "rsmentionpinger": {
            "name": "RS Mention Pinger",
            "service": "mirror-world-rsmentionpinger.service",
            "folder": "RSMentionPinger",
            "script": "rs_mention_pinger.py"  # For pkill command
        },
        "rscheckerbot": {
            "name": "RS Checker Bot",
            "service": "mirror-world-rscheckerbot.service",
            "folder": "RSCheckerbot",
            "script": "main.py"  # For pkill command
        },
        "rssuccessbot": {
            "name": "RS Success Bot",
            "service": "mirror-world-rssuccessbot.service",  # Note: double 's' in service name
            "folder": "RSuccessBot",
            "script": "bot_runner.py"  # For pkill command - from reference doc
        },
        "rsadminbot": {
            "name": "RSAdminBot",
            "service": "mirror-world-rsadminbot.service",
            "folder": "RSAdminBot",
            "script": "admin_bot.py"  # For pkill command
        }
    }
    
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.config_path = self.base_path / "config.json"
        self.config: Dict[str, Any] = {}
        
        self.load_config()
        
        # Validate required config
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        # Load SSH server config (self-contained - only from config.json)
        self.servers: List[Dict[str, Any]] = []
        self.current_server: Optional[Dict[str, Any]] = None
        self._load_ssh_config()
        
        # Initialize ServiceManager (canonical owner for bot management operations)
        self.service_manager: Optional[ServiceManager] = None
        if self.current_server:
            # Pass script executor and bot group getter to ServiceManager
            self.service_manager = ServiceManager(
                self._execute_sh_script,
                self._get_bot_group
            )
        
        # Initialize bot inspector (pass BOTS dict as canonical source)
        # Use parent directory of RSAdminBot as project root (self-contained)
        self.inspector: Optional[Any] = None
        if INSPECTOR_AVAILABLE:
            try:
                from bot_inspector import BotInspector  # lazy import
                project_root = self.base_path.parent  # Parent of RSAdminBot folder
                self.inspector = BotInspector(project_root, bots_dict=self.BOTS)
                self.inspector.discover_bots()
                print(f"{Colors.GREEN}[Inspector] Discovered {len(self.inspector.bots)} bot(s){Colors.RESET}")
            except Exception as e:
                print(f"{Colors.YELLOW}[Inspector] Failed to initialize: {e}{Colors.RESET}")
        
        # Trackers will be initialized in on_ready (after bot is created)
        self.whop_tracker: Optional[Any] = None
        self.bot_movement_tracker: Optional[Any] = None
        self.test_server_organizer: Optional[Any] = None
        
        # Setup bot with required intents
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True  # For admin commands
        
        # Use prefix commands only (no slash commands for privacy)
        self.bot = commands.Bot(command_prefix='!', intents=intents)
        
        self._setup_events()
        self._setup_commands()
    
    def _load_ssh_config(self):
        """Load SSH server configuration from config.json (self-contained).
        
        RSAdminBot is self-contained - all config comes from its own config.json.
        When RSAdminBot is running on the Ubuntu host it manages, it should prefer local execution
        (no SSH key needed). When running off-box (e.g. Windows), it will use SSH + key.
        """
        try:
            # First, try loading from config.json (self-contained)
            ssh_server_config = self.config.get("ssh_server")
            if ssh_server_config:
                # Convert to list format for compatibility
                self.servers = [ssh_server_config]
                self.current_server = ssh_server_config
                
                # Canonical remote repo root (also used for local-exec detection on Ubuntu)
                remote_user = ssh_server_config.get("user", "rsadmin") or "rsadmin"
                self.remote_root = str(Path(f"/home/{remote_user}/bots/mirror-world"))
                
                # Resolve SSH key path. Canonical location is repo root `oraclekeys/`.
                # Fallback to RSAdminBot folder for legacy setups.
                key_name = ssh_server_config.get("key")
                if key_name:
                    key_path = Path(key_name)
                    if not key_path.is_absolute():
                        candidates = [
                            self.base_path.parent / "oraclekeys" / key_name,
                            self.base_path / key_name,
                        ]
                        for c in candidates:
                            if c.exists():
                                key_path = c
                                break
                    if key_path.is_absolute() and key_path.exists():
                        # Fix SSH key permissions on Windows (required for SSH to work)
                        if platform.system() == "Windows":
                            self._fix_ssh_key_permissions(key_path)
                        ssh_server_config["key"] = str(key_path)
                        print(f"{Colors.GREEN}[SSH] Using SSH key: {key_path}{Colors.RESET}")
                else:
                    # On Ubuntu local-exec mode, a missing SSH key is expected (we don't need it).
                    if self._should_use_local_exec():
                        print(f"{Colors.GREEN}[Local Exec] SSH key not found (ok in local-exec): {key_name}{Colors.RESET}")
                    else:
                        print(f"{Colors.YELLOW}[SSH Warning] SSH key not found: {key_name}{Colors.RESET}")
                
                print(f"{Colors.GREEN}[SSH] Loaded server config from config.json: {ssh_server_config.get('name', 'Unknown')}{Colors.RESET}")
                print(f"{Colors.CYAN}[SSH] Host: {ssh_server_config.get('host', 'N/A')}, User: {ssh_server_config.get('user', 'N/A')}{Colors.RESET}")
                if self._should_use_local_exec():
                    print(f"{Colors.GREEN}[Local Exec] Enabled: running management commands locally on this host{Colors.RESET}")
                
                return
            
            # No server config found - RSAdminBot is self-contained, only uses config.json
            print(f"{Colors.YELLOW}[SSH] No SSH server configured in config.json{Colors.RESET}")
            print(f"{Colors.YELLOW}[SSH] Add 'ssh_server' section to config.json to enable SSH functionality{Colors.RESET}")
            print(f"{Colors.YELLOW}[SSH] RSAdminBot is self-contained - all config must be in config.json{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.RED}[SSH] Failed to load SSH config: {e}{Colors.RESET}")
            import traceback
            print(f"{Colors.RED}[SSH] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")
    
    def _build_ssh_base(self, server_config: Dict[str, Any]) -> List[str]:
        """Build SSH base command list (self-contained, no external dependencies).
        
        Args:
            server_config: Server config dict with 'host', 'user', 'key', 'ssh_options'
        
        Returns:
            List of command parts for subprocess (shell=False)
        """
        cmd = ['ssh']
        
        # Add SSH options from config
        ssh_options = server_config.get('ssh_options', '')
        if ssh_options:
            # Parse options string into list
            options = shlex.split(ssh_options)
            cmd.extend(options)
        
        # Add key file (path should already be resolved in _load_ssh_config)
        if server_config.get('key'):
            key_path = str(server_config['key'])
            cmd.extend(['-i', key_path])
        
        # Add connection string
        user = server_config.get('user', 'ubuntu')
        host = server_config.get('host', '')
        if not host:
            return []
        
        port = server_config.get('port', 22)
        if port != 22:
            cmd.extend(['-p', str(port)])
        
        cmd.append(f"{user}@{host}")
        
        return cmd
    
    def _fix_ssh_key_permissions(self, key_path: Path):
        """Fix SSH key file permissions on Windows (required for SSH to work).
        
        SSH requires private keys to have restricted permissions (only readable by owner).
        On Windows, we need to remove permissions for BUILTIN\\Users group.
        
        Args:
            key_path: Path to SSH private key file
        """
        if platform.system() != "Windows":
            return  # Only needed on Windows
        
        try:
            import win32security
            import ntsecuritycon as con
            
            # Get current file security descriptor
            sd = win32security.GetFileSecurity(str(key_path), win32security.DACL_SECURITY_INFORMATION)
            dacl = sd.GetSecurityDescriptorDacl()
            
            # Remove BUILTIN\\Users group permissions
            users_sid = win32security.LookupAccountName("", "BUILTIN\\Users")[0]
            
            # Check if Users group has permissions
            has_users_perms = False
            for i in range(dacl.GetAceCount()):
                ace = dacl.GetAce(i)
                if ace[2] == users_sid:
                    has_users_perms = True
                    break
            
            if has_users_perms:
                # Create new DACL without Users group
                new_dacl = win32security.ACL()
                
                # Add owner full control
                owner_sid = win32security.LookupAccountName("", os.environ.get("USERNAME", ""))[0]
                new_dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, owner_sid)
                
                # Add SYSTEM full control
                system_sid = win32security.LookupAccountName("", "NT AUTHORITY\\SYSTEM")[0]
                new_dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, system_sid)
                
                # Set new DACL
                sd.SetSecurityDescriptorDacl(1, new_dacl, 0)
                win32security.SetFileSecurity(str(key_path), win32security.DACL_SECURITY_INFORMATION, sd)
                print(f"{Colors.GREEN}[SSH] Fixed SSH key permissions (removed BUILTIN\\Users access){Colors.RESET}")
        except ImportError:
            # pywin32 not available - try using icacls command instead
            try:
                # Remove Users group permissions using icacls
                result = subprocess.run(
                    ["icacls", str(key_path), "/remove", "BUILTIN\\Users"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    print(f"{Colors.GREEN}[SSH] Fixed SSH key permissions using icacls{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[SSH Warning] Could not fix key permissions automatically{Colors.RESET}")
                    print(f"{Colors.YELLOW}[SSH Warning] Run manually: icacls \"{key_path}\" /remove BUILTIN\\Users{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.YELLOW}[SSH Warning] Could not fix key permissions: {e}{Colors.RESET}")
                print(f"{Colors.YELLOW}[SSH Warning] Run manually: icacls \"{key_path}\" /remove BUILTIN\\Users{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.YELLOW}[SSH Warning] Could not fix key permissions: {e}{Colors.RESET}")
            print(f"{Colors.YELLOW}[SSH Warning] Run manually: icacls \"{key_path}\" /remove BUILTIN\\Users{Colors.RESET}")
    
    def _check_ssh_available(self) -> Tuple[bool, str]:
        """Check if SSH is available and configured. Returns (is_available, error_message)"""
        if not self.current_server:
            error_msg = "No SSH server configured in config.json"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.RED}[SSH Error] Add 'ssh_server' section to config.json{Colors.RESET}")
            return False, error_msg

        # If we're running on Linux and the repo root exists locally, prefer local execution when
        # the SSH key is not present. This keeps RSAdminBot functional on the Ubuntu host without
        # storing private keys on the server.
        if self._should_use_local_exec():
            return True, ""
        
        # Check SSH key (should already be resolved to absolute path in _load_ssh_config)
        ssh_key = self.current_server.get("key")
        if ssh_key:
            key_path = Path(ssh_key)
            if not key_path.exists():
                error_msg = f"SSH key file not found: {key_path}"
                print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
                print(f"{Colors.YELLOW}[SSH Error] Expected SSH key in RSAdminBot folder{Colors.RESET}")
                return False, error_msg
        
        return True, ""

    def _should_use_local_exec(self) -> bool:
        """Return True when we should execute management commands locally (no SSH).

        This is intended for the Ubuntu deployment where RSAdminBot runs on the same host it manages.
        We avoid relying on SSH keys on the server (security) and still keep all commands functional.
        """
        try:
            if os.name == "nt":
                return False
            if not (self.config.get("local_exec") or {}).get("enabled", True):
                return False
            # If the configured repo root exists locally on this machine, we can run locally.
            repo_root = Path(getattr(self, "remote_root", "") or "")
            if repo_root.is_dir():
                    return True
        except Exception:
            return False
        return False

    async def _ensure_botctl_symlink(self) -> None:
        """Best-effort: ensure /home/rsadmin/bots/botctl.sh exists as a symlink to the canonical botctl.sh.

        Why:
        - Some older docs/tools expect /home/rsadmin/bots/botctl.sh
        - Canonical location is inside the live tree: /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh

        Safety:
        - Only runs in Ubuntu local-exec mode
        - If the link path exists and is NOT a symlink, we do not modify it
        - Never fails startup; it only logs/report status
        """
        try:
            if not self._should_use_local_exec():
                return

            link_path = "/home/rsadmin/bots/botctl.sh"
            target_path = "/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh"

            cmd = f"""
set -euo pipefail
LINK={shlex.quote(link_path)}
TARGET={shlex.quote(target_path)}

if [ ! -e "$TARGET" ]; then
  echo "STATUS=missing_target"
  echo "TARGET=$TARGET"
  exit 0
fi

mkdir -p "$(dirname "$LINK")"

if [ -e "$LINK" ] && [ ! -L "$LINK" ]; then
  echo "STATUS=exists_not_symlink"
  ls -l "$LINK" || true
  exit 0
fi

if [ -L "$LINK" ]; then
  CUR="$(readlink "$LINK" 2>/dev/null || true)"
  echo "STATUS=already_symlink"
  echo "CUR=$CUR"
  exit 0
fi

ln -s "$TARGET" "$LINK"
chmod +x "$TARGET" >/dev/null 2>&1 || true
echo "STATUS=created"
echo "LINK=$LINK"
echo "TARGET=$TARGET"
"""
            ok, out, err = self._execute_ssh_command(cmd, timeout=10)
            msg = (out or err or "").strip()
            if not msg:
                msg = "STATUS=unknown"

            print(f"[shim] botctl_symlink {msg[:400]}")
            try:
                await self._post_or_edit_progress(None, f"[shim] botctl_symlink\n{msg}"[:1900])
            except Exception:
                pass
        except Exception as e:
            try:
                print(f"[shim] botctl_symlink error: {str(e)[:200]}")
            except Exception:
                pass
    
    async def _log_to_discord(self, message: str, embed: Optional[discord.Embed] = None):
        """Log message to Discord status channel"""
        log_channel_id = self.config.get("log_channel_id")
        if not log_channel_id:
            return
        
        try:
            channel = self.bot.get_channel(int(log_channel_id))
            if channel:
                if embed:
                    await channel.send(embed=embed)
                else:
                    await channel.send(message)
        except Exception as e:
            print(f"{Colors.RED}[Discord Log] Failed to send message: {e}{Colors.RESET}")

    async def _get_update_progress_channel(self) -> Optional[discord.abc.Messageable]:
        """Return the configured update-progress channel, if enabled."""
        cfg = self.config.get("update_progress") or {}
        if not cfg.get("enabled"):
            return None
        chan_id_raw = cfg.get("channel_id")
        if not chan_id_raw:
            return None
        try:
            chan_id = int(str(chan_id_raw).strip())
        except Exception:
            return None

        # Prefer cache
        channel = self.bot.get_channel(chan_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(chan_id)  # type: ignore[attr-defined]
            except Exception:
                return None

        # Optional guild guard (helps prevent sending to the wrong server)
        guild_id = cfg.get("guild_id")
        try:
            guild_id_int = int(guild_id) if guild_id is not None else None
        except Exception:
            guild_id_int = None
        if guild_id_int is not None and hasattr(channel, "guild") and getattr(channel, "guild", None):
            if int(getattr(channel.guild, "id", 0)) != guild_id_int:
                return None

        return channel

    def _format_service_state(self, exists: bool, state: Optional[str], pid: Optional[int]) -> str:
        state_txt = state or "unknown"
        pid_txt = str(pid or 0)
        exists_txt = "exists" if exists else "missing"
        return f"exists={exists_txt} state={state_txt} pid={pid_txt}"

    def _get_backup_keep_count(self) -> int:
        try:
            cfg = self.config.get("backup_retention") or {}
            keep = int(cfg.get("keep_per_folder") or 10)
            return max(1, min(keep, 100))
        except Exception:
            return 10

    def _get_service_monitor_config(self) -> Dict[str, Any]:
        cfg = self.config.get("service_monitor") or {}
        try:
            return {
                "enabled": bool(cfg.get("enabled", True)),
                "interval_seconds": int(cfg.get("interval_seconds") or 30),
                "post_on_startup": bool(cfg.get("post_on_startup", True)),
                "post_heartbeat": bool(cfg.get("post_heartbeat", False)),  # Default: no periodic heartbeats
                "post_on_change": bool(cfg.get("post_on_change", True)),  # Default: post on state/pid changes
                "post_on_failure": bool(cfg.get("post_on_failure", True)),  # Default: post when not active
                "failure_log_lines": int(cfg.get("failure_log_lines") or 25),
                "min_seconds_between_posts": int(cfg.get("min_seconds_between_posts") or 120),
                "heartbeat_seconds": int(cfg.get("heartbeat_seconds") or 0),  # Legacy: kept for backward compat
                "test_server_channel_id": str(cfg.get("test_server_channel_id") or self.config.get("update_progress", {}).get("channel_id", "")),
                "rs_errors_channel_id": str(cfg.get("rs_errors_channel_id") or self.config.get("log_channel_id", "1452590450631376906")),
            }
        except Exception:
            return {
                "enabled": True,
                "interval_seconds": 30,
                "post_on_startup": True,
                "post_heartbeat": False,  # Quiet by default
                "post_on_change": True,
                "post_on_failure": True,
                "failure_log_lines": 25,
                "min_seconds_between_posts": 120,
                "heartbeat_seconds": 0,
                "test_server_channel_id": str(self.config.get("update_progress", {}).get("channel_id", "")),
                "rs_errors_channel_id": str(self.config.get("log_channel_id", "1452590450631376906")),
            }

    def _start_service_monitor_task(self) -> None:
        """Start background monitoring of RS bot systemd state (posts only on changes/failures)."""
        if getattr(self, "_service_monitor_task", None):
            return
        cfg = self._get_service_monitor_config()
        if not cfg.get("enabled"):
            return
        if not self.service_manager:
            return

        async def _loop():
            interval = max(10, min(int(cfg.get("interval_seconds") or 30), 3600))
            min_gap = max(0, min(int(cfg.get("min_seconds_between_posts") or 120), 3600))
            lines = max(5, min(int(cfg.get("failure_log_lines") or 25), 200))
            post_heartbeat = cfg.get("post_heartbeat", False)  # Default: quiet (no periodic heartbeats)
            post_on_change = cfg.get("post_on_change", True)  # Default: post on state/pid changes
            post_on_failure = cfg.get("post_on_failure", True)  # Default: post when not active
            # Legacy heartbeat_seconds (backward compat - only used if post_heartbeat is True)
            heartbeat = int(cfg.get("heartbeat_seconds") or 0)
            heartbeat = 0 if heartbeat < 30 else min(heartbeat, 6 * 3600)
            if not post_heartbeat:
                heartbeat = 0  # Disable heartbeat if post_heartbeat is False

            # Only monitor RS bots (including rsadminbot)
            bot_groups = self.config.get("bot_groups") or {}
            rs_keys = ["rsadminbot"] + list(bot_groups.get("rs_bots") or [])

            # Keep snapshot dict: {bot: (state, pid)} - state includes exists info
            last_snapshot: Dict[str, Tuple[str, int]] = {}
            last_post_ts: Dict[str, float] = {}
            last_heartbeat = 0.0

            async def post(bot_key: str, text: str, severity: str = "info", should_ping: bool = False):
                """Post monitor message to appropriate channels.
                
                Args:
                    bot_key: Bot key name
                    text: Message text
                    severity: "info" or "error"
                    should_ping: Whether to ping Neo (only for failures)
                """
                now = time.time()
                last = last_post_ts.get(bot_key, 0.0)
                if min_gap and (now - last) < min_gap:
                    return
                last_post_ts[bot_key] = now
                
                # Get channel IDs from config
                test_channel_id = cfg.get("test_server_channel_id", "")
                rs_errors_channel_id = cfg.get("rs_errors_channel_id", "1452590450631376906")
                
                # Route based on severity
                if severity == "error":
                    # Post to RS error channel
                    try:
                        rs_channel = self.bot.get_channel(int(rs_errors_channel_id))
                        if rs_channel:
                            await rs_channel.send(text[:1900])
                    except Exception:
                        pass
                    
                    # Also post to test server progress channel
                    await self._post_or_edit_progress(None, text)
                    
                    # Ping Neo in test server if requested
                    if should_ping and test_channel_id:
                        try:
                            test_channel = self.bot.get_channel(int(test_channel_id))
                            if test_channel:
                                ping_text = f"<@!{self.config.get('admin_user_ids', [])[0] if self.config.get('admin_user_ids') else ''}> {text[:1900]}"
                                await test_channel.send(ping_text[:2000])
                        except Exception:
                            # Fallback: just post without ping if ping fails
                            await self._post_or_edit_progress(None, text)
                else:
                    # Info messages go to test server progress channel only
                    await self._post_or_edit_progress(None, text)
                
                # Per-bot monitoring channel in test server (if organizer exists)
                if self.test_server_organizer:
                    await self.test_server_organizer.send_to_channel(f"{bot_key}_activity", content=text[:1900])

            # Always announce the monitor is running (one-time)
            try:
                heartbeat_str = f"heartbeat={heartbeat}s" if post_heartbeat and heartbeat > 0 else "heartbeat=disabled"
                await self._post_or_edit_progress(None, f"[monitor] started\ninterval={interval}s {heartbeat_str}")
            except Exception:
                pass

            # Optional initial snapshot
            if cfg.get("post_on_startup"):
                try:
                    lines_out = ["[monitor] RS service snapshot", "```"]
                    for key in rs_keys:
                        info = self.BOTS.get(key) or {}
                        svc = info.get("service", "")
                        if not svc:
                            continue
                        exists, state, _ = self.service_manager.get_status(svc, bot_name=key)
                        pid = self.service_manager.get_pid(svc) or 0
                        lines_out.append(f"{key}: {self._format_service_state(exists, state, pid)}")
                        # Store as (state, pid) - normalize None state to "not_found"
                        snapshot_state = state if state else ("not_found" if not exists else "unknown")
                        last_snapshot[key] = (snapshot_state, pid)
                    lines_out.append("```")
                    await self._post_or_edit_progress(None, "\n".join(lines_out)[:1900])
                except Exception:
                    pass

            while True:
                try:
                    # Optional periodic heartbeat (only if post_heartbeat is True)
                    if post_heartbeat and heartbeat and (time.time() - last_heartbeat) >= heartbeat:
                        last_heartbeat = time.time()
                        try:
                            lines_out = ["[monitor] heartbeat", "```"]
                            for key in rs_keys:
                                info = self.BOTS.get(key) or {}
                                svc = info.get("service", "")
                                if not svc:
                                    continue
                                exists, state, _ = self.service_manager.get_status(svc, bot_name=key)
                                pid = self.service_manager.get_pid(svc) or 0
                                lines_out.append(f"{key}: {self._format_service_state(exists, state, pid)}")
                            lines_out.append("```")
                            await self._post_or_edit_progress(None, "\n".join(lines_out)[:1900])
                        except Exception:
                            pass

                    # Build current snapshot
                    current_snapshot: Dict[str, Tuple[str, int]] = {}
                    for key in rs_keys:
                        info = self.BOTS.get(key) or {}
                        svc = info.get("service", "")
                        if not svc:
                            continue
                        exists, state, _ = self.service_manager.get_status(svc, bot_name=key)
                        pid = self.service_manager.get_pid(svc) or 0
                        # Store as (state, pid) - normalize None state to "not_found"
                        snapshot_state = state if state else ("not_found" if not exists else "unknown")
                        current_snapshot[key] = (snapshot_state, pid)

                    # Compare snapshots - only post when something changes
                    for key in rs_keys:
                        if key not in current_snapshot:
                            continue
                        
                        cur_state, cur_pid = current_snapshot[key]
                        prev = last_snapshot.get(key)
                        
                        # If snapshot hasn't changed, don't post (even if time passed)
                        if prev == (cur_state, cur_pid):
                            continue
                        
                        # Snapshot changed - detect if it's PID-only or state change
                        prev_state, prev_pid = prev if prev else (None, 0)
                        is_pid_only = (prev_state == cur_state) and (prev_state == "active") and (prev_pid != cur_pid)
                        is_failure = (cur_state in ("failed", "inactive", "not_found"))
                        
                        # Update snapshot
                        last_snapshot[key] = (cur_state, cur_pid)
                        
                        # Get full details for the change
                        info = self.BOTS.get(key) or {}
                        svc = info.get("service", "")
                        exists, state, _ = self.service_manager.get_status(svc, bot_name=key)
                        pid = self.service_manager.get_pid(svc) or 0
                        
                        # Build message based on change type
                        if is_pid_only:
                            # PID-only change while active = restart detected (compact message)
                            msg_lines = [
                                f"[monitor] {info.get('name', key)} ({key}) - restart detected",
                                f"PID: {prev_pid} → {cur_pid} (state: active)"
                            ]
                            severity = "info"
                            should_ping = False
                        else:
                            # State change or failure
                            msg_lines = [
                                f"[monitor] {info.get('name', key)} ({key}) - state changed",
                                f"State: {self._format_service_state(exists, state, pid)}"
                            ]
                            if prev_state:
                                msg_lines.insert(1, f"Previous: {prev_state} (pid: {prev_pid})")
                            
                            # If bot is not active, include !details and !logs output
                            if is_failure:
                                # Get detailed status (like !details command)
                                details_success, details_out, _ = self.service_manager.get_detailed_status(svc)
                                if details_success and details_out:
                                    msg_lines.append("\n**Details:**")
                                    msg_lines.append(f"```{details_out[:800]}```")
                                
                                # Get logs (like !logs command)
                                logs = self.service_manager.get_failure_logs(svc, lines=lines) or ""
                                if logs:
                                    msg_lines.append("\n**Recent logs:**")
                                    msg_lines.append(f"```{logs[-1200:]}```")
                                
                                severity = "error"
                                should_ping = True
                            else:
                                severity = "info"
                                should_ping = False
                        
                        msg = "\n".join(msg_lines)
                        await post(key, msg[:1900], severity=severity, should_ping=should_ping)
                        
                except Exception:
                    pass
                await asyncio.sleep(interval)

        self._service_monitor_task = asyncio.create_task(_loop())

    def _get_oraclefiles_sync_config(self) -> Dict[str, Any]:
        """Return OracleFiles snapshot sync config.

        This feature publishes a python-only snapshot of the live Ubuntu bot code to:
          https://github.com/neo-rs/oraclefiles  (repo should exist)

        Recommended config:
        - config.json (non-secret):
            "oraclefiles_sync": {
              "enabled": true,
              "interval_seconds": 14400,
              "repo_dir": "/home/rsadmin/bots/oraclefiles",
              "repo_url": "git@github.com:neo-rs/oraclefiles.git",
              "branch": "main",
              "include_folders": ["RSAdminBot","RSForwarder","RSCheckerbot","RSMentionPinger","RSOnboarding","RSuccessBot"]
            }
        - config.secrets.json (server-only):
            "oraclefiles_sync": {
              "deploy_key_path": "/home/rsadmin/.ssh/oraclefiles_deploy_key"
            }
        """
        base = (self.config.get("oraclefiles_sync") or {}) if isinstance(self.config, dict) else {}
        try:
            include = list(base.get("include_folders") or [])
            include = [str(x).strip() for x in include if str(x).strip()]
            if not include:
                include = ["RSAdminBot", "RSForwarder", "RSCheckerbot", "RSMentionPinger", "RSOnboarding", "RSuccessBot"]
            return {
                "enabled": bool(base.get("enabled", False)),  # Default: disabled (manual only)
                "periodic_enabled": bool(base.get("periodic_enabled", False)),  # Default: no periodic sync
                "interval_seconds": int(base.get("interval_seconds") or 4 * 3600),
                "repo_dir": str(base.get("repo_dir") or "/home/rsadmin/bots/oraclefiles"),
                "repo_url": str(base.get("repo_url") or "git@github.com:neo-rs/oraclefiles.git"),
                "branch": str(base.get("branch") or "main"),
                # NOTE: should be provided via config.secrets.json (merged by load_config_with_secrets).
                "deploy_key_path": str(base.get("deploy_key_path") or ""),
                "include_folders": include,
            }
        except Exception:
            return {
                "enabled": False,
                "interval_seconds": 4 * 3600,
                "repo_dir": "/home/rsadmin/bots/oraclefiles",
                "repo_url": "git@github.com:neo-rs/oraclefiles.git",
                "branch": "main",
                "deploy_key_path": "",
                "include_folders": ["RSAdminBot", "RSForwarder", "RSCheckerbot", "RSMentionPinger", "RSOnboarding", "RSuccessBot"],
            }

    def _oraclefiles_sync_once(self, trigger: str = "manual") -> Tuple[bool, Dict[str, Any]]:
        """Create/update oraclefiles repo and push a python-only snapshot (live Ubuntu -> GitHub)."""
        cfg = self._get_oraclefiles_sync_config()
        if not cfg.get("enabled"):
            return False, {"error": "oraclefiles_sync is disabled (enable it in RSAdminBot/config.json)."}
        if not self._should_use_local_exec():
            return False, {"error": "oraclefiles_sync requires Ubuntu local-exec mode (RSAdminBot must run on the same host)."}

        repo_dir = str(cfg.get("repo_dir") or "/home/rsadmin/bots/oraclefiles")
        repo_url = str(cfg.get("repo_url") or "git@github.com:neo-rs/oraclefiles.git")
        branch = str(cfg.get("branch") or "main")
        deploy_key = str(cfg.get("deploy_key_path") or "").strip()
        include = cfg.get("include_folders") or []
        include = [str(x).strip() for x in include if str(x).strip()]
        if not include:
            include = ["RSAdminBot", "RSForwarder", "RSCheckerbot", "RSMentionPinger", "RSOnboarding", "RSuccessBot"]

        if not deploy_key:
            return False, {"error": "oraclefiles_sync.deploy_key_path missing (put it in RSAdminBot/config.secrets.json)."}

        live_root = str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world")
        trigger_txt = (trigger or "manual").strip().lower()

        folders = " ".join(shlex.quote(x) for x in include)
        cmd = f"""
set -euo pipefail

REPO_DIR={shlex.quote(repo_dir)}
REPO_URL={shlex.quote(repo_url)}
BRANCH={shlex.quote(branch)}
LIVE_ROOT={shlex.quote(live_root)}
DEPLOY_KEY={shlex.quote(deploy_key)}
TRIGGER={shlex.quote(trigger_txt)}

command -v git >/dev/null 2>&1 || {{ echo \"ERR=git_missing\"; exit 2; }}

# Ensure GitHub SSH host key can be accepted non-interactively.
# Use a dedicated known_hosts file and `accept-new` so the first connection pins the key.
SSH_DIR=\"${{HOME:-/home/rsadmin}}/.ssh\"
KNOWN_HOSTS=\"$SSH_DIR/known_hosts\"
mkdir -p \"$SSH_DIR\"
chmod 700 \"$SSH_DIR\" || true
touch \"$KNOWN_HOSTS\"
chmod 600 \"$KNOWN_HOSTS\" || true

export GIT_SSH_COMMAND=\"ssh -i $DEPLOY_KEY -o IdentitiesOnly=yes -o UserKnownHostsFile=$KNOWN_HOSTS -o StrictHostKeyChecking=accept-new\"

mkdir -p \"$REPO_DIR\"
if [ ! -d \"$REPO_DIR/.git\" ]; then
  rm -rf \"$REPO_DIR\"
  git clone \"$REPO_URL\" \"$REPO_DIR\"
fi

cd \"$REPO_DIR\"
git config user.name \"RSAdminBot\"
git config user.email \"rsadminbot@users.noreply.github.com\"
git fetch origin || true

if git show-ref --verify --quiet \"refs/remotes/origin/$BRANCH\"; then
  git checkout -B \"$BRANCH\" \"origin/$BRANCH\"
  git reset --hard \"origin/$BRANCH\"
else
  git checkout -B \"$BRANCH\"
fi

rm -rf py_snapshot
mkdir -p py_snapshot

cd \"$LIVE_ROOT\"
TMP0=/tmp/mw_oraclefiles_py_list.bin
rm -f \"$TMP0\"
find {folders} -type f -name \"*.py\" ! -path \"RSAdminBot/original_files/*\" -print0 > \"$TMP0\"
tar --null -T \"$TMP0\" -cf - | (cd \"$REPO_DIR/py_snapshot\" && tar -xf -)

cd \"$REPO_DIR\"
git add -A

if git diff --cached --quiet; then
  echo \"OK=1\"
  echo \"NO_CHANGES=1\"
  echo \"HEAD=$(git rev-parse HEAD 2>/dev/null || echo '')\"
  exit 0
fi

TS=$(date +%Y%m%d_%H%M%S)
git commit -m \"oraclefiles py_snapshot: $TS trigger=$TRIGGER\" >/dev/null

git push origin \"$BRANCH\" >/dev/null

echo \"OK=1\"
echo \"PUSHED=1\"
echo \"HEAD=$(git rev-parse HEAD)\"
echo \"CHANGED_BEGIN\"
git show --name-only --pretty=format: HEAD | sed '/^$/d' | head -n 120
echo \"CHANGED_END\"
"""

        ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=300)
        out = (stdout or "").strip()
        err = (stderr or "").strip()
        if not ok:
            return False, {"error": (err or out or "oraclefiles sync failed")[:1600]}

        stats: Dict[str, Any] = {"raw": out[-1600:]}
        in_changed = False
        changed: List[str] = []
        for ln in out.splitlines():
            ln = ln.strip()
            if ln == "CHANGED_BEGIN":
                in_changed = True
                continue
            if ln == "CHANGED_END":
                in_changed = False
                continue
            if in_changed:
                if ln:
                    changed.append(ln)
                continue
            if "=" in ln:
                k, v = ln.split("=", 1)
                stats[k.strip().lower()] = v.strip()
        stats["changed_sample"] = changed[:120]
        return True, stats

    def _start_oraclefiles_sync_task(self) -> None:
        """Start periodic OracleFiles sync (only if periodic_enabled is True)."""
        if getattr(self, "_oraclefiles_sync_task", None):
            return
        cfg = self._get_oraclefiles_sync_config()
        if not cfg.get("enabled"):
            return
        if not cfg.get("periodic_enabled", False):  # Default: disabled (manual only)
            print(f"[oraclefiles] periodic sync disabled (use !oraclefilesupdate for manual sync)")
            return
        if not self._should_use_local_exec():
            return

        async def _loop():
            interval = max(300, min(int(cfg.get("interval_seconds") or 4 * 3600), 7 * 24 * 3600))
            await asyncio.sleep(15)
            consecutive_failures = 0
            while True:
                ok, stats = self._oraclefiles_sync_once(trigger="periodic")
                try:
                    if ok:
                        consecutive_failures = 0  # Reset failure counter on success
                        head = str(stats.get("head") or "")[:12]
                        pushed = "1" if str(stats.get("pushed") or "").strip() else "0"
                        # Only post if something was actually pushed
                        if pushed == "1":
                            msg = f"[oraclefiles] periodic OK\nPushed: {pushed}\nHead: {head}"
                            sample = stats.get("changed_sample") or []
                            if sample:
                                msg += "\nChanged sample:\n" + "\n".join(str(x) for x in sample[:30])
                            await self._post_or_edit_progress(None, msg[:1900])
                        # If nothing changed, don't post (quiet success)
                    else:
                        consecutive_failures += 1
                        error_msg = str(stats.get('error', '') or '')[:1600]
                        # Only post failures (to notify of issues)
                        await self._post_or_edit_progress(None, f"[oraclefiles] periodic FAILED\n{error_msg}")
                except Exception as e:
                    consecutive_failures += 1
                    await self._post_or_edit_progress(None, f"[oraclefiles] periodic ERROR\n{str(e)[:1600]}")
                await asyncio.sleep(interval)

        self._oraclefiles_sync_task = asyncio.create_task(_loop())
        print(f"[oraclefiles] periodic sync task started interval={cfg.get('interval_seconds')}s")

    def _get_rsbots_push_config(self) -> Dict[str, Any]:
        """Return RS bots push config for pushing to neo-rs/rsbots.
        
        Recommended config:
        - config.json (non-secret):
            "rsbots_push": {
              "repo_url": "git@github.com:neo-rs/rsbots.git",
              "branch": "main"
            }
        - config.secrets.json (server-only):
            "rsbots_push": {
              "deploy_key_path": "/home/rsadmin/.ssh/rsbots_deploy_key"
            }
        """
        base = (self.config.get("rsbots_push") or {}) if isinstance(self.config, dict) else {}
        try:
            return {
                "repo_url": str(base.get("repo_url") or "git@github.com:neo-rs/rsbots.git"),
                "branch": str(base.get("branch") or "main"),
                # NOTE: should be provided via config.secrets.json (merged by load_config_with_secrets).
                "deploy_key_path": str(base.get("deploy_key_path") or ""),
            }
        except Exception:
            return {
                "repo_url": "git@github.com:neo-rs/rsbots.git",
                "branch": "main",
                "deploy_key_path": "/home/rsadmin/.ssh/rsbots_deploy_key",
            }

    def _rsbots_push_once(self) -> Tuple[bool, Dict[str, Any]]:
        """Push changes from live mirror-world repo to neo-rs/rsbots GitHub repo."""
        cfg = self._get_rsbots_push_config()
        if not self._should_use_local_exec():
            return False, {"error": "rsbots_push requires Ubuntu local-exec mode (RSAdminBot must run on the same host)."}

        repo_url = str(cfg.get("repo_url") or "git@github.com:neo-rs/rsbots.git")
        branch = str(cfg.get("branch") or "main")
        deploy_key = str(cfg.get("deploy_key_path") or "").strip()

        # Default deploy key path if not specified
        if not deploy_key or deploy_key == "":
            deploy_key = "/home/rsadmin/.ssh/rsbots_deploy_key"
        
        # Check if key file exists
        check_cmd = f"test -f {shlex.quote(deploy_key)} && echo 'EXISTS' || echo 'MISSING'"
        check_ok, check_out, _ = self._execute_ssh_command(check_cmd, timeout=5)
        if not check_ok or "EXISTS" not in (check_out or ""):
            return False, {"error": f"rsbots_push deploy key not found at: {deploy_key}\nAdd deploy_key_path to RSAdminBot/config.secrets.json or ensure key exists at default path."}

        live_root = str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world")

        cmd = f"""
set -euo pipefail

REPO_URL={shlex.quote(repo_url)}
BRANCH={shlex.quote(branch)}
LIVE_ROOT={shlex.quote(live_root)}
DEPLOY_KEY={shlex.quote(deploy_key)}

command -v git >/dev/null 2>&1 || {{ echo "ERR=git_missing"; exit 2; }}

# Ensure GitHub SSH host key can be accepted non-interactively.
SSH_DIR="${{HOME:-/home/rsadmin}}/.ssh"
KNOWN_HOSTS="$SSH_DIR/known_hosts"
mkdir -p "$SSH_DIR"
chmod 700 "$SSH_DIR" || true
touch "$KNOWN_HOSTS"
chmod 600 "$KNOWN_HOSTS" || true

export GIT_SSH_COMMAND="ssh -i $DEPLOY_KEY -o IdentitiesOnly=yes -o UserKnownHostsFile=$KNOWN_HOSTS -o StrictHostKeyChecking=accept-new"

cd "$LIVE_ROOT"

# Initialize git repo if it doesn't exist
if [ ! -d ".git" ]; then
  git init
  git config user.name "RSAdminBot"
  git config user.email "rsadminbot@users.noreply.github.com"
  git remote add origin "$REPO_URL" 2>/dev/null || git remote set-url origin "$REPO_URL" 2>/dev/null || true
  git checkout -b "$BRANCH" 2>/dev/null || git branch -M "$BRANCH" 2>/dev/null || true
else
  # Ensure origin is set (best effort - don't fail if already set)
  git remote set-url origin "$REPO_URL" 2>/dev/null || git remote add origin "$REPO_URL" 2>/dev/null || true
  # Ensure we're on the correct branch
  git branch -M "$BRANCH" 2>/dev/null || true
  # Fetch to ensure we have remote refs
  git fetch origin "$BRANCH" 2>/dev/null || true
fi

# Stage all changes (only python files should be tracked per .gitignore)
git add -A

# Check if there are any staged changes
if git diff --cached --quiet; then
  echo "OK=1"
  echo "NO_CHANGES=1"
  HEAD_SHA=$(git rev-parse HEAD 2>/dev/null || echo '')
  echo "HEAD=$HEAD_SHA"
  exit 0
fi

# Check if this is the first commit (no HEAD exists)
FIRST_COMMIT=0
if ! git rev-parse HEAD >/dev/null 2>&1; then
  FIRST_COMMIT=1
fi

# Commit with timestamp
TS=$(date +%Y%m%d_%H%M%S)
git commit -m "rsbots py update: $TS" >/dev/null 2>&1 || {{ echo "ERR=commit_failed"; exit 3; }}

# Push to origin (use -u for first push to set upstream)
if [ "$FIRST_COMMIT" = "1" ]; then
  git push -u origin "$BRANCH" >/dev/null 2>&1 || {{ echo "ERR=push_failed"; exit 4; }}
else
  git push origin "$BRANCH" >/dev/null 2>&1 || {{ echo "ERR=push_failed"; exit 4; }}
fi

echo "OK=1"
echo "PUSHED=1"
echo "HEAD=$(git rev-parse HEAD)"
if [ "$FIRST_COMMIT" = "0" ]; then
  echo "OLD_HEAD=$(git rev-parse HEAD~1 2>/dev/null || echo '')"
fi
echo "CHANGED_BEGIN"
git show --name-only --pretty=format: HEAD | sed '/^$/d' | head -n 100
echo "CHANGED_END"
"""

        ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=180)
        out = (stdout or "").strip()
        err = (stderr or "").strip()
        if not ok:
            return False, {"error": (err or out or "rsbots push failed")[:1600]}

        stats: Dict[str, Any] = {"raw": out[-1600:]}
        in_changed = False
        changed: List[str] = []
        for ln in out.splitlines():
            ln = ln.strip()
            if ln == "CHANGED_BEGIN":
                in_changed = True
                continue
            if ln == "CHANGED_END":
                in_changed = False
                continue
            if in_changed:
                if ln:
                    changed.append(ln)
                continue
            if "=" in ln:
                k, v = ln.split("=", 1)
                stats[k.strip().lower()] = v.strip()
        stats["changed_sample"] = changed[:100]
        return True, stats

    async def _post_or_edit_progress(self, progress_msg, text: str):
        """Best-effort: edit an existing progress message, else send a new one."""
        try:
            if progress_msg is not None:
                await progress_msg.edit(content=text[:1900])
                return progress_msg
        except Exception:
            progress_msg = None
        try:
            ch = await self._get_update_progress_channel()
            if ch is None:
                return None
            return await ch.send(text[:1900])
        except Exception:
            return None

    @staticmethod
    def _truncate_for_discord(text: str, limit: int = 1800) -> str:
        """Truncate long outputs to fit Discord message limits, keeping the tail (usually most useful for logs)."""
        s = (text or "").strip()
        if not s:
            return "(no output)"
        if len(s) <= limit:
            return s
        return "…(truncated)…\n" + s[-limit:]

    @classmethod
    def _codeblock(cls, text: str, limit: int = 1800) -> str:
        return "```" + cls._truncate_for_discord(text, limit=limit) + "```"

    async def _is_progress_channel(self, channel: Optional[discord.abc.Messageable]) -> bool:
        """Return True if the provided channel is the configured update_progress channel."""
        if channel is None:
            return False
        try:
            prog = await self._get_update_progress_channel()
            if prog is None:
                return False
            if hasattr(channel, "id") and hasattr(prog, "id"):
                return int(getattr(channel, "id")) == int(getattr(prog, "id"))
        except Exception:
            return False
        return False
    
    def _get_response_channel(self, ctx) -> Optional[discord.TextChannel]:
        """Get the appropriate channel for command responses.
        
        Commands triggered in test server should also send to RS Server if configured.
        Returns the channel where the command was triggered (for immediate response).
        """
        return ctx.channel
    
    async def _send_response(self, ctx, content: str = None, embed: discord.Embed = None, 
                            also_send_to_rs_server: bool = False):
        """Send standardized command response.
        
        Args:
            ctx: Command context
            content: Text content (optional)
            embed: Embed (optional)
            also_send_to_rs_server: If True, also send to RS Server log channel
        """
        # Send to command channel (immediate response)
        try:
            if embed:
                await ctx.send(embed=embed)
            elif content:
                await ctx.send(content)
        except Exception as e:
            print(f"{Colors.RED}[Command Response] Failed to send: {e}{Colors.RESET}")
        
        # Optionally send to RS Server log channel
        if also_send_to_rs_server:
            rs_server_guild_id = self.config.get("rs_server_guild_id")
            if rs_server_guild_id and ctx.guild and ctx.guild.id != rs_server_guild_id:
                # Command was triggered in test server, also send to RS Server
                log_channel_id = self.config.get("log_channel_id")
                if log_channel_id:
                    try:
                        log_channel = self.bot.get_channel(int(log_channel_id))
                        if log_channel:
                            if embed:
                                # Clone embed and add context
                                rs_embed = discord.Embed(
                                    title=f"{embed.title} (from Test Server)" if embed.title else None,
                                    description=embed.description,
                                    color=embed.color,
                                    timestamp=embed.timestamp
                                )
                                for field in embed.fields:
                                    rs_embed.add_field(name=field.name, value=field.value, inline=field.inline)
                                rs_embed.set_footer(text=f"{embed.footer.text if embed.footer else ''} | Triggered by {ctx.author} in Test Server")
                                await log_channel.send(embed=rs_embed)
                            elif content:
                                await log_channel.send(f"**From Test Server** ({ctx.author}):\n{content}")
                    except Exception as e:
                        print(f"{Colors.YELLOW}[Command Response] Failed to send to RS Server: {e}{Colors.RESET}")
    
    def _build_expected_ssh_commands_content(self) -> str:
        """Build the expected .sh script commands content as a single string for comparison"""
        content_parts = []
        
        # Group bots by their script groups
        bot_groups = self.config.get("bot_groups", {})
        
        # RSAdminBot group
        if bot_groups.get("rsadminbot"):
            content_parts.append("**RSAdminBot**")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status rsadminbot\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh start rsadminbot\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh stop rsadminbot\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart rsadminbot\n```")
            content_parts.append("---")
        
        # RS Bots group
        rs_bots = bot_groups.get("rs_bots", [])
        if rs_bots:
            content_parts.append("**RS Bots** (rsforwarder, rsonboarding, rsmentionpinger, rscheckerbot, rssuccessbot)")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status all\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh start all\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh stop all\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart all\n```")
            content_parts.append("---")
        
        # Mirror-World Bots group
        mirror_bots = bot_groups.get("mirror_bots", [])
        if mirror_bots:
            content_parts.append("**Mirror-World Bots** (datamanagerbot, pingbot, discumbot)")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status all\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh start all\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh stop all\n```")
            content_parts.append("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart all\n```")
            content_parts.append("---")
        
        return "\n".join(content_parts)
    
    async def _check_channel_has_ssh_commands(self, channel) -> bool:
        """Check if channel already contains the expected SSH commands"""
        try:
            # Build expected content
            expected_content = self._build_expected_ssh_commands_content()
            
            # Fetch recent messages (check last 200 messages to find SSH commands)
            messages = []
            async for message in channel.history(limit=200):
                if message.author == self.bot.user and message.content:
                    messages.append(message.content)
            
            # Combine all messages from the bot into a single string
            existing_content = "\n".join(messages)
            
            # Normalize both strings for comparison (remove extra whitespace)
            expected_normalized = "\n".join(line.strip() for line in expected_content.split("\n") if line.strip())
            existing_normalized = "\n".join(line.strip() for line in existing_content.split("\n") if line.strip())
            
            # Check if expected content exists in channel (allowing for some variation)
            # We'll check if all script groups and key commands are present
            bot_groups = self.config.get("bot_groups", {})
            
            # Check for script group headers
            has_rsadminbot = "**RSAdminBot**" in existing_content or "botctl.sh" in existing_content
            has_rs_bots = "**RS Bots**" in existing_content or "botctl.sh" in existing_content
            has_mirror_bots = "**Mirror-World Bots**" in existing_content or "botctl.sh" in existing_content
            
            # Check if we have a reasonable match (all script groups present)
            groups_present = sum([has_rsadminbot, has_rs_bots, has_mirror_bots])
            expected_groups = len([g for g in bot_groups.keys() if bot_groups.get(g)])
            
            if groups_present >= expected_groups * 0.8:  # At least 80% of expected groups
                # Also check for script commands (status, start, stop, restart)
                has_status = "status" in existing_content
                has_start = "start" in existing_content
                has_stop = "stop" in existing_content
                has_restart = "restart" in existing_content
                
                if has_status and has_start and has_stop and has_restart:
                    return True
            
            return False
            
        except Exception as e:
            print(f"{Colors.YELLOW}[Startup] Error checking channel history: {e}{Colors.RESET}")
            # On error, assume content doesn't exist (will send to be safe)
            return False
    
    def _github_py_only_update(self, bot_folder: str) -> Tuple[bool, Dict[str, Any]]:
        """Pull python-only bot code from the server-side GitHub checkout and overwrite live *.py files.

        This is the canonical update path for `!selfupdate` and `!botupdate` when using GitHub as source of truth.

        Server expectations:
        - Git repo exists at: /home/rsadmin/bots/rsbots-code
        - Live bot tree exists at: self.remote_root (typically /home/rsadmin/bots/mirror-world)
        - GitHub repo contains only *.py under the RS bot folders

        Safety:
        - Never deletes first; overwrite-in-place only
        - Only copies files tracked by git and ending in .py under the target folder
        """
        try:
            folder = (bot_folder or "").strip()
            if not folder:
                return False, {"error": "bot_folder required"}

            code_root = "/home/rsadmin/bots/rsbots-code"
            live_root = str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world")

            cmd = f"""
set -euo pipefail

CODE_ROOT={shlex.quote(code_root)}
LIVE_ROOT={shlex.quote(live_root)}
BOT_FOLDER={shlex.quote(folder)}

if [ ! -d "$CODE_ROOT/.git" ]; then
  echo "ERR=missing_code_root"
  echo "DETAIL=$CODE_ROOT/.git not found"
  exit 2
fi
if [ ! -d "$LIVE_ROOT" ]; then
  echo "ERR=missing_live_root"
  echo "DETAIL=$LIVE_ROOT not found"
  exit 2
fi

cd "$CODE_ROOT"
OLD="$(git rev-parse HEAD 2>/dev/null || echo '')"
git fetch origin
git pull --ff-only origin main
NEW="$(git rev-parse HEAD)"

CHANGED="$(git diff --name-only "$OLD" "$NEW" -- "$BOT_FOLDER" 2>/dev/null | grep -E \"\\\\.py$\" || true)"
# NOTE: grep returns exit code 1 on empty input, which would abort under `set -e`.
# Use sed to drop empty lines (always exit 0), then count.
CHANGED_COUNT="$(echo \"$CHANGED\" | sed '/^$/d' | wc -l | tr -d \" \")"

TMP_LIST="/tmp/mw_pyonly_${{BOT_FOLDER}}.txt"
git ls-files "$BOT_FOLDER" 2>/dev/null | grep -E \"\\\\.py$\" > "$TMP_LIST" || true
PY_COUNT="$(wc -l < "$TMP_LIST" | tr -d \" \")"
if [ "$PY_COUNT" = "" ]; then PY_COUNT="0"; fi
if [ "$PY_COUNT" = "0" ]; then
  echo "ERR=no_python_files"
  echo "DETAIL=no tracked *.py under $BOT_FOLDER in $CODE_ROOT"
  exit 3
fi

tar -cf - -T "$TMP_LIST" | (cd "$LIVE_ROOT" && tar -xf -)

echo "OK=1"
echo "OLD=$OLD"
echo "NEW=$NEW"
echo "PY_COUNT=$PY_COUNT"
echo "CHANGED_COUNT=$CHANGED_COUNT"
echo "CHANGED_BEGIN"
echo "$CHANGED" | grep -v "^$" | head -n 30 || true
echo "CHANGED_END"
"""

            ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=180)
            out = (stdout or "").strip()
            err = (stderr or "").strip()
            if not ok:
                msg = err or out or "unknown error"
                return False, {"error": msg[:1200]}

            stats: Dict[str, Any] = {"raw": out[-1600:]}
            lines = [ln.rstrip("\r") for ln in out.splitlines()]
            in_changed = False
            changed_lines: List[str] = []
            for ln in lines:
                if ln == "CHANGED_BEGIN":
                    in_changed = True
                    continue
                if ln == "CHANGED_END":
                    in_changed = False
                    continue
                if in_changed:
                    if ln.strip():
                        changed_lines.append(ln.strip())
                    continue
                if "=" in ln:
                    k, v = ln.split("=", 1)
                    k = k.strip().lower()
                    v = v.strip()
                    if k:
                        stats[k] = v
            stats["changed_sample"] = changed_lines[:30]
            return True, stats
        except Exception as e:
            return False, {"error": f"github py-only update failed: {str(e)[:300]}"}
    
    # Legacy Phase 4 file sync / tree compare / auto-sync removed.
    # Legacy helper functions (_should_exclude_file, _is_unimportant_remote_file, _count_files_recursive) removed.
    
    # NOTE: Previously we supported rs-bot-tokens.txt + a command that scraped tokens from configs.
    # That approach is intentionally removed for safety: secrets now live in config.secrets.json (server-only),
    # and bot IDs for tracking are discovered from the RS Server guild at runtime.
    
    async def _send_ssh_commands_to_channel(self):
        """Send all .sh script commands used by the bot to the SSH commands channel"""
        ssh_commands_channel_id = self.config.get("ssh_commands_channel_id")
        if not ssh_commands_channel_id:
            print(f"{Colors.YELLOW}[Startup] SSH commands channel ID not configured, skipping{Colors.RESET}")
            return
        
        try:
            channel = self.bot.get_channel(int(ssh_commands_channel_id))
            if not channel:
                print(f"{Colors.YELLOW}[Startup] SSH commands channel not found (ID: {ssh_commands_channel_id}){Colors.RESET}")
                return
            
            print(f"{Colors.CYAN}[Startup] Checking SSH commands channel: {channel.name}{Colors.RESET}")
            
            # Check if channel already has the SSH commands
            if await self._check_channel_has_ssh_commands(channel):
                print(f"{Colors.GREEN}[Startup] SSH commands already exist in channel, skipping{Colors.RESET}")
                return
            
            print(f"{Colors.CYAN}[Startup] Sending .sh script commands to channel: {channel.name}{Colors.RESET}")
            
            bot_groups = self.config.get("bot_groups", {})
            
            # RSAdminBot group
            if bot_groups.get("rsadminbot"):
                await channel.send("**RSAdminBot**")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status rsadminbot\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh start rsadminbot\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh stop rsadminbot\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart rsadminbot\n```")
                await channel.send("---")
            
            # RS Bots group
            rs_bots = bot_groups.get("rs_bots", [])
            if rs_bots:
                await channel.send("**RS Bots** (rsforwarder, rsonboarding, rsmentionpinger, rscheckerbot, rssuccessbot)")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status all\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh start all\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh stop all\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart all\n```")
                await channel.send("---")
            
            # Mirror-World Bots group
            mirror_bots = bot_groups.get("mirror_bots", [])
            if mirror_bots:
                await channel.send("**Mirror-World Bots** (datamanagerbot, pingbot, discumbot)")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status all\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh start all\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh stop all\n```")
                await channel.send("```bash\nbash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart all\n```")
                await channel.send("---")
            
            print(f"{Colors.GREEN}[Startup] .sh script commands sent to channel successfully{Colors.RESET}")
            
        except Exception as e:
            print(f"{Colors.RED}[Startup] Failed to send SSH commands to channel: {e}{Colors.RESET}")
            import traceback
            print(f"{Colors.RED}[Startup] Traceback: {traceback.format_exc()[:300]}{Colors.RESET}")

    def _build_command_index_text(self) -> str:
        """Build a compact command index for Discord (no secrets)."""
        # Prefer registered_commands for admin flags, but also include every command name.
        reg = list(getattr(self, "registered_commands", []) or [])
        reg_map = {name: (desc, is_admin) for (name, desc, is_admin) in reg if name}

        cmds = []
        for c in list(self.bot.commands):
            name = getattr(c, "name", "")
            if not name:
                continue
            if name == "help":
                continue
            desc = ""
            is_admin = None
            if name in reg_map:
                desc, is_admin = reg_map[name]
            else:
                desc = (getattr(c, "help", "") or "").strip().splitlines()[0:1]
                desc = desc[0] if desc else ""
            cmds.append((name, desc, is_admin))

        cmds.sort(key=lambda x: x[0])
        lines = []
        lines.append("RSAdminBot Command Index")
        lines.append("Prefix: !")
        lines.append("")
        for name, desc, is_admin in cmds:
            admin_tag = " [ADMIN]" if is_admin else ""
            if desc:
                lines.append(f"!{name}{admin_tag} - {desc}")
            else:
                lines.append(f"!{name}{admin_tag}")
        return "\n".join(lines).strip()

    async def _publish_command_index_to_test_server(self) -> None:
        """Post or update the command index in the test server monitoring channel."""
        if not self.test_server_organizer:
            return

        # Ensure channels exist (creates commands channel if missing).
        try:
            await self.test_server_organizer.setup_monitoring_channels()
        except Exception:
            return

        channel_id = self.test_server_organizer.get_channel_id("commands")
        if not channel_id:
            return

        channel = self.bot.get_channel(int(channel_id))
        if not channel:
            try:
                channel = await self.bot.fetch_channel(int(channel_id))  # type: ignore[attr-defined]
            except Exception:
                return

        text = self._build_command_index_text()
        content = f"```{text[:1900]}```"
        content_hash = self.test_server_organizer._sha256_text(text)  # stable hash
        prev_hash = self.test_server_organizer.get_meta("commands_hash", "")
        msg_id = self.test_server_organizer.get_meta("commands_message_id", None)

        # If unchanged and we have a message id, do nothing.
        if prev_hash == content_hash and msg_id:
            return

        try:
            if msg_id:
                try:
                    msg = await channel.fetch_message(int(msg_id))  # type: ignore[attr-defined]
                    await msg.edit(content=content)
                    self.test_server_organizer.set_meta("commands_hash", content_hash)
                    return
                except Exception:
                    # fallthrough: create a new message
                    pass

            msg = await channel.send(content)
            self.test_server_organizer.set_meta("commands_hash", content_hash)
            self.test_server_organizer.set_meta("commands_message_id", int(msg.id))
        except Exception:
            return
    
    
    def _execute_ssh_command(self, command: str, timeout: int = 30) -> Tuple[bool, str, str]:
        """Execute SSH command and return (success, stdout, stderr)
        
        Uses shell=False to prevent PowerShell parsing on Windows.
        Commands are executed inside remote bash shell.
        """
        # Check if server is configured (self-contained - uses config.json)
        if not self.current_server:
            error_msg = "No SSH server configured in config.json"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            return False, "", error_msg

        # Local execution mode (Ubuntu host): run commands directly in bash without SSH.
        if self._should_use_local_exec():
            try:
                result = subprocess.run(
                    ["bash", "-lc", command],
                    shell=False,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    encoding="utf-8",
                    errors="replace",
                )
                stdout_clean = (result.stdout or "").strip()
                stderr_clean = (result.stderr or "").strip()
                if result.returncode != 0:
                    print(f"{Colors.RED}[Local Exec Error] Command failed: {command[:100]}{Colors.RESET}")
                    if stderr_clean:
                        print(f"{Colors.RED}[Local Exec Error] {stderr_clean[:200]}{Colors.RESET}")
                return result.returncode == 0, stdout_clean, stderr_clean
            except subprocess.TimeoutExpired:
                error_msg = f"Command timed out after {timeout}s"
                print(f"{Colors.RED}[Local Exec Error] {error_msg}{Colors.RESET}")
                return False, "", error_msg
            except Exception as e:
                error_msg = f"Unexpected error executing local command: {str(e)}"
                print(f"{Colors.RED}[Local Exec Error] {error_msg}{Colors.RESET}")
                return False, "", error_msg
        
        # Build SSH command locally (self-contained)
        # Check if SSH key exists (already resolved in _load_ssh_config)
        ssh_key = self.current_server.get("key")
        if ssh_key:
            key_path = Path(ssh_key)
            if not key_path.exists():
                error_msg = f"SSH key file not found: {key_path}"
                print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
                print(f"{Colors.YELLOW}[SSH Error] Expected key at: {key_path}{Colors.RESET}")
                return False, "", error_msg
        
        try:
            # Build SSH base command locally (self-contained)
            base = self._build_ssh_base(self.current_server)
            if not base:
                error_msg = "Failed to build SSH base command (check server config)"
                print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
                return False, "", error_msg
            
            # Escape command for bash -c
            escaped_cmd = shlex.quote(command)
            
            # Build command as list (no shell parsing on Windows)
            cmd = base + ["-t", "-o", "ConnectTimeout=10", "bash", "-lc", escaped_cmd]
            
            # Suppress verbose output - only log errors
            is_validation = command.strip() == "sudo -n true"
            
            result = subprocess.run(
                cmd,
                shell=False,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding='utf-8',
                errors='replace'
            )
            
            # Clean output (strip whitespace)
            stdout_clean = (result.stdout or "").strip()
            stderr_clean = (result.stderr or "").strip()
            
            # Only log errors, not every command execution
            if result.returncode != 0:
                if not is_validation:
                    print(f"{Colors.RED}[SSH Error] Command failed: {command[:100]}{Colors.RESET}")
                    if stderr_clean:
                        print(f"{Colors.RED}[SSH Error] {stderr_clean[:200]}{Colors.RESET}")
                    if stdout_clean:
                        print(f"{Colors.YELLOW}[SSH Error] {stdout_clean[:200]}{Colors.RESET}")
            
            return result.returncode == 0, stdout_clean, stderr_clean
        except subprocess.TimeoutExpired:
            error_msg = f"Command timed out after {timeout}s"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.RED}[SSH Error] Command: {command[:200]}{Colors.RESET}")
            return False, "", error_msg
        except FileNotFoundError as e:
            error_msg = f"SSH executable not found: {e}"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.YELLOW}[SSH Error] Make sure SSH is installed and in PATH{Colors.RESET}")
            return False, "", error_msg
        except Exception as e:
            error_msg = f"Unexpected error executing SSH command: {str(e)}"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.RED}[SSH Error] Command: {command[:200]}{Colors.RESET}")
            import traceback
            print(f"{Colors.RED}[SSH Error] Traceback: {traceback.format_exc()[:500]}{Colors.RESET}")
            return False, "", error_msg
    
    def _service_name_to_bot_name(self, service_name: str) -> Optional[str]:
        """Map service name to bot name.
        
        Args:
            service_name: Systemd service name (e.g., "mirror-world-rsforwarder.service")
            
        Returns:
            Bot name (e.g., "rsforwarder") or None if not found
        """
        # Remove .service suffix and mirror-world- prefix
        if service_name.endswith(".service"):
            service_name = service_name[:-8]
        if service_name.startswith("mirror-world-"):
            bot_name = service_name[13:]  # Remove "mirror-world-" prefix
            # Check if bot exists in BOTS dict
            if bot_name in self.BOTS:
                return bot_name
        return None
    
    def _is_rs_bot(self, bot_name: str) -> bool:
        """Check if a bot is an RS bot (excludes mirror_bots like datamanagerbot, discumbot, pingbot).
        
        Args:
            bot_name: Bot name (e.g., "rsforwarder", "datamanagerbot")
            
        Returns:
            True if bot is an RS bot (rsadminbot or rs_bots group), False otherwise
        """
        bot_group = self._get_bot_group(bot_name)
        return bot_group in ("rsadminbot", "rs_bots")
    
    def _get_bot_group(self, bot_name: str) -> Optional[str]:
        """Get bot group for a given bot name.
        
        Args:
            bot_name: Bot name (e.g., "rsforwarder", "datamanagerbot")
            
        Returns:
            "rsadminbot", "rs_bots", "mirror_bots", or None if not found
        """
        bot_groups = self.config.get("bot_groups", {})
        
        if bot_name == "rsadminbot":
            return "rsadminbot"
        
        for group_name, bots in bot_groups.items():
            if isinstance(bots, list) and bot_name in bots:
                return group_name
        
        return None
    
    def _get_script_name(self, bot_group: str) -> str:
        """Get script name for a bot group.
        
        Args:
            bot_group: Bot group name ("rsadminbot", "rs_bots", "mirror_bots")
            
        Returns:
            Script name (e.g., "manage_rsadminbot.sh")
        """
        script_map = {
            "rsadminbot": "manage_rsadminbot.sh",
            "rs_bots": "manage_rs_bots.sh",
            "mirror_bots": "manage_mirror_bots.sh"
        }
        return script_map.get(bot_group, "manage_bots.sh")
    
    def _execute_sh_script(self, script_name: str, action: str, bot_name: str, *args) -> Tuple[bool, Optional[str], Optional[str]]:
        """Execute a .sh script via SSH.
        
        Args:
            script_name: Script name (e.g., "manage_rs_bots.sh")
            action: Action (start, stop, restart, status)
            bot_name: Bot name
            *args: Additional arguments
            
        Returns:
            (success, stdout, stderr)
        """
        # Canonical entrypoint: always call botctl.sh on the remote server.
        # Keep signature for compatibility, but do not execute per-group scripts directly.
        botctl_path = "/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh"
        cmd_parts = [action, bot_name] + list(args)
        cmd = f"bash {botctl_path} {' '.join(shlex.quote(str(arg)) for arg in cmd_parts)}"
        
        return self._execute_ssh_command(cmd, timeout=120)
    
    def load_config(self):
        """Load configuration from JSON file"""
        default_config = {
            "guild_id": 0,
            "admin_role_ids": [],
            "admin_user_ids": [],
            "log_channel_id": ""
        }
        
        if self.config_path.exists():
            try:
                # Load config.json and merge config.secrets.json (server-only) on top
                self.config, _, secrets_path = load_config_with_secrets(self.base_path)
                # Merge with defaults for missing keys
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
                if not secrets_path.exists():
                    print(f"{Colors.YELLOW}[Config] Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
                    print(f"{Colors.YELLOW}[Config] Create it to provide required secrets like bot_token{Colors.RESET}")
                print(f"{Colors.GREEN}[Config] Loaded configuration{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}[Config] Failed to load config: {e}{Colors.RESET}")
                self.config = default_config
        else:
            self.config = default_config
            self.save_config()
            print(f"{Colors.YELLOW}[Config] Created default config.json - please configure it{Colors.RESET}")
    
    def save_config(self):
        """Save configuration to JSON file"""
        try:
            # Never write secrets back into config.json
            config_to_save = dict(self.config or {})
            config_to_save.pop("bot_token", None)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_to_save, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to save config: {e}{Colors.RESET}")
    
    def is_admin(self, user: discord.Member) -> bool:
        """Check if user is an admin"""
        admin_role_ids = self.config.get("admin_role_ids", [])
        admin_user_ids = self.config.get("admin_user_ids", [])
        
        # Check user ID
        if str(user.id) in [str(uid) for uid in admin_user_ids]:
            return True
        
        # Check roles
        user_role_ids = [str(r.id) for r in user.roles]
        for admin_role_id in admin_role_ids:
            if str(admin_role_id) in user_role_ids:
                return True
        
        # Check if user has administrator permission
        if user.guild_permissions.administrator:
            return True
        
        return False
    
    def _setup_events(self):
        """Setup Discord event handlers"""
        
        @self.bot.event
        async def on_ready():
            """Bot startup sequence - organized into clear phases using sequence modules"""
            
            # Prevent multiple on_ready triggers (discord.py can fire this multiple times)
            if not hasattr(self, '_startup_complete'):
                self._startup_complete = False
            
            if self._startup_complete:
                # Already completed startup - this is likely a reconnection
                print(f"{Colors.YELLOW}[Reconnect] Bot reconnected - skipping full startup sequence{Colors.RESET}")
                print(f"{Colors.GREEN}[Reconnect] ✓ Bot connected as: {self.bot.user}{Colors.RESET}")
                print(f"{Colors.GREEN}[Reconnect] ✓ Bot ID: {self.bot.user.id}{Colors.RESET}")
                print(f"{Colors.GREEN}[Reconnect] ✓ Bot latency: {round(self.bot.latency * 1000)}ms{Colors.RESET}\n")
                return
            
            # Mark startup as in progress - commands are already registered, so mark complete even if sequences fail
            self._startup_complete = True
            
            # Wrap entire startup sequence in try/except to ensure on_ready always completes
            try:
                # Best-effort runtime shims (Ubuntu local-exec only)
                try:
                    await self._ensure_botctl_symlink()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] botctl symlink setup failed (non-critical): {e}{Colors.RESET}")
                
                # Import and run startup sequences
                try:
                    from startup_sequences import (
                        sequence_1_initialization,
                        sequence_2_tracking,
                        sequence_3_server_status,
                        sequence_4_file_sync,
                        sequence_5_channels,
                        sequence_6_background
                    )
                    
                    # Run all sequences
                    await sequence_1_initialization.run(self)
                    await sequence_2_tracking.run(self)
                    await sequence_3_server_status.run(self)
                    await sequence_4_file_sync.run(self)
                    await sequence_5_channels.run(self)
                    await sequence_6_background.run(self)

                    # If a self-update was applied during restart, report it to the update-progress channel now.
                    try:
                        marker = self.base_path / ".last_selfupdate_applied.json"
                        if marker.exists():
                            data = json.loads(marker.read_text(encoding="utf-8") or "{}")
                            backup = str(data.get("backup") or "")
                            ts = str(data.get("timestamp") or "")
                            changes = data.get("changes") or {}
                            sample = changes.get("sample") or []
                            py_sample = changes.get("py_sample") or []
                            total = changes.get("total")
                            py_total = changes.get("py_total")
                            # Fetch some recent journal lines for context.
                            ok_j, out_j, _ = self._execute_ssh_command("journalctl -u mirror-world-rsadminbot.service -n 40 --no-pager | tail -n 40", timeout=20)
                            tail = (out_j or "").strip()
                            msg = (
                                "[selfupdate] APPLIED\n"
                                f"Timestamp: {ts}\n"
                                f"Backup: {backup}\n"
                            )
                            if isinstance(total, int) and isinstance(py_total, int):
                                msg += f"Files changed: {total} (py: {py_total})\n"
                            if py_sample:
                                msg += "\nChanged .py (sample):\n" + "\n".join(str(p) for p in py_sample[:20]) + "\n"
                            elif sample:
                                msg += "\nChanged files (sample):\n" + "\n".join(str(p) for p in sample[:20]) + "\n"
                            if ok_j and tail:
                                msg += "\nRecent service logs:\n" + tail[-1400:]
                            await self._post_or_edit_progress(None, msg)
                            try:
                                marker.unlink()
                            except Exception:
                                pass
                    except Exception as e:
                        print(f"{Colors.YELLOW}[Startup] Self-update marker processing failed (non-critical): {e}{Colors.RESET}")
                    
                except ImportError as e:
                    print(f"{Colors.YELLOW}[Startup] Startup sequences not available (non-critical): {e}{Colors.RESET}")
                    import traceback
                    print(f"{Colors.DIM}[Startup] Import traceback: {traceback.format_exc()[:300]}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] Startup sequences error (non-critical): {e}{Colors.RESET}")
                    import traceback
                    print(f"{Colors.DIM}[Startup] Sequence traceback: {traceback.format_exc()[:500]}{Colors.RESET}")
                
            except Exception as e:
                # Critical error - log but don't prevent bot from running
                print(f"{Colors.RED}[Startup] Critical error in on_ready (continuing anyway): {e}{Colors.RESET}")
                import traceback
                print(f"{Colors.RED}[Startup] Full traceback: {traceback.format_exc()}{Colors.RESET}")
            
            # Always log completion
            print(f"{Colors.GREEN}[Startup] ✓ on_ready completed successfully{Colors.RESET}")
            print(f"{Colors.GREEN}[Startup] ✓ Bot is ready and accepting commands{Colors.RESET}\n")
        
        # Bot movement tracking event listeners
        @self.bot.event
        async def on_message(message: discord.Message):
            """Track bot write operations"""
            # Skip bot's own messages to prevent loops
            if message.author == self.bot.user:
                return
            
            # Process commands first (required for bot commands to work)
            await self.bot.process_commands(message)
            
            # Then track bot movements
            if self.bot_movement_tracker:
                await self.bot_movement_tracker.track_message(message)
        
        @self.bot.event
        async def on_message_edit(before: discord.Message, after: discord.Message):
            """Track bot message edits"""
            if self.bot_movement_tracker:
                await self.bot_movement_tracker.track_message_edit(before, after)
        
        @self.bot.event
        async def on_message_delete(message: discord.Message):
            """Track bot message deletes"""
            if self.bot_movement_tracker:
                await self.bot_movement_tracker.track_message_delete(message)
        
        @self.bot.event
        async def on_command_error(ctx, error):
            """Handle command errors"""
            if isinstance(error, commands.CommandNotFound):
                return  # Ignore unknown commands
            elif isinstance(error, commands.MissingPermissions):
                print(f"{Colors.YELLOW}[Command Error] Missing permissions: {ctx.author} tried to use {ctx.command}{Colors.RESET}")
                await ctx.send("❌ **Error:** You don't have permission to use this command.")
            elif isinstance(error, commands.CommandOnCooldown):
                print(f"{Colors.YELLOW}[Command Error] Cooldown: {ctx.author} tried to use {ctx.command} too soon{Colors.RESET}")
                await ctx.send(f"❌ **Cooldown:** Please wait {error.retry_after:.1f} seconds.")
            else:
                error_msg = str(error)
                print(f"{Colors.RED}[Command Error] {error_msg}{Colors.RESET}")
                print(f"{Colors.RED}[Command Error] Command: {ctx.command}, User: {ctx.author}, Channel: {ctx.channel}{Colors.RESET}")
                import traceback
                print(f"{Colors.RED}[Command Error] Traceback:{Colors.RESET}")
                for line in traceback.format_exc().split('\n')[:10]:
                    if line.strip():
                        print(f"{Colors.RED}[Command Error]   {line}{Colors.RESET}")
                await ctx.send("❌ **Error:** An error occurred while executing the command.")
    
    def _setup_commands(self):
        """Setup prefix commands"""
        # Track registered commands for initialization logging
        self.registered_commands = []
        
        @self.bot.command(name="ping")
        async def ping(ctx):
            """Check bot latency"""
            latency = round(self.bot.latency * 1000)
            await ctx.send(f"🏓 Pong! Latency: {latency}ms")
        self.registered_commands.append(("ping", "Check bot latency", False))
        
        @self.bot.command(name="status")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def status(ctx):
            """Show bot status and readiness (admin only)"""
            embed = discord.Embed(
                title="🤖 RS Admin Bot Status",
                description="**Bot is ready and operational** ✅",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            
            # Bot connection status
            status_value = f"✅ **Online** (Invisible)\n"
            status_value += f"User: {self.bot.user}\n"
            status_value += f"ID: {self.bot.user.id}\n"
            status_value += f"Latency: {round(self.bot.latency * 1000)}ms"
            embed.add_field(
                name="🔌 Connection",
                value=status_value,
                inline=False
            )
            
            # Guilds
            guild_names = [g.name for g in self.bot.guilds]
            embed.add_field(
                name="📡 Servers",
                value=f"{len(self.bot.guilds)}\n" + "\n".join(f"• {name}" for name in guild_names[:5]),
                inline=True
            )
            
            # SSH Server status
            if self.current_server:
                ssh_status = f"✅ **Connected**\n"
                ssh_status += f"Server: {self.current_server.get('name', 'Unknown')}\n"
                ssh_status += f"Host: {self.current_server.get('host', 'N/A')}"
            else:
                ssh_status = "❌ **Not configured**\nAdd `ssh_server` to config.json"
            embed.add_field(
                name="🖥️ SSH Server",
                value=ssh_status,
                inline=True
            )
            
            # Module status
            modules_status = []
            modules_status.append("✅ Service Manager" if self.service_manager else "❌ Service Manager")
            modules_status.append("✅ Whop Tracker" if self.whop_tracker else "❌ Whop Tracker")
            modules_status.append("✅ Movement Tracker" if self.bot_movement_tracker else "❌ Movement Tracker")
            modules_status.append("✅ Bot Inspector" if self.inspector else "❌ Bot Inspector")
            
            embed.add_field(
                name="🔧 Modules",
                value="\n".join(modules_status),
                inline=False
            )
            
            # Quick commands reminder
            embed.add_field(
                name="💡 Quick Commands",
                value="`!botlist` - List all bots\n`!botstatus <bot>` - Check bot status\n`!botstart <bot>` - Start a bot\n`!botstop <bot>` - Stop a bot",
                    inline=False
            )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name="reload")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def reload(ctx):
            """Reload configuration (admin only)"""
            self.load_config()
            self._load_ssh_config()  # Self-contained - always reload from config.json
            await ctx.send("✅ Configuration reloaded!")
        
        @self.bot.command(name="restart")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def restart(ctx):
            """Restart RSAdminBot locally or remotely (admin only)"""
            # Reuse the same RestartView from restartadminbot
            class RestartView(ui.View):
                def __init__(self, admin_bot_instance):
                    super().__init__(timeout=60)
                    self.admin_bot = admin_bot_instance
                
                @ui.button(label="🖥️ Restart Locally", style=discord.ButtonStyle.primary)
                async def restart_local(self, interaction: discord.Interaction, button: ui.Button):
                    """Restart the bot locally (exit and let systemd restart)"""
                    await interaction.response.send_message("🔄 **Restarting RSAdminBot locally...**\nThe bot will exit and systemd will restart it automatically.", ephemeral=True)
                    
                    print(f"{Colors.YELLOW}[Restart] Local restart requested by {interaction.user} ({interaction.user.id}){Colors.RESET}")
                    print(f"{Colors.YELLOW}[Restart] Exiting bot to allow systemd restart...{Colors.RESET}")
                    
                    # Store restart info for followup message after restart
                    restart_info = {
                        "user_id": interaction.user.id,
                        "user_name": str(interaction.user),
                        "channel_id": interaction.channel.id if interaction.channel else None,
                        "guild_id": interaction.guild.id if interaction.guild else None,
                        "timestamp": datetime.now().isoformat(),
                        "restart_type": "local"
                    }
                    restart_info_file = self.admin_bot.base_path / "pending_restart_followup.json"
                    try:
                        with open(restart_info_file, 'w', encoding='utf-8') as f:
                            json.dump(restart_info, f, indent=2)
                        print(f"{Colors.CYAN}[Restart] Stored restart info for followup: {restart_info_file}{Colors.RESET}")
                    except Exception as e:
                        print(f"{Colors.YELLOW}[Restart] ⚠️  Failed to store restart info: {e}{Colors.RESET}")
                    
                    # Log to Discord before exit
                    await self.admin_bot._log_to_discord(f"🔄 **Local Restart Initiated**")
                    
                    # Close the bot gracefully
                    await self.admin_bot.bot.close()
                    
                    # Exit the process (systemd will restart it)
                    import sys
                    sys.exit(0)
                
                @ui.button(label="🌐 Restart Remotely", style=discord.ButtonStyle.secondary)
                async def restart_remote(self, interaction: discord.Interaction, button: ui.Button):
                    """Restart the bot on remote server via SSH"""
                    ssh_ok, error_msg = self.admin_bot._check_ssh_available()
                    if not ssh_ok:
                        await interaction.response.send_message(f"❌ **SSH not configured**: {error_msg}", ephemeral=True)
                        return
                    
                    await interaction.response.send_message("🔄 **Restarting RSAdminBot on remote server...**\nThis may take a few moments.", ephemeral=True)
                    
                    bot_info = self.admin_bot.BOTS["rsadminbot"]
                    service_name = bot_info["service"]
                    
                    print(f"{Colors.CYAN}[Restart] Remote restart requested by {interaction.user} ({interaction.user.id}){Colors.RESET}")
                    print(f"{Colors.CYAN}[Restart] Restarting service: {service_name}{Colors.RESET}")
                    
                    # Use ServiceManager to restart
                    if self.admin_bot.service_manager:
                        success, stdout, stderr = self.admin_bot.service_manager.restart(
                            service_name, 
                            script_pattern=bot_info.get("script"),
                            bot_name="rsadminbot"
                        )
                        
                        if success:
                            # Verify it started
                            await asyncio.sleep(2)
                            exists, state, error = self.admin_bot.service_manager.get_status(service_name, bot_name="rsadminbot")
                            
                            if exists and state == "active":
                                await interaction.followup.send("✅ **RSAdminBot restarted successfully on remote server!**\nThe bot will sync files on next startup.", ephemeral=True)
                                await self.admin_bot._log_to_discord(f"✅ **Remote Restart Successful**\nService: {service_name}")
                                print(f"{Colors.GREEN}[Restart] Remote restart successful{Colors.RESET}")
                            else:
                                await interaction.followup.send(f"⚠️ **Restart initiated but status unclear**\nState: {state if exists else 'Service not found'}", ephemeral=True)
                                await self.admin_bot._log_to_discord(f"⚠️ **Remote Restart Status Unclear**\nState: {state if exists else 'Service not found'}")
                        else:
                            error_msg = stderr or stdout or "Unknown error"
                            await interaction.followup.send(f"❌ **Restart failed**: {error_msg[:500]}", ephemeral=True)
                            await self.admin_bot._log_to_discord(f"❌ **Remote Restart Failed**\nError: {error_msg[:500]}")
                            print(f"{Colors.RED}[Restart] Remote restart failed: {error_msg}{Colors.RESET}")
                    else:
                        await interaction.followup.send("❌ **ServiceManager not available**", ephemeral=True)
            
            embed = discord.Embed(
                title="🔄 Restart RSAdminBot",
                description="Choose how to restart the bot:\n\n**After restart, the bot will automatically sync files on startup.**",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            embed.add_field(
                name="🖥️ Local Restart",
                value="Exits the bot and lets systemd restart it automatically.\n*Use this if running on the same machine.*",
                inline=False
            )
            embed.add_field(
                name="🌐 Remote Restart",
                value="Restarts the bot service on the remote Ubuntu server via SSH.\n*Use this if the bot runs on a remote server.*",
                inline=False
            )
            embed.set_footer(text="Select an option below (expires in 60 seconds)")
            
            view = RestartView(self)
            await ctx.send(embed=embed, view=view)
        self.registered_commands.append(("restart", "Restart RSAdminBot locally or remotely", True))
        
        # NOTE: !updatetokens removed (it encouraged storing plaintext tokens locally).

        @self.bot.command(name="details")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def details(ctx, bot_name: str = None):
            """Show systemd details for a bot (admin only)."""
            if not bot_name:
                view = BotSelectView(self, "details", "Details")
                embed = discord.Embed(
                    title="🧾 Select Bot for Details",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.blurple(),
                )
                await ctx.send(embed=embed, view=view)
                return
            bot_key = (bot_name or "").strip().lower()
            if bot_key not in self.BOTS:
                await ctx.send(f"❌ Unknown bot: {bot_key}\nUse `!botlist`.")
                return
            info = self.BOTS[bot_key]
            success, out, err = self._execute_sh_script("botctl.sh", "details", bot_key)
            await ctx.send(f"🧾 **Details: {info.get('name', bot_key)}**\n{self._codeblock(out or err or '')}"[:1900])
        self.registered_commands.append(("details", "Show systemctl status/details for a bot", True))

        @self.bot.command(name="logs")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def logs(ctx, bot_name: str = None, lines: str = "80"):
            """Show journal logs for a bot (admin only)."""
            if not bot_name:
                view = BotSelectView(self, "logs", "Logs", action_kwargs={"lines": 80})
                embed = discord.Embed(
                    title="📜 Select Bot for Logs",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.blurple(),
                )
                await ctx.send(embed=embed, view=view)
                return
            bot_key = (bot_name or "").strip().lower()
            if bot_key not in self.BOTS:
                await ctx.send(f"❌ Unknown bot: {bot_key}\nUse `!botlist`.")
                return
            try:
                n = int(str(lines).strip())
            except Exception:
                n = 80
            n = max(10, min(n, 400))
            info = self.BOTS[bot_key]
            success, out, err = self._execute_sh_script("botctl.sh", "logs", bot_key, str(n))
            await ctx.send(f"📜 **Logs: {info.get('name', bot_key)}** (last {n})\n{self._codeblock(out or err or '')}"[:1900])
        self.registered_commands.append(("logs", "Show journalctl logs for a bot", True))
        
        @self.bot.command(name="botlist")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botlist(ctx):
            """List all available bots (admin only)"""
            embed = discord.Embed(
                title="📋 Available Bots",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            bot_list = "\n".join([f"• `{key}` - {info['name']}" for key, info in self.BOTS.items()])
            embed.description = bot_list
            embed.set_footer(text="Use !botstatus <botname> to check status")
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name="botstatus")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botstatus(ctx, bot_name: str = None):
            """Check status of a bot or all bots (admin only)"""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if bot_name:
                bot_name = bot_name.lower()
                if bot_name not in self.BOTS:
                    available_bots = ", ".join(self.BOTS.keys())
                    print(f"{Colors.RED}[Command Error] Unknown bot: '{bot_name}'{Colors.RESET}")
                    print(f"{Colors.YELLOW}[Command Error] Available bots: {available_bots}{Colors.RESET}")
                    await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                    return
                
                bot_info = self.BOTS[bot_name]
                service_name = bot_info["service"]
                
                # Log to terminal
                guild_name = ctx.guild.name if ctx.guild else "DM"
                guild_id = ctx.guild.id if ctx.guild else 0
                print(f"{Colors.CYAN}[Command] Checking status of {bot_info['name']} (Service: {service_name}){Colors.RESET}")
                print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
                print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
                
                # First check if service exists
                check_exists_cmd = f"systemctl list-unit-files {service_name} 2>/dev/null | grep -q {service_name} && echo 'exists' || echo 'not_found'"
                exists_success, exists_output, _ = self._execute_ssh_command(check_exists_cmd, timeout=10)
                service_exists = exists_success and "exists" in (exists_output or "").lower()
                
                embed = discord.Embed(
                    title=f"📊 {bot_info['name']} Status",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                
                if not service_exists:
                    embed.add_field(
                        name="Status",
                        value="⚠️ Service not found on remote server",
                        inline=False
                    )
                    embed.add_field(
                        name="Service Name",
                        value=f"`{service_name}`",
                        inline=False
                    )
                    embed.description = "The service file does not exist on the remote server. The bot may need to be set up first."
                    is_active = False
                else:
                    # Use ServiceManager for reliable status check
                    if self.service_manager:
                        exists, state, error = self.service_manager.get_status(service_name, bot_name=bot_name)
                        if exists and state:
                            is_active = state == "active"
                        status_icon = "✅" if is_active else "❌"
                        embed.add_field(
                            name="Status",
                            value=f"{status_icon} {'Running' if is_active else 'Stopped'}",
                            inline=True
                        )
                        
                        # Get PID if running
                        if is_active:
                            pid = self.service_manager.get_pid(service_name)
                            if pid:
                                embed.add_field(name="PID", value=str(pid), inline=True)
                        
                        # Get detailed status
                        detail_success, detail_output, detail_stderr = self.service_manager.get_detailed_status(service_name)
                        if detail_success and detail_output:
                            status_lines = detail_output.split('\n')[-5:]
                            status_text = '\n'.join(status_lines)
                            if len(status_text) > 1000:
                                status_text = status_text[:1000] + "..."
                            embed.add_field(name="Details", value=f"```{status_text}```", inline=False)
                        else:
                            embed.add_field(name="Error", value=f"```{error or 'Status check failed'}```", inline=False)
                            is_active = False
                    else:
                        embed.add_field(name="Error", value="ServiceManager not available", inline=False)
                        is_active = False
                
                # Add "Start Bot" button if bot is not running
                view = None
                if not is_active:
                    view = StartBotView(self, bot_name, bot_info['name'])
                
                # Edit the status message we created earlier
                try:
                    await status_msg.edit(content="", embed=embed, view=view)
                except:
                    await ctx.send(embed=embed, view=view)
            else:
                # Check all bots - send immediate acknowledgment
                status_msg = await ctx.send("🔄 **Checking status of all bots...**\n```\nConnecting to server...\n```")
                
                guild_name = ctx.guild.name if ctx.guild else "DM"
                guild_id = ctx.guild.id if ctx.guild else 0
                print(f"{Colors.CYAN}[Command] Checking status of all bots{Colors.RESET}")
                print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
                print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
                
                embed = discord.Embed(
                    title="📊 All Bots Status",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                
                status_lines = []
                if self.service_manager:
                    for key, bot_info in self.BOTS.items():
                        service_name = bot_info["service"]
                        exists, state, error = self.service_manager.get_status(service_name, bot_name=key)
                        
                        if exists and state:
                            is_active = state == "active"
                            status_icon = "✅" if is_active else "❌"
                            status_text = "Running" if is_active else "Stopped"
                            status_lines.append(f"{status_icon} **{bot_info['name']}** - {status_text}")
                            print(f"{Colors.CYAN}[Status] {bot_info['name']}: {status_text}{Colors.RESET}")
                        else:
                            status_icon = "⚠️"
                            status_lines.append(f"{status_icon} **{bot_info['name']}** - Service not found")
                            print(f"{Colors.YELLOW}[Status] {bot_info['name']}: Service not found on remote server{Colors.RESET}")
                else:
                    status_lines.append("⚠️ ServiceManager not available")
                
                embed.description = "\n".join(status_lines)
                # Edit the status message we created earlier
                try:
                    await status_msg.edit(content="", embed=embed)
                except:
                    await ctx.send(embed=embed)
        
        @self.bot.command(name="botstart")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botstart(ctx, bot_name: str = None):
            """Start a bot (admin only)"""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not bot_name:
                # Show interactive SelectMenu instead of text list
                view = BotSelectView(self, "start", "Start")
                embed = discord.Embed(
                    title="🤖 Select Bot to Start",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.blue()
                )
                await ctx.send(embed=embed, view=view)
                return
            
            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                available_bots = ", ".join(self.BOTS.keys())
                print(f"{Colors.RED}[Command Error] Unknown bot: '{bot_name}'{Colors.RESET}")
                print(f"{Colors.YELLOW}[Command Error] Available bots: {available_bots}{Colors.RESET}")
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return
            
            bot_name_lower = bot_name.lower()
            bot_info = self.BOTS[bot_name_lower]
            service_name = bot_info["service"]
            
            # Log to terminal and Discord
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Starting {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            await self._log_to_discord(f"🟢 **Starting {bot_info['name']}**\nService: `{service_name}`\nRequested by: {ctx.author.mention}")
            
            # Send immediate acknowledgment
            status_msg = await ctx.send(f"🔄 **Starting {bot_info['name']}...**\n```\nConnecting to server...\n```")
            
            # Start service using ServiceManager
            if not self.service_manager:
                await ctx.send("❌ ServiceManager not available")
                return

            # Snapshot before action (so we can confirm PID/state changes)
            before_exists, before_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
            before_pid = self.service_manager.get_pid(service_name)
            
            success, stdout, stderr = self.service_manager.start(service_name, unmask=True, bot_name=bot_name_lower)
            
            if success:
                # Verify service actually started
                is_running, verify_error = self.service_manager.verify_started(service_name, bot_name=bot_name_lower)
                if is_running:
                    after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
                    after_pid = self.service_manager.get_pid(service_name)
                    pid_note = ""
                    if before_pid and after_pid and before_pid != after_pid:
                        pid_note = f" (pid {before_pid} -> {after_pid})"
                    elif before_pid is None and after_pid:
                        pid_note = f" (pid -> {after_pid})"
                    print(f"{Colors.GREEN}[Success] {bot_info['name']} started successfully!{Colors.RESET}")
                    before_state_txt = before_state or "unknown"
                    after_state_txt = after_state or "unknown"
                    before_pid_txt = str(before_pid or 0)
                    after_pid_txt = str(after_pid or 0)
                    await status_msg.edit(
                        content=(
                            f"✅ **{bot_info['name']}** started successfully!{pid_note}\n"
                            f"```\nBefore: state={before_state_txt} pid={before_pid_txt}\nAfter:  state={after_state_txt} pid={after_pid_txt}\n```"
                        )
                    )
                    await self._log_to_discord(
                        f"✅ **{bot_info['name']}** started\nState: `{after_state or 'unknown'}` | PID: `{after_pid or 0}`\nBefore: `{before_state or 'unknown'}` | PID: `{before_pid or 0}`"
                    )
                else:
                    error_msg = verify_error or stderr or stdout or "Unknown error"
                    print(f"{Colors.RED}[Error] Failed to start {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                    await status_msg.edit(content=f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```")
                    await self._log_to_discord(f"❌ **{bot_info['name']}** failed to start:\n```{error_msg[:500]}```")
            else:
                error_msg = stderr or stdout or "Unknown error"
                print(f"{Colors.RED}[Error] Failed to start {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                await status_msg.edit(content=f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```")
                await self._log_to_discord(f"❌ **{bot_info['name']}** failed to start:\n```{error_msg[:500]}```")
        
        @self.bot.command(name="botstop")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botstop(ctx, bot_name: str = None):
            """Stop a bot (admin only)"""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not bot_name:
                # Show interactive SelectMenu
                view = BotSelectView(self, "stop", "Stop")
                embed = discord.Embed(
                    title="🛑 Select Bot to Stop",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed, view=view)
                return
            
            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                available_bots = ", ".join(self.BOTS.keys())
                print(f"{Colors.RED}[Command Error] Unknown bot: '{bot_name}'{Colors.RESET}")
                print(f"{Colors.YELLOW}[Command Error] Available bots: {available_bots}{Colors.RESET}")
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return
            
            bot_name_lower = bot_name.lower()
            bot_info = self.BOTS[bot_name_lower]
            service_name = bot_info["service"]
            script_pattern = bot_info.get("script", bot_name_lower)
            
            # Log to terminal and Discord
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Stopping {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            await self._log_to_discord(f"🔴 **Stopping {bot_info['name']}**\nService: `{service_name}`\nRequested by: {ctx.author.mention}")
            
            # Send immediate acknowledgment
            status_msg = await ctx.send(f"🔄 **Stopping {bot_info['name']}...**\n```\nConnecting to server...\n```")
            
            # Stop service using ServiceManager
            if not self.service_manager:
                await ctx.send("❌ ServiceManager not available")
                return

            before_exists, before_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
            before_pid = self.service_manager.get_pid(service_name)
            
            success, stdout, stderr = self.service_manager.stop(service_name, script_pattern=script_pattern, bot_name=bot_name_lower)
            
            if success:
                after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
                after_pid = self.service_manager.get_pid(service_name)
                pid_note = ""
                if before_pid and not after_pid:
                    pid_note = f" (pid {before_pid} -> 0)"
                print(f"{Colors.GREEN}[Success] {bot_info['name']} stopped successfully!{Colors.RESET}")
                before_state_txt = before_state or "unknown"
                after_state_txt = after_state or "unknown"
                before_pid_txt = str(before_pid or 0)
                after_pid_txt = str(after_pid or 0)
                await status_msg.edit(
                    content=(
                        f"✅ **{bot_info['name']}** stopped successfully!{pid_note}\n"
                        f"```\nBefore: state={before_state_txt} pid={before_pid_txt}\nAfter:  state={after_state_txt} pid={after_pid_txt}\n```"
                    )
                )
                await self._log_to_discord(
                    f"✅ **{bot_info['name']}** stopped\nState: `{after_state or 'unknown'}` | PID: `{after_pid or 0}`\nBefore: `{before_state or 'unknown'}` | PID: `{before_pid or 0}`"
                )
            else:
                error_msg = stderr or stdout or "Unknown error"
                print(f"{Colors.RED}[Error] Failed to stop {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                await status_msg.edit(content=f"❌ Failed to stop {bot_info['name']}:\n```{error_msg[:500]}```")
                await self._log_to_discord(f"❌ **{bot_info['name']}** failed to stop:\n```{error_msg[:500]}```")
        
        @self.bot.command(name="botrestart")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botrestart(ctx, bot_name: str = None):
            """Restart a bot (admin only)"""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not bot_name:
                # Show interactive SelectMenu
                view = BotSelectView(self, "restart", "Restart")
                embed = discord.Embed(
                    title="🔄 Select Bot to Restart",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.orange()
                )
                await ctx.send(embed=embed, view=view)
                return
            
            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                available_bots = ", ".join(self.BOTS.keys())
                print(f"{Colors.RED}[Command Error] Unknown bot: '{bot_name}'{Colors.RESET}")
                print(f"{Colors.YELLOW}[Command Error] Available bots: {available_bots}{Colors.RESET}")
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return
            
            bot_name_lower = bot_name.lower()
            bot_info = self.BOTS[bot_name_lower]
            service_name = bot_info["service"]
            script_pattern = bot_info.get("script", bot_name_lower)
            
            # Log to terminal and Discord
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Restarting {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            await self._log_to_discord(f"🔄 **Restarting {bot_info['name']}**\nService: `{service_name}`\nRequested by: {ctx.author.mention}")
            
            # Send immediate acknowledgment
            status_msg = await ctx.send(f"🔄 **Restarting {bot_info['name']}...**\n```\nConnecting to server...\n```")
            
            # Restart service using ServiceManager
            if not self.service_manager:
                await ctx.send("❌ ServiceManager not available")
                return

            before_exists, before_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
            before_pid = self.service_manager.get_pid(service_name)
            
            success, stdout, stderr = self.service_manager.restart(service_name, script_pattern=script_pattern, bot_name=bot_name_lower)
            
            if success:
                # Verify service actually started
                is_running, verify_error = self.service_manager.verify_started(service_name, bot_name=bot_name_lower)
                if is_running:
                    after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
                    after_pid = self.service_manager.get_pid(service_name)
                    pid_note = ""
                    if before_pid and after_pid and before_pid != after_pid:
                        pid_note = f" (pid {before_pid} -> {after_pid})"
                    elif before_pid and after_pid and before_pid == after_pid:
                        pid_note = f" (pid unchanged: {after_pid})"
                    elif before_pid is None and after_pid:
                        pid_note = f" (pid -> {after_pid})"
                    print(f"{Colors.GREEN}[Success] {bot_info['name']} restarted successfully!{Colors.RESET}")
                    before_state_txt = before_state or "unknown"
                    after_state_txt = after_state or "unknown"
                    before_pid_txt = str(before_pid or 0)
                    after_pid_txt = str(after_pid or 0)
                    await status_msg.edit(
                        content=(
                            f"✅ **{bot_info['name']}** restarted successfully!{pid_note}\n"
                            f"```\nBefore: state={before_state_txt} pid={before_pid_txt}\nAfter:  state={after_state_txt} pid={after_pid_txt}\n```"
                        )
                    )
                    await self._log_to_discord(
                        f"✅ **{bot_info['name']}** restarted{pid_note}\nState: `{after_state or 'unknown'}` | PID: `{after_pid or 0}`\nBefore: `{before_state or 'unknown'}` | PID: `{before_pid or 0}`"
                    )
                else:
                    error_msg = verify_error or stderr or stdout or "Unknown error"
                    print(f"{Colors.YELLOW}[Warning] Restart completed but verification failed for {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                    await status_msg.edit(content=f"⚠️ Restart completed but verification failed for {bot_info['name']}:\n```{error_msg[:500]}```")
                    await self._log_to_discord(f"⚠️ **{bot_info['name']}** restart completed but verification failed:\n```{error_msg[:500]}```")
            else:
                error_msg = stderr or stdout or "Unknown error"
                print(f"{Colors.RED}[Error] Failed to restart {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                await status_msg.edit(content=f"❌ Failed to restart {bot_info['name']}:\n```{error_msg[:500]}```")
                await self._log_to_discord(f"❌ **{bot_info['name']}** failed to restart:\n```{error_msg[:500]}```")
        
        @self.bot.command(name="botupdate")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botupdate(ctx, bot_name: str = None):
            """Update a bot by pulling python-only code from GitHub and restarting it (admin only)."""
            # RS-only: exclude non-RS bots from updates
            if bot_name and not self._is_rs_bot(bot_name):
                await ctx.send(f"❌ `{bot_name}` is not an RS bot. Updates are only available for RS bots.\nUse `!start`, `!stop`, or `!restart` for non-RS bots.")
                return
            
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not bot_name:
                # Show interactive SelectMenu
                view = BotSelectView(self, "update", "Update")
                embed = discord.Embed(
                    title="📦 Select Bot to Update",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.blue()
                )
                await ctx.send(embed=embed, view=view)
                return

            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                available_bots = ", ".join(self.BOTS.keys())
                print(f"{Colors.RED}[Command Error] Unknown bot: '{bot_name}'{Colors.RESET}")
                print(f"{Colors.YELLOW}[Command Error] Available bots: {available_bots}{Colors.RESET}")
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return

            bot_info = self.BOTS[bot_name]
            bot_folder = bot_info["folder"]
            service_name = bot_info.get("service", "")

            # RSAdminBot must update itself via !selfupdate (it restarts the current process).
            if bot_name == "rsadminbot":
                await ctx.invoke(self.bot.get_command("selfupdate"))
                return

            # Log to terminal and Discord
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Updating {bot_info['name']} (Folder: {bot_folder}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            await self._log_to_discord(
                f"📦 **Updating {bot_info['name']} (GitHub python-only)**\nFolder: `{bot_folder}`"
            )

            status_msg = await ctx.send(
                f"📦 **Updating {bot_info['name']} from GitHub (python-only)...**\n"
                "```\nPulling + copying *.py from /home/rsadmin/bots/rsbots-code\n```"
            )
            print(f"{Colors.YELLOW}[Update] Starting GitHub py-only update for {bot_folder}...{Colors.RESET}")

            # Update-progress channel (test server): live systemd state around the sync.
            progress_msg = None
            should_post_progress = not (await self._is_progress_channel(ctx.channel))
            if should_post_progress and self.service_manager and service_name:
                before_exists, before_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name)
                before_pid = self.service_manager.get_pid(service_name)
                progress_msg = await self._post_or_edit_progress(
                    progress_msg,
                    (
                        f"[botupdate] {bot_info['name']} ({bot_name}) START\n"
                        f"Before: {self._format_service_state(before_exists, before_state, before_pid)}\n"
                        f"Requested by: {ctx.author} ({ctx.author.id})"
                    ),
                )
            
            success, stats = self._github_py_only_update(bot_folder)
            if not success:
                error_msg = stats.get("error", "Unknown error")
                print(f"{Colors.RED}[Error] GitHub py-only update failed for {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                await status_msg.edit(content=f"❌ GitHub py-only update failed for {bot_info['name']}:\n```{error_msg[:800]}```")
                await self._log_to_discord(f"❌ **{bot_info['name']}** update failed:\n```{error_msg[:800]}```")
                if should_post_progress and self.service_manager and service_name:
                    after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name)
                    after_pid = self.service_manager.get_pid(service_name)
                    await self._post_or_edit_progress(
                        progress_msg,
                        (
                            f"[botupdate] {bot_info['name']} ({bot_name}) FAILED\n"
                            f"Error: {error_msg[:500]}\n"
                            f"After: {self._format_service_state(after_exists, after_state, after_pid)}"
                        ),
                    )
                return

            old = (stats.get("old") or "").strip()
            new = (stats.get("new") or "").strip()
            py_count = str(stats.get("py_count") or "0").strip()
            changed_count = str(stats.get("changed_count") or "0").strip()
            changed_sample = stats.get("changed_sample") or []

            # Restart (required to pick up new code)
            restart_ok = False
            restart_err = ""
            if not self.service_manager:
                restart_err = "ServiceManager not available"
            elif not service_name:
                restart_err = "Missing service mapping"
            else:
                ok_r, out_r, err_r = self.service_manager.restart(service_name, bot_name=bot_name)
                if not ok_r:
                    restart_err = (err_r or out_r or "restart failed")[:800]
                else:
                    running, verify_err = self.service_manager.verify_started(service_name, bot_name=bot_name)
                    restart_ok = bool(running)
                    if not restart_ok:
                        restart_err = (verify_err or "service did not become active")[:800]

            summary = f"✅ **{bot_info['name']} updated from GitHub (python-only)**\n"
            summary += "```"
            summary += f"\nGit: {old[:12]} -> {new[:12]}"
            summary += f"\nPython copied: {py_count}"
            summary += f"\nChanged .py in folder: {changed_count}"
            summary += f"\nRestart: {'OK' if restart_ok else 'FAILED'}"
            summary += "```"
            if changed_sample:
                summary += "\n**Changed sample (first 30):**\n```"
                summary += "\n".join(str(x) for x in changed_sample[:30])
                summary += "```"
            if not restart_ok and restart_err:
                summary += "\n**Restart error:**\n```"
                summary += restart_err[:1200]
                summary += "```"

            await status_msg.edit(content=summary[:1900])
            await self._log_to_discord(f"✅ **{bot_info['name']}** updated from GitHub (python-only)")

            if should_post_progress and self.service_manager and service_name:
                after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name)
                after_pid = self.service_manager.get_pid(service_name)
                await self._post_or_edit_progress(
                    progress_msg,
                    (
                        f"[botupdate] {bot_info['name']} ({bot_name}) COMPLETE\n"
                        f"Git: {old[:7]} -> {new[:7]}\n"
                        f"After:  {self._format_service_state(after_exists, after_state, after_pid)}\n"
                        f"Python copied: {py_count} | Changed: {changed_count} | Restart: {'OK' if restart_ok else 'FAILED'}"
                    ),
                )

        @self.bot.command(name="selfupdate")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def selfupdate(ctx):
            """Update RSAdminBot from GitHub (python-only) then restart rsadminbot (admin only)."""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return

            status_msg = await ctx.send(
                "📦 **Updating RSAdminBot from GitHub (python-only)...**\n"
                "```\nPulling + copying RSAdminBot/*.py from /home/rsadmin/bots/rsbots-code\n```"
            )
            should_post_progress = not (await self._is_progress_channel(ctx.channel))
            progress_msg = None
            if should_post_progress:
                progress_msg = await self._post_or_edit_progress(
                    None,
                    f"[selfupdate] START\nRequested by: {ctx.author} ({ctx.author.id})",
                )
            success, stats = self._github_py_only_update("RSAdminBot")
            if not success:
                await status_msg.edit(content=f"❌ Failed to update RSAdminBot from GitHub:\n```{stats.get('error','Unknown error')[:800]}```")
                if should_post_progress:
                    await self._post_or_edit_progress(
                        progress_msg,
                        f"[selfupdate] FAILED\nError: {stats.get('error','Unknown error')[:500]}",
                    )
                return

            old = (stats.get("old") or "").strip()
            new = (stats.get("new") or "").strip()
            py_count = str(stats.get("py_count") or "0").strip()
            changed_count = str(stats.get("changed_count") or "0").strip()
            changed_sample = stats.get("changed_sample") or []
            
            # Check if actually updated (old != new)
            if old and new and old == new:
                # No changes - skip restart
                await status_msg.edit(
                    content=(
                        f"✅ **Up to date** (commit `{old[:12]}`).\n"
                        f"No changes detected. No restart needed."
                    )[:1900]
                )
                if should_post_progress:
                    await self._post_or_edit_progress(
                        progress_msg,
                        f"[selfupdate] UP_TO_DATE\nGit: {old[:7]}\nNo changes, no restart needed.",
                    )
                return
            
            # Has changes - proceed with update message and restart
            changed_block = "\n".join(str(x) for x in changed_sample[:15]) if changed_sample else "(none)"
            await status_msg.edit(
                content=(
                    "✅ **RSAdminBot updated from GitHub (python-only).**\n"
                    f"Git: `{old[:12]} -> {new[:12]}`\n"
                    f"Python copied: `{py_count}` | Changed: `{changed_count}`\n"
                    "Restarting RSAdminBot now to apply...\n"
                    "```"
                    f"\nChanged sample:\n{changed_block}"
                    "\n```"
                )[:1900]
            )
            if should_post_progress:
                await self._post_or_edit_progress(
                    progress_msg,
                    f"[selfupdate] UPDATED\nGit: {old[:7]} -> {new[:7]}\nRestarting service to apply.",
                )

            # Restart after sending the message. This will terminate the current process.
            try:
                subprocess.run(["sudo", "systemctl", "restart", "mirror-world-rsadminbot.service"], timeout=10)
            except Exception:
                # If restart fails, we can't reliably report it here because the process may already be terminating.
                pass
            return

        @self.bot.command(name="oraclefilesupdate", aliases=["oraclefilespush", "oraclepush"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def oraclefilesupdate(ctx):
            """Push a python-only snapshot of the live Ubuntu RS bot folders to neo-rs/oraclefiles (admin only)."""
            status_msg = await ctx.send("📦 **OracleFiles sync**\n```\nRunning snapshot export + git push...\n```")
            should_post_progress = not (await self._is_progress_channel(ctx.channel))
            progress_msg = None
            if should_post_progress:
                progress_msg = await self._post_or_edit_progress(
                    None,
                    f"[oraclefiles] MANUAL START",
                )

            ok, stats = self._oraclefiles_sync_once(trigger="manual")
            if not ok:
                err = str(stats.get("error") or "unknown error")
                await status_msg.edit(content=f"❌ OracleFiles sync failed:\n```{err[:1200]}```")
                if should_post_progress:
                    await self._post_or_edit_progress(progress_msg, f"[oraclefiles] MANUAL FAILED\n{err[:1600]}")
                return

            head = str(stats.get("head") or "")[:12]
            pushed = "YES" if str(stats.get("pushed") or "").strip() else "NO"
            no_changes = "YES" if str(stats.get("no_changes") or "").strip() else "NO"
            sample = stats.get("changed_sample") or []

            msg = (
                "✅ **OracleFiles sync complete**\n"
                "```"
                f"\nPushed: {pushed}"
                f"\nNo changes: {no_changes}"
                f"\nHead: {head}"
                "```"
            )
            if sample:
                msg += "\n**Changed files (sample):**\n```" + "\n".join(str(x) for x in sample[:40]) + "```"
            await status_msg.edit(content=msg[:1900])
            if should_post_progress:
                await self._post_or_edit_progress(
                    progress_msg,
                    f"[oraclefiles] MANUAL OK\nPushed: {pushed}\nHead: {head}",
                )

        @self.bot.command(name="pushrsbots", aliases=["pushrsbotsupdate", "pushrsbotspush"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def pushrsbots(ctx):
            """Push python-only changes from live Ubuntu repo to neo-rs/rsbots GitHub (admin only)."""
            status_msg = await ctx.send("📤 **RS Bots Push**\n```\nStaging changes + git push...\n```")
            should_post_progress = not (await self._is_progress_channel(ctx.channel))
            progress_msg = None
            if should_post_progress:
                progress_msg = await self._post_or_edit_progress(
                    None,
                    f"[pushrsbots] START",
                )

            ok, stats = self._rsbots_push_once()
            if not ok:
                err = str(stats.get("error") or "unknown error")
                await status_msg.edit(content=f"❌ RS Bots push failed:\n```{err[:1200]}```")
                if should_post_progress:
                    await self._post_or_edit_progress(progress_msg, f"[pushrsbots] FAILED\n{err[:1600]}")
                return

            head = str(stats.get("head") or "")[:12]
            old_head = str(stats.get("old_head") or "")[:12]
            pushed = "YES" if str(stats.get("pushed") or "").strip() else "NO"
            no_changes = "YES" if str(stats.get("no_changes") or "").strip() else "NO"
            sample = stats.get("changed_sample") or []

            msg = (
                "✅ **RS Bots push complete**\n"
                "```"
                f"\nPushed: {pushed}"
                f"\nNo changes: {no_changes}"
            )
            if old_head and pushed == "YES":
                msg += f"\nGit: {old_head} -> {head}"
            elif head:
                msg += f"\nHead: {head}"
            msg += "```"
            if sample:
                msg += "\n**Changed files (sample):**\n```" + "\n".join(str(x) for x in sample[:40]) + "```"
            await status_msg.edit(content=msg[:1900])
            if should_post_progress:
                await self._post_or_edit_progress(
                    progress_msg,
                    f"[pushrsbots] OK\nPushed: {pushed}\nHead: {head}",
                )
        self.registered_commands.append(("pushrsbots", "Push changes to neo-rs/rsbots GitHub", True))

        @self.bot.command(name="systemcheck")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def systemcheck(ctx):
            """Report where RSAdminBot is running and what execution mode it will use (admin only)."""
            try:
                os_name = os.name
                plat = platform.platform()
                cwd = os.getcwd()
                remote_root = getattr(self, "remote_root", "")
                remote_root_exists = bool(remote_root) and Path(remote_root).is_dir()
                local_exec_cfg = (self.config.get("local_exec") or {}).get("enabled", True)
                local_exec = self._should_use_local_exec()
                server = self.current_server or {}
                host = server.get("host", "")
                user = server.get("user", "")
                key = server.get("key", "")
                key_exists = bool(key) and Path(str(key)).exists()

                lines = [
                    "🧭 **RSAdminBot System Check**",
                    "```",
                    f"os.name: {os_name}",
                    f"platform: {plat}",
                    f"cwd: {cwd}",
                    f"remote_root: {remote_root or '(unset)'}",
                    f"remote_root_exists: {remote_root_exists}",
                    f"local_exec.config.enabled: {local_exec_cfg}",
                    f"local_exec.active: {local_exec}",
                    f"ssh.target: {user}@{host}" if host else "ssh.target: (none)",
                    f"ssh.key: {key or '(none)'}",
                    f"ssh.key.exists: {key_exists}",
                    "```",
                    "",
                    "Decision:",
                    f"- **Mode**: {'Ubuntu local-exec (no SSH key needed)' if local_exec else 'SSH mode (key required if not local)'}",
                ]
                await ctx.send("\n".join(lines)[:1900])
            except Exception as e:
                await ctx.send(f"❌ systemcheck failed: `{str(e)[:400]}`")

        @self.bot.command(name="secretsstatus")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def secretsstatus(ctx, bot_name: str = None):
            """Show which RS bots are missing config.secrets.json or required keys (admin only)."""
            repo_root = self.base_path.parent.resolve()
            bot_groups = self.config.get("bot_groups") or {}
            rs_keys = ["rsadminbot"] + list(bot_groups.get("rs_bots") or [])
            if bot_name:
                key = bot_name.strip().lower()
                if key not in rs_keys:
                    await ctx.send(f"❌ Unknown/unsupported bot for secretsstatus: `{key}`")
                    return
                rs_keys = [key]

            lines = ["🔐 **Secrets status (server-only files)**", "```"]
            for key in rs_keys:
                info = self.BOTS.get(key) or {}
                folder = info.get("folder", key)
                secrets_path = repo_root / folder / "config.secrets.json"
                if not secrets_path.exists():
                    lines.append(f"{key}: MISSING config.secrets.json")
                    continue
                try:
                    data = json.loads(secrets_path.read_text(encoding="utf-8") or "{}")
                except Exception as e:
                    lines.append(f"{key}: INVALID JSON ({str(e)[:60]})")
                    continue
                missing = []
                tok = (data.get("bot_token") or "").strip()
                if (not tok) or is_placeholder_secret(tok):
                    missing.append("bot_token")
                # Bot-specific checks
                if key == "rscheckerbot":
                    inv = data.get("invite_tracking") or {}
                    if not isinstance(inv, dict) or not (inv.get("ghl_api_key") or "").strip():
                        missing.append("invite_tracking.ghl_api_key")
                if missing:
                    lines.append(f"{key}: MISSING {', '.join(missing)}")
                else:
                    lines.append(f"{key}: OK")
            lines.append("```")
            await ctx.send("\n".join(lines)[:1900])

        @self.bot.command(name="rspids")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def rspids(ctx):
            """Print RS bot service state + PID list (admin only)."""
            if not self.service_manager:
                await ctx.send("❌ Service manager not available.")
                return

            bot_groups = self.config.get("bot_groups") or {}
            rs_keys = ["rsadminbot"] + list(bot_groups.get("rs_bots") or [])
            lines = ["🧾 **RS Bots: state + PID**", "```"]
            for key in rs_keys:
                info = self.BOTS.get(key) or {}
                svc = info.get("service", "")
                name = info.get("name", key)
                if not svc:
                    lines.append(f"{key}: missing service mapping")
                    continue
                exists, state, _ = self.service_manager.get_status(svc, bot_name=key)
                pid = self.service_manager.get_pid(svc) or 0
                state_txt = state or "unknown"
                prefix = "OK" if exists and state == "active" else "NO"
                lines.append(f"{prefix} {name} ({key}) state={state_txt} pid={pid}")
            lines.append("```")

            msg = "\n".join(lines)[:1900]
            await ctx.send(msg)
            # Also mirror to the test-server progress channel if configured,
            # but avoid double-posting if the command was run in that same channel.
            try:
                prog = await self._get_update_progress_channel()
                if prog is None:
                    return
                if hasattr(prog, "id") and hasattr(ctx.channel, "id"):
                    if int(getattr(prog, "id")) == int(getattr(ctx.channel, "id")):
                        return
                await prog.send(msg[:1900])
            except Exception:
                return

        @self.bot.command(name="moneyflowcheck", aliases=["moneyflow", "mfc"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def moneyflowcheck(ctx):
            """Run a production-safe health check for the money-flow bots (RSOnboarding + RSCheckerbot).

            - No restarts
            - Validates bot configs/secrets
            - Validates runtime JSON files exist + are parseable + basic schema checks
            - Prints service states
            """
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            status_msg = await ctx.send("🧪 Running money-flow safety check on Ubuntu... (no restarts)")

            remote_root = getattr(self, "remote_root", "/home/rsadmin/bots/mirror-world")
            cmd = """
set +e
cd __REMOTE_ROOT__
echo "=== moneyflowcheck ==="
echo "cwd=$(pwd)"
echo

echo "[1/4] systemd status"
systemctl is-active mirror-world-rsonboarding.service 2>/dev/null || true
systemctl is-active mirror-world-rscheckerbot.service 2>/dev/null || true
echo

echo "[2/4] venv + discord.py sanity"
if [ -x .venv/bin/python ]; then
  echo "venv_python=OK"
else
  echo "venv_python=MISSING"
fi
cat > /tmp/mw_check_discord.py <<'PY'
import discord
print("discord_version", getattr(discord, "__version__", "<none>"))
print("has_Color", hasattr(discord, "Color"))
print("discord_file", getattr(discord, "__file__", None))
PY
.venv/bin/python /tmp/mw_check_discord.py 2>&1 | tail -n 20
echo

echo "[3/4] bot config checks"
.venv/bin/python RSOnboarding/rs_onboarding_bot.py --check-config 2>&1 | tail -n 30
.venv/bin/python RSCheckerbot/main.py --check-config 2>&1 | tail -n 30
echo

echo "[4/4] runtime JSON sanity"
cat > /tmp/mw_check_runtime_json.py <<'PY'
import json
from pathlib import Path

root = Path("/home/rsadmin/bots/mirror-world")

def _load_json(path: Path):
    try:
        if not path.exists():
            return None, "missing"
        if path.stat().st_size == 0:
            return {}, "empty"
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), "ok"
    except Exception as e:
        return None, f"error:{type(e).__name__}:{e}"

def _print(name: str, status: str, extra: str = ""):
    line = f"{name}: {status}"
    if extra:
        line += f" | {extra}"
    print(line)

# RSOnboarding tickets.json
onb_dir = root / "RSOnboarding"
cfg, cfg_status = _load_json(onb_dir / "config.json")
tickets_name = "tickets.json"
if isinstance(cfg, dict):
    tickets_name = str(cfg.get("tickets_file") or "tickets.json")
tickets, t_status = _load_json(onb_dir / tickets_name)
if t_status.startswith("error") or t_status == "missing":
    _print(f"RSOnboarding/{tickets_name}", t_status)
                    else:
    count = len(tickets) if isinstance(tickets, dict) else 0
    bad = 0
    if isinstance(tickets, dict):
        for k, v in list(tickets.items())[:2000]:
            if not isinstance(k, str):
                bad += 1
                continue
            if not isinstance(v, dict):
                bad += 1
                continue
            if not isinstance(v.get("channel_id"), int):
                bad += 1
            if not isinstance(v.get("opened_at"), (int, float)):
                bad += 1
    _print(f"RSOnboarding/{tickets_name}", t_status, f"entries={count}, schema_bad={bad}")

# RSCheckerbot queue/registry/invites
chk_dir = root / "RSCheckerbot"
for filename in ("queue.json", "registry.json", "invites.json"):
    data, s = _load_json(chk_dir / filename)
    if s.startswith("error") or s == "missing":
        _print(f"RSCheckerbot/{filename}", s)
        continue
    entries = 0
    if isinstance(data, dict):
        # invites.json stores under {"invites": {...}}
        if filename == "invites.json" and isinstance(data.get("invites"), dict):
            entries = len(data.get("invites") or {})
            else:
            entries = len(data)
    _print(f"RSCheckerbot/{filename}", s, f"entries={entries}")
PY
.venv/bin/python /tmp/mw_check_runtime_json.py 2>&1 | tail -n 50
"""
            cmd = cmd.replace("__REMOTE_ROOT__", shlex.quote(remote_root))
            ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=60)
            output = (stdout or stderr or "").strip()
            if not output:
                output = "(no output)"
            if len(output) > 1800:
                output = output[-1800:]
                output = "…(truncated)…\n" + output

            header = "✅ moneyflowcheck complete" if ok else "⚠️ moneyflowcheck completed with warnings/errors"
            await status_msg.edit(content=f"{header}\n```{output}```")
        self.registered_commands.append(("moneyflowcheck", "Money-flow safety check (RSOnboarding + RSCheckerbot)", True))

        @self.bot.command(name="codehash")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def codehash(ctx, bot_name: str = ""):
            """Show sha256 hashes of key bot files on Ubuntu for quick 'what code is running' proof."""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return

            bot_key = (bot_name or "").strip().lower()
            if bot_key not in ("rsonboarding", "rscheckerbot", "all"):
                await ctx.send("Usage: `!codehash rsonboarding` | `!codehash rscheckerbot` | `!codehash all`")
                return

            remote_root = getattr(self, "remote_root", "/home/rsadmin/bots/mirror-world")
            targets = []
            if bot_key in ("rsonboarding", "all"):
                targets.extend([
                    "RSOnboarding/rs_onboarding_bot.py",
                    "RSOnboarding/config.json",
                    "RSOnboarding/messages.json",
                ])
            if bot_key in ("rscheckerbot", "all"):
                targets.extend([
                    "RSCheckerbot/main.py",
                    "RSCheckerbot/config.json",
                    "RSCheckerbot/messages.json",
                ])

            quoted_files = " ".join(shlex.quote(p) for p in targets)
            cmd = f"""
set +e
cd {shlex.quote(remote_root)}
echo "=== codehash ({bot_key}) ==="
sha256sum {quoted_files} 2>&1 | sed 's#^#sha256 #'
"""
            ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=30)
            output = (stdout or stderr or "").strip() or "(no output)"
            if len(output) > 1800:
                output = output[-1800:]
                output = "…(truncated)…\n" + output
            await ctx.send(f"```{output}```")
        self.registered_commands.append(("codehash", "Show sha256 hashes of bot files on Ubuntu", True))

        @self.bot.command(name="fileview")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def fileview(ctx, bot_name: str = "", mode: str = ""):
            """Show size + last-modified time for .py and config/message json files (admin only).

            Usage:
              !fileview rsadminbot
              !fileview rscheckerbot
              !fileview UpdateTest
              !fileview rscheckerbot alljson
            """
            target = (bot_name or "").strip()
            if not target:
                await ctx.send("Usage: `!fileview rsadminbot` | `!fileview rscheckerbot` | `!fileview UpdateTest` | `!fileview rscheckerbot alljson`")
                return

            key = target.strip().lower()
            folder = None
            if key in self.BOTS:
                folder = (self.BOTS.get(key) or {}).get("folder")
            elif key in ("updatetest", "update_test"):
                folder = "UpdateTest"
            else:
                folder = target  # allow raw folder name

            repo_root = self.base_path.parent.resolve()
            base = (repo_root / folder).resolve()
            if not base.exists():
                await ctx.send(f"❌ Folder not found on disk: `{base}`")
                return

            include_globs = ["*.py", "config.json", "messages.json", "vouch_config.json"]
            if (mode or "").strip().lower() == "alljson":
                include_globs.append("*.json")

            try:
                mf = rs_generate_manifest(repo_root, bot_folders=[folder], include_globs=include_globs, exclude_globs=list(RS_DEFAULT_EXCLUDE_GLOBS))
            except Exception as e:
                await ctx.send(f"❌ Failed to generate file list: `{str(e)[:200]}`")
                return

            files_map = ((mf.get("files") or {}).get(folder) or {})
            rels = [r for r in files_map.keys() if not str(r).startswith("__")]
            rels.sort()
            if not rels:
                await ctx.send(f"⚠️ No matching files in `{folder}` for include={include_globs}")
                return

            from datetime import timezone
            rows = []
            for rel in rels:
                p = base / rel
                try:
                    st = p.stat()
                    m = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
                    rows.append((rel, st.st_size, m))
                except Exception:
                    rows.append((rel, -1, "stat_error"))

            header = f"=== fileview ({folder}) include={','.join(include_globs)} ==="
            lines = [header, "relpath | bytes | mtime_utc", "-" * 72]
            for rel, size, m in rows:
                s = "?" if size < 0 else str(size)
                lines.append(f"{rel} | {s} | {m}")

            out = "\n".join(lines)
            if len(out) > 1850:
                # Truncate but keep tail so newest filenames still show
                out = "…(truncated)…\n" + out[-1850:]
            await ctx.send(f"```{out}```")
        self.registered_commands.append(("fileview", "Show file sizes + mtimes for bot code/config files", True))

        @self.bot.command(name="deploy")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def deploy(ctx, archive_path: str = None):
            """Deploy a server-side uploaded archive, refresh venv + systemd units, and restart bots (admin only).
            
            Usage:
              !deploy /tmp/mirror-world.tar.gz
            """
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not archive_path:
                await ctx.send("❌ Please provide the archive path on the Ubuntu server.\nExample: `!deploy /tmp/mirror-world.tar.gz`")
                return
            
            status_msg = await ctx.send(f"📦 **Deploying archive...**\n```\nChecking: {archive_path}\n```")
            
            # Validate archive exists on remote
            check_cmd = f"test -f {shlex.quote(archive_path)} && echo OK || echo MISSING"
            ok, stdout, stderr = self._execute_ssh_command(check_cmd, timeout=10)
            if not ok or "OK" not in (stdout or ""):
                await status_msg.edit(content=f"❌ Archive not found on server:\n```{archive_path}```")
                return
            
            # Canonical deploy path: deploy_apply (deploy_unpack + venv + systemd).
            # This avoids "messed up" states where code updates land but the shared venv is missing dependencies.
            await status_msg.edit(content="📦 **Deploying archive...**\n```\nApplying deploy (code + venv + systemd)...\n```")
            success, out, err = self._execute_sh_script("botctl.sh", "deploy_apply", archive_path)
            if not success:
                error_text = (err or out or "Unknown error")[:800]
                await status_msg.edit(content=f"❌ Deploy failed:\n```{error_text}```")
                return
            
            # Restart all bots except RSAdminBot (restarting rsadminbot from within itself is disruptive)
            await status_msg.edit(content="📦 **Deploying archive...**\n```\nRestarting bots (excluding rsadminbot)...\n```")
            restarted = []
            failed = []
            if not self.service_manager:
                await status_msg.edit(content="⚠️ Deploy applied, but ServiceManager is not available to restart bots. Use `bash botctl.sh restart all` on the server.")
                return
            
            for bot_key, bot_info in self.BOTS.items():
                if bot_key == "rsadminbot":
                    continue
                service_name = bot_info.get("service", "")
                ok_restart, stdout_r, stderr_r = self.service_manager.restart(service_name, bot_name=bot_key)
                if ok_restart:
                    restarted.append(bot_key)
                else:
                    failed.append(f"{bot_key}: {(stderr_r or stdout_r or 'Unknown error')[:80]}")
            
            summary_lines = []
            summary_lines.append(f"✅ Deploy applied: {archive_path}")
            summary_lines.append(f"✅ Restarted: {', '.join(restarted) if restarted else 'none'}")
            if failed:
                summary_lines.append("⚠️ Restart failures:")
                summary_lines.extend(f"- {line}" for line in failed[:10])
            summary_lines.append("")
            summary_lines.append("Next: run `!restart` if you want to restart RSAdminBot too.")
            await status_msg.edit(content="\n".join(summary_lines)[:1900])
        
        @self.bot.command(name="ssh")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def ssh_cmd(ctx, *, command: str):
            """Execute SSH command (admin only)"""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            # Log to terminal
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] SSH command requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.YELLOW}[SSH] Executing: {command}{Colors.RESET}")
            await ctx.send(f"🔄 Executing command...")
            
            success, stdout, stderr = self._execute_ssh_command(command, timeout=60)
            
            # Log output to terminal
            if stdout:
                print(f"{Colors.CYAN}[SSH Output] {stdout[:500]}{Colors.RESET}")
            if stderr:
                print(f"{Colors.YELLOW}[SSH Error] {stderr[:500]}{Colors.RESET}")
            print(f"{Colors.GREEN if success else Colors.RED}[SSH] Command {'succeeded' if success else 'failed'}{Colors.RESET}")
            
            embed = discord.Embed(
                title="🔧 SSH Command Result",
                color=discord.Color.green() if success else discord.Color.red(),
                timestamp=datetime.now()
            )
            
            embed.add_field(name="Command", value=f"```{command[:200]}```", inline=False)
            
            output = stdout or stderr or "No output"
            if len(output) > 1000:
                output = output[:1000] + "..."
            embed.add_field(name="Output", value=f"```{output}```", inline=False)
            
            await ctx.send(embed=embed)

        @self.bot.command(name="whereami")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def whereami(ctx):
            """Print runtime environment details (admin only).

            Use this when debugging: it proves which file is executing and what repo/commit is live.
            """
            try:
                import platform
                import sys as _sys

                cwd = os.getcwd()
                file_path = str(Path(__file__).resolve())
                py_exec = _sys.executable
                py_ver = _sys.version.split()[0]
                local_exec = "yes" if self._should_use_local_exec() else "no"
                live_root = str(getattr(self, "remote_root", "") or "")

                code_repo = "/home/rsadmin/bots/rsbots-code"
                live_repo = live_root if live_root else "/home/rsadmin/bots/mirror-world"

                def _git_head(path: str) -> str:
                    try:
                        if not Path(path).is_dir():
                            return "missing"
                        if not (Path(path) / ".git").exists():
                            return "no_git"
                        res = subprocess.run(
                            ["git", "-C", path, "rev-parse", "HEAD"],
                            capture_output=True,
                            text=True,
                            timeout=5,
                        )
                        if res.returncode != 0:
                            return "error"
                        return (res.stdout or "").strip()[:40] or "error"
                    except Exception:
                        return "error"

                head_code = _git_head(code_repo)
                head_live = _git_head(live_repo)

                lines = [
                    "WHEREAMI",
                    f"cwd={cwd}",
                    f"file={file_path}",
                    f"os={platform.system()} {platform.release()}",
                    f"python={py_exec}",
                    f"python_version={py_ver}",
                    f"local_exec={local_exec}",
                    f"live_root={live_repo}",
                    f"rsbots_code_head={head_code}",
                    f"live_tree_head={head_live}",
                ]
                await ctx.send("```text\n" + "\n".join(lines)[:1900] + "\n```")
            except Exception as e:
                await ctx.send(f"❌ whereami failed: {str(e)[:300]}")
        
        # botscan removed: legacy scan/tree compare was removed entirely.
        
        @self.bot.command(name="botinfo")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botinfo(ctx, bot_name: str = None):
            """Get detailed information about a bot (admin only)"""
            # RS-only: exclude non-RS bots from botinfo
            if bot_name and not self._is_rs_bot(bot_name):
                await ctx.send(f"❌ `{bot_name}` is not an RS bot. Bot info is only available for RS bots.")
                return
            
            if not INSPECTOR_AVAILABLE or not self.inspector:
                await ctx.send("❌ Bot inspector not available")
                return
            
            if not bot_name:
                await ctx.send("❓ **Bot Name Required**\nPlease specify which bot to get information about.\nUse `!botlist` to see configured bots.")
                return
            
            try:
                bot_info = self.inspector.get_bot_info(bot_name)
                
                if not bot_info:
                    await ctx.send(f"❌ Bot not found: {bot_name}")
                    return
                
                embed = discord.Embed(
                    title=f"📊 {bot_info.get('name', 'Unknown')} Information",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                
                # Basic info
                size_bytes, size_formatted = self.inspector.get_bot_size(bot_name)
                health = bot_info.get('health', {})
                health_score = health.get('score', 0)
                health_status = health.get('status', 'unknown')
                
                # Health status emoji
                health_emoji = {
                    'excellent': '🟢',
                    'good': '🟡',
                    'fair': '🟠',
                    'poor': '🔴'
                }.get(health_status, '⚪')
                
                embed.add_field(
                    name="📁 Folder",
                    value=f"`{bot_info.get('folder', 'Unknown')}`\n`{bot_info.get('path', 'Unknown')}`",
                    inline=False
                )
                
                # Script info (enhanced)
                script_info = f"`{bot_info.get('script', 'Unknown')}`"
                if bot_info.get('script_exists'):
                    script_info += " ✅"
                else:
                    script_info += " ❌"
                embed.add_field(
                    name="📝 Script",
                    value=script_info,
                    inline=True
                )
                
                embed.add_field(
                    name="⚙️ Service",
                    value=f"`{bot_info.get('service', 'Unknown')}`",
                    inline=True
                )
                
                embed.add_field(
                    name="💾 Size",
                    value=size_formatted,
                    inline=True
                )
                
                # Enhanced file info
                file_info = f"📄 {bot_info.get('file_count', 0)} files"
                python_count = bot_info.get('python_file_count', 0)
                if python_count > 0:
                    file_info += f"\n🐍 {python_count} Python files"
                embed.add_field(
                    name="📊 Files",
                    value=file_info,
                    inline=True
                )
                
                # Health score
                embed.add_field(
                    name=f"{health_emoji} Health",
                    value=f"**{health_score}/100** ({health_status})",
                    inline=True
                )
                
                # Last modified (enhanced)
                last_mod = bot_info.get('last_modified', 'Unknown')
                if last_mod and last_mod != 'Unknown':
                    try:
                        mod_time = datetime.fromisoformat(last_mod.replace('Z', '+00:00'))
                        days_ago = (datetime.now() - mod_time.replace(tzinfo=None)).days
                        last_mod_display = f"{mod_time.strftime('%Y-%m-%d')} ({days_ago}d ago)"
                    except:
                        last_mod_display = last_mod[:19] if len(last_mod) > 19 else last_mod
                else:
                    last_mod_display = 'Unknown'
                
                most_recent_file = bot_info.get('last_modified_file')
                if most_recent_file:
                    last_mod_display += f"\n📝 {most_recent_file['file']}"
                
                embed.add_field(
                    name="🕒 Last Modified",
                    value=last_mod_display,
                    inline=True
                )
                
                # Dependencies info
                deps_info = []
                if bot_info.get('has_requirements'):
                    req_count = bot_info.get('requirements_count', 0)
                    deps_info.append(f"✅ requirements.txt ({req_count} deps)")
                else:
                    deps_info.append("❌ No requirements.txt")
                
                if bot_info.get('has_readme'):
                    deps_info.append("✅ README.md")
                else:
                    deps_info.append("❌ No README")
                
                embed.add_field(
                    name="📦 Dependencies",
                    value="\n".join(deps_info),
                    inline=False
                )
                
                # Config info (enhanced)
                config = bot_info.get('config', {})
                config_status = []
                if bot_info.get('config_valid'):
                    config_status.append("✅ Config valid")
                    if bot_info.get('has_bot_token'):
                        config_status.append("✅ Has bot token")
                    else:
                        config_status.append("⚠️ Missing bot token")
                    
                    config_keys = list(config.keys())[:5]
                    config_preview = ", ".join(config_keys)
                    if len(config.keys()) > 5:
                        config_preview += f" (+{len(config.keys()) - 5} more)"
                    config_status.append(f"Keys: `{config_preview}`")
                else:
                    config_status.append("❌ Config invalid or missing")
                    if bot_info.get('config_error'):
                        config_status.append(f"Error: {bot_info['config_error'][:50]}")
                
                    embed.add_field(
                        name="⚙️ Config",
                    value="\n".join(config_status),
                        inline=False
                    )
                
                await ctx.send(embed=embed)
                
            except Exception as e:
                await ctx.send(f"❌ Error getting bot info: {str(e)[:500]}")
        
        @self.bot.command(name="botconfig")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botconfig(ctx, bot_name: str = None):
            """Get config.json for a bot in user-friendly format (admin only)"""
            # RS-only: exclude non-RS bots from botconfig
            if bot_name and not self._is_rs_bot(bot_name):
                await ctx.send(f"❌ `{bot_name}` is not an RS bot. Bot config is only available for RS bots.")
                return
            
            if not INSPECTOR_AVAILABLE or not self.inspector:
                await ctx.send("❌ Bot inspector not available")
                return
            
            if not bot_name:
                await ctx.send("❓ **Bot Name Required**\nPlease specify which bot's config to view.\nUse `!botlist` to see configured bots.")
                return
            
            try:
                config = self.inspector.get_bot_config(bot_name)
                
                if not config:
                    await ctx.send(f"❌ Bot not found or no config: {bot_name}")
                    return
                
                # Get bot display name
                bot_display_name = bot_name
                if bot_name.lower() in self.BOTS:
                    bot_display_name = self.BOTS[bot_name.lower()]["name"]
                
                embed = discord.Embed(
                    title=f"⚙️ {bot_display_name} Configuration",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                
                # Format config in user-friendly way
                description_parts = []
                
                # Basic settings
                if "bot_token" in config:
                    embed.add_field(
                        name="🔐 Authentication",
                        value="✅ Token configured (hidden)",
                        inline=False
                    )
                
                if "guild_id" in config:
                    guild_id = config["guild_id"]
                    embed.add_field(
                        name="🏠 Server ID",
                        value=f"`{guild_id}`",
                        inline=True
                    )
                
                # Brand/Name
                if "brand_name" in config:
                    embed.add_field(
                        name="🏷️ Brand Name",
                        value=config["brand_name"],
                        inline=True
                    )
                
                # Channel IDs
                channel_fields = []
                if "log_channel_id" in config:
                    channel_fields.append(f"📝 Log Channel: `{config['log_channel_id']}`")
                if "forwarding_logs_channel_id" in config:
                    channel_fields.append(f"📤 Forwarding Logs: `{config['forwarding_logs_channel_id']}`")
                if "whop_logs_channel_id" in config:
                    channel_fields.append(f"💳 Whop Logs: `{config['whop_logs_channel_id']}`")
                if "ssh_commands_channel_id" in config:
                    channel_fields.append(f"🖥️ SSH Commands: `{config['ssh_commands_channel_id']}`")
                
                if channel_fields:
                    embed.add_field(
                        name="📡 Channels",
                        value="\n".join(channel_fields),
                        inline=False
                    )
                
                # Channels array (for forwarder, etc.)
                if "channels" in config and isinstance(config["channels"], list):
                    channels_info = []
                    for i, channel in enumerate(config["channels"][:5], 1):  # Limit to 5
                        source_name = channel.get("source_channel_name", "Unknown")
                        source_id = channel.get("source_channel_id", "N/A")
                        role_id = channel.get("role_mention", {}).get("role_id", "None")
                        channels_info.append(f"**{i}. {source_name}**\n   Source: `{source_id}`\n   Role: `{role_id}`")
                    
                    if len(config["channels"]) > 5:
                        channels_info.append(f"\n*... and {len(config['channels']) - 5} more channel(s)*")
                    
                    embed.add_field(
                        name="🔄 Forwarding Channels",
                        value="\n".join(channels_info),
                        inline=False
                    )
                
                # Invite tracking (for checker bot)
                if "invite_tracking" in config:
                    invite = config["invite_tracking"]
                    invite_info = []
                    if "invite_channel_id" in invite:
                        invite_info.append(f"📨 Invite Channel: `{invite['invite_channel_id']}`")
                    if "fallback_invite" in invite:
                        invite_info.append(f"🔗 Fallback: `{invite['fallback_invite'][:50]}...`")
                    if invite_info:
                        embed.add_field(
                            name="📨 Invite Tracking",
                            value="\n".join(invite_info),
                            inline=False
                        )
                
                # DM Sequence (for checker bot)
                if "dm_sequence" in config:
                    dm = config["dm_sequence"]
                    dm_info = []
                    if "send_spacing_seconds" in dm:
                        dm_info.append(f"⏱️ Spacing: {dm['send_spacing_seconds']}s")
                    if "day_gap_hours" in dm:
                        dm_info.append(f"📅 Day Gap: {dm['day_gap_hours']}h")
                    if dm_info:
                        embed.add_field(
                            name="💬 DM Sequence",
                            value="\n".join(dm_info),
                            inline=True
                        )
                
                # Tickets (for onboarding)
                if "ticket_category_id" in config:
                    embed.add_field(
                        name="🎫 Tickets",
                        value=f"Category: `{config['ticket_category_id']}`",
                        inline=True
                    )
                
                # Success channels (for success bot)
                if "success_channel_ids" in config:
                    count = len(config["success_channel_ids"]) if isinstance(config["success_channel_ids"], list) else 1
                    embed.add_field(
                        name="🏆 Success Channels",
                        value=f"{count} channel(s) configured",
                        inline=True
                    )
                
                # Other important fields
                other_fields = []
                for key, value in config.items():
                    if key not in ["bot_token", "guild_id", "brand_name", "log_channel_id", 
                                  "forwarding_logs_channel_id", "whop_logs_channel_id",
                                  "ssh_commands_channel_id", "channels", "invite_tracking",
                                  "dm_sequence", "ticket_category_id", "success_channel_ids"]:
                        if isinstance(value, (str, int, float, bool)):
                            if len(str(value)) < 100:  # Only show short values
                                other_fields.append(f"**{key.replace('_', ' ').title()}**: `{value}`")
                
                if other_fields:
                    other_text = "\n".join(other_fields[:10])  # Limit to 10
                    if len(other_fields) > 10:
                        other_text += f"\n*... and {len(other_fields) - 10} more field(s)*"
                    embed.add_field(
                        name="⚙️ Other Settings",
                        value=other_text,
                        inline=False
                    )
                
                embed.set_footer(text="Use !botconfig <bot> to view full config")
                
                await ctx.send(embed=embed)
                
            except Exception as e:
                await ctx.send(f"❌ Error getting config: {str(e)[:500]}")

        # Whop tracking commands
        @self.bot.command(name="whopscan")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def whopscan(ctx, limit: int = 2000, days: int = 30):
            """Scan whop logs channel for membership events (admin only)"""
            if not self.whop_tracker:
                await ctx.send("❌ Whop tracker not available")
                return
            
            # Send initial acknowledgment
            status_msg = await ctx.send("🔍 **Scanning whop logs...**\n```\nInitializing scan...\n```")
            
            # Progress callback for real-time updates
            async def progress_update(progress_dict):
                """Update progress message"""
                bar = progress_dict.get("bar", "")
                pct = progress_dict.get("progress_pct", 0)
                scanned = progress_dict.get("messages_scanned", 0)
                total = progress_dict.get("limit", 0)
                events = progress_dict.get("events_found", 0)
                eta = progress_dict.get("eta_seconds", 0)
                rate = progress_dict.get("rate", 0)
                
                eta_str = f"ETA: {eta}s" if eta > 0 else ""
                rate_str = f"({rate:.1f} msg/s)" if rate > 0 else ""
                
                progress_text = f"🔍 **Scanning whop logs...**\n```\n[{bar}] {pct}% ({scanned}/{total}) {eta_str} {rate_str}\nEvents found: {events}\n```"
                
                try:
                    await status_msg.edit(content=progress_text)
                except:
                    pass  # Ignore edit errors
            
            # Log to terminal
            print(f"{Colors.CYAN}[Command] Starting whop scan (limit: {limit}, days: {days}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            
            result = await self.whop_tracker.scan_whop_logs(limit=limit, lookback_days=days, progress_callback=progress_update)
            
            if "error" in result:
                await status_msg.edit(content=f"❌ **Error:** {result['error']}")
                return
            
            # Final result embed
            embed = discord.Embed(title="✅ Whop Logs Scan Complete", color=0x5865F2)
            embed.add_field(name="Messages Scanned", value=result.get("messages_scanned", 0), inline=True)
            embed.add_field(name="Events Found", value=result.get("events_found", 0), inline=True)
            embed.add_field(name="Scan Date", value=result.get("scan_date", "N/A")[:19], inline=False)
            embed.add_field(name="Limit", value=result.get("limit", 0), inline=True)
            embed.add_field(name="Lookback Days", value=result.get("lookback_days", 0), inline=True)
            
            # Terminal output
            print(f"{Colors.GREEN}[WhopScan] Complete: {result.get('messages_scanned', 0)} messages, {result.get('events_found', 0)} events{Colors.RESET}")
            
            await status_msg.edit(content="", embed=embed)
        
        @self.bot.command(name="whopstats")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def whopstats(ctx):
            """Get membership statistics (admin only)"""
            if not self.whop_tracker:
                await ctx.send("❌ Whop tracker not available")
                return
            
            stats = self.whop_tracker.get_membership_stats()
            
            embed = discord.Embed(title="Membership Statistics", color=0x5865F2)
            embed.add_field(name="Total Members", value=stats.get("total_members", 0))
            embed.add_field(name="New Members", value=stats.get("new_members", 0))
            embed.add_field(name="Renewals", value=stats.get("renewals", 0))
            embed.add_field(name="Cancellations", value=stats.get("cancellations", 0))
            embed.add_field(name="Active Memberships", value=stats.get("active_memberships", 0))
            
            if stats.get("avg_duration_days"):
                embed.add_field(name="Avg Duration (days)", value=stats["avg_duration_days"])
            
            # Add note if database is empty
            if stats.get("total_members", 0) == 0:
                embed.add_field(
                    name="ℹ️ Note",
                    value="**Database is empty.** Run `!whopscan` first to scan the whop-logs channel and populate membership data.",
                    inline=False
                )
            
            embed.set_footer(text="Data source: whop_history.db | Run !whopscan to update")
            await ctx.send(embed=embed)
        
        @self.bot.command(name="whophistory")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def whophistory(ctx, discord_id: str = None):
            """Get user's membership history (admin only)"""
            if not self.whop_tracker:
                await ctx.send("❌ Whop tracker not available")
                return
            
            if not discord_id:
                await ctx.send("❓ Please provide a Discord ID: `!whophistory <discord_id>`")
                return
            
            history = self.whop_tracker.get_user_history(discord_id)
            
            if not history.get("events"):
                await ctx.send(f"❌ No membership history found for Discord ID: {discord_id}")
                return
            
            embed = discord.Embed(
                title=f"Membership History - {discord_id}",
                color=0x5865F2
            )
            
            embed.add_field(name="Total Events", value=history.get("total_events", 0))
            embed.add_field(name="Total Periods", value=history.get("total_periods", 0))
            
            # Show recent events
            recent_events = history.get("events", [])[-5:]
            if recent_events:
                events_text = "\n".join([
                    f"**{e.get('event_type', 'unknown')}** - {e.get('timestamp', 'N/A')[:10]}"
                    for e in recent_events
                ])
                embed.add_field(name="Recent Events", value=events_text, inline=False)
            
            # Show timeline
            timeline = history.get("timeline", [])
            if timeline:
                timeline_text = "\n".join([
                    f"**{t.get('status', 'unknown')}** - {t.get('duration_days', 'N/A')} days"
                    for t in timeline[:3]
                ])
                embed.add_field(name="Timeline", value=timeline_text, inline=False)
            
            await ctx.send(embed=embed)
        
        # Bot movement tracking commands
        @self.bot.command(name="botmovements")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botmovements(ctx, bot_name: str = None, limit: int = 50):
            """Show bot's activity log (admin only)"""
            # RS-only: exclude non-RS bots from movement tracking
            if bot_name and not self._is_rs_bot(bot_name):
                await ctx.send(f"❌ `{bot_name}` is not an RS bot. Movement tracking is only available for RS bots.")
                return
            
            if not self.bot_movement_tracker:
                error_embed = MessageHelper.create_error_embed(
                    "Bot Movement Tracker Not Available",
                    "The bot movement tracker module is not loaded or initialized.",
                    "Check bot startup logs for tracker initialization errors"
                )
                await self._send_response(ctx, embed=error_embed)
                return
            
            if not bot_name:
                error_embed = MessageHelper.create_error_embed(
                    "Bot Name Required",
                    "Please specify which bot's activity to view.",
                    error_details=f"Usage: `!botmovements <bot_name> [limit]`\nUse `!botlist` to see available bots"
                )
                await self._send_response(ctx, embed=error_embed)
                return
            
            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                error_embed = MessageHelper.create_error_embed(
                    "Unknown Bot",
                    f"Bot '{bot_name}' not found in bot registry.",
                    error_details=f"Use `!botlist` to see available bots"
                )
                await self._send_response(ctx, embed=error_embed)
                return
            
            try:
                movements = self.bot_movement_tracker.get_bot_movements(bot_name, limit=limit)
                stats = self.bot_movement_tracker.get_bot_stats(bot_name)
                
                embed = MessageHelper.create_info_embed(
                    title=f"{self.BOTS[bot_name]['name']} Activity",
                    description=f"Activity tracking for {self.BOTS[bot_name]['name']}"
                )
                
                total_movements = stats.get("total_movements", 0)
                embed.add_field(
                    name="Total Movements",
                    value=str(total_movements),
                    inline=True
                )
                
                by_action = stats.get("by_action", {})
                if by_action:
                    action_text = "\n".join([f"**{k}**: {v}" for k, v in sorted(by_action.items(), key=lambda x: x[1], reverse=True)])
                    embed.add_field(name="By Action", value=action_text, inline=False)
                
                by_channel = stats.get("by_channel", {})
                if by_channel:
                    channel_text = "\n".join([f"**{k}**: {v}" for k, v in sorted(by_channel.items(), key=lambda x: x[1], reverse=True)[:10]])
                    embed.add_field(name="By Channel", value=channel_text[:1024], inline=False)
                
                if movements:
                    recent = movements[-10:]
                    recent_text = "\n".join([
                        f"**{m.get('action', 'unknown')}** - `{m.get('channel_name', 'unknown')}` - {m.get('timestamp', 'N/A')[:10]}"
                        for m in recent
                    ])
                    embed.add_field(name="Recent Activity", value=recent_text[:1024], inline=False)
                elif total_movements == 0:
                    embed.add_field(
                        name="⚠️ No Activity Recorded",
                        value="No movements have been tracked for this bot yet. Make sure:\n"
                              "• Bot movement tracking is enabled in config\n"
                              "• Bot is posting messages in RS Server\n"
                              "• Bot ID was matched during initialization",
                        inline=False
                    )
                
                last_activity = stats.get("last_activity")
                if last_activity:
                    embed.add_field(name="Last Activity", value=last_activity[:19], inline=True)
                
                await self._send_response(ctx, embed=embed, also_send_to_rs_server=True)
                
            except Exception as e:
                error_embed = MessageHelper.create_error_embed(
                    "Error Getting Bot Movements",
                    str(e)[:500],
                    error_details="Check bot movement tracker logs for details"
                )
                await self._send_response(ctx, embed=error_embed)
        
        # Test server organization command
        @self.bot.command(name="setupmonitoring")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def setupmonitoring(ctx):
            """Initialize test server categories/channels for monitoring (admin only)"""
            if not self.test_server_organizer:
                await ctx.send("❌ Test server organizer not available")
                return
            
            await ctx.send("🔧 Setting up monitoring channels...")
            result = await self.test_server_organizer.setup_monitoring_channels()
            
            if "error" in result:
                await ctx.send(f"❌ Error: {result['error']}")
                return
            
            embed = discord.Embed(
                title="Monitoring Channels Setup",
                color=0x5865F2
            )
            
            embed.add_field(name="Category ID", value=result.get("category_id", "N/A"))
            
            channels = result.get("channels", {})
            if channels:
                channels_text = "\n".join([f"**{k}**: {v}" for k, v in channels.items()])
                embed.add_field(name="Channels Created", value=channels_text[:1024], inline=False)
            
            await ctx.send(embed=embed)
        
        # Run all commands for all bots
        @self.bot.command(name="runallcommands")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def runallcommands(ctx):
            """Run all commands for all bots (admin only)
            
            This command will run essential commands from:
            - Bot Management Commands (botstatus, botinfo, botconfig for each bot)
            - Bot Discovery & Inspection Commands (botscan)
            - Whop Tracking Commands (whopscan, whopstats)
            - Bot Movement Tracking Commands (botmovements for each bot)
            - Sync & Update Commands (sync, botupdate for each bot)
            - Show progress in terminal and Discord
            """
            print(f"\n{Colors.CYAN}{'='*70}{Colors.RESET}")
            print(f"{Colors.BOLD}{Colors.CYAN}[RunAllCommands] Starting comprehensive command execution{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*70}{Colors.RESET}")
            print(f"{Colors.CYAN}[RunAllCommands] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            print(f"{Colors.CYAN}[RunAllCommands] Server: {ctx.guild.name if ctx.guild else 'DM'} (ID: {ctx.guild.id if ctx.guild else 0}){Colors.RESET}\n")
            
            # Send initial status
            status_msg = await ctx.send("🔄 **Running ALL commands...**\n```\nInitializing comprehensive test...\n```")
            
            results = {
                "commands_executed": [],
                "success": [],
                "failed": [],
                "skipped": []
            }
            
            # Get all bot names - filter to RS bots only for advanced commands
            all_bot_names = list(self.BOTS.keys())
            bot_names = [name for name in all_bot_names if self._is_rs_bot(name)]
            non_rs_bots = [name for name in all_bot_names if not self._is_rs_bot(name)]
            
            if non_rs_bots:
                print(f"{Colors.YELLOW}[RunAllCommands] Excluding non-RS bots from advanced commands: {', '.join(non_rs_bots)}{Colors.RESET}")
                print(f"{Colors.YELLOW}[RunAllCommands] Non-RS bots can still use: !status, !start, !stop, !restart{Colors.RESET}\n")
            # Calculate total operations:
            # INITIALIZATION (Phase 0):
            # - ping (1)
            # - status (1)
            # - reload (1)
            # - botlist (1)
            # - setupmonitoring (1)
            # BOT MANAGEMENT (Phase 1):
            # - botstatus (1)
            # - botinfo for each bot (len(bot_names))
            # - botconfig for each bot (len(bot_names))
            # DISCOVERY (Phase 2):
            # - botscan (1)  (covers local+remote when scope=all)
            # WHOP TRACKING (Phase 3):
            # - whopscan (1)
            # - whopstats (1)
            # MOVEMENTS (Phase 4):
            # - botmovements for each bot (len(bot_names))
            # SYNC & UPDATE (Phase 5):
            # - sync (1)
            # - botupdate for each bot (len(bot_names))
            total_operations = 5 + 1 + (len(bot_names) * 2) + 1 + 2 + len(bot_names) + 1 + len(bot_names)
            
            print(f"{Colors.CYAN}[RunAllCommands] Will execute {total_operations} operations across {len(bot_names)} bot(s){Colors.RESET}\n")
            
            operation_count = 0
            phase = 0
            
            # Helper to invoke command directly using the original context
            async def invoke_command_direct(cmd_name, *args, **kwargs):
                """Invoke a command directly by getting it and calling its callback"""
                cmd = self.bot.get_command(cmd_name)
                if not cmd:
                    return False, f"Command {cmd_name} not found"
                try:
                    # Create a context from the original message
                    ctx_copy = await self.bot.get_context(ctx.message)
                    if not ctx_copy:
                        return False, "Could not create context"
                    
                    # Set the command on the context
                    ctx_copy.command = cmd
                    
                    # Invoke the command using ctx.invoke() which properly handles arguments
                    # ctx.invoke(cmd, *args, **kwargs) is the correct way to invoke commands with arguments
                    await ctx_copy.invoke(cmd, *args, **kwargs)
                    return True, None
                except Exception as e:
                    return False, str(e)
            
            # ============================================================
            # PHASE 0: Initialization Commands (Run First!)
            # ============================================================
            phase += 1
            print(f"\n{Colors.CYAN}[RunAllCommands] [Phase {phase}] Initialization Commands{Colors.RESET}")
            
            # 0.1 Run ping (check bot latency)
            print(f"{Colors.CYAN}[RunAllCommands] [0.1] Running !ping (check bot latency)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Initialization: !ping (checking bot latency)...\n```")
            success, error = await invoke_command_direct("ping")
            if success:
                results["commands_executed"].append("ping")
                results["success"].append("ping")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ ping completed{Colors.RESET}")
            else:
                results["failed"].append(f"ping: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ ping failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # 0.2 Run status (check bot readiness)
            print(f"{Colors.CYAN}[RunAllCommands] [0.2] Running !status (check bot readiness)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Initialization: !status (checking bot readiness)...\n```")
            success, error = await invoke_command_direct("status")
            if success:
                results["commands_executed"].append("status")
                results["success"].append("status")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ status completed{Colors.RESET}")
            else:
                results["failed"].append(f"status: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ status failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # 0.3 Run reload (reload configuration)
            print(f"{Colors.CYAN}[RunAllCommands] [0.3] Running !reload (reload configuration)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Initialization: !reload (reloading configuration)...\n```")
            success, error = await invoke_command_direct("reload")
            if success:
                results["commands_executed"].append("reload")
                results["success"].append("reload")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ reload completed{Colors.RESET}")
            else:
                results["failed"].append(f"reload: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ reload failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # 0.4 Run botlist (list all available bots)
            print(f"{Colors.CYAN}[RunAllCommands] [0.5] Running !botlist (list all bots)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Initialization: !botlist (listing all bots)...\n```")
            success, error = await invoke_command_direct("botlist")
            if success:
                results["commands_executed"].append("botlist")
                results["success"].append("botlist")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ botlist completed{Colors.RESET}")
            else:
                results["failed"].append(f"botlist: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ botlist failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # 0.6 Run setupmonitoring (setup test server monitoring channels)
            print(f"{Colors.CYAN}[RunAllCommands] [0.6] Running !setupmonitoring (setup monitoring channels)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Initialization: !setupmonitoring (setting up monitoring channels)...\n```")
            success, error = await invoke_command_direct("setupmonitoring")
            if success:
                results["commands_executed"].append("setupmonitoring")
                results["success"].append("setupmonitoring")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ setupmonitoring completed{Colors.RESET}")
            else:
                results["failed"].append(f"setupmonitoring: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ setupmonitoring failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # ============================================================
            # PHASE 1: Bot Management Commands
            # ============================================================
            phase += 1
            print(f"\n{Colors.CYAN}[RunAllCommands] [Phase {phase}] Bot Management Commands{Colors.RESET}")
            
            # 1.1 Run botstatus for all bots
            print(f"{Colors.CYAN}[RunAllCommands] [1.1] Running !botstatus (all bots)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Bot Management: !botstatus (all bots)...\n```")
            success, error = await invoke_command_direct("botstatus")
            if success:
                results["commands_executed"].append("botstatus (all)")
                results["success"].append("botstatus")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ botstatus completed{Colors.RESET}")
            else:
                results["failed"].append(f"botstatus: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ botstatus failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # 1.2 Run botinfo for each bot
            print(f"{Colors.CYAN}[RunAllCommands] [1.2] Running !botinfo for each bot...{Colors.RESET}")
            for idx, bot_name in enumerate(bot_names, 1):
                await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Bot Management: !botinfo {bot_name} ({idx}/{len(bot_names)})...\n```")
                success, error = await invoke_command_direct("botinfo", bot_name=bot_name)
                if success:
                    results["commands_executed"].append(f"botinfo ({bot_name})")
                    results["success"].append(f"botinfo-{bot_name}")
                    print(f"{Colors.GREEN}[RunAllCommands] ✓ botinfo {bot_name} completed{Colors.RESET}")
                else:
                    results["failed"].append(f"botinfo-{bot_name}: {error[:100] if error else 'Unknown error'}")
                    print(f"{Colors.RED}[RunAllCommands] ✗ botinfo {bot_name} failed: {error}{Colors.RESET}")
                operation_count += 1
                await asyncio.sleep(0.5)
            
            # 1.3 Run botconfig for each bot
            print(f"{Colors.CYAN}[RunAllCommands] [1.3] Running !botconfig for each bot...{Colors.RESET}")
            for idx, bot_name in enumerate(bot_names, 1):
                await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Bot Management: !botconfig {bot_name} ({idx}/{len(bot_names)})...\n```")
                success, error = await invoke_command_direct("botconfig", bot_name=bot_name)
                if success:
                    results["commands_executed"].append(f"botconfig ({bot_name})")
                    results["success"].append(f"botconfig-{bot_name}")
                    print(f"{Colors.GREEN}[RunAllCommands] ✓ botconfig {bot_name} completed{Colors.RESET}")
                else:
                    results["failed"].append(f"botconfig-{bot_name}: {error[:100] if error else 'Unknown error'}")
                    print(f"{Colors.RED}[RunAllCommands] ✗ botconfig {bot_name} failed: {error}{Colors.RESET}")
                operation_count += 1
                await asyncio.sleep(0.5)
            
            await asyncio.sleep(1)  # Delay between phases
            
            # ============================================================
            # PHASE 2: Whop Tracking Commands
            # ============================================================
            phase += 1
            print(f"\n{Colors.CYAN}[RunAllCommands] [Phase {phase}] Whop Tracking Commands{Colors.RESET}")
            
            # 3.1 Run whopscan (default: 2000 messages, 30 days)
            print(f"{Colors.CYAN}[RunAllCommands] [3.1] Running !whopscan (2000, 30)...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Whop: !whopscan (2000 messages, 30 days)...\n```")
            success, error = await invoke_command_direct("whopscan", limit=2000, days=30)
            if success:
                results["commands_executed"].append("whopscan (2000, 30)")
                results["success"].append("whopscan")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ whopscan completed{Colors.RESET}")
            else:
                results["failed"].append(f"whopscan: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ whopscan failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # 3.2 Run whopstats
            print(f"{Colors.CYAN}[RunAllCommands] [3.2] Running !whopstats...{Colors.RESET}")
            await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Whop: !whopstats...\n```")
            success, error = await invoke_command_direct("whopstats")
            if success:
                results["commands_executed"].append("whopstats")
                results["success"].append("whopstats")
                print(f"{Colors.GREEN}[RunAllCommands] ✓ whopstats completed{Colors.RESET}")
            else:
                results["failed"].append(f"whopstats: {error[:100] if error else 'Unknown error'}")
                print(f"{Colors.RED}[RunAllCommands] ✗ whopstats failed: {error}{Colors.RESET}")
            operation_count += 1
            await asyncio.sleep(1)
            
            # Note: whophistory requires a discord_id, so we skip it
            
            # ============================================================
            # PHASE 4: Bot Movement Tracking Commands
            # ============================================================
            phase += 1
            print(f"\n{Colors.CYAN}[RunAllCommands] [Phase {phase}] Bot Movement Tracking Commands{Colors.RESET}")
            
            # 4.1 Run botmovements for each bot
            print(f"{Colors.CYAN}[RunAllCommands] [4.1] Running !botmovements for each bot...{Colors.RESET}")
            for idx, bot_name in enumerate(bot_names, 1):
                await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Movements: !botmovements {bot_name} ({idx}/{len(bot_names)})...\n```")
                success, error = await invoke_command_direct("botmovements", bot_name=bot_name, limit=50)
                if success:
                    results["commands_executed"].append(f"botmovements ({bot_name})")
                    results["success"].append(f"botmovements-{bot_name}")
                    print(f"{Colors.GREEN}[RunAllCommands] ✓ botmovements {bot_name} completed{Colors.RESET}")
                else:
                    results["skipped"].append(f"botmovements-{bot_name}: {error[:100] if error else 'Unknown error'}")
                    print(f"{Colors.YELLOW}[RunAllCommands] ⚠ botmovements {bot_name} skipped: {error}{Colors.RESET}")
                operation_count += 1
                await asyncio.sleep(0.5)
            
            await asyncio.sleep(1)  # Delay between phases
            
            # ============================================================
            # PHASE 4: Update Commands
            # ============================================================
            phase += 1
            print(f"\n{Colors.CYAN}[RunAllCommands] [Phase {phase}] Update Commands{Colors.RESET}")
            
            # 4.1 Run botupdate for each bot
            print(f"{Colors.CYAN}[RunAllCommands] [4.1] Running !botupdate for each bot...{Colors.RESET}")
            for idx, bot_name in enumerate(bot_names, 1):
                await status_msg.edit(content=f"🔄 **Running ALL commands...**\n```\n[Phase {phase}] Sync: !botupdate {bot_name} ({idx}/{len(bot_names)})...\n```")
                success, error = await invoke_command_direct("botupdate", bot_name=bot_name)
                if success:
                    results["commands_executed"].append(f"botupdate ({bot_name})")
                    results["success"].append(f"botupdate-{bot_name}")
                    print(f"{Colors.GREEN}[RunAllCommands] ✓ botupdate {bot_name} completed{Colors.RESET}")
                else:
                    results["failed"].append(f"botupdate-{bot_name}: {error[:100] if error else 'Unknown error'}")
                    print(f"{Colors.RED}[RunAllCommands] ✗ botupdate {bot_name} failed: {error}{Colors.RESET}")
                operation_count += 1
                await asyncio.sleep(0.5)
            
            # Final summary
            print(f"\n{Colors.CYAN}{'='*70}{Colors.RESET}")
            print(f"{Colors.BOLD}{Colors.CYAN}[RunAllCommands] Execution Complete{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*70}{Colors.RESET}")
            print(f"{Colors.GREEN}✓ Successful: {len(results['success'])} command(s){Colors.RESET}")
            if results["failed"]:
                print(f"{Colors.RED}✗ Failed: {len(results['failed'])} command(s){Colors.RESET}")
            if results["skipped"]:
                print(f"{Colors.YELLOW}⚠ Skipped: {len(results['skipped'])} command(s){Colors.RESET}")
            print(f"{Colors.CYAN}Total operations: {operation_count}/{total_operations}{Colors.RESET}\n")
            
            # Send final summary embed
            embed = discord.Embed(
                title="✅ All Commands Execution Complete",
                description=f"Comprehensive test of all bot management, discovery, and tracking commands",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            
            # Summary by category
            initialization = [r for r in results["success"] if any(x in r for x in ["ping", "status", "reload", "botlist", "setupmonitoring"])]
            bot_mgmt = [r for r in results["success"] if any(x in r for x in ["botstatus", "botinfo", "botconfig"])]
            discovery = []
            whop = [r for r in results["success"] if any(x in r for x in ["whopscan", "whopstats"])]
            movements = [r for r in results["success"] if "botmovements" in r]
            sync_update = [r for r in results["success"] if "botupdate" in r]
            
            summary_text = f"✅ **Successful: {len(results['success'])}**\n"
            summary_text += f"  • Initialization: {len(initialization)}/6\n"
            summary_text += f"  • Bot Management: {len(bot_mgmt)}\n"
            summary_text += f"  • Discovery & Inspection: {len(discovery)}\n"
            summary_text += f"  • Whop Tracking: {len(whop)}\n"
            summary_text += f"  • Bot Movements: {len(movements)}\n"
            summary_text += f"  • Sync & Update: {len(sync_update)}\n"
            if results["failed"]:
                summary_text += f"\n❌ **Failed: {len(results['failed'])}**"
            if results["skipped"]:
                summary_text += f"\n⚠️ **Skipped: {len(results['skipped'])}**"
            
            embed.add_field(
                name="📊 Summary by Category",
                value=summary_text,
                inline=False
            )
            
            if results["commands_executed"]:
                commands_list = "\n".join(results["commands_executed"][:25])
                if len(results["commands_executed"]) > 25:
                    commands_list += f"\n... and {len(results['commands_executed']) - 25} more"
                embed.add_field(
                    name="✅ Commands Executed",
                    value=f"```{commands_list}```",
                    inline=False
                )
            
            if results["failed"]:
                failed_list = "\n".join(results["failed"][:10])
                if len(results["failed"]) > 10:
                    failed_list += f"\n... and {len(results['failed']) - 10} more"
                embed.add_field(
                    name="❌ Failed Commands",
                    value=f"```{failed_list}```",
                    inline=False
                )
            
            embed.set_footer(text=f"Total operations: {operation_count}/{total_operations} | Phases: {phase}")
            
            await status_msg.edit(content="", embed=embed)
            
            # Generate and upload report
            print(f"{Colors.CYAN}[RunAllCommands] Generating comprehensive report...{Colors.RESET}")
            await self._generate_and_upload_report(ctx, results, operation_count, total_operations, phase, bot_names)
        
        # Bot diagnostics command
        # Note: !restartadminbot was removed - use !restart instead (canonical command)
    
    async def _generate_and_upload_report(self, ctx, results: Dict, operation_count: int, total_operations: int, phase: int, bot_names: List[str]):
        """Generate comprehensive .md report and upload locally and remotely."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"runallcommands_report_{timestamp}.md"
            local_report_path = self.base_path / report_filename
            
            # Generate report content
            report_lines = []
            report_lines.append("# RunAllCommands Comprehensive Report")
            report_lines.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"**Requested by**: {ctx.author} ({ctx.author.id})")
            report_lines.append(f"**Server**: {ctx.guild.name if ctx.guild else 'DM'} (ID: {ctx.guild.id if ctx.guild else 0})")
            report_lines.append("")
            report_lines.append("---")
            report_lines.append("")
            
            # Summary
            report_lines.append("## Summary")
            report_lines.append(f"- **Total Operations**: {operation_count}/{total_operations}")
            report_lines.append(f"- **Phases Completed**: {phase}")
            report_lines.append(f"- **Successful Commands**: {len(results['success'])}")
            report_lines.append(f"- **Failed Commands**: {len(results['failed'])}")
            report_lines.append(f"- **Skipped Commands**: {len(results['skipped'])}")
            report_lines.append("")
            
            # Commands executed
            if results["commands_executed"]:
                report_lines.append("## Commands Executed")
                for cmd in results["commands_executed"]:
                    report_lines.append(f"- `{cmd}`")
                report_lines.append("")
            
            # Successful commands
            if results["success"]:
                report_lines.append("## ✅ Successful Commands")
                for cmd in results["success"]:
                    report_lines.append(f"- `{cmd}`")
                report_lines.append("")
            
            # Failed commands
            if results["failed"]:
                report_lines.append("## ❌ Failed Commands")
                for cmd in results["failed"]:
                    report_lines.append(f"- `{cmd}`")
                report_lines.append("")
            
            # Skipped commands
            if results["skipped"]:
                report_lines.append("## ⚠️ Skipped Commands")
                for cmd in results["skipped"]:
                    report_lines.append(f"- `{cmd}`")
                report_lines.append("")
            
            # Bot movements summary
            if self.bot_movement_tracker:
                # Clear cache to force reload from files (ensures fresh data)
                self.bot_movement_tracker.movements_cache = {}
                report_lines.append("## Bot Activity Summary")
                for bot_name in bot_names:
                    try:
                        stats = self.bot_movement_tracker.get_bot_stats(bot_name)
                        total = stats.get("total_movements", 0)
                        by_action = stats.get("by_action", {})
                        last_activity = stats.get("last_activity", "Never")
                        
                        report_lines.append(f"### {self.BOTS[bot_name]['name']} ({bot_name})")
                        report_lines.append(f"- **Total Movements**: {total}")
                        if by_action:
                            report_lines.append("- **By Action**:")
                            for action, count in sorted(by_action.items(), key=lambda x: x[1], reverse=True):
                                report_lines.append(f"  - {action}: {count}")
                        report_lines.append(f"- **Last Activity**: {last_activity[:19] if last_activity != 'Never' else 'Never'}")
                        report_lines.append("")
                    except Exception as e:
                        report_lines.append(f"### {self.BOTS[bot_name]['name']} ({bot_name})")
                        report_lines.append(f"- **Error**: {str(e)[:200]}")
                        report_lines.append("")
            
            # Whop stats
            if self.whop_tracker:
                try:
                    whop_stats = self.whop_tracker.get_membership_stats()
                    report_lines.append("## Whop Membership Statistics")
                    report_lines.append(f"- **Total Members**: {whop_stats.get('total_members', 0)}")
                    report_lines.append(f"- **New Members**: {whop_stats.get('new_members', 0)}")
                    report_lines.append(f"- **Renewals**: {whop_stats.get('renewals', 0)}")
                    report_lines.append(f"- **Cancellations**: {whop_stats.get('cancellations', 0)}")
                    report_lines.append(f"- **Active Memberships**: {whop_stats.get('active_memberships', 0)}")
                    if whop_stats.get('avg_duration_days'):
                        report_lines.append(f"- **Avg Duration**: {whop_stats['avg_duration_days']} days")
                    report_lines.append("")
                except Exception as e:
                    report_lines.append("## Whop Membership Statistics")
                    report_lines.append(f"- **Error**: {str(e)[:200]}")
                    report_lines.append("")
            
            # Write local file
            with open(local_report_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            
            print(f"{Colors.GREEN}[RunAllCommands] ✓ Report generated: {report_filename}{Colors.RESET}")
            
            # Upload to remote server
            remote_path = None
            ssh_ok, _ = self._check_ssh_available()
            if ssh_ok and self.current_server:
                try:
                    remote_path = f"/home/{self.current_server.get('user', 'rsadmin')}/mirror-world/RSAdminBot/{report_filename}"
                    
                    # Use SCP to upload
                    ssh_key_path = self.base_path / self.current_server.get("key", "ssh-key-2025-12-15.key")
                    scp_cmd = [
                        "scp",
                        "-i", str(ssh_key_path),
                        "-o", "StrictHostKeyChecking=no",
                        "-P", str(self.current_server.get("port", 22)),
                        str(local_report_path),
                        f"{self.current_server.get('user', 'rsadmin')}@{self.current_server.get('host')}:{remote_path}"
                    ]
                    
                    result = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=30)
                    if result.returncode == 0:
                        print(f"{Colors.GREEN}[RunAllCommands] ✓ Report uploaded to remote: {remote_path}{Colors.RESET}")
                    else:
                        print(f"{Colors.YELLOW}[RunAllCommands] ⚠️  Failed to upload report to remote: {result.stderr[:200]}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.YELLOW}[RunAllCommands] ⚠️  Error uploading report to remote: {str(e)[:200]}{Colors.RESET}")
            
            # Send report file to Discord
            try:
                with open(local_report_path, 'rb') as f:
                    report_file = discord.File(f, filename=report_filename)
                    await ctx.send(
                        f"📄 **Comprehensive Report Generated**\n"
                        f"Local: `{local_report_path}`\n"
                        f"{'Remote: `' + remote_path + '`' if remote_path else 'Remote: Not uploaded (SSH not available)'}",
                        file=report_file
                    )
            except Exception as e:
                print(f"{Colors.YELLOW}[RunAllCommands] ⚠️  Error sending report to Discord: {str(e)[:200]}{Colors.RESET}")
                await ctx.send(f"📄 **Report Generated**\nLocal path: `{local_report_path}`\n(Error sending file: {str(e)[:100]})")
            
        except Exception as e:
            print(f"{Colors.RED}[RunAllCommands] ✗ Error generating report: {str(e)[:500]}{Colors.RESET}")
            await ctx.send(f"⚠️ **Report generation failed**: {str(e)[:500]}")
        
        @self.bot.command(name="delete", aliases=["d"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def delete_channel(ctx, *channel_mentions):
            """Delete channel(s) - use in channel to delete current channel, or mention channels to delete multiple (admin only)"""
            if not ctx.guild:
                await ctx.send("❌ This command can only be used in a server")
                return
            
            # If no mentions, delete current channel
            if not channel_mentions:
                try:
                    await ctx.send("🗑️ **Deleting this channel...**")
                    await asyncio.sleep(1)
                    await ctx.channel.delete()
                except discord.Forbidden:
                    await ctx.send("❌ I don't have permission to delete this channel")
                except Exception as e:
                    await ctx.send(f"❌ Failed to delete channel: {str(e)[:200]}")
                return
            
            # Parse channel mentions
            channels_to_delete = []
            for mention in channel_mentions:
                try:
                    channel = commands.TextChannelConverter().convert(ctx, mention)
                    if channel.guild == ctx.guild:
                        channels_to_delete.append(channel)
                except:
                    try:
                        channel = await commands.TextChannelConverter().convert(ctx, mention)
                        if channel.guild == ctx.guild:
                            channels_to_delete.append(channel)
                    except:
                        pass
            
            if not channels_to_delete:
                await ctx.send("❌ No valid channels found to delete")
                return
            
            # Delete channels
            deleted = []
            failed = []
            for channel in channels_to_delete:
                try:
                    await channel.delete()
                    deleted.append(channel.name)
                except discord.Forbidden:
                    failed.append(f"{channel.name} (no permission)")
                except Exception as e:
                    failed.append(f"{channel.name} ({str(e)[:50]})")
            
            result_msg = "🗑️ **Channel Deletion Complete**\n"
            if deleted:
                result_msg += f"✅ Deleted: {', '.join(deleted)}\n"
            if failed:
                result_msg += f"❌ Failed: {', '.join(failed)}"
            await ctx.send(result_msg)
        self.registered_commands.append(("delete", "Delete channel(s)", True))
        
        @self.bot.command(name="transfer", aliases=["t"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def transfer_channel(ctx, channel_mention: str = None, category_mention: str = None):
            """Transfer a channel to another category - use channel mention and category mention (admin only)"""
            if not ctx.guild:
                await ctx.send("❌ This command can only be used in a server")
                return
            
            # If no arguments, show interactive selector
            if not channel_mention:
                view = ChannelTransferView(self, ctx)
                embed = discord.Embed(
                    title="📦 Transfer Channel to Category",
                    description="Select a channel and category from the dropdowns below:",
                    color=discord.Color.blue()
                )
                await ctx.send(embed=embed, view=view)
                return
            
            # Parse channel
            try:
                channel = await commands.TextChannelConverter().convert(ctx, channel_mention)
                if channel.guild != ctx.guild:
                    await ctx.send("❌ Channel must be in this server")
                    return
            except:
                await ctx.send(f"❌ Channel not found: {channel_mention}")
                return
            
            # Parse category
            if not category_mention:
                await ctx.send("❌ Please provide a category name or mention")
                return
            
            try:
                category = await commands.CategoryChannelConverter().convert(ctx, category_mention)
                if category.guild != ctx.guild:
                    await ctx.send("❌ Category must be in this server")
                    return
            except:
                await ctx.send(f"❌ Category not found: {category_mention}")
                return
            
            # Transfer channel
            try:
                await channel.edit(category=category)
                await ctx.send(f"✅ **Channel Transferred**\n`{channel.name}` → `{category.name}`")
            except discord.Forbidden:
                await ctx.send("❌ I don't have permission to edit this channel")
            except Exception as e:
                await ctx.send(f"❌ Failed to transfer channel: {str(e)[:200]}")
        self.registered_commands.append(("transfer", "Transfer channel to category", True))
        
        @self.bot.command(name="add", aliases=["a"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def add_channel(ctx, channel_mention: str = None, category_mention: str = None):
            """Add a channel to a category - use channel mention and category mention (admin only)"""
            # Same as transfer (transfer = move channel to category, add = same thing)
            await ctx.invoke(self.bot.get_command("transfer"), channel_mention=channel_mention, category_mention=category_mention)
        self.registered_commands.append(("add", "Add channel to category", True))
        
        @self.bot.command(name="botdiagnose")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botdiagnose(ctx, bot_name: str = None):
            """Diagnose bot startup issues (admin only)"""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not bot_name:
                # Show interactive SelectMenu
                view = BotSelectView(self, "diagnose", "Diagnose")
                embed = discord.Embed(
                    title="🔍 Select Bot to Diagnose",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.orange()
                )
                await ctx.send(embed=embed, view=view)
                return
            
            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return
            
            bot_info = self.BOTS[bot_name]
            service_name = bot_info["service"]
            
            # Send immediate acknowledgment
            status_msg = await ctx.send(f"🔍 **Diagnosing {bot_info['name']}...**\n```\nChecking service status...\n```")
            
            # Log to terminal
            print(f"{Colors.CYAN}[Command] Diagnosing {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            
            embed = discord.Embed(
                title=f"🔍 {bot_info['name']} Diagnostics",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            # Check service status
            if self.service_manager:
                exists, state, error = self.service_manager.get_status(service_name, bot_name=bot_name)
                if exists:
                    status_icon = "✅" if state == "active" else "❌"
                    embed.add_field(
                        name="Service Status",
                        value=f"{status_icon} {state.capitalize()}",
                        inline=True
                    )
                    
                    # Get PID if running
                    if state == "active":
                        pid = self.service_manager.get_pid(service_name)
                        if pid:
                            embed.add_field(name="PID", value=str(pid), inline=True)
                    
                    # Get detailed status
                    detail_success, detail_output, detail_stderr = self.service_manager.get_detailed_status(service_name)
                    if detail_success and detail_output:
                        # Extract key info from status
                        status_lines = detail_output.split('\n')
                        key_info = []
                        for line in status_lines:
                            if any(keyword in line.lower() for keyword in ['active', 'loaded', 'main pid', 'status', 'error']):
                                key_info.append(line.strip())
                        
                        if key_info:
                            embed.add_field(
                                name="Service Details",
                                value=f"```\n" + "\n".join(key_info[:10]) + "\n```",
                                inline=False
                            )
                    
                    # Get failure logs if stopped
                    if state != "active":
                        logs = self.service_manager.get_failure_logs(service_name, lines=30)
                        if logs:
                            # Extract error lines
                            error_lines = [line for line in logs.split('\n') if any(keyword in line.lower() for keyword in ['error', 'failed', 'exception', 'traceback', 'failed to'])]
                            if error_lines:
                                error_text = "\n".join(error_lines[-15:])  # Last 15 error lines
                                if len(error_text) > 1000:
                                    error_text = error_text[:1000] + "..."
                                embed.add_field(
                                    name="Recent Errors",
                                    value=f"```\n{error_text}\n```",
                                    inline=False
                                )
                else:
                    embed.add_field(
                        name="Service Status",
                        value="⚠️ Service not found",
                        inline=False
                    )
                    embed.add_field(
                        name="Service Name",
                        value=f"`{service_name}`",
                        inline=False
                    )
                    embed.description = "The service file does not exist. Check if the service was created properly."
            else:
                embed.add_field(name="Error", value="ServiceManager not available", inline=False)
            
            # Check bot folder and script
            bot_folder = bot_info.get("folder", "")
            script_name = bot_info.get("script", "")
            if bot_folder and script_name:
                remote_user = self.current_server.get("user", "rsadmin")
                remote_base = f"/home/{remote_user}/bots/mirror-world"
                script_path = f"{remote_base}/{bot_folder}/{script_name}"
                
                # Check if script exists
                check_script_cmd = f"test -f {script_path} && echo 'exists' || echo 'missing'"
                script_exists_success, script_exists_output, _ = self._execute_ssh_command(check_script_cmd, timeout=10)
                script_exists = script_exists_success and "exists" in (script_exists_output or "").lower()
                
                embed.add_field(
                    name="Script File",
                    value=f"{'✅' if script_exists else '❌'} `{script_path}`",
                    inline=False
                )
                
                # Check folder
                check_folder_cmd = f"test -d {remote_base}/{bot_folder} && echo 'exists' || echo 'missing'"
                folder_exists_success, folder_exists_output, _ = self._execute_ssh_command(check_folder_cmd, timeout=10)
                folder_exists = folder_exists_success and "exists" in (folder_exists_output or "").lower()
                
                embed.add_field(
                    name="Bot Folder",
                    value=f"{'✅' if folder_exists else '❌'} `{remote_base}/{bot_folder}`",
                    inline=False
                )
            
            await status_msg.edit(content="", embed=embed)
        
        # Log command registration after all commands are set up
        prefix = self.bot.command_prefix
        if isinstance(prefix, str):
            prefix_str = prefix
        elif callable(prefix):
            prefix_str = "callable"
        else:
            prefix_str = str(prefix)
        
        command_names = sorted([cmd.name for cmd in self.bot.commands if hasattr(cmd, 'name')])
        command_count = len(command_names)
        command_list_str = ", ".join(command_names[:30])  # Show first 30
        if command_count > 30:
            command_list_str += f", ... (+{command_count - 30} more)"
        
        print(f"{Colors.GREEN}[Startup] Command prefix: {prefix_str}{Colors.RESET}")
        print(f"{Colors.GREEN}[Startup] Registered {command_count} commands: {command_list_str}{Colors.RESET}")
    
    def _start_whop_scanning_task(self):
        """Start periodic whop scanning task"""
        from discord.ext import tasks
        
        @tasks.loop(hours=self.config.get("whop_scan_interval_hours", 24))
        async def periodic_whop_scan():
            if self.whop_tracker:
                try:
                    print(f"{Colors.CYAN}[WhopTracker] Starting periodic scan...{Colors.RESET}")
                    result = await self.whop_tracker.scan_whop_logs(limit=2000, lookback_days=1)
                    print(f"{Colors.GREEN}[WhopTracker] Scan complete: {result.get('events_found', 0)} events found{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.RED}[WhopTracker] Periodic scan error: {e}{Colors.RESET}")
        
        periodic_whop_scan.start()
        print(f"{Colors.GREEN}[WhopTracker] Periodic scanning task started (every {self.config.get('whop_scan_interval_hours', 24)} hours){Colors.RESET}")
    
    async def start(self):
        """Start the bot"""
        bot_token = self.config.get("bot_token", "").strip()
        if not bot_token:
            print(f"{Colors.RED}[Bot] ERROR: bot_token is required in config.secrets.json (server-only){Colors.RESET}")
            return
        
        try:
            await self.bot.start(bot_token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Bot] Shutting down...{Colors.RESET}")
            await self.bot.close()


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--check-config", action="store_true", help="Validate config + secrets and exit (no Discord connection).")
    args = parser.parse_args()

    if args.check_config:
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors: List[str] = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")

        ssh = cfg.get("ssh_server") or {}
        if ssh:
            for k in ("host", "user", "key"):
                if not (ssh.get(k) or "").strip():
                    errors.append(f"ssh_server.{k} missing in config.json")

        if errors:
            print(f"{Colors.RED}[ConfigCheck] FAILED{Colors.RESET}")
            for e in errors:
                print(f"- {e}")
            return

        print(f"{Colors.GREEN}[ConfigCheck] OK{Colors.RESET}")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        if ssh:
            print(f"- ssh_server.host: {ssh.get('host')}")
            print(f"- ssh_server.user: {ssh.get('user')}")
            print(f"- ssh_server.key: {ssh.get('key')}")
        return

    bot = RSAdminBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[Bot] Stopped{Colors.RESET}")


if __name__ == '__main__':
    main()

