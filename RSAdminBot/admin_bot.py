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
import re
from collections import deque
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Callable, Awaitable
from datetime import datetime, timezone, timedelta
from contextlib import suppress

import aiohttp
from aiohttp import web

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret
from mirror_world_config import _deep_merge_dict
from mirror_world_config import load_oracle_servers, pick_oracle_server, resolve_oracle_ssh_key_path
from shared.whop_webhook_utils import verify_standard_webhook

from rsbots_manifest import compare_manifests as rs_compare_manifests
from rsbots_manifest import generate_manifest as rs_generate_manifest
from rsbots_manifest import DEFAULT_EXCLUDE_GLOBS as RS_DEFAULT_EXCLUDE_GLOBS

import discord
from discord.ext import commands
from discord import ui
from discord import app_commands

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
    def create_success_embed(
        title: str,
        message: str,
        details: str = None,
        fields: List[Dict] = None,
        footer: str = None,
    ) -> discord.Embed:
        """Create a success embed with consistent formatting."""
        embed = MessageHelper.create_status_embed(
            title=f"✅ {title}",
            description=message,
            color=discord.Color.green(),
            fields=fields,
            footer=footer,
        )
        if details:
            embed.add_field(name="Details", value=f"```{details[:1000]}```", inline=False)
        return embed
    
    @staticmethod
    def create_error_embed(
        title: str,
        message: str,
        error_details: str = None,
        fields: List[Dict] = None,
        footer: str = None,
    ) -> discord.Embed:
        """Create an error embed with consistent formatting."""
        embed = MessageHelper.create_status_embed(
            title=f"❌ {title}",
            description=message,
            color=discord.Color.red(),
            fields=fields,
            footer=footer,
        )
        if error_details:
            embed.add_field(name="Error", value=f"```{error_details[:1000]}```", inline=False)
        return embed
    
    @staticmethod
    def create_warning_embed(
        title: str,
        message: str,
        details: str = None,
        fields: List[Dict] = None,
        footer: str = None,
    ) -> discord.Embed:
        """Create a warning embed with consistent formatting."""
        embed = MessageHelper.create_status_embed(
            title=f"⚠️ {title}",
            description=message,
            color=discord.Color.orange(),
            fields=fields,
            footer=footer,
        )
        if details:
            embed.add_field(name="Details", value=f"```{details[:1000]}```", inline=False)
        return embed
    
    @staticmethod
    def create_info_embed(
        title: str,
        message: str = "",
        fields: List[Dict] = None,
        footer: str = None,
        *,
        description: str = None,
    ) -> discord.Embed:
        """Create an info embed with consistent formatting.
        
        Args:
            title: Embed title
            message: Main message/description (can be empty)
            fields: Optional fields list
            footer: Optional footer text
            description: Alias for message parameter (for compatibility)
        """
        # Support both message and description for compatibility
        desc = description if description is not None else message
        return MessageHelper.create_status_embed(
            title=title,
            description=desc,
            color=discord.Color.blue(),
            fields=fields,
            footer=footer,
        )


class CommandLogger:
    """Centralized logging service for RSAdminBot.
    
    Handles structured JSON logging to files and Discord embed generation.
    All logs are written to remote server JSON files and formatted as Discord embeds.
    """
    
    def __init__(self, admin_bot_instance):
        """Initialize CommandLogger.
        
        Args:
            admin_bot_instance: RSAdminBot instance for accessing config and methods
        """
        self.admin_bot = admin_bot_instance
        self.log_config = admin_bot_instance.config.get("logging", {})
        self.file_logging_enabled = self.log_config.get("file_logging", {}).get("enabled", True)
        self.log_base_path = self.log_config.get("file_logging", {}).get("base_path", "/home/rsadmin/bots/logs/rsadminbot")
        self.log_ssh_commands = self.log_config.get("log_ssh_commands", True)
        self.log_config_validation_enabled = self.log_config.get("log_config_validation", True)
        self.log_all_commands = self.log_config.get("log_all_commands", True)
        self._current_command_context = None  # Track which command is currently executing
    
    def _get_context_from_ctx(self, ctx) -> Dict[str, Any]:
        """Extract context information from Discord command context.
        
        Args:
            ctx: Discord command context
            
        Returns:
            Dictionary with context information
        """
        try:
            return {
                "user_id": ctx.author.id if ctx.author else None,
                "user_name": str(ctx.author) if ctx.author else None,
                "guild_id": ctx.guild.id if ctx.guild else None,
                "guild_name": ctx.guild.name if ctx.guild else None,
                "channel_id": ctx.channel.id if ctx.channel else None,
                "channel_name": ctx.channel.name if hasattr(ctx.channel, 'name') else None,
            }
        except Exception:
            return {}
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _create_base_log_entry(self, log_type: str, level: str, **kwargs) -> Dict[str, Any]:
        """Create base log entry structure.
        
        Args:
            log_type: Type of log (command, ssh_command, config, system)
            level: Log level (info, success, error, warning)
            **kwargs: Additional fields to include
            
        Returns:
            Dictionary with log entry
        """
        entry = {
            "timestamp": self._get_timestamp(),
            "type": log_type,
            "level": level,
        }
        entry.update(kwargs)
        return entry
    
    def write_log_file(self, log_entry: Dict[str, Any]):
        """Write log entry to JSON file on remote server.
        
        Args:
            log_entry: Dictionary with log entry data
        """
        if not self.file_logging_enabled:
            return
        
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            log_file = f"{self.log_base_path}/rsadminbot_{date_str}.jsonl"
            
            # Create JSON line (compact format, one object per line)
            json_line = json.dumps(log_entry, ensure_ascii=False, separators=(',', ':'))
            
            # Write to remote file (avoid logger recursion by disabling ssh-command logging here)
            cmd = (
                f"mkdir -p {shlex.quote(self.log_base_path)} && "
                f"printf %s\\\\n {shlex.quote(json_line)} >> {shlex.quote(log_file)}"
            )
            self.admin_bot._execute_ssh_command(cmd, timeout=5, log_it=False)
        except Exception as e:
            # Don't fail if logging fails - just print error
            print(f"{Colors.YELLOW}[Logger] Failed to write log file: {e}{Colors.RESET}")
    
    def log_command(self, ctx, command_name: str, status: str, details: Dict[str, Any] = None) -> Dict[str, Any]:
        """Log command execution.
        
        Args:
            ctx: Discord command context
            command_name: Name of the command (e.g., "start", "stop")
            status: Status (pending, success, error)
            details: Additional details about the command execution
            
        Returns:
            Log entry dictionary
        """
        if not self.log_all_commands:
            return {}
        
        context = self._get_context_from_ctx(ctx)
        level = "info"
        if status == "success":
            level = "success"
        elif status == "error":
            level = "error"
        
        log_entry = self._create_base_log_entry(
            log_type="command",
            level=level,
            command=f"!{command_name}",
            status=status,
            **context
        )
        
        if details:
            log_entry["details"] = details
        
        # Write to file
        self.write_log_file(log_entry)
        
        # Set current command context for SSH command association
        self._current_command_context = {
            "command": command_name,
            "context": context,
            "log_entry": log_entry
        }
        
        return log_entry
    
    def log_ssh_command(self, command: str, success: Optional[bool], stdout: Optional[str] = None, 
                       stderr: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Log SSH/shell command execution.
        
        Args:
            command: Command string that was executed
            success: Whether command succeeded (None if not executed yet)
            stdout: Command stdout (truncated if too long)
            stderr: Command stderr (truncated if too long)
            context: Optional context from calling command
            
        Returns:
            Log entry dictionary
        """
        if not self.log_ssh_commands:
            return {}
        
        # Use current command context if available
        if context is None and self._current_command_context:
            context = self._current_command_context.get("context", {})
            command_name = self._current_command_context.get("command", "unknown")
        else:
            command_name = context.get("command", "unknown") if context else "unknown"
        
        level = "info"
        if success is True:
            level = "success"
        elif success is False:
            level = "error"
        
        # Truncate stdout/stderr for storage (keep last 2000 chars)
        stdout_truncated = None
        stderr_truncated = None
        if stdout:
            stdout_truncated = stdout[-2000:] if len(stdout) > 2000 else stdout
        if stderr:
            stderr_truncated = stderr[-2000:] if len(stderr) > 2000 else stderr
        
        log_entry = self._create_base_log_entry(
            log_type="ssh_command",
            level=level,
            ssh_command=command[:500],  # Truncate command to 500 chars
            success=success,
            **context if context else {}
        )
        
        if stdout_truncated:
            log_entry["ssh_stdout"] = stdout_truncated
        if stderr_truncated:
            log_entry["ssh_stderr"] = stderr_truncated
        if command_name != "unknown":
            log_entry["triggered_by_command"] = command_name
        
        # Write to file
        self.write_log_file(log_entry)
        
        return log_entry
    
    def log_config_validation(self, check_name: str, status: str, message: str, details: Dict[str, Any] = None) -> Dict[str, Any]:
        """Log configuration validation event.
        
        Args:
            check_name: Name of the config check (e.g., "ssh_config", "ssh_key")
            status: Status (valid, invalid, missing, warning)
            message: Human-readable message
            details: Additional details
            
        Returns:
            Log entry dictionary
        """
        if not self.log_config_validation_enabled:
            return {}
        
        level = "info"
        if status in ("invalid", "missing"):
            level = "error"
        elif status == "warning":
            level = "warning"
        elif status == "valid":
            level = "success"
        
        log_entry = self._create_base_log_entry(
            log_type="config",
            level=level,
            check_name=check_name,
            status=status,
            message=message
        )
        
        if details:
            log_entry["config_check"] = details
        
        # Write to file
        self.write_log_file(log_entry)
        
        return log_entry
    
    def create_embed(self, log_entry: Dict[str, Any], reply_context: Optional[Dict[str, Any]] = None) -> discord.Embed:
        """Convert log entry to Discord embed.
        
        Args:
            log_entry: Log entry dictionary
            reply_context: Optional context for reply footer (user info)
            
        Returns:
            Discord embed
        """
        log_type = log_entry.get("type", "system")
        level = log_entry.get("level", "info")
        timestamp = log_entry.get("timestamp", self._get_timestamp())
        
        # Determine color and emoji based on level
        if level == "success":
            color = discord.Color.green()
            emoji = "✅"
        elif level == "error":
            color = discord.Color.red()
            emoji = "❌"
        elif level == "warning":
            color = discord.Color.orange()
            emoji = "⚠️"
        else:
            color = discord.Color.blue()
            emoji = "ℹ️"
        
        # Build embed based on log type
        embed = discord.Embed(color=color, timestamp=datetime.now())
        
        # Command logs
        if log_type == "command":
            command = log_entry.get("command", "unknown")
            status = log_entry.get("status", "unknown")
            details = log_entry.get("details", {})
            bot_name = details.get("bot_name") or log_entry.get("bot_name")
            
            if status == "success":
                embed.title = f"{emoji} Command Succeeded"
            elif status == "error":
                embed.title = f"{emoji} Command Failed"
            else:
                embed.title = f"{emoji} Command Executed"
            
            embed.description = f"Command: `{command}`"
            
            if bot_name:
                embed.add_field(name="Bot", value=bot_name, inline=True)
            
            if status == "success" and details:
                if "after_state" in details:
                    state_change = ""
                    if "before_state" in details and details["before_state"] != details["after_state"]:
                        state_change = f"{details['before_state']} → {details['after_state']}"
                    else:
                        state_change = details["after_state"]
                    embed.add_field(name="Status", value=state_change, inline=True)
                
                if "pid" in details or "after_pid" in details:
                    pid = details.get("pid") or details.get("after_pid")
                    embed.add_field(name="PID", value=str(pid), inline=True)
                
                if "service" in details:
                    embed.add_field(name="Service", value=details["service"], inline=False)
            
            elif status == "error" and details:
                error_msg = details.get("error") or details.get("error_msg") or "Unknown error"
                embed.add_field(name="Error", value=f"```{error_msg[:500]}```", inline=False)
        
        # SSH command logs (usually don't create embeds for these unless error)
        elif log_type == "ssh_command" and level == "error":
            command = log_entry.get("ssh_command", "unknown")[:100]
            embed.title = f"{emoji} SSH Command Failed"
            embed.description = f"Command: `{command}...`"
            if log_entry.get("ssh_stderr"):
                embed.add_field(name="Error", value=f"```{log_entry['ssh_stderr'][:500]}```", inline=False)
        
        # Config validation logs
        elif log_type == "config":
            check_name = log_entry.get("check_name", "unknown")
            status = log_entry.get("status", "unknown")
            message = log_entry.get("message", "")
            
            embed.title = f"{emoji} Configuration Check"
            embed.description = message
            embed.add_field(name="Check", value=check_name, inline=True)
            embed.add_field(name="Status", value=status, inline=True)
        
        # System logs
        else:
            message = log_entry.get("message", log_entry.get("description", ""))
            embed.title = f"{emoji} System Event"
            embed.description = message
        
        # Add footer with user info if available
        footer_parts = []
        if reply_context:
            user_name = reply_context.get("user_name")
            if user_name:
                footer_parts.append(f"Triggered by {user_name}")
        if not footer_parts:
            footer_parts.append("RSAdminBot")
        embed.set_footer(text=" • ".join(footer_parts))
        
        return embed
    
    def clear_command_context(self):
        """Clear current command context (called after command completes)."""
        self._current_command_context = None

# RSAdminBot is self-contained - no external dependencies
# All functionality is within RSAdminBot folder

import importlib.util as _importlib_util

# Avoid import-time side effects. We only check module availability here; actual imports are lazy.
INSPECTOR_AVAILABLE = _importlib_util.find_spec("bot_inspector") is not None
ORGANIZER_AVAILABLE = _importlib_util.find_spec("test_server_organizer") is not None


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
    
    
    def get_detailed_status(self, service_name: str) -> Tuple[bool, str, Optional[str]]:
        """Get detailed service status output.
        
        Returns:
            (success, output, error_msg) where output is always a string (empty if error)
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
            return False, "", "Could not infer bot_name from service name"
        success, stdout, stderr = self._execute_script("botctl.sh", "details", bot_name)
        return success, (stdout or ""), stderr
    
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
    
    def get_failure_logs(self, service_name: str, lines: int = 50) -> str:
        """Get recent journalctl logs for service failures.
        
        Args:
            service_name: Systemd service name
            lines: Number of log lines to retrieve
        
        Returns:
            Log output as string (empty string if error)
        """
        bot_name = None
        if service_name:
            svc = service_name
            if svc.endswith(".service"):
                svc = svc[:-8]
            if svc.startswith("mirror-world-"):
                bot_name = svc[len("mirror-world-"):]
        if not bot_name:
            return ""
        success, stdout, _ = self._execute_script("botctl.sh", "logs", bot_name, str(lines))
        if success and stdout:
            return stdout
        return ""
    
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
        self.selected_channel_id = None
        self.selected_category_id = None
        
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
        else:
            self.channel_select = None
        
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
        else:
            self.category_select = None
    
    async def on_channel_select(self, interaction: discord.Interaction):
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return
        
        try:
            channel_id = int(self.channel_select.values[0])
            channel = interaction.guild.get_channel(channel_id)
            if not channel:
                await interaction.response.send_message("❌ Channel not found", ephemeral=True)
                return
            
            self.selected_channel_id = channel_id
            
            # Check if both are selected now - if category was already selected, perform transfer
            if self.selected_channel_id and self.selected_category_id:
                # Both selected - defer and transfer
                await interaction.response.defer(ephemeral=True)
                await self._perform_transfer(interaction)
            else:
                await interaction.response.send_message(f"✅ Channel selected: `{channel.name}`. Now select a category.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)[:200]}", ephemeral=True)
    
    async def on_category_select(self, interaction: discord.Interaction):
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return
        
        try:
            category_id = int(self.category_select.values[0])
            category = interaction.guild.get_channel(category_id)
            if not category or not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message("❌ Category not found", ephemeral=True)
                return
            
            self.selected_category_id = category_id
            
            # Check if both are selected now - if channel was already selected, perform transfer
            if self.selected_channel_id and self.selected_category_id:
                await interaction.response.defer(ephemeral=True)
                await self._perform_transfer(interaction)
            else:
                await interaction.response.send_message(f"✅ Category selected: `{category.name}`. Now select a channel.", ephemeral=True)
        except Exception as e:
            try:
                await interaction.response.send_message(f"❌ Error: {str(e)[:200]}", ephemeral=True)
            except:
                pass
    
    async def _perform_transfer(self, interaction: discord.Interaction):
        """Perform the channel transfer once both channel and category are selected."""
        try:
            channel = interaction.guild.get_channel(self.selected_channel_id)
            category = interaction.guild.get_channel(self.selected_category_id)
            
            if not channel or not isinstance(channel, discord.TextChannel):
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Channel not found", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Channel not found", ephemeral=True)
                return
            
            if not category or not isinstance(category, discord.CategoryChannel):
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ Category not found", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Category not found", ephemeral=True)
                return
            
            await channel.edit(category=category, reason=f"Transferred by {interaction.user} via RSAdminBot")
            
            success_msg = f"✅ **Channel Transferred**\n`{channel.name}` → `{category.name}`"
            await self.admin_bot._interaction_reply(interaction, content=success_msg, ephemeral=True)
        except discord.Forbidden:
            msg = "❌ I don't have permission to edit this channel"
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except discord.HTTPException as e:
            msg = f"❌ Failed to transfer channel: {str(e)[:200]}"
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)
        except Exception as e:
            msg = f"❌ Error: {str(e)[:200]}"
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)

class BotSelectView(ui.View):
    """View with SelectMenu for bot selection"""
    
    def __init__(
        self,
        admin_bot_instance,
        action: str,
        action_display: str,
        action_kwargs: Optional[Dict[str, Any]] = None,
        bot_keys: Optional[List[str]] = None,
    ):
        """
        Args:
            admin_bot_instance: RSAdminBot instance
            action: Action name ('start', 'stop', 'restart', 'status', 'update', 'sync', 'details', 'logs', 'info', 'config', 'secrets', 'commands', 'fileview', 'diagnose')
            action_display: Display name for action ('Start', 'Stop', etc.)
            action_kwargs: Optional extra params for handlers (e.g. logs lines)
            bot_keys: Optional subset of bot keys to show in the dropdown
        """
        super().__init__(timeout=300)  # 5 minute timeout
        self.admin_bot = admin_bot_instance
        self.action = action
        self.action_display = action_display
        self.action_kwargs = action_kwargs or {}
        
        # Create SelectMenu with selected bots (or all) + optional "All Bots" for service control actions
        keys = list(bot_keys) if bot_keys else list(admin_bot_instance.BOTS.keys())
        options: List[discord.SelectOption] = []
        for bot_key in keys:
            bot_info = admin_bot_instance.BOTS.get(bot_key)
            if not bot_info:
                continue
            options.append(
                discord.SelectOption(
                    label=bot_info.get("name", bot_key),
                    value=bot_key,
                    description=f"{action_display} {bot_info.get('name', bot_key)}"
                )
            )
        
        # Add "All Bots" option for service control actions
        if self.action in ["start", "stop", "restart"] and not bot_keys:
            options.insert(0, discord.SelectOption(
                label="🔄 All Bots",
                value="all_bots",
                description=f"{action_display} all bots"
            ))

        # Add group-scoped "All ..." options for update action (python-only).
        if self.action in ["update"]:
            groups = set()
            for k in keys:
                g = admin_bot_instance._get_bot_group(str(k).strip().lower()) or ""
                if g:
                    groups.add(g)

            # RS update dropdown (rsadminbot excluded by handler; still allowed in selection list).
            if "rs_bots" in groups and groups.issubset({"rs_bots", "rsadminbot"}):
                options.insert(
                    0,
                    discord.SelectOption(
                        label="📦 All RS Bots",
                        value="all_rs_bots",
                        description="Update all RS bots from GitHub (python-only) and restart services",
                    ),
                )
            # MW update dropdown
            elif groups == {"mirror_bots"}:
                options.insert(
                    0,
                    discord.SelectOption(
                        label="📦 All MW Bots",
                        value="all_mw_bots",
                        description="Update all MW bots from GitHub (python-only) and restart services",
                    ),
                )
        
        select = ui.Select(
            placeholder=f"Select bot to {action_display.lower()}...",
            options=options
        )
        select.callback = self.on_select
        self.add_item(select)
    
    async def on_select(self, interaction: discord.Interaction):
        """Handle bot selection"""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return
        
        bot_name = interaction.data['values'][0]
        
        # Defer to prevent timeout (slash-only: always ephemeral)
        await interaction.response.defer(ephemeral=True)

        # Enforce ephemeral followups for every handler call below, even if the call site
        # forgets to pass ephemeral=True.
        try:
            _orig_send = interaction.followup.send

            async def _ephemeral_send(*args, **kwargs):
                kwargs.setdefault("ephemeral", True)
                return await _orig_send(*args, **kwargs)

            interaction.followup.send = _ephemeral_send  # type: ignore[assignment]
        except Exception:
            pass
        
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
            if bot_name == "all_rs_bots":
                await self._handle_update_all_rs_bots(interaction)
            elif bot_name == "all_mw_bots":
                await self._handle_update_all_mw_bots(interaction)
            else:
                bot_info = self.admin_bot.BOTS[bot_name]
                await self._handle_update(interaction, bot_name, bot_info)
        elif self.action == "sync":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_sync(interaction, bot_name, bot_info)
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
        elif self.action == "secrets":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_secrets(interaction, bot_name, bot_info)
        elif self.action == "commands":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_commands(interaction, bot_name, bot_info)
        elif self.action == "fileview":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_fileview(interaction, bot_name, bot_info)
        elif self.action == "diagnose":
            bot_info = self.admin_bot.BOTS[bot_name]
            await self._handle_diagnose(interaction, bot_name, bot_info)
    
    async def _handle_start(self, interaction, bot_name):
        """Handle bot start (supports single bot or 'all_bots')"""
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available", ephemeral=True)
            return
        
        # Handle "all_bots" case - use group-specific scripts for efficiency
        if bot_name == "all_bots":
            status_msg = await interaction.followup.send(
                embed=MessageHelper.create_info_embed(
                    title="Starting All Bots",
                    message="Starting all bots using group scripts.",
                    fields=[
                        {"name": "Action", "value": "start", "inline": True},
                        {"name": "Mode", "value": "group scripts", "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            
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
            await status_msg.edit(
                embed=MessageHelper.create_info_embed(
                    title="Start All Complete",
                    message=summary[:1800],
                    footer=f"Triggered by {interaction.user}",
                )
            )
            try:
                embed = MessageHelper.create_info_embed(
                    title="Start All Complete",
                    message="All bots start sequence completed.",
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
            return
        
        # Handle single bot case
        bot_info = self.admin_bot.BOTS[bot_name]
        service_name = bot_info["service"]
        await interaction.followup.send(
            embed=MessageHelper.create_info_embed(
                title="Starting Bot",
                message=f"Starting {bot_info['name']}...",
                fields=[
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )
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
                fields = [
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                    {"name": "Status", "value": f"{before_state_txt} → {after_state_txt}" if before_state_txt != after_state_txt else after_state_txt, "inline": True},
                    {"name": "PID", "value": f"{before_pid_txt} → {after_pid_txt}" if before_pid_txt != after_pid_txt else after_pid_txt, "inline": True},
                ]
                await interaction.followup.send(
                    embed=MessageHelper.create_success_embed(
                        title="Bot Started",
                        message=f"{bot_info['name']} started successfully.",
                        fields=fields,
                        footer=f"Triggered by {interaction.user}",
                    ),
                    ephemeral=True,
                )
            try:
                fields = [
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ]
                if after_state:
                    state_display = after_state
                    if before_state and before_state != after_state:
                        state_display = f"{before_state} → {after_state}"
                    fields.append({"name": "Status", "value": state_display, "inline": True})
                if after_pid:
                    pid_display = str(after_pid)
                    if before_pid and before_pid != after_pid:
                        pid_display = f"{before_pid} → {after_pid}"
                    fields.append({"name": "PID", "value": pid_display, "inline": True})
                embed = MessageHelper.create_success_embed(
                    title="Bot Started",
                    message=f"{bot_info['name']} started successfully.",
                    fields=fields,
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
            else:
                error_msg = verify_error or stderr or stdout or "Unknown error"
                await interaction.followup.send(
                    embed=MessageHelper.create_error_embed(
                        title="Failed to Start Bot",
                        message=f"Failed to start {bot_info['name']}.",
                        error_details=error_msg[:500],
                        fields=[
                            {"name": "Bot", "value": bot_info["name"], "inline": True},
                            {"name": "Service", "value": service_name, "inline": True},
                        ],
                        footer=f"Triggered by {interaction.user}",
                    ),
                    ephemeral=True,
                )
        else:
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(
                embed=MessageHelper.create_error_embed(
                    title="Failed to Start Bot",
                    message=f"Failed to start {bot_info['name']}.",
                    error_details=error_msg[:500],
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
    
    async def _handle_stop(self, interaction, bot_name):
        """Handle bot stop (supports single bot or 'all_bots')"""
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available", ephemeral=True)
            return
        
        # Handle "all_bots" case - use group-specific scripts for efficiency
        if bot_name == "all_bots":
            status_msg = await interaction.followup.send(
                embed=MessageHelper.create_info_embed(
                    title="Stopping All Bots",
                    message="Stopping all bots using group scripts.",
                    fields=[
                        {"name": "Action", "value": "stop", "inline": True},
                        {"name": "Mode", "value": "group scripts", "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            
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
            await status_msg.edit(
                embed=MessageHelper.create_info_embed(
                    title="Stop All Complete",
                    message=summary[:1800],
                    footer=f"Triggered by {interaction.user}",
                )
            )
            try:
                embed = MessageHelper.create_info_embed(
                    title="Stop All Complete",
                    message="All bots stop sequence completed.",
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
            return
        
        # Handle single bot case
        bot_info = self.admin_bot.BOTS[bot_name]
        service_name = bot_info["service"]
        script_pattern = bot_info.get("script", bot_name)
        await interaction.followup.send(
            embed=MessageHelper.create_info_embed(
                title="Stopping Bot",
                message=f"Stopping {bot_info['name']}...",
                fields=[
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )
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
            fields = [
                {"name": "Bot", "value": bot_info["name"], "inline": True},
                {"name": "Service", "value": service_name, "inline": True},
                {"name": "Status", "value": f"{before_state_txt} → {after_state_txt}" if before_state_txt != after_state_txt else after_state_txt, "inline": True},
                {"name": "PID", "value": f"{before_pid_txt} → {after_pid_txt}" if before_pid_txt != after_pid_txt else after_pid_txt, "inline": True},
            ]
            await interaction.followup.send(
                embed=MessageHelper.create_success_embed(
                    title="Bot Stopped",
                    message=f"{bot_info['name']} stopped successfully.",
                    fields=fields,
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            try:
                fields = [
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ]
                if after_state:
                    state_display = after_state
                    if before_state and before_state != after_state:
                        state_display = f"{before_state} → {after_state}"
                    fields.append({"name": "Status", "value": state_display, "inline": True})
                if before_pid and not after_pid:
                    fields.append({"name": "PID", "value": f"{before_pid} → 0", "inline": True})
                embed = MessageHelper.create_success_embed(
                    title="Bot Stopped",
                    message=f"{bot_info['name']} stopped successfully.",
                    fields=fields,
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
        else:
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(
                embed=MessageHelper.create_error_embed(
                    title="Failed to Stop Bot",
                    message=f"Failed to stop {bot_info['name']}.",
                    error_details=error_msg[:500],
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
    
    async def _handle_restart(self, interaction, bot_name):
        """Handle bot restart (supports single bot or 'all_bots')"""
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available", ephemeral=True)
            return
        
        # Handle "all_bots" case - use group-specific scripts for efficiency
        if bot_name == "all_bots":
            status_msg = await interaction.followup.send(
                embed=MessageHelper.create_info_embed(
                    title="Restarting All Bots",
                    message="Restarting all bots using group scripts.",
                    fields=[
                        {"name": "Action", "value": "restart", "inline": True},
                        {"name": "Mode", "value": "group scripts", "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            
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
            await status_msg.edit(
                embed=MessageHelper.create_info_embed(
                    title="Restart All Complete",
                    message=summary[:1800],
                    footer=f"Triggered by {interaction.user}",
                )
            )
            try:
                embed = MessageHelper.create_info_embed(
                    title="Restart All Complete",
                    message="All bots restart sequence completed.",
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
            return
        
        # Handle single bot case
        bot_info = self.admin_bot.BOTS[bot_name]
        service_name = bot_info["service"]
        script_pattern = bot_info.get("script", bot_name)
        await interaction.followup.send(
            embed=MessageHelper.create_info_embed(
                title="Restarting Bot",
                message=f"Restarting {bot_info['name']}...",
                fields=[
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )
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
                fields = [
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                    {"name": "Status", "value": f"{before_state_txt} → {after_state_txt}" if before_state_txt != after_state_txt else after_state_txt, "inline": True},
                    {"name": "PID", "value": f"{before_pid_txt} → {after_pid_txt}" if before_pid_txt != after_pid_txt else after_pid_txt, "inline": True},
                ]
                await interaction.followup.send(
                    embed=MessageHelper.create_success_embed(
                        title="Bot Restarted",
                        message=f"{bot_info['name']} restarted successfully.",
                        fields=fields,
                        footer=f"Triggered by {interaction.user}",
                    ),
                    ephemeral=True,
                )
            try:
                fields = [
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ]
                if after_state:
                    state_display = after_state
                    if before_state and before_state != after_state:
                        state_display = f"{before_state} → {after_state}"
                    fields.append({"name": "Status", "value": state_display, "inline": True})
                if after_pid:
                    pid_display = str(after_pid)
                    if before_pid and before_pid != after_pid:
                        pid_display = f"{before_pid} → {after_pid}"
                    fields.append({"name": "PID", "value": pid_display, "inline": True})
                embed = MessageHelper.create_success_embed(
                    title="Bot Restarted",
                    message=f"{bot_info['name']} restarted successfully.",
                    fields=fields,
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
            else:
                error_msg = verify_error or stderr or stdout or "Unknown error"
                await interaction.followup.send(
                    embed=MessageHelper.create_error_embed(
                        title="Failed to Restart Bot",
                        message=f"Failed to restart {bot_info['name']}.",
                        error_details=error_msg[:500],
                        fields=[
                            {"name": "Bot", "value": bot_info["name"], "inline": True},
                            {"name": "Service", "value": service_name, "inline": True},
                        ],
                        footer=f"Triggered by {interaction.user}",
                    ),
                    ephemeral=True,
                )
        else:
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(
                embed=MessageHelper.create_error_embed(
                    title="Failed to Restart Bot",
                    message=f"Failed to restart {bot_info['name']}.",
                    error_details=error_msg[:500],
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
    
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
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def _handle_update(self, interaction, bot_name, bot_info):
        """Handle bot update (GitHub python-only) from the dropdown."""
        bot_key = (bot_name or "").strip().lower()
        group = self.admin_bot._get_bot_group(bot_key) or ""

        if bot_key == "rsadminbot" or group == "rsadminbot":
            await interaction.followup.send("ℹ️ Use `/selfupdate` to update RSAdminBot.", ephemeral=True)
            return

        if group == "rs_bots":
            code_root = self.admin_bot._get_update_code_root_for_group("rs_bots")
            await interaction.followup.send(
                f"📦 **Updating {bot_info['name']} from GitHub (python-only)...**\n"
                f"```\nPulling + copying tracked files from {code_root}\n```"
            ,
                ephemeral=True,
            )
            ok, result = self.admin_bot._botupdate_one_py_only(bot_key)
        elif group == "mirror_bots":
            code_root = self.admin_bot._get_update_code_root_for_group("mirror_bots")
            await interaction.followup.send(
                f"📦 **Updating {bot_info['name']} from GitHub (python-only)...**\n"
                f"```\nPulling + copying tracked files from {code_root}\n```"
            ,
                ephemeral=True,
            )
            ok, result = self.admin_bot._mwupdate_one_py_only(bot_key)
        else:
            await interaction.followup.send(f"❌ `{bot_key}` is not in an updatable bot group.", ephemeral=True)
            return

        if not ok:
            await interaction.followup.send(f"❌ Update failed:\n```{str(result.get('error') or 'unknown error')[:900]}```", ephemeral=True)
            return

        summary = str(result.get("summary") or "")[:1900]
        await interaction.followup.send(summary, ephemeral=True)

    async def _handle_update_all_rs_bots(self, interaction) -> None:
        """Update all RS bots (python-only) from the dropdown."""
        ssh_ok, error_msg = self.admin_bot._check_ssh_available()
        if not ssh_ok:
            await interaction.followup.send(f"❌ SSH not configured: {error_msg}", ephemeral=True)
            return

        rs_keys = [k for k in self.admin_bot._get_rs_bot_keys() if k in self.admin_bot.BOTS and k != "rsadminbot"]
        if not rs_keys:
            await interaction.followup.send("❌ No RS bots configured.", ephemeral=True)
            return

        code_root = self.admin_bot._get_update_code_root_for_group("rs_bots")
        status_msg = await interaction.followup.send(
            embed=MessageHelper.create_info_embed(
                title="Updating All RS Bots (python-only)",
                message=f"Pulling + copying tracked files from {code_root} and restarting each service.",
                fields=[
                    {"name": "Bots", "value": str(len(rs_keys)), "inline": True},
                    {"name": "Note", "value": "RSAdminBot is excluded (use !selfupdate).", "inline": False},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )

        ok_count = 0
        fail_count = 0
        lines: List[str] = []
        for bot_key in rs_keys:
            ok, result = self.admin_bot._botupdate_one_py_only(bot_key)
            if ok:
                ok_count += 1
                lines.append(f"✅ {bot_key}: changed={result.get('changed_count')} restart={result.get('restart')}")
            else:
                fail_count += 1
                err = str(result.get("error") or "update failed")[:120]
                lines.append(f"❌ {bot_key}: {err}")

        msg = "\n".join(lines)
        if len(msg) > 1800:
            msg = "…(truncated)…\n" + msg[-1800:]

        await status_msg.edit(
            embed=MessageHelper.create_info_embed(
                title="RS Bots Update Complete",
                message=f"✅ OK: {ok_count} | ❌ Failed: {fail_count}\n```{msg}```",
                footer=f"Triggered by {interaction.user}",
            )
        )

    async def _handle_update_all_mw_bots(self, interaction) -> None:
        """Update all Mirror-World bots (python-only) from the dropdown."""
        ssh_ok, error_msg = self.admin_bot._check_ssh_available()
        if not ssh_ok:
            await interaction.followup.send(f"❌ SSH not configured: {error_msg}", ephemeral=True)
            return

        mw_keys = [k for k in self.admin_bot._get_mw_bot_keys() if k in self.admin_bot.BOTS]
        if not mw_keys:
            await interaction.followup.send("❌ No Mirror-World bots configured.", ephemeral=True)
            return

        code_root = self.admin_bot._get_update_code_root_for_group("mirror_bots")
        status_msg = await interaction.followup.send(
            embed=MessageHelper.create_info_embed(
                title="Updating All MW Bots (python-only)",
                message=f"Pulling + copying tracked files from {code_root} and restarting each service.",
                fields=[
                    {"name": "Bots", "value": str(len(mw_keys)), "inline": True},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )

        ok_count = 0
        fail_count = 0
        lines: List[str] = []
        for bot_key in mw_keys:
            ok, result = self.admin_bot._mwupdate_one_py_only(bot_key)
            if ok:
                ok_count += 1
                lines.append(f"✅ {bot_key}: changed={result.get('changed_count')} restart={result.get('restart')}")
            else:
                fail_count += 1
                err = str(result.get("error") or "update failed")[:120]
                lines.append(f"❌ {bot_key}: {err}")

        msg = "\n".join(lines)
        if len(msg) > 1800:
            msg = "…(truncated)…\n" + msg[-1800:]

        await status_msg.edit(
            embed=MessageHelper.create_info_embed(
                title="MW Bots Update Complete",
                message=f"✅ OK: {ok_count} | ❌ Failed: {fail_count}\n```{msg}```",
                footer=f"Triggered by {interaction.user}",
            )
        )
    
    async def _handle_sync(self, interaction, bot_name, bot_info):
        """Handle bot sync from dropdown."""
        if bot_name == "all_bots":
            await interaction.followup.send("❌ Cannot sync all bots at once. Please select a specific bot.", ephemeral=True)
            return
        
        ssh_ok, error_msg = self.admin_bot._check_ssh_available()
        if not ssh_ok:
            await interaction.followup.send(f"❌ SSH not configured: {error_msg}", ephemeral=True)
            return
        
        bot_folder = str(bot_info.get("folder") or "")
        local_bot_path = self.admin_bot.base_path.parent / bot_folder
        if not local_bot_path.exists():
            await interaction.followup.send(f"❌ Local bot folder not found: {local_bot_path}", ephemeral=True)
            return
        
        remote_root = getattr(self.admin_bot, "remote_root", "") or "/home/rsadmin/bots/mirror-world"
        remote_bot_path = f"{remote_root}/{bot_folder}"
        
        await interaction.followup.send(
            f"📤 **Syncing {bot_info['name']} to server...**\n"
            f"Local: `{local_bot_path}`\n"
            f"Remote: `{remote_bot_path}`"
            ,
            ephemeral=True,
        )
        
        # Use the admin_bot's sync method
        status_msg = await interaction.followup.send("⏳ Starting sync...", ephemeral=True)
        rsync_script = self.admin_bot.base_path.parent / "Rsync" / "rsync_sync.py"
        
        if not rsync_script.exists():
            await self.admin_bot._sync_bot_via_ssh(None, status_msg, bot_info, bot_folder, local_bot_path, remote_bot_path, False, False)
        else:
            await self.admin_bot._sync_bot_via_script(None, status_msg, bot_info, bot_folder, local_bot_path, remote_bot_path, rsync_script, False, False)
    
    async def _handle_info(self, interaction, bot_name, bot_info):
        """Handle bot info (dropdown)."""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await interaction.followup.send(err, ephemeral=True)
            return

        bot_key = str(bot_name or "").strip().lower()
        info = self.admin_bot.BOTS.get(bot_key) or {}
        folder = str(info.get("folder") or "").strip()
        service = str(info.get("service") or "").strip()
        script = str(info.get("script") or "").strip()
        group = str(self.admin_bot._get_bot_group(bot_key) or "").strip()

        repo_root = self.admin_bot.base_path.parent
        local_folder = (repo_root / folder).resolve() if folder else None
        exists_folder = bool(local_folder and local_folder.exists())
        cfg_path = (local_folder / "config.json") if local_folder else None
        secrets_path = (local_folder / "config.secrets.json") if local_folder else None

        fields = [
            {"name": "Key", "value": f"`{bot_key}`", "inline": True},
            {"name": "Group", "value": f"`{group or '(unknown)'}`", "inline": True},
            {"name": "Service", "value": f"`{service or '(missing)'}`", "inline": False},
            {"name": "Folder", "value": f"`{folder or '(missing)'}`", "inline": True},
            {"name": "Folder exists", "value": "YES" if exists_folder else "NO", "inline": True},
        ]
        if script:
            fields.append({"name": "Script", "value": f"`{script}`", "inline": True})
        if cfg_path:
            fields.append({"name": "config.json", "value": "YES" if cfg_path.exists() else "NO", "inline": True})
        if secrets_path:
            fields.append({"name": "config.secrets.json", "value": "YES" if secrets_path.exists() else "NO", "inline": True})

        if self.admin_bot.service_manager and service:
            exists, state, _ = self.admin_bot.service_manager.get_status(service, bot_name=bot_key)
            pid = self.admin_bot.service_manager.get_pid(service)
            fields.append({"name": "Status", "value": str(state or "unknown") if exists else "not_found", "inline": True})
            if pid:
                fields.append({"name": "PID", "value": str(pid), "inline": True})

        embed = MessageHelper.create_info_embed(
            title=f"Bot Info: {info.get('name', bot_key)}",
            message="",
            fields=fields,
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def _handle_config(self, interaction, bot_name, bot_info):
        """Handle bot config"""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await interaction.followup.send(err, ephemeral=True)
            return
        embed = self.admin_bot._build_botconfig_embed(bot_name, triggered_by=interaction.user)
        await interaction.followup.send(embed=embed, view=BotConfigActionsView(self.admin_bot, bot_name), ephemeral=True)

    async def _handle_secrets(self, interaction, bot_name, bot_info):
        """Handle bot secrets status (masked) + update flow."""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await interaction.followup.send(err, ephemeral=True)
            return

        folder = self.admin_bot._bot_folder_path(bot_name)
        secrets_path = folder / "config.secrets.json"
        ok_j, data, err_j = self.admin_bot._json_load_file(secrets_path)
        if not ok_j:
            await interaction.followup.send(
                embed=MessageHelper.create_error_embed(
                    title="Secrets Not Available",
                    message=f"Failed to read `{bot_name}` secrets.",
                    error_details=err_j[:900],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            return

        lines: List[str] = []
        for k in sorted(data.keys()):
            v = data.get(k)
            if isinstance(v, str):
                lines.append(f"- **{k}**: `{mask_secret(v)}`")
            elif isinstance(v, dict):
                lines.append(f"- **{k}**: object ({len(v)} keys)")
            elif isinstance(v, list):
                lines.append(f"- **{k}**: list ({len(v)} items)")
            else:
                lines.append(f"- **{k}**: {type(v).__name__}")
        body = "\n".join(lines) if lines else "(empty)"
        if len(body) > 900:
            body = body[:900] + "\n…(truncated)…"

        embed = MessageHelper.create_info_embed(
            title="Secrets Status (masked)",
            message=body,
            fields=[{"name": "File", "value": f"`{str(secrets_path)}`", "inline": False}],
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, view=BotSecretsActionsView(self.admin_bot, bot_name), ephemeral=True)

    async def _handle_commands(self, interaction, bot_name, bot_info):
        """Handle COMMANDS.md view via dropdown selection."""
        try:
            async def _send(**kwargs):
                kwargs.setdefault("ephemeral", True)
                return await interaction.followup.send(**kwargs)

            await self.admin_bot._commands_send_for_bot(
                bot_key=bot_name,
                send=_send,
                triggered_by=interaction.user,
            )
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to show commands: {str(e)[:200]}", ephemeral=True)

    async def _handle_fileview(self, interaction, bot_name, bot_info):
        """Show a quick file listing (sizes + mtimes) for a bot folder."""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await interaction.followup.send(err, ephemeral=True)
            return

        bot_key = str(bot_name or "").strip().lower()
        folder = self.admin_bot._bot_folder_path(bot_key)
        if not folder.exists():
            await interaction.followup.send(f"❌ Folder not found: `{folder}`", ephemeral=True)
            return

        mode = str((self.action_kwargs or {}).get("mode") or "").strip().lower()
        include = ["*.py", "*.sh", "*.md", "*.txt", "requirements.txt", "config.json", "messages.json", "vouch_config.json"]
        if mode in ("alljson", "json", "all_json"):
            include.append("*.json")

        exclude_names = {"config.secrets.json"}
        skip_dirs = {"__pycache__", ".git", ".venv", "venv", "backups"}

        files: List[Path] = []
        for pat in include:
            try:
                for p in folder.rglob(pat):
                    if p.is_dir():
                        continue
                    if p.name in exclude_names:
                        continue
                    if any(part in skip_dirs for part in p.parts):
                        continue
                    files.append(p)
            except Exception:
                continue

        uniq: Dict[str, Path] = {}
        for p in files:
            try:
                rel = p.relative_to(folder).as_posix()
            except Exception:
                rel = str(p)
            uniq[rel] = p
        rows: List[tuple[float, str]] = []
        for rel, p in uniq.items():
            try:
                st = p.stat()
                mtime = st.st_mtime
                size = st.st_size
                ts = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
                rows.append((mtime, f"{size:>10}  {ts}  {rel}"))
            except Exception:
                continue
        rows.sort(key=lambda x: x[0], reverse=True)
        body = "\n".join([r for _, r in rows[:60]]) if rows else "(no matching files)"
        if len(body) > 1800:
            body = "…(truncated)…\n" + body[-1800:]

        embed = MessageHelper.create_info_embed(
            title=f"Fileview: {bot_key}",
            message=self.admin_bot._codeblock(body, limit=1800),
            fields=[
                {"name": "Folder", "value": f"`{str(folder)}`", "inline": False},
                {"name": "Mode", "value": mode or "default", "inline": True},
            ],
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
    
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
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    async def _handle_details(self, interaction, bot_name, bot_info):
        """Show systemd details via botctl.sh (dropdown action)."""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await interaction.followup.send(err, ephemeral=True)
            return
        svc = str(bot_info.get("service") or "")
        await interaction.followup.send(f"🧾 **Details: {bot_info.get('name', bot_name)}**\nService: `{svc}`", ephemeral=True)
        success, out, err = self.admin_bot._execute_sh_script("botctl.sh", "details", bot_name)
        await interaction.followup.send(self.admin_bot._codeblock(out or err or ""), ephemeral=True)

    async def _handle_logs(self, interaction, bot_name, bot_info):
        """Show journalctl logs via botctl.sh (dropdown action)."""
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await interaction.followup.send(err, ephemeral=True)
            return
        svc = str(bot_info.get("service") or "")
        lines = int(self.action_kwargs.get("lines") or 80)
        lines = max(10, min(lines, 400))
        await interaction.followup.send(f"📜 **Logs: {bot_info.get('name', bot_name)}**\nService: `{svc}`\nLines: `{lines}`", ephemeral=True)
        success, out, err = self.admin_bot._execute_sh_script("botctl.sh", "logs", bot_name, str(lines))
        await interaction.followup.send(self.admin_bot._codeblock(out or err or ""), ephemeral=True)


class BotConfigEditModal(ui.Modal):
    def __init__(self, admin_bot_instance: "RSAdminBot", bot_key: str):
        super().__init__(title=f"Edit {bot_key} config.json")
        self.admin_bot = admin_bot_instance
        self.bot_key = str(bot_key or "").strip().lower()

        self.key_path = ui.TextInput(
            label="Key path (dot notation)",
            placeholder="example: dm_sequence.send_spacing_seconds",
            required=True,
            max_length=200,
        )
        self.json_value = ui.TextInput(
            label="JSON value",
            placeholder='example: 10  (or "text", true, [1,2], {"a":1})',
            required=True,
            max_length=1000,
            style=discord.TextStyle.paragraph,
        )
        self.add_item(self.key_path)
        self.add_item(self.json_value)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return

        ok_u, result = self.admin_bot._update_bot_config_json(self.bot_key, str(self.key_path.value), str(self.json_value.value))
        if not ok_u:
            await self.admin_bot._interaction_reply(
                interaction,
                embed=MessageHelper.create_error_embed(
                    title="Config Update Failed",
                    message="Failed to update config.json.",
                    error_details=str((result or {}).get("error") or "unknown error")[:900],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            return

        await self.admin_bot._interaction_reply(
            interaction,
            embed=MessageHelper.create_success_embed(
                title="Config Updated",
                message=f"Updated `{self.bot_key}` config.json.",
                fields=[
                    {"name": "Path", "value": f"`{(result or {}).get('key_path')}`", "inline": False},
                    {"name": "Backup", "value": f"`{str((result or {}).get('backup') or '')[:200]}`" or "(none)", "inline": False},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )


class BotConfigActionsView(ui.View):
    def __init__(self, admin_bot_instance: "RSAdminBot", bot_key: str):
        super().__init__(timeout=300)
        self.admin_bot = admin_bot_instance
        self.bot_key = str(bot_key or "").strip().lower()

    async def _deny_if_needed(self, interaction: discord.Interaction) -> bool:
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return True
        return False

    @ui.button(label="Edit config.json", style=discord.ButtonStyle.primary)
    async def edit_config(self, interaction: discord.Interaction, button: ui.Button) -> None:
        if await self._deny_if_needed(interaction):
            return
        await interaction.response.send_modal(BotConfigEditModal(self.admin_bot, self.bot_key))

    @ui.button(label="Restart bot", style=discord.ButtonStyle.secondary)
    async def restart_bot(self, interaction: discord.Interaction, button: ui.Button) -> None:
        if await self._deny_if_needed(interaction):
            return
        info = self.admin_bot.BOTS.get(self.bot_key) or {}
        service = str(info.get("service") or "").strip()
        if not service or not self.admin_bot.service_manager:
            await self.admin_bot._interaction_reply(interaction, content="❌ ServiceManager not available or missing service mapping.", ephemeral=True)
            return
        ok_r, out_r, err_r = self.admin_bot.service_manager.restart(service, bot_name=self.bot_key)
        if not ok_r:
            msg = (err_r or out_r or "restart failed")[:500]
            await self.admin_bot._interaction_reply(interaction, content=f"❌ Restart failed: {msg}", ephemeral=True)
            return
        await self.admin_bot._interaction_reply(interaction, content="✅ Restart initiated.", ephemeral=True)


class BotSecretsEditModal(ui.Modal):
    def __init__(self, admin_bot_instance: "RSAdminBot", bot_key: str):
        super().__init__(title=f"Edit {bot_key} config.secrets.json")
        self.admin_bot = admin_bot_instance
        self.bot_key = str(bot_key or "").strip().lower()

        self.key_path = ui.TextInput(
            label="Secret key path (dot notation)",
            placeholder="example: bot_token  (or whop_webhook.secret)",
            required=True,
            max_length=200,
        )
        self.json_value = ui.TextInput(
            label="JSON value",
            placeholder='example: "my-secret-string"',
            required=True,
            max_length=1000,
            style=discord.TextStyle.paragraph,
        )
        self.add_item(self.key_path)
        self.add_item(self.json_value)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return

        ok_u, result = self.admin_bot._update_bot_secrets_json(self.bot_key, str(self.key_path.value), str(self.json_value.value))
        if not ok_u:
            await self.admin_bot._interaction_reply(
                interaction,
                embed=MessageHelper.create_error_embed(
                    title="Secrets Update Failed",
                    message="Failed to update config.secrets.json.",
                    error_details=str((result or {}).get("error") or "unknown error")[:900],
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            return

        await self.admin_bot._interaction_reply(
            interaction,
            embed=MessageHelper.create_success_embed(
                title="Secrets Updated",
                message=f"Updated `{self.bot_key}` config.secrets.json.",
                fields=[
                    {"name": "Path", "value": f"`{(result or {}).get('key_path')}`", "inline": False},
                    {"name": "Value (masked)", "value": f"`{str((result or {}).get('value_masked') or '')[:200]}`", "inline": False},
                    {"name": "Backup", "value": f"`{str((result or {}).get('backup') or '')[:200]}`" or "(none)", "inline": False},
                ],
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )


class BotSecretsActionsView(ui.View):
    def __init__(self, admin_bot_instance: "RSAdminBot", bot_key: str):
        super().__init__(timeout=300)
        self.admin_bot = admin_bot_instance
        self.bot_key = str(bot_key or "").strip().lower()

    async def _deny_if_needed(self, interaction: discord.Interaction) -> bool:
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return True
        return False

    @ui.button(label="Edit config.secrets.json", style=discord.ButtonStyle.danger)
    async def edit_secrets(self, interaction: discord.Interaction, button: ui.Button) -> None:
        if await self._deny_if_needed(interaction):
            return
        await interaction.response.send_modal(BotSecretsEditModal(self.admin_bot, self.bot_key))


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
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return
        
        # Disable button to prevent multiple clicks
        button.disabled = True
        button.label = "⏳ Starting..."
        await interaction.response.edit_message(view=self)
        
        # Start the bot
        bot_info = self.admin_bot.BOTS[self.bot_name]
        service_name = bot_info["service"]
        
        # Log to Discord (embed)
        try:
            start_embed = MessageHelper.create_info_embed(
                title="Starting Bot",
                message=f"Starting {bot_info['name']}...",
                fields=[
                    {"name": "Bot", "value": bot_info["name"], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ],
                footer=f"Triggered by {interaction.user}",
            )
            await self.admin_bot._log_to_discord(start_embed, interaction.channel if hasattr(interaction, "channel") else None)
        except Exception:
            pass
        
        # Start service using ServiceManager
        if not self.admin_bot.service_manager:
            await interaction.followup.send("❌ ServiceManager not available", ephemeral=True)
            return
        
        success, stdout, stderr = self.admin_bot.service_manager.start(service_name, unmask=True, bot_name=self.bot_name)
        
        if success:
            # Verify service actually started
            is_running, verify_error = self.admin_bot.service_manager.verify_started(service_name, bot_name=self.bot_name)
            if is_running:
                button.label = "✅ Started"
                button.style = discord.ButtonStyle.success
                await interaction.followup.send(f"✅ **{bot_info['name']}** started successfully!", ephemeral=True)
                try:
                    ok_embed = MessageHelper.create_success_embed(
                        title="Bot Started",
                        message=f"{bot_info['name']} started successfully.",
                        fields=[
                            {"name": "Bot", "value": bot_info["name"], "inline": True},
                            {"name": "Service", "value": service_name, "inline": True},
                        ],
                        footer=f"Triggered by {interaction.user}",
                    )
                    await self.admin_bot._log_to_discord(ok_embed, interaction.channel if hasattr(interaction, "channel") else None)
                except Exception:
                    pass
            else:
                button.label = "❌ Failed"
                button.style = discord.ButtonStyle.danger
                error_msg = verify_error or stderr or stdout or "Unknown error"
                await interaction.followup.send(f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```", ephemeral=True)
                try:
                    err_embed = MessageHelper.create_error_embed(
                        title="Failed to Start Bot",
                        message=f"Failed to start {bot_info['name']}.",
                        error_details=error_msg[:500],
                        fields=[
                            {"name": "Bot", "value": bot_info["name"], "inline": True},
                            {"name": "Service", "value": service_name, "inline": True},
                        ],
                        footer=f"Triggered by {interaction.user}",
                    )
                    await self.admin_bot._log_to_discord(err_embed, interaction.channel if hasattr(interaction, "channel") else None)
                except Exception:
                    pass
        else:
            button.label = "❌ Failed"
            button.style = discord.ButtonStyle.danger
            error_msg = stderr or stdout or "Unknown error"
            await interaction.followup.send(f"❌ Failed to start {bot_info['name']}:\n```{error_msg[:500]}```", ephemeral=True)
            try:
                err_embed = MessageHelper.create_error_embed(
                    title="Failed to Start Bot",
                    message=f"Failed to start {bot_info['name']}.",
                    error_details=error_msg[:500],
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ],
                    footer=f"Triggered by {interaction.user}",
                )
                await self.admin_bot._log_to_discord(err_embed, interaction.channel if hasattr(interaction, "channel") else None)
            except Exception:
                pass
        
        # Update the message
        await interaction.edit_original_response(view=self)


class RSAdminSlashCog(commands.Cog):
    """Slash-only command surface for RSAdminBot (ephemeral, allowed guilds, owner/admin-only)."""

    def __init__(self, admin_bot_instance: "RSAdminBot"):
        self.admin_bot = admin_bot_instance

    async def _guard(self, interaction: discord.Interaction) -> bool:
        ok, err = await self.admin_bot._slash_owner_guard(interaction)
        if not ok:
            await self.admin_bot._interaction_reply(interaction, content=err, ephemeral=True)
            return False
        return True

    async def _send_bot_select(
        self,
        interaction: discord.Interaction,
        *,
        action: str,
        action_display: str,
        title: str,
        description: str,
        bot_keys: Optional[List[str]] = None,
        action_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not await self._guard(interaction):
            return
        view = BotSelectView(self.admin_bot, action, action_display, action_kwargs=action_kwargs, bot_keys=bot_keys)
        embed = discord.Embed(title=title, description=description, color=discord.Color.blurple())
        await self.admin_bot._interaction_reply(interaction, embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="ping", description="Check RSAdminBot latency (owner-only).")
    async def ping(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        latency_ms = round(self.admin_bot.bot.latency * 1000)
        embed = MessageHelper.create_info_embed(
            title="Pong",
            message="RSAdminBot is responding.",
            fields=[{"name": "Latency", "value": f"{latency_ms}ms", "inline": True}],
            footer=f"Triggered by {interaction.user}",
        )
        await self.admin_bot._interaction_reply(interaction, embed=embed, ephemeral=True)

    @app_commands.command(name="reload", description="Reload RSAdminBot config (owner-only).")
    async def reload(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        try:
            self.admin_bot.load_config()
            self.admin_bot._load_ssh_config()
            await self.admin_bot._interaction_reply(interaction, content="✅ Configuration reloaded.", ephemeral=True)
        except Exception as e:
            await self.admin_bot._interaction_reply(interaction, content=f"❌ Reload failed: {str(e)[:200]}", ephemeral=True)

    @app_commands.command(name="restart", description="Restart RSAdminBot service (owner-only).")
    async def restart(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return

        admin_bot = self.admin_bot

        class _RestartView(ui.View):
            def __init__(self):
                super().__init__(timeout=60)
                self._started = False

            async def _deny_if_needed(self, i: discord.Interaction) -> bool:
                ok, err = await admin_bot._slash_owner_guard(i)
                if not ok:
                    await admin_bot._interaction_reply(i, content=err, ephemeral=True)
                    return True
                return False

            @ui.button(label="Restart now", style=discord.ButtonStyle.danger)
            async def do_restart(self, i: discord.Interaction, button: ui.Button) -> None:
                if await self._deny_if_needed(i):
                    return
                if self._started:
                    await admin_bot._interaction_reply(i, content="Already running.", ephemeral=True)
                    return
                self._started = True
                for child in self.children:
                    if isinstance(child, ui.Button):
                        child.disabled = True
                await admin_bot._interaction_reply(i, content="🔄 Restarting RSAdminBot service…", view=self, ephemeral=True)
                try:
                    subprocess.run(["sudo", "systemctl", "restart", "mirror-world-rsadminbot.service"], timeout=10)
                except Exception:
                    pass

            @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, i: discord.Interaction, button: ui.Button) -> None:
                if await self._deny_if_needed(i):
                    return
                for child in self.children:
                    if isinstance(child, ui.Button):
                        child.disabled = True
                await admin_bot._interaction_reply(i, content="Cancelled.", view=self, ephemeral=True)

        await admin_bot._interaction_reply(
            interaction,
            content="Restart RSAdminBot service now? (This will temporarily disconnect the bot.)",
            view=_RestartView(),
            ephemeral=True,
        )

    @app_commands.command(name="status", description="Show RSAdminBot runtime status (owner-only).")
    async def status(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        try:
            srv = self.admin_bot.current_server or {}
            host = str(srv.get("host") or "").strip()
            user = str(srv.get("user") or "").strip()
            local_exec_cfg = bool((self.admin_bot.config.get("local_exec") or {}).get("enabled", True))
            local_exec = bool((os.name != "nt") and local_exec_cfg)
            embed = MessageHelper.create_info_embed(
                title="RSAdminBot Status",
                message="Slash-only (ephemeral) admin surface is active.",
                fields=[
                    {"name": "Guild", "value": str(getattr(getattr(interaction, "guild", None), "id", "") or ""), "inline": True},
                    {"name": "Local exec", "value": "YES" if local_exec else "NO", "inline": True},
                    {"name": "SSH target", "value": f"{user}@{host}" if host else "(none)", "inline": False},
                    {"name": "ServiceManager", "value": "YES" if bool(self.admin_bot.service_manager) else "NO", "inline": True},
                    {"name": "Inspector", "value": "YES" if bool(self.admin_bot.inspector) else "NO", "inline": True},
                    {"name": "Organizer", "value": "YES" if bool(self.admin_bot.test_server_organizer) else "NO", "inline": True},
                ],
                footer=f"Triggered by {interaction.user}",
            )
            await self.admin_bot._interaction_reply(interaction, embed=embed, ephemeral=True)
        except Exception as e:
            await self.admin_bot._interaction_reply(interaction, content=f"❌ status failed: {str(e)[:200]}", ephemeral=True)

    @app_commands.command(name="ssh", description="Run an SSH command (owner-only).")
    @app_commands.describe(command="Command to run on the Oracle host (use carefully).")
    async def ssh(self, interaction: discord.Interaction, command: str) -> None:
        if not await self._guard(interaction):
            return
        cmd = str(command or "").strip()
        if not cmd:
            await self.admin_bot._interaction_reply(interaction, content="❌ command is required.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        ok, out, err = self.admin_bot._execute_ssh_command(cmd, timeout=30)
        payload = (out or err or "").strip()
        if not payload:
            payload = "(no output)"
        title = "SSH OK" if ok else "SSH FAILED"
        embed = MessageHelper.create_status_embed(
            title=title,
            description=self.admin_bot._codeblock(payload, limit=1800),
            color=discord.Color.green() if ok else discord.Color.red(),
            fields=[{"name": "Command", "value": f"`{cmd[:180]}`", "inline": False}],
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="syncstatus", description="Compare rsbots-code vs live tree (owner-only).")
    async def syncstatus(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        code_root = Path(self.admin_bot._get_update_code_root_for_group("rs_bots") or "").expanduser()
        live_root = Path(str(getattr(self.admin_bot, "remote_root", "") or "")).expanduser()
        if not code_root.exists():
            await interaction.followup.send(f"❌ Missing rsbots-code root: `{code_root}`", ephemeral=True)
            return
        if not live_root.exists():
            await interaction.followup.send(f"❌ Missing live root: `{live_root}`", ephemeral=True)
            return

        try:
            local_manifest = rs_generate_manifest(code_root, normalize_text_eol=True)
            live_manifest = rs_generate_manifest(live_root, normalize_text_eol=True)
            diff = rs_compare_manifests(local_manifest, live_manifest)
        except Exception as e:
            await interaction.followup.send(f"❌ syncstatus failed: {str(e)[:200]}", ephemeral=True)
            return

        lines: List[str] = []
        folders = (diff.get("folders") or {}) if isinstance(diff, dict) else {}
        for folder, d in folders.items():
            if not isinstance(d, dict):
                continue
            if d.get("missing_local") or d.get("missing_remote"):
                lines.append(f"⚠️ {folder}: missing_local={bool(d.get('missing_local'))} missing_remote={bool(d.get('missing_remote'))}")
                continue
            changed = len(d.get("changed") or [])
            only_local = len(d.get("only_local") or [])
            only_remote = len(d.get("only_remote") or [])
            if changed or only_local or only_remote:
                lines.append(f"❌ {folder}: changed={changed} only_local={only_local} only_remote={only_remote}")
            else:
                lines.append(f"✅ {folder}: OK")

        txt = "\n".join(lines)
        if len(txt) > 1800:
            txt = "…(truncated)…\n" + txt[-1800:]

        embed = MessageHelper.create_info_embed(
            title="Sync Status (rsbots-code vs live)",
            message=self.admin_bot._codeblock(txt, limit=1800),
            fields=[
                {"name": "rsbots-code", "value": f"`{str(code_root)}`", "inline": False},
                {"name": "live_root", "value": f"`{str(live_root)}`", "inline": False},
            ],
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="oraclefilesupdate", description="Push a bots-only snapshot to neo-rs/oraclefiles (owner-only).")
    async def oraclefilesupdate(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        ok, stats = self.admin_bot._oraclefiles_sync_once(trigger="manual")
        if not ok:
            err = str((stats or {}).get("error") or "oraclefiles sync failed")[:900]
            await interaction.followup.send(
                embed=MessageHelper.create_error_embed(
                    title="OracleFiles Sync Failed",
                    message="OracleFiles snapshot push failed.",
                    error_details=err,
                    footer=f"Triggered by {interaction.user}",
                ),
                ephemeral=True,
            )
            return
        head = str((stats or {}).get("head") or "")[:12]
        pushed = "YES" if str((stats or {}).get("pushed") or "").strip() else "NO"
        no_changes = "YES" if str((stats or {}).get("no_changes") or "").strip() else "NO"
        embed = MessageHelper.create_success_embed(
            title="OracleFiles Sync Complete",
            message="OracleFiles snapshot pushed successfully.",
            fields=[
                {"name": "Head", "value": head or "(unknown)", "inline": True},
                {"name": "Pushed", "value": pushed, "inline": True},
                {"name": "No changes", "value": no_changes, "inline": True},
            ],
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="systemcheck", description="Quick health check (owner-only).")
    async def systemcheck(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        local_exec_cfg = bool((self.admin_bot.config.get("local_exec") or {}).get("enabled", True))
        local_exec = bool((os.name != "nt") and local_exec_cfg)
        rs_code = Path(self.admin_bot._get_update_code_root_for_group("rs_bots") or "")
        mw_code = Path(self.admin_bot._get_update_code_root_for_group("mirror_bots") or "")
        live_root = Path(str(getattr(self.admin_bot, "remote_root", "") or ""))

        svc_ok = bool(self.admin_bot.service_manager)
        total = len(self.admin_bot.BOTS)
        running = 0
        stopped = 0
        unknown = 0
        if svc_ok:
            for k, info in self.admin_bot.BOTS.items():
                service = str((info or {}).get("service") or "").strip()
                if not service:
                    unknown += 1
                    continue
                exists, state, _ = self.admin_bot.service_manager.get_status(service, bot_name=k)
                if not exists:
                    unknown += 1
                elif state == "active":
                    running += 1
                else:
                    stopped += 1

        embed = MessageHelper.create_info_embed(
            title="System Check",
            message="",
            fields=[
                {"name": "Local exec", "value": "YES" if local_exec else "NO", "inline": True},
                {"name": "ServiceManager", "value": "YES" if svc_ok else "NO", "inline": True},
                {"name": "rsbots-code", "value": f"{'✅' if rs_code.exists() else '❌'} `{rs_code}`", "inline": False},
                {"name": "mwbots-code", "value": f"{'✅' if mw_code.exists() else '❌'} `{mw_code}`", "inline": False},
                {"name": "live_root", "value": f"{'✅' if live_root.exists() else '❌'} `{live_root}`", "inline": False},
                {"name": "Services", "value": f"total={total} running={running} stopped={stopped} unknown={unknown}", "inline": False},
            ],
            footer=f"Triggered by {interaction.user}",
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="botlist", description="List configured bots (owner-only).")
    async def botlist(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return

        rs_keys = self.admin_bot._get_rs_bot_keys()
        mw_keys = self.admin_bot._get_mw_bot_keys()

        def _fmt(keys: List[str]) -> str:
            out = []
            for k in keys:
                info = self.admin_bot.BOTS.get(k) or {}
                out.append(f"- `{k}`: {info.get('name', k)}")
            return "\n".join(out) if out else "(none)"

        embed = MessageHelper.create_info_embed(
            title="Bot List",
            message="Configured bots (from canonical registry).",
            fields=[
                {"name": "RS bots", "value": _fmt(rs_keys)[:1000], "inline": False},
                {"name": "MW bots", "value": _fmt(mw_keys)[:1000], "inline": False},
            ],
            footer=f"Triggered by {interaction.user}",
        )
        await self.admin_bot._interaction_reply(interaction, embed=embed, ephemeral=True)

    @app_commands.command(name="botstatus", description="Check bot status (owner-only).")
    async def botstatus(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="status",
            action_display="Status",
            title="📊 Bot Status",
            description="Pick a bot to view its service status.",
            bot_keys=self.admin_bot._get_rs_bot_keys() + self.admin_bot._get_mw_bot_keys(),
        )

    @app_commands.command(name="botinfo", description="Show bot metadata + service status (owner-only).")
    async def botinfo(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="info",
            action_display="Info",
            title="ℹ️ Bot Info",
            description="Pick a bot to view metadata + status.",
            bot_keys=self.admin_bot._get_rs_bot_keys() + self.admin_bot._get_mw_bot_keys(),
        )

    @app_commands.command(name="botstart", description="Start a bot (owner-only).")
    async def botstart(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="start",
            action_display="Start",
            title="🟢 Start Bot",
            description="Pick a bot (or All Bots) to start.",
        )

    @app_commands.command(name="botstop", description="Stop a bot (owner-only).")
    async def botstop(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="stop",
            action_display="Stop",
            title="🛑 Stop Bot",
            description="Pick a bot (or All Bots) to stop.",
        )

    @app_commands.command(name="botrestart", description="Restart a bot (owner-only).")
    async def botrestart(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="restart",
            action_display="Restart",
            title="🔄 Restart Bot",
            description="Pick a bot (or All Bots) to restart.",
        )

    @app_commands.command(name="botsync", description="Sync a bot folder to Oracle (owner-only).")
    async def botsync(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="sync",
            action_display="Sync",
            title="📤 Sync Bot",
            description="Pick a bot to sync local files to the server.",
        )

    @app_commands.command(name="botupdate", description="Update an RS bot from GitHub (python-only) and restart (owner-only).")
    async def botupdate(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="update",
            action_display="Update",
            title="📦 Update RS Bot (python-only)",
            description="Pick an RS bot (or All RS Bots) to update from GitHub.",
            bot_keys=self.admin_bot._get_rs_bot_keys(),
        )

    @app_commands.command(name="mwupdate", description="Update an MW bot from GitHub (python-only) and restart (owner-only).")
    async def mwupdate(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="update",
            action_display="Update",
            title="📦 Update MW Bot (python-only)",
            description="Pick an MW bot (or All MW Bots) to update from GitHub.",
            bot_keys=self.admin_bot._get_mw_bot_keys(),
        )

    @app_commands.command(name="selfupdate", description="Update RSAdminBot safely (staged) and restart to apply (owner-only).")
    async def selfupdate(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        status = await interaction.followup.send(
            embed=MessageHelper.create_info_embed(
                title="Selfupdate (staged)",
                message="Staging RSAdminBot update from GitHub checkout. A restart will apply it.",
                footer=f"Triggered by {interaction.user}",
            ),
            ephemeral=True,
        )

        ok, stats = self.admin_bot._stage_rsadminbot_selfupdate()
        if not ok:
            err = str((stats or {}).get("error") or "unknown error")[:1000]
            await status.edit(
                embed=MessageHelper.create_error_embed(
                    title="Selfupdate Failed",
                    message="Failed to stage RSAdminBot update.",
                    error_details=err,
                    footer=f"Triggered by {interaction.user}",
                )
            )
            return

        no_changes = str((stats or {}).get("no_changes") or "").strip() in ("1", "true", "yes")
        old = str((stats or {}).get("old") or "").strip()
        new = str((stats or {}).get("new") or "").strip()
        changed = str((stats or {}).get("changed_count") or "0").strip()
        backup = str((stats or {}).get("backup") or "").strip()
        sample = (stats or {}).get("changed_sample") or []

        if no_changes:
            await status.edit(
                embed=MessageHelper.create_success_embed(
                    title="Up to Date",
                    message="No changes detected. No restart needed.",
                    fields=[{"name": "Git", "value": old[:12] if old else "(unknown)", "inline": True}],
                    footer=f"Triggered by {interaction.user}",
                )
            )
            return

        fields = [
            {"name": "Git", "value": f"{old[:12]} -> {new[:12]}" if old and new else "(unknown)", "inline": False},
            {"name": "Changed", "value": changed, "inline": True},
            {"name": "Backup", "value": backup[:60] + ("…" if len(backup) > 60 else ""), "inline": False},
            {"name": "Next", "value": "Restarting service to apply staged update", "inline": False},
        ]
        ok_embed = MessageHelper.create_success_embed(
            title="Staged",
            message="RSAdminBot update is staged. Restarting now to apply.",
            fields=fields,
            footer=f"Triggered by {interaction.user}",
        )
        if sample:
            ok_embed.add_field(name="Changed sample", value=f"```{chr(10).join(str(x) for x in sample[:15])[:900]}```", inline=False)
        await status.edit(embed=ok_embed)

        # Restart after sending the message. This will terminate the current process.
        try:
            subprocess.run(["sudo", "systemctl", "restart", "mirror-world-rsadminbot.service"], timeout=10)
        except Exception:
            pass

    @app_commands.command(name="details", description="Show systemd details for a bot (owner-only).")
    async def details(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="details",
            action_display="Details",
            title="🧾 Details",
            description="Pick a bot to show systemd details.",
        )

    @app_commands.command(name="logs", description="Show journal logs for a bot (owner-only).")
    @app_commands.describe(lines="Number of log lines to show (10-400).")
    async def logs(self, interaction: discord.Interaction, lines: int = 80) -> None:
        lines = max(10, min(int(lines or 80), 400))
        await self._send_bot_select(
            interaction,
            action="logs",
            action_display="Logs",
            title="📜 Logs",
            description=f"Pick a bot to show journal logs (lines={lines}).",
            action_kwargs={"lines": lines},
        )

    @app_commands.command(name="fileview", description="Show file sizes + mtimes for a bot folder (owner-only).")
    @app_commands.describe(mode="Optional: alljson to include all *.json (excluding secrets).")
    async def fileview(self, interaction: discord.Interaction, mode: str = "") -> None:
        mode = str(mode or "").strip().lower()
        await self._send_bot_select(
            interaction,
            action="fileview",
            action_display="Fileview",
            title="🗂️ Fileview",
            description="Pick a bot to list files (sizes + mtimes).",
            action_kwargs={"mode": mode},
            bot_keys=self.admin_bot._get_rs_bot_keys() + self.admin_bot._get_mw_bot_keys(),
        )

    @app_commands.command(name="botconfig", description="View a bot's config.json summary (owner-only).")
    async def botconfig(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="config",
            action_display="Config",
            title="⚙️ Bot Config",
            description="Pick a bot to view its config summary.",
            bot_keys=self.admin_bot._get_rs_bot_keys() + self.admin_bot._get_mw_bot_keys(),
        )

    @app_commands.command(name="secretsstatus", description="View/update bot secrets (masked; owner-only).")
    async def secretsstatus(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="secrets",
            action_display="Secrets",
            title="🔐 Secrets Status",
            description="Pick a bot to view masked secrets and update keys securely.",
            bot_keys=self.admin_bot._get_rs_bot_keys() + self.admin_bot._get_mw_bot_keys(),
        )

    @app_commands.command(name="commands", description="Show COMMANDS.md for a bot (owner-only).")
    async def commands(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="commands",
            action_display="Commands",
            title="📚 Commands",
            description="Pick a bot to view its COMMANDS.md.",
            bot_keys=sorted(list(self.admin_bot.BOTS.keys())),
        )

    @app_commands.command(name="botdiagnose", description="Diagnose a bot (owner-only).")
    async def botdiagnose(self, interaction: discord.Interaction) -> None:
        await self._send_bot_select(
            interaction,
            action="diagnose",
            action_display="Diagnose",
            title="🔍 Diagnose Bot",
            description="Pick a bot to run a quick diagnosis.",
        )

    @app_commands.command(name="whereami", description="Runtime proof: show where RSAdminBot is running (owner-only).")
    async def whereami(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        try:
            cwd = os.getcwd()
            file_path = str(Path(__file__).resolve())
            py_exec = sys.executable
            py_ver = platform.python_version()
            local_exec_cfg = bool((self.admin_bot.config.get("local_exec") or {}).get("enabled", True))
            local_exec = "yes" if (os.name != "nt" and local_exec_cfg) else "no"
            live_repo = str(getattr(self.admin_bot, "remote_root", "") or self.admin_bot.base_path.parent)
            code_repo = str(self.admin_bot._get_update_code_root_for_group("rs_bots") or "")

            def _git_head(path: str) -> str:
                try:
                    if not path:
                        return "unknown"
                    if not (Path(path) / ".git").exists():
                        return "no_git"
                    res = subprocess.run(["git", "-C", path, "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5)
                    if res.returncode != 0:
                        return "error"
                    return (res.stdout or "").strip()[:40] or "error"
                except Exception:
                    return "error"

            head_code = _git_head(code_repo)
            head_live = _git_head(live_repo)
            payload = "\n".join(
                [
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
            )
            embed = MessageHelper.create_info_embed(
                title="Where Am I",
                message=self.admin_bot._codeblock(payload, limit=1800),
                footer=f"Triggered by {interaction.user}",
            )
            await self.admin_bot._interaction_reply(interaction, embed=embed, ephemeral=True)
        except Exception as e:
            await self.admin_bot._interaction_reply(interaction, content=f"❌ whereami failed: {str(e)[:200]}", ephemeral=True)

    @app_commands.command(name="clear", description="Clear messages in the current channel (owner-only).")
    @app_commands.describe(include_pins="Also delete pinned messages (dangerous).")
    async def clear(self, interaction: discord.Interaction, include_pins: bool = False) -> None:
        if not await self._guard(interaction):
            return
        if not interaction.channel or not isinstance(interaction.channel, discord.TextChannel):
            await self.admin_bot._interaction_reply(interaction, content="❌ Channel must be a server text channel.", ephemeral=True)
            return

        ch: discord.TextChannel = interaction.channel
        perms = ch.permissions_for(ch.guild.me) if ch.guild and ch.guild.me else None
        if not perms or not perms.manage_messages:
            await self.admin_bot._interaction_reply(interaction, content="❌ Missing permission: Manage Messages.", ephemeral=True)
            return

        admin_bot = self.admin_bot

        class _ClearConfirmView(ui.View):
            def __init__(self):
                super().__init__(timeout=60)
                self._started = False

            async def _deny_if_needed(self, i: discord.Interaction) -> bool:
                ok, err = await admin_bot._slash_owner_guard(i)
                if not ok:
                    await admin_bot._interaction_reply(i, content=err, ephemeral=True)
                    return True
                return False

            @ui.button(label="Confirm clear", style=discord.ButtonStyle.danger)
            async def confirm(self, i: discord.Interaction, btn: ui.Button) -> None:
                if await self._deny_if_needed(i):
                    return
                if self._started:
                    await admin_bot._interaction_reply(i, content="Already running.", ephemeral=True)
                    return
                self._started = True
                for child in self.children:
                    if isinstance(child, ui.Button):
                        child.disabled = True
                await admin_bot._interaction_reply(i, content="🧹 Clearing messages… (running)", view=self, ephemeral=True)

                # Discord bulk delete rule: messages older than 14 days cannot be bulk-deleted.
                BULK_DELETE_MAX_AGE_DAYS = 14
                cutoff = discord.utils.utcnow() - timedelta(days=BULK_DELETE_MAX_AGE_DAYS)

                total = 0
                bulk = 0
                single = 0
                skipped = 0
                failures = 0

                # We delete newest-first.
                recent_batch: List[discord.Message] = []
                try:
                    async for msg in ch.history(limit=None, oldest_first=False):
                        try:
                            if msg.pinned and not include_pins:
                                skipped += 1
                                continue
                            # Never try to delete system messages that are not deletable
                            if not getattr(msg, "deletable", True):
                                skipped += 1
                                continue
                        except Exception:
                            pass

                        created = getattr(msg, "created_at", None)
                        is_recent = bool(created and created.replace(tzinfo=timezone.utc) >= cutoff)

                        if is_recent:
                            recent_batch.append(msg)
                            if len(recent_batch) >= 100:
                                try:
                                    await ch.delete_messages(recent_batch)
                                    bulk += len(recent_batch)
                                    total += len(recent_batch)
                                except Exception:
                                    for m in recent_batch:
                                        try:
                                            await m.delete()
                                            single += 1
                                            total += 1
                                        except Exception:
                                            failures += 1
                                recent_batch = []
                        else:
                            # Flush any remaining recent batch before individual deletes.
                            if recent_batch:
                                try:
                                    await ch.delete_messages(recent_batch)
                                    bulk += len(recent_batch)
                                    total += len(recent_batch)
                                except Exception:
                                    for m in recent_batch:
                                        try:
                                            await m.delete()
                                            single += 1
                                            total += 1
                                        except Exception:
                                            failures += 1
                                recent_batch = []

                            try:
                                await msg.delete()
                                single += 1
                                total += 1
                            except Exception:
                                failures += 1

                        # Progress ping every ~50 deletions (ephemeral edit)
                        if total and total % 50 == 0:
                            await admin_bot._interaction_reply(
                                i,
                                content=f"🧹 Clearing… deleted={total} (bulk={bulk}, single={single}) skipped={skipped} failures={failures}",
                                ephemeral=True,
                            )
                    # Flush any remaining recent batch at the end.
                    if recent_batch:
                        try:
                            await ch.delete_messages(recent_batch)
                            bulk += len(recent_batch)
                            total += len(recent_batch)
                        except Exception:
                            for m in recent_batch:
                                try:
                                    await m.delete()
                                    single += 1
                                    total += 1
                                except Exception:
                                    failures += 1
                except Exception as e:
                    await admin_bot._interaction_reply(i, content=f"❌ Clear failed: {str(e)[:200]}", ephemeral=True)
                    return

                await admin_bot._interaction_reply(
                    i,
                    content=f"✅ Clear complete. deleted={total} (bulk={bulk}, single={single}) skipped={skipped} failures={failures}",
                    ephemeral=True,
                )

            @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, i: discord.Interaction, btn: ui.Button) -> None:
                if await self._deny_if_needed(i):
                    return
                for child in self.children:
                    if isinstance(child, ui.Button):
                        child.disabled = True
                await admin_bot._interaction_reply(i, content="Cancelled.", view=self, ephemeral=True)

        warn = (
            f"Channel: {ch.mention}\n"
            f"- This will delete messages in this channel.\n"
            f"- Bulk-delete works only for messages newer than ~14 days; older messages are deleted one-by-one.\n"
            f"- Pinned messages will {'also be deleted' if include_pins else 'be kept'}.\n\n"
            f"Proceed?"
        )
        await admin_bot._interaction_reply(interaction, content=warn, view=_ClearConfirmView(), ephemeral=True)


class RSAdminBot:
    """Main admin bot class"""
    
    # Bot definitions - Matched with BOT_SSH_COMMANDS_COMPLETE.md
    BOTS = {
        "datamanagerbot": {
            "name": "DataManager Bot",
            "service": "mirror-world-datamanagerbot.service",
            "folder": "MWDataManagerBot",
            "script": "datamanagerbot.py"  # For pkill command
        },
        "discumbot": {
            "name": "Discum Bot",
            "service": "mirror-world-discumbot.service",
            "folder": "MWDiscumBot",
            "script": "discumbot.py"  # For pkill command
        },
        "pingbot": {
            "name": "Ping Bot",
            "service": "mirror-world-pingbot.service",
            "folder": "MWPingBot",
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
        # Server-only secrets (merged on top of config.json by load_config_with_secrets)
        self.secrets_path = self.base_path / "config.secrets.json"
        self.config: Dict[str, Any] = {}
        
        self.load_config()

        self._whop_webhook_runner: Optional[web.AppRunner] = None
        self._whop_webhook_site: Optional[web.TCPSite] = None
        self._whop_webhook_lock: asyncio.Lock = asyncio.Lock()

        # Load SSH server config (must exist before logger init; logger may reference current_server)
        self.servers: List[Dict[str, Any]] = []
        self.current_server: Optional[Dict[str, Any]] = None

        # Canonical owner: Logging
        # Initialize after config load so it can read logging settings.
        self.logger: Optional[CommandLogger] = CommandLogger(self)
        try:
            self.logger.log_config_validation(
                "config_load",
                "valid",
                "Configuration loaded successfully",
                {"config_path": str(self.config_path)},
            )
        except Exception:
            pass
        
        # Validate required config
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            try:
                if self.logger:
                    self.logger.log_config_validation(
                        "bot_token",
                        "missing",
                        "bot_token is required in config.secrets.json (server-only)",
                        {},
                    )
            except Exception:
                pass
            sys.exit(1)
        
        # Load SSH server config (canonical: oraclekeys/servers.json + ssh_server_name selector)
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
        
        # Optional modules initialized in on_ready (after bot is created)
        self.test_server_organizer: Optional[Any] = None

        # Removed suites (Whop tracking + bot movement tracking).
        # Keep these attributes present (as None) so legacy code paths don't crash during the slash-only migration.
        self.whop_tracker: Optional[Any] = None
        self.bot_movement_tracker: Optional[Any] = None
        
        # Monitor channel mappings (per-bot channels in test server)
        self._bot_monitor_channel_ids: Dict[str, int] = {}
        self._monitor_category_id: Optional[int] = None
        self._last_service_snapshot: Dict[str, Tuple[str, int]] = {}  # {bot_key: (state, pid)}

        # Journal live streaming + webhooks (test server only; webhooks are stored server-side in config.secrets.json)
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._journal_channel_ids: Dict[str, int] = {}
        self._journal_webhook_urls_by_bot: Dict[str, str] = {}
        self._systemd_events_webhook_url: str = ""
        self._journal_tasks: Dict[str, asyncio.Task] = {}
        self._journal_last_alert_ts: Dict[str, float] = {}
        
        # Setup bot with required intents
        intents = discord.Intents.default()
        # Prefix commands are normally disabled; however we keep a small, RS-guild-only
        # prefix surface for !delete / !transfer / !archive (per operator request).
        # This requires Message Content intent to be enabled for the bot in the Discord dev portal.
        intents.message_content = True
        intents.guilds = True
        intents.members = True  # For admin commands
        
        # Primary interface: slash commands (ephemeral; test-server; owner-only).
        # Optional: RSNotes adds `/rsnote` (also synced to test server only).
        self.bot = commands.Bot(command_prefix='!', intents=intents)

        # Ensure RSNotes slash command registration runs once per process start.
        # setup_hook runs after login and before the gateway is connected (discord.py best-practice spot).
        async def _mw_setup_hook():
            try:
                await self._initialize_rsnotes()
                await self._initialize_admin_slash_commands()
            except Exception as e:
                try:
                    print(f"{Colors.YELLOW}[RSNotes] setup_hook init failed: {type(e).__name__}: {str(e)[:200]}{Colors.RESET}")
                except Exception:
                    pass

        # Only set if not already customized elsewhere.
        if not hasattr(self.bot, "setup_hook") or getattr(self.bot.setup_hook, "__name__", "") == "setup_hook":
            self.bot.setup_hook = _mw_setup_hook  # type: ignore
        
        self._setup_events()
        self._setup_rs_prefix_commands()
    
    def _setup_rs_prefix_commands(self) -> None:
        """Register RS-guild-only prefix commands: !delete !transfer !archive.

        All other admin operations remain slash-only and are synced only to neo-test-server.
        """

        def _rs_guild_id() -> int:
            try:
                return int(self.config.get("rs_server_guild_id") or 0)
            except Exception:
                return 0

        def _is_allowed_ctx(ctx: commands.Context) -> bool:
            try:
                gid = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
            except Exception:
                gid = 0
            rs_gid = _rs_guild_id()
            return bool(rs_gid and gid == rs_gid)

        def _is_allowed_author(ctx: commands.Context) -> bool:
            if not ctx or not getattr(ctx, "guild", None) or not getattr(ctx, "author", None):
                return False
            try:
                owner_id = int(getattr(ctx.guild, "owner_id", 0) or 0)
                user_id = int(getattr(ctx.author, "id", 0) or 0)
                if owner_id and user_id == owner_id:
                    return True
            except Exception:
                pass
            try:
                if isinstance(ctx.author, discord.Member) and self.is_admin(ctx.author, allow_administrator_permission=False):
                    return True
            except Exception:
                pass
            return False

        async def _deny(ctx: commands.Context, msg: str) -> None:
            try:
                m = await ctx.send(msg)
                try:
                    await asyncio.sleep(8)
                    await m.delete()
                except Exception:
                    pass
            except Exception:
                pass

        async def _guard(ctx: commands.Context) -> bool:
            if not _is_allowed_ctx(ctx):
                await _deny(ctx, "❌ This command is only enabled in **Reselling Secrets**.")
                return False
            if not _is_allowed_author(ctx):
                await _deny(ctx, "❌ Owner/Admin-only command.")
                return False
            return True

        class _ConfirmView(ui.View):
            def __init__(self, *, author_id: int, on_confirm):
                super().__init__(timeout=45)
                self.author_id = int(author_id or 0)
                self._on_confirm = on_confirm
                self._started = False

            async def interaction_check(self, interaction: discord.Interaction) -> bool:
                try:
                    return int(getattr(getattr(interaction, "user", None), "id", 0) or 0) == self.author_id
                except Exception:
                    return False

            @ui.button(label="Confirm", style=discord.ButtonStyle.danger)
            async def confirm_btn(self, interaction: discord.Interaction, button: ui.Button) -> None:
                if self._started:
                    return
                self._started = True
                for child in self.children:
                    if isinstance(child, ui.Button):
                        child.disabled = True
                try:
                    await interaction.response.edit_message(view=self)
                except Exception:
                    pass
                try:
                    await self._on_confirm(interaction)
                except Exception:
                    pass

            @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel_btn(self, interaction: discord.Interaction, button: ui.Button) -> None:
                for child in self.children:
                    if isinstance(child, ui.Button):
                        child.disabled = True
                try:
                    await interaction.response.edit_message(content="Cancelled.", view=self)
                except Exception:
                    pass

        async def _archive_replay(
            *,
            interaction: discord.Interaction,
            src: discord.TextChannel,
            cat: discord.CategoryChannel,
            mode: str,
            delay_ms: int,
        ) -> None:
            guild = src.guild
            me = guild.me
            if not me:
                await interaction.followup.send("❌ Cannot resolve bot member in this guild.", ephemeral=False)
                return
            if not src.permissions_for(me).read_message_history:
                await interaction.followup.send("❌ Missing permission: Read Message History.", ephemeral=False)
                return
            if not src.permissions_for(me).manage_channels:
                await interaction.followup.send("❌ Missing permission: Manage Channels.", ephemeral=False)
                return
            if not src.permissions_for(me).manage_webhooks:
                await interaction.followup.send("❌ Missing permission: Manage Webhooks.", ephemeral=False)
                return

            base = f"arch-{src.name}".lower()
            safe = "".join(ch if (ch.isalnum() or ch == "-") else "-" for ch in base).strip("-")
            safe = safe[:90] or "arch-channel"
            dest_name = safe
            n = 1
            while discord.utils.get(guild.text_channels, name=dest_name):
                n += 1
                dest_name = f"{safe}-{n}"[:100]

            try:
                dest = await guild.create_text_channel(name=dest_name, category=cat, reason=f"Archived by {interaction.user} via RSAdminBot")
                await dest.edit(sync_permissions=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to create archive channel: {str(e)[:200]}", ephemeral=False)
                return

            try:
                webhook = await dest.create_webhook(name="RSAdminBot Archive Mirror", reason="Mirror archive webhook")
            except Exception as e:
                await interaction.followup.send(f"❌ Failed to create webhook: {str(e)[:200]}", ephemeral=False)
                return

            await dest.send(
                embed=discord.Embed(
                    title="🗄️ Mirror Archive Started",
                    description=(
                        f"**Source:** {src.mention}\n"
                        f"**Archived By:** {interaction.user.mention}\n"
                        f"Note: Discord cannot backdate timestamps; original timestamps are appended to each message."
                    ),
                    color=discord.Color.blurple(),
                    timestamp=discord.utils.utcnow(),
                )
            )

            replayed = 0
            failed = 0
            async for msg in src.history(limit=None, oldest_first=True):
                try:
                    author_name = getattr(msg.author, "display_name", None) or getattr(msg.author, "name", "Unknown")
                    avatar_url = None
                    try:
                        avatar_url = msg.author.display_avatar.url
                    except Exception:
                        avatar_url = None

                    ts = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                    content = (msg.content or "").strip()
                    stamp = f"`(original: {ts})`"
                    if content:
                        content = f"{content}\n{stamp}"
                    else:
                        content = stamp
                    if len(content) > 1900:
                        content = content[:1800] + "\n…(truncated)…\n" + stamp

                    embeds_to_send: List[discord.Embed] = []
                    for e in (msg.embeds or [])[:6]:
                        try:
                            embeds_to_send.append(e)
                        except Exception:
                            pass

                    file_links: List[str] = []
                    image_urls: List[str] = []
                    for att in (msg.attachments or []):
                        ct = str(getattr(att, "content_type", "") or "").lower()
                        fn = str(getattr(att, "filename", "") or "").lower()
                        is_img = ct.startswith("image/") or fn.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
                        if is_img:
                            image_urls.append(att.url)
                        else:
                            file_links.append(att.url)
                    if file_links:
                        content += "\n\n**Attachments:**\n" + "\n".join(file_links[:10])
                    for u in image_urls[:4]:
                        em = discord.Embed()
                        em.set_image(url=u)
                        embeds_to_send.append(em)

                    if getattr(msg, "stickers", None):
                        st_lines: List[str] = []
                        for st in (msg.stickers or []):
                            u = getattr(st, "url", None)
                            st_lines.append(str(u or getattr(st, "name", "sticker")))
                        if st_lines:
                            content += "\n\n**Stickers:**\n" + "\n".join(st_lines[:10])

                    if msg.reference and msg.reference.message_id:
                        content = f"↪️ *replying to message ID {msg.reference.message_id}*\n" + content

                    await webhook.send(
                        content=content,
                        username=author_name,
                        avatar_url=avatar_url,
                        embeds=embeds_to_send[:10] if embeds_to_send else None,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    replayed += 1
                except Exception:
                    failed += 1
                if delay_ms:
                    await asyncio.sleep(delay_ms / 1000.0)
                if replayed and replayed % 100 == 0:
                    try:
                        await dest.send(f"…progress… replayed={replayed} failed={failed}")
                    except Exception:
                        pass

            await dest.send(
                embed=discord.Embed(
                    title="✅ Mirror Archive Completed",
                    description=f"Replayed: **{replayed}**\nFailed: **{failed}**",
                    color=discord.Color.green(),
                    timestamp=discord.utils.utcnow(),
                )
            )

            try:
                if mode == "delete":
                    await asyncio.sleep(1)
                    await src.delete(reason=f"Mirror archived by {interaction.user} via RSAdminBot → {dest.id}")
                else:
                    overwrites = src.overwrites_for(guild.default_role)
                    overwrites.send_messages = False
                    await src.set_permissions(guild.default_role, overwrite=overwrites, reason="Locked after mirror archive")
                    await src.edit(category=cat, sync_permissions=True, reason="Moved after mirror archive")
            except Exception as e:
                try:
                    await dest.send(f"⚠️ Post-archive source handling failed: `{str(e)[:200]}`")
                except Exception:
                    pass

        @self.bot.command(name="delete")
        async def _cmd_delete(ctx: commands.Context, channel: Optional[discord.TextChannel] = None) -> None:
            if not await _guard(ctx):
                return
            if not ctx.guild:
                return
            
            # If channel is provided, use it directly
            if channel and isinstance(channel, discord.TextChannel):
                ch = channel
                me = ctx.guild.me
                if not me or not ch.permissions_for(me).manage_channels:
                    await _deny(ctx, "❌ Missing permission: Manage Channels.")
                    return

                async def _do(interaction: discord.Interaction) -> None:
                    try:
                        await interaction.followup.send(f"🗑️ Deleting {ch.mention}…", ephemeral=False)
                    except Exception:
                        pass
                    try:
                        await ch.delete(reason=f"Deleted by {ctx.author} via RSAdminBot")
                    except Exception as e:
                        try:
                            await interaction.followup.send(f"❌ Delete failed: {str(e)[:200]}", ephemeral=False)
                        except Exception:
                            pass

                embed = discord.Embed(
                    title="Confirm delete",
                    description=f"Delete {ch.mention}? This cannot be undone.",
                    color=discord.Color.red(),
                )
                await ctx.send(embed=embed, view=_ConfirmView(author_id=int(ctx.author.id), on_confirm=_do))
                return
            
            # Show channel selection dropdown
            class _DeleteChannelView(ui.View):
                def __init__(self):
                    super().__init__(timeout=180)
                    self.selected_channel_id: Optional[int] = None
                    
                    channels = [ch for ch in ctx.guild.channels if isinstance(ch, discord.TextChannel)]
                    channels = sorted(channels, key=lambda x: x.position)
                    opts: List[discord.SelectOption] = []
                    for ch in channels[:25]:
                        opts.append(discord.SelectOption(
                            label=str(ch.name)[:100] or "channel",
                            value=str(ch.id),
                            description=f"#{ch.name}"
                        ))
                    if opts:
                        self.channel_select = ui.Select(placeholder="Select channel to delete…", options=opts, min_values=1, max_values=1)
                        self.channel_select.callback = self.on_channel_selected
                        self.add_item(self.channel_select)
                    else:
                        self.channel_select = None
                    
                    self._refresh_buttons()
                
                async def interaction_check(self, interaction: discord.Interaction) -> bool:
                    try:
                        return int(getattr(getattr(interaction, "user", None), "id", 0) or 0) == int(ctx.author.id)
                    except Exception:
                        return False
                
                def _refresh_buttons(self) -> None:
                    self.confirm_btn.disabled = not bool(self.selected_channel_id)  # type: ignore[attr-defined]
                
                async def on_channel_selected(self, i: discord.Interaction):
                    try:
                        self.selected_channel_id = int(self.channel_select.values[0])
                    except Exception:
                        self.selected_channel_id = None
                    self._refresh_buttons()
                    await i.response.edit_message(view=self)
                
                @ui.button(label="Confirm Delete", style=discord.ButtonStyle.danger)
                async def confirm_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    if not self.selected_channel_id:
                        return
                    ch = ctx.guild.get_channel(self.selected_channel_id)
                    if not isinstance(ch, discord.TextChannel):
                        try:
                            await i.response.send_message("❌ Channel not found.", ephemeral=True)
                        except Exception:
                            pass
                        return
                    
                    me = ctx.guild.me
                    if not me or not ch.permissions_for(me).manage_channels:
                        try:
                            await i.response.send_message("❌ Missing permission: Manage Channels.", ephemeral=True)
                        except Exception:
                            pass
                        return
                    
                    for child in self.children:
                        if isinstance(child, ui.Button) or isinstance(child, ui.Select):
                            child.disabled = True
                    try:
                        await i.response.edit_message(content="🗑️ Deleting channel…", view=self)
                    except Exception:
                        pass
                    try:
                        await ch.delete(reason=f"Deleted by {ctx.author} via RSAdminBot")
                        await i.followup.send(f"✅ Deleted {ch.name}", ephemeral=False)
                    except Exception as e:
                        try:
                            await i.followup.send(f"❌ Delete failed: {str(e)[:200]}", ephemeral=False)
                        except Exception:
                            pass
                
                @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                async def cancel_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    for child in self.children:
                        if isinstance(child, ui.Button) or isinstance(child, ui.Select):
                            child.disabled = True
                    await i.response.edit_message(content="Cancelled.", view=self)
            
            embed = discord.Embed(
                title="🗑️ Delete Channel",
                description="Select a channel from the dropdown to delete.",
                color=discord.Color.red(),
            )
            await ctx.send(embed=embed, view=_DeleteChannelView())

        @self.bot.command(name="transfer")
        async def _cmd_transfer(ctx: commands.Context, *args: str) -> None:
            if not await _guard(ctx):
                return
            if not ctx.guild:
                return
            if not isinstance(getattr(ctx, "channel", None), discord.TextChannel):
                await _deny(ctx, "❌ Run this in a server text channel.")
                return
            
            # Legacy support: if args provided, try to parse them
            if args:
                conv_ch = commands.TextChannelConverter()
                conv_cat = commands.CategoryChannelConverter()
                channel: discord.TextChannel = ctx.channel  # type: ignore[assignment]
                cat: Optional[discord.CategoryChannel] = None

                raw = list(args)
                if len(raw) >= 2:
                    try:
                        channel = await conv_ch.convert(ctx, raw[0])
                        raw = raw[1:]
                    except Exception:
                        channel = ctx.channel  # type: ignore[assignment]

                target = " ".join(raw).strip()
                try:
                    cat = await conv_cat.convert(ctx, target)
                except Exception:
                    cat = None
                if cat is None:
                    try:
                        if target.isdigit():
                            c = ctx.guild.get_channel(int(target))
                            if isinstance(c, discord.CategoryChannel):
                                cat = c
                    except Exception:
                        cat = None
                if cat is not None:
                    me = ctx.guild.me
                    if not me or not channel.permissions_for(me).manage_channels:
                        await _deny(ctx, "❌ Missing permission: Manage Channels.")
                        return

                    async def _do(interaction: discord.Interaction) -> None:
                        try:
                            await interaction.followup.send(f"📦 Moving {channel.mention} → **{cat.name}**…", ephemeral=False)
                        except Exception:
                            pass
                        try:
                            await channel.edit(category=cat, sync_permissions=True, reason=f"Transferred by {ctx.author} via RSAdminBot")
                        except Exception as e:
                            try:
                                await interaction.followup.send(f"❌ Transfer failed: {str(e)[:200]}", ephemeral=False)
                            except Exception:
                                pass

                    embed = discord.Embed(
                        title="Confirm transfer",
                        description=f"Move {channel.mention} into category **{cat.name}**?",
                        color=discord.Color.orange(),
                    )
                    await ctx.send(embed=embed, view=_ConfirmView(author_id=int(ctx.author.id), on_confirm=_do))
                    return
            
            # Interactive UI with dropdowns and search
            class _CategorySearchModal(ui.Modal, title="Search Category"):
                search_input = ui.TextInput(label="Category Name", placeholder="Type to search...", required=True, max_length=100)
                
                def __init__(self, view_instance):
                    super().__init__()
                    self.view_instance = view_instance
                
                async def on_submit(self, interaction: discord.Interaction):
                    search_term = self.search_input.value.lower().strip()
                    if not search_term:
                        await interaction.response.send_message("❌ Please enter a search term.", ephemeral=True)
                        return
                    
                    # Filter categories by search term
                    all_cats = [c for c in ctx.guild.categories if isinstance(c, discord.CategoryChannel)]
                    filtered = [c for c in all_cats if search_term in c.name.lower()]
                    filtered = sorted(filtered, key=lambda x: x.position)[:25]
                    
                    if not filtered:
                        await interaction.response.send_message(f"❌ No categories found matching '{search_term}'.", ephemeral=True)
                        return
                    
                    # Update category select options
                    opts: List[discord.SelectOption] = []
                    for cat in filtered:
                        opts.append(discord.SelectOption(
                            label=str(cat.name)[:100] or "category",
                            value=str(cat.id),
                            description=f"Category: {cat.name}"
                        ))
                    
                    # Remove old category select and add new one
                    for item in list(self.view_instance.children):
                        if isinstance(item, ui.Select) and item.placeholder and "category" in item.placeholder.lower():
                            self.view_instance.remove_item(item)
                    
                    self.view_instance.category_select = ui.Select(
                        placeholder=f"Select category (filtered: {len(filtered)})…",
                        options=opts,
                        min_values=1,
                        max_values=1
                    )
                    self.view_instance.category_select.callback = self.view_instance.on_category_selected
                    self.view_instance.add_item(self.view_instance.category_select)
                    
                    await interaction.response.edit_message(view=self.view_instance)
            
            class _TransferView(ui.View):
                def __init__(self):
                    super().__init__(timeout=300)
                    self.selected_channel_id: Optional[int] = None
                    self.selected_category_id: Optional[int] = None
                    
                    # Channel select
                    channels = [ch for ch in ctx.guild.channels if isinstance(ch, discord.TextChannel)]
                    channels = sorted(channels, key=lambda x: x.position)
                    channel_opts: List[discord.SelectOption] = []
                    for ch in channels[:25]:
                        channel_opts.append(discord.SelectOption(
                            label=str(ch.name)[:100] or "channel",
                            value=str(ch.id),
                            description=f"#{ch.name}"
                        ))
                    if channel_opts:
                        self.channel_select = ui.Select(placeholder="Select channel to transfer…", options=channel_opts, min_values=1, max_values=1)
                        self.channel_select.callback = self.on_channel_selected
                        self.add_item(self.channel_select)
                    else:
                        self.channel_select = None
                    
                    # Category select
                    categories = [c for c in ctx.guild.categories if isinstance(c, discord.CategoryChannel)]
                    categories = sorted(categories, key=lambda x: x.position)
                    category_opts: List[discord.SelectOption] = []
                    for cat in categories[:25]:
                        category_opts.append(discord.SelectOption(
                            label=str(cat.name)[:100] or "category",
                            value=str(cat.id),
                            description=f"Category: {cat.name}"
                        ))
                    if category_opts:
                        self.category_select = ui.Select(placeholder="Select category…", options=category_opts, min_values=1, max_values=1)
                        self.category_select.callback = self.on_category_selected
                        self.add_item(self.category_select)
                    else:
                        self.category_select = None
                    
                    self._refresh_buttons()
                
                async def interaction_check(self, interaction: discord.Interaction) -> bool:
                    try:
                        return int(getattr(getattr(interaction, "user", None), "id", 0) or 0) == int(ctx.author.id)
                    except Exception:
                        return False
                
                def _refresh_buttons(self) -> None:
                    self.confirm_btn.disabled = not bool(self.selected_channel_id and self.selected_category_id)  # type: ignore[attr-defined]
                
                async def on_channel_selected(self, i: discord.Interaction):
                    try:
                        self.selected_channel_id = int(self.channel_select.values[0])
                    except Exception:
                        self.selected_channel_id = None
                    self._refresh_buttons()
                    if self.selected_channel_id and self.selected_category_id:
                        await i.response.defer()
                        await self._perform_transfer(i)
                    else:
                        await i.response.send_message(f"✅ Channel selected. Now select a category.", ephemeral=True)
                
                async def on_category_selected(self, i: discord.Interaction):
                    try:
                        self.selected_category_id = int(self.category_select.values[0])
                    except Exception:
                        self.selected_category_id = None
                    self._refresh_buttons()
                    if self.selected_channel_id and self.selected_category_id:
                        await i.response.defer()
                        await self._perform_transfer(i)
                    else:
                        await i.response.send_message(f"✅ Category selected. Now select a channel.", ephemeral=True)
                
                @ui.button(label="🔍 Search Category", style=discord.ButtonStyle.secondary)
                async def search_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    await i.response.send_modal(_CategorySearchModal(self))
                
                @ui.button(label="Confirm Transfer", style=discord.ButtonStyle.success)
                async def confirm_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    if not self.selected_channel_id or not self.selected_category_id:
                        return
                    await i.response.defer()
                    await self._perform_transfer(i)
                
                @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                async def cancel_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    for child in self.children:
                        if isinstance(child, ui.Button) or isinstance(child, ui.Select):
                            child.disabled = True
                    await i.response.edit_message(content="Cancelled.", view=self)
                
                async def _perform_transfer(self, interaction: discord.Interaction):
                    try:
                        channel = ctx.guild.get_channel(self.selected_channel_id)
                        category = ctx.guild.get_channel(self.selected_category_id)
                        
                        if not channel or not isinstance(channel, discord.TextChannel):
                            await interaction.followup.send("❌ Channel not found", ephemeral=False)
                            return
                        
                        if not category or not isinstance(category, discord.CategoryChannel):
                            await interaction.followup.send("❌ Category not found", ephemeral=False)
                            return
                        
                        me = ctx.guild.me
                        if not me or not channel.permissions_for(me).manage_channels:
                            await interaction.followup.send("❌ Missing permission: Manage Channels.", ephemeral=False)
                            return
                        
                        for child in self.children:
                            if isinstance(child, ui.Button) or isinstance(child, ui.Select):
                                child.disabled = True
                        try:
                            await interaction.edit_original_response(content="📦 Transferring channel…", view=self)
                        except Exception:
                            pass
                        
                        await channel.edit(category=category, sync_permissions=True, reason=f"Transferred by {ctx.author} via RSAdminBot")
                        await interaction.followup.send(f"✅ Moved {channel.mention} → **{category.name}**", ephemeral=False)
                    except discord.Forbidden:
                        await interaction.followup.send("❌ I don't have permission to edit this channel", ephemeral=False)
                    except Exception as e:
                        await interaction.followup.send(f"❌ Transfer failed: {str(e)[:200]}", ephemeral=False)
            
            embed = discord.Embed(
                title="📦 Transfer Channel",
                description="Select a channel and category from the dropdowns, or use the search button to find a category.",
                color=discord.Color.orange(),
            )
            await ctx.send(embed=embed, view=_TransferView())

        @self.bot.command(name="archive")
        async def _cmd_archive(ctx: commands.Context) -> None:
            if not await _guard(ctx):
                return
            if not ctx.guild or not isinstance(getattr(ctx, "channel", None), discord.TextChannel):
                await _deny(ctx, "❌ Run this in a server text channel.")
                return
            src: discord.TextChannel = ctx.channel  # type: ignore[assignment]

            cfg = self.config.get("archive") if isinstance(self.config, dict) else {}
            if not isinstance(cfg, dict):
                cfg = {}
            delay_ms = int(cfg.get("replay_delay_ms") or 350)
            delay_ms = max(0, min(delay_ms, 2000))

            class _ArchiveCategorySearchModal(ui.Modal, title="Search Archive Category"):
                search_input = ui.TextInput(label="Category Name", placeholder="Type to search...", required=True, max_length=100)
                
                def __init__(self, view_instance):
                    super().__init__()
                    self.view_instance = view_instance
                
                async def on_submit(self, interaction: discord.Interaction):
                    search_term = self.search_input.value.lower().strip()
                    if not search_term:
                        await interaction.response.send_message("❌ Please enter a search term.", ephemeral=True)
                        return
                    
                    # Filter categories by search term
                    all_cats = [c for c in ctx.guild.categories if isinstance(c, discord.CategoryChannel)]
                    filtered = [c for c in all_cats if search_term in c.name.lower()]
                    filtered = sorted(filtered, key=lambda x: x.position)[:25]
                    
                    if not filtered:
                        await interaction.response.send_message(f"❌ No categories found matching '{search_term}'.", ephemeral=True)
                        return
                    
                    # Update category select options
                    opts: List[discord.SelectOption] = []
                    for cat in filtered:
                        opts.append(discord.SelectOption(
                            label=str(cat.name)[:100] or "category",
                            value=str(cat.id),
                            description=f"Category: {cat.name}"
                        ))
                    
                    # Remove old category select and add new one
                    for item in list(self.view_instance.children):
                        if isinstance(item, ui.Select) and item.placeholder and "category" in item.placeholder.lower():
                            self.view_instance.remove_item(item)
                    
                    self.view_instance.category_select = ui.Select(
                        placeholder=f"Select archive category (filtered: {len(filtered)})…",
                        options=opts,
                        min_values=1,
                        max_values=1
                    )
                    self.view_instance.category_select.callback = self.view_instance.on_category_selected
                    self.view_instance.add_item(self.view_instance.category_select)
                    
                    await interaction.response.edit_message(view=self.view_instance)

            class _ArchiveView(ui.View):
                def __init__(self):
                    super().__init__(timeout=180)
                    self.category_id: Optional[int] = None
                    self.mode: str = ""  # "lock_move" | "delete"

                    cats = [c for c in (ctx.guild.categories or []) if isinstance(c, discord.CategoryChannel)]
                    cats = sorted(cats, key=lambda c: c.position)
                    opts: List[discord.SelectOption] = []
                    for c in cats[:25]:
                        opts.append(discord.SelectOption(label=str(c.name)[:100] or "category", value=str(c.id), description=f"id={c.id}"))
                    self.category_select = ui.Select(placeholder="Select archive category…", options=opts, min_values=1, max_values=1)
                    self.category_select.callback = self.on_category_selected
                    self.add_item(self.category_select)
                    self._refresh_buttons()

                async def interaction_check(self, interaction: discord.Interaction) -> bool:
                    try:
                        return int(getattr(getattr(interaction, "user", None), "id", 0) or 0) == int(ctx.author.id)
                    except Exception:
                        return False

                def _refresh_buttons(self) -> None:
                    has_cat = bool(self.category_id)
                    self.archive_lock_move.disabled = not has_cat  # type: ignore[attr-defined]
                    self.archive_delete.disabled = not has_cat  # type: ignore[attr-defined]
                    self.start_btn.disabled = not bool(self.category_id and self.mode)  # type: ignore[attr-defined]

                async def on_category_selected(self, i: discord.Interaction):
                    try:
                        self.category_id = int(self.category_select.values[0])
                    except Exception:
                        self.category_id = None
                    self._refresh_buttons()
                    await i.response.edit_message(view=self)
                
                @ui.button(label="🔍 Search Category", style=discord.ButtonStyle.secondary)
                async def search_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    await i.response.send_modal(_ArchiveCategorySearchModal(self))

                @ui.button(label="Archive (lock + move)", style=discord.ButtonStyle.primary)
                async def archive_lock_move(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    self.mode = "lock_move"
                    self._refresh_buttons()
                    await i.response.edit_message(view=self)

                @ui.button(label="Archive (delete source)", style=discord.ButtonStyle.danger)
                async def archive_delete(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    self.mode = "delete"
                    self._refresh_buttons()
                    await i.response.edit_message(view=self)

                @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                async def cancel_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    for child in self.children:
                        if isinstance(child, ui.Button) or isinstance(child, ui.Select):
                            child.disabled = True
                    await i.response.edit_message(content="Cancelled.", view=self)

                @ui.button(label="Start archive", style=discord.ButtonStyle.success)
                async def start_btn(self, i: discord.Interaction, button: ui.Button):  # type: ignore[override]
                    if not self.category_id or not self.mode:
                        return
                    cat = ctx.guild.get_channel(int(self.category_id))
                    if not isinstance(cat, discord.CategoryChannel):
                        try:
                            await i.response.send_message("❌ Archive category not found.", ephemeral=True)
                        except Exception:
                            pass
                        return
                    for child in self.children:
                        if isinstance(child, ui.Button) or isinstance(child, ui.Select):
                            child.disabled = True
                    try:
                        await i.response.edit_message(content="📦 Starting mirror archive…", view=self)
                    except Exception:
                        pass
                    try:
                        await i.followup.send("⏳ Mirroring messages…", ephemeral=False)
                    except Exception:
                        pass
                    await _archive_replay(interaction=i, src=src, cat=cat, mode=self.mode, delay_ms=delay_ms)

            embed = discord.Embed(
                title="Mirror archive",
                description=(
                    f"Mirror-archive **#{src.name}**.\n"
                    f"Pick an archive category, choose lock+move vs delete, then click **Start archive**.\n"
                    f"Delay: {delay_ms}ms"
                ),
                color=discord.Color.blurple(),
            )
            await ctx.send(embed=embed, view=_ArchiveView())

        @self.bot.command(name="clear")
        async def _cmd_clear(ctx: commands.Context, include_pins: bool = False) -> None:
            if not await _guard(ctx):
                return
            if not ctx.guild or not isinstance(getattr(ctx, "channel", None), discord.TextChannel):
                await _deny(ctx, "❌ Run this in a server text channel.")
                return
            
            ch: discord.TextChannel = ctx.channel  # type: ignore[assignment]
            me = ctx.guild.me
            if not me or not ch.permissions_for(me).manage_messages:
                await _deny(ctx, "❌ Missing permission: Manage Messages.")
                return
            
            class _ClearConfirmView(ui.View):
                def __init__(self):
                    super().__init__(timeout=60)
                    self._started = False
                
                async def interaction_check(self, interaction: discord.Interaction) -> bool:
                    try:
                        return int(getattr(getattr(interaction, "user", None), "id", 0) or 0) == int(ctx.author.id)
                    except Exception:
                        return False
                
                @ui.button(label="Confirm clear", style=discord.ButtonStyle.danger)
                async def confirm(self, i: discord.Interaction, btn: ui.Button) -> None:
                    if self._started:
                        await i.response.send_message("Already running.", ephemeral=True)
                        return
                    self._started = True
                    for child in self.children:
                        if isinstance(child, ui.Button):
                            child.disabled = True
                    try:
                        await i.response.edit_message(content="🧹 Clearing messages… (running)", view=self)
                    except Exception:
                        pass
                    
                    # Discord bulk delete rule: messages older than 14 days cannot be bulk-deleted.
                    BULK_DELETE_MAX_AGE_DAYS = 14
                    cutoff = discord.utils.utcnow() - timedelta(days=BULK_DELETE_MAX_AGE_DAYS)
                    
                    total = 0
                    bulk = 0
                    single = 0
                    skipped = 0
                    failures = 0
                    
                    # We delete newest-first.
                    recent_batch: List[discord.Message] = []
                    try:
                        async for msg in ch.history(limit=None, oldest_first=False):
                            try:
                                if msg.pinned and not include_pins:
                                    skipped += 1
                                    continue
                                # Never try to delete system messages that are not deletable
                                if not getattr(msg, "deletable", True):
                                    skipped += 1
                                    continue
                            except Exception:
                                pass
                            
                            created = getattr(msg, "created_at", None)
                            is_recent = bool(created and created.replace(tzinfo=timezone.utc) >= cutoff)
                            
                            if is_recent:
                                recent_batch.append(msg)
                                if len(recent_batch) >= 100:
                                    try:
                                        await ch.delete_messages(recent_batch)
                                        bulk += len(recent_batch)
                                        total += len(recent_batch)
                                    except Exception:
                                        for m in recent_batch:
                                            try:
                                                await m.delete()
                                                single += 1
                                                total += 1
                                            except Exception:
                                                failures += 1
                                    recent_batch = []
                            else:
                                # Flush any remaining recent batch before individual deletes.
                                if recent_batch:
                                    try:
                                        await ch.delete_messages(recent_batch)
                                        bulk += len(recent_batch)
                                        total += len(recent_batch)
                                    except Exception:
                                        for m in recent_batch:
                                            try:
                                                await m.delete()
                                                single += 1
                                                total += 1
                                            except Exception:
                                                failures += 1
                                    recent_batch = []
                                
                                try:
                                    await msg.delete()
                                    single += 1
                                    total += 1
                                except Exception:
                                    failures += 1
                            
                            # Progress ping every ~50 deletions
                            if total and total % 50 == 0:
                                try:
                                    await i.edit_original_response(
                                        content=f"🧹 Clearing… deleted={total} (bulk={bulk}, single={single}) skipped={skipped} failures={failures}"
                                    )
                                except Exception:
                                    pass
                        # Flush any remaining recent batch at the end.
                        if recent_batch:
                            try:
                                await ch.delete_messages(recent_batch)
                                bulk += len(recent_batch)
                                total += len(recent_batch)
                            except Exception:
                                for m in recent_batch:
                                    try:
                                        await m.delete()
                                        single += 1
                                        total += 1
                                    except Exception:
                                        failures += 1
                    except Exception as e:
                        try:
                            await i.followup.send(f"❌ Clear failed: {str(e)[:200]}", ephemeral=False)
                        except Exception:
                            pass
                        return
                    
                    try:
                        await i.edit_original_response(
                            content=f"✅ Clear complete. deleted={total} (bulk={bulk}, single={single}) skipped={skipped} failures={failures}"
                        )
                    except Exception:
                        try:
                            await i.followup.send(
                                content=f"✅ Clear complete. deleted={total} (bulk={bulk}, single={single}) skipped={skipped} failures={failures}",
                                ephemeral=False
                            )
                        except Exception:
                            pass
                
                @ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
                async def cancel(self, i: discord.Interaction, btn: ui.Button) -> None:
                    for child in self.children:
                        if isinstance(child, ui.Button):
                            child.disabled = True
                    try:
                        await i.response.edit_message(content="Cancelled.", view=self)
                    except Exception:
                        pass
            
            warn = (
                f"Channel: {ch.mention}\n"
                f"- This will delete messages in this channel.\n"
                f"- Bulk-delete works only for messages newer than ~14 days; older messages are deleted one-by-one.\n"
                f"- Pinned messages will {'also be deleted' if include_pins else 'be kept'}.\n\n"
                f"Proceed?"
            )
            embed = discord.Embed(
                title="🧹 Clear Messages",
                description=warn,
                color=discord.Color.orange(),
            )
            await ctx.send(embed=embed, view=_ClearConfirmView())

    def _load_ssh_config(self):
        """Load SSH server configuration from the canonical oraclekeys/servers.json.

        Rules (CANONICAL_RULES.md):
        - Server list source of truth: <repo_root>/oraclekeys/servers.json
        - RSAdminBot/config.json should only select the server entry name (no duplicated host/user/key)
        - On the Ubuntu host itself, prefer local-exec when enabled (no SSH key needed)
        """
        try:
            # 1) Determine which server entry to use (selector only; no duplicated host/user/key).
            server_name = str(self.config.get("ssh_server_name") or "").strip()
            if not server_name:
                # Back-compat for configs that still have an ssh_server dict: accept ONLY the name selector.
                legacy = self.config.get("ssh_server")
                if isinstance(legacy, dict):
                    server_name = str(legacy.get("name") or "").strip()

            if not server_name:
                print(f"{Colors.YELLOW}[SSH] No SSH server selected (missing ssh_server_name){Colors.RESET}")
                print(f"{Colors.YELLOW}[SSH] Set ssh_server_name in RSAdminBot/config.json to a name from oraclekeys/servers.json{Colors.RESET}")
                if hasattr(self, "logger") and self.logger:
                    self.logger.log_config_validation("ssh_config", "missing", "Missing ssh_server_name (must match oraclekeys/servers.json entry name)", {})
                return

            # 2) Load canonical server list and pick the entry by exact name match.
            servers, servers_path = load_oracle_servers(self.base_path.parent)
            entry = pick_oracle_server(servers, server_name)

            host = str(entry.get("host") or "").strip()
            user = str(entry.get("user") or "").strip() or "rsadmin"
            key_value = str(entry.get("key") or "").strip()
            ssh_options = str(entry.get("ssh_options") or "").strip()

            # Optional: port (defaults to 22 if absent)
            port_val = entry.get("port", 22)
            try:
                port = int(port_val) if port_val is not None else 22
            except Exception:
                port = 22

            # Remote root can be specified per entry; otherwise derive from user.
            remote_root = str(entry.get("remote_root") or entry.get("live_root") or f"/home/{user}/bots/mirror-world").strip()
            self.remote_root = remote_root

            self.current_server = {
                "name": str(entry.get("name") or server_name),
                "host": host,
                "user": user,
                "key": key_value,
                "ssh_options": ssh_options,
                "port": port,
                "source": str(servers_path),
            }
            self.servers = servers

            # 3) Resolve key path when needed (Windows/off-box). In local-exec mode, key is not required.
            if key_value:
                key_path = resolve_oracle_ssh_key_path(key_value, self.base_path.parent)
                if key_path.exists():
                    if platform.system() == "Windows":
                        self._fix_ssh_key_permissions(key_path)
                    self.current_server["key"] = str(key_path)
                    if hasattr(self, "logger") and self.logger:
                        self.logger.log_config_validation("ssh_key", "valid", f"SSH key resolved: {key_path}", {"key_path": str(key_path)})
                else:
                    if self._should_use_local_exec():
                        # Local execution mode (Oracle Ubuntu host): key is not required.
                        self.current_server["key"] = ""
                        if hasattr(self, "logger") and self.logger:
                            self.logger.log_config_validation("ssh_key", "valid", "SSH key not required in local-exec mode", {"local_exec": True})
                    else:
                        # Key is optional for SSH (agent/default identity may be used). Never force `-i` if the file is missing.
                        self.current_server["key"] = ""
                        print(f"{Colors.YELLOW}[SSH] Warning: SSH key not found at {key_path}; continuing without -i (ssh-agent/default identity may be used){Colors.RESET}")
                        if hasattr(self, "logger") and self.logger:
                            self.logger.log_config_validation(
                                "ssh_key",
                                "missing",
                                f"SSH key not found: {key_path} (optional; continuing without -i)",
                                {"key_path": str(key_path), "optional": True},
                            )

            print(f"{Colors.GREEN}[SSH] Loaded server config from oraclekeys/servers.json: {self.current_server.get('name')}{Colors.RESET}")
            print(f"{Colors.CYAN}[SSH] Host: {host}, User: {user}, Port: {port}{Colors.RESET}")
            if self._should_use_local_exec():
                print(f"{Colors.GREEN}[Local Exec] Enabled: running management commands locally on this host{Colors.RESET}")
                if hasattr(self, "logger") and self.logger:
                    self.logger.log_config_validation("local_exec", "valid", "Local execution mode enabled", {"enabled": True, "remote_root": str(getattr(self, "remote_root", ""))})

            if hasattr(self, "logger") and self.logger:
                self.logger.log_config_validation(
                    "ssh_config",
                    "valid",
                    f"SSH config loaded: {self.current_server.get('name')}",
                    {
                        "server_name": self.current_server.get("name"),
                        "host": host,
                        "user": user,
                        "port": port,
                        "servers_path": str(servers_path),
                        "local_exec": self._should_use_local_exec(),
                    },
                )
            
        except Exception as e:
            print(f"{Colors.RED}[SSH] Failed to load SSH config: {e}{Colors.RESET}")
            import traceback
            print(f"{Colors.RED}[SSH] Traceback: {traceback.format_exc()[:200]}{Colors.RESET}")
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_config_validation("ssh_config", "invalid", f"Failed to load SSH config: {e}", {"error": str(e)})
    
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
            error_msg = "No SSH server configured (missing ssh_server_name / servers.json selection)"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.RED}[SSH Error] Set ssh_server_name in RSAdminBot/config.json to a name from oraclekeys/servers.json{Colors.RESET}")
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_config_validation("ssh_available", "invalid", error_msg, {})
            return False, error_msg

        # If we're running on Linux and the repo root exists locally, prefer local execution when
        # the SSH key is not present. This keeps RSAdminBot functional on the Ubuntu host without
        # storing private keys on the server.
        if self._should_use_local_exec():
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_config_validation("ssh_available", "valid", "SSH available via local execution", {"local_exec": True})
            return True, ""
        
        # Check SSH key (should already be resolved to absolute path in _load_ssh_config)
        ssh_key = str(self.current_server.get("key") or "").strip()
        if ssh_key:
            key_path = Path(ssh_key).expanduser()
            if not key_path.exists():
                # Key is optional; allow SSH to fall back to default identities/agent.
                self.current_server["key"] = ""
                warn_msg = f"SSH key file not found: {key_path} (optional; continuing without -i)"
                print(f"{Colors.YELLOW}[SSH] Warning: {warn_msg}{Colors.RESET}")
                if hasattr(self, 'logger') and self.logger:
                    self.logger.log_config_validation("ssh_key", "missing", warn_msg, {"key_path": str(key_path), "optional": True})
        
        effective_key = str(self.current_server.get("key") or "").strip()
        if hasattr(self, 'logger') and self.logger:
            self.logger.log_config_validation(
                "ssh_available",
                "valid",
                "SSH available and configured",
                {"key_path": effective_key if effective_key else None},
            )
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
    
    async def _log_to_discord(self, embed: discord.Embed, reply_channel: Optional[discord.TextChannel] = None):
        """Log embed message to Discord log channel.
        
        Args:
            embed: Discord embed to send
            reply_channel: Ignored (kept for backwards compatibility)
        """
        log_channel_id = "1452590450631376906"  # Hard-coded as specified
        
        try:
            # Only send to the log channel. Never echo into the invoking channel (prevents duplicates).
            log_channel = self.bot.get_channel(int(log_channel_id))
            if log_channel and getattr(log_channel, "id", None) is not None:
                await log_channel.send(embed=embed)
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

    def _get_monitor_channels_config(self) -> Dict[str, Any]:
        """Return monitor_channels config."""
        cfg = self.config.get("monitor_channels") or {}
        try:
            return {
                "enabled": bool(cfg.get("enabled", True)),
                "test_server_guild_id": int(cfg.get("test_server_guild_id", 1451275225512546497)),
                "category_name": str(cfg.get("category_name", "RS Bots Terminal Logs")),
                "channel_prefix": str(cfg.get("channel_prefix", "bot-")),
                "rs_error_channel_id": int(cfg.get("rs_error_channel_id", 1452590450631376906)),
                "ping_on_failure_user_ids": [int(uid) for uid in (cfg.get("ping_on_failure_user_ids") or []) if uid],
                "post_pid_change": bool(cfg.get("post_pid_change", False)),
                "failure_logs_lines": int(cfg.get("failure_logs_lines", 80)),
            }
        except Exception:
            return {
                "enabled": False,
                "test_server_guild_id": 1451275225512546497,
                "category_name": "RS Bots Terminal Logs",
                "channel_prefix": "bot-",
                "rs_error_channel_id": 1452590450631376906,
                "ping_on_failure_user_ids": [],
                "post_pid_change": False,
                "failure_logs_lines": 80,
            }

    def _get_systemd_events_config(self) -> Dict[str, Any]:
        """Return systemd_events config (IDs only; webhook URLs come from config.secrets.json)."""
        cfg = self.config.get("systemd_events") or {}
        try:
            return {
                "enabled": bool(cfg.get("enabled", False)),
                "test_server_channel_id": int(cfg.get("test_server_channel_id") or 0),
                "rs_server_enabled": bool(cfg.get("rs_server_enabled", False)),
                "rs_server_channel_id": int(cfg.get("rs_server_channel_id") or 0),
            }
        except Exception:
            return {
                "enabled": False,
                "test_server_channel_id": 0,
                "rs_server_enabled": False,
                "rs_server_channel_id": 0,
            }

    def _get_journal_live_config(self) -> Dict[str, Any]:
        """Return journal_live config (IDs only; webhooks are server-only secrets)."""
        cfg = self.config.get("journal_live") or {}
        try:
            return {
                "enabled": bool(cfg.get("enabled", False)),
                "category_id": int(cfg.get("category_id") or 0),
                "channel_prefix": str(cfg.get("channel_prefix") or "journal-"),
                "startup_backfill_lines": int(cfg.get("startup_backfill_lines") or 20),
                "flush_seconds": float(cfg.get("flush_seconds") or 1),
                "max_chars": int(cfg.get("max_chars") or 1800),
            }
        except Exception:
            return {
                "enabled": False,
                "category_id": 0,
                "channel_prefix": "journal-",
                "startup_backfill_lines": 20,
                "flush_seconds": 1,
                "max_chars": 1800,
            }

    def _get_webhooks_config(self) -> Dict[str, Any]:
        """Return webhooks config (merged from config.secrets.json)."""
        cfg = self.config.get("webhooks") or {}
        if not isinstance(cfg, dict):
            return {"journal_by_bot": {}, "systemd_events_url": ""}
        journal_by_bot = cfg.get("journal_by_bot") or {}
        if not isinstance(journal_by_bot, dict):
            journal_by_bot = {}
        return {
            "journal_by_bot": journal_by_bot,
            "systemd_events_url": str(cfg.get("systemd_events_url") or ""),
        }

    def _get_whop_webhook_config(self) -> Dict[str, Any]:
        """Return Whop webhook receiver config (merged from config.secrets.json)."""
        cfg = self.config.get("whop_webhook") if isinstance(self.config, dict) else {}
        cfg = cfg if isinstance(cfg, dict) else {}

        def _as_bool(v: object) -> bool:
            if isinstance(v, bool):
                return v
            return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}

        def _as_int(v: object) -> int:
            try:
                s = str(v).strip()
                return int(s) if s else 0
            except Exception:
                return 0

        def _as_str(v: object) -> str:
            return str(v or "").strip()

        return {
            "enabled": _as_bool(cfg.get("enabled", False)),
            "http_server_port": _as_int(cfg.get("http_server_port") or 0),
            "path": _as_str(cfg.get("path") or "/whop-webhook") or "/whop-webhook",
            "verify": _as_bool(cfg.get("verify", True)),
            "tolerance_seconds": _as_int(cfg.get("tolerance_seconds") or 300),
            "secret": _as_str(cfg.get("secret") or ""),
            "test_server_channel_id": _as_int(cfg.get("test_server_channel_id") or 0),
            "test_server_category_id": _as_int(cfg.get("test_server_category_id") or 0),
            "channel_name": _as_str(cfg.get("channel_name") or "whop-raw-logs") or "whop-raw-logs",
            "post_raw_payloads": _as_bool(cfg.get("post_raw_payloads", True)),
            "max_log_chars": _as_int(cfg.get("max_log_chars") or 1800),
        }

    async def _ensure_test_server_channel(
        self,
        *,
        channel_id: int,
        channel_name: str,
        category_id: int,
    ) -> Optional[discord.TextChannel]:
        """Return a test server channel, creating it by name if needed."""
        if channel_id:
            ch = self.bot.get_channel(int(channel_id))
            return ch if isinstance(ch, discord.TextChannel) else None

        try:
            gid = int(self.config.get("test_server_guild_id") or 0)
        except Exception:
            gid = 0
        if not gid:
            return None
        guild = self.bot.get_guild(gid)
        if not guild:
            return None

        # Resolve optional category
        category = None
        if category_id:
            cat = guild.get_channel(int(category_id))
            if isinstance(cat, discord.CategoryChannel):
                category = cat

        name = str(channel_name or "").strip().lower()
        if not name:
            return None

        found = discord.utils.get(guild.text_channels, name=name)
        if found and isinstance(found, discord.TextChannel):
            if category and found.category_id != category.id:
                with suppress(Exception):
                    await found.edit(category=category, reason="RSAdminBot whop webhook raw logs")
            return found

        try:
            created = await guild.create_text_channel(
                name,
                category=category,
                reason="RSAdminBot whop webhook raw logs",
            )
            return created
        except Exception:
            return None

    async def _append_whop_webhook_raw(self, record: Dict[str, Any]) -> None:
        """Append a raw webhook record to JSONL (best-effort)."""
        try:
            async with self._whop_webhook_lock:
                data_dir = self.base_path / "whop_data"
                data_dir.mkdir(parents=True, exist_ok=True)
                path = data_dir / "whop_webhook_raw_payloads.jsonl"
                line = json.dumps(record, ensure_ascii=True)
                with open(path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception:
            pass

    async def _post_whop_webhook_log(self, payload: dict, headers: dict, *, status: str) -> None:
        cfg = self._get_whop_webhook_config()
        if not cfg.get("post_raw_payloads"):
            return
        ch = await self._ensure_test_server_channel(
            channel_id=int(cfg.get("test_server_channel_id") or 0),
            channel_name=str(cfg.get("channel_name") or ""),
            category_id=int(cfg.get("test_server_category_id") or 0),
        )
        if not isinstance(ch, discord.TextChannel):
            return

        event_type = str(payload.get("type") or payload.get("event_type") or payload.get("event") or "").strip()
        event_id = str(payload.get("id") or headers.get("webhook-id") or "").strip()
        ts = str(headers.get("webhook-timestamp") or "")

        embed = MessageHelper.create_status_embed(
            title="Whop Webhook (Raw)",
            description="",
            color=discord.Color.blue() if status == "ok" else discord.Color.red(),
            fields=[
                {"name": "Status", "value": status, "inline": True},
                {"name": "Type", "value": event_type or "—", "inline": True},
                {"name": "Event ID", "value": event_id or "—", "inline": True},
                {"name": "Timestamp", "value": ts or "—", "inline": True},
            ],
            footer="RSAdminBot",
        )

        max_chars = int(cfg.get("max_log_chars") or 1800)
        raw = json.dumps(payload, ensure_ascii=True)
        if max_chars > 0:
            raw = raw[:max_chars]
        content = f"```json\n{raw}\n```" if raw else ""
        with suppress(Exception):
            await ch.send(content=content or None, embed=embed, allowed_mentions=discord.AllowedMentions.none())

    async def _handle_whop_webhook_receiver(self, request: web.Request) -> web.Response:
        cfg = self._get_whop_webhook_config()
        try:
            raw_body = await request.read()
            headers = {k.lower(): v for k, v in dict(request.headers).items()}

            ok, reason = verify_standard_webhook(
                headers,
                raw_body,
                secret=str(cfg.get("secret") or ""),
                tolerance_seconds=int(cfg.get("tolerance_seconds") or 0),
                verify=bool(cfg.get("verify", True)),
            )
            if not ok:
                log_msg = f"[WhopWebhook] Signature verification failed: {reason}"
                print(log_msg)
                return web.Response(text=f"Invalid webhook signature ({reason})", status=401)

            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except Exception:
                print("[WhopWebhook] Invalid JSON payload")
                return web.Response(text="Invalid JSON payload", status=400)

            record = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "headers": headers,
                "payload": payload,
            }
            await self._append_whop_webhook_raw(record)
            asyncio.create_task(self._post_whop_webhook_log(payload, headers, status="ok"))

            return web.Response(text="OK", status=200)
        except Exception as e:
            print(f"[WhopWebhook] Error handling webhook receiver: {e}")
            return web.Response(text=f"Error: {str(e)}", status=500)

    async def _initialize_whop_webhook_receiver(self) -> None:
        cfg = self._get_whop_webhook_config()
        if not cfg.get("enabled"):
            return
        if self._whop_webhook_runner:
            return
        port = int(cfg.get("http_server_port") or 0)
        if port <= 0:
            print("[WhopWebhook] Missing whop_webhook.http_server_port; receiver disabled")
            return
        path = str(cfg.get("path") or "/whop-webhook") or "/whop-webhook"
        app = web.Application()
        app.router.add_post(path, self._handle_whop_webhook_receiver)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        self._whop_webhook_runner = runner
        self._whop_webhook_site = site
        print(f"[WhopWebhook] Receiver listening on http://0.0.0.0:{port}{path}")

    def _load_secrets_dict(self) -> Dict[str, Any]:
        """Load RSAdminBot/config.secrets.json (server-only)."""
        try:
            if not self.secrets_path.exists():
                return {}
            data = json.loads(self.secrets_path.read_text(encoding="utf-8") or "{}")
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _merge_write_secrets(self, overlay: Dict[str, Any]) -> bool:
        """Deep-merge overlay into config.secrets.json and write atomically.

        Returns True if a write happened (best-effort), else False.
        """
        if not self._should_use_local_exec():
            # Never attempt to write secrets remotely from a non-local-exec context.
            return False
        try:
            base = self._load_secrets_dict()
            if not isinstance(base, dict):
                base = {}
            if not isinstance(overlay, dict):
                return False
            _deep_merge_dict(base, overlay)
            tmp = self.secrets_path.with_suffix(self.secrets_path.suffix + ".tmp")
            tmp.write_text(json.dumps(base, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self.secrets_path)
            try:
                os.chmod(self.secrets_path, 0o600)
            except Exception:
                pass
            return True
        except Exception:
            return False

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session and not self._http_session.closed:
            return self._http_session
        timeout = aiohttp.ClientTimeout(total=15)
        self._http_session = aiohttp.ClientSession(timeout=timeout)
        return self._http_session

    async def _send_webhook(
        self,
        url: str,
        *,
        content: Optional[str] = None,
        embed: Optional[discord.Embed] = None,
        allowed_user_ids: Optional[List[int]] = None,
    ) -> bool:
        """Send a message to a Discord webhook URL (webhooks-only delivery)."""
        u = str(url or "").strip()
        if not u:
            return False
        payload: Dict[str, Any] = {}
        if content:
            payload["content"] = str(content)[:1900]
        if embed is not None:
            try:
                payload["embeds"] = [embed.to_dict()]
            except Exception:
                pass
        # Prevent accidental mass-mentions; optionally allow explicit user mentions.
        if allowed_user_ids:
            payload["allowed_mentions"] = {"parse": [], "users": [int(x) for x in allowed_user_ids if x]}
        else:
            payload["allowed_mentions"] = {"parse": []}
        try:
            session = await self._get_http_session()
            async with session.post(u, json=payload) as resp:
                if 200 <= resp.status < 300:
                    return True
                # Best-effort: do not spam logs with webhook URL.
                return False
        except Exception:
            return False

    async def _send_systemd_event(self, bot_key: str, text: str, *, severity: str = "info", should_ping: bool = False) -> None:
        """Send a structured systemd movement/event message to the systemd events webhook (test server only)."""
        cfg = self._get_systemd_events_config()
        if not cfg.get("enabled"):
            return
        url = str(self._systemd_events_webhook_url or "").strip()
        if not url:
            # Allow config to provide it (merged secrets)
            url = str(self._get_webhooks_config().get("systemd_events_url") or "").strip()
        if not url:
            return

        title = "Systemd event"
        if severity == "error":
            title = "Systemd failure"

        details = (text or "").strip()
        # Avoid nested code fences inside an embed code block.
        details = details.replace("```", "").strip()
        # Embed field value hard-limit is 1024 chars; keep buffer for code block fences.
        if len(details) > 950:
            details = details[:950] + "\n...(truncated)"

        embed = MessageHelper.create_status_embed(
            title=title,
            description="",
            color=discord.Color.red() if severity == "error" else discord.Color.blue(),
            fields=[
                {"name": "Bot", "value": bot_key, "inline": True},
                {"name": "Severity", "value": severity, "inline": True},
                {"name": "Details", "value": f"```{details}```", "inline": False},
            ],
            footer="RSAdminBot",
        )

        ping_users = []
        if severity == "error" and should_ping:
            ping_users = list(self._get_monitor_channels_config().get("ping_on_failure_user_ids") or [])

        content = ""
        if ping_users:
            content = " ".join(f"<@{int(uid)}>" for uid in ping_users if uid)

        await self._send_webhook(url, content=content or None, embed=embed, allowed_user_ids=ping_users or None)

    @staticmethod
    def _strip_ansi(text: str) -> str:
        # Remove ANSI escape sequences (keeps Discord output clean)
        return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text or "")

    @staticmethod
    def _is_hint_banner_line(line: str) -> bool:
        s = (line or "").strip()
        return s.startswith("Hint: You are currently not seeing messages")

    @staticmethod
    def _looks_high_signal(line: str) -> bool:
        s = (line or "").lower()
        if not s:
            return False
        needles = (
            "error",
            "exception",
            "traceback",
            "failed",
            "missing",
            "not found",
            "unknown channel",
            "config missing",
            "permission",
            "denied",
        )
        return any(n in s for n in needles)

    async def _journal_follow_loop(self, bot_key: str, unit_name: str, webhook_url: str) -> None:
        """Follow journald for a unit and stream batched lines to the bot's journal channel webhook."""
        cfg = self._get_journal_live_config()
        backfill = max(0, min(int(cfg.get("startup_backfill_lines") or 20), 200))
        flush_seconds = max(0.5, min(float(cfg.get("flush_seconds") or 1), 10))
        max_chars = max(400, min(int(cfg.get("max_chars") or 1800), 1900))

        # Context window for high-signal extraction
        ctx_window = deque(maxlen=6)
        skip_hint_lines = 0
        buf: List[str] = []
        buf_chars = 0
        last_flush = time.time()

        while True:
            try:
                # Start journalctl - follow unit
                proc = await asyncio.create_subprocess_exec(
                    "journalctl",
                    "-q",
                    "-f",
                    "-o",
                    "cat",
                    "-n",
                    str(backfill),
                    "-u",
                    unit_name,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                while True:
                    if proc.stdout is None:
                        break
                    # Flush on timer even if no new lines arrive (prevents "silent channels")
                    try:
                        raw = await asyncio.wait_for(proc.stdout.readline(), timeout=flush_seconds)
                    except asyncio.TimeoutError:
                        if buf:
                            await self._flush_journal_batch(bot_key, webhook_url, buf)
                            buf = []
                            buf_chars = 0
                            last_flush = time.time()
                        continue
                    if not raw:
                        break
                    line = raw.decode(errors="replace").rstrip("\n")
                    line = self._strip_ansi(line).rstrip()
                    if not line:
                        continue

                    # Drop the multi-line hint banner if it appears
                    if skip_hint_lines > 0:
                        skip_hint_lines -= 1
                        continue
                    if self._is_hint_banner_line(line):
                        skip_hint_lines = 2
                        continue

                    ctx_window.append(line)

                    # High-signal mirror to systemd events (hybrid)
                    if self._looks_high_signal(line):
                        await self._maybe_send_journal_alert(bot_key, list(ctx_window))

                    # Add to batch buffer
                    buf.append(line)
                    buf_chars += len(line) + 1

                    now = time.time()
                    if buf and (buf_chars >= max_chars or (now - last_flush) >= flush_seconds):
                        await self._flush_journal_batch(bot_key, webhook_url, buf)
                        buf = []
                        buf_chars = 0
                        last_flush = now

                # Process ended; flush any remaining buffer
                if buf:
                    await self._flush_journal_batch(bot_key, webhook_url, buf)
                    buf = []
                    buf_chars = 0
                try:
                    proc.kill()
                except Exception:
                    pass
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                return
            except Exception:
                # Backoff on unexpected errors
                await asyncio.sleep(3)

    async def _flush_journal_batch(self, bot_key: str, webhook_url: str, lines: List[str]) -> None:
        try:
            joined = "\n".join(lines)
            if not joined:
                return
            # Keep Discord-safe size; rely on configured max_chars, but double-guard.
            if len(joined) > 1800:
                joined = joined[-1800:]
            content = f"journal: {bot_key}\n```log\n{joined}\n```"
            await self._send_webhook(webhook_url, content=content)
        except Exception:
            return

    async def _maybe_send_journal_alert(self, bot_key: str, context_lines: List[str]) -> None:
        """Mirror a condensed warning/error alert into systemd events channel (rate-limited per bot)."""
        sys_cfg = self._get_systemd_events_config()
        if not sys_cfg.get("enabled"):
            return
        url = str(self._systemd_events_webhook_url or "").strip()
        if not url:
            return

        now = time.time()
        last = float(self._journal_last_alert_ts.get(bot_key, 0.0))
        if (now - last) < 15:
            return
        self._journal_last_alert_ts[bot_key] = now

        ctx = "\n".join(context_lines[-6:])
        if len(ctx) > 900:
            ctx = ctx[-900:]
        embed = MessageHelper.create_warning_embed(
            title="Journal alert",
            message=f"High-signal log line detected for {bot_key}.",
            details=ctx,
        )
        await self._send_webhook(url, embed=embed)

    async def _initialize_rsnotes(self) -> None:
        """Load RSNotes module (private `/rsnote`) and sync app commands to configured guild(s)."""
        # If we already initialized (e.g. via setup_hook), do NOT overwrite the prior status line.
        # (We want to preserve sync results like status=sync_done.)
        if getattr(self, "_rsnotes_initialized", False):
            return

        # Always keep a short status line for end-of-startup visibility (journal-live truncates earlier logs).
        try:
            self._rsnotes_status_line = "[RSNotes] status=init_start"
        except Exception:
            pass
        self._rsnotes_initialized = True

        cfg = self.config.get("rsnotes")
        if isinstance(cfg, dict) and cfg.get("enabled") is False:
            try:
                print(f"{Colors.DIM}[RSNotes] Disabled in config (rsnotes.enabled=false){Colors.RESET}")
            except Exception:
                pass
            try:
                self._rsnotes_status_line = "[RSNotes] status=disabled"
            except Exception:
                pass
            return

        try:
            import sys
            sp0 = str(sys.path[0]) if sys.path else ""
            print(f"{Colors.CYAN}[RSNotes] Init: starting (sys.path[0]={sp0}){Colors.RESET}")
        except Exception:
            pass

        # Load extension from RSAdminBot/RSNotes (import root includes this folder).
        try:
            await self.bot.load_extension("RSNotes.rsnote")
        except commands.ExtensionAlreadyLoaded:
            pass
        except Exception as e:
            try:
                print(f"{Colors.YELLOW}[RSNotes] Failed to load extension: {type(e).__name__}: {str(e)[:200]}{Colors.RESET}")
            except Exception:
                pass
            try:
                self._rsnotes_status_line = f"[RSNotes] status=load_failed err={type(e).__name__}: {str(e)[:120]}"
            except Exception:
                pass
            return

        # RSNotes should only be available in the neo-test-server.
        # (Reselling Secrets should only have /delete /transfer /archive.)
        try:
            test_gid = int(self.config.get("test_server_guild_id") or 0)
        except Exception:
            test_gid = 0
        guild_ids: List[int] = [test_gid] if test_gid else []

        # Debug: do we actually have the command in the tree?
        try:
            tree_names = []
            try:
                tree_names = [c.name for c in (self.bot.tree.get_commands() or [])]
            except Exception:
                tree_names = []
            has_rsnote = "rsnote" in set(tree_names)
            print(f"{Colors.CYAN}[RSNotes] Extension loaded. tree.has_rsnote={has_rsnote} tree.count={len(tree_names)} guild_ids={guild_ids}{Colors.RESET}")
        except Exception:
            pass

        synced_any = False
        synced_ok: Dict[int, bool] = {}
        for gid in guild_ids:
            try:
                # `/rsnote` is defined as a global app command.
                # To make it appear quickly (guild-scope), copy global commands into this guild before syncing.
                try:
                    self.bot.tree.copy_global_to(guild=discord.Object(id=gid))
                except Exception:
                    pass
                synced = await self.bot.tree.sync(guild=discord.Object(id=gid))
                synced_any = True
                try:
                    names = [getattr(x, "name", "") for x in (synced or [])]
                    ok = "rsnote" in set(names)
                    synced_ok[int(gid)] = bool(ok)
                    print(f"{Colors.GREEN}[RSNotes] Sync OK: guild={gid} commands={len(names)} has_rsnote={ok}{Colors.RESET}")
                except Exception:
                    pass
            except Exception as e:
                try:
                    print(f"{Colors.YELLOW}[RSNotes] Sync failed: guild={gid} err={type(e).__name__}: {str(e)[:160]}{Colors.RESET}")
                except Exception:
                    pass
                try:
                    synced_ok[int(gid)] = False
                except Exception:
                    pass

        if synced_any:
            try:
                print(f"{Colors.GREEN}[RSNotes] Loaded + synced `/rsnote` to {len(guild_ids)} guild(s){Colors.RESET}")
            except Exception:
                pass
            try:
                ext_loaded = "RSNotes.rsnote" in set(getattr(self.bot, "extensions", {}).keys())
                self._rsnotes_status_line = (
                    f"[RSNotes] status=sync_done ext_loaded={ext_loaded} "
                    f"tree_has_rsnote={has_rsnote} guild_results={synced_ok}"
                )
            except Exception:
                pass
        else:
            try:
                ext_loaded = "RSNotes.rsnote" in set(getattr(self.bot, "extensions", {}).keys())
                self._rsnotes_status_line = f"[RSNotes] status=no_guilds ext_loaded={ext_loaded} guild_ids={guild_ids}"
            except Exception:
                pass

    async def _initialize_admin_slash_commands(self) -> None:
        """Register RSAdminBot slash commands and sync to allowed guild(s)."""
        if getattr(self, "_admin_slash_initialized", False):
            return
        self._admin_slash_initialized = True

        try:
            await self.bot.add_cog(RSAdminSlashCog(self))
        except Exception as e:
            try:
                print(f"{Colors.YELLOW}[Slash] Failed to add slash cog: {type(e).__name__}: {str(e)[:200]}{Colors.RESET}")
            except Exception:
                pass

        # Slash commands are ONLY enabled in neo-test-server.
        # Reselling Secrets should NOT have any slash commands from RSAdminBot.
        # (RS uses prefix: !delete / !transfer / !archive.)
        try:
            test_gid = int(self.config.get("test_server_guild_id") or 0)
        except Exception:
            test_gid = 0
        try:
            rs_gid = int(self.config.get("rs_server_guild_id") or 0)
        except Exception:
            rs_gid = 0

        # 1) Ensure Reselling Secrets has NO RSAdminBot slash commands (clear + sync empty guild set).
        if rs_gid:
            try:
                gobj = discord.Object(id=int(rs_gid))
                try:
                    self.bot.tree.clear_commands(guild=gobj)
                except Exception:
                    pass
                synced = await self.bot.tree.sync(guild=gobj)
                try:
                    names = sorted({str(getattr(x, "name", "") or "") for x in (synced or []) if getattr(x, "name", None)})
                    print(f"{Colors.GREEN}[Slash] Cleared: guild={rs_gid} commands={len(names)}{Colors.RESET}")
                except Exception:
                    pass
            except Exception as e:
                try:
                    print(f"{Colors.YELLOW}[Slash] Clear failed: guild={rs_gid} err={type(e).__name__}: {str(e)[:200]}{Colors.RESET}")
                except Exception:
                    pass

        # 2) Sync ALL slash commands to neo-test-server.
        if not test_gid:
            try:
                print(f"{Colors.YELLOW}[Slash] Missing test_server_guild_id; skipping slash sync{Colors.RESET}")
            except Exception:
                pass
            return

        try:
            gobj = discord.Object(id=int(test_gid))
            try:
                self.bot.tree.clear_commands(guild=gobj)
            except Exception:
                pass
            try:
                self.bot.tree.copy_global_to(guild=gobj)
            except Exception:
                pass
            synced = await self.bot.tree.sync(guild=gobj)
            try:
                names = sorted({str(getattr(x, "name", "") or "") for x in (synced or []) if getattr(x, "name", None)})
                print(f"{Colors.GREEN}[Slash] Sync OK: guild={test_gid} commands={len(names)}{Colors.RESET}")
            except Exception:
                pass
        except Exception as e:
            try:
                print(f"{Colors.YELLOW}[Slash] Sync failed: guild={test_gid} err={type(e).__name__}: {str(e)[:200]}{Colors.RESET}")
            except Exception:
                pass
    
    async def _initialize_monitor_channels(self) -> None:
        """Initialize monitor category and per-bot channels in test server."""
        monitor_cfg = self._get_monitor_channels_config()
        if not monitor_cfg.get("enabled"):
            return
        
        if not self.test_server_organizer:
            return
        
        # Get RS bot keys
        bot_groups = self.config.get("bot_groups") or {}
        rs_keys = ["rsadminbot"] + list(bot_groups.get("rs_bots") or [])
        
        # Create monitor channels
        channel_map = await self.test_server_organizer.ensure_monitor_category_and_bot_channels(rs_keys)
        self._bot_monitor_channel_ids = channel_map
        
        # Store category ID from organizer
        if hasattr(self.test_server_organizer, 'channels_data'):
            self._monitor_category_id = self.test_server_organizer.channels_data.get("monitor_category_id")
        
        if channel_map:
            print(f"{Colors.GREEN}[Monitor Channels] Initialized {len(channel_map)} per-bot monitor channels{Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}[Monitor Channels] No monitor channels created (disabled or failed){Colors.RESET}")

    async def _initialize_journal_live(self) -> None:
        """Initialize per-bot journal channels + webhooks and start journal follow tasks (test server only)."""
        cfg = self._get_journal_live_config()
        if not cfg.get("enabled"):
            return

        if not self._should_use_local_exec():
            # Journal streaming is only supported on the Ubuntu host.
            return

        if not self.test_server_organizer:
            return

        try:
            test_guild_id = int(self.config.get("test_server_guild_id") or 0)
        except Exception:
            test_guild_id = 0
        if not test_guild_id:
            return

        guild = self.bot.get_guild(test_guild_id)
        if not guild or guild.id != test_guild_id:
            return

        # All bots: rsadminbot + rs_bots + mirror_bots
        bot_groups = self.config.get("bot_groups") or {}
        raw_keys = ["rsadminbot"] + list(bot_groups.get("rs_bots") or []) + list(bot_groups.get("mirror_bots") or [])
        # Keep stable ordering + only include known bots
        all_keys: List[str] = []
        for x in raw_keys:
            k = str(x).strip().lower()
            if not k:
                continue
            if k not in self.BOTS:
                continue
            if k in all_keys:
                continue
            all_keys.append(k)

        # Ensure journal channels exist in configured category (do not create categories)
        channel_map = await self.test_server_organizer.ensure_journal_channels_in_category(all_keys)
        self._journal_channel_ids = channel_map
        try:
            print(f"{Colors.GREEN}[Journal Live] Enabled. Channels: {len(channel_map)} (category_id={cfg.get('category_id')}){Colors.RESET}")
        except Exception:
            pass

        # Ensure webhooks exist (auto-create) and persist URLs to config.secrets.json
        created_any = False
        webhook_cfg = self._get_webhooks_config()
        journal_by_bot = dict(webhook_cfg.get("journal_by_bot") or {})

        for bot_key, channel_id in channel_map.items():
            try:
                ch = guild.get_channel(int(channel_id))
                if not ch or not isinstance(ch, discord.TextChannel):
                    continue
                hook_name = f"rsadminbot-journal-{bot_key}"
                hooks = []
                try:
                    hooks = await ch.webhooks()
                except Exception:
                    hooks = []
                found = None
                for h in hooks:
                    try:
                        if h and str(getattr(h, "name", "")) == hook_name:
                            found = h
                            break
                    except Exception:
                        continue
                if found is None:
                    try:
                        found = await ch.create_webhook(name=hook_name, reason="RSAdminBot journal live (test server only)")
                    except Exception:
                        continue
                if found and getattr(found, "url", None):
                    url = str(found.url)
                    if journal_by_bot.get(bot_key) != url:
                        journal_by_bot[bot_key] = url
                        created_any = True
            except Exception:
                continue

        # Systemd events webhook (single shared channel)
        sys_cfg = self._get_systemd_events_config()
        systemd_url = str(webhook_cfg.get("systemd_events_url") or "").strip()
        if sys_cfg.get("enabled"):
            sys_ch_id = int(sys_cfg.get("test_server_channel_id") or 0)
            if sys_ch_id:
                sys_ch = guild.get_channel(sys_ch_id)
                if sys_ch and isinstance(sys_ch, discord.TextChannel):
                    hook_name = "rsadminbot-systemd-events"
                    hooks = []
                    try:
                        hooks = await sys_ch.webhooks()
                    except Exception:
                        hooks = []
                    found = None
                    for h in hooks:
                        try:
                            if h and str(getattr(h, "name", "")) == hook_name:
                                found = h
                                break
                        except Exception:
                            continue
                    if found is None:
                        try:
                            found = await sys_ch.create_webhook(name=hook_name, reason="RSAdminBot systemd events (test server only)")
                        except Exception:
                            found = None
                    if found and getattr(found, "url", None):
                        url = str(found.url)
                        if systemd_url != url:
                            systemd_url = url
                            created_any = True

        if created_any:
            overlay = {"webhooks": {"journal_by_bot": journal_by_bot}}
            if systemd_url:
                overlay["webhooks"]["systemd_events_url"] = systemd_url
            self._merge_write_secrets(overlay)
            # Update in-memory config (no restart required to use freshly created webhooks)
            self.config.setdefault("webhooks", {})
            if isinstance(self.config.get("webhooks"), dict):
                self.config["webhooks"]["journal_by_bot"] = journal_by_bot
                if systemd_url:
                    self.config["webhooks"]["systemd_events_url"] = systemd_url

        # Cache webhook URLs for runtime
        self._journal_webhook_urls_by_bot = {k: str(v) for k, v in journal_by_bot.items() if v}
        self._systemd_events_webhook_url = systemd_url

        # Start per-bot journal follow tasks
        self._start_journal_follow_tasks(all_keys)
        try:
            print(f"{Colors.GREEN}[Journal Live] Follow tasks started: {len(self._journal_tasks)}{Colors.RESET}")
        except Exception:
            pass

    def _start_journal_follow_tasks(self, bot_keys: List[str]) -> None:
        cfg = self._get_journal_live_config()
        if not cfg.get("enabled"):
            return
        if not self._should_use_local_exec():
            return
        for bot_key in bot_keys:
            if bot_key in self._journal_tasks:
                continue
            info = self.BOTS.get(bot_key) or {}
            svc = str(info.get("service") or "")
            if not svc:
                continue
            url = str(self._journal_webhook_urls_by_bot.get(bot_key) or "").strip()
            if not url:
                continue
            self._journal_tasks[bot_key] = asyncio.create_task(self._journal_follow_loop(bot_key, svc, url))
    
    def _get_bot_monitor_channel(self, bot_key: str) -> Optional[discord.TextChannel]:
        """Get the monitor channel for a bot key."""
        channel_id = self._bot_monitor_channel_ids.get(bot_key)
        if channel_id:
            return self.bot.get_channel(channel_id)
        return None
    
    def _get_rs_error_channel(self) -> Optional[discord.TextChannel]:
        """Get the RS server error channel."""
        monitor_cfg = self._get_monitor_channels_config()
        channel_id = monitor_cfg.get("rs_error_channel_id")
        if channel_id:
            return self.bot.get_channel(channel_id)
        return None
    
    def _failure_mentions(self) -> str:
        """Get failure ping mentions from config."""
        monitor_cfg = self._get_monitor_channels_config()
        user_ids = monitor_cfg.get("ping_on_failure_user_ids", [])
        if not user_ids:
            return ""
        return " ".join(f"<@!{int(uid)}>" for uid in user_ids if uid)
    
    def _truncate_codeblock(self, text: str, limit: int = 1800) -> str:
        """Truncate text for code blocks to fit Discord limits."""
        if not text:
            return "(no output)"
        text = str(text).strip()
        if len(text) <= limit:
            return text
        return text[:limit] + "\n…(truncated)"

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
            last_snapshot: Dict[str, Tuple[str, int]] = self._last_service_snapshot.copy() if hasattr(self, '_last_service_snapshot') else {}
            last_post_ts: Dict[str, float] = {}
            last_heartbeat = 0.0

            # Get monitor channels config
            monitor_cfg = self._get_monitor_channels_config()

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
                
                # Route to bot's monitor channel (test server)
                bot_channel = self._get_bot_monitor_channel(bot_key)
                
                if severity == "error":
                    # Error: post to bot's channel + RS error channel
                    ping_users = monitor_cfg.get("ping_on_failure_user_ids", [])
                    ping_text = " ".join(f"<@!{uid}>" for uid in ping_users) if ping_users and should_ping else ""
                    
                    if bot_channel:
                        full_text = f"{ping_text} {text}" if ping_text else text
                        try:
                            await bot_channel.send(full_text[:2000])
                        except Exception:
                            pass
                    
                    # Also post to RS error channel
                    rs_error_channel = self._get_rs_error_channel()
                    if rs_error_channel:
                        try:
                            await rs_error_channel.send(text[:1900])
                        except Exception:
                            pass
                else:
                    # Info messages: post to bot's channel only (if enabled)
                    if bot_channel and monitor_cfg.get("post_pid_change", False):
                        try:
                            await bot_channel.send(text[:1900])
                        except Exception:
                            pass
                    
                    # Also post to progress channel for info
                # Systemd events channel (webhooks-only)
                try:
                    await self._send_systemd_event(bot_key, text, severity=severity, should_ping=should_ping)
                except Exception:
                    pass

                await self._post_or_edit_progress(None, text)

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
                        # Snapshot output: keep human-friendly (no PID lists unless explicitly needed)
                        state_txt = state or "unknown"
                        exists_txt = "exists" if exists else "missing"
                        lines_out.append(f"{key}: exists={exists_txt} state={state_txt}")
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
                                state_txt = state or "unknown"
                                exists_txt = "exists" if exists else "missing"
                                lines_out.append(f"{key}: exists={exists_txt} state={state_txt}")
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
                        prev_state, prev_pid = prev if prev else ("<none>", 0)
                        pid_changed = (prev_pid != cur_pid)
                        state_changed = (prev_state != cur_state)
                        is_pid_only = pid_changed and not state_changed and (cur_state == "active")
                        
                        # Check for failure states (case-insensitive, check if state contains keywords)
                        cur_state_lower = (cur_state or "").lower()
                        prev_state_lower = (prev_state or "").lower()
                        is_failure = (
                            cur_state_lower in ("failed", "inactive", "not_found") or
                            "failed" in cur_state_lower or
                            "inactive" in cur_state_lower or
                            "deactivating" in cur_state_lower
                        )
                        is_recovered = (
                            prev_state and 
                            prev_state_lower != "<none>" and
                            ("failed" in prev_state_lower or "inactive" in prev_state_lower) and
                            cur_state_lower == "active"
                        )
                        
                        # Update snapshot (and persist to instance)
                        last_snapshot[key] = (cur_state, cur_pid)
                        self._last_service_snapshot[key] = (cur_state, cur_pid)
                        
                        # Get full details for the change
                        info = self.BOTS.get(key) or {}
                        svc = info.get("service", "")
                        exists, state, _ = self.service_manager.get_status(svc, bot_name=key)
                        pid = self.service_manager.get_pid(svc) or 0
                        
                        # Build message based on change type
                        if is_pid_only:
                            # PID-only change while active = restart detected (compact message)
                            msg_lines = [
                                f"✅ Restarted: pid {prev_pid} → {cur_pid}"
                            ]
                            severity = "info"
                            should_ping = False
                        elif is_recovered:
                            # State recovered (failed/inactive → active)
                            msg_lines = [
                                f"✅ **RECOVERED**: **{info.get('name', key)}** ({key}) back to `active` pid=`{cur_pid}`"
                            ]
                            severity = "info"
                            should_ping = False
                        elif is_failure and (state_changed or prev_state == "<none>"):
                            # Failure detected (only on transition or first detection) - include details and logs
                            mentions = self._failure_mentions()
                            msg_lines = [
                                f"🚨 **FAILURE**: **{info.get('name', key)}** ({key})",
                                f"State: `{cur_state}` | PID: `{cur_pid}`"
                            ]
                            if mentions:
                                msg_lines.append(mentions)
                            if prev_state and prev_state != "<none>":
                                msg_lines.insert(2, f"Previous: {prev_state} (pid: {prev_pid})")
                            
                            # Get detailed status (like !details command)
                            details_success, details_out, _ = self.service_manager.get_detailed_status(svc)
                            if details_success and details_out:
                                truncated_details = self._truncate_codeblock(details_out, limit=1800)
                                msg_lines.append("\n**Details:**")
                                msg_lines.append(f"```\n{truncated_details}\n```")
                            
                            # Get logs (like !logs command)
                            failure_lines = monitor_cfg.get("failure_logs_lines", 80)
                            logs = self.service_manager.get_failure_logs(svc, lines=failure_lines) or ""
                            if logs:
                                truncated_logs = self._truncate_codeblock(logs, limit=1800)
                                msg_lines.append(f"\n**logs (last {failure_lines})**")
                                msg_lines.append(f"```\n{truncated_logs}\n```")
                            
                            severity = "error"
                            should_ping = True
                        else:
                            # Other state change (not failure, not recovery)
                            msg_lines = [
                                f"[monitor] {info.get('name', key)} ({key}) - state changed",
                                f"State: {self._format_service_state(exists, state, pid)}"
                            ]
                            if prev_state:
                                msg_lines.insert(1, f"Previous: {prev_state} (pid: {prev_pid})")
                            severity = "info"
                            should_ping = False
                        
                        msg = "\n".join(msg_lines)
                        await post(key, msg, severity=severity, should_ping=should_ping)
                        
                except Exception:
                    pass
                await asyncio.sleep(interval)

        self._service_monitor_task = asyncio.create_task(_loop())

    def _get_oraclefiles_sync_config(self) -> Dict[str, Any]:
        """Return OracleFiles snapshot sync config.

        This feature publishes a bots-only snapshot of the live Ubuntu bot folders to:
          https://github.com/neo-rs/oraclefiles  (repo should exist)

        Snapshot rules (safety):
        - Never include config.secrets.json
        - Never include key material (*.key/*.pem/*.ppk)
        - Never include any *.json that contains the key "bot_token"

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
        """Create/update oraclefiles repo and push a bots-only snapshot (live Ubuntu -> GitHub)."""
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

        folders_arr = " ".join(shlex.quote(x) for x in include)
        cmd = f"""
set -euo pipefail

REPO_DIR={shlex.quote(repo_dir)}
REPO_URL={shlex.quote(repo_url)}
BRANCH={shlex.quote(branch)}
LIVE_ROOT={shlex.quote(live_root)}
DEPLOY_KEY={shlex.quote(deploy_key)}
TRIGGER={shlex.quote(trigger_txt)}

command -v git >/dev/null 2>&1 || {{ echo \"ERR=git_missing\"; exit 2; }}
test -f \"$DEPLOY_KEY\" || {{ echo \"ERR=deploy_key_missing\"; echo \"DEPLOY_KEY=$DEPLOY_KEY\"; exit 2; }}
test -d \"$LIVE_ROOT\" || {{ echo \"ERR=live_root_missing\"; echo \"LIVE_ROOT=$LIVE_ROOT\"; exit 2; }}

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
git fetch origin

if git show-ref --verify --quiet \"refs/remotes/origin/$BRANCH\"; then
  git checkout -B \"$BRANCH\" \"origin/$BRANCH\"
  git reset --hard \"origin/$BRANCH\"
else
  git checkout -B \"$BRANCH\"
fi

rm -rf snapshot py_snapshot
mkdir -p snapshot

cd \"$LIVE_ROOT\"
TMP0=/tmp/mw_oraclefiles_snapshot_list.bin
rm -f \"$TMP0\"

# Snapshot scope (bots-only) comes from include_folders.
FOLDERS=({folders_arr})
VALID_FOLDERS=()
for d in \"${{FOLDERS[@]}}\"; do
  if [ -d \"$LIVE_ROOT/$d\" ]; then
    VALID_FOLDERS+=(\"$d\")
  else
    echo \"WARN_MISSING_FOLDER=$d\"
  fi
done
if [ ${{#VALID_FOLDERS[@]}} -eq 0 ]; then
  echo \"ERR=no_valid_folders\"
  exit 2
fi

# Detect forbidden DB artifacts (visibility without uploading them).
DB_HITS=$(find \"${{VALID_FOLDERS[@]}}\" -type f \\( -name \"*.db\" -o -name \"*.sqlite\" -o -name \"*.sqlite3\" \\) 2>/dev/null | head -n 10 || true)
if [ -n \"$DB_HITS\" ]; then
  echo \"WARN_FORBIDDEN_DB_FILES=1\"
  echo \"$DB_HITS\" | while IFS= read -r ln; do echo \"WARN_DB_FILE=$ln\"; done
fi

# Build a null-delimited file list of all files, excluding secrets and junk.
find \"${{VALID_FOLDERS[@]}}\" -type f \
  ! -path \"*/.git/*\" \
  ! -path \"*/__pycache__/*\" \
  ! -path \"*/.venv/*\" \
  ! -path \"*/venv/*\" \
  ! -path \"RSAdminBot/original_files/*\" \
  ! -name \"*.pyc\" \
  ! -name \"*.log\" \
  ! -name \"config.secrets.json\" \
  ! -name \"*.key\" ! -name \"*.pem\" ! -name \"*.ppk\" \
  ! -name \"*.db\" ! -name \"*.sqlite\" ! -name \"*.sqlite3\" \
  -print0 \
| while IFS= read -r -d '' f; do
    # Extra safety: skip any JSON that appears to include bot_token.
    if [[ \"$f\" == *.json ]]; then
      if grep -a -q '\"bot_token\"' \"$f\" 2>/dev/null; then
        continue
      fi
    fi
    printf '%s\\0' \"$f\"
  done > \"$TMP0\"

tar --null -T \"$TMP0\" -cf - | (cd \"$REPO_DIR/snapshot\" && tar -xf -)

cd \"$REPO_DIR\"
git add -A

if git diff --cached --quiet; then
  echo \"OK=1\"
  echo \"NO_CHANGES=1\"
  echo \"HEAD=$(git rev-parse HEAD 2>/dev/null || echo '')\"
  exit 0
fi

TS=$(date +%Y%m%d_%H%M%S)
git commit -m \"oraclefiles snapshot: $TS trigger=$TRIGGER\" >/dev/null

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
            combined = "\n".join([x for x in (err, out) if x]).strip()
            return False, {"error": (combined or "oraclefiles sync failed")[-1600:]}

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

    async def _post_or_edit_progress(self, progress_msg, text: str):
        """Post progress updates as embeds to the log channel (no noisy progress channel).

        Note: We intentionally do not maintain/edit a dedicated progress message anymore.
        Progress updates are emitted as structured embeds to the log channel.
        """
        try:
            raw = (text or "").strip()
            if not raw:
                return None

            first_line = raw.splitlines()[0].strip()
            title = first_line if first_line else "Progress"
            # Keep the full text available (truncated) in a codeblock for readability.
            body = raw
            embed = MessageHelper.create_info_embed(
                title=title[:256],
                message=self._codeblock(body, limit=1700),
                footer="RSAdminBot",
            )
            await self._log_to_discord(embed, None)
            return None
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

    def _stage_rsadminbot_selfupdate(self) -> Tuple[bool, Dict[str, Any]]:
        """Stage an RSAdminBot update for safe apply on next restart.

        RSAdminBot must NOT overwrite its own running folder in-place.
        This stages into a temp dir and writes `RSAdminBot/.pending_update.json` so `RSAdminBot/run_bot.sh`
        applies the update before launching Python after restart.
        """
        try:
            checkouts = self.config.get("code_checkouts") if isinstance(self.config, dict) else {}
            if not isinstance(checkouts, dict):
                checkouts = {}
            code_root = str(checkouts.get("rsbots_code_root") or "/home/rsadmin/bots/rsbots-code").strip()
            live_root = str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world").strip()

            cmd_body = """
if [ ! -d "$CODE_ROOT/.git" ]; then
  echo "ERR=missing_code_root"
  echo "DETAIL=$CODE_ROOT/.git not found"
  exit 2
fi
if [ ! -d "$LIVE_ROOT/RSAdminBot" ]; then
  echo "ERR=missing_live_rsadminbot"
  echo "DETAIL=$LIVE_ROOT/RSAdminBot not found"
  exit 2
fi

cd "$CODE_ROOT"
OLD="$(git rev-parse HEAD 2>/dev/null || echo '')"
git fetch origin
git pull --ff-only origin main
NEW="$(git rev-parse HEAD 2>/dev/null || echo '')"

TS="$(date +%Y%m%d_%H%M%S)"
STAGING_DIR="/tmp/mw_rsadminbot_stage_$TS"
mkdir -p "$STAGING_DIR"

# Tracked file list for RSAdminBot only (safe types; never secrets)
TMP_ALL="/tmp/mw_rsadminbot_all_$TS.txt"
git ls-files "RSAdminBot" 2>/dev/null > "$TMP_ALL" || true

TMP_SYNC="/tmp/mw_rsadminbot_sync_$TS.txt"
grep -E "(\\.py$|\\.md$|\\.json$|\\.txt$|\\.sh$|(^|/)requirements\\.txt$)" "$TMP_ALL" | grep -v -E "(^|/)config\\.secrets\\.json$" > "$TMP_SYNC" || true
sort -u "$TMP_SYNC" -o "$TMP_SYNC" || true
SYNC_COUNT="$(wc -l < "$TMP_SYNC" | tr -d ' ')"
if [ "$SYNC_COUNT" = "" ]; then SYNC_COUNT="0"; fi
if [ "$SYNC_COUNT" = "0" ]; then
  echo "ERR=no_files"
  echo "DETAIL=no tracked RSAdminBot files to stage"
  exit 3
fi

# Change list (git)
TMP_CHANGED="/tmp/mw_rsadminbot_changed_$TS.txt"
git diff --name-only "$OLD" "$NEW" -- "RSAdminBot" 2>/dev/null > "$TMP_CHANGED" || true
CHANGED_COUNT="$(sed '/^$/d' "$TMP_CHANGED" | wc -l | tr -d ' ')"
if [ "$CHANGED_COUNT" = "" ]; then CHANGED_COUNT="0"; fi

# Drift check (live tree vs git checkout) so config-only changes still apply.
# This prevents the "NO_CHANGES but live config is stale" failure mode.
DRIFT_COUNT=0
while IFS= read -r p; do
  p="$(printf "%s" "$p" | tr -d "\r")"
  if [ -z "$p" ]; then continue; fi
  if [ -f "$CODE_ROOT/$p" ] && [ -f "$LIVE_ROOT/$p" ]; then
    A="$(sha256sum "$CODE_ROOT/$p" 2>/dev/null | awk '{print $1}' || true)"
    B="$(sha256sum "$LIVE_ROOT/$p" 2>/dev/null | awk '{print $1}' || true)"
    if [ "$A" != "$B" ]; then
      DRIFT_COUNT=$((DRIFT_COUNT+1))
    fi
  elif [ -f "$CODE_ROOT/$p" ] && [ ! -f "$LIVE_ROOT/$p" ]; then
    DRIFT_COUNT=$((DRIFT_COUNT+1))
  fi
done < "$TMP_SYNC"

if [ "$OLD" != "" ] && [ "$NEW" != "" ] && [ "$OLD" = "$NEW" ] && [ "$CHANGED_COUNT" = "0" ] && [ "$DRIFT_COUNT" = "0" ]; then
  echo "OK=1"
  echo "OLD=$OLD"
  echo "NEW=$NEW"
  echo "SYNC_COUNT=$SYNC_COUNT"
  echo "CHANGED_COUNT=0"
  echo "DRIFT_COUNT=0"
  echo "NO_CHANGES=1"
  echo "STAGING_DIR="
  echo "BACKUP="
  echo "CHANGED_BEGIN"
  echo "CHANGED_END"
  exit 0
fi

# Remote backup (server-side only)
BACKUP_DIR="$LIVE_ROOT/backups"
mkdir -p "$BACKUP_DIR"
BACKUP_TAR="$BACKUP_DIR/RSAdminBot_preupdate_$TS.tar.gz"
env -u TAR_OPTIONS /bin/tar -czf "$BACKUP_TAR" -C "$LIVE_ROOT" "RSAdminBot" || true

# Stage tracked files into STAGING_DIR (preserve paths like RSAdminBot/...)
env -u TAR_OPTIONS /bin/tar -cf - -T "$TMP_SYNC" | (cd "$STAGING_DIR" && env -u TAR_OPTIONS /bin/tar -xf - --overwrite)

# Write pending update marker for run_bot.sh
PENDING_JSON="$LIVE_ROOT/RSAdminBot/.pending_update.json"
TS="$TS" STAGING_DIR="$STAGING_DIR" BACKUP_TAR="$BACKUP_TAR" CHANGED_COUNT="$CHANGED_COUNT" SYNC_COUNT="$SYNC_COUNT" OLD="$OLD" NEW="$NEW" PENDING_JSON="$PENDING_JSON" python3 -c 'import json,os; staging=os.environ.get(\"STAGING_DIR\",\"\"); ts=os.environ.get(\"TS\",\"\"); backup=os.environ.get(\"BACKUP_TAR\",\"\"); changed=int(os.environ.get(\"CHANGED_COUNT\",\"0\") or \"0\"); sync=int(os.environ.get(\"SYNC_COUNT\",\"0\") or \"0\"); old=os.environ.get(\"OLD\",\"\"); new=os.environ.get(\"NEW\",\"\"); pending=os.environ.get(\"PENDING_JSON\",\"\"); data={\"timestamp\":ts,\"staging_dir\":staging,\"remote_backup\":backup,\"git_old\":old,\"git_new\":new,\"changes\":{\"total\":changed,\"sync_total\":sync}}; f=open(pending,\"w\",encoding=\"utf-8\"); f.write(json.dumps(data,indent=2,ensure_ascii=True)); f.close()'

echo "OK=1"
echo "OLD=$OLD"
echo "NEW=$NEW"
echo "SYNC_COUNT=$SYNC_COUNT"
echo "CHANGED_COUNT=$CHANGED_COUNT"
echo "DRIFT_COUNT=$DRIFT_COUNT"
echo "NO_CHANGES=0"
echo "STAGING_DIR=$STAGING_DIR"
echo "BACKUP=$BACKUP_TAR"
echo "CHANGED_BEGIN"
head -n 30 "$TMP_CHANGED" || true
echo "CHANGED_END"
"""

            # NOTE: cmd_body is a plain triple-quoted string because it contains many literal `{}` (bash/python)
            # that must not be interpreted by Python f-string formatting.
            cmd = "\n".join(
                [
                    "set -euo pipefail",
                    "",
                    f"CODE_ROOT={shlex.quote(code_root)}",
                    f"LIVE_ROOT={shlex.quote(live_root)}",
                    "",
                ]
            ) + cmd_body

            ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=180)
            out = (stdout or "").strip()
            err = (stderr or "").strip()

            # Parse stdout even on non-zero exit codes.
            #
            # Why: In some environments git emits output on stderr and certain host shell wrappers
            # can still propagate a non-zero exit status even though our script reached the explicit
            # OK=1 / NO_CHANGES=1 paths. We prefer trusting our sentinel lines when present.
            stats: Dict[str, Any] = {"raw": out[-1600:], "stderr": err[-1600:]}
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

            if not ok:
                # If the script self-reported OK and did not report an ERR, treat it as success.
                if str(stats.get("ok") or "").strip() == "1" and not str(stats.get("err") or "").strip():
                    return True, stats

                # IMPORTANT: include BOTH streams.
                # Git often writes progress/info to stderr, while the actual failure may be on stdout.
                if err and out:
                    msg = (err + "\n" + out).strip()
                else:
                    msg = err or out or "unknown error"
                return False, {"error": msg[:1200]}

            return True, stats
        except Exception as e:
            return False, {"error": f"rsadminbot stage update failed ({type(e).__name__}): {str(e)[:300]}"}
    
    def _github_py_only_update(self, bot_folder: str, *, code_root: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
        """Pull python-only bot code from the server-side GitHub checkout and overwrite live code files.

        This is the canonical update path for `!selfupdate`, `!botupdate`, and `!mwupdate` when using GitHub as source of truth.

        Server expectations:
        - Git repo exists at: code_root (configured by RSAdminBot/config.json)
        - Live bot tree exists at: self.remote_root (typically /home/rsadmin/bots/mirror-world)
        - GitHub repo contains bot code under the target bot folder path

        Safety:
        - Never deletes first; overwrite-in-place only
        - Only copies files tracked by git under the target folder:
          - *.py
          - COMMANDS.md (if present)
          - config.json / messages.json / vouch_config.json (if present + tracked)
        """
        try:
            folder = (bot_folder or "").strip()
            if not folder:
                return False, {"error": "bot_folder required"}

            checkouts = self.config.get("code_checkouts") if isinstance(self.config, dict) else {}
            if not isinstance(checkouts, dict):
                checkouts = {}
            default_code_root = "/home/rsadmin/bots/rsbots-code"
            code_root = str(code_root or checkouts.get("rsbots_code_root") or default_code_root).strip()
            if not code_root:
                return False, {"error": "code_root not configured (set config.code_checkouts.rsbots_code_root)"}
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

# Get list of tracked files in git repo (folder-scoped)
TMP_ALL_LIST="/tmp/mw_tracked_${{BOT_FOLDER}}.txt"
git ls-files "$BOT_FOLDER" 2>/dev/null > "$TMP_ALL_LIST" || true

# Python file count (sanity)
TMP_PY_LIST="/tmp/mw_pyonly_${{BOT_FOLDER}}.txt"
grep -E \"\\\\.py$\" "$TMP_ALL_LIST" > "$TMP_PY_LIST" || true
PY_COUNT="$(wc -l < "$TMP_PY_LIST" | tr -d \" \")"
if [ "$PY_COUNT" = "" ]; then PY_COUNT="0"; fi
if [ "$PY_COUNT" = "0" ]; then
  echo "ERR=no_python_files"
  echo "DETAIL=no tracked *.py under $BOT_FOLDER in $CODE_ROOT"
  exit 3
fi

# Build the sync list (tracked, safe, non-secret):
# - include: .py/.md/.json/.txt + requirements.txt
# - exclude: config.secrets.json (even if tracked by mistake)
TMP_SYNC_LIST="/tmp/mw_sync_${{BOT_FOLDER}}.txt"
grep -E \"(\\\\.py$|\\\\.md$|\\\\.json$|\\\\.txt$|(^|/)requirements\\\\.txt$)\" "$TMP_ALL_LIST" | grep -v -E \"(^|/)config\\\\.secrets\\\\.json$\" > "$TMP_SYNC_LIST" || true
sort -u "$TMP_SYNC_LIST" -o "$TMP_SYNC_LIST" || true

# Also include shared utilities if present (repo-level, not in BOT_FOLDER)
TMP_SHARED_LIST="/tmp/mw_shared_${{BOT_FOLDER}}.txt"
git ls-files "shared" 2>/dev/null | grep -E \"(\\\\.py$|\\\\.md$|\\\\.json$|\\\\.txt$|(^|/)requirements\\\\.txt$)\" | grep -v -E \"(^|/)config\\\\.secrets\\\\.json$\" > "$TMP_SHARED_LIST" || true
cat "$TMP_SYNC_LIST" "$TMP_SHARED_LIST" | sort -u > "${{TMP_SYNC_LIST}}.merged"
mv "${{TMP_SYNC_LIST}}.merged" "$TMP_SYNC_LIST"
SYNC_COUNT="$(wc -l < "$TMP_SYNC_LIST" | tr -d \" \")"
if [ "$SYNC_COUNT" = "" ]; then SYNC_COUNT="0"; fi

# Compare live files with git repo files to detect actual differences
# This catches cases where files differ even if commit hash didn't change
DIFF_FILES="/tmp/mw_pyonly_diff_${{BOT_FOLDER}}.txt"
> "$DIFF_FILES"
while IFS= read -r git_file; do
  if [ -z "$git_file" ]; then continue; fi
  git_path="$CODE_ROOT/$git_file"
  live_path="$LIVE_ROOT/$git_file"
  if [ ! -f "$live_path" ]; then
    echo "$git_file" >> "$DIFF_FILES"
  elif ! cmp -s "$git_path" "$live_path" 2>/dev/null; then
    echo "$git_file" >> "$DIFF_FILES"
  fi
done < "$TMP_SYNC_LIST"

# Also check for files changed in git commits (for reporting)
GIT_CHANGED="$(git diff --name-only "$OLD" "$NEW" -- "$BOT_FOLDER" 2>/dev/null | grep -E \"\\\\.py$\" || true)"
GIT_CHANGED_COUNT="$(echo \"$GIT_CHANGED\" | sed '/^$/d' | wc -l | tr -d \" \")"

# Count actual file differences
ACTUAL_CHANGED_COUNT="$(wc -l < "$DIFF_FILES" | tr -d \" \")"
if [ "$ACTUAL_CHANGED_COUNT" = "" ]; then ACTUAL_CHANGED_COUNT="0"; fi

# Backup the soon-to-be-overwritten files (tracked only; no secrets/runtime).
TS="$(date +%Y%m%d_%H%M%S)"
SAFE_BOT="$(echo "$BOT_FOLDER" | tr '/' '_')"
BACKUP_DIR="$LIVE_ROOT/backups"
mkdir -p "$BACKUP_DIR"
BACKUP_TAR="$BACKUP_DIR/${{SAFE_BOT}}_preupdate_${{TS}}.tar.gz"
(cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar --ignore-failed-read -czf "$BACKUP_TAR" -T "$TMP_SYNC_LIST") || true

# Copy files (always sync, even if no differences detected).
# Explicitly ignore TAR_OPTIONS and force overwrite to avoid host-level tar defaults like --keep-old-files.
env -u TAR_OPTIONS /bin/tar -cf - -T "$TMP_SYNC_LIST" | (cd "$LIVE_ROOT" && env -u TAR_OPTIONS /bin/tar -xf - --overwrite)

# Use actual changed count for reporting (more accurate than git diff)
CHANGED_COUNT="$ACTUAL_CHANGED_COUNT"
CHANGED="$(cat \"$DIFF_FILES\" | grep -v \"^$\" || true)"

echo "OK=1"
echo "OLD=$OLD"
echo "NEW=$NEW"
echo "PY_COUNT=$PY_COUNT"
echo "SYNC_COUNT=$SYNC_COUNT"
echo "CHANGED_COUNT=$CHANGED_COUNT"
echo "GIT_CHANGED_COUNT=$GIT_CHANGED_COUNT"
echo "BACKUP=$BACKUP_TAR"
echo "CHANGED_BEGIN"
echo "$CHANGED" | head -n 30 || true
echo "CHANGED_END"
"""

            ok, stdout, stderr = self._execute_ssh_command(cmd, timeout=180)
            out = (stdout or "").strip()
            err = (stderr or "").strip()
            if not ok:
                if err and out:
                    msg = (err + "\n" + out).strip()
                else:
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

    def _update_one_py_only_from_checkout(
        self,
        bot_key: str,
        *,
        allowed_group: str,
        code_root: str,
        allow_rsadminbot: bool,
    ) -> Tuple[bool, Dict[str, Any]]:
        """Update a single bot from a configured GitHub checkout (python-only) and restart the service."""
        key = (bot_key or "").strip().lower()
        if not key:
            return False, {"error": "bot_key required"}
        if key not in self.BOTS:
            return False, {"error": f"Unknown bot: {key}"}

        group = self._get_bot_group(key) or ""
        if group != allowed_group:
            return False, {"error": f"{key} is not in group {allowed_group} (updates are group-scoped)"}

        if key == "rsadminbot" and not allow_rsadminbot:
            return False, {"error": "RSAdminBot must be updated via !selfupdate"}

        info = self.BOTS.get(key) or {}
        folder = str(info.get("folder") or "").strip()
        service = str(info.get("service") or "").strip()
        if not folder:
            return False, {"error": f"Missing folder mapping for bot: {key}"}
        if not code_root:
            return False, {"error": f"Missing code_root for group: {allowed_group}"}

        ok, stats = self._github_py_only_update(folder, code_root=code_root)
        if not ok:
            return False, {"error": str((stats or {}).get("error") or "update failed")[:900]}

        # Restart service
        restart_ok = False
        restart_err = ""
        if self.service_manager and service:
            ok_r, out_r, err_r = self.service_manager.restart(service, bot_name=key)
            if not ok_r:
                restart_err = (err_r or out_r or "restart failed")[:800]
            else:
                running, verify_err = self.service_manager.verify_started(service, bot_name=key)
                restart_ok = bool(running)
                if not restart_ok:
                    restart_err = (verify_err or "service did not become active")[:800]
        else:
            restart_err = "ServiceManager not available or missing service mapping"

        old = str(stats.get("old") or "").strip()
        new = str(stats.get("new") or "").strip()
        py_count = str(stats.get("py_count") or "0").strip()
        sync_count = str(stats.get("sync_count") or "").strip()
        changed_count = str(stats.get("changed_count") or "0").strip()
        changed_sample = stats.get("changed_sample") or []

        summary = f"✅ **{info.get('name', key)} updated from GitHub (python-only)**\n```"
        if old or new:
            summary += f"\nGit: {old[:12]} -> {new[:12]}"
        summary += f"\nCode root: {code_root}"
        if sync_count:
            summary += f"\nPython copied: {py_count} | Total copied: {sync_count} | Changed: {changed_count}"
        else:
            summary += f"\nPython copied: {py_count} | Changed: {changed_count}"
        summary += f"\nRestart: {'OK' if restart_ok else 'FAILED'}"
        summary += "\n```"
        if changed_sample:
            summary += "\nChanged sample:\n```" + "\n".join(str(x) for x in changed_sample[:20]) + "```"
        if not restart_ok and restart_err:
            summary += "\nRestart error:\n```" + restart_err[:900] + "```"

        return True, {
            "bot": key,
            "folder": folder,
            "service": service,
            "old": old,
            "new": new,
            "code_root": code_root,
            "py_count": py_count,
            "changed_count": changed_count,
            "restart": "OK" if restart_ok else "FAILED",
            "restart_ok": restart_ok,
            "restart_err": restart_err,
            "summary": summary[:1900],
        }

    def _botupdate_one_py_only(self, bot_key: str) -> Tuple[bool, Dict[str, Any]]:
        """Update a single RS bot from rsbots-code (python-only) and restart the service."""
        code_root = self._get_update_code_root_for_group("rs_bots")
        return self._update_one_py_only_from_checkout(
            bot_key,
            allowed_group="rs_bots",
            code_root=code_root,
            allow_rsadminbot=False,
        )

    def _mwupdate_one_py_only(self, bot_key: str) -> Tuple[bool, Dict[str, Any]]:
        """Update a single Mirror-World bot from mwbots-code (python-only) and restart the service."""
        code_root = self._get_update_code_root_for_group("mirror_bots")
        return self._update_one_py_only_from_checkout(
            bot_key,
            allowed_group="mirror_bots",
            code_root=code_root,
            allow_rsadminbot=False,
        )
    
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
            is_admin: Optional[bool] = None
            aliases = list(getattr(c, "aliases", []) or [])
            if name in reg_map:
                desc, is_admin = reg_map[name]
            else:
                desc = (getattr(c, "help", "") or "").strip().splitlines()[0:1]
                desc = desc[0] if desc else ""
                # Best-effort: commands guarded by @commands.check(...) will have at least one check.
                try:
                    is_admin = bool(getattr(c, "checks", []) or [])
                except Exception:
                    is_admin = None
            cmds.append((name, aliases, desc, is_admin))

        cmds.sort(key=lambda x: x[0])
        lines = []
        lines.append("RSAdminBot Command Index")
        lines.append("Prefix: !")
        lines.append("")
        for name, aliases, desc, is_admin in cmds:
            admin_tag = " [ADMIN]" if is_admin else ""
            alias_txt = ""
            if aliases:
                alias_txt = f" (aliases: {', '.join('!' + str(a) for a in aliases[:6])}{'...' if len(aliases) > 6 else ''})"
            if desc:
                lines.append(f"!{name}{admin_tag}{alias_txt} - {desc}")
            else:
                lines.append(f"!{name}{admin_tag}{alias_txt}")
        return "\n".join(lines).strip()

    def _commands_catalog_state_path(self) -> Path:
        # Use .txt so deploy_unpack preserves it (it preserves *.txt under bot folders).
        return self.base_path / ".commands_catalog_state.txt"

    async def _publish_command_index_to_configured_channel(self) -> None:
        """Post or update the command index into a configured channel (on startup/restart)."""
        cfg = self.config.get("commands_catalog") if isinstance(self.config, dict) else None
        if not isinstance(cfg, dict) or not cfg.get("enabled"):
            return
        if not cfg.get("post_on_startup", True):
            return
        chan_id_raw = cfg.get("channel_id")
        if not chan_id_raw:
            return
        try:
            chan_id = int(str(chan_id_raw).strip())
        except Exception:
            return

        channel = self.bot.get_channel(chan_id)
        if not channel:
            try:
                channel = await self.bot.fetch_channel(chan_id)  # type: ignore[attr-defined]
            except Exception:
                print(f"{Colors.YELLOW}[Startup] Commands catalog channel not found (ID: {chan_id}){Colors.RESET}")
                return

        cmd_text = self._build_command_index_text()
        cmd_body = f"```{cmd_text[:1800]}```"

        # Hash for idempotent edits
        import hashlib
        cmd_hash = hashlib.sha256(cmd_text.encode("utf-8")).hexdigest()

        embed = MessageHelper.create_info_embed(
            title="RSAdminBot Commands",
            message="Auto-updated on RSAdminBot restart. Run commands anywhere; this is just an index.",
            footer="RSAdminBot",
        )

        # Load previous state (if present)
        state_path = self._commands_catalog_state_path()
        prev_msg_id: Optional[int] = None
        prev_hash: str = ""
        try:
            if state_path.exists():
                raw = (state_path.read_text(encoding="utf-8", errors="replace") or "").strip()
                if raw:
                    data = json.loads(raw)
                    if isinstance(data, dict):
                        prev_hash = str(data.get("hash") or "")
                        try:
                            prev_msg_id = int(data.get("message_id") or 0) or None
                        except Exception:
                            prev_msg_id = None
        except Exception:
            prev_msg_id = None
            prev_hash = ""

        # Try to edit existing message if we have one
        if prev_msg_id:
            try:
                msg = await channel.fetch_message(int(prev_msg_id))  # type: ignore[attr-defined]
                # Refresh even if unchanged (keeps it visible and ensures embed formatting)
                await msg.edit(content=cmd_body, embed=embed)
                if cmd_hash != prev_hash:
                    try:
                        state_path.write_text(json.dumps({"channel_id": chan_id, "message_id": int(prev_msg_id), "hash": cmd_hash}), encoding="utf-8")
                    except Exception:
                        pass
                print(f"{Colors.GREEN}[Startup] Commands catalog updated in #{getattr(channel, 'name', '')} ({chan_id}){Colors.RESET}")
                return
            except Exception:
                pass

        # Otherwise send a fresh message and persist state
        try:
            msg = await channel.send(content=cmd_body, embed=embed)
            try:
                state_path.write_text(json.dumps({"channel_id": chan_id, "message_id": int(msg.id), "hash": cmd_hash}), encoding="utf-8")
            except Exception:
                pass
            print(f"{Colors.GREEN}[Startup] Commands catalog posted to #{getattr(channel, 'name', '')} ({chan_id}){Colors.RESET}")
        except Exception as e:
            print(f"{Colors.YELLOW}[Startup] Failed to post commands catalog: {str(e)[:200]}{Colors.RESET}")

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

        # Build/refresh a multi-message command index:
        # - First message: RSAdminBot commands list
        # - One message per RS bot: management commands + link buttons to journal/log channels
        test_guild_id = int(self.config.get("test_server_guild_id") or 0)

        def ch_url(cid: int) -> str:
            return f"https://discord.com/channels/{test_guild_id}/{int(cid)}"

        class _LinksView(ui.View):
            def __init__(self, journal_cid: Optional[int], monitor_cid: Optional[int]):
                super().__init__(timeout=None)
                if journal_cid:
                    self.add_item(ui.Button(label="Journal", style=discord.ButtonStyle.link, url=ch_url(journal_cid)))
                if monitor_cid:
                    self.add_item(ui.Button(label="Monitor Logs", style=discord.ButtonStyle.link, url=ch_url(monitor_cid)))

        # Gather stored channel mappings (created elsewhere; do not create categories/channels here)
        channels_data = getattr(self.test_server_organizer, "channels_data", {}) or {}
        journal_map = channels_data.get("journal_channels") if isinstance(channels_data, dict) else {}
        monitor_map = channels_data.get("monitor_channels") if isinstance(channels_data, dict) else {}
        if not isinstance(journal_map, dict):
            journal_map = {}
        if not isinstance(monitor_map, dict):
            monitor_map = {}
        # If monitor_channels is disabled, don't link to (or recreate) bot-* monitor channels
        if not bool(self._get_monitor_channels_config().get("enabled")):
            monitor_map = {}

        pages: List[Tuple[str, str, Optional[discord.Embed], Optional[ui.View]]] = []

        # Page 1: RSAdminBot commands
        cmd_text = self._build_command_index_text()
        cmd_hash = self.test_server_organizer._sha256_text(cmd_text)
        cmd_embed = MessageHelper.create_info_embed(
            title="RSAdminBot Commands",
            message="This channel is an index (not where outputs go). Run commands in any channel you want.\n\nBelow is the live list of RSAdminBot commands:",
            footer="RSAdminBot",
        )
        cmd_body = f"```{cmd_text[:1800]}```"
        pages.append(("rsadminbot_commands", cmd_hash, cmd_embed, None))

        # Bot cards: management commands per bot (RS + MW)
        rs_keys = self._get_rs_bot_keys()
        mw_keys = self._get_mw_bot_keys()
        all_keys = rs_keys + [k for k in mw_keys if k not in rs_keys]
        # Prefer to show all management commands that exist in this running bot
        available_cmds = {c.name for c in list(self.bot.commands) if getattr(c, "name", "")}

        def bot_cmds(bot_key: str) -> str:
            lines = []
            group = self._get_bot_group(bot_key) or ""
            # Common management commands
            for name, fmt in [
                ("botstatus", "!botstatus {b}"),
                ("details", "!details {b}"),
                ("logs", "!logs {b} 80"),
                ("botstart", "!botstart {b}"),
                ("botstop", "!botstop {b}"),
                ("botrestart", "!botrestart {b}"),
                ("botupdate", "!botupdate {b}"),
                ("mwupdate", "!mwupdate {b}"),
                ("botinfo", "!botinfo {b}"),
                ("botconfig", "!botconfig {b}"),
            ]:
                if name in available_cmds:
                    # Only show the correct update command for the bot group
                    if name == "botupdate" and group == "mirror_bots":
                        continue
                    if name == "mwupdate" and group != "mirror_bots":
                        continue
                    lines.append(fmt.format(b=bot_key))
            return "\n".join(lines).strip()

        for bot_key in all_keys:
            bot_info = self.BOTS.get(bot_key) or {}
            title = f"{bot_info.get('name', bot_key)} ({bot_key})"
            body = bot_cmds(bot_key)
            if not body:
                continue
            h = self.test_server_organizer._sha256_text(body)
            embed = MessageHelper.create_info_embed(
                title=title,
                message="Common management commands:",
                footer="RSAdminBot",
            )
            embed.add_field(name="Commands", value=f"```{body[:950]}```", inline=False)
            jcid = None
            mcid = None
            try:
                if bot_key in journal_map:
                    jcid = int(journal_map.get(bot_key) or 0) or None
                if bot_key in monitor_map:
                    mcid = int(monitor_map.get(bot_key) or 0) or None
            except Exception:
                jcid = None
                mcid = None
            view = _LinksView(jcid, mcid) if (jcid or mcid) and test_guild_id else None
            pages.append((f"bot_{bot_key}", h, embed, view))

        # Persist/edit messages idempotently
        cards: Dict[str, Any] = self.test_server_organizer.get_meta("commands_cards", {}) or {}
        cards_hash: Dict[str, Any] = self.test_server_organizer.get_meta("commands_cards_hash", {}) or {}
        if not isinstance(cards, dict):
            cards = {}
        if not isinstance(cards_hash, dict):
            cards_hash = {}

        updated_cards: Dict[str, Any] = dict(cards)
        updated_hashes: Dict[str, Any] = dict(cards_hash)

        async def upsert(card_key: str, content_hash: str, embed: Optional[discord.Embed], view: Optional[ui.View], content: Optional[str] = None):
            msg_id = updated_cards.get(card_key)
            # If unchanged, still ensure message exists.
            if msg_id:
                try:
                    msg = await channel.fetch_message(int(msg_id))  # type: ignore[attr-defined]
                    if updated_hashes.get(card_key) != content_hash:
                        await msg.edit(content=content, embed=embed, view=view)
                        updated_hashes[card_key] = content_hash
                    else:
                        # Refresh view/embed to keep buttons alive after restarts
                        await msg.edit(content=content, embed=embed, view=view)
                    return
                except Exception:
                    # fallthrough to send new
                    pass
            msg = await channel.send(content=content, embed=embed, view=view)
            updated_cards[card_key] = int(msg.id)
            updated_hashes[card_key] = content_hash

        try:
            # First card includes the command text as message content (keeps it copyable).
            await upsert("rsadminbot_commands", cmd_hash, cmd_embed, None, content=cmd_body)
            for key, h, embed, view in pages:
                if key == "rsadminbot_commands":
                    continue
                await upsert(key, h, embed, view, content=None)
        except Exception:
            return

        self.test_server_organizer.set_meta("commands_cards", updated_cards)
        self.test_server_organizer.set_meta("commands_cards_hash", updated_hashes)
    
    
    def _execute_ssh_command(self, command: str, timeout: int = 30, *, log_it: bool = True) -> Tuple[bool, str, str]:
        """Execute SSH command and return (success, stdout, stderr)
        
        Uses shell=False to prevent PowerShell parsing on Windows.
        Commands are executed inside remote bash shell.
        """
        # Normalize newlines (CRLF -> LF) before execution.
        #
        # Why: On Oracle Ubuntu, if this file was ever copied from Windows with CRLF,
        # multi-line command strings can carry `\r` into bash and cause confusing non-zero exits
        # (e.g., `exit 0\r`), while still printing "OK=1" lines.
        cmd_txt = str(command or "")
        if "\r" in cmd_txt:
            cmd_txt = cmd_txt.replace("\r\n", "\n").replace("\r", "\n")

        # Check if server is configured (canonical: oraclekeys/servers.json + ssh_server_name selector)
        if not self.current_server:
            error_msg = "No SSH server configured (missing ssh_server_name / servers.json selection)"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            return False, "", error_msg

        # Local execution mode (Ubuntu host): run commands directly in bash without SSH.
        if self._should_use_local_exec():
            try:
                # Log SSH command before execution
                if log_it and hasattr(self, 'logger') and self.logger:
                    self.logger.log_ssh_command(cmd_txt, None, None, None, None)
                
                result = subprocess.run(
                    ["bash", "-lc", cmd_txt],
                    shell=False,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    encoding="utf-8",
                    errors="replace",
                )
                stdout_clean = (result.stdout or "").strip()
                stderr_clean = (result.stderr or "").strip()
                success = result.returncode == 0
                
                # Log SSH command result
                if log_it and hasattr(self, 'logger') and self.logger:
                    self.logger.log_ssh_command(cmd_txt, success, stdout_clean, stderr_clean, None)
                
                if not success:
                    print(f"{Colors.RED}[Local Exec Error] Command failed: {cmd_txt[:100]}{Colors.RESET}")
                    if stderr_clean:
                        print(f"{Colors.RED}[Local Exec Error] {stderr_clean[:200]}{Colors.RESET}")
                return success, stdout_clean, stderr_clean
            except subprocess.TimeoutExpired:
                error_msg = f"Command timed out after {timeout}s"
                print(f"{Colors.RED}[Local Exec Error] {error_msg}{Colors.RESET}")
                if log_it and hasattr(self, 'logger') and self.logger:
                    self.logger.log_ssh_command(command, False, None, error_msg, None)
                return False, "", error_msg
            except Exception as e:
                error_msg = f"Unexpected error executing local command: {str(e)}"
                print(f"{Colors.RED}[Local Exec Error] {error_msg}{Colors.RESET}")
                if log_it and hasattr(self, 'logger') and self.logger:
                    self.logger.log_ssh_command(command, False, None, error_msg, None)
                return False, "", error_msg
        
        # Build SSH command locally (self-contained)
        # Check if SSH key exists (already resolved in _load_ssh_config)
        ssh_key = str(self.current_server.get("key") or "").strip()
        if ssh_key:
            key_path = Path(ssh_key).expanduser()
            if not key_path.exists():
                # Key is optional; allow SSH to fall back to default identities/agent.
                self.current_server["key"] = ""
                print(f"{Colors.YELLOW}[SSH] Warning: SSH key file not found: {key_path}; continuing without -i{Colors.RESET}")
        
        try:
            # Log SSH command before execution
            if log_it and hasattr(self, 'logger') and self.logger:
                self.logger.log_ssh_command(cmd_txt, None, None, None, None)
            
            # Build SSH base command locally (self-contained)
            base = self._build_ssh_base(self.current_server)
            if not base:
                error_msg = "Failed to build SSH base command (check server config)"
                print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
                if log_it and hasattr(self, 'logger') and self.logger:
                    self.logger.log_ssh_command(cmd_txt, False, None, error_msg, None)
                return False, "", error_msg
            
            # Build command as list (no local shell parsing on Windows).
            #
            # IMPORTANT:
            # - SSH options MUST come before the `user@host` target.
            # - Do NOT shlex.quote() the bash -lc payload here; we're not going through a shell.
            #   If we pass a literally quoted string, bash will treat the quotes as part of the
            #   command text and fail (e.g., trying to execute a command named "set -euo ...").
            host_target = base[-1]
            base_no_host = base[:-1]
            # Avoid forcing a TTY; it can add control chars and isn't required for our non-interactive sudo usage.
            cmd = base_no_host + ["-o", "ConnectTimeout=10", host_target, "bash", "-lc", cmd_txt]
            
            # Suppress verbose output - only log errors
            is_validation = cmd_txt.strip() == "sudo -n true"
            
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
            success = result.returncode == 0
            
            # Log SSH command result
            if log_it and hasattr(self, 'logger') and self.logger:
                self.logger.log_ssh_command(cmd_txt, success, stdout_clean, stderr_clean, None)
            
            # Only log errors, not every command execution
            if not success:
                if not is_validation:
                    print(f"{Colors.RED}[SSH Error] Command failed: {cmd_txt[:100]}{Colors.RESET}")
                    if stderr_clean:
                        print(f"{Colors.RED}[SSH Error] {stderr_clean[:200]}{Colors.RESET}")
                    if stdout_clean:
                        print(f"{Colors.YELLOW}[SSH Error] {stdout_clean[:200]}{Colors.RESET}")
            
            return success, stdout_clean, stderr_clean
        except subprocess.TimeoutExpired:
            error_msg = f"Command timed out after {timeout}s"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.RED}[SSH Error] Command: {cmd_txt[:200]}{Colors.RESET}")
            if log_it and hasattr(self, 'logger') and self.logger:
                self.logger.log_ssh_command(cmd_txt, False, None, error_msg, None)
            return False, "", error_msg
        except FileNotFoundError as e:
            error_msg = f"SSH executable not found: {e}"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.YELLOW}[SSH Error] Make sure SSH is installed and in PATH{Colors.RESET}")
            if log_it and hasattr(self, 'logger') and self.logger:
                self.logger.log_ssh_command(cmd_txt, False, None, error_msg, None)
            return False, "", error_msg
        except Exception as e:
            error_msg = f"Unexpected error executing SSH command: {str(e)}"
            print(f"{Colors.RED}[SSH Error] {error_msg}{Colors.RESET}")
            print(f"{Colors.RED}[SSH Error] Command: {cmd_txt[:200]}{Colors.RESET}")
            import traceback
            print(f"{Colors.RED}[SSH Error] Traceback: {traceback.format_exc()[:500]}{Colors.RESET}")
            if log_it and hasattr(self, 'logger') and self.logger:
                self.logger.log_ssh_command(cmd_txt, False, None, error_msg, None)
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
                    if hasattr(self, 'logger') and self.logger:
                        self.logger.log_config_validation("config_secrets", "missing", f"Missing config.secrets.json: {secrets_path}", {"secrets_path": str(secrets_path)})
                print(f"{Colors.GREEN}[Config] Loaded configuration{Colors.RESET}")
                if hasattr(self, 'logger') and self.logger:
                    self.logger.log_config_validation("config_load", "valid", "Configuration loaded successfully", {"config_path": str(self.config_path)})
            except Exception as e:
                print(f"{Colors.RED}[Config] Failed to load config: {e}{Colors.RESET}")
                self.config = default_config
                if hasattr(self, 'logger') and self.logger:
                    self.logger.log_config_validation("config_load", "invalid", f"Failed to load config: {e}", {"error": str(e)})
        else:
            self.config = default_config
            self.save_config()
            print(f"{Colors.YELLOW}[Config] Created default config.json - please configure it{Colors.RESET}")
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_config_validation("config_load", "warning", "Created default config.json - needs configuration", {})
    
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
    
    def is_admin(self, user: discord.Member, *, allow_administrator_permission: bool = True) -> bool:
        """Check if user is an admin.

        Config-driven checks:
        - config.admin_user_ids
        - config.admin_role_ids

        Optionally:
        - allow_administrator_permission=True also treats Discord "Administrator" permission as admin.
        """
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
        
        # Optional: Check if user has administrator permission
        if allow_administrator_permission and user.guild_permissions.administrator:
            return True
        
        return False

    def _get_test_server_guild_id(self) -> int:
        """Return the configured neo-test-server guild id (0 if missing)."""
        try:
            return int(self.config.get("test_server_guild_id") or 0)
        except Exception:
            return 0

    def _get_allowed_slash_guild_ids(self) -> List[int]:
        """Guild ids where RSAdminBot slash commands are enabled."""
        # Slash commands are only enabled in neo-test-server.
        try:
            gid = int(self.config.get("test_server_guild_id") or 0)
        except Exception:
            gid = 0
        return [gid] if gid else []

    async def _slash_owner_guard(self, interaction: discord.Interaction) -> tuple[bool, str]:
        """Guard for all RSAdminBot slash commands: allowed guild(s) + owner/admin only.

        Important: Component interactions (buttons/selects) sometimes arrive without a cached
        `discord.Member` instance. In that case we fetch the member so role-based admin checks work.
        """
        if not interaction or not getattr(interaction, "user", None):
            return False, "❌ Missing interaction user."
        if not getattr(interaction, "guild", None):
            return False, "❌ This command can only be used in the server."

        allowed = self._get_allowed_slash_guild_ids()
        guild_id = int(getattr(interaction.guild, "id", 0) or 0)
        if allowed and guild_id not in allowed:
            return False, "❌ This command is not enabled in this guild."

        owner_id = int(getattr(interaction.guild, "owner_id", 0) or 0)
        user_id = int(getattr(interaction.user, "id", 0) or 0)

        # Allow server owner.
        if owner_id and user_id == owner_id:
            return True, ""

        # Allow configured admins (user allowlist) WITHOUT requiring a Member object.
        #
        # Why: some interaction payloads (especially component interactions) may not include a cached Member.
        # `admin_user_ids` should still work even if member/roles are unavailable.
        try:
            admin_user_ids = self.config.get("admin_user_ids", []) if isinstance(self.config, dict) else []
            if str(user_id) in [str(uid) for uid in (admin_user_ids or [])]:
                return True, ""
        except Exception:
            pass

        # Allow configured admins (role allowlist). Do NOT auto-allow "Administrator" permission here.
        member: Optional[discord.Member] = None
        try:
            if isinstance(interaction.user, discord.Member):
                member = interaction.user
            else:
                member = interaction.guild.get_member(user_id)
                if member is None:
                    try:
                        member = await interaction.guild.fetch_member(user_id)
                    except Exception:
                        member = None
        except Exception:
            member = None

        if member and self.is_admin(member, allow_administrator_permission=False):
            return True, ""

        return False, "❌ Owner/Admin-only command."

    async def _interaction_reply(
        self,
        interaction: discord.Interaction,
        *,
        content: str | None = None,
        embed: discord.Embed | None = None,
        view: ui.View | None = None,
        ephemeral: bool = True,
    ) -> None:
        """Send a reply to an interaction (handles response vs followup)."""
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content=content, embed=embed, view=view, ephemeral=ephemeral)
            else:
                await interaction.followup.send(content=content, embed=embed, view=view, ephemeral=ephemeral)
        except Exception:
            # Never raise from a response helper.
            return

    def _bot_folder_path(self, bot_key: str) -> Path:
        info = self.BOTS.get(str(bot_key or "").strip().lower()) or {}
        folder = str(info.get("folder") or "").strip()
        return (self.base_path.parent / folder).resolve()

    def _json_load_file(self, path: Path) -> Tuple[bool, Dict[str, Any], str]:
        try:
            if not path.exists():
                return False, {}, f"Missing file: {path}"
            data = json.loads(path.read_text(encoding="utf-8") or "{}")
            if not isinstance(data, dict):
                return False, {}, "JSON root must be an object"
            return True, data, ""
        except Exception as e:
            return False, {}, str(e)

    def _json_write_file(self, path: Path, data: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
            tmp.replace(path)
            return True, ""
        except Exception as e:
            return False, str(e)

    def _set_json_path(self, root: Dict[str, Any], key_path: str, value: Any) -> Tuple[bool, str]:
        path = str(key_path or "").strip()
        if not path:
            return False, "key_path is required"
        parts = [p for p in path.split(".") if p.strip()]
        if not parts:
            return False, "key_path is required"
        cur: Any = root
        for p in parts[:-1]:
            if not isinstance(cur, dict):
                return False, f"Cannot descend into non-object at '{p}'"
            if p not in cur or not isinstance(cur.get(p), dict):
                cur[p] = {}
            cur = cur[p]
        last = parts[-1]
        if not isinstance(cur, dict):
            return False, f"Cannot set '{last}' on non-object"
        cur[last] = value
        return True, ""

    def _update_bot_config_json(self, bot_key: str, key_path: str, json_value_text: str) -> Tuple[bool, Dict[str, Any]]:
        bot_key = str(bot_key or "").strip().lower()
        folder = self._bot_folder_path(bot_key)
        cfg_path = folder / "config.json"
        ok, data, err = self._json_load_file(cfg_path)
        if not ok:
            return False, {"error": err}

        try:
            new_value = json.loads(str(json_value_text or "").strip())
        except Exception as e:
            return False, {"error": f"Value must be valid JSON: {e}"}

        ok_set, err_set = self._set_json_path(data, key_path, new_value)
        if not ok_set:
            return False, {"error": err_set}

        # Backup then write
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = folder / "backups"
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"config.json.{ts}.bak"
            backup_path.write_text(cfg_path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            backup_path = None

        ok_w, err_w = self._json_write_file(cfg_path, data)
        if not ok_w:
            return False, {"error": err_w}

        return True, {"path": str(cfg_path), "backup": str(backup_path) if backup_path else "", "key_path": key_path, "value_masked": str(new_value)[:200]}

    def _update_bot_secrets_json(self, bot_key: str, key_path: str, json_value_text: str) -> Tuple[bool, Dict[str, Any]]:
        bot_key = str(bot_key or "").strip().lower()
        folder = self._bot_folder_path(bot_key)
        secrets_path = folder / "config.secrets.json"
        ok, data, err = self._json_load_file(secrets_path)
        if not ok:
            return False, {"error": err}

        try:
            new_value = json.loads(str(json_value_text or "").strip())
        except Exception as e:
            return False, {"error": f"Value must be valid JSON: {e}"}

        ok_set, err_set = self._set_json_path(data, key_path, new_value)
        if not ok_set:
            return False, {"error": err_set}

        # Backup then write
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = folder / "backups"
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = backup_dir / f"config.secrets.json.{ts}.bak"
            backup_path.write_text(secrets_path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            backup_path = None

        ok_w, err_w = self._json_write_file(secrets_path, data)
        if not ok_w:
            return False, {"error": err_w}
        try:
            os.chmod(secrets_path, 0o600)
        except Exception:
            pass

        return True, {"path": str(secrets_path), "backup": str(backup_path) if backup_path else "", "key_path": key_path, "value_masked": mask_secret(str(new_value)) if isinstance(new_value, str) else "(non-string)"}

    def _get_rs_bot_keys(self) -> List[str]:
        """Return RS-only bot keys (rsadminbot + bot_groups.rs_bots)."""
        bot_groups = self.config.get("bot_groups") or {}
        rs_keys = ["rsadminbot"] + list(bot_groups.get("rs_bots") or [])
        # Keep stable ordering and only include known BOTS
        out = []
        for k in rs_keys:
            k = str(k).strip().lower()
            if not k:
                continue
            if k in self.BOTS and k not in out:
                out.append(k)
        return out

    def _get_mw_bot_keys(self) -> List[str]:
        """Return Mirror-World-only bot keys (bot_groups.mirror_bots)."""
        bot_groups = self.config.get("bot_groups") or {}
        mw_keys = list(bot_groups.get("mirror_bots") or [])
        out: List[str] = []
        for k in mw_keys:
            k = str(k).strip().lower()
            if not k:
                continue
            if k in self.BOTS and k not in out:
                out.append(k)
        return out

    def _get_update_code_root_for_group(self, bot_group: str) -> str:
        """Return the configured GitHub checkout root used for python-only updates."""
        cfg = self.config.get("code_checkouts") if isinstance(self.config, dict) else {}
        if not isinstance(cfg, dict):
            cfg = {}

        group = str(bot_group or "").strip().lower()
        if group in ("rsadminbot", "rs_bots"):
            return str(cfg.get("rsbots_code_root") or "/home/rsadmin/bots/rsbots-code").strip()
        if group == "mirror_bots":
            return str(cfg.get("mwbots_code_root") or "/home/rsadmin/bots/mwbots-code").strip()
        return ""

    async def _commands_send_for_bot(
        self,
        *,
        bot_key: str,
        send: Callable[..., Awaitable[Any]],
        triggered_by: Optional[Any] = None,
        repo_root: Optional[Path] = None,
    ) -> None:
        """Send COMMANDS.md content for a bot key using the provided send() coroutine."""
        repo_root = repo_root or _REPO_ROOT
        who = f"Triggered by {triggered_by}" if triggered_by else None

        bot_key_norm = str(bot_key or "").strip().lower()
        if bot_key_norm not in self.BOTS:
            available_bots = ", ".join(sorted(self.BOTS.keys()))
            error_embed = MessageHelper.create_error_embed(
                title="Unknown Bot",
                message=f"Bot '{bot_key}' not found in bot registry.",
                error_details=f"Available bots: {available_bots}",
                footer=who,
            )
            await send(embed=error_embed)
            return

        bot_info = self.BOTS[bot_key_norm]
        bot_folder = bot_info.get("folder", "")
        if not bot_folder:
            error_embed = MessageHelper.create_error_embed(
                title="Bot Folder Not Configured",
                message=f"Bot '{bot_key_norm}' does not have a folder configured in bot registry.",
                footer=who,
            )
            await send(embed=error_embed)
            return

        commands_file = repo_root / bot_folder / "COMMANDS.md"
        if not commands_file.exists():
            error_embed = MessageHelper.create_error_embed(
                title="Commands File Not Found",
                message=f"COMMANDS.md not found for {bot_info.get('name', bot_key_norm)}.",
                error_details=f"Expected path: {commands_file}",
                footer=who,
            )
            await send(embed=error_embed)
            return

        try:
            content = commands_file.read_text(encoding="utf-8")
        except Exception as e:
            error_embed = MessageHelper.create_error_embed(
                title="File Read Error",
                message=f"Failed to read COMMANDS.md for {bot_info.get('name', bot_key_norm)}.",
                error_details=str(e)[:200],
                footer=who,
            )
            await send(embed=error_embed)
            return

        # Render COMMANDS.md as "real help" embeds (one field per command).
        # This avoids dumping the entire doc into a single wrapper block.

        def _strip_ticks(s: str) -> str:
            s = str(s or "").strip()
            if s.startswith("`") and s.endswith("`") and len(s) >= 2:
                s = s[1:-1].strip()
            return s

        def _infer_prefix_from_commands(cmd_names: List[str]) -> str:
            # Prefer the most common first token (e.g. ".checker") or the leading symbol (e.g. "!").
            toks: List[str] = []
            for name in cmd_names:
                n = str(name or "").strip()
                if not n:
                    continue
                first = n.split()[0]
                toks.append(first)
            if not toks:
                return "?"
            # Most common
            counts: Dict[str, int] = {}
            for t in toks:
                counts[t] = counts.get(t, 0) + 1
            best = max(counts.items(), key=lambda kv: kv[1])[0]
            # Normalize: "!something" -> "!" (unless it's a unique prefix like "!rs")
            if best.startswith("!") and len(best) > 1 and best != "!rs":
                return "!"
            return best

        def _parse_commands_md(text: str) -> Tuple[List[Dict[str, Any]], List[str]]:
            """Parse our COMMANDS.md convention into command dicts.

            Returns:
              (commands, categories_in_order)
            """
            commands_out: List[Dict[str, Any]] = []
            categories: List[str] = []
            category = "Commands"
            current: Optional[Dict[str, Any]] = None
            pending_key: Optional[str] = None

            def _finish_current():
                nonlocal current, pending_key
                if current:
                    commands_out.append(current)
                current = None
                pending_key = None

            for raw in (text or "").splitlines():
                line = (raw or "").rstrip("\n")
                if line.startswith("### ") and not line.startswith("#### "):
                    category = line[4:].strip() or "Commands"
                    if category not in categories:
                        categories.append(category)
                    pending_key = None
                    continue
                if line.startswith("#### "):
                    _finish_current()
                    cmd_name = _strip_ticks(line[5:].strip())
                    current = {
                        "name": cmd_name,
                        "category": category,
                        "kv": {},
                    }
                    continue
                if not current:
                    continue

                m = re.match(r"^- \*\*(.+?)\*\*:\s*(.*)$", line)
                if m:
                    key = str(m.group(1) or "").strip().lower()
                    val = str(m.group(2) or "").strip()
                    if val == "":
                        current["kv"][key] = []
                        pending_key = key
                    else:
                        current["kv"][key] = val
                        pending_key = key
                    continue

                # Continuation lines for multi-line fields like Parameters / Usage.
                if pending_key and (line.startswith("  -") or line.startswith("    -") or line.startswith("   ")):
                    bucket = current["kv"].get(pending_key)
                    if isinstance(bucket, list):
                        bucket.append(line.strip())
                    else:
                        # Convert a previous string value into a list, preserving it.
                        current["kv"][pending_key] = [str(bucket).strip(), line.strip()] if bucket else [line.strip()]
                    continue

                # If we hit a non-indented line, stop collecting continuation.
                pending_key = None

            _finish_current()
            return commands_out, categories

        def _format_value(cmd: Dict[str, Any]) -> str:
            kv = cmd.get("kv") or {}

            def _get(key: str):
                return kv.get(key)

            def _fmt_block(label: str, value: Any) -> List[str]:
                if value is None:
                    return []
                if isinstance(value, list):
                    if not value:
                        return []
                    # Keep list items readable; they already start with '-' in our docs.
                    body = "\n".join(value)
                    return [f"**{label}**:\n{body}"]
                s = str(value).strip()
                if not s:
                    return []
                return [f"**{label}**: {s}"]

            # Prefer a consistent "help" ordering
            parts: List[str] = []
            parts += _fmt_block("Description", _get("description"))
            parts += _fmt_block("Usage", _get("usage"))
            parts += _fmt_block("Aliases", _get("aliases"))
            parts += _fmt_block("Parameters", _get("parameters"))
            parts += _fmt_block("Returns", _get("returns"))
            parts += _fmt_block("Note", _get("note"))
            parts += _fmt_block("Admin Only", _get("admin only"))

            text = "\n".join(parts).strip() or "—"
            if len(text) > 1000:
                text = text[:997] + "..."
            return text

        # Prefer the explicit prefix declared in COMMANDS.md (most reliable).
        doc_prefix = None
        try:
            m_pref = re.search(r"\\*\\*Command Prefix\\*\\*:\\s*`([^`]+)`", content or "")
            if m_pref:
                doc_prefix = str(m_pref.group(1) or "").strip()
        except Exception:
            doc_prefix = None

        parsed_cmds, categories = _parse_commands_md(content)
        if not parsed_cmds:
            # Fallback: if parsing fails, still show raw doc (better than silence).
            embed = discord.Embed(
                title=f"📋 {bot_info.get('name', bot_key_norm)} Commands",
                description=(content[:3900] + "\n…(truncated)") if len(content) > 3900 else (content or "(empty)"),
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc),
            )
            if who:
                embed.set_footer(text=who)
            await send(embed=embed)
            return

        prefix = doc_prefix or _infer_prefix_from_commands([c.get("name") for c in parsed_cmds])
        by_cat: Dict[str, List[Dict[str, Any]]] = {}
        for c in parsed_cmds:
            by_cat.setdefault(str(c.get("category") or "Commands"), []).append(c)

        # Build and send embeds per category, chunked by field limits.
        for cat in (categories or list(by_cat.keys())):
            cmd_list = by_cat.get(cat) or []
            if not cmd_list:
                continue

            pages: List[discord.Embed] = []
            cur = discord.Embed(
                title=f"📖 {bot_info.get('name', bot_key_norm)} — {cat}",
                description=f"Prefix: `{prefix}` • Commands: {len(cmd_list)}",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc),
            )
            if who:
                cur.set_footer(text=who)

            for cmd in cmd_list:
                name = str(cmd.get("name") or "").strip() or "(unnamed)"
                value = _format_value(cmd)
                # Discord limits: 25 fields per embed.
                if len(cur.fields) >= 25:
                    pages.append(cur)
                    cur = discord.Embed(
                        title=f"📖 {bot_info.get('name', bot_key_norm)} — {cat}",
                        description=f"Prefix: `{prefix}` • Commands: {len(cmd_list)}",
                        color=discord.Color.blue(),
                        timestamp=datetime.now(timezone.utc),
                    )
                    if who:
                        cur.set_footer(text=who)
                cur.add_field(name=name[:256], value=value[:1024], inline=False)

            pages.append(cur)

            total_pages = len(pages)
            for idx, e in enumerate(pages, start=1):
                if total_pages > 1:
                    e.title = f"{e.title} ({idx}/{total_pages})"
                await send(embed=e)

    def _build_botconfig_embed(self, bot_name: str, *, triggered_by: Optional[Any] = None) -> discord.Embed:
        """Build a botconfig embed for a bot (RS inspector when available; MW file-based summary otherwise)."""
        who = f"Triggered by {triggered_by}" if triggered_by else None
        bot_key = str(bot_name or "").strip().lower()

        if not bot_key or bot_key not in self.BOTS:
            return MessageHelper.create_error_embed(
                title="Unknown Bot",
                message=f"Bot not found: `{bot_name}`",
                footer=who,
            )

        group = self._get_bot_group(bot_key) or ""
        info = self.BOTS.get(bot_key) or {}

        # Mirror bots are file-based (settings.json + tokens.env + optional routing maps)
        if group == "mirror_bots":
            bot_display_name = str(info.get("name") or bot_key)
            folder = str(info.get("folder") or "").strip() or bot_key
            service = str(info.get("service") or "").strip()
            remote_root = str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world").strip()
            config_dir = f"{remote_root}/{folder}/config"

            required = ["settings.json", "tokens.env"]
            optional: List[str] = []
            if bot_key == "discumbot":
                required = ["settings.json", "tokens.env", "channel_map.json", "destination_channels.json"]
                optional = ["source_channels.json"]
            elif bot_key == "datamanagerbot":
                optional = ["keywords.json", "fetchall_mappings.json"]

            files = required + optional
            checks: Dict[str, bool] = {}
            try:
                cmd = "set +e; " + " ; ".join(
                    f'test -f {shlex.quote(config_dir + "/" + fn)} && echo OK:{fn} || echo MISSING:{fn}'
                    for fn in files
                )
                ok, out, err = self._execute_ssh_command(cmd, timeout=12)
                text = (out or err or "").splitlines()
                for ln in text:
                    ln = (ln or "").strip()
                    if ln.startswith("OK:"):
                        checks[ln[3:].strip()] = True
                    elif ln.startswith("MISSING:"):
                        checks[ln[8:].strip()] = False
            except Exception:
                checks = {}

            lines_req = []
            for fn in required:
                state = checks.get(fn)
                icon = "✅" if state is True else ("❌" if state is False else "❓")
                label = fn
                if fn == "tokens.env":
                    label = "tokens.env (secret)"
                lines_req.append(f"{icon} {label}")

            lines_opt = []
            for fn in optional:
                state = checks.get(fn)
                icon = "✅" if state is True else ("➖" if state is False else "❓")
                lines_opt.append(f"{icon} {fn}")

            embed = MessageHelper.create_info_embed(
                title=f"⚙️ {bot_display_name} Configuration",
                message="Mirror-World bots use file-based config under `config/` (tokens.env is never shown in Discord).",
                fields=[
                    {"name": "Bot", "value": f"`{bot_key}`", "inline": True},
                    {"name": "Service", "value": f"`{service or '(missing)'}`", "inline": False},
                    {"name": "Config dir", "value": f"`{config_dir}`", "inline": False},
                    {"name": "Required files", "value": "\n".join(lines_req)[:1024], "inline": False},
                ],
                footer=who,
            )
            if lines_opt:
                embed.add_field(name="Optional files", value="\n".join(lines_opt)[:1024], inline=False)
            embed.add_field(
                name="Next",
                value="After config is in place: `!botrestart <bot>` or restart the service on Oracle.",
                inline=False,
            )
            return embed

        # RS bots: inspector-based config render
        if not INSPECTOR_AVAILABLE or not self.inspector:
            return MessageHelper.create_error_embed(
                title="Bot Inspector Not Available",
                message="Bot inspector module is not loaded or initialized (RS config view requires it).",
                footer=who,
            )

        config = self.inspector.get_bot_config(bot_key)
        if not config:
            return MessageHelper.create_error_embed(
                title="Config Not Found",
                message=f"Bot not found or no config: `{bot_key}`",
                footer=who,
            )

        # Get bot display name
        bot_display_name = bot_key
        if bot_key in self.BOTS:
            bot_display_name = self.BOTS[bot_key]["name"]

        try:
            embed = discord.Embed(
                title=f"⚙️ {bot_display_name} Configuration",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )

            # Basic settings
            if "bot_token" in config:
                embed.add_field(
                    name="🔐 Authentication",
                    value="✅ Token configured (hidden)",
                    inline=False
                )

            if "guild_id" in config:
                embed.add_field(
                    name="🏠 Server ID",
                    value=f"`{config.get('guild_id')}`",
                    inline=True
                )

            if "brand_name" in config:
                embed.add_field(
                    name="🏷️ Brand Name",
                    value=str(config.get("brand_name") or ""),
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
                    value="\n".join(channel_fields)[:1000],
                    inline=False
                )

            # Forwarder channels array
            if "channels" in config and isinstance(config["channels"], list):
                channels_info = []
                for i, channel in enumerate(config["channels"][:5], 1):
                    source_name = channel.get("source_channel_name", "Unknown")
                    source_id = channel.get("source_channel_id", "N/A")
                    role_id = (channel.get("role_mention") or {}).get("role_id", "None")
                    channels_info.append(f"**{i}. {source_name}**\n   Source: `{source_id}`\n   Role: `{role_id}`")
                if len(config["channels"]) > 5:
                    channels_info.append(f"\n*... and {len(config['channels']) - 5} more channel(s)*")
                embed.add_field(
                    name="🔄 Forwarding Channels",
                    value="\n".join(channels_info)[:1000],
                    inline=False
                )

            # Invite tracking
            if "invite_tracking" in config and isinstance(config.get("invite_tracking"), dict):
                invite = config["invite_tracking"]
                invite_info = []
                if "invite_channel_id" in invite:
                    invite_info.append(f"📨 Invite Channel: `{invite['invite_channel_id']}`")
                if "fallback_invite" in invite and invite.get("fallback_invite"):
                    fb = str(invite.get("fallback_invite"))
                    invite_info.append(f"🔗 Fallback: `{fb[:50]}...`" if len(fb) > 50 else f"🔗 Fallback: `{fb}`")
                if invite_info:
                    embed.add_field(
                        name="📨 Invite Tracking",
                        value="\n".join(invite_info)[:1000],
                        inline=False
                    )

            # DM sequence
            if "dm_sequence" in config and isinstance(config.get("dm_sequence"), dict):
                dm = config["dm_sequence"]
                dm_info = []
                if "send_spacing_seconds" in dm:
                    dm_info.append(f"⏱️ Spacing: {dm['send_spacing_seconds']}s")
                if "day_gap_hours" in dm:
                    dm_info.append(f"📅 Day Gap: {dm['day_gap_hours']}h")
                if dm_info:
                    embed.add_field(
                        name="💬 DM Sequence",
                        value="\n".join(dm_info)[:1000],
                        inline=True
                    )

            # Tickets / success
            if "ticket_category_id" in config:
                embed.add_field(
                    name="🎫 Tickets",
                    value=f"Category: `{config['ticket_category_id']}`",
                    inline=True
                )
            if "success_channel_ids" in config:
                count = len(config["success_channel_ids"]) if isinstance(config["success_channel_ids"], list) else 1
                embed.add_field(
                    name="🏆 Success Channels",
                    value=f"{count} channel(s) configured",
                    inline=True
                )

            # Other short scalar fields
            other_fields = []
            skip = {
                "bot_token", "guild_id", "brand_name", "log_channel_id",
                "forwarding_logs_channel_id", "whop_logs_channel_id", "ssh_commands_channel_id",
                "channels", "invite_tracking", "dm_sequence", "ticket_category_id", "success_channel_ids",
            }
            for key, value in config.items():
                if key in skip:
                    continue
                if isinstance(value, (str, int, float, bool)):
                    if len(str(value)) < 100:
                        other_fields.append(f"**{key.replace('_', ' ').title()}**: `{value}`")
            if other_fields:
                other_text = "\n".join(other_fields[:10])
                if len(other_fields) > 10:
                    other_text += f"\n*... and {len(other_fields) - 10} more field(s)*"
                embed.add_field(
                    name="⚙️ Other Settings",
                    value=other_text[:1000],
                    inline=False
                )

            embed.set_footer(text=who or "Use !botconfig <bot> to view full config")
            return embed
        except Exception as e:
            return MessageHelper.create_error_embed(
                title="Config Render Error",
                message=f"Failed to render config for `{bot_name}`.",
                error_details=str(e)[:900],
                footer=who,
            )
    
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
                try:
                    line = str(getattr(self, "_rsnotes_status_line", "") or "")
                    if line:
                        print(line[:400])
                except Exception:
                    pass
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

                # RSNotes (private slash command: /rsnote)
                try:
                    if not getattr(self, "_rsnotes_initialized", False):
                        await self._initialize_rsnotes()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] RSNotes initialization failed (non-critical): {str(e)[:200]}{Colors.RESET}")

                # Import and run startup sequences
                try:
                    from startup_sequences import (
                        sequence_1_initialization,
                        sequence_2_tracking,
                        sequence_3_server_status,
                        sequence_4_file_sync,
                        sequence_5_channels,
                        sequence_6_background,
                    )

                    await sequence_1_initialization.run(self)
                    await sequence_2_tracking.run(self)
                    await sequence_3_server_status.run(self)
                    await sequence_4_file_sync.run(self)
                    await sequence_5_channels.run(self)
                    await sequence_6_background.run(self)
                except ImportError as e:
                    print(f"{Colors.YELLOW}[Startup] Startup sequences not available (non-critical): {e}{Colors.RESET}")
                    import traceback
                    print(f"{Colors.DIM}[Startup] Import traceback: {traceback.format_exc()[:300]}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] Startup sequences error (non-critical): {e}{Colors.RESET}")
                    import traceback
                    print(f"{Colors.DIM}[Startup] Sequence traceback: {traceback.format_exc()[:500]}{Colors.RESET}")

                # Initialize monitor channels (per-bot channels in test server)
                try:
                    await self._initialize_monitor_channels()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] Monitor channel initialization failed (non-critical): {e}{Colors.RESET}")

                # Initialize per-bot journal live channels + webhooks (test server only)
                try:
                    await self._initialize_journal_live()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] Journal live initialization failed (non-critical): {e}{Colors.RESET}")

                # Initialize Whop webhook receiver (test server raw logs)
                try:
                    await self._initialize_whop_webhook_receiver()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] Whop webhook receiver failed (non-critical): {e}{Colors.RESET}")

                # Publish a command catalog to the configured channel (optional).
                try:
                    await self._publish_command_index_to_configured_channel()
                except Exception as e:
                    print(f"{Colors.YELLOW}[Startup] Commands catalog publish failed (non-critical): {str(e)[:200]}{Colors.RESET}")

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

                        ok_j, out_j, _ = self._execute_ssh_command(
                            "journalctl -u mirror-world-rsadminbot.service -n 40 --no-pager | tail -n 40",
                            timeout=20,
                        )
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

            except Exception as e:
                # Critical error - log but don't prevent bot from running
                print(f"{Colors.RED}[Startup] Critical error in on_ready (continuing anyway): {e}{Colors.RESET}")
                import traceback
                print(f"{Colors.RED}[Startup] Full traceback: {traceback.format_exc()}{Colors.RESET}")
            
            # Always emit RSNotes status near the end (journal-live truncates earlier lines)
            try:
                line = str(getattr(self, "_rsnotes_status_line", "") or "")
                if line:
                    print(line[:400])
            except Exception:
                pass

            # Always log completion
            print(f"{Colors.GREEN}[Startup] ✓ on_ready completed successfully{Colors.RESET}")
            print(f"{Colors.GREEN}[Startup] ✓ Bot is ready and accepting commands{Colors.RESET}\n")
        
        # No on_message-based tracking: RSAdminBot is slash-only and avoids channel spam.
        
        @self.bot.event
        async def on_command_error(ctx, error):
            """Handle command errors"""
            if isinstance(error, commands.CommandNotFound):
                try:
                    msg_txt = (getattr(getattr(ctx, "message", None), "content", "") or "")[:500]
                    who = str(getattr(ctx, "author", "") or "")
                    where = str(getattr(getattr(ctx, "channel", None), "name", "") or "")
                    print(f"{Colors.YELLOW}[CommandNotFound] user={who} channel={where} msg={msg_txt}{Colors.RESET}")
                except Exception:
                    pass
                return  # Ignore unknown commands (but log to terminal)
            elif isinstance(error, commands.CheckFailure):
                # Most admin-gated commands use commands.check(self.is_admin), which raises CheckFailure (not MissingPermissions).
                print(f"{Colors.YELLOW}[Command Error] CheckFailure: {ctx.author} tried to use {ctx.command}{Colors.RESET}")
                embed = MessageHelper.create_error_embed(
                    title="Missing Permissions",
                    message="You don't have permission to use this command.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                await self._log_to_discord(embed, None)
            elif isinstance(error, commands.MissingPermissions):
                print(f"{Colors.YELLOW}[Command Error] Missing permissions: {ctx.author} tried to use {ctx.command}{Colors.RESET}")
                embed = MessageHelper.create_error_embed(
                    title="Missing Permissions",
                    message="You don't have permission to use this command.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                await self._log_to_discord(embed, None)
            elif isinstance(error, commands.CommandOnCooldown):
                print(f"{Colors.YELLOW}[Command Error] Cooldown: {ctx.author} tried to use {ctx.command} too soon{Colors.RESET}")
                embed = MessageHelper.create_warning_embed(
                    title="Command Cooldown",
                    message=f"Please wait {error.retry_after:.1f} seconds.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                await self._log_to_discord(embed, None)
            else:
                error_msg = str(error)
                print(f"{Colors.RED}[Command Error] {error_msg}{Colors.RESET}")
                print(f"{Colors.RED}[Command Error] Command: {ctx.command}, User: {ctx.author}, Channel: {ctx.channel}{Colors.RESET}")
                import traceback
                print(f"{Colors.RED}[Command Error] Traceback:{Colors.RESET}")
                for line in traceback.format_exc().split('\n')[:10]:
                    if line.strip():
                        print(f"{Colors.RED}[Command Error]   {line}{Colors.RESET}")
                embed = MessageHelper.create_error_embed(
                    title="Command Error",
                    message="An error occurred while executing the command.",
                    error_details=error_msg[:500],
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                await self._log_to_discord(embed, None)

            # Structured logging for errors (file + log channel)
            if self.logger:
                try:
                    cmd_name = getattr(getattr(ctx, "command", None), "name", None) or "unknown"
                    log_entry = self.logger.log_command(
                        ctx,
                        cmd_name,
                        "error",
                        {"error": str(error)[:800], "error_type": type(error).__name__},
                    )
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), None)
                    self.logger.clear_command_context()
                except Exception:
                    pass

        @self.bot.event
        async def on_command(ctx):
            """Global command start hook (structured logging for ALL commands)."""
            if not self.logger:
                return
            try:
                cmd_name = getattr(getattr(ctx, "command", None), "name", None) or "unknown"
                try:
                    who = str(getattr(ctx, "author", "") or "")
                    where = str(getattr(getattr(ctx, "channel", None), "name", "") or "")
                    msg_txt = (getattr(getattr(ctx, "message", None), "content", "") or "")[:400]
                    print(f"{Colors.CYAN}[Command] user={who} channel={where} cmd={cmd_name} msg={msg_txt}{Colors.RESET}")
                except Exception:
                    pass
                # Avoid duplicating if command already started logging this run.
                current = getattr(self.logger, "_current_command_context", None) or {}
                if current.get("command") == cmd_name and (current.get("log_entry") or {}).get("status") == "pending":
                    return
                self.logger.log_command(
                    ctx,
                    cmd_name,
                    "pending",
                    {"content": (getattr(getattr(ctx, "message", None), "content", "") or "")[:300]},
                )
            except Exception:
                pass

        @self.bot.event
        async def on_command_completion(ctx):
            """Global command completion hook (structured logging for ALL commands)."""
            if not self.logger:
                return
            try:
                cmd_name = getattr(getattr(ctx, "command", None), "name", None) or "unknown"
                current = getattr(self.logger, "_current_command_context", None) or {}
                # If this command already logged a final status, don't double-log.
                if current.get("command") == cmd_name and (current.get("log_entry") or {}).get("status") in ("success", "error"):
                    self.logger.clear_command_context()
                    return
                log_entry = self.logger.log_command(ctx, cmd_name, "success", {})
                await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), None)
                self.logger.clear_command_context()
            except Exception:
                pass
    
    def _setup_commands(self):
        """Setup prefix commands"""
        # Track registered commands for initialization logging
        self.registered_commands = []
        
        @self.bot.command(name="ping")
        async def ping(ctx):
            """Check bot latency"""
            latency = round(self.bot.latency * 1000)
            embed = MessageHelper.create_info_embed(
                title="Pong",
                message="RSAdminBot is responding.",
                fields=[{"name": "Latency", "value": f"{latency}ms", "inline": True}],
                footer=f"Triggered by {ctx.author}",
            )
            await ctx.send(embed=embed)
            await self._log_to_discord(embed, ctx.channel)
            if self.logger:
                try:
                    log_entry = self.logger.log_command(ctx, "ping", "success", {"latency_ms": latency})
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                    self.logger.clear_command_context()
                except Exception:
                    pass
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
                ssh_status = "❌ **Not configured**\nSet `ssh_server_name` in RSAdminBot/config.json and ensure `oraclekeys/servers.json` exists"
            embed.add_field(
                name="🖥️ SSH Server",
                value=ssh_status,
                inline=True
            )
            
            # Module status
            modules_status = []
            modules_status.append("✅ Service Manager" if self.service_manager else "❌ Service Manager")
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

        @self.bot.command(name="commands", aliases=["listcommands", "cmds", "helpcommands"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def list_commands(ctx, bot_name: str = None):
            """List all available commands for a specific bot or all bots (admin only).
            
            Usage:
              !commands                    - Show all bots and their command counts
              !commands rsadminbot         - Show all RSAdminBot commands
              !commands rsforwarder        - Show all RSForwarder commands
              !commands rssuccessbot       - Show all RSSuccessBot commands
              !commands rsmentionpinger    - Show all RSMentionPinger commands
              !commands rsonboarding       - Show all RSOnboarding commands
              !commands rscheckerbot       - Show all RSCheckerbot commands
            """
            try:
                who = str(getattr(ctx, "author", "") or "")
                where = str(getattr(getattr(ctx, "channel", None), "name", "") or "")
                msg_txt = (getattr(getattr(ctx, "message", None), "content", "") or "")[:200]
                print(f"{Colors.CYAN}[commands] entered user={who} channel={where} msg={msg_txt}{Colors.RESET}")
            except Exception:
                pass
            try:
                repo_root = _REPO_ROOT
            except Exception:
                from pathlib import Path
                repo_root = Path(__file__).resolve().parents[1]

            async def _safe_send(
                *,
                content: str | None = None,
                embed: discord.Embed | None = None,
                view: ui.View | None = None,
            ) -> bool:
                """Send in-channel; if forbidden, fallback to DM so the command never appears silent."""
                try:
                    await ctx.send(content=content, embed=embed, view=view)
                    return True
                except Exception:
                    try:
                        await ctx.author.send(content=content, embed=embed, view=view)
                        return True
                    except Exception as e2:
                        print(f"{Colors.YELLOW}[commands] failed to send response: {str(e2)[:200]}{Colors.RESET}")
                        return False

            # If the user pasted multiple commands in one message, discord.py will treat the next line as an argument.
            # Example: a single message containing:
            #   !commands
            #   !testcards
            # becomes: bot_name="!testcards"
            if bot_name and str(bot_name).strip().startswith("!"):
                bot_name = None
            
            # If no bot_name provided, show summary of all bots
            if not bot_name:
                embed = discord.Embed(
                    title="📋 RS Bots Commands Reference",
                    description="Use `!commands <bot_name>` to view commands for a specific bot",
                    color=discord.Color.blue(),
                    timestamp=datetime.now(timezone.utc)
                )
                
                # Get all RS bots from BOTS registry
                rs_bot_keys = self._get_rs_bot_keys()
                
                for bot_key in rs_bot_keys:
                    if bot_key not in self.BOTS:
                        continue
                    
                    bot_info = self.BOTS[bot_key]
                    bot_folder = bot_info.get("folder", "")
                    
                    if not bot_folder:
                        continue
                    
                    commands_file = repo_root / bot_folder / "COMMANDS.md"
                    
                    # Count commands if file exists
                    command_count = "?"
                    if commands_file.exists():
                        try:
                            content = commands_file.read_text(encoding="utf-8")
                            # Count command definitions by COMMANDS.md convention:
                            # each command section begins with a "####" heading line.
                            command_count = str(len(re.findall(r"^####\s+", content, flags=re.MULTILINE)))
                        except Exception:
                            command_count = "?"
                    
                    embed.add_field(
                        name=f"🤖 {bot_info.get('name', bot_key.upper())}",
                        value=f"{command_count} commands\nUse: `!commands {bot_key}`",
                        inline=True
                    )
                
                embed.set_footer(text="Example: !commands rsadminbot")
                # Add dropdown selection (same pattern as !botupdate)
                view = BotSelectView(self, "commands", "view commands", bot_keys=rs_bot_keys)
                ok = await _safe_send(embed=embed, view=view)
                if not ok:
                    # Last-ditch: attempt plain text
                    await _safe_send(content="❌ Failed to send commands summary (no permission to post here and DM failed).")
                return
            
            # Resolve bot name using canonical BOTS registry
            bot_key = bot_name.strip().lower()
            
            if bot_key not in self.BOTS:
                # List available bots in error message
                available_bots = ", ".join(sorted(self.BOTS.keys()))
                error_embed = MessageHelper.create_error_embed(
                    title="Unknown Bot",
                    message=f"Bot '{bot_name}' not found in bot registry.",
                    error_details=f"Available bots: {available_bots}",
                    footer=f"Triggered by {ctx.author}"
                )
                await _safe_send(embed=error_embed)
                return
            
            await self._commands_send_for_bot(
                bot_key=bot_key,
                send=_safe_send,
                triggered_by=ctx.author,
                repo_root=repo_root,
            )
            return

        self.registered_commands.append(("commands", "List all commands for bots", True))
        
        @self.bot.command(name="reload")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def reload(ctx):
            """Reload configuration (admin only)"""
            self.load_config()
            self._load_ssh_config()  # Canonical: reload selection + servers.json mapping
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
                    
                    # Log to Discord before exit (embed)
                    try:
                        restart_embed = MessageHelper.create_warning_embed(
                            title="Local Restart Initiated",
                            message="RSAdminBot is restarting locally (systemd will bring it back).",
                            fields=[
                                {"name": "Service", "value": "mirror-world-rsadminbot.service", "inline": True},
                                {"name": "Mode", "value": "local", "inline": True},
                            ],
                            footer=f"Triggered by {interaction.user}",
                        )
                        await self.admin_bot._log_to_discord(
                            restart_embed,
                            interaction.channel if hasattr(interaction, "channel") else None,
                        )
                    except Exception:
                        pass
                    
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
                                try:
                                    ok_embed = MessageHelper.create_success_embed(
                                        title="Remote Restart Successful",
                                        message="RSAdminBot restarted successfully on remote server.",
                                        fields=[
                                            {"name": "Service", "value": service_name, "inline": True},
                                            {"name": "State", "value": "active", "inline": True},
                                        ],
                                        footer=f"Triggered by {interaction.user}",
                                    )
                                    await self.admin_bot._log_to_discord(
                                        ok_embed,
                                        interaction.channel if hasattr(interaction, "channel") else None,
                                    )
                                except Exception:
                                    pass
                                print(f"{Colors.GREEN}[Restart] Remote restart successful{Colors.RESET}")
                            else:
                                await interaction.followup.send(f"⚠️ **Restart initiated but status unclear**\nState: {state if exists else 'Service not found'}", ephemeral=True)
                                try:
                                    warn_embed = MessageHelper.create_warning_embed(
                                        title="Remote Restart Status Unclear",
                                        message="Restart initiated but status is unclear.",
                                        details=f"State: {state if exists else 'Service not found'}",
                                        fields=[
                                            {"name": "Service", "value": service_name, "inline": True},
                                        ],
                                        footer=f"Triggered by {interaction.user}",
                                    )
                                    await self.admin_bot._log_to_discord(
                                        warn_embed,
                                        interaction.channel if hasattr(interaction, "channel") else None,
                                    )
                                except Exception:
                                    pass
                        else:
                            error_msg = stderr or stdout or "Unknown error"
                            await interaction.followup.send(f"❌ **Restart failed**: {error_msg[:500]}", ephemeral=True)
                            try:
                                err_embed = MessageHelper.create_error_embed(
                                    title="Remote Restart Failed",
                                    message="Failed to restart RSAdminBot on remote server.",
                                    error_details=error_msg[:500],
                                    fields=[
                                        {"name": "Service", "value": service_name, "inline": True},
                                    ],
                                    footer=f"Triggered by {interaction.user}",
                                )
                                await self.admin_bot._log_to_discord(
                                    err_embed,
                                    interaction.channel if hasattr(interaction, "channel") else None,
                                )
                            except Exception:
                                pass
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
            svc = str(info.get("service") or "")
            output = out or err or ""
            embed = MessageHelper.create_status_embed(
                title="🧾 Details",
                description=self._codeblock(output, limit=1500),
                color=discord.Color.blurple(),
                fields=[
                    {"name": "Bot", "value": info.get("name", bot_key), "inline": True},
                    {"name": "Service", "value": svc or "(missing)", "inline": True},
                    {"name": "OK", "value": "YES" if success else "NO", "inline": True},
                ],
                footer=f"Triggered by {ctx.author}",
            )
            await ctx.send(embed=embed)
            await self._log_to_discord(embed, ctx.channel)
            if self.logger:
                try:
                    log_entry = self.logger.log_command(
                        ctx,
                        "details",
                        "success" if success else "error",
                        {"bot_name": bot_key, "service": svc, "ok": bool(success)},
                    )
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                    self.logger.clear_command_context()
                except Exception:
                    pass
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
            svc = str(info.get("service") or "")
            output = out or err or ""
            embed = MessageHelper.create_status_embed(
                title="📜 Logs",
                description=self._codeblock(output, limit=1500),
                color=discord.Color.blurple(),
                fields=[
                    {"name": "Bot", "value": info.get("name", bot_key), "inline": True},
                    {"name": "Lines", "value": str(n), "inline": True},
                    {"name": "OK", "value": "YES" if success else "NO", "inline": True},
                    {"name": "Service", "value": svc or "(missing)", "inline": False},
                ],
                footer=f"Triggered by {ctx.author}",
            )
            await ctx.send(embed=embed)
            await self._log_to_discord(embed, ctx.channel)
            if self.logger:
                try:
                    log_entry = self.logger.log_command(
                        ctx,
                        "logs",
                        "success" if success else "error",
                        {"bot_name": bot_key, "service": svc, "lines": n, "ok": bool(success)},
                    )
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                    self.logger.clear_command_context()
                except Exception:
                    pass
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
            
            # Log command triggered
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Starting {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_command(ctx, "start", "pending", {"bot_name": bot_name_lower, "service": service_name})
            
            # Send immediate acknowledgment
            loading_embed = MessageHelper.create_info_embed(
                title="🔄 Starting Bot",
                message=f"Starting {bot_info['name']}...",
                fields=[{"name": "Service", "value": service_name, "inline": True}]
            )
            status_msg = await ctx.send(embed=loading_embed)
            
            # Start service using ServiceManager
            if not self.service_manager:
                error_embed = MessageHelper.create_error_embed(
                    title="ServiceManager Not Available",
                    message="ServiceManager is not available. Cannot start bot."
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
                if hasattr(self, 'logger') and self.logger:
                    self.logger.log_command(ctx, "start", "error", {"bot_name": bot_name_lower, "error": "ServiceManager not available"})
                    log_entry = self.logger.log_command(ctx, "start", "error", {"bot_name": bot_name_lower, "error": "ServiceManager not available"})
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
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
                    print(f"{Colors.GREEN}[Success] {bot_info['name']} started successfully!{Colors.RESET}")
                    
                    # Create success embed
                    fields = [
                        {"name": "Bot", "value": bot_info['name'], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ]
                    if after_state:
                        state_display = after_state
                        if before_state and before_state != after_state:
                            state_display = f"{before_state} → {after_state}"
                        fields.append({"name": "Status", "value": state_display, "inline": True})
                    if after_pid:
                        fields.append({"name": "PID", "value": str(after_pid), "inline": True})
                    
                    success_embed = MessageHelper.create_success_embed(
                        title="Bot Started",
                        message=f"{bot_info['name']} started successfully!",
                        fields=fields
                    )
                    success_embed.set_footer(text=f"Triggered by {ctx.author}")
                    
                    await status_msg.edit(embed=success_embed)
                    
                    # Log and send to Discord
                    if hasattr(self, 'logger') and self.logger:
                        log_entry = self.logger.log_command(ctx, "start", "success", {
                            "bot_name": bot_name_lower,
                            "service": service_name,
                            "before_state": before_state,
                            "after_state": after_state,
                            "before_pid": before_pid,
                            "pid": after_pid,
                            "after_pid": after_pid
                        })
                        embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                        await self._log_to_discord(embed, ctx.channel)
                        self.logger.clear_command_context()
                    else:
                        await self._log_to_discord(success_embed, ctx.channel)
                else:
                    error_msg = verify_error or stderr or stdout or "Unknown error"
                    print(f"{Colors.RED}[Error] Failed to start {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                    
                    error_embed = MessageHelper.create_error_embed(
                        title="Failed to Start Bot",
                        message=f"Failed to start {bot_info['name']}",
                        error_details=error_msg[:500]
                    )
                    error_embed.add_field(name="Bot", value=bot_info['name'], inline=True)
                    error_embed.add_field(name="Service", value=service_name, inline=True)
                    error_embed.set_footer(text=f"Triggered by {ctx.author}")
                    
                    await status_msg.edit(embed=error_embed)
                    
                    # Log and send to Discord
                    if hasattr(self, 'logger') and self.logger:
                        log_entry = self.logger.log_command(ctx, "start", "error", {
                            "bot_name": bot_name_lower,
                            "service": service_name,
                            "error": error_msg[:500]
                        })
                        embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                        await self._log_to_discord(embed, ctx.channel)
                        self.logger.clear_command_context()
                    else:
                        await self._log_to_discord(error_embed, ctx.channel)
            else:
                error_msg = stderr or stdout or "Unknown error"
                print(f"{Colors.RED}[Error] Failed to start {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                
                error_embed = MessageHelper.create_error_embed(
                    title="Failed to Start Bot",
                    message=f"Failed to start {bot_info['name']}",
                    error_details=error_msg[:500]
                )
                error_embed.add_field(name="Bot", value=bot_info['name'], inline=True)
                error_embed.add_field(name="Service", value=service_name, inline=True)
                error_embed.set_footer(text=f"Triggered by {ctx.author}")
                
                await status_msg.edit(embed=error_embed)
                
                # Log and send to Discord
                if hasattr(self, 'logger') and self.logger:
                    log_entry = self.logger.log_command(ctx, "start", "error", {
                        "bot_name": bot_name_lower,
                        "service": service_name,
                        "error": error_msg[:500]
                    })
                    embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                    await self._log_to_discord(embed, ctx.channel)
                    self.logger.clear_command_context()
                else:
                    await self._log_to_discord(error_embed, ctx.channel)
        
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
            
            # Log command triggered
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Stopping {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_command(ctx, "stop", "pending", {"bot_name": bot_name_lower, "service": service_name})
            
            # Send immediate acknowledgment
            loading_embed = MessageHelper.create_info_embed(
                title="🔄 Stopping Bot",
                message=f"Stopping {bot_info['name']}...",
                fields=[{"name": "Service", "value": service_name, "inline": True}]
            )
            status_msg = await ctx.send(embed=loading_embed)
            
            # Stop service using ServiceManager
            if not self.service_manager:
                error_embed = MessageHelper.create_error_embed(
                    title="ServiceManager Not Available",
                    message="ServiceManager is not available. Cannot stop bot."
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
                if hasattr(self, 'logger') and self.logger:
                    log_entry = self.logger.log_command(ctx, "stop", "error", {"bot_name": bot_name_lower, "error": "ServiceManager not available"})
                    embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                    await self._log_to_discord(embed, ctx.channel)
                    self.logger.clear_command_context()
                return

            before_exists, before_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
            before_pid = self.service_manager.get_pid(service_name)
            
            success, stdout, stderr = self.service_manager.stop(service_name, script_pattern=script_pattern, bot_name=bot_name_lower)
            
            if success:
                after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name_lower)
                after_pid = self.service_manager.get_pid(service_name)
                print(f"{Colors.GREEN}[Success] {bot_info['name']} stopped successfully!{Colors.RESET}")
                
                # Create success embed
                fields = [
                    {"name": "Bot", "value": bot_info['name'], "inline": True},
                    {"name": "Service", "value": service_name, "inline": True},
                ]
                if after_state:
                    state_display = after_state
                    if before_state and before_state != after_state:
                        state_display = f"{before_state} → {after_state}"
                    fields.append({"name": "Status", "value": state_display, "inline": True})
                if before_pid and not after_pid:
                    fields.append({"name": "PID", "value": f"{before_pid} → 0", "inline": True})
                
                success_embed = MessageHelper.create_success_embed(
                    title="Bot Stopped",
                    message=f"{bot_info['name']} stopped successfully!",
                    fields=fields
                )
                success_embed.set_footer(text=f"Triggered by {ctx.author}")
                
                await status_msg.edit(embed=success_embed)
                
                # Log and send to Discord
                if hasattr(self, 'logger') and self.logger:
                    log_entry = self.logger.log_command(ctx, "stop", "success", {
                        "bot_name": bot_name_lower,
                        "service": service_name,
                        "before_state": before_state,
                        "after_state": after_state,
                        "before_pid": before_pid,
                        "after_pid": after_pid
                    })
                    embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                    await self._log_to_discord(embed, ctx.channel)
                    self.logger.clear_command_context()
                else:
                    await self._log_to_discord(success_embed, ctx.channel)
            else:
                error_msg = stderr or stdout or "Unknown error"
                print(f"{Colors.RED}[Error] Failed to stop {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                
                error_embed = MessageHelper.create_error_embed(
                    title="Failed to Stop Bot",
                    message=f"Failed to stop {bot_info['name']}",
                    error_details=error_msg[:500]
                )
                error_embed.add_field(name="Bot", value=bot_info['name'], inline=True)
                error_embed.add_field(name="Service", value=service_name, inline=True)
                error_embed.set_footer(text=f"Triggered by {ctx.author}")
                
                await status_msg.edit(embed=error_embed)
                
                # Log and send to Discord
                if hasattr(self, 'logger') and self.logger:
                    log_entry = self.logger.log_command(ctx, "stop", "error", {
                        "bot_name": bot_name_lower,
                        "service": service_name,
                        "error": error_msg[:500]
                    })
                    embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                    await self._log_to_discord(embed, ctx.channel)
                    self.logger.clear_command_context()
                else:
                    await self._log_to_discord(error_embed, ctx.channel)
        
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
            
            # Log command triggered
            guild_name = ctx.guild.name if ctx.guild else "DM"
            guild_id = ctx.guild.id if ctx.guild else 0
            print(f"{Colors.CYAN}[Command] Restarting {bot_info['name']} (Service: {service_name}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Server: {guild_name} (ID: {guild_id}){Colors.RESET}")
            print(f"{Colors.CYAN}[Command] Requested by: {ctx.author} ({ctx.author.id}){Colors.RESET}")
            
            if hasattr(self, 'logger') and self.logger:
                self.logger.log_command(ctx, "restart", "pending", {"bot_name": bot_name_lower, "service": service_name})
            
            # Send immediate acknowledgment
            loading_embed = MessageHelper.create_info_embed(
                title="🔄 Restarting Bot",
                message=f"Restarting {bot_info['name']}...",
                fields=[{"name": "Service", "value": service_name, "inline": True}]
            )
            status_msg = await ctx.send(embed=loading_embed)
            
            # Restart service using ServiceManager
            if not self.service_manager:
                error_embed = MessageHelper.create_error_embed(
                    title="ServiceManager Not Available",
                    message="ServiceManager is not available. Cannot restart bot."
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
                if hasattr(self, 'logger') and self.logger:
                    log_entry = self.logger.log_command(ctx, "restart", "error", {"bot_name": bot_name_lower, "error": "ServiceManager not available"})
                    embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                    await self._log_to_discord(embed, ctx.channel)
                    self.logger.clear_command_context()
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
                    print(f"{Colors.GREEN}[Success] {bot_info['name']} restarted successfully!{Colors.RESET}")
                    
                    # Create success embed
                    fields = [
                        {"name": "Bot", "value": bot_info['name'], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ]
                    if after_state:
                        state_display = after_state
                        if before_state and before_state != after_state:
                            state_display = f"{before_state} → {after_state}"
                        fields.append({"name": "Status", "value": state_display, "inline": True})
                    if after_pid:
                        pid_display = str(after_pid)
                        if before_pid and before_pid != after_pid:
                            pid_display = f"{before_pid} → {after_pid}"
                        fields.append({"name": "PID", "value": pid_display, "inline": True})
                    
                    success_embed = MessageHelper.create_success_embed(
                        title="Bot Restarted",
                        message=f"{bot_info['name']} restarted successfully!",
                        fields=fields
                    )
                    success_embed.set_footer(text=f"Triggered by {ctx.author}")
                    
                    await status_msg.edit(embed=success_embed)
                    
                    # Log and send to Discord
                    if hasattr(self, 'logger') and self.logger:
                        log_entry = self.logger.log_command(ctx, "restart", "success", {
                            "bot_name": bot_name_lower,
                            "service": service_name,
                            "before_state": before_state,
                            "after_state": after_state,
                            "before_pid": before_pid,
                            "pid": after_pid,
                            "after_pid": after_pid
                        })
                        embed = self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx))
                        await self._log_to_discord(embed, ctx.channel)
                        self.logger.clear_command_context()
                    else:
                        await self._log_to_discord(success_embed, ctx.channel)
                else:
                    error_msg = verify_error or stderr or stdout or "Unknown error"
                    print(f"{Colors.YELLOW}[Warning] Restart completed but verification failed for {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                    warning_embed = MessageHelper.create_warning_embed(
                        title="Restart Verification Failed",
                        message=f"Restart completed but verification failed for {bot_info['name']}.",
                        details=error_msg[:500],
                        fields=[
                            {"name": "Bot", "value": bot_info["name"], "inline": True},
                            {"name": "Service", "value": service_name, "inline": True},
                        ],
                        footer=f"Triggered by {ctx.author}",
                    )
                    await status_msg.edit(embed=warning_embed)
                    await self._log_to_discord(warning_embed, ctx.channel)
            else:
                error_msg = stderr or stdout or "Unknown error"
                print(f"{Colors.RED}[Error] Failed to restart {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                error_embed = MessageHelper.create_error_embed(
                    title="Failed to Restart Bot",
                    message=f"Failed to restart {bot_info['name']}.",
                    error_details=error_msg[:500],
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Service", "value": service_name, "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
        
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
                # RS-only dropdown (prevents accidental updates of non-RS bots).
                view = BotSelectView(self, "update", "Update", bot_keys=self._get_rs_bot_keys())
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
                embed=MessageHelper.create_info_embed(
                    title="Updating Bot (python-only)",
                    message=f"Updating {bot_info['name']} from GitHub and restarting service.",
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Folder", "value": bot_folder, "inline": True},
                        {"name": "Service", "value": service_name or "(missing)", "inline": False},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
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
                        f"Before: {self._format_service_state(before_exists, before_state, before_pid)}"
                    ),
                )
            
            success, stats = self._github_py_only_update(bot_folder)
            if not success:
                error_msg = stats.get("error", "Unknown error")
                print(f"{Colors.RED}[Error] GitHub py-only update failed for {bot_info['name']}: {error_msg[:500]}{Colors.RESET}")
                error_embed = MessageHelper.create_error_embed(
                    title="Update Failed",
                    message=f"GitHub py-only update failed for {bot_info['name']}.",
                    error_details=str(error_msg)[:800],
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Folder", "value": bot_folder, "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
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
            changed_count = int(stats.get("changed_count") or "0")
            git_changed_count = int(stats.get("git_changed_count") or "0")
            changed_sample = stats.get("changed_sample") or []

            # Determine status message
            if old == new:
                git_status = f"{old[:12]} (no new commits)"
                status_note = "Already up to date" if changed_count == 0 else f"{changed_count} file(s) synced"
            else:
                git_status = f"{old[:12]} -> {new[:12]}"
                status_note = f"{git_changed_count} file(s) changed in git"

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

            fields = [
                {"name": "Bot", "value": bot_info["name"], "inline": True},
                {"name": "Git", "value": git_status, "inline": False},
                {"name": "Python copied", "value": py_count, "inline": True},
                {"name": "Files synced", "value": str(changed_count), "inline": True},
                {"name": "Status", "value": status_note, "inline": True},
                {"name": "Restart", "value": "OK" if restart_ok else "FAILED", "inline": True},
            ]
            success_embed = MessageHelper.create_success_embed(
                title="Update Complete",
                message=f"{bot_info['name']} updated from GitHub (python-only).",
                fields=fields,
                footer=f"Triggered by {ctx.author}",
            )
            if changed_sample:
                sample_txt = "\n".join(str(x) for x in changed_sample[:30])
                success_embed.add_field(
                    name="Changed sample (first 30)",
                    value=f"```{sample_txt[:900]}```",
                    inline=False,
                )
            if not restart_ok and restart_err:
                success_embed.add_field(
                    name="Restart error",
                    value=f"```{restart_err[:900]}```",
                    inline=False,
                )

            await status_msg.edit(embed=success_embed)

            # Structured log entry (file + embed to log channel)
            if self.logger:
                try:
                    log_entry = self.logger.log_command(
                        ctx,
                        "botupdate",
                        "success" if restart_ok else "error",
                        {
                            "bot_name": bot_name,
                            "service": service_name,
                            "git_old": old[:12],
                            "git_new": new[:12],
                            "python_copied": py_count,
                            "files_synced": changed_count,
                            "git_changed_count": git_changed_count,
                            "status": status_note,
                            "restart_ok": restart_ok,
                            "restart_error": restart_err[:500] if restart_err else "",
                        },
                    )
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                    self.logger.clear_command_context()
                except Exception:
                    pass
            if should_post_progress and self.service_manager and service_name:
                after_exists, after_state, _ = self.service_manager.get_status(service_name, bot_name=bot_name)
                after_pid = self.service_manager.get_pid(service_name)
                await self._post_or_edit_progress(
                    progress_msg,
                    (
                        f"[botupdate] {bot_info['name']} ({bot_name}) COMPLETE\n"
                        f"Git: {git_status}\n"
                        f"After:  {self._format_service_state(after_exists, after_state, after_pid)}\n"
                        f"Python copied: {py_count} | Files synced: {changed_count} | Restart: {'OK' if restart_ok else 'FAILED'}"
                    ),
                )

        @self.bot.command(name="mwupdate", aliases=["mwbots"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def mwupdate(ctx, bot_name: str = None):
            """Update a Mirror-World bot by pulling python-only code from GitHub and restarting it (admin only)."""
            if bot_name:
                bot_name = bot_name.lower()
                if self._get_bot_group(bot_name) != "mirror_bots":
                    await ctx.send(f"❌ `{bot_name}` is not a Mirror-World bot.\nUse `!botlist` to see available bots.")
                    return

            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return

            if not bot_name:
                view = BotSelectView(self, "update", "Update", bot_keys=self._get_mw_bot_keys())
                embed = discord.Embed(
                    title="📦 Select MW Bot to Update",
                    description="Choose a Mirror-World bot from the dropdown menu below:",
                    color=discord.Color.blue(),
                )
                await ctx.send(embed=embed, view=view)
                return

            if bot_name not in self.BOTS:
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return

            bot_info = self.BOTS[bot_name]
            await self._log_to_discord(f"📦 **Updating {bot_info.get('name', bot_name)} (MWBots GitHub python-only)**")
            ok, result = self._mwupdate_one_py_only(bot_name)
            if not ok:
                await ctx.send(f"❌ Update failed:\n```{str(result.get('error') or 'unknown error')[:900]}```")
                return
            await ctx.send(str(result.get("summary") or "")[:1900])

        @self.bot.command(name="botsync", aliases=["syncbot"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botsync(ctx, bot_name: str = None, *, flags: str = ""):
            """Sync local bot files directly to Oracle server via rsync (admin only).
            
            Usage:
                !botsync <bot_name>          - Sync bot folder to server
                !botsync <bot_name> --dry-run - Preview changes without syncing
                !botsync <bot_name> --delete  - Delete remote files not present locally
            """
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return
            
            if not bot_name:
                # Show interactive SelectMenu
                view = BotSelectView(self, "sync", "Sync")
                embed = discord.Embed(
                    title="📤 Select Bot to Sync",
                    description="Choose a bot from the dropdown menu below:",
                    color=discord.Color.blue()
                )
                await ctx.send(embed=embed, view=view)
                return
            
            bot_name = bot_name.lower()
            if bot_name not in self.BOTS:
                available_bots = ", ".join(self.BOTS.keys())
                await ctx.send(f"❌ Unknown bot: {bot_name}\nUse `!botlist` to see available bots")
                return
            
            bot_info = self.BOTS[bot_name]
            bot_folder = bot_info["folder"]
            service_name = bot_info.get("service", "")
            
            # Parse flags
            flags_lower = flags.lower()
            dry_run = "--dry-run" in flags_lower or "-n" in flags_lower
            delete = "--delete" in flags_lower or "-d" in flags_lower
            
            # Get local bot folder path
            local_bot_path = self.base_path.parent / bot_folder
            if not local_bot_path.exists():
                await ctx.send(f"❌ Local bot folder not found: {local_bot_path}")
                return
            
            # Get remote path from config
            remote_root = getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world"
            remote_bot_path = f"{remote_root}/{bot_folder}"
            
            status_msg = await ctx.send(
                embed=MessageHelper.create_info_embed(
                    title="Syncing Bot Files",
                    message=f"Syncing {bot_info['name']} from local to server.",
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Local", "value": str(local_bot_path), "inline": False},
                        {"name": "Remote", "value": remote_bot_path, "inline": False},
                        {"name": "Mode", "value": "DRY RUN" if dry_run else "SYNC", "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
            )
            
            # Check if rsync script exists
            rsync_script = self.base_path.parent / "Rsync" / "rsync_sync.py"
            if not rsync_script.exists():
                # Fallback: use rsync directly via SSH
                await self._sync_bot_via_ssh(ctx, status_msg, bot_info, bot_folder, local_bot_path, remote_bot_path, dry_run, delete)
            else:
                # Use the dedicated rsync script
                await self._sync_bot_via_script(ctx, status_msg, bot_info, bot_folder, local_bot_path, remote_bot_path, rsync_script, dry_run, delete)

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
                payload = "\n".join(lines)
                embed = MessageHelper.create_info_embed(
                    title="Where Am I",
                    message=self._codeblock(payload, limit=1800),
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                await self._log_to_discord(embed, ctx.channel)
                if self.logger:
                    try:
                        log_entry = self.logger.log_command(
                            ctx,
                            "whereami",
                            "success",
                            {
                                "cwd": cwd,
                                "file": file_path,
                                "local_exec": local_exec,
                                "live_root": live_repo,
                                "rsbots_code_head": head_code,
                                "live_tree_head": head_live,
                            },
                        )
                        await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                        self.logger.clear_command_context()
                    except Exception:
                        pass
            except Exception as e:
                err_txt = str(e)[:300]
                embed = MessageHelper.create_error_embed(
                    title="whereami Failed",
                    message="whereami failed.",
                    error_details=err_txt,
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)

        @self.bot.command(name="appcmds", aliases=["slashcmds"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def appcmds(ctx):
            """List known application (slash) commands in the local command tree (admin only)."""
            try:
                # Show what the bot thinks exists locally (not a remote fetch).
                global_cmds = self.bot.tree.get_commands() or []
                global_names = sorted({str(getattr(c, "name", "") or "") for c in global_cmds if getattr(c, "name", None)})

                gid = 0
                try:
                    if getattr(ctx, "guild", None) and getattr(ctx.guild, "id", None):
                        gid = int(ctx.guild.id)
                except Exception:
                    gid = 0
                if not gid:
                    try:
                        gid = int(self.config.get("rs_server_guild_id") or 0)
                    except Exception:
                        gid = 0

                guild_names: List[str] = []
                if gid:
                    try:
                        guild_cmds = self.bot.tree.get_commands(guild=discord.Object(id=gid)) or []
                        guild_names = sorted({str(getattr(c, "name", "") or "") for c in guild_cmds if getattr(c, "name", None)})
                    except Exception:
                        guild_names = []

                lines = []
                lines.append("APP COMMANDS (local tree)")
                lines.append(f"global.count={len(global_names)} has_rsnote={'rsnote' in set(global_names)}")
                if global_names:
                    lines.append("global.names=" + ", ".join(global_names))
                if gid:
                    lines.append(f"guild_id={gid} guild.count={len(guild_names)} has_rsnote={'rsnote' in set(guild_names)}")
                    if guild_names:
                        lines.append("guild.names=" + ", ".join(guild_names))

                status_line = str(getattr(self, "_rsnotes_status_line", "") or "").strip()
                if status_line:
                    lines.append(status_line)

                embed = MessageHelper.create_info_embed(
                    title="Slash Commands (Local Tree)",
                    message=self._codeblock("\n".join(lines), limit=1800),
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
            except Exception as e:
                err_txt = str(e)[:300]
                embed = MessageHelper.create_error_embed(
                    title="appcmds Failed",
                    message="Failed to list app commands.",
                    error_details=err_txt,
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                await self._log_to_discord(embed, ctx.channel)

        self.registered_commands.append(("whereami", "Print runtime environment details", True))

        # Register the rest of the command suite (admin tooling, deploy, diagnostics, etc.).
        #
        # NOTE: These commands previously lived in the codebase but were accidentally nested under
        # `_sync_bot_via_ssh` due to indentation, which meant they only registered after running
        # botsync fallback. Keep them registered at startup.
        self._setup_extended_commands()
    
    async def _sync_bot_via_script(self, ctx, status_msg, bot_info, bot_folder, local_bot_path, remote_bot_path, rsync_script, dry_run, delete):
        """Sync bot using the dedicated rsync_sync.py script."""
        try:
            import subprocess
            import sys
            
            # Build command
            cmd = [
                sys.executable,
                "-u",  # Unbuffered output
                str(rsync_script),
                "--project-dir", str(local_bot_path),
                "--remote-dir", remote_bot_path,
                "--user", self.current_server.get("user", "rsadmin"),
                "--host", self.current_server.get("host", ""),
            ]
            
            if self.current_server.get("key"):
                key_path = Path(self.current_server["key"])
                if key_path.exists():
                    cmd.extend(["--key", str(key_path.resolve())])
            
            if self.current_server.get("ssh_options"):
                cmd.extend(["--ssh-options", self.current_server["ssh_options"]])
            
            if dry_run:
                cmd.append("--dry-run")
            
            if delete:
                cmd.append("--delete")
            
            # Exclude patterns (from CANONICAL_RULES.md - never sync secrets)
            exclude_patterns = [
                "config.secrets.json",
                "__pycache__",
                "*.pyc",
                "*.pyo",
                "*.log",
                "*.jsonl",
                ".git",
            ]
            for pattern in exclude_patterns:
                cmd.extend(["--exclude", pattern])
            
            # Run rsync script
            process = subprocess.Popen(
                cmd,
                cwd=str(self.base_path.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            output_lines = []
            for line in iter(process.stdout.readline, ''):
                if line:
                    line = line.rstrip()
                    output_lines.append(line)
            
            process.wait()
            success = process.returncode == 0
            
            output_text = "\n".join(output_lines[-50:])  # Last 50 lines
            
            if success:
                success_embed = MessageHelper.create_success_embed(
                    title="Bot Sync Complete",
                    message=f"Successfully synced {bot_info['name']} to server.",
                    details=output_text[-1000:] if output_text else None,
                    fields=[
                        {"name": "Bot", "value": bot_info["name"], "inline": True},
                        {"name": "Mode", "value": "DRY RUN" if dry_run else "SYNC", "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
                await status_msg.edit(embed=success_embed)
                await self._log_to_discord(success_embed, ctx.channel)
            else:
                error_embed = MessageHelper.create_error_embed(
                    title="Bot Sync Failed",
                    message=f"Failed to sync {bot_info['name']} to server.",
                    error_details=output_text[-1000:] if output_text else "Unknown error",
                    footer=f"Triggered by {ctx.author}",
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
                
        except Exception as e:
            error_embed = MessageHelper.create_error_embed(
                title="Bot Sync Error",
                message=f"Error during sync: {str(e)[:200]}",
                footer=f"Triggered by {ctx.author}",
            )
            await status_msg.edit(embed=error_embed)
            await self._log_to_discord(error_embed, ctx.channel if ctx else None)
    
    async def _sync_bot_via_ssh(self, ctx, status_msg, bot_info, bot_folder, local_bot_path, remote_bot_path, dry_run, delete):
        """Fallback: sync bot using rsync via SSH command."""
        try:
            import shutil
            import platform
            
            # Check if rsync is available
            rsync_exe = shutil.which("rsync")
            if not rsync_exe:
                error_embed = MessageHelper.create_error_embed(
                    title="rsync Not Found",
                    message="rsync is required for file syncing. Install it first.",
                    error_details="Windows: choco install rsync -y\nLinux: sudo apt install rsync\nMac: brew install rsync",
                    footer=f"Triggered by {ctx.author if ctx else 'Unknown'}",
                )
                await status_msg.edit(embed=error_embed)
                return
            
            # Build rsync command
            # Note: This is a simplified version - the rsync_sync.py script handles Windows path conversion
            user = self.current_server.get("user", "rsadmin")
            host = self.current_server.get("host", "")
            key = self.current_server.get("key", "")
            
            if not host:
                error_embed = MessageHelper.create_error_embed(
                    title="SSH Config Error",
                    message="SSH host not configured.",
                    footer=f"Triggered by {ctx.author if ctx else 'Unknown'}",
                )
                await status_msg.edit(embed=error_embed)
                return
            
            # Build SSH command for rsync -e option
            ssh_cmd_parts = ["ssh"]
            if key:
                key_path = Path(key)
                if key_path.exists():
                    ssh_cmd_parts.extend(["-i", str(key_path.resolve())])
            
            ssh_options = self.current_server.get("ssh_options", "")
            if ssh_options:
                ssh_cmd_parts.extend(shlex.split(ssh_options))
            
            ssh_cmd_str = " ".join(shlex.quote(str(p)) for p in ssh_cmd_parts)
            
            # Build rsync command
            rsync_cmd = [rsync_exe, "-avz", "--progress"]
            rsync_cmd.extend(["-e", ssh_cmd_str])
            
            if dry_run:
                rsync_cmd.append("--dry-run")
            
            if delete:
                rsync_cmd.append("--delete")
            
            # Exclude patterns
            rsync_cmd.extend(["--exclude", "config.secrets.json"])
            rsync_cmd.extend(["--exclude", "__pycache__"])
            rsync_cmd.extend(["--exclude", "*.pyc"])
            rsync_cmd.extend(["--exclude", "*.log"])
            
            # Source and destination
            local_str = str(local_bot_path).replace("\\", "/")
            if not local_str.endswith("/"):
                local_str += "/"
            
            remote_str = f"{user}@{host}:{remote_bot_path}/"
            
            rsync_cmd.append(local_str)
            rsync_cmd.append(remote_str)
            
            # Execute rsync
            process = subprocess.Popen(
                rsync_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            output_lines = []
            for line in iter(process.stdout.readline, ''):
                if line:
                    line = line.rstrip()
                    output_lines.append(line)
            
            process.wait()
            success = process.returncode == 0
            
            output_text = "\n".join(output_lines[-50:])
            
            if success:
                success_embed = MessageHelper.create_success_embed(
                    title="Bot Sync Complete",
                    message=f"Successfully synced {bot_info['name']} to server.",
                    details=output_text[-1000:] if output_text else None,
                    footer=f"Triggered by {ctx.author if ctx else 'Unknown'}",
                )
                await status_msg.edit(embed=success_embed)
                await self._log_to_discord(success_embed, ctx.channel if ctx else None)
            else:
                error_embed = MessageHelper.create_error_embed(
                    title="Bot Sync Failed",
                    message=f"Failed to sync {bot_info['name']} to server.",
                    error_details=output_text[-1000:] if output_text else "Unknown error",
                    footer=f"Triggered by {ctx.author if ctx else 'Unknown'}",
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel if ctx else None)
                
        except Exception as e:
            error_embed = MessageHelper.create_error_embed(
                title="Bot Sync Error",
                message=f"Error during sync: {str(e)[:200]}",
                footer=f"Triggered by {ctx.author if ctx else 'Unknown'}",
            )
            await status_msg.edit(embed=error_embed)
            await self._log_to_discord(error_embed, ctx.channel if ctx else None)

    def _setup_extended_commands(self) -> None:
        """Register extended/admin commands.

        Keep these separate from `_setup_commands` to keep the core startup path readable.
        """

        @self.bot.command(name="selfupdate")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def selfupdate(ctx):
            """Update RSAdminBot from GitHub (python-only) then restart rsadminbot (admin only)."""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return

            status_msg = await ctx.send(
                embed=MessageHelper.create_info_embed(
                    title="Updating RSAdminBot (python-only)",
                    message="Pulling + copying RSAdminBot/*.py from /home/rsadmin/bots/rsbots-code",
                    fields=[
                        {"name": "Service", "value": "mirror-world-rsadminbot.service", "inline": True},
                        {"name": "Next", "value": "Restart if changes detected", "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
            )
            should_post_progress = not (await self._is_progress_channel(ctx.channel))
            progress_msg = None
            if should_post_progress:
                progress_msg = await self._post_or_edit_progress(
                    None,
                    f"[selfupdate] START",
                )
            # First, sync RSAdminBot/*.py from rsbots-code to live tree
            success, stats = self._github_py_only_update("RSAdminBot")
            if not success:
                err_txt = str(stats.get("error", "Unknown error"))[:800]
                error_embed = MessageHelper.create_error_embed(
                    title="Selfupdate Failed",
                    message="Failed to update RSAdminBot from GitHub (python-only).",
                    error_details=err_txt,
                    footer=f"Triggered by {ctx.author}",
                )
                await status_msg.edit(embed=error_embed)
                await self._log_to_discord(error_embed, ctx.channel)
                if self.logger:
                    try:
                        log_entry = self.logger.log_command(ctx, "selfupdate", "error", {"error": err_txt})
                        await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                        self.logger.clear_command_context()
                    except Exception:
                        pass
                if should_post_progress:
                    await self._post_or_edit_progress(
                        progress_msg,
                        f"[selfupdate] FAILED\nError: {stats.get('error','Unknown error')[:500]}",
                    )
                return

            # Also sync shared scripts/*.py so admin tooling commands (!oracledatasync, etc.) are available.
            try:
                scripts_ok, scripts_stats = self._github_py_only_update("scripts")
                if not scripts_ok:
                    warn_txt = str((scripts_stats or {}).get("error", "unknown error"))
                    print(f"{Colors.YELLOW}[selfupdate] Warning: failed to sync scripts/: {warn_txt}{Colors.RESET}")
            except Exception as e:
                # Non-fatal; RSAdminBot code is already updated.
                print(f"{Colors.YELLOW}[selfupdate] Warning: exception while syncing scripts/: {e}{Colors.RESET}")

            old = (stats.get("old") or "").strip()
            new = (stats.get("new") or "").strip()
            py_count = str(stats.get("py_count") or "0").strip()
            changed_count = str(stats.get("changed_count") or "0").strip()
            changed_sample = stats.get("changed_sample") or []
            
            # Check if actually updated (old != new)
            if old and new and old == new:
                # No changes - skip restart
                ok_embed = MessageHelper.create_success_embed(
                    title="Up to Date",
                    message="No changes detected. No restart needed.",
                    fields=[{"name": "Git", "value": old[:12], "inline": True}],
                    footer=f"Triggered by {ctx.author}",
                )
                await status_msg.edit(embed=ok_embed)
                await self._log_to_discord(ok_embed, ctx.channel)
                if self.logger:
                    try:
                        log_entry = self.logger.log_command(ctx, "selfupdate", "success", {"git": old[:12], "no_changes": True})
                        await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                        self.logger.clear_command_context()
                    except Exception:
                        pass
                if should_post_progress:
                    await self._post_or_edit_progress(
                        progress_msg,
                        f"[selfupdate] UP_TO_DATE\nGit: {old[:7]}\nNo changes, no restart needed.",
                    )
                return
            
            # Has changes - proceed with update message and restart
            fields = [
                {"name": "Git", "value": f"{old[:12]} -> {new[:12]}", "inline": False},
                {"name": "Python copied", "value": py_count, "inline": True},
                {"name": "Changed", "value": changed_count, "inline": True},
                {"name": "Next", "value": "Restarting service to apply", "inline": False},
            ]
            ok_embed = MessageHelper.create_success_embed(
                title="Selfupdate Applied",
                message="RSAdminBot updated from GitHub (python-only).",
                fields=fields,
                footer=f"Triggered by {ctx.author}",
            )
            if changed_sample:
                changed_block = "\n".join(str(x) for x in changed_sample[:15])
                ok_embed.add_field(name="Changed sample (first 15)", value=f"```{changed_block[:900]}```", inline=False)
            await status_msg.edit(embed=ok_embed)
            await self._log_to_discord(ok_embed, ctx.channel)
            if self.logger:
                try:
                    log_entry = self.logger.log_command(
                        ctx,
                        "selfupdate",
                        "success",
                        {"git_old": old[:12], "git_new": new[:12], "python_copied": py_count, "changed_count": changed_count},
                    )
                    await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                    self.logger.clear_command_context()
                except Exception:
                    pass
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
            """Push a bots-only snapshot of the live Ubuntu RS bot folders to neo-rs/oraclefiles (admin only)."""
            status_msg = await ctx.send(
                embed=MessageHelper.create_info_embed(
                    title="OracleFiles Sync",
                    message="Running snapshot export + git push (bots-only; excludes secrets).",
                    footer=f"Triggered by {ctx.author}",
                )
            )
            try:
                should_post_progress = not (await self._is_progress_channel(ctx.channel))
                progress_msg = None
                if should_post_progress:
                    progress_msg = await self._post_or_edit_progress(
                        None,
                        f"[oraclefiles] MANUAL START",
                    )

                try:
                    ok, stats = self._oraclefiles_sync_once(trigger="manual")
                except Exception as e:
                    ok, stats = False, {"error": f"oraclefiles sync crashed: {str(e)[:300]}"}

                if not ok:
                    err = str(stats.get("error") or "unknown error")
                    error_embed = MessageHelper.create_error_embed(
                        title="OracleFiles Sync Failed",
                        message="OracleFiles sync failed.",
                        error_details=err[:1200],
                        footer=f"Triggered by {ctx.author}",
                    )
                    await status_msg.edit(embed=error_embed)
                    # Log-channel only (avoid duplicating the in-channel status message)
                    await self._log_to_discord(error_embed, None)
                    if self.logger:
                        try:
                            log_entry = self.logger.log_command(ctx, "oraclefilesupdate", "error", {"error": err[:1200]})
                            await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), None)
                            self.logger.clear_command_context()
                        except Exception:
                            pass
                    if should_post_progress:
                        await self._post_or_edit_progress(progress_msg, f"[oraclefiles] MANUAL FAILED\n{err[:1600]}")
                    return

                head = str(stats.get("head") or "")[:12]
                pushed = "YES" if str(stats.get("pushed") or "").strip() else "NO"
                no_changes = "YES" if str(stats.get("no_changes") or "").strip() else "NO"
                sample = stats.get("changed_sample") or []

                fields = [
                    {"name": "Pushed", "value": pushed, "inline": True},
                    {"name": "No changes", "value": no_changes, "inline": True},
                    {"name": "Head", "value": head, "inline": False},
                ]
                ok_embed = MessageHelper.create_success_embed(
                    title="OracleFiles Sync Complete",
                    message="OracleFiles snapshot pushed successfully.",
                    fields=fields,
                    footer=f"Triggered by {ctx.author}",
                )
                if sample:
                    sample_txt = "\n".join(str(x) for x in sample[:40])
                    ok_embed.add_field(name="Changed files (sample)", value=f"```{sample_txt[:900]}```", inline=False)
                await status_msg.edit(embed=ok_embed)
                # Log-channel only (avoid duplicating the in-channel status message)
                await self._log_to_discord(ok_embed, None)
                if self.logger:
                    try:
                        log_entry = self.logger.log_command(
                            ctx,
                            "oraclefilesupdate",
                            "success",
                            {"pushed": pushed, "no_changes": no_changes, "head": head},
                        )
                        await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), None)
                        self.logger.clear_command_context()
                    except Exception:
                        pass
                if should_post_progress:
                    await self._post_or_edit_progress(
                        progress_msg,
                        f"[oraclefiles] MANUAL OK\nPushed: {pushed}\nHead: {head}",
                    )
            except Exception as e:
                err = str(e)[:400]
                error_embed = MessageHelper.create_error_embed(
                    title="OracleFiles Sync Failed",
                    message="oraclefilesupdate crashed.",
                    error_details=err,
                    footer=f"Triggered by {ctx.author}",
                )
                try:
                    await status_msg.edit(embed=error_embed)
                except Exception:
                    pass
                try:
                    await self._log_to_discord(error_embed, None)
                except Exception:
                    pass
                return

        @self.bot.command(name="syncstatus", aliases=["outdated", "codestatus"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def syncstatus(ctx):
            """Compare rsbots-code (GitHub checkout) vs live tree and report which RS bots are outdated (admin only)."""
            ssh_ok, error_msg = self._check_ssh_available()
            if not ssh_ok:
                await ctx.send(f"❌ SSH not configured: {error_msg}")
                return

            code_root = Path("/home/rsadmin/bots/rsbots-code")
            live_root = Path(str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world"))

            if not (code_root / ".git").exists():
                await ctx.send(f"❌ Missing rsbots-code git repo: `{code_root}`")
                return
            if not live_root.is_dir():
                await ctx.send(f"❌ Missing live repo root: `{live_root}`")
                return

            # Update git refs (read-only aside from fetch)
            head_local = ""
            head_remote = ""
            ahead_behind = ""
            try:
                subprocess.run(["git", "-C", str(code_root), "fetch", "origin"], capture_output=True, text=True, timeout=25)
                head_local = (subprocess.run(["git", "-C", str(code_root), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=8).stdout or "").strip()
                head_remote = (subprocess.run(["git", "-C", str(code_root), "rev-parse", "origin/main"], capture_output=True, text=True, timeout=8).stdout or "").strip()
                ahead_behind = (subprocess.run(["git", "-C", str(code_root), "rev-list", "--left-right", "--count", "HEAD...origin/main"], capture_output=True, text=True, timeout=8).stdout or "").strip()
            except Exception:
                pass

            # Build RS bot folders dynamically (no hardcoded list).
            rs_keys = [k for k in self._get_rs_bot_keys() if k in self.BOTS]
            folders = sorted({str((self.BOTS.get(k) or {}).get("folder") or "").strip() for k in rs_keys if (self.BOTS.get(k) or {}).get("folder")})
            # Include shared scripts folder used by admin tooling.
            if (code_root / "scripts").is_dir():
                if "scripts" not in folders:
                    folders.append("scripts")

            embed = MessageHelper.create_info_embed(
                title="Sync Status (rsbots-code vs live)",
                message="Shows which folders differ between `/home/rsadmin/bots/rsbots-code` and the live tree.\n\nFix: run `!botupdate` (select a bot) or use the dropdown **All RS Bots** option.",
                footer=f"Triggered by {ctx.author}",
            )
            if head_local:
                embed.add_field(name="rsbots-code HEAD", value=f"`{head_local[:12]}`", inline=True)
            if head_remote:
                embed.add_field(name="origin/main HEAD", value=f"`{head_remote[:12]}`", inline=True)
            if ahead_behind:
                embed.add_field(name="ahead/behind", value=f"`{ahead_behind}`", inline=True)

            any_diff = False
            for folder in folders:
                try:
                    res = subprocess.run(
                        ["git", "-C", str(code_root), "ls-files", folder],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    files = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip().endswith(".py")]
                except Exception:
                    files = []
                if not files:
                    continue

                diff = 0
                missing = 0
                sample: List[str] = []
                for rel in files:
                    gp = code_root / rel
                    lp = live_root / rel
                    if not lp.exists():
                        diff += 1
                        missing += 1
                        if len(sample) < 5:
                            sample.append(f"{rel} (missing)")
                        continue
                    try:
                        if gp.read_bytes() != lp.read_bytes():
                            diff += 1
                            if len(sample) < 5:
                                sample.append(rel)
                    except Exception:
                        diff += 1
                        if len(sample) < 5:
                            sample.append(f"{rel} (read_error)")

                any_diff = any_diff or (diff > 0)
                status = "✅ in sync" if diff == 0 else f"⚠️ diff={diff} (missing={missing})"
                details = status
                if sample:
                    details += "\n```" + "\n".join(sample)[:900] + "```"
                embed.add_field(name=folder, value=details[:1024], inline=False)

            if not folders:
                embed.add_field(name="No folders", value="No RS folders found to compare.", inline=False)
            elif not any_diff:
                embed.add_field(name="Result", value="✅ Live tree matches rsbots-code for tracked .py files.", inline=False)

            await ctx.send(embed=embed)

        @self.bot.command(name="systemcheck", aliases=["systemstatus"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def systemcheck(ctx):
            """Report runtime mode + core Ubuntu health stats (admin only)."""
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
                key_exists_display = "N/A (local_exec)" if local_exec else str(bool(key_exists))
                mode_txt = "Ubuntu local-exec (no SSH key needed)" if local_exec else "SSH mode (key required if not local)"

                # --- System health stats (Ubuntu local-exec preferred; fall back to SSH if needed)
                def _cmd(cmd: str, timeout_s: int = 8) -> str:
                    ok, out, err = self._execute_ssh_command(cmd, timeout=timeout_s)
                    return (out or err or "").strip()

                # CPU/load snapshot
                uptime_txt = _cmd("uptime", timeout_s=5)
                top_head = _cmd("top -bn1 | head -n 5", timeout_s=6)

                # Memory + disk
                mem_txt = _cmd("free -h | head -n 3", timeout_s=5)
                disk_root = _cmd("df -h / | head -n 2", timeout_s=5)

                # journald size (good for catching runaway logs)
                journal_usage = _cmd("journalctl --disk-usage 2>/dev/null || true", timeout_s=6)

                # RSAdminBot file-logging folder size (if configured)
                log_base = ""
                try:
                    log_base = str(((self.config.get("logging") or {}).get("file_logging") or {}).get("base_path") or "")
                except Exception:
                    log_base = ""
                log_du = ""
                if log_base:
                    log_du = _cmd(f"du -sh {shlex.quote(log_base)} 2>/dev/null || true", timeout_s=6)

                # Total size of bots folder (fast-ish, bounded)
                bots_du = _cmd("du -sh /home/rsadmin/bots 2>/dev/null | head -n 1 || true", timeout_s=10)

                # Systemd services snapshot (all configured bots)
                svc_list = sorted({str((v or {}).get("service") or "").strip() for v in (self.BOTS or {}).values() if (v or {}).get("service")})
                svc_txt = ""
                if svc_list:
                    svc_cmd = (
                        "set +e; "
                        + "for s in "
                        + " ".join(shlex.quote(s) for s in svc_list)
                        + "; do "
                        + "st=$(systemctl is-active \"$s\" 2>/dev/null || echo unknown); "
                        + "pid=$(systemctl show \"$s\" -p ExecMainPID --value 2>/dev/null || echo 0); "
                        + "echo \"$s $st pid=$pid\"; "
                        + "done"
                    )
                    svc_txt = _cmd(svc_cmd, timeout_s=10)

                # Top 10 largest files under /home/rsadmin/bots (xdev avoids scanning mounted volumes)
                top_files = _cmd(
                    "find /home/rsadmin/bots -xdev -type f -printf '%s\\t%p\\n' 2>/dev/null | sort -nr | head -n 10 | "
                    "awk -F'\\t' '{printf \"%8.1f MB\\t%s\\n\", ($1/1024/1024), $2}'",
                    timeout_s=12,
                )

                # Truncate long blocks for embed fields (1024 char field limit)
                def _clip(s: str, n: int = 900) -> str:
                    s = (s or "").strip()
                    if not s:
                        return "(no output)"
                    if len(s) <= n:
                        return s
                    return s[:n] + "\n...(truncated)"

                embed = MessageHelper.create_info_embed(
                    title="System Check",
                    message="Runtime + connectivity + Ubuntu health snapshot.",
                    fields=[
                        {"name": "OS", "value": f"{os_name} | {plat[:70]}", "inline": False},
                        {"name": "cwd", "value": cwd[:100], "inline": False},
                        {"name": "local_exec.config", "value": str(bool(local_exec_cfg)), "inline": True},
                        {"name": "local_exec.active", "value": str(bool(local_exec)), "inline": True},
                        {"name": "remote_root", "value": remote_root or "(unset)", "inline": False},
                        {"name": "remote_root_exists", "value": str(bool(remote_root_exists)), "inline": True},
                        {"name": "ssh.target", "value": f"{user}@{host}" if host else "(none)", "inline": True},
                        {"name": "ssh.key.exists", "value": key_exists_display, "inline": True},
                        {"name": "Decision", "value": mode_txt, "inline": False},
                        {"name": "CPU/Load", "value": f"```{_clip(uptime_txt, 500)}```", "inline": False},
                        {"name": "top (header)", "value": f"```{_clip(top_head, 700)}```", "inline": False},
                        {"name": "Memory", "value": f"```{_clip(mem_txt, 700)}```", "inline": False},
                        {"name": "Disk (/)", "value": f"```{_clip(disk_root, 700)}```", "inline": False},
                        {"name": "journald", "value": f"```{_clip(journal_usage, 500)}```", "inline": True},
                        {"name": "bots folder", "value": f"```{_clip(bots_du, 250)}```", "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
                if svc_txt.strip():
                    embed.add_field(name="Services (systemd)", value=f"```{_clip(svc_txt, 950)}```", inline=False)

                await ctx.send(embed=embed)

                # Post large-file summary as a second embed (keeps main embed readable)
                lf_fields = []
                if log_base:
                    lf_fields.append({"name": "RSAdminBot log path", "value": f"`{log_base}`", "inline": False})
                    if log_du.strip():
                        lf_fields.append({"name": "RSAdminBot logs size", "value": f"```{_clip(log_du, 250)}```", "inline": True})
                lf_fields.append({"name": "Top 10 largest files (/home/rsadmin/bots)", "value": f"```{_clip(top_files, 950)}```", "inline": False})
                lf_embed = MessageHelper.create_info_embed(
                    title="Disk Hotspots",
                    message="Largest files under `/home/rsadmin/bots` (helps detect runaway logs/artifacts).",
                    fields=lf_fields,
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=lf_embed)
                if self.logger:
                    try:
                        log_entry = self.logger.log_command(
                            ctx,
                            "systemcheck",
                            "success",
                            {"local_exec": local_exec, "ssh_target": f"{user}@{host}" if host else "", "key_exists": key_exists},
                        )
                        await self._log_to_discord(self.logger.create_embed(log_entry, self.logger._get_context_from_ctx(ctx)), ctx.channel)
                        self.logger.clear_command_context()
                    except Exception:
                        pass
            except Exception as e:
                err_txt = str(e)[:400]
                embed = MessageHelper.create_error_embed(
                    title="System Check Failed",
                    message="systemcheck failed.",
                    error_details=err_txt,
                    footer=f"Triggered by {ctx.author}",
                )
                await self._log_to_discord(embed, ctx.channel)

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

        # botscan removed: legacy scan/tree compare was removed entirely.
        
        @self.bot.command(name="botinfo")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def botinfo(ctx, bot_name: str = None):
            """Get detailed information about a bot (admin only)"""
            if not bot_name:
                embed = MessageHelper.create_warning_embed(
                    title="Bot Name Required",
                    message="Please specify which bot to get information about.",
                    details="Usage: `!botinfo <bot>`",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return

            bot_key = str(bot_name or "").strip().lower()
            if bot_key not in self.BOTS:
                embed = MessageHelper.create_error_embed(
                    title="Bot Not Found",
                    message=f"Bot not found: `{bot_key}`",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return

            group = self._get_bot_group(bot_key) or ""
            if group == "mirror_bots":
                info = self.BOTS.get(bot_key) or {}
                folder = str(info.get("folder") or "").strip() or bot_key
                script = str(info.get("script") or "").strip()
                service = str(info.get("service") or "").strip()
                remote_root = str(getattr(self, "remote_root", "") or "/home/rsadmin/bots/mirror-world").strip()
                config_dir = f"{remote_root}/{folder}/config"

                exists = False
                state = ""
                pid = 0
                try:
                    if self.service_manager and service:
                        exists, state, _ = self.service_manager.get_status(service, bot_name=bot_key)
                        pid = int(self.service_manager.get_pid(service) or 0)
                except Exception:
                    exists = False

                # Non-secret config presence (do not read tokens.env)
                required = ["settings.json", "tokens.env"]
                if bot_key == "discumbot":
                    required = ["settings.json", "tokens.env", "channel_map.json", "destination_channels.json"]
                checks: Dict[str, bool] = {}
                try:
                    cmd = "set +e; " + " ; ".join(
                        f'test -f {shlex.quote(config_dir + "/" + fn)} && echo OK:{fn} || echo MISSING:{fn}'
                        for fn in required
                    )
                    ok, out, err = self._execute_ssh_command(cmd, timeout=10)
                    for ln in (out or err or "").splitlines():
                        ln = (ln or "").strip()
                        if ln.startswith("OK:"):
                            checks[ln[3:].strip()] = True
                        elif ln.startswith("MISSING:"):
                            checks[ln[8:].strip()] = False
                except Exception:
                    checks = {}

                cfg_lines = []
                for fn in required:
                    state2 = checks.get(fn)
                    icon = "✅" if state2 is True else ("❌" if state2 is False else "❓")
                    label = fn
                    if fn == "tokens.env":
                        label = "tokens.env (secret)"
                    cfg_lines.append(f"{icon} {label}")

                status_icon = "✅" if exists and state == "active" else "⚠️"
                embed = MessageHelper.create_info_embed(
                    title=f"📊 {info.get('name', bot_key)} Information",
                    message="Mirror-World bot status + config presence (no secrets).",
                    fields=[
                        {"name": "Service", "value": f"`{service or '(missing)'}`", "inline": False},
                        {"name": "Status", "value": f"{status_icon} `{state or 'unknown'}`", "inline": True},
                        {"name": "PID", "value": str(pid or 0), "inline": True},
                        {"name": "Folder", "value": f"`{folder}`", "inline": True},
                        {"name": "Entrypoint", "value": f"`{script or '(unknown)'}`", "inline": True},
                        {"name": "Config dir", "value": f"`{config_dir}`", "inline": False},
                        {"name": "Config files", "value": "\n".join(cfg_lines)[:1024], "inline": False},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return

            # RS bots: inspector-based (rich) info
            if not INSPECTOR_AVAILABLE or not self.inspector:
                embed = MessageHelper.create_error_embed(
                    title="Bot Inspector Not Available",
                    message="Bot inspector module is not loaded or initialized (RS botinfo requires it).",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return
            
            try:
                bot_info = self.inspector.get_bot_info(bot_key)
                
                if not bot_info:
                    embed = MessageHelper.create_error_embed(
                        title="Bot Not Found",
                        message=f"Bot not found: `{bot_key}`",
                        footer=f"Triggered by {ctx.author}",
                    )
                    await ctx.send(embed=embed)
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
            if not bot_name:
                embed = MessageHelper.create_info_embed(
                    title="Select a Bot",
                    message="Pick a bot to view its config summary.",
                    footer=f"Triggered by {ctx.author}",
                )
                keys = self._get_rs_bot_keys() + [k for k in self._get_mw_bot_keys() if k not in self._get_rs_bot_keys()]
                view = BotSelectView(self, "config", "Config", bot_keys=keys)
                await ctx.send(embed=embed, view=view)
                return
            
            embed = self._build_botconfig_embed(bot_name, triggered_by=ctx.author)
            await ctx.send(embed=embed)

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
            
            # Build rich embed matching RSCheckerbot support card style
            embed = discord.Embed(
                title="Membership Statistics",
                color=0x5865F2,  # Discord blurple
                timestamp=datetime.now()
            )
            
            total_members = stats.get("total_members", 0)
            new_members = stats.get("new_members", 0)
            renewals = stats.get("renewals", 0)
            cancellations = stats.get("cancellations", 0)
            active_memberships = stats.get("active_memberships", 0)
            avg_duration_days = stats.get("avg_duration_days")
            
            # Overview section (inline fields for compact display)
            embed.add_field(name="Total Members", value=f"**{total_members}**", inline=True)
            embed.add_field(name="Active Memberships", value=f"**{active_memberships}**", inline=True)
            
            if avg_duration_days:
                embed.add_field(name="Avg Duration", value=f"**{avg_duration_days}** days", inline=True)
            
            # Event breakdown section
            events_text = f"• New Members: **{new_members}**\n"
            events_text += f"• Renewals: **{renewals}**\n"
            events_text += f"• Cancellations: **{cancellations}**"
            
            embed.add_field(
                name="Event Breakdown",
                value=events_text,
                inline=False
            )
            
            # Add note if no data found
            if total_members == 0:
                embed.add_field(
                    name="ℹ️ Note",
                    value="**No membership data found.** Run `!whopscan` first to scan the whop-logs channel and populate membership data.",
                    inline=False
                )
            
            embed.set_footer(text="RSAdminBot • Whop Statistics | Data source: whop_history.json | Run !whopscan to update")
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
        
        # Oracle data sync commands
        @self.bot.command(name="oracledatasync")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def oracledatasync(ctx):
            """Sync runtime data from Oracle server to local (admin only)"""
            status_msg = await ctx.send("⏳ **Syncing Oracle server runtime data...**\n```\nDownloading data files...\n```")
            
            script_path = self.base_path.parent / "scripts" / "sync_oracle_runtime_data.py"
            if not script_path.exists():
                await status_msg.edit(content="❌ **Error:** Script not found. Expected: `scripts/sync_oracle_runtime_data.py`")
                return
            
            # Run script locally (subprocess)
            try:
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    cwd=str(self.base_path.parent)
                )
                
                stdout = result.stdout or ""
                stderr = result.stderr or ""
                
                if result.returncode == 0:
                    embed = MessageHelper.create_success_embed(
                        title="Oracle Data Sync Complete",
                        message="Runtime data downloaded from Oracle server.",
                        details=stdout[-1500:] if stdout else "Sync completed successfully.",
                        footer=f"Triggered by {ctx.author}"
                    )
                    await status_msg.edit(content="", embed=embed)
                else:
                    error_msg = stderr or stdout or "Unknown error"
                    embed = MessageHelper.create_error_embed(
                        title="Oracle Data Sync Failed",
                        message="Failed to sync runtime data from Oracle server.",
                        error_details=error_msg[-1500:],
                        footer=f"Triggered by {ctx.author}"
                    )
                    await status_msg.edit(content="", embed=embed)
            except subprocess.TimeoutExpired:
                await status_msg.edit(content="❌ **Error:** Sync script timed out after 5 minutes.")
            except Exception as e:
                embed = MessageHelper.create_error_embed(
                    title="Oracle Data Sync Error",
                    message="Exception occurred while running sync script.",
                    error_details=str(e)[:1500],
                    footer=f"Triggered by {ctx.author}"
                )
                await status_msg.edit(content="", embed=embed)
        self.registered_commands.append(("oracledatasync", "Sync runtime data from Oracle server to local", True))
        
        @self.bot.command(name="oracledataanalyze")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def oracledataanalyze(ctx):
            """Analyze downloaded Oracle server runtime data (admin only)"""
            status_msg = await ctx.send("⏳ **Analyzing Oracle server runtime data...**\n```\nGenerating statistics...\n```")
            
            script_path = self.base_path.parent / "scripts" / "analyze_oracle_data.py"
            if not script_path.exists():
                await status_msg.edit(content="❌ **Error:** Script not found. Expected: `scripts/analyze_oracle_data.py`")
                return
            
            try:
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=str(self.base_path.parent)
                )
                
                stdout = result.stdout or ""
                stderr = result.stderr or ""
                
                if result.returncode == 0:
                    embed = MessageHelper.create_success_embed(
                        title="Oracle Data Analysis Complete",
                        message="Analysis report generated successfully.",
                        details=stdout[-1500:] if stdout else "Analysis completed successfully.",
                        footer=f"Triggered by {ctx.author} | Report: OracleServerData/analysis_report.md"
                    )
                    await status_msg.edit(content="", embed=embed)
                else:
                    error_msg = stderr or stdout or "Unknown error"
                    embed = MessageHelper.create_error_embed(
                        title="Oracle Data Analysis Failed",
                        message="Failed to analyze runtime data.",
                        error_details=error_msg[-1500:],
                        footer=f"Triggered by {ctx.author}"
                    )
                    await status_msg.edit(content="", embed=embed)
            except subprocess.TimeoutExpired:
                await status_msg.edit(content="❌ **Error:** Analysis script timed out after 2 minutes.")
            except Exception as e:
                embed = MessageHelper.create_error_embed(
                    title="Oracle Data Analysis Error",
                    message="Exception occurred while running analysis script.",
                    error_details=str(e)[:1500],
                    footer=f"Triggered by {ctx.author}"
                )
                await status_msg.edit(content="", embed=embed)
        self.registered_commands.append(("oracledataanalyze", "Analyze downloaded Oracle server runtime data", True))
        
        @self.bot.command(name="oracledatadoc")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def oracledatadoc(ctx):
            """Generate documentation report of ask mode changes (admin only)"""
            status_msg = await ctx.send("⏳ **Generating documentation report...**\n```\nDocumenting changes...\n```")
            
            script_path = self.base_path.parent / "scripts" / "document_ask_mode_changes.py"
            if not script_path.exists():
                await status_msg.edit(content="❌ **Error:** Script not found. Expected: `scripts/document_ask_mode_changes.py`")
                return
            
            try:
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=str(self.base_path.parent)
                )
                
                stdout = result.stdout or ""
                stderr = result.stderr or ""
                
                if result.returncode == 0:
                    embed = MessageHelper.create_success_embed(
                        title="Documentation Report Generated",
                        message="Documentation report created successfully.",
                        details=stdout[-1500:] if stdout else "Report generated successfully.",
                        footer=f"Triggered by {ctx.author} | Report: docs/ASK_MODE_CHANGES_REPORT.md"
                    )
                    await status_msg.edit(content="", embed=embed)
                else:
                    error_msg = stderr or stdout or "Unknown error"
                    embed = MessageHelper.create_error_embed(
                        title="Documentation Generation Failed",
                        message="Failed to generate documentation report.",
                        error_details=error_msg[-1500:],
                        footer=f"Triggered by {ctx.author}"
                    )
                    await status_msg.edit(content="", embed=embed)
            except subprocess.TimeoutExpired:
                await status_msg.edit(content="❌ **Error:** Documentation script timed out after 1 minute.")
            except Exception as e:
                embed = MessageHelper.create_error_embed(
                    title="Documentation Generation Error",
                    message="Exception occurred while running documentation script.",
                    error_details=str(e)[:1500],
                    footer=f"Triggered by {ctx.author}"
                )
                await status_msg.edit(content="", embed=embed)
        self.registered_commands.append(("oracledatadoc", "Generate documentation report of ask mode changes", True))
        
        @self.bot.command(name="oracledatasample")
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def oracledatasample(ctx, post: str = "no", event_type: str = "all"):
            """Generate sample embed outputs from scanned data (admin only).

            Usage:
              - !oracledatasample
              - !oracledatasample post
              - !oracledatasample post cancellation
            """
            status_msg = await ctx.send("⏳ **Generating sample embed outputs...**\n```\nCreating sample embeds...\n```")
            
            script_path = self.base_path.parent / "scripts" / "generate_sample_embeds_from_data.py"
            if not script_path.exists():
                await status_msg.edit(content="❌ **Error:** Script not found. Expected: `scripts/generate_sample_embeds_from_data.py`")
                return
            
            try:
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=str(self.base_path.parent)
                )
                
                stdout = result.stdout or ""
                stderr = result.stderr or ""
                
                if result.returncode == 0:
                    embed = MessageHelper.create_success_embed(
                        title="Sample Embed Generation Complete",
                        message="Sample embed outputs generated successfully.",
                        details=stdout[-1500:] if stdout else "Sample generation completed successfully.",
                        footer=f"Triggered by {ctx.author} | Report: OracleServerData/sample_embeds_report.md"
                    )
                    await status_msg.edit(content="", embed=embed)

                    # Optional: post the generated sample embeds into the channel for visual verification.
                    try:
                        post_flag = str(post or "").strip().lower()
                        should_post = post_flag in ("yes", "y", "true", "1", "post")
                        type_filter = str(event_type or "all").strip().lower()

                        if should_post:
                            out_path = self.base_path.parent / "OracleServerData" / "sample_embeds_output.json"
                            if not out_path.exists():
                                await ctx.send("⚠️ Sample output JSON not found. Expected: `OracleServerData/sample_embeds_output.json`")
                                return

                            try:
                                payload = json.loads(out_path.read_text(encoding="utf-8") or "{}")
                            except Exception as e:
                                await ctx.send(f"⚠️ Failed to read sample output JSON: {str(e)[:200]}")
                                return

                            samples = (payload.get("sample_embeds") or {}) if isinstance(payload, dict) else {}
                            if not isinstance(samples, dict) or not samples:
                                await ctx.send("⚠️ No sample embeds were generated (sample_embeds is empty).")
                                return

                            ordered_types = ["new", "renewal", "cancellation", "completed"]
                            available_types = [t for t in ordered_types if t in samples] + [t for t in samples.keys() if t not in ordered_types]

                            if type_filter != "all":
                                # allow short aliases (e.g. cancel -> cancellation)
                                aliases = {
                                    "cancel": "cancellation",
                                    "canceled": "cancellation",
                                    "cancelled": "cancellation",
                                }
                                type_filter = aliases.get(type_filter, type_filter)
                                available_types = [t for t in available_types if t == type_filter]

                            if not available_types:
                                await ctx.send(f"⚠️ No samples match event_type `{type_filter}`.")
                                return

                            await ctx.send(f"🧪 Posting sample embeds ({', '.join(available_types)})...")

                            for t in available_types[:10]:
                                block = samples.get(t) or {}
                                embed_data = block.get("embed") or {}
                                sample_event = block.get("sample_event_data") or {}
                                if not isinstance(embed_data, dict):
                                    continue

                                title = str(embed_data.get("title") or f"Sample ({t})")
                                color = embed_data.get("color")
                                try:
                                    color_int = int(color) if color is not None else 0x5865F2
                                except Exception:
                                    color_int = 0x5865F2

                                rs_embed = discord.Embed(
                                    title=title,
                                    color=color_int,
                                    timestamp=datetime.now(timezone.utc),
                                )

                                # Try to mimic "support card" header using the real Discord member (if resolvable)
                                resolved_user = None
                                resolved_member = None
                                resolved_user_id = None
                                try:
                                    did = str(sample_event.get("discord_id") or "").strip()
                                    if did and ctx.guild:
                                        resolved_user_id = int(did)
                                        resolved_member = ctx.guild.get_member(resolved_user_id)
                                        if resolved_member:
                                            resolved_user = resolved_member
                                        else:
                                            # Not in this guild (e.g., posting samples in a test server). Fetch user anyway for avatar/name.
                                            resolved_user = await self.bot.fetch_user(resolved_user_id)
                                    elif did:
                                        resolved_user_id = int(did)
                                        resolved_user = await self.bot.fetch_user(resolved_user_id)
                                    if resolved_user:
                                        rs_embed.set_author(name=str(resolved_user), icon_url=resolved_user.display_avatar.url)
                                        rs_embed.set_thumbnail(url=resolved_user.display_avatar.url)
                                except Exception:
                                    pass

                                # Fields
                                fields = embed_data.get("fields") or []
                                if isinstance(fields, list):
                                    for f in fields[:25]:
                                        if not isinstance(f, dict):
                                            continue
                                        name = str(f.get("name") or "")[:256] or "Field"
                                        value = str(f.get("value") or "—")
                                        inline = bool(f.get("inline", False))
                                        # If we're posting in a server where the user isn't a member, raw <@id> mentions render as-is.
                                        # Improve readability while preserving the ID.
                                        try:
                                            if resolved_user_id and name.strip().lower() == "member":
                                                raw_mention = f"<@{resolved_user_id}>"
                                                if value.strip() == raw_mention and resolved_user and not resolved_member:
                                                    value = f"{resolved_user.name} ({raw_mention})"
                                        except Exception:
                                            pass
                                        # Discord embed field value limit is 1024 chars
                                        rs_embed.add_field(name=name, value=value[:1024], inline=inline)

                                footer = embed_data.get("footer")
                                if footer:
                                    rs_embed.set_footer(text=str(footer)[:2048])

                                await ctx.send(embed=rs_embed)

                    except Exception as e:
                        # Never fail the command after generation succeeded, but do report why posting failed.
                        try:
                            await ctx.send(f"⚠️ Sample embed posting failed: {str(e)[:200]}")
                        except Exception:
                            pass
                else:
                    error_msg = stderr or stdout or "Unknown error"
                    embed = MessageHelper.create_error_embed(
                        title="Sample Embed Generation Failed",
                        message="Failed to generate sample embed outputs.",
                        error_details=error_msg[-1500:],
                        footer=f"Triggered by {ctx.author}"
                    )
                    await status_msg.edit(content="", embed=embed)
            except subprocess.TimeoutExpired:
                await status_msg.edit(content="❌ **Error:** Sample generation script timed out after 2 minutes.")
            except Exception as e:
                embed = MessageHelper.create_error_embed(
                    title="Sample Embed Generation Error",
                    message="Exception occurred while running sample generation script.",
                    error_details=str(e)[:1500],
                    footer=f"Triggered by {ctx.author}"
                )
                await status_msg.edit(content="", embed=embed)
        self.registered_commands.append(("oracledatasample", "Generate sample embed outputs from scanned data", True))
        
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
            # Also (re)publish the RSAdminBot command index/cards into the commands channel.
            try:
                await self._publish_command_index_to_test_server()
            except Exception:
                pass

        @self.bot.command(name="testcards", aliases=["testcenter_cards", "tcards"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def testcards(ctx, *, args: str = ""):
            """Post RSCheckerbot sample staff cards into TestCenter channels + write a JSON trace artifact (admin only)."""
            # Resolve TestCenter guild
            try:
                test_gid = int(self.config.get("test_server_guild_id") or 0)
            except Exception:
                test_gid = 0
            test_guild = self.bot.get_guild(test_gid) if test_gid else None
            if not test_guild:
                await ctx.send("❌ TestCenter guild not found/available to RSAdminBot.")
                return

            # Parse args:
            # - Default: target is invoking user
            # - Accept a target (mention/user_id/name) and/or membership_id override (mem_...)
            raw = str(args or "").strip()
            tokens = [t for t in raw.split() if t.strip()]
            membership_override = ""
            target_token = ""
            for t in tokens:
                if t.lower().startswith("mem_"):
                    membership_override = t.strip()
                elif not target_token:
                    target_token = t.strip()

            # Resolve target member in TestCenter
            target_member: discord.Member | None = None
            target_id = 0
            try:
                import re

                tok = target_token.strip()
                if not tok:
                    target_id = int(ctx.author.id)
                else:
                    m = re.match(r"^<@!?(\\d+)>$", tok)
                    if m:
                        target_id = int(m.group(1))
                    elif tok.strip().lstrip("@").isdigit():
                        target_id = int(tok.strip().lstrip("@"))
                    else:
                        needle = tok.strip().lstrip("@").lower()
                        # Prefer exact display_name match, then exact username match, then substring match.
                        exact = [m for m in (test_guild.members or []) if str(getattr(m, "display_name", "")).lower() == needle]
                        if not exact:
                            exact = [m for m in (test_guild.members or []) if str(getattr(m, "name", "")).lower() == needle]
                        if exact:
                            target_member = exact[0]
                        else:
                            partial = [
                                m
                                for m in (test_guild.members or [])
                                if needle
                                and (
                                    needle in str(getattr(m, "display_name", "")).lower()
                                    or needle in str(getattr(m, "name", "")).lower()
                                    or needle in str(m).lower()
                                )
                            ]
                            if len(partial) == 1:
                                target_member = partial[0]
                            elif len(partial) > 1:
                                await ctx.send(
                                    "❌ Multiple TestCenter members match that name. Use an @mention or numeric user ID."
                                )
                                return
                        if target_member is None:
                            await ctx.send("❌ Target member not found in TestCenter. Use an @mention or user ID.")
                            return

                if target_member is None and target_id:
                    target_member = test_guild.get_member(target_id)
                    if target_member is None:
                        try:
                            target_member = await test_guild.fetch_member(target_id)
                        except Exception:
                            target_member = None
            except Exception:
                target_member = None

            if not isinstance(target_member, discord.Member):
                await ctx.send("❌ Target member is not in the TestCenter server (cannot post member-based cards).")
                return

            # Load RSCheckerbot runtime config+secrets (server-local)
            try:
                rs_cfg, _, _ = load_config_with_secrets(_REPO_ROOT / "RSCheckerbot")
            except Exception as e:
                await ctx.send(f"❌ Failed to load RSCheckerbot config: {e}")
                return

            # Resolve target channels in TestCenter (do NOT reuse RS server IDs/config)
            dm_cfg = rs_cfg.get("dm_sequence") if isinstance(rs_cfg, dict) else {}

            async def _get_or_create_category(name: str) -> tuple[discord.CategoryChannel | None, bool]:
                cat = discord.utils.get(test_guild.categories, name=name)
                if isinstance(cat, discord.CategoryChannel):
                    return cat, False
                try:
                    created = await test_guild.create_category(name, reason="RSAdminBot !testcards bootstrap")
                    return created, True
                except Exception as e:
                    await ctx.send(f"❌ Failed to create TestCenter category '{name}': {str(e)[:200]}")
                    return None, False

            async def _get_or_create_text(name: str, category: discord.CategoryChannel | None) -> tuple[discord.TextChannel | None, bool]:
                ch = discord.utils.get(test_guild.text_channels, name=name)
                if isinstance(ch, discord.TextChannel):
                    # Keep channel organized under the category (best-effort).
                    if category and ch.category_id != category.id:
                        try:
                            await ch.edit(category=category, reason="RSAdminBot !testcards bootstrap (organize)")
                        except Exception:
                            pass
                    return ch, False
                try:
                    created = await test_guild.create_text_channel(name, category=category, reason="RSAdminBot !testcards bootstrap")
                    return created, True
                except Exception as e:
                    await ctx.send(f"❌ Failed to create TestCenter channel '{name}': {str(e)[:200]}")
                    return None, False

            category_name = "RSCheckerbot Staff Alerts (TestCenter)"
            cat, cat_created = await _get_or_create_category(category_name)
            if cat is None:
                return

            status_ch, status_created = await _get_or_create_text("member-status-logs", cat)
            payment_ch, pay_created = await _get_or_create_text("payment-failure", cat)
            cancel_ch, cancel_created = await _get_or_create_text("member-cancelation", cat)

            if not isinstance(status_ch, discord.TextChannel):
                await ctx.send("❌ TestCenter channel not available: member-status-logs")
                return
            if not isinstance(payment_ch, discord.TextChannel):
                await ctx.send("❌ TestCenter channel not available: payment-failure")
                return
            if not isinstance(cancel_ch, discord.TextChannel):
                await ctx.send("❌ TestCenter channel not available: member-cancelation")
                return

            # Compute access roles (compact, access-relevant only)
            try:
                from RSCheckerbot.rschecker_utils import access_roles_plain as _access_roles_plain  # type: ignore
                from RSCheckerbot.rschecker_utils import coerce_role_ids as _coerce_role_ids  # type: ignore
            except Exception as e:
                await ctx.send(f"❌ Failed to import RSCheckerbot role helpers: {e}")
                return

            relevant = _coerce_role_ids(
                (dm_cfg or {}).get("role_cancel_a"),
                (dm_cfg or {}).get("role_cancel_b"),
                (dm_cfg or {}).get("welcome_role_id"),
                (dm_cfg or {}).get("role_trigger"),
                (dm_cfg or {}).get("former_member_role"),
            )
            try:
                for rid in ((dm_cfg or {}).get("roles_to_check") or []):
                    if str(rid).strip().isdigit():
                        relevant.add(int(str(rid).strip()))
            except Exception:
                pass
            # In TestCenter, RS server role IDs will not match, so the RSCheckerbot filter often returns "—".
            # Fallback to the member's visible role names in TestCenter to keep the test output human-usable.
            access_roles = _access_roles_plain(target_member, relevant)
            if not str(access_roles or "").strip() or str(access_roles).strip() == "—":
                try:
                    names: list[str] = []
                    for r in (getattr(target_member, "roles", None) or []):
                        nm = str(getattr(r, "name", "") or "").strip()
                        if not nm or nm == "@everyone":
                            continue
                        if nm not in names:
                            names.append(nm)
                    access_roles = ", ".join(names) if names else "—"
                except Exception:
                    access_roles = access_roles or "—"

            # Fetch Whop brief (best-effort)
            whop_brief = {}
            membership_id_used = membership_override
            whop_fetch = {"status": "skipped", "error": "", "membership_id": "", "used_override": bool(membership_override)}

            def _membership_id_from_history(discord_id: int) -> str:
                try:
                    hist_path = _REPO_ROOT / "RSCheckerbot" / "member_history.json"
                    if not hist_path.exists():
                        return ""
                    data = json.loads(hist_path.read_text(encoding="utf-8") or "{}")
                    if not isinstance(data, dict):
                        return ""
                    rec = data.get(str(discord_id), {})
                    wh = rec.get("whop") if isinstance(rec, dict) else None
                    if isinstance(wh, dict):
                        mid = str(wh.get("last_membership_id") or wh.get("last_whop_key") or "").strip()
                        if mid.startswith(("mem_", "R-")):
                            return mid
                except Exception:
                    return ""
                return ""
            try:
                from RSCheckerbot.whop_api_client import WhopAPIClient  # type: ignore
                from RSCheckerbot.whop_brief import fetch_whop_brief  # type: ignore

                wh = rs_cfg.get("whop_api") if isinstance(rs_cfg, dict) else {}
                if isinstance(wh, dict):
                    api_key = str(wh.get("api_key") or "").strip()
                    company_id = str(wh.get("company_id") or "").strip()
                    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
                    if api_key and company_id:
                        client = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)
                        if not membership_id_used:
                            membership_id_used = _membership_id_from_history(target_member.id)
                        mid = membership_id_used
                        whop_fetch["membership_id"] = mid or ""
                        if mid:
                            whop_brief = await fetch_whop_brief(
                                client,
                                mid,
                                enable_enrichment=bool(wh.get("enable_enrichment", True)),
                            )
                            whop_fetch["status"] = "ok"
                        else:
                            whop_fetch["status"] = "missing_membership_id"
                    else:
                        whop_fetch["status"] = "missing_whop_api_config"
            except Exception as e:
                whop_fetch["status"] = "error"
                whop_fetch["error"] = str(e)[:200]
                whop_brief = {}

            # Build + post sample cards (same builders as RSCheckerbot)
            try:
                from RSCheckerbot.staff_embeds import build_case_minimal_embed, build_member_status_detailed_embed  # type: ignore
            except Exception as e:
                await ctx.send(f"❌ Failed to import RSCheckerbot embed builders: {e}")
                return

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            trace = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "test_server_guild_id": test_guild.id,
                "target_member_id": target_member.id,
                "target_member_name": str(target_member),
                "membership_id_override": membership_override or "",
                "membership_id_used": membership_id_used or "",
                "whop_fetch": whop_fetch,
                "bootstrap": {
                    "category_name": category_name,
                    "category_id": cat.id,
                    "category_created": cat_created,
                    "channels_created": {
                        "member-status-logs": status_created,
                        "payment-failure": pay_created,
                        "member-cancelation": cancel_created,
                    },
                },
                "channels": {
                    "member_status_logs": status_ch.id,
                    "payment_failure": payment_ch.id,
                    "member_cancelation": cancel_ch.id,
                },
                "posts": [],
            }

            allowed = discord.AllowedMentions.none()

            detailed = build_member_status_detailed_embed(
                title="❌ Payment Failed — Action Needed",
                member=target_member,
                access_roles=access_roles,
                color=0xED4245,
                whop_brief=whop_brief,
                event_kind="payment_failed",
            )
            minimal_fail = build_case_minimal_embed(
                title="❌ Payment Failed — Action Needed",
                member=target_member,
                access_roles=access_roles,
                whop_brief=whop_brief,
                color=0xED4245,
                event_kind="payment_failed",
            )
            minimal_cancel = build_case_minimal_embed(
                title="⚠️ Cancellation Scheduled",
                member=target_member,
                access_roles=access_roles,
                whop_brief=whop_brief,
                color=0xFEE75C,
                event_kind="cancellation_scheduled",
            )

            # Post messages and record outcomes
            m1 = await status_ch.send(embed=detailed, allowed_mentions=allowed)
            trace["posts"].append(
                {
                    "channel": "member_status_logs",
                    "message_id": m1.id,
                    "embed_title": detailed.title,
                    "field_names": [f.name for f in (detailed.fields or [])],
                }
            )
            m2 = await payment_ch.send(embed=minimal_fail, allowed_mentions=allowed)
            trace["posts"].append(
                {
                    "channel": "payment_failure",
                    "message_id": m2.id,
                    "embed_title": minimal_fail.title,
                    "field_names": [f.name for f in (minimal_fail.fields or [])],
                }
            )
            m3 = await cancel_ch.send(embed=minimal_cancel, allowed_mentions=allowed)
            trace["posts"].append(
                {
                    "channel": "member_cancelation",
                    "message_id": m3.id,
                    "embed_title": minimal_cancel.title,
                    "field_names": [f.name for f in (minimal_cancel.fields or [])],
                }
            )

            # Write artifact on disk and upload to Discord
            artifact_dir = _REPO_ROOT / "RSAdminBot" / "test_artifacts"
            artifact_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = artifact_dir / f"testcenter_cards_trace_{ts}.json"
            artifact_path.write_text(json.dumps(trace, indent=2, ensure_ascii=True), encoding="utf-8")

            summary = MessageHelper.create_success_embed(
                title="TestCenter Cards Posted",
                message="Posted sample RSCheckerbot cards to TestCenter channels and wrote JSON trace artifact.",
                fields=[
                    {"name": "member-status-logs", "value": f"<#{status_ch.id}> (msg {m1.id})", "inline": False},
                    {"name": "payment-failure", "value": f"<#{payment_ch.id}> (msg {m2.id})", "inline": False},
                    {"name": "member-cancelation", "value": f"<#{cancel_ch.id}> (msg {m3.id})", "inline": False},
                    {"name": "artifact", "value": str(artifact_path), "inline": False},
                ],
            )
            await ctx.send(embed=summary, file=discord.File(str(artifact_path)))
        
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
            status_msg = await ctx.send(
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message="Initializing comprehensive test...",
                    footer=f"Triggered by {ctx.author}",
                )
            )
            
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Initialization: !ping (checking bot latency)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Initialization: !status (checking bot readiness)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Initialization: !reload (reloading configuration)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Initialization: !botlist (listing all bots)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Initialization: !setupmonitoring (setting up monitoring channels)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Bot Management: !botstatus (all bots)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
                await status_msg.edit(
                    content="",
                    embed=MessageHelper.create_info_embed(
                        title="Run All Commands",
                        message=f"[Phase {phase}] Bot Management: !botinfo {bot_name} ({idx}/{len(bot_names)})...",
                        footer=f"Triggered by {ctx.author}",
                    ),
                )
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
                await status_msg.edit(
                    content="",
                    embed=MessageHelper.create_info_embed(
                        title="Run All Commands",
                        message=f"[Phase {phase}] Bot Management: !botconfig {bot_name} ({idx}/{len(bot_names)})...",
                        footer=f"Triggered by {ctx.author}",
                    ),
                )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Whop: !whopscan (2000 messages, 30 days)...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
            await status_msg.edit(
                content="",
                embed=MessageHelper.create_info_embed(
                    title="Run All Commands",
                    message=f"[Phase {phase}] Whop: !whopstats...",
                    footer=f"Triggered by {ctx.author}",
                ),
            )
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
                await status_msg.edit(
                    content="",
                    embed=MessageHelper.create_info_embed(
                        title="Run All Commands",
                        message=f"[Phase {phase}] Movements: !botmovements {bot_name} ({idx}/{len(bot_names)})...",
                        footer=f"Triggered by {ctx.author}",
                    ),
                )
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
                await status_msg.edit(
                    content="",
                    embed=MessageHelper.create_info_embed(
                        title="Run All Commands",
                        message=f"[Phase {phase}] Sync: !botupdate {bot_name} ({idx}/{len(bot_names)})...",
                        footer=f"Triggered by {ctx.author}",
                    ),
                )
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
                    scp_cmd = [
                        "scp",
                        "-o", "StrictHostKeyChecking=no",
                        "-P", str(self.current_server.get("port", 22)),
                        str(local_report_path),
                        f"{self.current_server.get('user', 'rsadmin')}@{self.current_server.get('host')}:{remote_path}"
                    ]
                    ssh_key = str(self.current_server.get("key") or "").strip()
                    if ssh_key:
                        ssh_key_path = Path(ssh_key).expanduser()
                        if ssh_key_path.exists():
                            scp_cmd[1:1] = ["-i", str(ssh_key_path)]
                    
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
                embed = MessageHelper.create_error_embed(
                    title="Server Only",
                    message="This command can only be used in a server.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return
            
            # If no mentions, delete current channel
            if not channel_mentions:
                try:
                    confirm_embed = MessageHelper.create_warning_embed(
                        title="Deleting Channel",
                        message="Deleting this channel in 2 seconds...",
                        fields=[
                            {"name": "Channel", "value": f"#{getattr(ctx.channel, 'name', 'unknown')}", "inline": True},
                            {"name": "Channel ID", "value": str(getattr(ctx.channel, "id", "")), "inline": True},
                        ],
                        footer=f"Triggered by {ctx.author}",
                    )
                    confirm_msg = await ctx.send(embed=confirm_embed)
                    await asyncio.sleep(2)
                    await ctx.channel.delete(reason=f"Deleted by {ctx.author} via RSAdminBot")
                    # Log success to log channel (reply channel no longer exists)
                    try:
                        await self._log_to_discord(confirm_embed, None)
                    except Exception:
                        pass
                except discord.Forbidden:
                    try:
                        err_embed = MessageHelper.create_error_embed(
                            title="Delete Failed",
                            message="I don't have permission to delete this channel.",
                            footer=f"Triggered by {ctx.author}",
                        )
                        await confirm_msg.edit(embed=err_embed)
                        await self._log_to_discord(err_embed, None)
                    except:
                        await ctx.send(embed=MessageHelper.create_error_embed("Delete Failed", "I don't have permission to delete this channel."))
                except discord.HTTPException as e:
                    try:
                        err_embed = MessageHelper.create_error_embed(
                            title="Delete Failed",
                            message="Failed to delete channel.",
                            error_details=str(e)[:200],
                            footer=f"Triggered by {ctx.author}",
                        )
                        await confirm_msg.edit(embed=err_embed)
                        await self._log_to_discord(err_embed, None)
                    except:
                        await ctx.send(embed=MessageHelper.create_error_embed("Delete Failed", "Failed to delete channel.", str(e)[:200]))
                except Exception as e:
                    try:
                        err_embed = MessageHelper.create_error_embed(
                            title="Delete Failed",
                            message="Unexpected error while deleting channel.",
                            error_details=str(e)[:200],
                            footer=f"Triggered by {ctx.author}",
                        )
                        await confirm_msg.edit(embed=err_embed)
                        await self._log_to_discord(err_embed, None)
                    except:
                        await ctx.send(embed=MessageHelper.create_error_embed("Delete Failed", "Unexpected error while deleting channel.", str(e)[:200]))
                return
            
            # Parse channel mentions
            channels_to_delete = []
            for mention in channel_mentions:
                try:
                    channel = await commands.TextChannelConverter().convert(ctx, mention)
                    if channel and channel.guild == ctx.guild:
                        channels_to_delete.append(channel)
                except commands.ChannelNotFound:
                    pass
                except Exception as e:
                    pass
            
            if not channels_to_delete:
                embed = MessageHelper.create_error_embed(
                    title="No Valid Channels",
                    message="No valid channels found to delete. Use channel mentions like `#channel-name`.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return
            
            # Delete channels
            deleted = []
            failed = []
            for channel in channels_to_delete:
                try:
                    await channel.delete(reason=f"Deleted by {ctx.author} via RSAdminBot")
                    deleted.append(f"`{channel.name}`")
                except discord.Forbidden:
                    failed.append(f"`{channel.name}` (no permission)")
                except discord.HTTPException as e:
                    failed.append(f"`{channel.name}` ({str(e)[:50]})")
                except Exception as e:
                    failed.append(f"`{channel.name}` ({str(e)[:50]})")
            
            fields = []
            if deleted:
                fields.append({"name": "Deleted", "value": ", ".join(deleted)[:900], "inline": False})
            if failed:
                fields.append({"name": "Failed", "value": ", ".join(failed)[:900], "inline": False})
            result_embed = MessageHelper.create_info_embed(
                title="Channel Deletion Complete",
                message="Deletion run finished.",
                fields=fields or [{"name": "Result", "value": "No channels deleted.", "inline": False}],
                footer=f"Triggered by {ctx.author}",
            )
            await ctx.send(embed=result_embed)
            await self._log_to_discord(result_embed, None)
        self.registered_commands.append(("delete", "Delete channel(s)", True))
        
        @self.bot.command(name="transfer", aliases=["t"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def transfer_channel(ctx, channel_mention: str = None, category_mention: str = None):
            """Transfer a channel to another category - use channel mention and category mention (admin only)"""
            if not ctx.guild:
                embed = MessageHelper.create_error_embed(
                    title="Server Only",
                    message="This command can only be used in a server.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed)
                return
            
            # If no arguments, show interactive selector
            if not channel_mention:
                view = ChannelTransferView(self, ctx)
                embed = MessageHelper.create_info_embed(
                    title="Transfer Channel",
                    message="Select a channel and category from the dropdowns.",
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=embed, view=view)
                return
            
            # Parse channel
            try:
                channel = await commands.TextChannelConverter().convert(ctx, channel_mention)
                if not channel or channel.guild != ctx.guild:
                    await ctx.send(embed=MessageHelper.create_error_embed("Channel Not Found", "Channel not found or not in this server."))
                    return
            except commands.ChannelNotFound:
                await ctx.send(embed=MessageHelper.create_error_embed("Channel Not Found", f"Channel not found: {channel_mention}"))
                return
            except Exception as e:
                await ctx.send(embed=MessageHelper.create_error_embed("Parse Error", "Error parsing channel.", str(e)[:200]))
                return
            
            # Parse category
            if not category_mention:
                await ctx.send(embed=MessageHelper.create_warning_embed(
                    "Category Required",
                    "Please provide a category name or mention.",
                    details="Usage: `!transfer #channel CategoryName`",
                    footer=f"Triggered by {ctx.author}",
                ))
                return
            
            try:
                category = await commands.CategoryChannelConverter().convert(ctx, category_mention)
                if not category or category.guild != ctx.guild:
                    await ctx.send(embed=MessageHelper.create_error_embed("Category Not Found", "Category not found or not in this server."))
                    return
            except commands.ChannelNotFound:
                await ctx.send(embed=MessageHelper.create_error_embed("Category Not Found", f"Category not found: {category_mention}"))
                return
            except Exception as e:
                await ctx.send(embed=MessageHelper.create_error_embed("Parse Error", "Error parsing category.", str(e)[:200]))
                return
            
            # Transfer channel
            try:
                await channel.edit(category=category, reason=f"Transferred by {ctx.author} via RSAdminBot")
                ok_embed = MessageHelper.create_success_embed(
                    title="Channel Transferred",
                    message=f"`{channel.name}` → `{category.name}`",
                    fields=[
                        {"name": "Channel", "value": f"#{channel.name}", "inline": True},
                        {"name": "Category", "value": category.name, "inline": True},
                    ],
                    footer=f"Triggered by {ctx.author}",
                )
                await ctx.send(embed=ok_embed)
                await self._log_to_discord(ok_embed, None)
            except discord.Forbidden:
                await ctx.send(embed=MessageHelper.create_error_embed("Transfer Failed", "I don't have permission to edit this channel."))
            except discord.HTTPException as e:
                await ctx.send(embed=MessageHelper.create_error_embed("Transfer Failed", "Failed to transfer channel.", str(e)[:200]))
            except Exception as e:
                await ctx.send(embed=MessageHelper.create_error_embed("Transfer Failed", "Unexpected error.", str(e)[:200]))
        self.registered_commands.append(("transfer", "Transfer channel to category", True))
        
        @self.bot.command(name="add", aliases=["a"])
        @commands.check(lambda ctx: self.is_admin(ctx.author))
        async def add_channel(ctx, channel_mention: str = None, category_mention: str = None):
            """Add a channel to a category - use channel mention and category mention (admin only)"""
            # Same as transfer (transfer = move channel to category, add = same thing)
            transfer_cmd = self.bot.get_command("transfer")
            if transfer_cmd:
                await ctx.invoke(transfer_cmd, channel_mention=channel_mention, category_mention=category_mention)
            else:
                await ctx.send(embed=MessageHelper.create_error_embed("Command Missing", "Transfer command not found."))
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
        import os
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors: List[str] = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")

        # Canonical SSH config: oraclekeys/servers.json + ssh_server_name selector
        server_name = str(cfg.get("ssh_server_name") or "").strip()
        if not server_name:
            legacy = cfg.get("ssh_server")
            if isinstance(legacy, dict):
                server_name = str(legacy.get("name") or "").strip()

        if not server_name:
            errors.append("Missing ssh_server_name in RSAdminBot/config.json (must match oraclekeys/servers.json entry name)")
        else:
            try:
                servers, servers_path = load_oracle_servers(base.parent)
                entry = pick_oracle_server(servers, server_name)
                host = str(entry.get("host") or "").strip()
                user = str(entry.get("user") or "").strip() or "rsadmin"
                key_value = str(entry.get("key") or "").strip()
                remote_root = str(entry.get("remote_root") or entry.get("live_root") or f"/home/{user}/bots/mirror-world").strip()
                local_exec_cfg = bool((cfg.get("local_exec") or {}).get("enabled", True))
                local_exec_effective = (os.name != "nt") and local_exec_cfg
                if not host:
                    errors.append("servers.json entry missing host")
                if not key_value and not local_exec_effective:
                    errors.append("servers.json entry missing key (required when not in local-exec mode)")
                if key_value:
                    key_path = resolve_oracle_ssh_key_path(key_value, base.parent)
                    if not key_path.exists() and not local_exec_effective:
                        errors.append(f"SSH key not found: {key_path}")
            except Exception as e:
                errors.append(f"SSH config error: {e}")

        if errors:
            print(f"{Colors.RED}[ConfigCheck] FAILED{Colors.RESET}")
            for e in errors:
                print(f"- {e}")
            return

        print(f"{Colors.GREEN}[ConfigCheck] OK{Colors.RESET}")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        print(f"- ssh_server_name: {server_name}")
        try:
            servers, servers_path = load_oracle_servers(base.parent)
            entry = pick_oracle_server(servers, server_name)
            print(f"- servers.json: {servers_path}")
            print(f"- ssh.host: {entry.get('host')}")
            print(f"- ssh.user: {entry.get('user')}")
            print(f"- ssh.key: {entry.get('key')}")
        except Exception:
            pass
        return

    bot = RSAdminBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[Bot] Stopped{Colors.RESET}")


if __name__ == '__main__':
    main()

