# RSAdminBot Commands Reference

## Overview
RSAdminBot is the central administrative bot for managing all RS bots on the Oracle Ubuntu server. It provides comprehensive bot management, system monitoring, deployment, and diagnostic capabilities.

## Slash Commands (Private)

#### `/rsnote`
- **Description**: Private saved notes panel (SKU / RS post links / anything)
- **Usage**: `/rsnote`
- **Visibility**: Ephemeral (only you can see it)
- **Storage**: `RSAdminBot/RSNotes/data/rsnotes.json` (auto-created at runtime; not committed)
- **Config**: `RSAdminBot/RSNotes/configs.json` (tracked)

## Command Categories

### Core Commands

#### `!ping`
- **Description**: Check bot latency
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!ping`
- **Admin Only**: No
- **Returns**: Bot latency in milliseconds

#### `!status`
- **Description**: Show bot status and readiness
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!status`
- **Admin Only**: Yes
- **Returns**: Connection status, servers, SSH status, modules status, quick commands reminder

#### `!reload`
- **Description**: Reload configuration
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!reload`
- **Admin Only**: Yes
- **Returns**: Confirmation message

#### `!restart`
- **Description**: Restart RSAdminBot locally or remotely
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!restart`
- **Admin Only**: Yes
- **Returns**: Interactive buttons for local or remote restart

### Bot Management Commands

#### `!botlist`
- **Description**: List all available bots
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!botlist`
- **Admin Only**: Yes
- **Returns**: List of all bots with their names

#### `!botstatus`
- **Description**: Check status of a bot or all bots
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to check status for. If omitted, checks all bots
- **Usage**: `!botstatus` or `!botstatus rsadminbot`
- **Admin Only**: Yes
- **Returns**: Service status, PID, detailed status information

#### `!botstart`
- **Description**: Start a bot
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to start. If omitted, shows interactive dropdown
- **Usage**: `!botstart` or `!botstart rsadminbot`
- **Admin Only**: Yes
- **Returns**: Success/error message with service state changes

#### `!botstop`
- **Description**: Stop a bot
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to stop. If omitted, shows interactive dropdown
- **Usage**: `!botstop` or `!botstop rsadminbot`
- **Admin Only**: Yes
- **Returns**: Success/error message with service state changes

#### `!botrestart`
- **Description**: Restart a bot
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to restart. If omitted, shows interactive dropdown
- **Usage**: `!botrestart` or `!botrestart rsadminbot`
- **Admin Only**: Yes
- **Returns**: Success/error message with service state changes

#### `!botupdate`
- **Description**: Update a bot by pulling python-only code from GitHub and restarting it
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to update. If omitted, shows interactive dropdown (RS bots only)
- **Usage**: `!botupdate` or `!botupdate rsadminbot`
- **Admin Only**: Yes
- **Returns**: Git commit info, files synced, restart status

#### `!mwupdate`
- **Description**: Update a Mirror-World bot by pulling python-only code from GitHub and restarting it
- **Aliases**: `mwbots`
- **Parameters**:
  - `bot_name` (optional): Mirror-World bot name to update. If omitted, shows interactive dropdown (MW bots only)
- **Usage**: `!mwupdate` or `!mwupdate datamanagerbot`
- **Admin Only**: Yes
- **Returns**: Git commit info, files synced, restart status

#### `!botsync`
- **Description**: Sync local bot files directly to Oracle server via rsync
- **Aliases**: `syncbot`
- **Parameters**: 
  - `bot_name` (optional): Bot name to sync. If omitted, shows interactive dropdown
  - `flags` (optional): Additional flags
    - `--dry-run` or `-n`: Preview changes without syncing
    - `--delete` or `-d`: Delete remote files not present locally
- **Usage**: `!botsync rsadminbot` or `!botsync rsadminbot --dry-run`
- **Admin Only**: Yes
- **Returns**: Sync status and file changes

### System Information Commands

#### `!details`
- **Description**: Show systemd details for a bot
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name. If omitted, shows interactive dropdown
- **Usage**: `!details` or `!details rsadminbot`
- **Admin Only**: Yes
- **Returns**: systemctl status output

#### `!logs`
- **Description**: Show journal logs for a bot
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name. If omitted, shows interactive dropdown
  - `lines` (optional): Number of lines to show (default: 80, max: 400)
- **Usage**: `!logs rsadminbot` or `!logs rsadminbot 100`
- **Admin Only**: Yes
- **Returns**: journalctl output

#### `!whereami`
- **Description**: Print runtime environment details (canonical runtime proof)
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!whereami`
- **Admin Only**: Yes
- **Returns**: cwd, file path, OS, Python path/version, local_exec status, live_root, git commit hashes

#### `!commands`
- **Description**: List commands for a specific RS bot (reads `{BotFolder}/COMMANDS.md`) or show a summary for all bots
- **Aliases**: `listcommands`, `cmds`, `helpcommands`
- **Parameters**:
  - `bot_name` (optional): Bot key (e.g., `rsadminbot`, `rscheckerbot`). If omitted, shows all bots and command counts.
- **Usage**: `!commands` or `!commands rscheckerbot`
- **Admin Only**: Yes
- **Returns**: Markdown command reference (split across multiple messages if long) or a summary embed

### Extended Admin Commands

#### `!selfupdate`
- **Description**: Update RSAdminBot from GitHub (python-only) then restart rsadminbot; also syncs repo-level shared utilities
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!selfupdate`
- **Admin Only**: Yes
- **Returns**: Git commit info, files changed, restart status

#### `!oraclefilesupdate`
- **Description**: Push a bots-only snapshot of the live Ubuntu RS bot folders to neo-rs/oraclefiles
- **Aliases**: `oraclefilespush`, `oraclepush`
- **Parameters**: None
- **Usage**: `!oraclefilesupdate`
- **Admin Only**: Yes
- **Returns**: Push status, changed files sample

#### `!syncstatus`
- **Description**: Compare rsbots-code (GitHub checkout) vs live tree and report which RS bots are outdated
- **Aliases**: `outdated`, `codestatus`
- **Parameters**: None
- **Usage**: `!syncstatus`
- **Admin Only**: Yes
- **Returns**: Git HEAD info, folder-by-folder diff status

#### `!systemcheck`
- **Description**: Report runtime mode + core Ubuntu health stats
- **Aliases**: `systemstatus`
- **Parameters**: None
- **Usage**: `!systemcheck`
- **Admin Only**: Yes
- **Returns**: OS info, local_exec status, SSH config, CPU/load, memory, disk, journald, file sizes

#### `!secretsstatus`
- **Description**: Show which RS bots are missing config.secrets.json or required keys
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to check. If omitted, checks all RS bots
- **Usage**: `!secretsstatus` or `!secretsstatus rsadminbot`
- **Admin Only**: Yes
- **Returns**: Secrets validation status for each bot

#### `!rspids`
- **Description**: Print RS bot service state + PID list
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!rspids`
- **Admin Only**: Yes
- **Returns**: Service state and PID for all RS bots

#### `!moneyflowcheck`
- **Description**: Run a production-safe health check for the money-flow bots (RSOnboarding + RSCheckerbot)
- **Aliases**: `moneyflow`, `mfc`
- **Parameters**: None
- **Usage**: `!moneyflowcheck`
- **Admin Only**: Yes
- **Returns**: Systemd status, venv check, config validation, runtime JSON sanity checks

#### `!codehash`
- **Description**: Show sha256 hashes of key bot files on Ubuntu for quick 'what code is running' proof
- **Aliases**: None
- **Parameters**: 
  - `bot_name`: Bot name (must be `rsonboarding`, `rscheckerbot`, or `all`)
- **Usage**: `!codehash rsonboarding` or `!codehash all`
- **Admin Only**: Yes
- **Returns**: SHA256 hashes of main bot files

#### `!fileview`
- **Description**: Show size + last-modified time for .py and config/message json files
- **Aliases**: None
- **Parameters**: 
  - `bot_name`: Bot folder name
  - `mode` (optional): `alljson` to include all JSON files
- **Usage**: `!fileview rsadminbot` or `!fileview rscheckerbot alljson`
- **Admin Only**: Yes
- **Returns**: File list with sizes and modification times

#### `!deploy`
- **Description**: Deploy a server-side uploaded archive, refresh venv + systemd units, and restart bots
- **Aliases**: None
- **Parameters**: 
  - `archive_path`: Path to archive file on Ubuntu server
- **Usage**: `!deploy /tmp/mirror-world.tar.gz`
- **Admin Only**: Yes
- **Returns**: Deploy status, restart results

#### `!ssh`
- **Description**: Execute SSH command
- **Aliases**: None
- **Parameters**: 
  - `command`: SSH command to execute
- **Usage**: `!ssh uptime`
- **Admin Only**: Yes
- **Returns**: Command output

#### `!botinfo`
- **Description**: Get detailed information about a bot
- **Aliases**: None
- **Parameters**: 
  - `bot_name`: Bot name (RS bots only)
- **Usage**: `!botinfo rsadminbot`
- **Admin Only**: Yes
- **Returns**: Folder, script, service, size, files, health score, last modified, dependencies, config info

#### `!botconfig`
- **Description**: Get config.json for a bot in user-friendly format
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name (RS bots only). If omitted, shows interactive dropdown
- **Usage**: `!botconfig` or `!botconfig rsadminbot`
- **Admin Only**: Yes
- **Returns**: Formatted config.json display

### Whop Tracking Commands

#### `!whopscan`
- **Description**: Scan whop logs channel for membership events
- **Aliases**: None
- **Parameters**: 
  - `limit` (optional): Maximum messages to scan (default: 2000)
  - `days` (optional): Lookback days (default: 30)
- **Usage**: `!whopscan` or `!whopscan 5000 60`
- **Admin Only**: Yes
- **Returns**: Scan progress and results

#### `!whopstats`
- **Description**: Get membership statistics
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!whopstats`
- **Admin Only**: Yes
- **Returns**: Total members, active memberships, event breakdown

#### `!whophistory`
- **Description**: Get user's membership history
- **Aliases**: None
- **Parameters**: 
  - `discord_id`: Discord user ID
- **Usage**: `!whophistory 123456789012345678`
- **Admin Only**: Yes
- **Returns**: User's membership events and timeline

### Oracle Data Commands

#### `!oracledatasync`
- **Description**: Sync runtime data from Oracle server to local
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!oracledatasync`
- **Admin Only**: Yes
- **Returns**: Sync status and downloaded files

#### `!oracledataanalyze`
- **Description**: Analyze downloaded Oracle server runtime data
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!oracledataanalyze`
- **Admin Only**: Yes
- **Returns**: Analysis report

#### `!oracledatadoc`
- **Description**: Generate documentation report of ask mode changes
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!oracledatadoc`
- **Admin Only**: Yes
- **Returns**: Documentation report path

#### `!oracledatasample`
- **Description**: Generate sample embed outputs from scanned data
- **Aliases**: None
- **Parameters**: 
  - `post` (optional): `yes`/`y`/`true`/`1`/`post` to post embeds to channel (default: `no`)
  - `event_type` (optional): Filter by event type: `all`, `new`, `renewal`, `cancellation`, `completed` (default: `all`)
- **Usage**: `!oracledatasample` or `!oracledatasample post cancellation`
- **Admin Only**: Yes
- **Returns**: Sample embed generation status, optionally posts embeds

### Bot Movement Tracking Commands

#### `!botmovements`
- **Description**: Show bot's activity log
- **Aliases**: None
- **Parameters**: 
  - `bot_name`: Bot name (RS bots only)
  - `limit` (optional): Maximum movements to show (default: 50)
- **Usage**: `!botmovements rsadminbot` or `!botmovements rsadminbot 100`
- **Admin Only**: Yes
- **Returns**: Activity statistics and recent movements

### Test Server Commands

#### `!setupmonitoring`
- **Description**: Initialize test server categories/channels for monitoring
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!setupmonitoring`
- **Admin Only**: Yes
- **Returns**: Created channels and category info

#### `!testcards`
- **Description**: Post RSCheckerbot sample staff cards into TestCenter channels + write a JSON trace artifact
- **Aliases**: `testcenter_cards`, `tcards`
- **Parameters**: 
  - `member` (optional): Discord member to use for test cards. If omitted, uses command author
- **Usage**: `!testcards` or `!testcards @SomeMember`
- **Admin Only**: Yes
- **Returns**: Posted cards info and JSON artifact file

### Channel Management Commands

#### `!delete`
- **Description**: Delete Discord channels
- **Aliases**: `d`
- **Parameters**: 
  - `channel_mentions`: One or more channel mentions
- **Usage**: `!delete #channel1 #channel2`
- **Admin Only**: Yes
- **Returns**: Deletion confirmation

#### `!transfer`
- **Description**: Move channel to category
- **Aliases**: `t`
- **Parameters**: 
  - `channel_mention`: Channel to move
  - `category_mention`: Target category
- **Usage**: `!transfer #channel #category`
- **Admin Only**: Yes
- **Returns**: Transfer confirmation

#### `!add`
- **Description**: Create channel in category
- **Aliases**: `a`
- **Parameters**: 
  - `channel_mention`: Channel mention (optional)
  - `category_mention`: Target category
- **Usage**: `!add #category`
- **Admin Only**: Yes
- **Returns**: Channel creation confirmation

### Diagnostic Commands

#### `!botdiagnose`
- **Description**: Diagnose bot issues
- **Aliases**: None
- **Parameters**: 
  - `bot_name` (optional): Bot name to diagnose. If omitted, shows interactive dropdown
- **Usage**: `!botdiagnose` or `!botdiagnose rsadminbot`
- **Admin Only**: Yes
- **Returns**: Diagnostic information

#### `!runallcommands`
- **Description**: Run all commands for all bots (comprehensive test)
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!runallcommands`
- **Admin Only**: Yes
- **Returns**: Execution results for all commands

## Command Summary

- **Total Commands**: 44
- **Admin Commands**: 43
- **Public Commands**: 1 (`!ping`)
- **Commands with Aliases**: 9
- **Interactive Commands**: 8 (use dropdown menus when bot_name omitted)

## Notes

- All commands use the `!` prefix
- Most commands require admin permissions (checked via `@commands.check(lambda ctx: self.is_admin(ctx.author))`)
- Bot names are case-insensitive
- RS bots are: rsadminbot, rsforwarder, rsonboarding, rsmentionpinger, rscheckerbot, rssuccessbot
- Non-RS bots can use basic management commands (`!status`, `!start`, `!stop`, `!restart`) but not advanced features
- Commands that accept `bot_name` without a value will show an interactive dropdown menu
- File paths use canonical bot registry (`self.BOTS` dictionary)
- All errors are explicitly reported (no silent fallbacks)
