# RSForwarder Commands Reference

## Overview
RSForwarder is a standalone bot for forwarding messages from RS Server channels to webhooks. All messages are branded with "Reselling Secrets" name and avatar from RS Server.

## Command Categories

### Channel Management Commands

#### `!rsadd`
- **Description**: Add a new source channel to destination mapping
- **Aliases**: `add`
- **Parameters**: 
  - `source_channel`: Discord channel mention or channel ID (required)
  - `destination_webhook_url`: Discord webhook URL (required)
  - `role_id`: Role ID to mention (optional)
  - `text`: Text to include with role mention (optional)
- **Usage**: `!rsadd #personal-deals <WEBHOOK_URL> 886824827745337374 "leads found!"`
- **Admin Only**: No (but requires channel access)
- **Returns**: Confirmation embed with channel and webhook details

#### `!rslist`
- **Description**: List all configured channels
- **Aliases**: `list`
- **Parameters**: None
- **Usage**: `!rslist`
- **Admin Only**: No
- **Returns**: Embed listing all forwarding jobs with status

#### `!rsupdate`
- **Description**: Update an existing forwarding job
- **Aliases**: `update`
- **Parameters**: 
  - `source_channel`: Discord channel mention or channel ID (required)
  - `destination_webhook_url`: Webhook URL (optional)
  - `role_id`: Role ID (optional)
  - `text`: Text for role mention (optional)
- **Usage**: `!rsupdate #personal-deals <WEBHOOK_URL> 886824827745337374 "new text"`
- **Admin Only**: No (but requires channel access)
- **Returns**: Updated configuration confirmation

#### `!rsview`
- **Description**: View details of a specific forwarding job
- **Aliases**: `view`
- **Parameters**: 
  - `source_channel`: Discord channel mention or channel ID (required)
- **Usage**: `!rsview #personal-deals`
- **Admin Only**: No (but requires channel access)
- **Returns**: Detailed embed with channel, webhook, and role mention info

#### `!rsremove`
- **Description**: Remove a channel configuration
- **Aliases**: `remove`
- **Parameters**: 
  - `source_channel`: Discord channel mention or channel ID (required)
- **Usage**: `!rsremove #personal-deals`
- **Admin Only**: No (but requires channel access)
- **Returns**: Removal confirmation

### Testing Commands

#### `!rstest`
- **Description**: Test forwarding by forwarding recent messages from a source channel
- **Aliases**: `test`
- **Parameters**: 
  - `source_channel_id`: Channel ID to test (optional, defaults to current channel)
  - `limit`: Number of messages to forward (optional, default: 1)
- **Usage**: `!rstest` or `!rstest 1446174861931188387 5`
- **Admin Only**: No (but requires channel access)
- **Returns**: Forwarding results

#### `!rstestall`
- **Description**: Test forwarding for ALL configured channels by forwarding the most recent message(s) from each into a test channel via an auto-created webhook
- **Aliases**: `testall`
- **Parameters**:
  - `test_channel_id`: Test channel ID (optional; defaults to `1446372213757313034`)
  - `limit`: Number of messages per channel (optional; default: 1; max: 5)
- **Usage**: `!rstestall` or `!rstestall 1446372213757313034 1`
- **Admin Only**: Yes (can spam test channel)
- **Returns**: Summary counts (ok/fail)

### Status Commands

#### `!rsstatus`
- **Description**: Show bot status and configuration
- **Aliases**: `status`
- **Parameters**: None
- **Usage**: `!rsstatus`
- **Admin Only**: No
- **Returns**: Bot status, RS Server connection, icon status, statistics, configured channels

### Administrative Commands

#### `!rsfetchicon`
- **Description**: Manually fetch RS Server icon
- **Aliases**: `fetchicon`
- **Parameters**: None
- **Usage**: `!rsfetchicon`
- **Admin Only**: No
- **Returns**: Icon fetch status

#### `!rsstartadminbot`
- **Description**: Start RSAdminBot remotely on the server
- **Aliases**: `startadminbot`, `startadmin`
- **Parameters**: None
- **Usage**: `!rsstartadminbot`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Start status

#### `!rsrestartadminbot`
- **Description**: Restart RSAdminBot remotely on the server
- **Aliases**: `restartadminbot`, `restartadmin`, `restart`
- **Parameters**: None
- **Usage**: `!rsrestartadminbot`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Restart status

### Help Commands

#### `!rscommands`
- **Description**: Show available commands
- **Aliases**: `commands`
- **Parameters**: None
- **Usage**: `!rscommands`
- **Admin Only**: No
- **Returns**: Help embed with all commands

## Command Summary

- **Total Commands**: 13
- **Admin Commands**: 2 (`!rsstartadminbot`, `!rsrestartadminbot`)
- **Public Commands**: 10
- **Commands with Aliases**: 10
- **Command Prefix**: `!rs` (unique prefix to avoid conflicts)

## Notes

- All commands use the `!rs` prefix (or aliases without prefix)
- Commands that manage channels require access to the source channel
- Webhook URLs must be valid Discord webhook URLs (format: `https://discord.com/api/webhooks/...`)
- Role IDs must be valid numeric Discord role IDs
- The bot automatically brands forwarded messages with RS Server name and avatar
- Bot messages from RSForwarder itself are skipped to avoid loops
- RSAdminBot control commands require administrator permissions
- Local-exec mode is preferred when RSForwarder runs on Ubuntu host (no SSH key needed)
