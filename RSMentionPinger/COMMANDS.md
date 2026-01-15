# RSMentionPinger Commands Reference

## Overview
RSMentionPinger monitors Discord channels for mentions of specific roles and sends alerts to configured log channels. It helps track when important roles are mentioned across the server.

## Command Categories

### Basic Commands

#### `!reload`
- **Description**: Reload config from file
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!reload`
- **Admin Only**: No
- **Returns**: Confirmation message (auto-deletes after 5 seconds)

#### `!status`
- **Description**: Show bot status
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!status`
- **Admin Only**: No
- **Returns**: Bot status embed with connection info, guild status, watched roles count, excluded categories count (auto-deletes after 30 seconds)

#### `!configinfo`
- **Description**: Show what all the IDs in config.json actually represent
- **Aliases**: `config`, `ids`, `showids`
- **Parameters**: None
- **Usage**: `!configinfo`
- **Admin Only**: No
- **Returns**: Detailed embed showing:
  - Guild name and ID
  - Log channel name, mention, and ID
  - Watched roles (name and ID for each)
  - Excluded categories (name and ID for each)
  (Auto-deletes after 60 seconds)

#### `!help`
- **Description**: Show all available commands with detailed explanations
- **Aliases**: `commands`, `h`, `helpme`
- **Parameters**: None
- **Usage**: `!help`
- **Admin Only**: No
- **Returns**: Comprehensive help embed with:
  - Quick command reference
  - Command aliases
  - Detailed command descriptions
  (Does not auto-delete)

## Command Summary

- **Total Commands**: 5
- **Admin Commands**: 0
- **Public Commands**: 5
- **Commands with Aliases**: 2
- **Command Prefix**: `!`

## Notes

- All commands use the `!` prefix
- Commands auto-delete the user's command message when possible
- Replies are visible to everyone (not ephemeral)
- Most command replies auto-delete after a short time (5-60 seconds)
- Help command does not auto-delete
- Bot monitors channels for mentions of watched roles
- Mentions in excluded categories are ignored
- Alerts are sent to the configured log channel
- Configuration is stored in `config.json`
- Bot requires `message_content` intent to read messages
