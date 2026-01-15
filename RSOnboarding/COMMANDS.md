# RSOnboarding Commands Reference

## Overview
RSOnboarding manages the onboarding ticket system for new members. It automatically creates private ticket channels when members join and guides them through the onboarding process.

## Command Categories

### Configuration Commands

#### `!editmessages`
- **Description**: Edit all bot messages via embedded interface
- **Aliases**: `edit`, `emsg`
- **Parameters**: None
- **Usage**: `!editmessages`
- **Admin Only**: No (but typically used by admins)
- **Returns**: Interactive message editor interface
- **Note**: Command message is auto-deleted

#### `!editconfig`
- **Description**: Edit configuration via embedded interface
- **Aliases**: `econfig`, `config`
- **Parameters**: None
- **Usage**: `!editconfig`
- **Admin Only**: No (but typically used by admins)
- **Returns**: Interactive config editor interface
- **Note**: Command message is auto-deleted

#### `!reload`
- **Description**: Reload config and messages from files
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!reload`
- **Admin Only**: No (but typically used by admins)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)

### Ticket Management Commands

#### `!cleanup`
- **Description**: Manually trigger cleanup of stale tickets
- **Aliases**: `clean`, `cleanstale`
- **Parameters**: None
- **Usage**: `!cleanup`
- **Admin Only**: No (but typically used by admins)
- **Returns**: Cleanup status message (auto-deletes after 10 seconds)
- **Note**: Command message is auto-deleted

#### `!test`
- **Description**: Manually trigger ticket creation for testing
- **Aliases**: `openticket`, `testticket`
- **Parameters**: 
  - `args` (optional): Arguments string
    - Can include member mention: `@user`
    - Can include `force` flag: `@user force` or `force`
- **Usage**: 
  - `!test` - Create ticket for yourself
  - `!test @user` - Create ticket for specific user
  - `!test @user force` - Force create ticket even if they have Member role or existing ticket
- **Admin Only**: No (but typically used by admins)
- **Returns**: Ticket creation status with channel mention and jump link
- **Note**: Command message is auto-deleted

#### `!clearticket`
- **Description**: Manually close/clear a ticket for testing
- **Aliases**: `closeticket`, `removeticket`
- **Parameters**: 
  - `member`: Discord member mention (optional, defaults to command author)
- **Usage**: 
  - `!clearticket` - Clear your own ticket
  - `!clearticket @user` - Clear user's ticket
- **Admin Only**: No (but typically used by admins)
- **Returns**: Confirmation message (auto-deletes after 10 seconds)
- **Note**: Command message is auto-deleted

### Status Commands

#### `!status`
- **Description**: Show bot status
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!status`
- **Admin Only**: No (but typically used by admins)
- **Returns**: Status embed with bot info, guild connection, active ticket count
- **Note**: Command message is auto-deleted

#### `!configinfo`
- **Description**: Show what all the IDs in config.json actually represent
- **Aliases**: `ids`, `showids`
- **Parameters**: None
- **Usage**: `!configinfo`
- **Admin Only**: No (but typically used by admins)
- **Returns**: Detailed embed showing:
  - Guild name and ID
  - Welcome role name and ID
  - Member role name and ID
  - Cleanup roles (list with names and IDs)
  - Ticket category name and ID
  - Overflow category name and ID
  - Log channel name, mention, and ID
  - Welcome log channel name, mention, and ID
  - Staff user mention, name, and ID
  - Alert user mention, name, and ID
- **Note**: Command message is auto-deleted

### Help Commands

#### `!onboardhelp`
- **Description**: Show all available commands with detailed explanations
- **Aliases**: `commands`, `h`, `helpme`
- **Parameters**: None
- **Usage**: `!onboardhelp`
- **Admin Only**: No
- **Returns**: Comprehensive help embed with:
  - Quick command reference
  - Utility commands
  - Command aliases
  - Detailed command descriptions for:
    - Message editing
    - Config editing
    - Ticket testing
    - Status and info commands
- **Note**: Command message is auto-deleted

## Command Summary

- **Total Commands**: 10
- **Admin Commands**: 0 (but most commands are typically used by admins)
- **Public Commands**: 10
- **Commands with Aliases**: 6
- **Command Prefix**: `?` (question mark)

## Notes

- All commands use the `?` prefix
- Most command messages are automatically deleted
- Replies may auto-delete after a short time (Discord text commands cannot be truly ephemeral)
- Tickets are stored in `tickets.json` (JSON-only, no databases)
- Active tickets are tracked per user ID
- Cleanup automatically removes stale tickets (channels that no longer exist)
- Force mode bypasses checks for existing tickets and Member role
- Configuration can be edited via interactive interface (`!editconfig`)
- Messages can be edited via interactive interface (`!editmessages`)
- Bot automatically creates tickets when members join (if they don't have Member role)
- Tickets are created in the configured ticket category
- Overflow category is used when ticket category is full
