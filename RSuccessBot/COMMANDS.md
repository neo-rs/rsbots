# RSSuccessBot Commands Reference

## Overview
RSSuccessBot manages the success points system for Reselling Secrets. Members earn points by sharing verified success images in designated channels. Points can be redeemed for membership time and other rewards.

## Command Categories

### Points Management Commands (Admin)

#### `!addpoints`
- **Description**: Add points to a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
  - `amount`: Number of points to add (required, must be > 0)
- **Usage**: `!addpoints @member 10`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed with new total

#### `!removepoints`
- **Description**: Remove points from a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
  - `amount`: Number of points to remove (required, must be > 0)
- **Usage**: `!removepoints @member 5`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed with new total

#### `!checkpoints`
- **Description**: Check points for a specific user
- **Aliases**: `userpoints`, `points`
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `!checkpoints @member`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: User's current points

#### `!setpoints`
- **Description**: Set exact points for a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
  - `amount`: Exact points value (required, must be >= 0)
- **Usage**: `!setpoints @member 100`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed showing old and new totals

### Slash Commands (Public)

#### `/rspoints`
- **Description**: Check your current success points
- **Aliases**: None
- **Parameters**: None
- **Usage**: `/rspoints`
- **Admin Only**: No
- **Returns**: Private embed with user's current points

#### `/rsleaderboard`
- **Description**: View the top 10 members by success points
- **Aliases**: None
- **Parameters**: None
- **Usage**: `/rsleaderboard`
- **Admin Only**: No
- **Returns**: Public embed with leaderboard

#### `/rshelp`
- **Description**: Learn how the success points system works
- **Aliases**: None
- **Parameters**: None
- **Usage**: `/rshelp`
- **Admin Only**: No
- **Returns**: Private embed with system explanation

#### `/rsredeeminfo`
- **Description**: Learn about redeeming your success points
- **Aliases**: None
- **Parameters**: None
- **Usage**: `/rsredeeminfo`
- **Admin Only**: No
- **Returns**: Private embed with redemption tiers and interactive buttons

### Configuration Commands (Admin)

#### `!status`
- **Description**: Show bot status
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!status`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Bot status, JSON connection, statistics, uptime

#### `!reload`
- **Description**: Reload config and messages from files
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!reload`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Reload confirmation or error list

#### `!sync`
- **Description**: Manually sync slash commands
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!sync`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Sync status with command counts

#### `!configinfo`
- **Description**: Show configuration information
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!configinfo`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Guild info, success channels, watch role, storage, log channel

### Channel Setup Commands (Admin)

#### `!listsuccesschannels`
- **Description**: List configured success channels
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!listsuccesschannels`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Calls `!configinfo` to show channels

#### `!addsuccesschannel`
- **Description**: Add a channel to success_channel_ids
- **Aliases**: None
- **Parameters**: 
  - `channel`: Discord text channel mention (required)
- **Usage**: `!addsuccesschannel #success-stories`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation with total channel count

#### `!removesuccesschannel`
- **Description**: Remove a channel from success_channel_ids
- **Aliases**: None
- **Parameters**: 
  - `channel`: Discord text channel mention (required)
- **Usage**: `!removesuccesschannel #success-stories`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation with total channel count

#### `!setredemptioncategory`
- **Description**: Set the category for redemption tickets
- **Aliases**: None
- **Parameters**: 
  - `category`: Discord category channel mention (required)
- **Usage**: `!setredemptioncategory #Redemption Tickets`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed

#### `!setsupportrole`
- **Description**: Set the support role to ping in redemption tickets
- **Aliases**: None
- **Parameters**: 
  - `role`: Discord role mention (required)
- **Usage**: `!setsupportrole @Support`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed

### Tier Management Commands (Admin)

#### `!edittiers`
- **Description**: Open interactive tier editor
- **Aliases**: `tiereditor`, `tiers`
- **Parameters**: None
- **Usage**: `!edittiers`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Interactive tier editor interface

#### `!editmessages`
- **Description**: Open interactive message editor
- **Aliases**: `messageeditor`, `messages`
- **Parameters**: None
- **Usage**: `!editmessages`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Interactive message editor interface

#### `!showredemptiontiers`
- **Description**: Show current redemption tiers
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!showredemptiontiers`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: List of all tiers with points and descriptions

#### `!addtier`
- **Description**: Add a new redemption tier
- **Aliases**: None
- **Parameters**: 
  - `name`: Tier name (required)
  - `points`: Points required (required, must be >= 1)
  - `description`: Tier description (optional, default: "No description")
- **Usage**: `!addtier "1 Month Free" 100 "Get 1 month of free membership"`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed with tier details

#### `!removetier`
- **Description**: Remove a redemption tier by name
- **Aliases**: None
- **Parameters**: 
  - `tier_name`: Tier name to remove (required)
- **Usage**: `!removetier "1 Month Free"`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed

#### `!edittier`
- **Description**: Edit a redemption tier
- **Aliases**: None
- **Parameters**: 
  - `tier_name`: Tier name to edit (required)
  - `field`: Field to edit: `name`, `points`, or `description` (required)
  - `new_value`: New value for the field (required)
- **Usage**: `!edittier "1 Month Free" points 150` or `!edittier "1 Month Free" description "Updated description"`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation embed showing old and new values

### Data Operations Commands (Admin)

#### `!scanhistory`
- **Description**: Scan message history to extract points from bot messages
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!scanhistory`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Scan progress, results summary, points_history.txt file

#### `!importhistory`
- **Description**: Import points from the points_history.txt file into JSON
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!importhistory`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Import statistics (new users, updated users, errors)

#### `!postpointsguide`
- **Description**: Post the points guide to a channel
- **Aliases**: None
- **Parameters**: 
  - `channel`: Discord text channel mention (optional, defaults to current channel)
- **Usage**: `!postpointsguide` or `!postpointsguide #announcements`
- **Admin Only**: Yes (requires manage_messages permission)
- **Returns**: Confirmation with guide posted to channel

## Command Summary

- **Total Commands**: 27 (23 prefix + 4 slash)
- **Admin Commands**: 23 (all prefix commands)
- **Public Commands**: 4 (all slash commands)
- **Commands with Aliases**: 4
- **Command Prefix**: `!` for prefix commands, `/` for slash commands

## Notes

- Prefix commands require `manage_messages` permission
- Slash commands are available to all members
- Command messages are automatically deleted for admin commands
- Points are stored in `success_points.json` (JSON-only, no databases)
- Duplicate images are automatically detected and prevented
- Points are awarded automatically when valid success images are posted in success channels
- Redemption requires staff approval (points are not auto-deducted)
- All point movements are logged to a points log channel if configured
- Tier names must be unique (case-insensitive check)
