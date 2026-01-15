# RSCheckerbot Commands Reference

## Overview
RSCheckerbot manages member verification, payment tracking, and DM sequences for Reselling Secrets. It handles Whop API integration, membership status tracking, and automated DM sequences for members.

## Command Categories

### Content Management Commands

#### `!editmessages`
- **Description**: Edit DM messages via embedded interface
- **Aliases**: `checker-edit`, `cedit`, `checker-messages`
- **Parameters**: None
- **Usage**: `!editmessages`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Interactive message editor interface
- **Note**: Command message is auto-deleted

#### `!reloadmessages`
- **Description**: Reload messages from JSON file
- **Aliases**: `checker-reload`, `creload`
- **Parameters**: None
- **Usage**: `!reloadmessages`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

### Data Operations Commands

#### `!cleanup`
- **Description**: Manually trigger data cleanup
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!cleanup`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

#### `!purgecases`
- **Description**: Delete legacy per-user payment case channels under the configured category
- **Aliases**: `purgecasechannels`, `deletecases`, `deletecasechannels`
- **Parameters**: 
  - `confirm`: Must be exactly `confirm` to proceed (required)
- **Usage**: `.checker purgecases confirm`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Deletion summary (deleted count, skipped count, failed count)
- **Note**: Command message is auto-deleted

### Direct Message Controls

#### `!dmenable`
- **Description**: Enable DM sequence
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!dmenable`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

#### `!dmdisable`
- **Description**: Disable DM sequence
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!dmdisable`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

#### `!dmstatus`
- **Description**: Show DM sequence status
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!dmstatus`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Status message showing ENABLED or DISABLED (auto-deletes after 10 seconds)
- **Note**: Command message is auto-deleted

### Member Operations Commands

#### `!whois`
- **Description**: Whop API-first lookup for a Discord user
- **Aliases**: `whof`
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker whois @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Embed with member info, access roles, Whop membership summary
- **Note**: Command message is auto-deleted after 30 seconds

#### `!whopmembership`
- **Description**: Direct Whop membership lookup by membership_id
- **Aliases**: `whopmember`, `whopmem`
- **Parameters**: 
  - `membership_id`: Whop membership ID (required, format: `mem_...`)
- **Usage**: `.checker whopmembership mem_abc123...`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Embed with membership details (product, status, member since, renewal dates, payment info)
- **Note**: Command message is auto-deleted after 30 seconds

### Sequence Management Commands

#### `!start`
- **Description**: Start checker sequence for a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker start @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation that day_1 is queued
- **Note**: Only works if user has trigger role and hasn't had sequence before

#### `!cancel`
- **Description**: Cancel checker sequence for a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker cancel @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation that sequence is cancelled
- **Note**: Only works if user is in active queue

#### `!test`
- **Description**: Test checker sequence for a member (sends all day messages immediately)
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker test @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Test completion confirmation
- **Note**: Sends all day messages (day_1 through day_7b) with TEST_INTERVAL_SECONDS delay between each

#### `!relocate`
- **Description**: Relocate sequence to different day
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
  - `day`: Day identifier - can be:
    - Number (1-6): `1`, `2`, `3`, `4`, `5`, `6`
    - Special days: `7a`, `7b`
    - Full format: `day_1`, `day_2`, etc.
- **Usage**: `.checker relocate @user 3` or `.checker relocate @user day_7a`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation that member is relocated and will receive message in ~5 seconds

## Command Summary

- **Total Commands**: 12
- **Admin Commands**: 12 (all commands require administrator permissions)
- **Public Commands**: 0
- **Commands with Aliases**: 5
- **Command Prefix**: `.checker` (dot prefix)

## Notes

- All commands use the `.checker` prefix
- All commands require administrator permissions
- Command messages are automatically deleted
- Replies auto-delete after 5-30 seconds depending on command
- Data is stored in JSON files:
  - `queue.json` - Active DM sequence queue
  - `registry.json` - Member registry
  - `invites.json` - Invite tracking
- DM sequence is controlled by `dm_sequence_enabled` setting
- Sequence days: day_1, day_2, day_3, day_4, day_5, day_6, day_7a, day_7b
- Whop API integration requires valid API key and company ID in config
- Membership lookups use cached membership IDs when available
- Legacy case channels are identified by topic containing `rschecker_payment_case` or name starting with `pay-`
- Cleanup removes old data from registry and invites based on configured retention periods
