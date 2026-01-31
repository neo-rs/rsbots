# RSCheckerbot Commands Reference

## Overview
RSCheckerbot manages member verification, payment tracking, and DM sequences for Reselling Secrets. It handles Whop API integration, membership status tracking, and automated DM sequences for members.

## Command Categories

### Content Management Commands

#### `.checker editmessages`
- **Description**: Edit DM messages via embedded interface
- **Aliases**: `editmessage`, `checker-edit`, `cedit`, `checker-messages`
- **Parameters**: None
- **Usage**: `.checker editmessages`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Interactive message editor interface
- **Note**: Command message is auto-deleted

#### `.checker reloadmessages`
- **Description**: Reload messages from JSON file
- **Aliases**: `checker-reload`, `creload`
- **Parameters**: None
- **Usage**: `.checker reloadmessages`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

### Data Operations Commands

#### `.checker cleanup`
- **Description**: Manually trigger data cleanup
- **Aliases**: None
- **Parameters**: None
- **Usage**: `.checker cleanup`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

#### `.checker syncsummary`
- **Description**: DM a boss-friendly report to the invoker. With **no dates**, it re-sends the latest **Whop Sync Summary + CSV** (from `#whop-sync-summary` mirror). With **dates**, it generates a **Whop memberships joined report** filtered by **Whop “Joined at”** (membership `created_at`, with `date_joined` fallback) and DMs an embed + CSV.
- **Aliases**: `whopsync`, `whopsyncsummary`, `sync-report`
- **Parameters**:
  - `start` (optional): Start date (accepts `YYYY-MM-DD`, `MM-DD-YY`, `MM/DD`, `MM-DD`)
  - `end` (optional): End date (same formats). If omitted, uses `start`
- **Usage**:
  - `.checker syncsummary` (labels as today)
  - `.checker syncsummary 01-30-26`
  - `.checker syncsummary 01/30`
  - `.checker syncsummary 01-01-26 01-30-26`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**:
  - No dates: DM to the invoker containing the embed + `whop-sync-report_<label>.csv` (if available)
  - With dates: DM to the invoker containing the embed + `whop-joined-report_<label>.csv`
- **Note**: Date filtering uses day boundaries in `reporting.timezone`

#### `.checker canceling`
- **Description**: Manually run the Whop **canceling snapshot** (same as startup) and post the results into Neo Test Server `#set-to-cancel`.
- **Aliases**: `cancelling`, `set-to-cancel`, `settocancel`
- **Parameters**: None
- **Usage**: `.checker canceling`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Posts snapshot embeds into Neo `#set-to-cancel` and sends a short confirmation message (auto-deletes)
- **Notes**:
  - Snapshot output channel is controlled by `config.json -> reporting.cancel_reminders_output_guild_id` + `reporting.cancel_reminders_output_channel_name`
  - Optional clear-before-post behavior is controlled by `reporting.startup_canceling_snapshot_clear_channel`

#### `.checker purgecases`
- **Description**: Delete legacy per-user payment case channels under the configured category
- **Aliases**: `purgecasechannels`, `deletecases`, `deletecasechannels`
- **Parameters**: 
  - `confirm`: Must be exactly `confirm` to proceed (required)
- **Usage**: `.checker purgecases confirm`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Deletion summary (deleted count, skipped count, failed count)
- **Note**: Command message is auto-deleted

#### `.checker futurememberaudit`
- **Description**: Scan Discord members missing the Member role and (after an explicit confirmation in `member-status-logs`) add the Future Member role
- **Aliases**: `futureaudit`, `auditfuture`
- **Parameters**: None
- **Usage**: `.checker futurememberaudit`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Posts a preview + Confirm/Cancel buttons in `member-status-logs`, then (on Confirm) posts progress + a final summary
- **Notes**:
  - Role IDs come from `config.json` (`dm_sequence.role_cancel_a` = Member role, `dm_sequence.role_to_assign` = Future Member role)
  - Staff/admins are skipped (administrator/manage-guild/manage-roles permissions) and members with any role listed in `config.json -> future_member_audit.exclude_role_ids`
  - Sample list includes clickable member mentions (WILL ping) and also shows plain `@username` text
  - No roles are ever mentioned (no `<@&...>`); output is plain text names + IDs

### Direct Message Controls

#### `.checker dmenable`
- **Description**: Enable DM sequence
- **Aliases**: None
- **Parameters**: None
- **Usage**: `.checker dmenable`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

#### `.checker dmdisable`
- **Description**: Disable DM sequence
- **Aliases**: None
- **Parameters**: None
- **Usage**: `.checker dmdisable`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation message (auto-deletes after 5 seconds)
- **Note**: Command message is auto-deleted

#### `.checker dmstatus`
- **Description**: Show DM sequence status
- **Aliases**: None
- **Parameters**: None
- **Usage**: `.checker dmstatus`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Status message showing ENABLED or DISABLED (auto-deletes after 10 seconds)
- **Note**: Command message is auto-deleted

### Member Operations Commands

#### `.checker whois`
- **Description**: Whop API-first lookup for a Discord user
- **Aliases**: `whof`
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker whois @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Embed with member info, access roles, Whop membership summary
- **Note**: Command message is auto-deleted after 30 seconds

#### `.checker whopmembership`
- **Description**: Direct Whop membership lookup by membership_id
- **Aliases**: `whopmember`, `whopmem`
- **Parameters**: 
  - `membership_id`: Whop membership ID (required, format: `mem_...`)
- **Usage**: `.checker whopmembership mem_abc123...`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Embed with membership details (product, status, member since, renewal dates, payment info)
- **Note**: Command message is auto-deleted after 30 seconds

### Sequence Management Commands

#### `.checker start`
- **Description**: Start checker sequence for a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker start @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation that day_1 is queued
- **Note**: Only works if user has trigger role and hasn't had sequence before

#### `.checker cancel`
- **Description**: Cancel checker sequence for a member
- **Aliases**: None
- **Parameters**: 
  - `member`: Discord member mention (required)
- **Usage**: `.checker cancel @user`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Confirmation that sequence is cancelled
- **Note**: Only works if user is in active queue

### Support Ticket Commands (Ticket Channels Only)

These commands are only available **inside an OPEN support ticket channel** and are intended for staff workflows.

#### `!transcript`
- **Description**: Export transcript to the configured transcripts channel and close the ticket (deletes the ticket channel after export)
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!transcript` (run inside a ticket channel)
- **Admin Only**: No (staff-only via `config.json -> support_tickets.permissions.staff_role_ids` or Discord Administrator)
- **Returns**: Uploads a transcript file + summary embed in the appropriate transcripts channel, then closes/deletes the ticket channel

#### `!close`
- **Description**: Close the ticket (defaults to transcript + delete)
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!close` (run inside a ticket channel)
- **Admin Only**: No (staff-only via `config.json -> support_tickets.permissions.staff_role_ids` or Discord Administrator)
- **Returns**: Same as `!transcript` by default (exports then deletes)

## Command Summary

- **Total Commands**: 15
- **Admin Commands**: 13 (the `.checker` commands require administrator permissions)
- **Public Commands**: 0
- **Commands with Aliases**: 7
- **Command Prefix**: `.checker` (dot prefix) + `!` (ticket channels only)

## Notes

- Most commands use the `.checker` prefix and require administrator permissions
- Support ticket commands use the `!` prefix and only work inside ticket channels
- Command messages are automatically deleted
- Replies auto-delete after 5-30 seconds depending on command
- Whop startup/6h sync is **audit-first** by default: it only removes the Member role if `config.json -> whop_api.enforce_role_removals=true` (default is false, which logs “would remove” only)
- Data is stored in JSON files:
  - `queue.json` - Active DM sequence queue
  - `registry.json` - Member registry
  - `invites.json` - Invite tracking
- DM sequence is controlled by `dm_sequence_enabled` setting
- Sequence days: day_1, day_2, day_3, day_4, day_5, day_6, day_7a, day_7b
- Whop API integration requires valid API key and company ID in config
- Membership lookups use `member_history.json` (`whop.last_membership_id`) when available
- Legacy case channels are identified by topic containing `rschecker_payment_case` or name starting with `pay-`
- Cleanup removes old data from registry and invites based on configured retention periods
