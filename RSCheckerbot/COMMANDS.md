# RSCheckerbot Commands Reference

## Overview
RSCheckerbot manages member verification, payment tracking, and DM sequences for Reselling Secrets. It handles Whop API integration, membership status tracking, and automated DM sequences for members.

## When RSCheckerbot Removes the Member Role

RSCheckerbot is the **only** bot that removes the Member role. It does so in two situations:

| When | What happens | Config / condition |
|------|---------------------------|--------------------|
| **Whop sync** (startup + 6h loop) | For each Discord user linked to a Whop membership, the bot calls the Whop API. If the membership is **not entitled** (e.g. status `canceled`, `completed`, `past_due`, `unpaid`) and the user has the Member role, the bot can remove it. | **Only if** `config.json → whop_api.enforce_role_removals` is **`true`**. Default is **`false`** (audit-only: logs “would remove” but does not remove). Lifetime members are never stripped. |
| **Repeat-trial guard** | When the Member role is **added** (e.g. by payment_activation), the bot checks if the user had a trial before and total spend is at or below the threshold. If so, it removes the Member role again and posts a staff card. | `config.json → whop_api.repeat_trial_guard` (e.g. `enabled`, `max_total_spent_usd`). |

RSOnboarding never removes the Member role.

## Command Categories

### Slash Commands (Staff/Admin)

#### `/channel_limits`
- **Description**: Show current server channel counts vs Discord limits (and category count vs limit)
- **Staff/Admin Only**: Yes (Discord Administrator OR roles from `config.json -> support_tickets.permissions.staff_role_ids` / `admin_role_ids`)
- **Usage**: `/channel_limits`
- **Returns**: Ephemeral summary line (no public spam)

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

#### `.checker purgenowhop`
- **Description**: Delete ONLY open `no_whop_link` support ticket channels (safe: verified by ticket topic + ticket_type)
- **Aliases**: `purgenowhoplink`, `purgenowhoplinks`, `wipe-nowhop`, `wipe-nowhoplink`
- **Parameters**:
  - `confirm`: Must be exactly `confirm` to proceed (required)
- **Usage**: `.checker purgenowhop confirm`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Deletion summary (deleted count, skipped count, failed count)

#### `.checker scannowhop`
- **Description**: Force-run the `no_whop_link` scan immediately (bypasses interval throttling)
- **Aliases**: `scan-nowhop`, `nowhopscan`, `scan_nowhop`
- **Parameters**: None
- **Usage**: `.checker scannowhop`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Scan summary string

#### `.checker rebuildnowhop`
- **Description**: Purge + re-scan `no_whop_link` tickets (safe regen)
- **Aliases**: `rebuild-nowhop`, `rebuild-nowhoplink`, `regen-nowhop`
- **Parameters**:
  - `confirm`: Must be exactly `confirm` to proceed (required)
- **Usage**: `.checker rebuildnowhop confirm`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Purge deleted count + scan summary string

#### `.checker fixnowhoproles`
- **Description**: One-time cleanup: remove Billing role from members who have an OPEN `no_whop_link` ticket
- **Aliases**: `fix-nowhop-roles`, `fixnowhop`
- **Parameters**:
  - `billing_role_id` (optional): Override the configured Billing role id
  - `confirm`: Must be exactly `confirm` to proceed (required)
- **Usage**:
  - `.checker fixnowhoproles confirm`
  - `.checker fixnowhoproles 1467500050341957725 confirm`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Removal summary (removed count, failed count)

#### `.checker reconcilecancel`
- **Description**: Compare the configured **cancellation ticket category** to `RSCheckerbot/data/tickets_index.json`, then optionally fix drift (admin-only). **Scan** is read-only. **Apply** can delete Discord channels **without transcript** for zombies/orphans, or mark index CLOSED for ghosts (missing channel).
- **Aliases**: `reconcile-cancellation`, `reconcilecancellation`, `cancelreconcile`
- **Parameters**:
  - `scan` or `apply … confirm` (see below)
  - For `apply`: `zombies` | `orphans` | `ghosts` | `all`, then literal `confirm`, optional integer **max per bucket** (default 30, max 200)
- **Usage**:
  - `.checker reconcilecancel scan`
  - `.checker reconcilecancel apply zombies confirm`
  - `.checker reconcilecancel apply orphans confirm`
  - `.checker reconcilecancel apply ghosts confirm`
  - `.checker reconcilecancel apply all confirm 40`
  - `.checker reconcilecancel help`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Scan = embed with counts + sample IDs; apply = summary line (zombies_closed, orphans_deleted, ghosts_marked, failed)
- **Notes**:
  - Run only in `support_tickets.guild_id` (bot replies with an error in other guilds)
  - **Zombies**: index row is CLOSED `cancellation` but the channel still exists under the cancellation category (delete channel, no transcript)
  - **Orphans**: channel under cancellation category, name `cancel-*`, valid RS ticket topic, but **no** index row for that channel (delete channel, no transcript)
  - **Unsafe** (scan only): `cancel-*` name but missing/invalid ticket topic — **not** auto-deleted; fix manually
  - **Ghosts**: OPEN `cancellation` in index but Discord channel is gone — marks index CLOSED and removes cancellation ticket role best-effort
  - **OPEN but not in cancel category** (scan only): e.g. moved to churn manually — listed for visibility; not changed by apply

#### `.checker reconcilebillingcancel`
- **Description**: Find users who have **both** an OPEN **billing** ticket and an OPEN **cancellation** ticket (historical overlap), then optionally **close billing only** with transcript + channel delete. Cancellation ticket is unchanged. Complements the automatic billing→cancellation handoff on new `member-status-logs` cancellation cards.
- **Aliases**: `reconcile-billing-cancel`, `billingcancelreconcile`
- **Parameters**:
  - `scan` — read-only list (sample mentions + channel links)
  - `apply confirm` — close overlapping billing tickets; optional third arg **max closes** (default 30, max 200)
- **Usage**:
  - `.checker reconcilebillingcancel scan`
  - `.checker reconcilebillingcancel apply confirm`
  - `.checker reconcilebillingcancel apply confirm 50`
  - `.checker reconcilebillingcancel help`
- **Admin Only**: Yes (requires administrator permissions)
- **Returns**: Scan = embed with count + sample rows; apply = summary (`closed_billing`, `failed`, `skipped_cap`, `total_candidates`)
- **Notes**:
  - Run only in `support_tickets.guild_id` (bot replies with an error in other guilds)
  - **Apply** uses `close_open_ticket_for_user` for `billing` (transcript + delete per config); billing role removed by existing close path

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
- **Returns**: Uploads a transcript file + summary embed in the appropriate transcripts channel, then closes/deletes the ticket channel. If export or delete fails, replies with an error (ticket stays OPEN in the index so staff can retry after fixing permissions or config).

#### `!close`
- **Description**: Close the ticket (defaults to transcript + delete)
- **Aliases**: None
- **Parameters**: None
- **Usage**: `!close` (run inside a ticket channel)
- **Admin Only**: No (staff-only via `config.json -> support_tickets.permissions.staff_role_ids` or Discord Administrator)
- **Returns**: Same as `!transcript` by default (exports then deletes), including the same failure behavior

## Command Summary

- **Total Commands**: 22
- **Admin Commands**: 19 (the `.checker` commands require administrator permissions)
- **Public Commands**: 0
- **Commands with Aliases**: 12
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

## Local Maintenance Tools (CLI)

These are **local scripts** you run on the machine that has `RSCheckerbot/config.json` + `RSCheckerbot/config.secrets.json` (bot token). They are not Discord commands.

### `whop_api_probe.py memberstatus-cards`

- **Purpose**: Scan `#member-status-logs` history and generate an inventory of **every message card title** + **field label set**, then map each card back to the **producer/consumer code paths** that generate/use it (tickets/roles/reporting).

  This is specifically designed to handle historical drift from past formatting changes.

- **Default behavior**: Read-only. Writes a JSON report under `RSCheckerbot/backups/` and saves a resume cursor under `RSCheckerbot/.probe_memberstatus_cards_state.json`.

- **Dry run (recommended first)**:

```bash
py -3 RSCheckerbot/whop_api_probe.py memberstatus-cards --channel-id 1452835008170426368 --limit 5000 --resume --progress-every 200 --checkpoint-every 2000 --run-until-done
```

- **Interactive (safer for flaky connections)**:

```bash
py -3 RSCheckerbot/whop_api_probe.py memberstatus-cards --channel-id 1452835008170426368 --limit 5000 --interactive --checkpoint-every 2000 --run-until-done
```

- **Record into `member_history.json` (compact baseline, no PII)**:

  This writes per-user snapshots under `member_history.json -> whop.member_status_logs_latest`. It is **confirm-gated**.

```bash
py -3 RSCheckerbot/whop_api_probe.py memberstatus-cards --channel-id 1452835008170426368 --limit 5000 --resume --run-until-done --record-member-history --confirm confirm
```

- **Outputs**:

  - **Report JSON**: `RSCheckerbot/backups/memberstatus_cards_scan_*.json`
  - **Resume cursor**: `RSCheckerbot/.probe_memberstatus_cards_state.json`

- **How to interpret the report (high-signal fields)**:

  - `observed.titles`: exact observed Discord embed titles with counts, footers, and field label stats
  - `merge`: per-title “known vs unknown” + ticket relevance + producer callsites
  - `merge.<title>.producer_callsites`: where in code a literal `title="..."` is used
  - `merge.<title>.producer_title_for_event`: titles emitted by `main.py::_title_for_event()`
  - `observed.unknown_titles`: likely legacy titles or drifted variants that no longer match current producers
