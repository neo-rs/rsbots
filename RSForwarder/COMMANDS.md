# RSForwarder Commands Reference

## Overview
RSForwarder is a standalone bot for forwarding messages from RS Server channels to webhooks. All messages are branded with "Reselling Secrets" name and avatar from RS Server.

## Command Categories

### Channel Management Commands

#### `!rsadd`
- **Description**: Add a new source channel to destination mapping
- **Aliases**: `add`
- **Parameters**: 
  - `source_channel`: Discord channel mention or channel ID (manual mode)
  - `destination_webhook_url`: Discord webhook URL (manual mode)
  - `role_id`: Role ID to mention (optional)
  - `text`: Text to include with role mention (optional)
- **Usage**:
  - `!rsadd` (wizard: destination first → source → **Map → destination**; auto-creates/uses webhook)
  - Manual: `!rsadd #personal-deals <WEBHOOK_URL> 886824827745337374 "leads found!"`
  - Manual: `!rsadd 1446174806981480578 <WEBHOOK_URL>`
- **Admin Only**: No (but requires channel access)
- **Returns**: Confirmation embed. The webhook is saved server-side in `config.secrets.json -> destination_webhooks` (not in `config.json`).

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
- **Returns**: Updated configuration confirmation. If a webhook URL is provided, it is saved server-side in `config.secrets.json -> destination_webhooks`.

#### `!rsview`
- **Description**: View details of a specific forwarding job
- **Aliases**: `view`
- **Parameters**: 
  - `source_channel`: Discord channel mention or channel ID (required)
- **Usage**: `!rsview #personal-deals`
- **Admin Only**: No (but requires channel access)
- **Returns**: Detailed embed with channel, masked webhook, and role mention info

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

#### `!rsfscheck`
- **Description**: Validate RS-FS Google Sheet configuration/credentials (non-mutating)
- **Aliases**: `fscheck`, `rsfsstatus`
- **Parameters**: None
- **Usage**: `!rsfscheck`
- **Admin Only**: Yes (recommended)
- **Returns**: ✅/❌ status including spreadsheet ID, tab name, and existing SKU count

#### `!rsfstest`
- **Description**: Dry-run preview of the latest Zephyr Release Feed parse + title resolution (NO sheet writes)
- **Aliases**: `fstest`, `rsfs`
- **Parameters**:
  - `limit` (optional): Max items to preview (default: 25, max: 120)
- **Usage**: `!rsfstest 50`
- **Admin Only**: Yes (can spam output)
- **Returns**: Preview embeds with resolved title + URLs

#### `!rsfstestsku`
- **Description**: Dry-run ONE (store, sku) lookup (monitor lookup first, then website fallback)
- **Aliases**: `fstestsku`, `rsfssku`
- **Parameters**:
  - `store` (required)
  - `sku` (required)
- **Usage**: `!rsfstestsku gamestop 20023800`
- **Admin Only**: Yes
- **Returns**: Preview embed showing title + URLs

#### `!rsfsrun`
- **Description**: LIVE sync Zephyr Release Feed -> Google Sheet (writes A/B/C and also G/H)
- **Aliases**: `rsfslive`, `rsfswrite`
- **Parameters**:
  - `limit` (optional): Max items per run (default: 120, max: 500)
- **Usage**: `!rsfsrun 120`
- **Admin Only**: Yes
- **Returns**: Status messages and writes new rows into the sheet

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

- **Total Commands**: 18
- **Admin Commands**: 6 (`!rstestall`, `!rsfscheck`, `!rsfstest`, `!rsfstestsku`, `!rsfsrun`, `!rsrestartadminbot`, `!rsstartadminbot`)
- **Public Commands**: 12
- **Commands with Aliases**: 10
- **Command Prefix**: `!rs` (unique prefix to avoid conflicts)

## Notes

- All commands use the `!` prefix and are namespaced with `rs...` (example: `!rsadd`, `!rsfsrun`)
- Commands that manage channels require access to the source channel
- Webhook URLs must be valid Discord webhook URLs (format: `https://discord.com/api/webhooks/...`)
- Role IDs must be valid numeric Discord role IDs
- The bot automatically brands forwarded messages with RS Server name and avatar
- Bot messages from RSForwarder itself are skipped to avoid loops
- RSAdminBot control commands require administrator permissions
- Local-exec mode is preferred when RSForwarder runs on Ubuntu host (no SSH key needed)

## Optional: Mavely auto-auth (affiliate rewriting)

RSForwarder can generate Mavely affiliate links and auto-recover when tokens expire.

- **Cookies (recommended)**: use `RSForwarder/mavely_cookie_refresher.py` to keep a logged-in browser profile and write cookies to a file.
  - `MAVELY_COOKIES_FILE`: path to cookie header text file (default in RSForwarder folder)
  - `MAVELY_BASE_URL`: usually `https://creators.joinmavely.com`

- **OAuth refresh (optional, for hands-off token renewal)**:
  - `MAVELY_ENABLE_OAUTH_REFRESH=1`: allow refreshing access tokens when Mavely returns auth failures
  - `MAVELY_REFRESH_TOKEN_FILE`: file path where the bot can store the latest refresh token (handles rotation)
  - `MAVELY_TOKEN_ENDPOINT`: default is `https://auth.mave.ly/oauth/token`
  - `MAVELY_CLIENT_ID`: optional (can often be inferred from session/idToken)

If Mavely forces a real re-login (logout / Cloudflare / MFA), you may still need to refresh cookies manually.

### Manual re-login via noVNC (server desktop)

If RSForwarder runs on the Oracle Linux host, you can trigger an interactive Mavely login flow without SSHing into the shell:

#### `!rsmavelylogin` / `!refreshtoken` (admin only)
- **Description**: Starts (or reuses) a localhost-only noVNC desktop on the server and launches the Mavely browser login flow.
- **Usage**:
  - `!rsmavelylogin`
  - `!rsmavelylogin 900` (wait up to 900s for login)
- **What you do**:
  - Open the SSH tunnel command the bot prints
  - Open the noVNC URL the bot prints
  - Log into Mavely in the Chromium window
- **Security**: noVNC binds to `127.0.0.1` only (requires SSH tunnel; not exposed publicly).

#### `!rsmavelycheck`
- **Description**: Runs a non-mutating session preflight check (safe). Useful to confirm login succeeded.

### Headless Playwright auto-login (best-effort)

If you have Mavely email/password credentials available on the server, RSForwarder can attempt a headless auto-login using Playwright.

- **Enable on failure (automatic)**:
  - Set `mavely_autologin_on_fail=true` in `RSForwarder/config.json` (or set `MAVELY_AUTOLOGIN_ON_FAIL=1` in the environment).
  - Provide credentials server-side via `RSForwarder/config.secrets.json`:
    - `mavely_login_email`
    - `mavely_login_password`

#### `!rsmavelyautologin` (admin only)
- **Description**: Triggers headless Playwright auto-login immediately (no noVNC). Useful when you get a DM about session expiry and want the bot to try recovering without manual login.
- **Usage**:
  - `!rsmavelyautologin`
  - `!rsmavelyautologin 180` (wait up to 180s)
- **Afterwards**: run `!rsmavelycheck` to confirm the session is valid.
