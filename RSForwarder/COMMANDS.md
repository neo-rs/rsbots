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
  - `!rsadd` (Discum-style browse: destination guild→**searchable category/channel** (native Discord picker), then source guild→**searchable category/channels**; auto-creates/uses webhook. Guild steps still use page list. Run the command **in the server** whose channels you are picking when possible.)
  - Manual: `!rsadd #personal-deals <WEBHOOK_URL> 886824827745337374 "leads found!"`
  - Manual: `!rsadd 1446174806981480578 <WEBHOOK_URL>`
- **Admin Only**: No (but requires channel access)
- **Returns**: Confirmation embed. The webhook is saved server-side in `config.secrets.json -> destination_webhooks` (not in `config.json`).

#### `!rslist`
- **Description**: List all configured channels with clickable **`<#source_id>`** mentions (when numeric), copy-paste numeric ids, webhook vs **in-place repost** mode, and role `<@&id>` plus raw id (so roles still make sense when Discord shows `@unknown-role` outside the home guild).
- **Aliases**: `list`
- **Parameters**: None
- **Usage**: `!rslist`
- **Admin Only**: No
- **Returns**: Embed with a short **how to change** block (`!rsadd`, `!rsupdate`, `!rsview`, `!rsremove`) and pointer to **`Canonical_SOP_with_Explainable_Logging.md`** (mirror-world repo) for deploy / `!whereami` verification. Each webhook row shows **active** vs **⚠️ stale** (URL in secrets but webhook deleted in Discord) and, when valid, **Posts to: `<#destination>`** so you open the correct channel’s Integrations.

#### `!rsupdate`
- **Description**: Update an existing forwarding job — **interactive menus** (no pasted webhook URL) or **manual** line for scripts
- **Aliases**: `update`
- **Parameters**:
  - **Interactive:** none, or only `source_channel` to skip the “pick source” step
  - **Manual:** `source_channel` + at least one of: `destination_webhook_url` (full `https://discord.com/api/webhooks/...`), `role_id`, or keyword `text=…`
- **Usage**:
  - `!rsupdate` — dropdown: pick **source** → pick **Destination (webhook)** / **Role** / **Extra text** (same auto-webhook behavior as `!rsadd`)
  - `!rsupdate #price-error-glitched` — start at “what to update” for that source
  - Manual: `!rsupdate #personal-deals <WEBHOOK_URL> 886824827745337374 "new text"`
- **Admin Only**: No (but requires channel access; destination/role pickers use the server you run the command in)
- **Returns**: Confirmation embed (manual) or live message with selects (interactive). **In-place repost** sources only show role / extra text (no destination webhook).

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
- **Admin Commands**: 7 (`!rstestall`, `!rsfscheck`, `!rsfstest`, `!rsfstestsku`, `!rsfsrun`, `!rsrestartadminbot`, `!rsstartadminbot`)
- **Public Commands**: 12
- **Commands with Aliases**: 10
- **Command Prefix**: `!rs` (unique prefix to avoid conflicts)

## Notes

- All commands use the `!` prefix and are namespaced with `rs...` (example: `!rsadd`, `!rsfsrun`).
- Commands that manage channels require access to the source channel
- Webhook URLs must be valid Discord webhook URLs (format: `https://discord.com/api/webhooks/...`)
- Role IDs must be valid numeric Discord role IDs
- The bot automatically brands forwarded messages with RS Server name and avatar
- Bot messages from RSForwarder itself are skipped to avoid loops
- RSAdminBot control commands require administrator permissions
- Local-exec mode is preferred when RSForwarder runs on Ubuntu host (no SSH key needed)

## Optional: Mavely auto-auth (affiliate rewriting)

RSForwarder generates Mavely affiliate links using cookies harvested from the **shared Chromerrunner CDP Chrome** (`oracle_real_chrome_profile` on `http://127.0.0.1:9222`) — the same browser Instorebotforwarder uses and that `oracle_novnc_tunnel.bat` opens for manual login.

- **Config** (`RSForwarder/config.json`):
  - `chrome_cdp_url`: default `http://127.0.0.1:9222`
  - `chrome_profile_dir`: default `Chromerrunner/oracle_real_chrome_profile`
  - `mavely_cdp_harvest_on_fail`: when true, monitor loop re-harvests cookies from CDP on preflight failure
  - `mavely_cdp_harvest_cooldown_s`: minimum seconds between automatic harvest attempts
  - `mavely_cdp_autologin_on_fail`: when true, monitor tries CDP auto-fill login (server creds) if harvest alone fails

- **Cookie file**: `RSForwarder/mavely_cookies.txt` (written by CDP harvest; read by GraphQL client)

- **OAuth refresh (optional)**:
  - `MAVELY_ENABLE_OAUTH_REFRESH=1`, `MAVELY_REFRESH_TOKEN_FILE`, etc. (unchanged)

### Manual login (canonical)

1. On your PC: run `oracle_novnc_tunnel.bat` from the mirror-world repo root.
2. Open `http://127.0.0.1:6080/vnc.html` and log into `https://creators.joinmavely.com` in the CDP Chrome window.
3. In Discord: `!rsmavelysync` then `!rsmavelycheck`.

#### `!rsmavelylogin` / `!refreshtoken` (admin only)
- **Description**: DMs the CDP Chrome / `oracle_novnc_tunnel.bat` login steps (does not start a second browser).

#### `!rsmavelysync` (admin only)
- **Description**: Harvests Mavely cookies from the running CDP Chrome into `mavely_cookies.txt`, then reports preflight status.

#### `!rsmavelyautologin` (admin only)
- **Description**: Auto-fills email/password in the **shared CDP Chrome** (real profile, not a headless bot browser), harvests cookies, then preflight. Requires `mavely_login_email` / `mavely_login_password` in `config.secrets.json`. Monitor uses this automatically when `mavely_cdp_autologin_on_fail=true`.

#### `!rsmavelycheck`
- **Description**: Harvests from CDP (on Oracle) + runs non-mutating session preflight.

#### `!rsmavelystatus` (admin only)
- **Description**: CDP up/down, profile path, last harvest, preflight status.
