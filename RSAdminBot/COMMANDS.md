# RSAdminBot Commands Reference (Slash-only)

## Overview

RSAdminBot is the central administrative bot for managing all RS/MW bots on the Oracle Ubuntu host.

**Current policy (enforced in code):**

- **Guild-specific registration**: Discord `tree.sync(guild=…)` only uploads commands placed in that guild’s bucket. RSAdmin binds **admin slash** to **Neo Test Server** (`test_server_guild_id`) via `add_cog(..., guild=…)`, and binds **`/rsnote`** to **Reselling Secrets** (`rs_server_guild_id`) the same way — so `/rsnote` appears only on RS and `/ping`, `/botstatus`, etc. only on Neo Test Server.
- **Ephemeral-only** (admin suite): command responses are visible only to you.
- **Owner/admin** (admin suite): **guild owner**, `admin_user_ids`, or `admin_role_ids` (not “Administrator” unless those roles are listed).
- **`/rsnote`**: ephemeral notes UI; **not** owner-gated (any member in RS can open their own panel).
- **RS prefix**: `!delete` / `!transfer` / `!archive` remain RS-guild-only (message content intent).

## Slash commands

### Notes

#### `/rsnote`
- **Where**: **Reselling Secrets** only (`rs_server_guild_id`)
- **What**: private notes panel (SKU / links / anything)
- **Visibility**: ephemeral
- **Storage**: `RSAdminBot/RSNotes/data/rsnotes.json` (runtime; not committed)

### Core

#### `/ping`
- **What**: bot latency check

#### `/status`
- **What**: runtime status (local-exec/ssh target + module availability)

#### `/reload`
- **What**: reload `RSAdminBot/config.json` + SSH selector mapping

#### `/restart`
- **What**: restart `mirror-world-rsadminbot.service`
- **Safety**: asks for confirmation

### Bot management (interactive dropdown)

These commands open a **dropdown** to pick a bot. All outputs are ephemeral.

#### `/botlist`
- **What**: list configured RS + MW bots

#### `/botstatus`
- **What**: service status for a bot

#### `/botinfo`
- **What**: bot metadata (folder/service/script/group) + current service state

#### `/botstart`
- **What**: start a bot (also offers “All Bots”)

#### `/botstop`
- **What**: stop a bot (also offers “All Bots”)

#### `/botrestart`
- **What**: restart a bot (also offers “All Bots”)

#### `/botsync`
- **What**: sync a bot folder to the Oracle host

#### `/botupdate`
- **What**: update an **RS-group** bot from GitHub (**python-only**) and restart it
- **Note**: **`catalognavbot`** uses the same **`rsbots-code`** pull + sync-to-live path as other RS bots; **`catalog_nav_bot/`** must exist in that repo checkout.

#### `/mwupdate`
- **What**: update an MW bot from the **`mwbots-code`** checkout (**python-only**) and restart it
- **Note**: Does **not** include catalog nav (use **`/botupdate`**).

#### `/selfupdate`
- **What**: **safe RSAdminBot update** (staged) + service restart to apply
- **Implementation**: writes `RSAdminBot/.pending_update.json`; applied by `RSAdminBot/run_bot.sh` on boot

### Diagnostics / logs

#### `/delete`
- **What**: delete a selected text channel (dropdown + confirm)

#### `/transfer`
- **What**: move a selected channel into a selected category (dropdowns + confirm)

#### `!archive` (RS guild prefix only)
- **Where**: **Reselling Secrets** (`rs_server_guild_id`), in the **source** text channel to archive; owner/admin gated (same as `!delete` / `!transfer`).
- **Scope**: Archives **only the channel where you ran `!archive`** (`#current-channel` history → one new forum thread). It does **not** scan or archive other channels.
- **What**: Creates a **new forum thread** under `archive.forum_channel_id`, then **webhook-replays** that channel’s history into the thread (avatars, embeds, attachments, stickers; original UTC time appended per message). Does **not** create a new guild text channel.
- **After replay**: The source channel is **always deleted** on success (no lock/move mode).
- **UI**: One public button — **Pick forum tags** — opens an **ephemeral** panel: optional **string select** (up to **5** forum tags; choose **Use config tag defaults** alone to use `archive.applied_tag_ids`), then **Start archive**. If the forum reports no tags, only **Start archive** is shown (uses config defaults).
- **Config**: `RSAdminBot/config.json` → `archive`: `forum_channel_id`, `replay_delay_ms` (0–2000), optional `applied_tag_ids`. **`RSAdminBot/config.secrets.json`** (merged) → `archive.replay_webhook_url`: **required** full webhook URL for the **forum** channel (same channel as `forum_channel_id`).

#### `/details`
- **What**: systemd details for a bot

#### `/logs`
- **What**: journal logs for a bot
- **Args**: `lines` (10–400)

#### `/botdiagnose`
- **What**: quick diagnosis (status + recent errors)

#### `/whereami`
- **What**: runtime proof (cwd, file path, python, local_exec, git heads)

#### `/systemcheck`
- **What**: health report (paths + service counts) and **Oracle machine stats** when reachable: RAM (`free -h`), disk (`df -h /`), CPU/load (`uptime`, `top`), journald usage, bots folder size, systemd service list. Second ephemeral message: **Disk Hotspots** (top 10 largest files under `/home/rsadmin/bots`, RSAdminBot log size if configured).

#### `/fileview`
- **What**: file sizes + mtimes for a bot folder
- **Args**: `mode` (use `alljson` to include all `*.json`, excluding `config.secrets.json`)

### Config + secrets (interactive + modals)

#### `/botconfig`
- **What**: show a bot’s `config.json` summary
- **Buttons**:
  - **Edit config.json**: opens a modal to set `key.path` to a JSON value (backs up file first)
  - **Restart bot**: restarts the selected bot service

#### `/secretsstatus`
- **What**: show masked keys from `config.secrets.json`
- **Buttons**:
  - **Edit config.secrets.json**: opens a modal to set `key.path` to a JSON value (backs up file first; never prints secrets)

### Ops

#### `/ssh`
- **What**: run an SSH/local-exec command (use carefully)
- **Log trim on Oracle**: to cap movement logs (e.g. 30MB per file), run: `bash /home/rsadmin/bots/mirror-world/RSAdminBot/scripts/trim_oracle_logs.sh` (or set `MIRROR_WORLD`/`MAX_FILE_MB` if needed)

#### `/syncstatus`
- **What**: compare `rsbots-code` vs live tree using `rsbots_manifest.py`

#### `/oraclefilesupdate`
- **What**: push a bots-only snapshot to `neo-rs/oraclefiles`
- **Requires**: `oraclefiles_sync.enabled=true` + `oraclefiles_sync.deploy_key_path` in `RSAdminBot/config.secrets.json`

## Message triggers (Neo Test Server only)

These are **not** slash commands. They are **message listeners** scoped to a single channel to avoid spam.

#### `review rs` (in-channel trigger)
- **Where**: Neo Test Server channel `1496065906923540561`
- **Who**: Owner/Admin-only (uses RSAdminBot's canonical admin checks)
- **What**: replies with:
  - channel lists for RS categories (clickable `<#channel_id>` mentions only)
  - the last 3 message links for each configured RS “important” channel

#### Instore daily digest (in-channel trigger, same channel)
- **Where**: Neo Test Server channel `1496065906923540561` (same as `review rs`)
- **Who**: Owner/Admin-only
- **Input**: any message whose text matches a fuzzy **instore** check (e.g. contains `instore`, or `in store`, or common typos like `instroe` / `isntore`). Exact `review rs` still runs the original RS review only.
- **What**: for Reselling Secrets category `1341477669682024588` (configurable), lists **today’s posts** from **midnight** in `instore_daily_timezone` (default `America/New_York`) through **now**, per **text channel** in that category. Skips empty channels and default skip IDs (monitors/chats). **One Discord message per channel** with posts. Header: `new leads posted for today **<date>** - <#channel>`, then post count. **Role mention**: `<@&instore_daily_role_id>` is used only when that role exists in the **reply** guild (Neo Test). For RS-only roles, set `instore_daily_role_label` (shown as `**label**`) and optionally `instore_daily_ping_user_ids` for real `<@user>` pings in Neo. Each line: `- jump_url - Store - Product` when both **Where:** (line or inline) and a non-label first line parse from message text **or** embed title/description/fields; otherwise `- jump_url` only. Truncation: `instore_daily_product_max_chars` / `instore_daily_store_max_chars`. Cap: `instore_daily_max_messages_per_channel` (max 500).

#### Catalog formatting (message-link trigger)
- **Where**: Neo Test Server channel `1496075487452069920`
- **Input**: paste one or more Discord message links (supports `discord.com` and `ptb.discord.com`)
- **Who**: Owner/Admin-only
- **What**: for each link, replies with:
  - `Product Title` (embed title)
  - `SKU/TCIN/UPC` (from embed fields, when present)
  - `Image URL` (direct attachment URL or embed image/thumbnail URL)

## Removed command suites (no longer available)

- **Whop tracking**: `whopscan`, `whopstats`, `whophistory`
- **Bot movement tracking**: `botmovements`
- **Oracle data tooling**: `oracledatasync`, `oracledataanalyze`, `oracledatadoc`, `oracledatasample`
- **Test-server tooling**: `setupmonitoring`, `testcards`
- **Deploy / proc health**: `deploy`, `rspids`, `moneyflowcheck`, `codehash`
- **Legacy prefix suites**: broad `!` admin commands are retired; **Reselling Secrets** retains only `!delete` / `!transfer` / `!archive` (RS-guild-only; see `admin_bot.py`).
