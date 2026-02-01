# RSAdminBot Commands Reference (Slash-only)

## Overview

RSAdminBot is the central administrative bot for managing all RS/MW bots on the Oracle Ubuntu host.

**Current policy (enforced in code):**

- **Slash-only**: no `!` prefix commands.
- **Ephemeral-only**: command responses are visible only to you.
- **Owner-only**: commands run only for the **Discord server owner**.
- **Test-guild-only**: commands are registered and allowed only in **neo-test-server** (ID comes from `RSAdminBot/config.json:test_server_guild_id`).

## Slash commands

### Notes

#### `/rsnote`
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
- **What**: update an RS bot from the GitHub checkout (**python-only**) and restart it

#### `/mwupdate`
- **What**: update an MW bot from the GitHub checkout (**python-only**) and restart it

#### `/selfupdate`
- **What**: **safe RSAdminBot update** (staged) + service restart to apply
- **Implementation**: writes `RSAdminBot/.pending_update.json`; applied by `RSAdminBot/run_bot.sh` on boot

### Diagnostics / logs

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
- **What**: quick health report (roots exist + service counts)

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

#### `/syncstatus`
- **What**: compare `rsbots-code` vs live tree using `rsbots_manifest.py`

#### `/oraclefilesupdate`
- **What**: push a bots-only snapshot to `neo-rs/oraclefiles`
- **Requires**: `oraclefiles_sync.enabled=true` + `oraclefiles_sync.deploy_key_path` in `RSAdminBot/config.secrets.json`

## Removed command suites (no longer available)

- **Whop tracking**: `whopscan`, `whopstats`, `whophistory`
- **Bot movement tracking**: `botmovements`
- **Oracle data tooling**: `oracledatasync`, `oracledataanalyze`, `oracledatadoc`, `oracledatasample`
- **Test-server tooling**: `setupmonitoring`, `testcards`
- **Deploy / proc health**: `deploy`, `rspids`, `moneyflowcheck`, `codehash`
- **Prefix commands**: all `!` commands are retired
