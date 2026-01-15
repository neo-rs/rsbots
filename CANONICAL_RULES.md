# Canonical Project Rules

This document defines the non-negotiable architectural and coding rules for this project.
All enhancements, fixes, and refactors must follow these rules.

---

## 🔒 NO CODE LEFT BEHIND

When enhancing, fixing, or refactoring code:

1. **No parallel logic**
   - Existing functionality must be modified in place.
   - Creating a second implementation for the same responsibility is forbidden.

2. **Destructive refactors**
   - Superseded code must be deleted.
   - Commenting out old logic is not acceptable.

3. **No hardcoded values**
   - All values must come from configuration, constants, or existing structures.
   - Hardcoded values are only allowed if explicitly requested.

4. **One source of truth**
   - Each responsibility must exist in exactly one location.
   - Duplicated parsing, commands, or checks are defects.

5. **Enhancements upgrade existing paths**
   - Enhancements must replace or extend current logic.
   - Leaving old paths active is a bug.

6. **Mandatory cleanup report**
   - Every change must list:
     - what was removed
     - what was replaced
     - what is now canonical

7. **Stop if uncertain**
   - If it’s unclear whether old code should remain, stop and ask.
   - Silent duplication is never acceptable.

---

## 🧠 CANONICAL OWNERSHIP

| Responsibility | Canonical Owner |
|---------------|-----------------|
SSH execution | SSHExecutor |
Service control | ServiceManager |
Logging | CommandLogger |
Bot registry | BOTS |
UI output | Callers only |

---

## Ubuntu access and RS-bots deployment (canonical)

This section documents the canonical operational workflow for managing RS bots on the Oracle Ubuntu host.

### Source of truth: server target and key

- **Server list**: `oraclekeys/servers.json`
- **SSH key (Windows, when connecting FROM Windows to Oracle)**: `oraclekeys/ssh-key-*.key`
  - **Current Oracle key in this repo**: `oraclekeys/ssh-key-2025-12-15.key`
  - **Used by**:
    - `oraclekeys/servers.json` (`key: "ssh-key-2025-12-15.key"`)
    - `RSAdminBot/config.json` (`ssh_server.key: "ssh-key-2025-12-15.key"`; resolved relative to `RSAdminBot/`)
  - **Example (Windows)**:
    - `ssh -i oraclekeys/ssh-key-2025-12-15.key rsadmin@137.131.14.157`
- **Ubuntu repo root**: `/home/<user>/bots/mirror-world` (usually `/home/rsadmin/bots/mirror-world`)
  - **Note (Ubuntu local-exec mode)**: when you are already on Oracle Ubuntu and `local_exec=yes`, no SSH key is needed.

### Preferred update paths (in order)

1. **Server-side deploy (full update)**:
   - Run on Ubuntu: `bash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh deploy_apply <archive_path>`
   - This applies code update plus venv refresh plus systemd unit refresh in one deterministic sequence.

2. **Bot folder sync (targeted code-only update)**:
   - Use RSAdminBot (admin-only): `!botupdate <bot>` (pulls from GitHub) or `!botsync <bot>` (syncs local files)
   - Rules:
     - Never sync secrets (`config.secrets.json`) or runtime data
     - Always create a remote backup tar.gz before overwrite
     - Compare using sha256 manifests (not file sizes)
     - On Ubuntu, prefer **local-exec** (no SSH key required on the server)
     - `COMMANDS.md` files ARE synced (documentation files, same as .py files)

### Verification (canonical)

- **Hashed manifest verification** must be used to verify local vs server code state:
  - On Ubuntu: `python3 scripts/rsbots_manifest.py --normalize-text-eol --out /tmp/rsbots_manifest_server.json`
  - On Windows: `python scripts/rsbots_manifest.py --normalize-text-eol --out Oraclserver-files/rsbots_manifest_local.json`
  - Compare: `python scripts/compare_rsbots_python_only.py`

### Baseline sync rule (MANDATORY - prevents "Cursor edited the wrong code")

Before ANY debugging, refactor, cleanup, or feature work on RS bots, you MUST prove that the local workspace matches what is actually running on Oracle.

Reason: Oracle is not guaranteed to be a git checkout. If the server code is ahead/behind local, then "fixing local" can create drift and break deploys.

Required workflow (Windows):

1. Download a fresh Oracle snapshot:
   - `download_oracle_snapshot.bat`
2. Run the baseline compare:
   - `oracle_baseline_check.bat`
   - (or) `python scripts/oracle_baseline_check.py`

Enforcement:

- If the baseline check shows ANY changed/only-local/only-server python files, you MUST STOP and reconcile BEFORE making changes.
- Reconciliation means choosing one source of truth:
  - Make Oracle match local (deploy), OR
  - Make local match Oracle (import snapshot changes into this repo and commit), then deploy.
- Server code must not be silently updated out-of-band. If it is, it must be captured into this repo immediately to restore a single source of truth.

### RSAdminBot self-update requirement

RSAdminBot must not overwrite its own running folder in-place.
It must stage updates and apply them on restart via the systemd wrapper (`RSAdminBot/run_bot.sh`).

## 🚫 FORBIDDEN PATTERNS

- `shell=True`
- duplicate `systemctl` logic
- parsing `echo` output for state
- hardcoded sleeps
- parallel SSH helpers
- silent fallbacks
- **SQLite databases (`.db` files)** - All RS bots must use JSON only
- **Dual storage systems** - One source of truth per data type

---

## 🧪 VERIFICATION RULE

If old code is removed and something breaks:
- Fix the dependency
- Do not restore deprecated code

---

## 🧪 TestCenter output testing SOP (RS bots)

Purpose: verify Discord output cards (routing + formatting) in the **TestCenter Server** (`1451275225512546497`) with repeatable artifacts.

### Preconditions

- TestCenter channels exist in the category `1458533733681598654`:
  - `payment-failure`
  - `member-cancelation`
  - `member-status-logs`
- RSAdminBot is online in TestCenter.

### Procedure (canonical)

1. **Ensure monitoring channels exist** (one-time / as needed):
   - Run: `!setupmonitoring`
   - Verify the TestCenter monitoring category/channels were created/refreshed.

2. **Post sample RSCheckerbot cards + generate artifact**
   - Run: `!testcards @SomeMember`
   - If no member is provided, the command defaults to the invoking user.

3. **Verify expected results**
   - `#member-status-logs`: receives a **detailed** card with fields:
     - `Member Info`, `Discord Info`, `Payment Info`
   - `#payment-failure`: receives a **minimal** card for payment triage
   - `#member-cancelation`: receives a **minimal** cancellation card

4. **Collect artifacts (required)**
   - RSAdminBot uploads a JSON trace file as an attachment to the command response.
   - Server-side copy is written to: `RSAdminBot/test_artifacts/testcenter_cards_trace_<timestamp>.json`

Notes:
- Do not paste or embed code blocks in Discord output messages.
- Do not include Whop internal IDs in staff output cards for RSCheckerbot triage channels.

---

## 💾 STORAGE RULES (ALL RS BOTS)

**CRITICAL: All RS bots must follow these storage rules:**

1. **JSON-only storage**
   - All runtime data must be stored in JSON files
   - NO SQLite databases (`.db` files)
   - NO dual storage systems (JSON + database)
   - One source of truth per data type

2. **Allowed data files:**
   - `config.json` - Bot configuration (synced)
   - `messages.json` - Bot messages (synced)
   - `tickets.json` - Active tickets (RSOnboarding only, NOT synced)
   - `registry.json` - Runtime registry (RSCheckerbot only, NOT synced)
   - `queue.json` - Runtime queue (RSCheckerbot only, NOT synced)
   - `invites.json` - Invite tracking (RSCheckerbot only, NOT synced)
   - Other bot-specific runtime JSON files (NOT synced)

3. **Forbidden data files:**
   - `*.db` - SQLite databases (FORBIDDEN)
   - `*.sqlite` - SQLite databases (FORBIDDEN)
   - `*.sqlite3` - SQLite databases (FORBIDDEN)
   - Any database files must be deleted and replaced with JSON

4. **File sync rules:**
   - `.db` files are NEVER synced to remote
   - Runtime JSON files (tickets.json, registry.json, etc.) are NOT synced
   - Only `config.json` and `messages.json` are synced
   - All `.py` files are ALWAYS synced
   - `COMMANDS.md` files are ALWAYS synced (documentation files, same as .py files)
   - `COMMANDS.md` files are part of code deployment and must be kept in sync with command implementations

5. **Migration requirement:**
   - If a bot currently uses a database, it MUST be migrated to JSON
   - Database code must be completely removed
   - No `sqlite3` imports allowed in RS bots
   - No `_init_db`, `_save_to_db`, or similar database methods
   - **Data migration:** Before removing database code, export all data to JSON

6. **JSON Storage Structure (Standardized):**
   
   **RSuccessBot (`success_points.json`):**
   ```json
   {
     "points": {
       "<user_id>": {
         "points": <integer>,
         "last_updated": "<ISO timestamp>"
       }
     },
     "image_hashes": {
       "<hash>": {
         "user_id": <integer>,
         "created_at": "<ISO timestamp>"
       }
     },
     "point_movements": [
       {
         "user_id": <integer>,
         "change_amount": <integer>,
         "old_balance": <integer>,
         "new_balance": <integer>,
         "reason": "<string>",
         "admin_user_id": <integer|null>,
         "created_at": "<ISO timestamp>"
       }
     ],
     "migrated_at": "<ISO timestamp>"
   }
   ```
   
   **RSCheckerbot (`invites.json`):**
   ```json
   {
     "invites": {
       "<invite_code>": {
         "lead_id": "<string>",
         "email": "<string>",
         "utm_data": "<string>",
         "created_at": "<ISO timestamp>",
         "used_at": "<ISO timestamp|null>",
         "discord_user_id": "<string|null>",
         "discord_username": "<string|null>"
       }
     },
     "migrated_at": "<ISO timestamp>"
   }
   ```
   
   **RSOnboarding (`tickets.json`):**
   ```json
   {
     "<user_id>": {
       "channel_id": <integer>,
       "opened_at": <float>
     }
   }
   ```

## 📚 COMMANDS DOCUMENTATION MAINTENANCE (MANDATORY)

### Automatic COMMANDS.md Updates

**CRITICAL**: When ANY command is added, removed, or modified in ANY bot, the corresponding `COMMANDS.md` file MUST be updated immediately.

#### Cursor AI Instructions

**MANDATORY**: When making ANY changes to bot commands, you MUST update the corresponding `COMMANDS.md` file in the same commit.

1. **Detect command changes automatically**:
   - When you add/remove/modify `@bot.command()`, `@self.bot.command()`, or `@bot.tree.command()` decorators
   - When you change command names, aliases, parameters, docstrings, or descriptions
   - When you modify command functionality that affects usage or behavior
   - When you change admin/public permissions (`@commands.check()`, `@commands.has_permissions()`)
   - When you add/remove command parameters or change parameter types/defaults

2. **Update COMMANDS.md immediately** (same commit as code changes):
   - Locate the bot's `COMMANDS.md` file: `{BotFolder}/COMMANDS.md`
   - For NEW commands: Add complete entry with all details (name, aliases, parameters, description, admin status, usage, returns)
   - For MODIFIED commands: Update the existing entry with new information
   - For DELETED commands: Remove the entry completely
   - Update command summary counts at the bottom of the file (Total Commands, Admin Commands, Public Commands)

3. **Maintain format consistency**:
   - Follow the exact format used in existing COMMANDS.md files
   - Use `#### `!commandname`` for prefix commands
   - Use `#### `/commandname`` for slash commands
   - Include ALL aliases in the Aliases field (comma-separated)
   - Include ALL parameters with descriptions and types if applicable
   - Include usage examples
   - Mark admin-only commands clearly (Yes/No)
   - Include return value descriptions

4. **Verify completeness before committing**:
   - Count commands in code vs COMMANDS.md (must match exactly)
   - Ensure all aliases are documented
   - Ensure all parameters are documented
   - Update command summary section (Total Commands, Admin Commands, Public Commands)
   - Verify format matches existing entries

#### Enforcement

- **Before committing**: Verify COMMANDS.md matches actual commands in code
- **During code review**: Check that COMMANDS.md was updated for any command changes
- **After deployment**: Use `!commands <bot>` to verify documentation is accurate

#### File Locations

Each bot has its own COMMANDS.md file:
- `RSAdminBot/COMMANDS.md` - RSAdminBot commands (43 commands)
- `RSForwarder/COMMANDS.md` - RSForwarder commands (12 commands)
- `RSuccessBot/COMMANDS.md` - RSSuccessBot commands (27 commands: 23 prefix + 4 slash)
- `RSMentionPinger/COMMANDS.md` - RSMentionPinger commands (5 commands)
- `RSOnboarding/COMMANDS.md` - RSOnboarding commands (10 commands)
- `RSCheckerbot/COMMANDS.md` - RSCheckerbot commands (12 commands)

#### Viewing Commands

Use RSAdminBot command to view commands:
- `!commands` - Show summary of all bots
- `!commands <bot_name>` - Show all commands for specific bot

#### Canonical Rules for COMMANDS.md

- **One source of truth**: COMMANDS.md is the canonical documentation for each bot's commands
- **No duplication**: Commands are documented ONLY in COMMANDS.md (not in README or other docs)
- **Always synced**: COMMANDS.md files are synced like .py files (always synced, not runtime data)
- **Must match code**: COMMANDS.md MUST accurately reflect actual commands in bot code
- **Update immediately**: COMMANDS.md MUST be updated in the same commit as command changes

## ⚠️ STRICT MODE

This project is considered production-critical.
Stability and clarity take precedence over speed.

### 🧭 RUNTIME PROOF (CANONICAL – MANDATORY)

All debugging, refactoring, cleanup, and verification **MUST** begin with a runtime proof.

#### Canonical runtime proof command

RSAdminBot **must** expose an admin-only command:

```
!whereami
```

#### Required output fields (non-optional)

A valid `!whereami` output MUST include:

```
cwd=<current working directory>
file=<absolute path to executing admin_bot.py>
os=<OS + kernel>
python=<absolute path to python executable>
python_version=<major.minor.patch>
local_exec=<yes|no>
live_root=<expected runtime root>
rsbots_code_head=<git commit hash or unknown>
live_tree_head=<git commit hash or no_git>
```

#### Canonical expected values (Oracle Ubuntu)

On the Oracle Ubuntu server, the following MUST be true:

* `cwd` **starts with**
  `/home/rsadmin/bots/mirror-world`
* `file` **equals**
  `/home/rsadmin/bots/mirror-world/RSAdminBot/admin_bot.py`
* `python` **points to**
  `/home/rsadmin/bots/mirror-world/.venv/bin/python`
* `local_exec=yes`
* `live_root=/home/rsadmin/bots/mirror-world`
* `live_tree_head=no_git` is valid and expected

Any deviation means:

* the wrong code is running, OR
* systemd configuration is incorrect, OR
* an update was applied to the wrong path

#### Enforcement rule

* If `!whereami` output does not match expectations:

  * **STOP**
  * Do **not** refactor
  * Do **not** clean up
  * Do **not** add features
* Fix deployment or execution first.

---

## 3️⃣ Why this directly fixes your current problems

Your pasted output:

```
cwd=/home/rsadmin/bots/mirror-world/RSAdminBot
file=/home/rsadmin/bots/mirror-world/RSAdminBot/admin_bot.py
python=/home/rsadmin/bots/mirror-world/.venv/bin/python
local_exec=yes
live_root=/home/rsadmin/bots/mirror-world
rsbots_code_head=81c6dee2456af976491263af359503d0287fa8d5
live_tree_head=no_git
```


