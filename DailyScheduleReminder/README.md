# Daily Schedule Reminder

Sends reminders to a Discord channel **30 minutes before** each drop in the **DAILY SCHEDULE** category. Uses [Discum](https://github.com/Merubokkusu/Discord-S.C.U.M) (user token).

## Setup

1. **Token**  
   Copy `config.secrets.example.json` to `config.secrets.json` and set your Discord **user** token, or set env var `DISCORD_USER_TOKEN`.

2. **Config** (optional)  
   Edit `config.json` to change:
   - `category_id` – category containing the schedule channels (default: DAILY SCHEDULE)
   - `reminder_channel_id` – channel where reminders are posted
   - `reminder_mins_before` – minutes before drop (default: 30)
   - `ticket_startup.enabled` – if `true`, each minute the bot also reads RSCheckerbot’s pending ticket startup list and sends the “checking in” message to each ticket channel **as the Discord user** (not as RSCheckerbot). Requires RSCheckerbot’s `startup_messages.external_sender_enabled: true` and the same repo root

## Channel names

Channels under the category are parsed for:

- **Time**: `9pm-est`, `10am-local`, `12pm-est`, `1pm` (no suffix = EST). All times are treated as **EST**.
- **Drop name**: From the part after the time (or after ` | `). Leading numbers/dates are stripped; hyphens become spaces and the result is title-cased (e.g. `walmart-tuesday` → **Walmart Tuesday**).

Examples:

- `# 9pm-est | walmart-tuesday` → 9:00 PM EST, **Walmart Tuesday**
- `# 10am-local-fuerza-regida` → 10:00 AM EST, **Fuerza Regida**
- `# 12pm-est | 2025-26-topps-basketball-tuesday` → 12:00 PM EST, **Topps Basketball Tuesday**

## Run

From the **mirror-world** repo root (so Discumraw is on the path):

```bash
cd DailyScheduleReminder
python reminder_bot.py
```

**Default: stays on** and checks every minute. Channels in the category are re-read each time, so new or moved channels are picked up automatically. Reminders go out 30 minutes before each drop.

- **One-shot** (for cron/task scheduler): run once and exit. Use `--once` if you trigger the script on a schedule instead of leaving it running.
  ```bash
  python reminder_bot.py --once
  ```

## Testing (channel outside the category)

You can test with any channel (e.g. in "Testing Stuff") whose name has a time + event (e.g. `# 10pm-est-testing-stuffup`).

**Option 1 – CLI (one-off)**  
Send a test reminder for a specific channel by ID. The reminder is posted to your configured `reminder_channel_id` with a "Test reminder" header:

```bash
python reminder_bot.py --test <channel_id>
```

Example: right‑click the channel (e.g. `# 10pm-est-testing-stuffup`) → "Copy channel ID", then:

```bash
python reminder_bot.py --test 1234567890123456789
```

**Option 2 – Discord command**  
Run the bot in listen mode; then in the command channel type `!reminder` followed by the channel (mention or ID):

```bash
python reminder_bot.py --listen
```

In Discord type:

- `!reminder #10pm-est-testing-stuffup` (use the # to mention the channel), or  
- `!reminder 1234567890123456789`

By default the bot listens in the **reminder channel**. To use a different channel (e.g. `# test-reminder`), set in `config.json`:

```json
"command_channel_id": "ID_OF_TEST_REMINDER_CHANNEL"
```

Reminders use the formats you specified: first drop of the day gets the main heading; later drops get **## UP NEXT** and the same structure with `<t:TIMESTAMP:R>`, drop name, and channel link.

---

## Oracle server (Ubuntu)

DailyScheduleReminder runs on the same Oracle host as RSCheckerbot (required when using `ticket_startup.enabled`, so it can read `RSCheckerbot/data/`).

### Quick setup (Windows → Oracle)

**Step 1: Upload config and secrets**  
From Windows (repo root), run:
```cmd
DailyScheduleReminder\upload_config_oracle.bat
```
This uploads `config.json` and `config.secrets.json` (if it exists) to the Oracle server.

**Step 2: SSH to Oracle and run setup**  
```bash
ssh -i oraclekeys\ssh-key-2025-12-15.key rsadmin@137.131.14.157
bash /home/rsadmin/bots/mirror-world/DailyScheduleReminder/setup_oracle.sh
```

**Step 3: Verify it's running**  
From Windows:
```cmd
DailyScheduleReminder\verify_oracle.bat
```
Or from Oracle:
```bash
sudo systemctl status mirror-world-dailyschedulereminder.service
systemctl show mirror-world-dailyschedulereminder.service --property=MainPID --value
```

### Manual setup (if needed)

1. **Secrets (not synced)**  
   Create on the server (do **not** commit):
   - `DailyScheduleReminder/config.secrets.json`  
   With a Discord **user** token:
   ```json
   {"token": "YOUR_DISCORD_USER_TOKEN"}
   ```
   Or set env `DISCORD_USER_TOKEN` for the service if you prefer.

2. **Config**  
   `config.json` is synced with the repo. Adjust on server if needed (e.g. `category_id`, `reminder_channel_id`, `ticket_startup.enabled`).

3. **Install systemd unit**  
   From repo root on Oracle:
   ```bash
   bash RSAdminBot/install_services.sh
   ```
   This installs `mirror-world-dailyschedulereminder.service`. Start it after secrets are in place:
   ```bash
   sudo systemctl start mirror-world-dailyschedulereminder.service
   sudo systemctl enable mirror-world-dailyschedulereminder.service
   ```

### Service control

**From Oracle:**
- Start/stop/restart/status:
  ```bash
  bash RSAdminBot/botctl.sh start   dailyschedulereminder
  bash RSAdminBot/botctl.sh stop    dailyschedulereminder
  bash RSAdminBot/botctl.sh restart dailyschedulereminder
  bash RSAdminBot/botctl.sh status   dailyschedulereminder
  ```
- Or via mirror bots script: `bash RSAdminBot/manage_mirror_bots.sh restart dailyschedulereminder`

**From Discord (RSAdminBot):**
- `/botupdate` → select "Daily Schedule Reminder" (updates from GitHub and restarts)
- `/botstatus` → select "Daily Schedule Reminder" (shows systemd status)
- `/logs` → select "Daily Schedule Reminder" (shows journal logs)
- `/botinfo` → select "Daily Schedule Reminder" (shows metadata + PID)

### Journal logging (Discord)

RSAdminBot automatically creates a journal channel (`#journal-dailyschedulereminder`) in the Test Server when `journal_live.enabled: true` (default). Live logs stream to Discord in real-time.

### Sync rules

- **Synced:** `config.json`, all `.py` files, `README.md`, `requirements.txt`
- **Not synced:** `config.secrets.json` (create on server only; upload via `upload_config_oracle.bat`)
