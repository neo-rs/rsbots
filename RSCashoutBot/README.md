# RS Cashout Ticket Bot

A config-driven Discord ticket system for RS cashout requests. This setup follows the uploaded project rule set: no parallel logic, no hardcoded runtime values in code, JSON-only storage, and one source of truth through config/messages files.

## What it does
- Posts a private cashout ticket panel in one specific channel
- Creates ticket channels inside one configured category
- Includes three button flows:
  - Request/Submit (quotes + submissions)
  - Signup Form
  - Need Help?
- Uses modals for structured intake instead of messy freeform messages
- Stores open tickets in `tickets.json`
- Optionally forwards Signup Form submissions to Google Sheets through a configurable Apps Script endpoint
- Uses your RS emoji on all panel actions

## Files
- `bot.py` → canonical runtime bot
- `config.json` → runtime configuration (copy from `config.example.json`; in this repo it is **gitignored** so tokens are not committed)
- `config.example.json` → safe template copy
- `messages.json` → all panel/ticket copy in one place
- `tickets.json` → runtime JSON storage created automatically
- `google_apps_script_example.js` → optional Google Sheets intake example
- `COMMANDS.md` → command documentation
- `run_rscashout_bot.bat` → Windows launcher (`py -3` or `python`)

## Setup
1. Create a Discord bot in the Discord Developer Portal.
2. Enable **Server Members Intent** for the bot.
3. Invite the bot with permissions for:
   - Manage Channels
   - View Channels
   - Send Messages
   - Embed Links
   - Read Message History
   - Manage Messages
4. Open `config.json` and set:
   - `discord_bot_token`
   - real `support_role_ids`
   - real `admin_role_ids`
   - optional `transcript_channel_id` (staff log: **Ticket opened** when someone uses Request/Submit, Signup, or Help, and **Ticket closed** when the channel is closed)
5. Leave the supplied panel channel and category IDs as-is, or swap them in config only.
6. Install dependencies:
   - `pip install -r requirements.txt`
7. Run the bot:
   - Double-click `run_rscashout_bot.bat`, or from this folder: `py -3 bot.py` / `python bot.py`
8. On startup the bot **posts the panel embed + buttons** to `panel_channel_id` when `auto_post_panel_on_ready` is `true` (default). You can still run `/ticketpanel` in that same channel anytime to post another copy.
9. To stop auto-posting on every restart (avoid duplicate cards), set `ticket_system.auto_post_panel_on_ready` to `false` in `config.json`.

## Google Sheets intake
If you want the Signup Form button to populate a sheet:
1. Create a Google Sheet tab called `Signup Form`
2. Deploy the sample Apps Script as a web app
3. Put the web app URL into `google_sheet.endpoint_url`
4. Set `google_sheet.enabled` to `true`
5. Keep the route name `signup_form` unless you change it in both config and script

## Recommended copy for your two custom buttons
### Request/Submit
Use for custom quotes (unlisted items, price match) or submitting what you are sending in for staff review—see `messages.json` under `request_submit`.

### Signup Form
Fill out your cashout onboarding info so staff can review your profile faster, verify what you ship, and keep your payout setup organized.

### Need Help?
Open a private help ticket for payout questions, shipping issues, missing updates, account access problems, or anything else you need staff to review.

## Oracle server (Ubuntu)

Canonical live root: `/home/rsadmin/bots/mirror-world`. The bot runs under systemd as **`mirror-world-rscashoutbot.service`** (see repo `systemd/mirror-world-rscashoutbot.service`).

1. **Git:** Ensure `RSCashoutBot/` is pushed with the rest of the RS mirror-world repo (`push_rsbots_py_only.bat` → `update_rs_bots.bat` or Discord **`!botupdate`** → pick **`rscashoutbot`**).
2. **First-time on the box:** Install/refresh units: `bash RSAdminBot/install_services.sh` (copies `systemd/*.service`, enables services). Then create **`RSCashoutBot/config.json`** on the server (copy from `config.example.json`, set `discord_bot_token` and IDs); it is **not** in git.
3. **Venv:** Shared `.venv` — run `bash RSAdminBot/bootstrap_venv.sh` so `RSCashoutBot/requirements.txt` is installed (discord.py, aiohttp).
4. **Start:** `bash RSAdminBot/botctl.sh restart rscashoutbot` or `sudo systemctl restart mirror-world-rscashoutbot.service`.

Runtime files **`tickets.json`** and **`bot.log`** stay on the server and are not synced from git.

## Config note (Request/Submit)
The first panel button uses config key `request_submit` (label **Request/Submit**). If you still have open tickets stored as `request_custom_quote` in `tickets.json`, the bot still counts them toward the one-open limit for this flow.

## Cleanup report
- Removed: hardcoded button text and channel/category usage from code paths
- Replaced: fixed inline strings with `config.json` + `messages.json`
- Canonical now: `bot.py` for logic, `config.json` for IDs/settings, `messages.json` for wording
