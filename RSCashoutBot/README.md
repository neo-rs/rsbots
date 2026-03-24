# RS Cashout Ticket Bot

A config-driven Discord ticket system for RS cashout requests. This setup follows the uploaded project rule set: no parallel logic, no hardcoded runtime values in code, JSON-only storage, and one source of truth through config/messages files.

## What it does
- Posts a private cashout ticket panel in one specific channel
- Creates ticket channels inside one configured category
- Includes three button flows:
  - Request Custom Quote
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
   - optional `transcript_channel_id`
5. Leave the supplied panel channel and category IDs as-is, or swap them in config only.
6. Install dependencies:
   - `pip install -r requirements.txt`
7. Run the bot:
   - `python bot.py`
8. In the configured panel channel, run:
   - `/ticketpanel`

## Google Sheets intake
If you want the Signup Form button to populate a sheet:
1. Create a Google Sheet tab called `Signup Form`
2. Deploy the sample Apps Script as a web app
3. Put the web app URL into `google_sheet.endpoint_url`
4. Set `google_sheet.enabled` to `true`
5. Keep the route name `signup_form` unless you change it in both config and script

## Recommended copy for your two custom buttons
### Signup Form
Fill out your cashout onboarding info so staff can review your profile faster, verify what you ship, and keep your payout setup organized.

### Need Help?
Open a private help ticket for payout questions, shipping issues, missing updates, account access problems, or anything else you need staff to review.

## Cleanup report
- Removed: hardcoded button text and channel/category usage from code paths
- Replaced: fixed inline strings with `config.json` + `messages.json`
- Canonical now: `bot.py` for logic, `config.json` for IDs/settings, `messages.json` for wording
