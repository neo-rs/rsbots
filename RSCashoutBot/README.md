# RS Cashout Ticket Bot

Discord cashout intake bot for the RS cashout team.

## Current flow
- Posts one cashout panel in the configured channel
- Button set is now:
  - **Request/Submit**
  - **Need Help?**
- **Request/Submit** uses a multi-step modal so the fields can match your sheet headers plus a required email
- On submit, the bot can:
  - create a personal copy of your Google Sheet template through Apps Script
  - add the submitted email as an editor
  - leave the copy as **anyone with the link can view**
  - open a private ticket under the cashout category
  - ping the configured Cashout Mod role in that ticket
  - DM the member their sheet link for backup
- The bot stores the member's last Request/Submit values in `profiles.json`, so repeat submissions can auto-fill previous data, especially the email
- Open and close events still log to `transcript_channel_id` when that channel is configured

## Files
- `bot.py` → canonical runtime bot
- `config.json` → live runtime config
- `config.example.json` → safe template copy
- `messages.json` → panel copy + ticket copy
- `tickets.json` → open ticket storage
- `profiles.json` → saved member defaults / last cashout submission values
- `google_apps_script_example.js` → Apps Script sample for template-copy flow
- `COMMANDS.md` → command list
- `run_rscashout_bot.bat` → Windows launcher

## Required setup
1. Set your real Discord role IDs in `config.json`
   - `support_role_ids` should be the Cashout Mod role you want pinged
   - `admin_role_ids` should be the staff roles allowed to run `/cashout`
2. Install Python dependencies
   - `pip install -r requirements.txt`
3. Run the bot
   - `py -3 bot.py`

## Google Sheet template-copy setup
1. Make your template sheet and copy its spreadsheet ID
2. Deploy `google_apps_script_example.js` as a web app
3. In the script, set:
   - `templateSpreadsheetId`
   - `apiKey`
   - optional `rootFolderId`
   - optional `signatureImageUrl` for the banner in cell `A1`
   - optional `logSpreadsheetId`
4. In `config.json`, set:
   - `google_sheet.enabled = true`
   - `google_sheet.endpoint_url = <your web app URL>`
   - `google_sheet.auth_token = <same api key>`
5. Keep the Request/Submit route as `cashout_submit` unless you change both files

## Template mapping used by the bot
The Request/Submit modal writes these values into the sheet copy:
- `A3` → Name of Shoe
- `B3` → SKU
- `C3` → Condition
- `D3` → Size
- `E3` → QTY
- `F3` → Price
- `G3` → Notes
- `J1` → member email metadata

## Notes
- Discord modals only support 5 inputs at a time, so Request/Submit is split into multiple modal steps automatically
- `max_open_per_user` for Request/Submit is set to `0`, which means unlimited open cashout tickets
- `/cashoutnew` is the quick repeat-submission command for members
