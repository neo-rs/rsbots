# RSPromoBot

RSPromoBot is a standalone Discord bot for role-based DM promo campaigns.

## What it does

- `/promo_dm` opens a builder panel
- staff selects a target role
- staff writes the promo message in a modal
- preview + test send before launch
- sends in batches using configurable rate limits
- supports pause, resume, cancel, and status tracking
- stores runtime state in JSON files only

## Requirements

- Python 3.11+
- A Discord bot with the `MESSAGE CONTENT` intent **not required**
- Guild Members intent enabled in the Discord Developer Portal
- Bot invited with permissions to read members and use slash commands

## Setup

1. Copy `.env.example` to `.env` and set `DISCORD_TOKEN` to your bot token.
2. Copy `config.json.example` to `config.json` and set:
   - `guild_id`: your Discord server (guild) ID
   - `allowed_launcher_role_ids`: list of role IDs that can run promo commands (Admin/Owner or custom staff roles)
   - `log_channel_id`: channel ID for audit/status logs (optional but recommended)
3. Install packages:

```bash
pip install -r requirements.txt
```

4. Start the bot:

```bash
python promo_bot.py
```

## Config notes

- `allowed_launcher_role_ids`: only these roles can run staff promo commands
- `guild_id`: the server where slash commands are registered
- `log_channel_id`: private audit/status channel
- `default_batch_size` and `default_batch_interval_minutes`: default send rule
- `max_campaign_recipients`: hard stop for large accidental blasts

## Commands

- `/promo_dm` — open builder
- `/promo_status` — show campaign status
- `/promo_pause` — pause active campaign
- `/promo_resume` — resume paused campaign
- `/promo_cancel` — cancel active campaign
- `/promo_history` — recent campaigns

See `COMMANDS.md` for full command and UI documentation.

## Deploy notes

This project is structured so you can drop it into its own service or repo folder and run it as a separate process.

Example systemd unit:

```ini
[Unit]
Description=RSPromoBot
After=network.target

[Service]
Type=simple
User=rsadmin
WorkingDirectory=/home/rsadmin/bots/RSPromoBot
ExecStart=/home/rsadmin/bots/RSPromoBot/.venv/bin/python /home/rsadmin/bots/RSPromoBot/promo_bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Safety notes

- recipients are frozen into a snapshot at launch time
- bots are excluded when `exclude_bots` is true
- active queue state persists in JSON
- preview and test send are available before a real launch

## Canonical cleanup report

Because this is a new standalone bot, there was no superseded bot code removed inside this package.

- Removed: none
- Replaced: none inside this new package
- Canonical: `promo_bot.py` + `promo_views.py` + `promo_sender.py` + JSON stores now own promo DM behavior for this project
