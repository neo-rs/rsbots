# Amazon ASIN Promo Checker

Standalone batch checker for Amazon ASINs.

## What it checks

From PA-API (when enabled and working), we intentionally only use:
- title
- current price + before/list price (when present)
- availability
- deal window (badge + start/end when present)
- primary image URL

From Playwright page view:
- coupons / codes / Subscribe & Save
- ships-from / sold-by (used to compute Merchant Type: AMZ/FBA/FBM/Unknown)
- any fallback fields when PA-API is unavailable

## Windows quick start

Double click:

```bat
run_checker.bat
```

## Linux / Ubuntu quick start

```bash
chmod +x run_checker.sh
./run_checker.sh
```

## PA API keys

The script does **not** hard-code keys. You can paste them when prompted, or set a local `.env` file (recommended), or set env vars.

### Recommended: `.env` (secrets)

1. Copy `amazon_asin_promo_checker/.env.example` to `amazon_asin_promo_checker/.env`
2. Fill in:
   - `PAAPI_ACCESS_KEY`
   - `PAAPI_SECRET_KEY`
   - `PAAPI_PARTNER_TAG`

`.env` is ignored by git.

### Optional: `settings.json` (non-secrets)

If you want defaults for things like Playwright prompts/throttles/output dir, copy:

- `settings.example.json` → `settings.json`

`settings.json` is optional; the tool runs without it.

```bat
set PAAPI_ACCESS_KEY=YOUR_ACCESS_KEY
set PAAPI_SECRET_KEY=YOUR_SECRET_KEY
set PAAPI_PARTNER_TAG=amzlnk05-20
```

Linux:

```bash
export PAAPI_ACCESS_KEY="YOUR_ACCESS_KEY"
export PAAPI_SECRET_KEY="YOUR_SECRET_KEY"
export PAAPI_PARTNER_TAG="amzlnk05-20"
```

## Notes

- Amazon coupons/codes are UI/account/location sensitive. If they do not render in the browser session, the script will return N/A.
- First run should be browser/headful + manual pause so you can set ZIP/login if needed.
- Results save to `output/*.csv` and `output/*.jsonl`.

## Optional: Discord bot automation (Oracle-friendly)

This folder also includes a small Discord bot that listens to a single channel, extracts ASINs, and posts **one result per ASIN**.

### Setup

1. Install deps:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

2. Create `amazon_asin_promo_checker/settings.json` from `settings.example.json` and set:
- `discord_bot.enabled: true`
- `discord_bot.guild_id`: Mirror World server id
- `discord_bot.channel_id`: `amazon-checker` channel id
- `discord_bot.partner_tag`: your associate tag

3. Set env var (secret):
- `DISCORD_BOT_TOKEN`

Important: the bot requires the Discord privileged intent **Message Content Intent** enabled for the application.

### Run

```bash
python discord_bot.py
```
