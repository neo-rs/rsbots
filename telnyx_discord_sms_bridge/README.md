# Telnyx Discord SMS Bridge

A small deploy-ready Python service for:

- Showing inbound Telnyx SMS/MMS messages in Discord
- Sending outbound SMS through Telnyx from a local HTTP endpoint or CLI
- Explainable terminal logging
- No hardcoded secrets
- Windows batch setup/run support

## Flow

```txt
Inbound:
Customer SMS -> Telnyx webhook -> this app -> Discord webhook

Outbound:
You/automation -> this app /send endpoint or send_sms.py -> Telnyx Messages API
```

## What you need

1. A Telnyx API key
2. A Telnyx SMS-capable number
3. A Discord channel webhook URL
4. A public HTTPS URL pointing to this app for Telnyx inbound webhook delivery

For production on Oracle, use:

```txt
https://137.131.14.157.sslip.io/webhooks/telnyx
```

See `DEPLOY_ORACLE.md` for push/deploy workflow.

## Quick Windows setup

From this folder:

```bat
setup_windows.bat
```

Then edit:

```txt
.env
config\settings.json
```

Then run:

```bat
run_server.bat
```

## Telnyx setup

In Telnyx:

1. Go to your Messaging Profile.
2. Set the inbound webhook URL to:

```txt
https://YOUR_PUBLIC_DOMAIN/webhooks/telnyx
```

3. Send a test SMS to your Telnyx number.
4. The message should appear in your configured Discord channel.

## Discord setup

1. Open Discord channel settings.
2. Go to Integrations -> Webhooks.
3. Create a webhook.
4. Copy the webhook URL into `.env`:

```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

## Outbound SMS

### Option A: batch file

```bat
send_sms.bat +15551234567 "Testing from bridge"
```

### Option B: direct Python CLI

```bat
.venv\Scripts\python.exe send_sms.py --to +15551234567 --text "Testing from bridge"
```

### Option C: HTTP endpoint

```bash
curl -X POST http://127.0.0.1:8787/send \
  -H "Content-Type: application/json" \
  -H "X-Bridge-Key: YOUR_LOCAL_BRIDGE_API_KEY" \
  -d "{\"to\":\"+15551234567\",\"text\":\"Testing\"}"
```

## Required `.env` values

```env
TELNYX_API_KEY=
TELNYX_FROM_NUMBER=
DISCORD_WEBHOOK_URL=
BRIDGE_API_KEY=
```

## Optional `.env` values

```env
APP_HOST=0.0.0.0
APP_PORT=8787
CONFIG_PATH=config/settings.json
LOG_LEVEL=INFO
TELNYX_PUBLIC_KEY=
TELNYX_REQUIRE_SIGNATURE=false
TELNYX_MESSAGING_PROFILE_ID=
```

## Webhook signature verification

This app supports optional Telnyx webhook signature verification.

Set:

```env
TELNYX_PUBLIC_KEY=your_telnyx_public_key
TELNYX_REQUIRE_SIGNATURE=true
```

If signature verification is enabled and the request fails verification, the app rejects the webhook.

## Logs

Terminal logs are intentionally explainable. You will see messages like:

```txt
event=inbound_received reason=telnyx_webhook_parsed from=+1... to=+1... chars=42
event=discord_post_success reason=inbound_message_forwarded status=204
event=outbound_send_success reason=telnyx_api_accepted to=+1... telnyx_id=...
```

A rotating app log is also written to:

```txt
logs/bridge.log
```

## Oracle deploy

```bat
push_rsbots_py_only.bat
update_telnyx_bridge.bat
```

Then set Telnyx Messaging Profile webhook to `https://137.131.14.157.sslip.io/webhooks/telnyx`.

## Cleanup report

This is a new standalone bridge, so no existing code was removed or replaced.

- Removed: none
- Replaced: none
- Canonical owner now: `app/main.py` for inbound/outbound routing, `app/config.py` for config, `app/discord_client.py` for Discord posting, `app/telnyx_client.py` for Telnyx sends
