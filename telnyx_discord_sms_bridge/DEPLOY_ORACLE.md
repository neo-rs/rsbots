# Oracle deploy (canonical)

Live path on Oracle:

```txt
/home/rsadmin/bots/mirror-world/telnyx_discord_sms_bridge
```

Public inbound webhook URL (already wired via nginx on this server):

```txt
https://137.131.14.157.sslip.io/webhooks/telnyx
```

## One-time server secrets

Create `.env` on the server (never commit):

```bash
nano /home/rsadmin/bots/mirror-world/telnyx_discord_sms_bridge/.env
```

Required keys: `TELNYX_API_KEY`, `TELNYX_FROM_NUMBER`, `DISCORD_WEBHOOK_URL`, `BRIDGE_API_KEY`.

Conversation mode (phone-style threads + buttons) also needs:

```env
DISCORD_BOT_TOKEN=your_bot_token
DISCORD_CHANNEL_ID_2540=channel_id_for_local_line
DISCORD_CHANNEL_ID_2119=channel_id_for_toll_free_line
```

Optional legacy webhook fallback per line (if bot token missing):

```env
DISCORD_WEBHOOK_URL_2540=...
DISCORD_WEBHOOK_URL_2119=...
```

Upload secrets without git:

```bat
scp_env_oracle.bat
```

## Deploy workflow (Windows → Oracle)

### 1) Push code to GitHub

From repo root:

```bat
push_rsbots_py_only.bat
```

### 2) Deploy to Oracle

Fast path (upload from this PC):

```bat
update_telnyx_bridge.bat
```

Git-based path (after push, uses `rsbots-code` on server):

```bat
py -3 scripts\run_oracle_update_bots.py --group rs --bot telnyxsmsbridge
```

### 3) RSAdminBot + journal logging (required for `!botctl` + `#journal-telnyxsmsbridge`)

The bridge folder update alone does **not** sync `RSAdminBot`. For journal live logs and `botctl` routing:

```bat
py -3 scripts\run_oracle_update_bots.py --group rs --bot rsadminbot
```

RSAdminBot restarts on its own schedule (staged apply). After it reloads, run in TestCenter:

```txt
!setupmonitoring
```

That creates/refreshes `#journal-telnyxsmsbridge` and starts `journalctl -u mirror-world-telnyx-discord-sms-bridge` streaming.

First-time service/nginx on server:

```bash
bash /home/rsadmin/bots/mirror-world/telnyx_discord_sms_bridge/install_oracle.sh
```

### 4) Restart / status

```bash
sudo systemctl restart mirror-world-telnyx-discord-sms-bridge.service
sudo systemctl status mirror-world-telnyx-discord-sms-bridge.service
journalctl -u mirror-world-telnyx-discord-sms-bridge -f
```

Or via RSAdminBot:

```txt
!botctl restart telnyxsmsbridge
```

## Telnyx Messaging Profile webhook

In Telnyx → Messaging → **Reselling Secrets SMS** → set:

| Field | Value |
|-------|-------|
| Webhook URL | `https://137.131.14.157.sslip.io/webhooks/telnyx` |
| Webhook API version | `2` |

This replaces the old n8n URL (`https://n8n.ie-manage.com/webhook/send-message`) if you want inbound SMS in Discord.

## Verify

```bash
curl http://127.0.0.1:8787/health
curl -i https://137.131.14.157.sslip.io/webhooks/telnyx
```

Send a test SMS to either Telnyx number; Discord should show an **📨 Inbound SMS** embed.

## Cleanup report

- **Removed:** manual-only deploy notes without git/update path
- **Replaced:** ad-hoc systemd paste with `install_oracle.sh` + `mirror-world-telnyx-discord-sms-bridge.service`
- **Canonical:** `scripts/run_oracle_deploy_telnyx_bridge.py`, `update_telnyx_bridge.bat`, `install_oracle.sh`
