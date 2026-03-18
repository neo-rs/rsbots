# Whop Membership to Google Sheets Sync

Syncs Whop API membership data to Google Sheets for integration with GoHighLevel, KIT, and N8N.

## Setup

1. **Install dependencies:**
   ```bash
   pip install google-api-python-client google-auth aiohttp
   ```

2. **Configuration:**
   - `config.json` - Main configuration (already configured with product IDs)
   - `config.secrets.json` - Contains Whop API key and Google service account (already configured)

3. **Product IDs configured:**
   - Reselling Secrets: `prod_RrcvGelB8tVgu` → Tab: "Whop API - Reselling Secrets"
   - Reselling Secrets Lite: `prod_U52ytqRZdCFak` → Tab: "Whop API - Reselling Secrets Lite"

## Usage

### Run Once (Manual)
```bash
cd WhopMembershipSync
python main.py
```

### Continuous Sync Mode
Enable continuous sync in `config.json`:
```json
"continuous_sync": {
  "enabled": true,
  "check_interval_minutes": 15
}
```

Then run:
```bash
python main.py
```

The script will run continuously, checking for new members every 15 minutes (or your configured interval).

## How It Works

### Startup/Initial Run
1. **Syncs both products** (Reselling Secrets & Reselling Secrets Lite):
   - Fetches all memberships from Whop API
   - **Clears** and **writes** to respective tabs

2. **Segregates by Status** (Reselling Secrets only):
   - Reads from "Whop API - Reselling Secrets" tab
   - Groups members by their status
   - Writes to status-specific tabs:
     - Churned
     - Cancelling
     - Cancelled
     - Renewing
     - Left
     - Active
     - Trialing

### Continuous Mode
When `continuous_sync.enabled` is `true`:

1. **Initial sync** (runs once at startup):
   - Full sync of both products
   - Segregate by status

2. **Continuous cycles** (runs every N minutes):
   - **Updates source tab** ("Whop API - Reselling Secrets"):
     - Fetches current memberships from Whop API
     - Adds new members OR updates existing members (if status changed)
     - Checks by email/Discord ID to avoid duplicates
   - **Segregates by status**:
     - Reads updated source tab
     - Groups by status
     - Updates status tabs accordingly

### Status Tab Behavior
- **Source tab** ("Whop API - Reselling Secrets"): Incremental updates (add/update)
- **Status tabs** (Churned, Cancelling, etc.): Always cleared and rewritten (full sync)
- **Reselling Secrets Lite**: Always cleared and rewritten (full sync)

## Output Format

Each tab will have columns:
- **Name** - Member name
- **Phone Number** - Member phone number (if available)
- **Email** - Member email
- **Product** - Product name
- **Status** - Membership status (active, canceled, etc.)
- **Discord ID** - Discord ID if connected

## Integration

The synced data can be used by:
- **GoHighLevel (GHL)** - via webhook or API
- **KIT** - via webhook or API  
- **N8N** - via Google Sheets trigger or webhook

## Notes

- **Reselling Secrets Lite**: Always clears and replaces (full sync)
- **Reselling Secrets**: Initial sync clears/replaces, then status-based sync adds incrementally
- Discord IDs are extracted from `connected_accounts` in Whop member records
- Member records are fetched to get email/phone when available
- Duplicate prevention: Checks email and Discord ID before adding
