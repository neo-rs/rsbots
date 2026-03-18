# Whop Membership Sync - Complete Flow Documentation

## Overview
This system syncs Whop API membership data to Google Sheets, then segregates members by status into separate tabs for integration with GoHighLevel (GHL), KIT, and N8N.

---

## 🚀 Entry Point: `main.py`

### Step 1: Configuration Loading (`load_config()`)
```
1. Load config.json (public config)
   ├─ spreadsheet_id
   ├─ products[] (product_id, tab_name, enabled)
   ├─ status_tabs (enabled, source_tab, status_mapping)
   └─ continuous_sync (enabled, check_interval_minutes)

2. Load config.secrets.json (private secrets)
   ├─ whop_api.api_key
   └─ google_service_account_json

3. Fallback: Try RSForwarder/config.secrets.json for Google credentials
```

### Step 2: Mode Selection (`main()`)
```
IF continuous_sync.enabled == true:
    → Run INITIAL SYNC (full sync)
    → Then loop CONTINUOUS CYCLES every check_interval_minutes
ELSE:
    → Run ONCE (initial sync only)
```

---

## 📋 INITIAL SYNC Flow (`run_sync_once()` → `sync_all_products()`)

### Phase 1: Initialize Clients
```
1. Initialize WhopAPIClient
   ├─ API Key from config.secrets.json
   ├─ Base URL: https://api.whop.com/api/v1
   └─ Company ID from config.json

2. Initialize WhopSheetsSync
   ├─ Load Google Service Account JSON
   └─ Build Google Sheets API service
```

### Phase 2: Sync Each Product (`sync_product_memberships()`)

**For each enabled product in config.json:**

#### 2.1: Ensure Tab Exists (`_ensure_sheet_tab()`)
```
- Check if tab already exists in Google Sheet
- If exists → use existing tab name
- If not → create new tab with headers
- Handle variations (e.g., "Cancelled" vs "Canceled")
```

#### 2.2: Fetch Memberships from `/memberships` Endpoint
```
For each status: [trialing, active, past_due, completed, expired, unresolved, drafted]
  ├─ Call whop_client.list_memberships(product_id, status)
  ├─ Paginate through all pages (max 50 pages per status)
  ├─ Deduplicate by membership ID
  └─ Collect all memberships

Note: "canceled" status is SKIPPED (we removed it)
Note: "canceling" is detected later via cancel_at_period_end=true
```

#### 2.3: Fetch Special Status Members from `/members` Endpoint
```
Call whop_client.list_members(product_id)
  ├─ Filter for members with status="left"
  ├─ Filter for members with most_recent_action="churned"
  ├─ Filter for members with most_recent_action="renewing"
  ├─ Verify product association (check memberships[] array)
  └─ Create "fake" membership objects for these special members
```

#### 2.4: Fetch Detailed Member Records
```
Extract unique member IDs from all memberships
For each member_id:
  ├─ Call whop_client.get_member_by_id(member_id)
  ├─ Extract Discord ID from connected_accounts
  ├─ Extract phone number
  └─ Cache in member_cache dictionary

Note: NO LIMIT - fetches ALL members (removed 500 limit)
```

#### 2.5: Build Rows with Deduplication
```
For each membership:
  ├─ Extract: Name, Email, Product, Status
  ├─ Get Phone from member_cache
  ├─ Get Discord ID from member_cache
  ├─ Detect "canceling": status=active/trialing AND cancel_at_period_end=true
  └─ Skip if status="canceled"

Deduplication Logic:
  ├─ Group by email (or Discord ID if no email)
  ├─ Status Priority: canceling > renewing > active > trialing > churned > ... > left
  ├─ IMPORTANT: Active memberships override "left" status
  └─ Each member appears in ONLY ONE status (highest priority)

Result: List of unique members with their current status
```

#### 2.6: Write to Google Sheet
```
1. Clear existing data in tab
2. Write headers: ["Name", "Phone Number", "Email", "Product", "Status", "Discord ID"]
3. Write all deduplicated rows
4. Log status breakdown
```

### Phase 3: Segregate by Status (`segregate_by_status()`)

**Only runs for source_tab (usually "Whop API - Reselling Secrets"):**

#### 3.1: Read Source Tab
```
Read all rows from "Whop API - Reselling Secrets" tab
Skip header row
```

#### 3.2: Group by Status
```
For each row:
  ├─ Extract status from column E (Status column)
  ├─ Map status to tab name using status_mapping:
  │   ├─ "active" → "Active"
  │   ├─ "canceling" → "Canceling"
  │   ├─ "trialing" → "Trialing"
  │   ├─ "left" → "Left"
  │   ├─ "churned" → "Churned"
  │   └─ "renewing" → "Renewing"
  └─ Group rows by destination tab name
```

#### 3.3: Write to Status Tabs
```
For each status tab (Active, Canceling, Trialing, Left, Churned, Renewing):
  ├─ Find existing tab (handle variations like "Cancelled" vs "Canceled")
  ├─ Clear existing data
  ├─ Write headers
  ├─ Write all rows for that status
  └─ Log count and sample rows
```

---

## 🔄 CONTINUOUS SYNC Flow (`run_continuous_cycle()` → `sync_continuous_cycle()`)

**Runs every `check_interval_minutes` (default: 15 minutes)**

### Step 1: Update Source Tab Incrementally (`sync_source_incremental()`)

#### 1.1: Read Existing Data
```
Read "Whop API - Reselling Secrets" tab
Build lookup maps:
  ├─ existing_by_email: email → row_index
  └─ existing_by_discord: discord_id → row_index
```

#### 1.2: Fetch Current Memberships
```
Same as Initial Sync Phase 2.2 and 2.3:
  ├─ Fetch from /memberships endpoint (all statuses)
  ├─ Fetch special status members from /members endpoint
  └─ Fetch detailed member records (all members, no limit)
```

#### 1.3: Deduplicate and Build Updated Rows
```
Same deduplication logic as Initial Sync:
  ├─ Group by email/Discord ID
  ├─ Apply status priority
  └─ Each member → one status (highest priority)
```

#### 1.4: Compare with Existing
```
For each deduplicated member:
  ├─ Check if exists by email or Discord ID
  ├─ If exists → Update row (rows_to_update)
  └─ If new → Add row (rows_to_add)
```

#### 1.5: Write Updates
```
Update existing rows in place
Append new rows to end
Write back to source tab
```

### Step 2: Segregate Updated Data (`segregate_by_status()`)
```
Same as Initial Sync Phase 3:
  ├─ Read updated source tab
  ├─ Group by status
  └─ Write to each status tab (clear and rewrite)
```

---

## 🔗 How Steps Connect

```
START (main.py)
  │
  ├─ Load Config
  │   ├─ config.json
  │   └─ config.secrets.json
  │
  ├─ Initialize Clients
  │   ├─ WhopAPIClient
  │   └─ WhopSheetsSync
  │
  └─ IF continuous_sync.enabled:
      │
      ├─ INITIAL SYNC (run_sync_once)
      │   │
      │   ├─ sync_all_products()
      │   │   │
      │   │   ├─ For each product:
      │   │   │   ├─ sync_product_memberships()
      │   │   │   │   ├─ Fetch from /memberships
      │   │   │   │   ├─ Fetch from /members (special statuses)
      │   │   │   │   ├─ Fetch detailed records
      │   │   │   │   ├─ Deduplicate
      │   │   │   │   └─ Write to product tab
      │   │   │   │
      │   │   └─ segregate_by_status()
      │   │       ├─ Read source tab
      │   │       ├─ Group by status
      │   │       └─ Write to status tabs
      │   │
      │   └─ LOOP: Continuous Cycles
      │       │
      │       └─ sync_continuous_cycle()
      │           │
      │           ├─ sync_source_incremental()
      │           │   ├─ Read existing data
      │           │   ├─ Fetch current memberships
      │           │   ├─ Deduplicate
      │           │   ├─ Compare & update
      │           │   └─ Write updates
      │           │
      │           └─ segregate_by_status()
      │               ├─ Read updated source tab
      │               ├─ Group by status
      │               └─ Write to status tabs
      │
      └─ ELSE: Run once (same as INITIAL SYNC)
```

---

## 📊 Data Flow Diagram

```
Whop API
  │
  ├─ /memberships endpoint
  │   ├─ trialing, active, past_due, completed, expired, unresolved, drafted
  │   └─ Each membership has: user, member, product, status, cancel_at_period_end
  │
  └─ /members endpoint
      ├─ status="left"
      ├─ most_recent_action="churned"
      └─ most_recent_action="renewing"
      │
      └─ /members/{member_id} (detailed)
          ├─ phone
          └─ connected_accounts (Discord ID)

         ↓

Deduplication Engine
  ├─ Group by email/Discord ID
  ├─ Apply status priority
  └─ Output: One row per member with current status

         ↓

Google Sheets - Source Tab
  └─ "Whop API - Reselling Secrets"
      ├─ All members (deduplicated)
      └─ Columns: Name, Phone, Email, Product, Status, Discord ID

         ↓

Segregation Engine
  ├─ Read source tab
  ├─ Group by status
  └─ Write to status tabs

         ↓

Google Sheets - Status Tabs
  ├─ Active
  ├─ Canceling
  ├─ Trialing
  ├─ Left
  ├─ Churned
  └─ Renewing
```

---

## 🎯 Key Design Decisions

### 1. **Two-Endpoint Strategy**
- `/memberships`: Provides core membership statuses (active, trialing, etc.)
- `/members`: Provides user-level statuses (left, churned, renewing)
- **Why**: Whop API doesn't expose all statuses in one endpoint

### 2. **Deduplication Priority**
- Active memberships override "left" status
- **Why**: A member can have an active membership AND appear in "left" list
- **Rule**: If they have an active membership, they're NOT "left"

### 3. **Status Detection**
- "canceling": Detected via `cancel_at_period_end=true` AND `status=active/trialing`
- **Why**: "canceling" is a filter parameter, not a direct status value
- "canceled": Removed - focus on "canceling" only

### 4. **Incremental Updates**
- Continuous sync reads existing data first
- Compares by email/Discord ID
- Updates existing rows, adds new ones
- **Why**: Avoids data bloat, faster than full clear-and-write

### 5. **Tab Detection**
- Checks for existing tabs before creating
- Handles variations (Cancelled vs Canceled)
- **Why**: Prevents duplicate tabs, uses existing structure

---

## ⚙️ Configuration Structure

```json
{
  "enabled": true,
  "spreadsheet_id": "...",
  "products": [
    {
      "product_id": "prod_...",
      "tab_name": "Whop API - Reselling Secrets",
      "enabled": true
    }
  ],
  "status_tabs": {
    "enabled": true,
    "source_tab": "Whop API - Reselling Secrets",
    "status_mapping": {
      "active": "Active",
      "canceling": "Canceling",
      "trialing": "Trialing",
      "left": "Left",
      "churned": "Churned",
      "renewing": "Renewing"
    }
  },
  "continuous_sync": {
    "enabled": true,
    "check_interval_minutes": 15
  }
}
```

---

## 🔍 Troubleshooting Flow

If something goes wrong:

1. **Check logs** - All steps are logged with progress indicators
2. **Verify config** - Ensure `enabled: true` for products and status_tabs
3. **Check API limits** - Removed 500 limit, but rate limiting may apply
4. **Verify deduplication** - Check if "left" is overriding active memberships
5. **Check tab names** - Ensure status tabs exist or will be created

---

## 📝 Summary

**The code is fully connected and follows a clear pipeline:**

1. **Config** → Loads settings and credentials
2. **Initialize** → Sets up API clients
3. **Fetch** → Gets data from Whop API (two endpoints)
4. **Process** → Deduplicates and determines current status
5. **Write** → Updates Google Sheets (source tab)
6. **Segregate** → Splits source tab into status-specific tabs
7. **Repeat** → Continuous sync updates incrementally

Each step feeds into the next, creating a complete data pipeline from Whop API → Google Sheets → Status Tabs.
