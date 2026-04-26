# Amazon ASIN Checker — Oracle Deploy Runbook (end-to-end)

This document captures **everything done from first integration to a live Oracle service**, including:
- code + file changes
- git commits/pushes
- RSAdminBot wiring
- systemd unit installation
- Oracle update + troubleshooting steps
- exact commands used

Repo: `neo-rs/rsbots` (local workspace: `C:\Users\apaap\OneDrive\Desktop\mirror-world`)

Oracle live root: `/home/rsadmin/bots/mirror-world` (**NOT** a git checkout)
Oracle code checkout: `/home/rsadmin/bots/rsbots-code` (**IS** a git checkout)

---

## 0) Initial situation (starting point)

- The folder `amazon_asin_promo_checker/` existed locally and was also manually uploaded to Oracle under:
  - `/home/rsadmin/bots/mirror-world/amazon_asin_promo_checker`
- You wanted it to become a **managed bot** under RSAdminBot:
  - start/stop/restart
  - journal/log viewing
  - `/botupdate` support
  - systemd service installed + enabled
  - Oracle-ready deployment

---

## 1) Local code built/updated (Amazon checker + Discord automation)

### 1.1 New bot runner

Added `amazon_asin_promo_checker/discord_bot.py` implementing:
- listens to a single guild + channel
- extracts ASINs from messages and dedupes them
- posts a “Checking…” reply per ASIN, then edits into the final embed
- uses Playwright + optional PA-API based on `settings.json`
- keeps output compact and omits `N/A` fields (embed formatting evolved based on feedback)

### 1.2 Scrape correctness fixes

Updated `amazon_asin_promo_checker/amazon_asin_promo_checker.py`:
- **Merchant Type** logic: `Amazon Resale` + ships from Amazon → AMZ (Amazon-family seller)
- Preview cleanup: removed noisy labels you didn’t want
- Fixed availability scraping where `#availability` sometimes contained a JS blob; added a fallback extractor
- Seller/sold-by clipping: avoid returning “Returns / Secure transaction / protection plan …” boilerplate

### 1.3 Dependency + examples

Updated/created:
- `amazon_asin_promo_checker/requirements.txt`
  - includes `playwright` and `discord.py`
- `amazon_asin_promo_checker/.env.example`
- `amazon_asin_promo_checker/settings.example.json`
- `amazon_asin_promo_checker/README.md`
- `amazon_asin_promo_checker/.gitignore` (keeps secrets + runtime out of git)
  - ignores `.env`, `settings.json`, `output/`, `amazon_playwright_profile/`

### 1.4 Git ignore allow-list

Updated repo root `.gitignore` to explicitly keep this folder tracked while still ignoring secrets/runtime.

---

## 2) RSAdminBot integration (so it becomes “a managed bot”)

Goal: treat the checker like other managed services.

### 2.1 Systemd unit file

Created:
- `systemd/mirror-world-amazonasinchecker.service`

ExecStart uses the canonical wrapper:
- `/bin/bash /home/rsadmin/bots/mirror-world/RSAdminBot/run_bot.sh amazonasinchecker`

### 2.2 Add bot key to run wrapper

Updated:
- `RSAdminBot/run_bot.sh`

Added a case:
- `amazonasinchecker)` → `cd "$ROOT_DIR/amazon_asin_promo_checker"` → run `discord_bot.py`

### 2.3 Register in RSAdminBot bot registry + groups

Updated:
- `RSAdminBot/admin_bot.py` (`BOTS` dict): added `amazonasinchecker`
- `RSAdminBot/config.json` (`bot_groups.rs_bots`): appended `amazonasinchecker`

### 2.4 Service control plumbing

Updated:
- `RSAdminBot/botctl.sh` (service mapping)
- `RSAdminBot/manage_rs_bots.sh` (service mapping)
- `RSAdminBot/install_services.sh` (unit list to copy/install)
- `RSAdminBot/bootstrap_venv.sh` (install requirements includes `amazon_asin_promo_checker/requirements.txt`)

### 2.5 One additional fix discovered later

`/botupdate` restart flow hit “Unknown bot name” from `RSAdminBot/manage_bots.sh`.

Fixed by adding:
- `["amazonasinchecker"]="mirror-world-amazonasinchecker.service"`
to:
- `RSAdminBot/manage_bots.sh`

---

## 3) Git commits + pushes (what got pushed to GitHub)

### 3.1 Commit: add amazon bot + RSAdminBot wiring

Commit message:
- `rsbots py update: add amazon asin checker bot`

Included:
- new `amazon_asin_promo_checker/**`
- new `systemd/mirror-world-amazonasinchecker.service`
- RSAdminBot wiring edits listed above

Pushed to `origin/main`.

### 3.2 Commit: register service control map

Commit message:
- `rsbots py update: register amazonasinchecker service control`

File:
- `RSAdminBot/manage_bots.sh`

Pushed to `origin/main`.

### 3.3 Commit: add bot to Oracle updater menu

Commit message:
- `rsbots py update: add amazonasinchecker to oracle updater`

File:
- `scripts/run_oracle_update_bots.py` (`BOT_KEY_TO_FOLDER` mapping)

Pushed to `origin/main`.

### 3.4 Commit: sync .sh files in updater (critical fix)

Problem: Oracle live tree isn’t git; updater sync originally only copied `.py/.md/.json/.txt/requirements.txt`.
So **RSAdminBot scripts (`.sh`) never deployed**, causing “Unknown bot name”.

Commit message:
- `rsbots py update: sync sh/service files in oracle updater`

File:
- `scripts/run_oracle_update_bots.py`
  - added `.sh` to safe sync list

Pushed to `origin/main`.

---

## 4) Oracle deployment reality: live tree is NOT git

On Oracle:
- `/home/rsadmin/bots/mirror-world` is the live runtime tree (**no `.git`**)
- `git pull` fails there:
  - `fatal: not a git repository`

So the canonical update path must be:
- update the **git checkout** at `/home/rsadmin/bots/rsbots-code`
- then sync safe tracked files into the live tree `/home/rsadmin/bots/mirror-world`

This is what `update_rs_bots.bat` / `scripts/run_oracle_update_bots.py` does.

---

## 5) Troubleshooting timeline (what failed + why)

### 5.1 `/botupdate amazonasinchecker` restarted FAILED with “Unknown bot name”

Root cause:
- live `/home/rsadmin/bots/mirror-world/RSAdminBot/manage_bots.sh` was old
- restart path uses that script, which lacked the bot mapping

Fix:
- updated `RSAdminBot/manage_bots.sh` mapping and redeployed RSAdminBot to live.

### 5.2 Even after updating: `botctl.sh restart amazonasinchecker` still unknown

Root cause:
- updater sync list did not include `.sh`
- so live `botctl.sh` also stayed old

Fix:
- updated `scripts/run_oracle_update_bots.py` safe sync list to include `.sh`
- reran update for `rsadminbot` to push updated scripts into live tree.

### 5.3 Service restart then failed: “service does not exist”

Meaning:
- systemd unit wasn’t installed into `/etc/systemd/system/`

Fix:
- installed unit file:
  - `sudo cp -f .../systemd/mirror-world-amazonasinchecker.service /etc/systemd/system/`
  - `sudo systemctl daemon-reload`
  - `sudo systemctl enable mirror-world-amazonasinchecker.service`

---

## 6) Exact commands used (Windows + SSH + Oracle)

### 6.1 Windows → Oracle: confirm key + server config

Key exists locally:
- `oracleserverkeys/ssh-key-2025-12-15.key`

Server config:
- `oraclekeys/servers.json`

### 6.2 Windows → Oracle: inspect live state

PowerShell used the canonical OpenSSH path and key:

- `& "$env:WINDIR\System32\OpenSSH\ssh.exe" -i "oracleserverkeys\ssh-key-2025-12-15.key" rsadmin@137.131.14.157 "bash -lc '...'" `

Inspections performed:
- `grep -n amazonasinchecker /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh`
- `grep -n amazonasinchecker /home/rsadmin/bots/mirror-world/RSAdminBot/manage_bots.sh`
- `systemctl list-unit-files | grep mirror-world-amazonasinchecker.service`

### 6.3 Windows: run Oracle updater (correct path)

On Windows:

```bat
py -3 scripts\run_oracle_update_bots.py --group rs --bot rsadminbot
py -3 scripts\run_oracle_update_bots.py --group rs --bot amazonasinchecker
```

This performs on Oracle (over SSH):
- `git pull` in `/home/rsadmin/bots/rsbots-code`
- safe sync into `/home/rsadmin/bots/mirror-world`
- restart via `/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh restart <bot>`

### 6.4 Oracle: install systemd unit (sudo)

Because unit was missing:

```bash
sudo cp -f /home/rsadmin/bots/rsbots-code/systemd/mirror-world-amazonasinchecker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mirror-world-amazonasinchecker.service
```

### 6.5 Oracle: start + verify

```bash
sudo systemctl restart mirror-world-amazonasinchecker.service
sudo systemctl status mirror-world-amazonasinchecker.service --no-pager -n 30
sudo journalctl -u mirror-world-amazonasinchecker.service -n 80 --no-pager
```

Observed log proof:
- `AmazonCheckerBot logged in as ... (guild_id=..., channel_id=...)`

---

## 7) Files changed summary (canonical)

### New files
- `amazon_asin_promo_checker/discord_bot.py`
- `amazon_asin_promo_checker/amazon_asin_promo_checker.py`
- `amazon_asin_promo_checker/README.md`
- `amazon_asin_promo_checker/requirements.txt`
- `amazon_asin_promo_checker/.env.example`
- `amazon_asin_promo_checker/run_checker.bat`
- `amazon_asin_promo_checker/run_checker.sh`
- `systemd/mirror-world-amazonasinchecker.service`

### Updated files (RSAdminBot)
- `RSAdminBot/admin_bot.py`
- `RSAdminBot/config.json`
- `RSAdminBot/run_bot.sh`
- `RSAdminBot/botctl.sh`
- `RSAdminBot/manage_rs_bots.sh`
- `RSAdminBot/manage_bots.sh`  (**critical for restart path**)
- `RSAdminBot/install_services.sh`
- `RSAdminBot/bootstrap_venv.sh`

### Updated files (Oracle updater)
- `scripts/run_oracle_update_bots.py`
  - added `amazonasinchecker` mapping
  - added `.sh` to safe sync list so RSAdminBot scripts deploy

### Updated repo ignore
- `.gitignore` (allow-list the tool folder; keep secrets excluded)

---

## 8) Oracle readiness checklist (final state)

- [x] bot folder present in live tree: `/home/rsadmin/bots/mirror-world/amazon_asin_promo_checker`
- [x] RSAdminBot recognizes bot key: `amazonasinchecker`
- [x] `RSAdminBot/run_bot.sh amazonasinchecker` launches `discord_bot.py`
- [x] systemd unit installed + enabled: `/etc/systemd/system/mirror-world-amazonasinchecker.service`
- [x] service runs: `systemctl status mirror-world-amazonasinchecker.service`
- [x] logs show login + correct guild/channel IDs

---

## 9) Notes / required secrets

- The Discord token is **server-only secret**:
  - `DISCORD_BOT_TOKEN` must be provided to the systemd service environment.
- Playwright requires Chromium installed on Oracle:
  - `/home/rsadmin/bots/mirror-world/.venv/bin/python -m playwright install chromium`

