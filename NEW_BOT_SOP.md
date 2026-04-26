## New Bot SOP (Oracle + RSAdminBot) — one-command deploy

This is the canonical “spin up a new bot” workflow for this repo.

### What it automates

Running `new_bot_deploy.bat` will:
- **Ensure a systemd unit file exists** under `systemd/`
- **Wire the bot into RSAdminBot** (registry + run wrapper + service maps + venv bootstrap + bot_groups)
- **Wire the bot into Oracle updater menus** (`scripts/run_oracle_update_bots.py`)
- **Commit + push** to GitHub (unless `--no-push`)
- **Deploy to Oracle** (unless `--no-deploy`):
  - run `run_oracle_update_bots.py` for `rsadminbot` (deploys updated `.sh` maps)
  - run `run_oracle_update_bots.py` for the new bot folder
  - install the unit into `/etc/systemd/system/` (sudo copy from `rsbots-code`)
  - restart + show status + journal tail

### Preconditions

- `oraclekeys/servers.json` has the Oracle host info.
- The key in `servers.json` exists at either:
  - `oracleserverkeys/<keyfile>` (preferred), or
  - `oraclekeys/<keyfile>` (fallback)
- The bot folder already exists in this repo and contains the entry script you specify.
- Secrets are **not committed**. The service must receive its secret env via systemd override or server-only config.

---

## Usage

### CLI mode (recommended)

```bat
new_bot_deploy.bat --bot-key amazonasinchecker --folder amazon_asin_promo_checker --entry discord_bot.py --group rs --service mirror-world-amazonasinchecker.service --display-name "Amazon ASIN Checker"
```

Optional:
- `--server-name "instance-enhance (rsadmin)"` to pick a specific server from `oraclekeys/servers.json`
- `--no-push` (skip git commit/push)
- `--no-deploy` (skip Oracle deploy)

### Interactive mode

Run with no args and it will prompt you for each value:

```bat
new_bot_deploy.bat
```

---

## Notes / gotchas

- Oracle live root `/home/rsadmin/bots/mirror-world` is usually **not** a git checkout. Do **not** `git pull` there.
- Always deploy RSAdminBot first (the tool does this) so the live `.sh` service maps know your new bot key.

