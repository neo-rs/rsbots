# WhopMembershipSync – Oracle Server setup

Get WhopMembershipSync **live and running on Oracle** (Ubuntu server at `/home/rsadmin/bots/mirror-world`).

---

## Full setup from your PC (recommended)

1. **Ensure Oracle is in `oraclekeys/servers.json`** (name, user, host, key, remote_root). Same config as other Oracle tools.
2. **Run the full-setup script** (syncs code + systemd unit, then runs on-Oracle install and enable):
   ```bat
   python scripts/setup_whopmembershipsync_on_oracle.py
   ```
   Or use **Oracle Tools Menu (MWBots)**: run `oracle_tools_menu_mwbots.bat` → choose **[6] WhopMembershipSync full setup**.
3. **Add secrets on Oracle** (if not already there): create `WhopMembershipSync/config.secrets.json` with `whop_api` and `google_service_account_json`.
4. **Start the service on Oracle** (if the script didn’t start it because secrets were missing):
   ```bash
   sudo systemctl start mirror-world-whopmembershipsync.service
   journalctl -u mirror-world-whopmembershipsync.service -f
   ```

Optional: `--sync-only` only copies files; then SSH to Oracle and run `bash /home/rsadmin/bots/mirror-world/scripts/on_oracle_setup_whopmembershipsync.sh` yourself.

---

## 1. Prerequisites on Oracle

- **Python 3** and repo **shared venv** at `/home/rsadmin/bots/mirror-world/.venv`
- **Dependencies**: same as RSCheckerbot (Whop API client lives there). Ensure:
  - `aiohttp`, `google-auth`, `google-api-python-client` (and any other deps from WhopMembershipSync or RSCheckerbot) are installed in `.venv`.

If you use `RSAdminBot/bootstrap_venv.sh`, it should already install from the repo’s requirements. If WhopMembershipSync has its own `requirements.txt`, install it from repo root:

```bash
cd /home/rsadmin/bots/mirror-world
.venv/bin/pip install -r WhopMembershipSync/requirements.txt
```

---

## 2. Deploy code to Oracle

**Option A – From your PC (recommended)**  
Use RSAdminBot’s **/botsync** (or **!botsync**) and choose **Whop Membership Sync**, or sync the whole mirror-world repo to Oracle (e.g. `oracle_tools_menu.bat` → download/sync, or your existing deploy script).

**Option B – Git on Oracle**  
If the server pulls from GitHub:

```bash
cd /home/rsadmin/bots/mirror-world
git pull origin main
```

Ensure the `WhopMembershipSync/` folder and `systemd/mirror-world-whopmembershipsync.service` are present.

---

## 3. Secrets on Oracle

Create or edit **`WhopMembershipSync/config.secrets.json`** on the server. It must contain:

- **`whop_api.api_key`** – Whop Company API key  
- **`whop_api.company_id`** – e.g. `biz_s58kr1WWnL1bzH` (or from your `config.json`)  
- **`google_service_account_json`** – Google service account JSON object (for Sheets API)

You can copy from your local `WhopMembershipSync/config.secrets.json` (or from RSForwarder’s secrets if you share the same service account). Do **not** commit this file; it’s excluded from snapshots.

---

## 4. Install and enable the systemd service

On Oracle:

```bash
cd /home/rsadmin/bots/mirror-world

# Install/refresh all unit files (includes WhopMembershipSync)
sudo cp -f systemd/mirror-world-whopmembershipsync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mirror-world-whopmembershipsync.service
```

Or run the full install script if you use it:

```bash
bash RSAdminBot/install_services.sh
```

---

## 5. Start and verify

```bash
# Start
sudo systemctl start mirror-world-whopmembershipsync.service

# Check status
sudo systemctl status mirror-world-whopmembershipsync.service

# Follow logs
journalctl -u mirror-world-whopmembershipsync.service -f
```

You should see the usual WhopMembershipSync startup logs (initial sync, then “Waiting 15 minutes until next sync…” if continuous is enabled).

---

## 6. RSAdminBot (Discord)

- **Bot list**: Whop Membership Sync is registered in `RSAdminBot` (BOTS + `mirror_bots`). After deploying and restarting RSAdminBot, you should see it in:
  - **/botstatus** → choose “Whop Membership Sync”
  - **/logs** → choose “Whop Membership Sync”
  - **/botsync** / **!botsync** → sync “Whop Membership Sync” to Oracle
  - **/mwupdate** → update MW bots (includes WhopMembershipSync if in mirror_bots)

- **Service name**: `mirror-world-whopmembershipsync.service`  
- **bot_key**: `whopmembershipsync` (used by `run_bot.sh` and scripts).

---

## 7. Useful commands on Oracle

| Action   | Command |
|----------|--------|
| Status   | `sudo systemctl status mirror-world-whopmembershipsync.service` |
| Logs     | `journalctl -u mirror-world-whopmembershipsync.service -n 100` |
| Restart  | `sudo systemctl restart mirror-world-whopmembershipsync.service` |
| Stop     | `sudo systemctl stop mirror-world-whopmembershipsync.service` |
| Via botctl | `bash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status whopmembershipsync` |
|          | `bash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh logs whopmembershipsync 50` |
| Via manage_mirror_bots | `bash /home/rsadmin/bots/mirror-world/RSAdminBot/manage_mirror_bots.sh restart whopmembershipsync` |

---

## 8. Sync local → Oracle (from your PC)

1. **Sync only (no SSH restart)**  
   Use **/botsync** or **!botsync** in Discord and select “Whop Membership Sync”, or your existing script that rsyncs `WhopMembershipSync/` to Oracle.

2. **Restart after sync**  
   Either:
   - Use **/botrestart** (or **/logs** → restart) for “Whop Membership Sync”, or  
   - SSH to Oracle and run:
     ```bash
     sudo systemctl restart mirror-world-whopmembershipsync.service
     ```

---

## Summary checklist

- [ ] Repo (including `WhopMembershipSync/` and `systemd/mirror-world-whopmembershipsync.service`) is on Oracle.
- [ ] `WhopMembershipSync/config.secrets.json` exists on Oracle with `whop_api` and `google_service_account_json`.
- [ ] `.venv` has required dependencies (e.g. `pip install -r WhopMembershipSync/requirements.txt` if needed).
- [ ] Unit file installed and enabled: `sudo cp systemd/mirror-world-whopmembershipsync.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable mirror-world-whopmembershipsync.service`
- [ ] Service started: `sudo systemctl start mirror-world-whopmembershipsync.service`
- [ ] Logs show sync and “Waiting N minutes until next sync” (if continuous): `journalctl -u mirror-world-whopmembershipsync.service -n 50`

Once these are done, WhopMembershipSync is live on Oracle and will keep running across reboots (enabled service).
