# DailyScheduleReminder Oracle setup status

**Completed setup:**

1. **Code and config on Oracle**
   - Full `DailyScheduleReminder/` folder deployed (reminder_bot.py, schedule_parser.py, requirements.txt, config.json, config.secrets.json, etc.).
   - Systemd unit `mirror-world-dailyschedulereminder.service` installed under `/etc/systemd/system/`.
   - RSAdminBot scripts and config updated (run_bot.sh, install_services.sh, botctl.sh, manage_mirror_bots.sh, bootstrap_venv.sh, config.json, admin_bot.py).

2. **Service**
   - **DailyScheduleReminder:** `active (running)`, enabled on boot.
   - **Main PID:** check with `systemctl show mirror-world-dailyschedulereminder.service --property=MainPID --value`.
   - **Command:** `/home/rsadmin/bots/mirror-world/.venv/bin/python -u DailyScheduleReminder/reminder_bot.py`.

3. **Verification (Oracle)**
   - `systemctl status mirror-world-dailyschedulereminder.service` → active.
   - `journalctl -u mirror-world-dailyschedulereminder.service -n 20` → shows startup and daemon “checking every minute” logs.

4. **RSAdminBot**
   - Restarted so it loads the new config and code.
   - **Inspector:** “Discovered 11 bot(s)” (includes dailyschedulereminder).
   - **Journal logging:** RSAdminBot creates one Discord channel per bot in the `journal_live` category. With `dailyschedulereminder` in `bot_groups.mirror_bots`, the channel **#journal-dailyschedulereminder** is created (or updated) in the Test Server category (config: `journal_live.category_id`). Live logs stream via webhook to that channel.

**Discord checks (manual):**

- **Test Server (Neo):** Under the journal category, confirm **#journal-dailyschedulereminder** exists. New lines from `journalctl -u mirror-world-dailyschedulereminder.service` should appear there.
- **RSAdminBot commands:** `/botstatus` → choose “Daily Schedule Reminder”; `/logs` → choose “Daily Schedule Reminder”; `/mwupdate` → choose “Daily Schedule Reminder”.

**If the journal channel is missing:**

- Ensure RSAdminBot is in the Test Server and has permission to create channels in the journal category.
- Restart RSAdminBot again: `sudo systemctl restart mirror-world-rsadminbot.service` (on Oracle).
- Check RSAdminBot logs: `journalctl -u mirror-world-rsadminbot.service -n 100 | grep -i journal`.

**Sync local → Oracle (ticket startup flow):**

```bash
# From mirror-world repo root:
python scripts/sync_and_deploy_ticket_startup.py

# If SSH times out, use --sync-only then run on server:
python scripts/sync_and_deploy_ticket_startup.py --sync-only
# Then SSH to Oracle and run:
bash /home/rsadmin/bots/mirror-world/scripts/on_oracle_finish_ticket_startup.sh
```

**Useful commands (Oracle):**

```bash
# DailyScheduleReminder
sudo systemctl status mirror-world-dailyschedulereminder.service
journalctl -u mirror-world-dailyschedulereminder.service -f

# Via botctl
bash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh status dailyschedulereminder
bash /home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh logs dailyschedulereminder 50
```
