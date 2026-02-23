#!/bin/bash
# Install/refresh Mirror World systemd unit files from the repo.
#
# Canonical repo root:
#   /home/rsadmin/bots/mirror-world
#
# This script copies unit files from ./systemd into /etc/systemd/system,
# reloads systemd, enables services, and restarts them.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
UNIT_SRC_DIR="$ROOT_DIR/systemd"

if [ ! -d "$UNIT_SRC_DIR" ]; then
  echo "ERROR: systemd unit source dir not found: $UNIT_SRC_DIR"
  exit 1
fi

units=(
  "mirror-world-rsadminbot.service"
  "mirror-world-rsforwarder.service"
  "mirror-world-rsonboarding.service"
  "mirror-world-rscheckerbot.service"
  "mirror-world-rsmentionpinger.service"
  "mirror-world-rssuccessbot.service"
  "mirror-world-datamanagerbot.service"
  "mirror-world-pingbot.service"
  "mirror-world-discumbot.service"
  "mirror-world-dailyschedulereminder.service"
  "mirror-world-instorebotforwarder.service"
  "mirror-world-whopmembershipsync.service"
)

echo "Bootstrapping shared venv..."
if [ -f "$SCRIPT_DIR/bootstrap_venv.sh" ]; then
  bash "$SCRIPT_DIR/bootstrap_venv.sh"
else
  echo "WARNING: bootstrap_venv.sh not found; services may fail if .venv is missing."
fi

echo "Installing unit files from: $UNIT_SRC_DIR"
for unit in "${units[@]}"; do
  src="$UNIT_SRC_DIR/$unit"
  if [ ! -f "$src" ]; then
    echo "ERROR: missing unit file: $src"
    exit 1
  fi
  sudo cp -f "$src" "/etc/systemd/system/$unit"
done

echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "Enabling services..."
for unit in "${units[@]}"; do
  sudo systemctl enable "$unit" >/dev/null
done

echo "Restarting RS services (non-admin bots first, then RSAdminBot)..."
for unit in "mirror-world-rsforwarder.service" "mirror-world-rsonboarding.service" "mirror-world-rscheckerbot.service" "mirror-world-rsmentionpinger.service" "mirror-world-rssuccessbot.service"; do
  sudo systemctl restart "$unit" || true
done
sudo systemctl restart "mirror-world-rsadminbot.service" || true

echo "NOTE: MW bot services were installed/enabled but NOT restarted by this script."
echo "      Copy MW secrets (.env/tokens.env/channel_map.json) first, then start them:"
echo "        sudo systemctl restart mirror-world-datamanagerbot.service"
echo "        sudo systemctl restart mirror-world-pingbot.service"
echo "        sudo systemctl restart mirror-world-discumbot.service"
echo "      Instorebotforwarder: ensure Instorebotforwarder/config.secrets.json has bot_token, then:"
echo "        sudo systemctl restart mirror-world-instorebotforwarder.service"
echo "      DailyScheduleReminder: ensure DailyScheduleReminder/config.secrets.json has token (Discord user token), then:"
echo "        sudo systemctl restart mirror-world-dailyschedulereminder.service"
echo "      WhopMembershipSync: ensure WhopMembershipSync/config.secrets.json has whop_api.api_key and google_service_account_json, then:"
echo "        sudo systemctl restart mirror-world-whopmembershipsync.service"

echo "Done. Current status summary:"
for unit in "${units[@]}"; do
  state="$(systemctl show "$unit" --property=ActiveState --no-pager --value 2>/dev/null || echo unknown)"
  echo "  - $unit: $state"
done


