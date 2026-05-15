#!/bin/bash
# One-shot: install systemd unit, venv deps, enable + start RS Channel Relay on Oracle.
# Run on the Ubuntu host as rsadmin (sudo for systemd):
#   bash /home/rsadmin/bots/mirror-world/RSChannelRelay/install_on_oracle.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_SRC="$ROOT_DIR/systemd/mirror-world-rschannelrelay.service"
UNIT_DST="/etc/systemd/system/mirror-world-rschannelrelay.service"

if [ ! -f "$UNIT_SRC" ]; then
  echo "ERROR: missing unit file: $UNIT_SRC"
  echo "Copy systemd/mirror-world-rschannelrelay.service to the server (git pull or SCP), then re-run."
  exit 1
fi

if [ ! -f "$ROOT_DIR/RSChannelRelay/config.secrets.json" ]; then
  echo "ERROR: missing $ROOT_DIR/RSChannelRelay/config.secrets.json (discord_bot_token)."
  exit 1
fi

echo "[1/4] bootstrap venv (includes RSChannelRelay/requirements.txt)..."
bash "$ROOT_DIR/RSAdminBot/bootstrap_venv.sh"

echo "[2/4] install systemd unit..."
sudo cp -f "$UNIT_SRC" "$UNIT_DST"
sudo systemctl daemon-reload
sudo systemctl enable mirror-world-rschannelrelay.service

echo "[3/4] restart service..."
sudo systemctl restart mirror-world-rschannelrelay.service

echo "[4/4] status:"
systemctl --no-pager status mirror-world-rschannelrelay.service || true
echo ""
echo "Logs: journalctl -u mirror-world-rschannelrelay.service -f"
echo "RSAdmin: /botupdate rschannelrelay  |  botctl restart rschannelrelay"
