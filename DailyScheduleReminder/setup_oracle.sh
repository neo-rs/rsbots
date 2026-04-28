#!/bin/bash
# One-time setup for DailyScheduleReminder on Oracle Ubuntu.
#
# Ensures: shared venv + DailyScheduleReminder deps, systemd unit, service start.
# Requires: DailyScheduleReminder/config.secrets.json with valid token (Discord user token).
#
# Usage:
#   bash DailyScheduleReminder/setup_oracle.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DAILY_DIR="$ROOT_DIR/DailyScheduleReminder"

echo "=== DailyScheduleReminder Oracle Setup ==="
echo ""

# Check DailyScheduleReminder folder exists
if [ ! -f "$DAILY_DIR/reminder_bot.py" ]; then
  echo "ERROR: DailyScheduleReminder not found at $DAILY_DIR (missing reminder_bot.py)"
  exit 1
fi
echo "OK: DailyScheduleReminder folder present"
echo ""

# Check secrets file
if [ ! -f "$DAILY_DIR/config.secrets.json" ]; then
  echo "⚠️  WARNING: DailyScheduleReminder/config.secrets.json not found"
  echo "   Create it with: {\"token\": \"YOUR_DISCORD_USER_TOKEN\"}"
  echo "   Or set env var DISCORD_USER_TOKEN"
  echo ""
fi

echo "=== 1. Shared venv + DailyScheduleReminder requirements ==="
if [ -f "$ROOT_DIR/RSAdminBot/bootstrap_venv.sh" ]; then
  bash "$ROOT_DIR/RSAdminBot/bootstrap_venv.sh"
else
  echo "WARNING: bootstrap_venv.sh not found; skipping venv setup"
fi
echo ""

echo "=== 2. Install systemd unit ==="
if [ -f "$ROOT_DIR/RSAdminBot/install_services.sh" ]; then
  bash "$ROOT_DIR/RSAdminBot/install_services.sh"
else
  echo "WARNING: install_services.sh not found; skipping systemd install"
fi
echo ""

echo "=== 3. Enable and start service ==="
SERVICE="mirror-world-dailyschedulereminder.service"
if systemctl list-unit-files "$SERVICE" --no-pager 2>/dev/null | grep -q "$SERVICE"; then
  sudo systemctl unmask "$SERVICE" || true
  sudo systemctl enable "$SERVICE" || true
  echo "Starting $SERVICE..."
  sudo systemctl start "$SERVICE"
  sleep 2
  if systemctl is-active "$SERVICE" --quiet; then
    echo "✅ Service is active"
  else
    echo "⚠️  Service did not start; check logs:"
    echo "   sudo journalctl -u $SERVICE -n 30"
  fi
else
  echo "ERROR: Service $SERVICE not found (install_services.sh may have failed)"
  exit 1
fi
echo ""

echo "=== 4. Verify PID ==="
PID=$(systemctl show "$SERVICE" --property=MainPID --no-pager --value 2>/dev/null || echo "")
if [ -n "$PID" ] && [ "$PID" != "0" ]; then
  echo "✅ Service PID: $PID"
  if ps -p "$PID" > /dev/null 2>&1; then
    echo "✅ Process is running"
  else
    echo "⚠️  PID exists but process not found"
  fi
else
  echo "⚠️  Service PID not found (may be starting)"
fi
echo ""

echo "=== Setup Complete ==="
echo ""
echo "Service: $SERVICE"
echo "Status: $(systemctl is-active "$SERVICE" 2>/dev/null || echo 'unknown')"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status $SERVICE"
echo "  sudo journalctl -u $SERVICE -f"
echo "  bash $ROOT_DIR/RSAdminBot/botctl.sh status dailyschedulereminder"
echo "  bash $ROOT_DIR/RSAdminBot/botctl.sh logs dailyschedulereminder 50"
