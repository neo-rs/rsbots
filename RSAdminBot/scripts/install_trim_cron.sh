#!/bin/bash
# Install a daily cron job to run trim_oracle_logs.sh (e.g. 03:00).
# Run once on Oracle: bash RSAdminBot/scripts/install_trim_cron.sh

set -euo pipefail

# Use full path so cron is found when PATH is minimal (e.g. over SSH)
CRONTAB="${CRONTAB:-/usr/bin/crontab}"
if [ ! -x "$CRONTAB" ]; then
  echo "crontab not found or not executable: $CRONTAB (install cron: sudo apt install cron)"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIRROR_WORLD="${MIRROR_WORLD:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
TRIM_SCRIPT="${MIRROR_WORLD}/RSAdminBot/scripts/trim_oracle_logs.sh"
CRON_LOG="${MIRROR_WORLD}/logs/trim_oracle_logs.cron.log"
CRON_ENTRY="0 3 * * * /bin/bash ${TRIM_SCRIPT} >> ${CRON_LOG} 2>&1"

if [ ! -f "$TRIM_SCRIPT" ]; then
  echo "Trim script not found: $TRIM_SCRIPT"
  exit 1
fi

mkdir -p "$(dirname "$CRON_LOG")"

if "$CRONTAB" -l 2>/dev/null | grep -F "$TRIM_SCRIPT" >/dev/null 2>&1; then
  echo "Cron entry for trim script already present."
  "$CRONTAB" -l | grep -F "$TRIM_SCRIPT"
  exit 0
fi

# crontab -l exits 1 when no crontab; avoid failing with set -e
("$CRONTAB" -l 2>/dev/null || true; echo "$CRON_ENTRY") | "$CRONTAB" -
echo "Installed daily cron (03:00): $TRIM_SCRIPT"
echo "Log: $CRON_LOG"
"$CRONTAB" -l | grep -F "$TRIM_SCRIPT"
