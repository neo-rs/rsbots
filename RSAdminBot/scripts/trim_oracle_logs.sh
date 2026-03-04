#!/bin/bash
# Cap known bloated log files on Oracle to avoid filling disk.
# Run on server: bash RSAdminBot/scripts/trim_oracle_logs.sh
# Or via RSAdminBot /ssh: bash /home/rsadmin/bots/mirror-world/RSAdminBot/scripts/trim_oracle_logs.sh
#
# Limits (configurable below):
#   - Single log files: MAX_FILE_MB (default 30)
# Keeps the last MAX_FILE_MB of each file (truncates older content).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIRROR_WORLD="${MIRROR_WORLD:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
MAX_FILE_MB="${MAX_FILE_MB:-30}"
MAX_BYTES=$((MAX_FILE_MB * 1024 * 1024))

cap_file() {
  local f="$1"
  if [ ! -f "$f" ]; then
    return 0
  fi
  local sz
  sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
  if [ "$sz" -le "$MAX_BYTES" ]; then
    return 0
  fi
  echo "Trimming $f (${sz} bytes -> ${MAX_BYTES})"
  tail -c "$MAX_BYTES" "$f" > "${f}.trim" && mv "${f}.trim" "$f"
}

cd "$MIRROR_WORLD" || exit 1

# Single large log files (movement/bot logs) — cap at 30MB each.
# We only cap .jsonl and .log (tail -c keeps last N bytes). Do NOT cap .json array files
# (e.g. discumlogs.json, datamanagerbotlogs.json) here — they need bot-side rotation.
cap_file "MWDataManagerBot/logs/decision_traces.jsonl"
cap_file "logs/systemd_discumbot.log"
cap_file "logs/systemd_datamanagerbot.log"

# Any other systemd_*.log in logs/
for f in logs/systemd_*.log; do
  [ -f "$f" ] && cap_file "$f"
done

echo "Done. Log caps applied (max ${MAX_FILE_MB}MB per file)."
