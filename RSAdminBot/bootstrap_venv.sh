#!/bin/bash
# Ensure the shared venv exists and has the dependencies required for RS bots.
#
# Canonical:
# - Repo root: /home/rsadmin/bots/mirror-world
# - Shared venv: /home/rsadmin/bots/mirror-world/.venv
#
# This script is safe to run repeatedly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"

need_recreate=0
if [ ! -x "$VENV_DIR/bin/python" ]; then
  need_recreate=1
fi

# If pip is broken, recreate the venv.
if [ "$need_recreate" -eq 0 ]; then
  if ! "$VENV_DIR/bin/python" -m pip --version >/dev/null 2>&1; then
    need_recreate=1
  fi
fi

if [ "$need_recreate" -eq 1 ]; then
  echo "[bootstrap_venv] Recreating venv at: $VENV_DIR"
  rm -rf "$VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

echo "[bootstrap_venv] Upgrading pip..."
"$VENV_DIR/bin/python" -m pip install --upgrade pip >/dev/null

# Install requirements that exist (RS-only)
reqs=(
  "$ROOT_DIR/RSAdminBot/requirements.txt"
  "$ROOT_DIR/RSCheckerbot/requirements.txt"
  "$ROOT_DIR/RSMentionPinger/requirements.txt"
  "$ROOT_DIR/RSuccessBot/requirements.txt"
  "$ROOT_DIR/MWDataManagerBot/requirements.txt"
  "$ROOT_DIR/MWPingBot/requirements.txt"
  "$ROOT_DIR/MWDiscumBot/requirements.txt"
  "$ROOT_DIR/DailyScheduleReminder/requirements.txt"
  "$ROOT_DIR/Instorebotforwarder/requirements.txt"
)

args=()
for f in "${reqs[@]}"; do
  if [ -f "$f" ]; then
    args+=( -r "$f" )
  fi
done

if [ "${#args[@]}" -gt 0 ]; then
  echo "[bootstrap_venv] Installing requirements: ${args[*]}"
  "$VENV_DIR/bin/pip" install "${args[@]}"
fi

# Safety: remove the wrong 'discord' package if present (discord.py provides the correct discord module)
if "$VENV_DIR/bin/pip" show discord >/dev/null 2>&1; then
  echo "[bootstrap_venv] Removing conflicting 'discord' package..."
  "$VENV_DIR/bin/pip" uninstall -y discord >/dev/null || true
fi

echo "[bootstrap_venv] OK"


