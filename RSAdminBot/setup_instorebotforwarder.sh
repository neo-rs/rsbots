#!/bin/bash
# One-time setup for Instorebotforwarder on Oracle Ubuntu.
# Run from repo root or RSAdminBot: bash RSAdminBot/setup_instorebotforwarder.sh
#
# Ensures: shared venv + Instorebotforwarder deps, Playwright Chromium, systemd unit, service start.
# Requires: Instorebotforwarder/config.secrets.json with valid bot_token (create from config.secrets.example.json).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
INSTORE_DIR="$ROOT_DIR/Instorebotforwarder"
UNIT_NAME="mirror-world-instorebotforwarder.service"
UNIT_SRC="$ROOT_DIR/systemd/$UNIT_NAME"
# Fallback if systemd dir is not writable (e.g. root-owned): unit may be uploaded to repo root
if [ ! -f "$UNIT_SRC" ]; then
  UNIT_SRC="$ROOT_DIR/$UNIT_NAME"
fi

cd "$ROOT_DIR"

echo "=== 1. Repo layout ==="
if [ ! -f "$INSTORE_DIR/instore_auto_mirror_bot.py" ]; then
  echo "ERROR: Instorebotforwarder not found at $INSTORE_DIR (missing instore_auto_mirror_bot.py)"
  exit 1
fi
echo "OK: Instorebotforwarder folder present"

echo ""
echo "=== 2. Shared venv + Instorebotforwarder requirements ==="
if [ -f "$SCRIPT_DIR/bootstrap_venv.sh" ]; then
  bash "$SCRIPT_DIR/bootstrap_venv.sh"
else
  echo "ERROR: bootstrap_venv.sh not found"
  exit 1
fi

echo ""
echo "=== 3. Playwright Chromium (for Amazon/eBay/StockX scraping) ==="
if "$VENV_DIR/bin/python" -c "import playwright" 2>/dev/null; then
  echo "Installing Chromium for Playwright..."
  "$VENV_DIR/bin/python" -m playwright install chromium
  echo "Installing Playwright system dependencies (may prompt for sudo)..."
  "$VENV_DIR/bin/python" -m playwright install-deps chromium 2>/dev/null || true
else
  echo "WARNING: playwright not in venv; scraping may be limited. Run bootstrap_venv.sh and retry."
fi

echo ""
echo "=== 4. Secrets check ==="
SECRETS="$INSTORE_DIR/config.secrets.json"
if [ ! -f "$SECRETS" ]; then
  echo "WARNING: $SECRETS not found. Create it from config.secrets.example.json with bot_token (and openai_api_key if using rephrase)."
  echo "  Example: cp $INSTORE_DIR/config.secrets.example.json $SECRETS && nano $SECRETS"
  echo "Then run this script again or: sudo systemctl start $UNIT_NAME"
  DO_START=0
else
  if grep -q "PUT_DISCORD_BOT_TOKEN_HERE\|placeholder\|your_token" "$SECRETS" 2>/dev/null; then
    echo "WARNING: $SECRETS appears to have placeholder token. Replace with real bot_token."
    DO_START=0
  else
    echo "OK: config.secrets.json present"
    DO_START=1
  fi
fi

echo ""
echo "=== 5. Systemd unit ==="
if [ ! -f "$UNIT_SRC" ]; then
  echo "ERROR: Unit file not found: $UNIT_SRC"
  exit 1
fi
sudo cp -f "$UNIT_SRC" "/etc/systemd/system/$UNIT_NAME"
sudo systemctl daemon-reload
sudo systemctl enable "$UNIT_NAME"

if [ "${DO_START:-1}" = "1" ]; then
  echo ""
  echo "=== 6. Start service ==="
  sudo systemctl start "$UNIT_NAME"
  sleep 2
  echo ""
  sudo systemctl status "$UNIT_NAME" --no-pager -l || true
  echo ""
  echo "Instorebotforwarder is running. Use RSAdminBot /botstatus, /botstop, /botstart, /mwupdate to manage it."
else
  echo ""
  echo "After adding bot_token to $SECRETS, start with: sudo systemctl start $UNIT_NAME"
fi
echo "Done."
