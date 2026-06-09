#!/bin/bash
# Install/refresh Telnyx Discord SMS Bridge on Oracle Ubuntu.
# - Creates local venv + pip deps
# - Installs systemd unit
# - Adds nginx location for public Telnyx webhook (if missing)
#
# Usage (on server):
#   bash /home/rsadmin/bots/mirror-world/telnyx_discord_sms_bridge/install_oracle.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BRIDGE_DIR="$SCRIPT_DIR"
UNIT_NAME="mirror-world-telnyx-discord-sms-bridge.service"
UNIT_SRC="$ROOT_DIR/systemd/$UNIT_NAME"
if [ ! -f "$UNIT_SRC" ]; then
  UNIT_SRC="$BRIDGE_DIR/deploy/$UNIT_NAME"
fi
NGINX_CONF="/etc/nginx/sites-enabled/rscheckerbot-whop.conf"
PUBLIC_WEBHOOK_URL="https://137.131.14.157.sslip.io/webhooks/telnyx"

cd "$BRIDGE_DIR"

if [ ! -f ".env" ]; then
  echo "ERROR: Missing $BRIDGE_DIR/.env"
  echo "Create it on the server (never commit secrets). Copy from .env.example and fill values."
  exit 1
fi

echo "[install] Creating bridge venv..."
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

mkdir -p logs

if [ ! -f "$UNIT_SRC" ]; then
  echo "ERROR: Missing systemd unit: $UNIT_SRC"
  exit 1
fi

echo "[install] Installing systemd unit..."
sudo cp -f "$UNIT_SRC" "/etc/systemd/system/$UNIT_NAME"
sudo systemctl daemon-reload
sudo systemctl enable "$UNIT_NAME" >/dev/null
sudo systemctl restart "$UNIT_NAME"

echo "[install] Service status:"
sudo systemctl is-active "$UNIT_NAME" || true

if [ -f "$NGINX_CONF" ]; then
  if ! sudo grep -q 'location /webhooks/telnyx' "$NGINX_CONF"; then
    echo "[install] Adding nginx location /webhooks/telnyx ..."
    sudo python3 - "$NGINX_CONF" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
snippet = """
  location /webhooks/telnyx {
    if ($request_method != POST) { return 200; }
    proxy_pass http://127.0.0.1:8787/webhooks/telnyx;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

"""
marker = "  location / {"
if marker not in text:
    raise SystemExit("Could not find nginx location / block to insert Telnyx route")
path.write_text(text.replace(marker, snippet + marker, 1), encoding="utf-8")
print("nginx location inserted")
PY
    sudo nginx -t
    sudo systemctl reload nginx
    echo "[install] nginx reloaded"
  else
    echo "[install] nginx location /webhooks/telnyx already present"
  fi
else
  echo "WARNING: nginx config not found at $NGINX_CONF"
  echo "Add a proxy for /webhooks/telnyx -> http://127.0.0.1:8787/webhooks/telnyx manually."
fi

echo
echo "Local health:"
curl -sS "http://127.0.0.1:8787/health" || true
echo
echo
echo "Public Telnyx webhook URL (paste into Telnyx Messaging Profile):"
echo "  $PUBLIC_WEBHOOK_URL"
echo
echo "Done."
