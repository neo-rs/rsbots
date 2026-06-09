#!/usr/bin/env bash
set -uo pipefail

# Ensure CDP Chrome uses headed mode on Xvfb :99 (oracle_real_chrome_profile).
# Called from oracle_novnc_tunnel.ps1 before noVNC attach.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

chmod +x Chromerrunner/start_chrome_oracle_cdp.sh Chromerrunner/start_oracle_novnc_for_cdp.sh

systemctl is-active mirror-world-instorebotforwarder-chrome-cdp.service 2>/dev/null || true

need_recycle=no
cdp_json="$(curl -sf http://127.0.0.1:9222/json/version 2>/dev/null || true)"
if [[ -n "$cdp_json" ]] && echo "$cdp_json" | grep -qi HeadlessChrome; then
  need_recycle=yes
  echo "CDP_is_headless"
elif [[ -z "$cdp_json" ]]; then
  need_recycle=yes
  echo "CDP_not_running"
elif ! pgrep -af "Xvfb[[:space:]]+:99" >/dev/null 2>&1; then
  # Headed CDP without :99 Xvfb — recycle once so noVNC can attach to the right display.
  need_recycle=yes
  echo "Xvfb_:99_missing"
else
  echo "CDP_already_headed_on_:99"
fi

start_single_cdp_chrome() {
  if curl -sf http://127.0.0.1:9222/json/version >/dev/null 2>&1; then
    echo "CDP already up; skip start"
    return 0
  fi
  if pgrep -af "user-data-dir=.*oracle_real_chrome_profile" 2>/dev/null | grep -q 'remote-debugging-port=9222'; then
    echo "Profile Chrome already running; skip start"
    return 0
  fi
  echo "Starting single CDP Chrome instance..."
  if systemctl is-active mirror-world-instorebotforwarder-chrome-cdp.service >/dev/null 2>&1; then
    systemctl start mirror-world-instorebotforwarder-chrome-cdp.service 2>/dev/null || true
    sleep 8
    if curl -sf http://127.0.0.1:9222/json/version >/dev/null 2>&1; then
      return 0
    fi
  fi
  nohup bash Chromerrunner/start_chrome_oracle_cdp.sh --with-xvfb >>/tmp/cdp_chrome_headed.log 2>&1 &
  sleep 8
}

recycle_chrome() {
  echo "Recycling CDP Chrome for headed Xvfb :99 (one instance)..."
  pkill -f "user-data-dir=.*oracle_real_chrome_profile" 2>/dev/null || true
  sleep 3
  for _ in $(seq 1 15); do
    if ! curl -sf http://127.0.0.1:9222/json/version >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if systemctl is-active mirror-world-instorebotforwarder-chrome-cdp.service >/dev/null 2>&1; then
    if systemctl restart mirror-world-instorebotforwarder-chrome-cdp.service 2>/dev/null; then
      sleep 8
      return 0
    fi
  fi
  start_single_cdp_chrome
}

if [[ "$need_recycle" == "yes" ]]; then
  recycle_chrome
elif ! curl -sf http://127.0.0.1:9222/json/version >/dev/null 2>&1; then
  echo "CDP_not_up"
  start_single_cdp_chrome
fi

curl -s http://127.0.0.1:9222/json/version 2>/dev/null | head -c 220 || echo "CDP_unavailable"
echo
