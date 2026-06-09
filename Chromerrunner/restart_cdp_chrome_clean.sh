#!/usr/bin/env bash
set -euo pipefail

# Kill any oracle_real_chrome_profile Chrome and bring back exactly ONE headed CDP instance.
# Used at the start of oracle_novnc_tunnel.bat so noVNC never stacks duplicate windows.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PROFILE_MARKER="oracle_real_chrome_profile"
CDP_URL="http://127.0.0.1:9222/json/version"

profile_proc_count() {
  pgrep -cf "$PROFILE_MARKER" 2>/dev/null || echo 0
}

cdp_up() {
  curl -sf "$CDP_URL" >/dev/null 2>&1
}

before="$(profile_proc_count)"
echo "== CDP Chrome preflight (oracle_real_chrome_profile) =="
echo "Profile-related processes: ${before}"
if cdp_up; then
  echo "CDP :9222: up"
else
  echo "CDP :9222: down"
fi

needs_clean=no
if [[ "$before" -gt 0 ]]; then
  needs_clean=yes
elif cdp_up; then
  needs_clean=yes
fi

if [[ "$needs_clean" == "yes" ]]; then
  echo "Cleaning existing profile Chrome (single-instance restart)..."
  pkill -f "user-data-dir=.*${PROFILE_MARKER}" 2>/dev/null || true
  sleep 4
  for _ in $(seq 1 20); do
    if ! cdp_up; then
      break
    fi
    sleep 1
  done
else
  echo "No profile Chrome running; will start one instance."
fi

if systemctl is-active mirror-world-instorebotforwarder-chrome-cdp.service >/dev/null 2>&1; then
  echo "Starting via systemd..."
  systemctl restart mirror-world-instorebotforwarder-chrome-cdp.service 2>/dev/null || \
    systemctl start mirror-world-instorebotforwarder-chrome-cdp.service 2>/dev/null || true
  sleep 8
fi

if ! cdp_up; then
  echo "systemd did not bring CDP up; starting script..."
  nohup bash Chromerrunner/start_chrome_oracle_cdp.sh --with-xvfb >>/tmp/cdp_chrome_headed.log 2>&1 &
  sleep 8
fi

curl -s "$CDP_URL" 2>/dev/null | head -c 220 || echo "CDP still down"
echo
pages="$(curl -s http://127.0.0.1:9222/json/list 2>/dev/null | python3 -c 'import sys,json; t=json.load(sys.stdin); print(sum(1 for x in t if x.get("type")=="page"))' 2>/dev/null || echo '?')"
after="$(profile_proc_count)"
echo "After: profile processes=${after}, CDP page targets=${pages}"
