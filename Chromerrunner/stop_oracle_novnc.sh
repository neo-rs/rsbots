#!/usr/bin/env bash
set -euo pipefail

# Stop only the Chromerrunner *noVNC viewing* stack started by start_oracle_novnc.sh:
#   - websockify on localhost:6080 (browser UI for noVNC)
#   - x11vnc on localhost:5900 (rfbport 5900 in our starter)
#
# Does NOT stop:
#   - Chrome CDP on 9222 (RSAdminBot Chromerrunner, scripts)
#   - Xvfb / fluxbox (headed Chrome still needs DISPLAY if you use --headed)
#   - systemd Discord bots (they do not bind 6080/5900)
#
# Note: Port 6080 could theoretically be reused by RSForwarder Mavely noVNC on the same host.
# If something else owns 6080, this frees that listener too — RSForwarder can start noVNC again when needed.

LOG_TAG="[Chromerrunner stop_oracle_novnc]"

have_ss() {
  command -v ss >/dev/null 2>&1
}

kill_tcp_port() {
  local port="$1"
  local label="$2"
  if ! have_ss; then
    echo "$LOG_TAG ss not found; trying fuser only."
  elif ss -ltn 2>/dev/null | grep -qE "[:.]${port}\s"; then
    echo "$LOG_TAG ${label}: listener present on TCP ${port}"
  else
    echo "$LOG_TAG ${label}: nothing listening on TCP ${port}"
    return 0
  fi

  if command -v fuser >/dev/null 2>&1; then
    if sudo -n true 2>/dev/null; then
      sudo -n fuser -k "${port}/tcp" 2>/dev/null || true
    else
      fuser -k "${port}/tcp" 2>/dev/null || true
    fi
  fi
}

echo "== Stop Chromerrunner noVNC web stack (6080 + 5900 only) =="
echo "Chrome/CDP :9222 and Discord bot services are NOT targeted."
echo ""

kill_tcp_port 6080 "websockify/noVNC web"
sleep 0.3
kill_tcp_port 5900 "x11vnc (Chromerrunner default)"

echo ""
echo "$LOG_TAG Done. Headed Chrome + Xvfb were left running (CDP Chromerrunner still works)."
echo "$LOG_TAG To stop Chrome CDP separately, kill the Chrome process using --remote-debugging-port=9222."
