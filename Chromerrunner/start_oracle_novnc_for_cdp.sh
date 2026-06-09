#!/usr/bin/env bash
set -euo pipefail

# noVNC viewer for the *existing* Instore CDP Chrome display (amazon/ebay profile).
# Attaches x11vnc to the same Xvfb display used by start_chrome_oracle_cdp.sh --with-xvfb
# (default :99). Does NOT start a second Xvfb/desktop.

LOG="/tmp/chromerrunner_novnc_cdp.log"
DISPLAY_NUM="${XVFB_DISPLAY:-:99}"
DISPLAY_NUM="${DISPLAY_NUM#:}"
DISPLAY=":${DISPLAY_NUM}"

is_listening() {
  local port="$1"
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${port}\$" || return 1
  return 0
}

echo "== Chromerrunner noVNC for CDP Chrome (display ${DISPLAY}) =="
echo "log=${LOG}"
echo "profile: $(cd "$(dirname "$0")" && pwd)/oracle_real_chrome_profile"

if is_listening 6080 && pgrep -af "x11vnc.*-display ${DISPLAY}" >/dev/null 2>&1; then
  echo "OK: noVNC already listening on localhost:6080 (x11vnc on ${DISPLAY})"
  exit 0
fi

if is_listening 6080; then
  echo "NOTE: port 6080 in use but x11vnc not on ${DISPLAY}; restarting noVNC stack for CDP display..."
  pkill -f "websockify.*6080" 2>/dev/null || true
  pkill -f "x11vnc.*rfbport 5900" 2>/dev/null || true
  sleep 1
fi

need_bins=()
for b in x11vnc websockify; do
  if ! command -v "$b" >/dev/null 2>&1; then
    need_bins+=("$b")
  fi
done

if (( ${#need_bins[@]} > 0 )); then
  echo "Missing: ${need_bins[*]}"
  if sudo -n true 2>/dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y x11vnc novnc websockify
  else
    echo "ERROR: install manually: sudo apt-get install -y x11vnc novnc websockify"
    exit 2
  fi
fi

if ! pgrep -af "Xvfb[[:space:]]+${DISPLAY}([[:space:]]|$)" >/dev/null 2>&1; then
  echo "WARNING: Xvfb ${DISPLAY} not detected."
  echo "Start CDP Chrome first, e.g.:"
  echo "  systemctl start mirror-world-instorebotforwarder-chrome-cdp"
  echo "  or: bash Chromerrunner/start_chrome_oracle_cdp.sh --with-xvfb"
fi

NOVNC_DIR=""
for d in /usr/share/novnc /usr/share/noVNC /opt/novnc; do
  if [[ -d "$d" ]]; then
    NOVNC_DIR="$d"
    break
  fi
done
if [[ -z "$NOVNC_DIR" ]]; then
  echo "ERROR: noVNC web files not found (/usr/share/novnc)."
  exit 3
fi

echo "Starting x11vnc on ${DISPLAY} + websockify :6080 ..."
(
  set -x
  if ! is_listening 5900; then
    nohup bash -lc "DISPLAY=${DISPLAY} x11vnc -display ${DISPLAY} -localhost -shared -forever -nopw -rfbport 5900" >>"$LOG" 2>&1 &
    sleep 0.8
  fi
  nohup bash -lc "websockify --web=${NOVNC_DIR} 6080 localhost:5900" >>"$LOG" 2>&1 &
) || true

sleep 1
if is_listening 6080; then
  echo "OK: noVNC on http://127.0.0.1:6080/vnc.html (tunnel from your PC)"
  echo "CDP profile Chrome should be visible on display ${DISPLAY}"
  curl -s http://127.0.0.1:9222/json/version 2>/dev/null | head -c 200 || echo "(CDP :9222 not responding yet)"
  echo
  exit 0
fi

echo "ERROR: noVNC did not start. Log tail:"
tail -n 60 "$LOG" 2>/dev/null || true
exit 4
