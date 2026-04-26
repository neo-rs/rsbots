#!/usr/bin/env bash
set -euo pipefail

# Best-effort noVNC starter for Oracle Ubuntu.
#
# What it does:
# - Ensures a local virtual display exists (Xvfb :99)
# - Starts a tiny window manager (fluxbox)
# - Starts x11vnc on localhost:5900
# - Starts noVNC/websockify on localhost:6080 (serves /usr/share/novnc)
#
# Result:
# - Tunnel port 6080 to your PC and open: http://127.0.0.1:6080/vnc.html
#
# Notes:
# - Requires packages: xvfb, fluxbox, x11vnc, novnc, websockify
# - Will try to install via sudo apt-get if missing.
# - Uses a simple passwordless VNC (localhost only). Access is via your SSH tunnel.

LOG="/tmp/chromerrunner_novnc.log"

is_listening() {
  local port="$1"
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${port}\$" || return 1
  return 0
}

detect_xvfb_display_num() {
  # Prefer an already-running Xvfb display if present (common on long-lived servers).
  # Example process: Xvfb :1 -screen 0 ...
  local line disp num
  line="$(pgrep -af '^Xvfb[[:space:]]+:' || true)"
  if [[ -z "$line" ]]; then
    echo ""
    return 0
  fi
  line="$(echo "$line" | head -n 1)"
  disp="$(echo "$line" | awk '{for(i=1;i<=NF;i++){ if ($i ~ /^:[0-9]+$/) { print $i; exit } } }')"
  num="${disp#:}"
  echo "$num"
}

echo "== Chromerrunner noVNC starter =="
echo "log=${LOG}"

if is_listening 6080; then
  echo "OK: noVNC already listening on :6080"
  detected="$(detect_xvfb_display_num)"
  if [[ -n "${detected}" ]]; then
    echo "Detected existing Xvfb display: :${detected}"
    echo "If headed Chrome shows no window, align DISPLAY to this value."
  else
    echo "NOTE: Could not detect an Xvfb display number from running processes."
  fi
  exit 0
fi

DISPLAY_NUM="${DISPLAY_NUM:-}"
if [[ -z "${DISPLAY_NUM}" ]]; then
  DISPLAY_NUM="$(detect_xvfb_display_num)"
fi
if [[ -z "${DISPLAY_NUM}" ]]; then
  DISPLAY_NUM="99"
fi
DISPLAY=":${DISPLAY_NUM}"
echo "display=${DISPLAY}"

need_bins=()
for b in Xvfb fluxbox x11vnc websockify; do
  if ! command -v "$b" >/dev/null 2>&1; then
    need_bins+=("$b")
  fi
done

if (( ${#need_bins[@]} > 0 )); then
  echo "Missing binaries: ${need_bins[*]}"
  echo "Attempting apt-get install (requires sudo)..."
  if sudo -n true 2>/dev/null; then
    sudo apt-get update -y
    # Package names: websockify is separate on Ubuntu; novnc ships /usr/share/novnc
    sudo apt-get install -y xvfb fluxbox x11vnc novnc websockify
  else
    echo "ERROR: sudo password prompt required (or sudo not allowed)."
    echo "Run this once manually:"
    echo "  sudo apt-get update -y"
    echo "  sudo apt-get install -y xvfb fluxbox x11vnc novnc websockify"
    exit 2
  fi
fi

NOVNC_DIR=""
for d in /usr/share/novnc /usr/share/noVNC /opt/novnc; do
  if [[ -d "$d" ]]; then
    NOVNC_DIR="$d"
    break
  fi
done

if [[ -z "$NOVNC_DIR" ]]; then
  echo "ERROR: noVNC web files not found (expected /usr/share/novnc)."
  echo "Check package install: sudo apt-get install -y novnc"
  exit 3
fi

echo "Using noVNC dir: $NOVNC_DIR"

echo "Starting services (logs -> $LOG)..."
(
  set -x
  nohup bash -lc "Xvfb ${DISPLAY} -screen 0 1280x720x24 -ac +extension GLX +render -noreset" >>"$LOG" 2>&1 &
  sleep 0.5
  nohup bash -lc "DISPLAY=${DISPLAY} fluxbox" >>"$LOG" 2>&1 &
  sleep 0.5
  nohup bash -lc "DISPLAY=${DISPLAY} x11vnc -localhost -shared -forever -nopw -rfbport 5900" >>"$LOG" 2>&1 &
  sleep 0.5
  nohup bash -lc "websockify --web=${NOVNC_DIR} 6080 localhost:5900" >>"$LOG" 2>&1 &
) || true

sleep 1
if is_listening 6080; then
  echo "OK: noVNC listening on localhost:6080"
  echo "Next: SSH tunnel -L 6080:127.0.0.1:6080 then open http://127.0.0.1:6080/vnc.html"
  exit 0
fi

echo "ERROR: noVNC did not start. Tail of log:"
tail -n 80 "$LOG" 2>/dev/null || true
exit 4

