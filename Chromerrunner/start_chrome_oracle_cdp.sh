#!/usr/bin/env bash
set -euo pipefail

# Start a long-running Chrome on Oracle with CDP enabled.
# RSAdminBot Chromerrunner watcher can attach with:
#   --connect-cdp --cdp-url http://127.0.0.1:9222
#
# Notes:
# - This does NOT guarantee bypassing Cloudflare/PerimeterX. For strict retailers you typically
#   need a GUI session (noVNC/X11) once to solve the challenge and persist cookies in this profile.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE_DIR="$SCRIPT_DIR/oracle_real_chrome_profile"
mkdir -p "$PROFILE_DIR"

HEADED="no"
START_URL="https://www.google.com/"
DISPLAY_OVERRIDE=""
WITH_XVFB="no"
# Defaults match Instorebotforwarder/config.json (ebay_first8_xvfb_*) so the
# bot's CDP-attach scrapes see a viewport identical to what manual warmup used.
XVFB_DISPLAY_ARG="${XVFB_DISPLAY:-:99}"
XVFB_SCREEN_ARG="${XVFB_SCREEN:-1680x1500x24}"

# Order-independent flag parser. Replaces the previous fixed-position parsing
# so --with-xvfb / --headed / --display / --url can appear in any order. This
# also keeps a single source of truth for flag handling on this script.
while [[ $# -gt 0 ]]; do
  case "${1:-}" in
    --headed)
      HEADED="yes"; shift ;;
    --with-xvfb)
      # --with-xvfb implies headed: only useful when we want a real DISPLAY.
      HEADED="yes"; WITH_XVFB="yes"; shift ;;
    --display)
      shift || true; DISPLAY_OVERRIDE="${1:-}"; shift || true ;;
    --url)
      shift || true; START_URL="${1:-$START_URL}"; shift || true ;;
    "")
      shift || true ;;
    *)
      echo "WARNING: ignoring unknown arg: $1"
      shift || true ;;
  esac
done

# When --with-xvfb is set, this script owns the Xvfb lifecycle. We start it
# idempotently (skip if already running on the same display) so re-running the
# script or letting systemd restart it doesn't leak X servers.
if [[ "$WITH_XVFB" == "yes" ]]; then
  if pgrep -f "Xvfb[[:space:]]+${XVFB_DISPLAY_ARG}([[:space:]]|$)" >/dev/null 2>&1; then
    echo "Xvfb already running on ${XVFB_DISPLAY_ARG}; reusing"
  else
    if ! command -v Xvfb >/dev/null 2>&1; then
      echo "ERROR: --with-xvfb requested but Xvfb is not installed."
      echo "Install with:  sudo apt-get update && sudo apt-get install -y xvfb"
      exit 1
    fi
    echo "Starting Xvfb ${XVFB_DISPLAY_ARG} -screen 0 ${XVFB_SCREEN_ARG}"
    nohup Xvfb "${XVFB_DISPLAY_ARG}" -screen 0 "${XVFB_SCREEN_ARG}" \
      >/tmp/mirror-world-xvfb-${XVFB_DISPLAY_ARG#:}.log 2>&1 &
    # Brief wait for the socket; the bot uses the same wait window.
    sleep "${XVFB_WAIT_S:-2}"
  fi
  # Always export DISPLAY so the chrome launch below uses it without relying
  # on --display.
  if [[ -z "${DISPLAY_OVERRIDE:-}" ]]; then
    DISPLAY_OVERRIDE="${XVFB_DISPLAY_ARG}"
  fi
fi

is_chrome_for_testing() {
  local bin="$1"
  if [[ ! -x "$bin" ]]; then
    return 1
  fi
  local ver
  ver="$("$bin" --version 2>/dev/null || true)"
  if echo "$ver" | grep -qiE 'chrome for testing|google chrome for testing'; then
    return 0
  fi
  # Some installs only reveal this in the about/help UI, but the binary path is a strong signal.
  if echo "$bin" | grep -qiE 'chrome-for-testing|chrome[_-]for[_-]testing'; then
    return 0
  fi
  return 1
}

pick_chrome_bin() {
  # Prefer a "real" retail/stable Chrome over "Chrome for Testing" (common on automation hosts).
  # You can override explicitly:
  #   CHROME_BIN=/opt/google/chrome/google-chrome bash start_chrome_oracle_cdp.sh --headed
  if [[ -n "${CHROME_BIN:-}" ]]; then
    echo "$CHROME_BIN"
    return 0
  fi

  local candidates=(
    "/opt/google/chrome/google-chrome"
    "/usr/bin/google-chrome-stable"
    "/usr/bin/google-chrome"
    "$(command -v google-chrome-stable 2>/dev/null || true)"
    "$(command -v google-chrome 2>/dev/null || true)"
  )

  local c chosen=""
  for c in "${candidates[@]}"; do
    [[ -z "$c" ]] && continue
    [[ ! -x "$c" ]] && continue
    if is_chrome_for_testing "$c"; then
      continue
    fi
    chosen="$c"
    break
  done

  if [[ -z "$chosen" ]]; then
    # Last resort: use whatever google-chrome is, even if it's "for testing".
    chosen="$(command -v google-chrome || true)"
  fi

  echo "$chosen"
}

CHROME_BIN="$(pick_chrome_bin)"
if [[ -z "${CHROME_BIN:-}" ]] || [[ ! -x "$CHROME_BIN" ]]; then
  echo "ERROR: Chrome not found."
  echo "Install Google Chrome (stable) on Oracle, then re-run, or set CHROME_BIN explicitly."
  exit 1
fi

echo "Using: $CHROME_BIN"
if is_chrome_for_testing "$CHROME_BIN"; then
  echo "WARNING: Selected Chrome looks like **Chrome for Testing**."
  echo "Retail sites often block this harder. Install stable Google Chrome and set CHROME_BIN to it, e.g.:"
  echo "  CHROME_BIN=/opt/google/chrome/google-chrome"
fi
$CHROME_BIN --version || true
echo "Profile: $PROFILE_DIR"

# Always launch into the canonical signed-in profile (Neo Secrets / Work), never the picker.
PROFILE_SUBDIR="Default"
PATCH_PY=""
if command -v python3 >/dev/null 2>&1; then
  PATCH_PY="python3"
elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PATCH_PY="$REPO_ROOT/.venv/bin/python"
fi
if [[ -n "$PATCH_PY" ]] && [[ -f "$SCRIPT_DIR/cdp_chrome_profile.py" ]]; then
  PROFILE_SUBDIR="$("$PATCH_PY" "$SCRIPT_DIR/cdp_chrome_profile.py" patch "$PROFILE_DIR" 2>/dev/null || echo Default)"
fi
if [[ -z "${PROFILE_SUBDIR:-}" ]]; then
  PROFILE_SUBDIR="Default"
fi
echo "Profile subdirectory: $PROFILE_SUBDIR"
echo "CDP: http://127.0.0.1:9222"

cdp_listening() {
  curl -sf http://127.0.0.1:9222/json/version >/dev/null 2>&1
}

profile_chrome_running() {
  pgrep -af -- "--user-data-dir=${PROFILE_DIR}" 2>/dev/null | grep -q 'remote-debugging-port=9222'
}

# Never launch a second Chrome for the same profile (causes stacked blank windows in noVNC).
if cdp_listening; then
  echo "OK: CDP already on :9222; not launching another Chrome."
  exit 0
fi
if profile_chrome_running; then
  echo "Chrome with this profile is already running; waiting for CDP (not starting a duplicate)..."
  for _ in $(seq 1 20); do
    if cdp_listening; then
      echo "OK: CDP is up."
      exit 0
    fi
    sleep 1
  done
  echo "WARNING: Profile Chrome running but CDP not ready; exiting without a duplicate launch."
  exit 0
fi

EXTRA_ARGS=()
NAV_ARGS=()
if [[ "$HEADED" == "yes" ]]; then
  echo "Mode: HEADED (requires DISPLAY/noVNC/X11)"
  # Fill the Xvfb desktop in noVNC (otherwise Chrome opens a small window in a corner).
  EXTRA_ARGS+=(--start-maximized --window-size=1680,1500)
  detect_xvfb_display() {
    # Example: Xvfb :1 -screen 0 ...
    local line disp
    line="$(pgrep -af '^Xvfb[[:space:]]+:' || true)"
    if [[ -z "$line" ]]; then
      echo ""
      return 0
    fi
    line="$(echo "$line" | head -n 1)"
    disp="$(echo "$line" | awk '{for(i=1;i<=NF;i++){ if ($i ~ /^:[0-9]+$/) { print $i; exit } } }')"
    echo "$disp"
  }
  if [[ -n "$DISPLAY_OVERRIDE" ]]; then
    export DISPLAY="$DISPLAY_OVERRIDE"
  fi
  if [[ -z "${DISPLAY:-}" ]]; then
    d="$(detect_xvfb_display)"
    if [[ -n "$d" ]]; then
      export DISPLAY="$d"
    else
      # Common default for Chromerrunner noVNC stacks *when Xvfb is started on :99*.
      export DISPLAY=":99"
    fi
  fi
  echo "DISPLAY=${DISPLAY}"
else
  echo "Mode: HEADLESS"
  EXTRA_ARGS+=(--headless=new)
fi

# Opening a URL on every restart adds a Google tab + "Restore pages?" noise in noVNC.
# Only navigate on first profile creation; existing profiles keep their session/tabs.
if [[ ! -f "$PROFILE_DIR/Default/Preferences" ]]; then
  NAV_ARGS=("$START_URL")
  echo "First-run profile: opening $START_URL"
else
  echo "Existing profile: not forcing $START_URL (avoids extra tab on restart)"
fi

$CHROME_BIN \
  "${EXTRA_ARGS[@]}" \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9222 \
  --user-data-dir="$PROFILE_DIR" \
  --profile-directory="$PROFILE_SUBDIR" \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --disable-session-crashed-bubble \
  --no-sandbox \
  "${NAV_ARGS[@]}"

