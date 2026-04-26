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
PROFILE_DIR="$SCRIPT_DIR/oracle_real_chrome_profile"
mkdir -p "$PROFILE_DIR"

HEADED="no"
START_URL="https://www.google.com/"
DISPLAY_OVERRIDE=""
if [[ "${1:-}" == "--headed" ]]; then
  HEADED="yes"
  shift || true
fi
if [[ "${1:-}" == "--display" ]]; then
  shift || true
  DISPLAY_OVERRIDE="${1:-}"
  shift || true
fi
if [[ "${1:-}" == "--url" ]]; then
  shift || true
  START_URL="${1:-$START_URL}"
  shift || true
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
echo "CDP: http://127.0.0.1:9222"

EXTRA_ARGS=()
if [[ "$HEADED" == "yes" ]]; then
  echo "Mode: HEADED (requires DISPLAY/noVNC/X11)"
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

$CHROME_BIN \
  "${EXTRA_ARGS[@]}" \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9222 \
  --user-data-dir="$PROFILE_DIR" \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --no-sandbox \
  "$START_URL"

