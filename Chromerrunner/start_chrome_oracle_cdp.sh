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
if [[ "${1:-}" == "--headed" ]]; then
  HEADED="yes"
  shift || true
fi
if [[ "${1:-}" == "--url" ]]; then
  shift || true
  START_URL="${1:-$START_URL}"
  shift || true
fi

CHROME_BIN="${CHROME_BIN:-/usr/bin/google-chrome}"
if [[ ! -x "$CHROME_BIN" ]]; then
  CHROME_BIN="$(command -v google-chrome || true)"
fi
if [[ -z "${CHROME_BIN:-}" ]] || [[ ! -x "$CHROME_BIN" ]]; then
  echo "google-chrome not found."
  exit 1
fi

echo "Using: $CHROME_BIN"
$CHROME_BIN --version || true
echo "Profile: $PROFILE_DIR"
echo "CDP: http://127.0.0.1:9222"

EXTRA_ARGS=()
if [[ "$HEADED" == "yes" ]]; then
  echo "Mode: HEADED (requires DISPLAY/noVNC/X11)"
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

