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

# If you have a display/noVNC, remove --headless=new to run visibly.
$CHROME_BIN \
  --headless=new \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9222 \
  --user-data-dir="$PROFILE_DIR" \
  --no-first-run \
  --no-default-browser-check \
  --disable-dev-shm-usage \
  --no-sandbox \
  "https://www.google.com/"

