#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROFILE_DIR="$SCRIPT_DIR/target_real_chrome_profile_linux"
mkdir -p "$PROFILE_DIR"

if ! command -v google-chrome >/dev/null 2>&1; then
  echo "google-chrome not found in PATH."
  echo "Try: which google-chrome"
  exit 1
fi

echo "Using: $(command -v google-chrome)"
google-chrome --version || true
echo
echo "Starting Chrome with remote debugging on 127.0.0.1:9222"
echo "Profile dir: $PROFILE_DIR"
echo

google-chrome \
  --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9222 \
  --user-data-dir="$PROFILE_DIR" \
  --no-first-run \
  --no-default-browser-check \
  "https://www.target.com/"

