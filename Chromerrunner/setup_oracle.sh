#!/usr/bin/env bash
set -euo pipefail

# Oracle setup helper for Chromerrunner.
# Creates a local venv, installs requirements, and optionally installs Playwright browsers.
#
# Usage:
#   ./setup_oracle.sh
#   ./setup_oracle.sh --install-browsers
#
# Notes:
# - If you only use CDP mode (connect to system google-chrome), you usually do NOT need
#   Playwright's bundled browser downloads.
# - Headless mode via `p.chromium.launch()` requires Playwright browser binaries unless you
#   modify scripts to launch system Chrome explicitly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

INSTALL_BROWSERS="no"
if [[ "${1:-}" == "--install-browsers" ]]; then
  INSTALL_BROWSERS="yes"
fi

PY_BIN="${PY_BIN:-python3}"
if ! command -v "$PY_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PY_BIN"
  exit 1
fi

echo "Working dir: $SCRIPT_DIR"
echo "Python: $("$PY_BIN" --version 2>&1 || true)"

if [[ ! -d ".venv" ]]; then
  echo "Creating venv..."
  "$PY_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install -U pip wheel
python -m pip install -r requirements.txt

if [[ "$INSTALL_BROWSERS" == "yes" ]]; then
  echo
  echo "Installing Playwright browsers (chromium) + OS deps..."
  python -m playwright install --with-deps chromium
else
  echo
  echo "Skipping Playwright browser downloads."
  echo "CDP mode will use system google-chrome on Oracle."
fi

echo
echo "Setup complete."

