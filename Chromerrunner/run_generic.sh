#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PY_BIN="${PY_BIN:-python3}"
if ! command -v "$PY_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PY_BIN"
  exit 1
fi

if [[ ! -d ".venv" ]]; then
  "$PY_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install -U pip
python -m pip install -r requirements.txt

echo
echo "Example:"
echo "  python generic_product_checker.py --url \"https://www.walmart.com/ip/...\" --headless"
echo "  python generic_product_checker.py --url-file urls.txt --headless"
echo

python generic_product_checker.py "$@"

