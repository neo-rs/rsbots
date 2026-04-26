#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

python3 -m pip install -r requirements.txt
python3 -m playwright install chromium

python3 amazon_asin_promo_checker.py
#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
python3 -m pip install -r requirements.txt
python3 -m playwright install chromium
python3 amazon_asin_promo_checker.py
