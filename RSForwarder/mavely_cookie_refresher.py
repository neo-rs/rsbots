"""
Mavely cookie harvest (RSForwarder) — CDP Chrome only.

Reads session cookies from the shared Chromerrunner CDP Chrome (oracle_real_chrome_profile)
and writes RSForwarder/mavely_cookies.txt for GraphQL affiliate link creation.

Manual login: use oracle_novnc_tunnel.bat → log into creators.joinmavely.com in that Chrome.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from RSForwarder.mavely_cdp_session import harvest_mavely_cookies_from_cdp


def main() -> int:
    ap = argparse.ArgumentParser(description="Harvest Mavely cookies from shared CDP Chrome.")
    ap.add_argument(
        "--wait-login",
        type=int,
        default=0,
        help="Seconds to poll CDP Chrome for a logged-in Mavely session (default: 0 = harvest now only)",
    )
    args = ap.parse_args()

    ok, msg = harvest_mavely_cookies_from_cdp(wait_login_s=int(args.wait_login or 0))
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
