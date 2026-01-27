"""
Mavely Cookie Refresher (Option B)

Goal:
- Keep a persistent browser profile logged into https://creators.joinmavely.com
- Export fresh cookies to a file that the bot can read (MAVELY_COOKIES_FILE)

First run (interactive):
  python Instorebotforwarder/mavely_cookie_refresher.py --interactive
  - A browser opens. Log in manually.
  - Return to the terminal and press ENTER.

After that, schedule periodic runs (headless):
  python Instorebotforwarder/mavely_cookie_refresher.py

Env vars:
- MAVELY_BASE_URL: default https://creators.joinmavely.com
- MAVELY_PROFILE_DIR: where Playwright stores the persistent Chrome profile
- MAVELY_COOKIES_FILE: where to write the cookie header string (name=value; ...)

Notes:
- This does NOT print cookies/tokens to the terminal.
- If Mavely uses 2FA, the first interactive login may require a code.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List

from playwright.sync_api import sync_playwright  # type: ignore


def _env(name: str, default: str) -> str:
    return (os.getenv(name, default) or "").strip()


def _cookie_header_from_cookies(cookies: List[dict]) -> str:
    parts: List[str] = []
    for c in cookies or []:
        name = (c.get("name") or "").strip()
        value = (c.get("value") or "").strip()
        if not name:
            continue
        parts.append(f"{name}={value}")
    # Join in the same format browsers send
    return "; ".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interactive", action="store_true", help="Open a visible browser for manual login")
    ap.add_argument("--timeout", type=int, default=60, help="Seconds to wait for page navigation (default: 60)")
    args = ap.parse_args()

    base_url = _env("MAVELY_BASE_URL", "https://creators.joinmavely.com").rstrip("/")
    profile_dir = Path(_env("MAVELY_PROFILE_DIR", str(Path(__file__).parent / ".mavely_profile")))
    cookies_file = Path(_env("MAVELY_COOKIES_FILE", str(Path(__file__).parent / "mavely_cookies.txt")))

    profile_dir.mkdir(parents=True, exist_ok=True)
    cookies_file.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        # Persistent context keeps you logged in between runs.
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=(not args.interactive),
        )
        page = ctx.new_page()

        # Navigate to home (if logged in, this should load authenticated UI)
        try:
            page.goto(f"{base_url}/home", wait_until="domcontentloaded", timeout=max(5, int(args.timeout)) * 1000)
        except Exception:
            # still continue; maybe network hiccup
            pass

        if args.interactive:
            # Give user time to login (2FA etc). We can't reliably automate Cloudflare/2FA.
            print("Browser opened. Log into Mavely if needed, then come back here and press ENTER...")
            try:
                input()
            except KeyboardInterrupt:
                ctx.close()
                return 1

        # Export cookies for the base domain
        cookies = ctx.cookies()
        header = _cookie_header_from_cookies(cookies)
        if not header or ("next-auth" not in header and "__Secure-next-auth" not in header):
            # Not logged in (or wrong domain). Keep it simple.
            ctx.close()
            print("ERROR: Not logged in (missing next-auth cookies). Run with --interactive and log in.")
            return 2

        # Write atomically
        tmp = cookies_file.with_suffix(cookies_file.suffix + ".tmp")
        tmp.write_text(header, encoding="utf-8")
        os.replace(str(tmp), str(cookies_file))

        ctx.close()
        print(f"OK: Wrote MAVELY cookies to {cookies_file}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

