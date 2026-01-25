"""
Mavely Cookie Refresher (RSForwarder)

Goal:
- Keep a persistent browser profile logged into https://creators.joinmavely.com
- Export fresh cookies to a file that RSForwarder can read (MAVELY_COOKIES_FILE)
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
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=(not args.interactive),
        )
        page = ctx.new_page()
        try:
            page.goto(f"{base_url}/home", wait_until="domcontentloaded", timeout=max(5, int(args.timeout)) * 1000)
        except Exception:
            pass

        if args.interactive:
            print("Browser opened. Log into Mavely if needed, then come back here and press ENTER...")
            try:
                input()
            except KeyboardInterrupt:
                ctx.close()
                return 1

        cookies = ctx.cookies()
        header = _cookie_header_from_cookies(cookies)
        if not header or ("next-auth" not in header and "__Secure-next-auth" not in header):
            ctx.close()
            print("ERROR: Not logged in (missing next-auth cookies). Run with --interactive and log in.")
            return 2

        tmp = cookies_file.with_suffix(cookies_file.suffix + ".tmp")
        tmp.write_text(header, encoding="utf-8")
        os.replace(str(tmp), str(cookies_file))

        ctx.close()
        print(f"OK: Wrote MAVELY cookies to {cookies_file}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

