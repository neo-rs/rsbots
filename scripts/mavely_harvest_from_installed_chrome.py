#!/usr/bin/env python3
"""
Harvest Mavely-related cookies + probe a mavely.app.link URL using YOUR installed Chrome profile
(where you are already logged in).

This does NOT run the bundled Playwright Chromium profile (.mavely_profile). It uses:

  channel=\"chrome\" + Chrome's real User Data dir + --profile-directory=...

REQUIREMENTS
------------
- Close all Chrome windows first (otherwise the profile is locked and launch fails).
- Playwright installed:  py -3 -m pip install playwright && py -3 -m playwright install chrome
  (install chrome channel, not only chromium)

USAGE (Windows, default profile)
--------------------------------
  cd mirror-world
  py -3 scripts/mavely_harvest_from_installed_chrome.py --url \"https://mavely.app.link/4IJZuyvIH1b\"

Writes RSForwarder/mavely_cookies.txt (override with --out).

USAGE (Edge instead of Chrome)
------------------------------
  py -3 scripts/mavely_harvest_from_installed_chrome.py --browser edge --url \"https://mavely.app.link/xxxx\"

ENV
---
  CHROME_USER_DATA   Override User Data directory
  CHROME_PROFILE     Profile folder name (Default, Profile 1, ...)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RSF = REPO_ROOT / "RSForwarder"


def _default_user_data_dir(browser: str) -> str:
    b = (browser or "chrome").strip().lower()
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA") or ""
        if b == "edge":
            return str(Path(local) / "Microsoft" / "Edge" / "User Data")
        return str(Path(local) / "Google" / "Chrome" / "User Data")
    if sys.platform == "darwin":
        home = Path.home()
        if b == "edge":
            return str(home / "Library/Application Support/Microsoft Edge")
        return str(home / "Library/Application Support/Google/Chrome")
    # Linux
    home = Path.home()
    if b == "edge":
        return str(home / ".config/microsoft-edge")
    return str(home / ".config/google-chrome")


def _cookie_header_from_cookies(cookies: list) -> str:
    parts: list[str] = []
    for c in cookies or []:
        name = (c.get("name") or "").strip()
        value = (c.get("value") or "").strip()
        if not name:
            continue
        parts.append(f"{name}={value}")
    return "; ".join(parts)


def _mavely_related_cookie_header(all_cookies: list) -> str:
    """Keep cookies likely needed for Mavely / Branch / CF (drop unrelated noise)."""
    keep_suffixes = (
        "mavely",
        "joinmavely",
        "app.link",
        "branch.io",
        "cloudflare",
        "cf_clearance",
    )
    out: list[dict] = []
    for c in all_cookies or []:
        dom = (c.get("domain") or "").lower()
        name = (c.get("name") or "").lower()
        if any(x in dom for x in keep_suffixes) or any(x in name for x in ("cf_", "mavely", "branch")):
            out.append(c)
    if not out:
        return _cookie_header_from_cookies(all_cookies)
    return _cookie_header_from_cookies(out)


def main() -> int:
    ap = argparse.ArgumentParser(description="Harvest Mavely cookies from installed Chrome/Edge + probe short link.")
    ap.add_argument("--browser", choices=("chrome", "edge"), default="chrome")
    ap.add_argument(
        "--user-data-dir",
        default="",
        help="Chrome/Edge User Data directory (default: standard per-OS path)",
    )
    ap.add_argument(
        "--profile-directory",
        default="",
        help="Profile inside User Data (default: env CHROME_PROFILE or 'Default')",
    )
    ap.add_argument(
        "--url",
        default="https://mavely.app.link/4IJZuyvIH1b",
        help="Mavely short link or hub URL to open",
    )
    ap.add_argument(
        "--out",
        default=str(RSF / "mavely_cookies.txt"),
        help="Write Cookie header string for RSForwarder/mavely_client",
    )
    ap.add_argument("--timeout-ms", type=int, default=120_000)
    ap.add_argument(
        "--no-write",
        action="store_true",
        help="Only print diagnostics; do not write cookie file",
    )
    ap.add_argument(
        "--all-cookies",
        action="store_true",
        help="Write full cookie jar header (not filtered to Mavely-related domains)",
    )
    args = ap.parse_args()

    user_data = (args.user_data_dir or os.getenv("CHROME_USER_DATA") or "").strip() or _default_user_data_dir(
        args.browser
    )
    prof = (args.profile_directory or os.getenv("CHROME_PROFILE") or "Default").strip() or "Default"
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path

    channel = "chrome" if args.browser == "chrome" else "msedge"
    u = (args.url or "").strip()
    if not u.startswith("http"):
        print("ERROR: --url must be an http(s) URL", file=sys.stderr)
        return 2

    ud = Path(user_data)
    if not ud.is_dir():
        print(f"ERROR: User Data dir not found: {ud}", file=sys.stderr)
        return 2

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: pip install playwright && playwright install chrome", file=sys.stderr)
        return 3

    print(f"Using channel={channel!r} user_data_dir={ud} profile_directory={prof!r}")
    print("If launch fails with profile-in-use, close every Chrome/Edge window and retry.")

    launch_args = [
        f"--profile-directory={prof}",
        "--disable-session-crashed-bubble",
    ]

    with sync_playwright() as p:
        try:
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(ud),
                channel=channel,
                headless=False,
                args=launch_args,
                viewport={"width": 1280, "height": 800},
            )
        except Exception as e:
            print(f"ERROR: launch_persistent_context failed: {e}", file=sys.stderr)
            print("Hint: Close Chrome/Edge completely, or try --profile-directory Profile 1", file=sys.stderr)
            return 4

        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(u, wait_until="load", timeout=int(args.timeout_ms))
            try:
                page.wait_for_timeout(5000)
            except Exception:
                pass
            final_url = (page.url or "").strip()
            html = page.content() or ""
            has_next = "__NEXT_DATA__" in html
            print(f"final_url: {final_url}")
            print(f"html_len: {len(html)} __NEXT_DATA__: {has_next}")

            all_c = ctx.cookies()
            header = _cookie_header_from_cookies(all_c) if args.all_cookies else _mavely_related_cookie_header(all_c)
            print(f"cookies_kept: {len(all_c)} header_len: {len(header)}")
            if not args.no_write:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = out_path.with_suffix(out_path.suffix + ".tmp")
                tmp.write_text(header, encoding="utf-8")
                os.replace(str(tmp), str(out_path))
                print(f"OK: wrote {out_path}")
                print("Oracle: copy this file to the server RSForwarder dir (or sync) and restart the bot.")
        finally:
            ctx.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
