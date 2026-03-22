#!/usr/bin/env python3
"""
Harvest Mavely-related cookies + probe a mavely.app.link URL using YOUR installed Chrome profile
(where you are already logged in).

This does NOT run the bundled Playwright Chromium profile (.mavely_profile). It uses:

  channel=\"chrome\" + Chrome's real User Data dir + --profile-directory=...

REQUIREMENTS
------------
- Playwright:  py -3 -m pip install playwright && py -3 -m playwright install chrome

MODE A — Playwright launches Chrome (often breaks on Chrome 146+ real profiles → exit code 21)
------------------------------------------------------------------
- Close ALL Chrome first (SingletonLock). Then run the script as below.

MODE B — You start Chrome, script attaches over CDP (RECOMMENDED when Mode A crashes)
--------------------------------------------------------------------------------------
1) Close every Chrome window.
2) Start Chrome yourself (copy/paste; adjust profile if needed):

     \"C:\\\\Program Files\\\\Google\\\\Chrome\\\\Application\\\\chrome.exe\" ^
       --remote-debugging-port=9222 ^
       --user-data-dir=\"%LOCALAPPDATA%\\\\Google\\\\Chrome\\\\User Data\" ^
       --profile-directory=\"Profile 1\"

3) In another terminal:

     py -3 scripts\\\\mavely_harvest_from_installed_chrome.py --cdp-url http://127.0.0.1:9222 --url \"https://mavely.app.link/xxxx\"

Or:  py -3 scripts\\\\mavely_harvest_from_installed_chrome.py --print-chrome-debug-cmd
     (prints the exact chrome.exe line for your machine)

Writes RSForwarder/mavely_cookies.txt (override with --out).

USAGE (Windows, Mode A)
-----------------------
  cd mirror-world
  py -3 scripts/mavely_harvest_from_installed_chrome.py --url \"https://mavely.app.link/4IJZuyvIH1b\"

Profile: script reads User Data\\\\Local State (last used) or set CHROME_PROFILE / --profile-directory.

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
import json
import os
import sys
from pathlib import Path
from typing import Optional

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


def _last_used_profile_from_local_state(user_data: Path) -> Optional[str]:
    """
    Chrome/Edge store the last profile in User Data/Local State (e.g. Profile 1, not Default).
    """
    p = user_data / "Local State"
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
    except Exception:
        return None
    prof = data.get("profile")
    if not isinstance(prof, dict):
        return None
    lap = prof.get("last_active_profiles")
    if isinstance(lap, list) and lap:
        name = str(lap[0]).strip()
        if name:
            return name
    lu = str(prof.get("last_used") or "").strip()
    return lu or None


def _chrome_singleton_locked(user_data: Path) -> bool:
    """True if Chrome likely still has this User Data dir open."""
    lock = user_data / "SingletonLock"
    try:
        return lock.exists()
    except OSError:
        return False


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
        help="Profile inside User Data (default: CHROME_PROFILE, else last-used from Local State, else Default)",
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
    ap.add_argument(
        "--cdp-url",
        default="",
        help="Attach to an already running Chrome (e.g. http://127.0.0.1:9222) instead of Playwright launch",
    )
    ap.add_argument(
        "--print-chrome-debug-cmd",
        action="store_true",
        help="Print a chrome.exe command line with --remote-debugging-port=9222 and exit",
    )
    args = ap.parse_args()

    user_data = (args.user_data_dir or os.getenv("CHROME_USER_DATA") or "").strip() or _default_user_data_dir(
        args.browser
    )
    ud = Path(user_data)
    prof = (args.profile_directory or os.getenv("CHROME_PROFILE") or "").strip()
    if not prof:
        prof = _last_used_profile_from_local_state(ud) or "Default"
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path

    channel = "chrome" if args.browser == "chrome" else "msedge"
    u = (args.url or "").strip()
    if not u.startswith("http"):
        print("ERROR: --url must be an http(s) URL", file=sys.stderr)
        return 2

    if not ud.is_dir():
        print(f"ERROR: User Data dir not found: {ud}", file=sys.stderr)
        return 2

    cdp = (args.cdp_url or os.getenv("MAVELY_HARVEST_CDP_URL", "") or "").strip()

    if args.print_chrome_debug_cmd:
        exe = os.environ.get("CHROME_EXE", r"C:\Program Files\Google\Chrome\Application\chrome.exe")
        ud_q = str(ud)
        print("Close all Chrome windows, then run this ONE line in cmd.exe (not PowerShell if ^ breaks):\n")
        print(
            f'"{exe}" --remote-debugging-port=9222 '
            f'--user-data-dir="{ud_q}" --profile-directory="{prof}"\n'
        )
        print("Leave that Chrome open, then:")
        print(f'  py -3 scripts\\mavely_harvest_from_installed_chrome.py --cdp-url http://127.0.0.1:9222 --url "<mavely link>"')
        return 0

    if args.browser == "chrome" and (not cdp) and _chrome_singleton_locked(ud):
        print(
            "ERROR: Chrome still has this User Data directory open (SingletonLock exists).",
            file=sys.stderr,
        )
        print("Exit all Chrome windows (check tray), then run again.", file=sys.stderr)
        return 4

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: pip install playwright && playwright install chrome", file=sys.stderr)
        return 3

    def _probe(ctx, page) -> None:
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

    with sync_playwright() as p:
        if cdp:
            print(f"CDP attach {cdp!r} (Chrome must be running with --remote-debugging-port)")
            try:
                browser = p.chromium.connect_over_cdp(cdp)
            except Exception as e:
                print(f"ERROR: connect_over_cdp failed: {e}", file=sys.stderr)
                print("Start Chrome with: py -3 scripts\\mavely_harvest_from_installed_chrome.py --print-chrome-debug-cmd", file=sys.stderr)
                return 5
            try:
                if not browser.contexts:
                    print("ERROR: connected but no contexts (try opening a normal window in that Chrome)", file=sys.stderr)
                    return 6
                ctx = browser.contexts[0]
                page = ctx.new_page()
                try:
                    _probe(ctx, page)
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass
                return 0
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

        print(f"Using channel={channel!r} user_data_dir={ud} profile_directory={prof!r}")
        print("If this crashes with exit code 21, use --print-chrome-debug-cmd then --cdp-url.")

        launch_args = [
            f"--profile-directory={prof}",
            "--disable-session-crashed-bubble",
        ]

        try:
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(ud),
                channel=channel,
                headless=False,
                args=launch_args,
                viewport={"width": 1280, "height": 800},
                ignore_default_args=[
                    "--disable-extensions",
                    "--enable-automation",
                ],
            )
        except Exception as e:
            print(f"ERROR: launch_persistent_context failed: {e}", file=sys.stderr)
            print(
                "Chrome often exits immediately when Playwright launches a real profile. Do this instead:\n"
                "  py -3 scripts\\mavely_harvest_from_installed_chrome.py --print-chrome-debug-cmd\n"
                "Start Chrome with that line, then:\n"
                "  py -3 scripts\\mavely_harvest_from_installed_chrome.py --cdp-url http://127.0.0.1:9222 --url \"...\"",
                file=sys.stderr,
            )
            return 4

        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            _probe(ctx, page)
        finally:
            ctx.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
