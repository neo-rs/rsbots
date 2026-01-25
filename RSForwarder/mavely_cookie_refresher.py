"""
Mavely Cookie Refresher (RSForwarder)

Goal:
- Keep a persistent browser profile logged into https://creators.joinmavely.com
- Export fresh cookies to a file that RSForwarder can read (MAVELY_COOKIES_FILE)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
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
    ap.add_argument(
        "--devtools-port",
        type=int,
        default=0,
        help="Expose Chromium remote debugging on this localhost port (use ssh -L to access from your PC)",
    )
    ap.add_argument(
        "--wait-login",
        type=int,
        default=600,
        help="Seconds to wait for you to finish logging in (devtools mode only, default: 600)",
    )
    ap.add_argument("--timeout", type=int, default=60, help="Seconds to wait for page navigation (default: 60)")
    args = ap.parse_args()

    devtools_mode = int(args.devtools_port or 0) > 0

    repo_root = Path(__file__).resolve().parents[1]
    base_url = _env("MAVELY_BASE_URL", "https://creators.joinmavely.com").rstrip("/")
    profile_raw = _env("MAVELY_PROFILE_DIR", str(Path(__file__).parent / ".mavely_profile"))
    cookies_raw = _env("MAVELY_COOKIES_FILE", str(Path(__file__).parent / "mavely_cookies.txt"))

    profile_dir = Path(profile_raw)
    if not profile_dir.is_absolute():
        profile_dir = repo_root / profile_dir

    cookies_file = Path(cookies_raw)
    if not cookies_file.is_absolute():
        cookies_file = repo_root / cookies_file
    session_url = f"{base_url}/api/auth/session"

    profile_dir.mkdir(parents=True, exist_ok=True)
    cookies_file.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        # Extra stability flags: the "restore pages" bubble steals focus in VNC/noVNC
        # and makes login fields unusable. Disable it.
        launch_args: List[str] = ["--disable-session-crashed-bubble"]
        if devtools_mode:
            launch_args.extend(
                [
                    f"--remote-debugging-port={int(args.devtools_port)}",
                    "--remote-debugging-address=127.0.0.1",
                ]
            )

        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=(not args.interactive),
            args=launch_args,
        )
        page = ctx.new_page()
        try:
            page.goto(f"{base_url}/home", wait_until="domcontentloaded", timeout=max(5, int(args.timeout)) * 1000)
        except Exception:
            pass

        if devtools_mode:
            port = int(args.devtools_port)
            print("DevTools mode enabled.")
            print("On your PC, create an SSH tunnel to this server, then log in via Chrome DevTools:")
            print(f"- SSH tunnel: ssh -L {port}:127.0.0.1:{port} rsadmin@<oracle-host>")
            print(f"- Then open: http://localhost:{port}")
            print("Pick the Mavely page target, then enable DevTools Screencast to interact and log in.")
            print("This script will keep polling /api/auth/session and will exit once login is detected.")

        # In interactive mode, a visible browser window is created.
        # If devtools mode is also enabled, we do NOT block on stdin; we just poll /api/auth/session below.
        if args.interactive and (not devtools_mode):
            print("Browser opened. Log into Mavely if needed, then come back here and press ENTER...")
            try:
                input()
            except KeyboardInterrupt:
                ctx.close()
                return 1
            except EOFError:
                # Detached/daemonized run: no stdin available.
                pass
        elif args.interactive and devtools_mode:
            if sys.stdin.isatty():
                print("Browser opened. Log into Mavely in the browser window. (No ENTER needed; waiting for login.)")

        cookies = ctx.cookies()
        header = _cookie_header_from_cookies(cookies)
        if (not devtools_mode) and (not header or ("next-auth" not in header and "__Secure-next-auth" not in header)):
            ctx.close()
            print("ERROR: Not logged in (missing next-auth cookies). Run with --interactive and log in.")
            return 2

        # Validate login by calling the session endpoint (NextAuth returns {} when logged out).
        try:
            import requests

            def _session_ok(cookie_header: str) -> bool:
                r = requests.get(session_url, headers={"Cookie": cookie_header, "User-Agent": "Mozilla/5.0"}, timeout=20)
                ct = (r.headers.get("content-type") or "").lower()
                if r.status_code != 200 or "application/json" not in ct:
                    return False
                data = r.json()
                return isinstance(data, dict) and len(data) > 0

            if devtools_mode:
                wait_s = int(args.wait_login)
                deadline = None if wait_s <= 0 else (time.time() + max(15, wait_s))  # minimum 15s
                ok = False
                while (not ok) and (deadline is None or time.time() < deadline):
                    time.sleep(2)
                    header = _cookie_header_from_cookies(ctx.cookies())
                    if not header or ("next-auth" not in header and "__Secure-next-auth" not in header):
                        continue
                    ok = _session_ok(header)
                if not ok:
                    ctx.close()
                    if not header or ("next-auth" not in header and "__Secure-next-auth" not in header):
                        print("ERROR: Not logged in (missing next-auth cookies). Login did not complete in time.")
                    else:
                        print("ERROR: Not logged in (session endpoint returned empty JSON). Login did not complete in time.")
                    return 2
            else:
                if not _session_ok(header):
                    ctx.close()
                    print("ERROR: Not logged in (session endpoint returned empty JSON). Run with --interactive and log in.")
                    return 2
        except Exception as e:
            ctx.close()
            print(f"ERROR: Could not validate session: {e}")
            return 3

        tmp = cookies_file.with_suffix(cookies_file.suffix + ".tmp")
        tmp.write_text(header, encoding="utf-8")
        os.replace(str(tmp), str(cookies_file))

        ctx.close()
        print(f"OK: Wrote MAVELY cookies to {cookies_file}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

