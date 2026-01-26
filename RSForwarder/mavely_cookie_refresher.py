"""
Mavely Cookie Refresher (RSForwarder)

Goal:
- Keep a persistent browser profile logged into https://creators.joinmavely.com
- Export fresh cookies to a file that RSForwarder can read (MAVELY_COOKIES_FILE)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

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


def _has_next_auth(cookie_header: str) -> bool:
    h = (cookie_header or "").lower()
    return ("next-auth" in h) or ("__secure-next-auth" in h)


def _attempt_auto_login(page, base_url: str, timeout_s: int) -> Tuple[bool, Optional[str]]:
    """
    Best-effort login helper.

    Uses:
      - MAVELY_LOGIN_EMAIL
      - MAVELY_LOGIN_PASSWORD

    Returns: (attempted, error_message)
    - attempted=False means "not configured" (missing creds)
    - attempted=True means "we tried" (may still need manual captcha/MFA)
    """
    email = _env("MAVELY_LOGIN_EMAIL", "")
    password = _env("MAVELY_LOGIN_PASSWORD", "")
    if not email or not password:
        return False, "Missing MAVELY_LOGIN_EMAIL / MAVELY_LOGIN_PASSWORD."

    # Try a few likely entry points (providers vary).
    # On Mavely, the actual login form is served at /auth/login (and NextAuth at /api/auth/signin).
    candidates = [
        f"{base_url}/auth/login",
        f"{base_url}/api/auth/signin?callbackUrl=%2Fhome",
        f"{base_url}/api/auth/signin",
        f"{base_url}/home",
        base_url,
    ]
    for u in candidates:
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=max(5, int(timeout_s)) * 1000)
        except Exception:
            pass

        try:
            # Prefer direct selector-based fill() calls: they automatically re-try through
            # React hydration/re-rendering and avoid stale element issues.
            try:
                page.wait_for_timeout(500)
            except Exception:
                pass

            email_sel = (
                "input[type='email'],"
                "input[id*='email' i],"
                "input[name*='email' i],"
                "input[placeholder*='email' i]"
            )
            password_sel = (
                "input[type='password'],"
                "input[id*='pass' i],"
                "input[name*='pass' i],"
                "input[placeholder*='password' i]"
            )

            try:
                page.fill(email_sel, email, timeout=5000)
            except Exception:
                # Fallback: if there's a password field, treat first non-password input as email/username.
                pw_any = page.locator("input[type='password']").first
                other = page.locator("input:not([type='password'])").first
                if pw_any.is_visible(timeout=1500) and other.is_visible(timeout=1500):
                    other.fill(email, timeout=3000)
                else:
                    raise

            try:
                page.fill(password_sel, password, timeout=5000)
            except Exception:
                pw_any2 = page.locator("input[type='password']").first
                if pw_any2.is_visible(timeout=2000):
                    pw_any2.fill(password, timeout=3000)
                else:
                    raise

            submitted = False
            try:
                btn = page.get_by_role("button", name=re.compile(r"(sign in|log in|continue|next|submit)", re.I)).first
                if btn.is_visible(timeout=1500):
                    btn.click(timeout=3000)
                    submitted = True
            except Exception:
                submitted = False

            if not submitted:
                try:
                    page.locator("input[type='password']").first.press("Enter", timeout=800)
                except Exception:
                    pass

            return True, None
        except Exception:
            continue

    return True, "Could not find login form (may require manual login / captcha / MFA)."


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interactive", action="store_true", help="Open a visible browser for manual login")
    ap.add_argument(
        "--auto-login",
        action="store_true",
        help="Attempt to sign in using MAVELY_LOGIN_EMAIL/MAVELY_LOGIN_PASSWORD (best-effort; may still need manual MFA/CAPTCHA).",
    )
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
        help="Seconds to wait for login to complete (devtools/auto-login/detached interactive; default: 600)",
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

    print("Mavely cookie refresher starting...")
    print(f"- base_url: {base_url}")
    print(f"- interactive: {bool(args.interactive)}")
    print(f"- auto_login: {bool(args.auto_login)}")
    print(f"- devtools_port: {int(args.devtools_port or 0)}")
    print(f"- DISPLAY: {os.getenv('DISPLAY', '')}")
    print(f"- profile_dir: {profile_dir}")
    print(f"- cookies_file: {cookies_file}")

    try:
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

            # For auto-login, use a normal headless context (non-persistent) so we reliably
            # hit the email/password login form. Persistent profiles sometimes redirect to
            # a landing page where the login form isn't present.
            browser = None
            if bool(args.auto_login) and (not args.interactive) and (not devtools_mode):
                browser = p.chromium.launch(headless=True, args=launch_args)
                ctx = browser.new_context()
            else:
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

            try:
                import requests

                def _session_ok(cookie_header: str) -> bool:
                    # NextAuth returns {} when logged out.
                    r = requests.get(session_url, headers={"Cookie": cookie_header, "User-Agent": "Mozilla/5.0"}, timeout=20)
                    ct = (r.headers.get("content-type") or "").lower()
                    if r.status_code != 200 or "application/json" not in ct:
                        return False
                    data = r.json()
                    return isinstance(data, dict) and len(data) > 0
            except Exception as e:
                ctx.close()
                print(f"ERROR: Could not validate session: {e}")
                return 3

            # If auto-login is enabled, try to submit credentials (best effort).
            if bool(args.auto_login):
                attempted, err = _attempt_auto_login(page, base_url=base_url, timeout_s=int(args.timeout))
                if attempted and err:
                    # Not fatal; we may still succeed via existing session or manual completion.
                    print(f"WARNING: auto-login attempt incomplete: {err}")
                # Optional debug screenshot (never includes plaintext password; useful for diagnosing CAPTCHA/MFA/errors).
                dbg = (_env("MAVELY_AUTO_LOGIN_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
                if dbg:
                    try:
                        dbg_dir = _env("MAVELY_AUTO_LOGIN_DEBUG_DIR", "/tmp") or "/tmp"
                        ts = int(time.time())
                        out = str(Path(dbg_dir) / f"mavely_auto_login_{ts}.png")
                        page.screenshot(path=out, full_page=True)
                        print(f"DEBUG: wrote {out}")
                    except Exception as e:
                        print(f"DEBUG: screenshot failed: {e}")

            # Interactive mode UX: if this is a real TTY, let the user drive.
            # In devtools mode we never block on stdin; we poll below.
            if args.interactive and (not devtools_mode) and sys.stdin.isatty() and (not args.auto_login):
                print("Browser opened. Log into Mavely if needed, then come back here and press ENTER...")
                try:
                    input()
                except KeyboardInterrupt:
                    ctx.close()
                    return 1
                except EOFError:
                    pass
            elif args.interactive and devtools_mode and sys.stdin.isatty():
                print("Browser opened. Log into Mavely in the browser window. (No ENTER needed; waiting for login.)")

            # Validate login by polling the session endpoint (safe; does not mutate).
            try:
                header = _cookie_header_from_cookies(ctx.cookies())
                if _has_next_auth(header) and _session_ok(header):
                    ok = True
                else:
                    wait_s = int(args.wait_login)
                    deadline = None if wait_s <= 0 else (time.time() + max(15, wait_s))
                    ok = False
                    while (not ok) and (deadline is None or time.time() < deadline):
                        time.sleep(2)
                        header = _cookie_header_from_cookies(ctx.cookies())
                        if not _has_next_auth(header):
                            continue
                        ok = _session_ok(header)

                if not ok:
                    ctx.close()
                    if not _has_next_auth(header):
                        print("ERROR: Not logged in (missing next-auth cookies). Login did not complete in time.")
                    else:
                        print("ERROR: Not logged in (session endpoint returned empty JSON). Login did not complete in time.")
                    return 2
            except Exception as e:
                ctx.close()
                print(f"ERROR: Could not validate session: {e}")
                return 3

            tmp = cookies_file.with_suffix(cookies_file.suffix + ".tmp")
            tmp.write_text(header, encoding="utf-8")
            os.replace(str(tmp), str(cookies_file))

            ctx.close()
            try:
                if browser is not None:
                    browser.close()
            except Exception:
                pass
            print(f"OK: Wrote MAVELY cookies to {cookies_file}")
            return 0
    except Exception as e:
        # This is the common failure when Chromium isn't installed for Playwright or OS deps are missing.
        # Make the log actionable so users can fix it quickly on the server.
        print("ERROR: Failed to launch Chromium via Playwright.")
        print(f"ERROR: {e}")
        print("Fix on the Oracle server (run these in the mirror-world venv):")
        print(f"- {sys.executable} -m playwright install chromium")
        print(f"- {sys.executable} -m playwright install-deps chromium  (needs sudo)")
        return 4


if __name__ == "__main__":
    raise SystemExit(main())

