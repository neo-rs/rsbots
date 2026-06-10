"""
Canonical Mavely session + CDP Chrome helpers (RSForwarder).

Uses the shared Chromerrunner CDP Chrome (oracle_real_chrome_profile on :9222) — the same
browser Instorebotforwarder attaches to and that oracle_novnc_tunnel.bat exposes for manual login.

No separate Playwright browser profiles and no RSForwarder noVNC stack.
Optional CDP auto-fill login uses mavely_login_email/password in the real shared Chrome.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

DEFAULT_CDP_URL = "http://127.0.0.1:9222"
DEFAULT_BASE_URL = "https://creators.joinmavely.com"
MAVELY_COOKIE_DOMAIN_MARKERS = ("joinmavely.com",)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_chrome_cdp_url(cfg: Optional[dict] = None) -> str:
    raw = str((cfg or {}).get("chrome_cdp_url") or "").strip()
    if not raw:
        raw = (os.getenv("CHROME_CDP_URL", "") or os.getenv("MAVELY_CDP_URL", "") or "").strip()
    return raw or DEFAULT_CDP_URL


def resolve_chrome_profile_dir(cfg: Optional[dict] = None) -> Path:
    """Canonical real Chrome profile (Chromerrunner)."""
    raw = str((cfg or {}).get("chrome_profile_dir") or "").strip()
    if not raw:
        raw = (os.getenv("CHROME_PROFILE_DIR", "") or "").strip()
    if not raw:
        raw = str(repo_root() / "Chromerrunner" / "oracle_real_chrome_profile")
    p = Path(raw)
    if not p.is_absolute():
        p = repo_root() / p
    return p


def resolve_mavely_base_url(cfg: Optional[dict] = None) -> str:
    raw = str((cfg or {}).get("mavely_base_url") or "").strip()
    if not raw:
        raw = (os.getenv("MAVELY_BASE_URL", "") or "").strip()
    return (raw or DEFAULT_BASE_URL).rstrip("/")


def resolve_mavely_login_creds(cfg: Optional[dict] = None) -> Tuple[str, str]:
    email = str((cfg or {}).get("mavely_login_email") or os.getenv("MAVELY_LOGIN_EMAIL", "") or "").strip()
    password = str((cfg or {}).get("mavely_login_password") or os.getenv("MAVELY_LOGIN_PASSWORD", "") or "").strip()
    return email, password


def cdp_autologin_enabled(cfg: Optional[dict] = None) -> bool:
    raw = (cfg or {}).get("mavely_cdp_autologin_on_fail")
    if raw is None:
        raw = (cfg or {}).get("mavely_autologin_on_fail")
    if raw is None:
        raw = os.getenv("MAVELY_CDP_AUTOLOGIN_ON_FAIL", "") or os.getenv("MAVELY_AUTOLOGIN_ON_FAIL", "")
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(raw) if raw is not None else True


def resolve_mavely_cookies_file(cfg: Optional[dict] = None) -> Path:
    raw = str((cfg or {}).get("mavely_cookies_file") or "").strip()
    if not raw:
        raw = (os.getenv("MAVELY_COOKIES_FILE", "") or "").strip()
    if not raw:
        raw = str(Path(__file__).resolve().parent / "mavely_cookies.txt")
    p = Path(raw)
    if not p.is_absolute():
        p = repo_root() / p
    return p


def cdp_is_up(cdp_url: Optional[str] = None, *, timeout_s: float = 3.0) -> bool:
    url = (cdp_url or DEFAULT_CDP_URL).rstrip("/") + "/json/version"
    try:
        with urllib.request.urlopen(url, timeout=max(1.0, float(timeout_s))) as resp:
            return int(getattr(resp, "status", 200) or 200) == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False


def cookie_header_from_cookies(cookies: List[dict]) -> str:
    parts: List[str] = []
    for c in cookies or []:
        name = (c.get("name") or "").strip()
        value = (c.get("value") or "").strip()
        if name:
            parts.append(f"{name}={value}")
    return "; ".join(parts)


def filter_mavely_cookies(cookies: List[dict]) -> List[dict]:
    out: List[dict] = []
    for c in cookies or []:
        dom = (c.get("domain") or "").lower().lstrip(".")
        if any(m in dom for m in MAVELY_COOKIE_DOMAIN_MARKERS):
            out.append(c)
    return out


def has_next_auth(cookie_header: str) -> bool:
    h = (cookie_header or "").lower()
    return ("next-auth" in h) or ("__secure-next-auth" in h)


def session_ok(base_url: str, cookie_header: str, *, timeout_s: float = 20.0) -> bool:
    import requests

    session_url = f"{base_url.rstrip('/')}/api/auth/session"
    r = requests.get(
        session_url,
        headers={"Cookie": cookie_header, "User-Agent": "Mozilla/5.0"},
        timeout=max(5.0, float(timeout_s)),
    )
    ct = (r.headers.get("content-type") or "").lower()
    if r.status_code != 200 or "application/json" not in ct:
        return False
    data = r.json()
    return isinstance(data, dict) and len(data) > 0


@contextmanager
def connect_cdp(cdp_url: str) -> Iterator[Any]:
    """Attach Playwright to the shared CDP Chrome (does not launch or close Chrome)."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp((cdp_url or DEFAULT_CDP_URL).strip())
        try:
            yield browser
        finally:
            try:
                browser.close()
            except Exception:
                pass


def _collect_cdp_cookies(browser: Any) -> List[dict]:
    all_cookies: List[dict] = []
    for ctx in list(getattr(browser, "contexts", None) or []):
        try:
            all_cookies.extend(ctx.cookies())
        except Exception:
            continue
    return all_cookies


def _browser_pick_page(browser: Any, *, prefer_url_contains: Optional[str] = None) -> Optional[Any]:
    needle = (prefer_url_contains or "").strip().lower()
    for ctx in list(getattr(browser, "contexts", None) or []):
        for pg in list(getattr(ctx, "pages", None) or []):
            if not needle:
                return pg
            try:
                u = (pg.url or "").lower()
            except Exception:
                u = ""
            if needle in u:
                return pg
    return None


def _browser_new_page(browser: Any) -> Any:
    ctx = browser.contexts[0] if browser.contexts else browser.new_context()
    return ctx.new_page()


def attempt_cdp_auto_login(page: Any, *, base_url: str, email: str, password: str, timeout_s: int = 60) -> Tuple[bool, Optional[str]]:
    """
    Best-effort email/password submit in the shared CDP Chrome (real profile, not headless bot).
    Returns (attempted, error_message).
    """
    if not email or not password:
        return False, "Missing mavely_login_email / mavely_login_password."

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
            try:
                pw_any = page.locator("input[type='password']").first
                other = page.locator("input:not([type='password'])").first
                if pw_any.is_visible(timeout=1500) and other.is_visible(timeout=1500):
                    other.fill(email, timeout=3000)
                else:
                    continue
            except Exception:
                continue
        try:
            page.fill(password_sel, password, timeout=5000)
        except Exception:
            try:
                pw_any2 = page.locator("input[type='password']").first
                if pw_any2.is_visible(timeout=2000):
                    pw_any2.fill(password, timeout=3000)
                else:
                    continue
            except Exception:
                continue
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
        try:
            page.wait_for_timeout(3000)
        except Exception:
            pass
        return True, None
    return True, "Could not find login form (may require manual login / captcha / MFA)."


def _write_cookie_header(cookies_path: Path, header: str) -> None:
    tmp = cookies_path.with_suffix(cookies_path.suffix + ".tmp")
    tmp.write_text(header, encoding="utf-8")
    os.replace(str(tmp), str(cookies_path))


def harvest_mavely_cookies_from_cdp(
    *,
    cfg: Optional[dict] = None,
    cdp_url: Optional[str] = None,
    cookies_file: Optional[Path] = None,
    base_url: Optional[str] = None,
    wait_login_s: int = 0,
    try_autologin: bool = False,
) -> Tuple[bool, str]:
    """
    Read Mavely cookies from the shared CDP Chrome and write mavely_cookies.txt.

    wait_login_s > 0: poll until /api/auth/session is non-empty or timeout (manual login in noVNC).
    try_autologin: when session empty, submit mavely_login_email/password in CDP Chrome (best-effort).
    """
    cdp = (cdp_url or resolve_chrome_cdp_url(cfg)).strip()
    cookies_path = cookies_file or resolve_mavely_cookies_file(cfg)
    base = base_url or resolve_mavely_base_url(cfg)
    profile = resolve_chrome_profile_dir(cfg)

    if not cdp_is_up(cdp):
        return (
            False,
            "CDP Chrome is not running on "
            f"{cdp}. Start mirror-world-instorebotforwarder-chrome-cdp.service "
            "or run oracle_novnc_tunnel.bat, then log into Mavely in that browser.",
        )

    cookies_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = None if int(wait_login_s or 0) <= 0 else (time.time() + max(15, int(wait_login_s)))

    header = ""
    autologin_tried = False
    try:
        with connect_cdp(cdp) as browser:
            while True:
                raw = _collect_cdp_cookies(browser)
                mavely = filter_mavely_cookies(raw)
                header = cookie_header_from_cookies(mavely)
                if has_next_auth(header) and session_ok(base, header):
                    _write_cookie_header(cookies_path, header)
                    return True, f"OK: harvested Mavely cookies from CDP Chrome ({cdp}) -> {cookies_path}"

                should_autologin = bool(try_autologin) and (not autologin_tried)
                if should_autologin:
                    email, password = resolve_mavely_login_creds(cfg)
                    if email and password:
                        autologin_tried = True
                        page = _browser_pick_page(browser, prefer_url_contains="joinmavely")
                        ephemeral_login_tab = False
                        if page is None:
                            page = _browser_new_page(browser)
                            ephemeral_login_tab = True
                        try:
                            attempt_cdp_auto_login(
                                page, base_url=base, email=email, password=password, timeout_s=60
                            )
                            time.sleep(2)
                        finally:
                            if ephemeral_login_tab:
                                try:
                                    page.close()
                                except Exception:
                                    pass
                        continue

                if deadline is None or time.time() >= deadline:
                    break
                time.sleep(2)

        if not has_next_auth(header):
            return (
                False,
                "CDP Chrome has no Mavely login cookies. Open oracle_novnc_tunnel.bat, "
                f"log into {base} in the shared Chrome (profile: {profile}), then run !rsmavelysync.",
            )
        return (
            False,
            "Mavely session is empty in CDP Chrome. Log in via oracle_novnc_tunnel.bat, then run !rsmavelysync.",
        )
    except Exception as e:
        return False, f"CDP cookie harvest failed: {type(e).__name__}: {str(e)[:300]}"


def _close_cdp_page(playwright_handle: Any, page: Any, created_new_page: bool) -> None:
    if created_new_page:
        try:
            page.close()
        except Exception:
            pass
    if playwright_handle is not None:
        try:
            playwright_handle.stop()
        except Exception:
            pass


def playwright_page_via_cdp(
    cdp_url: str,
    *,
    prefer_url_contains: Optional[str] = None,
    ephemeral: bool = True,
) -> Tuple[Any, Any, Any, bool]:
    """Return (playwright_handle, browser, page, created_new_page). Prefer ephemeral for bot resolves."""
    from playwright.sync_api import sync_playwright

    p = sync_playwright().start()
    browser = p.chromium.connect_over_cdp(cdp_url.strip())
    if ephemeral:
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        return p, browser, ctx.new_page(), True

    page = None
    created = False
    needle = (prefer_url_contains or "").strip().lower()
    for ctx in list(browser.contexts or []):
        for pg in list(ctx.pages or []):
            try:
                u = (pg.url or "").lower()
            except Exception:
                u = ""
            if needle and needle in u:
                page = pg
                break
        if page:
            break
    if page is None:
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        page = ctx.new_page()
        created = True
    return p, browser, page, created


def resolve_url_via_cdp(
    url: str,
    *,
    cdp_url: Optional[str] = None,
    timeout_ms: int = 60_000,
    accept_url: Optional[Callable[[str], bool]] = None,
    settle_ms: int = 5_000,
    poll_s: float = 10.0,
) -> Optional[str]:
    """Navigate shared CDP Chrome to url and return merchant URL when accept_url passes."""
    u = (url or "").strip()
    if not u.startswith("http"):
        return None
    cdp = (cdp_url or resolve_chrome_cdp_url()).strip()
    if not cdp_is_up(cdp):
        return None

    t_ms = max(3_000, min(int(timeout_ms), 180_000))
    settle_ms = max(500, min(int(settle_ms), 60_000))
    poll_s = max(1.0, min(float(poll_s), 60.0))
    accept = accept_url or (lambda _x: True)

    p = None
    page = None
    created = False
    result: Optional[str] = None
    try:
        p, _browser, page, created = playwright_page_via_cdp(cdp, ephemeral=True)
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=t_ms)
        except Exception:
            pass
        try:
            page.wait_for_timeout(int(settle_ms))
        except Exception:
            pass
        cur = ""
        poll_end = time.time() + poll_s
        while time.time() < poll_end:
            try:
                cur = (page.url or "").strip()
            except Exception:
                cur = ""
            if cur.startswith("http") and accept(cur):
                result = cur
                break
            try:
                page.wait_for_timeout(1_000)
            except Exception:
                break
        if result is None and cur.startswith("http") and accept(cur):
            result = cur
    except Exception:
        return None
    finally:
        if p is not None:
            _close_cdp_page(p, page, created)
    return result


def write_status_snapshot(
    path: Path,
    *,
    preflight_ok: bool,
    preflight_status: Optional[int],
    preflight_err: str,
    last_harvest_ok: Optional[bool],
    last_harvest_msg: str,
    last_harvest_ts: float,
    cdp_url: str,
    cookies_file: Path,
    profile_dir: Path,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    data: Dict[str, Any] = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "preflight_ok": bool(preflight_ok),
        "preflight_status": preflight_status,
        "preflight_err": (preflight_err or "")[:500],
        "last_harvest_ok": last_harvest_ok,
        "last_harvest_msg": (last_harvest_msg or "")[:1200],
        "last_harvest_ts": float(last_harvest_ts or 0.0),
        "chrome_cdp_url": cdp_url,
        "chrome_profile_dir": str(profile_dir),
        "mavely_cookies_file": str(cookies_file),
    }
    if isinstance(extra, dict):
        data.update(extra)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(str(tmp), str(path))
