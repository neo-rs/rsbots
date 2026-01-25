"""
Sync Mavely tokens from /api/auth/session into local files/env.

Why:
- Using the full Cookie header (incl. cf_clearance) makes /api/auth/session reliable.
- Session JSON includes refreshToken/idToken/access token + expires.
- We can write refreshToken to MAVELY_REFRESH_TOKEN_FILE so rotation is handled.

Safe output:
- Never prints token/cookie values.
- Prints only booleans/lengths and the session "expires" timestamp.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import requests

# Ensure repo root is importable when running from scripts/.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from RSForwarder.mavely_client import MavelyClient, _session_url, _jwt_payload


def _read_text(path: str) -> str:
    try:
        return (Path(path).read_text(encoding="utf-8", errors="replace") or "").strip()
    except Exception:
        return ""


def _write_text(path: str, content: str) -> bool:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content + "\n", encoding="utf-8")
        return True
    except Exception:
        return False


def _pick_tokens(session_json: dict) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    user = session_json.get("user") if isinstance(session_json.get("user"), dict) else {}
    access = (
        session_json.get("token")
        or session_json.get("accessToken")
        or (user.get("token") if isinstance(user, dict) else None)
        or (user.get("accessToken") if isinstance(user, dict) else None)
    )
    idt = session_json.get("idToken") or (user.get("idToken") if isinstance(user, dict) else None)
    rt = session_json.get("refreshToken") or (user.get("refreshToken") if isinstance(user, dict) else None)
    expires = session_json.get("expires")
    return (
        str(access).strip() if isinstance(access, str) and access.strip() else None,
        str(idt).strip() if isinstance(idt, str) and idt.strip() else None,
        str(rt).strip() if isinstance(rt, str) and rt.strip() else None,
        str(expires).strip() if isinstance(expires, str) and expires.strip() else None,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cookies-file", default=os.environ.get("MAVELY_COOKIES_FILE", ""), help="Path to full Cookie header text file.")
    ap.add_argument("--refresh-file", default=os.environ.get("MAVELY_REFRESH_TOKEN_FILE", ""), help="Path to write refresh token.")
    ap.add_argument("--auth-file", default="", help="Optional path to write session bearer token.")
    ap.add_argument("--id-file", default="", help="Optional path to write session idToken.")
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    cookies_file = (args.cookies_file or "").strip()
    refresh_file = (args.refresh_file or "").strip()
    if not cookies_file:
        print("ERROR: missing --cookies-file (or MAVELY_COOKIES_FILE).")
        return 2
    if not refresh_file:
        print("ERROR: missing --refresh-file (or MAVELY_REFRESH_TOKEN_FILE).")
        return 2

    cookie_header = _read_text(cookies_file)
    print(f"cookies_file={cookies_file} len={len(cookie_header)}")

    os.environ["MAVELY_COOKIES"] = cookie_header
    base = (os.environ.get("MAVELY_BASE_URL", "") or "https://creators.joinmavely.com").strip()
    os.environ["MAVELY_BASE_URL"] = base

    c = MavelyClient(session_token=cookie_header, timeout_s=int(args.timeout))
    r = requests.get(_session_url(c.base_url), headers=c._session_headers(), timeout=int(args.timeout))
    ct = (r.headers.get("content-type") or "").lower()
    is_json = ("application/json" in ct) and (r.status_code == 200)
    print(f"session_status={r.status_code} is_json={is_json}")
    if not is_json:
        return 3

    data = r.json()
    if not isinstance(data, dict) or len(data) == 0:
        print("session_empty=true")
        return 4

    access, idt, rt, expires = _pick_tokens(data)
    print(f"session_expires={expires or '(missing)'}")
    print(f"has_access={bool(access)} has_idToken={bool(idt)} has_refreshToken={bool(rt)}")

    # Derive likely client_id from JWT claims (non-secret)
    cid = ""
    for t in (idt, access):
        p = _jwt_payload(t or "") or {}
        azp = p.get("azp")
        aud = p.get("aud")
        if isinstance(azp, str) and azp.strip():
            cid = azp.strip()
            break
        if isinstance(aud, str) and aud.strip():
            cid = aud.strip()
            break
    print(f"derived_client_id_present={bool(cid)}")
    if cid:
        print(f"MAVELY_CLIENT_ID={cid}")

    if rt:
        ok = _write_text(refresh_file, rt)
        print(f"wrote_refresh_file={ok} path={refresh_file} len={len(rt)}")

    if args.auth_file and access:
        ok = _write_text(args.auth_file, access)
        print(f"wrote_auth_file={ok} path={args.auth_file} len={len(access)}")
    if args.id_file and idt:
        ok = _write_text(args.id_file, idt)
        print(f"wrote_id_file={ok} path={args.id_file} len={len(idt)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

