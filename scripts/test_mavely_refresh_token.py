"""
Test whether MAVELY_REFRESH_TOKEN can mint a new access token (Windows-friendly).

This uses the existing refresh logic in RSForwarder/mavely_client.py so the test
matches production behavior.

Safe output:
- Never prints cookies/tokens.
- Prints only presence + lengths + endpoint + high-level errors.

Usage:
  .\.venv\Scripts\python.exe scripts\test_mavely_refresh_token.py --env-file Instorebotforwarder\\api-token.env
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Tuple

import requests

# Ensure repo root is importable when running from scripts/.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from RSForwarder.mavely_client import MavelyClient


def _parse_env_file(path: Path) -> Dict[str, str]:
    """
    Very small .env parser:
    - KEY=VALUE lines
    - ignores blank lines and # comments
    - strips surrounding single/double quotes
    """
    out: Dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(str(path))
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = (k or "").strip()
        v = (v or "").strip()
        if not k:
            continue
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        out[k] = v
    return out


def _load_env_file(path: Path, *, override: bool) -> Tuple[int, int]:
    data = _parse_env_file(path)
    loaded = 0
    skipped = 0
    for k, v in data.items():
        if not override and (os.environ.get(k) is not None) and os.environ.get(k) != "":
            skipped += 1
            continue
        os.environ[k] = v
        loaded += 1
    return loaded, skipped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default="", help="Optional path to an env file containing MAVELY_* keys.")
    ap.add_argument("--override", action="store_true", help="Override existing environment variables.")
    ap.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds (default: 20).")
    args = ap.parse_args()

    if args.env_file:
        p = Path(args.env_file)
        loaded, skipped = _load_env_file(p, override=bool(args.override))
        print(f"Loaded env file: {p} (loaded={loaded} skipped={skipped})")

    refresh = (os.environ.get("MAVELY_REFRESH_TOKEN", "") or "").strip()
    client_id_env = (os.environ.get("MAVELY_CLIENT_ID", "") or "").strip()

    print("Mavely refresh token check:")
    print(f"- MAVELY_REFRESH_TOKEN present: {bool(refresh)} (len={len(refresh)})")
    print(f"- MAVELY_CLIENT_ID present: {bool(client_id_env)}")

    # Instantiate client; session_token is only needed for cookie flows, but required by constructor.
    cookie_or_empty = (os.environ.get("MAVELY_COOKIES", "") or "").strip()
    if not cookie_or_empty:
        cookie_file = (os.environ.get("MAVELY_COOKIES_FILE", "") or "").strip()
        if cookie_file:
            try:
                cookie_or_empty = (Path(cookie_file).read_text(encoding="utf-8", errors="replace") or "").strip()
                os.environ["MAVELY_COOKIES"] = cookie_or_empty
                print(f"- Loaded MAVELY_COOKIES from file: {cookie_file} (len={len(cookie_or_empty)})")
            except Exception:
                print(f"- Failed to read MAVELY_COOKIES_FILE: {cookie_file}")
    c = MavelyClient(session_token=cookie_or_empty, timeout_s=int(args.timeout))
    print(f"- Using token endpoint: {c.token_endpoint}")

    # If client_id isn't configured, try to derive a bearer token from cookies first,
    # then the client can infer client_id from JWT payload (aud/azp).
    sess = requests.Session()
    if not client_id_env and cookie_or_empty:
        derived = False
        try:
            derived = bool(c._ensure_auth_token_from_session(sess, force=True))
        except Exception:
            derived = False
        print(f"- Derived bearer from cookies: {derived} (bearer_present={bool(c.auth_token)} len={len(c.auth_token or '')})")

    # Run refresh directly (does not require cookies, but may need client_id).
    new_token = c._refresh_access_token(sess)  # uses MAVELY_REFRESH_TOKEN + client_id resolution logic
    if not new_token:
        err = getattr(c, "_last_refresh_error", None) or "(unknown error)"
        print(f"RESULT: FAIL (refresh did not return an access token). Reason: {err}")
        return 2

    print(f"RESULT: OK (access token minted, len={len(new_token)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

