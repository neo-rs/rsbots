"""
RSForwarder - Mavely Client (standalone)

This is copied in directly so RSForwarder does NOT depend on Instorebotforwarder
being present on the deployment host.
"""

import os
import time
import json
import logging
import base64
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict

import requests

DEFAULT_BASE_URL = "https://creators.joinmavely.com"
DEFAULT_GRAPHQL_PATH = "/api/graphql"
DEFAULT_SESSION_PATH = "/api/auth/session"
DEFAULT_SEC_CH_UA = '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"'
DEFAULT_SEC_CH_UA_MOBILE = "?0"
DEFAULT_SEC_CH_UA_PLATFORM = '"Windows"'
DEFAULT_PRIORITY = "u=1, i"
DEFAULT_AUTH_BASE = "https://auth.mave.ly"
DEFAULT_TOKEN_ENDPOINT = f"{DEFAULT_AUTH_BASE}/oauth/token"


def _env_str(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def _read_text_file(path: str) -> str:
    try:
        p = Path(path)
        if not p.exists():
            return ""
        return (p.read_text(encoding="utf-8", errors="replace") or "").strip()
    except Exception:
        return ""


def _write_text_file(path: str, content: str) -> bool:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(p)
        return True
    except Exception:
        return False


def _maybe_add(headers: Dict[str, str], name: str, value: str) -> None:
    v = (value or "").strip()
    if v:
        headers[name] = v


def _normalize_base_url(base_url: str) -> str:
    b = (base_url or "").strip()
    if not b:
        b = DEFAULT_BASE_URL
    return b.rstrip("/")


def _trpc_url(base_url: str) -> str:
    b = _normalize_base_url(base_url)
    return f"{b}/api/trpc/links.create?batch=1"


def _graphql_url(base_url: str, graphql_endpoint: Optional[str]) -> str:
    if graphql_endpoint:
        g = (graphql_endpoint or "").strip()
        if g:
            return g
    b = _normalize_base_url(base_url)
    return f"{b}{DEFAULT_GRAPHQL_PATH}"


def _session_url(base_url: str) -> str:
    b = _normalize_base_url(base_url)
    return f"{b}{DEFAULT_SESSION_PATH}"


def _clean_token(raw: str) -> str:
    return (raw or "").strip().replace('"', "").replace("'", "")


def _b64url_decode(data: str) -> bytes:
    s = (data or "").strip()
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _jwt_payload(token: str) -> Optional[dict]:
    """
    Decode JWT payload WITHOUT verifying signature (metadata only).
    Never use this to trust claims for security decisions.
    """
    t = (token or "").strip()
    parts = t.split(".")
    if len(parts) < 2:
        return None
    try:
        payload = _b64url_decode(parts[1]).decode("utf-8", errors="ignore")
        data = json.loads(payload)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _is_token_expired_error(errors: object) -> bool:
    try:
        if not isinstance(errors, list):
            return False
        for e in errors:
            if not isinstance(e, dict):
                continue
            msg = (e.get("message") or "")
            ext = e.get("extensions") if isinstance(e.get("extensions"), dict) else {}
            code = (ext.get("code") or "")
            if isinstance(code, str) and code.strip().upper() == "TOKEN_EXPIRED":
                return True
            if isinstance(msg, str) and "token expired" in msg.lower():
                return True
        return False
    except Exception:
        return False


def _is_brand_not_found_error(errors: object) -> bool:
    """
    Mavely GraphQL sometimes returns BAD_USER_INPUT like:
      "Brand not found for url: https://go.sylikes.com/..."
    This is effectively "merchant not supported" for that URL.
    """
    try:
        if not isinstance(errors, list):
            return False
        for e in errors:
            if not isinstance(e, dict):
                continue
            msg = e.get("message")
            if isinstance(msg, str) and "brand not found for url" in msg.lower():
                return True
        return False
    except Exception:
        return False


def _parse_json_best_effort(text: str) -> Optional[object]:
    """
    Parse JSON from a response body defensively.
    Handles common prefixes/BOM issues.
    """
    try:
        t = (text or "").strip()
        if not t:
            return None
        # UTF-8 BOM
        t = t.lstrip("\ufeff")
        # Common anti-CSRF prefix used by some frameworks
        if t.startswith(")]}',"):
            t = t.split("\n", 1)[1] if "\n" in t else t[5:]
            t = t.strip()
        return json.loads(t)
    except Exception:
        return None


def _is_empty_session_json(data: object) -> bool:
    # NextAuth returns `{}` when not logged in (still HTTP 200 JSON).
    return isinstance(data, dict) and (len(data) == 0)


def _looks_like_cookie_header(value: str) -> bool:
    # very small heuristic: "a=b; c=d"
    v = (value or "").strip()
    return ("=" in v) and (";" in v)


def _build_cookie_header(session_token_or_cookie: str) -> str:
    """
    Accepts either:
    - the raw next-auth session-token VALUE (recommended for env), OR
    - a full cookie header fragment like "a=b; c=d" (advanced use).
    """
    raw = (session_token_or_cookie or "").strip()
    if not raw:
        return ""

    if _looks_like_cookie_header(raw):
        # User provided a full cookie fragment; use as-is.
        return raw

    token = _clean_token(raw)
    # Some deployments look for one name vs the other; send both.
    return f"__Secure-next-auth.session-token={token}; next-auth.session-token={token}"


@dataclass
class MavelyResult:
    ok: bool
    status_code: int
    mavely_link: Optional[str] = None
    error: Optional[str] = None
    raw_snippet: Optional[str] = None


class SimpleRateLimiter:
    # Enforces a minimum delay between outbound requests.
    def __init__(self, min_seconds: float):
        self.min_seconds = float(min_seconds)
        self._last_ts = 0.0

    def wait(self):
        now = time.time()
        delta = now - self._last_ts
        if delta < self.min_seconds:
            time.sleep(self.min_seconds - delta)
        self._last_ts = time.time()


class MavelyClient:
    def __init__(
        self,
        session_token: str,
        base_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        graphql_endpoint: Optional[str] = None,
        timeout_s: int = 20,
        max_retries: int = 3,
        min_seconds_between_requests: float = 2.0,
        user_agent: Optional[str] = None,
    ):
        self.session_token = _clean_token(session_token)
        # Prefer cookie/session from env even if caller passes per-guild tokens.
        env_cookies = (os.environ.get("MAVELY_COOKIES", "") or "").strip()
        cookie_source = env_cookies if env_cookies else session_token
        self.cookie_header = _build_cookie_header(cookie_source)
        self.auth_token = _clean_token(auth_token or os.environ.get("MAVELY_AUTH_TOKEN", ""))
        # Some Mavely endpoints require the NextAuth "idToken" rather than the access token.
        self.id_token = _clean_token(os.environ.get("MAVELY_ID_TOKEN", ""))
        self.refresh_token = _clean_token(os.environ.get("MAVELY_REFRESH_TOKEN", ""))
        if not self.refresh_token:
            rt_file = _env_str("MAVELY_REFRESH_TOKEN_FILE")
            if rt_file:
                self.refresh_token = _clean_token(_read_text_file(rt_file))
        self.token_endpoint = _env_str("MAVELY_TOKEN_ENDPOINT") or DEFAULT_TOKEN_ENDPOINT
        self.auth_audience = _env_str("MAVELY_AUTH_AUDIENCE")  # optional
        self.auth_scope = _env_str("MAVELY_AUTH_SCOPE")        # optional
        self.client_id = _env_str("MAVELY_CLIENT_ID")
        self._last_refresh_error: Optional[str] = None
        # If the refresh token is rejected with invalid_grant, stop retrying it for the
        # lifetime of this process (prevents log spam + wasted requests).
        self._oauth_refresh_disabled = False
        self._oauth_refresh_disabled_reason: Optional[str] = None
        self.base_url = _normalize_base_url(base_url or os.environ.get("MAVELY_BASE_URL", DEFAULT_BASE_URL))
        self.graphql_endpoint = (graphql_endpoint or os.environ.get("MAVELY_GRAPHQL_ENDPOINT", "") or "").strip() or None
        self.timeout_s = int(timeout_s)
        self.max_retries = int(max_retries)
        self.rate = SimpleRateLimiter(min_seconds_between_requests)
        self.log = logging.getLogger("mavely")

        self.user_agent = (user_agent or _env_str("MAVELY_USER_AGENT")) or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        )
        self.sec_ch_ua = _env_str("MAVELY_SEC_CH_UA") or DEFAULT_SEC_CH_UA
        self.sec_ch_ua_mobile = _env_str("MAVELY_SEC_CH_UA_MOBILE") or DEFAULT_SEC_CH_UA_MOBILE
        self.sec_ch_ua_platform = _env_str("MAVELY_SEC_CH_UA_PLATFORM") or DEFAULT_SEC_CH_UA_PLATFORM
        self.sec_fetch_site = _env_str("MAVELY_SEC_FETCH_SITE", "same-origin")
        self.sec_fetch_mode = _env_str("MAVELY_SEC_FETCH_MODE", "cors")
        self.sec_fetch_dest = _env_str("MAVELY_SEC_FETCH_DEST", "empty")
        self.priority = _env_str("MAVELY_PRIORITY") or DEFAULT_PRIORITY  # e.g. "u=1, i"

        # Non-secret diagnostics (do NOT print cookies/tokens)
        self._cookie_len = len(self.cookie_header or "")
        self._cookie_has_cf = "cf_clearance=" in (self.cookie_header or "")
        self._cookie_has_nextauth = "next-auth" in (self.cookie_header or "")
        self._cookie_has_session = ("session-token=" in (self.cookie_header or "")) or ("session_token=" in (self.cookie_header or ""))
        self._refresh_len = len(self.refresh_token or "")
        self.log.debug(
            "Init: base_url=%s cookie_len=%s has_cf=%s has_nextauth=%s has_session_cookie=%s refresh_len=%s",
            self.base_url,
            self._cookie_len,
            self._cookie_has_cf,
            self._cookie_has_nextauth,
            self._cookie_has_session,
            self._refresh_len,
        )
        # If MAVELY_REFRESH_TOKEN_FILE is configured, seed it with the current token.
        # (Useful on first run, before any rotation occurs.)
        try:
            self._maybe_persist_refresh_token()
        except Exception:
            pass

    def _maybe_persist_refresh_token(self) -> None:
        rt_file = _env_str("MAVELY_REFRESH_TOKEN_FILE")
        if not rt_file:
            return
        if not self.refresh_token:
            return
        _write_text_file(rt_file, self.refresh_token)

    def _resolve_client_id(self) -> str:
        if self.client_id:
            return self.client_id
        for t in (self.auth_token, self.id_token):
            p = _jwt_payload(t)
            azp = (p or {}).get("azp")
            if isinstance(azp, str) and azp.strip():
                return azp.strip()
            aud = (p or {}).get("aud")
            if isinstance(aud, str) and aud.strip():
                # id_token often has aud=client_id
                return aud.strip()
        return ""

    def _auth_token_expiring_soon(self) -> bool:
        """
        Best-effort preemptive refresh:
        If auth_token is a JWT with exp, refresh when exp is within MAVELY_REFRESH_SKEW_S seconds.
        """
        try:
            skew = int((_env_str("MAVELY_REFRESH_SKEW_S") or "").strip() or "300")
        except Exception:
            skew = 300
        skew = max(30, min(skew, 24 * 3600))

        p = _jwt_payload(self.auth_token or "")
        exp = (p or {}).get("exp") if isinstance(p, dict) else None
        if not isinstance(exp, int):
            try:
                exp = int(exp)
            except Exception:
                return False
        now = int(time.time())
        return now >= (int(exp) - int(skew))

    def _refresh_access_token(self, sess: requests.Session) -> Optional[str]:
        """
        Try to mint a new access token using refresh_token grant (Auth0-style).
        Requires MAVELY_REFRESH_TOKEN.
        """
        if self._oauth_refresh_disabled:
            self._last_refresh_error = self._oauth_refresh_disabled_reason or "OAuth refresh disabled"
            return None

        if not self.refresh_token:
            self.log.debug("Refresh skipped: no MAVELY_REFRESH_TOKEN configured")
            self._last_refresh_error = "no MAVELY_REFRESH_TOKEN configured"
            return None

        client_id = self._resolve_client_id()
        if not client_id:
            self.log.debug("Refresh skipped: missing client_id (set MAVELY_CLIENT_ID)")
            self._last_refresh_error = "missing client_id (set MAVELY_CLIENT_ID)"
            return None

        payload: dict = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": self.refresh_token,
        }
        if self.auth_audience:
            payload["audience"] = self.auth_audience
        if self.auth_scope:
            payload["scope"] = self.auth_scope

        self.log.info(
            "Refreshing bearer token via %s (client_id_present=%s audience_set=%s scope_set=%s)",
            self.token_endpoint,
            bool(client_id),
            bool(self.auth_audience),
            bool(self.auth_scope),
        )
        def _do_post() -> requests.Response:
            return sess.post(
                self.token_endpoint,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.timeout_s,
            )

        try:
            resp = _do_post()
        except requests.RequestException as e:
            self._last_refresh_error = str(e)
            return None

        self.log.debug("Refresh response: status=%s ct=%s", resp.status_code, (resp.headers.get("content-type") or ""))
        if resp.status_code != 200:
            err_code = ""
            err_desc = ""
            try:
                j = resp.json()
                err_code = str((j or {}).get("error") or "")
                err_desc = str((j or {}).get("error_description") or "")
            except Exception:
                pass
            # If the refresh token rotated (common) and cookies are still valid,
            # pull a fresh refreshToken from /api/auth/session and retry once.
            if (err_code or "").strip().lower() == "invalid_grant" and self.cookie_header:
                prev = self.refresh_token
                try:
                    self._ensure_auth_token_from_session(sess, force=True)
                except Exception:
                    pass
                if self.refresh_token and self.refresh_token != prev:
                    payload["refresh_token"] = self.refresh_token
                    try:
                        resp = _do_post()
                        if resp.status_code == 200:
                            try:
                                data = resp.json()
                            except Exception:
                                self._last_refresh_error = "failed to parse token endpoint JSON"
                                return None
                            rt2 = (data or {}).get("refresh_token")
                            if isinstance(rt2, str) and rt2.strip():
                                new_rt2 = _clean_token(rt2)
                                if new_rt2 and new_rt2 != self.refresh_token:
                                    self.refresh_token = new_rt2
                                    self._refresh_len = len(self.refresh_token or "")
                                    self.log.debug("Refresh response included new refresh_token (len=%s)", self._refresh_len)
                                    self._maybe_persist_refresh_token()
                            access_token = (data or {}).get("access_token")
                            if isinstance(access_token, str) and access_token.strip():
                                return _clean_token(access_token)
                            self._last_refresh_error = "token endpoint JSON missing access_token"
                            return None
                    except Exception:
                        pass
            extra = ""
            if err_code or err_desc:
                extra = f" ({err_code}{': ' if (err_code and err_desc) else ''}{err_desc})"
            self._last_refresh_error = f"token endpoint returned {resp.status_code}{extra}"
            # If refresh_token is definitively invalid, stop trying it repeatedly.
            if (err_code or "").strip().lower() == "invalid_grant":
                self._oauth_refresh_disabled = True
                self._oauth_refresh_disabled_reason = f"OAuth refresh disabled: {self._last_refresh_error}"
            return None
        try:
            data = resp.json()
        except Exception:
            self._last_refresh_error = "failed to parse token endpoint JSON"
            return None

        rt = (data or {}).get("refresh_token")
        if isinstance(rt, str) and rt.strip():
            new_rt = _clean_token(rt)
            if new_rt and new_rt != self.refresh_token:
                self.refresh_token = new_rt
                self._refresh_len = len(self.refresh_token or "")
                self.log.debug("Refresh response included new refresh_token (len=%s)", self._refresh_len)
                self._maybe_persist_refresh_token()

        access_token = (data or {}).get("access_token")
        if isinstance(access_token, str) and access_token.strip():
            return _clean_token(access_token)
        self._last_refresh_error = "token endpoint JSON missing access_token"
        return None

    def preflight(self) -> MavelyResult:
        """
        Lightweight startup check:
        - If refresh token is configured, try to mint a new access token.
        - If cookies are configured, call /api/auth/session once to see if they're accepted.
        Never returns sensitive response bodies.
        """
        sess = requests.Session()

        enable_oauth_refresh = (_env_str("MAVELY_ENABLE_OAUTH_REFRESH") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        if enable_oauth_refresh and self.refresh_token:
            new_token = self._refresh_access_token(sess)
            if new_token:
                self.auth_token = new_token
                self.log.debug("Preflight: refreshed access token ok (len=%s)", len(self.auth_token))

        if not self.cookie_header:
            return MavelyResult(ok=bool(self.auth_token), status_code=0, error=None if self.auth_token else "No cookie header and no auth token")

        url = _session_url(self.base_url)
        self.log.debug("Preflight: Session GET %s", url)
        try:
            resp = sess.get(url, headers=self._session_headers(), timeout=self.timeout_s)
        except requests.RequestException as e:
            return MavelyResult(ok=False, status_code=0, error=str(e))

        ct = (resp.headers.get("content-type") or "").lower()
        self.log.debug("Preflight: Session response status=%s ct=%s", resp.status_code, ct)
        ok = (resp.status_code == 200) and ("application/json" in ct)

        data: object = None
        if ok:
            try:
                data = resp.json()
            except Exception:
                data = _parse_json_best_effort(resp.text or "")
            if _is_empty_session_json(data):
                return MavelyResult(
                    ok=False,
                    status_code=200,
                    error="Session is empty (not logged in). Refresh MAVELY_COOKIES from creators.joinmavely.com.",
                )

        if ok and not self.auth_token:
            try:
                if data is None:
                    data = resp.json()
                if isinstance(data, dict):
                    token = (
                        (data.get("token") or "")
                        or (data.get("accessToken") or "")
                        or ((data.get("user") or {}).get("token") if isinstance(data.get("user"), dict) else "")
                        or ((data.get("user") or {}).get("accessToken") if isinstance(data.get("user"), dict) else "")
                    )
                    if isinstance(token, str) and token.strip():
                        self.auth_token = _clean_token(token)
                    self.log.debug("Preflight: derived bearer token from session JSON (len=%s)", len(self.auth_token))
            except Exception:
                pass

        return MavelyResult(ok=ok, status_code=resp.status_code, error=None if ok else f"Session status {resp.status_code}")

    def _session_headers(self) -> Dict[str, str]:
        origin = self.base_url
        referer = f"{self.base_url}/home"
        headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
            "Referer": referer,
            "Origin": origin,
            "Accept-Language": "en-US,en;q=0.9",
        }
        _maybe_add(headers, "sec-ch-ua", self.sec_ch_ua)
        _maybe_add(headers, "sec-ch-ua-mobile", self.sec_ch_ua_mobile)
        _maybe_add(headers, "sec-ch-ua-platform", self.sec_ch_ua_platform)
        _maybe_add(headers, "sec-fetch-site", self.sec_fetch_site)
        _maybe_add(headers, "sec-fetch-mode", self.sec_fetch_mode)
        _maybe_add(headers, "sec-fetch-dest", self.sec_fetch_dest)
        _maybe_add(headers, "priority", self.priority)
        if self.cookie_header:
            headers["Cookie"] = self.cookie_header
        return headers

    def _ensure_auth_token_from_session(self, sess: requests.Session, *, force: bool = False) -> bool:
        if (not force) and self.auth_token:
            return False
        if not self.cookie_header:
            return False

        url = _session_url(self.base_url)
        if force:
            self.log.info("Mavely: refreshing bearer from session cookies")
        self.log.debug("Session GET %s (attempt bearer derive%s)", url, " forced" if force else "")
        try:
            resp = sess.get(url, headers=self._session_headers(), timeout=self.timeout_s)
        except requests.RequestException as e:
            self.log.debug("Session GET failed: %s", e)
            return False

        ct = (resp.headers.get("content-type") or "").lower()
        self.log.debug("Session response: status=%s ct=%s", resp.status_code, ct)
        if resp.status_code != 200:
            return False
        if "application/json" not in ct:
            return False
        try:
            data = resp.json()
        except Exception:
            data = _parse_json_best_effort(resp.text or "")
            if not isinstance(data, dict):
                return False

        if _is_empty_session_json(data):
            if force:
                self.log.info("Mavely: session endpoint returned empty JSON. Your MAVELY_COOKIES are not logged in anymore.")
            return False

        # Keep idToken + refreshToken in sync with current session cookies.
        # This makes token rotation mostly hands-off as long as cookies remain valid.
        try:
            id_token = (
                (data or {}).get("idToken")
                or ((data.get("user") or {}).get("idToken") if isinstance(data.get("user"), dict) else None)
            )
            refresh_token = (
                (data or {}).get("refreshToken")
                or ((data.get("user") or {}).get("refreshToken") if isinstance(data.get("user"), dict) else None)
            )
            if isinstance(id_token, str) and id_token.strip():
                new_id = _clean_token(id_token)
                if new_id and new_id != self.id_token:
                    self.id_token = new_id
                    self.log.debug("Updated idToken from session JSON (len=%s)", len(self.id_token))
            if isinstance(refresh_token, str) and refresh_token.strip():
                new_refresh = _clean_token(refresh_token)
                if new_refresh and new_refresh != self.refresh_token:
                    self.refresh_token = new_refresh
                    self._refresh_len = len(self.refresh_token or "")
                    self.log.debug("Updated refreshToken from session JSON (len=%s)", self._refresh_len)
                    self._maybe_persist_refresh_token()
        except Exception:
            pass

        token = (
            (data or {}).get("token")
            or (data or {}).get("accessToken")
            or ((data.get("user") or {}).get("token") if isinstance(data.get("user"), dict) else None)
            or ((data.get("user") or {}).get("accessToken") if isinstance(data.get("user"), dict) else None)
        )
        if isinstance(token, str) and token.strip():
            self.auth_token = _clean_token(token)
            self.log.debug("Derived bearer token from session JSON (len=%s)", len(self.auth_token))
            return True

        try:
            keys = list((data or {}).keys()) if isinstance(data, dict) else [type(data).__name__]
        except Exception:
            keys = ["(unknown)"]
        self.log.debug("Session JSON did not include token. Keys=%s body_len=%s", keys, len(resp.text or ""))
        if force:
            self.log.info("Mavely: session refresh did not return a token (keys=%s)", keys)
        return False

    def _headers(self) -> Dict[str, str]:
        origin = self.base_url
        referer = f"{self.base_url}/tools"
        return {
            "Content-Type": "application/json",
            "Cookie": self.cookie_header,
            "x-trpc-source": "react",
            "User-Agent": self.user_agent,
            "Referer": referer,
            "Origin": origin,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }

    def _graphql_headers(self) -> Dict[str, str]:
        origin = self.base_url
        referer = f"{self.base_url}/tools"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
            "Referer": referer,
            "Origin": origin,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        _maybe_add(headers, "sec-ch-ua", self.sec_ch_ua)
        _maybe_add(headers, "sec-ch-ua-mobile", self.sec_ch_ua_mobile)
        _maybe_add(headers, "sec-ch-ua-platform", self.sec_ch_ua_platform)
        _maybe_add(headers, "sec-fetch-site", self.sec_fetch_site)
        _maybe_add(headers, "sec-fetch-mode", self.sec_fetch_mode)
        _maybe_add(headers, "sec-fetch-dest", self.sec_fetch_dest)
        _maybe_add(headers, "priority", self.priority)
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        if self.cookie_header:
            headers["Cookie"] = self.cookie_header
        return headers

    def _graphql_headers_with_bearer(self, bearer: str) -> Dict[str, str]:
        h = self._graphql_headers()
        b = _clean_token(bearer)
        if b:
            h["Authorization"] = f"Bearer {b}"
        return h

    def _graphql_headers_cookie_only(self) -> Dict[str, str]:
        origin = self.base_url
        referer = f"{self.base_url}/tools"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": self.user_agent,
            "Referer": referer,
            "Origin": origin,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        _maybe_add(headers, "sec-ch-ua", self.sec_ch_ua)
        _maybe_add(headers, "sec-ch-ua-mobile", self.sec_ch_ua_mobile)
        _maybe_add(headers, "sec-ch-ua-platform", self.sec_ch_ua_platform)
        _maybe_add(headers, "sec-fetch-site", self.sec_fetch_site)
        _maybe_add(headers, "sec-fetch-mode", self.sec_fetch_mode)
        _maybe_add(headers, "sec-fetch-dest", self.sec_fetch_dest)
        _maybe_add(headers, "priority", self.priority)
        if self.cookie_header:
            headers["Cookie"] = self.cookie_header
        return headers

    def _try_graphql_create_link(self, sess: requests.Session, url: str) -> Optional[MavelyResult]:
        if not self.auth_token and not self.cookie_header:
            return None

        self._ensure_auth_token_from_session(sess)
        # Preemptive refresh: if we have a bearer token that is near expiry, refresh before making the GraphQL call.
        enable_oauth_refresh = (_env_str("MAVELY_ENABLE_OAUTH_REFRESH") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        if enable_oauth_refresh and self.auth_token and self._auth_token_expiring_soon():
            refreshed = self._refresh_access_token(sess)
            if refreshed:
                self.auth_token = refreshed

        gql_url = _graphql_url(self.base_url, self.graphql_endpoint)
        self.log.debug("GraphQL POST %s (has_auth=%s has_cookie=%s)", gql_url, bool(self.auth_token), bool(self.cookie_header))
        query = """
mutation CreateAffiliateLink($url: String!) {
  createAffiliateLink(url: $url) {
    id
    link
    attributionUrl
    canonicalLink
    originalUrl
    metaTitle
    metaDescription
    metaImage
    metaUrl
    metaLogo
    metaSiteName
    metaVideo
    brand { id name slug }
  }
}
""".strip()
        payload = {"query": query, "variables": {"url": url}}

        def _do_post(headers: Dict[str, str]) -> requests.Response:
            return sess.post(gql_url, headers=headers, data=json.dumps(payload), timeout=self.timeout_s)

        try:
            resp = _do_post(self._graphql_headers())
        except requests.RequestException as e:
            return MavelyResult(ok=False, status_code=0, error=str(e))

        ct = (resp.headers.get("content-type") or "").lower()
        snippet = (resp.text or "")[:500]
        self.log.debug("GraphQL response: status=%s ct=%s", resp.status_code, ct)
        if "text/html" in ct:
            return MavelyResult(ok=False, status_code=resp.status_code, error="Blocked or not authenticated (HTML response from GraphQL).", raw_snippet=snippet)

        if resp.status_code != 200:
            last_err: Optional[MavelyResult] = None
            if resp.status_code == 401 and self.id_token and self.id_token != self.auth_token:
                self.log.debug("GraphQL 401 with MAVELY_AUTH_TOKEN; retrying with MAVELY_ID_TOKEN")
                try:
                    resp2 = _do_post(self._graphql_headers_with_bearer(self.id_token))
                    snippet2 = (resp2.text or "")[:500]
                    if resp2.status_code == 200:
                        try:
                            data2 = resp2.json()
                        except Exception:
                            data2 = _parse_json_best_effort(resp2.text or "")
                        if not isinstance(data2, dict):
                            return MavelyResult(ok=False, status_code=200, error="Failed to parse GraphQL JSON response", raw_snippet=snippet2)
                        if isinstance(data2, dict) and data2.get("errors"):
                            return MavelyResult(ok=False, status_code=200, error=f"GraphQL errors: {str(data2.get('errors'))[:300]}", raw_snippet=snippet2)
                        link_obj2 = (data2 or {}).get("data", {}).get("createAffiliateLink", {})
                        link2 = (link_obj2 or {}).get("link") or (link_obj2 or {}).get("attributionUrl")
                        if link2:
                            return MavelyResult(ok=True, status_code=200, mavely_link=link2)
                        return MavelyResult(ok=False, status_code=200, error="GraphQL response missing link", raw_snippet=snippet2)
                    last_err = MavelyResult(ok=False, status_code=resp2.status_code, error=f"GraphQL Status {resp2.status_code}", raw_snippet=snippet2)
                except requests.RequestException as e:
                    last_err = MavelyResult(ok=False, status_code=0, error=str(e))

            if resp.status_code == 401 or (last_err and last_err.status_code == 401):
                enable_oauth_refresh = (_env_str("MAVELY_ENABLE_OAUTH_REFRESH") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
                if enable_oauth_refresh:
                    refreshed = self._refresh_access_token(sess)
                    if refreshed:
                        self.log.debug("GraphQL 401; retrying with refreshed access token")
                        try:
                            resp3 = _do_post(self._graphql_headers_with_bearer(refreshed))
                            snippet3 = (resp3.text or "")[:500]
                            if resp3.status_code == 200:
                                try:
                                    data3 = resp3.json()
                                except Exception:
                                    data3 = _parse_json_best_effort(resp3.text or "")
                                if not isinstance(data3, dict):
                                    return MavelyResult(ok=False, status_code=200, error="Failed to parse GraphQL JSON response", raw_snippet=snippet3)
                                if isinstance(data3, dict) and data3.get("errors"):
                                    return MavelyResult(ok=False, status_code=200, error=f"GraphQL errors: {str(data3.get('errors'))[:300]}", raw_snippet=snippet3)
                                link_obj3 = (data3 or {}).get("data", {}).get("createAffiliateLink", {})
                                link3 = (link_obj3 or {}).get("link") or (link_obj3 or {}).get("attributionUrl")
                                if link3:
                                    self.auth_token = refreshed
                                    return MavelyResult(ok=True, status_code=200, mavely_link=link3)
                                return MavelyResult(ok=False, status_code=200, error="GraphQL response missing link", raw_snippet=snippet3)
                            return MavelyResult(ok=False, status_code=resp3.status_code, error=f"GraphQL Status {resp3.status_code}", raw_snippet=snippet3)
                        except requests.RequestException as e:
                            return MavelyResult(ok=False, status_code=0, error=str(e))

            return last_err or MavelyResult(ok=False, status_code=resp.status_code, error=f"GraphQL Status {resp.status_code}", raw_snippet=snippet)

        try:
            data = resp.json()
        except Exception:
            data = _parse_json_best_effort(resp.text or "")
        if not isinstance(data, dict):
            return MavelyResult(ok=False, status_code=200, error="Failed to parse GraphQL JSON response", raw_snippet=snippet)

        payload2 = data.get("response") if isinstance(data, dict) and isinstance(data.get("response"), dict) else data
        errors = (payload2 or {}).get("errors") if isinstance(payload2, dict) else None
        if isinstance(payload2, dict) and errors:
            if _is_brand_not_found_error(errors):
                # Provide a clear, non-robotic message.
                try:
                    from urllib.parse import urlparse

                    host = (urlparse(url).netloc or "").lower()
                except Exception:
                    host = ""
                extra = f" ({host})" if host else ""
                return MavelyResult(ok=False, status_code=200, error=f"Merchant not supported by Mavely for this URL{extra}.", raw_snippet=snippet)
            if _is_token_expired_error(errors):
                try:
                    resp_cookie = _do_post(self._graphql_headers_cookie_only())
                    if resp_cookie.status_code == 200:
                        try:
                            data_cookie = resp_cookie.json()
                        except Exception:
                            data_cookie = _parse_json_best_effort(resp_cookie.text or "")
                        if isinstance(data_cookie, dict):
                            payload_cookie = data_cookie.get("response") if isinstance(data_cookie.get("response"), dict) else data_cookie
                            errors_cookie = (payload_cookie or {}).get("errors") if isinstance(payload_cookie, dict) else None
                            if isinstance(payload_cookie, dict) and not errors_cookie:
                                link_obj_cookie = (payload_cookie or {}).get("data", {}).get("createAffiliateLink", {})
                                link_cookie = (link_obj_cookie or {}).get("link") or (link_obj_cookie or {}).get("attributionUrl")
                                if link_cookie:
                                    return MavelyResult(ok=True, status_code=200, mavely_link=link_cookie)
                except Exception:
                    pass

                if self.id_token and self.id_token != self.auth_token:
                    self.log.debug("GraphQL token expired; retrying with MAVELY_ID_TOKEN")
                    try:
                        resp_id = _do_post(self._graphql_headers_with_bearer(self.id_token))
                        snippet_id = (resp_id.text or "")[:500]
                        if resp_id.status_code == 200:
                            try:
                                data_id = resp_id.json()
                            except Exception:
                                data_id = _parse_json_best_effort(resp_id.text or "")
                            if isinstance(data_id, dict):
                                payload_id = data_id.get("response") if isinstance(data_id.get("response"), dict) else data_id
                                errors_id = (payload_id or {}).get("errors") if isinstance(payload_id, dict) else None
                                if not (isinstance(payload_id, dict) and errors_id):
                                    link_obj_id = (payload_id or {}).get("data", {}).get("createAffiliateLink", {}) if isinstance(payload_id, dict) else {}
                                    link_id = (link_obj_id or {}).get("link") or (link_obj_id or {}).get("attributionUrl")
                                    if link_id:
                                        self.auth_token = self.id_token
                                        return MavelyResult(ok=True, status_code=200, mavely_link=link_id)
                    except requests.RequestException:
                        pass

                if self._ensure_auth_token_from_session(sess, force=True):
                    try:
                        resp4 = _do_post(self._graphql_headers())
                        snippet4 = (resp4.text or "")[:500]
                        if resp4.status_code == 200:
                            try:
                                data4 = resp4.json()
                            except Exception:
                                data4 = _parse_json_best_effort(resp4.text or "")
                            if not isinstance(data4, dict):
                                return MavelyResult(ok=False, status_code=200, error="Failed to parse GraphQL JSON response", raw_snippet=snippet4)
                            payload4 = data4.get("response") if isinstance(data4, dict) and isinstance(data4.get("response"), dict) else data4
                            errors4 = (payload4 or {}).get("errors") if isinstance(payload4, dict) else None
                            if isinstance(payload4, dict) and errors4:
                                return MavelyResult(ok=False, status_code=200, error=f"GraphQL errors: {str(errors4)[:300]}", raw_snippet=snippet4)
                            link_obj4 = (payload4 or {}).get("data", {}).get("createAffiliateLink", {}) if isinstance(payload4, dict) else {}
                            link4 = (link_obj4 or {}).get("link") or (link_obj4 or {}).get("attributionUrl")
                            if link4:
                                return MavelyResult(ok=True, status_code=200, mavely_link=link4)
                            return MavelyResult(ok=False, status_code=200, error="GraphQL response missing link", raw_snippet=snippet4)
                        return MavelyResult(ok=False, status_code=resp4.status_code, error=f"GraphQL Status {resp4.status_code}", raw_snippet=snippet4)
                    except requests.RequestException as e:
                        return MavelyResult(ok=False, status_code=0, error=str(e))

                enable_oauth_refresh = (_env_str("MAVELY_ENABLE_OAUTH_REFRESH") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
                if enable_oauth_refresh:
                    refreshed = self._refresh_access_token(sess)
                    if refreshed:
                        try:
                            resp5 = _do_post(self._graphql_headers_with_bearer(refreshed))
                            snippet5 = (resp5.text or "")[:500]
                            if resp5.status_code == 200:
                                try:
                                    data5 = resp5.json()
                                except Exception:
                                    data5 = _parse_json_best_effort(resp5.text or "")
                                if not isinstance(data5, dict):
                                    return MavelyResult(ok=False, status_code=200, error="Failed to parse GraphQL JSON response", raw_snippet=snippet5)
                                payload5 = data5.get("response") if isinstance(data5, dict) and isinstance(data5.get("response"), dict) else data5
                                errors5 = (payload5 or {}).get("errors") if isinstance(payload5, dict) else None
                                if isinstance(payload5, dict) and errors5:
                                    return MavelyResult(ok=False, status_code=200, error=f"GraphQL errors: {str(errors5)[:300]}", raw_snippet=snippet5)
                                link_obj5 = (payload5 or {}).get("data", {}).get("createAffiliateLink", {}) if isinstance(payload5, dict) else {}
                                link5 = (link_obj5 or {}).get("link") or (link_obj5 or {}).get("attributionUrl")
                                if link5:
                                    self.auth_token = refreshed
                                    return MavelyResult(ok=True, status_code=200, mavely_link=link5)
                                return MavelyResult(ok=False, status_code=200, error="GraphQL response missing link", raw_snippet=snippet5)
                            return MavelyResult(ok=False, status_code=resp5.status_code, error=f"GraphQL Status {resp5.status_code}", raw_snippet=snippet5)
                        except requests.RequestException as e:
                            return MavelyResult(ok=False, status_code=0, error=str(e))

                reason = "token expired; could not refresh from session/idToken"
                if enable_oauth_refresh:
                    reason = self._last_refresh_error or ("no MAVELY_REFRESH_TOKEN configured" if not self.refresh_token else "refresh failed")
                return MavelyResult(ok=False, status_code=200, error=f"Mavely token expired. {reason}.", raw_snippet=snippet)

            return MavelyResult(ok=False, status_code=200, error=f"GraphQL errors: {str(errors)[:300]}", raw_snippet=snippet)

        try:
            link_obj = (payload2 or {}).get("data", {}).get("createAffiliateLink", {}) if isinstance(payload2, dict) else {}
            link = (link_obj or {}).get("link") or (link_obj or {}).get("attributionUrl")
            if link:
                return MavelyResult(ok=True, status_code=200, mavely_link=link)
            return MavelyResult(ok=False, status_code=200, error="GraphQL response missing link", raw_snippet=snippet)
        except Exception as e:
            return MavelyResult(ok=False, status_code=200, error=f"Unexpected GraphQL response: {e}", raw_snippet=snippet)

    def create_link(self, url: str) -> MavelyResult:
        # Allow "refresh-token-only" mode: if no cookies/bearer are present but OAuth refresh is enabled,
        # try minting a bearer token up-front.
        enable_oauth_refresh = (_env_str("MAVELY_ENABLE_OAUTH_REFRESH") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        if enable_oauth_refresh and (not self.auth_token) and self.refresh_token:
            try:
                minted = self._refresh_access_token(requests.Session())
                if minted:
                    self.auth_token = minted
            except Exception:
                pass

        if not self.cookie_header and not self.auth_token:
            return MavelyResult(ok=False, status_code=0, error="Missing MAVELY cookie/session token (or MAVELY_AUTH_TOKEN / OAuth refresh token)")

        if not url:
            return MavelyResult(ok=False, status_code=0, error="Missing URL")

        payload = {"0": {"json": {"url": url}}}
        trpc_url = _trpc_url(self.base_url)
        sess = requests.Session()

        for attempt in range(1, self.max_retries + 1):
            self.rate.wait()
            try:
                self.log.debug(
                    "Create link: base_url=%s graphql=%s has_cookie=%s has_auth=%s",
                    self.base_url,
                    _graphql_url(self.base_url, self.graphql_endpoint),
                    bool(self.cookie_header),
                    bool(self.auth_token),
                )
                gql_res = self._try_graphql_create_link(sess, url)
                if gql_res and gql_res.ok and gql_res.mavely_link:
                    return gql_res
                if gql_res and gql_res.status_code not in (0, 404):
                    return gql_res

                if "joinmavely.com" in (self.base_url or ""):
                    return MavelyResult(
                        ok=False,
                        status_code=405,
                        error=(
                            "GraphQL did not succeed, and legacy tRPC endpoint is not supported on joinmavely.com "
                            "(observed 405 HTML). Provide MAVELY_AUTH_TOKEN or refresh cookies / cf_clearance."
                        ),
                    )

                self.log.debug("tRPC POST %s (attempt %s/%s)", trpc_url, attempt, self.max_retries)
                resp = sess.post(trpc_url, headers=self._headers(), data=json.dumps(payload), timeout=self.timeout_s)

                ct = (resp.headers.get("content-type") or "").lower()
                snippet = (resp.text or "")[:500]
                self.log.debug("tRPC response: status=%s ct=%s", resp.status_code, ct)

                if "text/html" in ct:
                    return MavelyResult(
                        ok=False,
                        status_code=resp.status_code,
                        error=(
                            "Blocked or not authenticated (HTML response). "
                            "Common causes: expired cookie, wrong domain, or bot/challenge/rate-limit. "
                            f"Tip: ensure your cookie came from `{self.base_url}` (or set `MAVELY_BASE_URL`)."
                        ),
                        raw_snippet=snippet,
                    )

                if resp.status_code != 200:
                    if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                        self.log.warning("Transient error %s on attempt %s/%s", resp.status_code, attempt, self.max_retries)
                        time.sleep(min(2 ** attempt, 10))
                        continue
                    return MavelyResult(ok=False, status_code=resp.status_code, error=f"Mavely Status {resp.status_code}", raw_snippet=snippet)

                try:
                    data = resp.json()
                except Exception:
                    return MavelyResult(ok=False, status_code=200, error="Failed to parse JSON response", raw_snippet=snippet)

                try:
                    link = data[0]["result"]["data"]["json"]["link"]
                    if not link:
                        raise KeyError("Empty link")
                    return MavelyResult(ok=True, status_code=200, mavely_link=link)
                except Exception as e:
                    return MavelyResult(ok=False, status_code=200, error=f"Unexpected response shape: {e}", raw_snippet=snippet)

            except requests.RequestException as e:
                if attempt < self.max_retries:
                    self.log.warning("Network error on attempt %s/%s: %s", attempt, self.max_retries, e)
                    time.sleep(min(2 ** attempt, 10))
                    continue
                return MavelyResult(ok=False, status_code=0, error=str(e))

        return MavelyResult(ok=False, status_code=0, error="Unknown error")

