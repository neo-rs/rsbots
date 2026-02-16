#!/usr/bin/env python3
"""
Whop API Client
Handles all direct API calls to Whop Developer API

Canonical Owner: This module owns all Whop API interactions.
"""

import aiohttp
import asyncio
import logging
import json
import os
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, List
from aiohttp import ContentTypeError

log = logging.getLogger("rs-checker")

# Shared lock for payment_cache.json read/write (prevents lost updates within-process).
_PAYMENT_CACHE_LOCK: asyncio.Lock = asyncio.Lock()


class WhopAPIError(Exception):
    """Custom exception for Whop API errors"""
    pass


class WhopAPIClient:
    """Client for Whop Developer API (Company API)"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.whop.com/api/v1", company_id: Optional[str] = None):
        """
        Initialize Whop API client.
        
        Args:
            api_key: Whop Company API key (from dashboard)
            base_url: Base URL for Whop API (from config, defaults to v1)
            company_id: Whop Company ID (biz_...) required for Company API list endpoints
        """
        if not api_key:
            raise ValueError("Whop API key is required")
        if not company_id or not str(company_id).strip():
            raise ValueError("Whop company_id is required (set whop_api.company_id in config.json; format: biz_...)")
        
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.company_id = str(company_id).strip()
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def _parse_dt_any(self, ts_str: object) -> Optional[datetime]:
        """Parse ISO/unix-ish timestamps into UTC datetime (best-effort)."""
        if ts_str is None or ts_str == "":
            return None
        try:
            if isinstance(ts_str, (int, float)):
                val = float(ts_str)
                if abs(val) > 1.0e11:
                    val = val / 1000.0
                return datetime.fromtimestamp(val, tz=timezone.utc)
            s = str(ts_str).strip()
            if not s:
                return None
            if "T" in s or "-" in s:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            val = float(s)
            if abs(val) > 1.0e11:
                val = val / 1000.0
            return datetime.fromtimestamp(val, tz=timezone.utc)
        except Exception:
            return None

    def access_end_dt_from_membership(self, membership: Optional[Dict]) -> Optional[datetime]:
        """Primary entitlement end timestamp for a membership (free days included)."""
        if not isinstance(membership, dict):
            return None
        return self._parse_dt_any(
            membership.get("renewal_period_end")
            or membership.get("expires_at")
            or membership.get("trial_end")
            or membership.get("trial_ends_at")
            or membership.get("trial_end_at")
        )

    async def get_last_successful_payment_time(
        self,
        membership_id: str,
        *,
        cache_path: str | None = None,
        cache_ttl_hours: float = 24.0,
    ) -> Optional[datetime]:
        """Return most recent successful payment time (UTC) with optional JSON cache.

        Intended as a fallback when renewal_period_end is unavailable.
        """
        mid = str(membership_id or "").strip()
        if not mid:
            return None

        now = datetime.now(timezone.utc)
        ttl = timedelta(hours=float(cache_ttl_hours)) if cache_ttl_hours else timedelta(hours=24)

        # Cache read
        cache: dict = {}
        cache_file = Path(cache_path) if cache_path else None
        if cache_file:
            try:
                async with _PAYMENT_CACHE_LOCK:
                    if cache_file.exists():
                        cache = json.loads(cache_file.read_text(encoding="utf-8") or "{}")
            except Exception:
                cache = {}

            rec = cache.get(mid) if isinstance(cache, dict) else None
            if isinstance(rec, dict):
                fetched_at = self._parse_dt_any(rec.get("fetched_at"))
                last_iso = str(rec.get("last_success_paid_at") or "").strip()
                last_dt = self._parse_dt_any(last_iso) if last_iso else None
                if fetched_at and (now - fetched_at) < ttl:
                    return last_dt

        # Cache miss: fetch payments and compute
        payments = await self.get_payments_for_membership(mid)
        success_dt: Optional[datetime] = None
        for p in payments:
            if not isinstance(p, dict):
                continue
            st = str(p.get("status") or "").strip().lower()
            if st not in {"succeeded", "paid", "successful", "success"}:
                continue
            ts = p.get("paid_at") or p.get("created_at") or ""
            dt = self._parse_dt_any(ts)
            if dt:
                success_dt = dt
                break

        # Cache write (best-effort)
        if cache_file:
            try:
                async with _PAYMENT_CACHE_LOCK:
                    latest: dict = {}
                    with suppress(Exception):
                        if cache_file.exists():
                            latest = json.loads(cache_file.read_text(encoding="utf-8") or "{}")
                    if not isinstance(latest, dict):
                        latest = {}
                    latest[mid] = {
                        "last_success_paid_at": (success_dt.isoformat().replace("+00:00", "Z") if success_dt else ""),
                        "fetched_at": now.isoformat().replace("+00:00", "Z"),
                    }
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    # Atomic write (same-folder temp -> replace) to avoid truncated JSON on crash.
                    tmp = cache_file.with_suffix(cache_file.suffix + ".tmp")
                    tmp.write_text(json.dumps(latest, indent=2, ensure_ascii=False), encoding="utf-8")
                    try:
                        os.replace(tmp, cache_file)
                    except Exception:
                        with suppress(Exception):
                            tmp.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass

        return success_dt

    async def is_entitled_until_end(
        self,
        membership_id: str,
        membership: Optional[Dict],
        *,
        cache_path: str | None = None,
        monthly_days: int = 30,
        grace_days: int = 3,
        now: Optional[datetime] = None,
    ) -> tuple[bool, Optional[datetime], str]:
        """Entitlement check:
        - Primary: now < renewal_period_end (or equivalent end timestamp)
        - Fallback: if end missing, now < last_success_paid_at + monthly_days + grace_days
        """
        now_dt = now or datetime.now(timezone.utc)
        end_dt = self.access_end_dt_from_membership(membership)
        if end_dt:
            return (now_dt < end_dt, end_dt, "membership_end")

        last_paid = await self.get_last_successful_payment_time(membership_id, cache_path=cache_path)
        if last_paid:
            cutoff = last_paid + timedelta(days=int(monthly_days) + int(grace_days))
            return (now_dt < cutoff, cutoff, "last_success_paid_at")

        return (False, None, "no_end_or_payment")

    def _extract_error_message(self, data: object, status: int) -> str:
        """Extract a readable error message from Whop's API error formats."""
        try:
            if isinstance(data, dict):
                # Common Whop shape: {"error": {"message": "...", "type": "..."}}
                err = data.get("error")
                if isinstance(err, dict):
                    msg = err.get("message")
                    typ = err.get("type")
                    if msg and typ:
                        return f"{msg} ({typ})"
                    if msg:
                        return str(msg)
                # Sometimes: {"message": "..."}
                msg = data.get("message")
                if msg:
                    return str(msg)
        except Exception:
            pass
        return f"API error: {status}"
    
    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Dict:
        """
        Make API request to Whop.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., "/memberships")
            params: Query parameters
            json_data: Request body (for POST/PUT)
        
        Returns:
            JSON response as dict
        
        Raises:
            WhopAPIError: If request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    params=params,
                    json=json_data,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 401:
                        raise WhopAPIError("Invalid API key or expired token")
                    if resp.status == 403:
                        raise WhopAPIError("API key lacks required permissions")
                    if resp.status == 429:
                        raise WhopAPIError("Rate limit exceeded - wait before retrying")
                    
                    try:
                        data = await resp.json()
                    except ContentTypeError:
                        # Non-JSON error bodies happen; keep a small snippet for debugging.
                        txt = (await resp.text())[:2000]
                        data = {"error": {"message": txt, "type": "non_json"}}
                    
                    if resp.status >= 400:
                        raise WhopAPIError(self._extract_error_message(data, resp.status))
                    
                    return data
        except aiohttp.ClientError as e:
            raise WhopAPIError(f"Network error: {e}")
    
    async def get_membership_by_discord_id(self, discord_id: str) -> Optional[Dict]:
        """
        Deprecated: Discord IDs are not reliably queryable via Whop Company API.
        Keep this method for backwards compatibility but return None to avoid incorrect matches.
        """
        log.warning(
            "get_membership_by_discord_id is deprecated and disabled to prevent mismatched memberships. "
            "Use get_membership_by_id with a membership_id derived from webhook events."
        )
        return None
    
    async def get_membership_by_id(self, membership_id: str) -> Optional[Dict]:
        """
        Get membership by Whop membership ID.
        
        Args:
            membership_id: Whop membership ID
        
        Returns:
            Membership data dict or None
        """
        try:
            response = await self._request("GET", f"/memberships/{membership_id}")
            # Response may be direct object or wrapped in "data"
            return response.get("data") if "data" in response else response
        except WhopAPIError as e:
            log.warning(f"Failed to get membership {membership_id}: {e}")
            return None

    async def get_member_by_id(self, member_id: str) -> Optional[Dict]:
        """
        Get Whop Member (company member record) by member ID (mber_...).
        This endpoint includes user email/name which is useful for support tooling.
        """
        mid = str(member_id or "").strip()
        if not mid:
            return None
        try:
            response = await self._request("GET", f"/members/{mid}")
            # /members/{id} may be direct object or wrapped in "data" depending on API version.
            if isinstance(response, dict) and "data" in response:
                return response.get("data")
            return response if isinstance(response, dict) else None
        except WhopAPIError as e:
            log.warning(f"Failed to get member {mid}: {e}")
            return None

    async def get_payment_by_id(self, payment_id: str) -> Optional[Dict]:
        """Get a payment by ID (pay_...)."""
        pid = str(payment_id or "").strip()
        if not pid:
            return None
        try:
            response = await self._request("GET", f"/payments/{pid}")
            return response.get("data") if isinstance(response, dict) and "data" in response else response
        except WhopAPIError as e:
            log.warning(f"Failed to get payment {pid}: {e}")
            return None

    async def get_refund_by_id(self, refund_id: str) -> Optional[Dict]:
        """Get a refund by ID (rfnd_...)."""
        rid = str(refund_id or "").strip()
        if not rid:
            return None
        try:
            response = await self._request("GET", f"/refunds/{rid}")
            return response.get("data") if isinstance(response, dict) and "data" in response else response
        except WhopAPIError as e:
            log.warning(f"Failed to get refund {rid}: {e}")
            return None

    async def get_dispute_by_id(self, dispute_id: str) -> Optional[Dict]:
        """Get a dispute by ID (dspt_...)."""
        did = str(dispute_id or "").strip()
        if not did:
            return None
        try:
            response = await self._request("GET", f"/disputes/{did}")
            return response.get("data") if isinstance(response, dict) and "data" in response else response
        except WhopAPIError as e:
            log.warning(f"Failed to get dispute {did}: {e}")
            return None

    async def get_invoice_by_id(self, invoice_id: str) -> Optional[Dict]:
        """Get an invoice by ID (inv_...)."""
        iid = str(invoice_id or "").strip()
        if not iid:
            return None
        try:
            response = await self._request("GET", f"/invoices/{iid}")
            return response.get("data") if isinstance(response, dict) and "data" in response else response
        except WhopAPIError as e:
            log.warning(f"Failed to get invoice {iid}: {e}")
            return None

    async def get_setup_intent_by_id(self, setup_intent_id: str) -> Optional[Dict]:
        """Get a setup intent by ID (sint_...)."""
        sid = str(setup_intent_id or "").strip()
        if not sid:
            return None
        try:
            response = await self._request("GET", f"/setup_intents/{sid}")
            return response.get("data") if isinstance(response, dict) and "data" in response else response
        except WhopAPIError as e:
            log.warning(f"Failed to get setup_intent {sid}: {e}")
            return None
    
    async def get_user_memberships(self, user_id: str) -> List[Dict]:
        """
        Get all memberships for a Whop user ID.
        
        Args:
            user_id: Whop user ID
        
        Returns:
            List of membership dicts
        """
        try:
            response = await self._request(
                "GET",
                "/memberships",
                params={"company_id": self.company_id, "user_id": user_id}
            )
            memberships = response.get("data", [])
            return memberships if isinstance(memberships, list) else []
        except WhopAPIError as e:
            log.warning(f"Failed to get memberships for user {user_id}: {e}")
            return []

    async def list_memberships(
        self,
        *,
        first: int = 100,
        after: str | None = None,
        params: Optional[Dict] = None,
    ) -> tuple[List[Dict], Dict]:
        """
        List memberships for the company (company-scoped).

        Whop uses cursor pagination:
        - `first` controls page size
        - `after` is a cursor from `page_info.end_cursor`

        Returns: (memberships, page_info)
        """
        try:
            q: Dict = {"company_id": self.company_id}
            try:
                if int(first) > 0:
                    q["first"] = int(first)
            except Exception:
                q["first"] = 100
            if after:
                q["after"] = str(after)
            if isinstance(params, dict):
                q.update(params)

            response = await self._request("GET", "/memberships", params=q)
            data = response.get("data", []) if isinstance(response, dict) else []
            page_info = response.get("page_info", {}) if isinstance(response, dict) else {}

            memberships: List[Dict] = data if isinstance(data, list) else []
            page_info = page_info if isinstance(page_info, dict) else {}
            return (memberships, page_info)
        except WhopAPIError as e:
            log.warning(f"Failed to list memberships: {e}")
            return ([], {})

    async def list_members(
        self,
        *,
        first: int = 100,
        after: str | None = None,
        params: Optional[Dict] = None,
    ) -> tuple[List[Dict], Dict]:
        """List company members (Whop dashboard "Users" view).

        Returns: (members, page_info)
        """
        try:
            q: Dict = {"company_id": self.company_id}
            try:
                if int(first) > 0:
                    q["first"] = int(first)
            except Exception:
                q["first"] = 100
            if after:
                q["after"] = str(after)
            if isinstance(params, dict):
                q.update(params)

            response = await self._request("GET", "/members", params=q)
            data = response.get("data", []) if isinstance(response, dict) else []
            page_info = response.get("page_info", {}) if isinstance(response, dict) else {}

            members: List[Dict] = data if isinstance(data, list) else []
            page_info = page_info if isinstance(page_info, dict) else {}
            return (members, page_info)
        except WhopAPIError as e:
            log.warning(f"Failed to list members: {e}")
            return ([], {})
    
    async def list_payments(
        self,
        *,
        first: int = 100,
        after: str | None = None,
        params: Optional[Dict] = None,
    ) -> tuple[List[Dict], Dict]:
        """List payments for the company with optional filters (created_after, created_before, product_ids, statuses).

        Returns: (payments, page_info)
        """
        try:
            q: Dict = {"company_id": self.company_id}
            try:
                if int(first) > 0:
                    q["first"] = int(first)
            except Exception:
                q["first"] = 100
            if after:
                q["after"] = str(after)
            if isinstance(params, dict):
                q.update(params)
            response = await self._request("GET", "/payments", params=q)
            data = response.get("data", []) if isinstance(response, dict) else []
            page_info = response.get("page_info", {}) if isinstance(response, dict) else {}
            payments: List[Dict] = data if isinstance(data, list) else []
            return (payments, page_info)
        except WhopAPIError as e:
            log.warning(f"Failed to list payments: {e}")
            return ([], {})

    async def get_payments_for_membership(self, membership_id: str) -> List[Dict]:
        """
        Get payment history for a membership.
        
        Args:
            membership_id: Whop membership ID
        
        Returns:
            List of payment dicts (most recent first)
        """
        try:
            response = await self._request(
                "GET",
                "/payments",
                params={"company_id": self.company_id, "membership_id": membership_id}
            )
            payments = response.get("data", [])
            if isinstance(payments, list):
                # Sort by created_at descending (most recent first)
                try:
                    payments.sort(
                        key=lambda p: p.get("created_at", ""),
                        reverse=True
                    )
                except Exception:
                    pass  # Keep original order if sorting fails
                return payments
            return []
        except WhopAPIError as e:
            log.warning(f"Failed to get payments for membership {membership_id}: {e}")
            return []
    
    async def verify_membership_status(
        self,
        membership_id: str,
        expected_status: str = "active",
    ) -> Dict[str, any]:
        """
        Verify membership status matches expected value (membership_id-based).
        """
        membership = await self.get_membership_by_id(membership_id)
        if not membership:
            return {
                "matches": False,
                "actual_status": None,
                "membership_data": None,
                "error": "No membership found",
            }
        actual_status = str(membership.get("status", "")).lower()
        expected_lower = expected_status.lower()
        return {
            "matches": actual_status == expected_lower,
            "actual_status": actual_status,
            "membership_data": membership,
            "error": None,
        }
    
    # NOTE: Do not add /users lookups here. The v1 Company API does not expose a /users list endpoint
    # and will return 404. Any user enrichment should be derived from membership objects returned by
    # /memberships (company-scoped) or specific membership fetches.

