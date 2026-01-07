#!/usr/bin/env python3
"""
Whop API Client
Handles all direct API calls to Whop Developer API

Canonical Owner: This module owns all Whop API interactions.
"""

import aiohttp
import logging
from typing import Dict, Optional, List
from aiohttp import ContentTypeError

log = logging.getLogger("rs-checker")


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
        Get active membership for a Discord user ID.
        
        Args:
            discord_id: Discord user ID (as string)
        
        Returns:
            Membership data dict or None if not found
        """
        try:
            # Search memberships by Discord ID
            # Whop API v1: Company API requires company_id for /memberships list.
            response = await self._request(
                "GET",
                "/memberships",
                params={"company_id": self.company_id, "discord_id": discord_id}
            )
            
            # Response structure: { "data": [...] } or direct array
            memberships = response.get("data", [])
            if isinstance(memberships, list) and memberships:
                # Return first active membership, or first if no status filter
                for membership in memberships:
                    status = membership.get("status", "").lower()
                    if status in ("active", "trialing"):
                        return membership
                # If no active found, return first one
                return memberships[0]
            
            return None
        except WhopAPIError as e:
            log.warning(f"Failed to get membership for Discord ID {discord_id}: {e}")
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
        discord_id: str, 
        expected_status: str = "active"
    ) -> Dict[str, any]:
        """
        Verify membership status matches expected value.
        
        Args:
            discord_id: Discord user ID
            expected_status: Expected membership status (active, canceled, etc.)
        
        Returns:
            Dict with 'matches', 'actual_status', 'membership_data', 'error'
        """
        membership = await self.get_membership_by_discord_id(discord_id)
        
        if not membership:
            return {
                "matches": False,
                "actual_status": None,
                "membership_data": None,
                "error": "No membership found"
            }
        
        actual_status = membership.get("status", "").lower()
        expected_lower = expected_status.lower()
        matches = actual_status == expected_lower
        
        return {
            "matches": matches,
            "actual_status": actual_status,
            "membership_data": membership,
            "error": None
        }
    
    # NOTE: Do not add /users lookups here. The v1 Company API does not expose a /users list endpoint
    # and will return 404. Any user enrichment should be derived from membership objects returned by
    # /memberships (company-scoped) or specific membership fetches.

