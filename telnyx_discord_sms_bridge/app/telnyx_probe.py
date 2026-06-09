from __future__ import annotations

from typing import Any

import httpx

from app.config import AppConfig


async def fetch_messaging_profiles(config: AppConfig) -> list[dict[str, Any]]:
    url = f"{config.telnyx_api_base}/messaging_profiles"
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            url,
            headers={
                "Authorization": f"Bearer {config.telnyx_api_key}",
                "Accept": "application/json",
            },
        )
    response.raise_for_status()
    body = response.json()
    data = body.get("data", [])
    return data if isinstance(data, list) else []


async def fetch_phone_numbers(config: AppConfig) -> list[dict[str, Any]]:
    url = f"{config.telnyx_api_base}/phone_numbers"
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            url,
            params={"page[size]": 50},
            headers={
                "Authorization": f"Bearer {config.telnyx_api_key}",
                "Accept": "application/json",
            },
        )
    response.raise_for_status()
    body = response.json()
    data = body.get("data", [])
    return data if isinstance(data, list) else []


def summarize_messaging_setup(
    *,
    profiles: list[dict[str, Any]],
    phone_numbers: list[dict[str, Any]],
    configured_profile_id: str | None,
) -> dict[str, Any]:
    profile_by_id = {str(p.get("id")): p for p in profiles}
    selected = profile_by_id.get(str(configured_profile_id or "")) if configured_profile_id else None
    if not selected and len(profiles) == 1:
        selected = profiles[0]

    numbers_by_profile: dict[str, list[dict[str, str]]] = {}
    for entry in phone_numbers:
        profile_id = str(entry.get("messaging_profile_id") or "")
        numbers_by_profile.setdefault(profile_id, []).append(
            {
                "number": str(entry.get("phone_number") or ""),
                "type": str(entry.get("phone_number_type") or "unknown"),
                "status": str(entry.get("status") or "unknown"),
            }
        )

    return {
        "configured_profile_id": configured_profile_id,
        "selected_profile": selected,
        "profiles": profiles,
        "numbers_by_profile": numbers_by_profile,
    }
