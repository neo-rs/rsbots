from __future__ import annotations

import asyncio
import json
import logging

from app.config import AppConfig
from app.discord_format import format_phone_display
from app.logging_setup import setup_logging
from app.telnyx_probe import fetch_messaging_profiles, fetch_phone_numbers, summarize_messaging_setup


async def main() -> None:
    config = AppConfig.load()
    setup_logging(config.log_level)
    log = logging.getLogger("probe_telnyx")

    profiles = await fetch_messaging_profiles(config)
    phone_numbers = await fetch_phone_numbers(config)
    summary = summarize_messaging_setup(
        profiles=profiles,
        phone_numbers=phone_numbers,
        configured_profile_id=config.telnyx_messaging_profile_id,
    )

    selected = summary.get("selected_profile") or {}
    profile_id = str(selected.get("id") or "")
    numbers = summary.get("numbers_by_profile", {}).get(profile_id, [])

    log.info("event=telnyx_probe_start reason=operator_requested_profile_scan")
    print("=" * 78)
    print("TELNYX PROGRAMMABLE MESSAGING")
    print("=" * 78)
    print(f"Profiles found: {len(profiles)}")
    if selected:
        print(f"Selected profile: {selected.get('name')} ({profile_id})")
        print(f"Enabled: {selected.get('enabled')}")
        print(f"Webhook URL: {selected.get('webhook_url') or '(not set)'}")
        print(f"Webhook failover: {selected.get('webhook_failover_url') or '(not set)'}")
        print(f"Webhook API version: {selected.get('webhook_api_version')}")
        print(f"Mobile only: {selected.get('mobile_only')}")
        print(f"Smart encoding: {selected.get('smart_encoding')}")
        print("Numbers on profile:")
        for entry in numbers:
            print(
                f"  - {format_phone_display(entry.get('number', ''))} "
                f"[{entry.get('type')}] status={entry.get('status')}"
            )
    else:
        print("No messaging profile selected.")

    print()
    print("Bridge inbound webhook should be:")
    print("  https://YOUR_PUBLIC_DOMAIN/webhooks/telnyx")
    print()
    print("Outbound SMS does not require the Telnyx webhook.")
    print("Inbound SMS to Discord does require the messaging profile webhook above.")
    print("=" * 78)

    print(json.dumps(summary, indent=2, default=str)[:12000])


if __name__ == "__main__":
    asyncio.run(main())
