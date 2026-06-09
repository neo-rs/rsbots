#!/usr/bin/env python3
"""Re-render all conversation cards on Discord (run on Oracle from bridge venv)."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.runtime import BridgeRuntime  # noqa: E402


async def main() -> int:
    runtime = BridgeRuntime.build()
    if not runtime.discord_bot:
        print("No DISCORD_BOT_TOKEN configured.")
        return 1
    runtime.discord_bot.start_background()
    await runtime.discord_bot.wait_ready()
    count = await runtime.conversations.refresh_all_threads()
    print(f"Refreshed {count} thread(s).")
    await runtime.discord_bot.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
