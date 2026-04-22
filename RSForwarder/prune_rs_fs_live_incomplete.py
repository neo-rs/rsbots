from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from mirror_world_config import load_config_with_secrets

from RSForwarder.rs_fs_sheet_sync import RsFsSheetSync


async def _run(*, apply: bool, max_log: int) -> int:
    base = Path(__file__).resolve().parent
    cfg, _config_path, _secrets_path = load_config_with_secrets(base)

    sheet = RsFsSheetSync(cfg)
    if not sheet.enabled():
        print("ERROR: RS-FS sheet not enabled (missing credentials/config).")
        return 2

    print("===============================================================================")
    print("RSForwarder RS-FS Live List prune (incomplete rows)")
    print("===============================================================================")
    print(f"Mode: {'APPLY (deletes enabled)' if apply else 'DRY-RUN (no deletes)'}")
    print("")

    ok, msg, n = await sheet.prune_live_incomplete_rows(apply=apply, max_log=max_log)
    print("")
    print(f"RESULT: ok={ok} pruned={n} msg={msg}")
    return 0 if ok else 3


def main() -> int:
    ap = argparse.ArgumentParser(description="Prune incomplete rows from RS-FS Live List (title/url/affiliate missing).")
    ap.add_argument("--apply", action="store_true", help="Actually delete rows (default is dry-run).")
    ap.add_argument("--max-log", type=int, default=50, help="Max rows to print (default 50).")
    args = ap.parse_args()
    return int(asyncio.run(_run(apply=bool(args.apply), max_log=int(args.max_log or 0))))


if __name__ == "__main__":
    raise SystemExit(main())

