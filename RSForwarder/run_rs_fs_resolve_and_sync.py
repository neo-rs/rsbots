from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import List

from mirror_world_config import load_config_with_secrets

from RSForwarder.rs_fs_monitor_data_resolver import RsFsMonitorDataResolver
from RSForwarder.rs_fs_sheet_sync import RsFsSheetSync


def _is_full_send_row(row: List[str]) -> bool:
    # Column M (index 12) in Current List is "monitor tags" / labels.
    full_send = str(row[12] or "").strip() if len(row) > 12 else ""
    low = full_send.lower()
    return ("full-send" in low) or ("💶┃full-send-🤖" in full_send)


def _build_live_rows_from_current(current_rows: List[List[str]]) -> List[List[str]]:
    out: List[List[str]] = []
    for row in current_rows:
        if len(row) < 3:
            continue
        if str(row[0] or "").strip().lower() == "release id":
            continue
        if not _is_full_send_row(row):
            continue
        store = str(row[1] or "").strip()
        sku = str(row[2] or "").strip()
        if not (store and sku):
            continue
        title = str(row[6] or "").strip() if len(row) > 6 else ""
        url = str(row[7] or "").strip() if len(row) > 7 else ""
        aff = str(row[8] or "").strip() if len(row) > 8 else ""
        out.append([store, sku, title, aff, url])
    return out


async def _run(apply: bool, sync_live: bool, monitor_data_dir: str) -> int:
    ap = argparse.ArgumentParser(description="RS-FS local: resolve Current from monitor_data then sync Live.")
    base = Path(__file__).resolve().parent
    cfg, _config_path, _secrets_path = load_config_with_secrets(base)

    sheet = RsFsSheetSync(cfg)
    if not sheet.enabled():
        print("ERROR: RS-FS sheet not enabled (missing credentials/config).")
        return 2

    md_dir = Path(monitor_data_dir).resolve() if monitor_data_dir else (base / "monitor_data")
    resolver = RsFsMonitorDataResolver(md_dir)

    current = await sheet.fetch_current_list_rows()
    if not current:
        print("No Current List rows found.")
        return 0

    updated = 0
    scanned = 0
    rows_out: List[List[str]] = []
    for row in current:
        rows_out.append(list(row))
        if len(row) < 3:
            continue
        if str(row[0] or "").strip().lower() == "release id":
            continue
        if not _is_full_send_row(row):
            continue

        scanned += 1
        store = str(row[1] or "").strip()
        sku = str(row[2] or "").strip()
        if not (store and sku):
            continue

        title = str(row[6] or "").strip() if len(row) > 6 else ""
        url = str(row[7] or "").strip() if len(row) > 7 else ""

        if title and url:
            continue

        # Current List has Monitor Tag in column D (index 3) per _rsfs_write_current_list.
        monitor_tag = str(row[3] or "").strip() if len(row) > 3 else ""

        hit = resolver.resolve(store=store, sku=sku, monitor_tag=monitor_tag)
        if not hit:
            continue

        if not url and (hit.url or "").strip():
            rows_out[-1][7] = (hit.url or "").strip()
            url = rows_out[-1][7]
        if not title and (hit.title or "").strip():
            rows_out[-1][6] = (hit.title or "").strip()
            title = rows_out[-1][6]

        if title or url:
            updated += 1

    print(f"Scanned Full Send rows: {scanned}")
    print(f"Resolved/updated rows (from monitor_data): {updated}")
    if not apply:
        print("Dry-run: no sheet writes performed. Use --apply to write Current List.")
    else:
        ok, msg, n = await sheet.write_current_list_mirror(rows_out)
        print(f"WRITE Current List: ok={ok} rows={n} msg={msg}")
        if not ok:
            return 3

    if sync_live:
        live_rows = _build_live_rows_from_current(rows_out)
        if not live_rows:
            print("No Full Send rows found to sync.")
            return 0
        if not apply:
            print(f"Dry-run: would sync Live rows={len(live_rows)} (use --apply --sync-live).")
            return 0
        ok, msg, added, updated2, deleted = await sheet.sync_rows_mirror(live_rows, delete_stale=False)
        print(f"SYNC Live: ok={ok} added={added} updated={updated2} deleted={deleted} msg={msg}")
        if not ok:
            return 4

    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="RS-FS local: resolve Current from monitor_data then sync Live.")
    ap.add_argument("--apply", action="store_true", help="Write changes to sheet (default is dry-run).")
    ap.add_argument("--sync-live", action="store_true", help="After resolve, sync Full Send Current -> Live.")
    ap.add_argument("--monitor-data-dir", default="", help="Override monitor_data dir (default RSForwarder/monitor_data).")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(_run(args.apply, args.sync_live, args.monitor_data_dir)))

