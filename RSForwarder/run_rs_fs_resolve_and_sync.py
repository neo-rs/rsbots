from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import List, Tuple

from mirror_world_config import load_config_with_secrets

from RSForwarder import affiliate_rewriter
from RSForwarder.rs_forwarder_bot import _rsfs_is_valid_affiliate_url
from RSForwarder.rs_fs_monitor_data_resolver import RsFsMonitorDataResolver
from RSForwarder.rs_fs_sheet_sync import RsFsSheetSync


def _one_line(s: object, *, max_len: int = 140) -> str:
    t = str(s or "").replace("\r", " ").replace("\n", " ").strip()
    if len(t) > max_len:
        return t[: max_len - 3] + "..."
    return t


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
        # Live list must not be polluted with incomplete rows.
        if not (title and url and aff):
            continue
        out.append([store, sku, title, aff, url])
    return out


async def _run(
    apply: bool,
    sync_live: bool,
    monitor_data_dir: str,
    *,
    show_all: bool = False,
    json_out: str = "",
    fill_affiliate: bool = False,
    use_history: bool = True,
    upsert_history: bool = True,
) -> int:
    ap = argparse.ArgumentParser(description="RS-FS local: resolve Current from monitor_data then sync Live.")
    base = Path(__file__).resolve().parent
    cfg, _config_path, _secrets_path = load_config_with_secrets(base)

    sheet = RsFsSheetSync(cfg)
    if not sheet.enabled():
        print("ERROR: RS-FS sheet not enabled (missing credentials/config).")
        return 2

    md_dir = Path(monitor_data_dir).resolve() if monitor_data_dir else (base / "monitor_data")
    resolver = RsFsMonitorDataResolver(md_dir)
    try:
        md_count = len(list(md_dir.glob("*.json"))) if md_dir.is_dir() else 0
    except Exception:
        md_count = 0

    print("===============================================================================")
    print("RSForwarder RS-FS resolve + sync")
    print("===============================================================================")
    print(f"Mode: {'APPLY (writes enabled)' if apply else 'DRY-RUN (no writes)'}")
    print(f"Live sync: {'enabled' if sync_live else 'disabled'}")
    print(f"monitor_data dir: {md_dir} (json files: {md_count})")
    print(f"History: {'enabled' if use_history else 'disabled'}")
    if use_history:
        print(f"History upsert on apply: {'enabled' if (apply and upsert_history) else 'disabled'}")
    print("")

    history_cache: dict[str, dict] = {}
    if use_history:
        try:
            history_cache = await sheet.fetch_history_cache(force=True)
            print(f"Loaded History keys: {len(history_cache)}")
        except Exception as e:
            history_cache = {}
            print(f"WARNING: failed to load History cache: {type(e).__name__}: {e}")
        print("")

    current = await sheet.fetch_current_list_rows()
    if not current:
        print("No Current List rows found.")
        return 0

    updated = 0
    scanned = 0
    rows_out: List[List[str]] = []
    missing_both = 0
    hits: List[Tuple[str, str, str, str, str, str]] = []
    changes_for_json: List[dict] = []
    miss_reasons: dict[str, int] = {}
    aff_filled = 0
    hist_used = 0
    hist_miss = 0
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
        aff0 = str(row[8] or "").strip() if len(row) > 8 else ""

        if title and url:
            continue
        if not title and not url:
            missing_both += 1

        # Current List has Monitor Tag in column D (index 3) per _rsfs_write_current_list.
        monitor_tag = str(row[3] or "").strip() if len(row) > 3 else ""

        # 1) History-first (fast, deterministic)
        channel_key = (monitor_tag or "").strip().lower()
        key = f"{store.lower()}|{sku.lower()}"
        if use_history and key in history_cache:
            h = history_cache.get(key) or {}
            h_title = str(h.get("title") or "").strip()
            h_url = str(h.get("url") or "").strip()
            h_aff = str(h.get("affiliate_url") or "").strip()
            before_title = title
            before_url = url
            before_aff = aff0
            if not title and h_title:
                rows_out[-1][6] = h_title
                title = h_title
            if not url and h_url:
                rows_out[-1][7] = h_url
                url = h_url
            if len(rows_out[-1]) > 8 and not aff0 and h_aff:
                rows_out[-1][8] = h_aff
                aff0 = h_aff
            if title or url:
                hist_used += 1
                reason = "hit:history"
                changes_for_json.append(
                    {
                        "store": store,
                        "sku": sku,
                        "before": {"title": before_title, "url": before_url, "affiliate_url": before_aff},
                        "after": {"title": title, "url": url, "affiliate_url": aff0},
                        "monitor_tag": monitor_tag,
                        "monitor_channel_key": channel_key,
                        "reason": reason,
                    }
                )
                # Continue to affiliate fill step below (if requested) rather than monitor_data.
            else:
                hist_miss += 1
        else:
            hist_miss += 1

        # 2) monitor_data (only if still missing title/url)
        hit = None
        reason = ""
        if not (title and url):
            hit, reason, channel_key = resolver.explain_resolve(store=store, sku=sku, monitor_tag=monitor_tag)
            if not hit:
                miss_reasons[reason] = int(miss_reasons.get(reason) or 0) + 1
                if show_all:
                    print(f"- MISS {store}|{sku} tag={monitor_tag or '-'} ck={channel_key or '-'} reason={reason}")
                continue

        before_title = title
        before_url = url
        before_aff = aff0
        if not url and (hit.url or "").strip():
            rows_out[-1][7] = (hit.url or "").strip()
            url = rows_out[-1][7]
        if not title and (hit.title or "").strip():
            rows_out[-1][6] = (hit.title or "").strip()
            title = rows_out[-1][6]

        # Fill Affiliate URL (col I / index 8) only when requested.
        if fill_affiliate:
            try:
                existing_aff = str(rows_out[-1][8] or "").strip() if len(rows_out[-1]) > 8 else ""
            except Exception:
                existing_aff = ""
            if url and not existing_aff:
                # Fast-path: if resolved URL already looks like a valid affiliate, persist it.
                if _rsfs_is_valid_affiliate_url(url):
                    rows_out[-1][8] = url
                    aff_filled += 1
                else:
                    # Canonical compute path (may do network/Playwright as needed).
                    mapped, _notes = await affiliate_rewriter.compute_affiliate_rewrites_plain(cfg, [url])
                    cand = str(mapped.get(url) or "").strip()
                    if cand and _rsfs_is_valid_affiliate_url(cand):
                        rows_out[-1][8] = cand
                        aff_filled += 1

        if title or url:
            updated += 1
            hits.append((store, sku, _one_line(before_title), _one_line(before_url), _one_line(title), _one_line(url)))
            changes_for_json.append(
                {
                    "store": store,
                    "sku": sku,
                    "before": {"title": before_title, "url": before_url, "affiliate_url": before_aff},
                    "after": {"title": title, "url": url, "affiliate_url": str(rows_out[-1][8] or "").strip() if len(rows_out[-1]) > 8 else ""},
                    "monitor_tag": monitor_tag,
                    "monitor_channel_key": channel_key,
                    "reason": reason,
                }
            )

    print(f"Scanned Full Send rows: {scanned}")
    print(f"Rows missing BOTH title+url at scan-time: {missing_both}")
    print(f"Resolved/updated rows (from monitor_data): {updated}")
    if use_history:
        print(f"History hits used: {hist_used}")
        print(f"History misses: {hist_miss}")
    if fill_affiliate:
        print(f"Affiliate URL filled: {aff_filled}")
    if miss_reasons:
        print("")
        print("Resolve misses (counts):")
        for k in sorted(miss_reasons.keys()):
            print(f"- {k}: {miss_reasons[k]}")
    if hits:
        print("")
        if show_all:
            print("Resolved rows (store|sku):")
        else:
            print("Sample resolved rows (store|sku):")
        for store, sku, bt, bu, at, au in (hits if show_all else hits[:12]):
            print(f"- {store}|{sku}")
            if bt != at:
                print(f"    title: {bt!r} -> {at!r}")
            if bu != au:
                print(f"    url:   {bu!r} -> {au!r}")

    if json_out:
        try:
            p = Path(json_out).expanduser()
            if not p.is_absolute():
                p = (base / p).resolve()
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(changes_for_json, ensure_ascii=False, indent=2), encoding="utf-8")
            print("")
            print(f"JSON report written: {p}")
        except Exception as e:
            print("")
            print(f"WARNING: could not write JSON report {json_out!r}: {type(e).__name__}: {e}")
    if not apply:
        print("Dry-run: no sheet writes performed. Use --apply to write Current List.")
    else:
        ok, msg, n = await sheet.write_current_list_mirror(rows_out)
        print(f"WRITE Current List: ok={ok} rows={n} msg={msg}")
        if not ok:
            return 3
        if use_history and upsert_history:
            # Upsert History using the best-known values from Current List.
            # History columns: Store, SKU, Title, URL, Affiliate URL, First Seen, Last Seen, Last Release ID, Source
            hist_rows: List[List[str]] = []
            for r in rows_out:
                if len(r) < 3:
                    continue
                if str(r[0] or "").strip().lower() == "release id":
                    continue
                if not _is_full_send_row(r):
                    continue
                store = str(r[1] or "").strip()
                sku = str(r[2] or "").strip()
                if not (store and sku):
                    continue
                title = str(r[6] or "").strip() if len(r) > 6 else ""
                url = str(r[7] or "").strip() if len(r) > 7 else ""
                aff = str(r[8] or "").strip() if len(r) > 8 else ""
                release_id = str(r[0] or "").strip()
                source = "current_list_apply"
                # History must not be polluted with incomplete rows.
                if not (title and url and aff):
                    continue
                # Let upsert_history_rows fill first/last seen defaults.
                hist_rows.append([store, sku, title, url, aff, "", "", release_id, source])
            okh, msgh, added_h, updated_h = await sheet.upsert_history_rows(hist_rows)
            print(f"UPSERT History: ok={okh} added={added_h} updated={updated_h} msg={msgh}")
            if not okh:
                return 5

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
    ap.add_argument("--show-all", action="store_true", help="Print every resolved row (not just a small sample).")
    ap.add_argument("--json-out", default="", help="Write a JSON report of resolved rows (relative to RSForwarder/).")
    ap.add_argument("--fill-affiliate", action="store_true", help="Attempt to populate Affiliate URL for resolved rows.")
    ap.add_argument("--no-history", action="store_true", help="Disable History-first fills.")
    ap.add_argument("--no-history-upsert", action="store_true", help="On apply, do not upsert History.")
    args = ap.parse_args()
    raise SystemExit(
        asyncio.run(
            _run(
                args.apply,
                args.sync_live,
                args.monitor_data_dir,
                show_all=bool(args.show_all),
                json_out=str(args.json_out or ""),
                fill_affiliate=bool(args.fill_affiliate),
                use_history=(not bool(args.no_history)),
                upsert_history=(not bool(args.no_history_upsert)),
            )
        )
    )

