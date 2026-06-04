#!/usr/bin/env python3
"""CLI preview for Whop dispute + resolution case embeds (no Discord bot, no channels)."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from contextlib import suppress
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import whop_dispute_cases  # noqa: E402
from whop_api_client import WhopAPIClient  # noqa: E402
from whop_brief import fetch_whop_brief  # noqa: E402


def _deep_get(obj: object, path: str) -> object:
    cur = obj
    for part in str(path or "").split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _load_config() -> dict:
    p = ROOT / "config.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def _load_secrets() -> dict:
    p = ROOT / "config.secrets.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


async def _run(*, membership_id: str, dispute_id: str) -> int:
    cfg = _load_config()
    sec = _load_secrets()
    whop_cfg = cfg.get("whop_api") if isinstance(cfg.get("whop_api"), dict) else {}
    api_key = str((sec.get("whop_api") or {}).get("api_key") or sec.get("whop_api_key") or "").strip()
    if not api_key:
        print("Missing Whop API key in config.secrets.json", file=sys.stderr)
        return 2
    client = WhopAPIClient(api_key=api_key, company_id=str(whop_cfg.get("company_id") or ""))
    enrich = bool(whop_cfg.get("enable_enrichment", True))

    async def _fetch_brief(mid: str) -> dict:
        b = await fetch_whop_brief(client, mid, enable_enrichment=enrich)
        return b if isinstance(b, dict) else {}

    async def _best_payment(mid: str, limit: int = 25) -> dict:
        if hasattr(client, "list_payments_for_membership"):
            with suppress(Exception):
                rows = await client.list_payments_for_membership(mid, limit=limit)  # type: ignore[attr-defined]
                if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                    return rows[0]
        return {}

    whop_dispute_cases.initialize(
        bot=None,  # type: ignore[arg-type]
        whop_api_client=client,
        dispute_category_id=int(whop_cfg.get("dispute_case_category_id") or 0),
        resolution_category_id=int(whop_cfg.get("resolution_case_category_id") or 0),
        company_id=str(whop_cfg.get("company_id") or ""),
        ensure_channel=lambda **_: None,  # type: ignore[assignment]
        fetch_brief_by_membership=_fetch_brief,
        best_payment_for_membership=_best_payment,
        deep_get=_deep_get,
        extract_discord_id_from_connected=lambda s: 0,
    )
    embeds = await whop_dispute_cases.preview_case_embeds_from_api(
        membership_id=membership_id,
        dispute_id=dispute_id,
    )
    if not embeds:
        print("No previews built (check membership_id / dispute_id / API access).", file=sys.stderr)
        return 1
    for i, emb in enumerate(embeds, 1):
        print(f"\n--- embed {i}: {emb.title} ---")
        for f in emb.fields:
            print(f"  {f.name}: {f.value}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Preview Whop dispute/resolution case embed fields via API.")
    ap.add_argument("--membership-id", default="", help="Whop membership id (mem_...)")
    ap.add_argument("--dispute-id", default="", help="Whop dispute id (dspt_...)")
    args = ap.parse_args()
    return asyncio.run(_run(membership_id=str(args.membership_id or ""), dispute_id=str(args.dispute_id or "")))


if __name__ == "__main__":
    raise SystemExit(main())
