#!/usr/bin/env python3
"""
Probe Total Spend sources:
- Whop memberships payload (best-effort extraction)
- RSCheckerbot/member_history.json (whop.last_summary.total_spent)
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WHOP_SYNC_DIR = REPO_ROOT / "WhopMembershipSync"
if str(WHOP_SYNC_DIR) not in sys.path:
    sys.path.insert(0, str(WHOP_SYNC_DIR))


def _load_cfg() -> dict:
    cfg = json.loads((REPO_ROOT / "WhopMembershipSync" / "config.json").read_text(encoding="utf-8"))
    sec = json.loads((REPO_ROOT / "WhopMembershipSync" / "config.secrets.json").read_text(encoding="utf-8"))
    cfg.setdefault("whop_api", {}).update(sec.get("whop_api", {}) if isinstance(sec, dict) else {})
    if isinstance(sec.get("google_service_account_json"), dict):
        cfg["google_service_account_json"] = sec["google_service_account_json"]
    return cfg


async def main() -> int:
    cfg = _load_cfg()
    # RSCheckerbot isn't a package; add to sys.path
    rsc = REPO_ROOT / "RSCheckerbot"
    if str(rsc) not in sys.path:
        sys.path.insert(0, str(rsc))

    from whop_api_client import WhopAPIClient  # type: ignore
    # WhopMembershipSync isn't a package; import module directly from its folder
    from whop_sheets_sync import WhopSheetsSync, _extract_total_spend  # type: ignore

    wh = cfg.get("whop_api") if isinstance(cfg.get("whop_api"), dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    products = cfg.get("products") if isinstance(cfg.get("products"), list) else []
    product_id = str((products[0] or {}).get("product_id") or "").strip() if products else ""
    if not (api_key and company_id and product_id):
        print("Missing whop_api api_key/company_id/product_id")
        return 2

    c = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)
    ws = WhopSheetsSync(cfg)

    batch, _page = await c.list_memberships(first=30, params={"product_ids": [product_id], "statuses[]": ["active"]})
    print("memberships_fetched:", len(batch))
    shown = 0
    for m in batch:
        if not isinstance(m, dict):
            continue
        user = m.get("user") if isinstance(m.get("user"), dict) else {}
        email = str((user or {}).get("email") or "").strip().lower()
        if not email:
            continue
        spend_whop = _extract_total_spend(m, None)
        did = ws._enrich_discord_id(email=email, current_discord_id="") if email else ""
        spend_mh = ws._enrich_total_spend_from_member_history(discord_id=did, current_total_spend="")
        if spend_whop or spend_mh:
            print("---")
            print("email:", email)
            print("discord_id(from cache):", did or "-")
            print("total_spend(whop_extract):", spend_whop or "-")
            print("total_spent(member_history):", spend_mh or "-")
            shown += 1
        if shown >= 5:
            break
    if shown == 0:
        print("No spend found in sample (whop_extract or member_history).")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

