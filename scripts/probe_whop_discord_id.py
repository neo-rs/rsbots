#!/usr/bin/env python3
"""
Probe Whop API for Discord linkage on member records.

This is a local-only diagnostic script meant to answer:
  - Does Whop return Discord IDs anywhere (e.g. connected_accounts)?
  - Can our extractor resolve a Discord ID from the member payload?
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RSC_DIR = REPO_ROOT / "RSCheckerbot"
if str(RSC_DIR) not in sys.path:
    sys.path.insert(0, str(RSC_DIR))

from whop_api_client import WhopAPIClient  # type: ignore
from rschecker_utils import extract_discord_id_from_whop_member_record  # type: ignore


CFG_DIR = REPO_ROOT / "WhopMembershipSync"


def _load_cfg() -> dict:
    cfg = json.loads((CFG_DIR / "config.json").read_text(encoding="utf-8"))
    sec = json.loads((CFG_DIR / "config.secrets.json").read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        cfg = {}
    if isinstance(sec, dict) and isinstance(sec.get("whop_api"), dict):
        cfg.setdefault("whop_api", {}).update(sec.get("whop_api", {}))
    return cfg


def _connected_account_types(member: dict) -> list[str]:
    ca = member.get("connected_accounts")
    if not isinstance(ca, list):
        return []
    out: list[str] = []
    for a in ca:
        if not isinstance(a, dict):
            continue
        t = str(a.get("type") or a.get("provider") or a.get("kind") or "").strip()
        if t:
            out.append(t)
    return out


async def main() -> int:
    cfg = _load_cfg()
    wh = cfg.get("whop_api") if isinstance(cfg.get("whop_api"), dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    products = cfg.get("products") if isinstance(cfg.get("products"), list) else []
    product_id = str((products[0] or {}).get("product_id") or "").strip() if products else ""

    if not (api_key and company_id and product_id):
        print("Missing api_key/company_id/product_id in config.")
        return 2

    c = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)

    async def _probe(title: str, params: dict) -> None:
        batch, _page = await c.list_members(first=25, params=params)
        print()
        print("===")
        print(title)
        print("fetched:", len(batch))
        print("sample up to 10")
        with_discord = 0
        with_connected = 0
        for m in (batch or [])[:10]:
            if not isinstance(m, dict):
                continue
            u = m.get("user") if isinstance(m.get("user"), dict) else {}
            email = str((u or {}).get("email") or "").strip()
            did = ""
            try:
                did = str(extract_discord_id_from_whop_member_record(m) or "").strip()
            except Exception:
                did = ""
            types = _connected_account_types(m)
            if types:
                with_connected += 1
            if did:
                with_discord += 1
            print("---")
            print("member_id:", str(m.get("id") or "").strip())
            print("email:", email)
            print("connected_account_types:", types)
            print("extract_discord_id:", did)
        print("---")
        print("samples_checked:", min(10, len(batch or [])))
        print("with_connected_accounts:", with_connected)
        print("with_discord_id_extracted:", with_discord)

    # 1) churned sample (same approach as WhopMembershipSync)
    await _probe(
        "CHURNED (most_recent_actions=churned, product scoped)",
        {"product_ids": [product_id], "most_recent_actions[]": ["churned"]},
    )

    # 2) general sample (product scoped, no action filter)
    await _probe(
        "GENERAL (no most_recent_actions filter, product scoped)",
        {"product_ids": [product_id]},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

