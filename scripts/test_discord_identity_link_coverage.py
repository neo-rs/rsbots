#!/usr/bin/env python3
"""
Check Discord-side linking coverage:
- Load a sheet-export CSV (Name, Phone, Email, ...)
- Load RSCheckerbot/whop_identity_cache.json (email -> discord_id)
- Report how many CSV emails can be linked to a Discord ID

Optionally: if WhopMembershipSync secrets exist, fetch a small sample member list from Whop
and demonstrate that Whop doesn't provide discord_id, but identity cache can.
"""

from __future__ import annotations

import asyncio
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Row:
    name: str
    email: str
    status: str


def _load_csv_rows(path: Path) -> list[Row]:
    rows: list[Row] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for rec in r:
            if not isinstance(rec, dict):
                continue
            email = str(rec.get("Email") or "").strip().lower()
            if not email or "@" not in email:
                continue
            rows.append(
                Row(
                    name=str(rec.get("Name") or "").strip(),
                    email=email,
                    status=str(rec.get("Status") or "").strip().lower(),
                )
            )
    return rows


def _load_identity_cache(path: Path) -> dict:
    try:
        obj = json.loads(path.read_text(encoding="utf-8") or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _get_cache_did(cache: dict, email: str) -> str:
    rec = cache.get(email)
    if not isinstance(rec, dict):
        return ""
    did = str(rec.get("discord_id") or "").strip()
    return did if did.isdigit() else ""


async def _demo_whop_for_emails(emails: list[str]) -> None:
    """
    Best-effort demo:
    - Fetch a small member batch from Whop (/members)
    - Show for any member whose email is in our target list whether Whop includes connected_accounts
    """
    cfg_path = REPO_ROOT / "WhopMembershipSync" / "config.json"
    sec_path = REPO_ROOT / "WhopMembershipSync" / "config.secrets.json"
    if not (cfg_path.exists() and sec_path.exists()):
        print("WhopMembershipSync config.secrets.json not found; skipping Whop demo.")
        return

    # RSCheckerbot isn't a package; add it to sys.path
    rsc_dir = REPO_ROOT / "RSCheckerbot"
    if str(rsc_dir) not in sys.path:
        sys.path.insert(0, str(rsc_dir))

    from whop_api_client import WhopAPIClient  # type: ignore

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    sec = json.loads(sec_path.read_text(encoding="utf-8"))
    cfg.setdefault("whop_api", {}).update((sec.get("whop_api") if isinstance(sec, dict) else {}) or {})

    wh = cfg.get("whop_api") if isinstance(cfg.get("whop_api"), dict) else {}
    api_key = str(wh.get("api_key") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    company_id = str(wh.get("company_id") or "").strip()
    products = cfg.get("products") if isinstance(cfg.get("products"), list) else []
    product_id = str((products[0] or {}).get("product_id") or "").strip() if products else ""
    if not (api_key and company_id and product_id):
        print("Missing whop_api api_key/company_id/product_id; skipping Whop demo.")
        return

    target = {e.lower() for e in emails if e and "@" in e}

    c = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)
    batch, _page = await c.list_members(first=100, params={"product_ids": [product_id]})
    hits = 0
    for m in batch:
        if not isinstance(m, dict):
            continue
        u = m.get("user") if isinstance(m.get("user"), dict) else {}
        email = str((u or {}).get("email") or "").strip().lower()
        if not email or email not in target:
            continue
        hits += 1
        ca = m.get("connected_accounts")
        has_ca = isinstance(ca, list) and len(ca) > 0
        print("--- Whop demo match ---")
        print("email:", email)
        print("member_id:", str(m.get("id") or "").strip())
        print("connected_accounts_present:", bool(has_ca))
        if hits >= 3:
            break
    if hits == 0:
        print("Whop demo: no matching emails found in first 100 /members results (this is normal).")


async def main() -> int:
    csv_path = REPO_ROOT / "Website Opt-Ins - Churned.csv"
    cache_path = REPO_ROOT / "RSCheckerbot" / "whop_identity_cache.json"
    if not csv_path.exists():
        print(f"Missing CSV: {csv_path}")
        return 2
    if not cache_path.exists():
        print(f"Missing identity cache: {cache_path}")
        return 2

    rows = _load_csv_rows(csv_path)
    cache = _load_identity_cache(cache_path)

    emails = sorted({r.email for r in rows})
    linked = []
    for em in emails:
        did = _get_cache_did(cache, em)
        if did:
            linked.append((em, did))

    print("CSV unique emails:", len(emails))
    print("Identity cache entries:", len(cache))
    print("Linked (email -> discord_id) hits:", len(linked))

    if linked:
        print()
        print("Sample linked hits (up to 10):")
        for em, did in linked[:10]:
            print("-", em, "->", did)

        # Optional Whop-side demo for a few linked emails
        await _demo_whop_for_emails([em for em, _did in linked[:10]])
    else:
        print("No linked emails found between this CSV and whop_identity_cache.json.")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

