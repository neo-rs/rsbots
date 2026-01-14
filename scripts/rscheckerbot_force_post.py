#!/usr/bin/env python3
"""
RSCheckerbot Force Post (production-safe)
---------------------------------------
Force-post a staff alert for a given Discord member into:
- member-status-logs (detailed card)
- a case channel (minimal card)

This is used for:
- verifying routing/formatting without waiting for real triggers
- producing a structured JSON trace of API actions/outcomes

No secrets are embedded; this script reads server-only secrets on Oracle.

NOTE: This script intentionally reuses RSCheckerbot's canonical embed builders and Whop brief fetch
to avoid duplicate formatting logic.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
import urllib.request
import urllib.error


REPO_ROOT = Path(__file__).resolve().parents[1]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


from RSCheckerbot.staff_embeds import kv_block
from RSCheckerbot.whop_brief import fetch_whop_brief


def _dreq(token: str, method: str, path: str, body: object | None = None) -> Tuple[int, object]:
    api = "https://discord.com/api/v10"
    headers = {
        "Authorization": "Bot " + token,
        "User-Agent": "rscheckerbot-forcepost/1.0",
        "Content-Type": "application/json",
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(api + path, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            try:
                return resp.status, json.loads(raw) if raw else {}
            except Exception:
                return resp.status, {"raw": raw.decode("utf-8", "ignore")[:500]}
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else {}
        except Exception:
            return e.code, {"raw": raw.decode("utf-8", "ignore")[:500]}


async def _fetch_whop_brief(cfg: dict, membership_id: str) -> dict:
    if not membership_id:
        return {}
    from RSCheckerbot.whop_api_client import WhopAPIClient  # type: ignore
    wh = cfg.get("whop_api") if isinstance(cfg, dict) else {}
    if not isinstance(wh, dict):
        return {}
    api_key = str(wh.get("api_key") or "").strip()
    company_id = str(wh.get("company_id") or "").strip()
    base_url = str(wh.get("base_url") or "https://api.whop.com/api/v1").strip()
    if not api_key or not company_id:
        return {}
    client = WhopAPIClient(api_key=api_key, base_url=base_url, company_id=company_id)
    return await fetch_whop_brief(client, membership_id, enable_enrichment=bool(wh.get("enable_enrichment", True)))


def _resolve_membership_id(discord_id: int) -> str:
    # Primary: whop_discord_link.json
    p = REPO_ROOT / "RSCheckerbot" / "whop_discord_link.json"
    if p.exists():
        try:
            db = json.loads(p.read_text(encoding="utf-8") or "{}")
            rec = (db.get("by_discord_id") or {}).get(str(discord_id))
            if isinstance(rec, dict):
                mid = str(rec.get("membership_id") or "").strip()
                if mid:
                    return mid
        except Exception:
            pass

    # Fallback: member_history.json (whop.last_membership_id)
    p2 = REPO_ROOT / "RSCheckerbot" / "member_history.json"
    if p2.exists():
        try:
            db = json.loads(p2.read_text(encoding="utf-8") or "{}")
            rec = db.get(str(discord_id))
            wh = rec.get("whop") if isinstance(rec, dict) else None
            if isinstance(wh, dict):
                mid = str(wh.get("last_membership_id") or "").strip()
                if mid:
                    return mid
        except Exception:
            pass

    return ""


def _access_roles_from_discord_member(cfg: dict, member_payload: dict, role_name: dict) -> str:
    dm = cfg.get("dm_sequence") if isinstance(cfg, dict) else {}
    relevant: set[str] = set()
    if isinstance(dm, dict):
        for k in ["role_cancel_a", "role_cancel_b", "welcome_role_id", "role_trigger", "former_member_role"]:
            v = dm.get(k)
            if isinstance(v, int):
                relevant.add(str(v))
            elif isinstance(v, str) and v.strip().isdigit():
                relevant.add(v.strip())
        for rid in (dm.get("roles_to_check") or []):
            if str(rid).strip().isdigit():
                relevant.add(str(int(str(rid).strip())))

    member_role_ids = member_payload.get("roles") or []
    names: list[str] = []
    for rid in member_role_ids:
        sid = str(rid)
        if sid not in relevant:
            continue
        nm = str(role_name.get(sid) or "").strip()
        if nm and nm not in names:
            names.append(nm)
    return ", ".join(names) if names else "—"


def _embed_detailed(title: str, member_mention: str, access_roles: str, brief: dict) -> dict:
    return {
        "title": title,
        "color": 0xED4245,
        "fields": [
            {"name": "Member Info", "value": kv_block([("member", member_mention)]), "inline": False},
            {"name": "Discord Info", "value": kv_block([("access_roles", access_roles)]), "inline": False},
            {
                "name": "Payment Info",
                "value": kv_block(
                    [
                        ("status", brief.get("status")),
                        ("product", brief.get("product")),
                        ("member_since", brief.get("member_since")),
                        ("trial_end", brief.get("trial_end")),
                        ("renewal_start", brief.get("renewal_start")),
                        ("renewal_end", brief.get("renewal_end")),
                        ("cancel_at_period_end", brief.get("cancel_at_period_end")),
                        ("is_first_membership", brief.get("is_first_membership")),
                        ("last_payment_method", brief.get("last_payment_method")),
                        ("last_payment_type", brief.get("last_payment_type")),
                        ("last_payment_failure", brief.get("last_payment_failure")),
                    ],
                    keep_blank_keys={"is_first_membership"},
                ),
                "inline": False,
            },
        ],
        "footer": {"text": "RSCheckerbot • Member Status Tracking"},
    }


def _embed_minimal(title: str, member_mention: str, access_roles: str, brief: dict) -> dict:
    return {
        "title": title,
        "color": 0xED4245,
        "fields": [
            {
                "name": "Member Info",
                "value": kv_block(
                    [
                        ("member", member_mention),
                        ("product", brief.get("product")),
                        ("member_since", brief.get("member_since")),
                        ("renewal_start", brief.get("renewal_start")),
                        ("renewal_end", brief.get("renewal_end")),
                        ("last_payment_failure", brief.get("last_payment_failure")),
                    ]
                ),
                "inline": False,
            },
            {"name": "Discord Info", "value": kv_block([("access_roles", access_roles)]), "inline": False},
        ],
        "footer": {"text": "RSCheckerbot"},
    }


async def run(args: argparse.Namespace) -> int:
    # Load merged config/secrets from Oracle live tree
    import sys
    sys.path.insert(0, str(REPO_ROOT))
    from mirror_world_config import load_config_with_secrets

    cfg, _, _ = load_config_with_secrets(REPO_ROOT / "RSCheckerbot")
    token = str(cfg.get("bot_token") or "").strip()
    if not token:
        raise SystemExit("Missing bot_token in RSCheckerbot/config.secrets.json")

    discord_id = int(args.discord_id)
    guild_id = int(args.guild_id)

    trace: Dict[str, Any] = {
        "started_at": _utc_iso(),
        "discord_id": discord_id,
        "guild_id": guild_id,
        "membership_id": "",
        "sent": [],
        "errors": [],
    }

    membership_id = _resolve_membership_id(discord_id)
    trace["membership_id"] = membership_id

    # Fetch discord member + roles
    st, member = _dreq(token, "GET", f"/guilds/{guild_id}/members/{discord_id}")
    trace["member_fetch"] = {"status": st}
    if st != 200 or not isinstance(member, dict):
        trace["errors"].append({"where": "fetch_member", "status": st, "resp": member})
        _write_trace(args.trace_out, trace)
        return 2

    st, roles = _dreq(token, "GET", f"/guilds/{guild_id}/roles")
    if st != 200 or not isinstance(roles, list):
        role_name = {}
    else:
        role_name = {str(r.get("id")): r.get("name") for r in roles if isinstance(r, dict)}

    access_roles = _access_roles_from_discord_member(cfg, member, role_name)
    member_mention = f"<@{discord_id}>"

    brief = {}
    try:
        brief = await _fetch_whop_brief(cfg, membership_id)
    except Exception as e:
        trace["errors"].append({"where": "whop_brief", "error": repr(e)})
        brief = {}
    trace["whop_brief"] = brief

    title = args.title
    detailed = _embed_detailed(title, member_mention, access_roles, brief)
    minimal = _embed_minimal(title, member_mention, access_roles, brief)

    # Send embeds
    for channel_id, embed in [
        (int(args.member_status_channel_id), detailed),
        (int(args.case_channel_id), minimal),
    ]:
        st, resp = _dreq(
            token,
            "POST",
            f"/channels/{channel_id}/messages",
            {"embeds": [embed], "allowed_mentions": {"parse": []}},
        )
        trace["sent"].append({"channel_id": channel_id, "status": st, "resp": resp})

    trace["finished_at"] = _utc_iso()
    _write_trace(args.trace_out, trace)
    print(json.dumps({"ok": True, "trace_out": args.trace_out, "sent": trace["sent"]}, indent=2))
    return 0


def _write_trace(path: str, trace: dict) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        # redact anything that looks like a token (shouldn't be present anyway)
        txt = json.dumps(trace, indent=2, ensure_ascii=False)
        p.write_text(txt + "\n", encoding="utf-8")
    except Exception:
        pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--guild-id", type=int, default=876528050081251379)
    ap.add_argument("--discord-id", type=int, required=True)
    ap.add_argument("--member-status-channel-id", type=int, required=True)
    ap.add_argument("--case-channel-id", type=int, required=True)
    ap.add_argument("--title", type=str, default="❌ Payment Failed — Action Needed")
    ap.add_argument("--trace-out", type=str, default=f"/tmp/rscheckerbot_forcepost_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json")
    args = ap.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())

