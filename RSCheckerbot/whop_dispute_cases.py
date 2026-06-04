"""Canonical Whop dispute + resolution per-case Discord channels (webhook-driven).

Not part of support_tickets CRM (billing/cancellation/free_pass). One channel per dspt_* id;
category switches between dispute_case_category_id and resolution_case_category_id by status.
"""
from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

import discord

LogFn = Callable[[str], Awaitable[None]]
EnsureChannelFn = Callable[..., Awaitable[discord.TextChannel | None]]
FetchBriefFn = Callable[[str], Awaitable[dict]]
BestPaymentFn = Callable[..., Awaitable[dict]]
DeepGetFn = Callable[[object, str], object]

_BOT: discord.Client | None = None
_API = None
_LOG: LogFn | None = None
_ENSURE_CHANNEL: EnsureChannelFn | None = None
_FETCH_BRIEF: FetchBriefFn | None = None
_BEST_PAYMENT: BestPaymentFn | None = None
_DEEP_GET: DeepGetFn | None = None
_EXTRACT_DISCORD_ID: Callable[[str], int] | None = None

_DISPUTE_CAT_ID = 0
_RESOLUTION_CAT_ID = 0
_COMPANY_ID = ""


@dataclass
class DisputeWebhookCaseParams:
    issue_override: str = ""
    case_key_override: str = ""
    extra_topic: str = ""
    always_post: bool = False
    extra_fields: tuple[tuple[str, str], ...] = ()
    payment_id: str = ""
    dispute_id: str = ""


def initialize(
    *,
    bot: discord.Client,
    whop_api_client,
    dispute_category_id: int,
    resolution_category_id: int,
    company_id: str,
    ensure_channel: EnsureChannelFn,
    fetch_brief_by_membership: FetchBriefFn,
    best_payment_for_membership: BestPaymentFn,
    deep_get: DeepGetFn,
    extract_discord_id_from_connected: Callable[[str], int],
    log_func: LogFn | None = None,
) -> None:
    global _BOT, _API, _LOG, _ENSURE_CHANNEL, _FETCH_BRIEF, _BEST_PAYMENT
    global _DEEP_GET, _EXTRACT_DISCORD_ID, _DISPUTE_CAT_ID, _RESOLUTION_CAT_ID, _COMPANY_ID
    _BOT = bot
    _API = whop_api_client
    _LOG = log_func
    _ENSURE_CHANNEL = ensure_channel
    _FETCH_BRIEF = fetch_brief_by_membership
    _BEST_PAYMENT = best_payment_for_membership
    _DEEP_GET = deep_get
    _EXTRACT_DISCORD_ID = extract_discord_id_from_connected
    _DISPUTE_CAT_ID = int(dispute_category_id or 0)
    _RESOLUTION_CAT_ID = int(resolution_category_id or 0)
    _COMPANY_ID = str(company_id or "").strip()


def bucket_for_dispute_status(status: str) -> str:
    s = str(status or "").strip().lower()
    if not s:
        return "dispute"
    if any(k in s for k in ("won", "lost", "resolved", "closed", "settled", "completed")):
        return "resolution"
    return "dispute"


def payment_issue_bucket_from_payment(p: dict) -> str:
    if not isinstance(p, dict) or not p:
        return ""
    txt = " ".join(
        [
            str(p.get("status") or ""),
            str(p.get("substatus") or ""),
            str(p.get("billing_reason") or ""),
            str(p.get("failure_message") or ""),
            str(p.get("reason") or ""),
            str(p.get("note") or ""),
        ]
    ).strip().lower()
    if any(k in txt for k in ("dispute", "disputed", "chargeback", "under review", "under_review")):
        return "dispute"
    if any(k in txt for k in ("resolution", "resolved", "won", "lost")):
        return "resolution"
    return ""


def params_from_dispute_webhook(payload: dict, evt_l: str) -> DisputeWebhookCaseParams:
    """Build case-open kwargs from dispute.created / dispute.updated webhook payload."""
    if not _DEEP_GET:
        return DisputeWebhookCaseParams()
    el = str(evt_l or "").strip().lower()
    if not el.startswith("dispute."):
        return DisputeWebhookCaseParams()
    dspt = str(_DEEP_GET(payload, "data.id") or "").strip()
    if not dspt.startswith("dspt_"):
        return DisputeWebhookCaseParams()
    dstatus = str(_DEEP_GET(payload, "data.status") or "").strip()
    dreason = str(_DEEP_GET(payload, "data.reason") or "").strip()
    dby = str(_DEEP_GET(payload, "data.needs_response_by") or "").strip()
    damt = str(_DEEP_GET(payload, "data.amount") or "").strip()
    dcur = str(_DEEP_GET(payload, "data.currency") or "").strip()
    pay_id = str(_DEEP_GET(payload, "data.payment.id") or _DEEP_GET(payload, "data.payment") or "").strip()
    if pay_id and not pay_id.startswith("pay_"):
        pay_id = ""
    issue = bucket_for_dispute_status(dstatus)
    extra = [
        ("Dispute ID", dspt),
        ("Dispute status", dstatus),
        ("Reason", dreason),
        ("Needs response by", dby),
        ("Amount", (f"{damt} {dcur}".strip() if (damt or dcur) else "")),
        ("Event", el),
    ]
    if issue == "resolution":
        extra.append(("Case phase", "Resolved / closed (moved to resolution category)"))
    return DisputeWebhookCaseParams(
        issue_override=issue,
        case_key_override=f"rschecker_whop_case:dspt={dspt}",
        extra_topic=f"dspt={dspt}\nstatus={dstatus or '—'}\nreason={dreason or '—'}",
        always_post=True,
        extra_fields=tuple(extra),
        payment_id=pay_id,
        dispute_id=dspt,
    )


def _payment_id_any(p: dict) -> str:
    try:
        pid = str(p.get("id") or p.get("payment_id") or "").strip()
        if isinstance(p.get("payment"), dict):
            pid = str(p["payment"].get("id") or p["payment"].get("payment_id") or "").strip() or pid
        return pid
    except Exception:
        return ""


def _user_dashboard_url(whop_user_id: str) -> str:
    uid = str(whop_user_id or "").strip()
    cid = str(_COMPANY_ID or "").strip()
    if not uid.startswith("user_") or not cid.startswith("biz_"):
        return ""
    return f"https://whop.com/dashboard/{cid}/users/{uid}/"


def _fmt_pay_line(p: dict) -> str:
    parts = [
        str(p.get("status") or "").strip(),
        str(p.get("substatus") or "").strip(),
        str(p.get("billing_reason") or "").strip(),
    ]
    paid = str(p.get("paid_at") or p.get("created_at") or "").strip()
    if paid:
        parts.append(f"paid_at={paid}")
    pid = _payment_id_any(p)
    if pid:
        parts.append(f"id={pid}")
    return " ".join([x for x in parts if x]).strip()


async def _resolve_payment(
    *,
    membership_id: str,
    payment_id: str,
    dispute_id: str,
) -> dict:
    pid = str(payment_id or "").strip()
    if pid.startswith("pay_") and _API and hasattr(_API, "get_payment_by_id"):
        with suppress(Exception):
            p = await _API.get_payment_by_id(pid)  # type: ignore[attr-defined]
            if isinstance(p, dict) and p:
                return p
    dspt = str(dispute_id or "").strip()
    if dspt.startswith("dspt_") and _API and hasattr(_API, "get_dispute_by_id"):
        with suppress(Exception):
            ds = await _API.get_dispute_by_id(dspt)  # type: ignore[attr-defined]
            if isinstance(ds, dict):
                p_ref = ds.get("payment")
                if isinstance(p_ref, dict):
                    return p_ref
                p2 = str(ds.get("payment_id") or "").strip()
                if p2.startswith("pay_") and hasattr(_API, "get_payment_by_id"):
                    p = await _API.get_payment_by_id(p2)  # type: ignore[attr-defined]
                    if isinstance(p, dict) and p:
                        return p
    if _BEST_PAYMENT:
        with suppress(Exception):
            p = await _BEST_PAYMENT(str(membership_id or "").strip(), limit=25)
            if isinstance(p, dict) and p:
                return p
    return {}


def build_case_embed(
    *,
    issue: str,
    member_obj: discord.Member | None,
    brief: dict,
    membership_id: str,
    payment: dict,
    extra_fields: list[tuple[str, str]] | None = None,
    preview: bool = False,
) -> discord.Embed:
    iss = str(issue or "").strip().lower()
    title = "⚠️ Dispute Case" if iss == "dispute" else "🟡 Resolution Case"
    if preview:
        title = f"[PREVIEW] {title}"
    color = 0xED4245 if iss == "dispute" else 0xFEE75C
    e = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    b = brief if isinstance(brief, dict) else {}
    mname = str(getattr(member_obj, "display_name", "") or "").strip() if member_obj else ""
    if not mname:
        mname = str(b.get("user_name") or "").strip() or "—"
    e.add_field(name="Member", value=mname[:1024], inline=True)
    did_i = int(member_obj.id) if member_obj else 0
    if did_i <= 0 and _EXTRACT_DISCORD_ID:
        did_i = int(_EXTRACT_DISCORD_ID(str(b.get("connected_discord") or "")) or 0)
    e.add_field(name="Discord ID", value=(f"`{did_i}`" if did_i else "—"), inline=True)
    e.add_field(name="Membership ID", value=str(membership_id or "—")[:1024], inline=False)
    e.add_field(name="Membership", value=str(b.get("product") or "—")[:1024], inline=True)
    e.add_field(name="Whop status", value=str(b.get("status") or "—")[:1024], inline=True)
    cape = str(b.get("cancel_at_period_end") or "").strip()
    if cape:
        e.add_field(name="Cancel at period end", value=cape[:1024], inline=True)
    rend = str(b.get("renewal_end") or b.get("renewal_end_display") or "").strip()
    if rend and rend != "—":
        e.add_field(name="Access ends on", value=rend[:1024], inline=True)
    if isinstance(payment, dict) and payment:
        pay_line = _fmt_pay_line(payment)
        if pay_line:
            e.add_field(name="Disputed payment", value=pay_line[:1024], inline=False)
    dash = str(b.get("dashboard_url") or "").strip()
    if dash and dash != "—":
        e.add_field(name="Membership dashboard", value=dash[:1024], inline=False)
    uid = str(b.get("whop_user_id") or "").strip()
    uurl = _user_dashboard_url(uid)
    if uurl:
        e.add_field(name="User dashboard", value=uurl[:1024], inline=False)
    email = str(b.get("email") or "").strip()
    if email:
        e.add_field(name="Email", value=email[:1024], inline=False)
    if isinstance(extra_fields, list):
        for k, v in extra_fields:
            kk = str(k or "").strip()
            vv = str(v or "").strip()
            if kk and vv:
                e.add_field(name=kk[:256], value=vv[:1024], inline=False)
    e.set_footer(text="RSCheckerbot • Whop dispute/resolution case")
    return e


async def maybe_open_case(
    *,
    guild: discord.Guild,
    membership_id: str,
    updated_at: str,
    brief: dict,
    cases: dict,
    discord_id: int = 0,
    member_obj: discord.Member | None = None,
    params: DisputeWebhookCaseParams | None = None,
    pay_override: dict | None = None,
) -> None:
    """Open or update a per-case channel (dispute or resolution category)."""
    if not _BOT or not _ENSURE_CHANNEL:
        return
    if int(_DISPUTE_CAT_ID or 0) <= 0 and int(_RESOLUTION_CAT_ID or 0) <= 0:
        return
    mid_s = str(membership_id or "").strip()
    if not mid_s:
        return

    p = params if isinstance(params, DisputeWebhookCaseParams) else DisputeWebhookCaseParams()
    pay = pay_override if isinstance(pay_override, dict) and pay_override else None
    if pay is None:
        pay = await _resolve_payment(
            membership_id=mid_s,
            payment_id=str(p.payment_id or ""),
            dispute_id=str(p.dispute_id or ""),
        )
    issue = str(p.issue_override or "").strip().lower() or payment_issue_bucket_from_payment(pay if isinstance(pay, dict) else {})
    if issue not in {"dispute", "resolution"}:
        return

    cat_id = int(_DISPUTE_CAT_ID) if issue == "dispute" else int(_RESOLUTION_CAT_ID)
    if cat_id <= 0:
        return
    pid = _payment_id_any(pay) if isinstance(pay, dict) else ""
    key = str(p.case_key_override or "").strip() or f"rschecker_whop_case:{issue}:mid={mid_s}:pid={pid or updated_at or 'unknown'}"

    did_i = int(discord_id or 0)
    if did_i <= 0 and _EXTRACT_DISCORD_ID:
        did_i = int(_EXTRACT_DISCORD_ID(str((brief or {}).get("connected_discord") or "")) or 0)

    suffix = (pid[-6:] if pid else mid_s[-6:]).lower()
    ch_name = f"{issue}-{suffix}"
    topic = (
        f"rschecker_whop_case issue={issue}\n"
        f"mid={mid_s}\n"
        f"pid={pid or '—'}\n"
        f"did={did_i or '—'}\n"
        f"email={str((brief or {}).get('email') or '—').strip()}\n"
        f"product={str((brief or {}).get('product') or '—').strip()}\n"
    )
    if p.extra_topic:
        topic = (topic + "\n" + str(p.extra_topic).strip()).strip()

    case_ch = await _ENSURE_CHANNEL(
        guild=guild,
        category_id=cat_id,
        case_key=key,
        channel_name=ch_name,
        topic=topic,
    )
    if not isinstance(case_ch, discord.TextChannel):
        return

    first_seen = key not in cases
    cases[key] = int(case_ch.id)

    extra_list = list(p.extra_fields) if p.extra_fields else []
    if first_seen or p.always_post:
        with suppress(Exception):
            ecase = build_case_embed(
                issue=issue,
                member_obj=member_obj,
                brief=brief if isinstance(brief, dict) else {},
                membership_id=mid_s,
                payment=pay if isinstance(pay, dict) else {},
                extra_fields=extra_list,
            )
            mention = f"<@{int(member_obj.id)}>" if member_obj else ""
            await case_ch.send(
                content=mention,
                embed=ecase,
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                silent=True,
            )
    if first_seen and _LOG:
        with suppress(Exception):
            await _LOG(f"[Whop Case] opened issue={issue} mid={mid_s} ch=#{case_ch.name} ({case_ch.id})")


async def preview_case_embeds_from_api(*, membership_id: str = "", dispute_id: str = "") -> list[discord.Embed]:
    """Build dispute + resolution preview embeds using live Whop API (no Discord channels)."""
    out: list[discord.Embed] = []
    if not _API or not _FETCH_BRIEF:
        return out
    dspt_rec: dict = {}
    dspt = str(dispute_id or "").strip()
    if not dspt and hasattr(_API, "_request"):
        with suppress(Exception):
            cid = str(_COMPANY_ID or "").strip()
            if cid:
                resp = await _API._request("GET", "/disputes", params={"company_id": cid, "first": 1})  # type: ignore[attr-defined]
                data = resp.get("data") if isinstance(resp, dict) else None
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    dspt_rec = data[0]
                    dspt = str(dspt_rec.get("id") or "").strip()
    if dspt.startswith("dspt_") and hasattr(_API, "get_dispute_by_id"):
        with suppress(Exception):
            got = await _API.get_dispute_by_id(dspt)  # type: ignore[attr-defined]
            if isinstance(got, dict):
                dspt_rec = got
    mid_s = str(membership_id or "").strip()
    pay = await _resolve_payment(membership_id=mid_s, payment_id="", dispute_id=dspt)
    if not mid_s and isinstance(pay, dict):
        mid_s = str(pay.get("membership_id") or pay.get("membership") or "").strip()
        if isinstance(pay.get("membership"), dict):
            mid_s = str(pay["membership"].get("id") or "").strip() or mid_s
    if not mid_s:
        return out
    brief = {}
    with suppress(Exception):
        brief = await _FETCH_BRIEF(mid_s)
    if not isinstance(brief, dict):
        brief = {}
    dstatus = str(dspt_rec.get("status") or "needs_response").strip()
    dreason = str(dspt_rec.get("reason") or "").strip()
    dby = str(dspt_rec.get("needs_response_by") or "").strip()
    damt = str(dspt_rec.get("amount") or "").strip()
    dcur = str(dspt_rec.get("currency") or "").strip()
    base_extra = [
        ("Dispute ID", dspt or "—"),
        ("Dispute status", dstatus),
        ("Reason", dreason),
        ("Needs response by", dby),
        ("Amount", f"{damt} {dcur}".strip()),
    ]
    out.append(
        build_case_embed(
            issue="dispute",
            member_obj=None,
            brief=brief,
            membership_id=mid_s,
            payment=pay,
            extra_fields=base_extra + [("Preview note", "Sample open dispute — category: dispute")],
            preview=True,
        )
    )
    out.append(
        build_case_embed(
            issue="resolution",
            member_obj=None,
            brief=brief,
            membership_id=mid_s,
            payment=pay,
            extra_fields=base_extra
            + [
                ("Dispute status (resolved sample)", "won"),
                ("Preview note", "Same dspt_* after terminal status — category: resolution"),
            ],
            preview=True,
        )
    )
    return out
