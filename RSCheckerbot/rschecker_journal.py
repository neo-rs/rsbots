"""
RSCheckerbot — flow labels + optional journal Discord channels.

Canonical flow keys are stable strings used in config (journal_logs.channel_names)
and in code (log_other(..., flow=rj.WHOP_WEBHOOK)).

- flow_labels_enabled: add FLOW=<key> to embed footers / message prefix (operator clarity).
- route_to_journal_channels: when True, log_other posts to #journal-rscheckerbot-<flow>
  if that flow has a channel_names entry and the channel exists or can be created.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import discord

# --- Flow keys (use these from main / handlers; also JSON config keys) ---
GENERAL = "general"
STARTUP = "startup"
HTTP = "http"
WHOP_WEBHOOK = "whop_webhook"
WHOP_DISCORD = "whop_discord"
WHOP_API = "whop_api"
WHOP_SYNC = "whop_sync"
PERSIST = "persist"
STAFF_EMBEDS = "staff_embeds"
TICKETS = "tickets"
DISCORD_DM = "discord_dm"
ROLES = "roles"
ROLE_AUDIT = "role_audit"
REPORTING = "reporting"
MEMBER_HISTORY = "member_history"
SUPPORT_SWEEP = "support_sweep"
INVITE_GHL = "invite_ghl"
CHANNEL_LIMITS = "channel_limits"
WHOP_TEXT = "whop_text"
# Finer splits (RSAdminBot-style journal streams; map each to #journal-rscheckerbot-* when routing on)
WHOP_HTTP_INBOUND = "whop_http_inbound"
WHOP_HTTP_PROCESS = "whop_http_process"
WHOP_LOGS_SCAN = "whop_logs_scan"
MEMBER_STATUS_CRM = "member_status_crm"

# Human titles for terminal / ELI5 lines (short)
FLOW_TITLE: dict[str, str] = {
    GENERAL: "General / bot-logs",
    STARTUP: "Boot & health",
    HTTP: "HTTP server (invite API, Whop webhook receiver)",
    WHOP_WEBHOOK: "Whop Developer webhook (HTTP)",
    WHOP_DISCORD: "Whop messages in Discord (workflow / native)",
    WHOP_API: "Whop REST API calls",
    WHOP_SYNC: "Whop membership sync job",
    PERSIST: "Saving JSON / disk state",
    STAFF_EMBEDS: "Staff cards (member-status / payment / cancel)",
    TICKETS: "Support tickets (open/close/sweep)",
    DISCORD_DM: "DM sequence (day 1..7)",
    ROLES: "Discord role adds/removes (bot-logs mirror)",
    ROLE_AUDIT: "Discord role audit channel",
    REPORTING: "Weekly report / cancel reminders / reporting store",
    MEMBER_HISTORY: "Member history ingest / backfill",
    SUPPORT_SWEEP: "Support ticket sweeper / backfill",
    INVITE_GHL: "Invites + GHL / create-invite HTTP",
    CHANNEL_LIMITS: "Channel limits monitor / slash",
    WHOP_TEXT: "Whop text channel (legacy whop-logs)",
    WHOP_HTTP_INBOUND: "Whop HTTP webhook — POST receipt & payload shape",
    WHOP_HTTP_PROCESS: "Whop HTTP webhook — classify, API brief, staff card, CRM hints",
    WHOP_LOGS_SCAN: "Whop native #whop-logs history scan (identity / Discord ID resolve)",
    MEMBER_STATUS_CRM: "Member-status channel — ticket open/close automation",
}


def normalize_flow(flow: str | None) -> str:
    s = (flow or GENERAL).strip().lower()
    return s if s else GENERAL


def flow_prefix(flow: str | None) -> str:
    """Terminal / journald-friendly single-line prefix."""
    k = normalize_flow(flow).upper().replace("-", "_")
    return f"[RSCheckerbot][{k}]"


def flow_title(flow: str | None) -> str:
    return FLOW_TITLE.get(normalize_flow(flow), FLOW_TITLE[GENERAL])


def prefix_message(text: str, flow: str | None) -> str:
    """Prefix plain-text Discord or log lines when not using an embed."""
    body = str(text or "").strip()
    if not body:
        return flow_prefix(flow)
    return f"{flow_prefix(flow)} {body}"[:1900]


def parse_journal_config(root: dict[str, Any] | None) -> dict[str, Any]:
    raw = (root or {}).get("journal_logs") if isinstance(root, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    names_in = raw.get("channel_names") if isinstance(raw.get("channel_names"), dict) else {}
    channel_names: dict[str, str] = {}
    for k, v in names_in.items():
        ks = str(k or "").strip().lower()
        vs = str(v or "").strip()
        if ks and vs:
            channel_names[ks] = vs
    try:
        category_id = int(raw.get("category_id") or 0)
    except Exception:
        category_id = 0
    category_id = max(0, category_id)
    return {
        "flow_labels_enabled": bool(raw.get("flow_labels_enabled", True)),
        "route_to_journal_channels": bool(raw.get("route_to_journal_channels", False)),
        "mirror_tlog_to_discord": bool(raw.get("mirror_tlog_to_discord", True)),
        "category_id": category_id,
        "channel_names": channel_names,
    }


def journal_channel_name_for_flow(cfg: dict[str, Any], flow: str | None) -> str:
    key = normalize_flow(flow)
    names = cfg.get("channel_names") if isinstance(cfg, dict) else None
    if not isinstance(names, dict):
        return ""
    return str(names.get(key) or "").strip()


def enrich_embed_footer(
    embed: "discord.Embed",
    flow: str | None,
    *,
    channel_name: str = "",
    labels_enabled: bool = True,
) -> None:
    """Append FLOW=<key> to footer; preserve existing footer text when possible."""
    if not labels_enabled:
        return
    fk = normalize_flow(flow)
    flow_part = f"FLOW={fk}"
    try:
        old = str(getattr(getattr(embed, "footer", None), "text", None) or "").strip()
    except Exception:
        old = ""
    if flow_part in old:
        return
    if not old:
        parts = ["RSCheckerbot", flow_part]
        tail = str(channel_name or "").strip()
        if tail:
            parts.append(f"#{tail}")
        embed.set_footer(text=" • ".join(parts)[:2048])
        return
    embed.set_footer(text=(old + " • " + flow_part)[:2048])


def tlog(logger: Any, level: str, flow: str | None, msg: str, *args: object) -> None:
    """Structured python logging: one prefix for grep/journald filters (supports %-format args)."""
    prefix = flow_prefix(flow) + " "
    lv = str(level or "info").lower()
    if lv == "warning":
        logger.warning(prefix + msg, *args)
    elif lv == "error":
        logger.error(prefix + msg, *args)
    elif lv == "debug":
        logger.debug(prefix + msg, *args)
    else:
        logger.info(prefix + msg, *args)
