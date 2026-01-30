from __future__ import annotations

from contextlib import suppress
from datetime import datetime, timezone

import discord


def _member_avatar_url(user: discord.abc.User) -> str | None:
    """Best-effort avatar URL that works across discord.py versions and user types."""
    try:
        return str(user.display_avatar.url)
    except Exception:
        pass
    try:
        avatar = getattr(user, "avatar", None)
        if avatar:
            return str(avatar.url)
    except Exception:
        pass
    try:
        return str(user.default_avatar.url)
    except Exception:
        return None


def apply_member_header(embed: discord.Embed, user: discord.abc.User) -> None:
    """Apply author icon + thumbnail if an avatar URL is available."""
    url = _member_avatar_url(user)
    if not url:
        return
    with suppress(Exception):
        # Make the author name clickable without pinging.
        prof = f"https://discord.com/users/{int(getattr(user, 'id', 0) or 0)}"
        embed.set_author(name=str(user), url=prof, icon_url=url)
        embed.set_thumbnail(url=url)


_LABEL_OVERRIDES: dict[str, str] = {
    # Whop / membership
    "product": "Membership",
    "membership_id": "Membership ID",
    "member_since": "Member Since",
    "renewal_start": "Billing Period Started",
    "renewal_end": "Next Billing Date",
    "renewal_window": "Renewal Window",
    "trial_end": "Trial Ends",
    "trial_days": "Trial Days",
    "remaining_days": "Remaining Days",
    "dashboard_url": "Whop Dashboard",
    "checkout_url": "Checkout",
    "total_spent": "Total Spent",
    "mrr": "MRR",
    "customer_since": "Customer since",
    "connected_discord": "Connected Discord",
    "cancellation_reason": "Cancellation reason",
    "plan_is_renewal": "Plan Is Renewal",
    "promo": "Promo",
    "pricing": "Pricing",
    "last_success_paid_at": "Last Successful Payment",
    "last_payment_failure": "Payment Issue",
    "cancel_at_period_end": "Cancel At Period End",
    "is_first_membership": "First Membership",
    "last_payment_method": "Last Payment Method",
    "last_payment_type": "Last Payment Type",
    # Discord / staff
    "access_roles": "Current Roles",
    "account_created": "Discord Account Created",
    "roles_removed": "Roles Removed",
    # Member history (Discord-side)
    "ever_had_access_role": "Ever Had Access Role",
    "first_access": "First Access",
    "last_access": "Last Access",
    "ever_had_member_role": "Ever Had Member Role",
    "first_member_role": "First Member Role",
    "last_member_role": "Last Member Role",
}


def _human_label(key: str, *, label_overrides: dict[str, str] | None = None) -> str:
    k = str(key or "").strip()
    if not k:
        return ""
    if label_overrides and k in label_overrides:
        return str(label_overrides[k]).strip() or k
    if k in _LABEL_OVERRIDES:
        return _LABEL_OVERRIDES[k]
    # If already human-looking, keep as-is.
    if "_" not in k and "-" not in k:
        return k[:1].upper() + k[1:] if k else k
    # snake_case / kebab-case -> Title Case
    parts = [p for p in k.replace("-", "_").split("_") if p]
    return " ".join(p[:1].upper() + p[1:] for p in parts) if parts else k


def _kv_line(
    key: str,
    value: object,
    *,
    keep_blank: bool = False,
    label_overrides: dict[str, str] | None = None,
) -> str | None:
    """Format `Label: value` while hiding blanks by default."""
    raw = str(key or "").strip()
    label = _human_label(raw, label_overrides=label_overrides)
    if not label:
        return None
    if value is None:
        return f"{label}: —" if keep_blank else None
    s = str(value).strip()
    if not s or s == "—":
        return f"{label}: —" if keep_blank else None
    return f"{label}: {s}"


def kv_block(
    pairs: list[tuple[str, object]],
    *,
    keep_blank_keys: set[str] | None = None,
    label_overrides: dict[str, str] | None = None,
) -> str:
    keep = keep_blank_keys or set()
    lines: list[str] = []
    for k, v in pairs:
        line = _kv_line(k, v, keep_blank=(k in keep), label_overrides=label_overrides)
        if line:
            lines.append(line)
    return ("\n".join(lines)[:1024]) if lines else "—"


def _is_blank(v: object) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if (not s) or s == "—":
        return True
    low = s.lower()
    return low in {"n/a", "na", "none", "null"}

def _sanitize_value(v: object) -> str:
    """Normalize placeholders to a neutral dash for staff embeds.

    We never want user-visible placeholders like "Not linked yet" / "Linking…".
    """
    if v is None:
        return "—"
    s = str(v).strip()
    if not s or s == "—":
        return "—"
    low = s.lower()
    if low in {"n/a", "na", "none", "null"}:
        return "—"
    # Treat common linking placeholders as unknown (do not show them).
    if low.startswith("not linked yet"):
        return "—"
    if low.startswith("linking"):
        return "—"
    return s

def _add_field_force(embed: discord.Embed, name: str, value: object, *, inline: bool = True) -> None:
    """Always add a field, showing '—' for unknown."""
    v = _sanitize_value(value)
    embed.add_field(name=str(name)[:256], value=str(v)[:1024], inline=inline)


def _add_field(embed: discord.Embed, name: str, value: object, *, inline: bool = True) -> None:
    if _is_blank(value):
        return
    embed.add_field(name=str(name)[:256], value=str(value).strip()[:1024], inline=inline)


def _pairs_to_dict(pairs: list[tuple[str, object]] | None) -> dict[str, object]:
    out: dict[str, object] = {}
    for p in (pairs or []):
        if not (isinstance(p, tuple) and len(p) == 2):
            continue
        k, v = p
        ks = str(k or "").strip()
        if ks:
            out[ks] = v
    return out


def _human_value_for_field(key: str, value: object) -> tuple[str, object]:
    """Return (human_label, value) for a kv key."""
    return (_human_label(key), value)


def brief_payment_kv(brief: dict | None) -> list[tuple[str, object]]:
    b = brief if isinstance(brief, dict) else {}
    dash = b.get("dashboard_url")
    return [
        ("membership_id", b.get("membership_id")),
        ("status", b.get("status")),
        ("product", b.get("product")),
        ("member_since", b.get("member_since")),
        ("trial_end", b.get("trial_end")),
        ("trial_days", b.get("trial_days")),
        ("plan_is_renewal", b.get("plan_is_renewal")),
        ("promo", b.get("promo")),
        ("pricing", b.get("pricing")),
        ("renewal_start", b.get("renewal_start")),
        ("renewal_end", b.get("renewal_end")),
        ("renewal_window", b.get("renewal_window")),
        ("remaining_days", b.get("remaining_days")),
        ("dashboard_url", dash),
        ("checkout_url", b.get("checkout_url")),
        ("total_spent", b.get("total_spent")),
        ("mrr", b.get("mrr")),
        ("customer_since", b.get("customer_since")),
        ("connected_discord", b.get("connected_discord")),
        ("cancellation_reason", b.get("cancellation_reason")),
        ("last_success_paid_at", b.get("last_success_paid_at")),
        ("cancel_at_period_end", b.get("cancel_at_period_end")),
        ("is_first_membership", b.get("is_first_membership")),
        ("last_payment_method", b.get("last_payment_method")),
        ("last_payment_type", b.get("last_payment_type")),
        ("last_payment_failure", b.get("last_payment_failure")),
    ]


def _truthy(v: object) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _infer_event_kind(title: str) -> str:
    t = (title or "").lower()
    if "cancellation scheduled" in t:
        return "cancellation_scheduled"
    if "payment failed" in t:
        return "payment_failed"
    if "deactivated" in t:
        return "deactivated"
    return "active"


def build_case_minimal_embed(
    *,
    title: str,
    member: discord.Member,
    access_roles: str,
    whop_brief: dict | None,
    color: int,
    event_kind: str | None = None,
) -> discord.Embed:
    """Minimal staff case embed (for payment-failure / member-cancelation).

    Whop-style layout: compact inline fields + one long \"Payment Issue\" field when needed.
    """
    b = whop_brief if isinstance(whop_brief, dict) else {}
    kind = str(event_kind or "").strip().lower() or _infer_event_kind(title)
    cancel_at_period_end = _truthy(b.get("cancel_at_period_end"))
    label_overrides: dict[str, str] = {}
    if kind in {"cancellation_scheduled", "deactivated"}:
        label_overrides["renewal_start"] = "Current Period Started"
        label_overrides["renewal_end"] = "Access Ends On"
    else:
        label_overrides["renewal_start"] = "Billing Period Started"
        label_overrides["renewal_end"] = "Access Ends On" if cancel_at_period_end else "Next Billing Date"
    # Clarify what "Total Spent" means (lifetime vs membership-only fallback).
    spent_raw = b.get("total_spent")
    spent_s = _sanitize_value(spent_raw)
    if isinstance(spent_raw, str) and "(membership)" in spent_raw:
        label_overrides["total_spent"] = "Total Spent (membership)"
    elif not _is_blank(spent_s):
        label_overrides["total_spent"] = "Total Spent (lifetime)"
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    apply_member_header(embed, member)

    # Row 1 (inline x3)
    name = str(getattr(member, "display_name", "") or str(member))
    prof = f"https://discord.com/users/{int(getattr(member, 'id', 0) or 0)}"
    _add_field(embed, "Member", f"[{name}]({prof})", inline=True)
    _add_field(embed, "Discord ID", f"`{member.id}`", inline=True)
    _add_field(embed, _human_label("access_roles"), access_roles, inline=True)

    # Row 2 (inline x3)
    _add_field_force(embed, _human_label("status", label_overrides=label_overrides), b.get("status"), inline=True)
    _add_field(embed, _human_label("product", label_overrides=label_overrides), b.get("product"), inline=True)
    # Required for case channels: always show Total Spent (even if blank).
    _add_field_force(embed, _human_label("total_spent", label_overrides=label_overrides), spent_s, inline=True)

    # Row 3 (inline x3)
    _add_field(embed, _human_label("remaining_days", label_overrides=label_overrides), b.get("remaining_days"), inline=True)
    _add_field(embed, _human_label("renewal_end", label_overrides=label_overrides), b.get("renewal_end"), inline=True)
    # Required for case channels: always show Dashboard (never Manage).
    _add_field_force(embed, _human_label("dashboard_url", label_overrides=label_overrides), b.get("dashboard_url"), inline=True)
    _add_field_force(embed, _human_label("renewal_window", label_overrides=label_overrides), b.get("renewal_window"), inline=False)

    # Optional long text: payment issue (only when present)
    _add_field(embed, _human_label("last_payment_failure", label_overrides=label_overrides), b.get("last_payment_failure"), inline=False)

    embed.set_footer(text="RSCheckerbot")
    return embed


def build_member_status_detailed_embed(
    *,
    title: str,
    member: discord.Member,
    access_roles: str,
    color: int,
    discord_kv: list[tuple[str, object]] | None = None,
    member_kv: list[tuple[str, object]] | None = None,
    whop_brief: dict | None = None,
    event_kind: str | None = None,
    force_whop_core_fields: bool = True,
) -> discord.Embed:
    """Detailed staff embed for member-status-logs.

    Whop-style layout: compact inline fields for quick scan, plus one long notes field.
    """
    b = whop_brief if isinstance(whop_brief, dict) else {}
    kind = str(event_kind or "").strip().lower() or _infer_event_kind(title)
    cancel_at_period_end = _truthy(b.get("cancel_at_period_end"))
    label_overrides: dict[str, str] = {}
    if kind in {"cancellation_scheduled", "deactivated"}:
        label_overrides["renewal_start"] = "Current Period Started"
        label_overrides["renewal_end"] = "Access Ends On"
    else:
        label_overrides["renewal_start"] = "Billing Period Started"
        label_overrides["renewal_end"] = "Access Ends On" if cancel_at_period_end else "Next Billing Date"
    # Clarify what "Total Spent" means (lifetime vs membership-only fallback).
    spent_raw = b.get("total_spent")
    spent_s = _sanitize_value(spent_raw)
    if isinstance(spent_raw, str) and "(membership)" in spent_raw:
        label_overrides["total_spent"] = "Total Spent (membership)"
    elif not _is_blank(spent_s):
        label_overrides["total_spent"] = "Total Spent (lifetime)"
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
    apply_member_header(embed, member)

    # Header row (inline x3)
    name = str(getattr(member, "display_name", "") or str(member))
    prof = f"https://discord.com/users/{int(getattr(member, 'id', 0) or 0)}"
    _add_field(embed, "Member", f"[{name}]({prof})", inline=True)
    _add_field(embed, "Discord ID", f"`{member.id}`", inline=True)
    _add_field(embed, _human_label("access_roles"), access_roles, inline=True)

    mk = _pairs_to_dict(member_kv)
    dk = _pairs_to_dict(discord_kv)

    # High-signal member/activity details (inline; order matters)
    for key in (
        "account_created",
        "first_joined",
        "join_count",
        "returning_member",
        "left_at",
        "ever_had_member_role",
        "first_access",
        "last_access",
        "roles_added",
        "roles_removed",
        "reason",
        "invite_code",
        "tracked_invite",
        "source",
        "access_roles_at_leave",
        "whop_link",
    ):
        v = mk.get(key) if key in mk else dk.get(key)
        if _is_blank(v):
            continue
        name, val = _human_value_for_field(key, v)
        _add_field(embed, name, val, inline=True)

    def _has_any_whop_value(brief: dict) -> bool:
        if not isinstance(brief, dict):
            return False
        # "Core" values we want to avoid showing as forced blanks on join/leave cards.
        for k in ("status", "product", "total_spent", "dashboard_url", "renewal_window", "renewal_end", "remaining_days"):
            if not _is_blank(_sanitize_value(brief.get(k))):
                return True
        return False

    is_lifecycle = any(x in str(title or "").lower() for x in ("member joined", "member left"))
    has_whop = _has_any_whop_value(b)

    # Payment rows (inline x3)
    if is_lifecycle and not has_whop:
        # For join/leave cards, avoid spamming forced "—" fields when the member isn't linked yet.
        _add_field(embed, "Whop", "not linked (no membership_id recorded yet)", inline=False)
    else:
        # Always show the core Whop fields on non-lifecycle cards; for join/leave cards, hide blanks.
        if (not force_whop_core_fields) or is_lifecycle:
            add_core = _add_field
            add_window = _add_field
            add_spent = _add_field
        else:
            add_core = _add_field_force
            add_window = _add_field_force
            add_spent = _add_field_force

        # Membership ID (keep in Whop section, not in Discord section).
        mid_any = b.get("membership_id")
        if _is_blank(mid_any):
            mid_any = mk.get("membership_id") if isinstance(mk, dict) else ""
        if _is_blank(mid_any):
            mid_any = dk.get("membership_id") if isinstance(dk, dict) else ""
        _add_field(embed, _human_label("membership_id", label_overrides=label_overrides), mid_any, inline=True)
        add_core(embed, _human_label("status", label_overrides=label_overrides), b.get("status"), inline=True)
        _add_field(embed, _human_label("product", label_overrides=label_overrides), b.get("product"), inline=True)
        add_spent(embed, _human_label("total_spent", label_overrides=label_overrides), spent_s, inline=True)

        _add_field(embed, _human_label("trial_days", label_overrides=label_overrides), b.get("trial_days"), inline=True)
        _add_field(embed, _human_label("plan_is_renewal", label_overrides=label_overrides), b.get("plan_is_renewal"), inline=True)
        _add_field(embed, _human_label("promo", label_overrides=label_overrides), b.get("promo"), inline=True)
        _add_field(embed, _human_label("pricing", label_overrides=label_overrides), b.get("pricing"), inline=True)

        _add_field(embed, _human_label("remaining_days", label_overrides=label_overrides), b.get("remaining_days"), inline=True)
        _add_field(embed, _human_label("renewal_end", label_overrides=label_overrides), b.get("renewal_end"), inline=True)
        add_core(embed, _human_label("dashboard_url", label_overrides=label_overrides), b.get("dashboard_url"), inline=True)
        add_window(embed, _human_label("renewal_window", label_overrides=label_overrides), b.get("renewal_window"), inline=False)

    # Extra Whop UI-style fields (shown when available; never forced in the probe path).
    _add_field(embed, _human_label("mrr", label_overrides=label_overrides), b.get("mrr"), inline=True)
    _add_field(embed, _human_label("customer_since", label_overrides=label_overrides), b.get("customer_since"), inline=True)
    _add_field(embed, _human_label("connected_discord", label_overrides=label_overrides), b.get("connected_discord"), inline=False)
    _add_field(embed, _human_label("cancellation_reason", label_overrides=label_overrides), b.get("cancellation_reason"), inline=False)

    _add_field(embed, _human_label("last_success_paid_at", label_overrides=label_overrides), b.get("last_success_paid_at"), inline=True)
    _add_field(embed, _human_label("cancel_at_period_end", label_overrides=label_overrides), b.get("cancel_at_period_end"), inline=True)
    _add_field(embed, _human_label("is_first_membership", label_overrides=label_overrides), b.get("is_first_membership"), inline=True)

    # We intentionally omit noisy plan/payment-method fields. If there's a real failure message,
    # we still show it (and it won't render for N/A).
    _add_field(embed, _human_label("last_payment_failure", label_overrides=label_overrides), b.get("last_payment_failure"), inline=False)

    embed.set_footer(text="RSCheckerbot • Member Status Tracking")
    return embed

