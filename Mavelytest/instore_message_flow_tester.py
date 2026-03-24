#!/usr/bin/env python3
"""
instore_message_flow_tester.py

Dry-run (default) or live replay of Instorebotforwarder's real forwarding path for one
Discord message, with terminal logs matching production [FLOW:...] lines.

Paste a message link (stable, PTB, or Canary host is fine):
  https://discord.com/channels/<guild_id>/<channel_id>/<message_id>
  https://ptb.discord.com/channels/<guild_id>/<channel_id>/<message_id>

Requirements:
  - MWBots/Instorebotforwarder/config.json + config.secrets.json (valid bot_token)
  - Bot must be able to read the source channel (same as production)
  - Stop the live Instorebotforwarder service while using the SAME token, or Discord will
    reject the duplicate session (Invalid session / disconnect loop).

Examples (from repo root):
  python Mavelytest/instore_message_flow_tester.py --link "https://discord.com/channels/1/2/3"
  python Mavelytest/instore_message_flow_tester.py --link "..." --live-send

  # Each run writes a JSON audit trail by default to Mavelytest/audit_logs/ (disable: --no-audit-json).

  # Avoid cmd mangling URLs with & - use env (run_instore_message_flow_tester.bat does this):
  set INSTORE_FLOW_TEST_LINK=https://discord.com/channels/...
  python Mavelytest/instore_message_flow_tester.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MAVELYTEST = Path(__file__).resolve().parent
_MWBOTS = _REPO_ROOT / "MWBots"
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if str(_MWBOTS) not in sys.path:
    sys.path.insert(0, str(_MWBOTS))

# Stable + PTB + Canary clients (message IDs are the same).
DISCORD_MSG_RE = re.compile(
    r"https?://(?:www\.|(?:(?:ptb|canary)\.)?)discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)\b",
    re.I,
)

# Filled by dry-run patched send(); printed after forward.
CAPTURED_SENDS: List[Dict[str, Any]] = []

AUDIT_SCHEMA_VERSION = 2

_SOURCE_CHANNEL_PROFILES: Dict[int, Dict[str, str]] = {
    # Promo Deal (Gemini deals structured rewrite + embed rebuild)
    1438970053352751215: {"name": "Promo Deal", "mode": "gemini_rephrase_amz_deals"},
    # Mavely Leads (affiliate leads forward as an embed with original text)
    1435308472639160522: {"name": "Mavely Leads", "mode": "affiliated_leads"},
    # Amazon Lowkey Flips (full Amazon-card pipeline)
    1435066421133443174: {"name": "Amazon Lowkey Flips", "mode": "full_amazon_card"},
    # Price Error Glitched (full Amazon-card pipeline + stricter filtering)
    1435985494356918333: {"name": "Price Error Glitched", "mode": "full_amazon_card_price_error_glitched"},
}


def _profile_for_source_channel(src_channel_id: int) -> Dict[str, str]:
    p = _SOURCE_CHANNEL_PROFILES.get(int(src_channel_id))
    if p:
        return p
    return {"name": f"Unknown Source {src_channel_id}", "mode": "unknown"}


def _source_channels_for_profile(app: Any, *, expected_mode: str, fallback_src_id: int) -> List[int]:
    """
    Uses bot config to find all source channels that use the same simple_forward mode.
    For non-simple-forward pipelines, it falls back to the one channel we tested.
    """
    mode = (expected_mode or "").strip().lower()
    if not mode:
        return [int(fallback_src_id)]

    cfg = getattr(app, "config", None) or {}
    sfm = cfg.get("simple_forward_mappings") or {}
    if not isinstance(sfm, dict) or not sfm:
        return [int(fallback_src_id)]

    out: List[int] = []
    for k, v in sfm.items():
        if not isinstance(v, dict):
            continue
        vm = str(v.get("mode") or "").strip().lower()
        if vm and vm == mode:
            try:
                out.append(int(k))
            except Exception:
                continue

    return out or [int(fallback_src_id)]


def _fmt_channels(channel_ids: List[int]) -> str:
    # Keep it readable in terminals: "<#id1>, <#id2>".
    uniq = []
    for cid in channel_ids:
        try:
            i = int(cid)
        except Exception:
            continue
        if i not in uniq:
            uniq.append(i)
    return ", ".join(f"<#{i}>" for i in uniq)


def _format_captured_send_summaries() -> str:
    if not CAPTURED_SENDS:
        return ""
    lines: List[str] = []
    for i, cap in enumerate(CAPTURED_SENDS, 1):
        ch_id = cap.get("channel_id")
        embeds = cap.get("embeds") or []
        if embeds:
            title = (embeds[0].get("title") or "")[:80]
        else:
            title = ""
        lines.append(f"  {i}) <#{ch_id}> {title!r}".rstrip())
    return "\n".join(lines)


def _audit_pre_flight(audit: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Mutable pre_flight bucket; no-op sink when audit is None."""
    if audit is None:
        return {}
    audit.setdefault("pre_flight", {})
    return audit["pre_flight"]


def _default_audit_json_path(message_id: int) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = _MAVELYTEST / "audit_logs"
    return out_dir / f"instore_flow_{ts}_{message_id}.json"


def write_flow_audit_json(audit: Dict[str, Any], path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(audit, fh, indent=2, ensure_ascii=False)
        fh.write("\n")


def _configure_stdio_utf8() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def parse_discord_message_link(url: str) -> Tuple[int, int, int]:
    m = DISCORD_MSG_RE.search((url or "").strip())
    if not m:
        raise ValueError(
            "Expected a message link like "
            "https://discord.com/channels/<guild>/<channel>/<message_id> "
            "(or ptb.discord.com / canary.discord.com)"
        )
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def _noop_instance_lock(self: Any, base: Path) -> None:
    self._instance_lock_fh = None
    self._instance_lock_path = ""


def _noop_setup_events(self: Any) -> None:
    return None


def _noop_setup_slash(self: Any) -> None:
    return None


def _patch_discord_sends_for_dry_run() -> List[Any]:
    import discord

    fake_message = MagicMock()
    fake_message.id = 9_000_000_000_000_000_001

    async def _dry_send(self: Any, *args: Any, **kwargs: Any) -> Any:
        log = logging.getLogger("instorebotforwarder")
        bits: List[str] = []
        if args and args[0]:
            bits.append(f"content_len={len(str(args[0]))}")
        if kwargs.get("content"):
            bits.append(f"content_len={len(str(kwargs['content']))}")
        embed_payload: List[Dict[str, Any]] = []
        if kwargs.get("embeds"):
            bits.append(f"embeds={len(kwargs['embeds'])}")
            for e in kwargs["embeds"] or []:
                try:
                    embed_payload.append(e.to_dict() if hasattr(e, "to_dict") else {})
                except Exception:
                    embed_payload.append({})
        elif kwargs.get("embed") is not None:
            e = kwargs["embed"]
            try:
                d = e.to_dict() if hasattr(e, "to_dict") else {}
                embed_payload.append(d)
                title = (d.get("title") or "")[:80]
                desc = (d.get("description") or "")[:120]
                bits.append(f"embed title={title!r} desc_preview={desc!r}")
            except Exception:
                bits.append("embed=<present>")
        log.info("[DRY-RUN] Messageable.send %s", " ".join(bits) if bits else "(empty)")
        try:
            ch_id = int(getattr(self, "id", 0) or 0)
        except Exception:
            ch_id = 0
        CAPTURED_SENDS.append(
            {
                "channel_id": ch_id,
                "content": (kwargs.get("content") or (args[0] if args else None)),
                "embeds": embed_payload,
            }
        )
        return fake_message

    patches: List[Any] = []
    for cls in (discord.TextChannel, discord.Thread, discord.DMChannel):
        p = patch.object(cls, "send", new=_dry_send)
        p.start()
        patches.append(p)
    return patches


def _print_section(title: str) -> None:
    print()
    print("-" * 72)
    print(title)
    print("-" * 72)


_GEMINI_USAGE_DOCS_URL = "https://ai.google.dev/gemini-api/docs/rate-limits"


def _print_gemini_usage_footer(usage: Optional[Dict[str, Any]]) -> None:
    """Printed after forward; sums usageMetadata from generateContent responses this session."""
    _print_section("9) Gemini usage (this run)")
    if not isinstance(usage, dict):
        print("  (Not tracked — internal accumulator missing.)")
        print()
        print(
            f"  Remaining quota: not returned by the API. See Google AI Studio and {_GEMINI_USAGE_DOCS_URL}"
        )
        return
    calls = int(usage.get("generate_content_calls") or 0)
    pt = int(usage.get("prompt_token_count") or 0)
    ct = int(usage.get("candidates_token_count") or 0)
    tt = int(usage.get("total_token_count") or 0)
    if calls == 0 and pt == 0 and ct == 0 and tt == 0:
        print(
            "  No usageMetadata received (no Gemini generateContent in this run, or responses omitted it)."
        )
    else:
        print(f"  generateContent calls (counted): {calls}")
        print(f"  Prompt tokens (sum):            {pt}")
        print(f"  Output tokens (sum):             {ct}")
        print(f"  Total tokens (sum):             {tt}")
    print()
    print(
        "  Remaining quota / how many requests or tokens you have left: not included in API responses. "
        f"Check your project in Google AI Studio (Usage / billing) and the published limits: {_GEMINI_USAGE_DOCS_URL}"
    )


def _print_gemini_section5_result(
    *,
    skip_gemini_api: bool,
    gemini_err: Optional[str],
    gemini_api_fail: Optional[str],
    hdr_changed: bool,
    body_changed: bool,
    h1: str,
    b1: str,
) -> None:
    """One short outcome block for section 5 (audit + logs keep detail)."""
    print("  Result:")
    if skip_gemini_api:
        print("    Skipped (--skip-gemini-api). Forward may still call Gemini.")
        print(f"    Would use: {_gemini_one_line_headline_body(h1, b1)}")
        return
    if gemini_err:
        print(f"    Exception: {gemini_err}")
        print("    Using parsed headline/body (see Input above).")
        return
    if gemini_api_fail == "http_429":
        print("    HTTP 429 — Google rejected the request (quota / rate limit / no allowance on this project).")
        print("    You did not 'fail' the tester: no successful Gemini response happened, so there is nothing to compare.")
        print("    Log text like free_tier + limit: 0 often means billing not enabled or no free quota for that model — not 'you used everything'.")
        print("    Parse, Amazon, embed, and send paths still ran; only the optional rewrite step is blocked. No second Gemini call this run.")
        return
    if gemini_api_fail == "http_404":
        print("    HTTP 404 — Google does not serve this model id for your account (often retired for new users).")
        print("    Update gemini_model in Instore config.json (e.g. gemini-2.5-flash-lite or gemini-2.0-flash).")
        print("    Using parsed text; second tester call skipped.")
        return
    if gemini_api_fail and str(gemini_api_fail).startswith("http_"):
        print(f"    API HTTP error ({gemini_api_fail}) — using parsed text; second tester call skipped.")
        return
    if gemini_api_fail:
        print(f"    No usable model JSON ({gemini_api_fail!r}) — using parsed text; second call skipped.")
        return
    if not hdr_changed and not body_changed:
        print("    API OK — model kept the same wording as under Input (check logs for [FLOW:GEMINI]).")
        return
    print("    API OK — model revised text (Diff below).")


def _gemini_one_line_headline_body(h: str, b: str) -> str:
    hp = _preview_text(h, 90)
    bp = _preview_text(b, 90)
    if (b or "").strip():
        return f"Headline: {hp!r}  Body: {bp!r}"
    return f"Headline: {hp!r}  Body: (empty)"


async def _drain_bot_aiohttp(bot: Any) -> None:
    """Best-effort close of discord.py's aiohttp session/connector (reduces Unclosed connector noise)."""
    http = getattr(bot, "http", None)
    if http is None:
        return
    for name in ("_session", "session"):
        sess = getattr(http, name, None)
        if sess is None or getattr(sess, "closed", True):
            continue
        try:
            await sess.close()
        except Exception:
            pass
        conn = getattr(sess, "connector", None)
        if conn is not None and not getattr(conn, "closed", True):
            try:
                await conn.close()
            except TypeError:
                try:
                    conn.close()
                except Exception:
                    pass
            except Exception:
                pass
        break


def _preview_text(s: Any, limit: int = 600) -> str:
    t = (s or "") if isinstance(s, str) else str(s or "")
    t = t.replace("\r\n", "\n").strip()
    if len(t) > limit:
        return t[: limit - 3] + "..."
    return t


def _indent_lines(text: str, prefix: str = "    ") -> str:
    raw = (text or "").replace("\r\n", "\n")
    if not raw.strip():
        return f"{prefix}(empty)"
    return "\n".join(prefix + line for line in raw.split("\n"))


def _collect_source_snapshot_audit(app: Any, msg: Any) -> Dict[str, Any]:
    """Structured A–D snapshot for console + JSON audit (full text, not terminal previews)."""
    c = (getattr(msg, "content", None) or "").strip()
    embeds = list(getattr(msg, "embeds", None) or [])
    embed_audit: List[Dict[str, Any]] = []
    for e in embeds:
        title = (getattr(e, "title", None) or "") or ""
        desc = (getattr(e, "description", None) or "") or ""
        eu = (getattr(e, "url", None) or "") or ""
        ft = ""
        try:
            fo = getattr(e, "footer", None)
            ft = (getattr(fo, "text", None) or "").strip() if fo is not None else ""
        except Exception:
            ft = ""
        iu = ""
        tu = ""
        try:
            im = getattr(e, "image", None)
            iu = (getattr(im, "url", None) or "").strip() if im is not None else ""
        except Exception:
            pass
        try:
            th = getattr(e, "thumbnail", None)
            tu = (getattr(th, "url", None) or "").strip() if th is not None else ""
        except Exception:
            pass
        fields: List[Dict[str, str]] = []
        try:
            for f in getattr(e, "fields", None) or []:
                fields.append(
                    {
                        "name": (getattr(f, "name", None) or "") or "",
                        "value": (getattr(f, "value", None) or "") or "",
                    }
                )
        except Exception:
            pass
        embed_audit.append(
            {
                "title": title,
                "description": desc,
                "embed_url": eu,
                "footer_text": ft,
                "footer_forwarded": False,
                "image_url": iu,
                "thumbnail_url": tu,
                "fields": fields,
            }
        )

    atts = list(getattr(msg, "attachments", None) or [])
    att_audit = []
    for a in atts:
        att_audit.append(
            {
                "filename": getattr(a, "filename", "") or "",
                "url": (getattr(a, "url", None) or "") or "",
            }
        )

    block = ""
    block_err: Optional[str] = None
    try:
        block = (app._simple_message_block(msg) or "").strip()  # noqa: SLF001
    except Exception as ex:
        block_err = f"{type(ex).__name__}: {ex}"

    return {
        "message_content": c,
        "embeds": embed_audit,
        "attachments": att_audit,
        "simple_message_block": block,
        "simple_message_block_error": block_err,
    }


def _print_source_snapshot(app: Any, msg: Any) -> Dict[str, Any]:
    """What Discord gave us vs what _simple_message_block feeds into Gemini/embed body."""
    snap = _collect_source_snapshot_audit(app, msg)
    print("  A) message.content (top-level text only)")
    c = snap["message_content"]
    print(_indent_lines(c if c else "(empty)", "     "))

    embeds = snap["embeds"]
    print(f"  B) embeds from Discord (count={len(embeds)})")
    if not embeds:
        print("     (none)")
    else:
        for i, ed in enumerate(embeds, 1):
            title = ed.get("title") or ""
            desc = ed.get("description") or ""
            eu = ed.get("embed_url") or ""
            print(f"     [{i}] title ({len(title)} chars): {_preview_text(title, 160)!r}")
            if eu:
                print(f"         embed.url: {eu[:220]!r}")
            print(f"         description ({len(desc)} chars):")
            print(_indent_lines(_preview_text(desc, 2500), "         "))
            ft = ed.get("footer_text") or ""
            if ft:
                print(f"         footer (not forwarded): {_preview_text(ft, 200)!r}")
            iu = ed.get("image_url") or ""
            tu = ed.get("thumbnail_url") or ""
            if iu:
                print(f"         image.url: {_preview_text(iu, 200)!r}")
            if tu:
                print(f"         thumbnail.url: {_preview_text(tu, 200)!r}")

    atts = snap["attachments"]
    print(f"  C) attachments (count={len(atts)})")
    if not atts:
        print("     (none)")
    else:
        for i, a in enumerate(atts, 1):
            fn = a.get("filename") or ""
            u = a.get("url") or ""
            print(f"     [{i}] {fn!r}")
            print(f"         url: {_preview_text(u, 220)!r}")

    block = snap["simple_message_block"]
    err = snap.get("simple_message_block_error")
    if err:
        print(f"  D) _simple_message_block: error {err}")
    else:
        print(
            f"  D) _simple_message_block ({len(block)} chars) -> parsed headline/body for "
            f"gemini_rephrase_amz_deals (footer stripped; bot handles URLs/ASIN/embed.url/image/send)."
        )
        print(_indent_lines(_preview_text(block, 4000), "     "))
        if embeds:
            print(
                "     NOTE: Block = content + embed title/description/fields + attachment URLs; "
                "source embed footer is never included (no From/By line in D)."
            )
    return snap


async def print_pre_flight_report(
    app: Any,
    msg: Any,
    *,
    skip_gemini_api: bool,
    audit: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Human-readable checklist: which pipeline applies, gates passed/failed, detection details.
    Does not call _analyze_message (would break ASIN dedupe before the real forward).
    """
    from RSForwarder import affiliate_rewriter

    import Instorebotforwarder.instore_auto_mirror_bot as iam_mod

    _strip_emoji = iam_mod._strip_emoji_text
    _cfg_int = iam_mod._cfg_int

    pf = _audit_pre_flight(audit)
    src_id = int(msg.channel.id)
    sources = set(app._source_channel_ids())  # noqa: SLF001
    outputs = set(app._output_channel_ids())  # noqa: SLF001
    skip_rs_new_deal_found_card = False
    try:
        skip_rs_new_deal_found_card = bool(app._source_message_opens_with_new_deal_found(msg))  # noqa: SLF001
    except Exception:
        skip_rs_new_deal_found_card = False

    _print_section("1) Message shape")
    shape_err: Optional[str] = None
    content_len = embeds_n = comp_rows = -1
    try:
        content_len, embeds_n, comp_rows = app._message_shape(msg)  # noqa: SLF001
        print(f"  content_len={content_len}  embeds={embeds_n}  component_rows={comp_rows}")
    except Exception as e:
        shape_err = f"{type(e).__name__}: {e}"
        print(f"  (could not read shape: {e})")
    if audit is not None:
        pf["message_shape"] = {
            "content_len": content_len,
            "embeds": embeds_n,
            "component_rows": comp_rows,
            "error": shape_err,
        }

    _print_section("2) Source gates (same as _maybe_forward_message)")
    author_is_bot = bool(app.bot.user and msg.author and msg.author.id == app.bot.user.id)
    in_sources = src_id in sources
    in_outputs = src_id in outputs
    if author_is_bot:
        print("  FAIL: author is this bot - production would skip.")
    else:
        print("  OK: author is not the bot")
    if not in_sources:
        print(f"  FAIL: channel {src_id} not in source_channel_ids - production would skip.")
    else:
        print(f"  OK: channel {src_id} is a configured source")
    if in_outputs:
        print(f"  FAIL: channel {src_id} is listed as an output channel - production would skip.")
    else:
        print("  OK: source is not an output channel")
    if skip_rs_new_deal_found_card:
        print(
            "  SKIP: RS 'New Deal Found!' card (top line of embed description/title or content) — "
            "production exits in _maybe_forward_message before simple-forward / Gemini / send "
            "([FLOW:SKIP] new_deal_found_card_top)."
        )
    else:
        print("  OK: not flagged as RS deal-card skip pattern")
    if audit is not None:
        pf["source_gates"] = {
            "author_is_bot": author_is_bot,
            "channel_in_source_channel_ids": in_sources,
            "channel_is_output_channel": in_outputs,
            "skip_new_deal_found_card_top": skip_rs_new_deal_found_card,
            "would_skip_in_production": author_is_bot
            or (not in_sources)
            or in_outputs
            or skip_rs_new_deal_found_card,
        }

    _print_section("3) URLs collected (same helper as the bot)")
    urls: List[str] = []
    url_collect_err: Optional[str] = None
    try:
        urls = list(app._collect_message_urls(msg) or [])  # noqa: SLF001
    except Exception as e:
        url_collect_err = f"{type(e).__name__}: {e}"
        print(f"  error: {e}")
    print(f"  count={len(urls)}")
    url_items: List[Dict[str, Any]] = []
    for i, u in enumerate(urls[:12], 1):
        amz = affiliate_rewriter.is_amazon_like_url((u or "").strip())
        mv = affiliate_rewriter.is_mavely_link((u or "").strip())
        flags = []
        if amz:
            flags.append("amazon-ish")
        if mv:
            flags.append("mavely")
        asin = affiliate_rewriter.extract_asin(u or "") or ""
        extra = f"  [{', '.join(flags)}]" if flags else ""
        asin_part = f"  asin={asin}" if asin else ""
        print(f"  [{i}] {_preview_text(u, 120)}{extra}{asin_part}")
        url_items.append({"url": u, "flags": flags, "asin_guess": asin or None})
    if len(urls) > 12:
        print(f"  ... {len(urls) - 12} more")
    print(
        "  NOTE: Spurious entries like https://32.xx come from price text in embeds "
        "($32.xx) matched as URLs; the bot still resolves the real deal link."
    )
    if audit is not None:
        pf["urls"] = {
            "count": len(urls),
            "collect_error": url_collect_err,
            "items_preview": url_items,
            "truncated": max(0, len(urls) - 12),
        }

    mapping = None
    map_err: Optional[str] = None
    try:
        mapping = app._simple_forward_mapping_for_channel(src_id)  # noqa: SLF001
    except Exception as e:
        map_err = f"{type(e).__name__}: {e}"
        print(f"  (mapping read error: {e})")
    if audit is not None:
        if isinstance(mapping, dict):
            pf["simple_forward_mapping"] = {str(k): v for k, v in mapping.items()}
        else:
            pf["simple_forward_mapping"] = None
        pf["simple_forward_mapping_error"] = map_err

    _print_section("4) Source message snapshot (A-D)")
    snap = _print_source_snapshot(app, msg)
    if audit is not None:
        pf["source_snapshot"] = snap

    det = None
    det_err: Optional[str] = None
    try:
        det = await app._detect_amazon(urls)  # noqa: SLF001
    except Exception as e:
        det_err = f"{type(e).__name__}: {e}"
        print(f"  (_detect_amazon raised: {e})")
    if audit is not None:
        pf["amazon_detection"] = {
            "ok": bool(det and getattr(det, "asin", "")),
            "error": det_err,
            "asin": getattr(det, "asin", "") if det else None,
            "url_used": getattr(det, "url_used", "") if det else None,
            "final_url": getattr(det, "final_url", "") if det else None,
        }

    if not mapping:
        print()
        print("  No simple_forward mapping for this source -> full Amazon-card pipeline (_analyze_message).")
        print("  Tip: DEBUG log on the bot shows [FLOW:SCAN] / [FLOW:URLS] / [FLOW:AMZ_DETECT].")
        if audit is not None:
            pf["early_exit"] = {
                "stage": "no_simple_forward_mapping",
                "detail": "Falls through to _analyze_message / Amazon-card pipeline in production.",
            }
        return

    dest_raw = mapping.get("dest_channel_id")
    try:
        dest_id = int(dest_raw)
    except Exception:
        dest_id = 0
    mode = str(mapping.get("mode") or "").strip().lower()
    if audit is not None:
        pf["route"] = {"dest_channel_id": dest_id, "mode": mode or None}

    gemini_ctx: Dict[str, Any] = {
        "h0": "",
        "b0": "",
        "h1": "",
        "b1": "",
        "kept": [],
        "raw_url": "",
        "desc_in": "",
        "parsed": {},
        "desc_rebuilt": "",
        "gemini_err": None,
        "prose_unchanged": True,
        "can_assemble": False,
    }

    _print_section("5) Gemini (headline + compare-body; codes/short prices in Product info)")
    ga: Dict[str, Any] = {}
    if mode != "gemini_rephrase_amz_deals":
        print("  Not used — simple_forward mode is not gemini_rephrase_amz_deals.")
    else:
        if audit is not None:
            pf.setdefault("forward_assembly", {})["gemini_rephrase_amz_deals"] = ga
        print(
            "  Gemini rewrites the headline and any long compare/context lines parsed into body "
            "(e.g. Originally … / similar kits …). Coupon/checkout lines and short Now/Reg rows stay "
            "in kept lines under \"### Product info\". Use --log-level WARNING for quieter [FLOW:GEMINI] logs."
        )
        if skip_rs_new_deal_found_card:
            print(
                "  SKIP: matches production gate — no Gemini call from this tester (avoids billing noise); "
                "_maybe_forward_message also returns immediately so nothing is posted."
            )
            ga["early_exit"] = {"reason": "new_deal_found_card_top"}
        elif not msg.guild:
            print("  FAIL: DM / no guild — Gemini forward path does not run in production.")
            ga["early_exit"] = {"reason": "no_guild"}
        else:
            block = (app._simple_message_block(msg) or "").strip()  # noqa: SLF001
            if not block:
                print("  FAIL: empty 4D block — nothing to parse for Gemini.")
                ga["early_exit"] = {"reason": "empty_simple_message_block"}
            elif not urls:
                print("  FAIL: no URLs collected.")
                ga["early_exit"] = {"reason": "no_urls"}
            else:
                desc_in = app._neutralize_mentions(block)  # noqa: SLF001
                max_chars = int(
                    mapping.get("gemini_max_chars")
                    or _cfg_int(app.config, "openai_max_chars", "OPENAI_MAX_CHARS")
                    or 1800
                )
                if len(desc_in) > max_chars:
                    desc_in = desc_in[: max_chars - 3] + "..."
                parsed = app._parse_amz_deals_block_for_structured_gemini(desc_in)  # noqa: SLF001
                h0 = str(parsed.get("header") or "")
                b0 = str(parsed.get("body") or "")
                kept = list(parsed.get("kept_lines") or [])
                gemini_ctx["h0"] = h0
                gemini_ctx["b0"] = b0
                gemini_ctx["kept"] = kept
                gemini_ctx["desc_in"] = desc_in
                gemini_ctx["parsed"] = parsed
                try:
                    _loose_urls = [
                        u for (u, _, _) in affiliate_rewriter.extract_urls_with_spans(desc_in) if (u or "").strip()
                    ]
                except Exception:
                    _loose_urls = []
                http_urls = [u for u in _loose_urls if app._is_public_http_url_for_amz_deals(u)]  # noqa: SLF001
                temp_override = app._gemini_temperature_from_simple_forward_mapping(mapping)  # noqa: SLF001
                print(
                    f"  Input: {len(desc_in)} chars (cap {max_chars})  |  "
                    f"headline → model: {_preview_text(h0, 72)!r}  |  "
                    f"body → model: {(_preview_text(b0, 72) or '(empty)')!r}"
                )
                _kept_preview = "; ".join(_preview_text(kl, 56) for kl in kept[:4])
                if len(kept) > 4:
                    _kept_preview += f" … (+{len(kept) - 4} more)"
                print(
                    f"  Kept lines / Product info (not sent to model): {len(kept)}"
                    + (f" — {_kept_preview}" if kept else "")
                )
                gemini_err: Optional[str] = None
                gemini_api_fail: Optional[str] = None
                if not det or not getattr(det, "final_url", ""):
                    print("  Note: Amazon not resolved yet — production would not send; section 6 still shows assembly.")
                if skip_gemini_api:
                    h1, b1 = h0, b0
                else:
                    print(f"  Calling Gemini (temp {temp_override}) …")
                    try:
                        h1, b1, gemini_api_fail = await app._gemini_rephrase_amz_deals_structured(  # noqa: SLF001
                            h0,
                            b0,
                            temperature_override=temp_override,
                        )
                    except Exception as e:
                        h1, b1 = h0, b0
                        gemini_err = f"{type(e).__name__}: {e}"
                        gemini_api_fail = "exception"
                prose_unchanged = (h1.strip() == h0.strip() and b1.strip() == b0.strip())
                gemini_ctx["h1"] = h1
                gemini_ctx["b1"] = b1
                gemini_ctx["gemini_err"] = gemini_err
                gemini_ctx["prose_unchanged"] = prose_unchanged
                _hdr_changed = h1.strip() != h0.strip()
                _body_changed = b1.strip() != b0.strip()
                _print_gemini_section5_result(
                    skip_gemini_api=skip_gemini_api,
                    gemini_err=gemini_err,
                    gemini_api_fail=gemini_api_fail,
                    hdr_changed=_hdr_changed,
                    body_changed=_body_changed,
                    h1=h1,
                    b1=b1,
                )
                if (
                    not skip_gemini_api
                    and not gemini_err
                    and not gemini_api_fail
                    and (_hdr_changed or _body_changed)
                ):
                    print("  Diff (parse → model):")
                    print(f"    H: {_preview_text(h0, 100)!r}")
                    print(f"       → {_preview_text(h1, 100)!r}")
                    print(f"    B: {_preview_text(b0, 120)!r}")
                    print(f"       → {_preview_text(b1, 120)!r}")
                if audit is not None:
                    ga.update(
                        {
                            "gemini_max_chars_cap": max_chars,
                            "skip_gemini_api_in_preflight": bool(skip_gemini_api),
                            "gemini_temperature": temp_override,
                            "structured_parse": parsed,
                            "urls_loose_scan_count": len(_loose_urls),
                            "urls_http_diagnostic": http_urls,
                            "header_before_gemini": h0,
                            "body_before_gemini": b0,
                            "header_after_gemini": h1,
                            "body_after_gemini": b1,
                            "desc_in_neutralized": desc_in,
                            "prose_unchanged_vs_parse": prose_unchanged,
                            "gemini_error": gemini_err,
                            "gemini_api_fail_reason": gemini_api_fail,
                        }
                    )
                gemini_ctx["gemini_api_fail_reason"] = None if skip_gemini_api else gemini_api_fail

    _gaf_raw = gemini_ctx.get("gemini_api_fail_reason")
    _gaf = (_gaf_raw or "").strip() if isinstance(_gaf_raw, str) else None
    if mode == "gemini_rephrase_amz_deals" and _gaf and not skip_gemini_api:
        setattr(app, "_instore_flow_tester_suppress_struct_gemini_reason", _gaf)
        if audit is not None:
            pf["gemini_skip_duplicate_struct_forward_reason"] = _gaf

    _print_section("6) Forward assembly (bot)")
    print(
        "  Bot work: Amazon resolution, route, destination channel, rebuild embed.description "
        "(kept lines + plain store URL), embed.url / image, send."
    )
    print()
    print("  E) Amazon resolution (_detect_amazon)")
    if det:
        print("     OK: product URL with ASIN")
        print(f"       asin={getattr(det, 'asin', '')}")
        print(f"       url_used={_preview_text(getattr(det, 'url_used', ''), 200)}")
        print(f"       final_url (embed click / direct store link)={_preview_text(getattr(det, 'final_url', ''), 220)}")
    else:
        saw_amz = False
        try:
            saw_amz = any(affiliate_rewriter.is_amazon_like_url((u or "").strip()) for u in urls)
        except Exception:
            pass
        if saw_amz:
            print(
                "     FAIL: no resolvable ASIN from these URLs "
                "(Amazon-looking links present but expansion/scrape found no product ASIN)."
            )
        else:
            print("     FAIL: no Amazon product detection from these URLs.")

    print()
    print(f"  Route: dest_channel_id={dest_id}  mode={mode!r}" if mode else f"  Route: dest_channel_id={dest_id}  mode=<merge/shop-expand>")

    if mode == "gemini_rephrase_amz_deals":
        print()
        if skip_rs_new_deal_found_card:
            print(
                "  F) Skipped — production does not assemble or send RS 'New Deal Found!' rebroadcast cards "
                "(same as section 2 / 5)."
            )
        else:
            print("  F) Assemble outbound embed (uses header/body from section 5)")
            h1 = str(gemini_ctx.get("h1") or "")
            b1 = str(gemini_ctx.get("b1") or "")
            h0 = str(gemini_ctx.get("h0") or "")
            b0 = str(gemini_ctx.get("b0") or "")
            kept = list(gemini_ctx.get("kept") or [])
            raw_url = ""
            if det and str(getattr(det, "final_url", "") or "").strip():
                raw_url = str(getattr(det, "final_url", "") or "").strip()
            ch = None
            ch_fetch_err: Optional[str] = None
            if not msg.guild:
                print("  Note: no guild — bot would not resolve destination here (same gate as section 5).")
            else:
                ch = app.bot.get_channel(dest_id)
                if ch is None:
                    try:
                        ch = await app.bot.fetch_channel(dest_id)
                    except Exception as e:
                        ch = None
                        ch_fetch_err = f"{type(e).__name__}: {e}"
                        print(f"  FAIL: cannot fetch dest channel: {e}")
            if msg.guild and ch is None:
                print("  FAIL: no destination channel — production returns without send")
                if audit is not None:
                    ga.setdefault("early_exit", {"reason": "no_destination_channel", "fetch_error": ch_fetch_err})
            elif ch is not None:
                print(f"  OK: destination channel id={getattr(ch, 'id', dest_id)}")
                if audit is not None:
                    ga["destination_channel_id"] = int(getattr(ch, "id", dest_id) or dest_id)
            if not raw_url or not affiliate_rewriter.is_amazon_like_url(raw_url):
                print("  FAIL: need E) Amazon OK (final_url) to rebuild description with store link")
                if audit is not None:
                    ga.setdefault(
                        "assembly_block",
                        {"reason": "final_url_missing_or_not_amazon"},
                    )
            elif not (h1 or h0) and not (b1 or b0) and not kept:
                print("  SKIP: no structured parse from section 5 — nothing to rebuild")
            else:
                gemini_ctx["raw_url"] = raw_url
                gemini_ctx["can_assemble"] = True
                try:
                    desc_out = app._rebuild_amz_deals_embed_description(h1, b1, kept, raw_url)  # noqa: SLF001
                except Exception as ex_rb:
                    desc_out = app._rebuild_amz_deals_embed_description(h0, b0, kept, raw_url)  # noqa: SLF001
                    if audit is not None:
                        ga["rebuild_error"] = f"{type(ex_rb).__name__}: {ex_rb}"
                gemini_ctx["desc_rebuilt"] = desc_out
                print(f"  Rebuilt embed.description ({len(desc_out)} chars): {_preview_text(desc_out, 560)!r}")
                print()
                print("  G) Outbound embed summary")
                print(
                    "     embed.description = **bold** headline + body + \"### Product info\" + exact kept lines "
                    "+ plain Amazon URL (last line)"
                )
                print(f"     embed.url         = final_url from E)  ({_preview_text(raw_url, 120)})")
                print("     source embed footer was not in 4D, so it is not in OUTPUT")
                if audit is not None:
                    ga.update(
                        {
                            "desc_out_rebuilt": desc_out,
                            "embed_url": raw_url,
                        }
                    )

    elif mode == "affiliated_leads":
        print()
        print("  F) Affiliated-leads (bot rewriter + embed; not Gemini)")
        aa: Dict[str, Any] = {"mode": "affiliated_leads"}
        if audit is not None:
            pf.setdefault("forward_assembly", {})["affiliated_leads"] = aa
        if not msg.guild:
            print("  FAIL: no guild")
            if audit is not None:
                aa["early_exit"] = "no_guild"
            return
        block = _strip_emoji(app._simple_message_block(msg) or "").strip()  # noqa: SLF001
        if not block:
            print("  FAIL: empty after emoji strip")
            if audit is not None:
                aa["early_exit"] = "empty_block"
            return
        print(f"  OK: stripped block len={len(block)}")
        aa["stripped_block_len"] = len(block)
        if not urls:
            print("  FAIL: no URLs")
            if audit is not None:
                aa["early_exit"] = "no_urls"
            return
        has_amazonish = any(affiliate_rewriter.is_amazon_like_url((u or "").strip()) for u in urls)
        has_mavelyish = any(affiliate_rewriter.is_mavely_link((u or "").strip()) for u in urls)
        print(f"  prefilter amazon-ish={has_amazonish}  mavely-ish={has_mavelyish}")
        aa["prefilter_amazonish"] = has_amazonish
        aa["prefilter_mavelyish"] = has_mavelyish
        if not (has_amazonish or has_mavelyish):
            print("  FAIL: no Amazon/Mavely URLs - stops before rewriter")
            if audit is not None:
                aa["early_exit"] = "no_amazon_or_mavely"
            return
        try:
            mapped, _notes = await affiliate_rewriter.compute_affiliate_rewrites_plain(app.config or {}, urls)
        except Exception as e:
            print(f"  FAIL: affiliate rewriter error: {e}")
            if audit is not None:
                aa["rewriter_error"] = f"{type(e).__name__}: {e}"
            return
        print(f"  rewriter returned {len(mapped) if isinstance(mapped, dict) else 0} mapping(s)")
        if audit is not None:
            aa["rewriter_mapping_count"] = len(mapped) if isinstance(mapped, dict) else 0
            if isinstance(mapped, dict):
                aa["rewriter_sample"] = {str(k): str(v) for k, v in list(mapped.items())[:12]}
            else:
                aa["rewriter_sample"] = None
        associate_tag = str((app.config or {}).get("amazon_associate_tag") or "").strip()

        def _is_amazon_affiliate_url(u: str) -> bool:
            try:
                parsed = urlparse(u)
                host = (parsed.netloc or "").lower()
                if not (
                    affiliate_rewriter.is_amazon_like_url(u)
                    or "amazon." in host
                    or host.endswith("amazon.com")
                ):
                    return False
                q = parsed.query or ""
                return ("tag=" in q) and (not associate_tag or f"tag={associate_tag}" in q)
            except Exception:
                return False

        confirmed = False
        if isinstance(mapped, dict):
            for orig_u, repl in mapped.items():
                repl_s = str(repl or "").strip()
                if affiliate_rewriter.is_mavely_link(repl_s):
                    confirmed = True
                    print(f"  OK: Mavely rewrite for {_preview_text(orig_u, 80)!r}")
                    break
                if _is_amazon_affiliate_url(repl_s):
                    confirmed = True
                    print(f"  OK: Amazon affiliate URL for {_preview_text(orig_u, 80)!r}")
                    break
        if not confirmed:
            print("  FAIL: no Mavely / tagged-Amazon confirmation - production does not send")
            if audit is not None:
                aa["early_exit"] = "no_mavely_or_tagged_amazon_confirmation"
            return
        print("  Next: expand chosen URL, build embed(description=text, url=raw_store)")
        if audit is not None:
            aa["preflight_ok"] = True

    elif mapping.get("mirror_as_is"):
        print()
        print("  F) mirror_as_is: bot forwards content + attachments + copied embeds (no Gemini).")
        if audit is not None:
            pf.setdefault("forward_assembly", {})["mirror_as_is"] = {"note": "content + attachments + copied embeds"}

    else:
        merge_s = float(mapping.get("merge_window_s") or 3.0)
        if merge_s <= 0:
            merge_s = 3.0
        print()
        print("  F) Default simple-forward / merge (bot only; no Gemini)")
        print(f"  merge_window_s={merge_s} (flush after idle window)")
        print("  Shop-link expand + plain text forward.")
        if audit is not None:
            pf.setdefault("forward_assembly", {})["default_merge"] = {"merge_window_s": merge_s}

    _print_section("7) Config reference (this source row)")
    print(f"  source_channel_id={src_id}  dest_channel_id={dest_id}  mode={mode!r}" if mode else f"  source_channel_id={src_id}  dest_channel_id={dest_id}  mode=<default>")
    if audit is not None:
        pf["config_reference"] = {
            "source_channel_id": src_id,
            "dest_channel_id": dest_id,
            "mode": mode or None,
        }


def print_captured_sends_summary() -> None:
    if not CAPTURED_SENDS:
        print()
        _print_section("8) Outbound preview (dry-run)")
        print("  No channel.send captured - pipeline exited before send (silent drop) or live-send was used.")
        return
    print()
    _print_section("8) Outbound preview (dry-run captures)")
    for i, cap in enumerate(CAPTURED_SENDS, 1):
        print(f"  --- send #{i} dest_channel_id={cap.get('channel_id')} ---")
        c = cap.get("content")
        if c:
            print(f"  content: {_preview_text(c, 400)!r}")
        for j, ed in enumerate(cap.get("embeds") or [], 1):
            print(f"  embed[{j}] title: {(ed.get('title') or '')[:120]!r}")
            print(f"  embed[{j}] url:   {(ed.get('url') or '')[:200]!r}")
            desc = ed.get("description") or ""
            print(f"  embed[{j}] description ({len(desc)} chars): {_preview_text(desc, 700)!r}")


async def _await_simple_forward_tasks(fwd: Any) -> None:
    tasks = list((getattr(fwd, "_simple_forward_flush_tasks", None) or {}).values())
    if not tasks:
        return
    logging.getLogger("instorebotforwarder").info(
        "[TESTER] awaiting %s simple-forward flush task(s) (merge window)", len(tasks)
    )
    await asyncio.gather(*tasks, return_exceptions=True)


def _audit_mark(audit: Dict[str, Any], step: str, **extra: Any) -> None:
    audit.setdefault("timeline", []).append(
        {"t_utc": datetime.now(timezone.utc).isoformat(), "step": step, **extra}
    )


async def run_once(
    *,
    guild_id: int,
    channel_id: int,
    message_id: int,
    message_link: str,
    dry_run: bool,
    brief: bool,
    skip_gemini_api: bool,
    audit_json_path: Optional[Path],
) -> int:
    from Instorebotforwarder import instore_auto_mirror_bot as iam

    iam.InstorebotForwarder._acquire_single_instance_lock = _noop_instance_lock  # type: ignore[method-assign]
    iam.InstorebotForwarder._setup_events = _noop_setup_events  # type: ignore[method-assign]
    iam.InstorebotForwarder._setup_slash_commands = _noop_setup_slash  # type: ignore[method-assign]

    CAPTURED_SENDS.clear()
    patches: List[Any] = []
    if dry_run:
        patches = _patch_discord_sends_for_dry_run()

    exit_code = 0
    log = logging.getLogger("instorebotforwarder")

    audit: Dict[str, Any] = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "tool": "instore_message_flow_tester",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": {
            "discord_message_link": message_link,
            "guild_id": guild_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "dry_run": dry_run,
            "brief": brief,
            "skip_gemini_api_in_preflight": skip_gemini_api,
            "audit_json_path": str(audit_json_path) if audit_json_path else None,
        },
        "timeline": [],
    }
    _audit_mark(audit, "run_started")

    try:
        app = iam.InstorebotForwarder()
    except Exception as e:
        log.error("Failed to construct InstorebotForwarder: %s", e)
        audit["constructor_error"] = {"type": type(e).__name__, "message": str(e)}
        audit["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        audit["exit_code"] = 2
        if audit_json_path:
            try:
                write_flow_audit_json(audit, audit_json_path)
                print(f"Wrote audit JSON: {audit_json_path}")
            except Exception as exw:
                log.error("Failed to write audit JSON: %s", exw)
        for p in patches:
            try:
                p.stop()
            except Exception:
                pass
        return 2

    setattr(app, "_instore_flow_tester_suppress_struct_gemini_reason", None)

    token = str((app.config.get("bot_token") or "")).strip()
    if not token:
        log.error("bot_token missing after config merge.")
        audit["runtime_error"] = {"type": "ConfigError", "message": "bot_token missing after config merge"}
        audit["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        audit["exit_code"] = 2
        if audit_json_path:
            try:
                write_flow_audit_json(audit, audit_json_path)
                print(f"Wrote audit JSON: {audit_json_path}")
            except Exception as exw:
                log.error("Failed to write audit JSON: %s", exw)
        for p in patches:
            try:
                p.stop()
            except Exception:
                pass
        return 2

    sources = set(app._source_channel_ids())  # noqa: SLF001

    @app.bot.event
    async def on_ready() -> None:
        nonlocal exit_code
        try:
            # Capture only the key bot decision points so the output ends with a crisp PASS/FAIL.
            flow_trace: List[str] = []

            class _FlowTraceHandler(logging.Handler):
                def emit(self, record: logging.LogRecord) -> None:
                    try:
                        msg = record.getMessage()
                    except Exception:
                        return
                    # Example: "[FLOW:SKIP] reason=new_deal_found_card_top channel_id=<#...> message_id=..."
                    if not isinstance(msg, str):
                        return
                    if not msg.startswith("[FLOW:"):
                        return
                    if any(tag in msg for tag in ["[FLOW:MAPPING]", "[FLOW:ROUTE]", "[FLOW:SKIP]", "[FLOW:FILTER_SKIP]", "[FLOW:SEND_OK]", "[FLOW:SEND_FAIL]", "[FLOW:DUP_ASIN_SKIP]", "[FLOW:SKIP_OOS]", "[FLOW:EBAY_PW_FAIL]"]):
                        flow_trace.append(msg)
                # noinspection PyMethodMayBeStatic

            bot_logger = logging.getLogger("instorebotforwarder")
            flow_handler = _FlowTraceHandler()
            flow_handler.setLevel(logging.INFO)
            bot_logger.addHandler(flow_handler)

            app._gemini_usage_accumulator = {
                "prompt_token_count": 0,
                "candidates_token_count": 0,
                "total_token_count": 0,
                "generate_content_calls": 0,
            }
            log.info(
                "[TESTER] logged in as %s; fetching message ch=%s msg=%s (guild from link=%s)",
                app.bot.user,
                channel_id,
                message_id,
                guild_id,
            )
            ch = app.bot.get_channel(channel_id)
            if ch is None:
                ch = await app.bot.fetch_channel(channel_id)
            msg = await ch.fetch_message(message_id)
            src_id = int(msg.channel.id)
            audit["source_message"] = {
                "id": int(message_id),
                "channel_id": src_id,
                "guild_id": int(guild_id),
                "jump_url": getattr(msg, "jump_url", None) or "",
                "author_id": int(msg.author.id) if getattr(msg, "author", None) else None,
            }
            _audit_mark(audit, "message_fetched", source_channel_id=src_id)
            if src_id not in sources:
                log.warning(
                    "[TESTER] channel %s is NOT in config source_channel_ids - "
                    "bot will ignore this message in production.",
                    src_id,
                )

            profile = _profile_for_source_channel(src_id)
            if not brief:
                print()
                _print_section("PRE-FLIGHT (read this for gates, patterns, preview)")
                await print_pre_flight_report(app, msg, skip_gemini_api=skip_gemini_api, audit=audit)
                _audit_mark(audit, "pre_flight_report_complete")
            else:
                # Still run the same pre-flight logic in brief mode so we can compute
                # a meaningful PASS/FAIL reason from the audit object; hide noisy prints.
                import contextlib
                import io

                with contextlib.redirect_stdout(io.StringIO()):
                    await print_pre_flight_report(app, msg, skip_gemini_api=skip_gemini_api, audit=audit)
                _audit_mark(audit, "pre_flight_report_complete_silent_brief")
            await app._maybe_forward_message(msg)  # noqa: SLF001
            _audit_mark(audit, "maybe_forward_message_returned")
            await _await_simple_forward_tasks(app)
            _audit_mark(audit, "simple_forward_flush_tasks_settled")

            # PASS/FAIL summary (based on whether bot actually posted in this run).
            sent = bool(CAPTURED_SENDS)
            # Live-send mode may not populate CAPTURED_SENDS; fall back to flow trace.
            if not sent and not dry_run:
                sent = any("[FLOW:SEND_OK]" in line for line in flow_trace)

            pf = (audit.get("pre_flight") or {}) if isinstance(audit, dict) else {}
            src_gates = (pf.get("source_gates") or {}) if isinstance(pf, dict) else {}
            skip_new_deal = bool(src_gates.get("skip_new_deal_found_card_top"))

            forward_assembly = (pf.get("forward_assembly") or {}) if isinstance(pf, dict) else {}
            early_exits: List[Tuple[str, str]] = []
            if isinstance(forward_assembly, dict):
                for k, v in forward_assembly.items():
                    if isinstance(v, dict) and v.get("early_exit"):
                        early_exits.append((str(k), str(v.get("early_exit") or "")))

            # PASS logic (what "PASS" means):
            # - if it actually posted -> PASS
            # - OR if it correctly hit the explicit "New Deal Found!" rebroadcast skip gate -> PASS
            # - otherwise it was gated out -> FAIL
            passed = sent or skip_new_deal
            # Exit code: 1 when it fails criteria/gates, so multi-link runs can be automated.
            exit_code = 0 if passed else 1

            # Build tags (supports "multiple category" by allowing multiple tags).
            tags: List[str] = []
            tags.append(f"profile:{profile.get('name') or src_id}")
            tags.append(f"route_mode:{(pf.get('route') or {}).get('mode') or 'unknown'}")
            tags.append(f"sent:{str(sent).lower()}")
            if skip_new_deal:
                tags.append("gate:new_deal_found_skip")
            for cat, reason in early_exits:
                if reason:
                    tags.append(f"early_exit:{cat}:{reason}")

            if passed and sent:
                headline = "PASS: bot posted for this input."
            elif passed and skip_new_deal and not sent:
                headline = "PASS: correctly skipped rebroadcast card ('New Deal Found!')."
            else:
                headline = "FAIL: bot did not post (criteria/gates blocked forwarding)."

            # “Where did it read from?” for this profile: list all configured source channels
            # that use the same forward-mode when applicable.
            profile_mode = profile.get("mode") or ""
            profile_srcs = _source_channels_for_profile(
                app,
                expected_mode=str(profile_mode),
                fallback_src_id=int(src_id),
            )

            print()
            _print_section("RESULT (PASS/FAIL)")
            print(f"  Source channel: <#{src_id}>")
            print(f"  Profile: {profile.get('name')!r}  |  expected mode: {profile.get('mode')!r}")
            print(f"  {headline}")
            print(f"  Source channels for this profile: {_fmt_channels(profile_srcs)}")
            print(f"  Tags: {', '.join(tags)}")

            if not brief:
                if sent:
                    print("  Sent captures:")
                    if CAPTURED_SENDS:
                        print(_format_captured_send_summaries())
                    else:
                        print("  (No dry-run captures; using log trace.)")
                else:
                    if early_exit_v:
                        print(f"  FAIL reason: forward early_exit={early_exit_k}:{early_exit_v}")
                    if flow_trace:
                        print("  Key decision trace (last few):")
                        for line in flow_trace[-5:]:
                            print(f"    {line}")
                if dry_run:
                    print_captured_sends_summary()
                else:
                    print()
                    _print_section("8) Outbound")
                    print("  Live send was enabled - check the destination channel in Discord.")
            log.info("[TESTER] done.")
            try:
                bot_logger.removeHandler(flow_handler)
            except Exception:
                pass
        except Exception as e:
            exit_code = 2
            log.exception("[TESTER] probe failed: %s", e)
            audit["runtime_error"] = {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            }
        finally:
            acc = getattr(app, "_gemini_usage_accumulator", None)
            if isinstance(acc, dict):
                audit["gemini_usage_this_run"] = dict(acc)
            else:
                audit["gemini_usage_this_run"] = None
            try:
                delattr(app, "_gemini_usage_accumulator")
            except Exception:
                pass
            if not brief:
                _print_gemini_usage_footer(acc if isinstance(acc, dict) else None)
            try:
                await app.bot.close()
            except Exception:
                pass
            try:
                await _drain_bot_aiohttp(app.bot)
            except Exception:
                pass
            # Let aiohttp connector cleanup before the event loop stops (reduces "Unclosed connector" noise).
            try:
                await asyncio.sleep(0.4)
            except Exception:
                pass

    try:
        await app.bot.start(token)
    except Exception as e:
        log.error("[TESTER] bot.start failed: %s", e)
        exit_code = 2
        audit.setdefault("runtime_error", {})
        audit["runtime_error"].update(
            {"type": type(e).__name__, "message": str(e), "stage": "bot.start"}
        )
    finally:
        audit["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        audit["exit_code"] = exit_code
        audit["outbound"] = {
            "dry_run": dry_run,
            "channel_send_captures": list(CAPTURED_SENDS),
            "capture_count": len(CAPTURED_SENDS),
        }
        _audit_mark(audit, "run_finished", exit_code=exit_code)
        if audit_json_path:
            try:
                write_flow_audit_json(audit, audit_json_path)
                print(f"Wrote audit JSON: {audit_json_path}")
            except Exception as exw:
                log.error("Failed to write audit JSON: %s", exw)
        for p in patches:
            try:
                p.stop()
            except Exception:
                pass

    return exit_code


def main() -> int:
    _configure_stdio_utf8()
    ap = argparse.ArgumentParser(
        description="Replay Instorebotforwarder flow for one or more Discord message links."
    )
    ap.add_argument(
        "--link",
        action="append",
        default=[],
        help="Discord message URL. Provide multiple times to test multiple links (one per profile).",
    )
    ap.add_argument(
        "--links",
        default="",
        help="Comma-separated Discord message links (alternative to repeated --link).",
    )
    ap.add_argument(
        "--links-env",
        default="INSTORE_FLOW_TEST_LINKS",
        help="Env var holding multiple links (newline or comma separated). Default: INSTORE_FLOW_TEST_LINKS",
    )
    ap.add_argument(
        "--live-send",
        action="store_true",
        help="Actually post to destination channels (default is dry-run: log sends only).",
    )
    ap.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG shows more from dependencies). Default: INFO",
    )
    ap.add_argument(
        "--brief",
        action="store_true",
        help="Skip pre-flight report and outbound preview (logs only).",
    )
    ap.add_argument(
        "--skip-gemini-api",
        action="store_true",
        help="In pre-flight only: do not call Gemini (still runs on actual forward).",
    )
    ap.add_argument(
        "--no-audit-json",
        action="store_true",
        help="Do not write the run audit trail JSON file.",
    )
    ap.add_argument(
        "--audit-json",
        nargs="?",
        const=True,
        default=None,
        metavar="PATH",
        help=(
            "Write full run audit to JSON (default: Mavelytest/audit_logs/instore_flow_<utc>_<message_id>.json). "
            "Pass a path to choose the file; use with no value for default location."
        ),
    )
    args = ap.parse_args()

    links: List[str] = []
    # 1) repeated --link
    if isinstance(args.link, list):
        links.extend([str(x).strip() for x in args.link if str(x).strip()])
    # 2) --links comma-separated
    if str(args.links or "").strip():
        links.extend([x.strip() for x in str(args.links).split(",") if x.strip()])
    # 3) env var multiple links
    env_name = str(args.links_env or "").strip()
    if env_name:
        raw_env = os.environ.get(env_name) or ""
        if str(raw_env).strip():
            parts = re.split(r"[\n,]+", raw_env)
            links.extend([x.strip() for x in parts if x.strip()])
    # 4) legacy single env
    if not links:
        legacy = (os.environ.get("INSTORE_FLOW_TEST_LINK") or "").strip()
        if legacy:
            links.append(legacy)

    if not links:
        print("Provide --link/--links, or set INSTORE_FLOW_TEST_LINK(_S) env var.", file=sys.stderr)
        return 2

    # Parse and validate all links first, so we fail early.
    parsed_links: List[Tuple[int, int, int, str]] = []
    for link in links:
        try:
            gid, cid, mid = parse_discord_message_link(link)
            parsed_links.append((gid, cid, mid, link))
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 2

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if str(args.log_level).upper() != "DEBUG":
        logging.getLogger("discord").setLevel(logging.WARNING)
        logging.getLogger("discord.http").setLevel(logging.WARNING)

    print("=" * 72)
    print("INSTORE MESSAGE FLOW TESTER")
    print("  guild_id:", gid)
    print("  channel_id:", cid)
    print("  message_id:", mid)
    print("  dry_run:", not bool(args.live_send))
    print("=" * 72)

    # Run each link in sequence so token usage + output is easy to read.
    # Overall exit code: 1 if any link resulted in a FAIL (exit_code=1), else 0.
    overall_exit = 0
    for gid, cid, mid, link in parsed_links:
        if args.no_audit_json:
            audit_path: Optional[Path] = None
        elif args.audit_json is None or args.audit_json is True:
            audit_path = _default_audit_json_path(mid)
        else:
            # If user passed an explicit path, try to do something sensible with multi-link runs.
            audit_path_str = str(args.audit_json)
            p = Path(audit_path_str)
            if len(parsed_links) == 1:
                audit_path = p
            else:
                # If PATH looks like a dir, put the per-message audit file inside it.
                if str(audit_path_str).endswith(("/", "\\")) or (p.exists() and p.is_dir()):
                    audit_path = p / f"instore_flow_{mid}.json"
                else:
                    # Otherwise treat it as a prefix or full filename and append _<mid>.json
                    # (keep extension if any, otherwise default to .json).
                    stem = p.stem
                    ext = p.suffix or ".json"
                    audit_path = p.with_name(f"{stem}_{mid}{ext}")

        overall_exit = max(
            overall_exit,
            asyncio.run(
                run_once(
                    guild_id=gid,
                    channel_id=cid,
                    message_id=mid,
                    message_link=link,
                    dry_run=not bool(args.live_send),
                    brief=bool(args.brief),
                    skip_gemini_api=bool(args.skip_gemini_api),
                    audit_json_path=audit_path,
                )
            ),
        )
    return overall_exit


if __name__ == "__main__":
    raise SystemExit(main())
