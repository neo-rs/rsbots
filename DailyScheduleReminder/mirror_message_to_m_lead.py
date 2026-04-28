#!/usr/bin/env python3
"""
Fetch a Mirror World Discord message by jump link, parse a Deal-Soldier-style embed, print:

  !m lead <title> <MSRP> <As low as> <UPC|TCIN|SKU> <image url> <slug> <destination>

  Routes with \"command\": \"hdnation\" (e.g. hd-clearance restock) output instead:
  !m hdnation <SKU> <#destination>
  SKU is taken from embed fields \"SKU\" or \"Internet Number\".

  Home Depot: same !m lead — Deal Soldier uses \"Original Price\" / \"Price\" / UPC; Divine-style
  uses MSRP / As low as / SKU with title \"New Lead\" (product line taken from description).
  Plain message text may be digits-only SKU before the embed.

- Prices normalized to $X.XX; image prefers embed proxy_url.
- m_lead_routes.json maps Mirror World source channel_id -> m_lead_slug + destination_channel_id
  (the <#id> at the end of the !m line). mirror_forward_queue.py may POST that line to
  command_post_channel_id instead when set (see m_lead_routes.json _comment).
- User (non-bot) tokens: GET /channels/.../messages/{id} often returns 403 code 20002
  ("Only bots can use this endpoint"). We then use GET .../messages?around={id}&limit=9
  (channel history), which still works for normal user tokens.
- If the first token cannot read the channel at all, we retry MWDiscumBot tokens.env user token.

Batch forward (start link, then newer messages: send to RS, optional wait for monitor
post_confirmation in m_lead_routes.json, then react on Mirror): mirror_forward_queue.py

Auth: reminder_bot.load_token(); optional DISCUM_USER_DISCUMBOT in MWBots/MWDiscumBot/config/tokens.env.

Examples:
  py -3 mirror_message_to_m_lead.py "https://discord.com/channels/.../walmart-channel/..." 
  py -3 mirror_message_to_m_lead.py "https://..." --dest 999   # override RS destination
  py -3 mirror_message_to_m_lead.py --interactive
  py -3 mirror_message_to_m_lead.py --diagnose "https://discord.com/channels/.../msg"
      # prints HTTP matrix (@me, channel, direct message, around=). Token value never printed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Iterator
from urllib.parse import quote

_BOT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BOT_DIR.parent
for _p in (_REPO_ROOT, _BOT_DIR):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

import reminder_bot as _rb  # noqa: E402

try:
    import requests
except ImportError:
    print("Install requests:  py -3 -m pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROUTES_PATH = _BOT_DIR / "m_lead_routes.json"

# discord.com and discordapp.com; ptb / canary / www when copying from app or browser.
JUMP_RE = re.compile(
    r"https?://(?:ptb\.|canary\.|www\.)?discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)",
    re.IGNORECASE,
)


def load_m_lead_file() -> dict:
    """Full JSON object from m_lead_routes.json (routes + optional post_confirmation, etc.)."""
    if not ROUTES_PATH.is_file():
        return {}
    try:
        data = json.loads(ROUTES_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_routes() -> dict[str, dict]:
    data = load_m_lead_file()
    routes = data.get("routes")
    if not isinstance(routes, dict):
        return {}
    out: dict[str, dict] = {}
    for k, v in routes.items():
        if isinstance(v, dict) and str(k).isdigit():
            out[str(k)] = v
    return out


def _load_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip().lstrip("\ufeff")
            value = value.strip().strip('"').strip("'")
            if key:
                out[key] = value
    return out


def load_fetch_token_chain() -> list[tuple[str, str]]:
    """(label, token). DailyScheduleReminder first; MWDiscumBot user token second if different."""
    raw = _rb.load_token()
    primary = (raw if isinstance(raw, str) else str(raw or "")).strip()
    chain: list[tuple[str, str]] = []
    if primary:
        chain.append(("DailyScheduleReminder", primary))
    for cfg_dir in (_REPO_ROOT / "MWBots" / "MWDiscumBot" / "config", _REPO_ROOT / "MWDiscumBot" / "config"):
        env_path = cfg_dir / "tokens.env"
        if not env_path.is_file():
            continue
        env = {**os.environ, **_load_env_file(env_path)}
        t = (env.get("DISCUM_USER_DISCUMBOT") or env.get("DISCUM_BOT") or "").strip()
        if t and t != primary:
            chain.append(("MWDiscumBot tokens.env", t))
        break
    return chain


def parse_jump_url(url: str) -> tuple[str, str, str]:
    m = JUMP_RE.search((url or "").strip())
    if not m:
        raise ValueError(
            "Expected a Discord message link like "
            "https://discord.com/channels/<guild_id>/<channel_id>/<message_id>"
        )
    return m.group(1), m.group(2), m.group(3)


DISCORD_API_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# sha256 -> "raw" | "bearer" | "bot" — chosen by probing GET /users/@me once per token string.
_AUTH_MODE_CACHE: dict[str, str] = {}


def _token_fingerprint(token: str) -> str:
    return hashlib.sha256((token or "").strip().encode("utf-8", errors="ignore")).hexdigest()


def _probe_auth_mode(token: str) -> str:
    """
    Discord accepts different Authorization shapes. Probe GET /users/@me (no token printed).
    Order: raw user token, OAuth2 Bearer, Bot token.
    """
    t = (token or "").strip()
    if not t:
        return "raw"
    fp = _token_fingerprint(t)
    if fp in _AUTH_MODE_CACHE:
        return _AUTH_MODE_CACHE[fp]
    base_headers = {"Content-Type": "application/json", "User-Agent": DISCORD_API_UA}
    url = "https://discord.com/api/v10/users/@me"
    attempts: list[tuple[str, str]] = [
        ("raw_user", t),
        ("bearer", f"Bearer {t}"),
        ("bot", f"Bot {t}"),
    ]
    chosen = "raw"
    for _name, auth_val in attempts:
        r = requests.get(url, headers={**base_headers, "Authorization": auth_val}, timeout=15)
        if r.status_code == 200:
            if auth_val.startswith("Bearer "):
                chosen = "bearer"
            elif auth_val.startswith("Bot "):
                chosen = "bot"
            else:
                chosen = "raw"
            break
    _AUTH_MODE_CACHE[fp] = chosen
    return chosen


def discord_api_request_headers(token: str) -> dict[str, str]:
    t = (token or "").strip()
    if not t:
        raise ValueError("Empty Discord token (check DailyScheduleReminder/config.secrets.json or DISCORD_USER_TOKEN).")
    mode = _probe_auth_mode(t)
    if mode == "bot":
        auth = f"Bot {t}"
    elif mode == "bearer":
        auth = f"Bearer {t}"
    else:
        auth = t
    return {
        "Authorization": auth,
        "Content-Type": "application/json",
        "User-Agent": DISCORD_API_UA,
    }


def _request_with_transient_retry(
    do_request: Callable[[], requests.Response],
    *,
    max_attempts: int = 4,
) -> requests.Response:
    """
    Retry on dropped TLS / reset connections (e.g. WinError 10054) and timeouts.
    Backoff: 0.4s, 0.8s, 1.6s between attempts.
    """
    last: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return do_request()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            last = e
            if attempt + 1 >= max_attempts:
                raise
            time.sleep(0.4 * (2**attempt))
    assert last is not None
    raise last


def discord_get(url: str, token: str) -> requests.Response:
    """GET with probed Authorization mode (see _probe_auth_mode)."""
    return _request_with_transient_retry(
        lambda: requests.get(
            url, headers=discord_api_request_headers(token), timeout=20
        ),
    )


def discord_post(url: str, token: str, json_body: dict) -> requests.Response:
    """POST JSON with the same Authorization probing as discord_get."""
    return _request_with_transient_retry(
        lambda: requests.post(
            url,
            headers=discord_api_request_headers(token),
            json=json_body,
            timeout=30,
        ),
    )


def discord_patch(url: str, token: str, json_body: dict) -> requests.Response:
    """PATCH JSON with the same Authorization probing as discord_get."""
    return _request_with_transient_retry(
        lambda: requests.patch(
            url,
            headers=discord_api_request_headers(token),
            json=json_body,
            timeout=30,
        ),
    )


def discord_put(url: str, token: str) -> requests.Response:
    """PUT (e.g. add reaction) with the same Authorization probing as discord_get (no JSON body)."""
    def _put() -> requests.Response:
        h = dict(discord_api_request_headers(token))
        h.pop("Content-Type", None)
        return requests.put(url, headers=h, timeout=20)

    return _request_with_transient_retry(_put)


def run_diagnose(message_url: str) -> int:
    """
    Deterministic checks: for each token source, try raw / Bearer / Bot against @me,
    then (if URL given) channel + message. Never prints the token.
    """
    print("Discord API diagnosis (token strings are never printed)\n")
    url = (message_url or "").strip()
    cid = mid = ""
    if url:
        try:
            _gid, cid, mid = parse_jump_url(url)
        except ValueError as e:
            print(f"Bad message URL: {e}", file=sys.stderr)
            return 1

    for label, tok in load_fetch_token_chain():
        print(f"=== {label} ===")
        t = (tok or "").strip()
        if not t:
            print("  EMPTY: no token from this source.\n")
            continue
        dots = t.count(".")
        print(f"  token length={len(t)}  dot_count={dots}")
        base_h = {"Content-Type": "application/json", "User-Agent": DISCORD_API_UA}
        me_url = "https://discord.com/api/v10/users/@me"
        modes = [
            ("raw_user", t),
            ("bearer", f"Bearer {t}"),
            ("bot", f"Bot {t}"),
        ]
        for mname, auth in modes:
            r = requests.get(me_url, headers={**base_h, "Authorization": auth}, timeout=15)
            tail = (r.text or "").replace("\n", " ")[:200]
            print(f"  GET /users/@me   [{mname:10}] -> HTTP {r.status_code}  {tail}")
        if cid and mid:
            ch_url = f"https://discord.com/api/v10/channels/{cid}"
            msg_url = f"{ch_url}/messages/{mid}"
            for mname, auth in modes:
                h = {**base_h, "Authorization": auth}
                rc = requests.get(ch_url, headers=h, timeout=15)
                rm = requests.get(msg_url, headers=h, timeout=15)
                tc = (rc.text or "").replace("\n", " ")[:120]
                tm = (rm.text or "").replace("\n", " ")[:120]
                print(f"  GET /channels/{{id}}     [{mname:10}] -> HTTP {rc.status_code}  {tc}")
                print(f"  GET /channels/.../msg    [{mname:10}] -> HTTP {rm.status_code}  {tm}")
            hraw = {**base_h, "Authorization": t}
            rar = requests.get(
                f"https://discord.com/api/v10/channels/{cid}/messages?around={mid}&limit=9",
                headers=hraw,
                timeout=15,
            )
            ta = (rar.text or "").replace("\n", " ")[:140]
            print(f"  GET /messages?around=    [raw_user  ] -> HTTP {rar.status_code:3}  {ta}")
        print()
    print(
        "Interpretation:\n"
        "  - Exactly one @me row should be HTTP 200. That is the correct Authorization style.\n"
        "  - If direct single-message GET is 403 code 20002 but ?around= is 200, this script uses around= automatically.\n"
        "  - If all @me are 401, the token string is invalid for the API.\n"
    )
    return 0


def normalize_price(raw: str) -> str:
    s = (raw or "").strip().replace(",", "")
    s = re.sub(r"^[*_`]+|[*_`]+$", "", s)
    if not s:
        return "$0.00"
    low = s.lower()
    if low in ("free", "n/a", "na", "—", "-"):
        return s
    if not s.startswith("$"):
        s = "$" + s
    num = s[1:].strip()
    if not num:
        return "$0.00"
    if num.startswith("."):
        num = "0" + num
    try:
        v = float(num)
    except ValueError:
        return raw.strip()
    return f"${v:.2f}"


def _field_map(embed: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for f in embed.get("fields") or []:
        if not isinstance(f, dict):
            continue
        name = re.sub(r"\s+", " ", (f.get("name") or "").strip().lower())
        val = (f.get("value") or "").strip()
        if name:
            out[name] = val
    return out


def _get_field(fm: dict[str, str], *names: str) -> str:
    for n in names:
        if n in fm:
            return fm[n]
    for k, v in fm.items():
        for n in names:
            if n in k:
                return v
    return ""


def _lead_product_id_raw(fm: dict[str, str]) -> str:
    """Deal-Soldier / Divine style: UPC, or Target TCIN, or SKU (same !m lead slot)."""
    return (
        (_get_field(fm, "upc") or "").strip()
        or (_get_field(fm, "tcin") or "").strip()
        or (_get_field(fm, "sku") or "").strip()
    )


_LEAD_GENERIC_TITLES = frozenset(
    {
        "new lead",
        "deal",
        "lead",
        "store clearance deal",
        "clearance deal",
        "home depot store clearance deals - new item",
        "home depot store clearance deals \u2013 new item",
    }
)


def _first_meaningful_description_line(description: str) -> str:
    """First embed description line that looks like a product name (not MSRP/URLs)."""
    for line in (description or "").replace("\r\n", "\n").split("\n"):
        line = re.sub(r"^[*_`\s]+|[*_`\s]+$", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if len(line) < 6:
            continue
        low = line.lower()
        if low.startswith("msrp") or low.startswith("from:") or low.startswith("http"):
            continue
        if re.match(r"^\$[\d.,]", line):
            continue
        if re.fullmatch(r"\d[\d,\s]*", line.replace(",", "")):
            continue
        return line
    return ""


def _sku_digits_from_message_content(message: dict | None) -> str:
    """e.g. Divine posts SKU alone in message body before the embed."""
    if not isinstance(message, dict):
        return ""
    raw = (message.get("content") or "").strip()
    if not raw:
        return ""
    m = re.match(r"^\s*(\d{5,14})\s*$", raw)
    if m:
        return m.group(1)
    for tok in raw.split():
        t = tok.strip()
        if re.fullmatch(r"\d{5,14}", t):
            return t
    return ""


def _lead_display_title(embed: dict, fm: dict[str, str]) -> str:
    """Use embed title, or description first line when title is generic ('New Lead', etc.)."""
    t = re.sub(r"\s+", " ", (embed.get("title") or "").strip())
    if t and t.lower() not in _LEAD_GENERIC_TITLES:
        return t
    cand = _first_meaningful_description_line(str(embed.get("description") or ""))
    if cand:
        return cand
    return t


def _normalize_lead_product_code(raw: str) -> str:
    s = re.sub(r"^[*_`]+|[*_`]+$", "", (raw or "").strip())
    digits = re.sub(r"\D", "", s)
    if digits:
        return digits
    return re.sub(r"[^0-9A-Za-z]", "", s)


def _best_media_url(block: object) -> str:
    if not isinstance(block, dict):
        return ""
    return (block.get("proxy_url") or block.get("url") or "").strip()


def embed_image_url(embed: dict) -> str:
    u = _best_media_url(embed.get("image"))
    if u:
        return u
    return _best_media_url(embed.get("thumbnail"))


def route_command(route: dict | None) -> str:
    if not route:
        return "lead"
    c = str(route.get("command") or "lead").strip().lower()
    return c if c else "lead"


def pick_hd_clearance_embed(message: dict) -> dict:
    """FLIPFLUENCE-style embed: fields SKU / Internet Number."""
    embeds = message.get("embeds") or []
    if not isinstance(embeds, list) or not embeds:
        raise ValueError("Message has no embeds.")
    for e in embeds:
        if not isinstance(e, dict):
            continue
        fm = _field_map(e)
        v = (_get_field(fm, "sku") or _get_field(fm, "internet number") or "").strip()
        if v:
            return e
    e0 = embeds[0]
    if isinstance(e0, dict):
        return e0
    raise ValueError("No usable embed for hdnation (need SKU or Internet Number field).")


def pick_embed_for_route(message: dict, route: dict | None) -> dict:
    if route_command(route) == "hdnation":
        return pick_hd_clearance_embed(message)
    return pick_product_embed(message)


def extract_hdnation_sku(embed: dict) -> str:
    fm = _field_map(embed)
    raw = (_get_field(fm, "sku") or _get_field(fm, "internet number") or "").strip()
    raw = re.sub(r"^[*_`]+|[*_`]+$", "", raw)
    if not raw:
        raise ValueError('Could not find "SKU" or "Internet Number" in embed.')
    sku = re.sub(r"\D", "", raw)
    if sku:
        return sku
    return raw.replace(" ", "")


def pick_product_embed(message: dict) -> dict:
    embeds = message.get("embeds") or []
    if not isinstance(embeds, list) or not embeds:
        raise ValueError("Message has no embeds.")
    best: dict | None = None
    for e in embeds:
        if not isinstance(e, dict):
            continue
        fm = _field_map(e)
        pid_raw = _lead_product_id_raw(fm)
        if pid_raw and (e.get("title") or embed_image_url(e)):
            return e
        h_msrp = _get_field(fm, "msrp") or _get_field(fm, "original price")
        h_low = _get_field(fm, "as low as") or (h_msrp and (fm.get("price") or "").strip())
        if h_msrp and h_low and pid_raw:
            best = e
    if best:
        return best
    e0 = embeds[0]
    if isinstance(e0, dict):
        return e0
    raise ValueError("No usable embed found.")


def extract_lead_parts(embed: dict, message: dict | None = None) -> dict[str, str]:
    fm = _field_map(embed)
    title = _lead_display_title(embed, fm)
    msrp_raw = _get_field(fm, "msrp") or _get_field(fm, "original price")
    low_raw = _get_field(fm, "as low as")
    if not low_raw and msrp_raw:
        # Deal Soldier Home Depot store clearance: exact "Price" (not substring on "Original Price")
        low_raw = (fm.get("price") or "").strip()
    upc_raw = _lead_product_id_raw(fm)
    if not (upc_raw or "").strip():
        upc_raw = _sku_digits_from_message_content(message)
    upc = _normalize_lead_product_code(upc_raw)
    img = embed_image_url(embed)
    if not title:
        raise ValueError("Embed has no usable title (and no product line in description).")
    if not msrp_raw:
        raise ValueError("Could not find MSRP or Original Price field.")
    if not low_raw:
        raise ValueError('Could not find "As low as" or clearance "Price" field.')
    if not upc:
        raise ValueError("Could not find UPC, TCIN, or SKU field.")
    if not img:
        raise ValueError("Embed has no image or thumbnail URL.")
    return {
        "title": title,
        "msrp": normalize_price(msrp_raw),
        "as_low_as": normalize_price(low_raw),
        "upc": upc,
        "image": img,
    }


def channel_name_to_slug(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("┃", "|").replace("│", "|").replace("︱", "|")
    if "|" in s:
        s = s.split("|")[-1].strip()
    s = re.sub(r"^[^a-z0-9]+", "", s)
    s = re.sub(r"[^a-z0-9-]+", "-", s).strip("-")
    return s or "unknown"


def format_destination(dest: str) -> str:
    d = (dest or "").strip()
    m = re.fullmatch(r"<#(\d+)>", d)
    if m:
        return f"<#{m.group(1)}>"
    if re.fullmatch(r"\d{5,25}", d):
        return f"<#{d}>"
    return d


def build_m_lead_line(parts: dict[str, str], source_slug: str, destination: str) -> str:
    dest = format_destination(destination)
    return (
        f"!m lead {parts['title']} {parts['msrp']} {parts['as_low_as']} {parts['upc']} "
        f"{parts['image']} {source_slug} {dest}"
    )


def build_hdnation_line(sku: str, destination: str) -> str:
    dest = format_destination(destination)
    return f"!m hdnation {sku} {dest}"


def _discord_error_body(resp: requests.Response) -> dict:
    try:
        j = resp.json()
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


def _is_only_bots_single_message_403(resp: requests.Response) -> bool:
    """Discord returns 403 + code 20002 for user tokens on GET .../messages/{message_id}."""
    if resp.status_code != 403:
        return False
    j = _discord_error_body(resp)
    if int(j.get("code") or 0) == 20002:
        return True
    return "only bots" in str(j.get("message") or "").lower()


def _fetch_message_payload(
    channel_id: str, message_id: str, token: str,
) -> tuple[dict | None, requests.Response]:
    """
    Return (message dict, last_http_response). Uses direct GET first, then ?around= for user tokens.
    """
    base = "https://discord.com/api/v10"
    direct = f"{base}/channels/{channel_id}/messages/{message_id}"
    r = discord_get(direct, token)
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict) and data.get("id"):
            return data, r
    if _is_only_bots_single_message_403(r):
        print(
            "(Discord code 20002 on single-message GET; loading the same message via ?around= history.)",
            file=sys.stderr,
        )
        around = f"{base}/channels/{channel_id}/messages?around={message_id}&limit=9"
        r2 = discord_get(around, token)
        if r2.status_code == 200:
            arr = r2.json()
            if isinstance(arr, list):
                for m in arr:
                    if isinstance(m, dict) and str(m.get("id")) == str(message_id):
                        return m, r2
        return None, r2
    return None, r


def run_fetch(guild_id: str, channel_id: str, message_id: str, token: str) -> tuple[dict, dict]:
    base = "https://discord.com/api/v10"
    ch_url = f"{base}/channels/{channel_id}"
    r_ch = discord_get(ch_url, token)
    channel: dict = {}
    if r_ch.status_code == 200:
        ch_body = r_ch.json()
        if isinstance(ch_body, dict):
            channel = ch_body

    message, r_msg = _fetch_message_payload(channel_id, message_id, token)
    if isinstance(message, dict) and message.get("id"):
        return message, channel

    if r_msg.status_code == 401:
        raise RuntimeError(
            "DISCORD_READ_401:"
            f"401 on message fetch (channel_id={channel_id}). Body: {(r_msg.text or '')[:280]}"
        )
    if r_msg.status_code == 404:
        raise RuntimeError("404: message or channel not found.")
    if r_msg.status_code == 403:
        raise RuntimeError(
            "DISCORD_READ_403:"
            f"cannot read message in channel_id={channel_id}. "
            f"HTTP {(r_msg.text or '')[:240]}"
        )
    if r_msg.status_code == 200:
        raise RuntimeError(
            f"Message id {message_id} not present in ?around= batch (deleted or wrong channel?)."
        )
    raise RuntimeError(f"Discord API error {r_msg.status_code}: {(r_msg.text or '')[:300]}")


def fetch_message_with_token_fallback(
    guild_id: str, channel_id: str, message_id: str,
) -> tuple[dict, dict, str, str]:
    """Return (message, channel, token_label_used, token_string_used)."""
    chain = load_fetch_token_chain()
    last_err: str | None = None
    for label, tok in chain:
        try:
            msg, ch = run_fetch(guild_id, channel_id, message_id, tok)
            if label != chain[0][0]:
                print(
                    f"\n(Read OK using {label} - first token could not read this Mirror World channel.)\n"
                )
            return msg, ch, label, tok
        except RuntimeError as e:
            es = str(e)
            if es.startswith("DISCORD_READ_403:") or es.startswith("DISCORD_READ_401:"):
                last_err = es
                print(f"[{label}] {es.split(':', 1)[-1].strip()}", file=sys.stderr)
                continue
            raise
    raise RuntimeError(
        last_err or "Could not read message with any configured token (see lines above)."
    )


def list_messages_after(
    channel_id: str,
    after_message_id: str,
    token: str,
    *,
    limit: int = 100,
) -> list[dict]:
    """Messages strictly newer than after_message_id (sorted ascending by snowflake)."""
    lim = max(1, min(100, int(limit)))
    url = (
        f"https://discord.com/api/v10/channels/{channel_id}/messages"
        f"?after={after_message_id}&limit={lim}"
    )
    r = discord_get(url, token)
    if r.status_code != 200:
        raise RuntimeError(
            f"list_messages_after HTTP {r.status_code}: {(r.text or '')[:280]}"
        )
    data = r.json()
    if not isinstance(data, list):
        return []
    out = [m for m in data if isinstance(m, dict) and m.get("id")]
    out.sort(key=lambda m: int(str(m.get("id") or 0)))
    return out


def iter_channel_forward_from_start(
    start_message: dict,
    channel_id: str,
    token: str,
    *,
    max_messages: int,
) -> Iterator[dict]:
    """
    Yield start_message first, then channel messages with id greater than start, oldest-first
    per page, until max_messages total yields or no more history.

    max_messages <= 0 means no cap (walk until Discord returns no newer messages).
    """
    unlimited = max_messages <= 0
    if not unlimited and max_messages < 1:
        return
    yield start_message
    if not unlimited and max_messages == 1:
        return
    after = str(start_message.get("id") or "")
    if not after.isdigit():
        return
    count = 1
    after_i = int(after)
    while unlimited or count < max_messages:
        if unlimited:
            page_lim = 100
        else:
            remaining = max_messages - count
            page_lim = min(100, remaining)
        try:
            batch = list_messages_after(channel_id, after, token, limit=page_lim)
        except RuntimeError as e:
            raise RuntimeError(f"While listing after message_id={after}: {e}") from e
        if not batch:
            break
        max_id_this_page = 0
        advanced = False
        for m in batch:
            mid = int(str(m.get("id") or 0))
            if mid <= after_i:
                continue
            yield m
            count += 1
            max_id_this_page = max(max_id_this_page, mid)
            advanced = True
            if not unlimited and count >= max_messages:
                return
        if not advanced or max_id_this_page <= after_i:
            break
        after = str(max_id_this_page)
        after_i = max_id_this_page
        if len(batch) < page_lim:
            break


def build_command_line_for_route(
    message: dict,
    route: dict | None,
    *,
    channel: dict | None = None,
    dest_override: str = "",
    source_slug_override: str = "",
) -> str:
    """
    Full !m lead or !m hdnation line for this message and route.
    Raises ValueError if embed cannot be parsed.
    """
    cmd = route_command(route)
    dest_raw = (dest_override or "").strip()
    if not dest_raw and route and str(route.get("destination_channel_id") or "").strip():
        dest_raw = str(route["destination_channel_id"]).strip()
    if not dest_raw:
        raise ValueError("No RS destination (route missing destination_channel_id and no override).")
    embed = pick_embed_for_route(message, route)
    ch = channel if isinstance(channel, dict) else {}
    slug = resolve_slug(route, ch, source_slug_override)
    if cmd == "hdnation":
        sku = extract_hdnation_sku(embed)
        return build_hdnation_line(sku, dest_raw)
    parts = extract_lead_parts(embed, message)
    return build_m_lead_line(parts, slug, dest_raw)


def message_dedupe_key(message: dict, route: dict | None) -> str | None:
    """
    Stable product key for consecutive duplicate detection: UPC for lead, SKU for hdnation.
    None if this message cannot be parsed as a deal for the route's command style.
    """
    try:
        cmd = route_command(route)
        embed = pick_embed_for_route(message, route)
        if cmd == "hdnation":
            return f"hdnation:{extract_hdnation_sku(embed)}"
        parts = extract_lead_parts(embed, message)
        return f"lead:{parts['upc']}"
    except ValueError:
        return None


def post_channel_message(
    destination_channel_id: str,
    content: str,
    token: str,
) -> requests.Response:
    """POST a single chat message (content) to the channel."""
    url = f"https://discord.com/api/v10/channels/{destination_channel_id.strip()}/messages"
    return discord_post(url, token, {"content": content})


def post_channel_message_reply(
    channel_id: str,
    guild_id: str,
    reply_to_message_id: str,
    content: str,
    token: str,
) -> requests.Response:
    """POST a message in channel_id that replies to reply_to_message_id (Discord message reference)."""
    url = f"https://discord.com/api/v10/channels/{channel_id.strip()}/messages"
    body: dict = {
        "content": content,
        "message_reference": {
            "message_id": str(reply_to_message_id).strip(),
            "channel_id": str(channel_id).strip(),
            "guild_id": str(guild_id).strip(),
        },
    }
    return discord_post(url, token, body)


def _message_author_user_id(message: dict) -> str:
    a = message.get("author")
    if not isinstance(a, dict):
        return ""
    return str(a.get("id") or "").strip()


def _message_text_blob(msg: dict) -> str:
    """Lowercased text from content + embeds (title, description, fields, footer) for substring search."""
    parts: list[str] = []
    c = msg.get("content")
    if isinstance(c, str):
        parts.append(c)
    for emb in msg.get("embeds") or []:
        if not isinstance(emb, dict):
            continue
        for k in ("title", "description"):
            t = emb.get(k)
            if isinstance(t, str):
                parts.append(t)
        for f in emb.get("fields") or []:
            if isinstance(f, dict):
                parts.append(str(f.get("name") or ""))
                parts.append(str(f.get("value") or ""))
        foot = emb.get("footer")
        if isinstance(foot, dict) and isinstance(foot.get("text"), str):
            parts.append(foot["text"])
    return "\n".join(parts).lower()


def message_matches_post_confirmation(
    message: dict,
    *,
    author_user_id: str,
    text_substring: str,
) -> bool:
    """True if message is from author_user_id and substring appears in content or embed text."""
    aid = (author_user_id or "").strip()
    if not aid or _message_author_user_id(message) != aid:
        return False
    needle = (text_substring or "").strip().lower()
    if not needle:
        return False
    content = (message.get("content") or "").lower()
    if needle in content:
        return True
    for emb in message.get("embeds") or []:
        if not isinstance(emb, dict):
            continue
        chunks: list[str] = []
        for k in ("title", "description"):
            t = emb.get(k)
            if isinstance(t, str):
                chunks.append(t)
        foot = emb.get("footer")
        if isinstance(foot, dict):
            ft = foot.get("text")
            if isinstance(ft, str):
                chunks.append(ft)
        for t in chunks:
            if needle in t.lower():
                return True
    return False


def wait_for_monitor_outcome(
    observation_channel_id: str,
    after_message_id: str,
    token: str,
    *,
    author_user_id: str,
    success_needles: list[str],
    failure_needles: list[str],
    maintenance_start_needles: list[str] | None = None,
    maintenance_done_needles: list[str] | None = None,
    maintenance_extend_seconds: float = 600.0,
    timeout_seconds: float,
    poll_interval_seconds: float = 2.0,
) -> tuple[str, str]:
    """
    Poll for the monitor bot's next message(s) after after_message_id.

    For each new message from author_user_id (oldest first), checks failure_needles in the
    full text blob first, then success_needles.

    Returns:
      ("ok", "") — matched a success substring
      ("fail", reason) — matched a failure substring
      ("timeout", reason) — deadline elapsed
      ("error", reason) — bad args or unusable config
    """
    aid = str(author_user_id or "").strip()
    after = str(after_message_id or "").strip()
    succ = [s.strip().lower() for s in success_needles if (s or "").strip()]
    fail = [s.strip().lower() for s in failure_needles if (s or "").strip()]
    maint_start = [
        s.strip().lower()
        for s in (maintenance_start_needles or [])
        if (s or "").strip()
    ]
    maint_done = [
        s.strip().lower()
        for s in (maintenance_done_needles or [])
        if (s or "").strip()
    ]
    if not aid or not after.isdigit():
        return "error", "missing author_user_id or after_message_id"
    if not succ:
        return "error", "no success_substrings"
    timeout = max(1.0, float(timeout_seconds))
    poll = max(0.4, float(poll_interval_seconds))
    deadline = time.monotonic() + timeout
    maint_extend = max(0.0, float(maintenance_extend_seconds))
    maint_active_until = 0.0
    base = f"https://discord.com/api/v10/channels/{observation_channel_id.strip()}/messages"
    after_i = int(after)
    while time.monotonic() < deadline:
        url = f"{base}?after={after}&limit=100"
        r = discord_get(url, token)
        if r.status_code != 200:
            time.sleep(poll)
            continue
        data = r.json()
        if not isinstance(data, list):
            time.sleep(poll)
            continue
        candidates = [m for m in data if isinstance(m, dict) and m.get("id")]
        candidates.sort(key=lambda m: int(str(m.get("id") or 0)))
        for m in candidates:
            mid = int(str(m.get("id") or 0))
            if mid <= after_i:
                continue
            if _message_author_user_id(m) != aid:
                continue
            blob = _message_text_blob(m)
            # Maintenance / update mode: Tempo assistant can post "being updated" then later "has been updated".
            # When we see the start needle, extend the deadline and keep waiting instead of timing out.
            if maint_start and any(x in blob for x in maint_start):
                now = time.monotonic()
                maint_active_until = max(maint_active_until, now + maint_extend)
                deadline = max(deadline, maint_active_until)
                continue
            if maint_done and any(x in blob for x in maint_done):
                maint_active_until = 0.0
            for fneedle in fail:
                if fneedle in blob:
                    return "fail", f"monitor: matched {fneedle!r}"
            for sneedle in succ:
                if sneedle in blob:
                    return "ok", ""
        # If we're in maintenance window, wait it out (deadline already extended).
        time.sleep(poll)
    return "timeout", f"timeout after {timeout:.0f}s waiting for monitor"


def wait_for_post_confirmation(
    observation_channel_id: str,
    after_message_id: str,
    token: str,
    *,
    author_user_id: str,
    text_substring: str = "Lead posted",
    timeout_seconds: float = 120.0,
    poll_interval_seconds: float = 2.0,
) -> tuple[bool, str]:
    """
    Poll observation_channel_id for a message newer than after_message_id from author_user_id
    that contains text_substring (case-insensitive) in body or embed title/description/footer.
    Returns (True, "") on success, (False, reason) on timeout or repeated HTTP errors.
    """
    needle = (text_substring or "Lead posted").strip().lower()
    outcome, detail = wait_for_monitor_outcome(
        observation_channel_id,
        after_message_id,
        token,
        author_user_id=author_user_id,
        success_needles=[needle],
        failure_needles=[],
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    if outcome == "ok":
        return True, ""
    return False, detail or outcome


def add_message_reaction(
    mirror_channel_id: str,
    mirror_message_id: str,
    token: str,
    emoji: str = "\u2705",
) -> requests.Response:
    """PUT /reactions/{emoji}/@me on a message (default: check mark)."""
    enc = quote(emoji, safe="")
    base = "https://discord.com/api/v10"
    rurl = (
        f"{base}/channels/{mirror_channel_id.strip()}/messages/"
        f"{mirror_message_id.strip()}/reactions/{enc}/@me"
    )
    return discord_put(rurl, token)


def route_for_channel(routes: dict[str, dict], channel_id: str) -> dict | None:
    r = routes.get(str(channel_id))
    return r if isinstance(r, dict) else None


def resolve_slug(
    route: dict | None,
    channel: dict,
    source_slug_override: str,
) -> str:
    if source_slug_override.strip():
        return source_slug_override.strip()
    if route and str(route.get("m_lead_slug") or "").strip():
        return str(route["m_lead_slug"]).strip()
    return channel_name_to_slug(str((channel or {}).get("name") or ""))


def resolve_destination_cli(
    route: dict | None,
    dest_override: str,
    channel_id: str,
    routes: dict[str, dict],
) -> str:
    """Non-interactive: --dest or route entry required."""
    if dest_override.strip():
        return dest_override.strip()
    if route and str(route.get("destination_channel_id") or "").strip():
        return str(route["destination_channel_id"]).strip()
    keys = ", ".join(sorted(routes.keys())) if routes else "(none)"
    raise ValueError(
        f"No --dest and no route for channel_id={channel_id}. "
        f"Add it to m_lead_routes.json or pass --dest. Known route channel ids: {keys}"
    )


def print_message_preview(
    parts: dict[str, str] | None,
    *,
    mode: str,
    channel_name: str,
    channel_id: str,
    route: dict | None,
    dest_raw: str,
    slug: str,
    sku: str = "",
) -> None:
    print()
    print("=" * 60)
    print("  MESSAGE / EMBED (from link)")
    print("=" * 60)
    print(f"  Mirror World channel: #{channel_name}  (id {channel_id})")
    if route:
        ml = route.get("mirror_label") or route.get("m_lead_slug") or ""
        print(f"  Store route:        {ml} (m_lead_routes.json)")
    else:
        print("  Store route:        (no entry for this channel id - slug from channel name)")
    print()
    dest_line = format_destination(dest_raw) if dest_raw.strip() else "(not set yet - will prompt below)"
    if (mode or "lead").lower() == "hdnation":
        print("  Output format:      !m hdnation <SKU> <#destination>")
        print(f"  SKU:                {sku}")
        print(f"  RS destination:     {dest_line}")
        print("=" * 60)
        print()
        return
    assert parts is not None
    img_preview = parts["image"]
    if len(img_preview) > 120:
        img_preview = img_preview[:120] + "..."
    print(f"  Title:     {parts['title']}")
    print(f"  MSRP:      {parts['msrp']}")
    print(f"  As low as: {parts['as_low_as']}")
    print(f"  UPC/ID:    {parts['upc']}")
    print(f"  Image:     {img_preview}")
    print()
    print(f"  !m lead slug:      {slug}")
    print(f"  RS destination:   {dest_line}")
    print("=" * 60)
    print()


def _prompt_yes(prompt: str, default_yes: bool = True) -> bool:
    suffix = " [Y/n]: " if default_yes else " [y/N]: "
    raw = input(prompt + suffix).strip().lower()
    if not raw:
        return default_yes
    return raw in ("y", "yes")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build RS command from a Mirror World message link (!m lead or !m hdnation per m_lead_routes.json)."
    )
    ap.add_argument("url", nargs="?", default="", help="Discord message URL")
    ap.add_argument(
        "--dest",
        default="",
        help="Override RS destination (numeric id or <#id>). If omitted, uses m_lead_routes.json for known MW channels.",
    )
    ap.add_argument(
        "--inspect",
        action="store_true",
        help="Print JSON extract (no confirmation).",
    )
    ap.add_argument("--interactive", action="store_true", help="Prompt for link; show preview; confirm.")
    ap.add_argument(
        "--source-slug",
        default="",
        help="Override !m lead slug (default: route or channel name).",
    )
    ap.add_argument(
        "--verify-token",
        action="store_true",
        help="GET /users/@me for each token in the chain (debug which account Discord sees).",
    )
    ap.add_argument(
        "--diagnose",
        nargs="?",
        const="",
        default=None,
        metavar="MESSAGE_URL",
        help="Print HTTP status for raw vs Bearer vs Bot auth (@me, and optional channel+message URL).",
    )
    args = ap.parse_args()

    if args.verify_token:
        ok = True
        for label, tok in load_fetch_token_chain():
            r = discord_get("https://discord.com/api/v10/users/@me", tok)
            snippet = (r.text or "")[:240].replace("\n", " ")
            print(f"{label}: HTTP {r.status_code}  {snippet}")
            if r.status_code != 200:
                ok = False
        return 0 if ok else 1

    if args.diagnose is not None:
        return run_diagnose(args.diagnose)

    url = (args.url or "").strip()
    dest_override = (args.dest or "").strip()

    if args.interactive:
        print("Mirror World message -> RS command (!m lead or !m hdnation) [same token as manual send]\n")
        print("Paste the jump link to the deal message (Mirror World).\n")
        url = input("Message link: ").strip()

    if not url:
        ap.print_help()
        print("\nProvide a message URL, or use --interactive.", file=sys.stderr)
        return 1

    try:
        guild_id, channel_id, message_id = parse_jump_url(url)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1

    routes = load_routes()
    route = route_for_channel(routes, channel_id)

    if not load_fetch_token_chain():
        print(
            "No Discord token: configure DailyScheduleReminder/config.secrets.json or DISCORD_USER_TOKEN.",
            file=sys.stderr,
        )
        return 1

    try:
        message, channel, _used_label, _used_token = fetch_message_with_token_fallback(
            guild_id, channel_id, message_id
        )
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Request failed: {e}", file=sys.stderr)
        return 1

    cmd = route_command(route)
    sku = ""
    parts: dict[str, str] | None = None
    try:
        embed = pick_embed_for_route(message, route)
        if cmd == "hdnation":
            sku = extract_hdnation_sku(embed)
        else:
            parts = extract_lead_parts(embed, message)
    except ValueError as e:
        print(f"Parse error: {e}", file=sys.stderr)
        return 1

    ch_name = str((channel or {}).get("name") or "unknown")
    slug = resolve_slug(route, channel, args.source_slug)

    def _dest_from_override_or_route() -> str:
        if dest_override:
            return dest_override
        if route and str(route.get("destination_channel_id") or "").strip():
            return str(route["destination_channel_id"]).strip()
        return ""

    if args.inspect:
        dest_raw = _dest_from_override_or_route()
        out: dict = {
            "channel_name": ch_name,
            "channel_id": channel_id,
            "guild_id": guild_id,
            "message_id": message_id,
            "route": route,
            "command": cmd,
            "m_lead_slug": slug,
            "destination_resolved": dest_raw or None,
        }
        if cmd == "hdnation":
            out["sku"] = sku
        else:
            assert parts is not None
            out.update(parts)
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    dest_raw = ""
    if args.interactive:
        dest_raw = _dest_from_override_or_route()
        print_message_preview(
            parts,
            mode=cmd,
            channel_name=ch_name,
            channel_id=channel_id,
            route=route,
            dest_raw=dest_raw,
            slug=slug,
            sku=sku,
        )
        if not dest_raw.strip():
            print(
                f"No route for channel_id={channel_id} in m_lead_routes.json. "
                "Enter RS destination channel id or <#id>:"
            )
            dest_raw = input().strip()
        if not dest_raw.strip():
            print("Cancelled: need RS destination.", file=sys.stderr)
            return 1
        ov = input("Override RS destination (Enter to keep): ").strip()
        if ov:
            dest_raw = ov
        if not _prompt_yes("Print command line?", default_yes=True):
            print("Cancelled.")
            return 0
    else:
        try:
            dest_raw = resolve_destination_cli(route, dest_override, channel_id, routes)
        except ValueError as e:
            print(e, file=sys.stderr)
            return 1

    if cmd == "hdnation":
        line = build_hdnation_line(sku, dest_raw)
    else:
        assert parts is not None
        line = build_m_lead_line(parts, slug, dest_raw)
    print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
