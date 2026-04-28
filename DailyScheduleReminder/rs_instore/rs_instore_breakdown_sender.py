"""
Parse an RS In-Store breakdown post and send condensed lines to two Discord channels.

Source text can be:
  - A Discord message jump link (discord.com / ptb.discord.com / canary — same IDs).
  - Legacy: JSON file with { "message": "<markdown>" } e.g. payload.json

Sending uses DailyScheduleReminder/manual_batch_send.py (--payload) with the same user token
as other DSR scripts (fetch uses mirror_message_to_m_lead token chain).

Examples (run from DailyScheduleReminder/rs_instore or repo root):
  py -3 rs_instore_breakdown_sender.py --url "https://ptb.discord.com/channels/G/C/M"
  py -3 rs_instore_breakdown_sender.py --payload-file payload.json --send
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_DSR_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _DSR_ROOT.parent

for _p in (_DSR_ROOT, _REPO_ROOT):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)


CONFIG_PATH = _SCRIPT_DIR / "config.json"
MANUAL_SENDER = _DSR_ROOT / "manual_batch_send.py"


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data if isinstance(data, dict) else {}


def extract_products(text: str) -> list[dict]:
    products: list[dict] = []
    pattern = re.findall(
        r"\* \[(.*?)\]\(<(.*?)>\)\s*\n\s*\* \*\*(.*?)\:\*\* `(\d+)`[^\n]*",
        text,
        flags=re.MULTILINE,
    )
    for name, url, id_type, value in pattern:
        products.append({"name": name, "url": url, "type": id_type, "value": value})
    return products


def extract_barcode_link(text: str) -> str | None:
    m = re.search(r"https://www\.barcodelookup\.com/(\d+)", text)
    if m:
        return f"https://www.barcodelookup.com/{m.group(1)}"
    return None


def _clean_md(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def message_body_for_breakdown(message: dict) -> str:
    """
    Prefer raw message content; if empty (embed-only posts), approximate markdown for parsing.
    """
    content = str(message.get("content") or "").strip()
    if content:
        return content

    chunks: list[str] = []
    embeds = message.get("embeds") or []
    if isinstance(embeds, list):
        for emb in embeds:
            if not isinstance(emb, dict):
                continue
            title = _clean_md(str(emb.get("title") or ""))
            desc = str(emb.get("description") or "").strip()
            if title:
                chunks.append(f"### **{title}**")
            if desc:
                chunks.append(desc)
            for f in emb.get("fields") or []:
                if not isinstance(f, dict):
                    continue
                nm = _clean_md(str(f.get("name") or ""))
                val = str(f.get("value") or "").strip()
                if nm or val:
                    chunks.append(f"* **{nm}:** {val}")
            author = emb.get("author") if isinstance(emb.get("author"), dict) else {}
            if isinstance(author, dict):
                an = _clean_md(str(author.get("name") or ""))
                au = str(author.get("url") or "").strip()
                if an and au:
                    chunks.append(f"* [{an}](<{au}>)")

    return "\n".join(chunks).strip()


def fetch_message_text_from_jump_url(url: str) -> str:
    from mirror_message_to_m_lead import fetch_message_with_token_fallback, parse_jump_url  # noqa: PLC0415

    guild_id, channel_id, message_id = parse_jump_url(url.strip())
    message, _channel, label, _tok = fetch_message_with_token_fallback(
        guild_id, channel_id, message_id
    )
    body = message_body_for_breakdown(message)
    if not body:
        raise RuntimeError(
            "Message has no content and no usable embed text "
            f"(token={label}). Paste markdown into payload.json instead."
        )
    return body


def build_messages(products: list[dict], barcode_link: str | None) -> tuple[str, str]:
    links_msg: list[str] = []
    ids_msg: list[str] = []

    for p in products:
        links_msg.append(f"{p['name']} → {p['url']}")
        ids_msg.append(f"{p['type']}: {p['value']}")

    if barcode_link:
        ids_msg.append(f"UPC Lookup → {barcode_link}")

    return "\n".join(links_msg), "\n".join(ids_msg)


def build_payload(config: dict, links_msg: str, ids_msg: str) -> dict:
    ch = config.get("channels")
    if not isinstance(ch, dict):
        raise ValueError('config.json must contain object "channels".')
    pl = ch.get("product_links")
    pi = ch.get("product_ids")
    if not pl or not pi:
        raise ValueError('config channels need "product_links" and "product_ids" (channel ids).')
    return {
        "sends": [
            {"channel_id": str(pl).strip(), "message": links_msg},
            {"channel_id": str(pi).strip(), "message": ids_msg},
        ],
        "delay_seconds": float(config.get("delay_seconds") or 0),
    }


def run_manual_sender(payload: dict, *, dry_run: bool) -> int:
    temp_file = _SCRIPT_DIR / "_temp_rs_instore_send.json"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        cmd = [
            sys.executable,
            str(MANUAL_SENDER),
            "--payload",
            str(temp_file.resolve()),
        ]
        if dry_run:
            cmd.append("--dry-run")

        return subprocess.run(cmd, cwd=str(_DSR_ROOT), env=os.environ.copy()).returncode
    finally:
        try:
            if temp_file.is_file():
                temp_file.unlink()
        except OSError:
            pass


def build_payload_from_source_text(text: str) -> dict:
    products = extract_products(text)
    barcode_link = extract_barcode_link(text)
    links_msg, ids_msg = build_messages(products, barcode_link)

    print("\n====== PREVIEW ======")
    print("\n--- PRODUCT LINKS ---")
    print(links_msg or "(none — check regex vs source markdown)")

    print("\n--- PRODUCT IDS ---")
    print(ids_msg or "(none)")

    if not products:
        print(
            "\nWARN: No product rows matched the expected pattern:\n"
            "  * [Store](<url>) then line with * **SKU:** `digits` (or PID, etc.)",
            file=sys.stderr,
        )

    config = load_json(CONFIG_PATH)
    return build_payload(config, links_msg, ids_msg)


def _prompt_url() -> str:
    print("Paste a Discord message link (discord.com or ptb.discord.com both work).")
    print("Blank line to exit.\n")
    raw = input("Message URL: ").strip()
    return raw


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="RS In-Store breakdown -> two-channel send (from jump URL or payload JSON)."
    )
    ap.add_argument(
        "--url",
        default="",
        help="Discord message jump URL (discord.com or ptb.discord.com, same ids).",
    )
    ap.add_argument(
        "--payload-file",
        type=Path,
        default=None,
        help='JSON with top-level string key "message" (legacy offline paste).',
    )
    ap.add_argument(
        "--send",
        action="store_true",
        help="Send immediately (no confirmation). Same user token as manual_batch_send.",
    )
    ap.add_argument(
        "--no-confirm",
        action="store_true",
        help="After preview, do not ask to send; exit after dry-run only.",
    )
    args = ap.parse_args(argv)

    text = ""
    url = (args.url or "").strip()

    if url:
        try:
            text = fetch_message_text_from_jump_url(url)
        except Exception as e:
            print(f"Fetch failed: {e}", file=sys.stderr)
            return 1
    elif args.payload_file is not None:
        pfile = Path(args.payload_file)
        if not pfile.is_file():
            print(f"Payload file not found: {pfile}", file=sys.stderr)
            return 2
        data = load_json(pfile)
        text = str(data.get("message") or "").strip()
        if not text:
            print('Payload JSON needs non-empty string "message".', file=sys.stderr)
            return 2
    else:
        url = _prompt_url().strip()
        if not url:
            print("Exiting.")
            return 0
        try:
            text = fetch_message_text_from_jump_url(url)
        except Exception as e:
            print(f"Fetch failed: {e}", file=sys.stderr)
            return 1

    payload = build_payload_from_source_text(text)

    if args.send:
        return run_manual_sender(payload, dry_run=False)

    rc = run_manual_sender(payload, dry_run=True)
    if rc != 0:
        return rc

    if args.no_confirm or not sys.stdin.isatty():
        return 0

    try:
        ans = input("\nSend these messages to Discord now? [y/N]: ").strip().lower()
    except EOFError:
        return 0
    if ans not in ("y", "yes"):
        return 0
    return run_manual_sender(payload, dry_run=False)


if __name__ == "__main__":
    raise SystemExit(main())
