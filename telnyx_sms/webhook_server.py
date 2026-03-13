"""
Minimal webhook server for Telnyx inbound SMS.
Receives POSTs from Telnyx (message.received), appends to inbound.json.
Run this and point your Telnyx Messaging Profile webhook URL to your public URL (e.g. ngrok).
"""
from __future__ import annotations

import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            if k and v and k not in os.environ:
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                os.environ[k] = v

INBOUND_PATH = Path(__file__).resolve().parent / "inbound.json"
PORT = int(os.environ.get("TELNYX_WEBHOOK_PORT", "8765"))


def _append_inbound(entry: dict) -> None:
    entries = []
    if INBOUND_PATH.exists():
        try:
            entries = json.loads(INBOUND_PATH.read_text(encoding="utf-8"))
        except Exception:
            entries = []
    entries.append(entry)
    INBOUND_PATH.write_text(json.dumps(entries[-500:], indent=2), encoding="utf-8")


class TelnyxHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:
            self.send_response(400)
            self.end_headers()
            return
        event_type = (data.get("data") or {}).get("event_type")
        payload = (data.get("data") or {}).get("payload") or {}
        if event_type == "message.received":
            from_ = payload.get("from") or {}
            to_list = payload.get("to") or []
            to_num = to_list[0].get("phone_number", "—") if to_list else "—"
            entry = {
                "occurred_at": (data.get("data") or {}).get("occurred_at", ""),
                "from": from_.get("phone_number", "—"),
                "to": to_num,
                "text": payload.get("text", ""),
                "id": payload.get("id", ""),
            }
            _append_inbound(entry)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, format, *args):
        print(f"[Webhook] {args[0]}")


def main() -> None:
    server = HTTPServer(("", PORT), TelnyxHandler)
    print(f"Inbound webhook server listening on http://0.0.0.0:{PORT}")
    print("Set this URL in Telnyx Messaging Profile (use ngrok for local: ngrok http " + str(PORT) + ")")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    server.server_close()


if __name__ == "__main__":
    main()
