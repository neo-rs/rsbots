import os
import logging
import requests
from flask import Flask, request, jsonify

from mavely_client import MavelyClient

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return default

LOG_LEVEL = _env("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("app")

MAKE_WEBHOOK_URL = _env("MAKE_WEBHOOK_URL", "").strip()

client = MavelyClient(
    session_token=_env("MAVELY_COOKIES", ""),
    timeout_s=_env_int("REQUEST_TIMEOUT", 20),
    max_retries=_env_int("MAX_RETRIES", 3),
    min_seconds_between_requests=_env_float("MIN_SECONDS_BETWEEN_REQUESTS", 2.0),
)

app = Flask(__name__)

@app.get("/")
def root():
    return "Status: Online", 200

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "has_session_token": bool(_env("MAVELY_COOKIES", "").strip()),
        "make_webhook_configured": bool(MAKE_WEBHOOK_URL),
    })

@app.post("/generate")
def generate():
    data = request.get_json(force=True, silent=True) or {}
    product_url = (data.get("url") or "").strip()
    row_id = data.get("row_id")

    if not product_url:
        return jsonify({"status": "error", "message": "Missing 'url'"}), 400

    result = client.create_link(product_url)

    payload = {
        "row_id": row_id,
        "status": "success" if result.ok else "error",
        "mavely_link": result.mavely_link,
        "message": result.error,
        "status_code": result.status_code,
    }

    if MAKE_WEBHOOK_URL:
        try:
            requests.post(MAKE_WEBHOOK_URL, json=payload, timeout=10)
        except Exception as e:
            log.warning("Failed posting to MAKE_WEBHOOK_URL: %s", e)

    if result.ok:
        return jsonify({"status": "success", "link": result.mavely_link, "row_id": row_id}), 200

    return jsonify(payload), (result.status_code if result.status_code else 500)

if __name__ == "__main__":
    port = _env_int("PORT", 8080)
    app.run(host="0.0.0.0", port=port)
