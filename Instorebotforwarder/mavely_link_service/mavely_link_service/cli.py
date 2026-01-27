import argparse
import os
import sys
import json

from mavely_client import MavelyClient

def main():
    p = argparse.ArgumentParser(description="Generate a Mavely link for a given URL.")
    p.add_argument("url", help="Product URL to convert")
    p.add_argument("--row-id", default="", help="Optional row id for your workflow")
    args = p.parse_args()

    client = MavelyClient(
        session_token=os.environ.get("MAVELY_COOKIES", ""),
        timeout_s=int(os.environ.get("REQUEST_TIMEOUT", "20")),
        max_retries=int(os.environ.get("MAX_RETRIES", "3")),
        min_seconds_between_requests=float(os.environ.get("MIN_SECONDS_BETWEEN_REQUESTS", "2.0")),
    )

    res = client.create_link(args.url.strip())
    out = {
        "row_id": args.row_id,
        "ok": res.ok,
        "status_code": res.status_code,
        "mavely_link": res.mavely_link,
        "error": res.error,
        "raw_snippet": res.raw_snippet,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    sys.exit(0 if res.ok else 1)

if __name__ == "__main__":
    main()
