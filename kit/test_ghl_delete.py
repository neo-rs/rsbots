"""One-off test: fetch 1 contact, then call DELETE to verify the API works."""
import os
import sys

import requests

token = os.environ.get("GHL_BEARER_TOKEN", "").strip()
if not token:
    print("Set GHL_BEARER_TOKEN")
    sys.exit(1)

BASE = "https://services.leadconnectorhq.com"
headers = {"Authorization": f"Bearer {token}", "Version": "2021-07-28", "Content-Type": "application/json"}

# Fetch 1 contact
r = requests.post(
    f"{BASE}/contacts/search",
    headers=headers,
    json={"locationId": "GmTfoYNHLeHumMfDIset", "pageLimit": 1, "query": ""},
    timeout=60,
)
r.raise_for_status()
contacts = r.json().get("contacts", [])
if not contacts:
    print("No contacts returned from search")
    sys.exit(1)

c = contacts[0]
cid = c.get("id")
email = c.get("email") or "(no email)"
print(f"Fetched 1 contact: id={cid}, email={email}")

# DELETE
url_delete = f"{BASE}/contacts/{cid}"
rd = requests.delete(url_delete, headers=headers, timeout=30)
body = rd.text or "(empty body)"
print(f"DELETE {url_delete}")
print(f"  Status: {rd.status_code}")
print(f"  Body: {body[:400]}")

if rd.status_code in (200, 204):
    print("  Result: OK - delete succeeded")
else:
    print("  Result: FAILED")
    sys.exit(1)
