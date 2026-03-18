"""
Check what the GHL API returns for validEmail (and related fields) on one or a few contacts.
Run this to confirm: contacts that show "Verified" in GHL UI should have validEmail true in the API.
Usage:
  set GHL_BEARER_TOKEN=pit-...
  python kit/ghl_check_contact_validity.py
  python kit/ghl_check_contact_validity.py --email emmanuelcsst@gmail.com
"""
import json
import os
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("Install requests: pip install requests", file=sys.stderr)
    sys.exit(1)

KIT_DIR = Path(__file__).resolve().parent
BASE = "https://services.leadconnectorhq.com"
headers = lambda t: {"Authorization": f"Bearer {t}", "Version": "2021-07-28", "Content-Type": "application/json"}

def main():
    token = os.environ.get("GHL_BEARER_TOKEN", "").strip()
    if not token:
        print("Set GHL_BEARER_TOKEN", file=sys.stderr)
        return 1

    email_filter = None
    if "--email" in sys.argv:
        i = sys.argv.index("--email")
        if i + 1 < len(sys.argv):
            email_filter = sys.argv[i + 1].strip()

    # Fetch up to 5 contacts (or search by query if email given)
    body = {
        "locationId": "GmTfoYNHLeHumMfDIset",
        "pageLimit": 5,
        "query": email_filter or "",
    }
    r = requests.post(f"{BASE}/contacts/search", headers=headers(token), json=body, timeout=60)
    r.raise_for_status()
    data = r.json()
    contacts = data.get("contacts", [])

    if email_filter:
        contacts = [c for c in contacts if (c.get("email") or "").strip().lower() == email_filter.lower()]
        if not contacts:
            print(f"No contact found with email: {email_filter}")
            return 1

    if not contacts:
        print("No contacts returned. Try without --email to see first 5.")
        return 1

    print("Raw API contact fields related to email/verification:")
    print("(Compare with GHL UI: 'Verified' should match validEmail true)\n")
    for i, c in enumerate(contacts[:5]):
        email = c.get("email") or "(none)"
        valid = c.get("validEmail")
        print(f"--- Contact {i+1}: {email} ---")
        print(f"  id: {c.get('id')}")
        print(f"  validEmail: {valid!r} (type: {type(valid).__name__})")
        # Show any other keys that might indicate verification
        for key in sorted(c.keys()):
            if "valid" in key.lower() or "verif" in key.lower() or "email" in key.lower():
                if key != "validEmail":
                    print(f"  {key}: {c.get(key)!r}")
        print()
    return 0

if __name__ == "__main__":
    sys.exit(main())
