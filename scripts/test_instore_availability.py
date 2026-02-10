#!/usr/bin/env python3
"""
Verify Instorebotforwarder OOS (out-of-stock) logic.
Uses sample HTML to confirm we only mark OOS when the availability *block* says so,
not when "out of stock" appears elsewhere on the page (e.g. "Other sellers - out of stock").
"""
import re
import sys
from pathlib import Path

# Repo root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def extract_availability_OLD(html_txt: str) -> str:
    """Old logic: full-page scan (caused false OOS)."""
    t = html_txt or ""
    if not t:
        return ""
    low = t.lower()
    strong = (
        "currently unavailable",
        "temporarily out of stock",
        "we don't know when or if this item will be back in stock",
        "out of stock",
    )
    if any(s in low for s in strong):
        return "out_of_stock"
    try:
        m = re.search(r'id=["\']availability["\'][\s\S]{0,1200}?</', t, re.IGNORECASE)
        snippet = (m.group(0) or "").lower() if m else ""
    except Exception:
        snippet = ""
    if snippet and any(s in snippet for s in strong):
        return "out_of_stock"
    return ""


def extract_availability_NEW(html_txt: str) -> str:
    """New logic: only the availability block (current production)."""
    t = html_txt or ""
    if not t:
        return ""
    try:
        m = re.search(r'id=["\']availability["\'][\s\S]{0,1200}?</', t, re.IGNORECASE)
        snippet = (m.group(0) or "").lower() if m else ""
    except Exception:
        snippet = ""
    if not snippet:
        return ""
    strong = (
        "currently unavailable",
        "temporarily out of stock",
        "we don't know when or if this item will be back in stock",
        "out of stock",
    )
    if any(s in snippet for s in strong):
        return "out_of_stock"
    return ""


# Sample HTML that mimics real Amazon pages
# Case 1: In stock - availability says "In Stock", but elsewhere "out of stock" (other sellers)
PAGE_IN_STOCK_OTHER_SELLERS_OOS = """
<div class="something">Other sellers on Amazon - out of stock. Get it when it's back in stock.</div>
<span id="availability">In Stock.</span>
<p>Buy now before it's out of stock!</p>
"""

# Case 2: Actually out of stock - availability block says so
PAGE_REALLY_OOS = """
<div id="availability" class="a-section">
  Currently unavailable. We don't know when or if this item will be back in stock.
</div>
"""

# Case 3: No availability block at all; page has "out of stock" in footer/other
PAGE_NO_AVAILABILITY_BLOCK = """
<div class="footer">Some products may be out of stock. Check other sellers.</div>
<h1>Product Title</h1>
<p>Price $19.99</p>
"""

# Case 4: Availability block says "In Stock" only
PAGE_IN_STOCK_CLEAR = """
<span id="availability">In Stock.</span>
"""


def main() -> int:
    cases = [
        ("In stock, but 'out of stock' elsewhere (other sellers)", PAGE_IN_STOCK_OTHER_SELLERS_OOS),
        ("Actually OOS (availability block says so)", PAGE_REALLY_OOS),
        ("No availability block; 'out of stock' in footer", PAGE_NO_AVAILABILITY_BLOCK),
        ("In stock (availability block only)", PAGE_IN_STOCK_CLEAR),
    ]
    all_ok = True
    for name, html in cases:
        old_val = extract_availability_OLD(html)
        new_val = extract_availability_NEW(html)
        # We want NEW to not false-positive on case 1 and 3; to say OOS only on case 2; and "" on case 4
        if "elsewhere" in name or "No availability" in name:
            ok = new_val == "" and old_val == "out_of_stock"
            note = "OLD=false OOS, NEW=no skip (correct)"
        elif "Actually OOS" in name:
            ok = new_val == "out_of_stock" and old_val == "out_of_stock"
            note = "both OOS (correct)"
        else:
            ok = new_val == "" and old_val == ""
            note = "both in-stock (correct)"
        if not ok:
            all_ok = False
        print(f"  {name}")
        print(f"    OLD -> {old_val!r}  NEW -> {new_val!r}  {note}")
        print()
    print("Result: PASS" if all_ok else "Result: FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
