# Target Checker V4 - Real Chrome CDP + Network JSON

This version is the upgrade from V3.

V3 only read visible page text. V4 also captures Target network JSON responses while the page loads.

## Generic product checker (works for many sites)

If you just want product info (title/price/image/brand) from **any** product URL
(Walmart, Home Depot, GameStop, Costco, BestBuy, Sam’s Club, etc.), use:

```text
generic_product_checker.py
```

Output is saved under:

```text
generic_results/<host>/
```

### Run (headless, no real Chrome window)

```text
run_generic.sh --url "https://www.walmart.com/ip/..."
```

Batch file mode (Oracle/headless-friendly):

```text
python generic_product_checker.py --url-file urls.txt --headless
```

Windows:

```text
run_generic.bat https://www.walmart.com/ip/...
```

Or directly:

```text
python generic_product_checker.py --url "https://www.bestbuy.com/site/..." --headless
```

### Run (real Chrome via CDP, like Target)

1. Start Chrome with remote debugging (see `start_chrome_target.*` for an example).
2. Run:

```text
python generic_product_checker.py --url "https://www.homedepot.com/p/..." --connect-cdp --manual
```

Windows helpers:

```text
start_chrome_generic.bat
run_generic.bat "https://www.homedepot.com/p/..." --connect-cdp --manual
```

## Run

1. Double-click:

```text
start_chrome_target.bat
```

2. In that Chrome window, set your Target ZIP/store.

3. Double-click:

```text
run_checker.bat
```

4. Enter TCIN:

```text
53741664
```

## During each check

When the page opens:

1. Fix any Target popup/error manually.
2. Make sure ZIP/store is set.
3. Scroll until Pickup / Delivery / Shipping sections load.
4. Wait 5-10 seconds.
5. Press ENTER in terminal.

## Output

Saved to:

```text
target_results_v4/
```

For each run it saves:

```text
target_<TCIN>_<timestamp>.json
target_<TCIN>_<timestamp>_raw_payloads.json
target_<TCIN>_<timestamp>_captured_urls.txt
target_<TCIN>_<timestamp>_visible_text.txt
target_<TCIN>_<timestamp>.png
```

## Stock note

Exact stock only appears when Target exposes quantity in JSON.

If Target only gives availability status like Pickup / Delivery / Shipping, the checker prints:

```text
Total Network Stock: N/A
```

That means the data was not exposed, not necessarily zero stock.

## Oracle note: “Chrome for Testing” vs Playwright

- **Playwright is not “opening its own Walmart browser”** when you use `--connect-cdp`. In that mode Playwright is basically a **CDP remote-control client** attached to whatever Chrome is already listening on `9222`.
- If you see a banner like **“Chrome for Testing … only for automated testing”**, that means the **Chrome binary on the server** is Google’s *testing* distribution (common on automation hosts), not “Playwright pretending to be Chrome”.
- Prefer **stable Google Chrome** on Oracle. `start_chrome_oracle_cdp.sh` tries to avoid “Chrome for Testing” automatically, and you can force a binary:

```bash
CHROME_BIN=/opt/google/chrome/google-chrome bash start_chrome_oracle_cdp.sh --headed
```

Even with stable Chrome, strict retailers may still show bot pages (“Access Denied”, PerimeterX, etc.) from datacenter IPs — that’s separate from the CDP/Playwright wiring.
