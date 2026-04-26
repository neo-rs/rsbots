#!/usr/bin/env python3
"""
Export a de-duped URL list from generic_results/*/product_*.json.

This is useful to re-run the exact same links on Oracle in batch mode:
  python generic_product_checker.py --url-file urls_from_generic_results.txt --headless --chrome-exe /usr/bin/google-chrome
"""

import json
import re
from pathlib import Path
from typing import Iterable, List, Set


ROOT = Path(__file__).resolve().parent
GENERIC_RESULTS = ROOT / "generic_results"
OUT = ROOT / "urls_from_generic_results.txt"


def _iter_product_json_paths() -> Iterable[Path]:
    if not GENERIC_RESULTS.exists():
        return []
    # Only the main result JSON (exclude *_raw_payloads.json, *_jsonld_raw.json)
    return sorted(
        [
            p
            for p in GENERIC_RESULTS.rglob("product_*.json")
            if not re.search(r"_(raw_payloads|jsonld_raw)\.json$", p.name)
        ]
    )


def main() -> None:
    urls: List[str] = []
    seen: Set[str] = set()

    for p in _iter_product_json_paths():
        try:
            data = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
            url = str(data.get("url", "")).strip()
            if not url or url.lower() == "n/a":
                continue
            if url not in seen:
                seen.add(url)
                urls.append(url)
        except Exception:
            continue

    OUT.write_text("\n".join(urls) + ("\n" if urls else ""), encoding="utf-8")
    print(f"Wrote {len(urls)} urls to: {OUT}")


if __name__ == "__main__":
    main()

