#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert PNG image to Windows ICO.")
    ap.add_argument("--input", required=True, help="Input PNG path")
    ap.add_argument("--output", required=True, help="Output ICO path")
    args = ap.parse_args()

    in_path = Path(args.input).expanduser().resolve()
    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(in_path) as img:
        img = img.convert("RGBA")
        img.save(
            out_path,
            format="ICO",
            sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)],
        )
    print(f"Wrote icon: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

