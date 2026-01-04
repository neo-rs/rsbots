from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path("/home/rsadmin/bots/mirror-world").resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from rsbots_manifest import generate_manifest


def main() -> None:
    repo_root = REPO_ROOT
    out_path = Path("/tmp/rsbots_manifest_server.json")
    manifest = generate_manifest(repo_root)
    out_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()


