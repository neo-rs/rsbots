from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


class JSONStorage:
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def path(self, name: str) -> Path:
        return self.base_dir / name

    def read(self, name: str, default: Any) -> Any:
        path = self.path(name)
        if not path.exists():
            return default
        try:
            with path.open("r", encoding="utf-8") as fp:
                return json.load(fp)
        except json.JSONDecodeError:
            return default

    def write(self, name: str, data: Any) -> None:
        path = self.path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fp:
                json.dump(data, fp, indent=2, ensure_ascii=False)
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
