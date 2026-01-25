"""
RSForwarder-local Mavely client import.

To keep RSForwarder implementation self-contained without copying large API client logic,
we load the canonical Mavely client source file from the repo at runtime and re-export
`MavelyClient` and `MavelyResult`.

This avoids maintaining two separate Mavely client implementations.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Optional, Any


def _load_module_from_path(path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load spec for {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "Instorebotforwarder" / "mavely_link_service" / "mavely_link_service" / "mavely_client.py"
if not _SRC.exists():
    raise RuntimeError(f"Mavely client source not found: {_SRC}")

_mod = _load_module_from_path(_SRC, "rsforwarder_mavely_client")

# Re-export
MavelyClient = getattr(_mod, "MavelyClient")
MavelyResult = getattr(_mod, "MavelyResult")

