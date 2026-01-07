from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _deep_merge_dict(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merge overlay into base (in place) and return base.

    - Dict values are merged recursively
    - Other types overwrite
    """
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge_dict(base[k], v)  # type: ignore[index]
        else:
            base[k] = v
    return base


def load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_config_with_secrets(
    base_dir: Path,
    config_name: str = "config.json",
    secrets_name: str = "config.secrets.json",
) -> Tuple[Dict[str, Any], Path, Path]:
    """Load config.json and merge config.secrets.json on top.

    Returns: (merged_config, config_path, secrets_path)
    """
    config_path = base_dir / config_name
    secrets_path = base_dir / secrets_name

    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")

    config = load_json(config_path)

    if not secrets_path.exists():
        # Caller is expected to fail fast with a clear message.
        return config, config_path, secrets_path

    secrets = load_json(secrets_path)
    if not isinstance(secrets, dict):
        raise ValueError(f"Invalid secrets file (expected JSON object): {secrets_path}")

    _deep_merge_dict(config, secrets)
    return config, config_path, secrets_path


def is_placeholder_secret(value: Any) -> bool:
    """Return True if the provided secret looks like a template/placeholder value."""
    if value is None:
        return True
    s = str(value).strip()
    if not s:
        return True
    upper = s.upper()
    if upper.startswith("PUT_") or upper.endswith("_HERE"):
        return True
    if upper in {"CHANGEME", "REPLACE_ME", "YOUR_TOKEN_HERE"}:
        return True
    return False


def mask_secret(value: Any, show_last: int = 4) -> str:
    """Mask a secret for printing (never output full tokens)."""
    if value is None:
        return "<missing>"
    s = str(value)
    if not s:
        return "<missing>"
    if len(s) <= show_last:
        return "*" * len(s)
    return ("*" * (len(s) - show_last)) + s[-show_last:]


def get_repo_root() -> Path:
    """Return the repository root directory (where mirror_world_config.py lives)."""
    return Path(__file__).resolve().parent


def load_oracle_servers(repo_root: Optional[Path] = None) -> Tuple[List[Dict[str, Any]], Path]:
    """Load the canonical Oracle server list from oraclekeys/servers.json.

    Returns: (servers_list, servers_path)
    """
    root = (repo_root or get_repo_root()).resolve()
    servers_path = root / "oraclekeys" / "servers.json"
    if not servers_path.exists():
        raise FileNotFoundError(f"Missing servers.json: {servers_path}")
    raw = json.loads(servers_path.read_text(encoding="utf-8") or "[]")
    if not isinstance(raw, list):
        raise ValueError(f"Invalid servers.json (expected a list): {servers_path}")
    servers: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            servers.append(item)
    return servers, servers_path


def pick_oracle_server(servers: List[Dict[str, Any]], server_name: str) -> Dict[str, Any]:
    """Pick a server entry by exact name match (canonical)."""
    name = (server_name or "").strip()
    if not name:
        raise ValueError("Missing server name (expected a name from oraclekeys/servers.json)")
    for s in servers:
        if str(s.get("name", "")).strip() == name:
            return s
    raise ValueError(f"Server name not found in oraclekeys/servers.json: {name}")


def resolve_oracle_ssh_key_path(key_value: str, repo_root: Optional[Path] = None) -> Path:
    """Resolve a servers.json key field to an absolute path.

    Canonical location: <repo_root>/oraclekeys/<key_value>
    """
    root = (repo_root or get_repo_root()).resolve()
    p = Path(str(key_value or "")).expanduser()
    if p.is_absolute():
        return p
    # servers.json typically stores key as a filename.
    candidate = root / "oraclekeys" / p
    if candidate.exists():
        return candidate
    # Fallback: relative to repo root (useful if callers pass oraclekeys/ssh-key-*.key).
    return (root / p).resolve()


