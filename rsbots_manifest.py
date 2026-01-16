from __future__ import annotations

import fnmatch
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_RS_BOT_FOLDERS = [
    "RSAdminBot",
    "RSForwarder",
    "RSCheckerbot",
    "RSMentionPinger",
    "RSuccessBot",
    "RSOnboarding",
]


DEFAULT_INCLUDE_GLOBS = [
    "*.py",
    "*.sh",
    "*.md",
    "*.txt",
    "*.service",
    "requirements.txt",
    "config.json",
    "messages.json",
    "vouch_config.json",
]


DEFAULT_EXCLUDE_GLOBS = [
    "config.secrets.json",
    "*.key",
    "*.pem",
    "*.ppk",
    "rs-bot-tokens.txt",
    "*.db",
    "*.sqlite",
    "*.sqlite3",
    "*.log",
    "*.pyc",
    ".rs_onboarding_bot.lock",
    "tickets.json",
    "success_points.json",
    "vouches.json",
    "points_history.txt",
    "queue.json",
    "registry.json",
    "invites.json",
    "payment_cache.json",
    "missed_onboarding_report.json",
    "ticket_history_report.json",
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


_TEXT_EXTS_FOR_NORMALIZATION = {
    ".py",
    ".sh",
    ".md",
    ".txt",
    ".service",
    ".json",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
}
_TEXT_NAMES_FOR_NORMALIZATION = {
    "requirements.txt",
}


def _sha256_text_normalized(path: Path) -> str:
    """SHA256 over file bytes with EOL normalized to LF.

    This prevents false mismatches when comparing Windows checkouts (CRLF) vs Linux snapshots (LF).
    """
    b = path.read_bytes()
    b = b.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(b).hexdigest()


def _sha256_for_manifest(path: Path, *, normalize_text_eol: bool) -> str:
    if not normalize_text_eol:
        return _sha256(path)
    ext = path.suffix.lower()
    if ext in _TEXT_EXTS_FOR_NORMALIZATION or path.name in _TEXT_NAMES_FOR_NORMALIZATION:
        return _sha256_text_normalized(path)
    return _sha256(path)


def _match_any(name: str, globs: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(name, g) for g in globs)


def _should_skip_dir(path: Path) -> bool:
    return path.name in {"__pycache__", ".git", ".venv", "venv", ".staging-local"} or path.name.startswith(".staging-")


def _iter_included_files(
    root: Path,
    include_globs: List[str],
    exclude_globs: List[str],
) -> Iterable[Tuple[str, Path]]:
    for p in root.rglob("*"):
        if p.is_dir():
            if _should_skip_dir(p):
                # prune by skipping walk descendants via rglob (best-effort)
                continue
            continue

        rel = p.relative_to(root).as_posix()
        base = p.name

        # Exclude by base name or full relative path
        if _match_any(base, exclude_globs) or _match_any(rel, exclude_globs):
            continue

        # Include by base name or full relative path
        if _match_any(base, include_globs) or _match_any(rel, include_globs):
            yield rel, p


def generate_manifest(
    repo_root: Path,
    bot_folders: Optional[List[str]] = None,
    include_globs: Optional[List[str]] = None,
    exclude_globs: Optional[List[str]] = None,
    *,
    normalize_text_eol: bool = False,
) -> Dict:
    repo_root = Path(repo_root).resolve()
    bot_folders = bot_folders or list(DEFAULT_RS_BOT_FOLDERS)
    include_globs = include_globs or list(DEFAULT_INCLUDE_GLOBS)
    exclude_globs = exclude_globs or list(DEFAULT_EXCLUDE_GLOBS)

    out: Dict = {
        "repo_root": str(repo_root),
        "bot_folders": bot_folders,
        "include_globs": include_globs,
        "exclude_globs": exclude_globs,
        "files": {},  # folder -> relpath -> sha256
    }

    for folder in bot_folders:
        base = repo_root / folder
        if not base.exists():
            out["files"][folder] = {"__missing__": True}
            continue
        files: Dict[str, str] = {}
        for rel, p in _iter_included_files(base, include_globs, exclude_globs):
            files[rel] = _sha256_for_manifest(p, normalize_text_eol=normalize_text_eol)
        out["files"][folder] = files

    # systemd templates at root
    systemd_dir = repo_root / "systemd"
    if systemd_dir.exists():
        files: Dict[str, str] = {}
        for rel, p in _iter_included_files(systemd_dir, include_globs, exclude_globs):
            files[rel] = _sha256_for_manifest(p, normalize_text_eol=normalize_text_eol)
        out["files"]["systemd"] = files
    else:
        out["files"]["systemd"] = {"__missing__": True}

    # shared helpers at root
    for f in ["mirror_world_config.py", "check_rs_bots_configs.py", "rsbots_manifest.py"]:
        p = repo_root / f
        if p.exists():
            out.setdefault("root_files", {})[f] = _sha256_for_manifest(p, normalize_text_eol=normalize_text_eol)

    return out


def compare_manifests(local: Dict, remote: Dict) -> Dict:
    """Return a diff summary without including any file contents."""
    result: Dict = {
        "folders": {},
        "root_files": {"only_local": [], "only_remote": [], "changed": []},
    }

    local_root_files = local.get("root_files", {}) or {}
    remote_root_files = remote.get("root_files", {}) or {}
    for k in sorted(set(local_root_files) | set(remote_root_files)):
        if k not in remote_root_files:
            result["root_files"]["only_local"].append(k)
        elif k not in local_root_files:
            result["root_files"]["only_remote"].append(k)
        elif local_root_files[k] != remote_root_files[k]:
            result["root_files"]["changed"].append(k)

    local_files = local.get("files", {}) or {}
    remote_files = remote.get("files", {}) or {}
    for folder in sorted(set(local_files) | set(remote_files)):
        lf = local_files.get(folder, {}) or {}
        rf = remote_files.get(folder, {}) or {}

        # Missing markers
        if lf.get("__missing__") or rf.get("__missing__"):
            result["folders"][folder] = {
                "missing_local": bool(lf.get("__missing__")),
                "missing_remote": bool(rf.get("__missing__")),
                "only_local": [],
                "only_remote": [],
                "changed": [],
            }
            continue

        only_local = sorted(set(lf) - set(rf))
        only_remote = sorted(set(rf) - set(lf))
        changed = sorted([p for p in set(lf) & set(rf) if lf[p] != rf[p]])

        result["folders"][folder] = {
            "missing_local": False,
            "missing_remote": False,
            "only_local": only_local,
            "only_remote": only_remote,
            "changed": changed,
        }

    return result


def save_manifest_json(path: Path, manifest: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


