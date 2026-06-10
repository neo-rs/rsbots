#!/usr/bin/env python3
"""
Resolve and stabilize the canonical Chrome profile inside oracle_real_chrome_profile.

CDP Chrome (Instore, RSForwarder Mavely, Chromerrunner scrapers) must always launch
into the same signed-in profile (e.g. Neo Secrets / Work) — never the profile picker.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_USER_DATA = SCRIPT_DIR / "oracle_real_chrome_profile"
CONFIG_PATH = SCRIPT_DIR / "cdp_chrome_config.json"

DEFAULT_PREFERRED_NAMES = ("neo secrets", "neo", "work", "reselling secrets")
DEFAULT_PREFERRED_EMAIL_MARKERS = ("resellingsecrets.com",)


def _load_config() -> dict:
    try:
        if CONFIG_PATH.is_file():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8", errors="replace") or "{}")
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def _profile_score(folder: str, ent: dict, *, preferred_names: Tuple[str, ...], preferred_emails: Tuple[str, ...]) -> int:
    if not isinstance(ent, dict):
        return 0
    name = " ".join(
        [
            str(ent.get("name") or ""),
            str(ent.get("gaia_name") or ""),
            str(ent.get("shortcut_name") or ""),
        ]
    ).lower()
    email = str(ent.get("user_name") or ent.get("gaia_given_name") or "").lower()
    for p in preferred_names:
        if p and p in name:
            return 100
    for p in preferred_emails:
        if p and p in email:
            return 90
    if folder == "Default":
        return 10
    return 0


def resolve_profile_subdirectory(user_data_dir: Optional[Path] = None) -> str:
    """
    Return Chrome --profile-directory value (e.g. Default, Profile 1) inside user-data-dir.
    """
    cfg = _load_config()
    override = str(cfg.get("profile_directory") or os.getenv("CHROME_PROFILE_DIRECTORY", "") or "").strip()
    if override:
        return override

    root = Path(user_data_dir or DEFAULT_USER_DATA).resolve()
    local_state = root / "Local State"
    if not local_state.is_file():
        return "Default"

    preferred_names = tuple(
        str(x).lower()
        for x in (cfg.get("preferred_profile_names") or list(DEFAULT_PREFERRED_NAMES))
        if str(x).strip()
    ) or DEFAULT_PREFERRED_NAMES
    preferred_emails = tuple(
        str(x).lower()
        for x in (cfg.get("preferred_profile_email_domains") or list(DEFAULT_PREFERRED_EMAIL_MARKERS))
        if str(x).strip()
    ) or DEFAULT_PREFERRED_EMAIL_MARKERS

    try:
        data = json.loads(local_state.read_text(encoding="utf-8", errors="replace") or "{}")
    except Exception:
        return "Default"
    if not isinstance(data, dict):
        return "Default"

    info = data.get("profile")
    if not isinstance(info, dict):
        return "Default"
    cache = info.get("info_cache")
    if not isinstance(cache, dict) or not cache:
        return "Default"

    last_used = str(info.get("last_used") or "").strip()
    best = ""
    best_score = -1
    for folder, ent in cache.items():
        if not isinstance(folder, str) or not folder.strip():
            continue
        s = _profile_score(folder, ent if isinstance(ent, dict) else {}, preferred_names=preferred_names, preferred_emails=preferred_emails)
        if s > best_score:
            best_score = s
            best = folder

    if best_score <= 0 and last_used in cache:
        best = last_used
    if not best:
        best = "Default"
    return best


def patch_local_state(user_data_dir: Optional[Path] = None, *, profile_directory: Optional[str] = None) -> bool:
    """
    Disable profile picker on startup and pin last_used to the canonical profile.
    Idempotent; safe to run before every Chrome launch.
    """
    root = Path(user_data_dir or DEFAULT_USER_DATA).resolve()
    local_state = root / "Local State"
    prof_dir = (profile_directory or resolve_profile_subdirectory(root)).strip() or "Default"

    data: Dict[str, Any] = {}
    if local_state.is_file():
        try:
            raw = json.loads(local_state.read_text(encoding="utf-8", errors="replace") or "{}")
            if isinstance(raw, dict):
                data = raw
        except Exception:
            data = {}

    prof = data.setdefault("profile", {})
    if not isinstance(prof, dict):
        prof = {}
        data["profile"] = prof

    changed = False
    if prof.get("profiles_enabled_show_profile_picker_on_startup") is not False:
        prof["profiles_enabled_show_profile_picker_on_startup"] = False
        changed = True
    if str(prof.get("last_used") or "") != prof_dir:
        prof["last_used"] = prof_dir
        changed = True
    lap = prof.get("last_active_profiles")
    if not isinstance(lap, list) or prof_dir not in lap:
        prof["last_active_profiles"] = [prof_dir]
        changed = True

    if not changed and local_state.is_file():
        return False

    root.mkdir(parents=True, exist_ok=True)
    tmp = local_state.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    os.replace(str(tmp), str(local_state))
    return True


def main() -> int:
    if len(sys.argv) < 2:
        print(resolve_profile_subdirectory())
        return 0
    cmd = sys.argv[1].strip().lower()
    root = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else DEFAULT_USER_DATA
    if cmd == "resolve":
        print(resolve_profile_subdirectory(root))
        return 0
    if cmd == "patch":
        prof = resolve_profile_subdirectory(root)
        patch_local_state(root, profile_directory=prof)
        print(prof)
        return 0
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
