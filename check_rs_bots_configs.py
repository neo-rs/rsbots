from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from mirror_world_config import load_config_with_secrets, is_placeholder_secret, mask_secret


REPO_ROOT = Path(__file__).resolve().parent


class BotCheck:
    def __init__(self, key: str, base_dir: Path, entrypoint: str, required_secret_keys: List[str]):
        self.key = key
        self.base_dir = base_dir
        self.entrypoint = entrypoint
        self.required_secret_keys = required_secret_keys


def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _check_forwarder_webhooks(cfg: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    channels = cfg.get("channels") or []
    webhooks = cfg.get("destination_webhooks") or {}
    if channels and not isinstance(webhooks, dict):
        return ["destination_webhooks must be an object in config.secrets.json"]
    missing: List[str] = []
    for ch in channels:
        src = str((ch or {}).get("source_channel_id", "")).strip()
        if src and not (webhooks or {}).get(src):
            missing.append(src)
    if missing:
        errors.append(f"Missing destination_webhooks entries for source_channel_id(s): {', '.join(missing[:10])}")
    return errors


def run() -> int:
    checks: List[BotCheck] = [
        BotCheck("rsadminbot", REPO_ROOT / "RSAdminBot", "RSAdminBot/admin_bot.py", ["bot_token"]),
        BotCheck("rsforwarder", REPO_ROOT / "RSForwarder", "RSForwarder/rs_forwarder_bot.py", ["bot_token"]),
        BotCheck("instorebotforwarder", REPO_ROOT / "Instorebotforwarder", "Instorebotforwarder/instore_auto_mirror_bot.py", ["bot_token"]),
        BotCheck("rscheckerbot", REPO_ROOT / "RSCheckerbot", "RSCheckerbot/main.py", ["bot_token"]),
        BotCheck("rsmentionpinger", REPO_ROOT / "RSMentionPinger", "RSMentionPinger/rs_mention_pinger.py", ["bot_token"]),
        BotCheck("rssuccessbot", REPO_ROOT / "RSuccessBot", "RSuccessBot/bot_runner.py", ["bot_token"]),
        BotCheck("rsonboarding", REPO_ROOT / "RSOnboarding", "RSOnboarding/rs_onboarding_bot.py", ["bot_token"]),
    ]

    any_fail = False
    print("RS Bots config preflight (no Discord connection)\n")
    for chk in checks:
        config_path = chk.base_dir / "config.json"
        secrets_path = chk.base_dir / "config.secrets.json"
        example_path = chk.base_dir / "config.secrets.example.json"

        errors: List[str] = []
        try:
            cfg, _, secrets_candidate = load_config_with_secrets(chk.base_dir)
        except Exception as e:
            any_fail = True
            print(f"[{chk.key}] FAIL")
            print(f"  - entrypoint: {chk.entrypoint}")
            print(f"  - error: {e}")
            continue

        # load_config_with_secrets returns secrets_path even if missing
        if not secrets_candidate.exists():
            errors.append(f"Missing secrets file: {secrets_candidate}")

        for k in chk.required_secret_keys:
            val = (cfg.get(k) or "").strip()
            if is_placeholder_secret(val):
                errors.append(f"{k} missing/placeholder in config.secrets.json")

        # Bot-specific checks
        if chk.key == "rsforwarder":
            errors.extend(_check_forwarder_webhooks(cfg))

        if errors:
            any_fail = True
            print(f"[{chk.key}] FAIL")
        else:
            print(f"[{chk.key}] OK")

        print(f"  - entrypoint: {chk.entrypoint}")
        print(f"  - config: {config_path}")
        print(f"  - secrets: {secrets_path}")
        if example_path.exists():
            print(f"  - secrets template: {example_path}")

        token = (cfg.get("bot_token") or "").strip()
        if token:
            print(f"  - bot_token: {mask_secret(token)}")

        for e in errors:
            print(f"  - error: {e}")

        # Helpful next step
        if errors and example_path.exists() and not secrets_path.exists():
            print("  - next: copy the template to config.secrets.json and fill real values")
        print("")

    if any_fail:
        print("Result: FAIL (at least one bot is missing required secrets).")
        return 2

    print("Result: OK (all RS bots have required secrets and configs).")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())


