from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_merge(base[k], v)  # type: ignore[index]
        else:
            base[k] = v
    return base


def migrate_bot(root: Path, bot_dir: str) -> Tuple[str, List[str]]:
    notes: List[str] = []
    base = root / bot_dir
    cfg_path = base / "config.json"
    sec_path = base / "config.secrets.json"
    if not cfg_path.exists():
        return bot_dir, [f"SKIP: missing {cfg_path}"]

    cfg = load_json(cfg_path)
    secrets: Dict[str, Any] = {}
    if sec_path.exists():
        try:
            secrets = load_json(sec_path)
            if not isinstance(secrets, dict):
                secrets = {}
        except Exception:
            secrets = {}

    # Move bot_token
    token = (cfg.get("bot_token") or "").strip()
    if token:
        secrets["bot_token"] = token
        cfg.pop("bot_token", None)
        notes.append("moved bot_token -> config.secrets.json")

    # Bot-specific secret moves
    if bot_dir == "RSForwarder":
        # Convert per-channel destination_webhook_url into destination_webhooks mapping
        webhooks = secrets.get("destination_webhooks")
        if not isinstance(webhooks, dict):
            webhooks = {}
        channels = cfg.get("channels") or []
        if isinstance(channels, list):
            moved = 0
            for ch in channels:
                if not isinstance(ch, dict):
                    continue
                src = str(ch.get("source_channel_id") or "").strip()
                hook = str(ch.get("destination_webhook_url") or "").strip()
                if src and hook and hook.startswith("https://discord.com/api/webhooks/"):
                    webhooks[src] = hook
                    moved += 1
                # scrub from config.json
                if "destination_webhook_url" in ch:
                    ch["destination_webhook_url"] = ""
            if moved:
                secrets["destination_webhooks"] = webhooks
                notes.append(f"moved {moved} webhook(s) -> destination_webhooks")

    if bot_dir == "RSCheckerbot":
        inv = cfg.get("invite_tracking")
        if isinstance(inv, dict):
            ghl = str(inv.get("ghl_api_key") or "").strip()
            if ghl:
                # put under secrets invite_tracking.ghl_api_key
                s_inv = secrets.get("invite_tracking")
                if not isinstance(s_inv, dict):
                    s_inv = {}
                s_inv["ghl_api_key"] = ghl
                secrets["invite_tracking"] = s_inv
                inv["ghl_api_key"] = ""
                notes.append("moved invite_tracking.ghl_api_key -> config.secrets.json")

    if bot_dir == "RSAdminBot":
        ssh = cfg.get("ssh_server")
        if isinstance(ssh, dict) and "sudo_password" in ssh:
            # Never store sudo password in config.json
            ssh.pop("sudo_password", None)
            notes.append("removed ssh_server.sudo_password from config.json")

    # Save results
    save_json(cfg_path, cfg)
    if secrets:
        save_json(sec_path, secrets)
    else:
        # If no secrets, don't create an empty secrets file
        notes.append("no secrets found/created")

    return bot_dir, notes


def main() -> None:
    root = Path("/home/rsadmin/bots/mirror-world")
    bots = ["RSAdminBot", "RSForwarder", "RSCheckerbot", "RSMentionPinger", "RSuccessBot", "RSOnboarding"]
    results = []
    for b in bots:
        results.append(migrate_bot(root, b))

    # Print only high-level status; never print secrets
    for b, notes in results:
        print(f"[{b}]")
        for n in notes:
            print(f"- {n}")
        print()


if __name__ == "__main__":
    main()


