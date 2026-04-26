#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
RSADMINBOT_DIR = REPO_ROOT / "RSAdminBot"
SYSTEMD_DIR = REPO_ROOT / "systemd"


def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8", newline="\n")


def _run(cmd: List[str], *, cwd: Optional[Path] = None) -> None:
    print(f"+ {' '.join(shlex.quote(c) for c in cmd)}")
    subprocess.check_call(cmd, cwd=str(cwd) if cwd else None)


def _git(*args: str) -> None:
    _run(["git", *args], cwd=REPO_ROOT)


def _input_default(prompt: str, default: str) -> str:
    raw = input(f"{prompt} [{default}]: ").strip()
    return raw if raw else default


def _load_json(p: Path) -> Any:
    return json.loads(_read_text(p))


def _save_json(p: Path, obj: Any) -> None:
    _write_text(p, json.dumps(obj, indent=2, ensure_ascii=False) + "\n")


def _ensure_list_contains(lst: List[str], item: str) -> bool:
    item = str(item).strip()
    if not item:
        return False
    if item in lst:
        return False
    lst.append(item)
    return True


def _patch_rsadmin_config_group(*, bot_key: str, group: str) -> bool:
    cfg_path = RSADMINBOT_DIR / "config.json"
    cfg = _load_json(cfg_path)
    if not isinstance(cfg, dict):
        raise ValueError("RSAdminBot/config.json is not an object")
    bot_groups = cfg.get("bot_groups")
    if not isinstance(bot_groups, dict):
        raise ValueError("RSAdminBot/config.json missing bot_groups dict")
    key = "rs_bots" if group == "rs" else "mirror_bots"
    arr = bot_groups.get(key)
    if not isinstance(arr, list):
        arr = []
        bot_groups[key] = arr
    changed = _ensure_list_contains(arr, bot_key)
    if changed:
        _save_json(cfg_path, cfg)
    return changed


def _patch_run_oracle_update_bots_map(*, bot_key: str, folder: str) -> bool:
    p = REPO_ROOT / "scripts" / "run_oracle_update_bots.py"
    s = _read_text(p)
    if re.search(rf"^\s*{re.escape(json.dumps(bot_key))}\s*:", s, re.M):
        return False

    m = re.search(r"BOT_KEY_TO_FOLDER:\s*Dict\[str,\s*str\]\s*=\s*\{", s)
    if not m:
        raise ValueError("Could not find BOT_KEY_TO_FOLDER map in scripts/run_oracle_update_bots.py")
    insert_at = m.end()

    # Insert near other RS bots (after catalognavbot if present, else near top of dict).
    anchor = '"catalognavbot": "catalog_nav_bot",'
    idx = s.find(anchor)
    if idx != -1:
        line_end = s.find("\n", idx)
        insert_at = line_end + 1
    ins = f'    "{bot_key}": "{folder}",\n'
    out = s[:insert_at] + ins + s[insert_at:]
    _write_text(p, out)
    return True


def _ensure_systemd_unit(*, bot_key: str, service_name: str) -> Path:
    SYSTEMD_DIR.mkdir(exist_ok=True)
    unit_path = SYSTEMD_DIR / service_name
    if unit_path.exists():
        return unit_path
    content = "\n".join(
        [
            "[Unit]",
            f"Description=Mirror World - {bot_key} (managed bot)",
            "Wants=network-online.target",
            "After=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            "User=rsadmin",
            "WorkingDirectory=/home/rsadmin/bots/mirror-world",
            f"ExecStart=/bin/bash /home/rsadmin/bots/mirror-world/RSAdminBot/run_bot.sh {bot_key}",
            "Restart=always",
            "RestartSec=8",
            "Environment=PYTHONUNBUFFERED=1",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
    )
    _write_text(unit_path, content)
    return unit_path


def _patch_simple_service_map(*, path: Path, bot_key: str, service_name: str) -> bool:
    s = _read_text(path)
    if bot_key in s:
        return False
    # Insert into declare -A SERVICES=( ... ) block.
    m = re.search(r"declare\s+-A\s+SERVICES=\(\s*\n", s)
    if not m:
        raise ValueError(f"Could not find SERVICES map in {path}")
    # Insert near the end of the block (before closing ')').
    close = s.find(")\n", m.end())
    if close == -1:
        raise ValueError(f"Could not find end of SERVICES map in {path}")
    ins = f'    ["{bot_key}"]="{service_name}"\n'
    out = s[:close] + ins + s[close:]
    _write_text(path, out)
    return True


def _patch_install_services_unit_list(*, service_name: str) -> bool:
    p = RSADMINBOT_DIR / "install_services.sh"
    s = _read_text(p)
    if service_name in s:
        return False
    # insert before closing ')'
    m = re.search(r'units=\(\s*\n', s)
    if not m:
        raise ValueError("install_services.sh missing units=( list")
    close = s.find(")\n", m.end())
    if close == -1:
        raise ValueError("install_services.sh missing end of units list")
    ins = f'  "{service_name}"\n'
    out = s[:close] + ins + s[close:]
    _write_text(p, out)
    return True


def _patch_bootstrap_venv_requirements(*, folder: str) -> bool:
    p = RSADMINBOT_DIR / "bootstrap_venv.sh"
    s = _read_text(p)
    req_path = f'$ROOT_DIR/{folder}/requirements.txt'
    if req_path in s:
        return False
    m = re.search(r"reqs=\(\s*\n", s)
    if not m:
        raise ValueError("bootstrap_venv.sh missing reqs=( list")
    close = s.find(")\n", m.end())
    if close == -1:
        raise ValueError("bootstrap_venv.sh missing end of reqs list")
    ins = f'  "{req_path}"\n'
    out = s[:close] + ins + s[close:]
    _write_text(p, out)
    return True


def _patch_rsadmin_registry_bot(*, bot_key: str, display_name: str, service_name: str, folder: str, entry: str) -> bool:
    p = RSADMINBOT_DIR / "admin_bot.py"
    s = _read_text(p)
    if re.search(rf'^\s*"{re.escape(bot_key)}":\s*\{{', s, re.M):
        return False
    # Insert after catalognavbot block if present, else after pingbot block.
    anchor = '"catalognavbot": {'
    idx = s.find(anchor)
    if idx == -1:
        anchor = '"pingbot": {'
        idx = s.find(anchor)
    if idx == -1:
        raise ValueError("Could not find insertion anchor in RSAdminBot/admin_bot.py BOTS dict")
    # Find end of that dict entry by searching for the next line that starts with '        },'
    end = s.find("        },", idx)
    if end == -1:
        raise ValueError("Could not find end of anchor bot entry in admin_bot.py")
    end = s.find("\n", end) + 1
    ins = (
        f'        "{bot_key}": {{\n'
        f'            "name": "{display_name}",\n'
        f'            "service": "{service_name}",\n'
        f'            "folder": "{folder}",\n'
        f'            "script": "{entry}",\n'
        f"        }},\n"
    )
    out = s[:end] + ins + s[end:]
    _write_text(p, out)
    return True


def _patch_run_bot_sh(*, bot_key: str, folder: str, entry: str) -> bool:
    p = RSADMINBOT_DIR / "run_bot.sh"
    s = _read_text(p)
    if re.search(rf"^\s*{re.escape(bot_key)}\)\s*$", s, re.M):
        return False
    # Insert before default *) case.
    m = re.search(r"^\s*\*\)\s*$", s, re.M)
    if not m:
        raise ValueError("run_bot.sh missing default *) case")
    insert_at = m.start()
    ins = "\n".join(
        [
            f"  {bot_key})",
            f'    cd "$ROOT_DIR/{folder}"',
            f'    exec "$PY" -u "{entry}"',
            "    ;;",
            "",
        ]
    )
    out = s[:insert_at] + ins + s[insert_at:]
    # Update “Valid bot_key values” line if present.
    out = re.sub(
        r"^(\\s*echo \"Valid bot_key values:.*)\"$",
        lambda mm: mm.group(1) + f" {bot_key}" + "\"",
        out,
        flags=re.M,
    )
    _write_text(p, out)
    return True


def _oracle_install_unit_via_ssh(
    *,
    server: Dict[str, Any],
    service_name: str,
) -> None:
    user = str(server.get("user", "rsadmin"))
    host = str(server.get("host", "")).strip()
    if not host:
        raise ValueError("servers.json missing host")
    key = str(server.get("key", "")).strip()
    if not key:
        raise ValueError("servers.json missing key")
    ssh_options = str(server.get("ssh_options", "") or "")
    remote_root = str(server.get("remote_root") or "/home/rsadmin/bots/mirror-world")
    code_root = "/home/rsadmin/bots/rsbots-code"

    # Resolve key path locally: prefer oracleserverkeys/, fallback oraclekeys/
    key_path = (REPO_ROOT / "oracleserverkeys" / key)
    if not key_path.exists():
        key_path = (REPO_ROOT / "oraclekeys" / key)
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key not found locally: {key_path}")

    remote_cmd = (
        f"sudo cp -f {shlex.quote(code_root)}/systemd/{shlex.quote(service_name)} /etc/systemd/system/ && "
        f"sudo systemctl daemon-reload && "
        f"sudo systemctl enable {shlex.quote(service_name)} >/dev/null || true && "
        f"systemctl list-unit-files | grep {shlex.quote(service_name)} || true"
    )

    cmd = [
        os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "System32", "OpenSSH", "ssh.exe"),
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
    ]
    if ssh_options:
        cmd.extend(shlex.split(ssh_options))
    cmd.append(f"{user}@{host}")
    cmd.append(f"bash -lc {shlex.quote(remote_cmd)}")
    _run(cmd)


def _oracle_restart_and_verify_via_ssh(*, server: Dict[str, Any], service_name: str) -> None:
    user = str(server.get("user", "rsadmin"))
    host = str(server.get("host", "")).strip()
    key = str(server.get("key", "")).strip()
    ssh_options = str(server.get("ssh_options", "") or "")
    key_path = (REPO_ROOT / "oracleserverkeys" / key)
    if not key_path.exists():
        key_path = (REPO_ROOT / "oraclekeys" / key)
    remote_cmd = (
        f"sudo systemctl restart {shlex.quote(service_name)} || true; "
        f"sudo systemctl status {shlex.quote(service_name)} --no-pager -n 30 || true; "
        f"sudo journalctl -u {shlex.quote(service_name)} -n 80 --no-pager || true"
    )
    cmd = [
        os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "System32", "OpenSSH", "ssh.exe"),
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
    ]
    if ssh_options:
        cmd.extend(shlex.split(ssh_options))
    cmd.append(f"{user}@{host}")
    cmd.append(f"bash -lc {shlex.quote(remote_cmd)}")
    _run(cmd)


def _load_servers() -> List[Dict[str, Any]]:
    p = REPO_ROOT / "oraclekeys" / "servers.json"
    servers = _load_json(p)
    if not isinstance(servers, list):
        raise ValueError("oraclekeys/servers.json must be a list")
    return [s for s in servers if isinstance(s, dict)]


def _pick_server(servers: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    if not name:
        return servers[0]
    for s in servers:
        if str(s.get("name", "")).strip() == name:
            return s
    raise ValueError(f"Server name not found: {name}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="New bot SOP: register + commit + deploy to Oracle.")
    ap.add_argument("--bot-key", default="", help="bot key (e.g. amazonasinchecker)")
    ap.add_argument("--folder", default="", help="repo folder (e.g. amazon_asin_promo_checker)")
    ap.add_argument("--entry", default="", help="entry script inside folder (e.g. discord_bot.py)")
    ap.add_argument("--group", choices=["rs", "mw"], default="", help="deployment group")
    ap.add_argument("--service", default="", help="systemd unit filename (e.g. mirror-world-foo.service)")
    ap.add_argument("--display-name", default="", help="human-friendly name for RSAdminBot UI")
    ap.add_argument("--server-name", default="", help="oraclekeys/servers.json name (optional)")
    ap.add_argument("--no-push", action="store_true", help="skip git commit/push")
    ap.add_argument("--no-deploy", action="store_true", help="skip Oracle deploy steps")
    args = ap.parse_args(argv)

    # Interactive prompts if missing.
    if not args.bot_key:
        args.bot_key = _input_default("bot key", "mynewbot").strip().lower()
    if not args.folder:
        args.folder = _input_default("repo folder", args.bot_key).strip()
    if not args.entry:
        args.entry = _input_default("entry script (inside folder)", "main.py").strip()
    if not args.group:
        args.group = _input_default("group (rs or mw)", "rs").strip().lower()
        if args.group not in ("rs", "mw"):
            raise SystemExit("group must be rs or mw")
    if not args.service:
        args.service = _input_default("systemd unit filename", f"mirror-world-{args.bot_key}.service").strip()
    if not args.display_name:
        args.display_name = _input_default("display name", args.bot_key).strip()

    bot_key = str(args.bot_key).strip().lower()
    folder = str(args.folder).strip().rstrip("/\\")
    entry = str(args.entry).strip()
    group = str(args.group).strip().lower()
    service_name = str(args.service).strip()
    display_name = str(args.display_name).strip()

    bot_dir = REPO_ROOT / folder
    if not bot_dir.exists():
        raise SystemExit(f"Bot folder not found: {bot_dir}")
    if not (bot_dir / entry).exists():
        raise SystemExit(f"Entry script not found: {bot_dir / entry}")

    # Ensure systemd unit exists (repo file). Install to /etc happens in deploy step.
    unit_path = _ensure_systemd_unit(bot_key=bot_key, service_name=service_name)
    print(f"OK: unit file at {unit_path}")

    changed: List[str] = []

    # Patch RSAdminBot integration
    if _patch_rsadmin_registry_bot(bot_key=bot_key, display_name=display_name, service_name=service_name, folder=folder, entry=entry):
        changed.append("RSAdminBot/admin_bot.py (BOTS registry)")
    if _patch_rsadmin_config_group(bot_key=bot_key, group=("rs" if group == "rs" else "mw")):
        changed.append("RSAdminBot/config.json (bot_groups)")
    if _patch_run_bot_sh(bot_key=bot_key, folder=folder, entry=entry):
        changed.append("RSAdminBot/run_bot.sh (launcher case)")
    if _patch_simple_service_map(path=RSADMINBOT_DIR / "botctl.sh", bot_key=bot_key, service_name=service_name):
        changed.append("RSAdminBot/botctl.sh (service map)")
    if _patch_simple_service_map(path=RSADMINBOT_DIR / "manage_bots.sh", bot_key=bot_key, service_name=service_name):
        changed.append("RSAdminBot/manage_bots.sh (service map)")
    if group == "rs":
        if _patch_simple_service_map(path=RSADMINBOT_DIR / "manage_rs_bots.sh", bot_key=bot_key, service_name=service_name):
            changed.append("RSAdminBot/manage_rs_bots.sh (service map)")
    else:
        if _patch_simple_service_map(path=RSADMINBOT_DIR / "manage_mirror_bots.sh", bot_key=bot_key, service_name=service_name):
            changed.append("RSAdminBot/manage_mirror_bots.sh (service map)")
    if _patch_install_services_unit_list(service_name=service_name):
        changed.append("RSAdminBot/install_services.sh (units list)")
    if (bot_dir / "requirements.txt").exists() and _patch_bootstrap_venv_requirements(folder=folder):
        changed.append("RSAdminBot/bootstrap_venv.sh (requirements list)")

    if _patch_run_oracle_update_bots_map(bot_key=bot_key, folder=folder):
        changed.append("scripts/run_oracle_update_bots.py (BOT_KEY_TO_FOLDER)")

    print("\nPatched:")
    if changed:
        for c in changed:
            print(f"  - {c}")
    else:
        print("  (no changes needed; already wired)")

    if not args.no_push:
        # Stage only relevant paths
        stage_paths = [
            str(RSADMINBOT_DIR / "admin_bot.py"),
            str(RSADMINBOT_DIR / "config.json"),
            str(RSADMINBOT_DIR / "run_bot.sh"),
            str(RSADMINBOT_DIR / "botctl.sh"),
            str(RSADMINBOT_DIR / "manage_bots.sh"),
            str(RSADMINBOT_DIR / "manage_rs_bots.sh"),
            str(RSADMINBOT_DIR / "manage_mirror_bots.sh"),
            str(RSADMINBOT_DIR / "install_services.sh"),
            str(RSADMINBOT_DIR / "bootstrap_venv.sh"),
            str(REPO_ROOT / "scripts" / "run_oracle_update_bots.py"),
            str(unit_path),
            str(bot_dir),
        ]
        # filter missing
        stage_paths = [p for p in stage_paths if Path(p).exists()]
        _run(["git", "add", *stage_paths], cwd=REPO_ROOT)
        msg1 = f"rsbots py update: onboard {bot_key}"
        msg2 = f"Register {folder} as managed bot ({service_name}) and wire RSAdminBot + oracle updater for deploy."
        _run(["git", "commit", "-m", msg1, "-m", msg2], cwd=REPO_ROOT)
        _run(["git", "push"], cwd=REPO_ROOT)

    if args.no_deploy:
        print("Skipped deploy (--no-deploy).")
        return 0

    # Deploy to Oracle:
    # 1) Update rsadminbot first (ensures new .sh maps in live tree)
    # 2) Update the bot folder
    # 3) Install unit file into /etc/systemd/system + enable
    # 4) Restart + verify
    servers = _load_servers()
    server = _pick_server(servers, args.server_name)

    _run([sys.executable, str(REPO_ROOT / "scripts" / "run_oracle_update_bots.py"), "--group", ("rs" if group == "rs" else "mw"), "--bot", "rsadminbot"])
    _run([sys.executable, str(REPO_ROOT / "scripts" / "run_oracle_update_bots.py"), "--group", ("rs" if group == "rs" else "mw"), "--bot", bot_key])

    # Install unit into /etc (sudo) from rsbots-code on Oracle, then restart.
    _oracle_install_unit_via_ssh(server=server, service_name=service_name)
    _oracle_restart_and_verify_via_ssh(server=server, service_name=service_name)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

