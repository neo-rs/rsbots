#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mirror_world_config import load_oracle_servers, resolve_oracle_ssh_key_path


def _pick_server(servers: List[Dict[str, Any]], server_name: Optional[str]) -> Dict[str, Any]:
    if not servers:
        raise ValueError("No servers configured in oraclekeys/servers.json")
    if server_name and server_name.strip():
        target = server_name.strip()
        for s in servers:
            if str(s.get("name", "")).strip() == target:
                return s
        raise ValueError(f"Server name not found in oraclekeys/servers.json: {target}")
    return servers[0]


def _build_ssh_cmd(*, user: str, host: str, key_path: Path, ssh_options: str, remote_cmd: str) -> List[str]:
    cmd = [
        "ssh",
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
    ]
    if ssh_options:
        cmd.extend(shlex.split(ssh_options))
    cmd.append(f"{user}@{host}")
    # Pass a single remote command string so ssh does not split `bash -lc` arguments incorrectly.
    cmd.append(f"bash -lc {shlex.quote(remote_cmd)}")
    return cmd


def _scp_file(*, user: str, host: str, key_path: Path, ssh_options: str, local_path: Path, remote_path: str) -> int:
    """
    Copy a single file to Oracle via scp.
    Returns scp exit code.
    """
    cmd = [
        "scp",
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
    ]
    if ssh_options:
        cmd.extend(shlex.split(ssh_options))
    src = str(local_path)
    dst = f"{user}@{host}:{remote_path}"
    cmd.extend([src, dst])
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.stdout:
        print(res.stdout.strip())
    if res.stderr:
        err = (res.stderr or "").strip()
        if err:
            print(err[:2000])
    return res.returncode


def _action_remote_cmd(action: str, remote_root: str) -> str:
    cd_root = f"cd {shlex.quote(remote_root)}"
    if action == "verify":
        return f"{cd_root}; python3 scripts/verify_discum_channel_ids.py"
    if action == "verify-remove":
        return f"{cd_root}; python3 scripts/verify_discum_channel_ids.py --remove-failed"
    if action == "post":
        return f"{cd_root}; python3 MWDiscumBot/post_mirror_channel_map.py"
    if action == "post-no-cleanup":
        return f"{cd_root}; python3 MWDiscumBot/post_mirror_channel_map.py --no-cleanup"
    if action == "post-upsert":
        return f"{cd_root}; python3 MWDiscumBot/post_mirror_channel_map.py --upsert"
    if action == "refresh":
        return (
            f"{cd_root}; "
            "python3 scripts/verify_discum_channel_ids.py && "
            "python3 MWDiscumBot/post_mirror_channel_map.py"
        )
    raise ValueError(f"Unsupported action: {action}")


def _action_remote_cmd_with_ids(action: str, remote_root: str, ids: Optional[List[str]]) -> str:
    """
    Build remote verify/post command, optionally limiting verify to specific channel IDs.
    """
    cd_root = f"cd {shlex.quote(remote_root)}"
    if action == "verify":
        if ids:
            return f"{cd_root}; python3 scripts/verify_discum_channel_ids.py " + " ".join(shlex.quote(i) for i in ids)
        return f"{cd_root}; python3 scripts/verify_discum_channel_ids.py"
    if action == "verify-remove":
        if ids:
            return f"{cd_root}; python3 scripts/verify_discum_channel_ids.py --remove-failed " + " ".join(shlex.quote(i) for i in ids)
        return f"{cd_root}; python3 scripts/verify_discum_channel_ids.py --remove-failed"
    if action == "post":
        return f"{cd_root}; python3 MWDiscumBot/post_mirror_channel_map.py"
    if action == "post-no-cleanup":
        return f"{cd_root}; python3 MWDiscumBot/post_mirror_channel_map.py --no-cleanup"
    if action == "post-upsert":
        return f"{cd_root}; python3 MWDiscumBot/post_mirror_channel_map.py --upsert"
    if action == "refresh":
        # refresh ignores ids for now (verify -> post chain)
        return (
            f"{cd_root}; "
            "python3 scripts/verify_discum_channel_ids.py && "
            "python3 MWDiscumBot/post_mirror_channel_map.py"
        )
    raise ValueError(f"Unsupported action: {action}")


def _targets_for_action(action: str) -> List[str]:
    if action in {"verify", "verify-remove"}:
        return ["scripts/verify_discum_channel_ids.py"]
    if action in {"post", "post-no-cleanup", "post-upsert"}:
        return ["MWDiscumBot/post_mirror_channel_map.py"]
    if action == "refresh":
        return [
            "scripts/verify_discum_channel_ids.py",
            "MWDiscumBot/post_mirror_channel_map.py",
        ]
    raise ValueError(f"Unsupported action: {action}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Run Discum mapping tools on Oracle over SSH.")
    ap.add_argument(
        "action",
        choices=["verify", "verify-remove", "post", "post-no-cleanup", "post-upsert", "refresh"],
        help="Remote Discum mapping action to execute",
    )
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json (default: first)")
    ap.add_argument("--remote-root", default=None, help="Override remote repo root")
    ap.add_argument(
        "--ids",
        default=None,
        help="Optional comma-separated Discord channel IDs to verify (only for verify/verify-remove).",
    )
    args = ap.parse_args(argv)

    servers, _ = load_oracle_servers(REPO_ROOT)
    server = _pick_server(servers, args.server_name)

    user = str(server.get("user", "rsadmin"))
    host = str(server.get("host", "")).strip()
    key_value = str(server.get("key", "")).strip()
    ssh_options = str(server.get("ssh_options", "") or "")
    remote_root = str(args.remote_root or server.get("remote_root") or server.get("live_root") or "/home/rsadmin/bots/mirror-world")

    if not host:
        raise ValueError("servers.json entry missing host")
    if not key_value:
        raise ValueError("servers.json entry missing key")

    key_path = resolve_oracle_ssh_key_path(key_value, REPO_ROOT)
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key not found: {key_path}")

    ids: Optional[List[str]] = None
    if args.ids and args.action in {"verify", "verify-remove"}:
        raw = str(args.ids).strip()
        # Allow either comma-separated or space-separated tokens.
        parts = []
        for token in raw.replace(" ", ",").split(","):
            token = token.strip()
            if token:
                parts.append(token)
        ids = parts

    remote_cmd = _action_remote_cmd_with_ids(args.action, remote_root, ids)
    ssh_cmd = _build_ssh_cmd(
        user=user,
        host=host,
        key_path=key_path,
        ssh_options=ssh_options,
        remote_cmd=remote_cmd,
    )

    # Preflight: confirm remote root and required files before running action.
    targets = _targets_for_action(args.action)
    checks = " ".join(shlex.quote(t) for t in targets)
    preflight_cmd = (
        f"cd {shlex.quote(remote_root)}; "
        "pwd; "
        f"for f in {checks}; do "
        "if [ -f \"$f\" ]; then echo \"FOUND:$f\"; else echo \"MISSING:$f\"; fi; "
        "done"
    )
    preflight_ssh = _build_ssh_cmd(
        user=user,
        host=host,
        key_path=key_path,
        ssh_options=ssh_options,
        remote_cmd=preflight_cmd,
    )

    print(f"Server: {user}@{host}")
    print(f"Remote root: {remote_root}")
    print(f"Action: {args.action}")
    print(f"Targets: {', '.join(targets)}")
    print("Preflight: checking remote cwd + target files...")
    preflight_res = subprocess.run(preflight_ssh, capture_output=True, text=True)
    if preflight_res.stdout:
        print(preflight_res.stdout.rstrip())
    if preflight_res.stderr:
        print(preflight_res.stderr.rstrip())
    preflight_rc = preflight_res.returncode
    if preflight_rc != 0:
        print("\nERROR: Preflight check failed. Not running action.")
        return preflight_rc

    # For verify actions we want the *exact* current local script deployed to the server
    # (Oracle may be behind, and script paths differ between layouts).
    if args.action in {"verify", "verify-remove", "refresh"}:
        verify_local = REPO_ROOT / "scripts" / "verify_discum_channel_ids.py"
        verify_remote = f"{remote_root.rstrip('/')}/scripts/verify_discum_channel_ids.py"
        if not verify_local.exists():
            print(f"\nERROR: Local verify script missing: {verify_local}")
            return 3
        print("\nUploading (overwriting) verify script to Oracle...")
        rc = _scp_file(
            user=user,
            host=host,
            key_path=key_path,
            ssh_options=ssh_options,
            local_path=verify_local,
            remote_path=verify_remote,
        )
        if rc != 0:
            print("\nERROR: SCP upload failed. Not running action.")
            return rc

        # Re-run preflight so FOUND/MISSING reflects post-upload state.
        preflight_res = subprocess.run(preflight_ssh, capture_output=True, text=True)
        if preflight_res.stdout:
            print(preflight_res.stdout.rstrip())
        if preflight_res.stderr:
            print(preflight_res.stderr.rstrip())

        if "MISSING:" in (preflight_res.stdout or ""):
            print("\nERROR: Required Oracle file(s) are still missing after upload.")
            return 4

    missing_files = []
    for ln in (preflight_res.stdout or "").splitlines():
        if ln.startswith("MISSING:"):
            missing_files.append(ln.split("MISSING:", 1)[1].strip())

    if "MISSING:" in (preflight_res.stdout or "") and args.action not in {"verify", "verify-remove", "refresh"}:
        print("\nERROR: Required Oracle file(s) are missing. Not running action.")
        return 2

    print("Running on Oracle...\n")

    return subprocess.call(ssh_cmd)


if __name__ == "__main__":
    raise SystemExit(main())

