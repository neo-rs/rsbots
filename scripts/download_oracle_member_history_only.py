#!/usr/bin/env python3
"""
Download ONE Oracle runtime file: RSCheckerbot/member_history.json
-------------------------------------------------------------------
This intentionally fetches only this single JSON file (no full snapshot, no other runtime data).

Default behavior:
- Downloads into a timestamped folder under --out-dir
- Optionally overwrites the local `RSCheckerbot/member_history.json` when --apply-to-local is set.
"""

from __future__ import annotations

import argparse
import shutil
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mirror_world_config import load_oracle_servers, resolve_oracle_ssh_key_path


def _pick_server(servers: list[Dict[str, Any]], server_name: Optional[str]) -> Dict[str, Any]:
    if not servers:
        raise ValueError("No servers configured in oraclekeys/servers.json")
    if server_name and server_name.strip():
        name = server_name.strip()
        for s in servers:
            if str(s.get("name", "")).strip() == name:
                return s
        raise ValueError(f"Server name not found in oraclekeys/servers.json: {name}")
    return servers[0]


def _local_copy_if_possible(remote_root: str, local_out_file: Path) -> Tuple[bool, str]:
    """
    If we're running on the Oracle host itself (local-exec mode),
    the `remote_root` path can be accessed directly and we can copy without SSH.
    """
    p = Path(remote_root)
    src = p / "RSCheckerbot" / "member_history.json"
    if p.is_dir() and src.exists():
        local_out_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, local_out_file)
        return True, "local-exec copy"
    return False, ""


def _scp_one(*, user: str, host: str, key_path: Path, ssh_options: str, remote_path: str, local_path: Path) -> Tuple[bool, str]:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    scp_cmd = ["scp", "-i", str(key_path), "-o", "StrictHostKeyChecking=no"]
    if ssh_options:
        scp_cmd.extend(shlex.split(ssh_options))
    src = f"{user}@{host}:{remote_path}"
    scp_cmd.extend([src, str(local_path)])

    res = subprocess.run(scp_cmd, capture_output=True, text=True)
    if res.returncode == 0 and local_path.exists():
        return True, ""
    msg = (res.stderr or res.stdout or "").strip()
    return False, msg[:300] if msg else f"scp_failed_exit_{res.returncode}"


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Download ONLY RSCheckerbot/member_history.json from Oracle server.")
    ap.add_argument("--server-name", default=None, help="Server name from oraclekeys/servers.json (defaults to first entry)")
    ap.add_argument("--out-dir", default=str(REPO_ROOT / "Oraclserver-files"), help="Output dir for downloaded file")
    ap.add_argument("--remote-root", default=None, help="Remote root override (defaults to server's remote_root/live_root)")
    ap.add_argument(
        "--apply-to-local",
        action="store_true",
        help="After download, overwrite local RSCheckerbot/member_history.json with the downloaded file.",
    )
    args = ap.parse_args(argv)

    servers, _ = load_oracle_servers(REPO_ROOT)
    s = _pick_server(servers, args.server_name)

    user = str(s.get("user", "rsadmin"))
    host = str(s.get("host", "")).strip()
    key_val = str(s.get("key", "")).strip()
    ssh_options = str(s.get("ssh_options", "") or "")
    if not host:
        raise ValueError("servers.json entry missing host")
    if not key_val:
        raise ValueError("servers.json entry missing key")

    remote_root = str(
        args.remote_root
        or s.get("remote_root")
        or s.get("live_root")
        or "/home/rsadmin/bots/mirror-world"
    )
    remote_path = f"{remote_root.rstrip('/')}/RSCheckerbot/member_history.json"

    key_path = resolve_oracle_ssh_key_path(key_val, REPO_ROOT)
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key not found: {key_path}")

    out_dir = Path(args.out_dir).resolve()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir = out_dir / f"server_member_history_{ts}"
    downloaded_path = dest_dir / "member_history.json"

    print(f"Server:   {user}@{host}")
    print(f"Remote:   {remote_path}")
    print(f"Download: {downloaded_path}")
    print()

    ok_local, how = _local_copy_if_possible(remote_root, downloaded_path)
    if ok_local:
        print(f"DONE: {downloaded_path} ({how})")
    else:
        ok, msg = _scp_one(
            user=user,
            host=host,
            key_path=key_path,
            ssh_options=ssh_options,
            remote_path=remote_path,
            local_path=downloaded_path,
        )
        if not ok:
            raise RuntimeError(f"Failed to download member_history.json: {msg}")
        print(f"DONE: {downloaded_path}")

    if args.apply_to_local:
        local_dest = REPO_ROOT / "RSCheckerbot" / "member_history.json"
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(downloaded_path, local_dest)
        print(f"Applied to local: {local_dest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

