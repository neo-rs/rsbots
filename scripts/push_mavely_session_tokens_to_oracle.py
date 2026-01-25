"""
Push current Mavely session tokens from LOCAL -> Oracle RSForwarder.

Why:
- Oracle cannot reliably use your browser cookies (cf_clearance context binding).
- Oracle also rejects refreshToken (invalid_grant).
- But Oracle CAN use the current bearer token / idToken for GraphQL.

This script:
1) Syncs tokens from local /api/auth/session (requires valid cookies file)
2) Uploads bearer + idToken to Oracle (temporary files)
3) Updates RSForwarder/config.secrets.json on Oracle (server-only)
4) Restarts RSForwarder by killing the python process (systemd restarts it)

Safe output:
- Never prints token/cookie contents.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _run(args: list[str]) -> int:
    return subprocess.call(args)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]

    servers_path = repo_root / "oraclekeys" / "servers.json"
    servers = json.loads(servers_path.read_text(encoding="utf-8"))
    if not isinstance(servers, list) or not servers:
        print("ERROR: oraclekeys/servers.json is empty.")
        return 2

    srv = servers[0]
    user = str(srv.get("user") or "").strip()
    host = str(srv.get("host") or "").strip()
    key = str(srv.get("key") or "").strip()
    ssh_opts = str(srv.get("ssh_options") or "").strip()
    remote_root = str(srv.get("remote_root") or "").strip() or "/home/rsadmin/bots/mirror-world"
    if not (user and host and key):
        print("ERROR: missing user/host/key in oraclekeys/servers.json")
        return 2

    key_path = (repo_root / "oraclekeys" / key).resolve()
    if not key_path.exists():
        # fall back to repo root
        key_path = (repo_root / key).resolve()
    if not key_path.exists():
        print(f"ERROR: SSH key not found: {key}")
        return 2

    cookies_file = repo_root / "Instorebotforwarder" / "mavely_cookies.txt"
    refresh_file = repo_root / "Instorebotforwarder" / "mavely_refresh_token.txt"
    auth_file = repo_root / "Instorebotforwarder" / "mavely_auth_token.txt"
    id_file = repo_root / "Instorebotforwarder" / "mavely_id_token.txt"

    # 1) Sync from session
    sync = repo_root / "scripts" / "sync_mavely_session_tokens.py"
    code = _run(
        [
            sys.executable,
            str(sync),
            "--cookies-file",
            str(cookies_file),
            "--refresh-file",
            str(refresh_file),
            "--auth-file",
            str(auth_file),
            "--id-file",
            str(id_file),
        ]
    )
    if code != 0:
        return code

    # 2) Upload to Oracle (temp files under /home/<user>/)
    target = f"{user}@{host}"
    scp_base = ["scp", "-i", str(key_path)]
    if ssh_opts:
        # scp uses -o for options; split on spaces (best effort)
        for part in ssh_opts.split():
            if part.strip():
                scp_base.extend(part.strip().split())
    scp_base.extend(["-o", "StrictHostKeyChecking=no"])

    code = _run(scp_base + [str(auth_file), f"{target}:/home/{user}/mavely_auth_token.txt"])
    if code != 0:
        return code
    code = _run(scp_base + [str(id_file), f"{target}:/home/{user}/mavely_id_token.txt"])
    if code != 0:
        return code

    # 3) Update config.secrets.json on Oracle and delete temp files
    ssh_base = ["ssh", "-i", str(key_path)]
    if ssh_opts:
        for part in ssh_opts.split():
            if part.strip():
                ssh_base.extend(part.strip().split())
    ssh_base.extend(["-o", "StrictHostKeyChecking=no", target])

    remote_py = (
        "python3 -c \""
        "import json; from pathlib import Path; "
        f"root=Path('{remote_root}'); cfg=root/'RSForwarder'/'config.secrets.json'; "
        f"auth=Path('/home/{user}/mavely_auth_token.txt').read_text(encoding='utf-8',errors='replace').strip(); "
        f"idt=Path('/home/{user}/mavely_id_token.txt').read_text(encoding='utf-8',errors='replace').strip(); "
        "d=json.loads(cfg.read_text(encoding='utf-8')); "
        "d['mavely_auth_token']=auth; d['mavely_id_token']=idt; "
        "cfg.write_text(json.dumps(d, indent=2, ensure_ascii=True)+'\\n', encoding='utf-8'); "
        "print('updated=1');\""
    )
    remote_cmd = (
        f"cd {remote_root} && {remote_py} "
        f"&& rm -f /home/{user}/mavely_auth_token.txt /home/{user}/mavely_id_token.txt "
        "&& pkill -f '[r]s_forwarder_bot.py' || true"
    )
    code = _run(ssh_base + [remote_cmd])
    return code


if __name__ == "__main__":
    raise SystemExit(main())

