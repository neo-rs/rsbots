#!/usr/bin/env python3
"""
Sync Oracle Server Runtime Data
--------------------------------
Downloads all runtime JSON files from Oracle server to local for analysis.
These files are NOT synced per CANONICAL_RULES (runtime data only, not code).

Per CANONICAL_RULES:
- Runtime JSON files (tickets.json, registry.json, etc.) are NOT synced
- Only config.json and messages.json are synced
- This script downloads runtime data for analysis only
"""

import json
import sys
import subprocess
import shutil
import shlex
from pathlib import Path
from datetime import datetime, timezone

# Add repo root to path
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Load server config
SERVER_CONFIG_PATH = _REPO_ROOT / "oraclekeys" / "servers.json"
try:
    with open(SERVER_CONFIG_PATH, "r", encoding="utf-8") as f:
        servers = json.load(f)
    server_config = servers[0] if servers else None
    if not server_config:
        raise ValueError("No server config found")
except Exception as e:
    print(f"Error loading server config: {e}")
    sys.exit(1)

SSH_KEY_PATH = _REPO_ROOT / "oraclekeys" / server_config["key"]
SSH_USER = server_config["user"]
SSH_HOST = server_config["host"]
SSH_OPTIONS = server_config.get("ssh_options", "")
REMOTE_ROOT = str(server_config.get("remote_root") or server_config.get("live_root") or "/home/rsadmin/bots/mirror-world")

# If we are running on the Oracle host (local-exec mode), prefer direct filesystem copy.
# This avoids SSH-to-self failures (missing key, SSH blocked, etc.) that otherwise appear as "missing".
_LOCAL_REMOTE_ROOT = Path(REMOTE_ROOT)
LOCAL_MODE = _LOCAL_REMOTE_ROOT.is_dir() and (_LOCAL_REMOTE_ROOT / "RSAdminBot").is_dir()

# Output directory
OUTPUT_DIR = _REPO_ROOT / "OracleServerData"
OUTPUT_DIR.mkdir(exist_ok=True)

# Files to download per bot (runtime data only, NOT synced per CANONICAL_RULES)
BOT_RUNTIME_FILES = {
    "RSCheckerbot": [
        "member_history.json",
        "whop_identity_cache.json",
        "trial_history.json",
        "identity_conflicts.jsonl",
        "whop_webhook_raw_payloads.json",
        "whop_resolution_alert_state.json",
        "staff_alerts.json",
        "payment_cache.json",
        "boot_state.json",
        "reporting_store.json",
        "registry.json",
        "queue.json",
        "invites.json",
    ],
    "RSAdminBot": [
        "whop_data/whop_history.json",
        "whop_data/whop_scan_history.json",
        "whop_data/bot_movements/rscheckerbot_movements.json",
        "whop_data/bot_movements/rsforwarder_movements.json",
        "whop_data/bot_movements/rsmentionpinger_movements.json",
        "whop_data/bot_movements/rsonboarding_movements.json",
        "whop_data/bot_movements/rssuccessbot_movements.json",
    ],
    "RSOnboarding": [
        "tickets.json",
    ],
    "RSuccessBot": [
        "success_points.json",
    ],
}


def run_ssh_command(cmd: str) -> tuple[int, str, str]:
    """Run SSH command on Oracle server"""
    ssh_cmd = [
        "ssh",
        "-i", str(SSH_KEY_PATH),
        "-o", "StrictHostKeyChecking=no",
    ]
    if SSH_OPTIONS:
        ssh_cmd.extend(SSH_OPTIONS.split())
    ssh_cmd.append(f"{SSH_USER}@{SSH_HOST}")
    ssh_cmd.append(cmd)
    
    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", "SSH command timed out"
    except Exception as e:
        return 1, "", str(e)


def download_file(remote_path: str, local_path: Path) -> tuple[bool, str]:
    """Download a single file from Oracle server using scp (or local copy when LOCAL_MODE is True)."""
    if LOCAL_MODE:
        try:
            src = Path(remote_path)
            if not src.exists():
                return False, "missing_local_file"
            shutil.copy2(src, local_path)
            return True, ""
        except Exception as e:
            return False, f"local_copy_failed: {e}"

    scp_cmd = [
        "scp",
        "-i", str(SSH_KEY_PATH),
        "-o", "StrictHostKeyChecking=no",
        f"{SSH_USER}@{SSH_HOST}:{remote_path}",
        str(local_path),
    ]
    
    try:
        result = subprocess.run(
            scp_cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            return True, ""
        err = (result.stderr or result.stdout or "").strip()
        return False, (err[:300] if err else "scp_failed")
    except Exception as e:
        return False, str(e)


def check_file_exists(remote_path: str) -> tuple[bool, str]:
    """Check if file exists on remote server (or locally when LOCAL_MODE is True).

    Returns:
        (exists, err) where err is non-empty when the check itself failed (e.g., SSH error).
    """
    if LOCAL_MODE:
        try:
            return Path(remote_path).is_file(), ""
        except Exception as e:
            return False, f"local_stat_failed: {e}"

    cmd = f"test -f {shlex.quote(remote_path)} && echo 'EXISTS' || echo 'NOT_EXISTS'"
    code, stdout, stderr = run_ssh_command(cmd)
    if code != 0:
        err = (stderr or stdout or "").strip()
        return False, (err[:300] if err else "ssh_failed")
    return ("EXISTS" in (stdout or "")), ""


def get_file_meta(remote_path: str) -> tuple[bool, dict, str]:
    """Return (exists, meta, err) for a remote file (or local when LOCAL_MODE is True).

    meta keys:
      - size_bytes (int)
      - mtime_unix (int)
      - mtime_iso (str)
    """
    if LOCAL_MODE:
        try:
            p = Path(remote_path)
            if not p.is_file():
                return False, {}, ""
            st = p.stat()
            mtime = int(st.st_mtime)
            return True, {
                "size_bytes": int(st.st_size),
                "mtime_unix": mtime,
                "mtime_iso": datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat(),
            }, ""
        except Exception as e:
            return False, {}, f"local_stat_failed: {e}"

    # GNU stat: size + mtime epoch seconds
    cmd = f"stat -c '%s %Y' {shlex.quote(remote_path)} 2>/dev/null || echo 'MISSING'"
    code, stdout, stderr = run_ssh_command(cmd)
    if code != 0:
        err = (stderr or stdout or "").strip()
        return False, {}, (err[:300] if err else "ssh_failed")
    out = (stdout or "").strip()
    if not out or out == "MISSING":
        return False, {}, ""
    try:
        size_s, mtime_s = out.split(None, 1)
        size_i = int(size_s)
        mtime_i = int(mtime_s)
        return True, {
            "size_bytes": size_i,
            "mtime_unix": mtime_i,
            "mtime_iso": datetime.fromtimestamp(mtime_i, tz=timezone.utc).isoformat(),
        }, ""
    except Exception:
        return True, {"raw": out[:200]}, ""


def sync_bot_data(bot_name: str, files: list[str]) -> dict:
    """Sync runtime data files for a single bot"""
    bot_output_dir = OUTPUT_DIR / bot_name
    bot_output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        "bot": bot_name,
        "downloaded": [],
        "missing": [],
        "errors": [],
    }
    
    print(f"\n[{bot_name}]")
    print("-" * 60)
    
    for file_path in files:
        remote_path = f"{REMOTE_ROOT}/{bot_name}/{file_path}"
        local_path = bot_output_dir / file_path
        
        # Create subdirectories if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists + capture remote metadata
        exists, meta, meta_err = get_file_meta(remote_path)
        check_err = meta_err
        if check_err:
            results["errors"].append({"file": file_path, "remote_path": remote_path, "error": check_err})
            print(f"  [ERR]  {file_path} - Check failed ({check_err})")
            continue
        if not exists:
            results["missing"].append(file_path)
            print(f"  [WARN] {file_path} - Not found on server")
            continue
        
        # Download file
        ok_dl, dl_err = download_file(remote_path, local_path)
        if ok_dl:
            file_size = local_path.stat().st_size if local_path.exists() else 0
            results["downloaded"].append({
                "file": file_path,
                "size": file_size,
                "remote_meta": meta,
                "remote_path": remote_path,
                "local_path": str(local_path),
            })
            m_iso = str((meta or {}).get("mtime_iso") or "")
            m_s = f", mtime={m_iso}" if m_iso else ""
            print(f"  [OK]   {file_path} ({file_size:,} bytes{m_s})")
        else:
            results["errors"].append({"file": file_path, "remote_path": remote_path, "error": dl_err})
            print(f"  [ERR]  {file_path} - Download failed ({dl_err})")
    
    return results


def create_manifest(all_results: list[dict]) -> dict:
    """Create manifest file with sync results"""
    manifest = {
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
        "server": {
            "host": SSH_HOST,
            "user": SSH_USER,
            "remote_root": REMOTE_ROOT,
            "local_mode": bool(LOCAL_MODE),
        },
        "bots": {},
    }
    
    total_downloaded = 0
    total_missing = 0
    total_errors = 0
    
    for result in all_results:
        bot_name = result["bot"]
        manifest["bots"][bot_name] = {
            "downloaded": result["downloaded"],
            "missing": result["missing"],
            "errors": result["errors"],
            "summary": {
                "downloaded_count": len(result["downloaded"]),
                "missing_count": len(result["missing"]),
                "errors_count": len(result["errors"]),
            }
        }
        total_downloaded += len(result["downloaded"])
        total_missing += len(result["missing"])
        total_errors += len(result["errors"])
    
    manifest["summary"] = {
        "total_downloaded": total_downloaded,
        "total_missing": total_missing,
        "total_errors": total_errors,
    }
    
    return manifest


def main():
    """Main sync function"""
    print("=" * 60)
    print("Oracle Server Runtime Data Sync")
    print("=" * 60)
    print(f"Server: {SSH_USER}@{SSH_HOST}")
    print(f"Remote Root: {REMOTE_ROOT}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print("=" * 60)
    
    all_results = []
    
    for bot_name, files in BOT_RUNTIME_FILES.items():
        result = sync_bot_data(bot_name, files)
        all_results.append(result)
    
    # Create manifest
    manifest = create_manifest(all_results)
    manifest_path = OUTPUT_DIR / "sync_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 60)
    print("SYNC SUMMARY")
    print("=" * 60)
    print(f"Total Downloaded: {manifest['summary']['total_downloaded']}")
    print(f"Total Missing: {manifest['summary']['total_missing']}")
    print(f"Total Errors: {manifest['summary']['total_errors']}")
    print(f"\nManifest saved to: {manifest_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()

