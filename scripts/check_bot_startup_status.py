#!/usr/bin/env python3
"""
Helper script to check if MW bots have logged their startup status (no neonxt).
"""
import sys
import subprocess
from pathlib import Path

project_root = Path(__file__).parent.parent

# MW bots only
BOT_NAMES = ["datamanagerbot", "discumbot", "pingbot"]
BOT_SCRIPTS = {
    "datamanagerbot": "MWBots/MWDataManagerBot/datamanagerbot.py",
    "discumbot": "MWBots/MWDiscumBot/discumbot.py",
    "pingbot": "MWBots/MWPingBot/pingbot.py",
}
LOG_PREFIXES = {
    "datamanagerbot": "DATAMANAGER",
    "discumbot": "DISCUMBOT",
    "pingbot": "PINGBOT",
}


def _is_bot_running(script_name: str) -> bool:
    """True if a Python process is running that script."""
    try:
        out = subprocess.run(
            ["wmic", "process", "where", f'commandline like "%{script_name}%"', "get", "processid"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=project_root,
        )
        if out.returncode != 0:
            return False
        lines = [l.strip() for l in (out.stdout or "").strip().splitlines() if l.strip() and l.strip().isdigit()]
        return len(lines) > 0
    except Exception:
        return False


def check_startup_logs():
    """Check which bots are running and have logged startup status."""
    results = {}
    for name in BOT_NAMES:
        script_name = Path(BOT_SCRIPTS[name]).name
        running = _is_bot_running(script_name)
        log_path = project_root / "logs" / f"unified_runner_{name}.log"
        if not log_path.exists():
            log_path = project_root / "logs" / "Botlogs" / f"{name}logs.json"
        has_log = False
        if log_path.exists():
            try:
                content = log_path.read_text(encoding="utf-8", errors="ignore")[-10000:]
                prefix = LOG_PREFIXES.get(name, "")
                has_log = prefix in content or "STARTUP" in content or "START]" in content
            except Exception:
                pass
        results[name] = {"running": running, "has_log": has_log}
    return results

if __name__ == '__main__':
    results = check_startup_logs()
    
    # Count running and logged
    running_count = sum(1 for r in results.values() if r['running'])
    logged_count = sum(1 for r in results.values() if r['has_log'])
    total = len(results)
    
    # Print status
    print(f'Processes: {running_count}/{total} running')
    print(f'Startup Logs: {logged_count}/{total} bots logged status')
    
    # Print individual status
    for name, info in results.items():
        status = 'RUNNING' if info['running'] else 'STOPPED'
        log_status = 'HAS LOG' if info['has_log'] else 'NO LOG'
        print(f'  {name}: {status} - {log_status}')
    
    # Exit code: 0 if all running and logged, 1 otherwise
    all_ready = (
        all(r['running'] for r in results.values()) and
        all(r['has_log'] for r in results.values())
    )
    sys.exit(0 if all_ready else 1)

