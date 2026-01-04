#!/usr/bin/env python3
"""
Aggressive starter: run each bot script directly with a short timeout,
capture stdout/stderr, and print unified_runner log tails.

This avoids hanging inside unified_bot_runner when start_all does nothing.
"""
import sys
import os
import subprocess
import time
from pathlib import Path
from textwrap import indent

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

LOG_TAIL_LINES = 80
BOT_TIMEOUT_SECONDS = 12

# Bot script map (in priority order)
BOT_SCRIPTS = [
    ("testcenter", "neonxt/bots/testcenter_bot.py"),
    ("datamanagerbot", "neonxt/bots/datamanagerbot.py"),
    ("discumbot", "neonxt/bots/discumbot.py"),
    ("pingbot", "neonxt/bots/pingbot.py"),
]


def _safe_print(text: str, prefix: str = "    "):
    """Print text with replacement to avoid Windows cp1252 encode errors."""
    enc = sys.stdout.encoding or "utf-8"
    safe = text.strip().encode(enc, "replace").decode(enc, "replace")
    print(indent(safe, prefix))


def run_bot(bot_name: str, script_path: Path):
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["UNIFIED_FAST_START"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(project_root)
    cmd = [sys.executable, str(script_path)]

    print(f"- Starting {bot_name} (timeout {BOT_TIMEOUT_SECONDS}s)...", flush=True)
    proc = subprocess.Popen(
        cmd,
        cwd=project_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    try:
        out, err = proc.communicate(timeout=BOT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        print(f"  TIMEOUT {bot_name} (killed after {BOT_TIMEOUT_SECONDS}s)", flush=True)
    else:
        print(f"  EXIT {bot_name}: returncode={proc.returncode}", flush=True)

    if out:
        print("  STDOUT:")
        _safe_print(out)
    if err:
        print("  STDERR:")
        _safe_print(err)


def dump_log_tails():
    logs_dir = project_root / "logs"
    for bot in ["testcenter", "datamanagerbot", "discumbot", "pingbot"]:
        log_path = logs_dir / f"unified_runner_{bot}.log"
        print(f"\n=== {log_path.name} (last {LOG_TAIL_LINES} lines) ===")
        if not log_path.exists():
            print("  (no log file)")
            continue
        try:
            text = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            tail = text[-LOG_TAIL_LINES:] if len(text) > LOG_TAIL_LINES else text
            _safe_print("\n".join(tail), prefix="  ")
        except Exception as exc:
            print(f"  (failed to read log: {exc})")


def main():
    for name, rel_path in BOT_SCRIPTS:
        script_path = project_root / rel_path
        if not script_path.exists():
            print(f"- SKIP {name}: script not found at {script_path}")
            continue
        run_bot(name, script_path)
        # brief pause between bots
        time.sleep(1)

    print("\nLog tails after attempts:")
    dump_log_tails()


if __name__ == "__main__":
    main()

