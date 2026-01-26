"""
noVNC stack helper (RSForwarder)

Starts a localhost-only desktop stack on Linux:
- Xvfb (virtual X display)
- fluxbox (window manager)
- x11vnc (VNC server, localhost only)
- websockify/noVNC (browser client on localhost)

Used to run interactive Playwright login flows (Mavely) on a headless server.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _cfg_or_env_str(cfg: dict, cfg_key: str, env_key: str, default: str) -> str:
    v = str((cfg or {}).get(cfg_key) or "").strip()
    if v:
        return v
    v = (os.getenv(env_key, "") or "").strip()
    return v or default


def _cfg_or_env_int(cfg: dict, cfg_key: str, env_key: str, default: int) -> int:
    raw = str((cfg or {}).get(cfg_key) or "").strip() or (os.getenv(env_key, "") or "").strip()
    try:
        return int(raw) if raw else int(default)
    except Exception:
        return int(default)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_pid(p: Path) -> int:
    try:
        return int((p.read_text(encoding="utf-8", errors="replace") or "").strip() or "0")
    except Exception:
        return 0


def _write_pid(p: Path, pid: int) -> None:
    try:
        p.write_text(str(int(pid)) + "\n", encoding="utf-8")
    except Exception:
        pass


def _port_listening(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=0.4):
            return True
    except Exception:
        return False


def _which(name: str) -> Optional[str]:
    try:
        return shutil.which(name)
    except Exception:
        return None


def _find_novnc_web_root(cfg: dict) -> Optional[str]:
    explicit = _cfg_or_env_str(cfg, "mavely_novnc_web_root", "MAVELY_NOVNC_WEB_ROOT", "").strip()
    if explicit:
        p = Path(explicit)
        if p.exists() and p.is_dir():
            return str(p)
    candidates = [
        "/usr/share/novnc",
        "/usr/share/noVNC",
        "/usr/share/novnc/noVNC",
        "/opt/novnc",
        "/opt/noVNC",
        "/usr/local/share/novnc",
        "/usr/local/share/noVNC",
    ]
    for c in candidates:
        p = Path(c)
        if p.exists() and p.is_dir() and (p / "vnc.html").exists():
            return str(p)
    return None


def _ensure_proc(
    *,
    name: str,
    pid_file: Path,
    cmd: list,
    env: dict,
    cwd: Optional[Path],
    log_file: Path,
) -> Tuple[bool, Optional[str]]:
    existing = _read_pid(pid_file)
    if existing and _pid_alive(existing):
        return True, None

    # If port-based process restarted previously, PID file can be stale.
    try:
        pid_file.unlink(missing_ok=True)  # type: ignore[arg-type]
    except Exception:
        pass

    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a", encoding="utf-8", errors="replace") as lf:
            p = subprocess.Popen(
                cmd,
                cwd=str(cwd) if cwd else None,
                env=env,
                stdout=lf,
                stderr=lf,
                stdin=subprocess.DEVNULL,
                close_fds=True,
            )
        _write_pid(pid_file, int(p.pid))
        return True, None
    except FileNotFoundError:
        return False, f"{name} command not found: {cmd[0]}"
    except Exception as e:
        return False, f"Failed to start {name}: {e}"


def ensure_novnc(cfg: dict) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Ensure a localhost-only noVNC stack is running.

    Returns: (info, error)
    info contains: display, vnc_port, web_port, url_path
    """
    if os.name == "nt":
        return None, "noVNC stack is only supported on Linux hosts."

    display = _cfg_or_env_str(cfg, "mavely_novnc_display", "MAVELY_NOVNC_DISPLAY", ":99")
    vnc_port = _cfg_or_env_int(cfg, "mavely_novnc_vnc_port", "MAVELY_NOVNC_VNC_PORT", 5901)
    web_port = _cfg_or_env_int(cfg, "mavely_novnc_web_port", "MAVELY_NOVNC_WEB_PORT", 6080)
    screen = _cfg_or_env_str(cfg, "mavely_novnc_screen", "MAVELY_NOVNC_SCREEN", "1280x720x24")
    state_dir = Path(_cfg_or_env_str(cfg, "mavely_novnc_state_dir", "MAVELY_NOVNC_STATE_DIR", "/tmp/rsforwarder_mavely_novnc"))

    if _port_listening("127.0.0.1", int(web_port)) and _port_listening("127.0.0.1", int(vnc_port)):
        return {
            "display": display,
            "vnc_port": int(vnc_port),
            "web_port": int(web_port),
            "url_path": "/vnc.html",
            "state_dir": str(state_dir),
        }, None

    for required in ("Xvfb", "x11vnc", "websockify"):
        if not _which(required):
            return None, f"Missing required command on server: {required}"

    web_root = _find_novnc_web_root(cfg)
    if not web_root:
        return None, "Could not locate noVNC web root (expected vnc.html). Set MAVELY_NOVNC_WEB_ROOT."

    env = dict(os.environ)
    env["DISPLAY"] = display

    state_dir.mkdir(parents=True, exist_ok=True)
    pid_xvfb = state_dir / "xvfb.pid"
    pid_wm = state_dir / "fluxbox.pid"
    pid_vnc = state_dir / "x11vnc.pid"
    pid_ws = state_dir / "websockify.pid"

    ok, err = _ensure_proc(
        name="Xvfb",
        pid_file=pid_xvfb,
        cmd=["Xvfb", display, "-screen", "0", screen, "-nolisten", "tcp", "-ac"],
        env=env,
        cwd=None,
        log_file=state_dir / "xvfb.log",
    )
    if not ok:
        return None, err

    # Optional window manager; noVNC still works without it, but UX is much better.
    if _which("fluxbox"):
        _ensure_proc(
            name="fluxbox",
            pid_file=pid_wm,
            cmd=["fluxbox", "-display", display],
            env=env,
            cwd=None,
            log_file=state_dir / "fluxbox.log",
        )

    ok, err = _ensure_proc(
        name="x11vnc",
        pid_file=pid_vnc,
        cmd=[
            "x11vnc",
            "-display",
            display,
            "-rfbport",
            str(int(vnc_port)),
            "-localhost",
            "-forever",
            "-shared",
            "-nopw",
        ],
        env=env,
        cwd=None,
        log_file=state_dir / "x11vnc.log",
    )
    if not ok:
        return None, err

    ok, err = _ensure_proc(
        name="websockify",
        pid_file=pid_ws,
        cmd=[
            "websockify",
            "--web",
            web_root,
            f"127.0.0.1:{int(web_port)}",
            f"127.0.0.1:{int(vnc_port)}",
        ],
        env=env,
        cwd=None,
        log_file=state_dir / "websockify.log",
    )
    if not ok:
        return None, err

    # Give the listeners a moment to bind
    deadline = time.time() + 3.0
    while time.time() < deadline:
        if _port_listening("127.0.0.1", int(web_port)) and _port_listening("127.0.0.1", int(vnc_port)):
            break
        time.sleep(0.1)

    if not _port_listening("127.0.0.1", int(web_port)):
        return None, f"noVNC did not bind to localhost:{int(web_port)} (check {state_dir}/websockify.log)."
    if not _port_listening("127.0.0.1", int(vnc_port)):
        return None, f"VNC did not bind to localhost:{int(vnc_port)} (check {state_dir}/x11vnc.log)."

    return {
        "display": display,
        "vnc_port": int(vnc_port),
        "web_port": int(web_port),
        "url_path": "/vnc.html",
        "state_dir": str(state_dir),
    }, None


def start_cookie_refresher(cfg: dict, *, display: str, wait_login_s: int = 900) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Start mavely_cookie_refresher in interactive mode inside the given DISPLAY.

    Returns: (pid, log_path, error)
    """
    if os.name == "nt":
        return None, None, "Cookie refresher is intended to run on the Linux host."

    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "RSForwarder" / "mavely_cookie_refresher.py"
    if not script.exists():
        return None, None, f"Missing script: {script}"

    state_dir = Path(_cfg_or_env_str(cfg, "mavely_novnc_state_dir", "MAVELY_NOVNC_STATE_DIR", "/tmp/rsforwarder_mavely_novnc"))
    state_dir.mkdir(parents=True, exist_ok=True)
    pid_file = state_dir / "mavely_cookie_refresher.pid"
    log_file = state_dir / "mavely_cookie_refresher.log"

    existing = _read_pid(pid_file)
    if existing and _pid_alive(existing):
        return int(existing), str(log_file), None

    env = dict(os.environ)
    env["DISPLAY"] = str(display)

    cmd = [
        sys.executable,
        str(script),
        "--interactive",
        "--wait-login",
        str(max(30, int(wait_login_s))),
    ]

    try:
        with open(log_file, "a", encoding="utf-8", errors="replace") as lf:
            p = subprocess.Popen(
                cmd,
                cwd=str(repo_root / "RSForwarder"),
                env=env,
                stdout=lf,
                stderr=lf,
                stdin=subprocess.DEVNULL,
                close_fds=True,
            )
        _write_pid(pid_file, int(p.pid))
        return int(p.pid), str(log_file), None
    except Exception as e:
        return None, str(log_file), f"Failed to start cookie refresher: {e}"

