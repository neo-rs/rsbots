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


def _best_python(repo_root: Path) -> str:
    """
    Prefer the repo's venv python when present.

    On servers, the bot may run under system python, but Playwright and other deps
    are typically installed in the repo venv. Using sys.executable can therefore
    break noVNC login flows.
    """
    try:
        venv_py = repo_root / ".venv" / "bin" / "python"
        if venv_py.exists() and venv_py.is_file():
            return str(venv_py)
    except Exception:
        pass
    return str(sys.executable)


def _append_log(p: Path, text: str) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8", errors="replace") as f:
            f.write((text or "").rstrip() + "\n")
    except Exception:
        pass


def _tail_log(p: Path, max_lines: int = 60) -> str:
    try:
        lines = (p.read_text(encoding="utf-8", errors="replace") or "").splitlines()
        tail = lines[-max_lines:] if max_lines and len(lines) > max_lines else lines
        return "\n".join(tail).strip()
    except Exception:
        return ""


def _ensure_playwright_chromium(python_exe: str, *, cwd: Path, state_dir: Path) -> None:
    """
    Best-effort: ensure Playwright Chromium is installed.

    This prevents the common "no browser shows up in noVNC" case where Playwright is installed
    but the Chromium runtime wasn't downloaded on the server.
    """
    marker = state_dir / "playwright_chromium.ok"
    log = state_dir / "playwright_install.log"
    if marker.exists():
        return

    try:
        _append_log(log, f"[playwright] ensuring chromium via: {python_exe} -m playwright install chromium")
        r = subprocess.run(
            [python_exe, "-m", "playwright", "install", "chromium"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=600,
            encoding="utf-8",
            errors="replace",
        )
        out = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")
        if out.strip():
            _append_log(log, out.strip())
        if int(r.returncode or 0) == 0:
            try:
                marker.write_text(f"ok {int(time.time())}\n", encoding="utf-8")
            except Exception:
                pass
        else:
            _append_log(
                log,
                "[playwright] install chromium failed. If errors mention missing system libs, run:\n"
                f"  sudo {python_exe} -m playwright install-deps chromium",
            )
    except Exception as e:
        _append_log(log, f"[playwright] install chromium failed: {e}")


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
    # On Linux, a child can become a zombie (defunct). os.kill(pid, 0) still
    # succeeds, but the process is not actually running anymore.
    if os.name != "nt":
        try:
            stat = Path(f"/proc/{int(pid)}/stat").read_text(encoding="utf-8", errors="replace")
            # Format: pid (comm) state ...
            # comm can contain spaces, so split after ") ".
            after = stat.split(") ", 1)[1]
            state = (after.split(" ", 1)[0] or "").strip()
            if state.upper() == "Z":
                return False
        except Exception:
            pass
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _proc_cmdline(pid: int) -> list:
    if pid <= 0:
        return []
    try:
        raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
        parts = [p.decode("utf-8", errors="replace") for p in raw.split(b"\0") if p]
        return [p for p in parts if p]
    except Exception:
        return []


def _detect_running_display(state_dir: Path, fallback_display: str) -> str:
    """
    Best-effort: when the noVNC stack is already running, detect the *actual*
    X display it uses from PID files /proc instead of trusting config.

    This avoids a common mismatch where the stack is running on :1 but config
    defaults to :99, causing Playwright to fail with "Missing X server or $DISPLAY".
    """
    try:
        # Prefer x11vnc, since it directly tells us what display is being exported.
        pid_vnc = _read_pid(state_dir / "x11vnc.pid")
        if pid_vnc and _pid_alive(pid_vnc):
            cmd = _proc_cmdline(pid_vnc)
            if cmd:
                for i, tok in enumerate(cmd):
                    if tok == "-display" and i + 1 < len(cmd):
                        disp = (cmd[i + 1] or "").strip()
                        if disp:
                            return disp

        pid_xvfb = _read_pid(state_dir / "xvfb.pid")
        if pid_xvfb and _pid_alive(pid_xvfb):
            cmd = _proc_cmdline(pid_xvfb)
            # Our Xvfb command is: Xvfb <display> -screen ...
            if len(cmd) >= 2:
                disp = (cmd[1] or "").strip()
                if disp:
                    return disp
    except Exception:
        pass
    return (fallback_display or "").strip() or ":99"


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
        running_display = _detect_running_display(state_dir, display)
        return {
            "display": running_display,
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

    python_exe = _best_python(repo_root)
    _ensure_playwright_chromium(python_exe, cwd=(repo_root / "RSForwarder"), state_dir=state_dir)

    env = dict(os.environ)
    env["DISPLAY"] = str(display)

    # Optional auto-login credentials (server-only secrets)
    try:
        email = str((cfg or {}).get("mavely_login_email") or "").strip()
        password = str((cfg or {}).get("mavely_login_password") or "").strip()
        if email:
            env["MAVELY_LOGIN_EMAIL"] = email
        if password:
            env["MAVELY_LOGIN_PASSWORD"] = password
    except Exception:
        pass

    cmd = [
        python_exe,
        str(script),
        "--interactive",
        "--wait-login",
        str(max(30, int(wait_login_s))),
    ]
    if env.get("MAVELY_LOGIN_EMAIL") and env.get("MAVELY_LOGIN_PASSWORD"):
        cmd.append("--auto-login")

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
        # If the refresher exits immediately, surface the error to the command caller.
        time.sleep(0.8)
        if not _pid_alive(int(p.pid)):
            tail = _tail_log(log_file, max_lines=60)
            extra = _tail_log(state_dir / "playwright_install.log", max_lines=60)
            msg = "Cookie refresher exited immediately (Chromium likely failed to launch)."
            if tail:
                msg += "\n\nLast refresher log lines:\n" + tail[-1800:]
            if extra:
                msg += "\n\nLast playwright install lines:\n" + extra[-1800:]
            return None, str(log_file), msg
        return int(p.pid), str(log_file), None
    except Exception as e:
        return None, str(log_file), f"Failed to start cookie refresher: {e}"


def run_cookie_refresher_headless(cfg: dict, *, wait_login_s: int = 180) -> Tuple[bool, str]:
    """
    Run mavely_cookie_refresher headless with optional --auto-login.

    Returns: (ok, output_or_error)
    """
    if os.name == "nt":
        return False, "Cookie refresher is intended to run on the Linux host."

    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "RSForwarder" / "mavely_cookie_refresher.py"
    if not script.exists():
        return False, f"Missing script: {script}"

    env = dict(os.environ)
    try:
        email = str((cfg or {}).get("mavely_login_email") or "").strip()
        password = str((cfg or {}).get("mavely_login_password") or "").strip()
        if email:
            env["MAVELY_LOGIN_EMAIL"] = email
        if password:
            env["MAVELY_LOGIN_PASSWORD"] = password
    except Exception:
        email = ""
        password = ""

    # Headless auto-login works best with a dedicated clean profile directory,
    # so a stale interactive profile cannot redirect us away from the login form.
    headless_profile = _cfg_or_env_str(
        cfg,
        "mavely_headless_profile_dir",
        "MAVELY_HEADLESS_PROFILE_DIR",
        "/tmp/rsforwarder_mavely_profile_headless",
    )
    env["MAVELY_PROFILE_DIR"] = headless_profile

    python_exe = _best_python(repo_root)
    state_dir = Path(_cfg_or_env_str(cfg, "mavely_novnc_state_dir", "MAVELY_NOVNC_STATE_DIR", "/tmp/rsforwarder_mavely_novnc"))
    _ensure_playwright_chromium(python_exe, cwd=(repo_root / "RSForwarder"), state_dir=state_dir)

    cmd = [python_exe, str(script), "--wait-login", str(max(30, int(wait_login_s)))]
    auto_login = bool(env.get("MAVELY_LOGIN_EMAIL") and env.get("MAVELY_LOGIN_PASSWORD"))
    if auto_login:
        cmd.append("--auto-login")

    try:
        def _run() -> Tuple[int, str]:
            r = subprocess.run(
                cmd,
                cwd=str(repo_root / "RSForwarder"),
                env=env,
                capture_output=True,
                text=True,
                timeout=max(60, int(wait_login_s) + 30),
                encoding="utf-8",
                errors="replace",
            )
            out = (r.stdout or r.stderr or "").strip()
            return int(r.returncode or 0), out

        code, out = _run()
        if code == 0:
            return True, out

        # One retry with a fresh profile when auto-login is enabled.
        if auto_login:
            try:
                shutil.rmtree(headless_profile, ignore_errors=True)
            except Exception:
                pass
            code2, out2 = _run()
            if code2 == 0:
                return True, out2
            out = (out2 or out or "").strip()

        return False, out or f"cookie refresher exit={code}"
    except subprocess.TimeoutExpired:
        return False, "cookie refresher timed out"
    except Exception as e:
        return False, f"cookie refresher failed: {e}"

