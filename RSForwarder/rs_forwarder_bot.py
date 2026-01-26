#!/usr/bin/env python3
"""
RS Forwarder Bot
----------------
Standalone bot for forwarding messages from RS Server channels to webhooks.
All messages are branded with "Reselling Secrets" name and avatar from RS Server.
"""

import os
import sys
import json
import asyncio
import discord
from discord.ext import commands
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import platform
import subprocess
import shlex
import time

from RSForwarder import affiliate_rewriter
from RSForwarder import novnc_stack

# Ensure repo root is importable when executed as a script (matches Ubuntu run_bot.sh PYTHONPATH).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mirror_world_config import load_config_with_secrets
from mirror_world_config import is_placeholder_secret, mask_secret

# Colors for terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class RSForwarderBot:
    """Main bot class for forwarding messages with Reselling Secrets branding"""
    
    def __init__(self):
        self.config_path = Path(__file__).parent / "config.json"
        self.config: Dict[str, Any] = {}
        self.rs_guild: Optional[discord.Guild] = None
        self.rs_icon_url: Optional[str] = None
        self.stats = {
            'messages_forwarded': 0,
            'errors': 0,
            'started_at': None
        }
        # Mavely monitoring / alerting state (in-memory)
        self._mavely_monitor_task: Optional[asyncio.Task] = None
        self._mavely_last_preflight_ok: Optional[bool] = None
        self._mavely_last_preflight_status: Optional[int] = None
        self._mavely_last_preflight_err: Optional[str] = None
        self._mavely_last_alert_ts: float = 0.0
        self._mavely_last_autologin_ts: float = 0.0
        self._mavely_last_autologin_ok: Optional[bool] = None
        self._mavely_last_autologin_msg: Optional[str] = None
        self._mavely_last_refresher_pid: Optional[int] = None
        self._mavely_last_refresher_log_path: Optional[str] = None
        self.load_config()
        
        # Validate required config
        if not self.config.get("bot_token"):
            print(f"{Colors.RED}[Config] ERROR: 'bot_token' is required in config.secrets.json (server-only){Colors.RESET}")
            sys.exit(1)
        
        # Setup bot
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        
        # Use unique prefix to avoid conflicts with other bots
        self.bot = commands.Bot(command_prefix='!rs', intents=intents)
        self._setup_events()
        self._setup_commands()

    def _mavely_monitor_interval_s(self) -> int:
        try:
            v = int((self.config or {}).get("mavely_monitor_interval_s") or 300)
        except Exception:
            v = 300
        return max(60, min(v, 3600))

    def _mavely_alert_cooldown_s(self) -> int:
        try:
            v = int((self.config or {}).get("mavely_alert_cooldown_s") or 1800)
        except Exception:
            v = 1800
        return max(300, min(v, 24 * 3600))

    def _mavely_autologin_enabled(self) -> bool:
        try:
            v = (self.config or {}).get("mavely_autologin_on_fail")
            if v is None:
                v = os.getenv("MAVELY_AUTOLOGIN_ON_FAIL", "")
            if isinstance(v, str):
                return v.strip().lower() in {"1", "true", "yes", "y", "on"}
            return bool(v) if v is not None else True
        except Exception:
            return True

    def _mavely_autologin_cooldown_s(self) -> int:
        try:
            v = int((self.config or {}).get("mavely_autologin_cooldown_s") or 300)
        except Exception:
            v = 300
        return max(30, min(v, 6 * 3600))

    def _mavely_state_dir(self) -> Path:
        """
        Local durable state/log directory for Mavely automation.
        - On Oracle/Linux: defaults to the same dir used by noVNC stack.
        - Else: keep it local under RSForwarder/.tmp (git-ignored).
        """
        try:
            if self._is_local_exec():
                raw = (
                    str((self.config or {}).get("mavely_novnc_state_dir") or "").strip()
                    or (os.getenv("MAVELY_NOVNC_STATE_DIR", "") or "").strip()
                    or "/tmp/rsforwarder_mavely_novnc"
                )
                p = Path(raw)
            else:
                p = Path(__file__).parent / ".tmp" / "mavely_monitor"
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            return Path(__file__).parent

    def _mavely_status_path(self) -> Path:
        return self._mavely_state_dir() / "mavely_status.json"

    def _mavely_monitor_log_path(self) -> Path:
        return self._mavely_state_dir() / "mavely_monitor.log"

    def _mavely_append_log(self, msg: str) -> None:
        try:
            ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            line = f"[{ts}] {msg}".rstrip()
            print(f"{Colors.CYAN}[Mavely]{Colors.RESET} {line}")
            try:
                p = self._mavely_monitor_log_path()
                with open(p, "a", encoding="utf-8", errors="replace") as f:
                    f.write(line + "\n")
            except Exception:
                pass
        except Exception:
            pass

    def _mavely_write_status(self, extra: Optional[Dict[str, Any]] = None) -> None:
        try:
            def _short(s: Optional[str], n: int) -> str:
                t = (s or "").replace("\n", " ").strip()
                return t if len(t) <= n else (t[:n] + "...")

            data: Dict[str, Any] = {
                "ts_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "preflight_ok": bool(self._mavely_last_preflight_ok),
                "preflight_status": self._mavely_last_preflight_status,
                "preflight_err": _short(self._mavely_last_preflight_err, 500),
                "last_alert_ts": float(self._mavely_last_alert_ts or 0.0),
                "last_autologin_ts": float(self._mavely_last_autologin_ts or 0.0),
                "last_autologin_ok": self._mavely_last_autologin_ok,
                "last_autologin_msg": _short(self._mavely_last_autologin_msg, 1200),
                "last_refresher_pid": self._mavely_last_refresher_pid,
                "last_refresher_log_path": self._mavely_last_refresher_log_path,
            }
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in data:
                        data[k] = v
            tmp = self._mavely_status_path().with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8", errors="replace")
            os.replace(str(tmp), str(self._mavely_status_path()))
        except Exception:
            pass

    def _mavely_alert_user_ids(self) -> List[int]:
        """
        Read alert targets from config.secrets.json (server-only).
        """
        try:
            d = self._load_secrets_dict()
            raw = d.get("mavely_alert_user_ids") or []
            if isinstance(raw, int):
                raw = [raw]
            if not isinstance(raw, list):
                return []
            out: List[int] = []
            for x in raw:
                try:
                    out.append(int(str(x).strip()))
                except Exception:
                    continue
            # unique, stable order
            seen = set()
            uniq: List[int] = []
            for i in out:
                if i in seen:
                    continue
                seen.add(i)
                uniq.append(i)
            return uniq
        except Exception:
            return []

    def _mavely_admin_user_ids(self) -> List[int]:
        """
        Allowed to run Mavely login commands in DMs (server-only).
        """
        try:
            d = self._load_secrets_dict()
            raw = d.get("mavely_admin_user_ids") or []
            if isinstance(raw, int):
                raw = [raw]
            if not isinstance(raw, list):
                return []
            out: List[int] = []
            for x in raw:
                try:
                    out.append(int(str(x).strip()))
                except Exception:
                    continue
            seen = set()
            uniq: List[int] = []
            for i in out:
                if i in seen:
                    continue
                seen.add(i)
                uniq.append(i)
            return uniq
        except Exception:
            return []

    def _save_mavely_user_lists(self, *, alert_ids: List[int], admin_ids: List[int]) -> bool:
        try:
            d = self._load_secrets_dict()
            d["mavely_alert_user_ids"] = [int(x) for x in (alert_ids or [])]
            d["mavely_admin_user_ids"] = [int(x) for x in (admin_ids or [])]
            return self._save_secrets_dict(d)
        except Exception:
            return False

    def _ensure_mavely_user(self, user_id: int) -> bool:
        """
        Ensure user is both an alert target and DM-admin for Mavely login commands.
        """
        try:
            uid = int(user_id)
        except Exception:
            return False
        alert_ids = self._mavely_alert_user_ids()
        admin_ids = self._mavely_admin_user_ids()
        if uid not in alert_ids:
            alert_ids.append(uid)
        if uid not in admin_ids:
            admin_ids.append(uid)
        return self._save_mavely_user_lists(alert_ids=alert_ids, admin_ids=admin_ids)

    def _remove_mavely_user(self, user_id: int) -> bool:
        try:
            uid = int(user_id)
        except Exception:
            return False
        alert_ids = [i for i in self._mavely_alert_user_ids() if i != uid]
        admin_ids = [i for i in self._mavely_admin_user_ids() if i != uid]
        return self._save_mavely_user_lists(alert_ids=alert_ids, admin_ids=admin_ids)

    def _is_mavely_admin_ctx(self, ctx) -> bool:
        """
        Guild: require administrator permission.
        DM: require user id to be in mavely_admin_user_ids or mavely_alert_user_ids.
        """
        try:
            if getattr(ctx, "guild", None) is not None:
                return bool(getattr(ctx.author, "guild_permissions", None) and ctx.author.guild_permissions.administrator)
            uid = int(getattr(ctx.author, "id", 0) or 0)
            if not uid:
                return False
            return (uid in self._mavely_admin_user_ids()) or (uid in self._mavely_alert_user_ids())
        except Exception:
            return False

    async def _dm_user(self, user_id: int, content: str) -> bool:
        try:
            uid = int(user_id)
            if uid <= 0:
                return False
            user = self.bot.get_user(uid)
            if user is None:
                user = await self.bot.fetch_user(uid)
            if user is None:
                return False
            await user.send(content)
            return True
        except Exception:
            return False

    def _build_tunnel_instructions(self, web_port: int, url_path: str) -> str:
        host_hint = "<oracle-host>"
        user_hint = "rsadmin"
        try:
            oraclekeys_path = _REPO_ROOT / "oraclekeys"
            servers_json = oraclekeys_path / "servers.json"
            if servers_json.exists():
                servers = json.loads(servers_json.read_text(encoding="utf-8", errors="replace") or "[]")
                if isinstance(servers, list) and servers:
                    host_hint = str((servers[0] or {}).get("host") or host_hint)
                    user_hint = str((servers[0] or {}).get("user") or user_hint)
        except Exception:
            pass
        tunnel_cmd = f"ssh -i <YOUR_KEY> -L {int(web_port)}:127.0.0.1:{int(web_port)} {user_hint}@{host_hint}"
        return (
            "✅ noVNC is running (localhost-only on the server).\n\n"
            f"- If you already keep a tunnel running, just open:\n`http://localhost:{int(web_port)}{url_path}`\n\n"
            "Otherwise:\n"
            f"1) On your PC, open an SSH tunnel:\n```{tunnel_cmd}```\n"
            f"2) In your PC browser, open:\n`http://localhost:{int(web_port)}{url_path}`\n"
            "3) Log into Mavely in the Chromium window on that desktop.\n\n"
            "When you're done, run `!rsmavelycheck` to confirm the session is valid."
        )

    async def _mavely_monitor_loop(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            interval = self._mavely_monitor_interval_s()
            try:
                targets = self._mavely_alert_user_ids()
                prev_ok = self._mavely_last_preflight_ok
                ok, status, err = await affiliate_rewriter.mavely_preflight(self.config)
                self._mavely_last_preflight_ok = bool(ok)
                self._mavely_last_preflight_status = int(status or 0)
                self._mavely_last_preflight_err = (err or "").strip() if not ok else ""
                self._mavely_write_status()
                if ok:
                    if prev_ok is not True:
                        self._mavely_append_log(f"preflight OK (status={status})")
                    await asyncio.sleep(interval)
                    continue

                # Log the failure (never log tokens/cookies; err is already "safe"/short).
                try:
                    msg0 = (err or "unknown error").replace("\n", " ").strip()
                    if len(msg0) > 240:
                        msg0 = msg0[:240] + "..."
                    self._mavely_append_log(f"preflight FAIL (status={status}) {msg0}")
                except Exception:
                    pass

                # If credentials are configured, try a headless auto-login first.
                # If it succeeds, we recover without requiring user action.
                try:
                    email = str((self.config or {}).get("mavely_login_email") or "").strip()
                    password = str((self.config or {}).get("mavely_login_password") or "").strip()
                except Exception:
                    email, password = "", ""

                now = time.time()
                autologin_attempted = False
                if self._is_local_exec() and self._mavely_autologin_enabled() and email and password:
                    cooldown2 = self._mavely_autologin_cooldown_s()
                    if (now - float(self._mavely_last_autologin_ts or 0.0)) >= float(cooldown2):
                        autologin_attempted = True
                        self._mavely_last_autologin_ts = now
                        self._mavely_append_log("preflight FAIL -> attempting headless auto-login (cookie refresher)")
                        ok_run, out = await asyncio.to_thread(novnc_stack.run_cookie_refresher_headless, self.config, wait_login_s=180)
                        out_s = (out or "").strip()
                        if len(out_s) > 4000:
                            out_s = out_s[:4000] + "\n... (truncated)"
                        self._mavely_last_autologin_ok = bool(ok_run)
                        self._mavely_last_autologin_msg = out_s or ("ok" if ok_run else "failed")
                        self._mavely_write_status()
                        if ok_run:
                            ok2, status2, err2 = await affiliate_rewriter.mavely_preflight(self.config)
                            self._mavely_last_preflight_status = int(status2 or 0)
                            self._mavely_last_preflight_err = (err2 or "").strip() if not ok2 else ""
                            self._mavely_write_status()
                            if ok2:
                                self._mavely_append_log(f"auto-login recovered session (preflight OK status={status2})")
                                self._mavely_last_preflight_ok = True
                                await asyncio.sleep(interval)
                                continue

                # Failure path: alert (rate-limited) and optionally start noVNC + refresher automatically.
                cooldown = self._mavely_alert_cooldown_s()
                should_alert = (self._mavely_last_preflight_ok is True) or ((now - float(self._mavely_last_alert_ts)) >= float(cooldown))
                self._mavely_last_preflight_ok = False
                self._mavely_write_status()
                if not should_alert:
                    await asyncio.sleep(interval)
                    continue

                info = None
                pid = None
                log_path = None
                start_err = None
                # Only start noVNC if someone can be alerted (otherwise there's no human to complete login).
                if self._is_local_exec() and targets:
                    info, start_err = await asyncio.to_thread(novnc_stack.ensure_novnc, self.config)
                    if info and not start_err:
                        pid, log_path, _err2 = await asyncio.to_thread(
                            novnc_stack.start_cookie_refresher,
                            self.config,
                            display=str((info or {}).get("display") or ":99"),
                            wait_login_s=900,
                        )
                        self._mavely_last_refresher_pid = int(pid) if pid else None
                        self._mavely_last_refresher_log_path = str(log_path) if log_path else None
                        self._mavely_write_status(
                            {
                                "novnc_display": str((info or {}).get("display") or ""),
                                "novnc_web_port": int((info or {}).get("web_port") or 0),
                                "novnc_url_path": str((info or {}).get("url_path") or ""),
                            }
                        )

                msg = (err or "unknown error").replace("\n", " ").strip()
                if len(msg) > 240:
                    msg = msg[:240] + "..."
                header = f"⚠️ Mavely session check FAILED (status={status}).\n{msg}\n\n"
                if autologin_attempted:
                    if self._mavely_last_autologin_ok:
                        header = "⚠️ Mavely session check FAILED, but auto-login ran.\n(Preflight still failing; manual login may be required.)\n\n" + header
                    else:
                        header = "⚠️ Mavely session check FAILED and headless auto-login did NOT recover.\nManual login required.\n\n" + header

                if start_err:
                    body = f"noVNC auto-start failed: {str(start_err)[:300]}\n\nRun `!rsmavelylogin` in Discord to start login."
                else:
                    web_port = int((info or {}).get("web_port") or 6080)
                    url_path = str((info or {}).get("url_path") or "/vnc.html")
                    body = self._build_tunnel_instructions(web_port, url_path)
                    if pid:
                        body += f"\n\nCookie refresher PID: `{pid}`"
                    if log_path:
                        body += f"\nRefresher log (server path): `{log_path}`"

                # DM only if targets exist; automation still runs regardless.
                if targets:
                    for uid in targets:
                        await self._dm_user(uid, header + body)
                    self._mavely_last_alert_ts = now
                    self._mavely_write_status()
                else:
                    # No DM recipients configured; keep a clear log trail.
                    self._mavely_append_log("preflight still failing and no alert recipients are configured (run !rsmavelyalertme)")
            except Exception:
                pass

            await asyncio.sleep(interval)
    
    def load_config(self):
        """Load configuration from config.json + config.secrets.json (server-only)."""
        if self.config_path.exists():
            try:
                self.config, _, secrets_path = load_config_with_secrets(Path(__file__).parent)
                if not secrets_path.exists():
                    print(f"{Colors.YELLOW}[Config] Missing config.secrets.json (server-only): {secrets_path}{Colors.RESET}")
                print(f"{Colors.GREEN}[Config] Loaded configuration{Colors.RESET}")
                
                # Load saved icon URL from config
                saved_icon_url = self.config.get("rs_server_icon_url", "")
                if saved_icon_url:
                    self.rs_icon_url = saved_icon_url
                    print(f"{Colors.GREEN}[Config] Loaded RS Server icon from config: {saved_icon_url[:50]}...{Colors.RESET}")

                # Apply destination webhook URLs from secrets mapping (server-only)
                webhooks = self.config.get("destination_webhooks", {}) or {}
                if not isinstance(webhooks, dict):
                    webhooks = {}
                missing_hooks = []
                for ch in self.config.get("channels", []) or []:
                    src_id = str(ch.get("source_channel_id", "")).strip()
                    if not src_id:
                        continue
                    hook = webhooks.get(src_id)
                    if not hook:
                        missing_hooks.append(src_id)
                        continue
                    ch["destination_webhook_url"] = hook
                if missing_hooks:
                    print(f"{Colors.RED}[Config] ERROR: Missing destination webhook(s) for source_channel_id(s): {', '.join(missing_hooks[:5])}{Colors.RESET}")
                    print(f"{Colors.RED}[Config] Add them to config.secrets.json under destination_webhooks{{...}}{Colors.RESET}")
                    sys.exit(1)
            except Exception as e:
                print(f"{Colors.RED}[Config] Failed to load config: {e}{Colors.RESET}")
                self.config = {}
        else:
            # Create empty config structure
            self.config = {
                "guild_id": 0,
                "brand_name": "Reselling Secrets",
                "forwarding_logs_channel_id": "",
                "rs_server_icon_url": "",
                "channels": []
            }
            self.save_config()
            print(f"{Colors.YELLOW}[Config] Created empty config.json - please configure it{Colors.RESET}")
            print(f"{Colors.YELLOW}[Config] Required fields: guild_id, brand_name (secrets live in config.secrets.json){Colors.RESET}")

    def _is_local_exec(self) -> bool:
        """Return True when RSForwarder runs on the Ubuntu host and can manage services locally."""
        try:
            if os.name == "nt":
                return False
            return _REPO_ROOT.is_dir()
        except Exception:
            return False

    def _run_local_botctl(self, action: str) -> Tuple[bool, str]:
        """Run botctl.sh locally on Ubuntu to manage RSAdminBot."""
        botctl = _REPO_ROOT / "RSAdminBot" / "botctl.sh"
        if not botctl.exists():
            return False, f"botctl.sh not found at: {botctl}"
        cmd = f"bash {shlex.quote(str(botctl))} {shlex.quote(action)} rsadminbot"
        try:
            r = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=60, encoding="utf-8", errors="replace")
            out = (r.stdout or r.stderr or "").strip()
            return r.returncode == 0, out
        except Exception as e:
            return False, str(e)
    
    def save_config(self):
        """Save configuration to JSON file"""
        try:
            # Never write secrets back into config.json
            config_to_save = dict(self.config or {})
            config_to_save.pop("bot_token", None)
            config_to_save.pop("destination_webhooks", None)
            # Strip any other secrets that were merged in from config.secrets.json.
            # This prevents accidental leakage of tokens/cookies into the synced config.json.
            try:
                secrets_path = self._secrets_path()
                if secrets_path.exists():
                    secrets_obj = json.loads(secrets_path.read_text(encoding="utf-8", errors="replace") or "{}")
                    if isinstance(secrets_obj, dict):
                        for k in list(secrets_obj.keys()):
                            config_to_save.pop(k, None)
            except Exception:
                pass
            # Also strip webhook URLs from channel configs (these live in config.secrets.json)
            try:
                channels = config_to_save.get("channels")
                if isinstance(channels, list):
                    for ch in channels:
                        if isinstance(ch, dict):
                            ch.pop("destination_webhook_url", None)
            except Exception:
                pass
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_to_save, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to save config: {e}{Colors.RESET}")

    def _secrets_path(self) -> Path:
        """Return the server-only secrets file path for this bot."""
        return Path(__file__).parent / "config.secrets.json"

    def _load_secrets_dict(self) -> Dict[str, Any]:
        p = self._secrets_path()
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
        except Exception:
            return {}

    def _save_secrets_dict(self, d: Dict[str, Any]) -> bool:
        p = self._secrets_path()
        try:
            p.write_text(json.dumps(d, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            return True
        except Exception as e:
            print(f"{Colors.RED}[Config] Failed to save secrets: {e}{Colors.RESET}")
            return False

    def _set_destination_webhook_secret(self, source_channel_id: str, webhook_url: str) -> bool:
        """Persist destination webhook mapping into config.secrets.json (server-only)."""
        src = str(source_channel_id or "").strip()
        url = str(webhook_url or "").strip()
        if not src or not url:
            return False
        secrets = self._load_secrets_dict()
        wh = secrets.get("destination_webhooks")
        if not isinstance(wh, dict):
            wh = {}
        wh[src] = url
        secrets["destination_webhooks"] = wh
        return self._save_secrets_dict(secrets)

    def _delete_destination_webhook_secret(self, source_channel_id: str) -> bool:
        src = str(source_channel_id or "").strip()
        if not src:
            return False
        secrets = self._load_secrets_dict()
        wh = secrets.get("destination_webhooks")
        if not isinstance(wh, dict):
            return True
        if src in wh:
            wh.pop(src, None)
            secrets["destination_webhooks"] = wh
            return self._save_secrets_dict(secrets)
        return True
    
    def get_channel_config(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a source channel"""
        for channel in self.config.get("channels", []):
            if str(channel.get("source_channel_id")) == str(channel_id):
                return channel
        return None
    
    def _get_guild_icon_url(self, guild: discord.Guild) -> Optional[str]:
        """Get guild icon URL"""
        if not guild or not guild.icon:
            return None
        
        try:
            # Use the Asset's url property directly - it returns the full URL
            icon_url = str(guild.icon.url)
            # Ensure size=256 parameter
            if "?" in icon_url:
                # URL has query params - replace or add size parameter
                import re
                # Remove existing size parameter if present
                icon_url = re.sub(r'[?&]size=\d+', '', icon_url)
                # Add size=256
                separator = "&" if "?" in icon_url else "?"
                return f"{icon_url}{separator}size=256"
            else:
                # No query params, just add size=256
                return f"{icon_url}?size=256"
        except (AttributeError, Exception) as e:
            # Fallback: construct URL manually from icon hash
            try:
                # Get the icon hash/key from the Asset
                icon_hash = str(guild.icon.key) if hasattr(guild.icon, 'key') else None
                if not icon_hash:
                    # Last resort: try to extract from string representation
                    icon_str = str(guild.icon)
                    # If it's already a URL, extract the hash
                    if "/icons/" in icon_str:
                        # Extract hash from URL pattern: .../icons/GUILD_ID/HASH.ext
                        parts = icon_str.split("/")
                        if len(parts) >= 2:
                            hash_part = parts[-1].split(".")[0].split("?")[0]
                            icon_hash = hash_part
                    else:
                        icon_hash = icon_str
                
                if icon_hash and not icon_hash.startswith("http"):
                    # Construct URL from hash
                    ext = "gif" if icon_hash.startswith("a_") else "png"
                    return f"https://cdn.discordapp.com/icons/{guild.id}/{icon_hash}.{ext}?size=256"
                else:
                    print(f"{Colors.YELLOW}[Icon] Could not extract icon hash: {e}{Colors.RESET}")
                    return None
            except Exception as e2:
                print(f"{Colors.RED}[Icon] Error getting icon URL: {e2}{Colors.RESET}")
                return None
    
    async def _fetch_guild_icon_via_api(self, guild_id: int, save_to_config: bool = True):
        """Fetch guild icon via Discord API as fallback"""
        try:
            import requests
            bot_token = self.config.get("bot_token", "").strip()
            if not bot_token:
                print(f"{Colors.RED}[Icon] No bot token available for API fetch{Colors.RESET}")
                return False
            
            headers = {
                'Authorization': f'Bot {bot_token}',
                'User-Agent': 'DiscordBot (RSForwarder)'
            }
            
            print(f"{Colors.CYAN}[Icon] Fetching RS Server icon via API for guild {guild_id}...{Colors.RESET}")
            response = requests.get(
                f'https://discord.com/api/v10/guilds/{guild_id}?with_counts=false',
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                guild_data = response.json()
                icon_hash = guild_data.get("icon")
                if icon_hash:
                    ext = "gif" if str(icon_hash).startswith("a_") else "png"
                    self.rs_icon_url = f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}?size=256"
                    print(f"{Colors.GREEN}[Icon] ✅ RS Server icon fetched: {self.rs_icon_url}{Colors.RESET}")
                    
                    # Save to config if requested
                    if save_to_config:
                        self.config["rs_server_icon_url"] = self.rs_icon_url
                        self.save_config()
                        print(f"{Colors.GREEN}[Icon] ✅ Icon saved to config.json{Colors.RESET}")
                    
                    return True
                else:
                    print(f"{Colors.YELLOW}[Icon] ⚠️ RS Server has no icon (fetched via API){Colors.RESET}")
                    return False
            elif response.status_code == 404:
                print(f"{Colors.RED}[Icon] ❌ RS Server not found (404) - bot may not be in server{Colors.RESET}")
                return False
            elif response.status_code == 403:
                print(f"{Colors.RED}[Icon] ❌ No permission to fetch RS Server info (403){Colors.RESET}")
                return False
            else:
                print(f"{Colors.YELLOW}[Icon] ⚠️ Failed to fetch RS Server info: {response.status_code} - {response.text[:100]}{Colors.RESET}")
                return False
        except Exception as e:
            print(f"{Colors.RED}[Icon] ❌ Error fetching guild icon via API: {e}{Colors.RESET}")
            import traceback
            if "--verbose" in sys.argv:
                traceback.print_exc()
            return False
    
    def _setup_events(self):
        """Setup Discord event handlers"""
        
        @self.bot.event
        async def on_ready():
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  📤 RS Forwarder Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            
            # Get RS Server guild - try multiple times as guilds may not be cached immediately
            guild_id = self.config.get("guild_id", 0)
            if guild_id:
                # Try to get guild, wait a bit if not immediately available
                for attempt in range(3):
                    self.rs_guild = self.bot.get_guild(guild_id)
                    if self.rs_guild:
                        break
                    await asyncio.sleep(1)
                
                if self.rs_guild:
                    print(f"{Colors.GREEN}[Bot] Connected to: {self.rs_guild.name}{Colors.RESET}")
                
                # If icon not loaded from config, try to fetch it
                if not self.rs_icon_url:
                    if self.rs_guild:
                        self.rs_icon_url = self._get_guild_icon_url(self.rs_guild)
                        if self.rs_icon_url:
                            print(f"{Colors.GREEN}[Icon] RS Server icon from guild: {self.rs_icon_url[:50]}...{Colors.RESET}")
                            # Save to config
                            self.config["rs_server_icon_url"] = self.rs_icon_url
                            self.save_config()
                        else:
                            print(f"{Colors.YELLOW}[Icon] RS Server has no icon from guild object, trying API...{Colors.RESET}")
                            # Try API as fallback even if guild is found (icon might not be in cache)
                            await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
                    else:
                        print(f"{Colors.YELLOW}[Icon] RS Server (ID: {guild_id}) not found in cache - trying API...{Colors.RESET}")
                        # Try to fetch icon via API as fallback
                        icon_fetched = await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
                        if not icon_fetched:
                            print(f"{Colors.RED}[Icon] ⚠️ Could not fetch RS Server icon. Check that:{Colors.RESET}")
                            print(f"{Colors.RED}[Icon]   1. Bot is in RS Server (ID: {guild_id}){Colors.RESET}")
                            print(f"{Colors.RED}[Icon]   2. Bot has permission to view server info{Colors.RESET}")
                            print(f"{Colors.RED}[Icon]   3. RS Server has an icon set{Colors.RESET}")
                else:
                    print(f"{Colors.GREEN}[Icon] Using saved RS Server icon from config{Colors.RESET}")
            
            # Display config information
            print(f"\n{Colors.CYAN}[Config] Configuration Information:{Colors.RESET}")
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            
            if self.rs_guild:
                print(f"{Colors.GREEN}🏠 Guild:{Colors.RESET} {Colors.BOLD}{self.rs_guild.name}{Colors.RESET} (ID: {guild_id})")
            elif guild_id:
                print(f"{Colors.YELLOW}⚠️  Guild:{Colors.RESET} Not found (ID: {guild_id})")
            else:
                print(f"{Colors.YELLOW}⚠️  Guild:{Colors.RESET} Not configured")
            
            brand_name = self.config.get("brand_name", "Reselling Secrets")
            print(f"{Colors.GREEN}🏷️  Brand Name:{Colors.RESET} {Colors.BOLD}{brand_name}{Colors.RESET}")
            
            if self.rs_icon_url:
                print(f"{Colors.GREEN}🖼️  RS Server Icon:{Colors.RESET} {Colors.BOLD}Loaded{Colors.RESET}")
            else:
                print(f"{Colors.YELLOW}⚠️  RS Server Icon:{Colors.RESET} Not available")
            
            forwarding_logs_channel_id = self.config.get("forwarding_logs_channel_id")
            if forwarding_logs_channel_id and self.rs_guild:
                log_channel = self.rs_guild.get_channel(forwarding_logs_channel_id)
                if log_channel:
                    print(f"{Colors.GREEN}📝 Forwarding Logs Channel:{Colors.RESET} {Colors.BOLD}{log_channel.name}{Colors.RESET} (ID: {forwarding_logs_channel_id})")
                else:
                    print(f"{Colors.YELLOW}⚠️  Forwarding Logs Channel:{Colors.RESET} Not found (ID: {forwarding_logs_channel_id})")
            
            # Channel configurations
            channels = self.config.get("channels", [])
            print(f"{Colors.GREEN}📡 Forwarding Jobs:{Colors.RESET} {len(channels)} channel(s)")
            for i, channel_config in enumerate(channels[:5], 1):  # Show first 5
                source_id = channel_config.get("source_channel_id", "N/A")
                source_name = channel_config.get("source_channel_name", "N/A")
                if self.rs_guild and source_id != "N/A":
                    source_channel = self.rs_guild.get_channel(int(source_id))
                    if source_channel:
                        print(f"   {i}. {Colors.BOLD}{source_channel.name}{Colors.RESET} → Webhook")
                    else:
                        print(f"   {i}. {Colors.YELLOW}Channel {source_id}{Colors.RESET} → Webhook")
                else:
                    print(f"   {i}. {Colors.BOLD}{source_name}{Colors.RESET} → Webhook")
            if len(channels) > 5:
                print(f"   ... and {len(channels) - 5} more")
            
            print(f"{Colors.CYAN}{'-'*60}{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
            
            # Initialize stats
            from datetime import datetime, timezone
            self.stats['started_at'] = datetime.now(timezone.utc).isoformat()
            
            # ALWAYS try to fetch icon via API as final attempt (even if guild was found)
            if not self.rs_icon_url and guild_id:
                print(f"{Colors.CYAN}[Icon] Final attempt: Fetching icon via API...{Colors.RESET}")
                await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
            
            # Final check - if still no icon, log warning
            if not self.rs_icon_url:
                print(f"{Colors.RED}[Bot] ❌ CRITICAL: RS Server icon NOT available!{Colors.RESET}")
                print(f"{Colors.RED}[Bot] Branding will not work properly without the icon.{Colors.RESET}")
                print(f"{Colors.RED}[Bot] Use !rsfetchicon to manually fetch the icon.{Colors.RESET}")

            # Affiliate rewrite startup self-test
            try:
                if bool(self.config.get("affiliate_rewrite_enabled")):
                    print(f"\n{Colors.CYAN}[Affiliate] Startup self-test:{Colors.RESET}")

                    amazon_test_url = (os.getenv("RS_STARTUP_TEST_AMAZON_URL", "") or "").strip() or "https://www.amazon.com/dp/B000000000"
                    try:
                        mapped, notes = await affiliate_rewriter.compute_affiliate_rewrites(self.config, [amazon_test_url])
                        out = (mapped.get(amazon_test_url) or "").strip()
                        if out and out != amazon_test_url:
                            print(f"{Colors.GREEN}[Affiliate] ✅ Amazon PASS{Colors.RESET} -> {out}")
                        else:
                            why = (notes.get(amazon_test_url) or "no change").strip()
                            print(f"{Colors.YELLOW}[Affiliate] ⚠️  Amazon NO-CHANGE{Colors.RESET} ({why})")
                    except Exception as e:
                        print(f"{Colors.RED}[Affiliate] ❌ Amazon FAIL{Colors.RESET} ({e})")

                    # Mavely startup check (NON-mutating):
                    # Use a preflight/session check instead of creating an affiliate link (prevents dashboard spam).
                    try:
                        ok, status, err = await affiliate_rewriter.mavely_preflight(self.config)
                        if ok:
                            print(f"{Colors.GREEN}[Affiliate] ✅ Mavely preflight OK{Colors.RESET} (status={status})")
                        else:
                            # Keep error short (never print cookies/tokens)
                            msg = (err or "unknown error").replace("\n", " ").strip()
                            if len(msg) > 180:
                                msg = msg[:180] + "..."
                            print(f"{Colors.RED}[Affiliate] ❌ Mavely preflight FAIL{Colors.RESET} (status={status}) {msg}")
                    except Exception as e:
                        print(f"{Colors.RED}[Affiliate] ❌ Mavely preflight FAIL{Colors.RESET} ({e})")
            except Exception:
                pass

            # Start background Mavely monitor (DM alerts + auto-start login desktop)
            try:
                if self._mavely_monitor_task is None or self._mavely_monitor_task.done():
                    self._mavely_monitor_task = asyncio.create_task(self._mavely_monitor_loop())
            except Exception:
                pass
            
            print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}  🔄 RS Forwarder Bot{Colors.RESET}")
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}")
            print(f"{Colors.GREEN}[Bot] Ready as {self.bot.user}{Colors.RESET}")
            
            channels = self.config.get("channels", [])
            print(f"{Colors.GREEN}[Bot] Monitoring {len(channels)} channel(s){Colors.RESET}")
            
            if channels:
                # List monitored channels
                for channel in channels:
                    source_id = channel.get("source_channel_id", "unknown")
                    source_name = channel.get("source_channel_name", "unknown")
                    webhook_set = "✓" if channel.get("destination_webhook_url") else "✗"
                    role_mention = ""
                    role_config = channel.get("role_mention", {})
                    if role_config.get("role_id"):
                        role_id = role_config.get("role_id")
                        text = role_config.get("text", "")
                        role_mention = f" | Role: <@&{role_id}> {text}"
                    print(f"  {webhook_set} {source_name} ({source_id}){role_mention}")
            else:
                print(f"  {Colors.YELLOW}No channels configured. Use !add command to add channels.{Colors.RESET}")
            
            print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
            self.stats['started_at'] = datetime.now().isoformat()
        
        @self.bot.event
        async def on_message(message: discord.Message):
            # Process commands first
            await self.bot.process_commands(message)
            
            # Don't skip bot messages - we want to forward ALL messages including bot messages
            # Only skip our own bot's messages to avoid loops
            if message.author.id == self.bot.user.id:
                return
            
            # Check if this is a monitored channel
            channel_id = str(message.channel.id)
            channel_config = self.get_channel_config(channel_id)
            
            if channel_config:
                # Forward the message
                await self.forward_message(message, channel_id, channel_config)
    
    def _setup_commands(self):
        """Setup bot commands"""
        
        @self.bot.command(name='rsadd', aliases=['add'])
        async def add_channel(ctx, source_channel: discord.TextChannel = None, destination_webhook_url: str = None, 
                             role_id: str = None, *, text: str = None):
            """Add a new source channel to destination mapping
            
            Usage: !rsadd <#channel|channel_id> <webhook_url> [role_id] [text]
            
            Examples:
            !rsadd #personal-deals <WEBHOOK_URL> 886824827745337374 "leads found!"
            !rsadd 1446174806981480578 <WEBHOOK_URL> 886824827745337374
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsadd <#channel|channel_id> <webhook_url> [role_id] [text]`\n\n"
                    "**Examples:**\n"
                    "`!rsadd #personal-deals <WEBHOOK_URL> 886824827745337374 \"leads found!\"`\n"
                    "`!rsadd 1446174806981480578 <WEBHOOK_URL> 886824827745337374`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            if not destination_webhook_url:
                await ctx.send(
                    "❌ **Missing webhook URL!**\n"
                    "**Usage:** `!rsadd <#channel|channel_id> <webhook_url> [role_id] [text]`"
                )
                return
            
            # Validate webhook URL format
            if not destination_webhook_url.startswith('https://discord.com/api/webhooks/'):
                await ctx.send("❌ Invalid webhook URL format. Must be a Discord webhook URL.")
                return
            
            # Check if channel already exists
            existing = self.get_channel_config(source_channel_id)
            if existing:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) is already configured!\n"
                    f"Use `!rsupdate` to modify it or `!rsremove` to remove it first."
                )
                return
            
            # Validate role_id if provided
            if role_id:
                try:
                    # Just validate it's a valid number format
                    int(role_id)
                except ValueError:
                    await ctx.send("❌ Invalid role ID format. Role ID must be a number.")
                    return
            
            # Persist webhook into config.secrets.json (server-only) so RSForwarder startup validation passes.
            ok = self._set_destination_webhook_secret(source_channel_id, destination_webhook_url.strip())
            if not ok:
                await ctx.send("❌ Failed to write webhook into `config.secrets.json`. Check file permissions on the server.")
                return

            # Create new channel config (no secret URL stored in config.json)
            new_channel = {
                "source_channel_id": source_channel_id,
                "source_channel_name": channel_name,
                "role_mention": {
                    "role_id": role_id.strip() if role_id else "",
                    "text": text.strip() if text else ""
                }
            }
            
            # Add to config
            if "channels" not in self.config:
                self.config["channels"] = []
            self.config["channels"].append(new_channel)
            self.save_config()
            
            # Reload config
            self.load_config()
            
            # Build confirmation message
            role_info = ""
            if role_id:
                role_text = text if text else ""
                role_info = f"\n📢 Role Mention: <@&{role_id}> {role_text}"
            
            embed = discord.Embed(
                title="✅ New Forwarding Job Added",
                color=discord.Color.green(),
                description=f"Bot will start forwarding messages from this channel."
            )
            embed.add_field(
                name="📥 Source Channel",
                value=f"`{channel_name}`\nID: `{source_channel_id}`",
                inline=True
            )
            embed.add_field(
                name="📤 Destination",
                value="Webhook configured (saved to secrets)",
                inline=True
            )
            if role_info:
                embed.add_field(
                    name="📢 Role Mention",
                    value=f"<@&{role_id}> {text if text else ''}",
                    inline=False
                )
            embed.set_footer(text="Use !rslist to view all jobs or !rsview to see details")
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rslist', aliases=['list'])
        async def list_channels(ctx):
            """List all configured channels"""
            channels = self.config.get("channels", [])
            if not channels:
                await ctx.send("❌ No channels configured.")
                return
            
            embed = discord.Embed(
                title="📋 Configured Channels",
                color=discord.Color.blue()
            )
            
            for channel in channels:
                source_id = channel.get("source_channel_id", "unknown")
                source_name = channel.get("source_channel_name", "unknown")
                webhook = channel.get("destination_webhook_url", "")
                role_config = channel.get("role_mention", {})
                
                status = "✅" if webhook else "❌"
                value = f"Webhook: {'Configured' if webhook else 'Not set'}"
                
                if role_config.get("role_id"):
                    role_id = role_config.get("role_id")
                    text = role_config.get("text", "")
                    value += f"\nRole: <@&{role_id}> {text}"
                
                embed.add_field(
                    name=f"{status} {source_name}",
                    value=f"ID: `{source_id}`\n{value}",
                    inline=False
                )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsupdate', aliases=['update'])
        async def update_channel(ctx, source_channel: discord.TextChannel = None, destination_webhook_url: str = None,
                                role_id: str = None, *, text: str = None):
            """Update an existing forwarding job
            
            Usage: !rsupdate <#channel|channel_id> [webhook_url] [role_id] [text]
            
            Examples:
            !rsupdate #personal-deals <WEBHOOK_URL> 886824827745337374 "new text"
            !rsupdate 1446174806981480578 <WEBHOOK_URL>
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsupdate <#channel|channel_id> [webhook_url] [role_id] [text]`\n\n"
                    "**Examples:**\n"
                    "`!rsupdate #personal-deals <WEBHOOK_URL> 886824827745337374 \"new text\"`\n"
                    "`!rsupdate 1446174806981480578 <WEBHOOK_URL>`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            # Find existing channel config
            existing = self.get_channel_config(source_channel_id)
            if not existing:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) is not configured!\n"
                    f"Use `!rsadd` to add it first."
                )
                return
            
            # Update fields if provided
            updated = False
            if destination_webhook_url:
                if not destination_webhook_url.startswith('https://discord.com/api/webhooks/'):
                    await ctx.send("❌ Invalid webhook URL format. Must be a Discord webhook URL.")
                    return
                ok = self._set_destination_webhook_secret(source_channel_id, destination_webhook_url.strip())
                if not ok:
                    await ctx.send("❌ Failed to write webhook into `config.secrets.json`. Check file permissions on the server.")
                    return
                # Keep in-memory value for display; it won't be written to config.json
                existing["destination_webhook_url"] = destination_webhook_url.strip()
                updated = True
            
            if role_id is not None:
                try:
                    int(role_id)  # Validate format
                except ValueError:
                    await ctx.send("❌ Invalid role ID format. Role ID must be a number.")
                    return
                if "role_mention" not in existing:
                    existing["role_mention"] = {}
                existing["role_mention"]["role_id"] = role_id.strip()
                updated = True
            
            if text is not None:
                if "role_mention" not in existing:
                    existing["role_mention"] = {}
                existing["role_mention"]["text"] = text.strip()
                updated = True
            
            if not updated:
                await ctx.send("❌ No fields to update. Provide at least one: webhook_url, role_id, or text.")
                return
            
            # Save changes
            self.save_config()
            self.load_config()
            
            # Build confirmation message
            embed = discord.Embed(
                title="✅ Forwarding Job Updated",
                color=discord.Color.blue(),
                description=f"Updated configuration for `{channel_name}`"
            )
            
            webhook = existing.get("destination_webhook_url", "")
            role_config = existing.get("role_mention", {})
            
            embed.add_field(
                name="📥 Source Channel",
                value=f"`{channel_name}`\nID: `{source_channel_id}`",
                inline=True
            )
            embed.add_field(
                name="📤 Destination",
                value="Webhook configured (saved to secrets)" if webhook else "Not set",
                inline=True
            )
            
            if role_config.get("role_id"):
                embed.add_field(
                    name="📢 Role Mention",
                    value=f"<@&{role_config.get('role_id')}> {role_config.get('text', '')}",
                    inline=False
                )
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsview', aliases=['view'])
        async def view_channel(ctx, source_channel: discord.TextChannel = None):
            """View details of a specific forwarding job
            
            Usage: !rsview <#channel|channel_id>
            
            Example: !rsview #personal-deals
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsview <#channel|channel_id>`\n"
                    "**Example:** `!rsview #personal-deals`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            # Find channel config
            channel_config = self.get_channel_config(source_channel_id)
            if not channel_config:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) is not configured!\n"
                    f"Use `!rsadd` to add it."
                )
                return
            
            # Build detailed view
            webhook = channel_config.get("destination_webhook_url", "")
            role_config = channel_config.get("role_mention", {})
            
            embed = discord.Embed(
                title=f"📋 Forwarding Job: {channel_name}",
                color=discord.Color.blue(),
                description=f"Detailed configuration for this forwarding job"
            )
            
            embed.add_field(
                name="📥 Source Channel",
                value=f"**Name:** `{channel_name}`\n**ID:** `{source_channel_id}`",
                inline=False
            )
            
            embed.add_field(
                name="📤 Destination Webhook",
                value=f"`{mask_secret(webhook)}`" if webhook else "❌ Not configured",
                inline=False
            )
            
            role_id = role_config.get("role_id", "")
            role_text = role_config.get("text", "")
            if role_id:
                embed.add_field(
                    name="📢 Role Mention",
                    value=f"**Role:** <@&{role_id}>\n**Text:** {role_text if role_text else '(empty)'}",
                    inline=False
                )
            else:
                embed.add_field(
                    name="📢 Role Mention",
                    value="❌ Not configured",
                    inline=False
                )
            
            embed.set_footer(text="Use !rsupdate to modify or !rsremove to delete")
            
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsremove', aliases=['remove'])
        async def remove_channel(ctx, source_channel: discord.TextChannel = None):
            """Remove a channel configuration
            
            Usage: !rsremove <#channel|channel_id>
            
            Example: !rsremove #personal-deals
            """
            if not source_channel:
                await ctx.send(
                    "❌ **Usage:** `!rsremove <#channel|channel_id>`\n"
                    "**Example:** `!rsremove #personal-deals`"
                )
                return
            
            source_channel_id = str(source_channel.id)
            channel_name = source_channel.name
            
            channels = self.config.get("channels", [])
            original_count = len(channels)
            self.config["channels"] = [
                ch for ch in channels 
                if str(ch.get("source_channel_id")) != str(source_channel_id)
            ]
            
            if len(self.config["channels"]) < original_count:
                # Remove secret webhook mapping too (server-only)
                self._delete_destination_webhook_secret(source_channel_id)
                self.save_config()
                self.load_config()
                await ctx.send(
                    f"✅ **Channel removed!**\n"
                    f"`{channel_name}` ({source_channel_id}) has been removed from configuration."
                )
            else:
                await ctx.send(
                    f"❌ Channel `{channel_name}` ({source_channel_id}) not found in configuration."
                )
        
        @self.bot.command(name='rstest', aliases=['test'])
        async def test_forward(ctx, source_channel_id: str = None, limit: int = 1):
            """Test forwarding by forwarding recent messages from a source channel
            
            Usage: !rstest [source_channel_id] [limit]
            Example: !rstest 1446174861931188387 5
            """
            if not source_channel_id:
                # If no channel specified, test with current channel
                source_channel_id = str(ctx.channel.id)
            
            channel_config = self.get_channel_config(source_channel_id)
            if not channel_config:
                await ctx.send(f"❌ Channel `{source_channel_id}` is not configured. Use `!add` to add it first.")
                return
            
            webhook_url = channel_config.get("destination_webhook_url", "").strip()
            if not webhook_url:
                await ctx.send(f"❌ No webhook configured for channel `{source_channel_id}`")
                return
            
            try:
                source_channel = self.bot.get_channel(int(source_channel_id))
                if not source_channel:
                    await ctx.send(f"❌ Cannot access channel `{source_channel_id}`. Bot may not have permission.")
                    return
                
                await ctx.send(f"🔄 Fetching last {limit} message(s) from <#{source_channel_id}>...")
                
                forwarded_count = 0
                async for message in source_channel.history(limit=limit):
                    # Don't skip bot messages - forward ALL messages
                    # Only skip our own bot's messages to avoid loops
                    if message.author.id == self.bot.user.id:
                        continue
                    
                    await self.forward_message(message, source_channel_id, channel_config)
                    forwarded_count += 1
                
                if forwarded_count > 0:
                    await ctx.send(f"✅ Successfully forwarded {forwarded_count} message(s) to webhook!")
                else:
                    await ctx.send(f"ℹ️ No messages found to forward (only skipped our own bot's messages)")
                    
            except discord.Forbidden:
                await ctx.send(f"❌ Bot doesn't have permission to read messages in channel `{source_channel_id}`")
            except Exception as e:
                await ctx.send(f"❌ Error: {str(e)}")

        async def _get_or_create_test_webhook(channel: discord.TextChannel) -> Optional[str]:
            """
            Create/reuse a webhook in the given channel (requires Manage Webhooks permission).
            Returns webhook URL or None.
            """
            try:
                hooks = await channel.webhooks()
            except Exception:
                hooks = []
            me_id = int(self.bot.user.id) if self.bot.user else 0
            for h in hooks:
                try:
                    if h.user and me_id and int(h.user.id) == me_id:
                        return str(h.url)
                except Exception:
                    continue
            try:
                wh = await channel.create_webhook(name="RSForwarder Test")
                return str(wh.url)
            except Exception:
                return None

        @self.bot.command(name='rstestall', aliases=['testall'])
        async def test_forward_all(ctx, test_channel_id: str = "1446372213757313034", limit: int = 1):
            """
            Test forwarding for ALL configured channels by sending the most recent message(s)
            from each source channel into the given test channel via an auto-created webhook.

            Usage: !rstestall [test_channel_id] [limit]
            Example: !rstestall 1446372213757313034 1
            """
            # Admin-only (this can spam the test channel)
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ Admins only.")
                return

            try:
                test_ch = self.bot.get_channel(int(str(test_channel_id).strip()))
            except Exception:
                test_ch = None
            if not isinstance(test_ch, discord.TextChannel):
                await ctx.send(f"❌ Test channel not found or not accessible: `{test_channel_id}`")
                return

            webhook_url = await _get_or_create_test_webhook(test_ch)
            if not webhook_url:
                await ctx.send("❌ Could not create/reuse a webhook in the test channel. Check **Manage Webhooks** permission.")
                return

            channels = self.config.get("channels", []) or []
            if not channels:
                await ctx.send("❌ No channels configured in RSForwarder/config.json")
                return

            try:
                limit_i = int(limit)
            except Exception:
                limit_i = 1
            limit_i = max(1, min(limit_i, 5))

            await ctx.send(f"🧪 Testing `{len(channels)}` channel(s) → <#{test_channel_id}> (limit={limit_i}) ...")

            ok = 0
            fail = 0
            for chcfg in channels:
                src_id = str((chcfg or {}).get("source_channel_id", "")).strip()
                if not src_id:
                    continue
                src = self.bot.get_channel(int(src_id)) if src_id.isdigit() else None
                if not isinstance(src, discord.TextChannel):
                    fail += 1
                    continue

                # Clone config but override destination webhook to the test channel webhook
                tmp_cfg = dict(chcfg)
                tmp_cfg["destination_webhook_url"] = webhook_url
                tmp_cfg["source_channel_name"] = str((chcfg or {}).get("source_channel_name") or src.name)

                sent_any = False
                try:
                    async for message in src.history(limit=limit_i):
                        if self.bot.user and message.author.id == self.bot.user.id:
                            continue
                        await self.forward_message(message, src_id, tmp_cfg)
                        sent_any = True
                        break
                except Exception:
                    sent_any = False

                if sent_any:
                    ok += 1
                else:
                    fail += 1

            await ctx.send(f"✅ rstestall complete: ok={ok} fail={fail}")
        
        @self.bot.command(name='rsstatus', aliases=['status'])
        async def bot_status(ctx):
            """Show bot status and configuration"""
            embed = discord.Embed(
                title="🤖 RS Forwarder Bot Status",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            # Bot info
            embed.add_field(
                name="Bot Status",
                value=f"✅ Online\nUser: {self.bot.user}\nUptime: {self._get_uptime()}",
                inline=False
            )
            
            # RS Server info
            guild_id = self.config.get("guild_id", 0)
            if self.rs_guild:
                embed.add_field(
                    name="RS Server",
                    value=f"✅ Connected\nName: {self.rs_guild.name}\nID: {guild_id}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="RS Server",
                    value=f"❌ Not found\nID: {guild_id}",
                    inline=True
                )
            
            # Icon status
            if self.rs_icon_url:
                embed.add_field(
                    name="RS Server Icon",
                    value=f"✅ Loaded\n[View Icon]({self.rs_icon_url})",
                    inline=True
                )
            else:
                embed.add_field(
                    name="RS Server Icon",
                    value="❌ Not available",
                    inline=True
                )
            
            # Stats
            embed.add_field(
                name="Statistics",
                value=f"Messages Forwarded: {self.stats['messages_forwarded']}\nErrors: {self.stats['errors']}",
                inline=False
            )
            
            # Channels
            channels = self.config.get("channels", [])
            channels_info = []
            for ch in channels:
                status = "✅" if ch.get("destination_webhook_url") else "❌"
                channels_info.append(f"{status} {ch.get('source_channel_name', 'unknown')}")
            
            embed.add_field(
                name=f"Configured Channels ({len(channels)})",
                value="\n".join(channels_info) if channels_info else "No channels configured",
                inline=False
            )
            
            embed.set_footer(text="Use !rscommands for command list")
            await ctx.send(embed=embed)
        
        @self.bot.command(name='rsfetchicon', aliases=['fetchicon'])
        async def fetch_icon(ctx):
            """Manually fetch RS Server icon"""
            guild_id = self.config.get("guild_id", 0)
            if not guild_id:
                await ctx.send("❌ No `guild_id` configured in config.json")
                return
            
            await ctx.send(f"🔄 Fetching RS Server icon for guild {guild_id}...")
            
            # Try guild object first
            self.rs_guild = self.bot.get_guild(guild_id)
            if self.rs_guild:
                self.rs_icon_url = self._get_guild_icon_url(self.rs_guild)
                if self.rs_icon_url:
                    await ctx.send(f"✅ Icon fetched from guild object: {self.rs_icon_url[:50]}...")
                    return
            
            # Try API
            icon_fetched = await self._fetch_guild_icon_via_api(guild_id, save_to_config=True)
            if icon_fetched and self.rs_icon_url:
                await ctx.send(f"✅ Icon fetched via API and saved to config: {self.rs_icon_url[:50]}...")
            else:
                await ctx.send(f"❌ Failed to fetch icon. Check console for details.")
        
        @self.bot.command(name='rsstartadminbot', aliases=['startadminbot', 'startadmin'])
        async def start_admin_bot(ctx):
            """Start RSAdminBot remotely on the server (admin only)"""
            # Check if user has admin permissions
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            
            # Prefer local-exec when RSForwarder runs on the Ubuntu host.
            if self._is_local_exec():
                await ctx.send("🔄 Starting RSAdminBot locally on this server...")
                ok, out = self._run_local_botctl("start")
                if ok:
                    await ctx.send("✅ **RSAdminBot start requested (local)**")
                    if out:
                        await ctx.send(f"```{out[:1500]}```")
                else:
                    await ctx.send(f"❌ Failed to start RSAdminBot (local):\n```{out[:1500]}```")
                return

            await ctx.send("🔄 Starting RSAdminBot on remote server...")
            
            try:
                # subprocess/shlex imported at module level
                
                # Get SSH config from oraclekeys
                oraclekeys_path = Path(__file__).parent.parent / "oraclekeys"
                servers_json = oraclekeys_path / "servers.json"
                
                if not servers_json.exists():
                    await ctx.send("❌ Could not find servers.json configuration")
                    return
                
                import json
                with open(servers_json, 'r') as f:
                    servers = json.load(f)
                
                if not servers:
                    await ctx.send("❌ No servers configured")
                    return
                
                server = servers[0]
                remote_user = server.get("user", "rsadmin")
                remote_host = server.get("host", "")
                ssh_key = server.get("key")
                
                if not remote_host:
                    await ctx.send("❌ Server host not configured")
                    return
                
                # Build SSH command - check multiple locations for key
                ssh_key_path = None
                if ssh_key:
                    if Path(ssh_key).is_absolute():
                        ssh_key_path = Path(ssh_key)
                    else:
                        # Try oraclekeys folder first
                        ssh_key_path = oraclekeys_path / ssh_key
                        # If not found, try RSAdminBot folder (where key might also be)
                        if not ssh_key_path.exists():
                            rsadminbot_path = Path(__file__).parent.parent / "RSAdminBot" / ssh_key
                            if rsadminbot_path.exists():
                                ssh_key_path = rsadminbot_path
                
                # Fix SSH key permissions on Windows (required for SSH to work)
                if ssh_key_path and ssh_key_path.exists() and platform.system() == "Windows":
                    self._fix_ssh_key_permissions(ssh_key_path)
                
                if ssh_key and not (ssh_key_path and ssh_key_path.exists()):
                    await ctx.send(f"❌ SSH key not found: {ssh_key}\nChecked: oraclekeys/{ssh_key} and RSAdminBot/{ssh_key}")
                    return
                
                ssh_base = ["ssh"]
                if ssh_key_path and ssh_key_path.exists():
                    ssh_base.extend(["-i", str(ssh_key_path)])
                # Add options for better connection handling (match RSAdminBot)
                ssh_base.extend([
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10"
                ])
                
                # Use canonical botctl.sh script to start RSAdminBot
                botctl_path = "/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh"
                start_cmd = f"bash {botctl_path} start rsadminbot"
                
                # Escape command for bash -lc
                escaped_cmd = shlex.quote(start_cmd)
                
                # Build command (no -t flag needed, script handles everything)
                cmd = ssh_base + ["-o", "ConnectTimeout=10", f"{remote_user}@{remote_host}", "bash", "-lc", escaped_cmd]
                
                print(f"{Colors.CYAN}[RSForwarder] Starting RSAdminBot remotely using botctl.sh...{Colors.RESET}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, shell=False, encoding='utf-8', errors='replace')
                
                if result.returncode == 0:
                    # Script handles verification internally
                    output = result.stdout or result.stderr or ""
                    if "SUCCESS" in output or "active" in output.lower():
                        await ctx.send("✅ **RSAdminBot started successfully on remote server!**")
                        print(f"{Colors.GREEN}[RSForwarder] RSAdminBot started successfully{Colors.RESET}")
                    else:
                        await ctx.send(f"⚠️ RSAdminBot start completed but verification unclear:\n```{output[:300]}```")
                        print(f"{Colors.YELLOW}[RSForwarder] RSAdminBot start completed: {output[:200]}{Colors.RESET}")
                else:
                    error_msg = result.stderr or result.stdout or "Unknown error"
                    await ctx.send(f"❌ Failed to start RSAdminBot:\n```{error_msg[:500]}```")
                    print(f"{Colors.RED}[RSForwarder] Failed to start RSAdminBot: {error_msg[:200]}{Colors.RESET}")
                    
            except subprocess.TimeoutExpired:
                await ctx.send("❌ Command timed out - RSAdminBot may still be starting")
            except FileNotFoundError:
                await ctx.send("❌ SSH not found - make sure SSH is installed")
            except Exception as e:
                await ctx.send(f"❌ Error: {str(e)[:500]}")
                print(f"{Colors.RED}[RSForwarder] Error starting RSAdminBot: {e}{Colors.RESET}")
        
        @self.bot.command(name='rsrestartadminbot', aliases=['restartadminbot', 'restartadmin', 'restart'])
        async def restart_admin_bot(ctx):
            """Restart RSAdminBot remotely on the server (admin only)"""
            # Check if user has admin permissions
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            
            # Prefer local-exec when RSForwarder runs on the Ubuntu host.
            if self._is_local_exec():
                await ctx.send("🔄 Restarting RSAdminBot locally on this server...")
                ok, out = self._run_local_botctl("restart")
                if ok:
                    await ctx.send("✅ **RSAdminBot restart requested (local)**")
                    if out:
                        await ctx.send(f"```{out[:1500]}```")
                else:
                    await ctx.send(f"❌ Failed to restart RSAdminBot (local):\n```{out[:1500]}```")
                return

            await ctx.send("🔄 Restarting RSAdminBot on remote server...")
            
            try:
                # subprocess/shlex imported at module level
                
                # Get SSH config from oraclekeys
                oraclekeys_path = Path(__file__).parent.parent / "oraclekeys"
                servers_json = oraclekeys_path / "servers.json"
                
                if not servers_json.exists():
                    await ctx.send("❌ Could not find servers.json configuration")
                    return
                
                import json
                with open(servers_json, 'r') as f:
                    servers = json.load(f)
                
                if not servers:
                    await ctx.send("❌ No servers configured")
                    return
                
                server = servers[0]
                remote_user = server.get("user", "rsadmin")
                remote_host = server.get("host", "")
                ssh_key = server.get("key")
                
                if not remote_host:
                    await ctx.send("❌ Server host not configured")
                    return
                
                # Build SSH command - check multiple locations for key
                ssh_key_path = None
                if ssh_key:
                    if Path(ssh_key).is_absolute():
                        ssh_key_path = Path(ssh_key)
                    else:
                        # Try oraclekeys folder first
                        ssh_key_path = oraclekeys_path / ssh_key
                        # If not found, try RSAdminBot folder (where key actually is)
                        if not ssh_key_path.exists():
                            rsadminbot_path = Path(__file__).parent.parent / "RSAdminBot" / ssh_key
                            if rsadminbot_path.exists():
                                ssh_key_path = rsadminbot_path
                
                # Fix SSH key permissions on Windows (required for SSH to work)
                if ssh_key_path and ssh_key_path.exists() and platform.system() == "Windows":
                    self._fix_ssh_key_permissions(ssh_key_path)
                
                if ssh_key_path and not ssh_key_path.exists():
                    await ctx.send(f"❌ SSH key not found: {ssh_key_path}\nExpected at: oraclekeys/{ssh_key} or RSAdminBot/{ssh_key}")
                    return
                
                ssh_base = ["ssh"]
                if ssh_key_path and ssh_key_path.exists():
                    ssh_base.extend(["-i", str(ssh_key_path)])
                # Add options for better connection handling (match RSAdminBot)
                ssh_base.extend([
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=10"
                ])
                
                # Use canonical botctl.sh script to restart RSAdminBot
                botctl_path = "/home/rsadmin/bots/mirror-world/RSAdminBot/botctl.sh"
                restart_cmd = f"bash {botctl_path} restart rsadminbot"
                
                # Escape command for bash -lc
                escaped_cmd = shlex.quote(restart_cmd)
                
                # Build command (no -t flag needed, script handles everything)
                cmd = ssh_base + ["-o", "ConnectTimeout=10", f"{remote_user}@{remote_host}", "bash", "-lc", escaped_cmd]
                
                print(f"{Colors.CYAN}[RSForwarder] Restarting RSAdminBot remotely using botctl.sh...{Colors.RESET}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, shell=False, encoding='utf-8', errors='replace')
                
                if result.returncode == 0:
                    # Script handles verification internally
                    output = result.stdout or result.stderr or ""
                    if "SUCCESS" in output or "active" in output.lower():
                        await ctx.send("✅ **RSAdminBot restarted successfully on remote server!**")
                        print(f"{Colors.GREEN}[RSForwarder] RSAdminBot restarted successfully{Colors.RESET}")
                    else:
                        await ctx.send(f"⚠️ RSAdminBot restart completed but verification unclear:\n```{output[:300]}```")
                        print(f"{Colors.YELLOW}[RSForwarder] RSAdminBot restart completed: {output[:200]}{Colors.RESET}")
                else:
                    error_msg = result.stderr or result.stdout or "Unknown error"
                    await ctx.send(f"❌ Failed to restart RSAdminBot:\n```{error_msg[:500]}```")
                    print(f"{Colors.RED}[RSForwarder] Failed to restart RSAdminBot: {error_msg[:200]}{Colors.RESET}")
                    
            except subprocess.TimeoutExpired:
                await ctx.send("❌ Command timed out - RSAdminBot may still be restarting")
            except FileNotFoundError:
                await ctx.send("❌ SSH not found - make sure SSH is installed")
            except Exception as e:
                await ctx.send(f"❌ Error: {str(e)[:500]}")
                print(f"{Colors.RED}[RSForwarder] Error restarting RSAdminBot: {e}{Colors.RESET}")

        @self.bot.command(name="rsmavelylogin", aliases=["mavelylogin", "refreshtoken"])
        async def mavely_login(ctx, wait_seconds: str = None):
            """Start/ensure noVNC desktop and launch interactive Mavely cookie refresher (admin only).

            Usage:
              !rsmavelylogin
              !rsmavelylogin 900

            Notes:
            - This does NOT log in automatically; it opens a browser on the server desktop.
            - You connect via SSH tunnel to noVNC and log in manually.
            """
            if not self._is_mavely_admin_ctx(ctx):
                await ctx.send("❌ You don't have permission to use this command.")
                return

            if not self._is_local_exec():
                await ctx.send("❌ This command only works when RSForwarder is running on the Linux host (Oracle).")
                return

            try:
                wait_s = int((wait_seconds or "").strip() or "900")
            except Exception:
                wait_s = 900
            wait_s = max(60, min(wait_s, 3600))

            # Auto-enroll invoker for alerts + DM admin (guild-only).
            try:
                if getattr(ctx, "guild", None) is not None:
                    self._ensure_mavely_user(int(ctx.author.id))
            except Exception:
                pass

            # Best effort: delete the command message (keeps channels clean)
            try:
                if getattr(ctx, "guild", None) is not None:
                    await ctx.message.delete()
            except Exception:
                pass

            channel_ack = None
            try:
                if getattr(ctx, "guild", None) is not None:
                    channel_ack = await ctx.send("🔄 Starting noVNC desktop + launching Mavely login browser... I’ll DM you the tunnel + URL.")
            except Exception:
                channel_ack = None

            info, err = await asyncio.to_thread(novnc_stack.ensure_novnc, self.config)
            if err or not info:
                await ctx.send(f"❌ noVNC start failed: {str(err)[:500]}")
                return

            pid, log_path, err2 = await asyncio.to_thread(
                novnc_stack.start_cookie_refresher,
                self.config,
                display=str(info.get("display") or ":99"),
                wait_login_s=int(wait_s),
            )
            if err2:
                await ctx.send(f"❌ Failed to launch cookie refresher: {str(err2)[:500]}")
                return

            web_port = int(info.get("web_port") or 6080)
            url_path = str(info.get("url_path") or "/vnc.html")

            msg = self._build_tunnel_instructions(web_port, url_path)
            if pid:
                msg += f"\n\nCookie refresher PID: `{pid}`"
            if log_path:
                msg += f"\nRefresher log (server path): `{log_path}`"

            # Prefer DM. If DM fails, fall back to channel.
            sent_dm = False
            try:
                await ctx.author.send(msg)
                sent_dm = True
            except Exception:
                sent_dm = False

            if getattr(ctx, "guild", None) is not None:
                try:
                    if sent_dm:
                        done = await ctx.send("✅ Sent you a DM with the noVNC tunnel + URL. Run `!rsmavelycheck` after you log in.")
                    else:
                        done = await ctx.send(msg)
                    try:
                        await done.delete(delay=20)  # type: ignore[arg-type]
                    except Exception:
                        pass
                except Exception:
                    pass
                try:
                    if channel_ack:
                        await channel_ack.delete(delay=5)  # type: ignore[arg-type]
                except Exception:
                    pass
            else:
                # DM channel: we already sent details (or failed). Keep a short confirmation.
                if sent_dm:
                    await ctx.send("✅ Sent. After logging in, run `!rsmavelycheck`.")

        @self.bot.command(name="rsmavelyalertme", aliases=["mavelyalertme"])
        async def mavely_alert_me(ctx):
            """Enable DM alerts for Mavely session failures (admin only; must be run in a guild)."""
            if getattr(ctx, "guild", None) is None:
                await ctx.send("❌ Run this in a server channel (not DMs) so we can verify admin permission.")
                return
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            ok = self._ensure_mavely_user(int(ctx.author.id))
            if ok:
                await ctx.send("✅ Mavely alerts enabled for you. If the session expires, I’ll DM you with the noVNC login steps.")
            else:
                await ctx.send("❌ Failed to save alert settings (could not write config.secrets.json).")

        @self.bot.command(name="rsmavelyalertoff", aliases=["mavelyalertoff"])
        async def mavely_alert_off(ctx):
            """Disable DM alerts for Mavely session failures (admin only; must be run in a guild)."""
            if getattr(ctx, "guild", None) is None:
                await ctx.send("❌ Run this in a server channel (not DMs) so we can verify admin permission.")
                return
            if not ctx.author.guild_permissions.administrator:
                await ctx.send("❌ You don't have permission to use this command.")
                return
            ok = self._remove_mavely_user(int(ctx.author.id))
            if ok:
                await ctx.send("✅ Mavely alerts disabled for you.")
            else:
                await ctx.send("❌ Failed to save alert settings (could not write config.secrets.json).")

        @self.bot.command(name="rsmavelycheck", aliases=["mavelycheck"])
        async def mavely_check(ctx):
            """Run a non-mutating Mavely auth preflight check (safe)."""
            try:
                ok, status, err = await affiliate_rewriter.mavely_preflight(self.config)
                if ok:
                    await ctx.send(f"✅ Mavely preflight OK (status={status})")
                else:
                    msg = (err or "unknown error").replace("\n", " ").strip()
                    if len(msg) > 180:
                        msg = msg[:180] + "..."
                    await ctx.send(f"❌ Mavely preflight FAIL (status={status}) {msg}")
            except Exception as e:
                await ctx.send(f"❌ Mavely preflight FAIL ({str(e)[:300]})")

        @self.bot.command(name="rsmavelystatus", aliases=["mavelystatus"])
        async def mavely_status(ctx):
            """Show last Mavely automation status (admin only; safe, no tokens)."""
            if not self._is_mavely_admin_ctx(ctx):
                await ctx.send("❌ You don't have permission to use this command.")
                return
            try:
                p = self._mavely_status_path()
                data = {}
                if p.exists():
                    try:
                        data = json.loads(p.read_text(encoding="utf-8", errors="replace") or "{}")
                    except Exception:
                        data = {}
                if not isinstance(data, dict):
                    data = {}

                pre_ok = bool(data.get("preflight_ok"))
                pre_status = data.get("preflight_status")
                pre_err = str(data.get("preflight_err") or "").strip()
                al_ts = float(data.get("last_autologin_ts") or 0.0)
                al_ok = data.get("last_autologin_ok")
                al_msg = str(data.get("last_autologin_msg") or "").strip()
                pid = data.get("last_refresher_pid")
                lp = str(data.get("last_refresher_log_path") or "").strip()

                lines = []
                lines.append(f"**Mavely status** (from `{p}`)")
                lines.append(f"- preflight: {'✅ OK' if pre_ok else '❌ FAIL'} (status={pre_status})")
                if (not pre_ok) and pre_err:
                    lines.append(f"- error: `{pre_err[:240]}`")
                if al_ts > 0:
                    ts_str = datetime.utcfromtimestamp(al_ts).isoformat(timespec="seconds") + "Z"
                    lines.append(f"- last auto-login: {ts_str} (ok={al_ok})")
                if al_msg:
                    lines.append(f"- auto-login msg: `{al_msg[:240]}`")
                if pid:
                    lines.append(f"- last noVNC refresher PID: `{pid}`")
                if lp:
                    lines.append(f"- refresher log: `{lp}`")
                lines.append(f"- monitor log: `{self._mavely_monitor_log_path()}`")
                await ctx.send("\n".join(lines)[:1900])
            except Exception as e:
                await ctx.send(f"❌ Failed to read Mavely status: {str(e)[:300]}")
        
        @self.bot.command(name='rscommands', aliases=['commands'])
        async def bot_help(ctx):
            """Show available commands"""
            embed = discord.Embed(
                title="📋 RS Forwarder Bot Commands",
                color=discord.Color.green()
            )
            
            commands_list = [
                ("`!rsstatus`", "Show bot status and configuration"),
                ("`!rslist`", "List all configured forwarding jobs"),
                ("`!rsadd <#channel|id> <webhook_url> [role_id] [text]`", "Add a new forwarding job"),
                ("`!rsupdate <#channel|id> [webhook_url] [role_id] [text]`", "Update an existing forwarding job"),
                ("`!rsview <#channel|id>`", "View details of a specific forwarding job"),
                ("`!rsremove <#channel|id>`", "Remove a forwarding job"),
                ("`!rstest [channel_id] [limit]`", "Test forwarding by forwarding recent messages (default: 1 message)"),
                ("`!rsfetchicon`", "Manually fetch RS Server icon"),
                ("`!rsstartadminbot`", "Start RSAdminBot remotely on server (admin only)"),
                ("`!rsrestartadminbot` or `!restart`", "Restart RSAdminBot remotely on server (admin only)"),
                ("`!rsmavelylogin` or `!refreshtoken`", "Open noVNC + Mavely login browser (admin only; manual login)"),
                ("`!rsmavelycheck`", "Check if Mavely session is valid (safe)"),
                ("`!rsmavelystatus`", "Show last Mavely auto-login/noVNC status (admin only)"),
                ("`!rsmavelyalertme`", "DM me if Mavely session expires (admin only)"),
                ("`!rsmavelyalertoff`", "Disable Mavely expiry DMs (admin only)"),
                ("`!rscommands`", "Show this help message"),
            ]
            
            for cmd, desc in commands_list:
                embed.add_field(name=cmd, value=desc, inline=False)
            
            embed.set_footer(text="Example: !rstest 1446174861931188387 5")
            await ctx.send(embed=embed)
    
    def _get_uptime(self) -> str:
        """Get bot uptime as formatted string"""
        if not self.stats.get('started_at'):
            return "Unknown"
        
        try:
            started = datetime.fromisoformat(self.stats['started_at'])
            uptime = datetime.now() - started
            hours, remainder = divmod(int(uptime.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours}h {minutes}m {seconds}s"
        except:
            return "Unknown"
    
    def _fix_ssh_key_permissions(self, key_path: Path):
        """Fix SSH key file permissions on Windows (required for SSH to work).
        
        SSH requires private keys to have restricted permissions (only readable by owner).
        On Windows, we need to remove permissions for BUILTIN\\Users group.
        
        Args:
            key_path: Path to SSH private key file
        """
        if platform.system() != "Windows":
            return  # Only needed on Windows
        
        try:
            import win32security
            import ntsecuritycon as con
            
            # Get current file security descriptor
            sd = win32security.GetFileSecurity(str(key_path), win32security.DACL_SECURITY_INFORMATION)
            dacl = sd.GetSecurityDescriptorDacl()
            
            # Remove BUILTIN\\Users group permissions
            users_sid = win32security.LookupAccountName("", "BUILTIN\\Users")[0]
            
            # Check if Users group has permissions
            has_users_perms = False
            for i in range(dacl.GetAceCount()):
                ace = dacl.GetAce(i)
                if ace[2] == users_sid:
                    has_users_perms = True
                    break
            
            if has_users_perms:
                # Create new DACL without Users group
                new_dacl = win32security.ACL()
                
                # Add owner full control
                owner_sid = win32security.LookupAccountName("", os.environ.get("USERNAME", ""))[0]
                new_dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, owner_sid)
                
                # Add SYSTEM full control
                system_sid = win32security.LookupAccountName("", "NT AUTHORITY\\SYSTEM")[0]
                new_dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.FILE_ALL_ACCESS, system_sid)
                
                # Set new DACL
                sd.SetSecurityDescriptorDacl(1, new_dacl, 0)
                win32security.SetFileSecurity(str(key_path), win32security.DACL_SECURITY_INFORMATION, sd)
                print(f"{Colors.GREEN}[RSForwarder] Fixed SSH key permissions (removed BUILTIN\\Users access){Colors.RESET}")
        except ImportError:
            # pywin32 not available - try using icacls command instead
            try:
                import subprocess
                # Remove Users group permissions using icacls
                result = subprocess.run(
                    ["icacls", str(key_path), "/remove", "BUILTIN\\Users"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    print(f"{Colors.GREEN}[RSForwarder] Fixed SSH key permissions using icacls{Colors.RESET}")
                else:
                    print(f"{Colors.YELLOW}[RSForwarder] Warning: Could not fix SSH key permissions: {result.stderr}{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.YELLOW}[RSForwarder] Warning: Could not fix SSH key permissions: {e}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.YELLOW}[RSForwarder] Warning: Could not fix SSH key permissions: {e}{Colors.RESET}")
    
    def _get_channel_alert_footer(self, channel_name: str) -> str:
        """Get custom alert footer text based on channel name"""
        channel_lower = channel_name.lower()
        if "online-important" in channel_lower or "online important" in channel_lower:
            return "RS Leads - Online Important Alert!"
        elif "personal-deals" in channel_lower or "personal deals" in channel_lower:
            return "RS Leads - Personal Deals Alert!"
        elif "minor-deals" in channel_lower or "minor deals" in channel_lower:
            return "RS Leads - Minor Deals Alert!"
        else:
            return f"RS Leads - {channel_name.title()} Alert!"
    
    def _format_timestamp(self, timestamp: datetime = None) -> str:
        """Format timestamp as 'at 1:27 AM' format"""
        if timestamp is None:
            timestamp = datetime.now()
        # Format as "at 1:27 AM" - Windows compatible
        try:
            # Try Unix-style format first (removes leading zero)
            time_str = timestamp.strftime("%-I:%M %p")
            return f"at {time_str}"
        except ValueError:
            # Windows fallback - remove leading zero manually
            time_str = timestamp.strftime("%I:%M %p")
            if time_str.startswith("0"):
                time_str = time_str[1:]
            return f"at {time_str}"
    
    def _apply_rs_branding(self, embeds: List[Dict[str, Any]], channel_name: str = None) -> List[Dict[str, Any]]:
        """Apply Reselling Secrets branding to embeds"""
        branded_embeds = []
        
        # Get brand name once at the start - used throughout the function
        brand_name = self.config.get("brand_name", "Reselling Secrets")
        
        for embed in embeds:
            # Deep copy to avoid modifying original
            branded_embed = json.loads(json.dumps(embed))
            
            # Preserve image from original embed (if it exists)
            # Image will be added later if from message attachments
            
            # Update footer with RS branding and timestamp
            if "footer" in branded_embed:
                footer = branded_embed.get("footer", {}) or {}
            else:
                footer = {}
            
            # Set footer text to just "Reselling Secrets" (simple footer per boss request)
            footer["text"] = brand_name
            
            # Remove footer icon (no logo at bottom per boss request)
            if "icon_url" in footer:
                del footer["icon_url"]
            
            branded_embed["footer"] = footer
            
            # Remove author field completely (no "Reselling Secrets" at top per boss request)
            if "author" in branded_embed:
                del branded_embed["author"]
            
            # Remove title (simple embed per boss request)
            if "title" in branded_embed:
                del branded_embed["title"]
            
            # Set embed color to blue sidebar (branding blue)
            branded_embed["color"] = 0x0099FF  # Branding blue color
            
            branded_embeds.append(branded_embed)
        
        return branded_embeds
    
    def _create_embed_from_message(self, content: str, channel_name: str, author_name: str = None) -> Dict[str, Any]:
        """Create an embed from a normal text message"""
        brand_name = self.config.get("brand_name", "Reselling Secrets")
        timestamp_str = self._format_timestamp()
        alert_text = self._get_channel_alert_footer(channel_name)
        
        embed = {
            # No title (simple embed per boss request)
            "description": content[:4096] if content else "\u200b",  # Discord embed description limit
            "color": 0x0099FF,  # Branding blue color
            "footer": {
                "text": brand_name,  # Just "Reselling Secrets" in footer
            }
            # No author field (no "Reselling Secrets" at top per boss request)
        }
        
        return embed
    
    def _get_role_mention_text(self, channel_config: Dict[str, Any]) -> Optional[str]:
        """Get role mention text for a channel if configured"""
        role_mention = channel_config.get("role_mention", {})
        if not role_mention:
            return None
        
        role_id = role_mention.get("role_id", "").strip()
        text = role_mention.get("text", "").strip()
        
        if not role_id:
            return None
        
        # If text is blank, return just the role mention without text
        if not text:
            return f"<@&{role_id}>"
        
        return f"<@&{role_id}> {text}"

    def _members_role_ids(self) -> List[int]:
        """
        Return role IDs that correspond to a role named "Members" (case-insensitive).
        Used to prevent forwarding/pinging @Members.
        """
        try:
            if not self.rs_guild:
                guild_id = int(self.config.get("guild_id", 0) or 0)
                if guild_id:
                    self.rs_guild = self.bot.get_guild(guild_id)
            g = self.rs_guild
            if not g:
                return []
            out: List[int] = []
            for r in getattr(g, "roles", []) or []:
                name = (getattr(r, "name", "") or "").strip().lower()
                if name == "members":
                    out.append(int(getattr(r, "id", 0) or 0))
            return [rid for rid in out if rid > 0]
        except Exception:
            return []

    def _strip_members_mentions(self, text: str) -> str:
        """
        Remove @Members role mentions from forwarded content.
        Keeps other role mentions intact.
        """
        t = text or ""
        if not t:
            return t
        try:
            for rid in self._members_role_ids():
                t = t.replace(f"<@&{rid}>", "")
            # Also handle plain-text "@Members" if present
            t = t.replace("@Members", "")
            # Normalize whitespace / empty lines after removal
            lines = [ln.rstrip() for ln in t.splitlines()]
            # Drop lines that became empty due only to mention removal
            compact: List[str] = []
            for ln in lines:
                if ln.strip() == "":
                    if compact and compact[-1] != "":
                        compact.append("")
                    continue
                compact.append(ln)
            # Trim leading/trailing blank lines
            while compact and compact[0] == "":
                compact.pop(0)
            while compact and compact[-1] == "":
                compact.pop()
            return "\n".join(compact).strip()
        except Exception:
            return (text or "").strip()
    
    async def _send_forwarding_log(self, message: discord.Message, channel_config: Dict[str, Any], success: bool, error: str = None):
        """Send forwarding log to configured logging channel"""
        try:
            log_channel_id = self.config.get("forwarding_logs_channel_id")
            if not log_channel_id:
                return
            
            log_channel = self.bot.get_channel(int(log_channel_id))
            if not log_channel:
                return
            
            source_channel_id = channel_config.get("source_channel_id", "unknown")
            source_channel_name = channel_config.get("source_channel_name", "unknown")
            dest_webhook = channel_config.get("destination_webhook_url", "")
            
            # Get destination info from webhook URL if possible
            dest_info = "Unknown"
            if dest_webhook:
                try:
                    # Extract webhook ID from URL
                    parts = dest_webhook.split("/")
                    if len(parts) >= 2:
                        webhook_id = parts[-2] if parts[-1] else parts[-1]
                        dest_info = f"Webhook {webhook_id[:8]}..."
                except:
                    dest_info = "Webhook configured"
            
            embed = discord.Embed(
                color=discord.Color.green() if success else discord.Color.red(),
                timestamp=datetime.now()
            )
            
            if success:
                embed.title = "✅ Message Forwarded"
                embed.description = f"Successfully forwarded message from `{source_channel_name}`"
            else:
                embed.title = "❌ Forward Failed"
                embed.description = f"Failed to forward message from `{source_channel_name}`"
                if error:
                    embed.add_field(name="Error", value=error[:1024], inline=False)
            
            embed.add_field(name="Source Channel", value=f"`{source_channel_name}`\nID: `{source_channel_id}`", inline=True)
            embed.add_field(name="Destination", value=dest_info, inline=True)
            
            # Message info
            msg_content_preview = message.content[:100] + "..." if message.content and len(message.content) > 100 else (message.content or "[No content]")
            embed.add_field(name="Message", value=msg_content_preview, inline=False)
            
            # Embed count
            embed_count = len(message.embeds) if message.embeds else 0
            if embed_count > 0:
                embed.add_field(name="Embeds", value=str(embed_count), inline=True)
            
            # Attachment count
            attachment_count = len(message.attachments) if message.attachments else 0
            if attachment_count > 0:
                embed.add_field(name="Attachments", value=str(attachment_count), inline=True)
            
            # Author info
            if message.author:
                embed.add_field(name="Author", value=f"{message.author.name}#{message.author.discriminator}\nID: `{message.author.id}`", inline=True)
            
            # Message link
            if message.jump_url:
                embed.add_field(name="Message Link", value=f"[Jump to Message]({message.jump_url})", inline=True)
            
            # Stats
            embed.set_footer(text=f"Total forwarded: {self.stats['messages_forwarded']} | Errors: {self.stats['errors']}")
            
            await log_channel.send(embed=embed)
        except Exception as e:
            # Don't fail forwarding if logging fails
            print(f"{Colors.YELLOW}[Log] Failed to send forwarding log: {e}{Colors.RESET}")
    
    async def forward_message(self, message: discord.Message, channel_id: str, channel_config: Dict[str, Any]):
        """Forward a message to the configured webhook"""
        try:
            webhook_url = channel_config.get("destination_webhook_url", "").strip()
            source_channel_name = channel_config.get("source_channel_name", f"Channel {channel_id}")
            
            if not webhook_url:
                if self.stats['messages_forwarded'] == 0:  # Only warn once
                    print(f"{Colors.YELLOW}[Forward] ⚠️ No webhook configured for channel {source_channel_name} (ID: {channel_id}){Colors.RESET}")
                return
            
            print(f"{Colors.CYAN}[Forward] Forwarding message from {source_channel_name}...{Colors.RESET}")
            
            # Prepare message content
            content = message.content or ""

            # Affiliate rewrite (standalone, same behavior as Instorebotforwarder)
            rewrite_enabled = bool(self.config.get("affiliate_rewrite_enabled", True))
            affiliate_changed = False
            affiliate_notes: Dict[str, str] = {}
            if rewrite_enabled and content:
                content, _changed, _notes = await affiliate_rewriter.rewrite_text(self.config, content)
                if _changed:
                    affiliate_changed = True
                if isinstance(_notes, dict):
                    for k, v in _notes.items():
                        if k and v:
                            affiliate_notes[str(k)] = str(v)

            # Never forward/ping @Members
            if content:
                content = self._strip_members_mentions(content)
            
            # Get channel name for custom titles
            channel_name = channel_config.get("source_channel_name", "Unknown Channel")
            
            # Prepare embeds - convert normal messages to embeds if needed
            embeds_raw = [e.to_dict() for e in message.embeds] if message.embeds else []

            if rewrite_enabled and embeds_raw:
                rewritten_embeds = []
                for e in embeds_raw:
                    ee, _ch, _notes = await affiliate_rewriter.rewrite_embed_dict(self.config, e)
                    if _ch:
                        affiliate_changed = True
                    if isinstance(_notes, dict):
                        for k, v in _notes.items():
                            if k and v:
                                affiliate_notes[str(k)] = str(v)
                    rewritten_embeds.append(ee)
                embeds_raw = rewritten_embeds

            # Human-friendly affiliate signal (helps debug why links didn't change)
            if rewrite_enabled and affiliate_notes:
                try:
                    if affiliate_changed:
                        print(f"{Colors.GREEN}[Affiliate] ✅ Rewrote affiliate links ({len(affiliate_notes)} url(s)) {Colors.RESET}")
                    else:
                        print(f"{Colors.YELLOW}[Affiliate] ↩ No affiliate rewrite ({len(affiliate_notes)} url(s)) {Colors.RESET}")

                    shown = 0
                    limit = 4 if affiliate_changed else 2
                    for u, note in list(affiliate_notes.items()):
                        if shown >= limit:
                            break
                        # `note` may include "reason -> replacement" from affiliate_rewriter.rewrite_text
                        print(f"{Colors.CYAN}[Affiliate] - {u} ({note}){Colors.RESET}")
                        shown += 1
                except Exception:
                    pass
            
            # If no embeds and we have content, create an embed from the message
            if not embeds_raw and content:
                embed = self._create_embed_from_message(content, channel_name, str(message.author) if message.author else None)
                embeds_raw = [embed]
            
            # Apply RS branding to all embeds
            embeds = self._apply_rs_branding(embeds_raw, channel_name)
            
            # Check if we need to add role mention (works for both normal messages and embeds)
            role_mention_text = self._get_role_mention_text(channel_config)
            # Never add @Members even if configured
            if role_mention_text:
                for rid in self._members_role_ids():
                    if f"<@&{rid}>" in role_mention_text:
                        role_mention_text = None
                        break
            # Debug: Log role mention status
            if self.stats['messages_forwarded'] == 0:
                if role_mention_text:
                    print(f"{Colors.CYAN}[Debug] Role mention will be added: {role_mention_text[:50]}...{Colors.RESET}")
                else:
                    print(f"{Colors.CYAN}[Debug] No role mention (text is blank or not configured){Colors.RESET}")
            
            # Prepare attachments - get first image for embed
            attachment_urls = [att.url for att in message.attachments] if message.attachments else []
            first_image_url = None
            if message.attachments:
                # Find first image attachment
                for att in message.attachments:
                    if att.content_type and att.content_type.startswith('image/'):
                        first_image_url = att.url
                        break
                # If no image found but we have attachments, use first one
                if not first_image_url and attachment_urls:
                    first_image_url = attachment_urls[0]
            
            # Add image to embeds if we have one and embed doesn't already have an image
            if first_image_url and embeds:
                # Add image to first embed if it doesn't already have one
                if "image" not in embeds[0] or not embeds[0].get("image", {}).get("url"):
                    embeds[0]["image"] = {"url": first_image_url}
            
            # Build webhook payload
            import requests
            
            brand_name = self.config.get("brand_name", "Reselling Secrets")
            
            # ALWAYS set avatar from RS Server icon (for ALL message types)
            # If icon not loaded yet, try to fetch it now
            if not self.rs_icon_url:
                guild_id = self.config.get("guild_id", 0)
                if guild_id:
                    # Try to get from cached guild first
                    self.rs_guild = self.bot.get_guild(guild_id)
                    if self.rs_guild:
                        self.rs_icon_url = self._get_guild_icon_url(self.rs_guild)
                    # If still not found, try API synchronously
                    if not self.rs_icon_url:
                        # Fetch icon via API (synchronous for this message)
                        import requests
                        try:
                            bot_token = self.config.get("bot_token", "").strip()
                            if bot_token:
                                headers = {
                                    'Authorization': f'Bot {bot_token}',
                                    'User-Agent': 'DiscordBot (RSForwarder)'
                                }
                                response = requests.get(
                                    f'https://discord.com/api/v10/guilds/{guild_id}?with_counts=false',
                                    headers=headers,
                                    timeout=5
                                )
                                if response.status_code == 200:
                                    guild_data = response.json()
                                    icon_hash = guild_data.get("icon")
                                    if icon_hash:
                                        ext = "gif" if str(icon_hash).startswith("a_") else "png"
                                        self.rs_icon_url = f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}?size=256"
                                        print(f"{Colors.GREEN}[Forward] Fetched RS icon via API: {self.rs_icon_url[:50]}...{Colors.RESET}")
                        except Exception as e:
                            if self.stats['messages_forwarded'] == 0:
                                print(f"{Colors.YELLOW}[Warn] Could not fetch RS icon: {e}{Colors.RESET}")
            
            # Build payload - ALWAYS set username and avatar
            payload = {
                "username": brand_name,  # ALWAYS override with RS branding
            }
            
            # ALWAYS set avatar if we have it
            if self.rs_icon_url:
                payload["avatar_url"] = self.rs_icon_url
                if self.stats['messages_forwarded'] <= 1:  # Log first few times
                    print(f"{Colors.GREEN}[Forward] Using RS icon: {self.rs_icon_url[:50]}...{Colors.RESET}")
            else:
                # Log warning if icon still not available
                if self.stats['messages_forwarded'] == 0:
                    print(f"{Colors.RED}[Warn] RS Server icon not available - webhook will use default avatar{Colors.RESET}")
                    print(f"{Colors.YELLOW}[Warn] Check that bot is in RS Server (ID: {self.config.get('guild_id', 0)}){Colors.RESET}")
            
            # Build content with role mention if needed
            # Since we're converting everything to embeds, role mention goes in content before embed
            final_content_parts = []
            
            # Add role mention if configured (appears before embed)
            if role_mention_text:
                final_content_parts.append(role_mention_text)
            
            # If the original message had content and embeds, preserve the content too (forward full message).
            if content and embeds:
                final_content_parts.append(content)

            # Only add content if we have anything to post in content (role mention and/or original content)
            if final_content_parts:
                
                # Combine content
                final_content = "\n".join(final_content_parts) if final_content_parts else None
                
                # Truncate if too long
                if final_content and len(final_content) > 2000:
                    final_content = final_content[:1997] + "..."
                
                # Add content if present
                if final_content:
                    payload["content"] = final_content
            
            # Add embeds (max 10)
            if embeds:
                payload["embeds"] = embeds[:10]
                # Debug: Log embed titles (first message only)
                if self.stats['messages_forwarded'] == 0:
                    for i, embed in enumerate(embeds[:3]):  # Log first 3 embeds
                        embed_title = embed.get("title", "[No title]")
                        embed_footer = embed.get("footer", {}).get("text", "[No footer]")
                        print(f"{Colors.CYAN}[Debug] Embed {i+1} title: '{embed_title}' | footer: '{embed_footer[:50]}...'{Colors.RESET}")
            
            # Add attachment URLs to content if no embeds and content is short
            if attachment_urls and not embeds and final_content and len(final_content) + sum(len(url) for url in attachment_urls[:5]) < 2000:
                payload["content"] = final_content + "\n" + "\n".join(attachment_urls[:5])
            elif attachment_urls and not embeds and not final_content:
                payload["content"] = "\n".join(attachment_urls[:5])
            
            # Ensure at least content or embeds
            if not payload.get("content") and not payload.get("embeds"):
                payload["content"] = "[Message forwarded from RS Server]"
            
            # Debug: Log payload (first message only)
            if self.stats['messages_forwarded'] == 0:
                print(f"{Colors.CYAN}[Debug] Payload username: {payload.get('username')}{Colors.RESET}")
                print(f"{Colors.CYAN}[Debug] Payload avatar_url: {payload.get('avatar_url', 'NOT SET')[:50] if payload.get('avatar_url') else 'NOT SET'}{Colors.RESET}")
            
            # Send to webhook
            response = requests.post(webhook_url, json=payload, timeout=10)
            
            if response.status_code in [200, 204]:
                self.stats['messages_forwarded'] += 1
                channel_name = channel_config.get("source_channel_name", channel_id)
                embed_count = len(embeds)
                role_mention = " (with role mention)" if role_mention_text else ""
                print(f"{Colors.GREEN}[Forward] ✓ {channel_name} → {len(content)} chars, {embed_count} embed(s){role_mention}{Colors.RESET}")
                
                # Send forwarding log
                await self._send_forwarding_log(message, channel_config, success=True)
            else:
                self.stats['errors'] += 1
                error_msg = f"{response.status_code}: {response.text[:200]}"
                print(f"{Colors.RED}[Forward] ✗ Error {error_msg}{Colors.RESET}")
                
                # Send forwarding log with error
                await self._send_forwarding_log(message, channel_config, success=False, error=error_msg)
                
        except Exception as e:
            self.stats['errors'] += 1
            error_msg = str(e)
            print(f"{Colors.RED}[Forward] Exception: {error_msg}{Colors.RESET}")
            
            # Send forwarding log with exception
            await self._send_forwarding_log(message, channel_config, success=False, error=error_msg)
            
            import traceback
            if "--verbose" in sys.argv:
                traceback.print_exc()
    
    async def start(self):
        """Start the bot"""
        bot_token = self.config.get("bot_token", "").strip()
        if not bot_token:
            print(f"{Colors.RED}[Bot] ERROR: bot_token is required in config.secrets.json (server-only){Colors.RESET}")
            return
        
        try:
            await self.bot.start(bot_token)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}[Bot] Shutting down...{Colors.RESET}")
            await self.bot.close()
            print(f"{Colors.CYAN}Stats:{Colors.RESET}")
            print(f"  Messages forwarded: {self.stats['messages_forwarded']}")
            print(f"  Errors: {self.stats['errors']}")


def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--check-config", action="store_true", help="Validate config + secrets and exit (no Discord connection).")
    args = parser.parse_args()

    if args.check_config:
        base = Path(__file__).parent
        cfg, config_path, secrets_path = load_config_with_secrets(base)
        token = (cfg.get("bot_token") or "").strip()
        errors: List[str] = []
        if not secrets_path.exists():
            errors.append(f"Missing secrets file: {secrets_path}")
        if is_placeholder_secret(token):
            errors.append("bot_token missing/placeholder in config.secrets.json")

        channels = cfg.get("channels") or []
        webhooks = cfg.get("destination_webhooks") or {}
        if channels and not isinstance(webhooks, dict):
            errors.append("destination_webhooks must be an object in config.secrets.json")
        else:
            missing = []
            for ch in channels:
                src = str((ch or {}).get("source_channel_id", "")).strip()
                if src and not (webhooks or {}).get(src):
                    missing.append(src)
            if missing:
                errors.append(f"Missing destination_webhooks entries for source_channel_id(s): {', '.join(missing[:5])}")

        if errors:
            print(f"{Colors.RED}[ConfigCheck] FAILED{Colors.RESET}")
            for e in errors:
                print(f"- {e}")
            return

        print(f"{Colors.GREEN}[ConfigCheck] OK{Colors.RESET}")
        print(f"- config: {config_path}")
        print(f"- secrets: {secrets_path}")
        print(f"- bot_token: {mask_secret(token)}")
        print(f"- channels: {len(channels)}")
        return

    bot = RSForwarderBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[Bot] Stopped{Colors.RESET}")


if __name__ == '__main__':
    main()

