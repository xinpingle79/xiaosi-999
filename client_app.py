import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
import ctypes
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
import tkinter as tk
import yaml
from tkinter import messagebox, ttk

APP_NAME = "小四客户端"
DEFAULT_SERVER_URL = "http://154.64.255.139:43090"
CLIENT_ERROR_I18N = {
    "invalid_activation_code": "激活码无效，请确认后重新输入。",
    "missing_activation_code": "请输入激活码后再验证。",
    "missing_agent_token": "本地凭证不存在，请重新输入激活码完成验证。",
    "invalid_token": "本地凭证已失效，请重新输入激活码完成验证。",
    "token_expired": "本地凭证已过期，请重新输入激活码完成验证。",
    "account_expired": "账户已到期，请联系管理员续费或更换有效激活码。",
    "account_disabled": "账户已禁用，请联系管理员处理。",
    "device_limit_reached": "设备数量已达上限，请联系管理员处理。",
    "agent_not_registered": "执行端尚未注册，请稍后重试。",
    "unauthorized": "当前登录状态无效，请重新激活。",
}
CLIENT_REACTIVATION_ERRORS = {
    "account_expired",
    "account_disabled",
    "invalid_token",
    "token_expired",
    "missing_agent_token",
    "agent_not_registered",
    "unauthorized",
}


def _find_root_dir():
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "web_ui.py").exists() or (parent / "agent" / "worker.py").exists():
            return parent
    return current.parent


ROOT_DIR = _find_root_dir()
ENV_RUNTIME_DIR = "FB_RPA_RUNTIME_DIR"
DEFAULT_BIT_API = ""
STATUS_REFRESH_MS = 3000


def _user_data_root() -> Path:
    local_appdata = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
    if local_appdata:
        return Path(local_appdata) / APP_NAME
    return Path.home() / "AppData" / "Local" / APP_NAME


def _client_config_path() -> Path:
    if getattr(sys, "frozen", False):
        return _user_data_root() / "config" / "client.yaml"
    return ROOT_DIR / "config" / "client.yaml"


CLIENT_CONFIG_PATH = _client_config_path()


def _messages_config_path() -> Path:
    if getattr(sys, "frozen", False):
        return _user_data_root() / "config" / "messages.yaml"
    return ROOT_DIR / "config" / "messages.yaml"


MESSAGES_CONFIG_PATH = _messages_config_path()


def _bundled_messages_config_path() -> Path:
    return ROOT_DIR / "config" / "messages.yaml"


def _default_machine_id() -> str:
    hostname = socket.gethostname()
    node = uuid.getnode()
    return f"{hostname}-{node:x}"


def _default_client_config() -> dict:
    return {
        "client_version": "1.0.0",
        "runtime_dir": "",
        "server_url": DEFAULT_SERVER_URL,
        "machine_id": _default_machine_id(),
        "agent_token": "",
        "token_expires_at": None,
        "bit_api": DEFAULT_BIT_API,
        "api_token": "",
        "bitbrowser_cdp_connect_retry_count": 120,
        "bitbrowser_cdp_connect_retry_delay": 0.6,
        "bitbrowser_cdp_refresh_every": 6,
        "browser": {
            "auto_discover_bitbrowser": True,
        },
        "window_no": "",
        "task_settings": {
            "max_messages_per_account": 0,
            "scroll_times": 0,
            "cursor_require_found": True,
            "group_list_scroll_times": 0,
            "idle_scroll_limit": 3,
            "account_restricted_blocked_threshold": 2,
            "account_restricted_signal_threshold": 3,
            "speed_factor": 0.85,
            "stranger_limit_cache_ms": 1200,
            "send_interval_seconds": [0, 0],
            "max_windows": 0,
            "resume_search_times": 20,
        },
    }


def _sanitize_client_config(data: dict) -> dict:
    cleaned = _deep_merge(_default_client_config(), data or {})
    cleaned["server_url"] = _normalize_server_url(cleaned.get("server_url") or DEFAULT_SERVER_URL)
    cleaned["machine_id"] = str(cleaned.get("machine_id") or _default_machine_id()).strip()
    cleaned["bit_api"] = _normalize_bit_api(cleaned.get("bit_api") or DEFAULT_BIT_API)
    cleaned["api_token"] = str(cleaned.get("api_token") or "").strip()
    cleaned["agent_token"] = str(cleaned.get("agent_token") or "").strip()
    cleaned["window_no"] = str(cleaned.get("window_no") or "").strip()
    cleaned.pop("message_templates", None)
    task_settings = cleaned.get("task_settings")
    if isinstance(task_settings, dict):
        for key in (
            "fast_message_button_only",
            "force_hover_card",
            "fast_action_timeout_ms",
            "fast_visible_timeout_ms",
            "fast_hover_sleep_ms",
            "fast_retry_sleep_ms",
            "quick_no_button_precheck",
            "typing_delay",
        ):
            task_settings.pop(key, None)
    if cleaned.get("token_expires_at") in ("", []):
        cleaned["token_expires_at"] = None
    for key in ("activation_code", "register_token", "owner", "expire_at", "max_devices"):
        cleaned.pop(key, None)
    return cleaned


def _should_migrate_server_url(server_url: str) -> bool:
    normalized = _normalize_server_url(server_url)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    host = str(parsed.hostname or "").strip().lower()
    port = parsed.port
    if port is None:
        port = 80 if parsed.scheme == "http" else 443 if parsed.scheme == "https" else None
    return parsed.scheme == "http" and port == 43090 and host in {"127.0.0.1", "localhost"}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_message_templates(templates) -> list:
    return [
        str(item).strip()
        for item in (templates or [])
        if str(item).strip()
    ]


def load_messages_config() -> dict:
    if not MESSAGES_CONFIG_PATH.exists() and getattr(sys, "frozen", False):
        bundled_path = _bundled_messages_config_path()
        if bundled_path.exists():
            try:
                bundled_loaded = yaml.safe_load(bundled_path.read_text(encoding="utf-8")) or {}
            except Exception:
                bundled_loaded = {}
            payload = {
                "probe_templates": _normalize_message_templates(bundled_loaded.get("probe_templates") or []),
                "templates": _normalize_message_templates(bundled_loaded.get("templates") or []),
            }
            try:
                MESSAGES_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
                MESSAGES_CONFIG_PATH.write_text(
                    yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
                    encoding="utf-8",
                )
            except Exception:
                pass
    if not MESSAGES_CONFIG_PATH.exists():
        return {}
    try:
        loaded = yaml.safe_load(MESSAGES_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return {
        "probe_templates": _normalize_message_templates(loaded.get("probe_templates") or []),
        "templates": _normalize_message_templates(loaded.get("templates") or []),
    }


def save_messages_config(templates=None, probe_templates=None) -> None:
    current = load_messages_config()
    payload = {
        "probe_templates": _normalize_message_templates(current.get("probe_templates") or []),
        "templates": _normalize_message_templates(current.get("templates") or []),
    }
    if probe_templates is not None:
        payload["probe_templates"] = _normalize_message_templates(probe_templates)
    if templates is not None:
        payload["templates"] = _normalize_message_templates(templates)
    MESSAGES_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    MESSAGES_CONFIG_PATH.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _migrate_legacy_message_templates(loaded: dict) -> bool:
    if "message_templates" not in loaded:
        return False
    legacy_templates = _normalize_message_templates(loaded.get("message_templates") or [])
    if not legacy_templates:
        return False
    current_messages = load_messages_config()
    if current_messages.get("templates"):
        return False
    save_messages_config(
        templates=legacy_templates,
        probe_templates=current_messages.get("probe_templates") or [],
    )
    return True


def load_client_config() -> dict:
    config = _default_client_config()
    if not CLIENT_CONFIG_PATH.exists():
        return config
    try:
        loaded = yaml.safe_load(CLIENT_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return config
    if not isinstance(loaded, dict):
        return config
    migrated_legacy_templates = _migrate_legacy_message_templates(loaded)
    merged = _sanitize_client_config(_deep_merge(config, loaded))
    should_rewrite = False
    if _should_migrate_server_url(loaded.get("server_url") or ""):
        merged["server_url"] = DEFAULT_SERVER_URL
        should_rewrite = True
    if "message_templates" in loaded or migrated_legacy_templates:
        should_rewrite = True
    if any(key in loaded for key in ("activation_code", "register_token", "owner", "expire_at", "max_devices")):
        should_rewrite = True
    if should_rewrite:
        try:
            CLIENT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            CLIENT_CONFIG_PATH.write_text(
                yaml.safe_dump(merged, allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )
        except Exception:
            pass
    return merged


def save_client_config(data: dict) -> None:
    cleaned = _sanitize_client_config(data)
    CLIENT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CLIENT_CONFIG_PATH.write_text(
        yaml.safe_dump(cleaned, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _runtime_dir() -> Path:
    client_cfg = load_client_config()
    runtime_root = (os.environ.get(ENV_RUNTIME_DIR) or "").strip()
    if not runtime_root:
        runtime_root = str(client_cfg.get("runtime_dir") or "").strip()
    if runtime_root:
        base = Path(runtime_root).expanduser()
    else:
        base = _user_data_root() / "runtime"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _runtime_path(*parts: str) -> Path:
    return _runtime_dir().joinpath(*parts)


def _license_path() -> Path:
    return _runtime_path("license.json")


def _sanitize_owner(owner):
    if not owner:
        return ""
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(owner))


def _worker_pid_path() -> Path:
    return _runtime_path("data", "worker.pid")


def _stop_flag_path(owner: str) -> Path:
    base = "stop_default.flag"
    if owner:
        base = f"stop_{_sanitize_owner(owner)}.flag"
    return _runtime_path("data", base)


def _pause_flag_path(owner: str) -> Path:
    base = "pause_default.flag"
    if owner:
        base = f"pause_{_sanitize_owner(owner)}.flag"
    return _runtime_path("data", base)


def load_license() -> dict:
    path = _license_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    cleaned = {
        "owner": data.get("owner") or "",
        "expire_at": data.get("expire_at"),
        "max_devices": data.get("max_devices"),
    }
    if cleaned != data:
        try:
            save_license(cleaned)
        except Exception:
            pass
    return cleaned


def save_license(data: dict) -> None:
    path = _license_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = {
        "owner": (data or {}).get("owner") or "",
        "expire_at": (data or {}).get("expire_at"),
        "max_devices": (data or {}).get("max_devices"),
    }
    path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_server_url(server_url: str) -> str:
    return (server_url or "").strip().rstrip("/")


def _normalize_bit_api(bit_api: str) -> str:
    return (bit_api or "").strip().rstrip("/")


def _parse_int(value, fallback):
    try:
        return int(str(value).strip())
    except Exception:
        return fallback


def _translate_client_error(error_value):
    text = str(error_value or "").strip()
    if not text:
        return "请求失败"
    if any("\u4e00" <= ch <= "\u9fff" for ch in text):
        return text
    return CLIENT_ERROR_I18N.get(text, text)


def _parse_expire_at(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        pass
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).timestamp()
        except Exception:
            continue
    return None


def _format_expire_at(value):
    ts = _parse_expire_at(value)
    if ts is None:
        raw = str(value or "").strip()
        return raw or "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _license_is_expired(license_data):
    ts = _parse_expire_at((license_data or {}).get("expire_at"))
    return ts is not None and time.time() >= ts


def _token_is_expired(value):
    ts = _parse_expire_at(value)
    return ts is not None and time.time() >= ts


def _client_can_use_token(client_config, license_data):
    agent_token = str((client_config or {}).get("agent_token") or "").strip()
    if not agent_token:
        return False
    if _token_is_expired((client_config or {}).get("token_expires_at")):
        return False
    if _license_is_expired(license_data):
        return False
    return True


class ClientApp:
    WINDOW_WIDTH = 680
    WINDOW_HEIGHT = 560
    TITLE_BAR_HEIGHT = 38
    CONTENT_PAD_X = 8
    CONTENT_PAD_TOP = 8
    CONTENT_PAD_BOTTOM = 10
    PANEL_SPACING = 3
    LOCAL_FIELD_WIDTH = 190
    API_FIELD_WIDTH = 250
    INTERVAL_STACK_WIDTH = 150
    COUNT_FIELD_WIDTH = 46
    WINDOW_NO_FIELD_WIDTH = 64
    PARAM_LEFT_GROUP_WIDTH = 280
    PARAM_RIGHT_GROUP_WIDTH = 308
    PARAM_COLUMN_GAP = 12
    RIGHT_MAIN_LABEL_WIDTH = 64
    ACTION_LOCK_SECONDS = {
        "run": 4.0,
        "stop": 3.0,
        "pause": 3.0,
        "resume": 3.0,
        "single": 4.0,
    }
    SHELL_BG = "#edf4ff"
    WINDOW_BG = "#dbe7f6"
    PANEL_BG = "#eef5ff"
    BORDER_COLOR = "#8fb1e3"
    TITLE_BG = "#315f97"
    TITLE_FG = "#ffffff"
    ACTIVATION_BG = "#fbfdff"
    ACTIVATION_GLOW = "#eef6ff"
    ACTIVATION_LINE = "#d8e8ff"
    ACTIVATION_GRID = "#f2f8ff"
    TITLE_FONT = ("Microsoft YaHei", 14, "bold")
    TOOLBAR_FONT = ("Microsoft YaHei", 11, "bold")
    HEADER_FONT = ("Microsoft YaHei", 12, "bold")
    LABEL_FONT = ("Microsoft YaHei", 12, "bold")
    SECTION_LABEL_FONT = ("Microsoft YaHei", 11, "bold")
    SECTION_TITLE_FONT = ("Microsoft YaHei", 12, "bold")
    META_FONT = ("Microsoft YaHei", 10)
    TEXT_FONT = ("Consolas", 10)
    MESSAGE_FONT = ("Consolas", 11)
    NOTICE_FONT = ("Microsoft YaHei", 9)
    SESSION_NOTICE_TEXT = "激活码已失效请重新激活登录使用"

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("FB私聊助手 Pro")
        self.root.configure(bg=self.WINDOW_BG)
        self.root.overrideredirect(True)
        self.root.geometry(self._centered_geometry(self.WINDOW_WIDTH, self.WINDOW_HEIGHT))
        self.root.resizable(False, False)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.bind("<Map>", self._handle_window_map)

        self.client_config = load_client_config()
        self.license_data = load_license()
        self.machine_id = str(
            self.client_config.get("machine_id") or _default_machine_id()
        ).strip()
        self.server_url = _normalize_server_url(
            self.client_config.get("server_url") or DEFAULT_SERVER_URL
        )
        self.worker_process = None
        self.status_job = None
        self.log_job = None
        self.verified = False
        self.activation_screen = None
        self.ops_screen = None
        self._drag_origin = None
        self._is_minimizing = False
        self._is_maximized = False
        self._normal_geometry = self.root.geometry()
        self._button_texts = {}
        self._button_restore_modes = {}
        self._window_button_styles = {}
        self._action_button_styles = {}
        self._action_lock_until = {}
        self._taskbar_style_ready = False
        self._button_state_snapshot = {
            "agent_online": False,
            "task_running": False,
            "task_pending": False,
            "task_paused": False,
            "config_synced": False,
            "device_bound": False,
            "start_ready": False,
            "start_block_reason": "",
        }
        self._log_path = _runtime_path("logs", "worker.log")
        self._log_offsets = {}
        self._log_follow_tail = True

        self.activation_code = tk.StringVar(value="")
        self.bit_api = tk.StringVar(
            value=_normalize_bit_api(self.client_config.get("bit_api") or DEFAULT_BIT_API)
        )
        self.api_token = tk.StringVar(
            value=str(self.client_config.get("api_token") or "").strip()
        )
        self.activation_notice_text = tk.StringVar(value="")
        self.window_count_text = tk.StringVar(value=self._initial_window_count())
        self.window_no_text = tk.StringVar(value=str(self.client_config.get("window_no") or ""))
        self.sent_count_text = tk.StringVar(value="0")
        self.failed_count_text = tk.StringVar(value="0")
        self.maximize_button_text = tk.StringVar(value="□")
        self.activation_notice_label = None
        self.log_text = None
        self.message_text = None
        self.verify_btn = None
        self.single_run_btn = None
        self.save_btn = None

        self._build_ui()
        self._reset_log_session()
        self._load_message_templates()
        self._load_existing_state()
        self.root.after(0, self._initialize_window_shell)
        self._schedule_status_refresh(initial=True)
        self._schedule_log_refresh(initial=True)

    def _runtime_owner(self):
        return str((self.license_data or {}).get("owner") or "").strip()

    def _remove_runtime_marker(self, path: Path):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

    def _clear_runtime_control_flags(self):
        owner = self._runtime_owner()
        self._remove_runtime_marker(_stop_flag_path(owner))
        self._remove_runtime_marker(_pause_flag_path(owner))

    def _read_worker_pid(self):
        path = _worker_pid_path()
        if not path.exists():
            return None
        try:
            value = int(str(path.read_text(encoding="utf-8")).strip())
        except Exception:
            self._remove_runtime_marker(path)
            return None
        return value if value > 0 else None

    def _write_worker_pid(self, pid):
        path = _worker_pid_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(str(int(pid)), encoding="utf-8")
        except Exception:
            pass

    def _clear_worker_pid(self):
        self._remove_runtime_marker(_worker_pid_path())

    def _is_pid_running(self, pid):
        if not pid:
            return False
        try:
            os.kill(int(pid), 0)
        except OSError:
            return False
        except Exception:
            return False
        return True

    def _terminate_pid(self, pid, wait_seconds=6.0):
        if not pid:
            return False
        pid = int(pid)
        if not self._is_pid_running(pid):
            return True
        if sys.platform.startswith("win"):
            create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=create_no_window,
                )
            except Exception:
                pass
            deadline = time.time() + max(wait_seconds, 1.0)
            while time.time() < deadline:
                if not self._is_pid_running(pid):
                    return True
                time.sleep(0.2)
            return not self._is_pid_running(pid)
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        deadline = time.time() + max(wait_seconds, 1.0)
        while time.time() < deadline:
            if not self._is_pid_running(pid):
                return True
            time.sleep(0.2)
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass
        time.sleep(0.2)
        return not self._is_pid_running(pid)

    def _cleanup_stale_worker_process(self):
        pid = self._read_worker_pid()
        if not pid:
            return
        active_pid = None
        if self.worker_process and self.worker_process.poll() is None:
            active_pid = self.worker_process.pid
        if active_pid == pid:
            return
        if self._is_pid_running(pid):
            self._terminate_pid(pid)
        self._clear_worker_pid()

    def _wait_for_remote_task_clear(self, timeout=8.0):
        if not self.verified or not self._current_agent_token():
            return False
        deadline = time.time() + max(timeout, 0.5)
        while time.time() < deadline:
            info = self.refresh_remote_status(silent=True)
            if info and not info.get("task_running") and not info.get("task_pending"):
                return True
            time.sleep(0.5)
        return False

    def _shutdown_worker_runtime(self, send_stop=True):
        target_pids = []
        if self.worker_process and self.worker_process.poll() is None:
            target_pids.append(self.worker_process.pid)
        worker_pid = self._read_worker_pid()
        if worker_pid and worker_pid not in target_pids:
            target_pids.append(worker_pid)
        local_worker_alive = any(self._is_pid_running(pid) for pid in target_pids)
        stop_sent = False
        if send_stop and local_worker_alive and self.verified and self._current_agent_token():
            payload = {
                "agent_token": self._current_agent_token(),
                "machine_id": self.machine_id,
            }
            ok, _data = self._post_client_api("/api/client/stop", payload, timeout=10)
            stop_sent = bool(ok)
        if stop_sent:
            self._wait_for_remote_task_clear(timeout=8.0)
            deadline = time.time() + 4.0
            while time.time() < deadline:
                if not any(self._is_pid_running(pid) for pid in target_pids):
                    break
                time.sleep(0.2)
        for pid in target_pids:
            self._terminate_pid(pid)
        self.worker_process = None
        self._clear_worker_pid()
        self._clear_runtime_control_flags()
        self._apply_button_state(agent_online=False, task_running=False, task_pending=False)

    def _centered_geometry(self, width, height):
        self.root.update_idletasks()
        screen_w = max(self.root.winfo_screenwidth(), width)
        screen_h = max(self.root.winfo_screenheight(), height)
        pos_x = max((screen_w - width) // 2, 0)
        pos_y = max((screen_h - height) // 2, 0)
        return f"{width}x{height}+{pos_x}+{pos_y}"

    def _initialize_window_shell(self):
        self._apply_window_icon()
        self._ensure_taskbar_window(refresh_shell=True)

    def _apply_window_icon(self):
        if not sys.platform.startswith("win"):
            return
        candidates = [
            ROOT_DIR / "assets" / "fb_chat_helper.ico",
            ROOT_DIR / "fb_chat_helper.ico",
        ]
        for path in candidates:
            if not path.exists():
                continue
            try:
                self.root.iconbitmap(default=str(path))
                return
            except Exception:
                continue

    def _ensure_taskbar_window(self, refresh_shell=False):
        if not sys.platform.startswith("win"):
            return
        try:
            self.root.update_idletasks()
            hwnd = ctypes.windll.user32.GetParent(self.root.winfo_id()) or self.root.winfo_id()
            GWL_EXSTYLE = -20
            WS_EX_APPWINDOW = 0x00040000
            WS_EX_TOOLWINDOW = 0x00000080
            SWP_NOMOVE = 0x0002
            SWP_NOSIZE = 0x0001
            SWP_NOZORDER = 0x0004
            SWP_NOACTIVATE = 0x0010
            SWP_FRAMECHANGED = 0x0020

            style = ctypes.windll.user32.GetWindowLongW(hwnd, GWL_EXSTYLE)
            desired_style = (style & ~WS_EX_TOOLWINDOW) | WS_EX_APPWINDOW
            if desired_style != style:
                ctypes.windll.user32.SetWindowLongW(hwnd, GWL_EXSTYLE, desired_style)
                ctypes.windll.user32.SetWindowPos(
                    hwnd,
                    0,
                    0,
                    0,
                    0,
                    0,
                    SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER | SWP_NOACTIVATE | SWP_FRAMECHANGED,
                )
            if refresh_shell and not self._taskbar_style_ready:
                self._taskbar_style_ready = True
                self.root.withdraw()
                self.root.after(30, self.root.deiconify)
        except Exception:
            pass

    def _initial_window_count(self):
        raw = _parse_int(
            (self.client_config.get("task_settings") or {}).get("max_windows") or 10,
            10,
        )
        return str(raw if raw > 0 else 10)

    def _build_ui(self):
        self.viewport = tk.Frame(self.root, bg=self.WINDOW_BG)
        self.viewport.pack(fill="both", expand=True)

        self.shell = tk.Frame(
            self.viewport,
            bg=self.SHELL_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            bd=0,
            width=self.WINDOW_WIDTH,
            height=self.WINDOW_HEIGHT,
        )
        self.shell.place(relx=0.5, rely=0.5, anchor="center")
        self.shell.pack_propagate(False)

        self._build_title_bar(self.shell)

        self.screen_host = tk.Frame(self.shell, bg=self.SHELL_BG)
        self.screen_host.pack(fill="both", expand=True, padx=4, pady=(0, 4))
        self.screen_host.pack_propagate(False)

        self._build_activation_screen(self.screen_host)
        self._build_operation_screen(self.screen_host)

    def _build_title_bar(self, parent):
        title_bar = tk.Frame(parent, bg=self.TITLE_BG, height=self.TITLE_BAR_HEIGHT)
        title_bar.pack(fill="x")
        title_bar.pack_propagate(False)
        self.title_bar = title_bar

        title_label = tk.Label(
            title_bar,
            text="FB私聊助手 Pro",
            font=self.TITLE_FONT,
            bg=self.TITLE_BG,
            fg=self.TITLE_FG,
        )
        title_label.pack(side="left", padx=10)
        self.title_label = title_label

        button_frame = tk.Frame(title_bar, bg=self.TITLE_BG)
        button_frame.pack(side="right", padx=10, pady=9)

        self.minimize_btn = self._create_window_button(
            button_frame,
            "—",
            self._minimize_window,
            bg="#5f86b8",
            activebg="#4f74a5",
            pressbg="#3f6594",
        )
        self.minimize_btn.pack(side="left", padx=(0, 6))
        self.maximize_btn = self._create_window_button(
            button_frame,
            self.maximize_button_text,
            self._toggle_maximize,
            bg="#5f86b8",
            activebg="#4f74a5",
            pressbg="#3f6594",
        )
        self.maximize_btn.pack(side="left", padx=(0, 6))
        self.close_btn = self._create_window_button(
            button_frame,
            "✕",
            self._on_close,
            bg="#e56b6f",
            activebg="#d9485f",
            pressbg="#bf344d",
        )
        self.close_btn.pack(side="left")

        for widget in (title_bar, title_label):
            widget.bind("<ButtonPress-1>", self._start_window_drag)
            widget.bind("<B1-Motion>", self._drag_window)
            widget.bind("<Double-Button-1>", lambda _event: self._toggle_maximize())

    def _create_window_button(self, parent, text, command, bg="#5f86b8", activebg="#4f74a5", pressbg=None):
        textvariable = text if isinstance(text, tk.StringVar) else None
        btn = tk.Button(
            parent,
            text="" if textvariable else text,
            textvariable=textvariable,
            command=command,
            width=2,
            height=1,
            font=("Microsoft YaHei", 9, "bold"),
            bg=bg,
            fg="#ffffff",
            activebackground=activebg,
            activeforeground="#ffffff",
            relief="flat",
            bd=0,
            padx=0,
            pady=0,
            cursor="hand2",
        )
        self._window_button_styles[btn] = {
            "bg": bg,
            "hoverbg": activebg,
            "pressbg": pressbg or activebg,
        }
        btn.bind("<Enter>", lambda _event, target=btn: self._set_window_button_visual(target, "hover"))
        btn.bind("<Leave>", lambda _event, target=btn: self._set_window_button_visual(target, "normal"))
        btn.bind("<ButtonPress-1>", lambda _event, target=btn: self._set_window_button_visual(target, "press"))
        btn.bind("<ButtonRelease-1>", lambda event, target=btn: self._restore_window_button_visual(target, event))
        return btn

    def _set_window_button_visual(self, button, state):
        styles = self._window_button_styles.get(button)
        if not styles:
            return
        bg = styles["bg"]
        if state == "hover":
            bg = styles["hoverbg"]
        elif state == "press":
            bg = styles["pressbg"]
        button.configure(bg=bg, activebackground=bg)

    def _restore_window_button_visual(self, button, event):
        inside = (
            0 <= event.x <= button.winfo_width()
            and 0 <= event.y <= button.winfo_height()
        )
        self._set_window_button_visual(button, "hover" if inside else "normal")

    def _start_window_drag(self, event):
        if self._is_maximized:
            return
        self._drag_origin = (event.x_root - self.root.winfo_x(), event.y_root - self.root.winfo_y())

    def _drag_window(self, event):
        if self._is_maximized or not self._drag_origin:
            return
        offset_x, offset_y = self._drag_origin
        self.root.geometry(f"+{event.x_root - offset_x}+{event.y_root - offset_y}")

    def _handle_window_map(self, _event=None):
        if self._is_minimizing:
            self._is_minimizing = False
            self.root.after(10, self._restore_custom_window_chrome)
        elif not self.root.overrideredirect():
            self.root.after(10, self._restore_custom_window_chrome)

    def _restore_custom_window_chrome(self):
        self.root.overrideredirect(True)
        self._ensure_taskbar_window(refresh_shell=False)

    def _minimize_window(self):
        self._is_minimizing = True
        self.root.overrideredirect(False)
        self.root.iconify()

    def _toggle_maximize(self):
        if self._is_maximized:
            self._restore_window()
        else:
            self._maximize_window()

    def _maximize_window(self):
        self._normal_geometry = self.root.geometry()
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        self.root.geometry(f"{screen_w}x{screen_h}+0+0")
        self._is_maximized = True
        self.maximize_button_text.set("❐")

    def _restore_window(self):
        self.root.geometry(self._normal_geometry)
        self._is_maximized = False
        self.maximize_button_text.set("□")

    def _lock_action(self, key, seconds=None):
        if not key:
            return
        duration = float(seconds if seconds is not None else self.ACTION_LOCK_SECONDS.get(key, 2.5))
        self._action_lock_until[key] = time.monotonic() + max(duration, 0.5)

    def _unlock_action(self, key):
        if not key:
            return
        self._action_lock_until.pop(key, None)

    def _prune_action_locks(self):
        now = time.monotonic()
        expired = [key for key, deadline in self._action_lock_until.items() if now >= float(deadline or 0.0)]
        for key in expired:
            self._action_lock_until.pop(key, None)

    def _is_action_locked(self, key):
        self._prune_action_locks()
        return time.monotonic() < float(self._action_lock_until.get(key) or 0.0)

    def _build_activation_screen(self, parent):
        self.activation_screen = tk.Frame(parent, bg=self.SHELL_BG)
        self.activation_screen.place(x=0, y=0, relwidth=1, relheight=1)

        card = tk.Frame(
            self.activation_screen,
            bg=self.ACTIVATION_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            bd=0,
        )
        card.pack(fill="both", expand=True, padx=6, pady=6)

        stage = tk.Frame(card, bg=self.ACTIVATION_BG)
        stage.pack(fill="both", expand=True, padx=24, pady=(20, 28))
        self._build_activation_backdrop(stage, mode="body")

        content = tk.Frame(stage, bg=self.ACTIVATION_BG)
        content.pack(expand=True)

        tk.Label(
            content,
            text="FB私聊助手",
            font=("Microsoft YaHei", 28, "bold"),
            bg=self.ACTIVATION_BG,
            fg="#f2483d",
        ).pack(pady=(24, 30))

        form_block = tk.Frame(content, bg=self.ACTIVATION_BG)
        form_block.pack()

        tk.Label(
            form_block,
            text="激活码",
            font=("Microsoft YaHei", 15, "bold"),
            bg=self.ACTIVATION_BG,
            fg="#24416e",
        ).pack(fill="x", pady=(0, 10))

        entry_row = tk.Frame(form_block, bg=self.ACTIVATION_BG)
        entry_row.pack(anchor="w")

        self.code_entry = tk.Entry(
            entry_row,
            textvariable=self.activation_code,
            font=("Microsoft YaHei", 15),
            relief="solid",
            bd=1,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            highlightcolor="#5a8fe3",
            bg="#ffffff",
            fg="#24416e",
            width=24,
        )
        self.code_entry.pack(side="left", ipady=8)
        self.code_entry.bind("<Return>", lambda _event: self._trigger_verify())

        self.verify_btn = tk.Button(
            entry_row,
            text="验证激活",
            font=("Microsoft YaHei", 16, "bold"),
            bg="#4f7df0",
            fg="#ffffff",
            activebackground="#3f6de0",
            activeforeground="#ffffff",
            relief="raised",
            bd=1,
            width=8,
            cursor="hand2",
            command=self._trigger_verify,
            padx=0,
            pady=3,
        )
        self.verify_btn.pack(side="left", padx=(16, 0), ipady=3)
        self._register_feedback_button("verify", self.verify_btn, "验证激活", managed=False)

        self.activation_notice_label = tk.Label(
            content,
            textvariable=self.activation_notice_text,
            font=("Microsoft YaHei", 11),
            bg=self.ACTIVATION_BG,
            fg="#6b7280",
            justify="center",
            anchor="n",
            wraplength=420,
        )
        self.activation_notice_label.pack(pady=(20, 0))
    def _build_activation_backdrop(self, host, mode):
        canvas = tk.Canvas(
            host,
            bg=self.ACTIVATION_BG,
            highlightthickness=0,
            bd=0,
            relief="flat",
        )
        canvas.place(x=0, y=0, relwidth=1, relheight=1)
        canvas.bind(
            "<Configure>",
            lambda _event, target=canvas, canvas_mode=mode: self._paint_activation_backdrop(target, canvas_mode),
        )
        self.root.after_idle(lambda target=canvas, canvas_mode=mode: self._paint_activation_backdrop(target, canvas_mode))
        return canvas

    def _paint_activation_backdrop(self, canvas, mode):
        width = max(int(canvas.winfo_width()), 1)
        height = max(int(canvas.winfo_height()), 1)
        if width <= 1 or height <= 1:
            return
        canvas.delete("bg")
        canvas.create_rectangle(0, 0, width, height, fill=self.ACTIVATION_BG, outline="", tags="bg")
        if mode == "hero":
            canvas.create_oval(-96, -64, width * 0.46, height * 1.08, fill=self.ACTIVATION_GLOW, outline="", tags="bg")
            canvas.create_oval(width * 0.50, -42, width + 120, height * 0.88, fill="#f3f9ff", outline="", tags="bg")
            canvas.create_line(26, height - 20, width - 34, height - 20, fill=self.ACTIVATION_LINE, width=1, tags="bg")
            for index in range(2):
                y = 24 + (index * 14)
                canvas.create_line(
                    width * 0.62,
                    y,
                    width - 30,
                    y,
                    fill=self.ACTIVATION_LINE,
                    width=1,
                    dash=(3, 9),
                    tags="bg",
                )
            canvas.create_line(
                width * 0.16,
                18,
                width * 0.30,
                height - 28,
                fill="#edf5ff",
                width=1,
                tags="bg",
            )
            return

        inset_left = 16
        inset_top = 18
        inset_right = width - 18
        inset_bottom = height - 18
        for x in range(inset_left, inset_right, 38):
            canvas.create_line(x, inset_top, x, inset_bottom, fill=self.ACTIVATION_GRID, width=1, tags="bg")
        for y in range(inset_top, inset_bottom, 38):
            canvas.create_line(inset_left, y, inset_right, y, fill=self.ACTIVATION_GRID, width=1, tags="bg")
        points = [
            (width * 0.14, height * 0.24),
            (width * 0.24, height * 0.18),
            (width * 0.34, height * 0.28),
            (width * 0.80, height * 0.18),
            (width * 0.72, height * 0.30),
            (width * 0.86, height * 0.34),
        ]
        lines = ((0, 1), (1, 2), (3, 4), (4, 5))
        for start, end in lines:
            x1, y1 = points[start]
            x2, y2 = points[end]
            canvas.create_line(x1, y1, x2, y2, fill=self.ACTIVATION_LINE, width=1, tags="bg")
        for x, y in points:
            canvas.create_oval(x - 2, y - 2, x + 2, y + 2, fill=self.ACTIVATION_LINE, outline="", tags="bg")
        canvas.create_oval(width - 176, height - 148, width + 22, height + 30, fill="#f5faff", outline="", tags="bg")
        canvas.create_oval(-60, height - 120, width * 0.28, height + 42, fill="#f7fbff", outline="", tags="bg")

    def _build_operation_screen(self, parent):
        self.ops_screen = tk.Frame(parent, bg=self.SHELL_BG)
        self.ops_screen.place(x=0, y=0, relwidth=1, relheight=1)

        shell = tk.Frame(
            self.ops_screen,
            bg=self.SHELL_BG,
            highlightthickness=0,
            bd=0,
        )
        shell.pack(
            fill="both",
            expand=True,
            padx=self.CONTENT_PAD_X,
            pady=(self.CONTENT_PAD_TOP, self.CONTENT_PAD_BOTTOM),
        )
        shell.pack_propagate(False)

        toolbar = tk.Frame(
            shell,
            bg=self.PANEL_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            height=46,
        )
        toolbar.pack(fill="x", padx=0, pady=(0, self.PANEL_SPACING))
        toolbar.pack_propagate(False)

        button_row = tk.Frame(toolbar, bg=self.PANEL_BG)
        button_row.pack(side="left", padx=(10, 0), pady=6)
        self.run_btn = self._create_action_button(button_row, "启动", "#4f7df0", self._trigger_run, width=8)
        self.stop_btn = self._create_action_button(button_row, "停止", "#ea5b64", self._trigger_stop, width=8)
        self.pause_btn = self._create_action_button(
            button_row, "暂停", "#ffffff", self._trigger_pause, fg="#24416e", width=8
        )
        self.resume_btn = self._create_action_button(
            button_row, "继续", "#ffffff", self._trigger_resume, fg="#24416e", width=8
        )
        for index, button in enumerate((self.run_btn, self.stop_btn, self.pause_btn, self.resume_btn)):
            button.configure(font=("Microsoft YaHei", 11, "bold"), padx=0, pady=2)
            button.pack(side="left", padx=(0, 6 if index < 3 else 0), ipady=2)
        self._register_feedback_button("run", self.run_btn, "启动", managed=True)
        self._register_feedback_button("pause", self.pause_btn, "暂停", managed=True)
        self._register_feedback_button("resume", self.resume_btn, "继续", managed=True)
        self._register_feedback_button("stop", self.stop_btn, "停止", managed=True)

        right_meta = tk.Frame(toolbar, bg=self.PANEL_BG)
        right_meta.pack(side="right", padx=(0, 10), pady=6)
        self.save_btn = self._create_action_button(
            right_meta,
            "保存",
            "#ffffff",
            self._trigger_save,
            fg="#24416e",
            width=8,
        )
        self.save_btn.configure(font=("Microsoft YaHei", 11, "bold"), padx=0, pady=2)
        self.save_btn.pack(ipady=2)
        self._register_feedback_button("save", self.save_btn, "保存", managed=False)

        param_panel = tk.Frame(
            shell,
            bg=self.PANEL_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            height=92,
        )
        param_panel.pack(fill="x", padx=0, pady=(0, self.PANEL_SPACING))
        param_panel.pack_propagate(False)
        params_grid = tk.Frame(
            param_panel,
            bg=self.PANEL_BG,
            width=self.PARAM_LEFT_GROUP_WIDTH + self.PARAM_COLUMN_GAP + self.PARAM_RIGHT_GROUP_WIDTH,
            height=68,
        )
        params_grid.pack(anchor="w", padx=8, pady=(8, 8))
        params_grid.pack_propagate(False)
        params_grid.grid_columnconfigure(0, minsize=self.PARAM_LEFT_GROUP_WIDTH)
        params_grid.grid_columnconfigure(1, minsize=self.PARAM_RIGHT_GROUP_WIDTH)
        params_grid.grid_rowconfigure(0, minsize=32)
        params_grid.grid_rowconfigure(1, minsize=32)

        local_group = tk.Frame(params_grid, bg=self.PANEL_BG, width=self.PARAM_LEFT_GROUP_WIDTH, height=32)
        local_group.grid(row=0, column=0, sticky="w")
        local_group.grid_propagate(False)
        tk.Label(
            local_group,
            text="本地地址",
            font=self.SECTION_LABEL_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).grid(row=0, column=0, padx=(0, 8), sticky="w")
        local_field = tk.Frame(local_group, bg=self.PANEL_BG, width=self.LOCAL_FIELD_WIDTH, height=32)
        local_field.grid(row=0, column=1, sticky="w")
        local_field.grid_propagate(False)
        self.bit_api_entry = self._create_entry(local_field, self.bit_api, font=("Microsoft YaHei", 11))
        self.bit_api_entry.pack(fill="both", expand=True, ipady=4)

        api_group = tk.Frame(params_grid, bg=self.PANEL_BG, width=self.PARAM_RIGHT_GROUP_WIDTH, height=32)
        api_group.grid(row=0, column=1, sticky="w", padx=(self.PARAM_COLUMN_GAP, 0))
        api_group.grid_propagate(False)
        api_label_holder = tk.Frame(
            api_group,
            bg=self.PANEL_BG,
            width=self.RIGHT_MAIN_LABEL_WIDTH,
            height=32,
        )
        api_label_holder.grid(row=0, column=0, sticky="w")
        api_label_holder.grid_propagate(False)
        tk.Label(
            api_label_holder,
            text="浏览器 API",
            font=self.SECTION_LABEL_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).pack(side="left", fill="y")
        api_field = tk.Frame(api_group, bg=self.PANEL_BG, width=self.API_FIELD_WIDTH, height=32)
        api_field.grid(row=0, column=1, sticky="w")
        api_field.grid_propagate(False)
        self.api_token_entry = self._create_entry(api_field, self.api_token, font=("Microsoft YaHei", 11))
        self.api_token_entry.pack(fill="both", expand=True, ipady=4)

        freq_group = tk.Frame(params_grid, bg=self.PANEL_BG, width=self.PARAM_LEFT_GROUP_WIDTH, height=32)
        freq_group.grid(row=1, column=0, sticky="w", pady=(9, 0))
        freq_group.grid_propagate(False)
        tk.Label(
            freq_group,
            text="发送频率",
            font=self.SECTION_LABEL_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).grid(row=0, column=0, padx=(0, 8), sticky="w")
        interval_stack = tk.Frame(freq_group, bg=self.PANEL_BG, width=self.INTERVAL_STACK_WIDTH, height=32)
        interval_stack.grid(row=0, column=1, sticky="w")
        interval_stack.grid_propagate(False)
        interval_stack.grid_columnconfigure(0, minsize=self.INTERVAL_STACK_WIDTH)
        self.min_interval = self._create_entry(
            interval_stack, tk.StringVar(), width=5, justify="center", font=("Microsoft YaHei", 11)
        )
        self.min_interval.place(x=0, y=0, width=52, height=32)
        tk.Label(
            interval_stack,
            text="—",
            font=self.LABEL_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).place(x=70, y=6, width=14, height=20)
        self.max_interval = self._create_entry(
            interval_stack, tk.StringVar(), width=5, justify="center", font=("Microsoft YaHei", 11)
        )
        self.max_interval.place(x=98, y=0, width=52, height=32)

        window_group = tk.Frame(params_grid, bg=self.PANEL_BG, width=self.PARAM_RIGHT_GROUP_WIDTH, height=32)
        window_group.grid(row=1, column=1, sticky="w", padx=(self.PARAM_COLUMN_GAP, 0), pady=(9, 0))
        window_group.grid_propagate(False)
        window_label_holder = tk.Frame(
            window_group,
            bg=self.PANEL_BG,
            width=self.RIGHT_MAIN_LABEL_WIDTH,
            height=32,
        )
        window_label_holder.grid(row=0, column=0, sticky="w")
        window_label_holder.grid_propagate(False)
        tk.Label(
            window_label_holder,
            text="窗口数量",
            font=self.SECTION_LABEL_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).pack(side="left", fill="y")
        count_field = tk.Frame(window_group, bg=self.PANEL_BG, width=self.COUNT_FIELD_WIDTH, height=32)
        count_field.grid(row=0, column=1, sticky="w")
        count_field.grid_propagate(False)
        self.window_count_entry = self._create_entry(
            count_field,
            self.window_count_text,
            width=3,
            justify="center",
            font=("Microsoft YaHei", 11),
        )
        self.window_count_entry.pack(fill="both", expand=True, ipady=4)
        tk.Label(
            window_group,
            text="窗口号",
            font=self.SECTION_LABEL_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).grid(row=0, column=2, padx=(8, 6), sticky="w")
        no_field = tk.Frame(window_group, bg=self.PANEL_BG, width=self.WINDOW_NO_FIELD_WIDTH, height=32)
        no_field.grid(row=0, column=3, sticky="w")
        no_field.grid_propagate(False)
        self.window_no_entry = self._create_entry(
            no_field,
            self.window_no_text,
            width=7,
            justify="center",
            font=("Microsoft YaHei", 11),
        )
        self.window_no_entry.pack(fill="both", expand=True, ipady=4)
        self.single_run_btn = self._create_action_button(
            window_group, "单独启动", "#4f7df0", self._trigger_single_run, width=8
        )
        self.single_run_btn.configure(font=("Microsoft YaHei", 11, "bold"), padx=0, pady=2)
        self.single_run_btn.grid(row=0, column=4, padx=(8, 0), sticky="w", ipady=2)
        self._register_feedback_button("single", self.single_run_btn, "单独启动", managed=True)

        message_panel = tk.Frame(
            shell,
            bg=self.PANEL_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            height=136,
        )
        message_panel.pack(fill="x", padx=0, pady=(0, self.PANEL_SPACING))
        message_panel.pack_propagate(False)
        tk.Label(
            message_panel,
            text="发送文本",
            font=self.SECTION_TITLE_FONT,
            bg=self.PANEL_BG,
            fg="#24416e",
        ).pack(anchor="w", padx=10, pady=(3, 3))
        message_body = tk.Frame(
            message_panel,
            bg=self.PANEL_BG,
            highlightthickness=0,
            highlightbackground=self.PANEL_BG,
            bd=0,
        )
        message_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.message_text = tk.Text(
            message_body,
            font=self.MESSAGE_FONT,
            bg="#ffffff",
            fg="#24416e",
            relief="flat",
            bd=0,
            highlightthickness=0,
            wrap="word",
            height=5,
            padx=4,
            pady=0.5,
        )
        self.message_text.pack(side="left", fill="both", expand=True, padx=0, pady=0)
        message_scroll = tk.Scrollbar(message_body, orient="vertical", command=self.message_text.yview)
        message_scroll.pack(side="right", fill="y", padx=(0, 0), pady=0)
        self.message_text.configure(yscrollcommand=message_scroll.set)

        log_panel = tk.Frame(
            shell,
            bg=self.PANEL_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            bd=0,
        )
        log_panel.pack(fill="both", expand=True, padx=0, pady=0)
        log_header = tk.Frame(log_panel, bg=self.PANEL_BG)
        log_header.pack(fill="x", padx=10, pady=(3, 3))
        tk.Label(log_header, text="实时日志", font=self.SECTION_TITLE_FONT, bg=self.PANEL_BG, fg="#24416e").pack(side="left")
        counters = tk.Frame(log_header, bg=self.PANEL_BG)
        counters.pack(side="right")
        tk.Label(counters, text="发送：", font=self.META_FONT, bg=self.PANEL_BG, fg="#315070").pack(side="left")
        tk.Label(counters, textvariable=self.sent_count_text, font=self.META_FONT, bg=self.PANEL_BG, fg="#315070").pack(side="left", padx=(0, 10))
        tk.Label(counters, text="失败：", font=self.META_FONT, bg=self.PANEL_BG, fg="#315070").pack(side="left")
        tk.Label(counters, textvariable=self.failed_count_text, font=self.META_FONT, bg=self.PANEL_BG, fg="#315070").pack(side="left")

        log_body = tk.Frame(
            log_panel,
            bg="#233f6f",
            highlightthickness=1,
            highlightbackground="#2d528e",
            bd=0,
        )
        log_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log_text = tk.Text(
            log_body,
            font=self.TEXT_FONT,
            bg="#233f6f",
            fg="#ffffff",
            insertbackground="#ffffff",
            relief="flat",
            bd=0,
            highlightthickness=0,
            wrap="char",
            spacing1=1,
            spacing3=1,
            padx=10,
            pady=12,
        )
        self.log_text.pack(side="left", fill="both", expand=True, padx=(8, 0), pady=8)
        y_scroll = tk.Scrollbar(log_body, orient="vertical", command=self.log_text.yview)
        y_scroll.pack(side="right", fill="y", padx=(0, 8), pady=8)
        self.log_text.configure(yscrollcommand=y_scroll.set)
        self.log_text.bind("<Key>", self._block_log_edit)
        self.log_text.bind("<<Paste>>", lambda _event: "break")
        self.log_text.bind("<MouseWheel>", self._update_log_follow_state)
        self.log_text.bind("<Button-4>", self._update_log_follow_state)
        self.log_text.bind("<Button-5>", self._update_log_follow_state)
        self.log_text.bind("<ButtonRelease-1>", self._update_log_follow_state)
        self.log_text.bind("<KeyRelease>", self._update_log_follow_state)

    def _create_entry(self, parent, text_var, width=None, justify="left", font=None):
        return tk.Entry(
            parent,
            textvariable=text_var,
            width=width,
            justify=justify,
            font=font or ("Microsoft YaHei", 11),
            relief="solid",
            bd=1,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            highlightcolor="#5a8fe3",
            bg="#ffffff",
            fg="#24416e",
        )

    def _create_action_button(self, parent, text, bg, command, fg="#ffffff", width=9):
        button = tk.Button(
            parent,
            text=text,
            font=self.TOOLBAR_FONT,
            bg=bg,
            fg=fg,
            activebackground=bg,
            activeforeground=fg,
            relief="raised",
            bd=1,
            width=width,
            cursor="hand2",
            command=command,
            padx=0,
            pady=3,
            disabledforeground=fg,
        )
        self._action_button_styles[button] = self._build_action_button_style(bg, fg)
        button.bind("<Enter>", lambda _event, target=button: self._set_action_button_visual(target, "hover"))
        button.bind("<Leave>", lambda _event, target=button: self._sync_action_button_visual(target))
        button.bind("<ButtonPress-1>", lambda _event, target=button: self._set_action_button_visual(target, "press"))
        button.bind(
            "<ButtonRelease-1>",
            lambda event, target=button: self._restore_action_button_visual(target, event),
        )
        self._set_action_button_visual(button, "normal")
        return button

    def _register_feedback_button(self, key, button, default_text, managed):
        self._button_texts[key] = default_text
        self._button_restore_modes[key] = managed
        button._feedback_key = key

    def _shade_hex(self, color, factor):
        value = str(color or "").strip()
        if len(value) != 7 or not value.startswith("#"):
            return value
        try:
            red = max(0, min(255, int(value[1:3], 16)))
            green = max(0, min(255, int(value[3:5], 16)))
            blue = max(0, min(255, int(value[5:7], 16)))
        except ValueError:
            return value
        red = max(0, min(255, int(red * factor)))
        green = max(0, min(255, int(green * factor)))
        blue = max(0, min(255, int(blue * factor)))
        return f"#{red:02x}{green:02x}{blue:02x}"

    def _tint_hex(self, color, strength):
        value = str(color or "").strip()
        if len(value) != 7 or not value.startswith("#"):
            return value
        try:
            red = int(value[1:3], 16)
            green = int(value[3:5], 16)
            blue = int(value[5:7], 16)
        except ValueError:
            return value
        strength = max(0.0, min(1.0, float(strength)))
        red = int(red + (255 - red) * strength)
        green = int(green + (255 - green) * strength)
        blue = int(blue + (255 - blue) * strength)
        return f"#{red:02x}{green:02x}{blue:02x}"

    def _build_action_button_style(self, bg, fg):
        base_bg = str(bg or "").strip().lower()
        base_fg = str(fg or "").strip() or "#ffffff"
        is_light = base_bg in {"#fff", "#ffffff"}
        if is_light:
            normal_bg = "#f7faff"
            hover_bg = "#ffffff"
            press_bg = "#dce6f4"
            disabled_bg = "#edf1f6"
            normal_fg = "#24416e"
            disabled_fg = "#94a3b8"
        else:
            normal_bg = self._tint_hex(bg, 0.04)
            hover_bg = self._tint_hex(bg, 0.16)
            press_bg = self._shade_hex(bg, 0.76)
            disabled_bg = self._tint_hex(bg, 0.54)
            normal_fg = base_fg
            disabled_fg = "#e6edf9"
        return {
            "normal_bg": normal_bg,
            "hover_bg": hover_bg,
            "press_bg": press_bg,
            "disabled_bg": disabled_bg,
            "normal_fg": normal_fg,
            "disabled_fg": disabled_fg,
        }

    def _set_action_button_visual(self, button, state="normal", force=False):
        styles = self._action_button_styles.get(button)
        if not styles or not button.winfo_exists():
            return
        actual_state = str(button.cget("state") or "").strip()
        visual_state = state
        if actual_state == "disabled" and not force:
            visual_state = "disabled"
        if visual_state == "hover":
            bg = styles["hover_bg"]
            fg = styles["normal_fg"]
            relief = "raised"
        elif visual_state == "press":
            bg = styles["press_bg"]
            fg = styles["normal_fg"]
            relief = "sunken"
        elif visual_state == "disabled":
            bg = styles["disabled_bg"]
            fg = styles["disabled_fg"]
            relief = "raised"
        else:
            bg = styles["normal_bg"]
            fg = styles["normal_fg"]
            relief = "raised"
        button.configure(
            bg=bg,
            fg=fg,
            activebackground=styles["press_bg"] if visual_state == "press" else styles["hover_bg"],
            activeforeground=styles["normal_fg"],
            disabledforeground=styles["disabled_fg"],
            relief=relief,
            bd=1,
            overrelief="raised",
        )

    def _restore_action_button_visual(self, button, event):
        if not button.winfo_exists():
            return
        if str(button.cget("state") or "").strip() == "disabled":
            self._set_action_button_visual(button, "disabled")
            return
        inside = 0 <= event.x <= button.winfo_width() and 0 <= event.y <= button.winfo_height()
        self._set_action_button_visual(button, "hover" if inside else "normal")

    def _sync_action_button_visual(self, button):
        if button is None or not button.winfo_exists():
            return
        self._set_action_button_visual(
            button,
            "disabled" if str(button.cget("state") or "").strip() == "disabled" else "normal",
        )

    def _set_activation_notice(self, message="", error=False):
        self.activation_notice_text.set(str(message or "").strip())
        if self.activation_notice_label:
            self.activation_notice_label.configure(fg="#cf2d2d" if error else "#7f8ca3")

    def _show_activation_screen(self, message="", error=False):
        if self.activation_screen:
            self.activation_screen.lift()
        self._set_activation_notice(message, error=error)
        try:
            self.code_entry.focus_set()
        except Exception:
            pass

    def _show_operation_screen(self):
        if self.ops_screen:
            self.ops_screen.lift()
        self._set_activation_notice("", error=False)

    def _load_message_templates(self):
        templates = load_messages_config().get("templates") or []
        templates_text = "\n".join(templates)
        if self.message_text is not None:
            self.message_text.delete("1.0", tk.END)
            if templates_text:
                self.message_text.insert("1.0", templates_text)

    def _reset_log_session(self):
        self._log_offsets = {}
        self._log_follow_tail = True
        self._reset_status_summary()
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_path.write_text("", encoding="utf-8")
        except Exception:
            pass
        for path in self._iter_local_log_paths():
            try:
                self._log_offsets[str(path.resolve())] = path.stat().st_size
            except Exception:
                self._log_offsets[str(path.resolve())] = 0
        if self.log_text is not None:
            self.log_text.delete("1.0", tk.END)

    def _schedule_log_refresh(self, initial=False):
        if self.log_job:
            self.root.after_cancel(self.log_job)
            self.log_job = None
        delay = 500 if initial else 700
        self.log_job = self.root.after(delay, self._poll_log)

    def _poll_log(self):
        try:
            self._append_worker_log()
        finally:
            self._schedule_log_refresh()

    def _iter_local_log_paths(self):
        log_dir = _runtime_path("logs")
        paths = []
        worker_log = log_dir / "worker.log"
        if worker_log.exists() or worker_log == self._log_path:
            paths.append(worker_log)
        try:
            runtime_logs = sorted(
                log_dir.glob("runtime_*.log"),
                key=lambda item: item.stat().st_mtime,
            )
        except Exception:
            runtime_logs = []
        for path in runtime_logs[-4:]:
            if path not in paths:
                paths.append(path)
        return paths

    def _append_local_worker_log(self):
        if self.log_text is None:
            return
        chunks = []
        for path in self._iter_local_log_paths():
            try:
                resolved = str(path.resolve())
                previous_offset = int(self._log_offsets.get(resolved, 0) or 0)
                current_size = path.stat().st_size
                if current_size < previous_offset:
                    previous_offset = 0
                with path.open("r", encoding="utf-8", errors="ignore") as handle:
                    handle.seek(previous_offset)
                    chunk = handle.read()
                    self._log_offsets[resolved] = handle.tell()
                if chunk:
                    chunks.append(chunk)
            except Exception:
                continue
        chunk = "".join(chunks)
        if not chunk:
            return
        self.log_text.insert(tk.END, chunk)
        if self._log_follow_tail:
            self.log_text.see(tk.END)

    def _reset_status_summary(self):
        self.sent_count_text.set("0")
        self.failed_count_text.set("0")

    def _apply_remote_stats(self, info):
        info = info or {}
        stats = info.get("stats") or {}
        stats = stats or {}
        db_stats = stats.get("db") or {}
        self.sent_count_text.set(str(_parse_int(db_stats.get("sent_today"), 0)))
        self.failed_count_text.set(str(_parse_int(db_stats.get("failed_today"), 0)))

    def _collect_message_templates(self):
        if self.message_text is None:
            return []
        return [
            str(line).strip()
            for line in self.message_text.get("1.0", tk.END).splitlines()
            if str(line).strip()
        ]

    def _append_worker_log(self):
        self._append_local_worker_log()

    def _is_log_near_bottom(self):
        if self.log_text is None:
            return True
        try:
            _, last = self.log_text.yview()
        except Exception:
            return True
        return last >= 0.995

    def _update_log_follow_state(self, _event=None):
        self.root.after_idle(lambda: setattr(self, "_log_follow_tail", self._is_log_near_bottom()))

    def _block_log_edit(self, event):
        allowed_keys = {
            "Left",
            "Right",
            "Up",
            "Down",
            "Home",
            "End",
            "Prior",
            "Next",
            "Shift_L",
            "Shift_R",
            "Control_L",
            "Control_R",
        }
        if event.keysym in allowed_keys:
            return None
        if (event.state & 0x4) and event.keysym.lower() in {"c", "a"}:
            return None
        return "break"

    def _load_existing_state(self):
        send_interval = (
            self.client_config.get("task_settings", {}).get("send_interval_seconds") or [0, 0]
        )
        task_settings = self.client_config.get("task_settings") or {}
        min_value = str(_parse_int(send_interval[0] if len(send_interval) > 0 else 0, 0))
        max_value = str(_parse_int(send_interval[1] if len(send_interval) > 1 else 0, 0))
        self.min_interval.delete(0, tk.END)
        self.min_interval.insert(0, min_value)
        self.max_interval.delete(0, tk.END)
        self.max_interval.insert(0, max_value)
        window_count = _parse_int(task_settings.get("max_windows"), 10)
        if window_count <= 0 and str(task_settings.get("max_windows") or "").strip() == "":
            window_count = 10
        self.window_count_text.set(str(window_count if window_count >= 0 else 10))
        self.window_no_text.set(str(self.client_config.get("window_no") or "").strip())
        owner = str(self.license_data.get("owner") or "").strip()
        license_expired = _license_is_expired(self.license_data)
        token_expired = _token_is_expired(self.client_config.get("token_expires_at"))
        self.verified = _client_can_use_token(self.client_config, self.license_data)
        if license_expired:
            self._clear_local_auth(preserve_license=True)
            self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
        elif token_expired:
            self._clear_local_auth(preserve_license=True)
            self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
        elif self.verified:
            info = self.refresh_remote_status(silent=True)
            if self.verified:
                self._show_operation_screen()
            if not info:
                self._apply_button_state(agent_online=False, task_running=False, task_pending=False)
        else:
            self._show_activation_screen(
                "验证成功后自动进入操作界面",
                error=False,
            )
            self._apply_button_state(agent_online=False, task_running=False, task_pending=False)

    def _current_agent_token(self):
        return str(self.client_config.get("agent_token") or "").strip()

    def _clear_local_auth(self, preserve_license=True):
        config = load_client_config()
        config["agent_token"] = ""
        config["token_expires_at"] = None
        save_client_config(config)
        self.client_config = config
        self.verified = False
        self.activation_code.set("")
        if not preserve_license:
            self.license_data = {}
            save_license({})
        self.server_url = _normalize_server_url(config.get("server_url") or DEFAULT_SERVER_URL)
        self.machine_id = str(config.get("machine_id") or self.machine_id).strip()

    def _store_local_auth(self, agent_token, token_expires_at):
        config = self._build_client_config()
        config["agent_token"] = str(agent_token or "").strip()
        config["token_expires_at"] = token_expires_at
        save_client_config(config)
        self.client_config = config
        self.server_url = _normalize_server_url(config.get("server_url") or DEFAULT_SERVER_URL)
        self.machine_id = str(config.get("machine_id") or self.machine_id).strip()

    def _collect_send_interval(self):
        min_value = _parse_int(self.min_interval.get(), 0)
        max_value = _parse_int(self.max_interval.get(), 0)
        if min_value < 0 or max_value < 0:
            raise ValueError("发送频率不能小于 0")
        if min_value > max_value:
            min_value, max_value = max_value, min_value
        return [min_value, max_value]

    def _collect_window_count(self):
        raw = str(self.window_count_text.get() or "").strip()
        if not raw:
            return 10
        value = _parse_int(raw, 10)
        return max(0, value)

    def _build_client_config(self):
        interval = self._collect_send_interval()
        max_windows = self._collect_window_count()
        config = load_client_config()
        config["server_url"] = self.server_url or DEFAULT_SERVER_URL
        config["machine_id"] = self.machine_id
        config["agent_token"] = str(config.get("agent_token") or self.client_config.get("agent_token") or "").strip()
        config["token_expires_at"] = config.get("token_expires_at", self.client_config.get("token_expires_at"))
        config["bit_api"] = _normalize_bit_api(self.bit_api.get())
        config["api_token"] = str(self.api_token.get() or "").strip()
        config["window_no"] = str(self.window_no_text.get() or "").strip()
        config.setdefault("task_settings", {})
        config["task_settings"]["send_interval_seconds"] = interval
        config["task_settings"]["max_windows"] = max_windows
        return config

    def _save_local_config(self, require_connection_fields=False):
        config = self._build_client_config()
        if require_connection_fields:
            if not config["bit_api"] or not config["api_token"]:
                messagebox.showerror("提示", "BitBrowser 地址与 API Token 不能为空")
                return False
        try:
            save_messages_config(templates=self._collect_message_templates())
            save_client_config(config)
        except Exception as exc:
            messagebox.showerror("提示", f"本地配置保存失败：{exc}")
            return False
        self.client_config = config
        self.server_url = _normalize_server_url(config.get("server_url") or DEFAULT_SERVER_URL)
        self.machine_id = str(config.get("machine_id") or self.machine_id).strip()
        return True

    def _post_client_api(self, path, payload, timeout=12):
        try:
            response = requests.post(
                f"{self.server_url}{path}",
                json=payload,
                timeout=timeout,
            )
        except Exception as exc:
            return False, {"error": f"无法连接服务器：{exc}"}
        try:
            data = response.json()
        except Exception:
            data = {"error": response.text or "请求失败"}
        if response.status_code != 200 or not data.get("ok"):
            return False, data
        return True, data

    def _sync_config_to_server(self, show_error=True):
        if not self.verified:
            return True
        payload = {
            "agent_token": self._current_agent_token(),
            "machine_id": self.machine_id,
            "bit_api": _normalize_bit_api(self.bit_api.get()),
            "api_token": str(self.api_token.get() or "").strip(),
            "name": socket.gethostname(),
        }
        ok, data = self._post_client_api("/api/client/sync-config", payload)
        if not ok and show_error:
            messagebox.showerror("提示", f"同步本地配置失败：{data.get('error')}")
        return ok

    def _ensure_worker_running(self, wait_online=False):
        local_worker_alive = False
        if self.worker_process and self.worker_process.poll() is None:
            local_worker_alive = True
        else:
            if self.worker_process and self.worker_process.poll() is not None:
                self.worker_process = None
            worker_pid = self._read_worker_pid()
            local_worker_alive = self._is_pid_running(worker_pid)
        status = self.refresh_remote_status(silent=True)
        if status and status.get("agent_online") and local_worker_alive:
            return True
        if not local_worker_alive:
            self._cleanup_stale_worker_process()
        if self.worker_process and self.worker_process.poll() is None:
            if not wait_online:
                return True
        worker_cmd = None
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            worker_exe = exe_dir / "FB_RPA_Worker.exe"
            if not worker_exe.exists():
                messagebox.showerror("提示", "找不到 FB_RPA_Worker.exe")
                return False
            worker_cmd = [str(worker_exe)]
        else:
            worker_script = ROOT_DIR / "agent" / "worker.py"
            if not worker_script.exists():
                messagebox.showerror("提示", "找不到 worker.py")
                return False
            worker_cmd = [sys.executable, str(worker_script)]

        agent_token = self._current_agent_token()
        if not agent_token:
            messagebox.showerror("提示", "当前缺少有效 agent_token，请重新激活。")
            self._show_activation_screen("请重新输入激活码。", error=True)
            return False

        bit_api = _normalize_bit_api(self.bit_api.get())
        api_token = str(self.api_token.get() or "").strip()
        owner = str(self.license_data.get("owner") or "").strip()
        runtime_dir = str(_runtime_dir())
        client_version = str(self.client_config.get("client_version") or "").strip()
        token_expires_at = self.client_config.get("token_expires_at")

        worker_cmd.extend([
            "--server",
            self.server_url,
            "--bit-api",
            bit_api,
            "--api-token",
            api_token,
            "--owner",
            owner,
            "--machine-id",
            self.machine_id,
            "--agent-token",
            agent_token,
            "--runtime-dir",
            runtime_dir,
            "--client-version",
            client_version,
        ])
        if token_expires_at not in (None, ""):
            worker_cmd.extend(["--token-expires-at", str(token_expires_at)])

        log_path = _runtime_path("logs", "worker.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        creationflags = 0
        kwargs = {}
        stdout_handle = None
        if sys.platform.startswith("win"):
            create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            create_new_process_group = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            creationflags = create_no_window | create_new_process_group
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            kwargs["startupinfo"] = startupinfo
        else:
            kwargs["start_new_session"] = True
        try:
            stdout_handle = log_path.open("a", encoding="utf-8")
            self.worker_process = subprocess.Popen(
                worker_cmd,
                stdout=stdout_handle,
                stderr=stdout_handle,
                creationflags=creationflags,
                cwd=str(ROOT_DIR),
                env={**os.environ, ENV_RUNTIME_DIR: runtime_dir},
                **kwargs,
            )
            self._write_worker_pid(self.worker_process.pid)
        except Exception as exc:
            messagebox.showerror("提示", f"启动 worker 失败：{exc}")
            return False
        finally:
            if stdout_handle is not None:
                try:
                    stdout_handle.close()
                except Exception:
                    pass

        if not wait_online:
            return True
        for _ in range(8):
            time.sleep(1)
            status = self.refresh_remote_status(silent=True)
            if status and status.get("agent_online"):
                return True
        messagebox.showwarning("提示", "worker 已启动，但执行端尚未连上服务器")
        return False

    def _apply_button_state(
        self,
        agent_online,
        task_running,
        task_pending,
        task_paused=False,
        config_synced=False,
        device_bound=False,
        start_ready=False,
        start_block_reason="",
    ):
        self._button_state_snapshot = {
            "agent_online": bool(agent_online),
            "task_running": bool(task_running),
            "task_pending": bool(task_pending),
            "task_paused": bool(task_paused),
            "config_synced": bool(config_synced),
            "device_bound": bool(device_bound),
            "start_ready": bool(start_ready),
            "start_block_reason": str(start_block_reason or "").strip(),
        }
        self._prune_action_locks()
        is_running = bool(task_running)
        is_pending = bool(task_pending)
        is_paused = bool(task_paused)
        is_busy = bool(is_running or is_pending or is_paused)
        actions_enabled = bool(self.verified)
        self.run_btn.config(
            state="normal" if actions_enabled and not self._is_action_locked("run") else "disabled"
        )
        self._sync_action_button_visual(self.run_btn)
        self.stop_btn.config(
            state="normal" if actions_enabled and not self._is_action_locked("stop") else "disabled"
        )
        self._sync_action_button_visual(self.stop_btn)
        self.pause_btn.config(
            state="normal" if actions_enabled and not self._is_action_locked("pause") else "disabled"
        )
        self._sync_action_button_visual(self.pause_btn)
        self.resume_btn.config(
            state="normal" if actions_enabled and not self._is_action_locked("resume") else "disabled"
        )
        self._sync_action_button_visual(self.resume_btn)
        if self.single_run_btn is not None:
            self.single_run_btn.config(
                state="normal" if actions_enabled and not self._is_action_locked("single") else "disabled"
            )
            self._sync_action_button_visual(self.single_run_btn)

    def refresh_remote_status(self, silent=False):
        if not self.verified:
            return None
        payload = {
            "agent_token": self._current_agent_token(),
            "machine_id": self.machine_id,
        }
        ok, data = self._post_client_api("/api/client/status", payload, timeout=8)
        if not ok:
            error_code = str(data.get("error") or "").strip()
            if error_code in CLIENT_REACTIVATION_ERRORS:
                self._clear_local_auth(preserve_license=True)
                self._reset_status_summary()
                self._apply_button_state(agent_online=False, task_running=False, task_pending=False)
                self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
                return None
            if not silent:
                self._set_activation_notice(
                    f"执行端状态获取失败：{_translate_client_error(data.get('error'))}",
                    error=True,
                )
            self._reset_status_summary()
            self._apply_button_state(agent_online=False, task_running=False, task_pending=False)
            return None

        info = data.get("data") or {}
        self._apply_remote_stats(info)
        self._apply_button_state(
            agent_online=bool(info.get("agent_online")),
            task_running=bool(info.get("task_running")),
            task_pending=bool(info.get("task_pending")),
            task_paused=bool(info.get("task_paused")),
            config_synced=bool(info.get("config_synced")),
            device_bound=bool(info.get("device_bound")),
            start_ready=bool(info.get("start_ready")),
            start_block_reason=info.get("start_block_reason") or "",
        )
        return info

    def _translate_start_block_reason(self, reason):
        mapping = {
            "config_not_synced": "设备配置尚未同步完成",
            "device_not_bound": "设备绑定尚未完成",
            "missing_connection_fields": "设备配置缺少接口参数",
            "agent_offline": "执行端尚未在线",
        }
        return mapping.get(str(reason or "").strip(), "执行端尚未就绪")

    def _show_control_feedback(self, level, message):
        text = str(message or "").strip()
        if not text:
            return
        if level == "error":
            messagebox.showerror("提示", text)
            return
        if level == "warning":
            messagebox.showwarning("提示", text)
            return
        messagebox.showinfo("提示", text)

    def _control_action_key(self, action):
        mapping = {
            "start": "run",
            "stop": "stop",
            "pause": "pause",
            "resume": "resume",
            "restart_window": "single",
        }
        return mapping.get(str(action or "").strip(), "")

    def _precheck_control_action(self, action):
        action = str(action or "").strip()
        button_key = self._control_action_key(action)
        if button_key and self._is_action_locked(button_key):
            duplicate_message = {
                "start": "启动指令处理中，请勿重复点击",
                "stop": "停止指令已在处理中，请勿重复点击",
                "pause": "暂停指令已在处理中，请勿重复点击",
                "resume": "继续指令已在处理中，请勿重复点击",
                "restart_window": "单独启动任务已在处理中，请勿重复下发",
            }
            return False, "warning", duplicate_message.get(action, "当前操作正在处理中，请勿重复点击")
        snapshot = dict(self._button_state_snapshot or {})
        task_running = bool(snapshot.get("task_running"))
        task_pending = bool(snapshot.get("task_pending"))
        task_paused = bool(snapshot.get("task_paused"))
        if action == "start":
            if task_running:
                return False, "warning", "当前任务已在运行，无需重复启动"
            if task_pending:
                return False, "warning", "启动任务已在排队，请勿重复启动"
            if task_paused:
                return False, "warning", "当前任务处于暂停中，请先点击继续或停止"
        elif action == "stop":
            if not (task_running or task_pending or task_paused):
                return False, "warning", "当前没有可停止的运行任务"
        elif action == "pause":
            if task_paused:
                return False, "warning", "当前任务已处于暂停状态"
            if not task_running:
                return False, "warning", "当前没有可暂停的运行任务"
        elif action == "resume":
            if not task_paused:
                return False, "warning", "当前没有可继续的暂停任务"
        return True, "info", ""

    def _present_control_result(self, action, response):
        response = response or {}
        result = str(response.get("result") or "accepted").strip() or "accepted"
        message = str(response.get("message") or "").strip()
        if not message:
            defaults = {
                "start": "启动指令已下发，正在等待执行端响应",
                "stop": "停止指令已下发，正在等待执行端响应",
                "pause": "暂停指令已下发",
                "resume": "继续指令已下发",
                "restart_window": "单独启动指令已下发，正在等待执行端响应",
            }
            message = defaults.get(action, "操作已完成")
        if result == "accepted":
            self._show_control_feedback("info", message)
            return
        self._show_control_feedback("warning", message)

    def _wait_for_remote_start_ready(self, timeout=8.0):
        deadline = time.time() + max(timeout, 0.5)
        last_info = None
        while time.time() < deadline:
            info = self.refresh_remote_status(silent=True)
            if info and info.get("start_ready"):
                return info
            last_info = info or last_info
            time.sleep(0.5)
        return last_info

    def _schedule_status_refresh(self, initial=False):
        if self.status_job:
            self.root.after_cancel(self.status_job)
            self.status_job = None
        delay = 600 if initial else STATUS_REFRESH_MS
        self.status_job = self.root.after(delay, self._poll_status)

    def _poll_status(self):
        try:
            self.refresh_remote_status(silent=True)
        finally:
            self._schedule_status_refresh()

    def _with_button_feedback(self, key, busy_text, callback):
        button = getattr(self, f"{key}_btn", None)
        if button is None:
            callback()
            return
        default_text = self._button_texts.get(key, button.cget("text"))
        managed = self._button_restore_modes.get(key, False)
        previous_state = button.cget("state")
        original_bg = button.cget("bg")
        original_active_bg = button.cget("activebackground")
        pressed_bg = self._shade_hex(original_bg, 0.88)
        button.config(
            text=busy_text or default_text,
            state="disabled",
            relief="sunken",
            bg=pressed_bg,
            activebackground=pressed_bg,
        )
        self._set_action_button_visual(button, "press", force=True)
        self.root.configure(cursor="watch")
        self.root.update_idletasks()
        try:
            callback()
        finally:
            if button.winfo_exists():
                button.config(
                    text=default_text,
                    bg=original_bg,
                    activebackground=original_active_bg,
                )
                if managed:
                    self._apply_button_state(**self._button_state_snapshot)
                else:
                    button.config(state=previous_state if previous_state in {"normal", "disabled"} else "normal")
                    self._sync_action_button_visual(button)
            self.root.configure(cursor="")
            self.root.update_idletasks()

    def _trigger_verify(self):
        self._with_button_feedback("verify", "验证中...", self.on_verify)

    def _trigger_run(self):
        self._with_button_feedback("run", "启动中...", self.on_run)

    def _trigger_pause(self):
        self._with_button_feedback("pause", "暂停中...", self.on_pause)

    def _trigger_resume(self):
        self._with_button_feedback("resume", "继续中...", self.on_resume)

    def _trigger_stop(self):
        self._with_button_feedback("stop", "停止中...", self.on_stop)

    def _trigger_save(self):
        self._with_button_feedback("save", "保存中...", self.on_save_settings)

    def _trigger_single_run(self):
        self._with_button_feedback("single", "启动中...", self.on_single_run)

    def on_verify(self):
        code = str(self.activation_code.get() or "").strip()
        if not code:
            messagebox.showwarning("提示", "请输入激活码")
            return
        if not self._save_local_config(require_connection_fields=False):
            return
        payload = {
            "activation_code": code,
            "machine_id": self.machine_id,
            "client_version": str(self.client_config.get("client_version") or "").strip(),
        }
        ok, data = self._post_client_api("/api/client/activate", payload, timeout=15)
        if not ok:
            self.verified = False
            error_text = _translate_client_error(data.get("error"))
            if str(data.get("error") or "").strip() == "account_expired":
                self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
            else:
                self._show_activation_screen(error_text, error=True)
            messagebox.showerror("提示", error_text)
            return
        info = data.get("data") or {}
        self.server_url = _normalize_server_url(info.get("server_url") or self.server_url)
        self.license_data = {
            "owner": info.get("owner") or "",
            "expire_at": info.get("expire_at"),
            "max_devices": info.get("max_devices"),
        }
        save_license(self.license_data)
        self._store_local_auth(info.get("agent_token"), info.get("token_expires_at"))
        self.activation_code.set("")
        self.verified = bool(self._current_agent_token())
        if not self._save_local_config(require_connection_fields=False):
            return
        self._show_operation_screen()
        local_bit_api = _normalize_bit_api(self.bit_api.get())
        local_api_token = str(self.api_token.get() or "").strip()
        if local_bit_api and local_api_token:
            self._sync_config_to_server(show_error=False)
            self._ensure_worker_running(wait_online=False)
            self._wait_for_remote_start_ready(timeout=8.0)
            self.refresh_remote_status(silent=True)
        messagebox.showinfo("提示", "激活成功，已进入操作界面")

    def on_save_settings(self):
        if not self._save_local_config(require_connection_fields=False):
            return
        synced = self._sync_config_to_server(show_error=False)
        if self.verified and not synced:
            messagebox.showwarning("提示", "本地配置已保存，但尚未同步到服务器")
            return
        messagebox.showinfo("提示", "本地配置已保存")

    def _send_control(self, action):
        if not self.verified:
            messagebox.showwarning("提示", "请先完成激活验证")
            return False
        allowed, level, message = self._precheck_control_action(action)
        if not allowed:
            self._show_control_feedback(level, message)
            return False
        if not self._save_local_config(require_connection_fields=True):
            return False
        if action == "start":
            if not self._sync_config_to_server(show_error=True):
                return False
            if not self._ensure_worker_running(wait_online=True):
                return False
            ready_info = self._wait_for_remote_start_ready(timeout=8.0)
            if not ready_info or not ready_info.get("start_ready"):
                reason_text = self._translate_start_block_reason(
                    (ready_info or {}).get("start_block_reason")
                )
                self._show_control_feedback("warning", f"当前状态不允许启动：{reason_text}")
                return False
        payload = {
            "agent_token": self._current_agent_token(),
            "machine_id": self.machine_id,
        }
        action_key = self._control_action_key(action)
        ok, data = self._post_client_api(f"/api/client/{action}", payload, timeout=15)
        if not ok:
            self._unlock_action(action_key)
            self._show_control_feedback("error", f"{action} 失败：{data.get('error')}")
            return False
        result = str(data.get("result") or "accepted").strip() or "accepted"
        if result in {"accepted", "duplicate"} and action_key:
            self._lock_action(action_key)
        else:
            self._unlock_action(action_key)
        self.refresh_remote_status(silent=True)
        self._present_control_result(action, data)
        return result == "accepted"

    def on_run(self):
        self._send_control("start")

    def on_single_run(self):
        if not self.verified:
            messagebox.showwarning("提示", "请先完成激活验证")
            return
        if self._is_action_locked("single"):
            self._show_control_feedback("warning", "单独启动任务已在处理中，请勿重复下发")
            return
        window_token = str(self.window_no_text.get() or "").strip()
        if not window_token:
            messagebox.showwarning("提示", "请输入窗口号")
            return
        if not self._save_local_config(require_connection_fields=True):
            return
        if not self._sync_config_to_server(show_error=True):
            return
        if not self._ensure_worker_running(wait_online=True):
            return
        ready_info = self._wait_for_remote_start_ready(timeout=8.0)
        if not ready_info or not ready_info.get("start_ready"):
            reason_text = self._translate_start_block_reason(
                (ready_info or {}).get("start_block_reason")
            )
            self._show_control_feedback("warning", f"当前状态不允许单独启动：{reason_text}")
            return
        payload = {
            "agent_token": self._current_agent_token(),
            "machine_id": self.machine_id,
            "window_token": window_token,
        }
        ok, data = self._post_client_api("/api/client/restart-window", payload, timeout=15)
        if not ok:
            self._unlock_action("single")
            self._show_control_feedback("error", f"单独启动失败：{data.get('error')}")
            return
        result = str(data.get("result") or "accepted").strip() or "accepted"
        if result in {"accepted", "duplicate"}:
            self._lock_action("single")
        else:
            self._unlock_action("single")
        self.refresh_remote_status(silent=True)
        self._present_control_result("restart_window", data)

    def on_stop(self):
        self._send_control("stop")

    def on_pause(self):
        self._send_control("pause")

    def on_resume(self):
        self._send_control("resume")

    def _on_close(self):
        if self.status_job:
            self.root.after_cancel(self.status_job)
            self.status_job = None
        if self.log_job:
            self.root.after_cancel(self.log_job)
            self.log_job = None
        try:
            self._shutdown_worker_runtime(send_stop=True)
        except Exception:
            pass
        self.root.destroy()

def main():
    root = tk.Tk()
    app = ClientApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
