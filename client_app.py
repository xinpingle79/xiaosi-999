import json
import os
import socket
import subprocess
import sys
import time
import uuid
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
        "task_settings": {
            "max_messages_per_account": 0,
            "scroll_times": 0,
            "cursor_require_found": True,
            "group_list_scroll_times": 0,
            "idle_scroll_limit": 3,
            "account_restricted_blocked_threshold": 2,
            "account_restricted_signal_threshold": 3,
            "typing_delay": [100, 300],
            "speed_factor": 0.85,
            "fast_message_button_only": False,
            "force_hover_card": True,
            "fast_action_timeout_ms": 2000,
            "fast_visible_timeout_ms": 220,
            "fast_hover_sleep_ms": 120,
            "fast_retry_sleep_ms": 120,
            "quick_no_button_precheck": False,
            "stranger_limit_cache_ms": 1200,
            "send_interval_seconds": [2, 6],
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


def load_client_config() -> dict:
    config = _default_client_config()
    if not CLIENT_CONFIG_PATH.exists():
        return config
    try:
        loaded = yaml.safe_load(CLIENT_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return config
    merged = _sanitize_client_config(_deep_merge(config, loaded))
    should_rewrite = False
    if _should_migrate_server_url(loaded.get("server_url") or ""):
        merged["server_url"] = DEFAULT_SERVER_URL
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
    WINDOW_WIDTH = 602
    WINDOW_HEIGHT = 552
    TITLE_BAR_HEIGHT = 36
    SHELL_BG = "#edf4ff"
    WINDOW_BG = "#dbe7f6"
    PANEL_BG = "#eef5ff"
    BORDER_COLOR = "#8fb1e3"
    TITLE_BG = "#315f97"
    TITLE_FG = "#ffffff"
    HEADER_FONT = ("Microsoft YaHei", 12, "bold")
    LABEL_FONT = ("Microsoft YaHei", 10, "bold")
    TEXT_FONT = ("Consolas", 10)
    NOTICE_FONT = ("Microsoft YaHei", 9)
    SESSION_NOTICE_TEXT = "当前账号已到期或登录状态已失效，请重新输入有效激活码后继续使用。"

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
        self._button_state_snapshot = {
            "agent_online": False,
            "task_running": False,
            "task_pending": False,
        }
        self._log_path = _runtime_path("logs", "worker.log")
        self._log_offset = 0
        self._log_follow_tail = True

        self.activation_code = tk.StringVar(value="")
        self.bit_api = tk.StringVar(
            value=_normalize_bit_api(self.client_config.get("bit_api") or DEFAULT_BIT_API)
        )
        self.api_token = tk.StringVar(
            value=str(self.client_config.get("api_token") or "").strip()
        )
        self.activation_text = tk.StringVar(value="未激活")
        self.expire_text = tk.StringVar(value="到期时间：-")
        self.run_status_text = tk.StringVar(value="执行端未连接")
        self.server_text = tk.StringVar(value="")
        self.device_text = tk.StringVar(value="")
        self.activation_notice_text = tk.StringVar(value="")
        self.window_count_text = tk.StringVar(value=self._initial_window_count())
        self.window_no_text = tk.StringVar(value="")
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
        self._schedule_status_refresh(initial=True)
        self._schedule_log_refresh(initial=True)

    def _centered_geometry(self, width, height):
        self.root.update_idletasks()
        screen_w = max(self.root.winfo_screenwidth(), width)
        screen_h = max(self.root.winfo_screenheight(), height)
        pos_x = max((screen_w - width) // 2, 0)
        pos_y = max((screen_h - height) // 2, 0)
        return f"{width}x{height}+{pos_x}+{pos_y}"

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
            font=("Microsoft YaHei", 11, "bold"),
            bg=self.TITLE_BG,
            fg=self.TITLE_FG,
        )
        title_label.pack(side="left", padx=12)
        self.title_label = title_label

        button_frame = tk.Frame(title_bar, bg=self.TITLE_BG)
        button_frame.pack(side="right", padx=8, pady=7)

        self.minimize_btn = self._create_window_button(button_frame, "—", self._minimize_window)
        self.minimize_btn.pack(side="left", padx=(0, 6))
        self.maximize_btn = self._create_window_button(
            button_frame,
            self.maximize_button_text,
            self._toggle_maximize,
        )
        self.maximize_btn.pack(side="left", padx=(0, 6))
        self.close_btn = self._create_window_button(
            button_frame,
            "✕",
            self._on_close,
            bg="#e56b6f",
            activebg="#d9485f",
        )
        self.close_btn.pack(side="left")

        for widget in (title_bar, title_label):
            widget.bind("<ButtonPress-1>", self._start_window_drag)
            widget.bind("<B1-Motion>", self._drag_window)
            widget.bind("<Double-Button-1>", lambda _event: self._toggle_maximize())

    def _create_window_button(self, parent, text, command, bg="#5f86b8", activebg="#4f74a5"):
        textvariable = text if isinstance(text, tk.StringVar) else None
        btn = tk.Button(
            parent,
            text="" if textvariable else text,
            textvariable=textvariable,
            command=command,
            width=2,
            font=("Microsoft YaHei", 8, "bold"),
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
        return btn

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
            self.root.after(10, lambda: self.root.overrideredirect(True))
        elif not self.root.overrideredirect():
            self.root.after(10, lambda: self.root.overrideredirect(True))

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

    def _build_activation_screen(self, parent):
        self.activation_screen = tk.Frame(parent, bg=self.SHELL_BG)
        self.activation_screen.place(x=0, y=0, relwidth=1, relheight=1)

        card = tk.Frame(
            self.activation_screen,
            bg="#ffffff",
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            bd=0,
        )
        card.pack(fill="both", expand=True, padx=6, pady=6)

        hero = tk.Frame(card, bg="#ffffff", height=112)
        hero.pack(fill="x", padx=10, pady=(10, 8))
        hero.pack_propagate(False)
        tk.Label(
            hero,
            text="FB私聊助手",
            font=("Microsoft YaHei", 28, "bold"),
            bg="#ffffff",
            fg="#7ba6d8",
        ).pack(expand=True)

        body = tk.Frame(card, bg="#ffffff")
        body.pack(fill="both", expand=True, padx=26, pady=(0, 24))
        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=0)

        tk.Label(
            body,
            text="激活码",
            font=self.LABEL_FONT,
            bg="#ffffff",
            fg="#24416e",
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(6, 10))

        self.code_entry = tk.Entry(
            body,
            textvariable=self.activation_code,
            font=("Microsoft YaHei", 15),
            relief="solid",
            bd=1,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            highlightcolor="#5a8fe3",
            bg="#ffffff",
            fg="#24416e",
        )
        self.code_entry.grid(row=1, column=0, sticky="ew", ipady=8)
        self.code_entry.bind("<Return>", lambda _event: self._trigger_verify())

        self.verify_btn = tk.Button(
            body,
            text="验证激活",
            font=("Microsoft YaHei", 10, "bold"),
            bg="#4f7df0",
            fg="#ffffff",
            activebackground="#3f6de0",
            activeforeground="#ffffff",
            relief="flat",
            width=12,
            cursor="hand2",
            command=self._trigger_verify,
        )
        self.verify_btn.grid(row=1, column=1, padx=(12, 0), ipady=4)
        self._register_feedback_button("verify", self.verify_btn, "验证激活", managed=False)

        tip_frame = tk.Frame(body, bg="#ffffff")
        tip_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        tip_frame.grid_columnconfigure(0, weight=1)
        tip_frame.grid_columnconfigure(1, weight=1)

        self.activation_notice_label = tk.Label(
            tip_frame,
            textvariable=self.activation_notice_text,
            font=self.NOTICE_FONT,
            bg="#ffffff",
            fg="#6b7280",
            justify="left",
            anchor="w",
            wraplength=380,
        )
        self.activation_notice_label.grid(row=0, column=0, sticky="w")
        tk.Label(
            tip_frame,
            textvariable=self.server_text,
            font=self.NOTICE_FONT,
            bg="#ffffff",
            fg="#8a94a6",
            anchor="e",
        ).grid(row=0, column=1, sticky="e")

    def _build_operation_screen(self, parent):
        self.ops_screen = tk.Frame(parent, bg=self.SHELL_BG)
        self.ops_screen.place(x=0, y=0, relwidth=1, relheight=1)

        canvas = tk.Frame(
            self.ops_screen,
            bg=self.SHELL_BG,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            bd=0,
        )
        canvas.pack(fill="both", expand=True, padx=6, pady=6)
        canvas.pack_propagate(False)

        toolbar = tk.Frame(canvas, bg=self.PANEL_BG, highlightthickness=1, highlightbackground=self.BORDER_COLOR, height=52)
        toolbar.pack(fill="x", padx=6, pady=(6, 6))
        toolbar.pack_propagate(False)

        button_row = tk.Frame(toolbar, bg=self.PANEL_BG)
        button_row.pack(side="left", padx=8, pady=7)
        self.run_btn = self._create_action_button(button_row, "启动", "#4f7df0", self._trigger_run)
        self.pause_btn = self._create_action_button(button_row, "暂停", "#ffffff", self._trigger_pause, fg="#24416e")
        self.resume_btn = self._create_action_button(button_row, "继续", "#ffffff", self._trigger_resume, fg="#24416e")
        self.stop_btn = self._create_action_button(button_row, "停止", "#ea5b64", self._trigger_stop)
        for index, button in enumerate((self.run_btn, self.pause_btn, self.resume_btn, self.stop_btn)):
            button.grid(row=0, column=index, padx=(0, 8 if index < 3 else 0))
        self._register_feedback_button("run", self.run_btn, "启动", managed=True)
        self._register_feedback_button("pause", self.pause_btn, "暂停", managed=True)
        self._register_feedback_button("resume", self.resume_btn, "继续", managed=True)
        self._register_feedback_button("stop", self.stop_btn, "停止", managed=True)

        right_meta = tk.Frame(toolbar, bg=self.PANEL_BG)
        right_meta.pack(side="right", padx=8, pady=6)
        tk.Label(right_meta, text="窗口数量", font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").grid(row=0, column=0, padx=(0, 8))
        self.window_count_entry = self._create_entry(right_meta, self.window_count_text, width=6, justify="center")
        self.window_count_entry.grid(row=0, column=1, padx=(0, 8), ipady=5)
        self.save_btn = self._create_action_button(right_meta, "保存", "#ffffff", self._trigger_save, fg="#24416e", width=9)
        self.save_btn.grid(row=0, column=2)
        self._register_feedback_button("save", self.save_btn, "保存", managed=False)

        conn_panel = tk.Frame(canvas, bg=self.PANEL_BG, highlightthickness=1, highlightbackground=self.BORDER_COLOR, height=88)
        conn_panel.pack(fill="x", padx=6, pady=(0, 6))
        conn_panel.pack_propagate(False)
        left_block = tk.Frame(conn_panel, bg=self.PANEL_BG)
        left_block.pack(side="left", fill="both", expand=True, padx=(8, 6), pady=10)
        right_block = tk.Frame(conn_panel, bg=self.PANEL_BG)
        right_block.pack(side="left", fill="both", expand=True, padx=(6, 8), pady=10)
        self._build_labeled_row(left_block, 0, "本地地址", self.bit_api)
        self._build_labeled_row(right_block, 0, "浏览器 API", self.api_token)

        control_panel = tk.Frame(canvas, bg=self.PANEL_BG, highlightthickness=1, highlightbackground=self.BORDER_COLOR, height=70)
        control_panel.pack(fill="x", padx=6, pady=(0, 6))
        control_panel.pack_propagate(False)
        left_control = tk.Frame(control_panel, bg=self.PANEL_BG)
        left_control.pack(side="left", fill="y", expand=True, padx=(8, 6), pady=10)
        right_control = tk.Frame(control_panel, bg=self.PANEL_BG)
        right_control.pack(side="left", fill="y", expand=True, padx=(6, 8), pady=10)

        tk.Label(left_control, text="发送频率", font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").grid(row=0, column=0, sticky="w")
        self.min_interval = self._create_entry(left_control, tk.StringVar(), width=6, justify="center")
        self.min_interval.grid(row=0, column=1, padx=(12, 6), ipady=4)
        tk.Label(left_control, text="—", font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").grid(row=0, column=2, padx=4)
        self.max_interval = self._create_entry(left_control, tk.StringVar(), width=6, justify="center")
        self.max_interval.grid(row=0, column=3, padx=(4, 0), ipady=4)

        tk.Label(right_control, text="窗口号", font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").grid(row=0, column=0, sticky="w")
        self.window_no_entry = self._create_entry(right_control, self.window_no_text, width=8, justify="center")
        self.window_no_entry.grid(row=0, column=1, padx=(12, 10), ipady=4)
        self.single_run_btn = self._create_action_button(right_control, "单独启动", "#4f7df0", self._trigger_single_run, width=13)
        self.single_run_btn.grid(row=0, column=2)
        self._register_feedback_button("single", self.single_run_btn, "单独启动", managed=False)

        message_panel = tk.Frame(canvas, bg=self.PANEL_BG, highlightthickness=1, highlightbackground=self.BORDER_COLOR, height=146)
        message_panel.pack(fill="x", padx=6, pady=(0, 6))
        message_panel.pack_propagate(False)
        tk.Label(message_panel, text="发送文本", font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").pack(anchor="w", padx=10, pady=(8, 6))
        message_body = tk.Frame(message_panel, bg=self.PANEL_BG)
        message_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.message_text = tk.Text(
            message_body,
            font=("Consolas", 12),
            bg="#ffffff",
            fg="#24416e",
            relief="solid",
            bd=1,
            highlightthickness=1,
            highlightbackground="#4f7df0",
            highlightcolor="#4f7df0",
            wrap="word",
            height=5,
        )
        self.message_text.pack(side="left", fill="both", expand=True)
        message_scroll = tk.Scrollbar(message_body, orient="vertical", command=self.message_text.yview)
        message_scroll.pack(side="right", fill="y")
        self.message_text.configure(yscrollcommand=message_scroll.set)

        log_panel = tk.Frame(canvas, bg=self.PANEL_BG, highlightthickness=1, highlightbackground=self.BORDER_COLOR)
        log_panel.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        log_header = tk.Frame(log_panel, bg=self.PANEL_BG)
        log_header.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(log_header, text="实时日志", font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").pack(side="left")
        counters = tk.Frame(log_header, bg=self.PANEL_BG)
        counters.pack(side="right")
        tk.Label(counters, text="已发送：", font=self.NOTICE_FONT, bg=self.PANEL_BG, fg="#24416e").pack(side="left")
        tk.Label(counters, textvariable=self.sent_count_text, font=self.NOTICE_FONT, bg=self.PANEL_BG, fg="#24416e").pack(side="left", padx=(0, 12))
        tk.Label(counters, text="失败：", font=self.NOTICE_FONT, bg=self.PANEL_BG, fg="#24416e").pack(side="left")
        tk.Label(counters, textvariable=self.failed_count_text, font=self.NOTICE_FONT, bg=self.PANEL_BG, fg="#24416e").pack(side="left")

        log_body = tk.Frame(log_panel, bg=self.PANEL_BG)
        log_body.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log_text = tk.Text(
            log_body,
            font=self.TEXT_FONT,
            bg="#233f6f",
            fg="#ffffff",
            insertbackground="#ffffff",
            relief="solid",
            bd=1,
            wrap="none",
            spacing1=1,
            spacing3=1,
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        y_scroll = tk.Scrollbar(log_body, orient="vertical", command=self.log_text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = tk.Scrollbar(log_body, orient="horizontal", command=self.log_text.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        log_body.grid_rowconfigure(0, weight=1)
        log_body.grid_columnconfigure(0, weight=1)
        self.log_text.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.log_text.bind("<Key>", self._block_log_edit)
        self.log_text.bind("<<Paste>>", lambda _event: "break")
        self.log_text.bind("<MouseWheel>", self._update_log_follow_state)
        self.log_text.bind("<Button-4>", self._update_log_follow_state)
        self.log_text.bind("<Button-5>", self._update_log_follow_state)
        self.log_text.bind("<ButtonRelease-1>", self._update_log_follow_state)
        self.log_text.bind("<KeyRelease>", self._update_log_follow_state)

    def _build_labeled_row(self, parent, row, label_text, text_var):
        parent.grid_columnconfigure(1, weight=1)
        tk.Label(parent, text=label_text, font=self.LABEL_FONT, bg=self.PANEL_BG, fg="#24416e").grid(
            row=row,
            column=0,
            sticky="w",
        )
        entry = self._create_entry(parent, text_var)
        entry.grid(row=row, column=1, sticky="ew", padx=(12, 0), ipady=4)
        return entry

    def _create_entry(self, parent, text_var, width=None, justify="left"):
        return tk.Entry(
            parent,
            textvariable=text_var,
            width=width,
            justify=justify,
            font=("Microsoft YaHei", 11),
            relief="solid",
            bd=1,
            highlightthickness=1,
            highlightbackground=self.BORDER_COLOR,
            highlightcolor="#5a8fe3",
            bg="#ffffff",
            fg="#24416e",
        )

    def _create_action_button(self, parent, text, bg, command, fg="#ffffff", width=9):
        return tk.Button(
            parent,
            text=text,
            font=("Microsoft YaHei", 10, "bold"),
            bg=bg,
            fg=fg,
            activebackground=bg,
            activeforeground=fg,
            relief="flat",
            bd=0,
            width=width,
            cursor="hand2",
            command=command,
            padx=0,
            pady=6,
        )

    def _register_feedback_button(self, key, button, default_text, managed):
        self._button_texts[key] = default_text
        self._button_restore_modes[key] = managed
        button._feedback_key = key

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
        templates_text = ""
        message_path = ROOT_DIR / "config" / "messages.yaml"
        if message_path.exists():
            try:
                data = yaml.safe_load(message_path.read_text(encoding="utf-8")) or {}
                templates = [
                    str(item).strip()
                    for item in (data.get("templates") or [])
                    if str(item).strip()
                ]
                templates_text = "\n".join(templates)
            except Exception:
                templates_text = ""
        if self.message_text is not None:
            self.message_text.delete("1.0", tk.END)
            if templates_text:
                self.message_text.insert("1.0", templates_text)

    def _reset_log_session(self):
        self._log_offset = 0
        self._log_follow_tail = True
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_path.write_text("", encoding="utf-8")
        except Exception:
            pass
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

    def _append_worker_log(self):
        if self.log_text is None:
            return
        if not self._log_path.exists():
            return
        try:
            with self._log_path.open("r", encoding="utf-8", errors="ignore") as handle:
                handle.seek(self._log_offset)
                chunk = handle.read()
                self._log_offset = handle.tell()
        except Exception:
            return
        if not chunk:
            return
        self.log_text.insert(tk.END, chunk)
        if self._log_follow_tail:
            self.log_text.see(tk.END)

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
            self.client_config.get("task_settings", {}).get("send_interval_seconds") or [2, 6]
        )
        min_value = str(_parse_int(send_interval[0] if len(send_interval) > 0 else 2, 2))
        max_value = str(_parse_int(send_interval[1] if len(send_interval) > 1 else 6, 6))
        self.min_interval.delete(0, tk.END)
        self.min_interval.insert(0, min_value)
        self.max_interval.delete(0, tk.END)
        self.max_interval.insert(0, max_value)
        owner = str(self.license_data.get("owner") or "").strip()
        license_expired = _license_is_expired(self.license_data)
        token_expired = _token_is_expired(self.client_config.get("token_expires_at"))
        self.verified = _client_can_use_token(self.client_config, self.license_data)
        self._update_meta_text(owner=owner)
        if license_expired:
            self._clear_local_auth(preserve_license=True)
            self.activation_text.set("授权已到期")
            self.run_status_text.set("请重新输入激活码完成验证")
            self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
        elif token_expired:
            self._clear_local_auth(preserve_license=True)
            self.activation_text.set("激活已失效")
            self.run_status_text.set("请重新输入激活码完成验证")
            self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
        elif self.verified:
            self.activation_text.set(f"已激活：{owner}" if owner else "已激活")
            self.refresh_remote_status(silent=True)
            if self.verified:
                self._show_operation_screen()
        else:
            self.activation_text.set("未激活")
            self.run_status_text.set("请先完成激活验证")
            self._show_activation_screen(
                "验证成功后自动进入操作界面",
                error=False,
            )
        self._apply_button_state(agent_online=False, task_running=False, task_pending=False)

    def _update_meta_text(self, owner=""):
        self.server_text.set(f"服务器：{self.server_url or DEFAULT_SERVER_URL}")
        self.expire_text.set(
            f"到期时间：{_format_expire_at((self.license_data or {}).get('expire_at'))}"
        )
        owner_suffix = f" | 归属：{owner}" if owner else ""
        self.device_text.set(f"设备ID：{self.machine_id}{owner_suffix}")

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
        self._update_meta_text(owner=str(self.license_data.get("owner") or "").strip())

    def _store_local_auth(self, agent_token, token_expires_at):
        config = self._build_client_config()
        config["agent_token"] = str(agent_token or "").strip()
        config["token_expires_at"] = token_expires_at
        save_client_config(config)
        self.client_config = config
        self.server_url = _normalize_server_url(config.get("server_url") or DEFAULT_SERVER_URL)
        self.machine_id = str(config.get("machine_id") or self.machine_id).strip()

    def _collect_send_interval(self):
        min_value = _parse_int(self.min_interval.get(), 2)
        max_value = _parse_int(self.max_interval.get(), 6)
        if min_value <= 0 or max_value <= 0:
            raise ValueError("发送频率必须大于 0")
        if min_value > max_value:
            min_value, max_value = max_value, min_value
        return [min_value, max_value]

    def _build_client_config(self):
        interval = self._collect_send_interval()
        config = load_client_config()
        config["server_url"] = self.server_url or DEFAULT_SERVER_URL
        config["machine_id"] = self.machine_id
        config["agent_token"] = str(config.get("agent_token") or self.client_config.get("agent_token") or "").strip()
        config["token_expires_at"] = config.get("token_expires_at", self.client_config.get("token_expires_at"))
        config["bit_api"] = _normalize_bit_api(self.bit_api.get())
        config["api_token"] = str(self.api_token.get() or "").strip()
        config.setdefault("task_settings", {})
        config["task_settings"]["send_interval_seconds"] = interval
        return config

    def _save_local_config(self, require_connection_fields=False):
        config = self._build_client_config()
        if require_connection_fields:
            if not config["bit_api"] or not config["api_token"]:
                messagebox.showerror("提示", "BitBrowser 地址与 API Token 不能为空")
                return False
        save_client_config(config)
        self.client_config = config
        self.server_url = _normalize_server_url(config.get("server_url") or DEFAULT_SERVER_URL)
        self.machine_id = str(config.get("machine_id") or self.machine_id).strip()
        self._update_meta_text(owner=str(self.license_data.get("owner") or "").strip())
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
        status = self.refresh_remote_status(silent=True)
        if status and status.get("agent_online"):
            return True
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
        if sys.platform.startswith("win"):
            creationflags = 0x00000008 | 0x00000200
        else:
            kwargs["start_new_session"] = True
        try:
            self.worker_process = subprocess.Popen(
                worker_cmd,
                stdout=log_path.open("a", encoding="utf-8"),
                stderr=log_path.open("a", encoding="utf-8"),
                creationflags=creationflags,
                cwd=str(ROOT_DIR),
                env={**os.environ, ENV_RUNTIME_DIR: runtime_dir},
                **kwargs,
            )
        except Exception as exc:
            messagebox.showerror("提示", f"启动 worker 失败：{exc}")
            return False

        if not wait_online:
            return True
        for _ in range(8):
            time.sleep(1)
            status = self.refresh_remote_status(silent=True)
            if status and status.get("agent_online"):
                return True
        messagebox.showwarning("提示", "worker 已启动，但执行端尚未连上服务器")
        return False

    def _apply_button_state(self, agent_online, task_running, task_pending):
        self._button_state_snapshot = {
            "agent_online": bool(agent_online),
            "task_running": bool(task_running),
            "task_pending": bool(task_pending),
        }
        is_busy = bool(task_running or task_pending)
        self.run_btn.config(state="normal" if self.verified and not is_busy else "disabled")
        self.stop_btn.config(state="normal" if agent_online and is_busy else "disabled")
        self.pause_btn.config(state="normal" if agent_online and is_busy else "disabled")
        self.resume_btn.config(state="normal" if agent_online else "disabled")

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
                if error_code == "account_expired":
                    self.activation_text.set("授权已到期")
                elif error_code == "account_disabled":
                    self.activation_text.set("账户已禁用")
                else:
                    self.activation_text.set("激活已失效")
                self.run_status_text.set("请重新输入激活码完成验证")
                self._apply_button_state(agent_online=False, task_running=False, task_pending=False)
                self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
                return None
            if not silent:
                self.run_status_text.set(
                    f"执行端状态获取失败：{_translate_client_error(data.get('error'))}"
                )
            self._apply_button_state(agent_online=False, task_running=False, task_pending=False)
            return None

        info = data.get("data") or {}
        owner = str(info.get("owner") or self.license_data.get("owner") or "").strip()
        if owner:
            self.activation_text.set(f"已激活：{owner}")
        state_text = str(info.get("state_text") or "状态未知").strip()
        config_text = "配置已同步" if info.get("config_synced") else "配置未同步"
        bind_text = "自动绑定完成" if info.get("device_bound") else "等待绑定"
        self.run_status_text.set(f"{state_text} | {config_text} | {bind_text}")
        self._update_meta_text(owner=owner)
        self._apply_button_state(
            agent_online=bool(info.get("agent_online")),
            task_running=bool(info.get("task_running")),
            task_pending=bool(info.get("task_pending")),
        )
        return info

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
        button.config(text=busy_text, state="disabled", relief="sunken")
        self.root.configure(cursor="watch")
        self.root.update_idletasks()
        try:
            callback()
        finally:
            if button.winfo_exists():
                button.config(text=default_text, relief="flat")
                if managed:
                    self._apply_button_state(**self._button_state_snapshot)
                else:
                    button.config(state=previous_state if previous_state in {"normal", "disabled"} else "normal")
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
        self._with_button_feedback("single", "处理中...", self._show_single_run_hint)

    def _show_single_run_hint(self):
        messagebox.showinfo("提示", "单独启动当前版本仅保留界面入口，请使用正式启动链。")

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
                self.activation_text.set("授权已到期")
                self.run_status_text.set("账户已到期，请联系管理员续费")
                self._show_activation_screen(self.SESSION_NOTICE_TEXT, error=True)
            else:
                self.activation_text.set("未激活")
                self.run_status_text.set("激活失败，请重新输入有效激活码")
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
        self.activation_text.set(f"已激活：{self.license_data.get('owner') or ''}")
        self.run_status_text.set("激活成功，请继续配置本地连接参数后开始运行")
        self._update_meta_text(owner=str(self.license_data.get("owner") or "").strip())
        self._show_operation_screen()
        local_bit_api = _normalize_bit_api(self.bit_api.get())
        local_api_token = str(self.api_token.get() or "").strip()
        if local_bit_api and local_api_token:
            self._sync_config_to_server(show_error=False)
            self._ensure_worker_running(wait_online=False)
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
            return
        if not self._save_local_config(require_connection_fields=True):
            return
        if action == "start":
            if not self._sync_config_to_server(show_error=True):
                return
            if not self._ensure_worker_running(wait_online=True):
                return
        payload = {
            "agent_token": self._current_agent_token(),
            "machine_id": self.machine_id,
        }
        ok, data = self._post_client_api(f"/api/client/{action}", payload, timeout=15)
        if not ok:
            messagebox.showerror("提示", f"{action} 失败：{data.get('error')}")
            return
        self.refresh_remote_status(silent=True)

    def on_run(self):
        self._send_control("start")

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
        self.root.destroy()

def main():
    root = tk.Tk()
    app = ClientApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
