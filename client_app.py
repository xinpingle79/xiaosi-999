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
DEFAULT_BIT_API = "http://127.0.0.1:54345"
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
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("FB RPA 客户端")
        self.root.geometry("760x620")
        self.root.resizable(False, False)
        self.root.configure(bg="#f2f5f9")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

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
        self.verified = False
        self.activation_screen = None
        self.ops_screen = None

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
        self.activation_notice_label = None

        self._build_ui()
        self._load_existing_state()
        self._schedule_status_refresh(initial=True)

    def _build_ui(self):
        container = tk.Frame(self.root, bg="#f2f5f9")
        container.pack(fill="both", expand=True, padx=18, pady=18)

        self._build_activation_screen(container)
        self._build_operation_screen(container)

    def _build_activation_screen(self, parent):
        font_title = ("Microsoft YaHei", 16, "bold")
        font_label = ("Microsoft YaHei", 11)
        font_small = ("Microsoft YaHei", 10)

        self.activation_screen = tk.Frame(parent, bg="#f2f5f9")
        card = tk.Frame(
            self.activation_screen,
            bg="#ffffff",
            highlightthickness=1,
            highlightbackground="#dbe3ef",
            bd=0,
        )
        card.pack(fill="both", expand=True)

        tk.Label(
            card,
            text="FB RPA 客户端",
            font=font_title,
            bg="#ffffff",
            fg="#22324d",
        ).pack(pady=(32, 10))
        tk.Label(
            card,
            text="首次使用请先输入激活码，验证成功后才能进入操作界面。",
            font=font_small,
            bg="#ffffff",
            fg="#64748b",
        ).pack(pady=(0, 14))

        summary_frame = tk.Frame(
            card,
            bg="#f8fafc",
            highlightthickness=1,
            highlightbackground="#e2e8f0",
        )
        summary_frame.pack(fill="x", padx=20, pady=(0, 14))
        tk.Label(
            summary_frame,
            textvariable=self.server_text,
            font=font_small,
            bg="#f8fafc",
            fg="#4b5563",
        ).pack(anchor="w", padx=12, pady=(10, 4))
        tk.Label(
            summary_frame,
            textvariable=self.device_text,
            font=font_small,
            bg="#f8fafc",
            fg="#4b5563",
        ).pack(anchor="w", padx=12, pady=2)
        tk.Label(
            summary_frame,
            textvariable=self.expire_text,
            font=font_small,
            bg="#f8fafc",
            fg="#4b5563",
        ).pack(anchor="w", padx=12, pady=(2, 10))

        activation_frame = tk.Frame(card, bg="#ffffff")
        activation_frame.pack(fill="x", padx=20, pady=(0, 8))
        tk.Label(
            activation_frame,
            text="激活码",
            font=font_label,
            bg="#ffffff",
            fg="#22324d",
        ).grid(row=0, column=0, sticky="w")
        self.code_entry = tk.Entry(
            activation_frame,
            textvariable=self.activation_code,
            width=34,
            font=font_label,
            relief="solid",
            bd=1,
        )
        self.code_entry.grid(row=0, column=1, padx=(12, 12), ipady=6, sticky="ew")
        self.code_entry.bind("<Return>", lambda _event: self.on_verify())
        tk.Button(
            activation_frame,
            text="验证",
            font=font_label,
            bg="#4f7df0",
            fg="#ffffff",
            activebackground="#3f6de0",
            relief="flat",
            width=10,
            command=self.on_verify,
        ).grid(row=0, column=2)
        activation_frame.grid_columnconfigure(1, weight=1)
        self.activation_notice_label = tk.Label(
            card,
            textvariable=self.activation_notice_text,
            font=font_small,
            bg="#ffffff",
            fg="#475467",
            justify="left",
            wraplength=480,
        )
        self.activation_notice_label.pack(fill="x", padx=20, pady=(0, 12))

    def _build_operation_screen(self, parent):
        font_title = ("Microsoft YaHei", 16, "bold")
        font_label = ("Microsoft YaHei", 11)
        font_small = ("Microsoft YaHei", 10)

        self.ops_screen = tk.Frame(parent, bg="#f2f5f9")

        card = tk.Frame(
            self.ops_screen,
            bg="#ffffff",
            highlightthickness=1,
            highlightbackground="#dbe3ef",
            bd=0,
        )
        card.pack(fill="both", expand=True)

        tk.Label(
            card,
            text="FB RPA 客户端",
            font=font_title,
            bg="#ffffff",
            fg="#22324d",
        ).pack(pady=(18, 10))

        meta_frame = tk.Frame(card, bg="#f8fafc", highlightthickness=1, highlightbackground="#e2e8f0")
        meta_frame.pack(fill="x", padx=20, pady=(0, 14))
        tk.Label(
            meta_frame,
            textvariable=self.activation_text,
            font=font_small,
            bg="#f8fafc",
            fg="#0f766e",
        ).pack(anchor="w", padx=12, pady=(10, 4))
        tk.Label(
            meta_frame,
            textvariable=self.server_text,
            font=font_small,
            bg="#f8fafc",
            fg="#4b5563",
        ).pack(anchor="w", padx=12, pady=2)
        tk.Label(
            meta_frame,
            textvariable=self.expire_text,
            font=font_small,
            bg="#f8fafc",
            fg="#4b5563",
        ).pack(anchor="w", padx=12, pady=2)
        tk.Label(
            meta_frame,
            textvariable=self.device_text,
            font=font_small,
            bg="#f8fafc",
            fg="#4b5563",
        ).pack(anchor="w", padx=12, pady=(2, 10))

        config_frame = tk.Frame(card, bg="#ffffff")
        config_frame.pack(fill="x", padx=20)
        tk.Label(config_frame, text="BitBrowser 地址", font=font_label, bg="#ffffff").grid(
            row=0, column=0, sticky="w", pady=(0, 10)
        )
        tk.Entry(
            config_frame,
            textvariable=self.bit_api,
            font=font_label,
            relief="solid",
            bd=1,
        ).grid(row=0, column=1, sticky="ew", pady=(0, 10), ipady=6)
        tk.Label(config_frame, text="API Token", font=font_label, bg="#ffffff").grid(
            row=1, column=0, sticky="w", pady=(0, 10)
        )
        tk.Entry(
            config_frame,
            textvariable=self.api_token,
            font=font_label,
            relief="solid",
            bd=1,
            show="*",
        ).grid(row=1, column=1, sticky="ew", pady=(0, 10), ipady=6)
        config_frame.grid_columnconfigure(1, weight=1)

        interval_frame = tk.Frame(card, bg="#ffffff")
        interval_frame.pack(fill="x", padx=20, pady=(0, 12))
        tk.Label(interval_frame, text="发送频率", font=font_label, bg="#ffffff").grid(
            row=0, column=0, sticky="w"
        )
        self.min_interval = ttk.Combobox(
            interval_frame,
            width=6,
            values=["2", "5", "10", "30", "60", "90", "120"],
            state="readonly",
        )
        self.min_interval.grid(row=0, column=1, padx=(12, 8))
        tk.Label(interval_frame, text="~", font=font_label, bg="#ffffff").grid(
            row=0, column=2, padx=(0, 8)
        )
        self.max_interval = ttk.Combobox(
            interval_frame,
            width=6,
            values=["6", "10", "20", "60", "90", "120", "180"],
            state="readonly",
        )
        self.max_interval.grid(row=0, column=3, padx=(0, 8))
        tk.Label(interval_frame, text="秒", font=font_label, bg="#ffffff").grid(
            row=0, column=4, padx=(0, 16)
        )
        tk.Button(
            interval_frame,
            text="保存本地配置",
            font=font_label,
            bg="#ffffff",
            fg="#22324d",
            relief="solid",
            bd=1,
            width=16,
            command=self.on_save_settings,
        ).grid(row=0, column=5)

        status_card = tk.Frame(card, bg="#f8fafc", highlightthickness=1, highlightbackground="#e2e8f0")
        status_card.pack(fill="x", padx=20, pady=(4, 14))
        tk.Label(
            status_card,
            textvariable=self.run_status_text,
            font=font_small,
            bg="#f8fafc",
            fg="#374151",
            anchor="w",
            justify="left",
        ).pack(fill="x", padx=12, pady=12)

        action_row = tk.Frame(card, bg="#ffffff")
        action_row.pack(pady=(4, 0))
        self.run_btn = tk.Button(
            action_row,
            text="运行",
            font=font_label,
            bg="#4f7df0",
            fg="#ffffff",
            activebackground="#3f6de0",
            relief="flat",
            width=10,
            command=self.on_run,
        )
        self.run_btn.grid(row=0, column=0, padx=8)
        self.stop_btn = tk.Button(
            action_row,
            text="停止",
            font=font_label,
            bg="#ef4444",
            fg="#ffffff",
            activebackground="#dc2626",
            relief="flat",
            width=10,
            command=self.on_stop,
            state="disabled",
        )
        self.stop_btn.grid(row=0, column=1, padx=8)
        self.pause_btn = tk.Button(
            action_row,
            text="暂停",
            font=font_label,
            bg="#f59e0b",
            fg="#ffffff",
            activebackground="#d97706",
            relief="flat",
            width=10,
            command=self.on_pause,
            state="disabled",
        )
        self.pause_btn.grid(row=0, column=2, padx=8)
        self.resume_btn = tk.Button(
            action_row,
            text="继续",
            font=font_label,
            bg="#10b981",
            fg="#ffffff",
            activebackground="#059669",
            relief="flat",
            width=10,
            command=self.on_resume,
            state="disabled",
        )
        self.resume_btn.grid(row=0, column=3, padx=8)

    def _set_activation_notice(self, message="", error=False):
        self.activation_notice_text.set(str(message or "").strip())
        if self.activation_notice_label:
            self.activation_notice_label.configure(fg="#b42318" if error else "#475467")

    def _show_activation_screen(self, message="", error=False):
        if self.ops_screen:
            self.ops_screen.pack_forget()
        if self.activation_screen:
            self.activation_screen.pack(fill="both", expand=True)
        self.root.geometry("560x320")
        self._set_activation_notice(message, error=error)
        try:
            self.code_entry.focus_set()
        except Exception:
            pass

    def _show_operation_screen(self):
        if self.activation_screen:
            self.activation_screen.pack_forget()
        if self.ops_screen:
            self.ops_screen.pack(fill="both", expand=True)
        self.root.geometry("760x620")
        self._set_activation_notice("", error=False)

    def _load_existing_state(self):
        send_interval = (
            self.client_config.get("task_settings", {}).get("send_interval_seconds") or [2, 6]
        )
        min_value = str(_parse_int(send_interval[0] if len(send_interval) > 0 else 2, 2))
        max_value = str(_parse_int(send_interval[1] if len(send_interval) > 1 else 6, 6))
        self.min_interval.set(min_value)
        self.max_interval.set(max_value)
        owner = str(self.license_data.get("owner") or "").strip()
        license_expired = _license_is_expired(self.license_data)
        token_expired = _token_is_expired(self.client_config.get("token_expires_at"))
        self.verified = _client_can_use_token(self.client_config, self.license_data)
        self._update_meta_text(owner=owner)
        if license_expired:
            self._clear_local_auth(preserve_license=True)
            self.activation_text.set("授权已到期")
            self.run_status_text.set("账户已到期，请重新输入激活码验证")
            self._show_activation_screen(
                "当前账号已到期，请联系管理员续费或输入新的有效激活码后重新验证。",
                error=True,
            )
        elif token_expired:
            self._clear_local_auth(preserve_license=True)
            self.activation_text.set("激活已失效")
            self.run_status_text.set("本地凭证已过期，请重新输入激活码验证")
            self._show_activation_screen(
                "当前本地凭证已过期，请重新输入激活码完成验证。",
                error=True,
            )
        elif self.verified:
            self.activation_text.set(f"已激活：{owner}" if owner else "已激活")
            self._show_operation_screen()
        else:
            self.activation_text.set("未激活")
            self.run_status_text.set("请先完成激活验证")
            self._show_activation_screen(
                "首次使用请先输入激活码，验证成功后才能进入操作界面。"
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
                self._show_activation_screen(_translate_client_error(error_code), error=True)
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
        self.root.destroy()


def main():
    root = tk.Tk()
    app = ClientApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
