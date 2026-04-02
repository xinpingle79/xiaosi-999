import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path

import requests
import yaml

APP_NAME = "小四客户端"
IS_FROZEN = getattr(sys, "frozen", False)
DEFAULT_SERVER_URL = "http://154.64.255.139:43090"


def _find_root_dir():
    if IS_FROZEN:
        return Path(sys.executable).resolve().parent
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "main.py").exists():
            return parent
    return current.parent


ROOT_DIR = _find_root_dir()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

ENV_RUNTIME_DIR = "FB_RPA_RUNTIME_DIR"
ENV_SELECTED_WINDOW_GROUPS_FILE = "FB_RPA_SELECTED_WINDOW_GROUPS_FILE"
_RUNTIME_DIR = ""
DEFAULT_BIT_API = ""
DEFAULT_POLL_INTERVAL = 2.0
LEGACY_POPUP_STOP_RULE_GROUP_KEYS = (
    "stranger_limit",
    "message_request_limit",
)
LEGACY_PERMANENT_SKIP_RULE_GROUP_KEYS = (
    "messenger_login_required",
    "account_cannot_message",
)
SERVER_KEY_LOG_SUBSTRINGS = (
    "已自动识别 ",
    "个比特浏览器窗口，将顺序接入",
    "未获取到任何可用的比特浏览器窗口，任务结束。",
    "检测到 Facebook checkpoint 账号异常页",
    "温馨提示：",
    "已成功接入 BitBrowser 会话，开始按小组顺序执行成员私聊...",
    "✅ 成员页消息已发送:",
    "处理成员失败，保留原断点等待下次重试:",
    "断点保留：成员 ",
    "检测到静默失败信号:",
    "点赞型探针未恢复窗口健康，继续保留静默失败计数:",
    "检测到账号异常或风控限制，立即停止当前账号:",
    "当前窗口收到停止信号，已结束当前任务。",
    "当前窗口因账号异常或风控已停止",
    "当前窗口连接浏览器失败，未能启动发送任务。",
    "当前窗口未能生成窗口级运行标识，已跳过本窗口。",
    "当前窗口正式发送文案为空，未能启动发送任务。",
    "当前窗口未能收集到任何已加入小组，流程已结束。",
    "当前窗口未配置任何已选群，已跳过正式运行。",
    "当前运行未命中窗口白名单，已跳过正式运行。",
    "当前窗口白名单中的群未出现在已加入小组列表中，未启动正式运行。",
    "当前窗口因陌生消息额度上限已停止",
    "当前窗口因消息请求发送上限已停止",
    "当前窗口因当前无法发送已停止",
    "当前窗口因聊天窗口状态异常已停止",
    "当前窗口已达到发送额度上限，停止继续处理后续成员",
    "当前窗口账号已禁用，已停止运行",
    "当前窗口任务已正常完成。",
    "当前窗口任务已结束，完成 ",
    "当前任务结束仅退出执行进程，保留 BitBrowser 窗口。",
    "本轮全部窗口已处理结束，",
)


def _user_data_root() -> Path:
    local_appdata = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
    if local_appdata:
        return Path(local_appdata) / APP_NAME
    return Path.home() / "AppData" / "Local" / APP_NAME


def _client_config_path() -> Path:
    if IS_FROZEN:
        return _user_data_root() / "config" / "client.yaml"
    return ROOT_DIR / "config" / "client.yaml"


CLIENT_CONFIG_PATH = _client_config_path()


def _set_runtime_dir(value: str) -> None:
    global _RUNTIME_DIR
    _RUNTIME_DIR = (value or "").strip()


def _runtime_dir() -> Path:
    runtime_root = (os.environ.get(ENV_RUNTIME_DIR) or "").strip() or _RUNTIME_DIR
    if runtime_root:
        base = Path(runtime_root).expanduser()
    elif IS_FROZEN:
        base = _user_data_root() / "runtime"
    else:
        base = ROOT_DIR / "runtime"
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


def _stop_flag_path(owner):
    base = "stop_default.flag"
    if owner:
        base = f"stop_{_sanitize_owner(owner)}.flag"
    return _runtime_path("data", base)


def _pause_flag_path(owner):
    base = "pause_default.flag"
    if owner:
        base = f"pause_{_sanitize_owner(owner)}.flag"
    return _runtime_path("data", base)


def _selected_window_groups_path(owner, task_id=None, window_token=""):
    owner_part = _sanitize_owner(owner) or "default"
    token_part = _sanitize_owner(window_token) or "all"
    task_part = _sanitize_owner(task_id) if task_id not in (None, "") else ""
    suffix = f"_{task_part}" if task_part else ""
    return _runtime_path(
        "data",
        f"selected_window_groups_{owner_part}_{token_part}{suffix}.json",
    )


def _collect_task_payload_path(owner, task_id=None):
    owner_part = _sanitize_owner(owner) or "default"
    task_part = _sanitize_owner(task_id) if task_id not in (None, "") else "collect"
    return _runtime_path(
        "data",
        f"collect_task_payload_{owner_part}_{task_part}.json",
    )


def _runtime_messages_path():
    return _runtime_path("data", "runtime_messages.yaml")


def _load_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_yaml(path):
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _load_license():
    data = _load_json(_license_path())
    cleaned = {
        "owner": data.get("owner") or "",
        "expire_at": data.get("expire_at"),
        "max_devices": data.get("max_devices"),
    }
    if cleaned != data:
        _save_license(cleaned)
    return cleaned


def _save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_yaml(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _save_license(data):
    cleaned = {
        "owner": (data or {}).get("owner") or "",
        "expire_at": (data or {}).get("expire_at"),
        "max_devices": (data or {}).get("max_devices"),
    }
    _save_json(_license_path(), cleaned)


def _default_machine_id():
    hostname = socket.gethostname()
    node = uuid.getnode()
    return f"{hostname}-{node:x}"


def _load_client_config(config_path=None):
    target = Path(config_path) if config_path else CLIENT_CONFIG_PATH
    config = _load_yaml(target)
    if not config:
        return {}
    changed = False
    for key in ("activation_code", "register_token", "owner", "expire_at", "max_devices"):
        if key in config:
            config.pop(key, None)
            changed = True
    if "agent_token" in config:
        config["agent_token"] = str(config.get("agent_token") or "").strip()
    if changed:
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )
        except Exception:
            pass
    return config


def _refresh_runtime_config(cfg, config_path=None):
    config = _load_client_config(config_path)
    locked_fields = cfg.get("_locked_fields") or set()
    runtime_dir = str(config.get("runtime_dir") or "").strip()
    if "runtime_dir" not in locked_fields and runtime_dir:
        cfg["runtime_dir"] = runtime_dir
        _set_runtime_dir(runtime_dir)
        os.environ[ENV_RUNTIME_DIR] = runtime_dir
    elif cfg.get("runtime_dir"):
        _set_runtime_dir(str(cfg["runtime_dir"]))
        os.environ[ENV_RUNTIME_DIR] = str(cfg["runtime_dir"])

    server_url = str(config.get("server_url") or "").strip().rstrip("/")
    if "server_url" not in locked_fields and server_url:
        cfg["server_url"] = server_url
    bit_api = str(config.get("bit_api") or "").strip()
    if "bit_api" not in locked_fields and bit_api:
        cfg["bit_api"] = bit_api
    api_token = str(config.get("api_token") or "").strip()
    if "api_token" not in locked_fields and api_token:
        cfg["api_token"] = api_token
    machine_id = str(config.get("machine_id") or "").strip()
    if "machine_id" not in locked_fields and machine_id:
        cfg["machine_id"] = machine_id
    if "agent_token" not in locked_fields:
        agent_token = str(config.get("agent_token") or "").strip()
        if agent_token or not cfg.get("agent_token"):
            cfg["agent_token"] = agent_token
    client_version = str(config.get("client_version") or "").strip()
    if "client_version" not in locked_fields and client_version:
        cfg["client_version"] = client_version
    poll_interval = config.get("poll_interval")
    if poll_interval not in (None, ""):
        try:
            cfg["poll_interval"] = float(poll_interval)
        except Exception:
            pass


def _build_config(args):
    config = _load_client_config(args.config)
    license_data = _load_license()
    server_url = (
        args.server
        or os.environ.get("FB_RPA_SERVER_URL")
        or config.get("server_url")
        or DEFAULT_SERVER_URL
    )
    bit_api = (
        args.bit_api
        or os.environ.get("FB_RPA_BIT_API")
        or config.get("bit_api")
        or DEFAULT_BIT_API
    )
    api_token = (
        args.api_token
        or os.environ.get("FB_RPA_API_TOKEN")
        or config.get("api_token")
        or ""
    )
    owner = (
        args.owner
        or os.environ.get("FB_RPA_AGENT_OWNER")
        or license_data.get("owner")
        or ""
    )
    label = (
        args.label
        or os.environ.get("FB_RPA_AGENT_LABEL")
        or config.get("label")
        or socket.gethostname()
    )
    poll_interval = float(
        args.poll_interval
        or os.environ.get("FB_RPA_AGENT_POLL")
        or config.get("poll_interval")
        or DEFAULT_POLL_INTERVAL
    )
    runtime_dir = (
        args.runtime_dir
        or os.environ.get(ENV_RUNTIME_DIR)
        or config.get("runtime_dir")
        or ""
    )
    if runtime_dir:
        _set_runtime_dir(runtime_dir)
        os.environ[ENV_RUNTIME_DIR] = runtime_dir
    client_version = (
        args.client_version
        or os.environ.get("FB_RPA_CLIENT_VERSION")
        or config.get("client_version")
        or ""
    )
    machine_id = (
        args.machine_id
        or os.environ.get("FB_RPA_AGENT_ID")
        or config.get("machine_id")
        or _default_machine_id()
    )
    if license_data.get("expire_at") is not None:
        config["expire_at"] = license_data.get("expire_at")
    if license_data.get("max_devices") is not None:
        config["max_devices"] = license_data.get("max_devices")
    agent_token = str(args.agent_token or config.get("agent_token") or "").strip()
    locked_fields = {
        key
        for key, value in {
            "server_url": args.server,
            "bit_api": args.bit_api,
            "api_token": args.api_token,
            "owner": args.owner,
            "machine_id": args.machine_id,
            "runtime_dir": args.runtime_dir,
            "client_version": args.client_version,
            "agent_token": args.agent_token,
        }.items()
        if value not in (None, "")
    }
    return {
        "server_url": server_url.rstrip("/"),
        "bit_api": bit_api,
        "api_token": api_token,
        "label": label,
        "poll_interval": poll_interval,
        "machine_id": machine_id,
        "owner": owner,
        "runtime_dir": runtime_dir,
        "client_version": client_version,
        "agent_token": agent_token,
        "expire_at": config.get("expire_at"),
        "max_devices": config.get("max_devices"),
        "_locked_fields": locked_fields,
        "config_path": args.config,
    }


def _post_json(url, payload, timeout=10):
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        return response.status_code, response.json()
    except Exception:
        return 0, {}


def _post_agent_json(cfg, url, payload, timeout=10):
    base = dict(payload or {})
    base.setdefault("machine_id", cfg.get("machine_id"))
    if cfg.get("agent_token"):
        base.setdefault("agent_token", cfg.get("agent_token"))
    return _post_json(url, base, timeout=timeout)


def _normalize_message_templates(templates):
    return [
        str(item).strip()
        for item in (templates or [])
        if str(item).strip()
    ]


def _normalize_message_lines(values):
    return [
        str(item).strip()
        for item in (values or [])
        if str(item).strip()
    ]


def _merge_message_lines(*groups):
    merged = []
    seen = set()
    for group in groups:
        for item in _normalize_message_lines(group or []):
            if item in seen:
                continue
            seen.add(item)
            merged.append(item)
    return merged


def _normalize_runtime_messages(messages):
    payload = messages if isinstance(messages, dict) else {}
    popup_stop_texts = _normalize_message_lines(payload.get("popup_stop_texts") or [])
    restriction_payload = payload.get("restriction_detection_texts")
    if not popup_stop_texts and isinstance(restriction_payload, dict):
        popup_stop_texts = _merge_message_lines(
            *[
                restriction_payload.get(key) or []
                for key in LEGACY_POPUP_STOP_RULE_GROUP_KEYS
            ]
        )
    legacy_permanent_skip_texts = []
    if isinstance(restriction_payload, dict):
        legacy_permanent_skip_texts = _merge_message_lines(
            *[
                restriction_payload.get(key) or []
                for key in LEGACY_PERMANENT_SKIP_RULE_GROUP_KEYS
            ]
        )
    return {
        "popup_stop_texts": popup_stop_texts,
        "permanent_skip_texts": _merge_message_lines(
            payload.get("permanent_skip_texts") or [],
            legacy_permanent_skip_texts,
        ),
    }


def _normalize_runtime_task_settings(task_settings):
    if not isinstance(task_settings, dict):
        return {}
    normalized = {}
    for key in (
        "max_messages_per_account",
        "scroll_times",
        "group_list_scroll_times",
        "resume_search_times",
        "idle_scroll_limit",
        "max_windows",
    ):
        if key not in task_settings:
            continue
        try:
            normalized[key] = max(0, int(task_settings.get(key) or 0))
        except Exception:
            continue
    return normalized


def _save_client_config(target, config):
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _sync_runtime_owner_message_templates(owner, messages):
    from utils.helpers import RuntimeDatabase

    owner = str(owner or "").strip()
    if not owner or not isinstance(messages, dict):
        return False
    templates = _normalize_message_templates(messages.get("templates") or [])
    db = RuntimeDatabase(db_path=str(_runtime_path("data", "server.db")))
    try:
        existing = db.get_message_templates(owner) or {}
        existing_templates = _normalize_message_templates(existing.get("templates") or [])
        if existing_templates == templates:
            return False
        if templates:
            db.set_message_templates(owner, templates)
        else:
            db.delete_message_templates(owner)
        return True
    finally:
        db.close()


def _sync_runtime_message_rules(messages):
    normalized = _normalize_runtime_messages(messages)
    path = _runtime_messages_path()
    existing = _normalize_runtime_messages(_load_yaml(path))
    if existing == normalized:
        return False
    _save_yaml(path, normalized)
    return True


def _sync_authoritative_task_settings(config_path=None, task_settings=None):
    normalized = _normalize_runtime_task_settings(task_settings)
    if not normalized:
        return False
    target = Path(config_path) if config_path else CLIENT_CONFIG_PATH
    config = _load_client_config(target)
    if not config:
        config = {}
    current = dict(config.get("task_settings") or {})
    changed = False
    for key, value in normalized.items():
        if current.get(key) != value:
            current[key] = value
            changed = True
    if not changed:
        return False
    config["task_settings"] = current
    _save_client_config(target, config)
    return True


def _sync_runtime_bundle(cfg, config_path=None):
    _, result = _post_agent_json(
        cfg,
        f"{cfg['server_url']}/api/client/status",
        {"machine_id": cfg.get("machine_id") or ""},
        timeout=12,
    )
    if not isinstance(result, dict) or not result.get("ok"):
        return False
    info = result.get("data") or {}
    _sync_authoritative_task_settings(
        config_path=config_path,
        task_settings=info.get("task_settings"),
    )
    _sync_runtime_message_rules(info.get("messages") or {})
    _sync_runtime_owner_message_templates(
        info.get("owner") or cfg.get("owner") or "",
        info.get("messages") or {},
    )
    return True


def _persist_client_auth(config_path=None, agent_token=""):
    target = Path(config_path) if config_path else CLIENT_CONFIG_PATH
    config = _load_client_config(target)
    if not config:
        config = {}
    config["agent_token"] = str(agent_token or "").strip()
    for key in ("activation_code", "register_token", "owner", "expire_at", "max_devices"):
        config.pop(key, None)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _attempt_runtime_token_recovery(cfg, config_path=None):
    owner = str(cfg.get("owner") or "").strip()
    machine_id = str(cfg.get("machine_id") or "").strip()
    api_token = str(cfg.get("api_token") or "").strip()
    bit_api = str(cfg.get("bit_api") or "").strip()
    client_version = str(cfg.get("client_version") or "").strip()
    if not (owner and machine_id and api_token):
        return False

    status_code, data = _post_json(
        f"{cfg['server_url']}/api/client/activate",
        {
            "owner": owner,
            "machine_id": machine_id,
            "bit_api": bit_api,
            "api_token": api_token,
            "client_version": client_version,
        },
        timeout=12,
    )
    if status_code != 200 or not isinstance(data, dict) or not data.get("ok"):
        return False

    info = data.get("data") or {}
    agent_token = str(info.get("agent_token") or "").strip()
    if not agent_token:
        return False

    cfg["agent_token"] = agent_token
    server_url = str(info.get("server_url") or "").strip().rstrip("/")
    if server_url:
        cfg["server_url"] = server_url
    if "expire_at" in info:
        cfg["expire_at"] = info.get("expire_at")
    if "max_devices" in info:
        cfg["max_devices"] = info.get("max_devices")
    _persist_client_auth(config_path=config_path, agent_token=agent_token)
    return True


def _clear_runtime_auth(cfg, config_path=None):
    cfg["agent_token"] = ""
    _persist_client_auth(config_path=config_path, agent_token="")


def _license_expired(expire_at):
    if expire_at in (None, "", 0):
        return False
    try:
        return time.time() > float(expire_at)
    except Exception:
        return False


def _ensure_runtime_token(cfg, config_path=None):
    if _license_expired(cfg.get("expire_at")):
        _clear_runtime_auth(cfg, config_path=config_path)
        print("账号已到期，请先在客户端重新激活")
        return False
    agent_token = str(cfg.get("agent_token") or "").strip()
    if not agent_token:
        if _attempt_runtime_token_recovery(cfg, config_path=config_path):
            return True
        print("缺少有效凭证，请先在客户端输入激活码完成激活")
        return False
    return True


def _runtime_auth_message(error_code):
    mapping = {
        "invalid_token": "执行端认证失效（invalid_token），请回到客户端重新激活。",
        "account_expired": "账号已到期，请回到客户端重新激活。",
        "account_disabled": "账号已禁用，请回到客户端重新激活。",
    }
    return mapping.get(str(error_code or "").strip(), "执行端凭证异常，请回到客户端重新激活。")


def _handle_runtime_auth_failure(worker, cfg, stop_event, error_code, config_path=None):
    message = _runtime_auth_message(error_code)
    print(message)
    _clear_runtime_auth(cfg, config_path=config_path)
    owner = str(cfg.get("owner") or "").strip()
    if worker and owner:
        worker._stop_process(owner)
    stop_event.set()


def _heartbeat_loop(cfg, stop_event, worker, config_path=None):
    while not stop_event.is_set():
        _refresh_runtime_config(cfg, config_path)
        if not _ensure_runtime_token(cfg, config_path=config_path):
            stop_event.set()
            break
        _, result = _post_agent_json(
            cfg,
            f"{cfg['server_url']}/api/agent/heartbeat",
            {
                "machine_id": cfg["machine_id"],
                "client_version": cfg.get("client_version") or "",
                "owner": cfg.get("owner") or "",
                "bit_api": cfg.get("bit_api") or "",
                "api_token": cfg.get("api_token") or "",
            },
        )
        if isinstance(result, dict):
            if result.get("error") == "invalid_token":
                if _attempt_runtime_token_recovery(cfg, config_path=config_path):
                    continue
                _handle_runtime_auth_failure(
                    worker,
                    cfg,
                    stop_event,
                    result.get("error"),
                    config_path=config_path,
                )
                break
            if result.get("error") in ("account_expired", "account_disabled"):
                _handle_runtime_auth_failure(
                    worker,
                    cfg,
                    stop_event,
                    result.get("error"),
                    config_path=config_path,
                )
                break
        try:
            _sync_runtime_bundle(
                cfg,
                config_path=config_path,
            )
        except Exception:
            pass
        stop_event.wait(30)


class Worker:
    def __init__(self, cfg):
        self.cfg = cfg
        self.main_process = None
        self.single_processes = {}
        self.collect_process = None
        self.collect_task_id = None
        self.collect_owner = ""
        self.collect_payload_file = ""
        self.lock = threading.Lock()

    def _emit_local_log(self, owner, line):
        normalized_line = str(line or "").rstrip("\r\n")
        if not normalized_line:
            return
        normalized_owner = str(owner or "").strip()
        text = f"[{normalized_owner}] {normalized_line}" if normalized_owner else normalized_line
        try:
            print(text, flush=True)
        except Exception:
            try:
                sys.stdout.write(text + "\n")
                sys.stdout.flush()
            except Exception:
                pass

    def _should_forward_server_log(self, line):
        normalized_line = str(line or "").strip()
        if not normalized_line:
            return False
        return any(token in normalized_line for token in SERVER_KEY_LOG_SUBSTRINGS)

    def _emit_server_key_log(self, owner, line):
        normalized_line = str(line or "").rstrip("\r\n")
        if not normalized_line or not self._should_forward_server_log(normalized_line):
            return
        _post_agent_json(
            self.cfg,
            f"{self.cfg['server_url']}/api/agent/log",
            {
                "owner": owner or "",
                "lines": [normalized_line],
            },
            timeout=5,
        )

    def _emit_collect_log(self, owner, line):
        normalized_line = str(line or "").rstrip("\r\n")
        if not normalized_line:
            return
        self._emit_local_log(owner, normalized_line)
        _post_agent_json(
            self.cfg,
            f"{self.cfg['server_url']}/api/agent/log",
            {
                "owner": owner or "",
                "lines": [normalized_line],
            },
            timeout=5,
        )

    def _decode_output_line(self, payload):
        if isinstance(payload, str):
            return payload
        if not isinstance(payload, (bytes, bytearray)):
            return str(payload)
        raw = bytes(payload)
        for encoding in ("utf-8", "gbk"):
            try:
                return raw.decode(encoding)
            except UnicodeDecodeError:
                continue
        for encoding in ("utf-8", "gbk"):
            try:
                return raw.decode(encoding, errors="replace")
            except Exception:
                continue
        return raw.decode("utf-8", errors="replace")

    def _stream_output(self, process, owner, task_id):
        if not process.stdout:
            return
        try:
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                decoded = self._decode_output_line(line).rstrip("\r\n")
                self._emit_server_key_log(owner, decoded)
        except Exception as exc:
            self._emit_local_log(owner, f"[worker] 输出流读取异常: {exc}")
        finally:
            exit_code = process.wait()
            with self.lock:
                if self.main_process is process:
                    self.main_process = None
                stale_tokens = [
                    token
                    for token, current in self.single_processes.items()
                    if current is process or current.poll() is not None
                ]
                for token in stale_tokens:
                    self.single_processes.pop(token, None)
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "done", "exit_code": exit_code},
            )

    def _cleanup_finished_processes_locked(self):
        self._cleanup_collect_process_locked()
        if self.main_process and self.main_process.poll() is not None:
            self.main_process = None
        dead_tokens = [
            token for token, process in self.single_processes.items() if process.poll() is not None
        ]
        for token in dead_tokens:
            self.single_processes.pop(token, None)

    def _cleanup_collect_process_locked(self):
        process = self.collect_process
        if process and process.poll() is not None:
            self.collect_process = None
            self.collect_task_id = None
            self.collect_owner = ""
            payload_file = str(self.collect_payload_file or "").strip()
            self.collect_payload_file = ""
            if payload_file:
                try:
                    Path(payload_file).unlink(missing_ok=True)
                except Exception:
                    pass

    def _has_active_collect_task_locked(self):
        self._cleanup_collect_process_locked()
        process = self.collect_process
        return bool(process and process.poll() is None)

    def _build_env(self, owner, payload):
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        if self.cfg.get("runtime_dir"):
            env["FB_RPA_RUNTIME_DIR"] = str(self.cfg["runtime_dir"])
        if owner:
            env["FB_RPA_OWNER"] = owner
            env["FB_RPA_STOP_FLAG"] = str(_stop_flag_path(owner))
            env["FB_RPA_PAUSE_FLAG"] = str(_pause_flag_path(owner))
        bit_api = (payload.get("bit_api") or self.cfg["bit_api"]).strip()
        api_token = (payload.get("api_token") or self.cfg["api_token"]).strip()
        if bit_api:
            env["FB_RPA_BIT_API"] = bit_api
        if api_token:
            env["FB_RPA_API_TOKEN"] = api_token
        selected_window_groups_file = str(payload.get("selected_window_groups_file") or "").strip()
        if selected_window_groups_file:
            env[ENV_SELECTED_WINDOW_GROUPS_FILE] = selected_window_groups_file
        return env

    def _normalize_selected_window_groups(self, selected_window_groups, window_token=""):
        normalized = {}
        if not isinstance(selected_window_groups, dict):
            return normalized
        target_window_token = str(window_token or "").strip()
        for raw_window_token, raw_groups in selected_window_groups.items():
            normalized_window_token = str(raw_window_token or "").strip()
            if not normalized_window_token:
                continue
            if target_window_token and normalized_window_token != target_window_token:
                continue
            if not isinstance(raw_groups, list):
                continue
            normalized_groups = []
            seen_group_ids = set()
            for item in raw_groups:
                if not isinstance(item, dict):
                    continue
                group_id = str(item.get("group_id") or "").strip()
                if not group_id or group_id in seen_group_ids:
                    continue
                seen_group_ids.add(group_id)
                normalized_groups.append(
                    {
                        "group_id": group_id,
                        "group_name": str(item.get("group_name") or "").strip(),
                        "group_url": str(item.get("group_url") or "").strip(),
                    }
                )
            normalized[normalized_window_token] = normalized_groups
        if target_window_token and target_window_token not in normalized:
            normalized[target_window_token] = []
        return normalized

    def _prepare_selected_window_groups_runtime(self, owner, task_id, payload, window_token=""):
        if "selected_window_groups" not in payload:
            return ""
        normalized = self._normalize_selected_window_groups(
            payload.get("selected_window_groups"),
            window_token=window_token,
        )
        path = _selected_window_groups_path(owner, task_id=task_id, window_token=window_token)
        _save_json(path, normalized)
        return str(path)

    def _launch_process(self, owner, window_token, payload, task_id):
        try:
            _sync_runtime_bundle(
                self.cfg,
                config_path=self.cfg.get("config_path"),
            )
        except Exception as exc:
            self._emit_local_log(owner, f"[worker] 启动前同步运行配置失败：{exc}")
        if IS_FROZEN:
            main_exe = ROOT_DIR / "FB_RPA_Main.exe"
            if not main_exe.exists():
                _post_agent_json(
                    self.cfg,
                    f"{self.cfg['server_url']}/api/agent/report",
                    {"task_id": task_id, "status": "failed", "error": "missing_main_exe"},
                )
                return None
            command = [str(main_exe)]
        else:
            command = [sys.executable, str(ROOT_DIR / "main.py")]
        if window_token:
            command.append(f"--window-token={window_token}")
        launch_payload = dict(payload or {})
        try:
            selected_window_groups_file = self._prepare_selected_window_groups_runtime(
                owner,
                task_id,
                launch_payload,
                window_token=window_token,
            )
            if selected_window_groups_file:
                launch_payload["selected_window_groups_file"] = selected_window_groups_file
        except Exception as exc:
            self._emit_local_log(owner, f"[worker] 写入窗口白名单运行投影失败：{exc}")
        env = self._build_env(owner, launch_payload)
        if owner:
            stop_flag = _stop_flag_path(owner)
            if stop_flag.exists():
                stop_flag.unlink()
            pause_flag = _pause_flag_path(owner)
            if pause_flag.exists():
                pause_flag.unlink()
        creationflags = 0
        startupinfo = None
        if sys.platform.startswith("win"):
            creationflags = (
                getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            )
            startupinfo = self._build_hidden_startupinfo()
        process = subprocess.Popen(
            command,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=0,
            env=env,
            creationflags=creationflags,
            startupinfo=startupinfo,
        )
        threading.Thread(
            target=self._stream_output,
            args=(process, owner, task_id),
            daemon=True,
        ).start()
        return process

    def _launch_collect_process(self, owner, payload, task_id):
        owner = str(owner or self.cfg.get("owner") or "").strip()
        try:
            _sync_runtime_bundle(
                self.cfg,
                config_path=self.cfg.get("config_path"),
            )
        except Exception as exc:
            self._emit_local_log(owner, f"[worker] 采集启动前同步运行配置失败：{exc}")
        payload_path = _collect_task_payload_path(owner, task_id)
        _save_json(payload_path, dict(payload or {}))
        command = [sys.executable]
        if not IS_FROZEN:
            command.append(str(Path(__file__).resolve()))
        if self.cfg.get("server_url"):
            command.append(f"--server={self.cfg['server_url']}")
        if self.cfg.get("bit_api"):
            command.append(f"--bit-api={self.cfg['bit_api']}")
        if self.cfg.get("api_token"):
            command.append(f"--api-token={self.cfg['api_token']}")
        if owner:
            command.append(f"--owner={owner}")
        if self.cfg.get("machine_id"):
            command.append(f"--machine-id={self.cfg['machine_id']}")
        if self.cfg.get("label"):
            command.append(f"--label={self.cfg['label']}")
        if self.cfg.get("poll_interval") not in (None, ""):
            command.append(f"--poll-interval={self.cfg['poll_interval']}")
        if self.cfg.get("config_path"):
            command.append(f"--config={self.cfg['config_path']}")
        if self.cfg.get("agent_token"):
            command.append(f"--agent-token={self.cfg['agent_token']}")
        if self.cfg.get("runtime_dir"):
            command.append(f"--runtime-dir={self.cfg['runtime_dir']}")
        if self.cfg.get("client_version"):
            command.append(f"--client-version={self.cfg['client_version']}")
        command.append(f"--collect-task-id={task_id}")
        command.append(f"--collect-owner={owner}")
        command.append(f"--collect-payload-file={payload_path}")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        if self.cfg.get("runtime_dir"):
            env[ENV_RUNTIME_DIR] = str(self.cfg["runtime_dir"])
        creationflags = 0
        startupinfo = None
        if sys.platform.startswith("win"):
            creationflags = (
                getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            )
            startupinfo = self._build_hidden_startupinfo()
        try:
            process = subprocess.Popen(
                command,
                cwd=str(ROOT_DIR),
                env=env,
                creationflags=creationflags,
                startupinfo=startupinfo,
            )
        except Exception:
            try:
                payload_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        return process, str(payload_path)

    def _build_hidden_startupinfo(self):
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0
        return startupinfo

    def _stop_process(self, owner):
        stop_flag = _stop_flag_path(owner)
        stop_flag.parent.mkdir(parents=True, exist_ok=True)
        stop_flag.write_text("stop\n", encoding="utf-8")
        pause_flag = _pause_flag_path(owner)
        if pause_flag.exists():
            pause_flag.unlink()
        with self.lock:
            self._cleanup_finished_processes_locked()
            targets = []
            if self.main_process and self.main_process.poll() is None:
                targets.append(self.main_process)
            for process in self.single_processes.values():
                if process.poll() is None:
                    targets.append(process)
            collect_process = None
            if self.collect_process and self.collect_process.poll() is None:
                collect_process = self.collect_process
        time.sleep(2)
        for process in targets:
            try:
                if process.poll() is None:
                    process.terminate()
            except Exception:
                pass
        deadline = time.time() + 8
        for process in targets:
            while time.time() < deadline:
                if process.poll() is not None:
                    break
                time.sleep(0.2)
            if process.poll() is None:
                try:
                    process.kill()
                except Exception:
                    pass
        if collect_process and collect_process.poll() is None:
            deadline = time.time() + 8
            while time.time() < deadline:
                if collect_process.poll() is not None:
                    break
                time.sleep(0.2)
            if collect_process.poll() is None:
                try:
                    collect_process.terminate()
                except Exception:
                    pass
                kill_deadline = time.time() + 5
                while time.time() < kill_deadline:
                    if collect_process.poll() is not None:
                        break
                    time.sleep(0.2)
                if collect_process.poll() is None:
                    try:
                        collect_process.kill()
                    except Exception:
                        pass
        with self.lock:
            self._cleanup_finished_processes_locked()

    def _stop_collect(self, owner):
        stop_flag = _stop_flag_path(owner)
        stop_flag.parent.mkdir(parents=True, exist_ok=True)
        stop_flag.write_text("stop\n", encoding="utf-8")
        pause_flag = _pause_flag_path(owner)
        if pause_flag.exists():
            pause_flag.unlink()
        with self.lock:
            self._cleanup_finished_processes_locked()
            process = None
            if (
                self.collect_process
                and self.collect_process.poll() is None
                and str(self.collect_owner or "").strip() == str(owner or "").strip()
            ):
                process = self.collect_process
        if not process:
            return
        graceful_deadline = time.time() + 8
        while time.time() < graceful_deadline:
            if process.poll() is not None:
                break
            time.sleep(0.2)
        if process.poll() is None:
            try:
                process.terminate()
            except Exception:
                pass
            kill_deadline = time.time() + 5
            while time.time() < kill_deadline:
                if process.poll() is not None:
                    break
                time.sleep(0.2)
            if process.poll() is None:
                try:
                    process.kill()
                except Exception:
                    pass
        with self.lock:
            self._cleanup_finished_processes_locked()

    def shutdown(self, owner):
        try:
            self._stop_process(owner)
        except Exception:
            pass

    def _pause(self, owner):
        pause_flag = _pause_flag_path(owner)
        pause_flag.parent.mkdir(parents=True, exist_ok=True)
        pause_flag.write_text("pause\n", encoding="utf-8")

    def _resume(self, owner):
        pause_flag = _pause_flag_path(owner)
        was_paused = pause_flag.exists()
        if pause_flag.exists():
            pause_flag.unlink()
        if was_paused:
            stop_flag = _stop_flag_path(owner)
            if stop_flag.exists():
                stop_flag.unlink()

    def _has_active_runtime_processes_locked(self):
        self._cleanup_finished_processes_locked()
        if self.main_process and self.main_process.poll() is None:
            return True
        for process in self.single_processes.values():
            if process.poll() is None:
                return True
        return False

    def _build_collect_browser_settings(self, payload):
        from main import build_browser_settings

        runtime_config = _load_client_config(self.cfg.get("config_path"))
        if not isinstance(runtime_config, dict):
            runtime_config = {}
        runtime_config = dict(runtime_config)

        bit_api = str(payload.get("bit_api") or self.cfg.get("bit_api") or runtime_config.get("bit_api") or "").strip()
        api_token = str(
            payload.get("api_token") or self.cfg.get("api_token") or runtime_config.get("api_token") or ""
        ).strip()
        if bit_api:
            runtime_config["bit_api"] = bit_api
        if api_token:
            runtime_config["api_token"] = api_token
        return build_browser_settings(runtime_config)

    def _handle_collect_groups(self, owner, payload, task_id):
        owner = str(owner or self.cfg.get("owner") or "").strip()
        if not owner:
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "failed", "error": "missing_owner"},
            )
            return

        try:
            from core.group_collector import collect_groups_for_machine
        except Exception as exc:
            self._emit_collect_log(owner, f"[采集] 初始化失败：{exc}")
            traceback.print_exc()
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {
                    "task_id": task_id,
                    "status": "failed",
                    "error": "collect_import_failed",
                },
            )
            return

        browser_settings = self._build_collect_browser_settings(payload)
        bit_api = str(browser_settings.get("bit_api") or "").strip()
        api_token = str(browser_settings.get("api_token") or "").strip()
        if not (bit_api and api_token):
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "failed", "error": "missing_bitbrowser_credentials"},
            )
            return

        runtime_config = _load_client_config(self.cfg.get("config_path"))
        task_settings = dict((runtime_config or {}).get("task_settings") or {})
        max_scrolls = payload.get("max_scrolls")
        if max_scrolls in (None, ""):
            max_scrolls = task_settings.get("group_list_scroll_times", 8)
        try:
            max_scrolls = int(max_scrolls or 0)
        except Exception:
            max_scrolls = 8

        stop_flag = _stop_flag_path(owner)
        pause_flag = _pause_flag_path(owner)
        stop_flag.parent.mkdir(parents=True, exist_ok=True)
        if stop_flag.exists():
            stop_flag.unlink()
        if pause_flag.exists():
            pause_flag.unlink()
        previous_stop_env = os.environ.get("FB_RPA_STOP_FLAG")
        previous_pause_env = os.environ.get("FB_RPA_PAUSE_FLAG")
        os.environ["FB_RPA_STOP_FLAG"] = str(stop_flag)
        os.environ["FB_RPA_PAUSE_FLAG"] = str(pause_flag)
        self._emit_collect_log(owner, "[采集] 已收到群组采集任务，开始准备窗口采集。")
        try:
            result = collect_groups_for_machine(
                browser_settings=browser_settings,
                max_scrolls=max_scrolls,
                log_callback=lambda message: self._emit_collect_log(owner, message),
            )
        except Exception as exc:
            self._emit_collect_log(owner, f"[采集] 执行失败：{exc}")
            traceback.print_exc()
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {
                    "task_id": task_id,
                    "status": "failed",
                    "error": "collect_runtime_failed",
                },
            )
            return
        finally:
            if previous_stop_env is None:
                os.environ.pop("FB_RPA_STOP_FLAG", None)
            else:
                os.environ["FB_RPA_STOP_FLAG"] = previous_stop_env
            if previous_pause_env is None:
                os.environ.pop("FB_RPA_PAUSE_FLAG", None)
            else:
                os.environ["FB_RPA_PAUSE_FLAG"] = previous_pause_env
        windows = list(result.get("windows") or [])
        if int(result.get("profile_count") or 0) <= 0:
            self._emit_collect_log(owner, "[采集] 未获取到任何可用窗口，任务结束。")
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "failed", "error": "no_profiles"},
            )
            return

        if windows:
            status_code, collect_result = _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/collect-report",
                {
                    "owner": owner,
                    "machine_id": self.cfg.get("machine_id") or "",
                    "windows": windows,
                },
                timeout=60,
            )
            if status_code != 200 or not isinstance(collect_result, dict) or not collect_result.get("ok"):
                self._emit_collect_log(owner, "[采集] 采集结果上报失败。")
                _post_agent_json(
                    self.cfg,
                    f"{self.cfg['server_url']}/api/agent/report",
                    {"task_id": task_id, "status": "failed", "error": "collect_report_failed"},
                )
                return
        if result.get("stopped"):
            self._emit_collect_log(owner, "[采集] 已根据停止指令提前结束，本轮已获取结果已上报。")
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "failed", "error": "collect_stopped"},
            )
            return
        self._emit_collect_log(
            owner,
            (
                f"[采集] 已完成：窗口 {int(result.get('collected_window_count') or 0)} 个，"
                f"失败窗口 {int(result.get('failed_window_count') or 0)} 个，"
                f"群组 {int(result.get('total_groups') or 0)} 个。"
            ),
        )
        _post_agent_json(
            self.cfg,
            f"{self.cfg['server_url']}/api/agent/report",
            {"task_id": task_id, "status": "done"},
        )

    def _start_collect_task(self, owner, payload, task_id):
        owner = str(owner or self.cfg.get("owner") or "").strip()
        with self.lock:
            self._cleanup_finished_processes_locked()
            if self._has_active_collect_task_locked():
                _post_agent_json(
                    self.cfg,
                    f"{self.cfg['server_url']}/api/agent/report",
                    {"task_id": task_id, "status": "failed", "error": "busy_collect_task"},
                )
                return
            if self._has_active_runtime_processes_locked():
                _post_agent_json(
                    self.cfg,
                    f"{self.cfg['server_url']}/api/agent/report",
                    {"task_id": task_id, "status": "failed", "error": "busy_running_task"},
                )
                return
        try:
            process, payload_file = self._launch_collect_process(owner, payload, task_id)
        except Exception as exc:
            self._emit_collect_log(owner, f"[采集] 启动失败：{exc}")
            traceback.print_exc()
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "failed", "error": "collect_launch_failed"},
            )
            return
        with self.lock:
            self.collect_process = process
            self.collect_task_id = task_id
            self.collect_owner = owner
            self.collect_payload_file = payload_file

    def handle_task(self, task):
        task_id = task.get("id")
        action = task.get("action")
        owner = task.get("owner")
        window_token = task.get("window_token")
        payload = task.get("payload") or {}

        if action == "start":
            with self.lock:
                self._cleanup_finished_processes_locked()
                if self.main_process and self.main_process.poll() is None:
                    _post_agent_json(
                        self.cfg,
                        f"{self.cfg['server_url']}/api/agent/report",
                        {"task_id": task_id, "status": "failed", "error": "already_running"},
                    )
                    return
                process = self._launch_process(owner, None, payload, task_id)
                if not process:
                    return
                self.main_process = process
            return

        if action == "restart_window":
            token = str(window_token or "").strip()
            if not token:
                _post_agent_json(
                    self.cfg,
                    f"{self.cfg['server_url']}/api/agent/report",
                    {"task_id": task_id, "status": "failed", "error": "missing_window_token"},
                )
                return
            with self.lock:
                self._cleanup_finished_processes_locked()
                existing = self.single_processes.get(token)
                if existing and existing.poll() is None:
                    _post_agent_json(
                        self.cfg,
                        f"{self.cfg['server_url']}/api/agent/report",
                        {"task_id": task_id, "status": "failed", "error": "window_running"},
                    )
                    return
                process = self._launch_process(
                    owner,
                    token,
                    payload,
                    task_id,
                )
                if not process:
                    return
                self.single_processes[token] = process
            return

        if action == "pause":
            self._pause(owner)
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "done"},
            )
            return

        if action == "resume":
            self._resume(owner)
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "done"},
            )
            return

        if action == "stop":
            self._stop_process(owner)
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "done"},
            )
            return

        if action == "stop_collect":
            self._stop_collect(owner)
            _post_agent_json(
                self.cfg,
                f"{self.cfg['server_url']}/api/agent/report",
                {"task_id": task_id, "status": "done"},
            )
            return

        if action == "collect_groups":
            self._start_collect_task(owner, payload, task_id)
            return

        _post_agent_json(
            self.cfg,
            f"{self.cfg['server_url']}/api/agent/report",
            {"task_id": task_id, "status": "failed", "error": "unknown_action"},
        )


def main():
    parser = argparse.ArgumentParser(description="FB Group RPA Worker")
    parser.add_argument("--server", dest="server")
    parser.add_argument("--bit-api", dest="bit_api")
    parser.add_argument("--api-token", dest="api_token")
    parser.add_argument("--owner", dest="owner")
    parser.add_argument("--machine-id", dest="machine_id")
    parser.add_argument("--label", dest="label")
    parser.add_argument("--poll-interval", dest="poll_interval")
    parser.add_argument("--config", dest="config")
    parser.add_argument("--agent-token", dest="agent_token")
    parser.add_argument("--runtime-dir", dest="runtime_dir")
    parser.add_argument("--client-version", dest="client_version")
    parser.add_argument("--collect-task-id", dest="collect_task_id")
    parser.add_argument("--collect-owner", dest="collect_owner")
    parser.add_argument("--collect-payload-file", dest="collect_payload_file")
    args = parser.parse_args()

    cfg = _build_config(args)
    api_token = str(cfg.get("api_token") or "").strip()
    if not api_token:
        print("缺少 API Token，无法启动 worker")
        return
    if not _ensure_runtime_token(cfg, config_path=args.config):
        return

    if args.collect_task_id:
        worker = Worker(cfg)
        payload_path = Path(str(args.collect_payload_file or "").strip()) if args.collect_payload_file else None
        payload = _load_json(payload_path) if payload_path else {}
        try:
            worker._handle_collect_groups(
                str(args.collect_owner or cfg.get("owner") or "").strip(),
                payload,
                args.collect_task_id,
            )
        finally:
            if payload_path:
                try:
                    payload_path.unlink(missing_ok=True)
                except Exception:
                    pass
        return

    stop_event = threading.Event()
    worker = Worker(cfg)

    def _request_shutdown(*_args):
        stop_event.set()
        worker.shutdown(cfg.get("owner"))

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _request_shutdown)
        except Exception:
            continue

    threading.Thread(
        target=_heartbeat_loop,
        args=(cfg, stop_event, worker, args.config),
        daemon=True,
    ).start()
    try:
        while not stop_event.is_set():
            _refresh_runtime_config(cfg, args.config)
            if not _ensure_runtime_token(cfg, config_path=args.config):
                break
            _, result = _post_agent_json(
                cfg,
                f"{cfg['server_url']}/api/agent/task",
                {"machine_id": cfg["machine_id"], "client_version": cfg.get("client_version") or ""},
                timeout=15,
            )
            if isinstance(result, dict):
                if result.get("error") == "invalid_token":
                    if _attempt_runtime_token_recovery(cfg, config_path=args.config):
                        time.sleep(cfg["poll_interval"])
                        continue
                    _handle_runtime_auth_failure(
                        worker,
                        cfg,
                        stop_event,
                        result.get("error"),
                        config_path=args.config,
                    )
                    break
                if result.get("error") in ("account_expired", "account_disabled"):
                    _handle_runtime_auth_failure(
                        worker,
                        cfg,
                        stop_event,
                        result.get("error"),
                        config_path=args.config,
                    )
                    break
                if result.get("error") == "agent_disabled":
                    time.sleep(max(5.0, cfg["poll_interval"]))
                    continue
            task = result.get("task") if isinstance(result, dict) else None
            if task:
                worker.handle_task(task)
            time.sleep(cfg["poll_interval"])
    finally:
        stop_event.set()
        worker.shutdown(cfg.get("owner"))


if __name__ == "__main__":
    main()
