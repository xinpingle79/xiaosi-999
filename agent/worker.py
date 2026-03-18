import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import requests
import yaml

APP_NAME = "小四客户端"
IS_FROZEN = getattr(sys, "frozen", False)
DEFAULT_SERVER_URL = "http://154.64.255.139:43090"
LEGACY_SERVER_URLS = {
    "http://127.0.0.1:43090",
    "http://localhost:43090",
}


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
ENV_ACTIVATION_CODE = "FB_RPA_ACTIVATION_CODE"
_RUNTIME_DIR = ""
DEFAULT_BIT_API = "http://127.0.0.1:54345"
DEFAULT_POLL_INTERVAL = 2.0


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


def _normalize_server_url(server_url):
    return str(server_url or "").strip().rstrip("/")


def _should_migrate_server_url(server_url):
    return _normalize_server_url(server_url) in LEGACY_SERVER_URLS


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


def _state_path() -> Path:
    return _runtime_path("agent", "worker_state.json")


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
    return _load_json(_license_path())


def _save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_license(data):
    _save_json(_license_path(), data)


def _default_machine_id():
    hostname = socket.gethostname()
    node = uuid.getnode()
    return f"{hostname}-{node:x}"


def _load_client_config(config_path=None):
    target = Path(config_path) if config_path else CLIENT_CONFIG_PATH
    config = _load_yaml(target)
    if config and _should_migrate_server_url(config.get("server_url") or ""):
        config["server_url"] = DEFAULT_SERVER_URL
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
    runtime_dir = str(config.get("runtime_dir") or "").strip()
    if runtime_dir:
        cfg["runtime_dir"] = runtime_dir
        _set_runtime_dir(runtime_dir)
        os.environ[ENV_RUNTIME_DIR] = runtime_dir
    elif cfg.get("runtime_dir"):
        _set_runtime_dir(str(cfg["runtime_dir"]))
        os.environ[ENV_RUNTIME_DIR] = str(cfg["runtime_dir"])

    server_url = str(config.get("server_url") or "").strip().rstrip("/")
    if server_url:
        cfg["server_url"] = server_url
    bit_api = str(config.get("bit_api") or "").strip()
    if bit_api:
        cfg["bit_api"] = bit_api
    api_token = str(config.get("api_token") or "").strip()
    if api_token:
        cfg["api_token"] = api_token
    activation_code = str(config.get("activation_code") or "").strip()
    if activation_code:
        cfg["activation_code"] = activation_code
    machine_id = str(config.get("machine_id") or "").strip()
    if machine_id:
        cfg["machine_id"] = machine_id
    client_version = str(config.get("client_version") or "").strip()
    if client_version:
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
    register_token = (
        args.register_token
        or os.environ.get("FB_RPA_AGENT_REGISTER_TOKEN")
        or license_data.get("register_token")
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
        os.environ.get(ENV_RUNTIME_DIR)
        or config.get("runtime_dir")
        or ""
    )
    if runtime_dir:
        _set_runtime_dir(runtime_dir)
        os.environ[ENV_RUNTIME_DIR] = runtime_dir
    client_version = (
        os.environ.get("FB_RPA_CLIENT_VERSION")
        or config.get("client_version")
        or license_data.get("client_version")
        or ""
    )
    state = _load_json(_state_path())
    activation_code = (
        args.activation_code
        or os.environ.get(ENV_ACTIVATION_CODE)
        or config.get("activation_code")
        or state.get("activation_code")
        or ""
    )
    machine_id = (
        args.machine_id
        or os.environ.get("FB_RPA_AGENT_ID")
        or config.get("machine_id")
        or state.get("machine_id")
        or _default_machine_id()
    )
    state["machine_id"] = machine_id
    if activation_code:
        state["activation_code"] = activation_code
    if "agent_token" in state:
        config["agent_token"] = state.get("agent_token")
    if "token_expires_at" in state:
        config["token_expires_at"] = state.get("token_expires_at")
    if "owner" in state and not owner:
        owner = state.get("owner") or ""
    if license_data.get("expire_at") is not None:
        config["expire_at"] = license_data.get("expire_at")
    if license_data.get("max_devices") is not None:
        config["max_devices"] = license_data.get("max_devices")
    _save_json(_state_path(), state)
    return {
        "server_url": server_url.rstrip("/"),
        "bit_api": bit_api,
        "api_token": api_token,
        "label": label,
        "poll_interval": poll_interval,
        "machine_id": machine_id,
        "register_token": register_token,
        "owner": owner,
        "activation_code": activation_code,
        "runtime_dir": runtime_dir,
        "client_version": client_version,
        "agent_token": config.get("agent_token") or "",
        "token_expires_at": config.get("token_expires_at"),
        "expire_at": config.get("expire_at"),
        "max_devices": config.get("max_devices"),
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


def _register_agent(cfg):
    payload = {
        "machine_id": cfg["machine_id"],
        "label": cfg["label"],
        "mode": "worker",
        "owner": cfg.get("owner") or "",
        "client_version": cfg.get("client_version") or "",
        "bit_api": cfg.get("bit_api") or "",
        "api_token": cfg.get("api_token") or "",
    }
    if cfg.get("register_token"):
        payload["register_token"] = cfg.get("register_token")
    _, data = _post_json(f"{cfg['server_url']}/api/agent/register", payload)
    if isinstance(data, dict) and data.get("ok"):
        info = data.get("data") or {}
        cfg["agent_token"] = str(info.get("agent_token") or "")
        cfg["token_expires_at"] = info.get("token_expires_at")
        state = _load_json(_state_path())
        state["agent_token"] = cfg.get("agent_token")
        state["token_expires_at"] = cfg.get("token_expires_at")
        if cfg.get("owner"):
            state["owner"] = cfg.get("owner")
        _save_json(_state_path(), state)
    return data


def _license_expired(expire_at):
    if expire_at in (None, "", 0):
        return False
    try:
        return time.time() > float(expire_at)
    except Exception:
        return False


def _is_placeholder_activation_code(code):
    return str(code or "").strip().upper() in ("", "CHANGE_ME")


def _prompt_activation_code():
    try:
        import tkinter as tk
        from tkinter import simpledialog

        root = tk.Tk()
        root.withdraw()
        try:
            root.attributes("-topmost", True)
        except Exception:
            pass
        code = simpledialog.askstring("激活", "请输入授权码")
        try:
            root.destroy()
        except Exception:
            pass
        return (code or "").strip()
    except Exception:
        try:
            return input("请输入授权码：").strip()
        except Exception:
            return ""


def _activate_client(cfg, activation_code):
    payload = {
        "activation_code": activation_code,
        "machine_id": cfg.get("machine_id") or "",
        "client_version": cfg.get("client_version") or "",
    }
    _, data = _post_json(f"{cfg['server_url']}/api/client/activate", payload, timeout=15)
    if not isinstance(data, dict) or not data.get("ok"):
        return data
    info = data.get("data") or {}
    license_data = {
        "owner": info.get("owner") or "",
        "register_token": info.get("register_token") or "",
        "expire_at": info.get("expire_at"),
        "max_devices": info.get("max_devices"),
    }
    _save_license(license_data)
    cfg["owner"] = license_data.get("owner") or cfg.get("owner")
    cfg["register_token"] = license_data.get("register_token") or cfg.get("register_token")
    cfg["expire_at"] = license_data.get("expire_at")
    cfg["max_devices"] = license_data.get("max_devices")
    cfg["activation_code"] = activation_code
    state = _load_json(_state_path())
    state["activation_code"] = activation_code
    if cfg.get("owner"):
        state["owner"] = cfg.get("owner")
    _save_json(_state_path(), state)
    return data


def _ensure_activation(cfg):
    license_data = _load_license()
    if license_data:
        cfg["owner"] = license_data.get("owner") or cfg.get("owner")
        cfg["register_token"] = license_data.get("register_token") or cfg.get("register_token")
        cfg["expire_at"] = license_data.get("expire_at")
        cfg["max_devices"] = license_data.get("max_devices")
        if _license_expired(cfg.get("expire_at")):
            print("账号已到期，请联系管理员续费")
            return False
        if str(cfg.get("activation_code") or "").strip():
            return True

    activation_code = str(cfg.get("activation_code") or "").strip()
    if _is_placeholder_activation_code(activation_code):
        activation_code = ""
    if not activation_code:
        activation_code = _prompt_activation_code()
        if not activation_code:
            print("未输入授权码，无法继续运行")
            return False

    result = _activate_client(cfg, activation_code)
    if not isinstance(result, dict) or not result.get("ok"):
        error = result.get("error") if isinstance(result, dict) else None
        if error == "account_expired":
            print("账号已到期，请联系管理员续费")
        elif error == "account_disabled":
            print("账号已被禁用，请联系管理员")
        elif error == "device_limit_reached":
            print("设备数量已达上限，请联系管理员")
        elif error == "device_bound":
            print("该设备已绑定其他账号")
        elif error == "device_disabled":
            print("设备已被禁用，请联系管理员")
        else:
            print("激活失败，请检查授权码或网络")
        return False
    return True


def _token_expired(cfg):
    expires_at = cfg.get("token_expires_at")
    if not expires_at:
        return False
    try:
        return time.time() >= float(expires_at)
    except Exception:
        return False


def _heartbeat_loop(cfg, stop_event, config_path=None):
    while not stop_event.is_set():
        _refresh_runtime_config(cfg, config_path)
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
            if result.get("error") in ("token_expired", "invalid_token"):
                _register_agent(cfg)
            if result.get("error") in ("account_expired", "account_disabled"):
                stop_event.set()
                break
        stop_event.wait(30)


class Worker:
    def __init__(self, cfg):
        self.cfg = cfg
        self.main_process = None
        self.single_processes = {}
        self.lock = threading.Lock()

    def _send_log(self, owner, line):
        if not line:
            return
        _post_agent_json(
            self.cfg,
            f"{self.cfg['server_url']}/api/agent/log",
            {
                "owner": owner or "",
                "lines": [line],
            },
            timeout=5,
        )

    def _stream_output(self, process, owner, task_id):
        if not process.stdout:
            return
        for line in process.stdout:
            self._send_log(owner, line.rstrip())
        exit_code = process.poll()
        _post_agent_json(
            self.cfg,
            f"{self.cfg['server_url']}/api/agent/report",
            {"task_id": task_id, "status": "done", "exit_code": exit_code},
        )

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
        return env

    def _launch_process(self, owner, window_token, payload, task_id):
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
            command.extend(["--window-token", str(window_token)])
        env = self._build_env(owner, payload)
        if owner:
            stop_flag = _stop_flag_path(owner)
            if stop_flag.exists():
                stop_flag.unlink()
            pause_flag = _pause_flag_path(owner)
            if pause_flag.exists():
                pause_flag.unlink()
        process = subprocess.Popen(
            command,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        threading.Thread(
            target=self._stream_output,
            args=(process, owner, task_id),
            daemon=True,
        ).start()
        return process

    def _stop_process(self, owner):
        stop_flag = _stop_flag_path(owner)
        stop_flag.parent.mkdir(parents=True, exist_ok=True)
        stop_flag.write_text("stop\n", encoding="utf-8")
        pause_flag = _pause_flag_path(owner)
        if pause_flag.exists():
            pause_flag.unlink()
        with self.lock:
            targets = []
            if self.main_process and self.main_process.poll() is None:
                targets.append(self.main_process)
            for process in self.single_processes.values():
                if process.poll() is None:
                    targets.append(process)
        time.sleep(2)
        for process in targets:
            try:
                if process.poll() is None:
                    process.terminate()
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

    def handle_task(self, task):
        task_id = task.get("id")
        action = task.get("action")
        owner = task.get("owner")
        window_token = task.get("window_token")
        payload = task.get("payload") or {}

        if action == "start":
            with self.lock:
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
    parser.add_argument("--register-token", dest="register_token")
    parser.add_argument("--activation-code", dest="activation_code")
    parser.add_argument("--owner", dest="owner")
    parser.add_argument("--machine-id", dest="machine_id")
    parser.add_argument("--label", dest="label")
    parser.add_argument("--poll-interval", dest="poll_interval")
    parser.add_argument("--config", dest="config")
    args = parser.parse_args()

    cfg = _build_config(args)
    api_token = str(cfg.get("api_token") or "").strip()
    if not api_token:
        print("缺少 API Token，无法启动 worker")
        return
    if not _ensure_activation(cfg):
        return
    _register_agent(cfg)

    stop_event = threading.Event()
    threading.Thread(
        target=_heartbeat_loop,
        args=(cfg, stop_event, args.config),
        daemon=True,
    ).start()

    worker = Worker(cfg)
    while True:
        _refresh_runtime_config(cfg, args.config)
        if _license_expired(cfg.get("expire_at")):
            print("账号已到期，请联系管理员续费")
            break
        if not cfg.get("agent_token") or _token_expired(cfg):
            _register_agent(cfg)
        _, result = _post_agent_json(
            cfg,
            f"{cfg['server_url']}/api/agent/task",
            {"machine_id": cfg["machine_id"], "client_version": cfg.get("client_version") or ""},
            timeout=15,
        )
        if isinstance(result, dict):
            if result.get("error") in ("token_expired", "invalid_token"):
                _register_agent(cfg)
                time.sleep(cfg["poll_interval"])
                continue
            if result.get("error") in ("account_expired", "account_disabled"):
                print("账号已到期，请联系管理员续费")
                break
            if result.get("error") == "agent_disabled":
                time.sleep(max(5.0, cfg["poll_interval"]))
                continue
        task = result.get("task") if isinstance(result, dict) else None
        if task:
            worker.handle_task(task)
        time.sleep(cfg["poll_interval"])

    stop_event.set()


if __name__ == "__main__":
    main()
