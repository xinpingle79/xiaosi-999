import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

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
_RUNTIME_DIR = ""
DEFAULT_BIT_API = ""
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
    normalized = _normalize_server_url(server_url)
    if not normalized:
        return False
    parsed = urlparse(normalized)
    host = str(parsed.hostname or "").strip().lower()
    port = parsed.port
    if port is None:
        port = 80 if parsed.scheme == "http" else 443 if parsed.scheme == "https" else None
    return parsed.scheme == "http" and port == 43090 and host in {"127.0.0.1", "localhost"}


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
    if _should_migrate_server_url(config.get("server_url") or ""):
        config["server_url"] = DEFAULT_SERVER_URL
        changed = True
    for key in ("activation_code", "register_token", "owner", "expire_at", "max_devices"):
        if key in config:
            config.pop(key, None)
            changed = True
    if "agent_token" in config:
        config["agent_token"] = str(config.get("agent_token") or "").strip()
    if config.get("token_expires_at") in ("", []):
        config["token_expires_at"] = None
        changed = True
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
    if "token_expires_at" not in locked_fields:
        token_expires_at = config.get("token_expires_at")
        if token_expires_at not in (None, "", []):
            cfg["token_expires_at"] = token_expires_at
        elif not cfg.get("agent_token"):
            cfg["token_expires_at"] = None
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
    token_expires_at = args.token_expires_at if args.token_expires_at not in (None, "") else config.get("token_expires_at")
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
            "token_expires_at": args.token_expires_at,
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
        "token_expires_at": token_expires_at,
        "expire_at": config.get("expire_at"),
        "max_devices": config.get("max_devices"),
        "_locked_fields": locked_fields,
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


def _persist_client_auth(config_path=None, agent_token="", token_expires_at=None):
    target = Path(config_path) if config_path else CLIENT_CONFIG_PATH
    config = _load_client_config(target)
    if not config:
        config = {}
    config["agent_token"] = str(agent_token or "").strip()
    config["token_expires_at"] = token_expires_at
    for key in ("activation_code", "register_token", "owner", "expire_at", "max_devices"):
        config.pop(key, None)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _clear_runtime_auth(cfg, config_path=None):
    cfg["agent_token"] = ""
    cfg["token_expires_at"] = None
    _persist_client_auth(config_path=config_path, agent_token="", token_expires_at=None)


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
        print("缺少有效凭证，请先在客户端输入激活码完成激活")
        return False
    if _token_expired(cfg):
        _clear_runtime_auth(cfg, config_path=config_path)
        print("本地凭证已过期，请先在客户端重新激活")
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


def _runtime_auth_message(error_code):
    mapping = {
        "invalid_token": "执行端认证失效（invalid_token），请回到客户端重新激活。",
        "token_expired": "执行端本地凭证已过期，请回到客户端重新激活。",
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
            if result.get("error") in ("token_expired", "invalid_token"):
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
                self._send_log(owner, decoded)
        except Exception as exc:
            self._send_log(owner, f"[worker] 输出流读取异常: {exc}")
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
        if self.main_process and self.main_process.poll() is not None:
            self.main_process = None
        dead_tokens = [
            token for token, process in self.single_processes.items() if process.poll() is not None
        ]
        for token in dead_tokens:
            self.single_processes.pop(token, None)

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
            command.append(f"--window-token={window_token}")
        env = self._build_env(owner, payload)
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
    parser.add_argument("--token-expires-at", dest="token_expires_at")
    parser.add_argument("--runtime-dir", dest="runtime_dir")
    parser.add_argument("--client-version", dest="client_version")
    args = parser.parse_args()

    cfg = _build_config(args)
    api_token = str(cfg.get("api_token") or "").strip()
    if not api_token:
        print("缺少 API Token，无法启动 worker")
        return
    if not _ensure_runtime_token(cfg, config_path=args.config):
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
                if result.get("error") in ("token_expired", "invalid_token"):
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
