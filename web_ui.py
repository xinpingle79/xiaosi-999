import calendar
import json
import mimetypes
import os
import re
import secrets
import uuid
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import hashlib
import hmac

import yaml

from utils.helpers import Database
from utils.logger import log


ROOT_DIR = (
    Path(sys.executable).resolve().parent
    if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent
)
os.environ.setdefault("FB_RPA_DB_ROLE", "server")
STATIC_DIR = ROOT_DIR / "webui"
SETTINGS_PATH = ROOT_DIR / "config" / "server.yaml"
MESSAGES_PATH = ROOT_DIR / "config" / "messages.server.yaml"

SESSION_COOKIE_NAME = "FB_RPA_SESSION"
ADMIN_SESSION_COOKIE_NAME = "FB_RPA_ADMIN_SESSION"
SUB_SESSION_COOKIE_NAME = "FB_RPA_SUB_SESSION"
SESSION_LOCK = threading.Lock()
SESSION_STORE = {}
SESSION_USER_INDEX = {}
SESSION_TTL_SECONDS = 0
CLEANUP_HISTORY_DAYS = 15
CLEANUP_LOG_DAYS = 7
AGENT_LOCK = threading.Lock()
DEFAULT_WEB_PORT = 43090


def _runtime_dir():
    runtime_root = (os.environ.get("FB_RPA_RUNTIME_DIR") or "").strip()
    if runtime_root:
        base = Path(runtime_root).expanduser()
    else:
        base = ROOT_DIR / "runtime"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _build_server_base_url(request_host=""):
    host = (request_host or "").strip()
    if host:
        return f"http://{host}"
    settings = read_yaml(SETTINGS_PATH)
    web_cfg = settings.get("web_ui", {}) or {}
    host = str(web_cfg.get("host") or "127.0.0.1").strip()
    port = int(web_cfg.get("port", DEFAULT_WEB_PORT) or DEFAULT_WEB_PORT)
    if host in ("0.0.0.0", "::"):
        host = "127.0.0.1"
    return f"http://{host}:{port}"


def _drop_session(token):
    session = SESSION_STORE.pop(token, None)
    if not session:
        return
    try:
        db = Database()
        try:
            db.delete_session(token)
        finally:
            db.close()
    except Exception:
        pass
    username = session.get("username")
    if not username:
        return
    tokens = SESSION_USER_INDEX.get(username)
    if not tokens:
        return
    tokens.discard(token)
    if not tokens:
        SESSION_USER_INDEX.pop(username, None)


def _purge_expired_sessions(now=None):
    if not SESSION_TTL_SECONDS:
        return
    now = now or time.time()
    expired = [
        token
        for token, session in SESSION_STORE.items()
        if now - float(session.get("created_at", 0) or 0) > SESSION_TTL_SECONDS
    ]
    for token in expired:
        _drop_session(token)


class TaskManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._logs = deque(maxlen=1200)

    def _resolve_owner_agent_with_retry(
        self,
        db,
        owner_name,
        heartbeat_ttl,
        wait_timeout=0.0,
        retry_interval=0.5,
    ):
        deadline = time.time() + max(float(wait_timeout or 0.0), 0.0)
        last_error = None
        while True:
            agent, agent_error = self._resolve_owner_agent(db, owner_name, heartbeat_ttl)
            if agent:
                return agent, None
            last_error = agent_error
            if time.time() >= deadline:
                return None, last_error
            time.sleep(max(float(retry_interval or 0.5), 0.1))

    def _resolve_owner_agent(self, db, owner_name, heartbeat_ttl):
        agent = db.get_online_agent_for_owner(owner_name, heartbeat_ttl)
        if agent:
            return agent, None
        latest = db.get_latest_agent_for_owner(owner_name, include_disabled=True)
        if not latest:
            return None, "未绑定执行端"
        status_raw = latest.get("status", 1)
        if status_raw is None:
            status_raw = 1
        if int(status_raw) == 0:
            return None, "执行端已禁用"
        return None, "执行端离线"

    def _resolve_machine_agent(self, db, owner_name, machine_id, heartbeat_ttl, allow_disabled):
        machine_id = (machine_id or "").strip()
        if not machine_id:
            return None, "未绑定设备"
        agent = db.get_agent(machine_id)
        if not agent:
            return None, "执行端不存在"
        agent_owner = str(agent.get("owner") or "").strip()
        if owner_name and agent_owner != owner_name:
            return None, "执行端归属不匹配"
        status_raw = agent.get("status", 1)
        if status_raw is None:
            status_raw = 1
        if not allow_disabled and int(status_raw) == 0:
            return None, "执行端已禁用"
        last_seen = agent.get("last_seen")
        if last_seen is None:
            return None, "执行端离线"
        try:
            if time.time() - float(last_seen) > float(heartbeat_ttl or 0):
                return None, "执行端离线"
        except Exception:
            return None, "执行端离线"
        return agent, None

    def _find_existing_machine_action_task(self, db, owner_name, machine_id, action):
        row = db.conn.execute(
            """
            SELECT id, status
            FROM agent_tasks
            WHERE owner = ?
              AND assigned_machine_id = ?
              AND action = ?
              AND status IN ('pending', 'running')
            ORDER BY id DESC
            LIMIT 1
            """,
            (owner_name, machine_id, action),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "status": row["status"],
        }

    def _enqueue_owner_device_actions(
        self,
        action,
        owner_name,
        window_token=None,
        allow_disabled=False,
        wait_online_seconds=0.0,
    ):
        owner_account = self._load_owner_account(owner_name)
        if not owner_account:
            return False, "账号不存在"
        if not allow_disabled and int(owner_account.get("status", 0)) == 0:
            return False, "账号已禁用"
        settings = _load_agent_settings()
        heartbeat_ttl = settings.get("heartbeat_ttl", 180)
        deadline = time.time() + max(float(wait_online_seconds or 0.0), 0.0)
        db = Database()
        try:
            while True:
                configs = db.list_device_configs(owner_name)
                enabled_configs = [
                    cfg for cfg in configs if int(cfg.get("status", 1) or 0) == 1
                ]
                errors = []
                if not enabled_configs:
                    return False, "未配置设备"
                task_ids = []
                has_online = False
                has_valid_config = False
                for cfg in enabled_configs:
                    machine_id = str(cfg.get("machine_id") or "").strip()
                    if not machine_id:
                        cfg_label = str(cfg.get("name") or cfg.get("id") or "")
                        self._logs.append(
                            f"=== 跳过配置{cfg_label}：未绑定设备 ==="
                        )
                        continue
                    has_valid_config = True
                    agent, agent_error = self._resolve_machine_agent(
                        db,
                        owner_name,
                        machine_id,
                        heartbeat_ttl,
                        allow_disabled,
                    )
                    if not agent:
                        self._logs.append(
                            f"=== 跳过设备 {machine_id}：{agent_error} ==="
                        )
                        continue
                    has_online = True
                    existing_task = self._find_existing_machine_action_task(
                        db,
                        owner_name,
                        machine_id,
                        action,
                    )
                    if existing_task:
                        self._logs.append(
                            f"=== 跳过重复 {action} 指令：执行端 {machine_id} 已存在任务#{existing_task['id']} ({existing_task['status']}) ==="
                        )
                        task_ids.append((machine_id, existing_task["id"]))
                        continue
                    bit_api = str(cfg.get("bit_api") or "").strip()
                    api_token = str(cfg.get("api_token") or "").strip()
                    if not bit_api or not api_token:
                        self._logs.append(
                            f"=== 跳过设备 {machine_id}：设备配置缺少接口参数 ==="
                        )
                        continue
                    payload = {
                        "owner": owner_name,
                        "bit_api": bit_api,
                        "api_token": api_token,
                        "device_config_id": cfg.get("id"),
                    }
                    task_id = db.enqueue_agent_task(
                        owner_name,
                        action,
                        window_token=window_token,
                        machine_id=machine_id,
                        payload=payload,
                    )
                    task_ids.append((machine_id, task_id))
                if task_ids:
                    for machine_id, task_id in task_ids:
                        self._logs.append(
                            f"=== 已下发 {action} 指令到执行端 {machine_id} (任务#{task_id}) ==="
                        )
                    return True, None
                if not has_valid_config:
                    return False, "未绑定设备"
                if time.time() >= deadline:
                    if has_valid_config and not has_online:
                        return False, "暂无在线设备"
                    return False, errors[0] if errors else "执行端离线"
                time.sleep(0.5)
        finally:
            db.close()

    def _enqueue_agent_action(
        self,
        action,
        owner=None,
        window_token=None,
        allow_disabled=False,
        wait_online_seconds=0.0,
    ):
        owner_name = owner or "admin"
        owner_account = self._load_owner_account(owner_name)
        if not owner_account:
            return False, "账号不存在"
        if not allow_disabled and int(owner_account.get("status", 0)) == 0:
            return False, "账号已禁用"
        settings = _load_agent_settings()
        heartbeat_ttl = settings.get("heartbeat_ttl", 180)
        db = Database()
        try:
            if wait_online_seconds:
                agent, agent_error = self._resolve_owner_agent_with_retry(
                    db,
                    owner_name,
                    heartbeat_ttl,
                    wait_timeout=wait_online_seconds,
                )
            else:
                agent, agent_error = self._resolve_owner_agent(db, owner_name, heartbeat_ttl)
            if not agent:
                return False, agent_error or "执行端离线"
            machine_id = (agent.get("machine_id") or "").strip()
            existing_task = self._find_existing_machine_action_task(
                db,
                owner_name,
                machine_id,
                action,
            )
            if existing_task:
                self._logs.append(
                    f"=== 跳过重复 {action} 指令：执行端 {machine_id} 已存在任务#{existing_task['id']} ({existing_task['status']}) ==="
                )
                return True, None
            configs = db.list_device_configs(owner_name)
            target_cfg = None
            for cfg in configs:
                if str(cfg.get("machine_id") or "").strip() == machine_id:
                    target_cfg = cfg
                    break
            if not target_cfg:
                return False, "未配置设备"
            bit_api = str(target_cfg.get("bit_api") or "").strip()
            api_token = str(target_cfg.get("api_token") or "").strip()
            if not bit_api or not api_token:
                return False, "设备配置缺少接口参数"
            payload = {
                "owner": owner_name,
                "bit_api": bit_api,
                "api_token": api_token,
                "device_config_id": target_cfg.get("id"),
            }
            task_id = db.enqueue_agent_task(
                owner_name,
                action,
                window_token=window_token,
                machine_id=machine_id,
                payload=payload,
            )
        finally:
            db.close()
        self._logs.append(f"=== 已下发 {action} 指令到执行端 {machine_id} (任务#{task_id}) ===")
        return True, None

    def enqueue_machine_action(
        self,
        action,
        owner_name,
        machine_id,
        api_token="",
        window_token=None,
        allow_disabled=False,
        wait_online_seconds=0.0,
    ):
        owner_name = str(owner_name or "").strip()
        machine_id = str(machine_id or "").strip()
        api_token = str(api_token or "").strip()
        if not owner_name:
            return False, "账号不存在"
        if not machine_id:
            return False, "未绑定设备"
        owner_account = self._load_owner_account(owner_name)
        if not owner_account:
            return False, "账号不存在"
        if not allow_disabled and int(owner_account.get("status", 0) or 0) == 0:
            return False, "账号已禁用"

        settings = _load_agent_settings()
        heartbeat_ttl = settings.get("heartbeat_ttl", 180)
        deadline = time.time() + max(float(wait_online_seconds or 0.0), 0.0)
        db = Database()
        try:
            while True:
                configs = db.list_device_configs(owner_name)
                target_cfg = None
                for cfg in configs:
                    if str(cfg.get("machine_id") or "").strip() == machine_id:
                        target_cfg = cfg
                        break
                if not target_cfg and api_token:
                    for cfg in configs:
                        if str(cfg.get("api_token") or "").strip() == api_token:
                            target_cfg = cfg
                            break
                if not target_cfg:
                    return False, "未配置设备"

                agent, agent_error = self._resolve_machine_agent(
                    db,
                    owner_name,
                    machine_id,
                    heartbeat_ttl,
                    allow_disabled,
                )
                if not agent:
                    if time.time() >= deadline:
                        return False, agent_error or "执行端离线"
                    time.sleep(0.5)
                    continue

                existing_task = self._find_existing_machine_action_task(
                    db,
                    owner_name,
                    machine_id,
                    action,
                )
                if existing_task:
                    self._logs.append(
                        f"=== 跳过重复 {action} 指令：执行端 {machine_id} 已存在任务#{existing_task['id']} ({existing_task['status']}) ==="
                    )
                    return True, None
                bit_api = str(target_cfg.get("bit_api") or "").strip()
                target_api_token = str(target_cfg.get("api_token") or "").strip()
                if not bit_api or not target_api_token:
                    return False, "设备配置缺少接口参数"
                payload = {
                    "owner": owner_name,
                    "bit_api": bit_api,
                    "api_token": target_api_token,
                    "device_config_id": target_cfg.get("id"),
                }
                task_id = db.enqueue_agent_task(
                    owner_name,
                    action,
                    window_token=window_token,
                    machine_id=machine_id,
                    payload=payload,
                )
                self._logs.append(
                    f"=== 已下发 {action} 指令到执行端 {machine_id} (任务#{task_id}) ==="
                )
                return True, None
        finally:
            db.close()

    def start(self, owner=None):
        with self._lock:
            owner_name = owner or "admin"
            self._clear_pause_state_db(owner_name, clear_global=owner is None)
            self._logs.clear()
            self._logs.append("=== 任务已下发 ===")
            self._logs.append("=== 正在确认执行端在线状态 ===")
            if owner is None:
                return self._enqueue_agent_action(
                    "start",
                    owner=owner_name,
                    wait_online_seconds=4.0,
                )
            return self._enqueue_owner_device_actions(
                "start",
                owner_name,
                wait_online_seconds=4.0,
            )

    def stop(self, owner=None):
        with self._lock:
            owner_name = owner or "admin"
            self._clear_pause_state_db(owner_name, clear_global=owner is None)
            if owner is None:
                return self._enqueue_agent_action(
                    "stop", owner=owner_name, allow_disabled=True
                )
            return self._enqueue_owner_device_actions(
                "stop", owner_name, allow_disabled=True
            )

    def pause(self, owner=None):
        with self._lock:
            owner_name = owner or "admin"
            if not self._agent_has_running(owner_name):
                return False, "当前没有运行中的任务"
            self._set_pause_state_db(True, owner=owner_name)
            if owner is None:
                return self._enqueue_agent_action("pause", owner=owner_name)
            return self._enqueue_owner_device_actions("pause", owner_name)

    def resume(self, owner=None):
        with self._lock:
            owner_name = owner or "admin"
            if not self._agent_has_running(owner_name):
                return False, "当前没有运行中的任务"
            self._clear_pause_state_db(owner_name, clear_global=owner is None)
            if owner is None:
                return self._enqueue_agent_action("resume", owner=owner_name)
            return self._enqueue_owner_device_actions("resume", owner_name)

    def restart_window(self, window_token, owner=None):
        with self._lock:
            return self._enqueue_agent_action(
                "restart_window",
                owner=owner,
                window_token=str(window_token or "").strip(),
            )

    def status(self, owner=None):
        with self._lock:
            db = Database()
            try:
                if hasattr(db, "count_agent_tasks"):
                    running_tasks = db.count_agent_tasks(status="running", owner=owner)
                else:
                    running_tasks = 0
            except Exception:
                running_tasks = 0
            finally:
                db.close()
            running = running_tasks > 0
            logs = list(self._logs)
            stats = build_runtime_stats(logs, account_id=owner)
            if owner:
                stats["runtime"] = {
                    "running_windows": running_tasks,
                    "running_accounts": 1 if running else 0,
                }
            else:
                stats["runtime"] = {
                    "running_windows": running_tasks,
                    "running_accounts": running_tasks,
                }
            return {
                "running": running,
                "pid": None,
                "exit_code": None,
                "started_at": None,
                "logs": logs,
                "single_window_tokens": [],
                "stats": stats,
            }

    def _set_pause_state_db(self, paused, owner=None):
        db = Database()
        try:
            db.set_pause_state(paused, owner=owner)
        finally:
            db.close()

    def _clear_pause_state_db(self, owner=None, clear_global=False):
        db = Database()
        try:
            if owner:
                db.set_pause_state(False, owner=owner)
                if clear_global:
                    db.set_pause_state(False, owner=None)
            else:
                db.set_pause_state(False, owner=None)
        finally:
            db.close()

    def _agent_has_running(self, owner=None):
        db = Database()
        try:
            if hasattr(db, "count_agent_tasks"):
                return db.count_agent_tasks(status="running", owner=owner) > 0
        finally:
            db.close()
        return False

    def stop_for_owner(self, owner):
        if not owner:
            return
        with self._lock:
            self._clear_pause_state_db(owner, clear_global=False)
            ok, error = self._enqueue_owner_device_actions(
                "stop", owner, allow_disabled=True
            )
            if ok:
                self._logs.append(f"=== 已禁用账号 {owner}，已下发停止指令 ===")
            else:
                self._logs.append(f"=== 已禁用账号 {owner}，停止指令下发失败: {error} ===")

    def _load_owner_account(self, owner):
        if not owner:
            return None
        db = Database()
        try:
            return db.get_user_account(owner)
        finally:
            db.close()

    def _append_runtime_log(self, message):
        with self._lock:
            self._logs.append(message)


TASK_MANAGER = TaskManager()


def read_yaml(path):
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_agent_settings():
    settings = read_yaml(SETTINGS_PATH)
    security_cfg = settings.get("agent_security", {}) or {}
    heartbeat_ttl = int(security_cfg.get("heartbeat_ttl_seconds", 180) or 180)
    return {
        "heartbeat_ttl": heartbeat_ttl,
    }


def _load_agent_security():
    settings = read_yaml(SETTINGS_PATH)
    security_cfg = settings.get("agent_security", {}) or {}
    token_ttl = int(security_cfg.get("token_ttl_seconds", 3600) or 3600)
    max_devices = int(security_cfg.get("max_devices_per_account", 1) or 1)
    allow_rebind = bool(security_cfg.get("allow_rebind", False))
    return {
        "token_ttl_seconds": token_ttl,
        "max_devices_per_account": max_devices,
        "allow_rebind": allow_rebind,
    }


def _normalize_host(raw_host):
    if not raw_host:
        return ""
    host = raw_host.split(",")[0].strip()
    if ":" in host:
        host = host.split(":")[0]
    return host


def write_yaml(path, data):
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def web_ui_config():
    settings = read_yaml(SETTINGS_PATH)
    return settings.get("web_ui", {})


def hash_password(password, iterations=120_000):
    if not password:
        return ""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        int(iterations),
    ).hex()
    return f"pbkdf2${int(iterations)}${salt}${digest}"


def verify_password(password, stored_hash):
    if not stored_hash:
        return False
    if stored_hash.startswith("pbkdf2$"):
        try:
            _, iter_str, salt, digest = stored_hash.split("$", 3)
            iterations = int(iter_str)
        except Exception:
            return False
        candidate = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iterations,
        ).hex()
        return hmac.compare_digest(digest, candidate)
    if "$" not in stored_hash:
        legacy = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(stored_hash, legacy)
    salt, digest = stored_hash.split("$", 1)
    legacy = hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()
    return hmac.compare_digest(digest, legacy)


def create_session(username, role):
    token = secrets.token_urlsafe(32)
    created_at = time.time()
    with SESSION_LOCK:
        _purge_expired_sessions()
        SESSION_STORE[token] = {
            "username": username,
            "role": role,
            "created_at": created_at,
        }
        SESSION_USER_INDEX.setdefault(username, set()).add(token)
    try:
        db = Database()
        try:
            db.save_session(token, username, role, created_at=created_at)
        finally:
            db.close()
    except Exception:
        pass
    return token


def invalidate_user_sessions(username):
    with SESSION_LOCK:
        tokens = SESSION_USER_INDEX.pop(username, set())
        for token in tokens:
            SESSION_STORE.pop(token, None)
    try:
        db = Database()
        try:
            db.delete_sessions_by_username(username)
        finally:
            db.close()
    except Exception:
        pass


def get_session_from_token(token):
    if not token:
        return None
    with SESSION_LOCK:
        session = SESSION_STORE.get(token)
        if not session:
            session = None
        else:
            if SESSION_TTL_SECONDS:
                created_at = float(session.get("created_at", 0) or 0)
                if time.time() - created_at > SESSION_TTL_SECONDS:
                    _drop_session(token)
                    return None
            return session
    try:
        db = Database()
        try:
            row = db.get_session(token)
        finally:
            db.close()
    except Exception:
        row = None
    if not row:
        return None
    created_at = float(row.get("created_at", 0) or 0)
    if SESSION_TTL_SECONDS and time.time() - created_at > SESSION_TTL_SECONDS:
        try:
            db = Database()
            try:
                db.delete_session(token)
            finally:
                db.close()
        except Exception:
            pass
        return None
    session = {
        "username": row.get("username"),
        "role": row.get("role"),
        "created_at": created_at,
    }
    with SESSION_LOCK:
        SESSION_STORE[token] = session
        SESSION_USER_INDEX.setdefault(session["username"], set()).add(token)
    return session


def _trim_ui_log(path, keep_days):
    if keep_days <= 0 or not path.exists():
        return
    cutoff = datetime.now() - timedelta(days=keep_days)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    kept = []
    for line in lines:
        ts = None
        if line.startswith("[") and len(line) >= 21:
            raw = line[1:20]
            try:
                ts = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                ts = None
        if ts and ts < cutoff:
            continue
        kept.append(line)
    try:
        path.write_text(("\n".join(kept) + ("\n" if kept else "")), encoding="utf-8")
    except Exception:
        return


def _cleanup_log_files(keep_days):
    if keep_days <= 0:
        return
    cutoff_ts = time.time() - (keep_days * 24 * 60 * 60)
    logs_dir = ROOT_DIR / "data" / "logs"
    if logs_dir.exists():
        for path in logs_dir.glob("*.log"):
            try:
                if path.stat().st_mtime < cutoff_ts:
                    path.unlink()
            except Exception:
                continue
    data_dir = ROOT_DIR / "data"
    if data_dir.exists():
        for path in data_dir.glob("window*_run.log"):
            try:
                if path.stat().st_mtime < cutoff_ts:
                    path.unlink()
            except Exception:
                continue
        for path in data_dir.glob("webui.out"):
            try:
                if path.stat().st_mtime < cutoff_ts:
                    path.unlink()
            except Exception:
                continue


def _cleanup_db_history(history_days):
    if history_days <= 0:
        return
    db = Database()
    try:
        db.cleanup_old_history(history_days=history_days, claim_days=3)
    finally:
        db.close()


def _perform_cleanup():
    try:
        _cleanup_db_history(CLEANUP_HISTORY_DAYS)
        _cleanup_log_files(CLEANUP_LOG_DAYS)
        _trim_ui_log(_runtime_dir() / "logs" / "ui.log", CLEANUP_LOG_DAYS)
    except Exception as exc:
        log.error(f"自动清理失败: {exc}")


def _seconds_until_next_midnight(now=None):
    now = now or datetime.now()
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return max(1, int((next_midnight - now).total_seconds()))


def start_cleanup_daemon():
    def _runner():
        while True:
            time.sleep(_seconds_until_next_midnight())
            _perform_cleanup()

    threading.Thread(target=_runner, daemon=True).start()


class WebUIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        self._query = parse_qs(parsed.query)
        if path.startswith("/api/agent/"):
            self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            return
        if path.startswith("/api/admin/"):
            self._handle_api_get(path[len("/api/admin"):], scope="admin")
            return
        if path.startswith("/api/sub/"):
            self._handle_api_get(path[len("/api/sub"):], scope="sub")
            return
        if path in ("/login", "/admin/login"):
            self._serve_static("/login.html")
            return
        if path in ("/sub/login", "/sub/login/"):
            self._serve_static("/sub_login.html")
            return
        if path in ("/", "/index.html"):
            if not self._has_role("admin"):
                self._redirect("/login")
                return
            self._serve_static("/index.html")
            return
        if path in ("/sub", "/sub/", "/sub/index.html"):
            if not self._has_role("user", prefer_sub=True):
                self._redirect("/sub/login")
                return
            self._serve_static("/sub.html")
            return
        self._serve_static(path)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/agent/"):
            self._handle_api_agent_post(path)
            return
        if path.startswith("/api/client/"):
            self._handle_api_client_post(path)
            return
        if path.startswith("/api/admin/"):
            self._handle_api_post(path[len("/api/admin"):], scope="admin")
            return
        if path.startswith("/api/sub/"):
            self._handle_api_post(path[len("/api/sub"):], scope="sub")
            return
        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
        return

    def log_message(self, format, *args):
        return

    def _handle_api_get(self, path, scope):
        if scope == "admin":
            if not self._has_role("admin", prefer_sub=False):
                self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
        elif scope == "sub":
            if not self._has_role("user", prefer_sub=True):
                self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
        else:
            self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            return

        if scope == "admin":
            if path == "/agent/list":
                owner = ""
                if isinstance(getattr(self, "_query", None), dict):
                    owner = (self._query.get("owner") or [""])[0].strip()
                self._send_json(handle_agent_list(owner=owner))
                return
            if path == "/device-info":
                owner = ""
                if isinstance(getattr(self, "_query", None), dict):
                    owner = (self._query.get("owner") or [""])[0].strip()
                self._send_json(handle_device_info_list(owner=owner))
                return
            if path == "/config":
                self._send_json(load_full_config())
                return
            if path == "/status":
                self._send_json(TASK_MANAGER.status(owner=None))
                return
            if path == "/users":
                self._send_json(_load_user_list())
                return
        if scope == "sub":
            if path == "/config":
                username = self._current_username(prefer_sub=True)
                self._send_json(load_full_config(username))
                return
            if path == "/device_summary":
                owner = self._current_username(prefer_sub=True)
                self._send_json(handle_sub_device_summary(owner))
                return
            if path == "/status":
                owner = self._current_username(prefer_sub=True)
                self._send_json(TASK_MANAGER.status(owner=owner))
                return
            if path == "/agent/list":
                owner = self._current_username(prefer_sub=True)
                self._send_json(handle_agent_list(owner=owner))
                return
            if path == "/device-info":
                owner = self._current_username(prefer_sub=True)
                self._send_json(handle_device_info_list(owner=owner))
                return

        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_api_post(self, path, scope):
        if scope == "admin":
            if path == "/login":
                payload = self._read_json()
                result = handle_login(payload, role="admin")
                if not result["ok"]:
                    self._send_json({"error": result["error"]}, status=HTTPStatus.UNAUTHORIZED)
                    return
                cookie = self._build_session_cookie(
                    result["token"],
                    ADMIN_SESSION_COOKIE_NAME,
                    path="/",
                )
                self._send_json({"ok": True}, cookies=[cookie])
                return
            if not self._has_role("admin", prefer_sub=False):
                self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if path == "/agent/disable":
                payload = self._read_json()
                machine_id = str(payload.get("machine_id") or "").strip()
                if not machine_id:
                    self._send_json({"error": "missing_machine_id"}, status=HTTPStatus.BAD_REQUEST)
                    return
                status_raw = payload.get("status", 1)
                try:
                    status = 1 if int(status_raw) else 0
                except Exception:
                    self._send_json({"error": "invalid_status"}, status=HTTPStatus.BAD_REQUEST)
                    return
                db = Database()
                try:
                    db.set_agent_status(machine_id, status)
                finally:
                    db.close()
                self._send_json({"ok": True})
                return
            if path == "/agent/unbind":
                payload = self._read_json()
                machine_id = str(payload.get("machine_id") or "").strip()
                if not machine_id:
                    self._send_json({"error": "missing_machine_id"}, status=HTTPStatus.BAD_REQUEST)
                    return
                db = Database()
                try:
                    agent = db.get_agent(machine_id)
                    if not agent:
                        self._send_json({"error": "agent_not_found"}, status=HTTPStatus.NOT_FOUND)
                        return
                    db.unbind_agent(machine_id)
                finally:
                    db.close()
                self._send_json({"ok": True})
                return
            if path == "/config":
                payload = self._read_json()
                session = self._current_session(prefer_sub=False) or {}
                save_full_config(
                    payload,
                    actor_username=session.get("username"),
                    actor_role=session.get("role"),
                )
                self._send_json({"ok": True})
                return
            if path == "/restart-window":
                payload = self._read_json()
                ok, error = TASK_MANAGER.restart_window(payload.get("window_token"))
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/start":
                ok, error = TASK_MANAGER.start(owner=None)
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/stop":
                ok, error = TASK_MANAGER.stop(owner=None)
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/pause":
                ok, error = TASK_MANAGER.pause(owner=None)
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/resume":
                ok, error = TASK_MANAGER.resume(owner=None)
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/users/add":
                payload = self._read_json()
                result = handle_user_add(payload)
                if not result["ok"]:
                    self._send_json({"error": result["error"]}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json({"ok": True})
                return
            if path == "/users/update":
                payload = self._read_json()
                result = handle_user_update(payload)
                if not result["ok"]:
                    self._send_json({"error": result["error"]}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json({"ok": True})
                return
            if path == "/users/disable":
                payload = self._read_json()
                result = handle_user_disable(payload)
                if not result["ok"]:
                    self._send_json({"error": result["error"]}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json({"ok": True})
                return
            if path == "/users/delete":
                payload = self._read_json()
                result = handle_user_delete(payload)
                if not result["ok"]:
                    self._send_json({"error": result["error"]}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json({"ok": True})
                return
            self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            return

        if scope == "sub":
            if path == "/login":
                payload = self._read_json()
                result = handle_login(payload, role="user")
                if not result["ok"]:
                    self._send_json({"error": result["error"]}, status=HTTPStatus.UNAUTHORIZED)
                    return
                cookie = self._build_session_cookie(
                    result["token"],
                    SUB_SESSION_COOKIE_NAME,
                    path="/",
                )
                self._send_json({"ok": True}, cookies=[cookie])
                return
            if not self._has_role("user", prefer_sub=True):
                self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                return
            if path == "/config":
                payload = self._read_json()
                session = self._current_session(prefer_sub=True) or {}
                save_full_config(
                    payload,
                    actor_username=session.get("username"),
                    actor_role=session.get("role"),
                )
                self._send_json({"ok": True})
                return
            if path == "/restart-window":
                payload = self._read_json()
                ok, error = TASK_MANAGER.restart_window(
                    payload.get("window_token"),
                    owner=self._current_username(prefer_sub=True),
                )
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/start":
                ok, error = TASK_MANAGER.start(owner=self._current_username(prefer_sub=True))
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/stop":
                ok, error = TASK_MANAGER.stop(owner=self._current_username(prefer_sub=True))
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/pause":
                ok, error = TASK_MANAGER.pause(owner=self._current_username(prefer_sub=True))
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/resume":
                ok, error = TASK_MANAGER.resume(owner=self._current_username(prefer_sub=True))
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/agent/disable":
                payload = self._read_json()
                machine_id = str(payload.get("machine_id") or "").strip()
                if not machine_id:
                    self._send_json({"error": "missing_machine_id"}, status=HTTPStatus.BAD_REQUEST)
                    return
                status_raw = payload.get("status", 1)
                try:
                    status = 1 if int(status_raw) else 0
                except Exception:
                    self._send_json({"error": "invalid_status"}, status=HTTPStatus.BAD_REQUEST)
                    return
                owner = self._current_username(prefer_sub=True)
                db = Database()
                try:
                    agent = db.get_agent(machine_id)
                    if not agent:
                        self._send_json({"error": "agent_not_found"}, status=HTTPStatus.NOT_FOUND)
                        return
                    agent_owner = str(agent.get("owner") or "").strip()
                    if not owner or agent_owner != owner:
                        self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                        return
                    db.set_agent_status(machine_id, status)
                finally:
                    db.close()
                self._send_json({"ok": True})
                return
            if path == "/agent/unbind":
                payload = self._read_json()
                machine_id = str(payload.get("machine_id") or "").strip()
                if not machine_id:
                    self._send_json({"error": "missing_machine_id"}, status=HTTPStatus.BAD_REQUEST)
                    return
                owner = self._current_username(prefer_sub=True)
                db = Database()
                try:
                    agent = db.get_agent(machine_id)
                    if not agent:
                        self._send_json({"error": "agent_not_found"}, status=HTTPStatus.NOT_FOUND)
                        return
                    agent_owner = str(agent.get("owner") or "").strip()
                    if not owner or agent_owner != owner:
                        self._send_json({"error": "unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
                        return
                    db.unbind_agent(machine_id, owner=owner)
                finally:
                    db.close()
                self._send_json({"ok": True})
                return
            if path == "/device-info/add":
                self._send_json(
                    {"error": "client_config_managed_locally"},
                    status=HTTPStatus.CONFLICT,
                )
                return
            if path == "/device-info/update":
                self._send_json(
                    {"error": "client_config_managed_locally"},
                    status=HTTPStatus.CONFLICT,
                )
                return
            if path == "/device-info/delete":
                self._send_json(
                    {"error": "client_config_managed_locally"},
                    status=HTTPStatus.CONFLICT,
                )
                return
            self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            return

        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_api_agent_post(self, path):
        if path == "/api/agent/heartbeat":
            payload = self._read_json()
            result = handle_agent_heartbeat(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/agent/task":
            payload = self._read_json()
            result = handle_agent_task(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/agent/log":
            payload = self._read_json()
            result = handle_agent_log(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/agent/report":
            payload = self._read_json()
            result = handle_agent_report(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_api_client_post(self, path):
        if path == "/api/client/activate":
            payload = self._read_json()
            result = handle_client_activate(payload, request_host=self.headers.get("Host", ""))
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/sync-config":
            payload = self._read_json()
            result = handle_client_sync_config(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/status":
            payload = self._read_json()
            result = handle_client_status(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/start":
            payload = self._read_json()
            result = handle_client_action(payload, "start")
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/stop":
            payload = self._read_json()
            result = handle_client_action(payload, "stop")
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/pause":
            payload = self._read_json()
            result = handle_client_action(payload, "pause")
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/resume":
            payload = self._read_json()
            result = handle_client_action(payload, "resume")
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if path == "/api/client/unbind":
            payload = self._read_json()
            result = handle_client_unbind(payload)
            if not result.get("ok"):
                self._send_json(result, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def _serve_static(self, path):
        if path in ("", "/"):
            file_path = STATIC_DIR / "index.html"
        else:
            file_path = STATIC_DIR / path.lstrip("/")

        if not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return

        mime_type, _ = mimetypes.guess_type(str(file_path))
        mime_type = mime_type or "application/octet-stream"
        with open(file_path, "rb") as f:
            content = f.read()

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", f"{mime_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            length = 0
        body = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(body.decode("utf-8") or "{}")

    def _send_json(self, data, status=HTTPStatus.OK, cookies=None):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        for cookie in cookies or []:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _redirect(self, location):
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        self.end_headers()

    def _parse_cookies(self):
        cookie = self.headers.get("Cookie", "")
        if not cookie:
            return {}
        parsed = {}
        for part in cookie.split(";"):
            if "=" not in part:
                continue
            key, value = part.strip().split("=", 1)
            parsed[key] = value
        return parsed

    def _get_session_token(self, cookie_name, allow_legacy=False):
        cookies = self._parse_cookies()
        token = cookies.get(cookie_name)
        if token:
            return token
        if allow_legacy:
            return cookies.get(SESSION_COOKIE_NAME)
        return None

    def _current_session(self, prefer_sub=False):
        if prefer_sub:
            token = self._get_session_token(SUB_SESSION_COOKIE_NAME)
        else:
            token = self._get_session_token(ADMIN_SESSION_COOKIE_NAME, allow_legacy=True)
        return get_session_from_token(token)

    def _current_username(self, prefer_sub=False):
        session = self._current_session(prefer_sub=prefer_sub)
        return session.get("username") if session else None

    def _has_role(self, required, prefer_sub=False):
        session = self._current_session(prefer_sub=prefer_sub)
        if not session:
            return False
        role = session.get("role")
        if required == "admin":
            return role == "admin"
        if required == "user":
            return role in ("user", "admin")
        return False

    def _build_session_cookie(self, token, cookie_name, path="/"):
        return f"{cookie_name}={token}; Path={path}; HttpOnly; SameSite=Lax"

def load_full_config(username=None):
    settings = read_yaml(SETTINGS_PATH)
    messages = read_yaml(MESSAGES_PATH)
    account_info = {}

    if username:
        db = Database()
        try:
            account = db.get_user_account(username)
            if account is not None:
                expire_at = account.get("expire_at")
                expire_at_val = None
                if expire_at not in (None, ""):
                    try:
                        expire_at_val = float(expire_at)
                    except Exception:
                        expire_at_val = None
                days_left = None
                if expire_at_val:
                    remaining = expire_at_val - time.time()
                    days_left = int((remaining + 86399) // 86400)
                account_info = {
                    "expire_at": expire_at_val,
                    "days_left": days_left,
                }
            templates = db.get_message_templates(username)
            if templates is not None:
                messages["templates"] = templates.get("templates") or []
                messages["probe_templates"] = templates.get("probe_templates") or []
        finally:
            db.close()

    return {
        "settings": settings,
        "messages": messages,
        "account": account_info,
    }


def ui_log(message):
    try:
        log_dir = _runtime_dir() / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "ui.log"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass


def show_ui_alert(title, message):
    if sys.platform.startswith("win"):
        try:
            import ctypes

            ctypes.windll.user32.MessageBoxW(0, message, title, 0x40)
            return
        except Exception:
            pass
    print(f"{title}: {message}")


def start_server(host, port, max_attempts=10):
    last_error = None
    for offset in range(max_attempts):
        current_port = port + offset
        try:
            server = ThreadingHTTPServer((host, current_port), WebUIHandler)
            return server, current_port, None
        except OSError as exc:
            last_error = exc
            continue
    return None, port, last_error


def build_runtime_stats(logs, account_id=None):
    db_stats = _load_db_stats(account_id=account_id) if account_id else _load_db_stats()
    return {
        "sent": db_stats.get("sent_today", 0),
        "failed": db_stats.get("failed_today", 0),
        "db": db_stats,
    }


def _extract_detected_window_count(logs):
    if not logs:
        return 0
    pattern = re.compile(r"已自动识别\s+(\d+)\s+个比特浏览器窗口")
    for line in reversed(logs):
        match = pattern.search(line)
        if match:
            try:
                return max(0, int(match.group(1)))
            except Exception:
                return 0
    return 0


def _load_owner_device_summary(owner, db=None):
    owner = str(owner or "").strip()
    if not owner:
        return {
            "online_devices": 0,
            "max_devices": None,
        }

    owns_db = db is None
    db = db or Database()
    try:
        row = db.conn.execute(
            """
            SELECT COUNT(1) AS total
            FROM agent_registry
            WHERE owner = ?
              AND status = 1
            """,
            (owner,),
        ).fetchone()
        online_devices = int(row["total"] or 0) if row else 0

        max_devices = None
        account = db.get_user_account(owner)
        if account:
            raw_max_devices = account.get("max_devices")
            if raw_max_devices not in (None, ""):
                try:
                    max_devices = int(raw_max_devices)
                except Exception:
                    max_devices = None

        return {
            "online_devices": online_devices,
            "max_devices": max_devices,
        }
    finally:
        if owns_db:
            db.close()


def _load_db_stats(account_id=None):
    db = Database()
    try:
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        today_text = today_start.strftime("%Y-%m-%d %H:%M:%S")
        yesterday_text = yesterday_start.strftime("%Y-%m-%d %H:%M:%S")

        account_filter = str(account_id or "").strip()
        device_total = 0
        device_online = 0
        device_yesterday = 0
        max_devices = None
        if account_filter:
            today_start_ts = today_start.timestamp()
            yesterday_start_ts = yesterday_start.timestamp()
            row = db.conn.execute(
                "SELECT COUNT(1) AS total FROM agent_registry WHERE owner = ?",
                (account_filter,),
            ).fetchone()
            device_total = int(row["total"] or 0) if row else 0
            device_summary = _load_owner_device_summary(account_filter, db=db)
            device_online = int(device_summary.get("online_devices") or 0)
            max_devices = device_summary.get("max_devices")
            row = db.conn.execute(
                """
                SELECT COUNT(1) AS total FROM agent_registry
                WHERE owner = ?
                  AND status = 1
                  AND COALESCE(last_seen, 0) >= ?
                  AND COALESCE(last_seen, 0) < ?
                """,
                (account_filter, yesterday_start_ts, today_start_ts),
            ).fetchone()
            device_yesterday = int(row["total"] or 0) if row else 0

        def count_status(status, start=None, end=None):
            clauses = ["status = ?"]
            params = [status]
            if account_filter:
                clauses.append("account_id = ?")
                params.append(account_filter)
            if start:
                clauses.append("created_at >= ?")
                params.append(start)
            if end:
                clauses.append("created_at < ?")
                params.append(end)
            query = f"SELECT COUNT(1) AS total FROM sent_history WHERE {' AND '.join(clauses)}"
            row = db.conn.execute(query, tuple(params)).fetchone()
            return int(row["total"] or 0) if row else 0

        def count_distinct(field, start=None, end=None, status=None, require_status=False):
            clauses = [f"{field} IS NOT NULL", f"{field} != ''"]
            params = []
            if account_filter:
                clauses.append("account_id = ?")
                params.append(account_filter)
            if require_status and status is not None:
                clauses.append("status = ?")
                params.append(status)
            if start:
                clauses.append("created_at >= ?")
                params.append(start)
            if end:
                clauses.append("created_at < ?")
                params.append(end)
            query = f"SELECT COUNT(DISTINCT {field}) AS total FROM sent_history WHERE {' AND '.join(clauses)}"
            row = db.conn.execute(query, tuple(params)).fetchone()
            return int(row["total"] or 0) if row else 0

        def count_user_accounts(start=None, end=None):
            clauses = []
            params = []
            if account_filter:
                clauses.append("username = ?")
                params.append(account_filter)
            if start:
                clauses.append("created_at >= ?")
                params.append(start)
            if end:
                clauses.append("created_at < ?")
                params.append(end)
            query = "SELECT COUNT(1) AS total FROM user_accounts"
            if clauses:
                query += f" WHERE {' AND '.join(clauses)}"
            row = db.conn.execute(query, tuple(params)).fetchone()
            return int(row["total"] or 0) if row else 0

        user_field = "profile_url" if account_filter else "account_id"
        active_field = user_field

        def build_trend_series(reference_time):
            day_start = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
            boundaries = [day_start + timedelta(hours=idx) for idx in range(25)]
            labels = [
                (day_start + timedelta(hours=idx)).strftime("%H:%M")
                for idx in range(0, 24, 3)
            ]

            send_series = []
            fail_series = []
            user_series = []
            active_series = []

            for idx in range(24):
                start = boundaries[idx].strftime("%Y-%m-%d %H:%M:%S")
                end = boundaries[idx + 1].strftime("%Y-%m-%d %H:%M:%S")
                send_series.append(count_status(1, start=start, end=end))
                fail_series.append(count_status(0, start=start, end=end))
                user_series.append(count_distinct(user_field, start=start, end=end))
                active_series.append(
                    count_distinct(
                        active_field,
                        start=start,
                        end=end,
                        status=1,
                        require_status=True,
                    )
                )

            return {
                "labels": labels,
                "send": send_series,
                "fail": fail_series,
                "user": user_series,
                "active": active_series,
            }

        sent_total = count_status(1)
        failed_total = count_status(0)
        sent_today = count_status(1, start=today_text)
        sent_yesterday = count_status(1, start=yesterday_text, end=today_text)
        failed_today = count_status(0, start=today_text)
        failed_yesterday = count_status(0, start=yesterday_text, end=today_text)

        user_total = count_user_accounts()
        user_today = count_user_accounts(start=today_text)
        user_yesterday = count_user_accounts(start=yesterday_text, end=today_text)

        active_total = count_distinct(active_field, status=1, require_status=True)
        active_today = count_distinct(
            active_field, start=today_text, status=1, require_status=True
        )
        active_yesterday = count_distinct(
            active_field, start=yesterday_text, end=today_text, status=1, require_status=True
        )

        trend = build_trend_series(now)

        return {
            "sent_today": sent_today,
            "sent_yesterday": sent_yesterday,
            "sent_total": sent_total,
            "failed_today": failed_today,
            "failed_yesterday": failed_yesterday,
            "failed_total": failed_total,
            "user_today": user_today,
            "user_yesterday": user_yesterday,
            "user_total": user_total,
            "active_today": active_today,
            "active_yesterday": active_yesterday,
            "active_total": active_total,
            "device_total": device_total,
            "device_online": device_online,
            "device_yesterday": device_yesterday,
            "max_devices": max_devices,
            "trend": trend,
        }
    except Exception as exc:
        log.error(f"读取统计数据失败: {exc}")
        return {}


def _load_user_list():
    db = Database()
    try:
        db.seed_user_accounts_from_sent_history()
        rows = db.list_user_accounts()
        users = []
        for row in rows:
            expire_at_val = None
            days_left = None
            expire_at = row.get("expire_at")
            activation_code = row.get("activation_code") or ""
            if expire_at not in (None, ""):
                try:
                    expire_at_val = float(expire_at)
                except Exception:
                    expire_at_val = None
            if expire_at_val:
                remaining = expire_at_val - time.time()
                days_left = max(0, int((remaining + 86399) // 86400))
            users.append(
                {
                    "username": row.get("username") or "",
                    "created_at": row.get("created_at") or "",
                    "status": row.get("status", 1),
                    "role": row.get("role") or "user",
                    "max_devices": row.get("max_devices"),
                    "expire_at": expire_at_val,
                    "days_left": days_left,
                    "activation_code": activation_code,
                }
            )
        return {
            "users": users,
            "defaults": {},
        }
    except Exception as exc:
        log.error(f"读取用户列表失败: {exc}")
        return {
            "users": [],
            "defaults": {},
        }
    finally:
        db.close()


def handle_sub_device_summary(owner):
    summary = _load_owner_device_summary(owner)
    return {
        "ok": True,
        "online_devices": int(summary.get("online_devices") or 0),
        "max_devices": summary.get("max_devices"),
    }


def handle_login(payload, role="user"):
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    if not username or not password:
        return {"ok": False, "error": "请输入用户名和密码"}
    db = Database()
    try:
        account = db.get_user_account(username)
        if not account:
            return {"ok": False, "error": "账号不存在"}
        if int(account.get("status") or 0) == 0:
            return {"ok": False, "error": "此账户已禁用"}
        account_role = account.get("role") or "user"
        if role == "admin" and account_role != "admin":
            return {"ok": False, "error": "无管理权限"}
        if role == "user":
            expire_at = account.get("expire_at")
            if expire_at not in (None, ""):
                try:
                    expire_at_val = float(expire_at)
                except Exception:
                    expire_at_val = None
                if expire_at_val and time.time() > expire_at_val:
                    return {"ok": False, "error": "账号已到期，请联系管理员续费"}
        stored_hash = account.get("password_hash") or ""
        if not verify_password(password, stored_hash):
            return {"ok": False, "error": "密码错误"}
        if stored_hash and not stored_hash.startswith("pbkdf2$"):
            try:
                db.set_user_password_hash(username, hash_password(password))
            except Exception as exc:
                log.error(f"升级密码哈希失败: {exc}")
        token = create_session(username, account_role)
        return {"ok": True, "token": token}
    finally:
        db.close()


def _verify_agent_request(payload):
    machine_id = str(payload.get("machine_id") or "").strip()
    if not machine_id:
        return False, "missing_machine_id", None
    agent_token = str(payload.get("agent_token") or "").strip()
    if not agent_token:
        return False, "missing_agent_token", None
    db = Database()
    try:
        agent = db.get_agent(machine_id)
        if not agent:
            return False, "agent_not_registered", None
        status_raw = agent.get("status", 1)
        if status_raw is None:
            status_raw = 1
        if int(status_raw) == 0:
            return False, "agent_disabled", None
        owner = str(agent.get("owner") or "").strip()
        if owner:
            account = db.get_user_account(owner)
            expired, _expire_at = _is_account_expired(account)
            if expired:
                return False, "account_expired", None
        stored_token = str(agent.get("token") or "").strip()
        if not stored_token or stored_token != agent_token:
            return False, "invalid_token", None
        expires_at = agent.get("token_expires_at")
        if expires_at and time.time() > float(expires_at):
            return False, "token_expired", None
        return True, None, agent
    finally:
        db.close()


def _auto_bind_device_config(owner, machine_id, bit_api, api_token, source=""):
    owner = str(owner or "").strip()
    machine_id = str(machine_id or "").strip()
    bit_api = str(bit_api or "").strip()
    api_token = str(api_token or "").strip()
    if not owner or not machine_id:
        return False
    if not api_token:
        TASK_MANAGER._append_runtime_log(
            f"=== [自动绑定] 跳过：缺少 api_token owner={owner} machine_id={machine_id} source={source} ==="
        )
        return False
    db = Database()
    try:
        configs = db.list_device_configs(owner)
        matches = []
        for cfg in configs:
            try:
                status = int(cfg.get("status", 1) or 0)
            except Exception:
                status = 0
            if status != 1:
                continue
            if str(cfg.get("api_token") or "").strip() != api_token:
                continue
            matches.append(cfg)
        if not matches:
            TASK_MANAGER._append_runtime_log(
                f"=== [自动绑定] token 未匹配 owner={owner} api_token={api_token} machine_id={machine_id} source={source} ==="
            )
            return False
        if len(matches) > 1:
            ids = ",".join(str(m.get("id") or "") for m in matches)
            TASK_MANAGER._append_runtime_log(
                f"=== [自动绑定] token 冲突 owner={owner} api_token={api_token} configs={ids} source={source} ==="
            )
            return False
        cfg = matches[0]
        existing_machine = str(cfg.get("machine_id") or "").strip()
        if existing_machine and existing_machine != machine_id:
            TASK_MANAGER._append_runtime_log(
                f"=== [自动绑定] 已绑定其他设备 owner={owner} config_id={cfg.get('id')} existing={existing_machine} incoming={machine_id} source={source} ==="
            )
            return False

        current_bit_api = str(cfg.get("bit_api") or "").strip()
        incoming_bit_api = bit_api or current_bit_api
        bit_api_changed = bool(incoming_bit_api) and incoming_bit_api != current_bit_api
        new_machine_id = existing_machine or machine_id

        updated = True
        if (not existing_machine) or bit_api_changed:
            updated = db.update_device_config(
                cfg.get("id"),
                owner,
                cfg.get("name") or "",
                new_machine_id,
                incoming_bit_api,
                api_token,
                cfg.get("status", 1),
            )

        if not existing_machine:
            if updated:
                TASK_MANAGER._append_runtime_log(
                    f"=== [自动绑定] token 匹配成功，已绑定 owner={owner} machine_id={machine_id} config_id={cfg.get('id')} source={source} ==="
                )
                if bit_api_changed:
                    TASK_MANAGER._append_runtime_log(
                        f"=== [自动绑定] bit_api 更新 owner={owner} config_id={cfg.get('id')} old={current_bit_api} new={incoming_bit_api} source={source} ==="
                    )
                return True
            TASK_MANAGER._append_runtime_log(
                f"=== [自动绑定] 绑定失败 owner={owner} machine_id={machine_id} config_id={cfg.get('id')} source={source} ==="
            )
            return False

        if bit_api_changed:
            if updated:
                TASK_MANAGER._append_runtime_log(
                    f"=== [自动绑定] bit_api 更新 owner={owner} config_id={cfg.get('id')} old={current_bit_api} new={incoming_bit_api} source={source} ==="
                )
            else:
                TASK_MANAGER._append_runtime_log(
                    f"=== [自动绑定] bit_api 更新失败 owner={owner} config_id={cfg.get('id')} source={source} ==="
                )

        # 已绑定且无变化时不重复刷成功日志
        if bit_api_changed:
            TASK_MANAGER._append_runtime_log(
                f"=== [自动绑定] token 匹配成功，已绑定 owner={owner} machine_id={new_machine_id} config_id={cfg.get('id')} source={source} ==="
            )
        return True
    finally:
        db.close()

def handle_client_activate(payload, request_host=""):
    activation_code = str(payload.get("activation_code") or "").strip()
    machine_id = str(payload.get("machine_id") or "").strip()
    client_version = str(payload.get("client_version") or "").strip()
    if not activation_code:
        return {"ok": False, "error": "missing_activation_code"}
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    security = _load_agent_security()
    max_devices_default = int(security.get("max_devices_per_account", 1) or 1)
    allow_rebind = bool(security.get("allow_rebind", False))
    token_ttl = int(security.get("token_ttl_seconds", 3600) or 3600)

    db = Database()
    try:
        account = db.get_user_account_by_activation_code(activation_code)
        if not account:
            return {"ok": False, "error": "invalid_activation_code"}
        if int(account.get("status") or 0) == 0:
            return {"ok": False, "error": "account_disabled"}
        expired, expire_at = _is_account_expired(account)
        if expired:
            return {"ok": False, "error": "account_expired"}

        owner = str(account.get("username") or "").strip()
        max_devices = max_devices_default
        account_max = account.get("max_devices")
        if account_max not in (None, ""):
            try:
                max_devices = int(account_max)
            except Exception:
                pass

        existing = db.get_agent(machine_id)
        if existing:
            status_raw = existing.get("status", 1)
            if status_raw is None:
                status_raw = 1
            if int(status_raw) == 0:
                return {"ok": False, "error": "device_disabled"}
            existing_owner = str(existing.get("owner") or "").strip()
            if existing_owner and existing_owner != owner and not allow_rebind:
                return {"ok": False, "error": "device_bound"}

        if max_devices > 0:
            current_count = db.count_agents_for_owner(owner, include_disabled=False)
            if (not existing or str(existing.get("owner") or "").strip() != owner) and current_count >= max_devices:
                return {"ok": False, "error": "device_limit_reached"}

        agent_token = secrets.token_urlsafe(32)
        token_expires_at = time.time() + token_ttl
        existing_last_seen = None
        existing_label = ""
        existing_status = 1
        if existing:
            existing_last_seen = existing.get("last_seen")
            existing_label = str(existing.get("label") or "").strip()
            status_raw = existing.get("status", 1)
            existing_status = 1 if status_raw is None else int(status_raw)
        seed_last_seen = existing_last_seen if existing_last_seen not in (None, "") else 0.1
        db.upsert_agent(
            machine_id,
            existing_label or machine_id,
            seed_last_seen,
            owner=owner,
            status=existing_status,
            token=agent_token,
            token_expires_at=token_expires_at,
            client_version=client_version or None,
        )

        server_url = _build_server_base_url(request_host=request_host)
        return {
            "ok": True,
            "data": {
                "owner": owner,
                "server_url": server_url,
                "expire_at": expire_at,
                "max_devices": max_devices,
                "agent_token": agent_token,
                "token_expires_at": token_expires_at,
                "client_version": client_version or "",
            },
        }
    finally:
        db.close()


def handle_client_unbind(payload):
    verified = _verify_client_identity(payload)
    if not verified.get("ok"):
        return verified
    db = Database()
    try:
        db.unbind_agent(verified.get("machine_id"), owner=verified.get("owner"))
    finally:
        db.close()
    return {"ok": True}


def _verify_client_identity(payload, require_registered=False):
    machine_id = str(payload.get("machine_id") or "").strip()
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    ok, error, agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    owner = str((agent or {}).get("owner") or "").strip()
    if not owner:
        return {"ok": False, "error": "unauthorized"}
    db = Database()
    try:
        account = db.get_user_account(owner)
        if not account:
            return {"ok": False, "error": "owner_not_found"}
        if int(account.get("status") or 0) == 0:
            return {"ok": False, "error": "account_disabled"}
        expired, expire_at = _is_account_expired(account)
        if expired:
            return {"ok": False, "error": "account_expired"}
        if require_registered and not agent:
            return {"ok": False, "error": "agent_not_registered"}
        return {
            "ok": True,
            "owner": owner,
            "machine_id": machine_id,
            "agent": agent,
            "expire_at": expire_at,
            "max_devices": account.get("max_devices"),
        }
    finally:
        db.close()


def _sync_client_device_config(owner, machine_id, bit_api, api_token, name=""):
    owner = str(owner or "").strip()
    machine_id = str(machine_id or "").strip()
    bit_api = str(bit_api or "").strip()
    api_token = str(api_token or "").strip()
    display_name = str(name or "").strip() or machine_id
    if not owner:
        return False, "owner_not_found", None
    if not bit_api or not api_token:
        return False, "missing_fields", None

    db = Database()
    try:
        configs = db.list_device_configs(owner)
        matches = [
            cfg for cfg in configs if str(cfg.get("api_token") or "").strip() == api_token
        ]
        if len(matches) > 1:
            return False, "api_token_conflict", None
        if matches:
            cfg = matches[0]
            existing_machine = str(cfg.get("machine_id") or "").strip()
            if existing_machine and existing_machine != machine_id:
                return False, "device_bound", None
            updated = db.update_device_config(
                cfg.get("id"),
                owner,
                display_name or str(cfg.get("name") or "").strip() or machine_id,
                existing_machine,
                bit_api,
                api_token,
                1,
            )
            if not updated:
                return False, "not_found", None
            return True, None, int(cfg.get("id") or 0)
        new_id = db.add_device_config(owner, display_name, "", bit_api, api_token, 1)
        return True, None, new_id
    finally:
        db.close()


def handle_client_sync_config(payload):
    verified = _verify_client_identity(payload)
    if not verified.get("ok"):
        return verified

    bit_api = str(payload.get("bit_api") or "").strip()
    api_token = str(payload.get("api_token") or "").strip()
    display_name = str(payload.get("name") or payload.get("label") or "").strip()
    ok, error, config_id = _sync_client_device_config(
        verified.get("owner"),
        verified.get("machine_id"),
        bit_api,
        api_token,
        name=display_name,
    )
    if not ok:
        return {"ok": False, "error": error}
    return {
        "ok": True,
        "data": {
            "config_id": config_id,
        },
    }


def handle_client_status(payload):
    verified = _verify_client_identity(payload)
    if not verified.get("ok"):
        return verified

    machine_id = verified.get("machine_id") or ""
    owner = verified.get("owner") or ""
    api_token = str(payload.get("api_token") or "").strip()
    settings = _load_agent_settings()
    heartbeat_ttl = float(settings.get("heartbeat_ttl", 180) or 180)
    now = time.time()

    db = Database()
    try:
        agent = db.get_agent(machine_id)
        agent_online = False
        client_version = ""
        if agent:
            last_seen = agent.get("last_seen")
            try:
                agent_online = bool(last_seen) and (now - float(last_seen) <= heartbeat_ttl)
            except Exception:
                agent_online = False
            client_version = str(agent.get("client_version") or "").strip()

        configs = db.list_device_configs(owner)
        target_cfg = None
        for cfg in configs:
            if str(cfg.get("machine_id") or "").strip() == machine_id:
                target_cfg = cfg
                break
        if not target_cfg and api_token:
            for cfg in configs:
                if str(cfg.get("api_token") or "").strip() == api_token:
                    target_cfg = cfg
                    break

        running_row = db.conn.execute(
            """
            SELECT COUNT(1) AS total
            FROM agent_tasks
            WHERE status = 'running' AND assigned_machine_id = ?
            """,
            (machine_id,),
        ).fetchone()
        pending_row = db.conn.execute(
            """
            SELECT COUNT(1) AS total
            FROM agent_tasks
            WHERE status = 'pending' AND assigned_machine_id = ?
            """,
            (machine_id,),
        ).fetchone()
    finally:
        db.close()

    running_count = int(running_row["total"] or 0) if running_row else 0
    pending_count = int(pending_row["total"] or 0) if pending_row else 0
    state = "offline"
    state_text = "执行端离线"
    if running_count > 0:
        state = "running"
        state_text = "任务运行中"
    elif pending_count > 0:
        state = "queued"
        state_text = "任务排队中"
    elif agent_online:
        state = "idle"
        state_text = "执行端在线，等待任务"

    return {
        "ok": True,
        "data": {
            "owner": owner,
            "machine_id": machine_id,
            "config_synced": bool(target_cfg),
            "device_bound": bool(
                target_cfg and str(target_cfg.get("machine_id") or "").strip() == machine_id
            ),
            "agent_online": agent_online,
            "task_running": running_count > 0,
            "task_pending": pending_count > 0,
            "state": state,
            "state_text": state_text,
            "client_version": client_version,
            "bit_api": str((target_cfg or {}).get("bit_api") or "").strip(),
            "api_token": str((target_cfg or {}).get("api_token") or "").strip(),
        },
    }


def handle_client_action(payload, action):
    verified = _verify_client_identity(payload, require_registered=True)
    if not verified.get("ok"):
        return verified

    bit_api = str(payload.get("bit_api") or "").strip()
    api_token = str(payload.get("api_token") or "").strip()
    if bit_api and api_token:
        ok, error, _config_id = _sync_client_device_config(
            verified.get("owner"),
            verified.get("machine_id"),
            bit_api,
            api_token,
            name=str(payload.get("name") or payload.get("label") or "").strip(),
        )
        if not ok:
            return {"ok": False, "error": error}

    ok, error = TASK_MANAGER.enqueue_machine_action(
        action,
        owner_name=verified.get("owner"),
        machine_id=verified.get("machine_id"),
        api_token=api_token,
        allow_disabled=(action == "stop"),
        wait_online_seconds=4.0 if action == "start" else 0.0,
    )
    if not ok:
        return {"ok": False, "error": error or "执行失败"}
    return {"ok": True}


def handle_agent_heartbeat(payload):
    ok, error, _agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    machine_id = str(payload.get("machine_id") or "").strip()
    client_version = str(payload.get("client_version") or "").strip() or None
    bit_api = str(payload.get("bit_api") or "").strip()
    api_token = str(payload.get("api_token") or "").strip()
    now = time.time()
    db = Database()
    try:
        if not db.get_agent(machine_id):
            return {"ok": False, "error": "agent_not_registered"}
        db.mark_agent_seen(machine_id, now, client_version=client_version)
    finally:
        db.close()
    owner = str((_agent or {}).get("owner") or "").strip()
    _auto_bind_device_config(owner, machine_id, bit_api, api_token, source="heartbeat")
    return {"ok": True}


def handle_agent_list(owner=""):
    settings = _load_agent_settings()
    ttl = settings.get("heartbeat_ttl")
    now = time.time()
    db = Database()
    try:
        agents = []
        owner = (owner or "").strip()
        for row in db.list_agents():
            if owner and str(row.get("owner") or "").strip() != owner:
                continue
            last_seen = float(row.get("last_seen") or 0)
            online = (now - last_seen) <= ttl if last_seen else False
            agents.append(
                {
                    "machine_id": row.get("machine_id") or "",
                    "label": row.get("label") or "",
                    "last_seen": last_seen,
                    "online": online,
                    "owner": row.get("owner") or "",
                    "status": int(1 if row.get("status", None) is None else row.get("status")),
                    "client_version": row.get("client_version") or "",
                }
            )
        return {"ok": True, "agents": agents}
    finally:
        db.close()


def handle_device_info_list(owner=""):
    settings = _load_agent_settings()
    ttl = float(settings.get("heartbeat_ttl", 180) or 180)
    now = time.time()
    owner = (owner or "").strip()
    db = Database()
    try:
        configs = db.list_device_configs(owner) if owner else db.list_all_device_configs()
        agent_map = {
            str(row.get("machine_id") or "").strip(): row
            for row in db.list_agents()
            if str(row.get("machine_id") or "").strip()
        }
        rows = []
        for cfg in configs:
            machine_id = str(cfg.get("machine_id") or "").strip()
            agent = agent_map.get(machine_id) if machine_id else None
            last_seen = 0.0
            online = False
            client_version = ""
            if agent:
                try:
                    last_seen = float(agent.get("last_seen") or 0)
                except Exception:
                    last_seen = 0.0
                online = bool(last_seen) and (now - last_seen <= ttl)
                client_version = str(agent.get("client_version") or "").strip()
            rows.append(
                {
                    "id": cfg.get("id"),
                    "owner": cfg.get("owner") or "",
                    "name": cfg.get("name") or "",
                    "machine_id": machine_id,
                    "bit_api": str(cfg.get("bit_api") or "").strip(),
                    "api_token": str(cfg.get("api_token") or "").strip(),
                    "status": int(1 if cfg.get("status", None) is None else cfg.get("status")),
                    "online": online,
                    "last_seen": last_seen,
                    "client_version": client_version,
                }
            )
        return {"ok": True, "configs": rows}
    finally:
        db.close()


def handle_agent_task(payload):
    ok, error, _agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    machine_id = str(payload.get("machine_id") or "").strip()
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    client_version = str(payload.get("client_version") or "").strip() or None
    db = Database()
    try:
        db.mark_agent_seen(machine_id, time.time(), client_version=client_version)
        pause_global = False
        pause_owner = None
        try:
            pause_global = db.is_pause_state()
        except Exception:
            pause_global = False
        task = db.claim_agent_task(machine_id)
        if not task:
            return {"ok": True, "task": None, "pause_global": pause_global}
        try:
            payload_data = json.loads(task.get("payload") or "{}")
        except Exception:
            payload_data = {}
        task_owner = task.get("owner") or ""
        if task_owner:
            try:
                pause_owner = db.is_pause_state(task_owner)
            except Exception:
                pause_owner = None
        return {
            "ok": True,
            "task": {
                "id": task.get("id"),
                "owner": task.get("owner") or "",
                "action": task.get("action") or "",
                "window_token": task.get("window_token") or "",
                "payload": payload_data,
            },
            "pause_global": pause_global,
            "pause_owner": pause_owner,
        }
    finally:
        db.close()


def handle_agent_log(payload):
    ok, error, _agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    machine_id = str(payload.get("machine_id") or "").strip()
    owner = str(payload.get("owner") or "").strip()
    lines = payload.get("lines")
    if isinstance(lines, str):
        lines = [lines]
    if not isinstance(lines, list):
        lines = []
    prefix = ""
    if owner and machine_id:
        prefix = f"[{owner}@{machine_id}] "
    elif owner:
        prefix = f"[{owner}] "
    elif machine_id:
        prefix = f"[{machine_id}] "
    for line in lines:
        if not line:
            continue
        TASK_MANAGER._append_runtime_log(f"{prefix}{line}")
    return {"ok": True}


def handle_agent_report(payload):
    ok, error, _agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    task_id = payload.get("task_id")
    status = str(payload.get("status") or "").strip() or "done"
    error = str(payload.get("error") or "").strip() or None
    exit_code = payload.get("exit_code")
    if not task_id:
        return {"ok": False, "error": "missing_task_id"}
    db = Database()
    try:
        db.update_agent_task(task_id, status, error=error, exit_code=exit_code)
    finally:
        db.close()
    return {"ok": True}


def _parse_expire_at(raw_value):
    if raw_value in (None, ""):
        return None, None
    if isinstance(raw_value, (int, float)):
        return float(raw_value), None
    text = str(raw_value).strip()
    if not text:
        return None, None
    if text.isdigit():
        return float(text), None
    try:
        normalized = text.replace(" ", "T") if " " in text and "T" not in text else text
        dt = datetime.fromisoformat(normalized)
        return dt.timestamp(), None
    except Exception:
        return None, "到期时间格式不正确"


PLAN_MONTH_OPTIONS = (1, 3, 6, 12)


def _parse_plan_months(raw_value, allow_empty=False):
    if raw_value in (None, ""):
        return (None, None) if allow_empty else (None, "请选择租期")
    try:
        months = int(raw_value)
    except Exception:
        return None, "租期必须为数字"
    if months not in PLAN_MONTH_OPTIONS:
        return None, "租期只能选择 1/3/6/12 个月"
    return months, None


def _add_months(base_dt, months):
    month_index = base_dt.month - 1 + months
    year = base_dt.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base_dt.day, calendar.monthrange(year, month)[1])
    return base_dt.replace(year=year, month=month, day=day)


def _compute_expire_at_from_months(months, base_ts=None):
    base_dt = datetime.fromtimestamp(base_ts) if base_ts else datetime.now()
    return _add_months(base_dt, months).timestamp()


def _generate_activation_code():
    return str(uuid.uuid4()).upper()


def _is_account_expired(account):
    if not account:
        return False, None
    expire_at = account.get("expire_at")
    if expire_at in (None, ""):
        return False, None
    try:
        expire_ts = float(expire_at)
    except Exception:
        return False, None
    return (bool(expire_ts) and time.time() > expire_ts), expire_ts


def handle_user_add(payload):
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    raw_max_devices = payload.get("max_devices")
    plan_months, plan_error = _parse_plan_months(payload.get("plan_months"))
    if plan_error:
        return {"ok": False, "error": plan_error}
    max_devices = None
    if raw_max_devices not in (None, ""):
        try:
            max_devices = int(raw_max_devices)
        except Exception:
            return {"ok": False, "error": "设备数量必须为数字"}
    if not username:
        return {"ok": False, "error": "用户名不能为空"}
    if not password:
        return {"ok": False, "error": "密码不能为空"}
    db = Database()
    try:
        if db.get_user_account(username):
            return {"ok": False, "error": "用户名已存在"}
        password_hash = hash_password(password)
        expire_at = _compute_expire_at_from_months(plan_months)
        activation_code = _generate_activation_code()
        db.add_user_account(
            username,
            password_hash,
            role="user",
            max_devices=max_devices,
            expire_at=expire_at,
            activation_code=activation_code,
        )
        return {"ok": True}
    finally:
        db.close()


def handle_user_update(payload):
    original_username = str(payload.get("original_username") or "").strip()
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    raw_max_devices = payload.get("max_devices")
    plan_months, plan_error = _parse_plan_months(
        payload.get("plan_months"),
        allow_empty=True,
    )
    if plan_error:
        return {"ok": False, "error": plan_error}
    max_devices = None
    if raw_max_devices not in (None, ""):
        try:
            max_devices = int(raw_max_devices)
        except Exception:
            return {"ok": False, "error": "设备数量必须为数字"}
    if not original_username:
        return {"ok": False, "error": "缺少原始用户名"}
    if not username:
        return {"ok": False, "error": "用户名不能为空"}
    db = Database()
    try:
        current = db.get_user_account(original_username)
        if not current:
            return {"ok": False, "error": "账号不存在"}
        if username != original_username and db.get_user_account(username):
            return {"ok": False, "error": "用户名已存在"}
        password_hash = hash_password(password) if password else None
        expire_at = None
        if plan_months:
            expire_at = _compute_expire_at_from_months(plan_months)
        db.update_user_account(
            original_username,
            username,
            password_hash,
            role=current.get("role"),
            max_devices=max_devices,
            expire_at=expire_at,
        )
        if username != original_username:
            invalidate_user_sessions(original_username)
        return {"ok": True}
    finally:
        db.close()


def handle_user_disable(payload):
    username = str(payload.get("username") or "").strip()
    disabled = bool(payload.get("disabled", True))
    if not username:
        return {"ok": False, "error": "用户名不能为空"}
    db = Database()
    try:
        if not db.get_user_account(username):
            return {"ok": False, "error": "账号不存在"}
        db.set_user_account_status(username, 0 if disabled else 1)
    finally:
        db.close()
    if disabled:
        invalidate_user_sessions(username)
        TASK_MANAGER.stop_for_owner(username)
    return {"ok": True}


def handle_user_delete(payload):
    username = str(payload.get("username") or "").strip()
    if not username:
        return {"ok": False, "error": "用户名不能为空"}
    db = Database()
    try:
        if not db.get_user_account(username):
            return {"ok": False, "error": "账号不存在"}
        db.delete_user_account(username)
    finally:
        db.close()
    invalidate_user_sessions(username)
    TASK_MANAGER.stop_for_owner(username)
    return {"ok": True}


def save_full_config(payload, actor_username=None, actor_role=None):
    current_settings = read_yaml(SETTINGS_PATH)
    settings_payload = payload.get("settings") or {}
    messages_payload = payload.get("messages") or {}
    current_messages = read_yaml(MESSAGES_PATH) or {}

    if actor_role != "admin":
        if actor_username:
            db = Database()
            try:
                existing = db.get_message_templates(actor_username) or {}
                templates = messages_payload.get("templates")
                probe_templates = messages_payload.get("probe_templates")
                if templates is None:
                    templates = existing.get("templates") or []
                if probe_templates is None:
                    probe_templates = existing.get("probe_templates") or []
                db.set_message_templates(actor_username, templates or [], probe_templates or [])
            finally:
                db.close()
        return

    settings = dict(current_settings or {})
    settings.update(settings_payload)
    settings["browser"] = {"auto_discover_bitbrowser": True}

    task_settings = settings.setdefault("task_settings", {})
    task_settings.update(settings_payload.get("task_settings") or {})
    task_settings["max_messages_per_account"] = 0
    try:
        task_settings["max_windows"] = max(0, int(task_settings.get("max_windows", 0)))
    except Exception:
        task_settings["max_windows"] = 0
    task_settings["scroll_times"] = 0
    task_settings["group_list_scroll_times"] = 0
    task_settings["resume_search_times"] = 20
    task_settings["idle_scroll_limit"] = 3
    task_settings["account_restricted_blocked_threshold"] = 2
    task_settings["account_restricted_signal_threshold"] = 3
    task_settings["typing_delay"] = [100, 300]
    messages = dict(current_messages)
    messages.update(messages_payload)
    messages["probe_templates"] = (
        messages.get("probe_templates")
        or current_messages.get("probe_templates")
        or []
    )
    messages["templates"] = (
        messages.get("templates")
        or current_messages.get("templates")
        or []
    )
    write_yaml(SETTINGS_PATH, settings)
    if actor_role == "admin":
        write_yaml(MESSAGES_PATH, messages)

    if actor_username:
        db = Database()
        try:
            account = db.get_user_account(actor_username)
            if account:
                db.update_user_account(
                    actor_username,
                    actor_username,
                    None,
                    role=account.get("role"),
                )
        finally:
            db.close()


def ensure_web_ui_defaults():
    settings = read_yaml(SETTINGS_PATH)
    web_cfg = settings.setdefault("web_ui", {})
    changed = False
    defaults = {
        "host": "127.0.0.1",
        "port": DEFAULT_WEB_PORT,
    }
    for key, value in defaults.items():
        if key not in web_cfg:
            web_cfg[key] = value
            changed = True
    if changed:
        write_yaml(SETTINGS_PATH, settings)


def ensure_admin_account():
    settings = read_yaml(SETTINGS_PATH)
    admin_cfg = settings.setdefault("admin_account", {})
    username = (admin_cfg.get("username") or "admin").strip()
    password = (admin_cfg.get("password") or "CHANGE_ME").strip()
    if not admin_cfg.get("username") or not admin_cfg.get("password"):
        admin_cfg["username"] = username
        admin_cfg["password"] = password
        write_yaml(SETTINGS_PATH, settings)
        ui_log("已初始化默认管理员账号，请尽快修改密码。")

    db = Database()
    try:
        account = db.get_user_account(username)
        if account is None:
            db.add_user_account(
                username,
                hash_password(password),
                role="admin",
            )
            return
        if (account.get("role") or "user") != "admin":
            db.set_user_role(username, "admin")
    finally:
        db.close()


def ensure_activation_codes_uuid():
    db = Database()
    try:
        result = db.migrate_activation_codes_to_uuid()
        updated = int(result.get("updated") or 0)
        if updated:
            ui_log(f"已迁移授权码为 UUID：{updated} 条")
    finally:
        db.close()


def main():
    ensure_web_ui_defaults()
    ensure_admin_account()
    ensure_activation_codes_uuid()
    start_cleanup_daemon()
    cfg = web_ui_config()
    host = cfg.get("host", "127.0.0.1")
    port = int(cfg.get("port", DEFAULT_WEB_PORT))
    server, bound_port, error = start_server(host, port, max_attempts=1)
    if not server:
        ui_log(f"启动失败，端口 {port} 无法监听: {error}")
        show_ui_alert("启动失败", f"端口 {port} 已被占用，请关闭占用进程后重试。")
        return

    if bound_port != port:
        settings = read_yaml(SETTINGS_PATH)
        settings.setdefault("web_ui", {})["port"] = bound_port
        write_yaml(SETTINGS_PATH, settings)
        ui_log(f"端口 {port} 被占用，已切换到 {bound_port}")
        show_ui_alert("端口已切换", f"端口 {port} 被占用，已切换到 {bound_port}")

    url = f"http://{host}:{bound_port}"
    print(f"本地管理后台已启动：{url}")
    ui_log(f"管理后台已启动：{url}")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    server.serve_forever()


if __name__ == "__main__":
    main()
