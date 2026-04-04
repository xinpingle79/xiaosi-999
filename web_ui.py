import errno
import calendar
import copy
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

from utils.helpers import ServerDatabase as Database
from utils.logger import log


ROOT_DIR = (
    Path(sys.executable).resolve().parent
    if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent
)
STATIC_DIR = ROOT_DIR / "webui"
SETTINGS_PATH = ROOT_DIR / "config" / "server.yaml"
MESSAGES_PATH = ROOT_DIR / "config" / "messages.server.yaml"

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
STALE_PENDING_SECONDS = 60.0
STALE_RUNNING_SECONDS = 120.0
DB_STATS_CACHE_TTL_SECONDS = 5.0
USER_LIST_CACHE_TTL_SECONDS = 10.0
RUNTIME_READ_CACHE_LOCK = threading.Lock()
DB_STATS_CACHE = {}
USER_LIST_CACHE = {}
FORMAL_RUNTIME_ACTIONS = ("start", "restart_window")
COLLECT_ACTIONS = ("collect_groups",)
CONTROL_ACTIONS = ("stop", "pause", "resume", "stop_collect")
LEGACY_POPUP_STOP_RULE_GROUP_KEYS = (
    "stranger_limit",
    "message_request_limit",
)
LEGACY_PERMANENT_SKIP_RULE_GROUP_KEYS = (
    "messenger_login_required",
    "account_cannot_message",
)


def _parse_window_token_list(value):
    text = str(value or "").strip()
    if not text:
        return []
    tokens = []
    seen = set()
    for raw in re.split(r"[、,，/\s]+", text):
        token = str(raw or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _format_window_token_list(value):
    tokens = _parse_window_token_list(value)
    if not tokens:
        return ""
    return "/".join(tokens) + "/"


def _clone_runtime_read_cache_value(value):
    return copy.deepcopy(value)


def _get_runtime_read_cache(cache_store, cache_key):
    now = time.time()
    with RUNTIME_READ_CACHE_LOCK:
        entry = cache_store.get(cache_key)
        if not entry:
            return None
        if float(entry.get("expires_at") or 0) <= now:
            cache_store.pop(cache_key, None)
            return None
        return _clone_runtime_read_cache_value(entry.get("value"))


def _set_runtime_read_cache(cache_store, cache_key, value, ttl_seconds):
    with RUNTIME_READ_CACHE_LOCK:
        cache_store[cache_key] = {
            "expires_at": time.time() + max(float(ttl_seconds or 0), 0.0),
            "value": _clone_runtime_read_cache_value(value),
        }


def _invalidate_runtime_read_caches(clear_db_stats=False, clear_user_list=False):
    with RUNTIME_READ_CACHE_LOCK:
        if clear_db_stats:
            DB_STATS_CACHE.clear()
        if clear_user_list:
            USER_LIST_CACHE.clear()


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


def _sanitize_server_settings(data):
    settings = dict(data or {})
    settings.pop("browser", None)
    settings["task_settings"] = _normalize_runtime_task_settings(settings.get("task_settings"))
    return settings


def _normalize_runtime_task_settings(data):
    raw = dict(data or {}) if isinstance(data, dict) else {}
    normalized = {
        "max_messages_per_account": 0,
        "scroll_times": 0,
        "group_list_scroll_times": 0,
        "resume_search_times": 20,
        "idle_scroll_limit": 3,
        "max_windows": 0,
    }
    for key, default in tuple(normalized.items()):
        try:
            normalized[key] = max(0, int(raw.get(key, default) or default))
        except Exception:
            normalized[key] = default
    return normalized


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
        self._runtime_logs = deque(maxlen=1200)
        self._control_logs = deque(maxlen=400)
        self._log_sequence = 0

    def _next_log_sequence_locked(self):
        self._log_sequence += 1
        return self._log_sequence

    def _normalize_log_scope(self, owner="", machine_id=""):
        return (
            str(owner or "").strip(),
            str(machine_id or "").strip(),
        )

    def _decorate_control_log_message(self, message, owner="", machine_id=""):
        text = str(message or "").strip()
        if not text:
            return ""
        owner, machine_id = self._normalize_log_scope(owner, machine_id)
        if owner and machine_id:
            return f"[{owner}@{machine_id}] {text}"
        if owner:
            return f"[{owner}] {text}"
        if machine_id:
            return f"[{machine_id}] {text}"
        return text

    def _make_log_entry_locked(self, message, owner="", machine_id="", decorate=False):
        text = str(message or "").strip()
        if decorate:
            text = self._decorate_control_log_message(text, owner=owner, machine_id=machine_id)
        if not text:
            return None
        owner, machine_id = self._normalize_log_scope(owner, machine_id)
        return {
            "seq": self._next_log_sequence_locked(),
            "owner": owner,
            "machine_id": machine_id,
            "message": text,
        }

    def _append_control_log(self, message, owner="", machine_id="", already_locked=False):
        if already_locked:
            entry = self._make_log_entry_locked(
                message,
                owner=owner,
                machine_id=machine_id,
                decorate=True,
            )
            if entry:
                self._control_logs.append(entry)
            return
        with self._lock:
            entry = self._make_log_entry_locked(
                message,
                owner=owner,
                machine_id=machine_id,
                decorate=True,
            )
            if entry:
                self._control_logs.append(entry)

    def _append_runtime_log(self, message, owner="", machine_id="", already_locked=False):
        if already_locked:
            entry = self._make_log_entry_locked(
                message,
                owner=owner,
                machine_id=machine_id,
                decorate=False,
            )
            if entry:
                self._runtime_logs.append(entry)
            return
        with self._lock:
            entry = self._make_log_entry_locked(
                message,
                owner=owner,
                machine_id=machine_id,
                decorate=False,
            )
            if entry:
                self._runtime_logs.append(entry)

    def _log_entry_matches_scope(self, entry, owner="", machine_id=""):
        owner_filter, machine_filter = self._normalize_log_scope(owner, machine_id)
        if not owner_filter and not machine_filter:
            return True
        if isinstance(entry, dict):
            owner_hit = (not owner_filter) or str(entry.get("owner") or "").strip() == owner_filter
            machine_hit = (not machine_filter) or str(entry.get("machine_id") or "").strip() == machine_filter
            return owner_hit and machine_hit
        message = str(entry or "").strip()
        if not message:
            return False
        return bool(_filter_runtime_logs([message], account_id=owner_filter, machine_id=machine_filter))

    def _filter_log_entries_locked(self, entries, owner="", machine_id=""):
        return [
            entry
            for entry in (entries or [])
            if self._log_entry_matches_scope(entry, owner=owner, machine_id=machine_id)
        ]

    def _render_log_messages_locked(self, entries):
        normalized = []
        fallback_index = 0
        for entry in entries or []:
            if isinstance(entry, dict):
                message = str(entry.get("message") or "").strip()
                if not message:
                    continue
                try:
                    seq = int(entry.get("seq") or 0)
                except Exception:
                    seq = 0
            else:
                message = str(entry or "").strip()
                if not message:
                    continue
                seq = -(10**9) + fallback_index
                fallback_index += 1
            normalized.append((seq, message))
        normalized.sort(key=lambda item: item[0])
        return [message for _, message in normalized]

    def _clear_scoped_logs_locked(self, queue_name, owner="", machine_id=""):
        attr = "_runtime_logs" if queue_name == "runtime" else "_control_logs"
        current_queue = getattr(self, attr)
        maxlen = current_queue.maxlen
        owner_filter, machine_filter = self._normalize_log_scope(owner, machine_id)
        if not owner_filter and not machine_filter:
            setattr(self, attr, deque(maxlen=maxlen))
            return
        retained = [
            entry
            for entry in current_queue
            if not self._log_entry_matches_scope(entry, owner=owner_filter, machine_id=machine_filter)
        ]
        setattr(self, attr, deque(retained, maxlen=maxlen))

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

    def _find_existing_machine_action_task(
        self,
        db,
        owner_name,
        machine_id,
        action,
        window_token=None,
    ):
        clauses = [
            "owner = ?",
            "assigned_machine_id = ?",
            "action = ?",
            "status IN ('pending', 'running')",
        ]
        params = [owner_name, machine_id, action]
        if str(action or "").strip() == "restart_window":
            clauses.append("ifnull(window_token, '') = ?")
            params.append(str(window_token or "").strip())
        row = db.fetch_one(
            f"""
            SELECT id, status
            FROM agent_tasks
            WHERE {' AND '.join(clauses)}
            ORDER BY id DESC
            LIMIT 1
            """,
            tuple(params),
        )
        if not row:
            return None
        return {
            "id": row["id"],
            "status": row["status"],
        }

    def _find_later_terminal_machine_action(self, db, machine_id, task_id, action="", window_token=None):
        clauses = [
            "assigned_machine_id = ?",
            "id > ?",
            "status IN ('done', 'failed')",
        ]
        params = [machine_id, task_id]
        action = str(action or "").strip()
        if action:
            clauses.append("action = ?")
            params.append(action)
        if window_token is not None:
            clauses.append("ifnull(window_token, '') = ?")
            params.append(str(window_token or "").strip())
        row = db.fetch_one(
            f"""
            SELECT id, action, status, error
            FROM agent_tasks
            WHERE {' AND '.join(clauses)}
            ORDER BY id DESC
            LIMIT 1
            """,
            tuple(params),
        )
        if not row:
            return None
        return {
            "id": int(row["id"]),
            "action": str(row["action"] or "").strip(),
            "status": str(row["status"] or "").strip(),
            "error": str(row["error"] or "").strip(),
        }

    def _cleanup_stale_machine_action_tasks(
        self,
        db,
        owner_name,
        machine_id,
        heartbeat_ttl,
        trigger="",
        already_locked=False,
    ):
        machine_id = str(machine_id or "").strip()
        if not machine_id:
            return []
        now = time.time()
        stale_rows = []
        agent = db.get_agent(machine_id)
        last_seen = 0.0
        if agent:
            try:
                last_seen = float(agent.get("last_seen") or 0)
            except Exception:
                last_seen = 0.0
        ttl = max(float(heartbeat_ttl or 180), 60.0)
        agent_online = bool(last_seen) and (now - last_seen <= ttl)
        trigger_label = {
            "client_status": "客户端状态刷新",
            "enqueue_machine_action": "下发客户端任务前",
            "enqueue_owner_action": "下发管理端任务前",
            "agent_heartbeat": "执行端心跳",
            "agent_task_poll": "执行端取任务",
        }.get(str(trigger or "").strip(), "任务状态治理")
        rows = db.fetch_all(
            """
            SELECT id, action, window_token, status, updated_at
            FROM agent_tasks
            WHERE assigned_machine_id = ?
              AND status IN ('pending', 'running')
            ORDER BY id ASC
            """,
            (machine_id,),
        )
        for row in rows:
            task_id = int(row["id"])
            action = str(row["action"] or "").strip()
            window_token = str(row["window_token"] or "").strip()
            status = str(row["status"] or "").strip()
            updated_at = float(row["updated_at"] or 0.0)
            age_seconds = max(now - updated_at, 0.0) if updated_at else 0.0
            stale_error = None
            cleanup_message = ""
            if status == "pending":
                if age_seconds >= STALE_PENDING_SECONDS:
                    stale_error = "stale_queue_cleanup"
                    cleanup_message = (
                        f"=== 检测到陈旧排队任务#{task_id}，已自动清理：执行端 {machine_id} "
                        f"排队 {age_seconds:.0f} 秒仍未被真正接手（{trigger_label}） ==="
                    )
            elif status == "running":
                if age_seconds < STALE_RUNNING_SECONDS:
                    continue
                if action == "start":
                    later_stop = self._find_later_terminal_machine_action(
                        db,
                        machine_id,
                        task_id,
                        action="stop",
                        window_token=None,
                    )
                    if later_stop:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"后续 stop 任务#{later_stop['id']} 已结束（{trigger_label}） ==="
                        )
                    elif not agent_online:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"持续 {age_seconds:.0f} 秒无状态更新且执行端无心跳（{trigger_label}） ==="
                        )
                elif action == "collect_groups":
                    later_collect = self._find_later_terminal_machine_action(
                        db,
                        machine_id,
                        task_id,
                        action="collect_groups",
                        window_token=None,
                    )
                    if later_collect:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧采集任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"后续采集任务#{later_collect['id']} 已结束（{trigger_label}） ==="
                        )
                    elif not agent_online:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧采集任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"持续 {age_seconds:.0f} 秒无状态更新且执行端无心跳（{trigger_label}） ==="
                        )
                elif action in {"stop", "pause", "resume", "stop_collect"}:
                    later_same = self._find_later_terminal_machine_action(
                        db,
                        machine_id,
                        task_id,
                        action=action,
                        window_token=None,
                    )
                    if later_same:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"后续 {action} 任务#{later_same['id']} 已结束（{trigger_label}） ==="
                        )
                    elif not agent_online:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"持续 {age_seconds:.0f} 秒无状态更新且执行端无心跳（{trigger_label}） ==="
                        )
                elif action == "restart_window":
                    later_same_window = self._find_later_terminal_machine_action(
                        db,
                        machine_id,
                        task_id,
                        action="restart_window",
                        window_token=window_token,
                    )
                    if later_same_window:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"窗口 {window_token or '-'} 的后续任务#{later_same_window['id']} 已结束（{trigger_label}） ==="
                        )
                    elif not agent_online:
                        stale_error = "stale_running_cleanup"
                        cleanup_message = (
                            f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                            f"持续 {age_seconds:.0f} 秒无状态更新且执行端无心跳（{trigger_label}） ==="
                        )
                elif not agent_online:
                    stale_error = "stale_running_cleanup"
                    cleanup_message = (
                        f"=== 检测到陈旧运行任务#{task_id}，已自动清理：执行端 {machine_id} "
                        f"持续 {age_seconds:.0f} 秒无状态更新且执行端无心跳（{trigger_label}） ==="
                    )
            if not stale_error:
                continue
            db.execute_statement(
                """
                UPDATE agent_tasks
                SET status = 'failed', error = ?, updated_at = ?
                WHERE id = ? AND status IN ('pending', 'running')
                """,
                (stale_error, now, task_id),
            )
            stale_rows.append(
                {
                    "id": task_id,
                    "action": action,
                    "status": status,
                    "error": stale_error,
                    "message": cleanup_message,
                }
            )
            if stale_rows:
                db.commit()
                for row in stale_rows:
                    self._append_control_log(
                        row["message"],
                        owner=owner_name,
                        machine_id=machine_id,
                        already_locked=already_locked,
                    )
                active_row = db.fetch_one(
                """
                SELECT COUNT(1) AS total
                FROM agent_tasks
                WHERE assigned_machine_id = ?
                  AND status IN ('pending', 'running')
                """,
                (machine_id,),
                )
                if int(active_row["total"] or 0) == 0:
                    self._append_control_log(
                        f"=== 当前状态已恢复，可重新启动：执行端 {machine_id}（{trigger_label}） ===",
                        owner=owner_name,
                        machine_id=machine_id,
                        already_locked=already_locked,
                    )
        return stale_rows

    def _build_machine_action_payload(
        self,
        db,
        owner_name,
        cfg,
        action="",
        window_token=None,
    ):
        owner_name = str(owner_name or "").strip()
        action = str(action or "").strip()
        machine_id = str((cfg or {}).get("machine_id") or "").strip()
        bit_api = str((cfg or {}).get("bit_api") or "").strip()
        api_token = str((cfg or {}).get("api_token") or "").strip()
        if not bit_api or not api_token:
            return None, "设备配置缺少接口参数"
        payload = {
            "owner": owner_name,
            "bit_api": bit_api,
            "api_token": api_token,
            "device_config_id": (cfg or {}).get("id"),
        }
        if action in {"start", "restart_window", "collect_groups"} and machine_id:
            explicit_window_token = str(window_token or "").strip()
            if explicit_window_token:
                effective_window_tokens = [explicit_window_token]
            elif action in {"start", "collect_groups"}:
                effective_window_tokens = _parse_window_token_list(
                    (cfg or {}).get("preferred_window_token")
                )
            else:
                effective_window_tokens = []
            if len(effective_window_tokens) == 1:
                payload["window_token"] = effective_window_tokens[0]
            elif len(effective_window_tokens) > 1:
                payload["window_tokens"] = effective_window_tokens
            if action in FORMAL_RUNTIME_ACTIONS:
                selected_window_groups = db.list_selected_window_groups(owner_name, machine_id)
                if effective_window_tokens:
                    projected_window_groups = {}
                    for token in effective_window_tokens:
                        projected_window_groups[token] = list(
                            selected_window_groups.get(token, [])
                        )
                    payload["selected_window_groups"] = projected_window_groups
                elif action == "restart_window":
                    selected_window_groups = {}
                    payload["selected_window_groups"] = selected_window_groups
                else:
                    payload["selected_window_groups"] = selected_window_groups
        return payload, None

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
                if not enabled_configs:
                    return False, "未配置设备"
                bound_configs = [
                    cfg
                    for cfg in enabled_configs
                    if str(cfg.get("machine_id") or "").strip()
                ]
                if not bound_configs:
                    return False, "未绑定设备"
                task_ids = []
                has_online = False
                for cfg in bound_configs:
                    machine_id = str(cfg.get("machine_id") or "").strip()
                    agent, agent_error = self._resolve_machine_agent(
                        db,
                        owner_name,
                        machine_id,
                        heartbeat_ttl,
                        allow_disabled,
                    )
                    if not agent:
                        self._append_control_log(
                            f"=== 跳过设备 {machine_id}：{agent_error} ===",
                            owner=owner_name,
                            machine_id=machine_id,
                            already_locked=True,
                        )
                        continue
                    has_online = True
                    self._cleanup_stale_machine_action_tasks(
                        db,
                        owner_name,
                        machine_id,
                        heartbeat_ttl,
                        trigger="enqueue_owner_action",
                        already_locked=True,
                    )
                    existing_task = self._find_existing_machine_action_task(
                        db,
                        owner_name,
                        machine_id,
                        action,
                        window_token=window_token,
                    )
                    if existing_task:
                        self._append_control_log(
                            f"=== 跳过重复 {action} 指令：执行端 {machine_id} 已存在任务#{existing_task['id']} ({existing_task['status']}) ===",
                            owner=owner_name,
                            machine_id=machine_id,
                            already_locked=True,
                        )
                        task_ids.append((machine_id, existing_task["id"]))
                        continue
                    payload, payload_error = self._build_machine_action_payload(
                        db,
                        owner_name,
                        cfg,
                        action=action,
                        window_token=window_token,
                    )
                    if payload_error:
                        self._append_control_log(
                            f"=== 跳过设备 {machine_id}：{payload_error} ===",
                            owner=owner_name,
                            machine_id=machine_id,
                            already_locked=True,
                        )
                        continue
                    task_id = db.enqueue_agent_task(
                        owner_name,
                        action,
                        window_token=(
                            str((payload or {}).get("window_token") or "").strip()
                            or str(window_token or "").strip()
                            or None
                        ),
                        machine_id=machine_id,
                        payload=payload,
                    )
                    task_ids.append((machine_id, task_id))
                if task_ids:
                    for machine_id, task_id in task_ids:
                        self._append_control_log(
                            f"=== 已下发 {action} 指令到执行端 {machine_id} (任务#{task_id}) ===",
                            owner=owner_name,
                            machine_id=machine_id,
                            already_locked=True,
                        )
                    return True, None
                if time.time() >= deadline:
                    if not has_online:
                        return False, "暂无在线设备"
                    return False, "执行端离线"
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
            self._cleanup_stale_machine_action_tasks(
                db,
                owner_name,
                machine_id,
                heartbeat_ttl,
                trigger="enqueue_owner_action",
            )
            existing_task = self._find_existing_machine_action_task(
                db,
                owner_name,
                machine_id,
                action,
            )
            if existing_task:
                self._append_control_log(
                    f"=== 跳过重复 {action} 指令：执行端 {machine_id} 已存在任务#{existing_task['id']} ({existing_task['status']}) ===",
                    owner=owner_name,
                    machine_id=machine_id,
                )
                return True, None
            configs = db.list_device_configs(owner_name)
            target_cfg = _select_device_config(configs, machine_id=machine_id)
            if not target_cfg:
                return False, "未配置设备"
            payload, payload_error = self._build_machine_action_payload(
                db,
                owner_name,
                target_cfg,
                action=action,
                window_token=window_token,
            )
            if payload_error:
                return False, payload_error
            task_id = db.enqueue_agent_task(
                owner_name,
                action,
                window_token=(
                    str((payload or {}).get("window_token") or "").strip()
                    or str(window_token or "").strip()
                    or None
                ),
                machine_id=machine_id,
                payload=payload,
            )
        finally:
            db.close()
        with self._lock:
            self._append_control_log(
                f"=== 已下发 {action} 指令到执行端 {machine_id} (任务#{task_id}) ===",
                owner=owner_name,
                machine_id=machine_id,
                already_locked=True,
            )
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
            return False, "账号不存在", None
        if not machine_id:
            return False, "未绑定设备", None
        owner_account = self._load_owner_account(owner_name)
        if not owner_account:
            return False, "账号不存在", None
        if not allow_disabled and int(owner_account.get("status", 0) or 0) == 0:
            return False, "账号已禁用", None

        settings = _load_agent_settings()
        heartbeat_ttl = settings.get("heartbeat_ttl", 180)
        deadline = time.time() + max(float(wait_online_seconds or 0.0), 0.0)
        db = Database()
        try:
            while True:
                configs = db.list_device_configs(owner_name)
                target_cfg = _select_device_config(
                    configs,
                    machine_id=machine_id,
                    api_token=api_token,
                )
                if not target_cfg:
                    if time.time() >= deadline:
                        return False, "未配置设备", None
                    time.sleep(0.5)
                    continue

                agent, agent_error = self._resolve_machine_agent(
                    db,
                    owner_name,
                    machine_id,
                    heartbeat_ttl,
                    allow_disabled,
                )
                if not agent:
                    if time.time() >= deadline:
                        return False, agent_error or "执行端离线", None
                    time.sleep(0.5)
                    continue

                payload, payload_error = self._build_machine_action_payload(
                    db,
                    owner_name,
                    target_cfg,
                    action=action,
                    window_token=window_token,
                )
                if payload_error:
                    return False, payload_error, None
                # Keep duplicate detection and enqueue in one critical section so
                # concurrent requests cannot observe different queue states.
                with self._lock:
                    self._cleanup_stale_machine_action_tasks(
                        db,
                        owner_name,
                        machine_id,
                        heartbeat_ttl,
                        trigger="enqueue_machine_action",
                        already_locked=True,
                    )
                    existing_task = self._find_existing_machine_action_task(
                        db,
                        owner_name,
                        machine_id,
                        action,
                        window_token=window_token,
                    )
                    if existing_task:
                        self._append_control_log(
                            f"=== 跳过重复 {action} 指令：执行端 {machine_id} 已存在任务#{existing_task['id']} ({existing_task['status']}) ===",
                            owner=owner_name,
                            machine_id=machine_id,
                            already_locked=True,
                        )
                        return True, None, {
                            "result": "duplicate",
                            "task_id": existing_task["id"],
                            "existing_status": existing_task["status"],
                        }
                    task_id = db.enqueue_agent_task(
                        owner_name,
                        action,
                        window_token=(
                            str((payload or {}).get("window_token") or "").strip()
                            or str(window_token or "").strip()
                            or None
                        ),
                        machine_id=machine_id,
                        payload=payload,
                    )
                    self._append_control_log(
                        f"=== 已下发 {action} 指令到执行端 {machine_id} (任务#{task_id}) ===",
                        owner=owner_name,
                        machine_id=machine_id,
                        already_locked=True,
                    )
                    return True, None, {
                        "result": "accepted",
                        "task_id": task_id,
                    }
        finally:
            db.close()

    def start(self, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            state = self._get_owner_runtime_state(owner_name)
            if state["collect_running"] or state["collect_pending"]:
                return False, "当前正在采集群组，请等待采集完成后再启动"
            if state["task_paused"]:
                return False, "当前任务处于暂停中，请先点击继续或停止"
            if state["task_running"]:
                return False, "当前任务已在运行，无需重复启动"
            if state["task_pending"]:
                return False, "启动任务已在排队，请勿重复启动"
            self._clear_scoped_logs_locked("runtime", owner=owner_name)
            self._clear_scoped_logs_locked("control", owner=owner_name)
            self._append_control_log("=== 任务已下发 ===", owner=owner_name, already_locked=True)
            self._append_control_log("=== 正在确认执行端在线状态 ===", owner=owner_name, already_locked=True)
            return self._enqueue_owner_device_actions(
                "start",
                owner_name,
                wait_online_seconds=4.0,
            )

    def stop(self, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            state = self._get_owner_runtime_state(owner_name)
            if not (
                state["task_running"]
                or state["task_pending"]
                or state["task_paused"]
                or state["collect_running"]
                or state["collect_pending"]
            ):
                return False, "当前没有可停止的运行/采集任务"
            db = Database()
            try:
                cancelled_start_rows = self._cancel_pending_start_tasks_locked(db, owner_name)
                cancelled_collect_rows = self._cancel_pending_collect_tasks_locked(db, owner_name)
            finally:
                db.close()
            for row in cancelled_start_rows:
                machine_id = str(row.get("assigned_machine_id") or "").strip()
                task_id = row.get("id")
                window_token = str(row.get("window_token") or "").strip()
                action = str(row.get("action") or "").strip()
                window_label = f" 窗口{window_token}" if window_token else ""
                action_label = "单窗口启动任务" if action == "restart_window" else "启动任务"
                self._append_control_log(
                    f"=== 已取消排队中的{action_label}#{task_id}：执行端 {machine_id or '-'}{window_label} ===",
                    owner=owner_name,
                    machine_id=machine_id or None,
                    already_locked=True,
                )
            for row in cancelled_collect_rows:
                machine_id = str(row.get("assigned_machine_id") or "").strip()
                task_id = row.get("id")
                self._append_control_log(
                    f"=== 已取消排队中的采集任务#{task_id}：执行端 {machine_id or '-'} ===",
                    owner=owner_name,
                    machine_id=machine_id or None,
                    already_locked=True,
                )
            if state["task_running"] or state["task_paused"] or state["collect_running"]:
                return self._enqueue_owner_device_actions(
                    "stop", owner_name, allow_disabled=True
                )
            return True, None

    def pause(self, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            state = self._get_owner_runtime_state(owner_name)
            if state["task_paused"]:
                return False, "当前任务已处于暂停状态"
            if not state["task_running"]:
                return False, "当前没有可暂停的运行任务"
            return self._enqueue_owner_device_actions("pause", owner_name)

    def resume(self, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            state = self._get_owner_runtime_state(owner_name)
            if not state["task_paused"]:
                return False, "当前没有可继续的暂停任务"
            return self._enqueue_owner_device_actions("resume", owner_name)

    def restart_window(self, window_token, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            state = self._get_owner_runtime_state(owner_name)
            if state["collect_running"] or state["collect_pending"]:
                return False, "当前正在采集群组，请等待采集完成后再单独启动"
            return self._enqueue_owner_device_actions(
                "restart_window",
                owner_name,
                window_token=str(window_token or "").strip(),
            )

    def collect(self, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            state = self._get_owner_runtime_state(owner_name)
            if state["task_running"] or state["task_pending"] or state["task_paused"]:
                return False, "当前存在正式运行任务，请先停止或等待任务结束后再采集"
            if state["collect_running"]:
                return False, "当前正在采集群组，请勿重复下发"
            if state["collect_pending"]:
                return False, "采集任务已在排队中，请稍后再试"
            self._clear_scoped_logs_locked("runtime", owner=owner_name)
            self._clear_scoped_logs_locked("control", owner=owner_name)
            self._append_control_log("=== 采集指令已下发 ===", owner=owner_name, already_locked=True)
            self._append_control_log("=== 正在确认执行端在线状态 ===", owner=owner_name, already_locked=True)
            return self._enqueue_owner_device_actions(
                "collect_groups",
                owner_name,
                wait_online_seconds=4.0,
            )

    def _cancel_pending_collect_tasks_locked(self, db, owner_name):
        owner_name = str(owner_name or "").strip()
        if not owner_name:
            return []
        rows = db.fetch_all(
            """
            SELECT id, assigned_machine_id
            FROM agent_tasks
            WHERE owner = ?
              AND action = 'collect_groups'
              AND status = 'pending'
            ORDER BY id DESC
            """,
            (owner_name,),
        )
        if not rows:
            return []
        now = time.time()
        task_ids = [int(row["id"]) for row in rows if row and row.get("id") is not None]
        if not task_ids:
            return []
        placeholders = ",".join("?" for _ in task_ids)
        db.execute_statement(
            f"""
            UPDATE agent_tasks
            SET status = 'failed', error = ?, updated_at = ?
            WHERE id IN ({placeholders})
              AND status = 'pending'
            """,
            ("collect_cancelled", now, *task_ids),
        )
        db.commit()
        return [dict(row) for row in rows]

    def _cancel_pending_start_tasks_locked(self, db, owner_name):
        owner_name = str(owner_name or "").strip()
        if not owner_name:
            return []
        rows = db.fetch_all(
            """
            SELECT id, assigned_machine_id, window_token, action
            FROM agent_tasks
            WHERE owner = ?
              AND action IN ('start', 'restart_window')
              AND status = 'pending'
            ORDER BY id DESC
            """,
            (owner_name,),
        )
        if not rows:
            return []
        now = time.time()
        task_ids = [int(row["id"]) for row in rows if row and row.get("id") is not None]
        if not task_ids:
            return []
        placeholders = ",".join("?" for _ in task_ids)
        db.execute_statement(
            f"""
            UPDATE agent_tasks
            SET status = 'failed', error = ?, updated_at = ?
            WHERE id IN ({placeholders})
              AND status = 'pending'
            """,
            ("task_cancelled", now, *task_ids),
        )
        db.commit()
        return [dict(row) for row in rows]

    def stop_collect(self, owner=None):
        owner_name = str(owner or "").strip()
        if not owner_name:
            return False, "总后台不参与任务操作，请在子后台执行"
        with self._lock:
            logged_cancelled_task_ids = set()
            deadline = time.time() + 1.6
            while True:
                db = Database()
                try:
                    cancelled_rows = self._cancel_pending_collect_tasks_locked(db, owner_name)
                    state = self._load_owner_runtime_state(db, owner=owner_name)
                finally:
                    db.close()
                for row in cancelled_rows:
                    task_id = row.get("id")
                    if task_id in logged_cancelled_task_ids:
                        continue
                    logged_cancelled_task_ids.add(task_id)
                    machine_id = str(row.get("assigned_machine_id") or "").strip()
                    self._append_control_log(
                        f"=== 已取消排队中的采集任务#{task_id}：执行端 {machine_id or '-'} ===",
                        owner=owner_name,
                        machine_id=machine_id or None,
                        already_locked=True,
                    )
                if state["collect_running"]:
                    self._append_control_log(
                        "=== 停止采集指令已下发 ===",
                        owner=owner_name,
                        already_locked=True,
                    )
                    return self._enqueue_owner_device_actions(
                        "stop_collect",
                        owner_name,
                        allow_disabled=True,
                    )
                if cancelled_rows:
                    return True, None
                if not state["collect_pending"]:
                    if time.time() >= deadline:
                        return False, "当前没有可停止的采集任务"
                    time.sleep(0.2)
                    continue
                if time.time() >= deadline:
                    return False, "当前没有可停止的采集任务"
                time.sleep(0.2)

    def status(self, owner=None):
        with self._lock:
            db = Database()
            try:
                state = self._load_owner_runtime_state(db, owner=owner)
            except Exception:
                state = {
                    "running_tasks": 0,
                    "pending_tasks": 0,
                    "collect_running_tasks": 0,
                    "collect_pending_tasks": 0,
                    "running_accounts": 0,
                    "task_running": False,
                    "task_pending": False,
                    "task_paused": False,
                    "collect_running": False,
                    "collect_pending": False,
                }
            finally:
                db.close()
            runtime_entries = self._filter_log_entries_locked(
                list(self._runtime_logs),
                owner=owner,
            )
            control_entries = self._filter_log_entries_locked(
                list(self._control_logs),
                owner=owner,
            )
            runtime_logs = self._render_log_messages_locked(runtime_entries)
            logs = self._render_log_messages_locked(list(runtime_entries) + list(control_entries))
            stats = build_runtime_stats(
                runtime_logs,
                account_id=owner,
                running_tasks=state["running_tasks"],
                running_accounts=state["running_accounts"],
            )
            return {
                "task_running": state["task_running"],
                "task_pending": state["task_pending"],
                "task_paused": state["task_paused"],
                "collect_running": state["collect_running"],
                "collect_pending": state["collect_pending"],
                "logs": logs,
                "control_logs": [],
                "stats": stats,
            }

    def _load_owner_runtime_state(self, db, owner=None):
        task_counts = _load_runtime_task_counts(owner=owner, db=db)
        running_tasks = int(task_counts.get("running_tasks") or 0)
        pending_tasks = int(task_counts.get("pending_tasks") or 0)
        collect_running_tasks = int(task_counts.get("collect_running_tasks") or 0)
        collect_pending_tasks = int(task_counts.get("collect_pending_tasks") or 0)
        running_accounts = int(task_counts.get("running_accounts") or 0)
        task_paused = bool(
            running_tasks > 0
            and self._is_task_paused_from_actions(db, owner=owner)
        )
        return {
            "running_tasks": running_tasks,
            "pending_tasks": pending_tasks,
            "collect_running_tasks": collect_running_tasks,
            "collect_pending_tasks": collect_pending_tasks,
            "running_accounts": running_accounts,
            "task_running": running_tasks > 0,
            "task_pending": pending_tasks > 0,
            "task_paused": task_paused,
            "collect_running": collect_running_tasks > 0,
            "collect_pending": collect_pending_tasks > 0,
        }

    def _get_owner_runtime_state(self, owner=None):
        db = Database()
        try:
            return self._load_owner_runtime_state(db, owner=owner)
        finally:
            db.close()

    def _is_task_paused_from_actions(self, db, owner=None, machine_id=None):
        row = _load_latest_agent_task_by_scope(
            db,
            owner=owner,
            machine_id=machine_id,
            statuses=("done",),
            actions=FORMAL_RUNTIME_ACTIONS + ("pause", "resume", "stop"),
        )
        return bool(row and str(row.get("action") or "").strip() == "pause")

    def stop_for_owner(self, owner):
        if not owner:
            return
        with self._lock:
            ok, error = self._enqueue_owner_device_actions(
                "stop", owner, allow_disabled=True
            )
            if ok:
                self._append_control_log(
                    f"=== 已禁用账号 {owner}，已下发停止指令 ===",
                    owner=owner,
                    already_locked=True,
                )
            else:
                self._append_control_log(
                    f"=== 已禁用账号 {owner}，停止指令下发失败: {error} ===",
                    owner=owner,
                    already_locked=True,
                )

    def _load_owner_account(self, owner):
        if not owner:
            return None
        db = Database()
        try:
            return db.get_user_account(owner)
        finally:
            db.close()

TASK_MANAGER = TaskManager()


def read_yaml(path):
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


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


def _decode_message_json_list(raw_value):
    if raw_value is None:
        return []
    try:
        payload = json.loads(raw_value) if raw_value else []
    except Exception:
        payload = []
    return _normalize_message_lines(payload)


def _default_messages_config():
    return {
        "templates": [],
        "popup_stop_texts": [],
        "permanent_skip_texts": [],
    }


def _normalize_messages_config(data):
    payload = data if isinstance(data, dict) else {}
    normalized = _default_messages_config()
    normalized["templates"] = _normalize_message_lines(payload.get("templates") or [])
    normalized["popup_stop_texts"] = _normalize_message_lines(
        payload.get("popup_stop_texts") or []
    )
    restriction_payload = payload.get("restriction_detection_texts")
    if not normalized["popup_stop_texts"] and isinstance(restriction_payload, dict):
        normalized["popup_stop_texts"] = _merge_message_lines(
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
    normalized["permanent_skip_texts"] = _merge_message_lines(
        payload.get("permanent_skip_texts") or [],
        legacy_permanent_skip_texts,
    )
    return normalized


def _clone_messages_config(data):
    normalized = _normalize_messages_config(data)
    return {
        "templates": list(normalized["templates"]),
        "popup_stop_texts": list(normalized["popup_stop_texts"]),
        "permanent_skip_texts": list(normalized["permanent_skip_texts"]),
    }


def _merge_messages_config(current, incoming):
    merged = _clone_messages_config(current)
    payload = incoming if isinstance(incoming, dict) else {}

    if "templates" in payload:
        merged["templates"] = _normalize_message_lines(payload.get("templates") or [])

    if "popup_stop_texts" in payload:
        merged["popup_stop_texts"] = _normalize_message_lines(
            payload.get("popup_stop_texts") or []
        )

    if "permanent_skip_texts" in payload:
        merged["permanent_skip_texts"] = _normalize_message_lines(
            payload.get("permanent_skip_texts") or []
        )

    return merged


def _apply_owner_messages_config(base_messages, owner_messages):
    merged = _clone_messages_config(base_messages)
    payload = owner_messages if isinstance(owner_messages, dict) else {}
    templates = payload.get("templates")
    if templates is not None:
        merged["templates"] = _normalize_message_lines(templates or [])
    popup_stop_texts = payload.get("popup_stop_texts")
    if popup_stop_texts is not None:
        merged["popup_stop_texts"] = _normalize_message_lines(popup_stop_texts or [])
    permanent_skip_texts = payload.get("permanent_skip_texts")
    if permanent_skip_texts is not None:
        merged["permanent_skip_texts"] = _normalize_message_lines(
            permanent_skip_texts or []
        )
    return merged


def _build_admin_restriction_summary(base_messages, rows):
    popup_stop_groups = [base_messages.get("popup_stop_texts") or []]
    permanent_skip_groups = [base_messages.get("permanent_skip_texts") or []]
    for row in rows or []:
        popup_stop_groups.append(
            _decode_message_json_list(row.get("popup_stop_texts"))
        )
        permanent_skip_groups.append(
            _decode_message_json_list(row.get("permanent_skip_texts"))
        )
    return {
        "popup_stop_texts": _merge_message_lines(*popup_stop_groups),
        "permanent_skip_texts": _merge_message_lines(*permanent_skip_groups),
    }


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
    max_devices = int(security_cfg.get("max_devices_per_account", 1) or 1)
    allow_rebind = bool(security_cfg.get("allow_rebind", False))
    return {
        "max_devices_per_account": max_devices,
        "allow_rebind": allow_rebind,
    }

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
    if not stored_hash.startswith("pbkdf2$"):
        return False
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
        if path in ("/", "/index.html", "/admin", "/admin/", "/admin/index.html"):
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
            if path == "/me":
                self._send_json({"account": self._current_account_info(prefer_sub=False)})
                return
            if path == "/device-detail":
                owner = ""
                machine_id = ""
                if isinstance(getattr(self, "_query", None), dict):
                    owner = (self._query.get("owner") or [""])[0].strip()
                    machine_id = (self._query.get("machine_id") or [""])[0].strip()
                result = handle_device_detail(owner=owner, machine_id=machine_id)
                status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
                self._send_json(result, status=status)
                return
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
            if path == "/me":
                self._send_json({"account": self._current_account_info(prefer_sub=True)})
                return
            if path == "/device-detail":
                machine_id = ""
                if isinstance(getattr(self, "_query", None), dict):
                    machine_id = (self._query.get("machine_id") or [""])[0].strip()
                result = handle_device_detail(
                    owner=self._current_username(prefer_sub=True),
                    machine_id=machine_id,
                )
                status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
                self._send_json(result, status=status)
                return
            if path == "/config":
                username = self._current_username(prefer_sub=True)
                self._send_json(load_full_config(username))
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
            if path == "/logout":
                token = self._get_session_token(ADMIN_SESSION_COOKIE_NAME)
                if token:
                    _drop_session(token)
                self._send_json(
                    {"ok": True},
                    cookies=[self._clear_session_cookie(ADMIN_SESSION_COOKIE_NAME, path="/")],
                )
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
            if path == "/device-groups/save":
                payload = self._read_json()
                result = handle_device_group_selection_save(payload)
                if not result.get("ok"):
                    self._send_json({"error": result.get("error")}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(result)
                return
            if path == "/agent/window-token":
                payload = self._read_json()
                result = handle_device_preferred_window_token_save(payload)
                if not result.get("ok"):
                    self._send_json({"error": result.get("error")}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(result)
                return
            if path in {"/restart-window", "/start", "/stop", "/pause", "/resume", "/stop-collect"}:
                self._send_json(
                    {"error": "总后台不参与任务操作，请在子后台执行"},
                    status=HTTPStatus.FORBIDDEN,
                )
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
            if path == "/logout":
                token = self._get_session_token(SUB_SESSION_COOKIE_NAME)
                if token:
                    _drop_session(token)
                self._send_json(
                    {"ok": True},
                    cookies=[self._clear_session_cookie(SUB_SESSION_COOKIE_NAME, path="/")],
                )
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
            if path == "/device-groups/save":
                payload = self._read_json()
                payload["owner"] = self._current_username(prefer_sub=True)
                result = handle_device_group_selection_save(payload)
                if not result.get("ok"):
                    self._send_json({"error": result.get("error")}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(result)
                return
            if path == "/agent/window-token":
                payload = self._read_json()
                payload["owner"] = self._current_username(prefer_sub=True)
                result = handle_device_preferred_window_token_save(payload)
                if not result.get("ok"):
                    self._send_json({"error": result.get("error")}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(result)
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
            if path == "/collect-groups":
                ok, error = TASK_MANAGER.collect(owner=self._current_username(prefer_sub=True))
                if not ok:
                    self._send_json({"error": error}, status=HTTPStatus.CONFLICT)
                    return
                self._send_json({"ok": True})
                return
            if path == "/stop-collect":
                ok, error = TASK_MANAGER.stop_collect(owner=self._current_username(prefer_sub=True))
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
        if path == "/api/agent/collect-report":
            payload = self._read_json()
            result = handle_agent_collect_report(payload)
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
        if path == "/api/client/restart-window":
            payload = self._read_json()
            result = handle_client_action(payload, "restart_window")
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

    def _get_session_token(self, cookie_name):
        cookies = self._parse_cookies()
        token = cookies.get(cookie_name)
        return token if token else None

    def _current_session(self, prefer_sub=False):
        if prefer_sub:
            token = self._get_session_token(SUB_SESSION_COOKIE_NAME)
        else:
            token = self._get_session_token(ADMIN_SESSION_COOKIE_NAME)
        return get_session_from_token(token)

    def _current_username(self, prefer_sub=False):
        session = self._current_session(prefer_sub=prefer_sub)
        return session.get("username") if session else None

    def _current_account_info(self, prefer_sub=False):
        session = self._current_session(prefer_sub=prefer_sub) or {}
        role = str(session.get("role") or "").strip() or ("user" if prefer_sub else "admin")
        username = str(session.get("username") or "").strip() or ("admin" if role == "admin" else "")
        role_label = "管理员" if role == "admin" else "子账户"
        return {
            "username": username,
            "role_label": role_label,
        }

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

    def _clear_session_cookie(self, cookie_name, path="/"):
        return (
            f"{cookie_name}=; Path={path}; HttpOnly; SameSite=Lax; "
            "Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0"
        )

def load_full_config(username=None):
    messages = _normalize_messages_config(read_yaml(MESSAGES_PATH))
    result = {
        "messages": messages,
    }

    db = Database()
    try:
        if username:
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
                result["account"] = {
                    "expire_at": expire_at_val,
                    "days_left": days_left,
                }
            owner_messages = db.get_message_templates(username)
            if owner_messages is not None:
                messages = _apply_owner_messages_config(messages, owner_messages)
                result["messages"] = messages
        else:
            rows = db.fetch_all(
                """
                SELECT username, popup_stop_texts, permanent_skip_texts
                FROM message_templates
                ORDER BY updated_at DESC, username ASC
                """
            )
            result["restriction_summary"] = _build_admin_restriction_summary(
                messages,
                rows,
            )
    finally:
        db.close()

    return result


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


def start_server(host, port):
    try:
        server = ThreadingHTTPServer((host, port), WebUIHandler)
        return server, None
    except OSError as exc:
        return None, exc


def _filter_runtime_logs(logs, account_id=None, machine_id=None):
    normalized_logs = [
        str(line or "").strip()
        for line in (logs or [])
        if str(line or "").strip()
    ]
    account_filter = str(account_id or "").strip()
    machine_filter = str(machine_id or "").strip()
    if not account_filter and not machine_filter:
        return normalized_logs
    filtered = []
    for line in normalized_logs:
        account_hit = True
        machine_hit = True
        if account_filter:
            account_hit = line.startswith((f"[{account_filter}] ", f"[{account_filter}@")) or (
                f"owner={account_filter}" in line
            )
        if machine_filter:
            machine_hit = (
                f"@{machine_filter}]" in line
                or line.startswith(f"[{machine_filter}] ")
                or f"machine_id={machine_filter}" in line
                or f"执行端 {machine_filter}" in line
            )
        if account_hit and machine_hit:
            filtered.append(line)
    return filtered


def build_runtime_stats(logs, account_id=None, machine_id=None, running_tasks=0, running_accounts=None):
    db_stats = _load_db_stats(account_id=account_id, machine_id=machine_id)
    logs = _filter_runtime_logs(logs, account_id=account_id, machine_id=machine_id)
    sent_patterns = ("✅ 成员页消息已发送",)
    failed_patterns = (
        "处理成员失败",
        "发送流程中断",
        "消息被拒绝",
        "聊天窗口限制提示命中",
        "输入框未准备就绪",
        "连接浏览器失败",
        "未能收集到任何已加入小组",
        "正式发送文案为空",
        "当前窗口已被系统限制继续发送消息",
        "当前账号已被限制继续向该类用户发起消息",
    )
    runtime_sent = sum(1 for line in logs if any(token in line for token in sent_patterns))
    runtime_failed = sum(1 for line in logs if any(token in line for token in failed_patterns))
    runtime_running_windows = max(0, int(running_tasks or 0))
    if running_accounts is None:
        runtime_running_accounts = (
            1 if account_id and runtime_running_windows > 0 else runtime_running_windows
        )
    else:
        runtime_running_accounts = max(0, int(running_accounts or 0))
    return {
        "sent": runtime_sent,
        "failed": runtime_failed,
        "runtime_sent": runtime_sent,
        "runtime_failed": runtime_failed,
        "runtime": {
            "sent": runtime_sent,
            "failed": runtime_failed,
            "running_windows": runtime_running_windows,
            "running_accounts": runtime_running_accounts,
        },
        "db": db_stats,
    }


def _translate_client_start_block_reason(reason):
    mapping = {
        "config_not_synced": "设备配置尚未同步完成",
        "device_not_bound": "设备绑定尚未完成",
        "missing_connection_fields": "设备配置缺少接口参数",
        "agent_offline": "执行端尚未在线",
    }
    return mapping.get(str(reason or "").strip(), "执行端尚未就绪")


def _translate_control_action_state(action):
    mapping = {
        "stop": "正在处理停止指令",
        "pause": "正在处理暂停指令",
        "resume": "正在处理继续指令",
        "restart_window": "正在处理单独启动指令",
    }
    return mapping.get(str(action or "").strip(), "正在处理控制指令")


def _client_action_message(action, result, existing_status=""):
    action = str(action or "").strip()
    result = str(result or "").strip()
    existing_status = str(existing_status or "").strip()
    if result == "duplicate":
        if action == "start":
            return "当前任务已在运行，无需重复启动" if existing_status == "running" else "启动任务已在排队，请勿重复启动"
        if action == "stop":
            return "停止指令已在处理中，请勿重复点击"
        if action == "pause":
            return "暂停指令已在处理中，请勿重复点击"
        if action == "resume":
            return "继续指令已在处理中，请勿重复点击"
        if action == "restart_window":
            return "单独启动任务已在处理中，请勿重复下发"
    accepted = {
        "start": "启动指令已下发，正在等待执行端响应",
        "stop": "停止指令已下发，正在等待执行端响应",
        "pause": "暂停指令已下发",
        "resume": "继续指令已下发",
        "restart_window": "单独启动指令已下发，正在等待执行端响应",
    }
    return accepted.get(action, "操作已完成")

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
        settings = _load_agent_settings()
        heartbeat_ttl = max(float(settings.get("heartbeat_ttl", 180) or 180), 0.0)
        cutoff = time.time() - heartbeat_ttl
        row = db.fetch_one(
            """
            SELECT COUNT(1) AS total
            FROM agent_registry
            WHERE owner = ?
              AND status = 1
              AND COALESCE(last_seen, 0) >= ?
            """,
            (owner, cutoff),
        )
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


def _count_agent_tasks_by_scope(db, owner=None, machine_id=None, statuses=None, actions=None):
    clauses = []
    params = []
    owner_name = str(owner or "").strip()
    machine_name = str(machine_id or "").strip()
    status_values = [str(item or "").strip() for item in (statuses or []) if str(item or "").strip()]
    action_values = [str(item or "").strip() for item in (actions or []) if str(item or "").strip()]
    if owner_name:
        clauses.append("owner = ?")
        params.append(owner_name)
    if machine_name:
        clauses.append("assigned_machine_id = ?")
        params.append(machine_name)
    if status_values:
        placeholders = ",".join("?" for _ in status_values)
        clauses.append(f"status IN ({placeholders})")
        params.extend(status_values)
    if action_values:
        placeholders = ",".join("?" for _ in action_values)
        clauses.append(f"action IN ({placeholders})")
        params.extend(action_values)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    row = db.fetch_one(
        f"SELECT COUNT(1) AS total FROM agent_tasks {where}",
        tuple(params),
    )
    return int(row["total"] or 0) if row else 0


def _load_latest_agent_task_by_scope(db, owner=None, machine_id=None, statuses=None, actions=None):
    clauses = []
    params = []
    owner_name = str(owner or "").strip()
    machine_name = str(machine_id or "").strip()
    status_values = [str(item or "").strip() for item in (statuses or []) if str(item or "").strip()]
    action_values = [str(item or "").strip() for item in (actions or []) if str(item or "").strip()]
    if owner_name:
        clauses.append("owner = ?")
        params.append(owner_name)
    if machine_name:
        clauses.append("assigned_machine_id = ?")
        params.append(machine_name)
    if status_values:
        placeholders = ",".join("?" for _ in status_values)
        clauses.append(f"status IN ({placeholders})")
        params.extend(status_values)
    if action_values:
        placeholders = ",".join("?" for _ in action_values)
        clauses.append(f"action IN ({placeholders})")
        params.extend(action_values)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    row = db.fetch_one(
        f"""
        SELECT *
        FROM agent_tasks
        {where}
        ORDER BY id DESC
        LIMIT 1
        """,
        tuple(params),
    )
    return row if row else None


def _count_distinct_running_start_accounts(db, owner=None):
    owner_name = str(owner or "").strip()
    if owner_name:
        return 1 if _count_agent_tasks_by_scope(
            db,
            owner=owner_name,
            statuses=("running",),
            actions=FORMAL_RUNTIME_ACTIONS,
        ) > 0 else 0
    row = db.fetch_one(
        """
        SELECT COUNT(DISTINCT owner) AS total
        FROM agent_tasks
        WHERE status = 'running'
          AND action IN (?, ?)
          AND COALESCE(owner, '') != ''
        """,
        FORMAL_RUNTIME_ACTIONS,
    )
    return int(row["total"] or 0) if row else 0


def _load_runtime_task_counts(owner=None, db=None):
    owns_db = db is None
    db = db or Database()
    try:
        running_tasks = _count_agent_tasks_by_scope(
            db,
            owner=owner,
            statuses=("running",),
            actions=FORMAL_RUNTIME_ACTIONS,
        )
        pending_tasks = _count_agent_tasks_by_scope(
            db,
            owner=owner,
            statuses=("pending",),
            actions=FORMAL_RUNTIME_ACTIONS,
        )
        collect_running_tasks = _count_agent_tasks_by_scope(
            db,
            owner=owner,
            statuses=("running",),
            actions=COLLECT_ACTIONS,
        )
        collect_pending_tasks = _count_agent_tasks_by_scope(
            db,
            owner=owner,
            statuses=("pending",),
            actions=COLLECT_ACTIONS,
        )
        running_accounts = _count_distinct_running_start_accounts(db, owner=owner)
        return {
            "running_tasks": int(running_tasks or 0),
            "pending_tasks": int(pending_tasks or 0),
            "collect_running_tasks": int(collect_running_tasks or 0),
            "collect_pending_tasks": int(collect_pending_tasks or 0),
            "running_accounts": int(running_accounts or 0),
        }
    finally:
        if owns_db:
            db.close()


def _load_db_stats(account_id=None, machine_id=None):
    account_filter = str(account_id or "").strip()
    machine_filter = str(machine_id or "").strip()
    cache_key = (account_filter, machine_filter)
    cached = _get_runtime_read_cache(DB_STATS_CACHE, cache_key)
    if cached is not None:
        return cached

    db = Database()
    try:
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        today_text = today_start.strftime("%Y-%m-%d %H:%M:%S")
        yesterday_text = yesterday_start.strftime("%Y-%m-%d %H:%M:%S")

        device_total = 0
        device_online = 0
        device_yesterday = 0
        max_devices = None
        if account_filter and not machine_filter:
            today_start_ts = today_start.timestamp()
            yesterday_start_ts = yesterday_start.timestamp()
            row = db.fetch_one(
                "SELECT COUNT(1) AS total FROM agent_registry WHERE owner = ?",
                (account_filter,),
            )
            device_total = int(row["total"] or 0) if row else 0
            device_summary = _load_owner_device_summary(account_filter, db=db)
            device_online = int(device_summary.get("online_devices") or 0)
            max_devices = device_summary.get("max_devices")
            row = db.fetch_one(
                """
                SELECT COUNT(1) AS total FROM agent_registry
                WHERE owner = ?
                  AND status = 1
                  AND COALESCE(last_seen, 0) >= ?
                  AND COALESCE(last_seen, 0) < ?
                """,
                (account_filter, yesterday_start_ts, today_start_ts),
            )
            device_yesterday = int(row["total"] or 0) if row else 0

        def count_status(status, start=None, end=None):
            clauses = ["status = ?"]
            params = [status]
            if account_filter:
                clauses.append("account_id = ?")
                params.append(account_filter)
            if machine_filter:
                clauses.append("machine_id = ?")
                params.append(machine_filter)
            if start:
                clauses.append("created_at >= ?")
                params.append(start)
            if end:
                clauses.append("created_at < ?")
                params.append(end)
            query = f"SELECT COUNT(1) AS total FROM sent_history WHERE {' AND '.join(clauses)}"
            row = db.fetch_one(query, tuple(params))
            return int(row["total"] or 0) if row else 0

        def count_distinct(field, start=None, end=None, status=None, require_status=False):
            clauses = [f"{field} IS NOT NULL", f"{field} != ''"]
            params = []
            if account_filter:
                clauses.append("account_id = ?")
                params.append(account_filter)
            if machine_filter:
                clauses.append("machine_id = ?")
                params.append(machine_filter)
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
            row = db.fetch_one(query, tuple(params))
            return int(row["total"] or 0) if row else 0

        def count_user_accounts(start=None, end=None):
            clauses = ["role = ?"]
            params = ["user"]
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
            row = db.fetch_one(query, tuple(params))
            return int(row["total"] or 0) if row else 0

        user_field = "profile_url" if (account_filter or machine_filter) else "account_id"
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

        result = {
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
        _set_runtime_read_cache(
            DB_STATS_CACHE,
            cache_key,
            result,
            DB_STATS_CACHE_TTL_SECONDS,
        )
        return result
    except Exception as exc:
        log.error(f"读取统计数据失败: {exc}")
        return {}
    finally:
        db.close()


def _load_user_list():
    cache_key = "all"
    cached = _get_runtime_read_cache(USER_LIST_CACHE, cache_key)
    if cached is not None:
        return cached

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
                    "max_devices": row.get("max_devices"),
                    "expire_at": expire_at_val,
                    "days_left": days_left,
                    "activation_code": activation_code,
                }
            )
        result = {
            "users": users,
        }
        _set_runtime_read_cache(
            USER_LIST_CACHE,
            cache_key,
            result,
            USER_LIST_CACHE_TTL_SECONDS,
        )
        return result
    except Exception as exc:
        log.error(f"读取用户列表失败: {exc}")
        return {
            "users": [],
        }
    finally:
        db.close()

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
        return True, None, agent
    finally:
        db.close()


def _select_device_config(configs, machine_id="", api_token=""):
    machine_id = str(machine_id or "").strip()
    api_token = str(api_token or "").strip()
    ordered = list(configs or [])
    if not ordered:
        return None

    if machine_id and api_token:
        for cfg in reversed(ordered):
            if (
                str(cfg.get("machine_id") or "").strip() == machine_id
                and str(cfg.get("api_token") or "").strip() == api_token
            ):
                return cfg

    if machine_id:
        for cfg in reversed(ordered):
            if str(cfg.get("machine_id") or "").strip() == machine_id:
                return cfg

    if api_token:
        for cfg in reversed(ordered):
            if str(cfg.get("api_token") or "").strip() == api_token:
                return cfg

    return None


def _cleanup_device_config_duplicates(db, owner, keep_id, machine_id="", api_token=""):
    owner = str(owner or "").strip()
    machine_id = str(machine_id or "").strip()
    api_token = str(api_token or "").strip()
    keep_id = int(keep_id or 0)
    if not owner or not keep_id:
        return []

    removed_ids = []
    for cfg in db.list_device_configs(owner):
        cfg_id = int(cfg.get("id") or 0)
        if not cfg_id or cfg_id == keep_id:
            continue
        same_machine = machine_id and str(cfg.get("machine_id") or "").strip() == machine_id
        same_token = api_token and str(cfg.get("api_token") or "").strip() == api_token
        if not (same_machine or same_token):
            continue
        if db.delete_device_config(cfg_id, owner):
            removed_ids.append(cfg_id)
    return removed_ids


def _build_client_activation_response(owner, expire_at, max_devices, agent_token, request_host=""):
    return {
        "ok": True,
        "data": {
            "owner": owner,
            "server_url": _build_server_base_url(request_host=request_host),
            "expire_at": expire_at,
            "max_devices": max_devices,
            "agent_token": agent_token,
        },
    }


def _issue_agent_token_for_machine(db, owner, machine_id, client_version=""):
    owner = str(owner or "").strip()
    machine_id = str(machine_id or "").strip()
    if not owner or not machine_id:
        return ""

    existing = db.get_agent(machine_id)
    existing_last_seen = None
    existing_label = ""
    existing_status = 1
    if existing:
        existing_last_seen = existing.get("last_seen")
        existing_label = str(existing.get("label") or "").strip()
        status_raw = existing.get("status", 1)
        existing_status = 1 if status_raw is None else int(status_raw)

    agent_token = secrets.token_urlsafe(32)
    seed_last_seen = existing_last_seen if existing_last_seen not in (None, "") else 0.1
    db.upsert_agent(
        machine_id,
        existing_label or machine_id,
        seed_last_seen,
        owner=owner,
        status=existing_status,
        token=agent_token,
        client_version=client_version or None,
    )
    _invalidate_runtime_read_caches(clear_db_stats=True)
    return agent_token


def _try_recover_client_activation(db, payload, request_host=""):
    owner = str(payload.get("owner") or "").strip()
    machine_id = str(payload.get("machine_id") or "").strip()
    client_version = str(payload.get("client_version") or "").strip()
    bit_api = str(payload.get("bit_api") or "").strip()
    api_token = str(payload.get("api_token") or "").strip()

    if not (owner and machine_id and api_token):
        return None

    account = db.get_user_account(owner)
    if not account:
        return {"ok": False, "error": "unauthorized"}
    if int(account.get("status") or 0) == 0:
        return {"ok": False, "error": "account_disabled"}
    expired, expire_at = _is_account_expired(account)
    if expired:
        return {"ok": False, "error": "account_expired"}

    configs = db.list_device_configs(owner)
    device_cfg = _select_device_config(configs, machine_id=machine_id, api_token=api_token)
    if not device_cfg:
        return {"ok": False, "error": "unauthorized"}

    status_raw = device_cfg.get("status", 1)
    if status_raw is None:
        status_raw = 1
    if int(status_raw) == 0:
        return {"ok": False, "error": "device_disabled"}

    bound_machine = str(device_cfg.get("machine_id") or "").strip()
    if bound_machine and bound_machine != machine_id:
        return {"ok": False, "error": "device_bound"}

    existing_agent = db.get_agent(machine_id)
    if existing_agent:
        existing_owner = str(existing_agent.get("owner") or "").strip()
        if existing_owner and existing_owner != owner:
            return {"ok": False, "error": "device_bound"}
        existing_agent_status = existing_agent.get("status", 1)
        if existing_agent_status is None:
            existing_agent_status = 1
        if int(existing_agent_status) == 0:
            return {"ok": False, "error": "device_disabled"}

    max_devices = int(_load_agent_security().get("max_devices_per_account", 1) or 1)
    account_max = account.get("max_devices")
    if account_max not in (None, ""):
        try:
            max_devices = int(account_max)
        except Exception:
            pass

    agent_token = _issue_agent_token_for_machine(
        db,
        owner=owner,
        machine_id=machine_id,
        client_version=client_version,
    )
    return _build_client_activation_response(
        owner=owner,
        expire_at=expire_at,
        max_devices=max_devices,
        agent_token=agent_token,
        request_host=request_host,
    )

def handle_client_activate(payload, request_host=""):
    activation_code = str(payload.get("activation_code") or "").strip()
    machine_id = str(payload.get("machine_id") or "").strip()
    client_version = str(payload.get("client_version") or "").strip()
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    security = _load_agent_security()
    max_devices_default = int(security.get("max_devices_per_account", 1) or 1)
    allow_rebind = bool(security.get("allow_rebind", False))

    db = Database()
    try:
        if not activation_code:
            recovered = _try_recover_client_activation(
                db,
                payload,
                request_host=request_host,
            )
            if recovered is not None:
                return recovered
            return {"ok": False, "error": "missing_activation_code"}

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

        agent_token = _issue_agent_token_for_machine(
            db,
            owner=owner,
            machine_id=machine_id,
            client_version=client_version,
        )
        return _build_client_activation_response(
            owner=owner,
            expire_at=expire_at,
            max_devices=max_devices,
            agent_token=agent_token,
            request_host=request_host,
        )
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
    _invalidate_runtime_read_caches(clear_db_stats=True)
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
        token_matches = [
            cfg for cfg in configs if str(cfg.get("api_token") or "").strip() == api_token
        ]
        if len(token_matches) > 1:
            return False, "api_token_conflict", None

        if token_matches:
            token_cfg = token_matches[0]
            existing_machine = str(token_cfg.get("machine_id") or "").strip()
            if existing_machine and existing_machine != machine_id:
                return False, "device_bound", None

        target_cfg = _select_device_config(configs, machine_id=machine_id, api_token=api_token)
        if target_cfg:
            existing_machine = str(target_cfg.get("machine_id") or "").strip()
            target_machine_id = machine_id or existing_machine
            updated = db.update_device_config(
                target_cfg.get("id"),
                owner,
                display_name or str(target_cfg.get("name") or "").strip() or machine_id,
                target_machine_id,
                bit_api,
                api_token,
                1,
            )
            if not updated:
                return False, "not_found", None
            config_id = int(target_cfg.get("id") or 0)
            _cleanup_device_config_duplicates(
                db,
                owner,
                config_id,
                machine_id=target_machine_id,
                api_token=api_token,
            )
            return True, None, config_id
        new_id = db.add_device_config(owner, display_name, machine_id, bit_api, api_token, 1)
        _cleanup_device_config_duplicates(
            db,
            owner,
            new_id,
            machine_id=machine_id,
            api_token=api_token,
        )
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


def _build_client_runtime_snapshot(verified, api_token=""):
    machine_id = verified.get("machine_id") or ""
    owner = verified.get("owner") or ""
    api_token = str(api_token or "").strip()
    settings = _load_agent_settings()
    server_settings = _sanitize_server_settings(read_yaml(SETTINGS_PATH))
    runtime_task_settings = dict(server_settings.get("task_settings") or {})
    heartbeat_ttl = float(settings.get("heartbeat_ttl", 180) or 180)
    now = time.time()
    runtime_messages = _normalize_messages_config(read_yaml(MESSAGES_PATH))

    db = Database()
    try:
        agent = db.get_agent(machine_id)
        agent_online = False
        if agent:
            last_seen = agent.get("last_seen")
            try:
                agent_online = bool(last_seen) and (now - float(last_seen) <= heartbeat_ttl)
            except Exception:
                agent_online = False

        configs = db.list_device_configs(owner)
        target_cfg = _select_device_config(
            configs,
            machine_id=machine_id,
            api_token=api_token,
        )
        owner_messages = db.get_message_templates(owner) or {}
        runtime_messages = _apply_owner_messages_config(runtime_messages, owner_messages)

        TASK_MANAGER._cleanup_stale_machine_action_tasks(
            db,
            owner,
            machine_id,
            heartbeat_ttl,
            trigger="client_status",
        )
        running_count = _count_agent_tasks_by_scope(
            db,
            machine_id=machine_id,
            statuses=("running",),
            actions=FORMAL_RUNTIME_ACTIONS,
        )
        pending_count = _count_agent_tasks_by_scope(
            db,
            machine_id=machine_id,
            statuses=("pending",),
            actions=FORMAL_RUNTIME_ACTIONS,
        )
        collect_running_count = _count_agent_tasks_by_scope(
            db,
            machine_id=machine_id,
            statuses=("running",),
            actions=COLLECT_ACTIONS,
        )
        collect_pending_count = _count_agent_tasks_by_scope(
            db,
            machine_id=machine_id,
            statuses=("pending",),
            actions=COLLECT_ACTIONS,
        )
        control_task = _load_latest_agent_task_by_scope(
            db,
            machine_id=machine_id,
            statuses=("pending", "running"),
            actions=("stop", "pause", "resume"),
        )
        control_action_name = str((control_task or {}).get("action") or "").strip()
        control_action_pending = bool(control_action_name)
        task_paused = bool(
            int(running_count or 0) > 0
            and TASK_MANAGER._is_task_paused_from_actions(
                db,
                owner=owner,
                machine_id=machine_id,
            )
        )
    finally:
        db.close()

    with TASK_MANAGER._lock:
        owner_logs = _filter_runtime_logs(
            list(TASK_MANAGER._runtime_logs),
            account_id=owner,
            machine_id=machine_id,
        )
    stats = build_runtime_stats(
        owner_logs,
        account_id=owner,
        machine_id=machine_id,
        running_tasks=running_count,
        running_accounts=(1 if running_count > 0 else 0),
    )
    config_synced = bool(target_cfg)
    device_bound = bool(
        target_cfg and str(target_cfg.get("machine_id") or "").strip() == machine_id
    )
    has_connection_fields = bool(
        target_cfg
        and str((target_cfg or {}).get("bit_api") or "").strip()
        and str((target_cfg or {}).get("api_token") or "").strip()
    )
    start_ready = bool(agent_online and device_bound and has_connection_fields)
    if not config_synced:
        start_block_reason = "config_not_synced"
    elif not device_bound:
        start_block_reason = "device_not_bound"
    elif not has_connection_fields:
        start_block_reason = "missing_connection_fields"
    elif not agent_online:
        start_block_reason = "agent_offline"
    else:
        start_block_reason = ""

    return {
        "owner": owner,
        "config_synced": config_synced,
        "device_bound": device_bound,
        "start_ready": start_ready,
        "start_block_reason": start_block_reason,
        "agent_online": agent_online,
        "task_running": running_count > 0,
        "task_pending": pending_count > 0,
        "task_paused": task_paused,
        "collect_running": collect_running_count > 0,
        "collect_pending": collect_pending_count > 0,
        "control_action_pending": control_action_pending,
        "control_action_name": control_action_name,
        "messages": runtime_messages,
        "task_settings": runtime_task_settings,
        "bit_api": str((target_cfg or {}).get("bit_api") or "").strip(),
        "api_token": str((target_cfg or {}).get("api_token") or "").strip(),
        "stats": stats,
    }


def handle_client_status(payload):
    verified = _verify_client_identity(payload)
    if not verified.get("ok"):
        return verified

    runtime_snapshot = _build_client_runtime_snapshot(
        verified,
        api_token=str(payload.get("api_token") or "").strip(),
    )

    return {
        "ok": True,
        "data": {
            "owner": runtime_snapshot.get("owner"),
            "config_synced": runtime_snapshot.get("config_synced"),
            "device_bound": runtime_snapshot.get("device_bound"),
            "start_ready": runtime_snapshot.get("start_ready"),
            "start_block_reason": runtime_snapshot.get("start_block_reason"),
            "agent_online": runtime_snapshot.get("agent_online"),
            "task_running": runtime_snapshot.get("task_running"),
            "task_pending": runtime_snapshot.get("task_pending"),
            "task_paused": runtime_snapshot.get("task_paused"),
            "collect_running": runtime_snapshot.get("collect_running"),
            "collect_pending": runtime_snapshot.get("collect_pending"),
            "messages": runtime_snapshot.get("messages"),
            "task_settings": runtime_snapshot.get("task_settings"),
            "bit_api": runtime_snapshot.get("bit_api"),
            "api_token": runtime_snapshot.get("api_token"),
            "stats": runtime_snapshot.get("stats"),
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

    window_token = str(payload.get("window_token") or "").strip()
    if action == "restart_window" and not window_token:
        return {"ok": False, "error": "请输入窗口号"}

    status_info = _build_client_runtime_snapshot(
        verified,
        api_token=api_token,
    )
    task_running = bool(status_info.get("task_running"))
    task_pending = bool(status_info.get("task_pending"))
    task_paused = bool(status_info.get("task_paused"))
    collect_running = bool(status_info.get("collect_running"))
    collect_pending = bool(status_info.get("collect_pending"))
    control_action_pending = bool(status_info.get("control_action_pending"))
    control_action_name = str(status_info.get("control_action_name") or "").strip()
    if control_action_pending:
        if action == control_action_name:
            return {
                "ok": True,
                "result": "duplicate",
                "message": _client_action_message(action, "duplicate", existing_status="running"),
            }
        return {
            "ok": True,
            "result": "blocked",
            "message": f"{_translate_control_action_state(control_action_name)}，请稍后再试",
        }
    if action == "start":
        if task_running:
            return {"ok": True, "result": "duplicate", "message": "当前任务已在运行，无需重复启动"}
        if task_pending:
            return {"ok": True, "result": "duplicate", "message": "启动任务已在排队，请勿重复启动"}
        if task_paused:
            return {"ok": True, "result": "blocked", "message": "当前任务处于暂停中，请先点击继续或停止"}
        if collect_running or collect_pending:
            return {"ok": True, "result": "blocked", "message": "当前正在采集群组，请等待采集完成后再启动"}
        if not status_info.get("start_ready"):
            return {
                "ok": True,
                "result": "blocked",
                "message": f"当前状态不允许启动：{_translate_client_start_block_reason(status_info.get('start_block_reason'))}",
            }
    elif action == "stop":
        if not (task_running or task_pending or task_paused or collect_running or collect_pending):
            return {"ok": True, "result": "blocked", "message": "当前没有可停止的运行/采集任务"}
    elif action == "pause":
        if task_paused:
            return {"ok": True, "result": "blocked", "message": "当前任务已处于暂停状态"}
        if not task_running:
            return {"ok": True, "result": "blocked", "message": "当前没有可暂停的运行任务"}
    elif action == "resume":
        if not task_paused:
            return {"ok": True, "result": "blocked", "message": "当前没有可继续的暂停任务"}
    elif action == "restart_window":
        if collect_running or collect_pending:
            return {"ok": True, "result": "blocked", "message": "当前正在采集群组，请等待采集完成后再单独启动"}
        if not status_info.get("start_ready"):
            return {
                "ok": True,
                "result": "blocked",
                "message": f"当前状态不允许单独启动：{_translate_client_start_block_reason(status_info.get('start_block_reason'))}",
            }

    ok, error, detail = TASK_MANAGER.enqueue_machine_action(
        action,
        owner_name=verified.get("owner"),
        machine_id=verified.get("machine_id"),
        api_token=api_token,
        window_token=window_token,
        allow_disabled=(action == "stop"),
        wait_online_seconds=4.0 if action in {"start", "restart_window"} else 0.0,
    )
    if not ok:
        return {"ok": False, "error": error or "执行失败"}
    detail = detail or {}
    result = str(detail.get("result") or "accepted").strip() or "accepted"
    return {
        "ok": True,
        "result": result,
        "message": _client_action_message(
            action,
            result,
            existing_status=detail.get("existing_status") or "",
        ),
        "task_id": detail.get("task_id"),
    }


def handle_agent_heartbeat(payload):
    ok, error, _agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    machine_id = str(payload.get("machine_id") or "").strip()
    client_version = str(payload.get("client_version") or "").strip() or None
    bit_api = str(payload.get("bit_api") or "").strip()
    api_token = str(payload.get("api_token") or "").strip()
    now = time.time()
    settings = _load_agent_settings()
    heartbeat_ttl = float(settings.get("heartbeat_ttl", 180) or 180)
    db = Database()
    try:
        if not db.get_agent(machine_id):
            return {"ok": False, "error": "agent_not_registered"}
        db.mark_agent_seen(machine_id, now, client_version=client_version)
        TASK_MANAGER._cleanup_stale_machine_action_tasks(
            db,
            str((_agent or {}).get("owner") or "").strip(),
            machine_id,
            heartbeat_ttl,
            trigger="agent_heartbeat",
        )
    finally:
        db.close()
    _invalidate_runtime_read_caches(clear_db_stats=True)
    return {"ok": True}


def handle_agent_list(owner=""):
    settings = _load_agent_settings()
    ttl = settings.get("heartbeat_ttl")
    now = time.time()
    db = Database()
    try:
        owner = (owner or "").strip()
        config_map = {}
        configs = db.list_device_configs(owner) if owner else db.list_all_device_configs()
        for cfg in configs:
            machine_id = str(cfg.get("machine_id") or "").strip()
            if not machine_id:
                continue
            config_map[machine_id] = cfg
        agents = []
        for row in db.list_agents():
            if owner and str(row.get("owner") or "").strip() != owner:
                continue
            last_seen = float(row.get("last_seen") or 0)
            online = (now - last_seen) <= ttl if last_seen else False
            machine_id = str(row.get("machine_id") or "").strip()
            cfg = config_map.get(machine_id) or {}
            agents.append(
                {
                    "machine_id": machine_id,
                    "last_seen": last_seen,
                    "online": online,
                    "owner": row.get("owner") or "",
                    "status": int(1 if row.get("status", None) is None else row.get("status")),
                    "client_version": row.get("client_version") or "",
                    "preferred_window_token": _format_window_token_list(
                        cfg.get("preferred_window_token")
                    ),
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
            if agent:
                try:
                    last_seen = float(agent.get("last_seen") or 0)
                except Exception:
                    last_seen = 0.0
                online = bool(last_seen) and (now - last_seen <= ttl)
            rows.append(
                {
                    "owner": cfg.get("owner") or "",
                    "name": cfg.get("name") or "",
                    "machine_id": machine_id,
                    "bit_api": str(cfg.get("bit_api") or "").strip(),
                    "api_token": str(cfg.get("api_token") or "").strip(),
                    "status": int(1 if cfg.get("status", None) is None else cfg.get("status")),
                    "online": online,
                    "last_seen": last_seen,
                }
            )
        return {"ok": True, "configs": rows}
    finally:
        db.close()


def _resolve_device_detail_owner(db, machine_id, owner=""):
    machine_id = str(machine_id or "").strip()
    owner = str(owner or "").strip()
    if not machine_id:
        return "", None
    configs = db.list_all_device_configs()
    matched_cfg = None
    for cfg in configs:
        if str(cfg.get("machine_id") or "").strip() != machine_id:
            continue
        cfg_owner = str(cfg.get("owner") or "").strip()
        if owner and cfg_owner != owner:
            continue
        matched_cfg = cfg
        owner = cfg_owner
        break
    if matched_cfg:
        return owner, matched_cfg
    agent = db.get_agent(machine_id)
    agent_owner = str((agent or {}).get("owner") or "").strip()
    if owner and agent_owner and agent_owner != owner:
        return "", None
    owner = owner or agent_owner
    if not owner:
        return "", None
    configs = db.list_device_configs(owner)
    return owner, _select_device_config(configs, machine_id=machine_id)


def handle_device_detail(owner="", machine_id=""):
    owner = str(owner or "").strip()
    machine_id = str(machine_id or "").strip()
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    settings = _load_agent_settings()
    ttl = float(settings.get("heartbeat_ttl", 180) or 180)
    now = time.time()
    db = Database()
    try:
        resolved_owner, cfg = _resolve_device_detail_owner(db, machine_id, owner=owner)
        if not resolved_owner:
            return {"ok": False, "error": "device_not_found"}
        agent = db.get_agent(machine_id)
        last_seen = 0.0
        online = False
        if agent:
            try:
                last_seen = float(agent.get("last_seen") or 0)
            except Exception:
                last_seen = 0.0
            online = bool(last_seen) and (now - last_seen <= ttl)
        detail = db.get_device_window_group_detail(resolved_owner, machine_id)
        device_info = {
            "owner": resolved_owner,
            "name": str((cfg or {}).get("name") or "").strip(),
            "machine_id": machine_id,
            "bit_api": str((cfg or {}).get("bit_api") or "").strip(),
            "api_token": str((cfg or {}).get("api_token") or "").strip(),
            "preferred_window_token": _format_window_token_list(
                (cfg or {}).get("preferred_window_token")
            ),
            "status": int(1 if (cfg or {}).get("status", None) is None else (cfg or {}).get("status")),
            "online": online,
            "last_seen": last_seen,
        }
        return {
            "ok": True,
            "device": device_info,
            "windows": detail.get("windows") or [],
        }
    finally:
        db.close()


def handle_device_group_selection_save(payload):
    owner = str(payload.get("owner") or "").strip()
    machine_id = str(payload.get("machine_id") or "").strip()
    selections = payload.get("selections") or []
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    if not isinstance(selections, list):
        return {"ok": False, "error": "invalid_selections"}
    db = Database()
    try:
        resolved_owner, _cfg = _resolve_device_detail_owner(db, machine_id, owner=owner)
        if not resolved_owner:
            return {"ok": False, "error": "device_not_found"}
        db.save_window_group_selections(resolved_owner, machine_id, selections)
        return {"ok": True}
    finally:
        db.close()


def handle_device_preferred_window_token_save(payload):
    owner = str(payload.get("owner") or "").strip()
    machine_id = str(payload.get("machine_id") or "").strip()
    preferred_window_token = _format_window_token_list(payload.get("preferred_window_token"))
    if not machine_id:
        return {"ok": False, "error": "missing_machine_id"}
    db = Database()
    try:
        resolved_owner, cfg = _resolve_device_detail_owner(db, machine_id, owner=owner)
        if not resolved_owner or not cfg:
            return {"ok": False, "error": "device_not_found"}
        db.update_device_preferred_window_token(
            cfg.get("id"),
            resolved_owner,
            preferred_window_token=preferred_window_token,
        )
        return {"ok": True}
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
    settings = _load_agent_settings()
    heartbeat_ttl = float(settings.get("heartbeat_ttl", 180) or 180)
    db = Database()
    try:
        db.mark_agent_seen(machine_id, time.time(), client_version=client_version)
        TASK_MANAGER._cleanup_stale_machine_action_tasks(
            db,
            str((_agent or {}).get("owner") or "").strip(),
            machine_id,
            heartbeat_ttl,
            trigger="agent_task_poll",
        )
        task = db.claim_agent_task(machine_id)
        if not task:
            return {"ok": True, "task": None}
        try:
            payload_data = json.loads(task.get("payload") or "{}")
        except Exception:
            payload_data = {}
        return {
            "ok": True,
            "task": {
                "id": task.get("id"),
                "owner": task.get("owner") or "",
                "action": task.get("action") or "",
                "window_token": task.get("window_token") or "",
                "payload": payload_data,
            },
        }
    finally:
        db.close()


def handle_agent_log(payload):
    ok, error, _agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    machine_id = str(payload.get("machine_id") or "").strip()
    owner = str(payload.get("owner") or "").strip()
    delivery_event = payload.get("delivery_event")
    lines = payload.get("lines")
    if isinstance(lines, str):
        lines = [lines]
    if not isinstance(lines, list):
        lines = []
    if isinstance(delivery_event, dict):
        group_id = str(delivery_event.get("group_id") or "").strip()
        profile_url = str(delivery_event.get("profile_url") or "").strip()
        account_id = str(delivery_event.get("account_id") or (_agent or {}).get("owner") or "").strip()
        scope_id = str(delivery_event.get("scope_id") or "").strip() or None
        user_id = str(delivery_event.get("user_id") or "").strip() or None
        success = bool(delivery_event.get("success"))
        if group_id and profile_url and account_id:
            if not scope_id:
                log.warning(
                    f"[delivery_event] 缺少 scope_id，已跳过运行时入库："
                    f"machine_id={machine_id or '-'} account_id={account_id or '-'} "
                    f"group_id={group_id or '-'} profile_url={profile_url or '-'}"
                )
            else:
                db = Database()
                try:
                    db.mark_done(
                        group_id=group_id,
                        profile_url=profile_url,
                        account_id=account_id,
                        success=success,
                        user_id=user_id,
                        machine_id=machine_id,
                        scope_id=scope_id,
                    )
                finally:
                    db.close()
                _invalidate_runtime_read_caches(
                    clear_db_stats=True,
                    clear_user_list=True,
                )
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
        TASK_MANAGER._append_runtime_log(
            f"{prefix}{line}",
            owner=owner,
            machine_id=machine_id,
        )
    return {"ok": True}


def handle_agent_report(payload):
    ok, error, agent = _verify_agent_request(payload)
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
        task_row = db.fetch_one(
            """
            SELECT id, owner, assigned_machine_id, action, status, error
            FROM agent_tasks
            WHERE id = ?
            LIMIT 1
            """,
            (task_id,),
        )
        action = str((task_row or {}).get("action") or "").strip()
        owner = str((task_row or {}).get("owner") or (agent or {}).get("owner") or "").strip()
        machine_id = str((task_row or {}).get("assigned_machine_id") or payload.get("machine_id") or "").strip()
        current_status = str((task_row or {}).get("status") or "").strip()
        current_error = str((task_row or {}).get("error") or "").strip()
        preserve_stopped_state = (
            action in {"start", "restart_window", "collect_groups"}
            and current_status == "failed"
            and current_error in {"task_stopped", "collect_stopped"}
            and status == "done"
        )
        if not preserve_stopped_state:
            db.update_agent_task(task_id, status, error=error, exit_code=exit_code)
        if action == "stop" and status == "done" and owner and machine_id:
            stop_time = time.time()
            db.execute_statement(
                """
                UPDATE agent_tasks
                SET status = 'failed', error = ?, updated_at = ?
                WHERE owner = ?
                  AND assigned_machine_id = ?
                  AND action IN ('start', 'restart_window')
                  AND status IN ('pending', 'running')
                """,
                ("task_stopped", stop_time, owner, machine_id),
            )
            db.execute_statement(
                """
                UPDATE agent_tasks
                SET status = 'failed', error = ?, updated_at = ?
                WHERE owner = ?
                  AND assigned_machine_id = ?
                  AND action = 'collect_groups'
                  AND status IN ('pending', 'running')
                """,
                ("collect_stopped", stop_time, owner, machine_id),
            )
        if action == "stop_collect" and status == "done" and owner and machine_id:
            db.execute_statement(
                """
                UPDATE agent_tasks
                SET status = 'failed', error = ?, updated_at = ?
                WHERE owner = ?
                  AND assigned_machine_id = ?
                  AND action = 'collect_groups'
                  AND status IN ('pending', 'running')
                """,
                ("collect_stopped", time.time(), owner, machine_id),
            )
        db.commit()
    finally:
        db.close()
    return {"ok": True}


def handle_agent_collect_report(payload):
    ok, error, agent = _verify_agent_request(payload)
    if not ok:
        return {"ok": False, "error": error}
    machine_id = str(payload.get("machine_id") or "").strip()
    owner = str((agent or {}).get("owner") or "").strip()
    payload_owner = str(payload.get("owner") or "").strip()
    if payload_owner and owner and payload_owner != owner:
        return {"ok": False, "error": "owner_mismatch"}
    windows = payload.get("windows")
    if windows is None:
        windows = []
    if not isinstance(windows, list):
        return {"ok": False, "error": "invalid_windows"}
    collected_at = time.time()
    summaries = []
    db = Database()
    try:
        for item in windows:
            if not isinstance(item, dict):
                continue
            window_token = str(item.get("window_token") or item.get("window_id") or "").strip()
            if not window_token:
                continue
            window_name = str(item.get("window_name") or "").strip()
            groups = item.get("groups")
            if groups is None:
                groups = []
            if not isinstance(groups, list):
                groups = []
            collect_error = str(item.get("error") or "").strip()
            if collect_error == "facebook_not_logged_in":
                last_collect_message = "未登录 Facebook，当前窗口采集已跳过"
            elif collect_error == "account_restricted":
                last_collect_message = "命中 Facebook checkpoint/风控页，当前窗口采集已跳过"
            elif collect_error == "session_open_failed":
                last_collect_message = "连接浏览器失败，当前窗口采集未执行"
            elif collect_error:
                last_collect_message = f"采集失败：{collect_error}"
            else:
                last_collect_message = ""
            result = db.replace_window_group_collections(
                owner=owner,
                machine_id=machine_id,
                window_token=window_token,
                groups=groups,
                collected_at=collected_at,
                window_name=window_name,
                last_collect_status=collect_error or "",
                last_collect_message=last_collect_message,
            )
            summaries.append(result)
    finally:
        db.close()

    prefix = f"[{owner}@{machine_id}] " if owner and machine_id else ""
    for row in summaries:
        TASK_MANAGER._append_runtime_log(
            f"{prefix}窗口 {row.get('window_token') or '-'} 采集完成，共采集 {int(row.get('saved') or 0)} 个群",
            owner=owner,
            machine_id=machine_id,
        )
    TASK_MANAGER._append_runtime_log(
        f"{prefix}群组采集上报完成，共处理 {len(summaries)} 个窗口",
        owner=owner,
        machine_id=machine_id,
    )
    return {
        "ok": True,
        "data": {
            "owner": owner,
            "machine_id": machine_id,
            "windows": summaries,
        },
    }

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
        _invalidate_runtime_read_caches(
            clear_db_stats=True,
            clear_user_list=True,
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
        _invalidate_runtime_read_caches(
            clear_db_stats=True,
            clear_user_list=True,
        )
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
    _invalidate_runtime_read_caches(
        clear_db_stats=True,
        clear_user_list=True,
    )
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
    _invalidate_runtime_read_caches(
        clear_db_stats=True,
        clear_user_list=True,
    )
    invalidate_user_sessions(username)
    TASK_MANAGER.stop_for_owner(username)
    return {"ok": True}


def save_full_config(payload, actor_username=None, actor_role=None):
    messages_payload = payload.get("messages") or {}

    if actor_role != "admin":
        if actor_username:
            db = Database()
            try:
                existing = db.get_message_templates(actor_username) or {}
                templates = existing.get("templates")
                if "templates" in messages_payload:
                    templates = messages_payload.get("templates") or []
                popup_stop_texts = existing.get("popup_stop_texts")
                if "popup_stop_texts" in messages_payload:
                    popup_stop_texts = _normalize_message_lines(
                        messages_payload.get("popup_stop_texts") or []
                    )
                permanent_skip_texts = existing.get("permanent_skip_texts")
                if "permanent_skip_texts" in messages_payload:
                    permanent_skip_texts = _normalize_message_lines(
                        messages_payload.get("permanent_skip_texts") or []
                    )
                db.set_message_templates(
                    actor_username,
                    templates,
                    popup_stop_texts=popup_stop_texts,
                    permanent_skip_texts=permanent_skip_texts,
                )
            finally:
                db.close()
        return

    current_messages = _normalize_messages_config(read_yaml(MESSAGES_PATH) or {})
    messages = _merge_messages_config(current_messages, messages_payload)
    write_yaml(MESSAGES_PATH, messages)


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


def ensure_server_task_settings_defaults():
    settings = read_yaml(SETTINGS_PATH)
    cleaned = _sanitize_server_settings(settings)
    if cleaned != settings:
        write_yaml(SETTINGS_PATH, cleaned)


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
    ensure_server_task_settings_defaults()
    ensure_admin_account()
    ensure_activation_codes_uuid()
    start_cleanup_daemon()
    cfg = web_ui_config()
    host = cfg.get("host", "127.0.0.1")
    port = int(cfg.get("port", DEFAULT_WEB_PORT))
    server, error = start_server(host, port)
    if not server:
        ui_log(f"启动失败，端口 {port} 无法监听: {error}")
        if getattr(error, "errno", None) == errno.EADDRINUSE:
            message = f"端口 {port} 已被占用，请关闭占用进程后重试。"
        else:
            message = f"端口 {port} 无法监听：{error or '未知错误'}"
        show_ui_alert("启动失败", message)
        return

    url = f"http://{host}:{port}"
    print(f"本地管理后台已启动：{url}")
    ui_log(f"管理后台已启动：{url}")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    server.serve_forever()


if __name__ == "__main__":
    main()
