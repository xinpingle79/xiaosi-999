import argparse
import hashlib
import os
import random
import re
import subprocess
import sys
import threading
import time
import warnings
import ctypes
from datetime import datetime, timedelta
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

import requests
import yaml
from playwright.sync_api import sync_playwright

from core.browser_manager import BrowserManager
from core.messager import Messager
from core.scraper import Scraper
from utils.helpers import Database
from utils.logger import log

warnings.filterwarnings("ignore")

APP_NAME = "小四客户端"
IS_FROZEN = getattr(sys, "frozen", False)
BASE_DIR = (
    Path(sys.executable).resolve().parent
    if IS_FROZEN
    else Path(__file__).resolve().parent
)
os.chdir(BASE_DIR)

ENV_RUNTIME_DIR = "FB_RPA_RUNTIME_DIR"
ALERT_HELPER_PATH = (
    str(BASE_DIR / "desktop_alert.py")
    if not IS_FROZEN
    else ""
)
ENV_BIT_API = "FB_RPA_BIT_API"
ENV_API_TOKEN = "FB_RPA_API_TOKEN"
ENV_OWNER = "FB_RPA_OWNER"
ENV_STOP_FLAG = "FB_RPA_STOP_FLAG"
ENV_PAUSE_FLAG = "FB_RPA_PAUSE_FLAG"
CLEANUP_HISTORY_DAYS = 15
CLEANUP_LOG_DAYS = 7
ALERT_COOLDOWN_SECONDS = 60
_WINDOW_ALERT_CACHE = {}


def _user_data_root():
    local_appdata = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
    if local_appdata:
        return Path(local_appdata) / APP_NAME
    return Path.home() / "AppData" / "Local" / APP_NAME


def _client_config_path():
    if IS_FROZEN:
        return _user_data_root() / "config" / "client.yaml"
    return BASE_DIR / "config" / "client.yaml"


def _messages_config_path():
    if IS_FROZEN:
        return _user_data_root() / "config" / "messages.yaml"
    return BASE_DIR / "config" / "messages.yaml"


def run_task(window_token=None):
    config, msgs = load_runtime_files()
    runtime_dir = (config.get("runtime_dir") or "").strip()
    if runtime_dir and not os.environ.get(ENV_RUNTIME_DIR):
        os.environ[ENV_RUNTIME_DIR] = runtime_dir
    bootstrap_db = Database()
    bootstrap_db.close()
    start_cleanup_daemon(config)

    browser_settings = build_browser_settings(config)

    try:
        if window_token:
            run_single_bitbrowser_window(config, msgs, browser_settings, window_token)
        else:
            run_bitbrowser_batch(config, msgs, browser_settings)
    except KeyboardInterrupt:
        log.warning("检测到手动中断，任务已停止。")


def load_runtime_files():
    client_path = _client_config_path()
    config = {}
    if client_path.exists():
        with open(client_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

    messages_path = _messages_config_path()
    msgs = {}
    if messages_path.exists():
        with open(messages_path, "r", encoding="utf-8") as f:
            msgs = yaml.safe_load(f) or {}
    return config, msgs


def _normalize_message_templates(templates):
    return [
        str(item).strip()
        for item in (templates or [])
        if str(item).strip()
    ]


def _sync_delivery_event(
    config,
    group_id,
    profile_url,
    account_id,
    success,
    user_id=None,
    machine_id="",
    reason="",
    scope_id=None,
):
    server_url = str(config.get("server_url") or "").strip().rstrip("/")
    agent_token = str(config.get("agent_token") or "").strip()
    resolved_machine_id = str(machine_id or config.get("machine_id") or "").strip()
    normalized_group_id = str(group_id or "").strip()
    normalized_profile_url = str(profile_url or "").strip()
    normalized_account_id = str(account_id or "").strip()
    normalized_reason = str(reason or "").strip()
    normalized_scope_id = str(scope_id or "").strip()
    if not (
        server_url
        and agent_token
        and resolved_machine_id
        and normalized_group_id
        and normalized_profile_url
        and normalized_account_id
    ):
        return False
    try:
        response = requests.post(
            f"{server_url}/api/agent/log",
            json={
                "machine_id": resolved_machine_id,
                "agent_token": agent_token,
                "delivery_event": {
                    "group_id": normalized_group_id,
                    "profile_url": normalized_profile_url,
                    "account_id": normalized_account_id,
                    "user_id": str(user_id or "").strip(),
                    "success": bool(success),
                    "reason": normalized_reason,
                    "machine_id": resolved_machine_id,
                    "scope_id": normalized_scope_id,
                },
            },
            timeout=5,
        )
        return response.ok
    except Exception as exc:
        log.warning(f"发送结果同步服务器失败: {exc}")
        return False


def _record_delivery_result(
    db,
    config,
    group_id,
    target,
    account_id,
    machine_id,
    success,
    reason="",
    scope_id=None,
):
    profile_url = str(target.get("profile_url") or target.get("group_user_url") or "").strip()
    if not profile_url:
        return
    user_id = target.get("user_id")
    db.mark_done(
        group_id=group_id,
        profile_url=profile_url,
        account_id=account_id,
        user_id=user_id,
        machine_id=machine_id,
        success=success,
        scope_id=scope_id,
    )
    _sync_delivery_event(
        config=config,
        group_id=group_id,
        profile_url=profile_url,
        account_id=account_id,
        success=success,
        user_id=user_id,
        machine_id=machine_id,
        reason=reason,
        scope_id=scope_id,
    )


def resolve_message_templates(local_messages, account_messages=None):
    local_payload = local_messages if isinstance(local_messages, dict) else {}
    account_payload = account_messages if isinstance(account_messages, dict) else {}
    local_formal = _normalize_message_templates(local_payload.get("templates") or [])
    local_probe = _normalize_message_templates(local_payload.get("probe_templates") or [])
    owner_formal = _normalize_message_templates(account_payload.get("templates") or [])
    owner_probe = _normalize_message_templates(account_payload.get("probe_templates") or [])

    if local_formal:
        selected_templates = local_formal
        template_source = "local_messages_yaml"
    elif owner_formal:
        selected_templates = owner_formal
        template_source = "db_owner_templates"
    elif local_probe:
        selected_templates = local_probe
        template_source = "local_probe_templates"
    elif owner_probe:
        selected_templates = owner_probe
        template_source = "db_owner_probe_templates"
    else:
        selected_templates = []
        template_source = "missing_templates"

    return {
        "templates": selected_templates,
        "probe_templates": local_probe or owner_probe,
        "template_source": template_source,
        "stats": {
            "local_formal": len(local_formal),
            "owner_formal": len(owner_formal),
            "local_probe": len(local_probe),
            "owner_probe": len(owner_probe),
        },
    }


def _runtime_dir(config=None):
    runtime_root = (os.environ.get(ENV_RUNTIME_DIR) or "").strip()
    if not runtime_root and config:
        runtime_root = (config.get("runtime_dir") or "").strip()
    if runtime_root:
        base = Path(runtime_root).expanduser()
    elif IS_FROZEN:
        base = _user_data_root() / "runtime"
    else:
        base = BASE_DIR / "runtime"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _runtime_path(config, *parts):
    return _runtime_dir(config).joinpath(*parts)


def _resolve_runtime_path(config, path_value):
    if not path_value:
        return ""
    path_str = str(path_value)
    if os.path.isabs(path_str):
        return path_str
    return str(_runtime_path(config, path_str))


def _restart_requests_file(config):
    return _runtime_path(config, "data", "restart_window_requests.txt")


def build_browser_settings(config):
    browser_cfg = config.get("browser", {})
    bit_api_override = (os.environ.get(ENV_BIT_API) or "").strip()
    api_token_override = (os.environ.get(ENV_API_TOKEN) or "").strip()
    return {
        "bit_api": bit_api_override or config.get("bit_api"),
        "api_token": api_token_override or config.get("api_token"),
        "auto_discover_bitbrowser": browser_cfg.get("auto_discover_bitbrowser", True),
        "bitbrowser_open_timeout_seconds": config.get("bitbrowser_open_timeout_seconds"),
        "bitbrowser_active_poll_seconds": config.get("bitbrowser_active_poll_seconds"),
        "bitbrowser_active_poll_interval_seconds": config.get("bitbrowser_active_poll_interval_seconds"),
        "bitbrowser_open_retry_count": config.get("bitbrowser_open_retry_count"),
        "bitbrowser_cdp_connect_retry_count": config.get("bitbrowser_cdp_connect_retry_count"),
        "bitbrowser_cdp_connect_retry_delay": config.get("bitbrowser_cdp_connect_retry_delay"),
        "bitbrowser_cdp_refresh_every": config.get("bitbrowser_cdp_refresh_every"),
    }


def run_bitbrowser_batch(config, msgs, browser_settings):
    bm = BrowserManager(browser_settings)
    profiles = sort_bitbrowser_profiles(bm.list_bitbrowser_profiles())
    max_windows = int(config.get("task_settings", {}).get("max_windows", 0) or 0)
    if max_windows > 0:
        profiles = profiles[:max_windows]
    if not profiles:
        log.error("未获取到任何可用的比特浏览器窗口，任务结束。")
        return

    log.info(
        f"已自动识别 {len(profiles)} 个比特浏览器窗口，将顺序接入：前一窗口进入小组成员页后再放行下一窗口，并发执行。"
    )
    total_sent = 0
    finished_groups = 0
    profile_index = build_profile_index(profiles)
    partition_total = len(profiles)
    profile_partition_index = {
        str(profile.get("id") or "").strip(): index
        for index, profile in enumerate(profiles)
        if str(profile.get("id") or "").strip()
    }

    with ThreadPoolExecutor(
        max_workers=max(1, len(profiles)),
        thread_name_prefix="fb-bitbrowser",
    ) as executor:
        future_map = {}

        def submit_profile(profile, startup_ready_event=None):
            profile_id = str(profile.get("id") or "").strip()
            partition_index = profile_partition_index.get(profile_id, 0)
            future = executor.submit(
                run_single_session,
                config,
                msgs,
                browser_settings,
                profile,
                None,
                {
                    "index": partition_index,
                    "count": partition_total,
                },
                startup_ready_event,
            )
            future_map[future] = profile

        previous_startup_ready_event = None
        for profile in profiles:
            if previous_startup_ready_event is not None:
                previous_startup_ready_event.wait()
            current_startup_ready_event = threading.Event()
            submit_profile(profile, startup_ready_event=current_startup_ready_event)
            previous_startup_ready_event = current_startup_ready_event

        while future_map:
            done, _ = wait(
                list(future_map.keys()),
                timeout=1,
                return_when=FIRST_COMPLETED,
            )

            for future in done:
                profile = future_map.pop(future)
                label = format_account_label(profile)
                try:
                    summary = future.result()
                except Exception as exc:
                    log.error(f"[{label}] 窗口任务异常退出: {exc}")
                    continue

                total_sent += summary.get("messages_sent", 0)
                finished_groups += summary.get("finished_groups", 0)
                reason = summary.get("reason")
                if reason == "account_restricted":
                    log.warning(f"[{label}] 当前窗口因风控/无法发送已停止，其它正常窗口继续运行。")
                elif reason == "stranger_message_limit_reached":
                    log.warning(f"[{label}] 当前窗口因陌生消息额度上限已停止，其它正常窗口继续运行。")
                elif reason == "account_disabled":
                    log.warning(f"[{label}] 当前窗口账号已禁用，已停止运行。")

            for request in consume_restart_requests(config):
                resolved_profile = resolve_restart_profile(request, profile_index, bm)
                if not resolved_profile:
                    log.warning(f"未识别到需要单独重启的窗口: {request}")
                    continue

                if any(
                    future_profile.get("id") == resolved_profile.get("id")
                    for future_profile in future_map.values()
                ):
                    log.info(
                        f"[{format_account_label(resolved_profile)}] 当前窗口任务仍在运行，无需重复重启。"
                    )
                    continue

                log.info(f"[{format_account_label(resolved_profile)}] 收到单独重启请求，开始重新接入。")
                submit_profile(resolved_profile)

    log.info(
        f"本轮全部窗口处理结束，共处理 {len(profiles)} 个窗口，"
        f"完成 {finished_groups} 个小组，成功发送 {total_sent} 条消息。"
    )


def run_single_bitbrowser_window(config, msgs, browser_settings, window_token):
    bm = BrowserManager(browser_settings)
    profiles = sort_bitbrowser_profiles(bm.list_bitbrowser_profiles())
    profile_index = build_profile_index(profiles)
    profile = resolve_restart_profile(window_token, profile_index, bm)
    if not profile:
        log.error(f"未识别到需要启动的窗口: {window_token}")
        return

    log.info(f"[{format_account_label(profile)}] 已收到单独启动请求。")
    summary = run_single_session(
        config,
        msgs,
        browser_settings,
        profile,
        window_token=window_token,
    )
    log.info(
        f"[{summary['account_id']}] 单独窗口任务结束，"
        f"完成 {summary['finished_groups']} 个小组，成功发送 {summary['messages_sent']} 条消息。"
    )


def run_single_session(
    config,
    msgs,
    browser_settings,
    profile=None,
    window_token=None,
    window_partition=None,
    startup_ready_event=None,
):
    db = Database()
    task_cfg = config["task_settings"]
    bm = BrowserManager(browser_settings)
    session = None
    label = format_account_label(profile)
    window_token_label = resolve_window_token(profile, window_token)
    owner_username = (os.environ.get(ENV_OWNER) or "").strip()
    machine_id = str(config.get("machine_id") or "").strip()
    account_messages = None
    if owner_username:
        account_messages = db.get_message_templates(owner_username)
    template_resolution = resolve_message_templates(msgs, account_messages)
    probe_templates = template_resolution["probe_templates"]
    formal_templates = template_resolution["templates"]
    template_source = template_resolution["template_source"]
    template_stats = template_resolution["stats"]
    startup_gate_released = False

    def release_startup_gate():
        nonlocal startup_gate_released
        if startup_ready_event and not startup_gate_released:
            startup_ready_event.set()
            startup_gate_released = True

    try:
        with sync_playwright() as p:
            try:
                browser_id = profile.get("id") if profile else None
                session = bm.open_for_browser(p, browser_id=browser_id)
                if not session:
                    release_startup_gate()
                    log.error(f"[{label or '默认窗口'}] 连接浏览器失败，跳过当前窗口。")
                    return {
                        "account_id": label or "unknown",
                        "messages_sent": 0,
                        "finished_groups": 0,
                        "reason": "session_open_failed",
                    }

                page = session["page"]
                profile_account_id = session["account_id"]
                stats_account_id = owner_username or profile_account_id
                account_id = stats_account_id
                alert_account_id = label or profile_account_id
                scope_id = resolve_window_scope_id(
                    profile=profile,
                    browser_id=browser_id,
                    window_token=window_token_label,
                    account_id=profile_account_id,
                    machine_id=machine_id,
                )
                scraper = Scraper(page, window_label=alert_account_id)
                sender = Messager(page, task_cfg, window_label=alert_account_id)
                max_messages = normalize_message_limit(task_cfg.get("max_messages_per_account", 0))
                if template_source == "local_messages_yaml":
                    log.info(f"[{alert_account_id}] 使用本地文案文件 ({len(formal_templates)} 条)")
                elif template_source == "db_owner_templates":
                    log.info(f"[{alert_account_id}] 本地正式文案为空，回退子账户文案 ({len(formal_templates)} 条)")
                elif template_source == "local_probe_templates":
                    log.warning(f"[{alert_account_id}] 正式文案为空，改用本地探针文案发送。")
                elif template_source == "db_owner_probe_templates":
                    log.warning(f"[{alert_account_id}] 正式文案为空，改用子账户探针文案发送。")
                log.info(
                    f"[{alert_account_id}] 文案装载完成：selected={template_source}，"
                    f"local_formal={template_stats['local_formal']}，"
                    f"owner_formal={template_stats['owner_formal']}，"
                    f"local_probe={template_stats['local_probe']}，"
                    f"owner_probe={template_stats['owner_probe']}，"
                    f"active={len(formal_templates)}，probe={len(probe_templates)}"
                )
                log.info(
                    f"[{alert_account_id}] 发送频率来源：source=client_yaml.task_settings.send_interval_seconds"
                )

                if owner_username and db.is_account_disabled(owner_username):
                    release_startup_gate()
                    log.warning(f"[{alert_account_id}] 当前账号已禁用，停止任务。")
                    return {
                        "account_id": alert_account_id,
                        "messages_sent": 0,
                        "finished_groups": 0,
                        "reason": "account_disabled",
                    }

                if not formal_templates:
                    release_startup_gate()
                    log.error(f"[{alert_account_id}] 正式发送文案为空，当前窗口无法启动。")
                    return {
                        "account_id": alert_account_id,
                        "messages_sent": 0,
                        "finished_groups": 0,
                        "reason": "missing_templates",
                    }

                log.info(
                    f"[{alert_account_id}] 已成功接入 BitBrowser 会话，开始按小组顺序执行成员私聊..."
                )

                group_urls = collect_target_groups(scraper, task_cfg)
                if not group_urls:
                    release_startup_gate()
                    log.error(f"[{alert_account_id}] 未能收集到任何已加入小组，流程结束。")
                    return {
                        "account_id": alert_account_id,
                        "messages_sent": 0,
                        "finished_groups": 0,
                        "reason": "no_groups",
                    }
                partition_count = int((window_partition or {}).get("count") or 0)
                partition_index = int((window_partition or {}).get("index") or 0)
                if partition_count > 1:
                    total_group_count = len(group_urls)
                    group_urls = partition_group_urls(
                        group_urls,
                        partition_index,
                        partition_count,
                    )
                    if not group_urls:
                        release_startup_gate()
                        log.info(
                            f"[{alert_account_id}] 小组分片已启用：窗口 {partition_index + 1}/{partition_count}，"
                            "当前窗口本轮未分配到小组。"
                        )
                        return {
                            "account_id": alert_account_id,
                            "messages_sent": 0,
                            "finished_groups": 0,
                            "reason": "no_partition_groups",
                        }
                    log.info(
                        f"[{alert_account_id}] 小组分片已启用：窗口 {partition_index + 1}/{partition_count}，"
                        f"分配 {len(group_urls)}/{total_group_count} 个小组。"
                    )

                total_sent = 0
                finished_groups = 0
                final_reason = "completed"
                for idx, group_url in enumerate(group_urls, start=1):
                    cont, _ = wait_if_paused(
                        task_cfg,
                        db=db,
                        group_id=None,
                        account_id=account_id,
                        scope_id=scope_id,
                    )
                    if not cont:
                        log.warning(f"[{alert_account_id}] 检测到停止标记，任务提前结束。")
                        final_reason = "stopped"
                        break
                    if should_stop(task_cfg):
                        log.warning(f"[{alert_account_id}] 检测到停止标记，任务提前结束。")
                        final_reason = "stopped"
                        break
                    if owner_username and db.is_account_disabled(owner_username):
                        log.warning(f"[{alert_account_id}] 当前账号已禁用，停止任务。")
                        final_reason = "account_disabled"
                        break
                    if max_messages is not None and total_sent >= max_messages:
                        log.info(f"[{alert_account_id}] 当前窗口已达到发送上限。")
                        final_reason = "message_limit"
                        break

                    if not scraper.open_group_members_page(group_url):
                        log.warning(f"[{alert_account_id}] 进入小组成员页失败，跳过当前小组。")
                        continue
                    release_startup_gate()
                    group_id = scraper.get_current_group_id()
                    if group_id and db.is_group_done(group_id, account_id=account_id, scope_id=scope_id):
                        log.info(f"[{alert_account_id}] 当前小组已标记完成，跳过: {group_url}")
                        continue
                    log.info(f"[{alert_account_id}] 开始处理第 {idx}/{len(group_urls)} 个小组: {group_url}")

                    remaining_budget = None
                    if max_messages is not None:
                        remaining_budget = max_messages - total_sent

                    result = process_current_group(
                        scraper=scraper,
                        sender=sender,
                        db=db,
                        config=config,
                        account_id=account_id,
                        scope_id=scope_id,
                        machine_id=machine_id,
                        owner_username=owner_username,
                        probe_templates=probe_templates,
                        templates=formal_templates,
                        template_source=template_source,
                        task_cfg=task_cfg,
                        message_budget=remaining_budget,
                    )
                    total_sent += result["messages_sent"]
                    if result["reason"] not in {
                        "group_id_not_found",
                        "newcomers_section_not_found",
                        "newcomers_scope_unconfirmed",
                        "cursor_not_found_required",
                    }:
                        finished_groups += 1

                    if result["reason"] == "stopped":
                        log.warning(f"[{alert_account_id}] 收到停止信号，结束当前窗口。")
                        final_reason = "stopped"
                        break
                    if result["reason"] == "account_restricted":
                        blocked_targets = result.get("blocked_targets") or []
                        silent_targets = result.get("silent_targets") or []
                        show_account_restricted_alert(alert_account_id, blocked_targets, silent_targets)
                        final_reason = "account_restricted"
                        break
                    if result["reason"] == "stranger_message_limit_reached":
                        show_window_limit_alert(
                            window_token_label,
                            "stranger_message_limit_reached",
                            result.get("alert_text")
                            or "你的陌生消息已达数量上限\n24 小时内可发送的陌生消息数量有上限。",
                        )
                        final_reason = "stranger_message_limit_reached"
                        break
                    if result["reason"] in {
                        "message_request_limit_reached",
                        "account_cannot_message",
                        "send_restricted_ui",
                        "account_restricted_by_dialog",
                        "send_blocked_popup",
                    }:
                        show_window_limit_alert(
                            window_token_label,
                            result["reason"],
                            result.get("alert_text"),
                        )
                        final_reason = result["reason"]
                        break
                    if result["reason"] == "message_limit":
                        log.info(f"[{alert_account_id}] 当前窗口达到发送上限，停止继续处理后续小组。")
                        final_reason = "message_limit"
                        break
                    if result["reason"] == "cursor_not_found_required":
                        log.error(
                            f"[{alert_account_id}] 当前小组断点未找到且配置要求必须命中断点，保留原断点并继续下一个小组。"
                        )
                        continue
                    if result["reason"] == "newcomers_scope_unconfirmed":
                        log.error(f"[{alert_account_id}] 当前小组新人区目标来源无法确认，跳过当前小组。")
                        continue
                    if result["reason"] == "account_disabled":
                        log.warning(f"[{alert_account_id}] 当前账号已禁用，停止任务。")
                        final_reason = "account_disabled"
                        break
                    if result["reason"] == "group_exhausted":
                        log.info(f"[{alert_account_id}] 当前小组已扫描到底部且没有新用户，继续下一个小组。")
                        db.mark_group_done(group_id, account_id=account_id, scope_id=scope_id)

                log.info(
                    f"[{alert_account_id}] 当前窗口处理结束，共完成 {finished_groups} 个小组，"
                    f"成功发送 {total_sent} 条消息。"
                )
                return {
                    "account_id": alert_account_id,
                    "messages_sent": total_sent,
                    "finished_groups": finished_groups,
                    "reason": final_reason,
                }
            finally:
                release_startup_gate()
                bm.close(session)
    finally:
        db.close()


def sort_bitbrowser_profiles(profiles):
    unique_profiles = {}
    for item in profiles or []:
        browser_id = item.get("id")
        if not browser_id:
            continue
        unique_profiles[browser_id] = item

    def safe_seq(value):
        try:
            return int(value)
        except Exception:
            return 10**9

    return sorted(
        unique_profiles.values(),
        key=lambda item: (
            safe_seq(item.get("seq")),
            item.get("name") or "",
            item["id"],
        ),
    )


def build_profile_index(profiles):
    index = {}
    for profile in profiles or []:
        browser_id = (profile.get("id") or "").strip()
        if browser_id:
            index[browser_id] = profile
        seq = profile.get("seq")
        if seq not in (None, ""):
            index[str(seq).strip()] = profile
    return index


def consume_restart_requests(config):
    requests_file = _restart_requests_file(config)
    if not os.path.exists(requests_file):
        return []

    try:
        with open(requests_file, "r", encoding="utf-8") as f:
            requests = [line.strip() for line in f if line.strip()]
        os.remove(requests_file)
        return requests
    except Exception as exc:
        log.warning(f"读取单独重启请求失败: {exc}")
        return []


def resolve_restart_profile(request, profile_index, browser_manager):
    token = str(request or "").strip()
    if not token:
        return None

    if token in profile_index:
        return profile_index[token]

    refreshed_profiles = sort_bitbrowser_profiles(browser_manager.list_bitbrowser_profiles())
    profile_index.update(build_profile_index(refreshed_profiles))
    return profile_index.get(token)


def format_account_label(profile):
    if not profile:
        return ""

    parts = []
    if profile.get("seq") not in (None, ""):
        parts.append(f"窗口{profile['seq']}")
    if profile.get("name"):
        parts.append(str(profile["name"]))
    if profile.get("id"):
        parts.append(profile["id"])
    return " / ".join(parts)


def resolve_window_token(profile, provided=None):
    token = str(provided or "").strip()
    if token:
        return token
    if not profile:
        return ""
    seq = profile.get("seq")
    if seq not in (None, ""):
        return str(seq).strip()
    browser_id = profile.get("id")
    if browser_id:
        return str(browser_id).strip()
    return ""


def resolve_window_scope_id(profile=None, browser_id=None, window_token=None, account_id=None, machine_id=None):
    candidates = [
        str(browser_id or "").strip(),
        str((profile or {}).get("id") or "").strip(),
        str(window_token or "").strip(),
        str(account_id or "").strip(),
        str(machine_id or "").strip(),
    ]
    for value in candidates:
        if value:
            return value
    return "global"


def stable_partition_slot(value, partition_count):
    normalized = str(value or "").strip()
    if partition_count <= 1 or not normalized:
        return 0
    digest = hashlib.sha1(normalized.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partition_count


def partition_group_urls(group_urls, partition_index, partition_count):
    if partition_count <= 1:
        return list(group_urls or [])
    return [
        url
        for url in (group_urls or [])
        if stable_partition_slot(url, partition_count) == partition_index
    ]


def collect_target_groups(scraper, task_cfg):
    current_group_root = scraper.get_current_group_root_url() if scraper.is_group_members_page() else None
    group_urls = scraper.collect_joined_group_urls(
        max_scrolls=task_cfg.get("group_list_scroll_times", 8)
    )

    if current_group_root:
        if current_group_root in group_urls:
            group_urls = [current_group_root] + [
                url for url in group_urls if url != current_group_root
            ]
        else:
            group_urls.insert(0, current_group_root)

    seen = set()
    ordered = []
    for url in group_urls:
        if url in seen:
            continue
        seen.add(url)
        ordered.append(url)
    return ordered


def describe_cursor_target(cursor):
    if not cursor:
        return "未知断点"
    return (
        cursor.get("name")
        or cursor.get("profile_url")
        or cursor.get("group_user_url")
        or cursor.get("user_id")
        or "未知断点"
    )


def build_cursor_search_result(reason, found_cursor, messages_sent, cursor, **extra):
    result = {
        "found_cursor": found_cursor,
        "messages_sent": messages_sent,
        "reason": reason,
        "cursor_target": describe_cursor_target(cursor),
    }
    result.update(extra)
    return result


def process_current_group(
    scraper,
    sender,
    db,
    config,
    account_id,
    scope_id,
    machine_id,
    owner_username,
    probe_templates,
    templates,
    task_cfg,
    message_budget,
    template_source="local_messages_yaml",
):
    group_id = scraper.get_current_group_id()
    if not group_id:
        log.error("未能识别当前小组 ID，跳过该小组。")
        return {"messages_sent": 0, "reason": "group_id_not_found"}

    newcomers_search_rounds = task_cfg.get("newcomers_search_rounds", 40)
    newcomers_found = scraper.focus_newcomers_section(
        max_scroll_rounds=newcomers_search_rounds
    )
    if not newcomers_found:
        return {"messages_sent": 0, "reason": "newcomers_section_not_found"}
    cursor = db.get_cursor(group_id, account_id=account_id, scope_id=scope_id)
    max_scroll_rounds = task_cfg.get("scroll_times", 60)
    idle_scroll_limit = task_cfg.get("idle_scroll_limit", 3)
    cursor_search_rounds = task_cfg.get("resume_search_times", 20)

    if cursor:
        cursor_target = describe_cursor_target(cursor)
        log.info(
            f"🧠 检测到小组断点: {cursor_target}，优先从旧位置继续..."
        )
        result = process_member_targets(
            scraper=scraper,
            sender=sender,
            db=db,
            config=config,
            account_id=account_id,
            scope_id=scope_id,
            machine_id=machine_id,
            group_id=group_id,
            owner_username=owner_username,
            probe_templates=probe_templates,
            templates=templates,
            template_source=template_source,
            task_cfg=task_cfg,
            max_scroll_rounds=max_scroll_rounds,
            idle_scroll_limit=idle_scroll_limit,
            cursor_search_rounds=cursor_search_rounds,
            message_budget=message_budget,
            cursor=cursor,
            skip_until_cursor=True,
        )
        if result["reason"] == "cursor_not_found_fallback":
            log.warning(
                f"🪃 未找到断点用户 {cursor_target}，配置允许回退，重置到新人区域顶部后重新全量扫描。"
            )
            scraper.scroll_to_top()
            newcomers_found = scraper.focus_newcomers_section(
                max_scroll_rounds=newcomers_search_rounds
            )
            if not newcomers_found:
                log.error("断点回退时未能重新定位到小组新人区块，终止当前小组。")
                return {
                    "messages_sent": result["messages_sent"],
                    "reason": "newcomers_section_not_found",
                }
            return process_member_targets(
                scraper=scraper,
                sender=sender,
                db=db,
                config=config,
                account_id=account_id,
                scope_id=scope_id,
                machine_id=machine_id,
                group_id=group_id,
                owner_username=owner_username,
                probe_templates=probe_templates,
                templates=templates,
                template_source=template_source,
                task_cfg=task_cfg,
                max_scroll_rounds=max_scroll_rounds,
                idle_scroll_limit=idle_scroll_limit,
                cursor_search_rounds=cursor_search_rounds,
                message_budget=message_budget,
                cursor=None,
                skip_until_cursor=False,
            )
        if result["reason"] == "cursor_not_found_required":
            log.error(
                f"⛔ 断点用户 {cursor_target} 未找到，配置要求必须命中断点，终止当前小组。"
            )
        return result

    log.info("🆕 当前账号在当前小组没有历史断点，将从“小组新人”列表第一位开始。")
    return process_member_targets(
        scraper=scraper,
        sender=sender,
        db=db,
        config=config,
        account_id=account_id,
        scope_id=scope_id,
        machine_id=machine_id,
        group_id=group_id,
        owner_username=owner_username,
        probe_templates=probe_templates,
        templates=templates,
        template_source=template_source,
        task_cfg=task_cfg,
        max_scroll_rounds=max_scroll_rounds,
        idle_scroll_limit=idle_scroll_limit,
        cursor_search_rounds=cursor_search_rounds,
        message_budget=message_budget,
        cursor=None,
        skip_until_cursor=False,
    )


def process_member_targets(
    scraper,
    sender,
    db,
    config,
    account_id,
    scope_id,
    machine_id,
    group_id,
    owner_username,
    probe_templates,
    templates,
    task_cfg,
    max_scroll_rounds,
    idle_scroll_limit,
    cursor_search_rounds,
    message_budget,
    cursor=None,
    skip_until_cursor=False,
    template_source="local_messages_yaml",
):
    seen_profiles = set()
    found_cursor = not skip_until_cursor
    messages_sent = 0
    idle_rounds = 0
    cursor_rounds = 0
    cursor_skip_count = 0
    cursor_require_found = bool(task_cfg.get("cursor_require_found", False))
    no_scroll_rounds = 0
    list_unchanged_rounds = 0
    last_list_fingerprint = None
    confirm_exhausted = False
    last_anchor_was_tail = False
    allow_anchor_reseed = False
    blocked_targets = []
    blocked_target_keys = set()
    silent_targets = []
    silent_target_keys = set()
    restricted_blocked_threshold = int(
        task_cfg.get("account_restricted_blocked_threshold", 2) or 2
    )
    restricted_signal_threshold = int(
        task_cfg.get("account_restricted_signal_threshold", 3) or 3
    )

    round_idx = 0
    unlimited_scroll = not max_scroll_rounds or int(max_scroll_rounds) <= 0
    last_anchor = None
    anchor_attempts = 0
    while unlimited_scroll or round_idx <= int(max_scroll_rounds):
        if should_stop(task_cfg):
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "stopped",
            }
        cont, _ = wait_if_paused(
            task_cfg,
            db=db,
            group_id=group_id,
            account_id=account_id,
            scope_id=scope_id,
            anchor=last_anchor,
        )
        if not cont:
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "stopped",
            }
        if owner_username and db.is_account_disabled(owner_username):
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "account_disabled",
            }

        anchor_required = last_anchor is not None
        capture = scraper.collect_visible_new_member_targets(
            anchor=last_anchor if anchor_required else None,
            require_after_anchor=anchor_required,
        )
        targets = list((capture or {}).get("targets") or [])
        fallback_targets = list((capture or {}).get("fallback_targets") or [])
        anchor_found = bool((capture or {}).get("anchor_found", True))
        scope_status = scraper.get_last_scope_status()
        member_list_snapshot = scraper.get_last_member_list_snapshot()
        if not scope_status.get("confirmed"):
            reason = str(scope_status.get("reason") or "newcomers_scope_unconfirmed").strip()
            label = str(scope_status.get("label") or "小组新人").strip()
            log.error(
                f"⛔ 当前视图无法确认目标仍来自 {label} 区块，停止当前小组发送: {reason}"
            )
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "newcomers_scope_unconfirmed",
            }

        if anchor_required and not anchor_found and fallback_targets and allow_anchor_reseed:
            log.info("尾部锚点已离开当前视图，直接从当前新视图顶部继续顺序采集。")
            targets = fallback_targets
            anchor_found = True
            allow_anchor_reseed = False
        if (
            anchor_required
            and not anchor_found
            and fallback_targets
            and not skip_until_cursor
            and int(member_list_snapshot.get("visible_count") or 0) > 0
        ):
            log.info("锚点因列表刷新离开当前视图，直接从当前视图顶部重新衔接顺序采集。")
            targets = fallback_targets
            anchor_found = True
        if anchor_required and not anchor_found:
            anchor_attempts += 1
            if anchor_attempts == 1:
                log.warning("未在当前视图找到断点锚点，继续滚动搜索锚点。")
            targets = []
            anchor_found = False
        else:
            anchor_attempts = 0
        target = None
        for candidate in targets:
            profile_url = candidate.get("profile_url")
            if not profile_url:
                continue
            if profile_url in seen_profiles:
                last_anchor = candidate
                continue
            target = candidate
            break

        if not target:
            idle_rounds += 1
            current_fingerprint = (
                member_list_snapshot.get("fingerprint")
                or fingerprint_targets(targets)
            )
            anchor_at_tail = bool(member_list_snapshot.get("anchor_at_tail"))
            anchor_exhausted_hint = bool(anchor_at_tail or last_anchor_was_tail)
            if current_fingerprint and current_fingerprint == last_list_fingerprint:
                list_unchanged_rounds += 1
            else:
                list_unchanged_rounds = 0
            last_list_fingerprint = current_fingerprint
            log.info(
                f"当前视图未发现可用成员，继续滚动 (轮次: {round_idx + 1}"
                f"{'' if unlimited_scroll else '/' + str(max_scroll_rounds)})..."
            )
            moved = scraper.scroll_member_list()
            if not moved:
                no_scroll_rounds += 1
            else:
                no_scroll_rounds = 0
            allow_anchor_reseed = bool(moved and last_anchor_was_tail)

            immediate_exhausted = (
                anchor_required
                and anchor_exhausted_hint
                and not moved
                and list_unchanged_rounds >= 1
                and int(member_list_snapshot.get("visible_count") or 0) > 0
            )
            if immediate_exhausted:
                if skip_until_cursor and not found_cursor:
                    cursor_target = describe_cursor_target(cursor)
                    if cursor_require_found:
                        log.error(
                            f"⛔ 断点搜索结束，未找到断点用户 {cursor_target}，且 cursor_require_found=true。"
                        )
                        return build_cursor_search_result(
                            "cursor_not_found_required",
                            found_cursor,
                            messages_sent,
                            cursor,
                        )
                    log.warning(
                        f"🪃 断点搜索结束，未找到断点用户 {cursor_target}，允许回退为当前小组全量重扫。"
                    )
                    return build_cursor_search_result(
                        "cursor_not_found_fallback",
                        found_cursor,
                        messages_sent,
                        cursor,
                    )
                return {
                    "found_cursor": found_cursor,
                    "messages_sent": messages_sent,
                    "reason": "group_exhausted",
                }

            should_confirm_exhausted = (
                idle_rounds >= max(idle_scroll_limit, 4)
                and no_scroll_rounds >= 2
                and list_unchanged_rounds >= 2
            )
            if (
                anchor_required
                and anchor_exhausted_hint
                and not moved
                and list_unchanged_rounds >= 1
            ):
                should_confirm_exhausted = True

            if should_confirm_exhausted and not confirm_exhausted:
                confirm_exhausted = True
                log.info("疑似已到达列表底部，执行一次确认滚动...")
                confirm_moved = scraper.scroll_member_list()
                allow_anchor_reseed = bool(confirm_moved and last_anchor_was_tail)
                continue

            if should_confirm_exhausted and confirm_exhausted:
                if skip_until_cursor and not found_cursor:
                    cursor_target = describe_cursor_target(cursor)
                    if cursor_require_found:
                        log.error(
                            f"⛔ 断点搜索结束，未找到断点用户 {cursor_target}，且 cursor_require_found=true。"
                        )
                        return build_cursor_search_result(
                            "cursor_not_found_required",
                            found_cursor,
                            messages_sent,
                            cursor,
                        )
                    log.warning(
                        f"🪃 断点搜索结束，未找到断点用户 {cursor_target}，允许回退为当前小组全量重扫。"
                    )
                    return build_cursor_search_result(
                        "cursor_not_found_fallback",
                        found_cursor,
                        messages_sent,
                        cursor,
                    )
                return {
                    "found_cursor": found_cursor,
                    "messages_sent": messages_sent,
                    "reason": "group_exhausted",
                }
            round_idx += 1
            if skip_until_cursor and not found_cursor:
                cursor_rounds += 1
                if cursor_rounds >= cursor_search_rounds and cursor_rounds % cursor_search_rounds == 0:
                    cursor_target = describe_cursor_target(cursor)
                    if cursor_require_found:
                        log.error(
                            f"⛔ 未找到断点用户 {cursor_target}，已连续滚动 {cursor_rounds} 轮，配置要求必须命中断点。"
                        )
                        return build_cursor_search_result(
                            "cursor_not_found_required",
                            found_cursor,
                            messages_sent,
                            cursor,
                            searched_rounds=cursor_rounds,
                        )
                    log.warning(
                        f"🪃 未找到断点用户 {cursor_target}，已连续滚动 {cursor_rounds} 轮，回退为当前小组全量重扫。"
                    )
                    return build_cursor_search_result(
                        "cursor_not_found_fallback",
                        found_cursor,
                        messages_sent,
                        cursor,
                        searched_rounds=cursor_rounds,
                    )
            else:
                cursor_rounds = 0
            continue

        idle_rounds = 0
        no_scroll_rounds = 0
        list_unchanged_rounds = 0
        confirm_exhausted = False
        cursor_rounds = 0
        allow_anchor_reseed = False
        profile_url = target["profile_url"]
        if target.get("source_scope") != "newcomers":
            log.error(f"⛔ 目标来源不属于小组新人区块，拒绝发送: {target.get('name')}")
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "newcomers_scope_unconfirmed",
            }
        seen_profiles.add(profile_url)
        last_anchor = target
        last_anchor_was_tail = bool(target.get("is_tail"))
        log.info(f"➡️ 正在处理成员卡片: {target.get('name')}")
        cont, _ = wait_if_paused(
            task_cfg,
            db=db,
            group_id=group_id,
            account_id=account_id,
            scope_id=scope_id,
            anchor=last_anchor,
        )
        if not cont:
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "stopped",
            }

        if skip_until_cursor and not found_cursor:
            cursor_skip_count += 1
            if target_matches_cursor(target, cursor):
                found_cursor = True
                cursor_skip_count = 0
                log.info(f"🎯 已找到断点用户 {target['name']}，从下一位继续发送。")
                continue
            if cursor_skip_count % 50 == 0:
                log.info(
                    f"未找到断点用户，已连续扫描 {cursor_skip_count} 人，继续搜索断点..."
                )
            continue

        if db.is_sent(
            group_id=group_id,
            account_id=account_id,
            profile_url=profile_url,
            user_id=target.get("user_id"),
            scope_id=scope_id,
        ):
            log.info(f"已发过，跳过: {target['name']} ({profile_url})")
            db.save_cursor(group_id, target, account_id=account_id, scope_id=scope_id)
            ui_settle_sleep(task_cfg)
            continue

        if not db.try_claim_target(
            group_id=group_id,
            profile_url=profile_url,
            account_id=account_id,
            user_id=target.get("user_id"),
            scope_id=scope_id,
        ):
            log.info(f"其他窗口正在处理，跳过: {target['name']} ({profile_url})")
            db.save_cursor(group_id, target, account_id=account_id, scope_id=scope_id)
            ui_settle_sleep(task_cfg)
            continue

        template_index = random.randrange(len(templates))
        selected_template = templates[template_index]
        greeting_text = render_message(selected_template, target["name"])
        log.info(
            f"📝 文案选取: source={template_source} "
            f"index={template_index + 1}/{len(templates)} preview={_template_log_preview(selected_template)}"
        )
        probe_text = ""
        result = None
        try:
            result = sender.send_member_card_dm(
                target,
                probe_text,
                greeting_text,
            )
        finally:
            if not result or result.get("status") != "sent":
                db.release_claim(
                    group_id=group_id,
                    account_id=account_id,
                    profile_url=profile_url,
                    user_id=target.get("user_id"),
                    scope_id=scope_id,
                )

        if result["status"] == "sent":
            _record_delivery_result(
                db=db,
                config=config,
                group_id=group_id,
                target=target,
                account_id=account_id,
                machine_id=machine_id,
                success=True,
                scope_id=scope_id,
            )
            messages_sent += 1
            db.save_cursor(group_id, target, account_id=account_id, scope_id=scope_id)
            if reached_message_limit(message_budget, messages_sent):
                return {
                    "found_cursor": found_cursor,
                    "messages_sent": messages_sent,
                    "reason": "message_limit",
                }
            min_wait, max_wait, interval_meta = get_send_interval_detail(task_cfg)
            wait_seconds = random.randint(min_wait, max_wait)
            log.info(
                f"⏱ 发送频率读取: source={interval_meta.get('source')} "
                f"value=[{min_wait}, {max_wait}] next_wait={wait_seconds}"
            )
            log.info(f"任务间歇休息 {wait_seconds} 秒后继续下一位...")
            if not interruptible_sleep(task_cfg, wait_seconds):
                return {
                    "found_cursor": found_cursor,
                    "messages_sent": messages_sent,
                    "reason": "stopped",
                }
            blocked_targets.clear()
            blocked_target_keys.clear()
            silent_targets.clear()
            silent_target_keys.clear()
        elif result["status"] == "skip":
            log.info(f"跳过成员 {target['name']}: {result['reason']}")
            if should_advance_cursor_for_skip(result.get("reason")):
                db.save_cursor(group_id, target, account_id=account_id, scope_id=scope_id)
                log.info(f"断点推进：成员 {target['name']} 属于永久跳过，已更新 cursor。")
            else:
                log.warning(f"断点保留：成员 {target['name']} 属于暂时性失败，当前不推进 cursor。")
            ui_settle_sleep(task_cfg)
        elif result["status"] == "blocked":
            _record_delivery_result(
                db=db,
                config=config,
                group_id=group_id,
                target=target,
                account_id=account_id,
                machine_id=machine_id,
                success=False,
                reason=result.get("reason") or "blocked",
                scope_id=scope_id,
            )
            reason = result.get("reason") or "account_restricted"
            window_block_reasons = {
                "message_request_limit_reached",
                "account_cannot_message",
                "send_restricted_ui",
                "account_restricted_by_dialog",
                "send_blocked_popup",
            }
            if reason in window_block_reasons:
                log.warning(
                    f"检测到窗口级发送限制，停止当前窗口: {target['name']} ({reason})"
                )
                return {
                    "found_cursor": found_cursor,
                    "messages_sent": messages_sent,
                    "reason": reason,
                    "alert_text": result.get("alert_text"),
                }

            block_key = target.get("user_id") or profile_url
            if block_key not in blocked_target_keys:
                blocked_target_keys.add(block_key)
                blocked_targets.append(
                    {
                        "name": target.get("name"),
                        "user_id": target.get("user_id"),
                        "profile_url": profile_url,
                    }
                )
            log.warning(
                f"检测到消息被拒绝，按单次明确失败立即停号: {target['name']}"
            )
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "account_restricted",
                "blocked_targets": blocked_targets,
                "silent_targets": silent_targets,
            }
        elif result["status"] == "limit":
            _record_delivery_result(
                db=db,
                config=config,
                group_id=group_id,
                target=target,
                account_id=account_id,
                machine_id=machine_id,
                success=False,
                reason=result.get("reason") or "stranger_message_limit_reached",
                scope_id=scope_id,
            )
            return {
                "found_cursor": found_cursor,
                "messages_sent": messages_sent,
                "reason": "stranger_message_limit_reached",
                "alert_text": result.get("alert_text"),
            }
        else:
            _record_delivery_result(
                db=db,
                config=config,
                group_id=group_id,
                target=target,
                account_id=account_id,
                machine_id=machine_id,
                success=False,
                reason=result.get("reason") or "error",
                scope_id=scope_id,
            )
            log.warning(
                f"处理成员失败，保留原断点等待下次重试: {target['name']} - {result['reason']}"
            )
            log.info(f"断点保留：成员 {target['name']} 失败类型为 {result['reason']}，当前不推进 cursor。")
            ui_settle_sleep(task_cfg)
            if result["reason"] == "delivery_not_confirmed":
                silent_key = target.get("user_id") or profile_url
                if silent_key not in silent_target_keys:
                    silent_target_keys.add(silent_key)
                    silent_targets.append(
                        {
                            "name": target.get("name"),
                            "user_id": target.get("user_id"),
                            "profile_url": profile_url,
                        }
                    )
                log.warning(
                    f"检测到静默失败信号: {target['name']} "
                    f"(signals={len(blocked_targets) + len(silent_targets)}/{restricted_signal_threshold})"
                )
                if len(blocked_targets) + len(silent_targets) >= restricted_signal_threshold:
                    return {
                        "found_cursor": found_cursor,
                        "messages_sent": messages_sent,
                        "reason": "account_restricted",
                        "blocked_targets": blocked_targets,
                        "silent_targets": silent_targets,
                    }

    if skip_until_cursor and not found_cursor:
        cursor_target = describe_cursor_target(cursor)
        if cursor_require_found:
            log.error(
                f"⛔ 扫描达到滚动上限后仍未找到断点用户 {cursor_target}，配置要求必须命中断点。"
            )
            return build_cursor_search_result(
                "cursor_not_found_required",
                found_cursor,
                messages_sent,
                cursor,
                searched_rounds=round_idx,
            )
        log.warning(
            f"🪃 扫描达到滚动上限后仍未找到断点用户 {cursor_target}，回退为当前小组全量重扫。"
        )
        return build_cursor_search_result(
            "cursor_not_found_fallback",
            found_cursor,
            messages_sent,
            cursor,
            searched_rounds=round_idx,
        )

    return {
        "found_cursor": found_cursor,
        "messages_sent": messages_sent,
        "reason": "group_exhausted" if confirm_exhausted or (last_anchor_was_tail and no_scroll_rounds >= 1) else "round_limit",
    }


def target_matches_cursor(target, cursor):
    if not cursor:
        return False
    return any(
        [
            cursor.get("profile_url") and cursor["profile_url"] == target["profile_url"],
            cursor.get("group_user_url") and cursor["group_user_url"] == target["group_user_url"],
            cursor.get("user_id") and cursor["user_id"] == target["user_id"],
        ]
    )


def fingerprint_targets(targets, limit=5):
    if not targets:
        return None
    keys = []
    for item in targets:
        if not item:
            continue
        key = item.get("user_id") or item.get("profile_url") or item.get("group_user_url")
        if key:
            keys.append(str(key))
        if len(keys) >= limit:
            break
    return "|".join(keys) if keys else None


def render_message(template, name):
    safe_name = (name or "").strip()
    try:
        return template.format(name=safe_name)
    except Exception:
        return template.replace("{name}", safe_name)


def normalize_message_limit(value):
    try:
        limit = int(value)
    except Exception:
        return None
    return None if limit <= 0 else limit


def reached_message_limit(message_budget, sent_now):
    return message_budget is not None and sent_now >= message_budget


def remaining_budget(message_budget, sent_now):
    if message_budget is None:
        return None
    return max(message_budget - sent_now, 0)


def get_send_interval_detail(task_cfg):
    raw = None
    settings_path = _client_config_path()
    try:
        mtime = settings_path.stat().st_mtime
    except Exception:
        mtime = None

    cache = getattr(get_send_interval_seconds, "_cache", {"mtime": None, "value": None})
    if mtime and cache.get("mtime") != mtime:
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                settings = yaml.safe_load(f) or {}
            raw = settings.get("task_settings", {}).get("send_interval_seconds")
            cache = {"mtime": mtime, "value": raw}
            setattr(get_send_interval_seconds, "_cache", cache)
        except Exception:
            raw = None
    elif cache.get("value") is not None:
        raw = cache.get("value")

    if raw is None:
        raw = [0, 0]
    if not isinstance(raw, (list, tuple)) or len(raw) != 2:
        raw = [0, 0]

    try:
        min_seconds = max(0, int(raw[0]))
        max_seconds = max(0, int(raw[1]))
    except Exception:
        min_seconds, max_seconds = 0, 0

    if min_seconds > max_seconds:
        min_seconds, max_seconds = max_seconds, min_seconds
    return min_seconds, max_seconds, {
        "source": "client_yaml.task_settings.send_interval_seconds",
        "path": str(settings_path),
        "raw": list(raw) if isinstance(raw, (list, tuple)) else [min_seconds, max_seconds],
    }


def get_send_interval_seconds(task_cfg):
    min_seconds, max_seconds, _ = get_send_interval_detail(task_cfg)
    return min_seconds, max_seconds


def _template_log_preview(template, limit=60):
    text = str(template or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def should_advance_cursor_for_skip(reason):
    permanent_reasons = {
        "add_friend_required",
        "follow_only",
        "friend_required",
        "invite_only",
        "profile_message_unavailable",
        "message_button_not_found",
    }
    return str(reason or "").strip() in permanent_reasons


def should_stop(task_cfg):
    stop_flag_file = (os.environ.get(ENV_STOP_FLAG) or "").strip()
    return bool(stop_flag_file and os.path.exists(stop_flag_file))

def should_pause(task_cfg):
    pause_flag_file = (os.environ.get(ENV_PAUSE_FLAG) or "").strip()
    return bool(pause_flag_file and os.path.exists(pause_flag_file))

def wait_if_paused(task_cfg, db=None, group_id=None, account_id=None, scope_id=None, anchor=None):
    if not should_pause(task_cfg):
        return True, False
    log.info("⏸ 已暂停，等待继续...")
    if db and group_id and anchor:
        try:
            db.save_cursor(group_id, anchor, account_id=account_id, scope_id=scope_id)
        except Exception:
            pass
    while should_pause(task_cfg):
        if should_stop(task_cfg):
            return False, True
        time.sleep(0.5)
    log.info("▶️ 已继续，恢复执行。")
    return True, False


def interruptible_sleep(task_cfg, seconds, step=0.2):
    deadline = time.time() + max(0, seconds)
    while time.time() < deadline:
        cont, _ = wait_if_paused(task_cfg)
        if not cont:
            return False
        if should_stop(task_cfg):
            return False
        remaining = deadline - time.time()
        time.sleep(min(step, max(0.01, remaining)))
    return True


def ui_settle_sleep(task_cfg, base_ms=60, jitter_ms=80):
    delay = (base_ms + random.randint(0, jitter_ms)) / 1000
    interruptible_sleep(task_cfg, delay, step=0.05)


def _seconds_until_next_midnight(now=None):
    now = now or datetime.now()
    next_midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return max(1, int((next_midnight - now).total_seconds()))


def _cleanup_worker_logs(keep_days, config=None):
    if keep_days <= 0:
        return
    cutoff_ts = time.time() - (keep_days * 24 * 60 * 60)
    logs_dir = _runtime_path(config, "logs")
    if logs_dir.exists():
        for path in logs_dir.glob("*.log"):
            try:
                if path.stat().st_mtime < cutoff_ts:
                    path.unlink()
            except Exception:
                continue
    data_dir = _runtime_path(config, "data")
    if data_dir.exists():
        for path in data_dir.glob("window*_run.log"):
            try:
                if path.stat().st_mtime < cutoff_ts:
                    path.unlink()
            except Exception:
                continue


def start_cleanup_daemon(config=None):
    def _runner():
        while True:
            time.sleep(_seconds_until_next_midnight())
            try:
                db = Database()
                try:
                    db.cleanup_old_history(history_days=CLEANUP_HISTORY_DAYS, claim_days=3)
                finally:
                    db.close()
                _cleanup_worker_logs(CLEANUP_LOG_DAYS, config=config)
            except Exception as exc:
                log.error(f"自动清理失败: {exc}")

    threading.Thread(target=_runner, daemon=True).start()


def show_desktop_alert(title, message, critical=False):
    try:
        if sys.platform == "darwin":
            if _try_macos_dialog_alert(title, message, critical):
                log.info("桌面提醒弹窗已显示成功（macOS 系统弹窗），等待用户确认。")
                return True
        if sys.platform.startswith("win"):
            if _try_windows_dialog_alert(title, message, critical):
                log.info("桌面提醒弹窗已显示成功（Windows 系统弹窗），等待用户确认。")
                return True
        if not ALERT_HELPER_PATH or not Path(ALERT_HELPER_PATH).exists():
            log.warning("桌面弹窗 helper 不存在，已跳过 helper 回退链路。")
            return False
        command = [
            sys.executable,
            ALERT_HELPER_PATH,
            "--title",
            title,
            "--message",
            message,
            "--critical" if critical else "--info",
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            close_fds=not sys.platform.startswith("win"),
            start_new_session=not sys.platform.startswith("win"),
        )

        time.sleep(0.8)
        if process.poll() is not None:
            _, stderr = process.communicate(timeout=1)
            error_text = (stderr or "").strip() or "提醒脚本已提前退出"
            log.warning(f"桌面弹窗未能显示: {error_text}")
            return False
        log.info("桌面提醒弹窗已显示成功（顶层提醒窗口），等待用户确认。")
        return True
    except Exception as exc:
        log.warning(f"启动桌面弹窗失败: {exc}")
        return False


def _try_macos_dialog_alert(title, message, critical):
    if sys.platform != "darwin":
        return False
    try:
        def escape_applescript(text):
            return text.replace('"', '\\"')

        parts = message.splitlines() or [message]
        if not parts:
            parts = [""]
        msg_expr = " & return & ".join([f"\"{escape_applescript(part)}\"" for part in parts])
        title_expr = escape_applescript(title or "提醒")
        icon = "stop" if critical else "note"
        script = (
            f"display dialog {msg_expr} "
            f"with title \"{title_expr}\" "
            "buttons {\"确定\"} default button 1 "
            f"with icon {icon}"
        )

        process = subprocess.Popen(
            ["osascript", "-e", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            close_fds=True,
        )
        time.sleep(0.6)
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            error_text = (stderr or "").strip()
            if error_text:
                log.warning(f"AppleScript 弹窗失败: {error_text}")
                return False
            result_text = (stdout or "").strip().lower()
            if "button returned" in result_text:
                return True
            log.warning("AppleScript 弹窗提前退出，未检测到用户确认，回退到顶层提醒窗口。")
            return False
        return True
    except Exception as exc:
        log.warning(f"AppleScript 弹窗失败: {exc}")
        return False


def _try_windows_dialog_alert(title, message, critical):
    if not sys.platform.startswith("win"):
        return False
    try:
        flags = 0x00000010 if critical else 0x00000040
        ctypes.windll.user32.MessageBoxW(0, message or "", title or "提醒", flags)
        return True
    except Exception as exc:
        log.warning(f"Windows 系统弹窗失败: {exc}")
        return False


def _window_alert_cache_key(window_token, reason):
    try:
        token = str(window_token or "").strip()
        reason_key = str(reason or "").strip()
        if not token or not reason_key:
            return ""
        return f"{token}|{reason_key}"
    except Exception:
        return ""


def _is_window_alert_throttled(window_token, reason, cooldown_seconds=ALERT_COOLDOWN_SECONDS):
    cache_key = _window_alert_cache_key(window_token, reason)
    if not cache_key:
        return False
    last_ts = _WINDOW_ALERT_CACHE.get(cache_key)
    return bool(last_ts and time.time() - last_ts < cooldown_seconds)


def _mark_window_alert_shown(window_token, reason):
    cache_key = _window_alert_cache_key(window_token, reason)
    if not cache_key:
        return
    _WINDOW_ALERT_CACHE[cache_key] = time.time()


def show_window_limit_alert(window_token, reason, detail_text=None):
    token_text = str(window_token or "").strip()
    reason_text = str(reason or "").strip()
    if _is_window_alert_throttled(token_text, reason_text):
        log.warning(
            f"发送限制提醒已节流，跳过重复弹窗: window={token_text or 'unknown'} "
            f"reason={reason_text or 'unknown'}"
        )
        return False

    display_token = token_text or "unknown"
    detail = (detail_text or "").strip()
    lines = [
        f"窗口: {display_token}",
        f"原因: {reason_text or 'unknown'}",
    ]
    if detail:
        lines.append(f"提示: {detail}")
    message = "\n".join(lines)
    log.error(message.replace("\n", " | "))
    shown = show_desktop_alert("发送限制提醒", message, critical=True)
    if shown:
        _mark_window_alert_shown(token_text, reason_text)
    else:
        log.warning(
            f"发送限制提醒弹窗显示失败，未写入节流缓存，将允许下次继续重试: "
            f"window={token_text or 'unknown'} reason={reason_text or 'unknown'}"
        )
    return shown


def show_account_restricted_alert(account_id, blocked_targets=None, silent_targets=None):
    window_label = format_window_notice_label(account_id)
    message = f"温馨提示：{window_label}Facebook账号疑似风控，请更换新的账户。"
    log.error(message.replace("\n", " | "))
    show_desktop_alert("温馨提示", message, critical=True)


def format_window_notice_label(account_id):
    text = str(account_id or "").strip()
    match = re.search(r"窗口\s*(\d+)", text)
    if match:
        return f"{match.group(1)}号窗口"
    return f"{text} " if text else ""

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-token", dest="window_token", default="")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_task(window_token=args.window_token)
