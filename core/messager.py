import os
import random
import time
from pathlib import Path

from utils.logger import log


class Messager:
    ENV_PAUSE_FLAG = "FB_RPA_PAUSE_FLAG"
    CHAT_CLOSE_SELECTORS = [
        'div[aria-label="关闭聊天窗口"]',
        'div[aria-label="Close chat window"]',
        'div[aria-label="Close chat"]',
    ]
    MEMBER_CARD_CLOSE_SELECTORS = [
        'button[aria-label="关闭个人资料"]',
        'button[aria-label="Close profile"]',
        'div[role="button"][aria-label="关闭个人资料"]',
        'div[role="button"][aria-label="Close profile"]',
        'button[aria-label="关闭"]',
        'button[aria-label="Close"]',
        'div[role="button"][aria-label="关闭"]',
        'div[role="button"][aria-label="Close"]',
        'div[aria-label="关闭"]',
        'div[aria-label="Close"]',
        'span[aria-label="关闭"]',
        'span[aria-label="Close"]',
    ]
    MEMBER_CARD_MESSAGE_SELECTORS = [
        'div[aria-label="发消息"]',
        'div[aria-label="发送消息"]',
        'div[aria-label="發送訊息"]',
        'div[aria-label="發消息"]',
        'div[aria-label="Message"]',
        'div[role="button"]:has-text("发消息")',
        'div[role="button"]:has-text("发送消息")',
        'div[role="button"]:has-text("發送訊息")',
        'div[role="button"]:has-text("發消息")',
        'div[role="button"]:has-text("Message")',
    ]
    MEMBER_CARD_MESSAGE_TEXTS = [
        "发消息",
        "发送消息",
        "發送訊息",
        "發消息",
        "Message",
        "Send message",
    ]
    MESSAGE_EDITOR_SELECTORS = [
        'div[role="textbox"][aria-label="发消息"]',
        'div[role="textbox"][aria-label="发送消息"]',
        'div[role="textbox"][aria-label="發送訊息"]',
        'div[role="textbox"][aria-label="發消息"]',
        'div[role="textbox"][aria-label="Message"]',
        'div[role="textbox"][aria-label="Send a message"]',
        'div[role="textbox"][aria-label="信息"]',
        'div[role="textbox"][aria-label="Aa"]',
        'div[role="textbox"][aria-label="Aa..."]',
        'div[contenteditable="true"][role="textbox"]',
    ]
    DELIVERY_STATUS_TEXTS = ["已发送", "已傳送", "Sent"]
    DELIVERY_FAILURE_TEXTS = [
        "无法发送",
        "無法傳送",
        "Couldn't send",
        "Could not send",
        "This message can't be sent",
        "This message can’t be sent",
        "This message cannot be sent",
        "发送失败",
    ]
    MESSAGE_REQUEST_LIMIT_TEXTS = [
        "You've reached the message request limit",
        "You’ve reached the message request limit",
        "There's a limit to how many requests you can send in 24 hours",
        "There’s a limit to how many requests you can send in 24 hours",
    ]
    ACCOUNT_CANNOT_MESSAGE_TEXTS = [
        "You can't message this account right now",
        "You can’t message this account right now",
        "当前无法向此用户发送消息",
        "当前无法向此帳號发送消息",
        "当前无法向此账号发送消息",
    ]
    SEND_RESTRICTED_TEXTS = [
        "当前无法发送",
        "无法继续发送",
        "发送被限制",
        "发送被系統阻斷",
        "发送被系统阻断",
        "不能继续发送",
        "You can't send messages right now",
        "You can’t send messages right now",
        "Sending is restricted",
        "Message sending is restricted",
    ]
    STRANGER_LIMIT_TEXTS = [
        "你的陌生消息已达数量上限",
        "你的陌生消息已達數量上限",
        "陌生消息已达数量上限",
        "陌生消息已達數量上限",
        "你的陌生訊息已達數量上限",
        "24 小时内可发送的陌生消息数量有上限",
        "24小时内可发送的陌生消息数量有上限",
        "24小时内可发送的陌生消息数量有上限。",
        "24 小时内可发送的陌生消息数量有上限。",
        "24 小時內可傳送的陌生訊息數量有上限",
        "24小時內可傳送的陌生訊息數量有上限",
        "24 小時內可傳送的陌生訊息數量有上限。",
    ]
    STRANGER_LIMIT_ALERT_TEXT = "你的陌生消息已达数量上限\n24 小时内可发送的陌生消息数量有上限。"
    MESSAGE_REQUEST_LIMIT_ALERT_TEXT = (
        "你已达到消息请求发送上限\n24 小时内可发起的新对话数量有限，请稍后再试。"
    )
    LIKE_BUTTON_SELECTORS = [
        'div[aria-label="发个赞"]',
        'div[aria-label="發個讚"]',
        'div[aria-label="Like"]',
        'div[aria-label="Send a like"]',
    ]
    ADD_FRIEND_TEXTS = [
        "Add friend",
        "Add Friend",
        "添加好友",
        "加好友",
        "加為好友",
        "加为好友",
    ]
    FOLLOW_ONLY_TEXTS = [
        "Follow",
        "关注",
        "關注",
        "追蹤",
        "追踪",
    ]
    CONNECT_TEXTS = [
        "Connect",
        "连接",
        "連接",
        "建立关系",
        "建立關係",
    ]
    PROFILE_ACTION_TEXTS = ADD_FRIEND_TEXTS + FOLLOW_ONLY_TEXTS + CONNECT_TEXTS

    def __init__(self, page, task_cfg=None, window_label=None):
        self.page = page
        self.window_label = (window_label or "").strip()
        task_cfg = task_cfg or {}
        self._pause_flag_path = self._resolve_pause_flag(task_cfg)
        try:
            speed_factor = float(task_cfg.get("speed_factor", 1.0) or 1.0)
        except Exception:
            speed_factor = 1.0
        self.speed_factor = max(0.35, min(1.5, speed_factor))
        self.fast_message_button_only = bool(task_cfg.get("fast_message_button_only", True))
        self.force_hover_card = bool(task_cfg.get("force_hover_card", False))
        self.fast_action_timeout_ms = int(task_cfg.get("fast_action_timeout_ms", 600) or 600)
        self.fast_visible_timeout_ms = int(task_cfg.get("fast_visible_timeout_ms", 80) or 80)
        self.fast_hover_sleep_ms = int(task_cfg.get("fast_hover_sleep_ms", 80) or 80)
        self.fast_retry_sleep_ms = int(task_cfg.get("fast_retry_sleep_ms", 120) or 120)
        self.quick_no_button_hover_timeout_ms = int(
            task_cfg.get("quick_no_button_hover_timeout_ms", 700) or 700
        )
        self.quick_no_button_confirm_ms = int(
            task_cfg.get("quick_no_button_confirm_ms", 200) or 200
        )
        self.quick_no_button_precheck = bool(task_cfg.get("quick_no_button_precheck", False))
        self.stranger_limit_cache_ms = int(task_cfg.get("stranger_limit_cache_ms", 1200) or 1200)
        self._last_limit_check_ts = 0.0
        self._last_limit_check_result = False
        self.delivery_confirm_timeout_ms = int(
            task_cfg.get("delivery_confirm_timeout_ms", 5000) or 5000
        )
        self.like_probe_timeout_ms = int(
            task_cfg.get("like_probe_timeout_ms", 4000) or 4000
        )
        self.delivery_settle_ms = int(
            task_cfg.get("delivery_settle_ms", 2200) or 2200
        )
        self.success_close_delay_ms = int(
            task_cfg.get("success_close_delay_ms", 200) or 200
        )
        self.default_close_delay_ms = int(
            task_cfg.get("default_close_delay_ms", 800) or 800
        )
        self.hover_card_wait_timeout_ms = int(
            task_cfg.get("hover_card_wait_timeout_ms", 8000) or 8000
        )
        self.message_button_wait_timeout_ms = int(
            task_cfg.get("message_button_wait_timeout_ms", 10000) or 10000
        )
        self.chat_session_wait_timeout_ms = int(
            task_cfg.get("chat_session_wait_timeout_ms", 12000) or 12000
        )
        self.editor_ready_wait_timeout_ms = int(
            task_cfg.get("editor_ready_wait_timeout_ms", 8000) or 8000
        )

    def _prefix(self):
        return f"[{self.window_label}] " if self.window_label else ""

    def _wait_for_condition(self, step_name, detail, checker, timeout_ms, interval=0.2):
        timeout_ms = max(300, int(timeout_ms or 300))
        interval = max(0.05, float(interval or 0.05))
        start = time.time()
        log.info(f"{self._prefix()}{step_name}，{detail}...")
        last_error = None
        while (time.time() - start) * 1000 < timeout_ms:
            self._wait_if_paused()
            try:
                result = checker()
            except Exception as exc:
                result = None
                last_error = exc
            if result:
                waited = time.time() - start
                log.info(f"{self._prefix()}{step_name}等待成功，耗时 {waited:.1f} 秒。")
                return result
            self._sleep(interval, min_seconds=0.02)
        waited = time.time() - start
        suffix = f": {last_error}" if last_error else ""
        log.warning(f"{self._prefix()}{step_name}等待超时，已等待 {waited:.1f} 秒，{detail}失败{suffix}")
        return None

    def _resolve_pause_flag(self, task_cfg):
        pause_flag = (os.environ.get(self.ENV_PAUSE_FLAG) or "").strip()
        if pause_flag:
            return Path(pause_flag)
        owner = (os.environ.get("FB_RPA_OWNER") or "").strip()
        runtime_root = (os.environ.get("FB_RPA_RUNTIME_DIR") or "").strip()
        if not runtime_root:
            runtime_root = (task_cfg.get("runtime_dir") or "").strip()
        base = Path(runtime_root).expanduser() if runtime_root else Path(os.getcwd()) / "runtime"
        base.mkdir(parents=True, exist_ok=True)
        safe_owner = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in owner) if owner else "default"
        return base / "data" / f"pause_{safe_owner}.flag"

    def _is_paused(self):
        try:
            return bool(self._pause_flag_path and self._pause_flag_path.exists())
        except Exception:
            return False

    def _wait_if_paused(self):
        if not self._is_paused():
            return False
        log.info("⏸ 已暂停，等待继续...")
        while self._is_paused():
            time.sleep(0.3)
        log.info("▶️ 已继续，恢复执行。")
        return True

    def _scale_ms(self, value, min_ms=80):
        try:
            scaled = int(float(value) * self.speed_factor)
        except Exception:
            scaled = int(value)
        return max(min_ms, scaled)

    def _sleep(self, seconds, min_seconds=0.03):
        try:
            scaled = float(seconds) * self.speed_factor
        except Exception:
            scaled = float(seconds)
        time.sleep(max(min_seconds, scaled))

    def _visible_timeout(self, default_ms=200, min_ms=80):
        if self.fast_message_button_only:
            return self._scale_ms(self.fast_visible_timeout_ms, min_ms=min_ms)
        return self._scale_ms(default_ms, min_ms=min_ms)

    def send_member_card_dm(self, target, probe_text, greeting_text, delay_range):
        name = target.get("name") or ""
        user_id = target.get("user_id") or ""
        use_fast = self.fast_message_button_only and not self.force_hover_card
        try:
            self._wait_if_paused()
            log.info(f"💬 正在处理成员卡片: {name} ({user_id})")
            self._dismiss_member_hover_card()
            if not use_fast:
                if self._check_stranger_limit_global():
                    log.warning(f"检测到陌生消息数量已达上限: {name}")
                    return {
                        "status": "limit",
                        "reason": "stranger_message_limit_reached",
                        "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    }

            link = self._find_member_link(target)
            if not link:
                log.warning(f"未找到成员列表中的目标链接，跳过: {name}")
                return {"status": "skip", "reason": "member_link_not_found"}
            self._wait_if_paused()

            action_timeout = self._scale_ms(
                self.fast_action_timeout_ms if use_fast else 2000,
                min_ms=900 if use_fast else 1500,
            )
            if not use_fast:
                try:
                    box = link.bounding_box()
                except Exception:
                    box = None
                if box:
                    try:
                        viewport_h = self.page.evaluate("window.innerHeight")
                        viewport_w = self.page.evaluate("window.innerWidth")
                    except Exception:
                        viewport_h = None
                        viewport_w = None
                    in_view = True
                    if viewport_h is not None:
                        in_view = in_view and box["y"] >= 0 and (box["y"] + box["height"]) <= viewport_h
                    if viewport_w is not None:
                        in_view = in_view and box["x"] >= 0 and (box["x"] + box["width"]) <= viewport_w
                    if not in_view:
                        link.scroll_into_view_if_needed(timeout=action_timeout)
                        self._sleep(0.2)
                else:
                    link.scroll_into_view_if_needed(timeout=action_timeout)
                    self._sleep(0.2)
                self._wait_if_paused()
            if use_fast:
                button = self._wait_for_fast_message_entry(
                    link,
                    name=name,
                    timeout_ms=self.message_button_wait_timeout_ms,
                )
                if button is None:
                    gate_reason = self._detect_profile_message_gate(link)
                    if gate_reason:
                        log.info(f"{self._prefix()}成员资料卡限制，跳过: {name} ({gate_reason})")
                        self._fast_close_hover_card()
                        return {"status": "skip", "reason": gate_reason}
                    log.info(f"{self._prefix()}等待“发消息”按钮超时，跳过当前成员: {name}")
                    self._fast_close_hover_card()
                    return {"status": "skip", "reason": "message_button_timeout"}
            else:
                if not self.force_hover_card and self.quick_no_button_precheck:
                    quick_state = self._fast_try_click_message_button(link, click=False)
                    if quick_state == "no_button":
                        gate_reason = self._detect_profile_message_gate(link)
                        if gate_reason:
                            log.info(f"{self._prefix()}成员资料卡限制，跳过: {name} ({gate_reason})")
                            self._fast_close_hover_card()
                            return {"status": "skip", "reason": gate_reason}
                        log.info(f"{self._prefix()}当前成员卡片没有可用的发消息按钮，跳过: {name}")
                        self._fast_close_hover_card()
                        return {"status": "skip", "reason": "message_button_not_found"}
                try:
                    link.hover(timeout=action_timeout)
                except Exception:
                    try:
                        link.hover(timeout=max(action_timeout, 1200))
                    except Exception:
                        log.info(f"{self._prefix()}成员卡片无法稳定悬停，跳过: {name}")
                        return {"status": "skip", "reason": "member_hover_timeout"}
                hover_card = self._wait_for_hover_card(link, timeout_ms=self.hover_card_wait_timeout_ms)
                if not hover_card:
                    log.info(f"{self._prefix()}等待成员资料卡超时，跳过: {name}")
                    self._dismiss_member_hover_card()
                    return {"status": "skip", "reason": "member_hover_timeout"}
                button = self._wait_for_member_card_message_button(
                    link,
                    hover_card=hover_card,
                    timeout_ms=self.message_button_wait_timeout_ms,
                )
                if not button:
                    gate_reason = self._detect_profile_message_gate(link, hover_card=hover_card)
                    if gate_reason:
                        log.info(f"{self._prefix()}成员资料卡限制，跳过: {name} ({gate_reason})")
                        self._fast_close_hover_card()
                        return {"status": "skip", "reason": gate_reason}
                    if hover_card:
                        log.info(f"{self._prefix()}等待“发消息”按钮超时，跳过当前成员: {name}")
                        self._fast_close_hover_card()
                        return {"status": "skip", "reason": "message_button_timeout"}
                    log.info(f"{self._prefix()}当前成员卡片没有可用的发消息按钮，跳过: {name}")
                    self._fast_close_hover_card()
                    return {"status": "skip", "reason": "message_button_not_found"}

            # 只有在确定要点击发消息按钮时才关闭聊天窗口，避免无按钮时浪费时间
            # 只有在确定要点击发消息按钮时才关闭聊天窗口，避免无按钮时浪费时间
            self._wait_if_paused()
            self._close_visible_chat_windows()

            if button is not True:
                try:
                    button.click(timeout=action_timeout)
                except Exception:
                    try:
                        button.evaluate("el => el.click()")
                    except Exception:
                        log.warning(f"发消息按钮点击失败，跳过: {name}")
                        self._dismiss_member_hover_card()
                        return {"status": "skip", "reason": "message_button_click_failed"}
            self._wait_if_paused()

            chat_session = self._wait_for_chat_session(
                expected_name=name,
                timeout_ms=self.chat_session_wait_timeout_ms,
            )
            if not chat_session:
                restriction = self._detect_chat_restriction(expected_name=name)
                if restriction:
                    log.warning(f"聊天窗口限制提示命中: {name} ({restriction['reason']})")
                    return restriction
                if self._check_stranger_limit_global():
                    log.warning(f"未找到输入框但检测到陌生消息上限: {name}")
                    return {
                        "status": "limit",
                        "reason": "stranger_message_limit_reached",
                        "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    }
                self._close_visible_chat_windows()
                log.warning(f"{self._prefix()}聊天窗口未正常打开，跳过: {name}")
                return {"status": "skip", "reason": "chat_editor_not_found"}

            if not self._wait_for_editor_ready(
                chat_session["editor"],
                timeout_ms=self.editor_ready_wait_timeout_ms,
            ):
                log.warning(f"{self._prefix()}输入框未准备就绪，跳过: {name}")
                return {"status": "skip", "reason": "chat_editor_not_ready"}

            if self._check_stranger_limit_global():
                log.warning(f"进入聊天后检测到陌生消息数量已达上限: {name}")
                return {
                    "status": "limit",
                    "reason": "stranger_message_limit_reached",
                    "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                }
            restriction = self._detect_chat_restriction(expected_name=name)
            if restriction:
                log.warning(f"聊天窗口限制提示命中: {name} ({restriction['reason']})")
                return restriction

            delivery_state = "timeout"
            try:
                primary_text = self._normalize_text(greeting_text) or self._normalize_text(probe_text)
                if not primary_text:
                    return {"status": "error", "reason": "empty_message"}

                delivery_state = self._type_and_send(
                    chat_session["editor"],
                    primary_text,
                    delay_range,
                )

                if delivery_state == "limit_reached":
                    log.warning(f"聊天窗提示陌生消息数量已达上限: {name}")
                    return {
                        "status": "limit",
                        "reason": "stranger_message_limit_reached",
                        "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    }

                if delivery_state == "message_request_limit_reached":
                    log.warning(f"聊天窗提示消息请求发送达到上限: {name}")
                    return {
                        "status": "blocked",
                        "reason": "message_request_limit_reached",
                        "alert_text": self.MESSAGE_REQUEST_LIMIT_ALERT_TEXT,
                    }

                if delivery_state == "account_cannot_message":
                    log.warning(f"聊天窗明确提示当前账号无法继续联系用户: {name}")
                    return {
                        "status": "blocked",
                        "reason": "account_cannot_message",
                        "alert_text": "当前账号已被限制继续向该类用户发起消息。",
                    }

                if delivery_state in ("failed", "send_restricted_ui"):
                    log.warning(f"聊天窗明确提示无法发送: {name}")
                    return {
                        "status": "blocked",
                        "reason": "send_restricted_ui",
                        "alert_text": "当前窗口已被系统限制继续发送消息，请更换账户。",
                    }

                if delivery_state == "sent":
                    log.success(f"✅ 成员页消息已发送: {name}")
                    return {"status": "sent"}

                if delivery_state == "timeout":
                    log.warning(f"发送消息未拿到明确状态，尝试发送点赞探针: {name}")
                    delivery_state = self._send_like_probe(chat_session["editor"])

                if delivery_state == "limit_reached":
                    log.warning(f"点赞探针后确认陌生消息数量已达上限: {name}")
                    return {
                        "status": "limit",
                        "reason": "stranger_message_limit_reached",
                        "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    }

                if delivery_state == "message_request_limit_reached":
                    log.warning(f"点赞探针后确认消息请求发送达到上限: {name}")
                    return {
                        "status": "blocked",
                        "reason": "message_request_limit_reached",
                        "alert_text": self.MESSAGE_REQUEST_LIMIT_ALERT_TEXT,
                    }

                if delivery_state == "account_cannot_message":
                    log.warning(f"点赞探针后确认当前账号无法继续联系用户: {name}")
                    return {
                        "status": "blocked",
                        "reason": "account_cannot_message",
                        "alert_text": "当前账号已被限制继续向该类用户发起消息。",
                    }

                if delivery_state in ("failed", "send_restricted_ui"):
                    log.warning(f"点赞探针后确认无法发送: {name}")
                    return {
                        "status": "blocked",
                        "reason": "send_restricted_ui",
                        "alert_text": "当前窗口已被系统限制继续发送消息，请更换账户。",
                    }

                if delivery_state == "sent":
                    log.success(f"✅ 成员页消息已发送: {name}")
                    return {"status": "sent"}

                log.warning(f"发送状态未确认，不写入历史，等待下次重试: {name}")
                return {"status": "error", "reason": "delivery_not_confirmed"}
            finally:
                close_delay_ms = (
                    self.success_close_delay_ms if delivery_state == "sent" else self.default_close_delay_ms
                )
                self._sleep(max(close_delay_ms, 0) / 1000, min_seconds=0.05)
                self._close_chat_button(chat_session["close_button"])
                if use_fast:
                    self._fast_close_hover_card()
                else:
                    self._dismiss_member_hover_card()
        except Exception as exc:
            self._close_visible_chat_windows()
            log.error(f"⚠️ 成员页发送流程中断: {exc}")
            return {"status": "error", "reason": repr(exc)}

    def _type_and_send(self, editor, text, delay_range):
        before_hits = self._count_message_hits(editor, text)
        editor.click()
        self._sleep(0.08)

        min_delay, max_delay = delay_range
        for char in text:
            self.page.keyboard.type(char)
            self._sleep(random.uniform(min_delay / 1000, max_delay / 1000), min_seconds=0.01)

        self.page.keyboard.press("Enter")
        return self._wait_for_delivery(editor, text, before_hits)

    def _wait_for_delivery(self, editor, text, before_hits, timeout_ms=None):
        timeout_ms = timeout_ms or self.delivery_confirm_timeout_ms
        deadline = time.time() + timeout_ms / 1000
        success_seen_at = None
        while time.time() < deadline:
            self._wait_if_paused()
            state = self._get_delivery_state(editor, text)
            if not state:
                if self._check_stranger_limit_global():
                    return "limit_reached"
                self._sleep(0.25)
                continue

            if state["has_stranger_limit"]:
                return "limit_reached"
            if state.get("has_message_request_limit"):
                return "message_request_limit_reached"
            if self._check_stranger_limit_global():
                return "limit_reached"

            if state["has_failure"]:
                return state.get("failure_reason") or "failed"

            delivered = (
                state["message_hits"] > before_hits
                or state["has_status"]
                or state["has_sent_time"]
            )
            if delivered:
                if state["has_status"] or state["has_sent_time"]:
                    return "sent"

                if success_seen_at is None:
                    success_seen_at = time.time()
                elif (time.time() - success_seen_at) * 1000 >= self.delivery_settle_ms:
                    return "sent"
            else:
                success_seen_at = None
            self._sleep(0.25)
        if self._check_stranger_limit_global():
            return "limit_reached"
        return "timeout"

    def _send_like_probe(self, editor, timeout_ms=None):
        timeout_ms = timeout_ms or self.like_probe_timeout_ms
        like_button = self._find_like_button(editor)
        if not like_button:
            return "timeout"

        before_state = self._get_delivery_state(editor, "")
        before_fingerprint = before_state["window_fingerprint"] if before_state else ""
        before_media_count = before_state["media_count"] if before_state else 0

        try:
            like_button.click(timeout=2000)
        except Exception:
            return "timeout"

        deadline = time.time() + timeout_ms / 1000
        success_seen_at = None
        while time.time() < deadline:
            self._wait_if_paused()
            state = self._get_delivery_state(editor, "")
            if not state:
                if self._check_stranger_limit_global():
                    return "limit_reached"
                self._sleep(0.25)
                continue

            if state["has_stranger_limit"]:
                return "limit_reached"
            if state.get("has_message_request_limit"):
                return "message_request_limit_reached"
            if self._check_stranger_limit_global():
                return "limit_reached"

            if state["has_failure"]:
                return state.get("failure_reason") or "failed"

            delivered = (
                state["has_status"]
                or state["has_sent_time"]
                or state["window_fingerprint"] != before_fingerprint
                or state["media_count"] > before_media_count
            )
            if delivered:
                if state["has_status"] or state["has_sent_time"]:
                    return "sent"

                if success_seen_at is None:
                    success_seen_at = time.time()
                elif (time.time() - success_seen_at) * 1000 >= self.delivery_settle_ms:
                    return "sent"
            else:
                success_seen_at = None

            self._sleep(0.25)

        if self._check_stranger_limit_global():
            return "limit_reached"
        return "timeout"

    def _detect_profile_message_gate(self, link, hover_card=None):
        texts = []
        if hover_card is not None:
            texts.extend(self._extract_action_texts_from_scope(hover_card))
        texts.extend(self._extract_action_texts_near_link(link))
        normalized = " ".join(texts)
        if not normalized:
            return None
        if any(token in normalized for token in self.ADD_FRIEND_TEXTS):
            return "add_friend_required"
        if any(token in normalized for token in self.FOLLOW_ONLY_TEXTS):
            return "follow_only"
        if any(token in normalized for token in self.CONNECT_TEXTS):
            return "friend_required"
        if hover_card is not None:
            return "profile_message_unavailable"
        return None

    def _extract_action_texts_from_scope(self, scope):
        try:
            return scope.evaluate(
                """(node) => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const values = [];
                    const seen = new Set();
                    for (const el of Array.from(node.querySelectorAll('button, a, div[role="button"], [aria-label]'))) {
                        if (el.offsetParent === null) continue;
                        const text = normalize(el.innerText);
                        const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
                        const combined = normalize(`${aria} ${text}`);
                        if (!combined || seen.has(combined)) continue;
                        seen.add(combined);
                        values.push(combined);
                    }
                    return values;
                }"""
            ) or []
        except Exception:
            return []

    def _extract_action_texts_near_link(self, link):
        try:
            return link.evaluate(
                """(node) => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const rect = node.getBoundingClientRect();
                    const topMin = rect.top - 140;
                    const topMax = rect.bottom + 440;
                    const leftMin = rect.left - 220;
                    const leftMax = rect.right + 680;
                    let root = node.closest('div[role="listitem"]')
                        || node.closest('div[data-pagelet]')
                        || node.parentElement;
                    const values = [];
                    const seen = new Set();
                    for (let depth = 0; depth < 4 && root; depth += 1) {
                        for (const el of Array.from(root.querySelectorAll('button, a, div[role="button"], [aria-label]'))) {
                            if (el === node || node.contains(el) || el.offsetParent === null) continue;
                            const box = el.getBoundingClientRect();
                            if (!box.width || !box.height) continue;
                            if (box.top < topMin || box.top > topMax) continue;
                            if (box.left < leftMin || box.left > leftMax) continue;
                            const text = normalize(el.innerText);
                            const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
                            const combined = normalize(`${aria} ${text}`);
                            if (!combined || seen.has(combined)) continue;
                            seen.add(combined);
                            values.push(combined);
                        }
                        root = root.parentElement;
                    }
                    return values;
                }"""
            ) or []
        except Exception:
            return []

    def _detect_chat_restriction(self, expected_name=None):
        payload = {
            "strangerLimitTexts": self.STRANGER_LIMIT_TEXTS,
            "messageRequestLimitTexts": self.MESSAGE_REQUEST_LIMIT_TEXTS,
            "accountCannotMessageTexts": self.ACCOUNT_CANNOT_MESSAGE_TEXTS,
            "sendRestrictedTexts": self.SEND_RESTRICTED_TEXTS + self.DELIVERY_FAILURE_TEXTS,
            "expectedName": expected_name or "",
        }
        frames = [self.page.main_frame] + [f for f in self.page.frames if f is not self.page.main_frame]
        for frame in frames:
            try:
                result = frame.evaluate(
                    """(payload) => {
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                        const areas = Array.from(
                            document.querySelectorAll(
                                'div[role="dialog"], div[role="alert"], div[role="status"], [aria-live], div[role="presentation"]'
                            )
                        );
                        const expectedName = normalize(payload.expectedName);
                        const visibleAreas = areas.filter((el) => {
                            if (el.offsetParent === null) return false;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return false;
                            if (rect.right < window.innerWidth * 0.45) return false;
                            return true;
                        });
                        const pickReason = (text) => {
                            if (payload.strangerLimitTexts.some((token) => text.includes(token))) {
                                return {reason: 'stranger_message_limit_reached', alert_text: text};
                            }
                            if (payload.messageRequestLimitTexts.some((token) => text.includes(token))) {
                                return {reason: 'message_request_limit_reached', alert_text: text};
                            }
                            if (payload.accountCannotMessageTexts.some((token) => text.includes(token))) {
                                return {reason: 'account_cannot_message', alert_text: text};
                            }
                            if (payload.sendRestrictedTexts.some((token) => text.includes(token))) {
                                return {reason: 'account_restricted_by_dialog', alert_text: text};
                            }
                            return null;
                        };

                        for (const area of visibleAreas) {
                            const text = normalize(`${area.innerText || ''} ${area.getAttribute && area.getAttribute('aria-label') || ''}`);
                            if (!text) continue;
                            if (expectedName && !text.includes(expectedName) && text.length < 600) {
                                // 允许无名字的限制提示，但尽量优先当前会话区域
                            }
                            const hit = pickReason(text);
                            if (hit) return hit;
                        }

                        const bodyText = normalize(document.body && document.body.innerText);
                        return pickReason(bodyText);
                    }""",
                    payload,
                )
                if not result:
                    continue
                if result.get("reason") == "stranger_message_limit_reached":
                    return {
                        "status": "limit",
                        "reason": "stranger_message_limit_reached",
                        "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    }
                if result.get("reason") == "message_request_limit_reached":
                    return {
                        "status": "blocked",
                        "reason": "message_request_limit_reached",
                        "alert_text": self.MESSAGE_REQUEST_LIMIT_ALERT_TEXT,
                    }
                if result.get("reason") == "account_cannot_message":
                    return {
                        "status": "blocked",
                        "reason": "account_cannot_message",
                        "alert_text": result.get("alert_text") or "当前账号已被限制继续向该类用户发起消息。",
                    }
                if result.get("reason") == "account_restricted_by_dialog":
                    return {
                        "status": "blocked",
                        "reason": "account_restricted_by_dialog",
                        "alert_text": result.get("alert_text") or "聊天对话框出现限制提示，当前窗口停止继续发送。",
                    }
            except Exception:
                continue
        return None

    def _check_stranger_limit_global(self):
        if self.stranger_limit_cache_ms > 0:
            now = time.time()
            if (
                self._last_limit_check_ts
                and (now - self._last_limit_check_ts) * 1000 < self.stranger_limit_cache_ms
            ):
                return self._last_limit_check_result

        payload = {"strangerLimitTexts": self.STRANGER_LIMIT_TEXTS}
        frames = [self.page.main_frame] + [f for f in self.page.frames if f is not self.page.main_frame]
        for frame in frames:
            try:
                matched = frame.evaluate(
                    """(payload) => {
                        const texts = payload.strangerLimitTexts || [];
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                        const bodyText = normalize(document.body && document.body.innerText);
                        if (texts.some((t) => bodyText.includes(t))) {
                            return true;
                        }

                        const nodes = Array.from(
                            document.querySelectorAll('[role="alert"], [role="status"], [aria-live], [aria-label]')
                        );
                        return nodes.some((el) => {
                            const text = normalize(el.innerText);
                            const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
                            return texts.some((s) => text.includes(s) || aria.includes(s));
                        });
                    }""",
                    payload,
                )
                if matched:
                    if self.stranger_limit_cache_ms > 0:
                        self._last_limit_check_ts = time.time()
                        self._last_limit_check_result = True
                    return True
            except Exception:
                continue
        if self.stranger_limit_cache_ms > 0:
            self._last_limit_check_ts = time.time()
            self._last_limit_check_result = False
        return False

    def _get_delivery_state(self, editor, text):
        normalized_text = self._normalize_text(text)
        try:
            return editor.evaluate(
                """(node, payload) => {
                    const expectedText = payload.expectedText;
                    const statusTexts = payload.statusTexts || [];
                    const failureTexts = payload.failureTexts || [];
                    const strangerLimitTexts = payload.strangerLimitTexts || [];
                    const messageRequestLimitTexts = payload.messageRequestLimitTexts || [];
                    const accountCannotMessageTexts = payload.accountCannotMessageTexts || [];
                    const sendRestrictedTexts = payload.sendRestrictedTexts || [];
                    const editorRect = node.getBoundingClientRect();
                    const minX = Math.max(window.innerWidth * 0.55, editorRect.left - 140);
                    const maxX = window.innerWidth;
                    const minY = Math.max(0, editorRect.top - 520);
                    const maxY = Math.min(window.innerHeight, editorRect.bottom + 120);

                    let messageHits = 0;
                    let hasStatus = false;
                    let hasFailure = false;
                    let hasStrangerLimit = false;
                    let hasMessageRequestLimit = false;
                    let hasSentTime = false;
                    let failureReason = '';
                    let mediaCount = 0;
                    const seen = new Set();
                    const fingerprintParts = [];
                    const sentTimeRe = /(刚刚发送|刚剛傳送|\\d+\\s*(秒|分钟|分鐘|小时|小時|天)前发送|\\d+\\s*(seconds?|minutes?|hours?|days?)\\s+ago)/i;
                    const ignoredControls = new Set([
                        '发消息', 'Message', '信息',
                        '发送语音留言', '附加不超过25MB 的文件', '附加不超過25MB 的檔案',
                        '选择贴图', '選擇貼圖', '选择动图', '選擇動圖',
                        '选择表情', '選擇表情', '发个赞', '發個讚', 'Like', 'Send a like',
                        '关闭聊天窗口', 'Close chat window', 'Close chat',
                        '最小化聊天窗口', 'Minimize chat window'
                    ]);

                    for (const el of Array.from(document.querySelectorAll('div, span, a, svg, img'))) {
                        if (el === node || node.contains(el)) continue;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.right < minX || rect.left > maxX) continue;
                        if (rect.bottom < minY || rect.top > maxY) continue;

                        if ((el.tagName === 'SVG' || el.tagName === 'IMG') && rect.bottom < editorRect.top - 8) {
                            mediaCount += 1;
                        }

                        const text = (el.innerText || '').trim().replace(/\\s+/g, ' ');
                        const aria = (el.getAttribute && el.getAttribute('aria-label')) || '';
                        const combined = (text || aria || '').trim();
                        if (!combined) continue;
                        if (ignoredControls.has(combined)) continue;

                        if (expectedText && text.includes(expectedText)) {
                            const key = `${Math.round(rect.x)}:${Math.round(rect.y)}:${text}`;
                            if (!seen.has(key)) {
                                seen.add(key);
                                messageHits += 1;
                            }
                        }

                        if (fingerprintParts.length < 40) {
                            fingerprintParts.push(
                                `${el.tagName}:${Math.round(rect.x)}:${Math.round(rect.y)}:${combined.slice(0, 40)}`
                            );
                        }

                        if (!hasStatus && statusTexts.some((status) => combined.includes(status))) {
                            hasStatus = true;
                        }
                        if (!hasFailure && failureTexts.some((status) => combined.includes(status))) {
                            hasFailure = true;
                            failureReason = 'send_restricted_ui';
                        }
                        if (
                            !hasStrangerLimit
                            && strangerLimitTexts.some((status) => combined.includes(status))
                        ) {
                            hasStrangerLimit = true;
                        }
                        if (
                            !hasMessageRequestLimit
                            && messageRequestLimitTexts.some((status) => combined.includes(status))
                        ) {
                            hasMessageRequestLimit = true;
                        }
                        if (
                            !hasFailure
                            && accountCannotMessageTexts.some((status) => combined.includes(status))
                        ) {
                            hasFailure = true;
                            failureReason = 'account_cannot_message';
                        }
                        if (
                            !hasFailure
                            && sendRestrictedTexts.some((status) => combined.includes(status))
                        ) {
                            hasFailure = true;
                            failureReason = 'send_restricted_ui';
                        }
                        if (!hasSentTime && sentTimeRe.test(combined)) {
                            hasSentTime = true;
                        }
                    }

                    let windowText = '';
                    let root = node;
                    while (root.parentElement) {
                        const parent = root.parentElement;
                        const rect = parent.getBoundingClientRect();
                        if (
                            rect.width >= 260
                            && rect.height >= 180
                            && rect.right > window.innerWidth * 0.55
                        ) {
                            root = parent;
                        } else {
                            break;
                        }
                    }
                    windowText = (root.innerText || '').trim().replace(/\s+/g, ' ');

                    if (!hasFailure && failureTexts.some((status) => windowText.includes(status))) {
                        hasFailure = true;
                        failureReason = failureReason || 'send_restricted_ui';
                    }
                    if (
                        !hasStrangerLimit
                        && strangerLimitTexts.some((status) => windowText.includes(status))
                    ) {
                        hasStrangerLimit = true;
                    }
                    if (
                        !hasMessageRequestLimit
                        && messageRequestLimitTexts.some((status) => windowText.includes(status))
                    ) {
                        hasMessageRequestLimit = true;
                    }
                    if (
                        !hasFailure
                        && accountCannotMessageTexts.some((status) => windowText.includes(status))
                    ) {
                        hasFailure = true;
                        failureReason = 'account_cannot_message';
                    }
                    if (
                        !hasFailure
                        && sendRestrictedTexts.some((status) => windowText.includes(status))
                    ) {
                        hasFailure = true;
                        failureReason = 'send_restricted_ui';
                    }
                    if (!hasStatus && statusTexts.some((status) => windowText.includes(status))) {
                        hasStatus = true;
                    }
                    if (!hasSentTime && sentTimeRe.test(windowText)) {
                        hasSentTime = true;
                    }

                    return {
                        message_hits: messageHits,
                        has_status: hasStatus,
                        has_failure: hasFailure,
                        has_stranger_limit: hasStrangerLimit,
                        has_message_request_limit: hasMessageRequestLimit,
                        has_sent_time: hasSentTime,
                        media_count: mediaCount,
                        window_fingerprint: fingerprintParts.join('|'),
                        failure_reason: failureReason
                    };
                }""",
                {
                    "expectedText": normalized_text,
                    "statusTexts": self.DELIVERY_STATUS_TEXTS,
                    "failureTexts": self.DELIVERY_FAILURE_TEXTS,
                    "strangerLimitTexts": self.STRANGER_LIMIT_TEXTS,
                    "messageRequestLimitTexts": self.MESSAGE_REQUEST_LIMIT_TEXTS,
                    "accountCannotMessageTexts": self.ACCOUNT_CANNOT_MESSAGE_TEXTS,
                    "sendRestrictedTexts": self.SEND_RESTRICTED_TEXTS,
                },
            )
        except Exception:
            return None

    def _count_message_hits(self, editor, text):
        state = self._get_delivery_state(editor, text)
        if not state:
            return 0
        return state["message_hits"]

    def _normalize_text(self, value):
        return " ".join((value or "").split())

    def _close_visible_chat_windows(self):
        try:
            if self.page.is_closed():
                return
        except Exception:
            return
        for _ in range(2 if self.fast_message_button_only else 6):
            closed = False
            for selector in self.CHAT_CLOSE_SELECTORS:
                locator = self.page.locator(selector)
                count = locator.count()
                if not count:
                    continue
                for idx in range(count - 1, -1, -1):
                    button = locator.nth(idx)
                    try:
                        if not button.is_visible(timeout=self._visible_timeout()):
                            continue
                        button.click(timeout=self._scale_ms(1500, min_ms=300))
                        self._sleep(0.08, min_seconds=0.03)
                        closed = True
                    except Exception:
                        continue
            if not closed:
                return

    def _dismiss_member_hover_card(self):
        try:
            if self.page.is_closed():
                return
        except Exception:
            return
        if self.fast_message_button_only:
            self._fast_close_hover_card()
            return
        # 多轮关闭，避免浮层叠加
        for _ in range(3 if self.fast_message_button_only else 5):
            closed_any = False
            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            try:
                self.page.mouse.move(2, 2)
                self.page.mouse.click(2, 2)
            except Exception:
                pass
            for selector in self.MEMBER_CARD_CLOSE_SELECTORS:
                try:
                    locator = self.page.locator(selector)
                    count = locator.count()
                    if count <= 0:
                        continue
                    for idx in range(count - 1, -1, -1):
                        btn = locator.nth(idx)
                        if not btn.is_visible(timeout=self._scale_ms(220, min_ms=160)):
                            continue
                        try:
                            btn.click(timeout=self._scale_ms(450, min_ms=180))
                            closed_any = True
                        except Exception:
                            continue
                except Exception:
                    continue
            try:
                js_closed = self.page.evaluate(
                    """() => {
                        const labels = ['关闭','Close'];
                        const nodes = Array.from(document.querySelectorAll('[aria-label]'));
                        let closed = false;
                        for (const el of nodes) {
                            const label = el.getAttribute('aria-label') || '';
                            if (!labels.includes(label)) continue;
                            if (el.offsetParent === null) continue;
                            if (el.click) { el.click(); closed = true; }
                        }
                        return closed;
                    }"""
                )
                closed_any = closed_any or bool(js_closed)
            except Exception:
                pass
            self._sleep(0.04)
            if not closed_any:
                break

    def _fast_try_click_message_button(self, link, hover_delay_ms=None, click=True):
        try:
            return link.evaluate(
                """(node, payload) => {
                    const labels = payload.labels || [];
                    const closeLabels = payload.closeLabels || [];
                    const hoverDelay = payload.hoverDelay || 0;
                    const doClick = payload.click !== false;
                    const rect = node.getBoundingClientRect();
                    const topMin = rect.top - 120;
                    const topMax = rect.bottom + 420;
                    const leftMin = rect.left - 200;
                    const leftMax = rect.right + 600;
                    const matches = (el) => {
                        const aria = (el.getAttribute && el.getAttribute('aria-label')) || '';
                        const text = (el.innerText || '').trim();
                        const combined = `${aria} ${text}`;
                        return labels.some((label) => combined.includes(label));
                    };
                    node.scrollIntoView({block:'center', inline:'nearest'});
                    node.dispatchEvent(new MouseEvent('mouseover', {bubbles:true}));
                    if (hoverDelay > 0) {
                        const until = Date.now() + hoverDelay;
                        while (Date.now() < until) {}
                    }
                    let root = node.closest('div[role="listitem"]')
                        || node.closest('div[data-pagelet]')
                        || node.parentElement;
                    const roots = [];
                    for (let depth = 0; depth < 4 && root; depth += 1) {
                        roots.push(root);
                        root = root.parentElement;
                    }
                    for (const scope of roots) {
                        const candidates = Array.from(scope.querySelectorAll('div[role="button"], div[aria-label]'));
                        for (const el of candidates) {
                            if (!matches(el)) continue;
                            const r = el.getBoundingClientRect();
                            if (r.top < topMin || r.top > topMax) continue;
                            if (r.left < leftMin || r.left > leftMax) continue;
                            if (el.getAttribute && el.getAttribute('aria-disabled') === 'true') continue;
                            if (!doClick) return 'found';
                            try { el.click(); return 'clicked'; } catch (e) {}
                        }
                    }
                    // 没有按钮，尝试关闭浮层
                    const closers = Array.from(document.querySelectorAll('[aria-label]')).filter(el => {
                        const label = el.getAttribute('aria-label') || '';
                        return closeLabels.includes(label) && el.offsetParent !== null;
                    });
                    for (const el of closers) {
                        try { el.click(); return 'no_button'; } catch (e) {}
                    }
                    try {
                        node.dispatchEvent(new MouseEvent('mouseout', {bubbles:true}));
                        node.dispatchEvent(new MouseEvent('mouseleave', {bubbles:true}));
                    } catch (e) {}
                    // 兜底：点空白处
                    document.body.dispatchEvent(new MouseEvent('click', {clientX: 2, clientY: 2, bubbles: true}));
                    return 'no_button';
                }""",
                {
                    "labels": self.MEMBER_CARD_MESSAGE_TEXTS,
                    "closeLabels": ["关闭个人资料", "Close profile", "关闭", "Close"],
                    "hoverDelay": int(self.fast_hover_sleep_ms if hover_delay_ms is None else hover_delay_ms),
                    "click": bool(click),
                },
            )
        except Exception:
            return "hover_fail"

    def _fast_close_hover_card(self):
        try:
            if self.page.is_closed():
                return
        except Exception:
            return
        for _ in range(3):
            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            try:
                self.page.evaluate(
                    """() => {
                        const labels = ['关闭个人资料','Close profile','关闭','Close'];
                        const nodes = Array.from(document.querySelectorAll('[aria-label]'));
                        let closed = false;
                        for (const el of nodes) {
                            const label = el.getAttribute('aria-label') || '';
                            if (!labels.some(l => label.includes(l))) continue;
                            if (el.offsetParent === null) continue;
                            try { el.click(); closed = true; } catch (e) {}
                        }
                        if (!closed) {
                            // 尝试在卡片内找“X”按钮（无 aria-label 的情况）
                            const cards = Array.from(document.querySelectorAll('div[role="dialog"], div[role="presentation"], div[aria-modal="true"]'));
                            for (const card of cards) {
                                const btns = Array.from(card.querySelectorAll('div[role="button"], button'));
                                for (const btn of btns) {
                                    const txt = (btn.innerText || '').trim();
                                    if (txt === '×' || txt === '✕' || txt.toLowerCase() === 'x') {
                                        try { btn.click(); closed = true; } catch (e) {}
                                    }
                                    const svg = btn.querySelector('svg');
                                    if (svg && btn.getAttribute('aria-label') == null && btn.offsetParent !== null) {
                                        try { btn.click(); closed = true; } catch (e) {}
                                    }
                                }
                                if (closed) break;
                            }
                        }
                        if (!closed) {
                            // 再兜底：按卡片右上角坐标点击
                            const cards = Array.from(document.querySelectorAll('div[role="dialog"], div[role="presentation"], div[aria-modal="true"]'));
                            for (const card of cards) {
                                const rect = card.getBoundingClientRect();
                                if (!rect || rect.width < 120 || rect.height < 60) continue;
                                const x = Math.max(0, rect.right - 12);
                                const y = Math.max(0, rect.top + 12);
                                const el = document.elementFromPoint(x, y);
                                if (el) {
                                    try { el.click(); closed = true; } catch (e) {}
                                }
                                if (closed) break;
                            }
                        }
                        if (!closed) {
                            // 兜底：点空白区域
                            document.body.dispatchEvent(new MouseEvent('click', {clientX: 2, clientY: 2, bubbles: true}));
                        }
                    }"""
                )
            except Exception:
                pass
            try:
                self.page.mouse.move(2, 2)
                self.page.mouse.click(2, 2)
            except Exception:
                pass
            self._sleep(0.02, min_seconds=0.01)

    def _quick_no_button_precheck(self, link):
        try:
            quick = self._fast_try_click_message_button(
                link,
                hover_delay_ms=max(0, int(self.fast_hover_sleep_ms)),
                click=False,
            )
        except Exception:
            quick = "hover_fail"
        if quick == "found":
            return "found"
        if quick == "hover_fail":
            return "unknown"
        if quick != "no_button":
            return "unknown"
        try:
            link.hover(timeout=self._scale_ms(self.quick_no_button_hover_timeout_ms, min_ms=300))
        except Exception:
            return "no_button"
        self._sleep(max(0.1, self.quick_no_button_confirm_ms / 1000.0))
        link_box = self._wait_for_stable_box(link)
        if not link_box:
            return "no_button"
        hover_card = self._find_hover_card_container(link_box)
        if not hover_card:
            return "no_button"
        button = self._find_message_button_in_container(hover_card)
        if button:
            return "found"
        return "no_button"

    def _find_member_link(self, target):
        user_id = target.get("user_id")
        group_user_url = target.get("group_user_url")
        abs_top = target.get("abs_top")
        abs_left = target.get("abs_left")

        candidates = []
        selectors = []
        if group_user_url:
            selectors.append(f'a[href="{group_user_url}"]')
        if user_id:
            selectors.append(f'a[href*="/groups/"][href*="/user/{user_id}/"]')
            selectors.append(f'a[href*="/user/{user_id}/"]')

        for selector in selectors:
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count):
                link = locator.nth(idx)
                try:
                    if not link.is_visible(timeout=self._visible_timeout()):
                        continue
                    box = link.bounding_box()
                    if not box:
                        continue
                    if abs_top is not None:
                        dist = abs(box["y"] - abs_top)
                        if abs_left is not None:
                            dist += abs(box["x"] - abs_left)
                    else:
                        dist = box["y"]
                    candidates.append((dist, box["y"], box["x"], link))
                except Exception:
                    continue

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _find_member_card_message_button(self, link, hover_card=None):
        if self.fast_message_button_only and not self.force_hover_card:
            fast_button = self._fast_find_member_card_message_button(link)
            if fast_button:
                return fast_button
            return None

        link_box = self._wait_for_stable_box(link)
        if not link_box:
            return None

        if hover_card is None:
            hover_card = self._find_hover_card_container(link_box)
            if self.force_hover_card and not hover_card:
                return None

        if hover_card is not None:
            button = self._find_message_button_in_container(hover_card)
            if button:
                return button
            if self.force_hover_card:
                return None

        top_min = link_box["y"] - 120
        top_max = link_box["y"] + 420
        link_cx = link_box["x"] + (link_box["width"] / 2)
        link_cy = link_box["y"] + (link_box["height"] / 2)
        candidates = []
        for selector in self.MEMBER_CARD_MESSAGE_SELECTORS:
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count):
                button = locator.nth(idx)
                try:
                    if not button.is_visible(timeout=self._visible_timeout()):
                        continue
                    if button.get_attribute("aria-disabled") == "true":
                        continue
                    box = button.bounding_box()
                    if not box:
                        continue
                    if box["y"] < top_min or box["y"] > top_max:
                        continue
                    btn_cx = box["x"] + (box["width"] / 2)
                    btn_cy = box["y"] + (box["height"] / 2)
                    dist = abs(btn_cy - link_cy) + abs(btn_cx - link_cx)
                    candidates.append((dist, box["y"], box["x"], button))
                except Exception:
                    continue

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _find_message_button_in_container(self, container):
        for selector in self.MEMBER_CARD_MESSAGE_SELECTORS:
            try:
                locator = container.locator(selector)
                count = locator.count()
            except Exception:
                continue
            for idx in range(count):
                button = locator.nth(idx)
                try:
                    if not button.is_visible(timeout=self._visible_timeout()):
                        continue
                    if button.get_attribute("aria-disabled") == "true":
                        continue
                    return button
                except Exception:
                    continue
        return None

    def _find_hover_card_container(self, link_box):
        if not link_box:
            return None
        link_cx = link_box["x"] + (link_box["width"] / 2)
        link_cy = link_box["y"] + (link_box["height"] / 2)
        candidates = []
        for selector in ('div[role="dialog"]', 'div[role="presentation"]', 'div[aria-modal="true"]'):
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count):
                card = locator.nth(idx)
                try:
                    if not card.is_visible(timeout=self._scale_ms(260, min_ms=160)):
                        continue
                    box = card.bounding_box()
                    if not box:
                        continue
                    if box["width"] < 120 or box["height"] < 60:
                        continue
                    card_cx = box["x"] + (box["width"] / 2)
                    card_cy = box["y"] + (box["height"] / 2)
                    dist = abs(card_cy - link_cy) + abs(card_cx - link_cx)
                    if dist > 1200:
                        continue
                    candidates.append((dist, box["y"], box["x"], card))
                except Exception:
                    continue
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _ensure_hover_card(self, link):
        link_box = self._wait_for_stable_box(link)
        if not link_box:
            return None
        for attempt in range(2):
            try:
                link.hover(timeout=self._scale_ms(1400, min_ms=900))
            except Exception:
                pass
            card = self._find_hover_card_container(link_box)
            if card:
                return card
            self._sleep(random.uniform(0.18, 0.32))
            card = self._find_hover_card_container(link_box)
            if card:
                return card
            try:
                link.evaluate(
                    "node => node.dispatchEvent(new MouseEvent('mouseover', {bubbles:true}))"
                )
            except Exception:
                pass
            self._sleep(0.08)
            card = self._find_hover_card_container(link_box)
            if card:
                return card
        return None

    def _fast_find_member_card_message_button(self, link):
        try:
            handle = link.evaluate_handle(
                """(node, payload) => {
                    const labels = payload.labels || [];
                    const matches = (el) => {
                        const aria = (el.getAttribute && el.getAttribute('aria-label')) || '';
                        const text = (el.innerText || '').trim();
                        const combined = `${aria} ${text}`;
                        return labels.some((label) => combined.includes(label));
                    };
                    let root = node.closest('div[role="listitem"]')
                        || node.closest('div[data-pagelet]')
                        || node.parentElement;
                    for (let depth = 0; depth < 5 && root; depth += 1) {
                        const candidates = root.querySelectorAll('div[role="button"], div[aria-label]');
                        for (const el of candidates) {
                            if (matches(el)) return el;
                        }
                        root = root.parentElement;
                    }
                    return null;
                }""",
                {"labels": self.MEMBER_CARD_MESSAGE_TEXTS},
            )
            return handle.as_element()
        except Exception:
            return None

    def _wait_for_stable_box(self, locator, max_rounds=3, tolerance=2.0, delay=0.15):
        last = None
        for _ in range(max_rounds):
            try:
                box = locator.bounding_box()
            except Exception:
                box = None
            if not box:
                self._sleep(delay, min_seconds=0.02)
                continue
            if last:
                if (
                    abs(box["y"] - last["y"]) <= tolerance
                    and abs(box["x"] - last["x"]) <= tolerance
                    and abs(box["width"] - last["width"]) <= tolerance
                    and abs(box["height"] - last["height"]) <= tolerance
                ):
                    return box
            last = box
            self._sleep(delay, min_seconds=0.02)
        return last

    def _find_like_button(self, editor):
        editor_box = editor.bounding_box()
        if not editor_box:
            return None

        candidates = []
        for selector in self.LIKE_BUTTON_SELECTORS:
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count):
                button = locator.nth(idx)
                try:
                    if not button.is_visible(timeout=self._scale_ms(200, min_ms=80)):
                        continue
                    box = button.bounding_box()
                    if not box:
                        continue
                    if abs(box["y"] - editor_box["y"]) > 80:
                        continue
                    if box["x"] + box["width"] < editor_box["x"] + editor_box["width"] - 20:
                        continue
                    candidates.append((abs(box["y"] - editor_box["y"]), box["x"], button))
                except Exception:
                    continue

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[0][2]

    def _wait_for_chat_session(self, expected_name=None, timeout_ms=10000):
        timeout_ms = self._scale_ms(timeout_ms, min_ms=2000)
        result = self._wait_for_condition(
            "聊天窗口",
            "等待聊天输入框出现",
            lambda: self._locate_chat_session(expected_name=expected_name),
            timeout_ms=timeout_ms,
            interval=0.25,
        )
        return result

    def _locate_chat_session(self, expected_name=None):
        fallback_sessions = []
        for selector in self.MESSAGE_EDITOR_SELECTORS:
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count - 1, -1, -1):
                editor = locator.nth(idx)
                try:
                    if not editor.is_visible(timeout=self._scale_ms(200, min_ms=80)):
                        continue
                    editor_box = editor.bounding_box()
                    if not editor_box:
                        continue
                    viewport_size = self.page.viewport_size
                    viewport_width = (
                        viewport_size["width"]
                        if viewport_size
                        else self.page.evaluate("window.innerWidth")
                    )
                    if editor_box["x"] < viewport_width * 0.35:
                        continue

                    close_button = self._find_chat_close_button(editor, expected_name)
                    if close_button:
                        return {"editor": editor, "close_button": close_button}

                    close_button = self._find_chat_close_button(editor, None)
                    if close_button:
                        fallback_sessions.append({"editor": editor, "close_button": close_button})
                except Exception:
                    continue
        if expected_name and len(fallback_sessions) == 1:
            return fallback_sessions[0]
        return None

    def _wait_for_editor_ready(self, editor, timeout_ms=None):
        timeout_ms = self._scale_ms(timeout_ms or self.editor_ready_wait_timeout_ms, min_ms=1500)
        result = self._wait_for_condition(
            "聊天输入框",
            "等待输入框可见且可写",
            lambda: self._editor_ready(editor),
            timeout_ms=timeout_ms,
            interval=0.2,
        )
        return bool(result)

    def _editor_ready(self, editor):
        try:
            if not editor.is_visible(timeout=self._scale_ms(160, min_ms=80)):
                return False
            return bool(
                editor.evaluate(
                    """(el) => {
                        if (!el) return false;
                        if (el.getAttribute && el.getAttribute('aria-disabled') === 'true') return false;
                        const style = window.getComputedStyle(el);
                        if (!style || style.visibility === 'hidden' || style.display === 'none') return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        return !!(el.isContentEditable || el.getAttribute('contenteditable') === 'true');
                    }"""
                )
            )
        except Exception:
            return False

    def _wait_for_hover_card(self, link, timeout_ms=None):
        timeout_ms = self._scale_ms(timeout_ms or self.hover_card_wait_timeout_ms, min_ms=1500)
        link_box = self._wait_for_stable_box(link, max_rounds=2, delay=0.08)
        if not link_box:
            return None
        return self._wait_for_condition(
            "成员资料卡",
            "等待资料卡区域渲染完成",
            lambda: self._find_hover_card_container(link_box) or self._ensure_hover_card(link),
            timeout_ms=timeout_ms,
            interval=0.25,
        )

    def _wait_for_member_card_message_button(self, link, hover_card=None, timeout_ms=None):
        timeout_ms = self._scale_ms(timeout_ms or self.message_button_wait_timeout_ms, min_ms=1500)
        state = {"hover_card": hover_card}

        def _locate():
            current_card = state["hover_card"]
            if current_card is None:
                current_card = self._ensure_hover_card(link)
                state["hover_card"] = current_card
            if current_card is None:
                return None
            button = self._find_member_card_message_button(link, hover_card=current_card)
            return button

        return self._wait_for_condition(
            "发消息按钮",
            "等待资料卡中的“发消息”按钮可点击",
            _locate,
            timeout_ms=timeout_ms,
            interval=0.25,
        )

    def _wait_for_fast_message_entry(self, link, name=None, timeout_ms=None):
        timeout_ms = self._scale_ms(timeout_ms or self.message_button_wait_timeout_ms, min_ms=1500)

        def _attempt():
            try:
                result = self._fast_try_click_message_button(link)
            except Exception:
                result = "error"
            if result == "clicked":
                return True
            if result == "no_button":
                return None
            return False

        result = self._wait_for_condition(
            "发消息按钮",
            f"等待成员卡片上的“发消息”按钮就绪{f' ({name})' if name else ''}",
            _attempt,
            timeout_ms=timeout_ms,
            interval=0.25,
        )
        return True if result else None

    def _find_chat_close_button(self, editor, expected_name=None):
        try:
            if expected_name and not self._editor_window_mentions_name(editor, expected_name):
                return None

            editor_box = editor.bounding_box()
            if not editor_box:
                return None

            candidates = []
            for selector in self.CHAT_CLOSE_SELECTORS:
                locator = self.page.locator(selector)
                count = locator.count()
                for idx in range(count - 1, -1, -1):
                    button = locator.nth(idx)
                    try:
                        if not button.is_visible(timeout=self._visible_timeout()):
                            continue
                        box = button.bounding_box()
                        if not box:
                            continue
                        if box["y"] >= editor_box["y"]:
                            continue
                        if editor_box["y"] - box["y"] > 520:
                            continue
                        if box["x"] + box["width"] < editor_box["x"] - 80:
                            continue
                        candidates.append(
                            (
                                editor_box["y"] - box["y"],
                                abs(box["x"] - editor_box["x"]),
                                button,
                            )
                        )
                    except Exception:
                        continue

            if not candidates:
                return None
            candidates.sort(key=lambda item: (item[0], item[1]))
            return candidates[0][2]
        except Exception:
            return None

    def _editor_window_mentions_name(self, editor, expected_name):
        try:
            return bool(
                editor.evaluate(
                    """(node, expectedName) => {
                        let el = node;
                        while (el) {
                            const rect = el.getBoundingClientRect();
                            if (rect.width && rect.height && rect.right > window.innerWidth * 0.55) {
                                const text = (el.innerText || '').trim().replace(/\\s+/g, ' ');
                                if (text.includes(expectedName)) return true;
                            }
                            el = el.parentElement;
                        }
                        return false;
                    }""",
                    expected_name,
                )
            )
        except Exception:
            return False

    def _close_chat_button(self, close_button):
        if not close_button:
            self._close_visible_chat_windows()
            return

        try:
            close_button.click(timeout=self._scale_ms(1500, min_ms=300))
            self._sleep(0.25)
        except Exception:
            self._close_visible_chat_windows()
