import os
import random
import time
from pathlib import Path

from core.scraper import Scraper
from utils.logger import log


class Messager:
    ENV_PAUSE_FLAG = "FB_RPA_PAUSE_FLAG"
    CHAT_CLOSE_SELECTORS = [
        'button[aria-label="ปิดแชท"]',
        'button[aria-label="关闭聊天窗口"]',
        'button[aria-label="关闭聊天"]',
        'button[aria-label="Close chat window"]',
        'button[aria-label="Close chat"]',
        'div[aria-label="ปิดแชท"]',
        'div[aria-label="关闭聊天窗口"]',
        'div[aria-label="关闭聊天"]',
        'div[aria-label="Close chat window"]',
        'div[aria-label="Close chat"]',
        'div[role="button"][aria-label="ปิดแชท"]',
        'div[role="button"][aria-label="关闭聊天窗口"]',
        'div[role="button"][aria-label="关闭聊天"]',
        'div[role="button"][aria-label="Close chat window"]',
        'div[role="button"][aria-label="Close chat"]',
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
        "Send Message",
        "Enviar mensaje",
        "Mensaje",
        "Enviar mensagem",
        "Mensagem",
        "Envoyer un message",
        "Message",
        "Nachricht senden",
        "Nachricht",
        "Invia messaggio",
        "Messaggio",
        "ส่งข้อความ",
        "Nhắn tin",
        "Gửi tin nhắn",
        "Kirim pesan",
        "Hantar mesej",
        "Kirim mesaj",
        "Mesaj gönder",
        "Сообщение",
        "Отправить сообщение",
        "Wyślij wiadomość",
        "메시지 보내기",
        "メッセージを送信",
        "إرسال رسالة",
        "বার্তা পাঠান",
        "मैसेज भेजें",
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
    DELIVERY_STATUS_TEXTS = [
        "已发送",
        "已傳送",
        "已發送",
        "Sent",
        "Delivered",
        "Enviado",
        "Enviada",
        "Envoyé",
        "Envoyée",
        "Envoyé(e)",
        "Gesendet",
        "Inviato",
        "Sended",
        "Đã gửi",
        "ส่งแล้ว",
        "已送出",
        "Отправлено",
        "Wysłano",
        "Gönderildi",
        "Terkirim",
        "Dihantar",
        "전송됨",
        "送信済み",
        "تم الإرسال",
        "পাঠানো হয়েছে",
        "भेजा गया",
    ]
    DELIVERY_FAILURE_TEXTS = [
        "无法发送",
        "無法傳送",
        "Couldn't send",
        "Could not send",
        "This message can't be sent",
        "This message can’t be sent",
        "This message cannot be sent",
        "发送失败",
        "No se pudo enviar",
        "Não foi possível enviar",
        "Impossible d'envoyer",
        "Impossible d’envoyer",
        "Nachricht konnte nicht gesendet werden",
        "Nicht gesendet",
        "Impossibile inviare",
        "Không thể gửi",
        "ส่งข้อความไม่ได้",
        "Nie udało się wysłać",
        "Gönderilemedi",
        "Tidak dapat mengirim",
        "Tidak boleh menghantar",
        "تعذر الإرسال",
        "পাঠানো যায়নি",
        "भेजा नहीं जा सका",
    ]
    MESSAGE_REQUEST_LIMIT_TEXTS = [
        "You've reached the message request limit",
        "You’ve reached the message request limit",
        "There's a limit to how many requests you can send in 24 hours",
        "There’s a limit to how many requests you can send in 24 hours",
        "你已达到消息请求发送上限",
        "你已達到訊息請求發送上限",
        "Has alcanzado el límite de solicitudes de mensajes",
        "Você atingiu o limite de solicitações de mensagem",
        "Vous avez atteint la limite des demandes de message",
        "Bạn đã đạt giới hạn yêu cầu nhắn tin",
        "คุณถึงขีดจำกัดคำขอข้อความแล้ว",
    ]
    ACCOUNT_CANNOT_MESSAGE_TEXTS = [
        "You can't message this account right now",
        "You can’t message this account right now",
        "You can't send messages to this account right now",
        "You can’t send messages to this account right now",
        "You can't send a message to this account right now",
        "You can’t send a message to this account right now",
        "当前无法向此用户发送消息",
        "当前无法向此帳號发送消息",
        "当前无法向此账号发送消息",
        "您无法向此帐户发送消息",
        "您无法向此账户发送消息",
        "您無法向此帳戶傳送訊息",
        "您無法向此帳號傳送訊息",
        "你无法向此帐户发送消息",
        "你无法向此账户发送消息",
        "你無法向此帳戶傳送訊息",
        "你無法向此帳號傳送訊息",
        "現在無法向此帳戶傳送訊息",
        "現在無法向此帳號傳送訊息",
        "No puedes enviar mensajes a esta cuenta ahora",
        "Você não pode enviar mensagem para esta conta agora",
        "Vous ne pouvez pas envoyer de message à ce compte pour le moment",
        "Bạn không thể nhắn tin cho tài khoản này ngay lúc này",
        "ขณะนี้คุณไม่สามารถส่งข้อความถึงบัญชีนี้ได้",
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
        "No puedes enviar mensajes ahora",
        "Você não pode enviar mensagens agora",
        "Vous ne pouvez pas envoyer de messages pour le moment",
        "Bạn không thể gửi tin nhắn lúc này",
        "คุณไม่สามารถส่งข้อความได้ในขณะนี้",
        "Tidak dapat mengirim pesan sekarang",
        "Gönderim kısıtlandı",
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
        "You've reached the limit for message requests to people you don't know",
        "You’ve reached the limit for message requests to people you don’t know",
        "There is a limit for how many message requests you can send in 24 hours",
        "Has alcanzado el límite de mensajes para personas desconocidas",
        "Você atingiu o limite de mensagens para pessoas desconhecidas",
        "Vous avez atteint la limite de messages adressés à des inconnus",
        "Bạn đã đạt giới hạn tin nhắn cho người lạ",
        "คุณถึงขีดจำกัดข้อความถึงคนแปลกหน้าแล้ว",
    ]
    POPUP_CLOSE_LABELS = [
        "关闭",
        "Close",
        "Confirm",
        "确认",
        "確認",
        "确定",
        "確定",
        "关闭个人资料",
        "Close profile",
        "关闭聊天窗口",
        "Close chat window",
        "Close chat",
        "Discard",
        "Leave",
        "Leave chat",
        "放弃",
        "放棄",
        "丢弃",
        "丟棄",
        "离开",
        "離開",
        "离开聊天",
        "離開聊天",
        "稍后",
        "稍後",
        "稍后再说",
        "稍後再說",
        "Not now",
        "Not Now",
        "Dismiss",
        "Got it",
        "Understood",
        "Fermer",
        "Cerrar",
        "Schließen",
        "Chiudi",
        "Fechar",
        "Đóng",
        "ปิด",
        "知道了",
        "我知道了",
        "OK",
        "确定",
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
        'div[aria-label="Me gusta"]',
        'div[aria-label="Curtir"]',
        'div[aria-label="J’aime"]',
        'div[aria-label="Gefällt mir"]',
    ]
    ADD_FRIEND_TEXTS = [
        "Add friend",
        "Add Friend",
        "添加好友",
        "加好友",
        "加為好友",
        "加为好友",
        "Ajouter",
        "Agregar amigo",
        "Adicionar amigo",
        "Freund/in hinzufügen",
        "เพิ่มเพื่อน",
        "Kết bạn",
        "Tambahkan teman",
        "Tambah rakan",
        "Dodaj do znajomych",
        "Arkadaş ekle",
        "إضافة صديق",
        "বন্ধু যোগ করুন",
        "मित्र जोड़ें",
    ]
    FOLLOW_ONLY_TEXTS = [
        "Follow",
        "关注",
        "關注",
        "追蹤",
        "追踪",
        "Seguir",
        "Suivre",
        "Theo dõi",
        "Mengikuti",
        "Ikut",
        "Sledź",
        "Takip et",
        "متابعة",
    ]
    CONNECT_TEXTS = [
        "Connect",
        "连接",
        "連接",
        "建立关系",
        "建立關係",
        "Kết nối",
        "Conectar",
        "Conectar-se",
        "Hubungkan",
        "Sambung",
        "Połącz",
        "Bağlan",
        "ربط",
        "联系",
    ]
    INVITE_TEXTS = [
        "Invite",
        "Invites",
        "邀请",
        "邀請",
        "Invitar",
        "Convidar",
        "Inviter",
        "Einladen",
        "Invita",
        "เชิญ",
        "Mời",
        "Undang",
        "Jemput",
        "Zaproś",
        "Davet et",
        "دعوة",
        "আমন্ত্রণ",
        "आमंत्रित करें",
    ]
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
            task_cfg.get("default_close_delay_ms", 350) or 350
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

    def _is_reusable_facebook_url(self, url):
        return Scraper._is_reusable_facebook_url(self, url)

    def _has_real_facebook_surface(self, page=None):
        return Scraper._has_real_facebook_surface(self, page)

    def _adopt_existing_facebook_page(self):
        try:
            if self._is_reusable_facebook_url(self.page.url) and self._has_real_facebook_surface(self.page):
                return True
            candidate_pages = []
            try:
                candidate_pages.extend(list(self.page.context.pages or []))
            except Exception:
                pass
            try:
                browser = self.page.context.browser
                if browser is not None:
                    for context in list(getattr(browser, "contexts", []) or []):
                        candidate_pages.extend(list(getattr(context, "pages", []) or []))
            except Exception:
                pass

            seen = set()
            for candidate in candidate_pages:
                marker = id(candidate)
                if marker in seen:
                    continue
                seen.add(marker)
                if candidate.is_closed():
                    continue
                if candidate is self.page:
                    continue
                if not self._is_reusable_facebook_url(candidate.url):
                    continue
                if not self._has_real_facebook_surface(candidate):
                    continue
                self.page = candidate
                try:
                    self.page.bring_to_front()
                except Exception:
                    pass
                log.info(f"{self._prefix()}发送链已切换到真实 Facebook 页面: {self.page.url}")
                return True
        except Exception:
            return False
        return False

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
        return self._scale_ms(default_ms, min_ms=min_ms)

    def send_member_card_dm(self, target, probe_text, greeting_text, delay_range):
        name = target.get("name") or ""
        user_id = target.get("user_id") or ""
        try:
            self._adopt_existing_facebook_page()
            self._wait_if_paused()
            log.info(f"💬 正在处理成员卡片: {name} ({user_id})")
            preclear_chat = self._close_visible_chat_windows()
            preclear_popup = self._dismiss_interfering_popups(context=f"成员 {name} 入口前置清场")
            preclear_card = self._dismiss_member_hover_card()
            preclear_overlay = self._detect_blocking_overlay()
            preclear_chat_left = bool(self._locate_chat_session())
            if preclear_overlay and (preclear_chat > 0 or preclear_popup > 0 or preclear_card > 0):
                preclear_popup += self._dismiss_interfering_popups(context=f"成员 {name} 二次前置清场")
                preclear_card += self._dismiss_member_hover_card()
                preclear_overlay = self._detect_blocking_overlay()
                preclear_chat_left = bool(self._locate_chat_session())
            log.info(
                f"{self._prefix()}入口前置全量清场完成：关闭聊天={preclear_chat}，关闭弹层={preclear_popup}，"
                f"关闭资料卡={preclear_card}。"
            )
            log.info(
                f"{self._prefix()}当前成员开始前页面已清干净：overlay_blocking={bool(preclear_overlay)}，"
                f"residual_chat={preclear_chat_left}。"
            )
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
                return {"status": "error", "reason": "member_link_not_found"}
            self._wait_if_paused()
            action_timeout = self._scale_ms(2000, min_ms=1500)
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
                    try:
                        link.scroll_into_view_if_needed(timeout=action_timeout)
                    except Exception:
                        refound_link = self._find_member_link(target)
                        if refound_link:
                            link = refound_link
                    self._wait_for_stable_box(link, max_rounds=3, delay=0.06)
            else:
                try:
                    link.scroll_into_view_if_needed(timeout=action_timeout)
                except Exception:
                    refound_link = self._find_member_link(target)
                    if refound_link:
                        link = refound_link
                self._wait_for_stable_box(link, max_rounds=3, delay=0.06)
            self._wait_if_paused()

            try:
                link.hover(timeout=action_timeout)
            except Exception:
                try:
                    link.hover(timeout=max(action_timeout, 1200))
                except Exception:
                    failure = self._classify_message_button_failure(
                        link,
                        name,
                        hover_card=None,
                        timed_out=False,
                    )
                    log.info(failure["log"])
                    result = {"status": failure.get("status", "error"), "reason": failure["reason"]}
                    if failure.get("alert_text"):
                        result["alert_text"] = failure["alert_text"]
                    return result

            hover_card = self._wait_for_hover_card(link, timeout_ms=self.hover_card_wait_timeout_ms)
            if not hover_card:
                retry_link, retry_hover_card = self._retry_hover_card_after_refind(
                    target,
                    timeout_ms=min(self.hover_card_wait_timeout_ms, 3200),
                )
                if retry_link:
                    link = retry_link
                if retry_hover_card:
                    hover_card = retry_hover_card
            if not hover_card:
                failure = self._classify_message_button_failure(
                    link,
                    name,
                    hover_card=None,
                    timed_out=False,
                )
                log.info(failure["log"])
                self._dismiss_member_hover_card()
                result = {"status": failure.get("status", "skip"), "reason": failure["reason"]}
                if failure.get("alert_text"):
                    result["alert_text"] = failure["alert_text"]
                return result

            button = self._find_member_card_message_button(link, hover_card=hover_card)
            if not button:
                failure = self._classify_message_button_failure(
                    link,
                    name,
                    hover_card=hover_card,
                    timed_out=False,
                )
                if failure["reason"] != "message_button_blocked_popup" and not self._should_wait_for_message_button(hover_card):
                    log.info(failure["log"])
                    self._dismiss_member_hover_card()
                    result = {"status": failure.get("status", "skip"), "reason": failure["reason"]}
                    if failure.get("alert_text"):
                        result["alert_text"] = failure["alert_text"]
                    return result

                button = self._wait_for_member_card_message_button(
                    link,
                    hover_card=hover_card,
                    timeout_ms=self.message_button_wait_timeout_ms,
                )
            if not button:
                failure = self._classify_message_button_failure(
                    link,
                    name,
                    hover_card=hover_card,
                    timed_out=True,
                )
                log.info(failure["log"])
                self._dismiss_member_hover_card()
                result = {"status": failure.get("status", "skip"), "reason": failure["reason"]}
                if failure.get("alert_text"):
                    result["alert_text"] = failure["alert_text"]
                return result

            # 只有在确定要点击发消息按钮时才关闭聊天窗口，避免无按钮时浪费时间
            self._wait_if_paused()
            self._close_visible_chat_windows()

            if button is not True:
                try:
                    button.click(timeout=action_timeout)
                    log.info(f"{self._prefix()}发消息按钮点击成功: {name}")
                except Exception:
                    try:
                        button.evaluate("el => el.click()")
                        log.info(f"{self._prefix()}发消息按钮点击成功(JS): {name}")
                    except Exception:
                        log.warning(f"发消息按钮点击失败，跳过: {name}")
                        self._dismiss_member_hover_card()
                        return {"status": "error", "reason": "message_button_click_failed"}
            self._wait_if_paused()

            chat_session = self._wait_for_chat_session(
                expected_name=name,
                timeout_ms=self.chat_session_wait_timeout_ms,
            )
            if not chat_session:
                restriction = self._detect_chat_restriction(expected_name=name)
                if restriction:
                    log.warning(f"聊天窗口限制提示命中: {name} ({restriction['reason']})")
                    self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                    self._close_visible_chat_windows()
                    self._dismiss_member_hover_card()
                    self._log_limit_dialog_page_state(name)
                    return restriction
                closed_popups = self._dismiss_interfering_popups(context=f"成员 {name} 聊天窗口未打开")
                if closed_popups:
                    chat_session = self._wait_for_chat_session(
                        expected_name=name,
                        timeout_ms=min(self.chat_session_wait_timeout_ms, 3200),
                    )
                    if chat_session:
                        log.info(f"{self._prefix()}关闭干扰弹层后，聊天窗口恢复可用: {name}")
                if not chat_session:
                    overlay = self._detect_blocking_overlay()
                    if overlay:
                        self._dismiss_interfering_popups(context=f"成员 {name} 聊天窗口被弹层阻断")
                        self._close_visible_chat_windows()
                        self._dismiss_member_hover_card()
                        log.warning(
                            f"{self._prefix()}聊天窗口被干扰弹层阻断，跳过: {name} ({overlay.get('text') or 'popup'})"
                        )
                        return {"status": "error", "reason": "chat_popup_blocking"}
                if self._check_stranger_limit_global():
                    log.warning(f"未找到输入框但检测到陌生消息上限: {name}")
                    return {
                        "status": "limit",
                        "reason": "stranger_message_limit_reached",
                        "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    }
                self._close_visible_chat_windows()
                self._dismiss_member_hover_card()
                log.warning(f"{self._prefix()}聊天窗口未正常打开，跳过: {name}")
                return {"status": "error", "reason": "chat_editor_not_found"}

            delivery_state = "timeout"
            cleanup_result = {"closed_chat": 0, "closed_card": 0}
            try:
                if not self._wait_for_editor_ready(
                    chat_session["editor"],
                    timeout_ms=self.editor_ready_wait_timeout_ms,
                ):
                    log.warning(f"{self._prefix()}输入框未准备就绪，跳过: {name}")
                    return {"status": "error", "reason": "chat_editor_not_ready"}

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
                    self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                    self._close_visible_chat_windows()
                    self._dismiss_member_hover_card()
                    self._log_limit_dialog_page_state(name)
                    return restriction

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

                if delivery_state in ("failed", "send_failed_ui"):
                    log.warning(f"聊天窗明确提示发送失败，保留当前目标待后续重试: {name}")
                    return {
                        "status": "error",
                        "reason": "send_failed_ui",
                    }

                if delivery_state == "send_restricted_ui":
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

                if delivery_state in ("failed", "send_failed_ui"):
                    log.warning(f"点赞探针后确认发送失败，保留当前目标待后续重试: {name}")
                    return {
                        "status": "error",
                        "reason": "send_failed_ui",
                    }

                if delivery_state == "send_restricted_ui":
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
                close_button = chat_session.get("close_button")
                close_editor = chat_session.get("editor")
                if not self._chat_editor_gone(close_editor):
                    refreshed_session = self._locate_chat_session(expected_name=name) or self._locate_chat_session()
                    if refreshed_session:
                        close_button = refreshed_session.get("close_button") or close_button
                        close_editor = refreshed_session.get("editor") or close_editor
                close_wait_ms = self._scale_ms(2600 if delivery_state == "sent" else 1200, min_ms=480)
                cleanup_result["closed_chat"] = self._close_chat_button(
                    close_button,
                    editor=close_editor,
                    auto_wait_ms=close_wait_ms,
                )
                cleanup_result["closed_card"] = self._dismiss_member_hover_card()
                log.info(
                    f"{self._prefix()}成员 {name} 收尾完成：关闭聊天={cleanup_result['closed_chat']}，"
                    f"关闭资料卡={cleanup_result['closed_card']}。"
                )
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
        if hover_card is not None:
            texts = self._extract_action_texts_from_scope(hover_card)
        else:
            texts = self._extract_action_texts_near_link(link)
        normalized = " ".join(texts)
        if not normalized:
            return None
        if self._matches_any_text(normalized, self.STRANGER_LIMIT_TEXTS):
            return "stranger_message_limit_reached"
        if self._matches_any_text(normalized, self.MESSAGE_REQUEST_LIMIT_TEXTS):
            return "message_request_limit_reached"
        if self._matches_any_text(normalized, self.ACCOUNT_CANNOT_MESSAGE_TEXTS):
            return "account_cannot_message"
        if self._matches_any_text(normalized, self.SEND_RESTRICTED_TEXTS):
            return "send_restricted_ui"
        if any(token in normalized for token in self.ADD_FRIEND_TEXTS):
            return "add_friend_required"
        if any(token in normalized for token in self.FOLLOW_ONLY_TEXTS):
            return "follow_only"
        if any(token in normalized for token in self.CONNECT_TEXTS):
            return "friend_required"
        if any(token in normalized for token in self.INVITE_TEXTS):
            return "invite_only"
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
                }""",
                timeout=self._scale_ms(700, min_ms=250),
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
                }""",
                timeout=self._scale_ms(700, min_ms=250),
            ) or []
        except Exception:
            return []

    def _detect_chat_restriction(self, expected_name=None):
        payload = {
            "strangerLimitTexts": self.STRANGER_LIMIT_TEXTS,
            "messageRequestLimitTexts": self.MESSAGE_REQUEST_LIMIT_TEXTS,
            "accountCannotMessageTexts": self.ACCOUNT_CANNOT_MESSAGE_TEXTS,
            "sendRestrictedTexts": self.SEND_RESTRICTED_TEXTS,
            "failureTexts": self.DELIVERY_FAILURE_TEXTS,
            "expectedName": expected_name or "",
        }
        frames = [self.page.main_frame] + [f for f in self.page.frames if f is not self.page.main_frame]
        for frame in frames:
            try:
                result = frame.evaluate(
                    """(payload) => {
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                        const isVisibleArea = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return false;
                            const style = window.getComputedStyle(el);
                            if (!style) return false;
                            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                                return false;
                            }
                            return true;
                        };
                        const areas = Array.from(
                            document.querySelectorAll(
                                'div[role="dialog"], div[role="alert"], div[role="status"], [aria-live], div[role="presentation"]'
                            )
                        );
                        const expectedName = normalize(payload.expectedName);
                        const visibleAreas = areas.filter((el) => {
                            if (!isVisibleArea(el)) return false;
                            const rect = el.getBoundingClientRect();
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
                            if (payload.failureTexts.some((token) => text.includes(token))) {
                                return {reason: 'send_failed_ui', alert_text: text};
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
                if result.get("reason") == "send_failed_ui":
                    return {
                        "status": "error",
                        "reason": "send_failed_ui",
                        "alert_text": result.get("alert_text") or "聊天对话框明确提示本次发送失败。",
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
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const expectedText = payload.expectedText;
                    const statusTexts = payload.statusTexts || [];
                    const failureTexts = payload.failureTexts || [];
                    const strangerLimitTexts = payload.strangerLimitTexts || [];
                    const messageRequestLimitTexts = payload.messageRequestLimitTexts || [];
                    const accountCannotMessageTexts = payload.accountCannotMessageTexts || [];
                    const sendRestrictedTexts = payload.sendRestrictedTexts || [];
                    const isVisibleArea = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                            return false;
                        }
                        return true;
                    };
                    const overlayAreas = Array.from(
                        document.querySelectorAll(
                            'div[role="dialog"], div[role="alert"], div[role="status"], [aria-live], div[role="presentation"]'
                        )
                    );
                    const editorRect = node.getBoundingClientRect();
                    const hasAnyToken = (textValue, tokens) => tokens.some((token) => textValue.includes(token));

                    let root = node;
                    let bestRoot = node;
                    let bestScore = -1;
                    let current = node;
                    let depth = 0;
                    while (current && depth < 16) {
                        const rect = current.getBoundingClientRect();
                        const textValue = normalize(current.innerText || '');
                        const aria = normalize(current.getAttribute && current.getAttribute('aria-label'));
                        const role = normalize(current.getAttribute && current.getAttribute('role'));
                        const widthOk = (
                            rect.width >= Math.max(editorRect.width + 120, 260)
                            && rect.width <= window.innerWidth * 0.62
                        );
                        const rightOk = rect.right > window.innerWidth * 0.48;
                        const verticalOk = (
                            rect.top <= editorRect.top + 40
                            && rect.bottom >= editorRect.bottom - 4
                        );
                        if (widthOk && rightOk && verticalOk) {
                            let score = 0;
                            if (rect.height >= 120) score += 2;
                            if (rect.height >= 220) score += 3;
                            if (role === 'group' || role === 'dialog' || role === 'none') score += 2;
                            if (aria.includes('发消息给') || aria.toLowerCase().includes('message')) score += 4;
                            if (textValue.includes('发消息给') || textValue.includes('Message')) score += 2;
                            if (expectedText && textValue.includes(expectedText)) score += 6;
                            if (hasAnyToken(textValue, statusTexts)) score += 4;
                            if (hasAnyToken(textValue, failureTexts)) score += 4;
                            if (hasAnyToken(textValue, strangerLimitTexts)) score += 4;
                            if (hasAnyToken(textValue, messageRequestLimitTexts)) score += 4;
                            if (hasAnyToken(textValue, accountCannotMessageTexts)) score += 4;
                            if (hasAnyToken(textValue, sendRestrictedTexts)) score += 4;
                            score += Math.min(6, Math.round(rect.height / 90));
                            if (score >= bestScore) {
                                bestScore = score;
                                bestRoot = current;
                            }
                        }
                        current = current.parentElement;
                        depth += 1;
                    }
                    root = bestRoot || node;

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
                    const sentTimeRe = /(刚刚发送|剛剛傳送|已發送|\\d+\\s*(秒|分钟|分鐘|小时|小時|天)前发送|\\d+\\s*(seconds?|minutes?|hours?|days?)\\s+ago|hace\\s+\\d+\\s*(segundos?|minutos?|horas?|días?)|há\\s+\\d+\\s*(segundos?|minutos?|horas?|dias?)|il\\s+y\\s+a\\s+\\d+\\s*(secondes?|minutes?|heures?|jours?)|vor\\s+\\d+\\s*(Sekunden?|Minuten?|Stunden?|Tagen?)|\\d+\\s*(secondi|minuti|ore|giorni)\\s+fa|\\d+\\s*(секунд|секунды|минут|минуты|часов|часа|дней|дня)\\s+назад|\\d+\\s*(sekund|minut|godzin|dni)\\s+temu|\\d+\\s*(saniye|dakika|saat|gün)\\s+önce|منذ\\s+\\d+\\s*(ثانية|دقيقة|ساعة|يوم)|\\d+\\s*(秒|分|時間|日)前|\\d+\\s*(초|분|시간|일)\\s*전|\\d+\\s*(วินาที|นาที|ชั่วโมง|วัน)ที่แล้ว|\\d+\\s*(giây|phút|giờ|ngày)\\s+trước|\\d+\\s*(detik|menit|jam|hari)\\s+yang\\s+lalu|\\d+\\s*(saat|minit|jam|hari)\\s+yang\\s+lalu|\\d+\\s*(সেকেন্ড|মিনিট|ঘণ্টা|দিন)\\s+আগে|\\d+\\s*(सेकंड|मिनट|घंटे|दिन)\\s+पहले)/i;
                    const ignoredControls = new Set([
                        '发消息', 'Message', '信息',
                        '发送语音留言', '附加不超过25MB 的文件', '附加不超過25MB 的檔案',
                        '选择贴图', '選擇貼圖', '选择动图', '選擇動圖',
                        '选择表情', '選擇表情', '发个赞', '發個讚', 'Like', 'Send a like',
                        '关闭聊天窗口', 'Close chat window', 'Close chat',
                        '最小化聊天窗口', 'Minimize chat window'
                    ]);

                    for (const el of Array.from(root.querySelectorAll('div, span, a, svg, img'))) {
                        if (el === node || node.contains(el)) continue;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;

                        if ((el.tagName === 'SVG' || el.tagName === 'IMG') && rect.bottom < editorRect.top - 8) {
                            mediaCount += 1;
                        }

                        const text = normalize(el.innerText || '');
                        const aria = (el.getAttribute && el.getAttribute('aria-label')) || '';
                        const combined = normalize(text || aria || '');
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
                            failureReason = 'send_failed_ui';
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

                    const windowText = normalize(root.innerText || '');

                    if (!hasFailure && failureTexts.some((status) => windowText.includes(status))) {
                        hasFailure = true;
                        failureReason = failureReason || 'send_failed_ui';
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

                    const shouldFallbackToOverlay = (
                        messageHits <= 0
                        && !hasStatus
                        && !hasFailure
                        && !hasStrangerLimit
                        && !hasMessageRequestLimit
                        && !hasSentTime
                    );

                    if (shouldFallbackToOverlay) {
                        const candidates = overlayAreas.filter((el) => {
                            if (!el || el === root || !isVisibleArea(el)) return false;
                            const rect = el.getBoundingClientRect();
                            if (rect.right < window.innerWidth * 0.45) return false;
                            if (rect.bottom < editorRect.top - 260 || rect.top > editorRect.bottom + 260) return false;
                            return true;
                        });
                        for (const area of candidates) {
                            const areaText = normalize(
                                `${area.innerText || ''} ${area.getAttribute && area.getAttribute('aria-label') || ''}`
                            );
                            if (!areaText) continue;
                            if (expectedText && areaText.includes(expectedText) && messageHits <= 0) {
                                messageHits = 1;
                            }
                            if (!hasStatus && statusTexts.some((status) => areaText.includes(status))) {
                                hasStatus = true;
                            }
                            if (!hasFailure && failureTexts.some((status) => areaText.includes(status))) {
                                hasFailure = true;
                                failureReason = failureReason || 'send_failed_ui';
                            }
                            if (!hasStrangerLimit && strangerLimitTexts.some((status) => areaText.includes(status))) {
                                hasStrangerLimit = true;
                            }
                            if (
                                !hasMessageRequestLimit
                                && messageRequestLimitTexts.some((status) => areaText.includes(status))
                            ) {
                                hasMessageRequestLimit = true;
                            }
                            if (
                                !hasFailure
                                && accountCannotMessageTexts.some((status) => areaText.includes(status))
                            ) {
                                hasFailure = true;
                                failureReason = 'account_cannot_message';
                            }
                            if (
                                !hasFailure
                                && sendRestrictedTexts.some((status) => areaText.includes(status))
                            ) {
                                hasFailure = true;
                                failureReason = 'send_restricted_ui';
                            }
                            if (!hasSentTime && sentTimeRe.test(areaText)) {
                                hasSentTime = true;
                            }
                        }
                    }

                    if (expectedText && windowText.includes(expectedText) && messageHits <= 0) {
                        messageHits = 1;
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

    def _detect_blocking_overlay(self):
        try:
            if self.page.is_closed():
                return None
        except Exception:
            return None
        try:
            result = self.page.evaluate(
                """(payload) => {
                    const labels = payload.labels || [];
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const isVisibleArea = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                            return false;
                        }
                        return true;
                    };
                    const dialogs = Array.from(
                        document.querySelectorAll(
                            'div[role="dialog"], div[aria-modal="true"], div[role="alertdialog"], div[role="alert"]'
                        )
                    ).filter((el) => {
                        if (!isVisibleArea(el)) return false;
                        const rect = el.getBoundingClientRect();
                        if (rect.width < 180 || rect.height < 80) return false;
                        return rect.right > window.innerWidth * 0.4;
                    });
                    for (const dialog of dialogs) {
                        const text = normalize(
                            `${dialog.innerText || ''} ${dialog.getAttribute && dialog.getAttribute('aria-label') || ''}`
                        );
                        const controls = Array.from(
                            dialog.querySelectorAll('button, div[role="button"], [aria-label]')
                        );
                        const hasCloser = controls.some((el) => {
                            if (el.offsetParent === null) return false;
                            const label = normalize(el.getAttribute && el.getAttribute('aria-label'));
                            const inner = normalize(el.innerText);
                            const combined = `${label} ${inner}`.trim();
                            return labels.some((token) => combined.includes(token));
                        });
                        if (!hasCloser) continue;
                        return {
                            text: text.slice(0, 160),
                            hasCloser: true,
                        };
                    }
                    return null;
                }""",
                {"labels": self.POPUP_CLOSE_LABELS},
            )
            return result or None
        except Exception:
            return None

    def _dismiss_interfering_popups(self, context=""):
        try:
            if self.page.is_closed():
                return 0
        except Exception:
            return 0

        closed_total = 0
        for _ in range(3):
            closed_round = 0
            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            try:
                closed_round += int(
                    self.page.evaluate(
                        """(labels) => {
                            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                            let closed = 0;
                            const nodes = Array.from(document.querySelectorAll('button, div[role="button"], [aria-label]'));
                            for (const el of nodes) {
                                if (el.offsetParent === null) continue;
                                const text = normalize(el.innerText);
                                const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
                                const combined = `${aria} ${text}`.trim();
                                if (!labels.some((token) => combined.includes(token))) continue;
                                try {
                                    el.click();
                                    closed += 1;
                                } catch (e) {}
                            }
                            return closed;
                        }""",
                        self.POPUP_CLOSE_LABELS,
                    )
                    or 0
                )
            except Exception:
                pass
            try:
                self.page.mouse.move(2, 2)
                self.page.mouse.click(2, 2)
            except Exception:
                pass
            closed_total += closed_round
            if closed_round <= 0:
                break
            self._sleep(0.08, min_seconds=0.03)

        if closed_total > 0:
            prefix = f"{context}：" if context else ""
            log.info(f"{self._prefix()}{prefix}已关闭 {closed_total} 个干扰弹层/提醒。")
        return closed_total

    def _log_limit_dialog_page_state(self, name):
        overlay_blocking = bool(self._detect_blocking_overlay())
        residual_chat = bool(self._locate_chat_session())
        if not overlay_blocking and not residual_chat:
            log.info(
                f"{self._prefix()}成员 {name} 限制弹窗挂页收尾后页面已恢复干净："
                "overlay_blocking=False，residual_chat=False。"
            )
        else:
            log.warning(
                f"{self._prefix()}成员 {name} 限制弹窗挂页收尾后页面仍有残留："
                f"overlay_blocking={overlay_blocking}，residual_chat={residual_chat}。"
            )
        return {
            "overlay_blocking": overlay_blocking,
            "residual_chat": residual_chat,
        }

    def _classify_message_button_failure(self, link, name, hover_card=None, timed_out=False):
        gate_reason = self._detect_profile_message_gate(link, hover_card=hover_card) if hover_card is not None else None
        if gate_reason:
            if gate_reason == "stranger_message_limit_reached":
                return {
                    "status": "limit",
                    "reason": "stranger_message_limit_reached",
                    "alert_text": self.STRANGER_LIMIT_ALERT_TEXT,
                    "log": f"{self._prefix()}成员资料卡命中陌生消息上限提示，停止当前窗口: {name}",
                }
            if gate_reason == "message_request_limit_reached":
                return {
                    "status": "blocked",
                    "reason": "message_request_limit_reached",
                    "alert_text": self.MESSAGE_REQUEST_LIMIT_ALERT_TEXT,
                    "log": f"{self._prefix()}成员资料卡命中消息请求上限提示，停止当前窗口: {name}",
                }
            if gate_reason == "account_cannot_message":
                return {
                    "status": "blocked",
                    "reason": "account_cannot_message",
                    "alert_text": "当前账号已被限制继续向该类用户发起消息。",
                    "log": f"{self._prefix()}成员资料卡明确提示当前无法联系该成员，停止当前窗口: {name}",
                }
            if gate_reason == "send_restricted_ui":
                return {
                    "status": "blocked",
                    "reason": "send_restricted_ui",
                    "alert_text": "当前窗口已被系统限制继续发送消息，请更换账户。",
                    "log": f"{self._prefix()}成员资料卡明确提示当前无法发送，停止当前窗口: {name}",
                }
            return {
                "status": "skip",
                "reason": gate_reason,
                "log": f"{self._prefix()}成员资料卡限制，跳过: {name} ({gate_reason})",
            }

        overlay = self._detect_blocking_overlay()
        if overlay:
            self._dismiss_interfering_popups(context=f"成员 {name} 发消息按钮识别阶段")
            return {
                "status": "error",
                "reason": "message_button_blocked_popup",
                "log": f"{self._prefix()}检测到干扰弹层，当前成员暂不发送: {name} ({overlay.get('text') or 'popup'})",
            }

        if hover_card is None:
            return {
                "status": "error",
                "reason": "member_hover_timeout",
                "log": f"{self._prefix()}等待成员资料卡超时，跳过: {name}",
            }

        if timed_out:
            return {
                "status": "error",
                "reason": "message_button_timeout",
                "log": f"{self._prefix()}等待“发消息”按钮超时，跳过当前成员: {name}",
            }

        return {
            "status": "skip",
            "reason": "message_button_not_found",
            "log": f"{self._prefix()}当前成员卡片没有可用的发消息按钮，跳过: {name}",
        }

    def _close_visible_chat_windows(self):
        try:
            if self.page.is_closed():
                return 0
        except Exception:
            return 0
        closed_total = 0
        for _ in range(6):
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
                        closed_total += 1
                    except Exception:
                        continue
            if not closed:
                break
        if closed_total > 0:
            log.info(f"{self._prefix()}已关闭 {closed_total} 个残留聊天窗口。")
        return closed_total

    def _dismiss_member_hover_card(self):
        try:
            if self.page.is_closed():
                return 0
        except Exception:
            return 0
        # 多轮关闭，避免浮层叠加
        closed_total = 0
        for _ in range(5):
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
                            closed_total += 1
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
                if js_closed:
                    closed_any = True
                    closed_total += 1
            except Exception:
                pass
            self._sleep(0.04)
            if not closed_any:
                break
        if closed_total > 0:
            log.info(f"{self._prefix()}已关闭 {closed_total} 个成员资料卡/悬浮层。")
        return closed_total

    def _find_member_link(self, target):
        user_id = target.get("user_id")
        group_user_url = target.get("group_user_url")
        target_name = str(target.get("name") or "").strip()
        abs_top = target.get("abs_top")
        abs_left = target.get("abs_left")

        selectors = []
        if group_user_url:
            selectors.append(f'a[href="{group_user_url}"]')
        if user_id:
            selectors.append(f'a[href*="/groups/"][href*="/user/{user_id}/"]')
            selectors.append(f'a[href*="/user/{user_id}/"]')

        def _collect_candidates():
            candidates = []
            for selector in selectors:
                locator = self.page.locator(selector)
                count = locator.count()
                for idx in range(count):
                    link = locator.nth(idx)
                    try:
                        if not link.is_visible(timeout=self._scale_ms(700, min_ms=260)):
                            continue
                        box = self._safe_bounding_box(link, timeout_ms=900)
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
                fallback = self._find_visible_member_link_by_target(
                    user_id=user_id,
                    group_user_url=group_user_url,
                    target_name=target_name,
                    abs_top=abs_top,
                    abs_left=abs_left,
                )
                if fallback:
                    candidates.append(fallback)
            return candidates

        wait_timeout = self._scale_ms(1200, min_ms=700)
        candidates = self._wait_for_condition(
            "成员链接",
            "等待成员列表中的目标链接稳定可见",
            lambda: _collect_candidates() or None,
            timeout_ms=wait_timeout,
            interval=0.12,
        )
        if not candidates and abs_top is not None:
            try:
                scroll_target = max(0, int(abs_top) - 220)
                self.page.evaluate("(y) => window.scrollTo(0, y)", scroll_target)
                self._wait_for_condition(
                    "成员链接",
                    "等待滚动后的成员列表稳定",
                    lambda: self._find_visible_member_link_by_target(
                        user_id=user_id,
                        group_user_url=group_user_url,
                        target_name=target_name,
                        abs_top=abs_top,
                        abs_left=abs_left,
                    ),
                    timeout_ms=self._scale_ms(1400, min_ms=900),
                    interval=0.12,
                )
            except Exception:
                pass
            candidates = self._wait_for_condition(
                "成员链接",
                "等待滚动后的目标链接稳定可见",
                lambda: _collect_candidates() or None,
                timeout_ms=self._scale_ms(1400, min_ms=900),
                interval=0.12,
            )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _find_visible_member_link_by_target(self, user_id=None, group_user_url=None, target_name="", abs_top=None, abs_left=None):
        try:
            payload = self.page.evaluate(
                """(payload) => {
                    const targetUserId = String(payload.userId || '').trim();
                    const targetGroupUrl = String(payload.groupUserUrl || '').trim().replace(/\\/$/, '');
                    const targetName = String(payload.targetName || '').trim().replace(/\\s+/g, ' ').toLowerCase();
                    const absTop = Number(payload.absTop);
                    const absLeft = Number(payload.absLeft);
                    const normalizeText = (value) => String(value || '').trim().replace(/\\s+/g, ' ').toLowerCase();
                    const normalizeHref = (href) => {
                        try {
                            return new URL(href, location.origin).toString().replace(/\\/$/, '');
                        } catch (e) {
                            return String(href || '').replace(/\\/$/, '');
                        }
                    };
                    const cssPath = (el) => {
                        if (!el || !(el instanceof Element)) return null;
                        const parts = [];
                        let node = el;
                        while (node && node.nodeType === Node.ELEMENT_NODE && node !== document.body) {
                            const tag = node.tagName.toLowerCase();
                            let index = 1;
                            let sibling = node.previousElementSibling;
                            while (sibling) {
                                if (sibling.tagName === node.tagName) index += 1;
                                sibling = sibling.previousElementSibling;
                            }
                            parts.unshift(`${tag}:nth-of-type(${index})`);
                            node = node.parentElement;
                        }
                        return parts.length ? parts.join(' > ') : null;
                    };
                    const links = Array.from(document.querySelectorAll('a[href*="/groups/"][href*="/user/"]'));
                    const candidates = [];
                    for (const link of links) {
                        const rect = link.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.bottom < 0 || rect.top > window.innerHeight) continue;
                        const href = normalizeHref(link.getAttribute('href') || '');
                        const match = href.match(/\\/groups\\/[^/]+\\/user\\/(\\d+)\\/?/i);
                        const currentUserId = match && match[1] ? String(match[1]) : '';
                        const text = normalizeText(link.innerText || link.getAttribute('aria-label') || '');
                        let score = 0;
                        if (targetUserId && currentUserId === targetUserId) score += 100;
                        if (targetGroupUrl && href === targetGroupUrl) score += 80;
                        if (targetGroupUrl && href.startsWith(targetGroupUrl)) score += 70;
                        if (targetName && text && (text === targetName || text.includes(targetName))) score += 50;
                        if (score <= 0) continue;
                        if (Number.isFinite(absTop)) {
                            score -= Math.min(120, Math.abs((rect.top + window.scrollY) - absTop) / 6);
                        }
                        if (Number.isFinite(absLeft)) {
                            score -= Math.min(40, Math.abs((rect.left + window.scrollX) - absLeft) / 8);
                        }
                        const selector = cssPath(link);
                        if (!selector) continue;
                        candidates.push({
                            selector,
                            score,
                            y: rect.top + window.scrollY,
                            x: rect.left + window.scrollX,
                        });
                    }
                    if (!candidates.length) return null;
                    candidates.sort((a, b) => b.score - a.score || a.y - b.y || a.x - b.x);
                    return candidates[0];
                }""",
                {
                    "userId": user_id or "",
                    "groupUserUrl": group_user_url or "",
                    "targetName": target_name or "",
                    "absTop": abs_top if abs_top is not None else None,
                    "absLeft": abs_left if abs_left is not None else None,
                },
            )
        except Exception:
            return None
        if not payload or not payload.get("selector"):
            return None
        try:
            locator = self.page.locator(payload["selector"]).first
            if not locator.is_visible(timeout=self._scale_ms(700, min_ms=260)):
                return None
            box = self._safe_bounding_box(locator, timeout_ms=900)
            if not box:
                return None
            dist_y = abs(box["y"] - abs_top) if abs_top is not None else box["y"]
            dist_x = abs(box["x"] - abs_left) if abs_left is not None else box["x"]
            return (dist_y + dist_x, box["y"], box["x"], locator)
        except Exception:
            return None

    def _retry_hover_card_after_refind(self, target, timeout_ms):
        try:
            self._dismiss_interfering_popups(context="成员资料卡重试阶段")
        except Exception:
            pass
        self._dismiss_member_hover_card()
        retry_link = self._find_member_link(target)
        if not retry_link:
            return None, None
        try:
            retry_link.scroll_into_view_if_needed(timeout=self._scale_ms(1600, min_ms=800))
        except Exception:
            pass
        try:
            retry_link.hover(timeout=self._scale_ms(1800, min_ms=900))
        except Exception:
            return retry_link, None
        hover_card = self._wait_for_hover_card(retry_link, timeout_ms=timeout_ms)
        return retry_link, hover_card

    def _find_member_card_message_button(self, link, hover_card=None):
        link_box = self._wait_for_stable_box(link)
        if not link_box:
            return None

        if hover_card is None:
            hover_card = self._find_hover_card_container(link_box)
        if hover_card is None:
            return None
        return self._find_message_button_in_container(hover_card)

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
        try:
            locator = container.locator('button, a[role="button"], div[role="button"], span[role="button"], [aria-label]')
            count = locator.count()
        except Exception:
            count = 0
            locator = None
        for idx in range(count):
            button = locator.nth(idx)
            try:
                if not button.is_visible(timeout=self._visible_timeout()):
                    continue
                if button.get_attribute("aria-disabled") == "true":
                    continue
                label = self._normalize_text(button.get_attribute("aria-label") or "")
                text = self._normalize_text(button.inner_text() or "")
                combined = f"{label} {text}".strip()
                if not self._matches_any_text(combined, self.MEMBER_CARD_MESSAGE_TEXTS):
                    continue
                return button
            except Exception:
                continue
        return None

    def _matches_any_text(self, text, tokens):
        normalized = self._normalize_text(text).lower()
        if not normalized:
            return False
        for token in tokens or []:
            normalized_token = self._normalize_text(token).lower()
            if normalized_token and normalized_token in normalized:
                return True
        return False

    def _should_wait_for_message_button(self, hover_card):
        if hover_card is None:
            return False
        try:
            texts = self._extract_action_texts_from_scope(hover_card)
        except Exception:
            texts = []
        normalized = " ".join(self._normalize_text(item) for item in texts if item).strip()
        if not normalized:
            return False
        if self._matches_any_text(normalized, self.ADD_FRIEND_TEXTS):
            return False
        if self._matches_any_text(normalized, self.FOLLOW_ONLY_TEXTS):
            return False
        if self._matches_any_text(normalized, self.CONNECT_TEXTS):
            return False
        if self._matches_any_text(normalized, self.INVITE_TEXTS):
            return False
        if self._matches_any_text(normalized, self.STRANGER_LIMIT_TEXTS):
            return False
        if self._matches_any_text(normalized, self.MESSAGE_REQUEST_LIMIT_TEXTS):
            return False
        if self._matches_any_text(normalized, self.ACCOUNT_CANNOT_MESSAGE_TEXTS):
            return False
        if self._matches_any_text(normalized, self.SEND_RESTRICTED_TEXTS):
            return False
        if self._matches_any_text(normalized, self.DELIVERY_FAILURE_TEXTS):
            return False
        if self._matches_any_text(normalized, self.MEMBER_CARD_MESSAGE_TEXTS):
            return True
        return any(keyword in normalized.lower() for keyword in ["message", "mensaje", "mensagem", "messaggio"])

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
                    box = self._safe_bounding_box(card, timeout_ms=400)
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
            card = self._wait_for_condition(
                "成员资料卡",
                "等待悬停后的资料卡出现",
                lambda: self._find_hover_card_container(link_box),
                timeout_ms=self._scale_ms(520, min_ms=280),
                interval=0.08,
            )
            if card:
                return card
            try:
                link.evaluate(
                    "node => node.dispatchEvent(new MouseEvent('mouseover', {bubbles:true}))"
                )
            except Exception:
                pass
            card = self._wait_for_condition(
                "成员资料卡",
                "等待 mouseover 后资料卡出现",
                lambda: self._find_hover_card_container(link_box),
                timeout_ms=self._scale_ms(320, min_ms=180),
                interval=0.06,
            )
            if card:
                return card
        return None

    def _wait_for_stable_box(self, locator, max_rounds=3, tolerance=2.0, delay=0.15):
        last = None
        for _ in range(max_rounds):
            box = self._safe_bounding_box(locator, timeout_ms=650)
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

    def _safe_bounding_box(self, locator, timeout_ms=None):
        try:
            return locator.bounding_box(
                timeout=self._scale_ms(timeout_ms or 700, min_ms=250)
            )
        except Exception:
            return None

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
        bare_sessions = []
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
                        continue

                    if expected_name and self._editor_window_mentions_name(editor, expected_name):
                        bare_sessions.append({"editor": editor, "close_button": None})
                except Exception:
                    continue
        if expected_name and len(fallback_sessions) == 1:
            return fallback_sessions[0]
        if expected_name and len(bare_sessions) == 1:
            return bare_sessions[0]
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
        card = self._wait_for_condition(
            "成员资料卡",
            "等待资料卡区域渲染完成",
            lambda: self._find_hover_card_container(link_box) or self._ensure_hover_card(link),
            timeout_ms=timeout_ms,
            interval=0.25,
        )
        if card:
            return card
        self._dismiss_interfering_popups(context="成员资料卡等待阶段")
        self._dismiss_member_hover_card()
        try:
            link.scroll_into_view_if_needed(timeout=self._scale_ms(1600, min_ms=800))
            self._wait_for_stable_box(link, max_rounds=3, delay=0.06)
        except Exception:
            pass
        return self._wait_for_condition(
            "成员资料卡",
            "二次等待资料卡区域渲染完成",
            lambda: self._ensure_hover_card(link),
            timeout_ms=min(timeout_ms, self._scale_ms(3200, min_ms=1600)),
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
            if not button and self._detect_blocking_overlay():
                self._dismiss_interfering_popups(context="成员资料卡按钮识别阶段")
                state["hover_card"] = self._ensure_hover_card(link)
                current_card = state["hover_card"]
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

    def _chat_editor_gone(self, editor):
        if not editor:
            return True
        try:
            return not editor.is_visible(timeout=self._scale_ms(180, min_ms=80))
        except Exception:
            return True

    def _close_chat_button(self, close_button, editor=None, auto_wait_ms=None):
        auto_wait_ms = self._scale_ms(auto_wait_ms or 1200, min_ms=480)
        if self._chat_editor_gone(editor):
            log.info(f"{self._prefix()}聊天窗口已自行关闭，无需额外点击。")
            return 1
        if not close_button:
            auto_gone = self._wait_for_condition(
                "聊天窗口收尾",
                "等待聊天窗口自行关闭",
                lambda: self._chat_editor_gone(editor),
                timeout_ms=auto_wait_ms,
                interval=0.12,
            )
            if auto_gone:
                log.info(f"{self._prefix()}聊天窗口已自行关闭。")
                return 1
            closed = self._close_visible_chat_windows()
            if closed > 0:
                log.info(f"{self._prefix()}聊天窗口批量关闭成功，数量={closed}。")
                return closed
            gone_after_fallback = self._wait_for_condition(
                "聊天窗口收尾",
                "等待批量关闭后的聊天窗口消失",
                lambda: self._chat_editor_gone(editor),
                timeout_ms=self._scale_ms(900, min_ms=360),
                interval=0.12,
            )
            if gone_after_fallback:
                log.info(f"{self._prefix()}聊天窗口已在批量收尾后关闭。")
                return 1
            return 0

        try:
            close_button.click(timeout=self._scale_ms(1500, min_ms=300))
            closed = self._wait_for_condition(
                "聊天窗口收尾",
                "等待聊天窗口关闭",
                lambda: self._chat_editor_gone(editor),
                timeout_ms=max(auto_wait_ms, self._scale_ms(1600, min_ms=600)),
                interval=0.12,
            )
            if closed:
                log.info(f"{self._prefix()}聊天窗口关闭成功。")
                return 1
            dismissed = self._dismiss_interfering_popups(context="聊天窗口关闭确认")
            if dismissed > 0:
                closed = self._wait_for_condition(
                    "聊天窗口收尾",
                    "等待确认关闭后的聊天窗口关闭",
                    lambda: self._chat_editor_gone(editor),
                    timeout_ms=self._scale_ms(1200, min_ms=480),
                    interval=0.12,
                )
                if closed:
                    log.info(f"{self._prefix()}聊天窗口确认弹窗已自动确认并关闭成功。")
                    return 1
        except Exception:
            log.warning(f"{self._prefix()}聊天窗口关闭按钮点击失败，改用批量关闭兜底。")
        closed = self._close_visible_chat_windows()
        if closed > 0:
            log.info(f"{self._prefix()}聊天窗口批量关闭成功，数量={closed}。")
            return closed
        gone_after_fallback = self._wait_for_condition(
            "聊天窗口收尾",
            "等待兜底关闭后的聊天窗口消失",
            lambda: self._chat_editor_gone(editor),
            timeout_ms=self._scale_ms(900, min_ms=360),
            interval=0.12,
        )
        if gone_after_fallback:
            log.info(f"{self._prefix()}聊天窗口已在兜底收尾后关闭。")
            return 1
        return 0
