import os
import time
from urllib.parse import urlsplit

from core.scraper import Scraper
from utils.helpers import run_pause_aware_action, wait_if_runtime_paused
from utils.logger import log


ARIA_LABEL_SELECTOR_BASES = (
    "button",
    'div[role="button"]',
    "div",
    'span[role="button"]',
    "span",
)


def _build_aria_label_selectors(labels):
    selectors = []
    seen = set()
    for label in labels:
        safe_label = str(label or "").replace('"', '\\"')
        if not safe_label:
            continue
        for base in ARIA_LABEL_SELECTOR_BASES:
            selector = f'{base}[aria-label="{safe_label}"]'
            if selector in seen:
                continue
            seen.add(selector)
            selectors.append(selector)
    return selectors


class Messager:
    ENV_PAUSE_FLAG = "FB_RPA_PAUSE_FLAG"
    GENERIC_CLOSE_LABELS = (
        "关闭",
        "關閉",
        "Close",
        "Fermer",
        "Cerrar",
        "Schließen",
        "Chiudi",
        "Fechar",
        "Đóng",
        "ปิด",
        "Закрыть",
        "Zamknij",
        "Kapat",
        "إغلاق",
        "বন্ধ করুন",
        "बंद करें",
        "閉じる",
        "닫기",
        "Tutup",
    )
    CHAT_CLOSE_LABELS = (
        "ปิดแชท",
        "关闭聊天窗口",
        "关闭聊天",
        "關閉聊天視窗",
        "關閉聊天",
        "Close chat window",
        "Close chat",
        "Cerrar chat",
        "Fechar chat",
        "Fermer le chat",
        "Fermer la discussion",
        "Chat schließen",
        "Chiudi la chat",
        "Đóng đoạn chat",
        "Đóng cuộc trò chuyện",
        "Tutup chat",
        "Tutup obrolan",
        "Закрыть чат",
        "Zamknij czat",
        "Sohbeti kapat",
        "إغلاق الدردشة",
        "チャットを閉じる",
        "채팅 닫기",
    )
    CHAT_CLOSE_SELECTORS = _build_aria_label_selectors(CHAT_CLOSE_LABELS)
    MEMBER_CARD_CLOSE_LABELS = (
        "关闭个人资料",
        "關閉個人資料",
        "关闭资料卡",
        "關閉資料卡",
        "Close profile",
    ) + GENERIC_CLOSE_LABELS
    MEMBER_CARD_CLOSE_SELECTORS = _build_aria_label_selectors(MEMBER_CARD_CLOSE_LABELS)
    ACTION_ELEMENT_SELECTOR = 'button, a, [role="button"], [aria-label]'
    MEMBER_CARD_MESSAGE_TEXTS = [
        "发消息",
        "发送消息",
        "發送訊息",
        "發消息",
        "Message",
        "Send message",
        "Send a message",
        "Send Message",
        "Enviar mensaje",
        "Enviar un mensaje",
        "Mensaje",
        "Enviar mensagem",
        "Enviar uma mensagem",
        "Mensagem",
        "Envoyer un message",
        "Envoyer message",
        "Message",
        "Nachricht senden",
        "Nachricht",
        "Invia messaggio",
        "Messaggio",
        "ส่งข้อความ",
        "ข้อความ",
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
    MEMBER_CARD_MESSAGE_HINT_KEYWORDS = [
        "message",
        "mensaje",
        "mensagem",
        "messaggio",
        "nachricht",
        "ส่งข้อความ",
        "ข้อความ",
        "nhắn tin",
        "tin nhắn",
        "pesan",
        "mesej",
        "mesaj",
        "сообщение",
        "wiadomość",
        "메시지",
        "メッセージ",
        "رسالة",
        "বার্তা",
        "मैसेज",
    ]
    MESSAGE_HEADING_HINT_TOKENS = tuple(
        dict.fromkeys(tuple(MEMBER_CARD_MESSAGE_TEXTS) + tuple(MEMBER_CARD_MESSAGE_HINT_KEYWORDS))
    )
    CHAT_MINIMIZE_TEXTS = (
        "最小化聊天窗口",
        "最小化聊天",
        "Minimize chat window",
        "Minimize chat",
        "ย่อหน้าต่างแชท",
        "Thu nhỏ cửa sổ chat",
        "Minimalkan chat",
        "Minimizekan sembang",
        "Minimiser le chat",
        "Minimizar chat",
    )
    CHAT_REACTION_CONTROL_TEXTS = (
        "发个赞",
        "發個讚",
        "点赞",
        "按讚",
        "Like",
        "Send a like",
        "Me gusta",
        "Curtir",
        "J’aime",
        "Gefällt mir",
        "Mi piace",
        "ถูกใจ",
        "Thích",
        "Suka",
        "Polub",
        "Beğen",
        "أعجبني",
        "いいね",
        "좋아요",
    )
    CHAT_COMPOSER_CONTROL_TEXTS = (
        "信息",
        "發送語音留言",
        "发送语音留言",
        "附加不超过25MB 的文件",
        "附加不超過25MB 的檔案",
        "选择贴图",
        "選擇貼圖",
        "选择动图",
        "選擇動圖",
        "选择表情",
        "選擇表情",
        "Attach files",
        "Sticker",
        "GIF",
        "Emoji",
    )
    CHAT_IGNORED_CONTROL_TEXTS = tuple(
        dict.fromkeys(
            tuple(MEMBER_CARD_MESSAGE_TEXTS)
            + CHAT_COMPOSER_CONTROL_TEXTS
            + CHAT_REACTION_CONTROL_TEXTS
            + CHAT_CLOSE_LABELS
            + CHAT_MINIMIZE_TEXTS
        )
    )
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
        "You've reached the limit for message requests",
        "You’ve reached the limit for message requests",
        "You can only send a limited number of message requests in 24 hours",
        "You can only send a limited number of requests in 24 hours",
        "你已达到消息请求发送上限",
        "你已達到訊息請求發送上限",
        "Has alcanzado el límite de solicitudes de mensajes",
        "Você atingiu o limite de solicitações de mensagem",
        "Vous avez atteint la limite des demandes de message",
        "Bạn đã đạt giới hạn yêu cầu nhắn tin",
        "คุณถึงขีดจำกัดคำขอข้อความแล้ว",
        "คุณส่งคำขอส่งข้อความถึงขีดจำกัดแล้ว",
        "คุณสามารถส่งคำขอได้อย่างจำกัดจำนวนครั้งในรอบ 24 ชั่วโมง",
        "คุณสามารถส่งคำขอได้อย่างจำกัดจำนวนครั้ง ในรอบ 24 ชั่วโมง",
        "ส่งคำขอได้อย่างจำกัดจำนวนครั้งในรอบ 24 ชั่วโมง",
        "ส่งคำขอได้อย่างจำกัดจำนวนครั้ง ในรอบ 24 ชั่วโมง",
    ]
    ACCOUNT_CANNOT_MESSAGE_TEXTS = [
        "You can't message this account right now",
        "You can’t message this account right now",
        "You can't message this account",
        "You can’t message this account",
        "You can't send messages to this account right now",
        "You can’t send messages to this account right now",
        "You can't send messages to this account",
        "You can’t send messages to this account",
        "You can't send a message to this account right now",
        "You can’t send a message to this account right now",
        "当前无法向此用户发送消息",
        "当前无法向此帳號发送消息",
        "当前无法向此账号发送消息",
        "无法向此帐户发送消息",
        "无法向此账户发送消息",
        "您无法向此帐户发送消息",
        "您无法向此账户发送消息",
        "您無法向此帳戶傳送訊息",
        "您無法向此帳號傳送訊息",
        "你无法向此帐户发送消息",
        "你无法向此账户发送消息",
        "無法向此帳戶傳送訊息",
        "無法向此帳號傳送訊息",
        "你無法向此帳戶傳送訊息",
        "你無法向此帳號傳送訊息",
        "現在無法向此帳戶傳送訊息",
        "現在無法向此帳號傳送訊息",
        "No puedes enviar mensajes a esta cuenta ahora",
        "Você não pode enviar mensagem para esta conta agora",
        "Vous ne pouvez pas envoyer de message à ce compte pour le moment",
        "Bạn không thể nhắn tin cho tài khoản này ngay lúc này",
        "ขณะนี้คุณไม่สามารถส่งข้อความถึงบัญชีนี้ได้",
        "คุณไม่สามารถส่งข้อความถึงบัญชีนี้",
    ]
    MESSENGER_LOGIN_REQUIRED_TEXTS = [
        "目前无法访问此聊天",
        "目前無法存取此聊天",
        "无法访问此聊天",
        "無法存取此聊天",
        "下次登录 Messenger 时",
        "下次登入 Messenger 時",
        "你可以在对方下次登录 Messenger 时发送消息",
        "你可以在他下次登录 Messenger 时发送消息",
        "你可以在她下次登录 Messenger 时发送消息",
        "你可以在其下次登录 Messenger 时发送消息",
        "你可以在对方下次登入 Messenger 时傳送訊息",
        "你可以在他下次登入 Messenger 時傳送訊息",
        "你可以在她下次登入 Messenger 時傳送訊息",
        "你可以在其下次登入 Messenger 時傳送訊息",
        "还无法看到这个聊天",
        "還無法看到這個聊天",
        "你还无法看到这个聊天",
        "你還無法看到這個聊天",
        "can't access this chat",
        "cannot access this chat",
        "log in to Messenger",
        "You can send a message when they next log in to Messenger",
        "You can send a message when he next logs in to Messenger",
        "You can send a message when she next logs in to Messenger",
        "You can't see this chat yet",
        "You cannot see this chat yet",
        "ยังไม่สามารถเข้าถึงแชทนี้ได้",
        "ยังไม่สามารถเข้าถึงแชทนี้",
        "คุณจะส่งข้อความได้เมื่อ",
        "ใช้หรือรับชม Messenger ในครั้งถัดไป",
        "ใช้ Messenger ในครั้งถัดไป",
        "รับชม Messenger ในครั้งถัดไป",
        "เข้าสู่ระบบ Messenger",
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
        *GENERIC_CLOSE_LABELS,
        "Confirm",
        "确认",
        "確認",
        "确定",
        "確定",
        "关闭个人资料",
        "Close profile",
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
        "知道了",
        "我知道了",
        "OK",
    ]
    STRANGER_LIMIT_ALERT_TEXT = "你的陌生消息已达数量上限\n24 小时内可发送的陌生消息数量有上限。"
    MESSAGE_REQUEST_LIMIT_ALERT_TEXT = (
        "你已达到消息请求发送上限\n24 小时内可发起的新对话数量有限，请稍后再试。"
    )
    def __init__(self, page, task_cfg=None, window_label=None):
        self.page = page
        self.window_label = (window_label or "").strip()
        task_cfg = task_cfg or {}
        try:
            speed_factor = float(task_cfg.get("speed_factor", 1.0) or 1.0)
        except Exception:
            speed_factor = 1.0
        self.speed_factor = max(0.35, min(1.5, speed_factor))
        self.stranger_limit_cache_ms = int(task_cfg.get("stranger_limit_cache_ms", 1200) or 1200)
        self._last_limit_check_ts = 0.0
        self._last_limit_check_result = False
        self.delivery_confirm_timeout_ms = int(
            task_cfg.get("delivery_confirm_timeout_ms", 2600) or 2600
        )
        self.delivery_recheck_timeout_ms = max(self.delivery_confirm_timeout_ms, 4200)
        self.delivery_probe_trigger_timeout_ms = 3000
        self.delivery_settle_ms = int(
            task_cfg.get("delivery_settle_ms", 900) or 900
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
        self.chat_session_wait_timeout_ms = int(
            task_cfg.get("chat_session_wait_timeout_ms", 12000) or 12000
        )
        self.editor_ready_wait_timeout_ms = int(
            task_cfg.get("editor_ready_wait_timeout_ms", 8000) or 8000
        )

    def _prefix(self):
        return f"[{self.window_label}] " if self.window_label else ""

    def _safe_page_url(self, page=None):
        target_page = page or self.page
        if target_page is None:
            return ""
        try:
            return str(target_page.url or "").strip()
        except Exception:
            return ""

    def _is_reusable_facebook_url(self, url):
        return Scraper._is_reusable_facebook_url(self, url)

    def _is_checkpoint_surface(self, url):
        return Scraper._is_checkpoint_surface(self, url)

    def _has_real_facebook_surface(self, page=None):
        return Scraper._has_real_facebook_surface(self, page)

    def _score_runtime_page_candidate(self, page):
        url = self._safe_page_url(page)
        if not url:
            return -1
        lower_url = url.lower()
        if lower_url.startswith("chrome://"):
            return -1
        if self._is_checkpoint_surface(url):
            return 400
        if self._is_reusable_facebook_url(url) and self._has_real_facebook_surface(page):
            return 300
        if "facebook.com" in lower_url and not lower_url.startswith("about:blank"):
            return 120
        if lower_url.startswith("about:blank"):
            return 0
        return -1

    def _adopt_existing_facebook_page(self):
        try:
            current_url = self._safe_page_url(self.page)
            if self._is_checkpoint_surface(current_url):
                return True
            if self._is_reusable_facebook_url(current_url) and self._has_real_facebook_surface(self.page):
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
            best_page = None
            best_score = -1
            for candidate in candidate_pages:
                marker = id(candidate)
                if marker in seen:
                    continue
                seen.add(marker)
                if candidate.is_closed():
                    continue
                if candidate is self.page:
                    continue
                score = self._score_runtime_page_candidate(candidate)
                if score > best_score:
                    best_score = score
                    best_page = candidate
            if best_page is not None:
                self.page = best_page
                try:
                    self.page.bring_to_front()
                except Exception:
                    pass
                if self._is_checkpoint_surface(self._safe_page_url(self.page)):
                    log.warning(f"{self._prefix()}发送链已切换到 Facebook checkpoint 页面: {self.page.url}")
                else:
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
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
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

    def _wait_if_paused(self):
        pause_logged = False

        def _on_pause():
            nonlocal pause_logged
            if not pause_logged:
                log.info("⏸ 已暂停，等待继续...")
                pause_logged = True

        def _on_resume():
            if pause_logged:
                log.info("▶️ 已继续，恢复执行。")

        paused_seconds, _ = wait_if_runtime_paused(
            pause_env=self.ENV_PAUSE_FLAG,
            stop_env=None,
            poll_seconds=0.1,
            on_pause=_on_pause,
            on_resume=_on_resume,
        )
        return paused_seconds

    def _scale_ms(self, value, min_ms=80):
        try:
            scaled = int(float(value) * self.speed_factor)
        except Exception:
            scaled = int(value)
        return max(min_ms, scaled)

    def _chat_open_timeout_ms(self, phase="default"):
        total_timeout_ms = self._scale_ms(self.chat_session_wait_timeout_ms, min_ms=1600)
        phase_ratio_map = {
            "initial": 0.50,
            "retry": 0.35,
            "reopen": 0.35,
            "probe": 0.35,
            "postsend_retry": 0.35,
            "default": 1.0,
        }
        ratio = phase_ratio_map.get(str(phase or "default").strip(), 1.0)
        return max(1600, int(total_timeout_ms * ratio))

    def _editor_ready_timeout_ms(self, phase="default"):
        total_timeout_ms = self._scale_ms(self.editor_ready_wait_timeout_ms, min_ms=1500)
        phase_ratio_map = {
            "initial": 0.40,
            "probe": 0.35,
            "postsend_retry": 0.35,
            "default": 1.0,
        }
        ratio = phase_ratio_map.get(str(phase or "default").strip(), 1.0)
        return max(1500, int(total_timeout_ms * ratio))

    def _sleep(self, seconds, min_seconds=0.03):
        try:
            scaled = float(seconds) * self.speed_factor
        except Exception:
            scaled = float(seconds)
        time.sleep(max(min_seconds, scaled))

    def _run_pause_aware_action(self, action, timeout_ms, slice_ms=220, retry_interval=0.05):
        pause_logged = False

        def _on_pause():
            nonlocal pause_logged
            if not pause_logged:
                log.info("⏸ 已暂停，等待继续...")
                pause_logged = True

        def _on_resume():
            if pause_logged:
                log.info("▶️ 已继续，恢复执行。")

        return run_pause_aware_action(
            action,
            timeout_ms=timeout_ms,
            pause_env=self.ENV_PAUSE_FLAG,
            stop_env=None,
            slice_ms=slice_ms,
            retry_interval=retry_interval,
            on_pause=_on_pause,
            on_resume=_on_resume,
        )

    def _visible_timeout(self, default_ms=200, min_ms=80):
        return self._scale_ms(default_ms, min_ms=min_ms)

    def _build_surface_cleanup_state(
        self,
        has_chat_close=False,
        has_chat_editor=False,
        has_member_card=False,
        has_popup_closer=False,
    ):
        return {
            "has_chat_close": bool(has_chat_close),
            "has_chat_editor": bool(has_chat_editor),
            "has_member_card": bool(has_member_card),
            "has_popup_closer": bool(has_popup_closer),
        }

    def _empty_surface_cleanup_state(self):
        return self._build_surface_cleanup_state()

    def _inspect_surface_cleanup_state(self):
        try:
            if self.page.is_closed():
                return self._empty_surface_cleanup_state()
        except Exception:
            return self._empty_surface_cleanup_state()
        try:
            state = self.page.evaluate(
                """(payload) => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const isVisible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
                    };
                    const hasVisibleSelector = (selectors) => {
                        for (const selector of selectors || []) {
                            try {
                                if (Array.from(document.querySelectorAll(selector)).some((el) => isVisible(el))) {
                                    return true;
                                }
                            } catch (e) {}
                        }
                        return false;
                    };
                    const labels = payload.labels || [];
                    const dialogs = Array.from(
                        document.querySelectorAll(
                            'div[role="dialog"], div[aria-modal="true"], div[role="alertdialog"], div[role="alert"]'
                        )
                    ).filter((el) => {
                        if (!isVisible(el)) return false;
                        const rect = el.getBoundingClientRect();
                        return rect.width >= 180 && rect.height >= 80 && rect.right > window.innerWidth * 0.4;
                    });
                    const hasPopupCloser = dialogs.some((dialog) => {
                        return Array.from(
                            dialog.querySelectorAll('button, div[role="button"], [aria-label]')
                        ).some((el) => {
                            if (!isVisible(el)) return false;
                            const text = normalize(el.innerText);
                            const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
                            const combined = `${aria} ${text}`.trim();
                            return labels.some((token) => combined.includes(token));
                        });
                    });
                    return {
                        has_chat_close: hasVisibleSelector(payload.chatCloseSelectors || []),
                        has_chat_editor: hasVisibleSelector(payload.editorSelectors || []),
                        has_member_card: hasVisibleSelector(payload.memberCardCloseSelectors || []),
                        has_popup_closer: hasPopupCloser,
                    };
                }""",
                {
                    "chatCloseSelectors": self.CHAT_CLOSE_SELECTORS,
                    "editorSelectors": self.MESSAGE_EDITOR_SELECTORS,
                    "memberCardCloseSelectors": self.MEMBER_CARD_CLOSE_SELECTORS,
                    "labels": self.POPUP_CLOSE_LABELS,
                },
            )
            payload = state if isinstance(state, dict) else {}
            return self._build_surface_cleanup_state(
                has_chat_close=payload.get("has_chat_close"),
                has_chat_editor=payload.get("has_chat_editor"),
                has_member_card=payload.get("has_member_card"),
                has_popup_closer=payload.get("has_popup_closer"),
            )
        except Exception:
            return self._empty_surface_cleanup_state()

    def send_member_card_dm(self, target, probe_text, greeting_text, enable_delivery_probe=False):
        name = target.get("name") or ""
        user_id = target.get("user_id") or ""
        try:
            self._adopt_existing_facebook_page()
            if self._is_checkpoint_surface(self._safe_page_url(self.page)):
                log.warning(f"{self._prefix()}发送链起始即命中 Facebook checkpoint，停止当前窗口: {name}")
                return self._build_dm_result(
                    "blocked",
                    "account_restricted",
                    "当前账号命中 Facebook checkpoint，请更换账户。",
                )
            self._wait_if_paused()
            log.info(f"💬 正在处理成员卡片: {name} ({user_id})")
            cleanup_state = self._inspect_surface_cleanup_state()
            preclear_chat = (
                self._close_visible_chat_windows()
                if cleanup_state.get("has_chat_close") or cleanup_state.get("has_chat_editor")
                else 0
            )
            preclear_popup = (
                self._dismiss_interfering_popups(context=f"成员 {name} 入口前置清场")
                if cleanup_state.get("has_popup_closer")
                else 0
            )
            preclear_card = (
                self._dismiss_member_hover_card()
                if cleanup_state.get("has_member_card")
                else 0
            )
            preclear_overlay = self._detect_blocking_overlay() if cleanup_state.get("has_popup_closer") else None
            preclear_chat_left = (
                bool(self._locate_chat_session())
                if cleanup_state.get("has_chat_close") or cleanup_state.get("has_chat_editor")
                else False
            )
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
                result = self._build_stranger_limit_result(
                    name=name,
                    stage="preflight",
                )
                self._log_restriction_result(result)
                return self._restriction_to_dm_result(result)

            link = self._find_member_link(target)
            if not link:
                log.warning(f"未找到成员列表中的目标链接，当前保留成员待下次重试: {name}")
                return self._build_dm_result("error", "member_link_not_found")
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
                        self._run_pause_aware_action(
                            lambda step_timeout: link.scroll_into_view_if_needed(timeout=step_timeout),
                            timeout_ms=action_timeout,
                        )
                    except Exception:
                        refound_link = self._find_member_link(target)
                        if refound_link:
                            link = refound_link
                    self._wait_for_stable_box(link, max_rounds=3, delay=0.06)
            else:
                try:
                    self._run_pause_aware_action(
                        lambda step_timeout: link.scroll_into_view_if_needed(timeout=step_timeout),
                        timeout_ms=action_timeout,
                    )
                except Exception:
                    refound_link = self._find_member_link(target)
                    if refound_link:
                        link = refound_link
                self._wait_for_stable_box(link, max_rounds=3, delay=0.06)
            self._wait_if_paused()

            button, failure = self._resolve_member_card_message_button(link, name)
            if failure:
                self._log_restriction_result(failure)
                self._dismiss_member_hover_card()
                return self._restriction_to_dm_result(failure)

            # 只有在确定要点击发消息按钮时才关闭聊天窗口，避免无按钮时浪费时间
            self._wait_if_paused()
            ready_cleanup_state = self._inspect_surface_cleanup_state()
            if ready_cleanup_state.get("has_chat_close") or ready_cleanup_state.get("has_chat_editor"):
                self._close_visible_chat_windows()

            try:
                self._run_pause_aware_action(
                    lambda step_timeout: button.click(timeout=step_timeout),
                    timeout_ms=action_timeout,
                )
                log.info(f"{self._prefix()}发消息按钮点击成功: {name}")
            except Exception:
                try:
                    button.evaluate("el => el.click()")
                    log.info(f"{self._prefix()}发消息按钮点击成功(JS): {name}")
                except Exception:
                    click_failure = self._detect_prechat_restriction_result(
                        name,
                        stage="发消息按钮点击失败后复核",
                    )
                    if click_failure:
                        self._log_restriction_result(click_failure)
                        self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                        self._close_visible_chat_windows()
                        self._dismiss_member_hover_card()
                        return self._restriction_to_dm_result(click_failure)
                    log.warning(f"发消息按钮点击失败，当前保留成员待下次重试: {name}")
                    self._dismiss_member_hover_card()
                    return self._build_dm_result("error", "message_button_click_failed")
            self._wait_if_paused()

            chat_open_result = self._wait_for_chat_open_result(
                expected_name=name,
                timeout_ms=self._chat_open_timeout_ms("initial"),
                stage="聊天窗口首次复核",
            )
            chat_session, restriction, chat_shell_seen = self._extract_chat_open_result(chat_open_result)
            if not chat_session:
                if restriction:
                    self._log_restriction_result(restriction)
                    self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                    self._close_visible_chat_windows()
                    self._dismiss_member_hover_card()
                    self._log_limit_dialog_page_state(name)
                    return self._restriction_to_dm_result(restriction)
                closed_popups = self._dismiss_interfering_popups(context=f"成员 {name} 聊天窗口未打开")
                if closed_popups or chat_shell_seen:
                    chat_open_result = self._wait_for_chat_open_result(
                        expected_name=name,
                        timeout_ms=self._chat_open_timeout_ms("retry"),
                        stage="聊天窗口二次复核",
                    )
                    chat_session, restriction, retry_shell_seen = self._extract_chat_open_result(chat_open_result)
                    chat_shell_seen = chat_shell_seen or retry_shell_seen
                    if chat_session:
                        if closed_popups:
                            log.info(f"{self._prefix()}关闭干扰弹层后，聊天窗口恢复可用: {name}")
                    else:
                        if restriction:
                            self._log_restriction_result(restriction)
                            self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                            self._close_visible_chat_windows()
                            self._dismiss_member_hover_card()
                            self._log_limit_dialog_page_state(name)
                            return self._restriction_to_dm_result(restriction)
                if not chat_session:
                    overlay_text = self._detect_blocking_overlay()
                    if overlay_text:
                        overlay_restriction = self._build_restriction_result(
                            self._match_restriction_reason_from_text(overlay_text),
                            overlay_text,
                            name=name,
                            stage="聊天窗口被限制弹层阻断",
                        )
                        if overlay_restriction:
                            self._log_restriction_result(overlay_restriction)
                            self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                            self._close_visible_chat_windows()
                            self._dismiss_member_hover_card()
                            self._log_limit_dialog_page_state(name)
                            return self._restriction_to_dm_result(overlay_restriction)
                        self._dismiss_interfering_popups(context=f"成员 {name} 聊天窗口被弹层阻断")
                        self._close_visible_chat_windows()
                        self._dismiss_member_hover_card()
                        final_restriction = self._detect_prechat_restriction_result(
                            name,
                            stage="聊天窗口被弹层阻断后最终复核",
                        )
                        if final_restriction:
                            self._log_restriction_result(final_restriction)
                            self._log_limit_dialog_page_state(name)
                            return self._restriction_to_dm_result(final_restriction)
                        log.warning(
                            f"{self._prefix()}聊天窗口被干扰弹层阻断，当前保留成员待下次重试: {name} ({overlay_text or 'popup'})"
                        )
                        return self._build_dm_result("error", "chat_popup_blocking")
                if self._check_stranger_limit_global():
                    result = self._build_stranger_limit_result(
                        name=name,
                        stage="聊天窗口未打开前最终复核",
                    )
                    self._log_restriction_result(result)
                    return self._restriction_to_dm_result(result)
                final_restriction = self._detect_prechat_restriction_result(
                    name,
                    stage="聊天窗口未打开前最终复核",
                )
                if final_restriction:
                    self._log_restriction_result(final_restriction)
                    self._log_limit_dialog_page_state(name)
                    return self._restriction_to_dm_result(final_restriction)
                closed_chats = self._close_visible_chat_windows()
                if closed_chats > 0:
                    retry_button = None
                    try:
                        if button.is_visible(timeout=self._scale_ms(220, min_ms=100)):
                            retry_button = button
                    except Exception:
                        retry_button = None
                    if retry_button is None:
                        retry_link = self._find_member_link(target)
                        if retry_link:
                            retry_button, retry_failure = self._resolve_member_card_message_button(retry_link, name)
                            if retry_failure:
                                log.warning(
                                    f"{self._prefix()}聊天重试阶段未重新获取到发消息按钮，继续按当前聊天状态处理: {name}"
                                )
                                self._dismiss_member_hover_card()
                                retry_button = None
                    if retry_button is not None:
                        try:
                            self._run_pause_aware_action(
                                lambda step_timeout: retry_button.click(timeout=step_timeout),
                                timeout_ms=action_timeout,
                            )
                        except Exception:
                            try:
                                retry_button.evaluate("el => el.click()")
                            except Exception:
                                retry_button = None
                        if retry_button is not None:
                            retry_open_result = self._wait_for_chat_open_result(
                                expected_name=name,
                                timeout_ms=self._chat_open_timeout_ms("reopen"),
                                stage="聊天窗口重试复核",
                            )
                            chat_session, retry_restriction, retry_shell_seen = self._extract_chat_open_result(
                                retry_open_result
                            )
                            chat_shell_seen = chat_shell_seen or retry_shell_seen
                            if retry_restriction:
                                self._log_restriction_result(retry_restriction)
                                self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                                self._close_visible_chat_windows()
                                self._dismiss_member_hover_card()
                                self._log_limit_dialog_page_state(name)
                                return self._restriction_to_dm_result(retry_restriction)
                            if chat_session:
                                log.info(f"{self._prefix()}关闭残留聊天窗后，聊天重试恢复成功: {name}")
                
                if not chat_session:
                    self._dismiss_member_hover_card()
                    if chat_shell_seen:
                        alert_text = "右侧聊天窗口已出现，但未识别到可用输入框或限制提示，请检查当前窗口聊天状态。"
                        log.warning(
                            f"{self._prefix()}右侧聊天壳已出现，但未识别到可用输入框或限制提示，停止当前窗口: {name}"
                        )
                        return self._build_dm_result("blocked", "chat_shell_unrecognized", alert_text)
                    log.warning(f"{self._prefix()}聊天窗口未正常打开，当前保留成员待下次重试: {name}")
                    return self._build_dm_result("error", "chat_editor_not_found")

            delivery_state = "timeout"
            closed_chat = 0
            closed_card = 0
            try:
                ready_session = self._wait_for_ready_chat_session(
                    chat_session,
                    expected_name=name,
                    timeout_ms=self._editor_ready_timeout_ms("initial"),
                )
                if not ready_session:
                    log.warning(f"{self._prefix()}输入框未准备就绪，当前保留成员待下次重试: {name}")
                    return self._build_dm_result("error", "chat_editor_not_ready")
                chat_session = ready_session

                if self._check_stranger_limit_global():
                    result = self._build_stranger_limit_result(
                        name=name,
                        stage="chat_open",
                    )
                    self._log_restriction_result(result)
                    return self._restriction_to_dm_result(result)
                restriction = self._detect_chat_restriction(
                    expected_name=name,
                    stage="聊天窗口发送前复核",
                )
                if restriction:
                    self._log_restriction_result(restriction)
                    self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                    self._close_visible_chat_windows()
                    self._dismiss_member_hover_card()
                    self._log_limit_dialog_page_state(name)
                    return self._restriction_to_dm_result(restriction)

                primary_text = self._normalize_text(greeting_text) or self._normalize_text(probe_text)
                if not primary_text:
                    return self._build_dm_result("error", "empty_message")

                primary_delivery_timeout_ms = (
                    self.delivery_probe_trigger_timeout_ms
                    if enable_delivery_probe and self._normalize_text(probe_text)
                    else None
                )
                delivery_state, rebound_session = self._paste_and_send(
                    chat_session,
                    primary_text,
                    expected_name=name,
                    total_timeout_ms=primary_delivery_timeout_ms,
                )
                if rebound_session:
                    chat_session = rebound_session

                if delivery_state == "paste_failed":
                    log.warning(f"{self._prefix()}聊天输入框粘贴消息失败，当前保留成员待下次重试: {name}")
                    return self._build_dm_result("error", "chat_editor_paste_failed")

                if delivery_state == "limit_reached":
                    result = self._build_stranger_limit_result(
                        name=name,
                        stage="postsend",
                    )
                    self._log_restriction_result(result)
                    return self._restriction_to_dm_result(result)

                if delivery_state == "message_request_limit_reached":
                    result = self._build_message_request_limit_result(
                        name=name,
                        stage="postsend",
                    )
                    self._log_restriction_result(result)
                    return self._restriction_to_dm_result(result)

                if self._is_skip_restriction_reason(delivery_state):
                    result = self._build_skip_restriction_result(
                        delivery_state,
                        name=name,
                        stage="postsend",
                    )
                    self._log_restriction_result(result)
                    return self._restriction_to_dm_result(result)

                if delivery_state in ("failed", "send_restricted_ui"):
                    result = self._build_send_restricted_result(
                        name=name,
                        stage="postsend",
                        alert_text="聊天窗口明确提示发送失败，当前窗口停止继续发送，请更换账户。",
                    )
                    self._log_restriction_result(result)
                    return self._restriction_to_dm_result(result)

                if delivery_state == "sent":
                    log.success(f"✅ 成员页消息已发送: {name}")
                    return self._build_dm_result("sent", "sent")

                if delivery_state == "timeout":
                    if enable_delivery_probe and self._normalize_text(probe_text):
                        log.info(f"{self._prefix()}正文未确认，开始发送点赞型探针: {name}")
                        probe_state, probe_session = self._run_delivery_probe(
                            chat_session,
                            probe_text,
                            expected_name=name,
                        )
                        if probe_session:
                            chat_session = probe_session

                        if probe_state == "sent":
                            log.success(f"{self._prefix()}点赞型探针发送成功，当前窗口恢复正常: {name}")
                            return self._build_dm_result("skip", "delivery_probe_sent")

                        if probe_state == "limit_reached":
                            result = self._build_stranger_limit_result(
                                name=name,
                                stage="probe",
                            )
                            self._log_restriction_result(result)
                            return self._restriction_to_dm_result(result)

                        if probe_state == "message_request_limit_reached":
                            result = self._build_message_request_limit_result(
                                name=name,
                                stage="probe",
                            )
                            self._log_restriction_result(result)
                            return self._restriction_to_dm_result(result)

                        if self._is_skip_restriction_reason(probe_state):
                            result = self._build_skip_restriction_result(
                                probe_state,
                                name=name,
                                stage="probe",
                            )
                            self._log_restriction_result(result)
                            return self._restriction_to_dm_result(result)

                        if probe_state in ("failed", "send_restricted_ui"):
                            result = self._build_send_restricted_result(
                                name=name,
                                stage="probe",
                                alert_text="点赞探针发送后明确提示当前无法发送，当前窗口停止继续发送，请更换账户。",
                            )
                            self._log_restriction_result(result)
                            return self._restriction_to_dm_result(result)

                        log.warning(f"{self._prefix()}点赞型探针未确认成功，保留静默失败计数: {name}")
                    log.warning(f"发送消息在两轮确认后仍未命中，当前按未确认处理: {name}")
                    return self._build_dm_result("error", "delivery_not_confirmed")

                log.warning(f"发送状态未确认，不写入历史，等待下次重试: {name}")
                return self._build_dm_result("error", "delivery_not_confirmed")
            finally:
                close_delay_ms = (
                    self.success_close_delay_ms if delivery_state == "sent" else self.default_close_delay_ms
                )
                self._sleep(max(close_delay_ms, 0) / 1000, min_seconds=0.05)
                close_editor, close_button = self._extract_chat_session(chat_session)
                if not self._chat_editor_gone(close_editor):
                    refreshed_session = self._locate_chat_session(expected_name=name) or self._locate_chat_session()
                    if refreshed_session:
                        refreshed_editor, refreshed_close_button = self._extract_chat_session(refreshed_session)
                        close_button = refreshed_close_button or close_button
                        close_editor = refreshed_editor or close_editor
                close_wait_ms = self._scale_ms(2600 if delivery_state == "sent" else 1200, min_ms=480)
                closed_chat = self._close_chat_button(
                    close_button,
                    editor=close_editor,
                    auto_wait_ms=close_wait_ms,
                )
                closed_card = self._dismiss_member_hover_card()
                log.info(
                    f"{self._prefix()}成员 {name} 收尾完成：关闭聊天={closed_chat}，"
                    f"关闭资料卡={closed_card}。"
                )
        except Exception as exc:
            self._close_visible_chat_windows()
            log.error(f"⚠️ 成员页发送流程中断: {exc}")
            return self._build_dm_result("error", repr(exc))

    def _run_delivery_probe(self, chat_session, probe_text, expected_name=None):
        normalized_probe = self._normalize_text(probe_text)
        if not normalized_probe:
            return "timeout", chat_session

        probe_session = None
        session_editor, _ = self._extract_chat_session(chat_session)
        if chat_session and not self._chat_editor_gone(session_editor):
            probe_session = chat_session
        if not probe_session:
            probe_session = self._locate_chat_session(expected_name=expected_name)
        if not probe_session:
            probe_open_result = self._wait_for_chat_open_result(
                expected_name=expected_name,
                timeout_ms=self._chat_open_timeout_ms("probe"),
                stage="点赞探针前复核",
            )
            probe_session, probe_restriction = self._extract_postsend_open_result(probe_open_result)
            if probe_restriction:
                return probe_restriction, None
        if not probe_session:
            return self._resolve_postsend_timeout_result(expected_name=expected_name)

        probe_editor, _ = self._extract_chat_session(probe_session)
        if not probe_editor:
            return "timeout", probe_session

        ready_probe_session = self._wait_for_ready_chat_session(
            probe_session,
            expected_name=expected_name,
            timeout_ms=self._editor_ready_timeout_ms("probe"),
        )
        if not ready_probe_session:
            return self._resolve_postsend_timeout_result(
                expected_name=expected_name,
                session=probe_session,
            )
        probe_session = ready_probe_session

        probe_state, rebound_session = self._paste_and_send(
            probe_session,
            normalized_probe,
            expected_name=expected_name,
        )
        return probe_state, rebound_session or probe_session

    def _wait_for_chat_session_text(self, chat_session, text, expected_name=None, timeout_ms=None):
        normalized_text = self._normalize_text(text)
        if not normalized_text:
            return None
        timeout_ms = self._scale_ms(timeout_ms or max(1200, len(normalized_text) * 18), min_ms=900)
        start = time.time()
        deadline = start + timeout_ms / 1000
        current_session = chat_session
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds

            editor, close_button = self._extract_chat_session(current_session)
            if editor and self._editor_contains_text(editor, text):
                return self._build_chat_session(editor, close_button=close_button)

            refreshed_session = self._locate_chat_session(expected_name=expected_name) or self._locate_chat_session()
            if refreshed_session:
                refreshed_editor, refreshed_close_button = self._extract_chat_session(refreshed_session)
                merged_close_button = refreshed_close_button or close_button
                current_session = self._build_chat_session(
                    refreshed_editor,
                    close_button=merged_close_button,
                )
                if refreshed_editor and self._editor_contains_text(refreshed_editor, text):
                    return current_session

            self._sleep(0.08, min_seconds=0.02)
        return None

    def _paste_text_into_chat_session(self, chat_session, text, expected_name=None):
        normalized_text = self._normalize_text(text)
        if not normalized_text:
            return None
        current_session = chat_session
        editor, close_button = self._extract_chat_session(current_session)
        if not editor:
            return None
        is_macos = (
            os.name != "nt"
            and getattr(os, "uname", None) is not None
            and os.uname().sysname == "Darwin"
        )
        select_all_shortcut = "Meta+A" if is_macos else "Control+A"
        paste_shortcut = "Meta+V" if is_macos else "Control+V"
        try:
            self._run_pause_aware_action(
                lambda step_timeout: editor.click(timeout=step_timeout),
                timeout_ms=self._scale_ms(1200, min_ms=600),
            )
        except Exception:
            return None
        self._sleep(0.04, min_seconds=0.01)
        try:
            self._run_pause_aware_action(
                lambda step_timeout: editor.press(
                    select_all_shortcut,
                    timeout=step_timeout,
                ),
                timeout_ms=self._scale_ms(600, min_ms=240),
                slice_ms=180,
            )
            self._sleep(0.02, min_seconds=0.01)
            self._run_pause_aware_action(
                lambda step_timeout: editor.press("Backspace", timeout=step_timeout),
                timeout_ms=self._scale_ms(600, min_ms=240),
                slice_ms=180,
            )
        except Exception:
            pass
        self._sleep(0.04, min_seconds=0.01)
        if not self._editor_empty(editor):
            try:
                clear_timeout_ms = self._scale_ms(900, min_ms=420)
                self._run_pause_aware_action(
                    lambda step_timeout: editor.fill("", timeout=step_timeout),
                    timeout_ms=clear_timeout_ms,
                    slice_ms=clear_timeout_ms,
                )
            except Exception:
                pass
            self._sleep(0.04, min_seconds=0.01)
        if not self._editor_empty(editor):
            refreshed_session = self._locate_chat_session(expected_name=expected_name) or self._locate_chat_session()
            if refreshed_session:
                refreshed_editor, refreshed_close_button = self._extract_chat_session(refreshed_session)
                merged_close_button = refreshed_close_button or close_button
                current_session = self._build_chat_session(
                    refreshed_editor,
                    close_button=merged_close_button,
                )
                editor, close_button = self._extract_chat_session(current_session)
        if not self._editor_empty(editor):
            return None
        try:
            if self._write_text_to_clipboard(text):
                paste_timeout_ms = self._scale_ms(1200, min_ms=720)
                self._run_pause_aware_action(
                    lambda step_timeout: editor.press(paste_shortcut, timeout=step_timeout),
                    timeout_ms=paste_timeout_ms,
                    slice_ms=paste_timeout_ms,
                )
                self._sleep(0.08, min_seconds=0.02)
                pasted_session = self._wait_for_chat_session_text(
                    current_session,
                    text,
                    expected_name=expected_name,
                    timeout_ms=self._scale_ms(max(1200, len(normalized_text) * 20), min_ms=900),
                )
                if pasted_session:
                    return pasted_session
        except Exception:
            pass
        try:
            if not editor or self._chat_editor_gone(editor):
                refreshed_session = self._locate_chat_session(expected_name=expected_name) or self._locate_chat_session()
                if refreshed_session:
                    refreshed_editor, refreshed_close_button = self._extract_chat_session(refreshed_session)
                    merged_close_button = refreshed_close_button or close_button
                    current_session = self._build_chat_session(
                        refreshed_editor,
                        close_button=merged_close_button,
                    )
                    editor, close_button = self._extract_chat_session(current_session)
            if not editor:
                return None
            fill_timeout_ms = self._scale_ms(max(1200, len(normalized_text) * 24), min_ms=900)
            self._run_pause_aware_action(
                lambda step_timeout: editor.fill(text, timeout=step_timeout),
                timeout_ms=fill_timeout_ms,
                slice_ms=fill_timeout_ms,
            )
            self._sleep(0.05, min_seconds=0.02)
            return self._wait_for_chat_session_text(
                current_session,
                text,
                expected_name=expected_name,
                timeout_ms=fill_timeout_ms,
            )
        except Exception:
            return None

    def _write_text_to_clipboard(self, text):
        normalized_text = self._normalize_text(text)
        if not normalized_text or not self.page:
            return False
        try:
            origin = None
            try:
                page_url = str(self.page.url or "").strip()
                if page_url.startswith(("http://", "https://")):
                    parsed = urlsplit(page_url)
                    if parsed.scheme and parsed.netloc:
                        origin = f"{parsed.scheme}://{parsed.netloc}"
            except Exception:
                origin = None
            try:
                if origin:
                    self.page.context.grant_permissions(["clipboard-read", "clipboard-write"], origin=origin)
                else:
                    self.page.context.grant_permissions(["clipboard-read", "clipboard-write"])
            except Exception:
                pass

            copy_timeout_ms = self._scale_ms(max(1200, len(normalized_text) * 18), min_ms=900)
            copied = self._run_pause_aware_action(
                lambda step_timeout: self.page.evaluate(
                    """async (payload) => {
                        const text = String((payload || {}).text || '');
                        if (!text) return false;
                        try {
                            if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') {
                                return false;
                            }
                            await navigator.clipboard.writeText(text);
                            return true;
                        } catch (error) {
                            return false;
                        }
                    }""",
                    {"text": text},
                ),
                timeout_ms=copy_timeout_ms,
                slice_ms=copy_timeout_ms,
            )
            return bool(copied)
        except Exception:
            return False

    def _map_postsend_restriction_to_delivery_state(self, restriction):
        if not restriction:
            return None
        status, reason, _ = self._extract_restriction_payload(restriction)
        if reason == "stranger_message_limit_reached" or status == "limit":
            return "limit_reached"
        if reason == "message_request_limit_reached":
            return "message_request_limit_reached"
        if self._is_skip_restriction_reason(reason):
            return reason
        if reason == "send_restricted_ui":
            return "send_restricted_ui"
        return None

    def _check_postsend_delivery_restriction(self, expected_name=None):
        if self._check_stranger_limit_global():
            return "limit_reached"
        restriction = self._detect_chat_restriction(
            expected_name=expected_name,
            stage="postsend",
        )
        return self._map_postsend_restriction_to_delivery_state(restriction)

    def _extract_postsend_open_result(self, open_result):
        chat_session, restriction, _ = self._extract_chat_open_result(open_result)
        return chat_session, self._map_postsend_restriction_to_delivery_state(restriction)

    def _infer_postsend_success_from_session(self, session):
        editor, _ = self._extract_chat_session(session)
        if not editor:
            return False
        if self._chat_editor_gone(editor):
            return False
        return self._editor_empty(editor)

    def _resolve_postsend_delivery_state(self, expected_name=None, default_state="sent"):
        postsend_restriction = self._check_postsend_delivery_restriction(
            expected_name=expected_name,
        )
        return postsend_restriction or default_state

    def _resolve_postsend_timeout_result(self, expected_name=None, session=None):
        if self._infer_postsend_success_from_session(session):
            return self._resolve_postsend_delivery_state(
                expected_name=expected_name,
                default_state="sent",
            ), session
        return self._resolve_postsend_delivery_state(
            expected_name=expected_name,
            default_state="timeout",
        ), session

    def _retry_delivery_confirmation(
        self,
        editor,
        text,
        before_state,
        expected_name=None,
        timeout_ms=None,
    ):
        log.info(
            f"{self._prefix()}发送确认首轮未命中，保留当前聊天窗口并进行二次核对: "
            f"{expected_name or 'unknown'}"
        )
        timeout_ms = timeout_ms or self.delivery_recheck_timeout_ms
        postsend_restriction = self._check_postsend_delivery_restriction(
            expected_name=expected_name,
        )
        if postsend_restriction:
            return postsend_restriction, None

        rebound_session = None
        if not self._chat_editor_gone(editor):
            rebound_session = self._locate_chat_session(expected_name=expected_name) or self._build_chat_session(
                editor,
                close_button=None,
            )
        if not rebound_session:
            rebound_open_result = self._wait_for_chat_open_result(
                expected_name=expected_name,
                timeout_ms=self._chat_open_timeout_ms("postsend_retry"),
                stage="发送后二次确认复核",
            )
            rebound_session, rebound_restriction = self._extract_postsend_open_result(rebound_open_result)
            if rebound_restriction:
                return rebound_restriction, None
        if not rebound_session:
            return self._resolve_postsend_timeout_result(expected_name=expected_name)

        rebound_editor, _ = self._extract_chat_session(rebound_session)
        if not rebound_editor:
            return "timeout", rebound_session

        ready_rebound_session = self._wait_for_ready_chat_session(
            rebound_session,
            expected_name=expected_name,
            timeout_ms=self._editor_ready_timeout_ms("postsend_retry"),
        )
        if not ready_rebound_session:
            return self._resolve_postsend_timeout_result(
                expected_name=expected_name,
                session=rebound_session,
            )
        rebound_session = ready_rebound_session
        rebound_editor, _ = self._extract_chat_session(rebound_session)

        second_pass_state = self._wait_for_delivery(
            rebound_editor,
            text,
            before_state,
            expected_name=expected_name,
            timeout_ms=timeout_ms,
        )
        if second_pass_state == "timeout":
            return self._resolve_postsend_timeout_result(
                expected_name=expected_name,
                session=rebound_session,
            )
        return second_pass_state, rebound_session

    def _paste_and_send(self, chat_session, text, expected_name=None, total_timeout_ms=None):
        normalized_text = self._normalize_text(text)
        editor, _ = self._extract_chat_session(chat_session)
        if not editor:
            return "paste_failed", chat_session
        before_state = self._normalize_delivery_state(self._get_delivery_state(editor, text))
        active_session = self._paste_text_into_chat_session(
            chat_session,
            text,
            expected_name=expected_name,
        )
        if not active_session:
            return "paste_failed", chat_session
        presend_session = self._wait_for_chat_session_text(
            active_session,
            text,
            expected_name=expected_name,
            timeout_ms=self._scale_ms(max(900, len(normalized_text) * 12), min_ms=720),
        )
        if presend_session:
            active_session = presend_session
        editor, _ = self._extract_chat_session(active_session)
        if not editor:
            return "paste_failed", active_session
        self._sleep(0.04, min_seconds=0.01)
        try:
            enter_timeout_ms = self._scale_ms(900, min_ms=360)
            self._run_pause_aware_action(
                lambda step_timeout: editor.press("Enter", timeout=step_timeout),
                timeout_ms=enter_timeout_ms,
                slice_ms=enter_timeout_ms,
            )
        except Exception:
            return "paste_failed", active_session
        first_timeout_ms = self.delivery_confirm_timeout_ms
        retry_timeout_ms = None
        if total_timeout_ms is not None:
            try:
                bounded_total_timeout_ms = int(total_timeout_ms)
            except Exception:
                bounded_total_timeout_ms = None
            if bounded_total_timeout_ms is not None and bounded_total_timeout_ms > 0:
                first_timeout_ms = min(self.delivery_confirm_timeout_ms, bounded_total_timeout_ms)
                retry_timeout_ms = max(bounded_total_timeout_ms - first_timeout_ms, 0)
        delivery_state = self._wait_for_delivery(
            editor,
            text,
            before_state,
            expected_name=expected_name,
            timeout_ms=first_timeout_ms,
        )
        if delivery_state != "timeout":
            return delivery_state, active_session
        if retry_timeout_ms is not None and retry_timeout_ms <= 0:
            return "timeout", active_session
        retry_state, rebound_session = self._retry_delivery_confirmation(
            editor,
            text,
            before_state,
            expected_name=expected_name,
            timeout_ms=retry_timeout_ms,
        )
        return retry_state, rebound_session or active_session

    def _wait_for_delivery(self, editor, text, before_state, expected_name=None, timeout_ms=None):
        timeout_ms = timeout_ms or self.delivery_confirm_timeout_ms
        before_state = self._normalize_delivery_state(before_state)
        before_hits = before_state["message_hits"]
        before_fingerprint = before_state["window_fingerprint"]
        fast_settle_ms = min(self.delivery_settle_ms, 350)
        commit_settle_ms = min(self.delivery_settle_ms, 220)
        weak_commit_settle_ms = max(self.delivery_settle_ms, 650)
        deadline = time.time() + timeout_ms / 1000
        success_seen_at = None
        success_mode = None
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                deadline += paused_seconds
                if success_seen_at is not None:
                    success_seen_at += paused_seconds
            state = self._normalize_delivery_state(self._get_delivery_state(editor, text))
            if not state["state_ready"]:
                if self._check_stranger_limit_global():
                    return "limit_reached"
                self._sleep(0.12)
                continue

            editor_empty = self._editor_empty(editor)
            fingerprint_changed = (
                bool(before_fingerprint)
                and bool(state["window_fingerprint"])
                and state["window_fingerprint"] != before_fingerprint
            )

            if state["has_stranger_limit"]:
                return "limit_reached"
            if state.get("has_message_request_limit"):
                return "message_request_limit_reached"
            if self._check_stranger_limit_global():
                return "limit_reached"

            if state["has_failure"]:
                return state.get("failure_reason") or "failed"

            if state["has_status"] or state["has_sent_time"]:
                return self._resolve_postsend_delivery_state(
                    expected_name=expected_name,
                    default_state="sent",
                )

            if state["message_hits"] > before_hits:
                strong_increment = (
                    (not before_fingerprint or fingerprint_changed or not state["window_fingerprint"])
                    and editor_empty
                )
                settle_ms = fast_settle_ms if strong_increment else self.delivery_settle_ms
                current_mode = "strong_increment" if strong_increment else "increment"
                if success_seen_at is None or success_mode != current_mode:
                    success_seen_at = time.time()
                    success_mode = current_mode
                elif (time.time() - success_seen_at) * 1000 >= settle_ms:
                    return self._resolve_postsend_delivery_state(
                        expected_name=expected_name,
                        default_state="sent",
                    )
            elif editor_empty and fingerprint_changed:
                current_mode = "editor_commit"
                if success_seen_at is None or success_mode != current_mode:
                    success_seen_at = time.time()
                    success_mode = current_mode
                elif (time.time() - success_seen_at) * 1000 >= commit_settle_ms:
                    return self._resolve_postsend_delivery_state(
                        expected_name=expected_name,
                        default_state="sent",
                    )
            elif editor_empty:
                current_mode = "weak_editor_commit"
                if success_seen_at is None or success_mode != current_mode:
                    success_seen_at = time.time()
                    success_mode = current_mode
                elif (time.time() - success_seen_at) * 1000 >= weak_commit_settle_ms:
                    return self._resolve_postsend_delivery_state(
                        expected_name=expected_name,
                        default_state="sent",
                    )
            else:
                success_seen_at = None
                success_mode = None
            self._sleep(0.12)
        return self._resolve_postsend_delivery_state(
            expected_name=expected_name,
            default_state="timeout",
        )

    def _build_delivery_state(self, state_ready=False, state=None):
        payload = state if isinstance(state, dict) else {}
        return {
            "state_ready": bool(state_ready),
            "message_hits": int(payload.get("message_hits") or 0),
            "has_status": bool(payload.get("has_status")),
            "has_failure": bool(payload.get("has_failure")),
            "has_stranger_limit": bool(payload.get("has_stranger_limit")),
            "has_message_request_limit": bool(payload.get("has_message_request_limit")),
            "has_sent_time": bool(payload.get("has_sent_time")),
            "window_fingerprint": self._normalize_text(payload.get("window_fingerprint") or ""),
            "failure_reason": self._normalize_text(payload.get("failure_reason") or ""),
        }

    def _normalize_delivery_state(self, state):
        if not isinstance(state, dict):
            return self._build_delivery_state(state_ready=False)
        return self._build_delivery_state(state_ready=True, state=state)

    def _editor_empty(self, editor):
        try:
            content = editor.evaluate(
                """(el) => {
                    if (!el) return '';
                    if (el.isContentEditable || el.getAttribute('contenteditable') === 'true') {
                        return (el.innerText || el.textContent || '');
                    }
                    if ('value' in el) {
                        return el.value || '';
                    }
                    return el.textContent || '';
                }"""
            )
            return not self._normalize_text(content)
        except Exception:
            return False

    def _editor_contains_text(self, editor, text):
        normalized_text = self._normalize_text(text)
        if not normalized_text:
            return False
        try:
            return bool(
                editor.evaluate(
                    """(el, payload) => {
                        const text = String((payload || {}).text || '');
                        const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
                        if (!el || !text) return false;
                        const actual = el.isContentEditable || el.getAttribute('contenteditable') === 'true'
                            ? (el.innerText || el.textContent || '')
                            : ('value' in el ? el.value : (el.textContent || ''));
                        return normalize(actual).includes(normalize(text));
                    }""",
                    {"text": text},
                )
            )
        except Exception:
            return False

    def _detect_chat_restriction(self, expected_name=None, stage=""):
        payload = {
            "strangerLimitTexts": self.STRANGER_LIMIT_TEXTS,
            "messageRequestLimitTexts": self.MESSAGE_REQUEST_LIMIT_TEXTS,
            "messengerLoginRequiredTexts": self.MESSENGER_LOGIN_REQUIRED_TEXTS,
            "accountCannotMessageTexts": self.ACCOUNT_CANNOT_MESSAGE_TEXTS,
            "sendRestrictedTexts": self.SEND_RESTRICTED_TEXTS,
            "failureTexts": self.DELIVERY_FAILURE_TEXTS,
            "chatCloseSelectors": self.CHAT_CLOSE_SELECTORS,
            "editorSelectors": self.MESSAGE_EDITOR_SELECTORS,
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
                        const matchesAnySelectorInside = (root, selectors) => {
                            for (const selector of selectors || []) {
                                try {
                                    if (Array.from(root.querySelectorAll(selector)).some((el) => isVisibleArea(el))) {
                                        return true;
                                    }
                                } catch (error) {
                                }
                            }
                            return false;
                        };
                        const containsRestrictionText = (text) => {
                            if (!text) return false;
                            const lowerText = String(text || '').toLowerCase();
                            if ((payload.strangerLimitTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.messageRequestLimitTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.messengerLoginRequiredTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.accountCannotMessageTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.failureTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.sendRestrictedTexts || []).some((token) => text.includes(token))) return true;
                            if (
                                (
                                    lowerText.includes('24 hours')
                                    && (lowerText.includes('message request') || lowerText.includes('requests'))
                                )
                                || (
                                    text.includes('24 ชั่วโมง')
                                    && (
                                        text.includes('คำขอส่งข้อความ')
                                        || text.includes('ส่งคำขอ')
                                        || (text.includes('คำขอ') && text.includes('ขีดจำกัด'))
                                    )
                                )
                            ) {
                                return true;
                            }
                            return false;
                        };
                        const isChatShell = (root) => {
                            if (!root || !isVisibleArea(root)) return false;
                            const rect = root.getBoundingClientRect();
                            if (rect.right < window.innerWidth * 0.55) return false;
                            if (matchesAnySelectorInside(root, payload.editorSelectors)) return true;
                            if (matchesAnySelectorInside(root, payload.chatCloseSelectors)) return true;
                            const expectedName = normalize(payload.expectedName);
                            const shellText = normalize(
                                `${root.innerText || ''} ${root.getAttribute && root.getAttribute('aria-label') || ''}`
                            );
                            if (expectedName && shellText.includes(expectedName) && containsRestrictionText(shellText)) {
                                return true;
                            }
                            return false;
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
                            if (!isChatShell(el)) return false;
                            return true;
                        });
                        const pickReason = (text) => {
                            const lowerText = String(text || '').toLowerCase();
                            if (payload.strangerLimitTexts.some((token) => text.includes(token))) {
                                return {reason: 'stranger_message_limit_reached', alert_text: text};
                            }
                            if (payload.messageRequestLimitTexts.some((token) => text.includes(token))) {
                                return {reason: 'message_request_limit_reached', alert_text: text};
                            }
                            if (
                                (
                                    lowerText.includes('24 hours')
                                    && (lowerText.includes('message request') || lowerText.includes('requests'))
                                )
                                || (
                                    text.includes('24 ชั่วโมง')
                                    && (
                                        text.includes('คำขอส่งข้อความ')
                                        || text.includes('ส่งคำขอ')
                                        || (text.includes('คำขอ') && text.includes('ขีดจำกัด'))
                                    )
                                )
                            ) {
                                return {reason: 'message_request_limit_reached', alert_text: text};
                            }
                            if ((payload.messengerLoginRequiredTexts || []).some((token) => text.includes(token))) {
                                return {reason: 'messenger_login_required', alert_text: text};
                            }
                            if (payload.accountCannotMessageTexts.some((token) => text.includes(token))) {
                                return {reason: 'account_cannot_message', alert_text: text};
                            }
                            if (payload.failureTexts.some((token) => text.includes(token))) {
                                return {reason: 'send_restricted_ui', alert_text: text};
                            }
                            if (payload.sendRestrictedTexts.some((token) => text.includes(token))) {
                                return {reason: 'send_restricted_ui', alert_text: text};
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

                        const chatShellCandidates = [];
                        const collectShellCandidate = (seed, rect, bonus = 0) => {
                            let shell = seed;
                            let bestShell = null;
                            let bestArea = Number.POSITIVE_INFINITY;
                            while (shell) {
                                if (isChatShell(shell)) {
                                    const shellRect = shell.getBoundingClientRect();
                                    const area = shellRect.width * shellRect.height;
                                    if (area < bestArea) {
                                        bestArea = area;
                                        bestShell = shell;
                                    }
                                }
                                shell = shell.parentElement;
                            }
                            if (!bestShell) return;
                            const text = normalize(
                                `${bestShell.innerText || ''} ${bestShell.getAttribute && bestShell.getAttribute('aria-label') || ''}`
                            );
                            if (!text) return;
                            let score = bonus;
                            if (expectedName && text.includes(expectedName)) score += 1000;
                            try {
                                if (bestShell.contains(document.activeElement)) score += 500;
                            } catch (error) {
                            }
                            score += Math.round((rect && rect.x) || 0);
                            chatShellCandidates.push({ score, text });
                        };
                        for (const selector of payload.chatCloseSelectors || []) {
                            for (const button of Array.from(document.querySelectorAll(selector))) {
                                if (!isVisibleArea(button)) continue;
                                const buttonRect = button.getBoundingClientRect();
                                if (buttonRect.right < window.innerWidth * 0.55) continue;
                                collectShellCandidate(button.parentElement, buttonRect, 220);
                            }
                        }
                        for (const selector of payload.editorSelectors || []) {
                            for (const editor of Array.from(document.querySelectorAll(selector))) {
                                if (!isVisibleArea(editor)) continue;
                                const editorRect = editor.getBoundingClientRect();
                                if (editorRect.right < window.innerWidth * 0.55) continue;
                                collectShellCandidate(editor.parentElement, editorRect, 320);
                            }
                        }
                        chatShellCandidates.sort((a, b) => b.score - a.score);
                        const seenShellTexts = new Set();
                        for (const candidate of chatShellCandidates) {
                            if (!candidate || !candidate.text || seenShellTexts.has(candidate.text)) continue;
                            seenShellTexts.add(candidate.text);
                            const hit = pickReason(candidate.text);
                            if (hit) return hit;
                        }

                        if (expectedName) {
                            const genericShellCandidates = [];
                            for (const root of Array.from(document.querySelectorAll('div, aside, section, article'))) {
                                if (!isVisibleArea(root)) continue;
                                const rect = root.getBoundingClientRect();
                                if (!rect.width || !rect.height) continue;
                                if (rect.right < window.innerWidth * 0.55) continue;
                                if (rect.width < 180 || rect.height < 120) continue;
                                const text = normalize(
                                    `${root.innerText || ''} ${root.getAttribute && root.getAttribute('aria-label') || ''}`
                                );
                                if (!text || !text.includes(expectedName) || !containsRestrictionText(text)) continue;
                                genericShellCandidates.push({
                                    area: rect.width * rect.height,
                                    x: rect.x,
                                    y: rect.y,
                                    text,
                                });
                            }
                            genericShellCandidates.sort((a, b) => {
                                if (a.area !== b.area) return a.area - b.area;
                                if (a.x !== b.x) return b.x - a.x;
                                return a.y - b.y;
                            });
                            const seenGenericTexts = new Set();
                            for (const candidate of genericShellCandidates) {
                                if (!candidate || !candidate.text || seenGenericTexts.has(candidate.text)) continue;
                                seenGenericTexts.add(candidate.text);
                                const hit = pickReason(candidate.text);
                                if (hit) return hit;
                            }
                        }
                        return null;
                    }""",
                    payload,
                )
                if not result:
                    continue
                _, reason, alert_text = self._extract_restriction_payload(result)
                restriction_result = self._build_restriction_result(
                    reason,
                    alert_text,
                    name=expected_name or "",
                    stage=stage,
                )
                if restriction_result:
                    return restriction_result
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
                        const isVisible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return false;
                            if (rect.bottom < 0 || rect.top > window.innerHeight) return false;
                            const style = window.getComputedStyle(el);
                            if (!style) return false;
                            return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
                        };
                        const nodes = Array.from(
                            document.querySelectorAll(
                                'div[role="dialog"], div[role="alertdialog"], div[role="alert"], div[role="status"], [aria-live]'
                            )
                        );
                        return nodes.some((el) => {
                            if (!isVisible(el)) return false;
                            const rect = el.getBoundingClientRect();
                            if (rect.width < 140 || rect.height < 24) return false;
                            if (rect.right < window.innerWidth * 0.3 && rect.width < window.innerWidth * 0.5) {
                                return false;
                            }
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
                    const messageTexts = payload.messageTexts || [];
                    const messageHintKeywords = (payload.messageHintKeywords || [])
                        .map((token) => normalize(token).toLowerCase())
                        .filter(Boolean);
                    const ignoredControlTextsLower = new Set(
                        (payload.ignoredControlTexts || [])
                            .map((token) => normalize(token).toLowerCase())
                            .filter(Boolean)
                    );
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
                        const textValueLower = textValue.toLowerCase();
                        const aria = normalize(current.getAttribute && current.getAttribute('aria-label'));
                        const ariaLower = aria.toLowerCase();
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
                            if (hasAnyToken(aria, messageTexts) || hasAnyToken(ariaLower, messageHintKeywords)) score += 4;
                            if (hasAnyToken(textValue, messageTexts) || hasAnyToken(textValueLower, messageHintKeywords)) score += 2;
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
                    const seen = new Set();
                    const fingerprintParts = [];
                    const sentTimeRe = /(刚刚发送|剛剛傳送|已發送|\\d+\\s*(秒|分钟|分鐘|小时|小時|天)前发送|\\d+\\s*(seconds?|minutes?|hours?|days?)\\s+ago|hace\\s+\\d+\\s*(segundos?|minutos?|horas?|días?)|há\\s+\\d+\\s*(segundos?|minutos?|horas?|dias?)|il\\s+y\\s+a\\s+\\d+\\s*(secondes?|minutes?|heures?|jours?)|vor\\s+\\d+\\s*(Sekunden?|Minuten?|Stunden?|Tagen?)|\\d+\\s*(secondi|minuti|ore|giorni)\\s+fa|\\d+\\s*(секунд|секунды|минут|минуты|часов|часа|дней|дня)\\s+назад|\\d+\\s*(sekund|minut|godzin|dni)\\s+temu|\\d+\\s*(saniye|dakika|saat|gün)\\s+önce|منذ\\s+\\d+\\s*(ثانية|دقيقة|ساعة|يوم)|\\d+\\s*(秒|分|時間|日)前|\\d+\\s*(초|분|시간|일)\\s*전|\\d+\\s*(วินาที|นาที|ชั่วโมง|วัน)ที่แล้ว|\\d+\\s*(giây|phút|giờ|ngày)\\s+trước|\\d+\\s*(detik|menit|jam|hari)\\s+yang\\s+lalu|\\d+\\s*(saat|minit|jam|hari)\\s+yang\\s+lalu|\\d+\\s*(সেকেন্ড|মিনিট|ঘণ্টা|দিন)\\s+আগে|\\d+\\s*(सेकंड|मिनट|घंटे|दिन)\\s+पहले)/i;

                    for (const el of Array.from(root.querySelectorAll('div, span, a, svg, img'))) {
                        if (el === node || node.contains(el)) continue;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;

                        const text = normalize(el.innerText || '');
                        const aria = (el.getAttribute && el.getAttribute('aria-label')) || '';
                        const combined = normalize(text || aria || '');
                        const combinedLower = combined.toLowerCase();
                        if (!combined) continue;
                        if (ignoredControlTextsLower.has(combinedLower)) continue;

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

                    const windowText = normalize(root.innerText || '');

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
                    "messageTexts": self.MEMBER_CARD_MESSAGE_TEXTS,
                    "messageHintKeywords": self.MESSAGE_HEADING_HINT_TOKENS,
                    "ignoredControlTexts": self.CHAT_IGNORED_CONTROL_TEXTS,
                },
            )
        except Exception:
            return None

    def _normalize_text(self, value):
        return " ".join((value or "").split())

    def _match_restriction_reason_from_text(self, text):
        normalized = self._normalize_text(text)
        if not normalized:
            return None
        lower_text = normalized.lower()
        if self._matches_any_text(normalized, self.STRANGER_LIMIT_TEXTS):
            return "stranger_message_limit_reached"
        if self._matches_any_text(normalized, self.MESSAGE_REQUEST_LIMIT_TEXTS):
            return "message_request_limit_reached"
        if (
            (
                "24 hours" in lower_text
                and ("message request" in lower_text or "requests" in lower_text)
            )
            or (
                "24 ชั่วโมง" in normalized
                and (
                    "คำขอส่งข้อความ" in normalized
                    or "ส่งคำขอ" in normalized
                    or ("คำขอ" in normalized and "ขีดจำกัด" in normalized)
                )
            )
        ):
            return "message_request_limit_reached"
        if self._matches_any_text(normalized, self.MESSENGER_LOGIN_REQUIRED_TEXTS):
            return "messenger_login_required"
        if self._matches_any_text(normalized, self.ACCOUNT_CANNOT_MESSAGE_TEXTS):
            return "account_cannot_message"
        if self._matches_any_text(normalized, self.DELIVERY_FAILURE_TEXTS):
            return "send_restricted_ui"
        if self._matches_any_text(normalized, self.SEND_RESTRICTED_TEXTS):
            return "send_restricted_ui"
        return None

    def _build_restriction_result(self, reason, alert_text=None, name="", stage=""):
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            return None
        if normalized_reason == "stranger_message_limit_reached":
            return self._build_stranger_limit_result(
                name=name,
                stage=stage,
                alert_text=alert_text,
            )
        if normalized_reason == "message_request_limit_reached":
            return self._build_message_request_limit_result(
                name=name,
                stage=stage,
                alert_text=alert_text,
            )
        if self._is_skip_restriction_reason(normalized_reason):
            return self._build_skip_restriction_result(
                normalized_reason,
                name=name,
                stage=stage,
                alert_text=alert_text,
            )
        if normalized_reason == "send_restricted_ui":
            return self._build_send_restricted_result(
                name=name,
                stage=stage,
                alert_text=alert_text,
            )
        return None

    def _normalize_restriction_stage(self, stage):
        normalized_stage = str(stage or "").strip()
        if not normalized_stage:
            return ""
        stage_map = {
            "preflight": "成员开始前",
            "chat_open": "进入聊天后",
            "postsend": "发送后聊天窗",
            "probe": "点赞探针发送后",
            "点赞探针前复核": "点赞探针前复核",
            "发送后二次确认复核": "发送后二次确认复核",
            "发消息按钮点击失败后复核": "发消息按钮点击失败后复核",
            "聊天窗口首次复核": "聊天窗口首次复核",
            "聊天窗口二次复核": "聊天窗口二次复核",
            "聊天窗口重试复核": "聊天窗口重试复核",
            "聊天窗口被限制弹层阻断": "聊天窗口被限制弹层阻断",
            "聊天窗口被弹层阻断后最终复核": "聊天窗口被弹层阻断后最终复核",
            "聊天窗口未打开前最终复核": "聊天窗口未打开前最终复核",
            "聊天窗口发送前复核": "聊天窗口发送前复核",
        }
        return stage_map.get(normalized_stage, normalized_stage)

    def _build_restriction_payload(self, status, reason, alert_text="", log_text=""):
        payload = {
            "status": str(status or "").strip(),
            "reason": str(reason or "").strip(),
        }
        normalized_alert = str(alert_text or "").strip()
        normalized_log = str(log_text or "").strip()
        if normalized_alert:
            payload["alert_text"] = normalized_alert
        if normalized_log:
            payload["log"] = normalized_log
        return payload

    def _extract_restriction_payload(self, restriction, default_status=""):
        payload = restriction if isinstance(restriction, dict) else {}
        return (
            str(payload.get("status") or default_status).strip(),
            str(payload.get("reason") or "").strip(),
            str(payload.get("alert_text") or "").strip(),
        )

    def _build_skip_restriction_result(self, reason, name="", stage="", alert_text=None):
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            return None
        normalized_stage = self._normalize_restriction_stage(stage)
        normalized_name = str(name or "").strip()
        detail = self._sanitize_restriction_alert_text(normalized_reason, alert_text)
        if not detail:
            detail = self._default_skip_alert_text(normalized_reason)
        reason_label_map = {
            "messenger_login_required": "未登录 Messenger",
            "account_cannot_message": "当前无法私聊该成员",
        }
        reason_label = reason_label_map.get(normalized_reason, normalized_reason)
        log_text = ""
        if normalized_stage:
            log_text = f"{self._prefix()}{normalized_stage}命中{reason_label}限制，已跳过: {normalized_name}"
        return self._build_restriction_payload(
            "skip",
            normalized_reason,
            alert_text=detail,
            log_text=log_text,
        )

    def _build_stranger_limit_result(self, name="", stage="", alert_text=None):
        normalized_stage = self._normalize_restriction_stage(stage)
        normalized_name = str(name or "").strip()
        detail = self._sanitize_restriction_alert_text("stranger_message_limit_reached", alert_text)
        if not detail:
            if normalized_stage == "发送后聊天窗":
                detail = "聊天窗口在发送后明确提示陌生消息数量已达上限，当前窗口停止继续发送，请更换账户。"
            elif normalized_stage == "点赞探针发送后":
                detail = "点赞探针发送后明确提示陌生消息数量已达上限，当前窗口停止继续发送，请更换账户。"
            elif normalized_stage == "成员开始前":
                detail = self.STRANGER_LIMIT_ALERT_TEXT
            elif normalized_stage == "进入聊天后":
                detail = self.STRANGER_LIMIT_ALERT_TEXT
            else:
                detail = self.STRANGER_LIMIT_ALERT_TEXT
        log_text = ""
        if normalized_stage == "发送后聊天窗":
            log_text = f"{self._prefix()}发送后聊天窗明确提示陌生消息数量已达上限，停止当前窗口: {normalized_name}"
        elif normalized_stage == "点赞探针发送后":
            log_text = f"{self._prefix()}点赞探针发送后明确提示陌生消息数量已达上限，停止当前窗口: {normalized_name}"
        elif normalized_stage == "成员开始前":
            log_text = f"{self._prefix()}成员开始前检测到陌生消息数量已达上限，停止当前窗口: {normalized_name}"
        elif normalized_stage == "进入聊天后":
            log_text = f"{self._prefix()}进入聊天后检测到陌生消息数量已达上限，停止当前窗口: {normalized_name}"
        elif normalized_stage:
            log_text = f"{self._prefix()}{normalized_stage}命中陌生消息上限，停止当前窗口: {normalized_name}"
        return self._build_restriction_payload(
            "limit",
            "stranger_message_limit_reached",
            alert_text=detail,
            log_text=log_text,
        )

    def _build_message_request_limit_result(self, name="", stage="", alert_text=None):
        normalized_stage = self._normalize_restriction_stage(stage)
        normalized_name = str(name or "").strip()
        detail = self._sanitize_restriction_alert_text("message_request_limit_reached", alert_text)
        if not detail:
            if normalized_stage == "发送后聊天窗":
                detail = "聊天窗口在发送后明确提示消息请求发送达到上限，当前窗口停止继续发送，请更换账户。"
            elif normalized_stage == "点赞探针发送后":
                detail = "点赞探针发送后明确提示消息请求发送达到上限，当前窗口停止继续发送，请更换账户。"
            else:
                detail = self.MESSAGE_REQUEST_LIMIT_ALERT_TEXT
        log_text = ""
        if normalized_stage == "发送后聊天窗":
            log_text = f"{self._prefix()}发送后聊天窗明确提示消息请求发送达到上限，停止当前窗口: {normalized_name}"
        elif normalized_stage == "点赞探针发送后":
            log_text = f"{self._prefix()}点赞探针发送后明确提示消息请求发送达到上限，停止当前窗口: {normalized_name}"
        elif normalized_stage:
            log_text = f"{self._prefix()}{normalized_stage}命中消息请求发送上限，停止当前窗口: {normalized_name}"
        return self._build_restriction_payload(
            "blocked",
            "message_request_limit_reached",
            alert_text=detail,
            log_text=log_text,
        )

    def _build_send_restricted_result(self, name="", stage="", alert_text=None):
        normalized_stage = self._normalize_restriction_stage(stage)
        normalized_name = str(name or "").strip()
        detail = self._sanitize_restriction_alert_text("send_restricted_ui", alert_text)
        if not detail:
            if normalized_stage == "发送后聊天窗":
                detail = "聊天窗口在发送后明确提示当前无法发送，当前窗口停止继续发送，请更换账户。"
            elif normalized_stage == "点赞探针发送后":
                detail = "点赞探针发送后明确提示当前无法发送，当前窗口停止继续发送，请更换账户。"
            else:
                detail = "当前窗口已被系统限制继续发送消息，请更换账户。"
        log_text = ""
        if normalized_stage == "发送后聊天窗":
            log_text = f"{self._prefix()}发送后聊天窗明确提示无法发送，停止当前窗口: {normalized_name}"
        elif normalized_stage == "点赞探针发送后":
            log_text = f"{self._prefix()}点赞探针发送后明确提示无法发送，停止当前窗口: {normalized_name}"
        elif normalized_stage:
            log_text = f"{self._prefix()}{normalized_stage}命中当前无法发送提示，停止当前窗口: {normalized_name}"
        return self._build_restriction_payload(
            "blocked",
            "send_restricted_ui",
            alert_text=detail,
            log_text=log_text,
        )

    def _log_restriction_result(self, result):
        if not result:
            return
        log_text = str(result.get("log") or "").strip()
        if not log_text:
            return
        if str(result.get("status") or "").strip() == "skip":
            log.info(log_text)
            return
        log.warning(log_text)

    def _is_skip_restriction_reason(self, reason):
        normalized_reason = str(reason or "").strip()
        return normalized_reason in {
            "messenger_login_required",
            "account_cannot_message",
        }

    def _default_skip_alert_text(self, reason):
        normalized_reason = str(reason or "").strip()
        if normalized_reason == "messenger_login_required":
            return "对方当前未登录 Messenger，暂时无法发起对话，已跳过。"
        if normalized_reason == "account_cannot_message":
            return "当前无法向该成员发起私聊，已跳过。"
        return ""

    def _sanitize_restriction_alert_text(self, reason, alert_text):
        normalized_reason = str(reason or "").strip()
        detail = self._normalize_text(alert_text)
        if not detail:
            return ""
        if normalized_reason == "stranger_message_limit_reached":
            return self.STRANGER_LIMIT_ALERT_TEXT
        if normalized_reason == "message_request_limit_reached":
            return self.MESSAGE_REQUEST_LIMIT_ALERT_TEXT
        if self._is_skip_restriction_reason(normalized_reason):
            return self._default_skip_alert_text(normalized_reason)
        if normalized_reason == "send_restricted_ui":
            if (
                self._matches_any_text(detail, self.DELIVERY_FAILURE_TEXTS)
                or self._matches_any_text(detail, self.SEND_RESTRICTED_TEXTS)
            ):
                return "当前窗口已被系统限制继续发送消息，请更换账户。"
        return detail

    def _detect_prechat_restriction_result(
        self,
        name="",
        stage="",
    ):
        if self._check_stranger_limit_global():
            return self._build_stranger_limit_result(
                name=name,
                stage=stage,
            )

        page_restriction = self._detect_chat_restriction(
            expected_name=name,
            stage=stage,
        )
        if page_restriction:
            return page_restriction

        overlay_text = self._detect_blocking_overlay()
        overlay_restriction = self._build_restriction_result(
            self._match_restriction_reason_from_text(overlay_text or ""),
            overlay_text or "",
            name=name,
            stage=stage,
        )
        if overlay_restriction:
            return overlay_restriction
        return None

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
                    const chatCloseSelectors = payload.chatCloseSelectors || [];
                    const editorSelectors = payload.editorSelectors || [];
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
                    const matchesAnySelectorInside = (root, selectors) => {
                        for (const selector of selectors || []) {
                            try {
                                if (Array.from(root.querySelectorAll(selector)).some((el) => isVisibleArea(el))) {
                                    return true;
                                }
                            } catch (e) {}
                        }
                        return false;
                    };
                    const isChatContainer = (root) => {
                        if (!root) return false;
                        if (matchesAnySelectorInside(root, editorSelectors)) return true;
                        if (matchesAnySelectorInside(root, chatCloseSelectors)) return true;
                        return false;
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
                        if (isChatContainer(dialog)) continue;
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
                        return text.slice(0, 1200);
                    }
                    return null;
                }""",
                {
                    "labels": self.POPUP_CLOSE_LABELS,
                    "chatCloseSelectors": self.CHAT_CLOSE_SELECTORS,
                    "editorSelectors": self.MESSAGE_EDITOR_SELECTORS,
                },
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
                        """(payload) => {
                            const labels = payload.labels || [];
                            const chatCloseSelectors = payload.chatCloseSelectors || [];
                            const editorSelectors = payload.editorSelectors || [];
                            const strangerLimitTexts = payload.strangerLimitTexts || [];
                            const messageRequestLimitTexts = payload.messageRequestLimitTexts || [];
                            const messengerLoginRequiredTexts = payload.messengerLoginRequiredTexts || [];
                            const accountCannotMessageTexts = payload.accountCannotMessageTexts || [];
                            const sendRestrictedTexts = payload.sendRestrictedTexts || [];
                            const failureTexts = payload.failureTexts || [];
                            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                            const matchesProtectedText = (text) => {
                                const normalized = normalize(text);
                                if (!normalized) return false;
                                const lowerText = normalized.toLowerCase();
                                if (strangerLimitTexts.some((token) => normalized.includes(token))) return true;
                                if (messageRequestLimitTexts.some((token) => normalized.includes(token))) return true;
                                if (messengerLoginRequiredTexts.some((token) => normalized.includes(token))) return true;
                                if (accountCannotMessageTexts.some((token) => normalized.includes(token))) return true;
                                if (sendRestrictedTexts.some((token) => normalized.includes(token))) return true;
                                if (failureTexts.some((token) => normalized.includes(token))) return true;
                                if (
                                    (
                                        lowerText.includes('24 hours')
                                        && (lowerText.includes('message request') || lowerText.includes('requests'))
                                    )
                                    || (
                                        normalized.includes('24 ชั่วโมง')
                                        && (
                                            normalized.includes('คำขอส่งข้อความ')
                                            || normalized.includes('ส่งคำขอ')
                                            || (normalized.includes('คำขอ') && normalized.includes('ขีดจำกัด'))
                                        )
                                    )
                                ) {
                                    return true;
                                }
                                return false;
                            };
                            const isVisible = (el) => {
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                if (!rect.width || !rect.height) return false;
                                const style = window.getComputedStyle(el);
                                if (!style) return false;
                                return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
                            };
                            const matchesAnySelectorInside = (root, selectors) => {
                                for (const selector of selectors || []) {
                                    try {
                                        if (Array.from(root.querySelectorAll(selector)).some((el) => isVisible(el))) {
                                            return true;
                                        }
                                    } catch (e) {}
                                }
                                return false;
                            };
                            const isChatContainer = (root) => {
                                if (!root) return false;
                                if (matchesAnySelectorInside(root, editorSelectors)) return true;
                                if (matchesAnySelectorInside(root, chatCloseSelectors)) return true;
                                return false;
                            };
                            let closed = 0;
                            const nodes = Array.from(document.querySelectorAll('button, div[role="button"], [aria-label]'));
                            for (const el of nodes) {
                                if (el.offsetParent === null) continue;
                                const text = normalize(el.innerText);
                                const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
                                const combined = `${aria} ${text}`.trim();
                                if (!labels.some((token) => combined.includes(token))) continue;
                                const container = el.closest('[role="dialog"], [role="alertdialog"], [role="alert"], [role="status"], [aria-live], [role="presentation"]');
                                if (!container) continue;
                                const rect = container.getBoundingClientRect();
                                if (rect.width < 180 || rect.height < 60) continue;
                                if (rect.right < window.innerWidth * 0.4) continue;
                                if (isChatContainer(container)) continue;
                                const containerText = normalize(
                                    `${container.innerText || ''} ${container.getAttribute && container.getAttribute('aria-label') || ''}`
                                );
                                if (!containerText) continue;
                                if (matchesProtectedText(containerText)) continue;
                                try {
                                    el.click();
                                    closed += 1;
                                } catch (e) {}
                            }
                            return closed;
                        }""",
                        {
                            "labels": self.POPUP_CLOSE_LABELS,
                            "chatCloseSelectors": self.CHAT_CLOSE_SELECTORS,
                            "editorSelectors": self.MESSAGE_EDITOR_SELECTORS,
                            "strangerLimitTexts": self.STRANGER_LIMIT_TEXTS,
                            "messageRequestLimitTexts": self.MESSAGE_REQUEST_LIMIT_TEXTS,
                            "messengerLoginRequiredTexts": self.MESSENGER_LOGIN_REQUIRED_TEXTS,
                            "accountCannotMessageTexts": self.ACCOUNT_CANNOT_MESSAGE_TEXTS,
                            "sendRestrictedTexts": self.SEND_RESTRICTED_TEXTS,
                            "failureTexts": self.DELIVERY_FAILURE_TEXTS,
                        },
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

    def _build_message_button_skip_result(self, reason, name=""):
        normalized_reason = str(reason or "").strip()
        normalized_name = str(name or "").strip()
        reason_log_map = {
            "member_hover_timeout": f"{self._prefix()}等待成员资料卡超时，当前轮次跳过当前成员: {normalized_name}",
            "message_button_timeout": f"{self._prefix()}等待“发消息”按钮超时，当前轮次跳过当前成员: {normalized_name}",
            "message_button_not_found": f"{self._prefix()}当前成员资料卡未识别到可用的发消息按钮，当前轮次跳过: {normalized_name}",
        }
        return self._build_restriction_payload(
            "skip",
            normalized_reason,
            log_text=reason_log_map.get(
                normalized_reason,
                f"{self._prefix()}资料卡按钮阶段命中 {normalized_reason}，当前轮次跳过: {normalized_name}",
            ),
        )

    def _classify_message_button_failure(self, name, hover_card=None, timed_out=False):
        if hover_card is None:
            return self._build_message_button_skip_result("member_hover_timeout", name=name)

        if timed_out:
            return self._build_message_button_skip_result("message_button_timeout", name=name)

        return self._build_message_button_skip_result("message_button_not_found", name=name)

    def _build_dm_result(self, status, reason, alert_text=None):
        return self._build_restriction_payload(
            status,
            reason,
            alert_text=alert_text,
        )

    def _restriction_to_dm_result(self, restriction, default_status="skip"):
        status, reason, alert_text = self._extract_restriction_payload(
            restriction,
            default_status=default_status,
        )
        return self._build_dm_result(status, reason, alert_text)

    def _extract_chat_open_result(self, open_result):
        payload = open_result if isinstance(open_result, dict) else {}
        return (
            payload.get("chat_session"),
            payload.get("restriction"),
            bool(payload.get("shell_seen")),
        )

    def _build_chat_open_result(self, chat_session=None, restriction=None, shell_seen=False):
        return {
            "chat_session": chat_session,
            "restriction": restriction,
            "shell_seen": bool(shell_seen),
        }

    def _build_chat_session(self, editor, close_button=None):
        return {
            "editor": editor,
            "close_button": close_button,
        }

    def _extract_chat_session(self, session):
        payload = session or {}
        return payload.get("editor"), payload.get("close_button")

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
                        self._run_pause_aware_action(
                            lambda step_timeout: button.click(timeout=step_timeout),
                            timeout_ms=self._scale_ms(1500, min_ms=300),
                        )
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
                            self._run_pause_aware_action(
                                lambda step_timeout: btn.click(timeout=step_timeout),
                                timeout_ms=self._scale_ms(450, min_ms=180),
                            )
                            closed_any = True
                            closed_total += 1
                        except Exception:
                            continue
                except Exception:
                    continue
            try:
                js_closed = self.page.evaluate(
                    """(payload) => {
                        const normalize = (value) => (value || '').trim().toLowerCase();
                        const labels = new Set((payload.labels || []).map(normalize).filter(Boolean));
                        const nodes = Array.from(document.querySelectorAll('[aria-label]'));
                        let closed = false;
                        for (const el of nodes) {
                            const label = normalize(el.getAttribute('aria-label') || '');
                            if (!labels.has(label)) continue;
                            if (el.offsetParent === null) continue;
                            if (el.click) { el.click(); closed = true; }
                        }
                        return closed;
                    }""",
                    {"labels": self.GENERIC_CLOSE_LABELS},
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
        link_selector = str(target.get("link_selector") or "").strip()
        abs_top = target.get("abs_top")
        abs_left = target.get("abs_left")

        if link_selector:
            direct_locator = self._find_member_link_by_selector(link_selector)
            if direct_locator:
                return direct_locator

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

    def _find_member_link_by_selector(self, selector):
        try:
            locator = self.page.locator(selector).first
            if not locator.is_visible(timeout=self._scale_ms(260, min_ms=100)):
                return None
            box = self._safe_bounding_box(locator, timeout_ms=320)
            if not box:
                return None
            return locator
        except Exception:
            return None

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

    def _resolve_member_card_message_button(self, link, name):
        link_box = self._wait_for_stable_box(link, max_rounds=2, delay=0.06)
        if not link_box:
            return None, self._classify_message_button_failure(
                name,
                hover_card=None,
                timed_out=False,
            )

        hover_wait_total_ms = max(1, int(self.hover_card_wait_timeout_ms or 0))
        first_hover_wait_ms = max(1, int(hover_wait_total_ms * 0.65))
        second_hover_wait_ms = max(1, hover_wait_total_ms - first_hover_wait_ms)

        hover_card = self._find_hover_card_container(link_box)

        if hover_card is None:
            self._trigger_hover_card(link)
            hover_card = self._wait_for_condition(
                "成员资料卡",
                "等待资料卡区域渲染完成",
                lambda: self._find_hover_card_container(link_box),
                timeout_ms=first_hover_wait_ms,
                interval=0.12,
            )

        if hover_card is None:
            if self._detect_blocking_overlay():
                self._dismiss_interfering_popups(context=f"成员 {name} 资料卡识别阶段")
                self._dismiss_member_hover_card()
            try:
                self._run_pause_aware_action(
                    lambda step_timeout: link.scroll_into_view_if_needed(timeout=step_timeout),
                    timeout_ms=self._scale_ms(1200, min_ms=600),
                )
                link_box = self._wait_for_stable_box(link, max_rounds=2, delay=0.06) or link_box
            except Exception:
                pass
            hover_card = self._find_hover_card_container(link_box)
            if hover_card is None:
                self._trigger_hover_card(link)
                hover_card = self._wait_for_condition(
                    "成员资料卡",
                    "二次等待资料卡区域渲染完成",
                    lambda: self._find_hover_card_container(link_box),
                    timeout_ms=second_hover_wait_ms,
                    interval=0.12,
                )

        if hover_card is None:
            return None, self._classify_message_button_failure(
                name,
                hover_card=None,
                timed_out=False,
            )

        button_wait_ms = max(240, int(hover_wait_total_ms * 0.2))
        button = self._wait_for_condition(
            "成员资料卡",
            "等待发消息按钮渲染完成",
            lambda: self._find_message_button_in_container(hover_card),
            timeout_ms=button_wait_ms,
            interval=0.12,
        )
        if button:
            return button, None

        return None, self._classify_message_button_failure(
            name,
            hover_card=hover_card,
            timed_out=True,
        )

    def _collect_action_candidates_in_container(self, container):
        try:
            return container.evaluate(
                """(node, payload) => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const isVisible = (el) => {
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
                    const results = [];
                    const seen = new Set();
                    const nodes = Array.from(node.querySelectorAll(payload.selector));
                    for (let idx = 0; idx < nodes.length; idx += 1) {
                        const el = nodes[idx];
                        if (!isVisible(el)) continue;
                        if ((el.getAttribute && el.getAttribute('aria-disabled')) === 'true') continue;
                        const label = normalize(el.getAttribute && el.getAttribute('aria-label'));
                        const text = normalize(el.innerText || el.textContent || '');
                        const combined = normalize(`${label} ${text}`);
                        if (!combined) continue;
                        const dedupeKey = combined.toLowerCase();
                        if (seen.has(dedupeKey)) continue;
                        seen.add(dedupeKey);
                        results.push({index: idx, combined});
                    }
                    return results;
                }""",
                {"selector": self.ACTION_ELEMENT_SELECTOR},
            ) or []
        except Exception:
            return []

    def _find_message_button_in_container(self, container):
        candidates = self._collect_action_candidates_in_container(container)
        if not candidates:
            return None
        candidate_locator = container.locator(self.ACTION_ELEMENT_SELECTOR)
        for candidate in candidates:
            combined = self._normalize_text(candidate.get("combined") or "")
            candidate_index = candidate.get("index")
            if candidate_index is None:
                continue
            if (
                self._matches_any_text(combined, self.MEMBER_CARD_MESSAGE_TEXTS)
                or any(keyword in combined.lower() for keyword in self.MEMBER_CARD_MESSAGE_HINT_KEYWORDS)
            ):
                try:
                    return candidate_locator.nth(int(candidate_index))
                except Exception:
                    return None
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

    def _find_hover_card_container(self, link_box):
        if not link_box:
            return None
        link_cx = link_box["x"] + (link_box["width"] / 2)
        link_cy = link_box["y"] + (link_box["height"] / 2)
        viewport_size = getattr(self.page, "viewport_size", None) or {}
        viewport_w = viewport_size.get("width")
        viewport_h = viewport_size.get("height")
        if viewport_w is None or viewport_h is None:
            try:
                viewport_w = self.page.evaluate("window.innerWidth")
                viewport_h = self.page.evaluate("window.innerHeight")
            except Exception:
                viewport_w = None
                viewport_h = None
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
                    if box["width"] < 96 or box["height"] < 52:
                        continue
                    if viewport_w and box["width"] > viewport_w * 0.88:
                        continue
                    if viewport_h and box["height"] > viewport_h * 0.82:
                        continue
                    if viewport_w and viewport_h:
                        if (box["width"] * box["height"]) > (viewport_w * viewport_h * 0.48):
                            continue
                    card_cx = box["x"] + (box["width"] / 2)
                    card_cy = box["y"] + (box["height"] / 2)
                    dist = abs(card_cy - link_cy) + abs(card_cx - link_cx)
                    if dist > 1500:
                        continue
                    candidates.append((dist, box["y"], box["x"], card))
                except Exception:
                    continue
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _trigger_hover_card(self, link):
        triggered = False
        try:
            self._run_pause_aware_action(
                lambda step_timeout: link.hover(timeout=step_timeout),
                timeout_ms=self._scale_ms(900, min_ms=450),
                slice_ms=180,
            )
            triggered = True
        except Exception:
            pass
        try:
            link.evaluate(
                "node => node.dispatchEvent(new MouseEvent('mouseover', {bubbles:true}))"
            )
            triggered = True
        except Exception:
            pass
        return triggered

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

    def _wait_for_chat_open_result(self, expected_name=None, timeout_ms=10000, stage=""):
        timeout_ms = self._scale_ms(timeout_ms, min_ms=1600)
        start = time.time()
        deadline = start + timeout_ms / 1000
        shell_seen = False
        shell_seen_at = None
        shell_grace_applied = False
        shell_grace_ms = max(900, min(2600, int(timeout_ms * 0.35)))
        log.info(f"{self._prefix()}聊天窗口，等待聊天输入框或限制提示出现...")
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds
                if shell_seen_at is not None:
                    shell_seen_at += paused_seconds
            chat_session = self._locate_chat_session(expected_name=expected_name)
            if chat_session:
                waited = time.time() - start
                log.info(f"{self._prefix()}聊天窗口等待成功，耗时 {waited:.1f} 秒。")
                return self._build_chat_open_result(
                    chat_session=chat_session,
                    shell_seen=True,
                )
            restriction = self._detect_chat_restriction(
                expected_name=expected_name,
                stage=stage,
            )
            if restriction:
                waited = time.time() - start
                log.info(f"{self._prefix()}聊天窗口限制识别完成，耗时 {waited:.1f} 秒。")
                return self._build_chat_open_result(
                    restriction=restriction,
                    shell_seen=True,
                )
            shell = self._locate_chat_shell(expected_name=expected_name) or self._locate_chat_shell()
            if shell:
                if not shell_seen:
                    shell_seen = True
                    shell_seen_at = time.time()
                if (
                    shell_seen_at is not None
                    and not shell_grace_applied
                    and deadline - time.time() <= shell_grace_ms / 1000
                ):
                    deadline = max(deadline, shell_seen_at + shell_grace_ms / 1000)
                    shell_grace_applied = True
            self._sleep(0.12, min_seconds=0.02)
        waited = time.time() - start
        detail = "等待聊天输入框或限制提示出现失败"
        if shell_seen:
            detail = "右侧聊天壳已出现，但未识别到可用输入框或限制提示"
        log.warning(f"{self._prefix()}聊天窗口等待超时，已等待 {waited:.1f} 秒，{detail}")
        return self._build_chat_open_result(shell_seen=shell_seen)

    def _locate_chat_shell(self, expected_name=None):
        candidates = []
        try:
            viewport_size = self.page.viewport_size
            viewport_width = (
                viewport_size["width"]
                if viewport_size
                else self.page.evaluate("window.innerWidth")
            )
        except Exception:
            viewport_width = None
        for selector in self.CHAT_CLOSE_SELECTORS:
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count - 1, -1, -1):
                button = locator.nth(idx)
                try:
                    if not button.is_visible(timeout=self._scale_ms(200, min_ms=80)):
                        continue
                    box = self._safe_bounding_box(button, timeout_ms=320)
                    if not box:
                        continue
                    if viewport_width is not None and box["x"] < viewport_width * 0.55:
                        continue
                    shell_matches_name = False
                    if expected_name:
                        try:
                            shell_matches_name = bool(
                                button.evaluate(
                                    """(node, expectedName) => {
                                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                                        let el = node;
                                        while (el) {
                                            const rect = el.getBoundingClientRect();
                                            if (rect.width && rect.height && rect.right > window.innerWidth * 0.55) {
                                                const text = normalize(el.innerText || '');
                                                if (text && text.includes(expectedName)) return true;
                                            }
                                            el = el.parentElement;
                                        }
                                        return false;
                                    }""",
                                    expected_name,
                                )
                            )
                        except Exception:
                            shell_matches_name = False
                    score = 0
                    if shell_matches_name:
                        score += 1000
                    score += int(box["x"] or 0)
                    candidates.append((score, int(box["x"] or 0), -int(box["y"] or 0), button))
                except Exception:
                    continue
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        return candidates[0][3]

    def _locate_chat_session(self, expected_name=None):
        candidates = []
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

                    mentions_expected_name = (
                        bool(expected_name)
                        and self._editor_window_mentions_name(editor, expected_name)
                    )
                    active_window = self._editor_window_is_active(editor)
                    close_button = None
                    if mentions_expected_name:
                        close_button = self._find_chat_close_button(editor, expected_name)
                    generic_close_button = self._find_chat_close_button(editor, None)
                    close_button = close_button or generic_close_button

                    score = 0
                    if mentions_expected_name:
                        score += 1000
                    if active_window:
                        score += 500
                    if close_button:
                        score += 220
                    elif not mentions_expected_name and not active_window:
                        # 前置清场已经尽量收干净残留聊天窗，这里保留右侧可见 editor 作为弱候选，
                        # 避免聊天窗已打开却因为名字/激活态/关闭按钮尚未稳定而被误判成未打开。
                        score -= 260
                    score += int(editor_box["x"] or 0)
                    candidates.append(
                        (
                            score,
                            int(editor_box["x"] or 0),
                            -int(editor_box["y"] or 0),
                            self._build_chat_session(editor, close_button=close_button),
                        )
                    )
                except Exception:
                    continue
        if candidates:
            candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
            return candidates[0][3]
        return None

    def _wait_for_ready_chat_session(self, chat_session, expected_name=None, timeout_ms=None):
        timeout_ms = self._scale_ms(timeout_ms or self.editor_ready_wait_timeout_ms, min_ms=1500)
        start = time.time()
        deadline = start + timeout_ms / 1000
        current_session = chat_session
        log.info(f"{self._prefix()}聊天输入框，等待输入框可见且可写...")
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds

            editor, close_button = self._extract_chat_session(current_session)
            if editor and self._editor_ready(editor):
                waited = time.time() - start
                log.info(f"{self._prefix()}聊天输入框等待成功，耗时 {waited:.1f} 秒。")
                return self._build_chat_session(editor, close_button=close_button)

            refreshed_session = self._locate_chat_session(expected_name=expected_name) or self._locate_chat_session()
            if refreshed_session:
                refreshed_editor, refreshed_close_button = self._extract_chat_session(refreshed_session)
                merged_close_button = refreshed_close_button or close_button
                current_session = self._build_chat_session(
                    refreshed_editor,
                    close_button=merged_close_button,
                )
                if refreshed_editor and self._editor_ready(refreshed_editor):
                    waited = time.time() - start
                    log.info(f"{self._prefix()}聊天输入框等待成功，耗时 {waited:.1f} 秒。")
                    return current_session

            self._sleep(0.12, min_seconds=0.02)
        waited = time.time() - start
        log.warning(f"{self._prefix()}聊天输入框等待超时，已等待 {waited:.1f} 秒，等待输入框可见且可写失败")
        return None

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

    def _editor_window_is_active(self, editor):
        try:
            return bool(
                editor.evaluate(
                    """(node) => {
                        if (!node) return false;
                        const active = document.activeElement;
                        if (!active) return false;
                        if (node === active) return true;
                        if (node.contains && node.contains(active)) return true;
                        let el = node;
                        while (el) {
                            if (el === active) return true;
                            if (el.contains && el.contains(active)) return true;
                            const rect = el.getBoundingClientRect();
                            if (rect.width && rect.height && rect.right > window.innerWidth * 0.55) {
                                if (el.contains && el.contains(active)) return true;
                            }
                            el = el.parentElement;
                        }
                        return false;
                    }"""
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
            self._run_pause_aware_action(
                lambda step_timeout: close_button.click(timeout=step_timeout),
                timeout_ms=self._scale_ms(1500, min_ms=300),
            )
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
